//! The mapped region: lifecycle (create, attach, rebuild, remove), the seqno and watermark
//! protocol, view publication and the reader-slot table. Layout per `FORMAT.md` §11; the
//! offsets come from `pigeonhole_format::shm`.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use pigeonhole_format::shm::{
    DIRECTORY_MAGIC, HEADER_LEN, READER_SLOT_LEN, ROW_EPOCH_LEN, ShmHeader, ViewRecord,
    WATERMARK_STRIDE, directory, directory_name, header, reader_slot, region_name, watermark,
};
use pigeonhole_format::superblock::READER_LOCK_BASE;
use pigeonhole_format::{ManifestVersion, Seqno, ShmLayoutVersion};
use pigeonhole_io::{
    ErrorKind, FileIdentity, FileRef, LockMode, ProcessId, SharedOpen, SharedRegion, VfsRef,
};

use crate::lock::{ShmInit, alone, present};
use crate::{Error, Generation, ReaderSlot, Result, Role, ShmConfig};

/// `state` values of the region header.
const STATE_INITIALIZING: u32 = 0;
const STATE_READY: u32 = 1;
const STATE_ABANDONED: u32 = 2;

/// `state` values of a reader slot.
const SLOT_FREE: u32 = 0;
const SLOT_CLAIMING: u32 = 1;
const SLOT_ACTIVE: u32 = 2;

/// Directory `format` field.
const DIRECTORY_FORMAT: u32 = 1;

/// Longest region name the smallest platform limit accepts: macOS `shm_open` allows 31 bytes
/// including the leading `/` it needs.
const MAX_REGION_NAME: usize = 30;

/// How long a reader waits for a region still marked `initializing`. The builder holds the
/// shm-init lock until the region is ready and records its generation in the directory only
/// then, so this is a guard against a corrupt directory, not a normal wait.
const READY_WAIT_ATTEMPTS: u32 = 100;
const READY_WAIT_DELAY: std::time::Duration = std::time::Duration::from_millis(1);

/// Consecutive bad copies of an unchanged view buffer before `read_view` gives up.
const VIEW_READ_ATTEMPTS: u32 = 8;

/// Size of the fixed part of a view record (`FORMAT.md` §11.4).
const VIEW_HEADER_LEN: usize = 32;

/// The mapped region for one database.
///
/// Cheap to clone: every clone shares one mapping. See the [crate docs](crate) for the
/// lifecycle and an example.
#[derive(Debug, Clone)]
pub struct ShmRegion {
    inner: Arc<Inner>,
}

/// Everything a mapping needs, shared by the region's clones and its reader slots.
#[derive(Debug)]
pub(crate) struct Inner {
    region: SharedRegion,
    /// The directory region; `None` for [`ShmRegion::in_memory`].
    directory: Option<SharedRegion>,
    header: ShmHeader,
    generation: Generation,
    db_id: [u8; 16],
    identity: FileIdentity,
    config: ShmConfig,
    /// Serializes in-process publishers (`publish_view` writes the inactive buffer, then
    /// swaps). Across processes the writer lock already allows one publisher.
    /// Serializes view publishes; holds the encoding buffer they reuse (#499).
    publish_lock: Mutex<Vec<u8>>,
}

impl Inner {
    fn h32(&self, offset: usize) -> &AtomicU32 {
        self.region.atomic_u32(offset)
    }

    fn h64(&self, offset: usize) -> &AtomicU64 {
        self.region.atomic_u64(offset)
    }

    fn pending(&self, shard: u32) -> &AtomicU64 {
        assert!(
            shard < self.header.shard_count,
            "shard {shard} out of range (region has {} shards)",
            self.header.shard_count
        );
        let off = self.header.watermarks_off as usize + WATERMARK_STRIDE * shard as usize;
        self.region.atomic_u64(off + watermark::PENDING)
    }

    fn durable(&self, shard: u32) -> &AtomicU64 {
        assert!(
            shard < self.header.shard_count,
            "shard {shard} out of range (region has {} shards)",
            self.header.shard_count
        );
        let off = self.header.watermarks_off as usize + WATERMARK_STRIDE * shard as usize;
        self.region.atomic_u64(off + watermark::DURABLE)
    }

    fn slot_off(&self, index: u32) -> usize {
        self.header.reader_slots_off as usize + READER_SLOT_LEN * index as usize
    }

    fn slot32(&self, index: u32, field: usize) -> &AtomicU32 {
        self.region.atomic_u32(self.slot_off(index) + field)
    }

    fn slot64(&self, index: u32, field: usize) -> &AtomicU64 {
        self.region.atomic_u64(self.slot_off(index) + field)
    }

    fn view_pointer(&self, order: Ordering) -> u64 {
        self.h64(header::VIEW_POINTER).load(order)
    }

    /// `NEXT_SEQNO` then `CROSS_HELD` (Acquire, in that order: FORMAT §11.3), through one
    /// checked run of header words rather than two checked accesses.
    #[inline(always)]
    fn next_and_held(&self) -> (Seqno, Seqno) {
        const WORDS: usize = (header::CROSS_HELD - header::NEXT_SEQNO) / 8 + 1;
        const _: () = assert!(
            header::CROSS_HELD > header::NEXT_SEQNO
                && (header::CROSS_HELD - header::NEXT_SEQNO).is_multiple_of(8)
        );
        let words = self.region.atomic_u64s(header::NEXT_SEQNO, WORDS);
        let next = words[0].load(Ordering::Acquire);
        // After `NEXT_SEQNO`: a decision holds its seqno here before the mirror passes it.
        (next, words[WORDS - 1].load(Ordering::Acquire))
    }

    fn visible_seqno(&self) -> Seqno {
        let (next, held) = self.next_and_held();
        let mut lowest = next.min(held);
        for shard in 0..self.header.shard_count {
            lowest = lowest.min(self.pending(shard).load(Ordering::Acquire));
        }
        lowest.saturating_sub(1)
    }

    /// As [`visible_seqno`](Self::visible_seqno), also below every shard's durable
    /// watermark: never above the visible seqno.
    fn durable_seqno(&self) -> Seqno {
        let (next, held) = self.next_and_held();
        let mut lowest = next.min(held);
        for shard in 0..self.header.shard_count {
            lowest = lowest
                .min(self.pending(shard).load(Ordering::Acquire))
                .min(self.durable(shard).load(Ordering::Acquire));
        }
        lowest.saturating_sub(1)
    }

    fn clear_pins(&self, index: u32) {
        self.slot64(index, reader_slot::PINNED_SEQNO)
            .store(0, Ordering::SeqCst);
        self.slot64(index, reader_slot::PINNED_VIEW)
            .store(0, Ordering::SeqCst);
    }

    /// Whether slot `index` is claimed (not free) and records `owner` as its process.
    pub(crate) fn slot_owned_by(&self, index: u32, owner: ProcessId) -> bool {
        self.slot32(index, reader_slot::STATE)
            .load(Ordering::Acquire)
            != SLOT_FREE
            && self.slot32(index, reader_slot::PID).load(Ordering::Relaxed) == owner.pid
            && self
                .slot64(index, reader_slot::START_TIME)
                .load(Ordering::Relaxed)
                == owner.start_time
    }

    /// The owner frees its slot: pins first, so a concurrent
    /// [`ShmRegion::oldest_reader_pin`] never sees a free slot still pinning, then `active`
    /// to `free`. The CAS fails only while the writer has the slot in `claiming` to check
    /// its owner's liveness ([`ShmRegion::reclaim_dead_slots_locked`]); the writer puts it
    /// back to `active` at once because the owner is alive, so retry. If the writer died in that
    /// window the slot stays `claiming`; give up and leave it to the next generation.
    ///
    /// Every store an owner makes to its slot (here, and its pins) is to its own slot
    /// (#693 PR 3, perf287's review of #894): a slot claimed with its liveness lock
    /// ([`ShmRegion::claim_reader_slot_locked`]) is freed only by a handle that has taken
    /// that lock ([`ShmRegion::reclaim_dead_slots_locked`]), and the owner holds it from
    /// before its claim until after this release. So the slot cannot be freed and handed to
    /// another reader under a live owner. The ownership check below is a guard for a handle
    /// whose slot was freed after it gave up its lock ([`ReaderSlot::abandon`]).
    fn release_slot(&self, index: u32, owner: ProcessId) {
        if !self.slot_owned_by(index, owner) {
            return;
        }
        self.clear_pins(index);
        let state = self.slot32(index, reader_slot::STATE);
        for attempt in 0..RELEASE_ATTEMPTS {
            if state
                .compare_exchange(SLOT_ACTIVE, SLOT_FREE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
            if attempt < 64 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }
}

/// Retries of the owner's `active -> free` transition while the writer probes the slot.
const RELEASE_ATTEMPTS: u32 = 64 + 200;

/// Maps a failure to create a region: `Unavailable` when it could not be allocated (no
/// space, or no such directory: `/dev/shm` missing in a distroless or Lambda container, or a
/// bad `shm_dir`), `Io` for anything else.
fn unavailable(e: pigeonhole_io::Error) -> Error {
    match e.kind {
        ErrorKind::NoSpace | ErrorKind::NotFound | ErrorKind::Other => Error::Unavailable,
        _ => Error::Io(e),
    }
}

fn io_error(kind: ErrorKind, context: &'static str) -> Error {
    Error::Io(pigeonhole_io::Error::new(kind, context))
}

/// Writes the header, including the fields `ShmHeader` leaves to this crate, and idles every
/// watermark. `state` stays `initializing`; the caller stores `ready` once everything is in.
/// A seed for a new generation's row-epoch hash (D281): unpredictable from outside the
/// process (`foldhash`'s per-process random state), different per generation.
fn row_epoch_seed(generation: Generation, writer: ProcessId) -> u64 {
    use std::hash::BuildHasher;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    pigeonhole_format::hash::FastBuildHasher::default().hash_one((
        generation.0,
        writer.pid,
        writer.start_time,
        now,
    ))
}

fn init_region(
    region: &SharedRegion,
    h: &ShmHeader,
    generation: Generation,
    writer: ProcessId,
    first_seqno: Seqno,
) {
    let mut page = [0u8; HEADER_LEN];
    h.encode(&mut page);
    page[header::GENERATION..header::GENERATION + 8].copy_from_slice(&generation.0.to_le_bytes());
    page[header::WRITER_PID..header::WRITER_PID + 4].copy_from_slice(&writer.pid.to_le_bytes());
    page[header::WRITER_START_TIME..header::WRITER_START_TIME + 8]
        .copy_from_slice(&writer.start_time.to_le_bytes());
    page[header::NEXT_SEQNO..header::NEXT_SEQNO + 8]
        .copy_from_slice(&first_seqno.max(1).to_le_bytes());
    page[header::CROSS_HELD..header::CROSS_HELD + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    region.write(0, &page);
    for shard in 0..h.shard_count {
        let off = h.watermarks_off as usize + WATERMARK_STRIDE * shard as usize;
        for field in [watermark::PENDING, watermark::DURABLE] {
            region
                .atomic_u64(off + field)
                .store(u64::MAX, Ordering::Relaxed);
        }
    }
}

/// Attaches to the directory region, creating and initializing it if absent. The caller
/// holds the shm-init lock, so a zero magic (just created, or left by a creator that died
/// before writing it) is safe to initialize here.
fn open_directory(
    vfs: &VfsRef,
    identity: FileIdentity,
    dir: Option<&Path>,
) -> Result<SharedRegion> {
    let name = directory_name(identity.device, identity.inode);
    let len = directory::LEN as u64;
    let region = match vfs.open_shared(&name, dir, len, SharedOpen::Attach) {
        Ok(r) => r,
        Err(e) if e.kind == ErrorKind::NotFound => {
            match vfs.open_shared(&name, dir, len, SharedOpen::CreateNew) {
                Ok(r) => r,
                Err(e) if e.kind == ErrorKind::AlreadyExists => {
                    vfs.open_shared(&name, dir, len, SharedOpen::Attach)?
                }
                Err(e) => return Err(unavailable(e)),
            }
        }
        Err(e) => return Err(e.into()),
    };
    let mut magic = [0u8; 8];
    region.read(directory::MAGIC, &mut magic);
    if magic == [0u8; 8] {
        region.write(directory::FORMAT, &DIRECTORY_FORMAT.to_le_bytes());
        region.write(directory::MAGIC, &DIRECTORY_MAGIC);
    } else if magic != DIRECTORY_MAGIC {
        return Err(Error::Corrupt("shared-memory directory magic"));
    }
    let mut format = [0u8; 4];
    region.read(directory::FORMAT, &mut format);
    if u32::from_le_bytes(format) != DIRECTORY_FORMAT {
        return Err(Error::Corrupt("shared-memory directory format"));
    }
    Ok(region)
}

/// Reads and validates the header of `region` (mapped for at least [`HEADER_LEN`] bytes).
fn read_header(
    region: &SharedRegion,
    db_id: [u8; 16],
    identity: FileIdentity,
    generation: Generation,
) -> Result<ShmHeader> {
    let mut page = [0u8; HEADER_LEN];
    region.read(0, &mut page);
    let h = ShmHeader::decode(&page)?;
    let found = u64::from_le_bytes(
        page[header::GENERATION..header::GENERATION + 8]
            .try_into()
            .expect("8 bytes"),
    );
    if found != generation.0 {
        return Err(Error::Corrupt("region generation does not match its name"));
    }
    if h.db_id != db_id {
        return Err(Error::Corrupt("region belongs to another database"));
    }
    if h.file_device != identity.device || h.file_inode != identity.inode {
        return Err(Error::Corrupt("region belongs to another file"));
    }
    Ok(h)
}

/// Attaches to generation `generation` named by the directory (reader open and re-attach).
fn attach(
    vfs: &VfsRef,
    directory: SharedRegion,
    identity: FileIdentity,
    db_id: [u8; 16],
    config: &ShmConfig,
    generation: Generation,
) -> Result<Inner> {
    if generation.0 == 0 {
        // No writer has built a region (or the last one removed the directory's generation).
        return Err(Error::NoWriter);
    }
    let found = directory
        .atomic_u32(directory::LAYOUT_VERSION)
        .load(Ordering::Acquire);
    if found != ShmLayoutVersion::CURRENT.0 {
        return Err(Error::VersionMismatch {
            found,
            expected: ShmLayoutVersion::CURRENT.0,
        });
    }
    let dir = config.dir.as_deref();
    let name = region_name(identity.device, identity.inode, generation.0);
    // The generation the directory names may be gone: its writer closed and no process
    // maps it (on Windows a mapping disappears with its last handle, #844). Only that is
    // `NoWriter`; any other failure stays an I/O error.
    let no_writer = |e: pigeonhole_io::Error| match e.kind {
        ErrorKind::NotFound => Error::NoWriter,
        _ => Error::Io(e),
    };
    let probe = vfs
        .open_shared(&name, dir, HEADER_LEN as u64, SharedOpen::Attach)
        .map_err(no_writer)?;
    let h = read_header(&probe, db_id, identity, generation)?;
    let len = usize::try_from(h.region_len).map_err(|_| Error::Corrupt("region length"))?;
    let region = if probe.len() >= len {
        probe
    } else {
        vfs.open_shared(&name, dir, h.region_len, SharedOpen::Attach)
            .map_err(no_writer)?
    };
    let state = region.atomic_u32(header::STATE);
    let mut waited = 0;
    loop {
        match state.load(Ordering::Acquire) {
            STATE_READY => break,
            STATE_ABANDONED => return Err(Error::Stale),
            STATE_INITIALIZING if waited < READY_WAIT_ATTEMPTS => {
                waited += 1;
                std::thread::sleep(READY_WAIT_DELAY);
            }
            STATE_INITIALIZING => return Err(Error::Corrupt("region never became ready")),
            _ => return Err(Error::Corrupt("region state")),
        }
    }
    Ok(Inner {
        region,
        directory: Some(directory),
        header: h,
        generation,
        db_id,
        identity,
        config: config.clone(),
        publish_lock: Mutex::new(Vec::new()),
    })
}

/// Builds a new generation (writer open): creates and initializes the new region, marks the
/// old one abandoned, records the new generation in the directory, and removes the old name.
fn build(
    vfs: &VfsRef,
    file: &FileRef,
    directory: SharedRegion,
    identity: FileIdentity,
    db_id: [u8; 16],
    config: &ShmConfig,
    current: u64,
) -> Result<Inner> {
    let dir = config.dir.as_deref();
    let old = if current == 0 {
        None
    } else {
        let name = region_name(identity.device, identity.inode, current);
        match vfs.open_shared(&name, dir, HEADER_LEN as u64, SharedOpen::Attach) {
            Ok(old) => {
                // A live region with another layout version is refused unless no other
                // process has the database open (then nobody can be attached to it).
                let found = old
                    .atomic_u32(header::LAYOUT_VERSION)
                    .load(Ordering::Acquire);
                if found != ShmLayoutVersion::CURRENT.0 && !alone(file)? {
                    return Err(Error::VersionMismatch {
                        found,
                        expected: ShmLayoutVersion::CURRENT.0,
                    });
                }
                Some(old)
            }
            Err(e) if e.kind == ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        }
    };
    // Present before the new generation is published, so a closing process cannot pass
    // `Presence::try_become_last` and remove it. After the probe above this is a no-op.
    present(file)?;

    let generation = Generation(
        current
            .checked_add(1)
            .ok_or(Error::Corrupt("generation counter exhausted"))?,
    );
    let name = region_name(identity.device, identity.inode, generation.0);
    if name.len() > MAX_REGION_NAME {
        return Err(io_error(
            ErrorKind::Other,
            "shared-memory region name exceeds 30 bytes (31 with the leading `/`)",
        ));
    }
    let mut h = ShmHeader::layout(
        db_id,
        config.shards,
        config.reader_slots,
        config.view_buffer_bytes,
        config.arena_bytes,
        config.row_epoch_slots,
        identity.device,
        identity.inode,
    );
    h.row_epoch_seed = row_epoch_seed(generation, vfs.current_process());
    if config.row_epochs_maintained {
        h.row_epoch_flags |= pigeonhole_format::shm::ROW_EPOCHS_MAINTAINED;
    }
    let region = match vfs.open_shared(&name, dir, h.region_len, SharedOpen::CreateNew) {
        Ok(r) => r,
        Err(e) if e.kind == ErrorKind::AlreadyExists => {
            // Left by a writer that died after creating it and before recording it in the
            // directory, so no process can be attached to it: replace it.
            vfs.remove_shared(&name, dir)?;
            vfs.open_shared(&name, dir, h.region_len, SharedOpen::CreateNew)
                .map_err(unavailable)?
        }
        Err(e) => return Err(unavailable(e)),
    };
    init_region(
        &region,
        &h,
        generation,
        vfs.current_process(),
        config.first_seqno,
    );
    region
        .atomic_u32(header::STATE)
        .store(STATE_READY, Ordering::Release);
    if let Some(old) = &old {
        old.atomic_u32(header::STATE)
            .store(STATE_ABANDONED, Ordering::Release);
    }
    directory
        .atomic_u32(directory::LAYOUT_VERSION)
        .store(ShmLayoutVersion::CURRENT.0, Ordering::Release);
    directory
        .atomic_u64(directory::GENERATION)
        .store(generation.0, Ordering::Release);
    if old.is_some() {
        // Mappings other processes still hold stay valid; only the name goes.
        let _ = vfs.remove_shared(&region_name(identity.device, identity.inode, current), dir);
    }
    Ok(Inner {
        region,
        directory: Some(directory),
        header: h,
        generation,
        db_id,
        identity,
        config: config.clone(),
        publish_lock: Mutex::new(Vec::new()),
    })
}

impl ShmRegion {
    /// Creates or attaches to the region for the database file `identity`, holding the
    /// shm-init lock byte on `file` while creating or validating. Finds the current
    /// generation through the directory region (FORMAT §11). A writer always builds a new
    /// generation: it creates the new region, marks the old one abandoned, then records the
    /// new generation in the directory. Refuses a live region with another layout version
    /// ([`Error::VersionMismatch`]) unless no other process is attached.
    ///
    /// `file` must be opened for writing in both roles (the shm-init byte is an exclusive
    /// lock).
    ///
    /// **Writer call order:** [`WriterLock::acquire`](crate::WriterLock::acquire), then this,
    /// then [`Presence::acquire`](crate::Presence::acquire) on the same handle (decision
    /// D37). The writer role takes the presence byte shared itself, after the layout-version
    /// probe and before it publishes the new generation, so a closing process can never
    /// remove a generation being built; the later `Presence::acquire` only returns the
    /// guard. Taking `Presence` first would make the probe upgrade a lock the writer must
    /// keep, which Windows cannot do atomically. If this fails, the presence byte may stay
    /// held until `file` is closed. A reader with no region to attach to (no writer has built one yet) gets
    /// [`Error::Io`] with `ErrorKind::NotFound`. A writer's new generation starts its seqno
    /// counter at `config.first_seqno` (ICR 0002). `config` must pass
    /// [`ShmConfig::validate`].
    pub fn open(
        vfs: &VfsRef,
        file: &FileRef,
        identity: FileIdentity,
        db_id: [u8; 16],
        role: Role,
        config: &ShmConfig,
    ) -> Result<ShmRegion> {
        config.validate()?;
        let _init = ShmInit::acquire(file)?;
        let directory = open_directory(vfs, identity, config.dir.as_deref())?;
        let current = directory
            .atomic_u64(directory::GENERATION)
            .load(Ordering::Acquire);
        let inner = match role {
            Role::Reader => attach(vfs, directory, identity, db_id, config, Generation(current))?,
            Role::Writer => build(vfs, file, directory, identity, db_id, config, current)?,
        };
        Ok(ShmRegion {
            inner: Arc::new(inner),
        })
    }

    /// A private heap-backed region with the same layout (the mock for engine tests).
    ///
    /// Generation 1, never stale; [`ShmRegion::reattach`] returns a clone. Panics if
    /// `config` fails [`ShmConfig::validate`].
    pub fn in_memory(db_id: [u8; 16], config: &ShmConfig) -> ShmRegion {
        if let Err(e) = config.validate() {
            panic!("{e}");
        }
        let identity = FileIdentity {
            device: 0,
            inode: 0,
        };
        let mut h = ShmHeader::layout(
            db_id,
            config.shards,
            config.reader_slots,
            config.view_buffer_bytes,
            config.arena_bytes,
            config.row_epoch_slots,
            identity.device,
            identity.inode,
        );
        h.row_epoch_seed = row_epoch_seed(
            Generation(1),
            ProcessId {
                pid: 0,
                start_time: 0,
            },
        );
        if config.row_epochs_maintained {
            h.row_epoch_flags |= pigeonhole_format::shm::ROW_EPOCHS_MAINTAINED;
        }
        let len = usize::try_from(h.region_len).expect("region fits in memory");
        let region = SharedRegion::heap(len);
        let generation = Generation(1);
        init_region(
            &region,
            &h,
            generation,
            ProcessId {
                pid: 0,
                start_time: 0,
            },
            config.first_seqno,
        );
        region
            .atomic_u32(header::STATE)
            .store(STATE_READY, Ordering::Release);
        ShmRegion {
            inner: Arc::new(Inner {
                region,
                directory: None,
                header: h,
                generation,
                db_id,
                identity,
                config: config.clone(),
                publish_lock: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Removes the region's and the directory's names (by the last process, after
    /// [`Presence::try_become_last`](crate::Presence::try_become_last)). Mappings other
    /// processes still hold stay valid. Nothing to remove is not an error.
    pub fn remove(vfs: &VfsRef, identity: FileIdentity, dir: Option<&Path>) -> Result<()> {
        let name = directory_name(identity.device, identity.inode);
        let directory = match vfs.open_shared(&name, dir, directory::LEN as u64, SharedOpen::Attach)
        {
            Ok(d) => d,
            Err(e) if e.kind == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let mut magic = [0u8; 8];
        directory.read(directory::MAGIC, &mut magic);
        if magic == DIRECTORY_MAGIC {
            let generation = directory
                .atomic_u64(directory::GENERATION)
                .load(Ordering::Acquire);
            if generation != 0 {
                let region = region_name(identity.device, identity.inode, generation);
                match vfs.remove_shared(&region, dir) {
                    Ok(()) => {}
                    Err(e) if e.kind == ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        match vfs.remove_shared(&name, dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Whether a newer generation replaced this mapping (its state is abandoned or the
    /// directory names another generation). Cheap: two atomic loads.
    pub fn is_stale(&self) -> bool {
        let Some(directory) = &self.inner.directory else {
            return false;
        };
        self.inner.h32(header::STATE).load(Ordering::Acquire) != STATE_READY
            || directory
                .atomic_u64(directory::GENERATION)
                .load(Ordering::Acquire)
                != self.inner.generation.0
    }

    /// Attaches to the current generation (reader processes, after [`ShmRegion::is_stale`]).
    /// The caller re-claims its reader slot and re-pins in the new region.
    pub fn reattach(&self, vfs: &VfsRef, file: &FileRef) -> Result<ShmRegion> {
        if self.inner.directory.is_none() {
            return Ok(self.clone());
        }
        ShmRegion::open(
            vfs,
            file,
            self.inner.identity,
            self.inner.db_id,
            Role::Reader,
            &self.inner.config,
        )
    }

    /// This generation's row-epoch table (D281): the write watermarks of every process's row
    /// cache, with the seed of the hash that picks a row's slot. Both come from this region's
    /// header, validated at attach, so a reader never pairs one generation's seed with
    /// another's table.
    pub fn row_epochs(&self) -> RowEpochTable {
        let h = &self.inner.header;
        RowEpochTable {
            region: self.inner.region.clone(),
            off: h.row_epochs_off as usize,
            mask: u64::from(h.row_epoch_slots) - 1,
            seed: h.row_epoch_seed,
            maintained: h.row_epoch_flags & pigeonhole_format::shm::ROW_EPOCHS_MAINTAINED != 0,
        }
    }

    /// Current generation.
    pub fn generation(&self) -> Generation {
        self.inner.generation
    }

    /// Shard count of the region's layout.
    pub fn shard_count(&self) -> u32 {
        self.inner.header.shard_count
    }

    /// The underlying mapping and the `(offset, len)` of `shard`'s arena within it, for
    /// `pigeonhole_memtable::ArenaRegion::new`. Panics if `shard` is out of range.
    pub fn arena(&self, shard: u32) -> (SharedRegion, usize, usize) {
        let h = &self.inner.header;
        assert!(
            shard < h.shard_count,
            "shard {shard} out of range (region has {} shards)",
            h.shard_count
        );
        let len = h.arena_len as usize;
        let offset = h.arenas_off as usize + len * shard as usize;
        (self.inner.region.clone(), offset, len)
    }

    /// The `(offset, len)` of every arena together: the region's tail, from the first
    /// arena's start to the end.
    fn arenas_range(&self) -> (usize, usize) {
        let h = &self.inner.header;
        (
            h.arenas_off as usize,
            h.arena_len as usize * h.shard_count as usize,
        )
    }

    /// Frees the memory of every arena ([`SharedRegion::discard`]): the header, the
    /// watermarks, the view buffers and the reader slots stay intact, so the region's
    /// protocol keeps working, and a later read of an arena sees zeros. For a writer
    /// releasing a closed instance's memory (D265), once no one still reads the arenas as
    /// data: in this process, no view that lists a memtable lives; in other processes,
    /// none can take the region for current ([`ShmRegion::is_superseded`]), so a reader
    /// still attached fails its reads with the engine's `SnapshotExpired` after reading
    /// them, and `pigeonhole-memtable`'s reader never faults on zeroed pages. That a reader
    /// which saw a zero also sees the new generation is the kernel's doing, not the memory
    /// model's: the writer stores the new generation in the directory (Release) before the
    /// `madvise`, which zaps every process's page-table entries for the range and waits for
    /// the TLB shootdowns before it returns, so a reader sees zeros only by faulting after
    /// that, and the fault and the shootdown's interrupt are full barriers ahead of its
    /// `Acquire` fence and directory load. Linux only: an error elsewhere
    /// ([`pigeonhole_io::ErrorKind::Unsupported`] on macOS and Windows, `EOPNOTSUPP` from a
    /// `shm_dir` file system without hole punching), and nothing is freed.
    pub fn discard_arenas(&self) -> Result<()> {
        let (offset, len) = self.arenas_range();
        Ok(self.inner.region.discard(offset, len)?)
    }

    /// How many bytes of the arenas are resident in memory
    /// ([`SharedRegion::resident_bytes`]; Linux mappings only). A test probe for
    /// [`ShmRegion::discard_arenas`].
    pub fn arena_resident_bytes(&self) -> Result<usize> {
        let (offset, len) = self.arenas_range();
        Ok(self.inner.region.resident_bytes(offset, len)?)
    }

    /// Whether no reader process can take this mapping for the current generation: an
    /// in-memory region (no other process can attach to it), or a mapping a newer
    /// generation replaced ([`ShmRegion::is_stale`]). A reader attached to such a region
    /// fails its reads with `SnapshotExpired` and re-attaches (D148).
    pub fn is_superseded(&self) -> bool {
        self.inner.directory.is_none() || self.is_stale()
    }

    // ---- seqnos and watermarks ----

    /// Raises the next seqno to reserve to at least `to` (`fetch_max`, release ordering): the
    /// mirror of a writer that reserves seqnos in its own process (D271, ICR 0063). It never
    /// moves past a reservation the writer made, and the writer published its pending
    /// watermark before reserving, so a reader that sees the mirror at `to` sees that
    /// watermark too (`FORMAT.md` §11.3).
    pub fn advance_next_seqno(&self, to: Seqno) {
        self.inner
            .h64(header::NEXT_SEQNO)
            .fetch_max(to, Ordering::AcqRel);
    }

    /// Publishes `shard`'s pending watermark with release ordering: its current group's
    /// lower bound; `u64::MAX` when it holds nothing, so an idle shard never holds back
    /// snapshots.
    pub fn publish_pending(&self, shard: u32, pending: Seqno) {
        self.inner.pending(shard).store(pending, Ordering::Release);
    }

    /// Publishes the lowest decided cross-shard seqno not yet applied on every participant
    /// (layout version 6, D278), with release ordering; `u64::MAX` when none. Snapshots stay
    /// below it. One writer at a time: the writer process's allocator stores it under its
    /// lock, before the `NEXT_SEQNO` mirror passes a new decision's seqno.
    pub fn publish_cross_held(&self, held: Seqno) {
        self.inner
            .h64(header::CROSS_HELD)
            .store(held, Ordering::Release);
    }

    /// The highest seqno a new snapshot may include: every commit at or below it is applied
    /// on every shard: the minimum of `NEXT_SEQNO`, the cross-shard hold (`CROSS_HELD`, D278)
    /// and every shard's pending watermark, so one Acquire load per shard plus two. Always
    /// inlined: left to the linker it was outlined into a call on the point-get path (12
    /// instructions, #836 round 6).
    #[inline(always)]
    pub fn visible_seqno(&self) -> Seqno {
        self.inner.visible_seqno()
    }

    /// Publishes `shard`'s durable watermark with release ordering (D227): the lowest seqno
    /// it holds that is not yet durable at its commit's level (never above its pending
    /// watermark); `u64::MAX` when it holds nothing.
    pub fn publish_durable(&self, shard: u32, durable: Seqno) {
        self.inner.durable(shard).store(durable, Ordering::Release);
    }

    /// The highest seqno a durable snapshot may include (D227, ICR 0037): every commit at or
    /// below it is applied on every shard and durable at its level. Never above
    /// [`ShmRegion::visible_seqno`].
    pub fn durable_seqno(&self) -> Seqno {
        self.inner.durable_seqno()
    }

    /// The mirror of the next seqno the writer will reserve ([`ShmRegion::advance_next_seqno`];
    /// acquire load). It never runs ahead of the writer's reservations, so step 1 of the
    /// protocol publishes it as the lower bound before reserving.
    pub fn next_seqno(&self) -> Seqno {
        self.inner.h64(header::NEXT_SEQNO).load(Ordering::Acquire)
    }

    // ---- views and manifest ----

    /// Publishes a view (writer only): writes the inactive buffer, then swaps the view
    /// pointer with release ordering. Fails with [`Error::ViewTooLarge`] (publishing nothing)
    /// if the encoded view exceeds the buffer.
    ///
    /// `view.view_version` becomes the published version and must be greater than the
    /// published one ([`Error::ViewVersionNotNewer`] otherwise; readers pin by version, and 0
    /// means "no view"). There is one publisher: the writer process (its lock excludes
    /// others), and within it a mutex serializes callers, so a second thread publishing
    /// concurrently waits rather than racing on the inactive buffer.
    pub fn publish_view(&self, view: &ViewRecord) -> Result<()> {
        let mut bytes = self
            .inner
            .publish_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let current = self.inner.view_pointer(Ordering::Relaxed);
        let published = current >> 1;
        if view.view_version <= published {
            return Err(Error::ViewVersionNotNewer {
                published,
                offered: view.view_version,
            });
        }
        let capacity = self.inner.header.view_buffer_len as usize;
        let needed = view.encoded_len();
        if needed > capacity {
            return Err(Error::ViewTooLarge { needed, capacity });
        }
        bytes.clear();
        bytes.reserve(needed);
        view.encode(&mut bytes);
        let index = if current == 0 { 0 } else { (current & 1) ^ 1 };
        self.inner.region.write(
            self.inner.header.views_off as usize + index as usize * capacity,
            &bytes,
        );
        // SeqCst, not just Release: it pairs with the SeqCst stores in `ReaderSlot::pin`, so
        // the writer scanning slots after publishing and a reader re-checking the pointer
        // after pinning cannot both miss each other.
        self.inner
            .h64(header::VIEW_POINTER)
            .store((view.view_version << 1) | index, Ordering::SeqCst);
        Ok(())
    }

    /// Copies and decodes the current view, retrying if the writer swapped buffers mid-copy.
    ///
    /// Before any view is published, returns an empty record with `view_version` 0 and the
    /// current manifest version.
    pub fn read_view(&self) -> Result<ViewRecord> {
        let inner = &self.inner;
        let capacity = inner.header.view_buffer_len as usize;
        let mut bad_copies = 0;
        loop {
            let pointer = inner.view_pointer(Ordering::Acquire);
            if pointer == 0 {
                return Ok(ViewRecord {
                    view_version: 0,
                    manifest_version: self.manifest_version(),
                    ..ViewRecord::default()
                });
            }
            let base = inner.header.views_off as usize + (pointer & 1) as usize * capacity;
            let mut head = [0u8; VIEW_HEADER_LEN];
            inner.region.read(base, &mut head);
            let byte_len = u32::from_le_bytes(head[16..20].try_into().expect("4 bytes")) as usize;
            let decoded = if (VIEW_HEADER_LEN..=capacity).contains(&byte_len) {
                let mut record = vec![0u8; byte_len];
                record[..VIEW_HEADER_LEN].copy_from_slice(&head);
                inner
                    .region
                    .read(base + VIEW_HEADER_LEN, &mut record[VIEW_HEADER_LEN..]);
                ViewRecord::decode(&record)
                    .ok()
                    .filter(|v| v.view_version == pointer >> 1)
            } else {
                None
            };
            if inner.view_pointer(Ordering::Acquire) != pointer {
                // Swapped under us: the copy may mix two views. Start over.
                bad_copies = 0;
                continue;
            }
            match decoded {
                Some(view) => return Ok(view),
                None => {
                    // The pointer did not move, so the writer was not touching this buffer.
                    bad_copies += 1;
                    if bad_copies >= VIEW_READ_ATTEMPTS {
                        return Err(Error::Corrupt("view record checksum"));
                    }
                }
            }
        }
    }

    /// Current view version, without copying the view.
    pub fn view_version(&self) -> u64 {
        self.inner.view_pointer(Ordering::Acquire) >> 1
    }

    /// Records the manifest version readers should load.
    pub fn set_manifest_version(&self, version: ManifestVersion) {
        self.inner
            .h64(header::MANIFEST_VERSION)
            .store(version, Ordering::Release);
    }

    /// The manifest version readers should load.
    pub fn manifest_version(&self) -> ManifestVersion {
        self.inner
            .h64(header::MANIFEST_VERSION)
            .load(Ordering::Acquire)
    }

    // ---- reader slots ----

    /// Claims a free reader slot for `process` and takes its liveness lock through `file`,
    /// the reader's handle on the database file (layout 3, #693 PR 3): byte
    /// [`READER_LOCK_BASE`] `+ index`, exclusive, taken **before** the slot's `free ->
    /// claiming` CAS and held until the slot is freed. The writer frees a slot only once it
    /// can take that byte itself ([`ShmRegion::reclaim_dead_slots_locked`]), so a live
    /// reader is never judged dead (whatever its pid namespace), and a claimer that died
    /// between its CAS and its `active` store is reclaimed too. A byte another handle holds
    /// is skipped: a reader that died with its slot still `active` until the writer frees it.
    pub fn claim_reader_slot_locked(
        &self,
        process: ProcessId,
        file: &FileRef,
    ) -> Result<ReaderSlot> {
        let inner = &self.inner;
        for index in 0..inner.header.reader_slot_count {
            let byte = READER_LOCK_BASE + u64::from(index);
            // A byte this handle already holds is another live slot's (locks never conflict
            // within a handle, so locking it would succeed, and the failure path below would
            // release it under that slot): taken, like a byte another handle holds
            // (perf287's review of #898, S2).
            if file.holds(byte).is_some() {
                continue;
            }
            match file.lock(byte, LockMode::Exclusive) {
                Ok(()) => {}
                Err(e) if e.kind == ErrorKind::Locked => continue,
                Err(e) => return Err(e.into()),
            }
            let state = inner.slot32(index, reader_slot::STATE);
            if state
                .compare_exchange(
                    SLOT_FREE,
                    SLOT_CLAIMING,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                let _ = file.unlock(byte);
                continue;
            }
            inner
                .slot32(index, reader_slot::PID)
                .store(process.pid, Ordering::Relaxed);
            inner
                .slot64(index, reader_slot::START_TIME)
                .store(process.start_time, Ordering::Relaxed);
            inner
                .slot64(index, reader_slot::PINNED_SEQNO)
                .store(0, Ordering::Relaxed);
            inner
                .slot64(index, reader_slot::PINNED_VIEW)
                .store(0, Ordering::Relaxed);
            inner
                .slot64(index, reader_slot::GENERATION)
                .store(inner.generation.0, Ordering::Relaxed);
            state.store(SLOT_ACTIVE, Ordering::Release);
            return Ok(ReaderSlot {
                region: inner.clone(),
                index,
                owner: process,
                lock: file.clone(),
                abandoned: Arc::new(AtomicBool::new(false)),
                claimed_in: pigeonhole_io::sys::process_id(),
            });
        }
        Err(Error::NoReaderSlot)
    }

    /// Reader slots in use (claimed, not free) and the slot count.
    pub fn reader_slot_usage(&self) -> (u32, u32) {
        let inner = &self.inner;
        let total = inner.header.reader_slot_count;
        let used = (0..total)
            .filter(|&i| inner.slot32(i, reader_slot::STATE).load(Ordering::Relaxed) != SLOT_FREE)
            .count();
        (u32::try_from(used).unwrap_or(u32::MAX), total)
    }

    /// The oldest `(seqno, view_version)` pinned by any live reader slot, or `None`.
    ///
    /// Each component is the minimum over the slots that pin one (a slot pins when either
    /// field is nonzero); a 0 component means no slot names one. The writer frees memtables
    /// and extents only below these. Pins are read regardless of slot state: a slot clears
    /// its pins before it is freed, so a free slot never contributes.
    pub fn oldest_reader_pin(&self) -> Option<(Seqno, u64)> {
        let inner = &self.inner;
        let mut oldest: Option<(Seqno, u64)> = None;
        for index in 0..inner.header.reader_slot_count {
            // SeqCst: pairs with `ReaderSlot::pin` (see `publish_view`).
            let seqno = inner
                .slot64(index, reader_slot::PINNED_SEQNO)
                .load(Ordering::SeqCst);
            let view = inner
                .slot64(index, reader_slot::PINNED_VIEW)
                .load(Ordering::SeqCst);
            if seqno == 0 && view == 0 {
                continue;
            }
            let min_nonzero = |a: u64, b: u64| match (a, b) {
                (0, b) => b,
                (a, 0) => a,
                (a, b) => a.min(b),
            };
            oldest = Some(match oldest {
                None => (seqno, view),
                Some((s, v)) => (min_nonzero(s, seqno), min_nonzero(v, view)),
            });
        }
        oldest
    }

    /// Frees the slots whose reader process is gone, judged by each slot's liveness lock
    /// (layout 3, #693 PR 3): the writer probes byte [`READER_LOCK_BASE`] `+ index` through
    /// `file`, its own handle on the database file, with a non-blocking exclusive lock. If it
    /// gets the byte, no process holds the slot, so the slot is freed (and the byte
    /// released); if the byte is held, the reader is alive. Independent of pids, so a reader
    /// in another pid namespace is never judged dead. Slots stuck in `claiming` (a claimer
    /// that died between its CAS and its `active` store, D46) are probed and freed the same
    /// way: a live claimer holds its byte from before its CAS. Returns how many were freed.
    ///
    /// Each `active` slot is first moved to `claiming` by CAS, which keeps its owner from
    /// freeing it and a new reader from claiming it while the probe runs; a live owner gets
    /// the slot back as `active`.
    pub fn reclaim_dead_slots_locked(&self, file: &FileRef) -> usize {
        let alive = |index: u32| {
            let byte = READER_LOCK_BASE + u64::from(index);
            match file.lock(byte, LockMode::Exclusive) {
                Ok(()) => {
                    let _ = file.unlock(byte);
                    false
                }
                // Held by its reader, or the probe failed: alive (conservative).
                Err(_) => true,
            }
        };
        let inner = &self.inner;
        let mut reclaimed = 0;
        for index in 0..inner.header.reader_slot_count {
            let state = inner.slot32(index, reader_slot::STATE);
            let was = match state.compare_exchange(
                SLOT_ACTIVE,
                SLOT_CLAIMING,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => SLOT_ACTIVE,
                // A claimer that may have died before recording itself: only the lock knows.
                Err(SLOT_CLAIMING) => SLOT_CLAIMING,
                Err(_) => continue,
            };
            // Only this writer can change the slot now; the owner's release waits for it.
            if alive(index) {
                // Back as it was: a live claimer finishes its own claim.
                if was == SLOT_ACTIVE {
                    state.store(SLOT_ACTIVE, Ordering::Release);
                }
                continue;
            }
            inner.clear_pins(index);
            state.store(SLOT_FREE, Ordering::Release);
            reclaimed += 1;
        }
        reclaimed
    }
}

impl ReaderSlot {
    /// Whether the slot is still this handle's (see [`crate::SlotOwnership::is_current`]).
    pub fn is_owned(&self) -> bool {
        !self.inert() && self.region.slot_owned_by(self.index, self.owner)
    }

    /// Abandoned ([`ReaderSlot::abandon`]), or used in a process other than the one that
    /// claimed it (a forked child, perf287's review of #898, S1): the handle stores nothing
    /// to the slot, and its drop frees nothing and unlocks nothing. One cached-id compare
    /// ([`pigeonhole_io::sys::process_id`]), no system call.
    fn inert(&self) -> bool {
        self.abandoned.load(Ordering::Acquire)
            || pigeonhole_io::sys::process_id() != self.claimed_in
    }

    /// Gives the slot up without freeing it: releases its liveness lock now, and from then on
    /// this handle stores nothing to the slot (its pin calls and its drop do nothing) and
    /// [`ReaderSlot::is_owned`] and every [`crate::SlotOwnership`] taken from it are false.
    /// For a reader about to claim another slot through the same file handle while this one
    /// lives on: the slot of a superseded generation, which no writer reclaims, or one freed
    /// under it. Byte-range locks do not conflict within one handle, so if this slot kept its
    /// lock, a new claim could take the same byte and this slot's drop would then release it
    /// under the new slot (and, in the same region, free the new slot, which names the same
    /// process).
    pub fn abandon(&self) {
        if self.abandoned.swap(true, Ordering::AcqRel) {
            return;
        }
        // A forked child's copy releases nothing: the byte is the parent's.
        if pigeonhole_io::sys::process_id() != self.claimed_in {
            return;
        }
        let _ = self.lock.unlock(READER_LOCK_BASE + u64::from(self.index));
    }

    /// A handle that checks [`ReaderSlot::is_owned`] without holding the slot.
    pub fn ownership(&self) -> crate::SlotOwnership {
        crate::SlotOwnership {
            region: self.region.clone(),
            index: self.index,
            owner: self.owner,
            abandoned: Arc::clone(&self.abandoned),
            claimed_in: self.claimed_in,
        }
    }

    /// Slot index.
    pub fn index(&self) -> u32 {
        self.index
    }

    /// Pins a snapshot: record the view version and seqno before reading through them.
    /// Protocol: store view, store seqno, re-read the view pointer; if it moved past the
    /// pinned version, take a fresh snapshot seqno and pin again with the new version
    /// (`FORMAT.md` §11.5). Returns the `(seqno, view_version)` pair actually pinned.
    ///
    /// Call this with the version from [`ShmRegion::view_version`] *before*
    /// [`ShmRegion::read_view`]: if the writer published a newer view meanwhile (and may
    /// already have freed what the older one named), the pin moves up to the newer version
    /// together with a seqno taken after it, and the view read afterwards is at least that
    /// version. A pin protects its version and every newer one. The caller uses the returned
    /// pair, not the one it passed.
    pub fn pin(&self, seqno: Seqno, view_version: u64) -> (Seqno, u64) {
        self.pin_from(seqno, view_version, false)
    }

    /// As [`ReaderSlot::pin`], for a durable snapshot (D227, ICR 0037): a retry takes its
    /// fresh seqno from [`ShmRegion::durable_seqno`].
    pub fn pin_durable(&self, seqno: Seqno, view_version: u64) -> (Seqno, u64) {
        self.pin_from(seqno, view_version, true)
    }

    fn pin_from(&self, seqno: Seqno, view_version: u64, durable: bool) -> (Seqno, u64) {
        let inner = &self.region;
        let mut pair = (seqno, view_version);
        // An inert handle pins nothing ([`ReaderSlot::is_owned`] is false: its owner claims
        // a fresh slot before pinning).
        if self.inert() {
            return pair;
        }
        loop {
            inner
                .slot64(self.index, reader_slot::PINNED_VIEW)
                .store(pair.1, Ordering::SeqCst);
            inner
                .slot64(self.index, reader_slot::PINNED_SEQNO)
                .store(pair.0, Ordering::SeqCst);
            let current = inner.view_pointer(Ordering::SeqCst) >> 1;
            if current <= pair.1 {
                return pair;
            }
            let fresh = if durable {
                inner.durable_seqno()
            } else {
                inner.visible_seqno()
            };
            pair = (fresh.max(pair.0), current);
        }
    }

    /// Lowers the pinned seqno to `seqno` if it is above it, keeping the pinned view (live
    /// snapshots still read through it): a durable snapshot taken while newer snapshots hold
    /// the pin (D227). Safe because every entry outside the memtables is at or below the
    /// durable seqno it was frozen under, so nothing a snapshot at `seqno` reads has been
    /// collapsed yet.
    pub fn lower_pin(&self, seqno: Seqno) {
        if self.inert() {
            return;
        }
        self.region
            .slot64(self.index, reader_slot::PINNED_SEQNO)
            .fetch_min(seqno, Ordering::SeqCst);
    }

    /// Raises the pin to `seqno` and `view_version` where it is lower (#693 PR 2): the
    /// reader's oldest snapshot was dropped, so what it pinned for that snapshot alone may be
    /// reclaimed. Only ever raises: a field already at or above its new value stays.
    pub fn raise_pin(&self, seqno: Seqno, view_version: u64) {
        if self.inert() {
            return;
        }
        self.region
            .slot64(self.index, reader_slot::PINNED_SEQNO)
            .fetch_max(seqno, Ordering::SeqCst);
        self.region
            .slot64(self.index, reader_slot::PINNED_VIEW)
            .fetch_max(view_version, Ordering::SeqCst);
    }

    /// Clears the pin (nothing once abandoned or in a forked child, as for
    /// [`ReaderSlot::lower_pin`] and [`ReaderSlot::raise_pin`]).
    pub fn unpin(&self) {
        if self.inert() {
            return;
        }
        self.region
            .slot64(self.index, reader_slot::PINNED_SEQNO)
            .store(0, Ordering::SeqCst);
        self.region
            .slot64(self.index, reader_slot::PINNED_VIEW)
            .store(0, Ordering::SeqCst);
    }
}

impl Drop for ReaderSlot {
    fn drop(&mut self) {
        if self.inert() {
            return;
        }
        self.region.release_slot(self.index, self.owner);
        // After the slot is free: until then the writer must see this reader alive.
        let _ = self.lock.unlock(READER_LOCK_BASE + u64::from(self.index));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small() -> ShmConfig {
        let mut c = ShmConfig::new(2);
        c.arena_bytes = 2 << 20;
        c.reader_slots = 3;
        c.view_buffer_bytes = 4096;
        c
    }

    #[test]
    fn fresh_region_is_idle() {
        let shm = ShmRegion::in_memory([3; 16], &small());
        assert_eq!(shm.generation(), Generation(1));
        assert_eq!(shm.shard_count(), 2);
        assert_eq!(shm.visible_seqno(), 0);
        assert_eq!(shm.view_version(), 0);
        assert_eq!(shm.manifest_version(), 0);
        assert!(!shm.is_stale());
        assert_eq!(shm.oldest_reader_pin(), None);
        let view = shm.read_view().unwrap();
        assert_eq!(view, ViewRecord::default());
        let (region, off, len) = shm.arena(1);
        assert_eq!(len, 2 << 20);
        assert_eq!(off % (2 << 20), 0);
        assert!(off + len <= region.len());
    }

    #[test]
    fn create_failures_map_to_unavailable() {
        use pigeonhole_io::Error as IoError;
        for kind in [ErrorKind::NoSpace, ErrorKind::NotFound, ErrorKind::Other] {
            let e = unavailable(IoError::new(kind, "size shared region"));
            assert!(matches!(e, Error::Unavailable), "{kind:?} gave {e:?}");
        }
        let e = unavailable(IoError::new(ErrorKind::Crashed, "open shared region"));
        assert!(matches!(e, Error::Io(_)), "{e:?}");
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn arena_out_of_range_panics() {
        let shm = ShmRegion::in_memory([3; 16], &small());
        let _ = shm.arena(2);
    }

    #[test]
    fn view_version_zero_is_refused() {
        let shm = ShmRegion::in_memory([3; 16], &small());
        assert!(matches!(
            shm.publish_view(&ViewRecord::default()),
            Err(Error::ViewVersionNotNewer {
                published: 0,
                offered: 0
            })
        ));
        assert_eq!(shm.view_version(), 0);
    }

    #[test]
    fn errors_display() {
        let e = Error::ViewTooLarge {
            needed: 10,
            capacity: 5,
        };
        assert!(e.to_string().contains("10 bytes"));
        let e = Error::from(pigeonhole_format::Error::UnsupportedVersion {
            what: "shm header",
            found: 9,
        });
        assert!(matches!(
            e,
            Error::VersionMismatch {
                found: 9,
                expected: 6
            }
        ));
        let e = Error::from(pigeonhole_format::Error::Corrupt { what: "x" });
        assert!(matches!(e, Error::Corrupt("x")));
        let io = Error::from(pigeonhole_io::Error::new(ErrorKind::Locked, "lock"));
        assert!(std::error::Error::source(&io).is_some());
        assert!(std::error::Error::source(&Error::Stale).is_none());
    }
}

/// A region's row-epoch table (D281, `FORMAT.md` §11): one atomic `u64` per slot, indexed by a
/// hash of `(table, row)`. The writer raises a row's slot to a commit's seqno before the
/// commit's write to the row can be visible; a row cache in any process stores and serves a
/// row only under its slot's current value (D201's epoch protocol, across processes).
///
/// ```
/// use pigeonhole_shm::{ShmConfig, ShmRegion};
///
/// let shm = ShmRegion::in_memory([1; 16], &ShmConfig::new(1));
/// let epochs = shm.row_epochs();
/// assert_eq!(epochs.epoch(42, 10), Some(0));
/// epochs.note_write_prev(42, 7);
/// assert_eq!(epochs.epoch(42, 10), Some(7));
/// assert_eq!(epochs.epoch(42, 6), None); // written, not visible at 6
/// ```
#[derive(Debug, Clone)]
pub struct RowEpochTable {
    region: SharedRegion,
    off: usize,
    mask: u64,
    seed: u64,
    maintained: bool,
}

impl RowEpochTable {
    /// The seed of the hash that maps a `(table, row)` to its slot (random per generation).
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// How many slots the table has (a power of two).
    pub fn slots(&self) -> usize {
        self.mask as usize + 1
    }

    /// Whether the writer raises the slots (the header's `ROW_EPOCHS_MAINTAINED`): a reader
    /// may cache rows only then.
    pub fn maintained(&self) -> bool {
        self.maintained
    }

    #[inline]
    fn slot(&self, hash: u64) -> &AtomicU64 {
        // In bounds: `decode` checked the table lies inside the region (perf287's M1).
        self.region
            .atomic_u64(self.off + (hash & self.mask) as usize * ROW_EPOCH_LEN)
    }

    /// The row hashing to `hash` is written at `seqno` (call before the write can be
    /// visible): raises its slot to `seqno`, returning the slot's value before the raise
    /// (ICR 0082). That is below `seqno` for a commit's first write to the row since the slot
    /// last moved, `seqno` itself for a later write of the same commit, and above it when a
    /// commit with a higher seqno already wrote a row of this slot. Writer only.
    ///
    /// ```
    /// use pigeonhole_shm::{ShmConfig, ShmRegion};
    ///
    /// let shm = ShmRegion::in_memory([1; 16], &ShmConfig::new(1));
    /// let epochs = shm.row_epochs();
    /// assert_eq!(epochs.note_write_prev(42, 7), 0);
    /// assert_eq!(epochs.note_write_prev(42, 7), 7); // the same commit again
    /// assert_eq!(epochs.note_write_prev(42, 5), 7); // a lower seqno applied later
    /// assert_eq!(epochs.epoch(42, 10), Some(7));
    /// ```
    #[inline]
    pub fn note_write_prev(&self, hash: u64, seqno: u64) -> u64 {
        self.slot(hash).fetch_max(seqno, Ordering::Release)
    }

    /// The epoch of the row hashing to `hash` for a read at `seqno` (load it after taking
    /// the read point): `None` when a write to it is applied but not visible at `seqno`.
    #[inline]
    pub fn epoch(&self, hash: u64, seqno: u64) -> Option<u64> {
        let e = self.slot(hash).load(Ordering::Acquire);
        (e <= seqno).then_some(e)
    }
}

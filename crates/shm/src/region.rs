//! The mapped region: lifecycle (create, attach, rebuild, remove), the seqno and watermark
//! protocol, view publication and the reader-slot table. Layout per `FORMAT.md` §11; the
//! offsets come from `pigeonhole_format::shm`.

use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use pigeonhole_format::shm::{
    DIRECTORY_MAGIC, HEADER_LEN, READER_SLOT_LEN, ShmHeader, ViewRecord, WATERMARK_STRIDE,
    directory, directory_name, header, reader_slot, region_name, watermark,
};
use pigeonhole_format::{ManifestVersion, Seqno, ShmLayoutVersion};
use pigeonhole_io::{
    ErrorKind, FileIdentity, FileRef, ProcessId, SharedOpen, SharedRegion, VfsRef,
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
    publish_lock: Mutex<()>,
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

    fn visible_seqno(&self) -> Seqno {
        let next = self.h64(header::NEXT_SEQNO).load(Ordering::Acquire);
        let mut lowest = next;
        for shard in 0..self.header.shard_count {
            lowest = lowest.min(self.pending(shard).load(Ordering::Acquire));
        }
        lowest.saturating_sub(1)
    }

    fn clear_pins(&self, index: u32) {
        self.slot64(index, reader_slot::PINNED_SEQNO)
            .store(0, Ordering::SeqCst);
        self.slot64(index, reader_slot::PINNED_VIEW)
            .store(0, Ordering::SeqCst);
    }

    /// The owner frees its slot: pins first, so a concurrent
    /// [`ShmRegion::oldest_reader_pin`] never sees a free slot still pinning, then `active`
    /// to `free`. The CAS fails only while the writer has the slot in `claiming` to check
    /// its owner's liveness ([`ShmRegion::reclaim_dead_slots`]); the writer puts it back to
    /// `active` at once because the owner is alive, so retry. If the writer died in that
    /// window the slot stays `claiming`; give up and leave it to the next generation.
    fn release_slot(&self, index: u32) {
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

/// `Unavailable` for an allocation failure, `Io` for anything else.
fn unavailable(e: pigeonhole_io::Error) -> Error {
    match e.kind {
        ErrorKind::NoSpace | ErrorKind::Other => Error::Unavailable,
        _ => Error::Io(e),
    }
}

fn io_error(kind: ErrorKind, context: &'static str) -> Error {
    Error::Io(pigeonhole_io::Error::new(kind, context))
}

/// Writes the header, including the fields `ShmHeader` leaves to this crate, and idles every
/// watermark. `state` stays `initializing`; the caller stores `ready` once everything is in.
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
    region.write(0, &page);
    for shard in 0..h.shard_count {
        let off = h.watermarks_off as usize + WATERMARK_STRIDE * shard as usize;
        region
            .atomic_u64(off + watermark::PENDING)
            .store(u64::MAX, Ordering::Relaxed);
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
        return Err(io_error(
            ErrorKind::NotFound,
            "no shared-memory region: no writer has built one",
        ));
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
    let probe = vfs.open_shared(&name, dir, HEADER_LEN as u64, SharedOpen::Attach)?;
    let h = read_header(&probe, db_id, identity, generation)?;
    let len = usize::try_from(h.region_len).map_err(|_| Error::Corrupt("region length"))?;
    let region = if probe.len() >= len {
        probe
    } else {
        vfs.open_shared(&name, dir, h.region_len, SharedOpen::Attach)?
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
        publish_lock: Mutex::new(()),
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
    let h = ShmHeader::layout(
        db_id,
        config.shards,
        config.reader_slots,
        config.view_buffer_bytes,
        config.arena_bytes,
        identity.device,
        identity.inode,
    );
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
        publish_lock: Mutex::new(()),
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
        let h = ShmHeader::layout(
            db_id,
            config.shards,
            config.reader_slots,
            config.view_buffer_bytes,
            config.arena_bytes,
            identity.device,
            identity.inode,
        );
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
                publish_lock: Mutex::new(()),
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

    /// Binds `shard`'s arena to NUMA node `node` (writer, at shard start). `mbind` on Linux;
    /// a no-op elsewhere and for heap regions.
    pub fn bind_arena(&self, shard: u32, node: u32) -> Result<()> {
        let (region, offset, len) = self.arena(shard);
        Ok(region.bind_numa(offset, len, node)?)
    }

    // ---- seqnos and watermarks ----

    /// Reserves `count` consecutive seqnos (one atomic `fetch_add` per commit group) and
    /// returns the first. The caller must already have published a pending watermark no
    /// higher than the result (see `FORMAT.md` §11.3). A cross-shard commit reserves one.
    pub fn reserve_seqnos(&self, count: u64) -> Seqno {
        self.inner
            .h64(header::NEXT_SEQNO)
            .fetch_add(count, Ordering::AcqRel)
    }

    /// Publishes `shard`'s pending watermark with release ordering: the minimum of its current
    /// group's lower bound and every cross-shard seqno it coordinates and has not released;
    /// `u64::MAX` when it holds nothing, so an idle shard never holds back snapshots.
    pub fn publish_pending(&self, shard: u32, pending: Seqno) {
        self.inner.pending(shard).store(pending, Ordering::Release);
    }

    /// The highest seqno a new snapshot may include: every commit at or below it is applied
    /// on every shard.
    pub fn visible_seqno(&self) -> Seqno {
        self.inner.visible_seqno()
    }

    /// The next seqno [`ShmRegion::reserve_seqnos`] will hand out (acquire load). Step 1 of
    /// the protocol publishes this as the lower bound before reserving.
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
        let _publishing = self
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
        let mut bytes = Vec::with_capacity(needed);
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

    /// Claims a free reader slot for `process`.
    pub fn claim_reader_slot(&self, process: ProcessId) -> Result<ReaderSlot> {
        let inner = &self.inner;
        for index in 0..inner.header.reader_slot_count {
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
            });
        }
        Err(Error::NoReaderSlot)
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

    /// Frees slots whose process is gone (pid missing or start time changed). Returns how
    /// many were reclaimed. Run by the writer before computing reclamation bounds.
    ///
    /// Each `active` slot is first moved to `claiming` by CAS, which keeps its owner from
    /// freeing it and a new reader from claiming it while the owner recorded in the slot is
    /// checked; a live owner gets the slot back as `active`. Slots found in `claiming` are
    /// skipped: their owner has not recorded itself yet, so the pid there is not trustworthy
    /// (a process that dies between its CAS and its `active` store leaks its slot until the
    /// next generation).
    pub fn reclaim_dead_slots(&self, vfs: &VfsRef) -> usize {
        let inner = &self.inner;
        let mut reclaimed = 0;
        for index in 0..inner.header.reader_slot_count {
            let state = inner.slot32(index, reader_slot::STATE);
            if state
                .compare_exchange(
                    SLOT_ACTIVE,
                    SLOT_CLAIMING,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                continue;
            }
            // Only this writer can change the slot now; the owner's release waits for it.
            let owner = ProcessId {
                pid: inner
                    .slot32(index, reader_slot::PID)
                    .load(Ordering::Relaxed),
                start_time: inner
                    .slot64(index, reader_slot::START_TIME)
                    .load(Ordering::Relaxed),
            };
            if vfs.process_alive(owner) {
                state.store(SLOT_ACTIVE, Ordering::Release);
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
        let inner = &self.region;
        let mut pair = (seqno, view_version);
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
            pair = (inner.visible_seqno().max(pair.0), current);
        }
    }

    /// Clears the pin.
    pub fn unpin(&self) {
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
        self.region.release_slot(self.index);
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
        shm.bind_arena(1, 0).unwrap();
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
                expected: 1
            }
        ));
        let e = Error::from(pigeonhole_format::Error::Corrupt { what: "x" });
        assert!(matches!(e, Error::Corrupt("x")));
        let io = Error::from(pigeonhole_io::Error::new(ErrorKind::Locked, "lock"));
        assert!(std::error::Error::source(&io).is_some());
        assert!(std::error::Error::source(&Error::Stale).is_none());
    }
}

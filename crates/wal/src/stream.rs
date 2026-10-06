//! The sidecar-file stream: one file per stream, made of preallocated slots that are recycled
//! after checkpoint.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use pigeonhole_format::wal::{
    FRAGMENT_HEADER_LEN, FRAME_SIZE, FrameEncoder, SegmentHeader, WalRecord,
};
use pigeonhole_format::{Durability, FormatVersion, Lsn, StreamId};
use pigeonhole_io::{Completion, FileRef, OpenOptions, VfsRef};

use crate::{CommitTicket, Error, Result, Wal, WalOptions, stream_path, sync_parent};

/// Frame size as a file offset.
pub(crate) const FRAME: u64 = FRAME_SIZE as u64;

/// Largest segment: 4 GiB minus one frame, so a successor's `prev_end` always fits a `u32`
/// (decisions log, open question on `prev_end`).
pub(crate) const MAX_SEGMENT_SIZE: u64 = (1 << 32) - FRAME;

/// Bytes written per call when zero-filling a slot.
const ZERO_CHUNK: usize = 1 << 20;

/// Epoch recorded in the stream's slot table for a slot the spare pool owns (blank, never
/// held a segment the stream knows of). Real epochs never reach this value.
const POOLED: u32 = u32::MAX;

pub(crate) fn corrupt(what: &'static str) -> Error {
    Error::Format(pigeonhole_format::Error::Corrupt { what })
}

pub(crate) fn check_options(opts: &WalOptions) -> Result<()> {
    let size = opts.segment_size;
    if !size.is_multiple_of(FRAME) || !(2 * FRAME..=MAX_SEGMENT_SIZE).contains(&size) {
        return Err(Error::InvalidArgument {
            what: "wal segment size",
        });
    }
    Ok(())
}

/// Writable, no creation.
pub(crate) fn open_existing() -> OpenOptions {
    let mut o = OpenOptions::read();
    o.write = true;
    o
}

/// The slot grid of an existing stream file: its slot size and the valid header in each slot.
#[derive(Debug)]
pub(crate) struct Grid {
    pub(crate) segment_size: u64,
    /// `None` where the slot holds no valid header (blank, or torn and therefore unused).
    pub(crate) headers: Vec<Option<SegmentHeader>>,
}

impl Grid {
    /// Reads every slot header. Returns `None` if no valid header exists anywhere in the
    /// file, in which case its slot size is unknown and nothing in it is reachable.
    pub(crate) fn read(file: &FileRef, stream: StreamId, db_id: [u8; 16]) -> Result<Option<Self>> {
        let len = file.len()?;
        let mut probe = [0u8; 64];
        // Slot 0 normally holds a valid header. If its last header write was torn, find the
        // slot size from the first valid header at any frame boundary.
        let mut segment_size = None;
        let mut off = 0;
        while off + probe.len() as u64 <= len {
            file.read_at(&mut probe, off)?;
            if let Ok(h) = SegmentHeader::decode(&probe)
                && off.is_multiple_of(h.segment_size)
            {
                segment_size = Some(h.segment_size);
                break;
            }
            off += FRAME;
        }
        let Some(segment_size) = segment_size else {
            return Ok(None);
        };
        let slots = usize::try_from(len / segment_size).map_err(|_| corrupt("wal file size"))?;
        let mut headers = Vec::with_capacity(slots);
        for slot in 0..slots {
            file.read_at(&mut probe, slot as u64 * segment_size)?;
            headers.push(match SegmentHeader::decode(&probe) {
                Ok(h) => {
                    if h.db_id != db_id {
                        return Err(Error::ForeignSegment);
                    }
                    if h.stream != stream {
                        return Err(corrupt("wal segment stream id"));
                    }
                    if h.segment_size != segment_size {
                        return Err(corrupt("wal segment size"));
                    }
                    Some(h)
                }
                Err(_) => None,
            });
        }
        Ok(Some(Self {
            segment_size,
            headers,
        }))
    }

    /// The largest epoch in any header.
    pub(crate) fn max_epoch(&self) -> u32 {
        self.headers
            .iter()
            .flatten()
            .map(|h| h.epoch)
            .max()
            .unwrap_or(0)
    }

    /// The slot holding the segment with `epoch`.
    pub(crate) fn slot_of(&self, epoch: u32) -> Option<usize> {
        self.headers
            .iter()
            .position(|h| h.is_some_and(|h| h.epoch == epoch))
    }
}

/// State shared between a [`WalStream`] (shard thread), its sync completions (I/O threads)
/// and its [`SpareSegments`] handle (a background task).
struct Shared {
    /// Durable position, raised by sync completions.
    durable: AtomicU64,
    /// Set by the first failed write or sync; every later append, write or sync is refused.
    poisoned: AtomicBool,
    pool: Mutex<Pool>,
}

/// Blank slots and the file's slot count. The file is `total * segment_size` bytes long (or
/// longer, if a preparation failed after extending it).
#[derive(Debug, Default)]
struct Pool {
    total: usize,
    /// Allocated, zero-filled and synced: a segment started here needs no metadata update.
    ready: Vec<usize>,
    /// Allocated but not zero-filled (grown inline, or found blank at recovery).
    blank: Vec<usize>,
    /// Slots the stream can recycle (below its checkpoint), published by the stream.
    recyclable: usize,
    /// Rollovers that found no recyclable or ready slot and grew the file inline.
    inline_grows: u64,
}

impl Shared {
    fn pool(&self) -> MutexGuard<'_, Pool> {
        self.pool.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Zero-fills slot `slot` of a stream file.
fn zero_fill(file: &FileRef, segment_size: u64, slot: usize, zeros: &[u8]) -> Result<()> {
    let start = slot as u64 * segment_size;
    let mut off = 0;
    while off < segment_size {
        let n = zeros.len().min((segment_size - off) as usize);
        file.write_at(&zeros[..n], start + off)?;
        off += n as u64;
    }
    Ok(())
}

/// Prepares spare slots for a [`WalStream`] off the shard thread.
///
/// Obtained from [`WalStream::spares`] (or [`Wal::spares`]); `Send + Sync`, so the engine
/// runs [`SpareSegments::prepare`] on a background task. A prepared slot is allocated,
/// zero-filled and synced, so a segment started in it needs no metadata update at its
/// fdatasyncs. The stream's rollover takes a recyclable slot first, then a prepared one, and
/// only grows the file inline when it has neither ([`WalStream::inline_grows`] counts those).
///
/// ```
/// use std::path::Path;
/// use pigeonhole_format::StreamId;
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_wal::{Wal, WalOptions, WalStream};
///
/// # fn main() -> pigeonhole_wal::Result<()> {
/// let vfs: pigeonhole_io::VfsRef = SimVfs::new(1);
/// let mut opts = WalOptions::default();
/// opts.segment_size = 2 * 32 * 1024;
/// let wal = WalStream::create(&vfs, Path::new("/db/x.phdb"), StreamId(0), [0; 16], opts)?;
/// let spares = wal.spares();
/// // On a background task:
/// let worker = std::thread::spawn(move || spares.prepare(spares.target()));
/// assert_eq!(worker.join().unwrap()?, 2);
/// assert_eq!(wal.spares().ready(), 2);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct SpareSegments {
    file: FileRef,
    segment_size: u64,
    target: u32,
    shared: Arc<Shared>,
}

impl fmt::Debug for SpareSegments {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pool = self.shared.pool();
        f.debug_struct("SpareSegments")
            .field("segment_size", &self.segment_size)
            .field("target", &self.target)
            .field("total", &pool.total)
            .field("ready", &pool.ready.len())
            .field("blank", &pool.blank.len())
            .field("recyclable", &pool.recyclable)
            .finish()
    }
}

impl SpareSegments {
    /// The stream's configured `spare_segments`: what the engine normally passes to
    /// [`SpareSegments::prepare`].
    pub fn target(&self) -> u32 {
        self.target
    }

    /// Prepared slots not yet taken by the stream.
    pub fn ready(&self) -> u32 {
        self.shared.pool().ready.len() as u32
    }

    /// Makes sure the stream has at least `n` free slots (recyclable, blank or prepared),
    /// zero-filling blank slots first and growing the file by whole slots after that, then
    /// syncs once. Returns how many slots it zero-filled. Blocking file I/O: run it on a
    /// background task, never on a shard thread.
    pub fn prepare(&self, n: u32) -> Result<u32> {
        let (mut to_fill, old_total, grown) = {
            let mut pool = self.shared.pool();
            let have = pool.recyclable + pool.ready.len() + pool.blank.len();
            let need = (n as usize).saturating_sub(have);
            // Blank slots are zero-filled first; the rest are new slots at the file's end.
            let from_blank = need.min(pool.blank.len());
            let mut to_fill: Vec<usize> = pool.blank.drain(..from_blank).collect();
            let grow = need - from_blank;
            let old_total = pool.total;
            to_fill.extend(old_total..old_total + grow);
            pool.total += grow;
            (to_fill, old_total, grow)
        };
        if to_fill.is_empty() {
            return Ok(0);
        }
        let zeros = vec![0u8; ZERO_CHUNK.min(self.segment_size as usize)];
        let result = (|| {
            for &slot in &to_fill {
                self.file
                    .allocate(slot as u64 * self.segment_size, self.segment_size)?;
                zero_fill(&self.file, self.segment_size, slot, &zeros)?;
            }
            self.file.sync_all()?;
            Ok(())
        })();
        let mut pool = self.shared.pool();
        match result {
            Ok(()) => {
                let filled = to_fill.len() as u32;
                pool.ready.append(&mut to_fill);
                pool.ready.sort_unstable();
                Ok(filled)
            }
            Err(e) => {
                // Give back what was reserved: blank slots stay blank, and the new slots are
                // forgotten if nothing else extended the file meanwhile (otherwise they stay
                // as blank slots; the first write to one extends the file if it must).
                if pool.total == old_total + grown {
                    pool.total = old_total;
                    to_fill.truncate(to_fill.len() - grown);
                }
                pool.blank.append(&mut to_fill);
                pool.blank.sort_unstable();
                Err(e)
            }
        }
    }
}

/// The sidecar-file implementation: one file per stream, made of preallocated segments that
/// are recycled after checkpoint.
///
/// Appends are buffered in memory and framed as they arrive; [`Wal::write`] hands the buffer
/// to the kernel with one positional write. When a segment fills, the stream writes what is
/// left of it, fdatasyncs it (the one blocking sync on the shard's path, once per segment) and
/// then buffers the successor's header with `prev_end` set to where the full segment ended.
///
/// The successor goes into a slot whose epoch is below the latest checkpoint if there is one,
/// else into a slot prepared by [`SpareSegments::prepare`] (zero-filled and synced, so its
/// fdatasyncs update no metadata), and only otherwise into a slot allocated inline
/// ([`WalStream::inline_grows`] counts those). [`WalStream::create`] zero-fills the first slot
/// before writing to it.
///
/// **Poisoning.** A failed write or sync leaves the kernel's view of the file unknown: a later
/// successful sync could acknowledge commits behind a hole that replay would drop. So the
/// first such failure poisons the stream: every later [`Wal::append`], [`Wal::write`],
/// [`Wal::sync`] and [`Wal::submit_sync`] fails with [`Error::Poisoned`] until the engine
/// reopens the stream through [`Recovery`](crate::Recovery). Argument errors
/// ([`Error::RecordTooLarge`], [`Error::InvalidArgument`]) do not poison.
///
/// ```
/// use std::path::Path;
/// use pigeonhole_format::wal::{StreamList, WalRecord};
/// use pigeonhole_format::{Durability, StreamId};
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_wal::{Wal, WalOptions, WalStream};
///
/// # fn main() -> pigeonhole_wal::Result<()> {
/// let vfs: pigeonhole_io::VfsRef = SimVfs::new(1);
/// let mut opts = WalOptions::default();
/// opts.segment_size = 2 * 32 * 1024;
/// let mut wal = WalStream::create(&vfs, Path::new("/db/x.phdb"), StreamId(0), [0; 16], opts)?;
/// let rec = WalRecord::Commit { seqno: 1, participants: StreamList::new(&[0, 0]).unwrap() };
/// let t = wal.append(&rec, Durability::Sync)?;
/// assert!(!wal.satisfies(&t));
/// wal.sync()?;
/// assert!(wal.satisfies(&t) && wal.durable() == t.end);
/// Box::new(wal).remove()?;
/// assert!(!vfs.exists(Path::new("/db/x.phdb-wal-0"))?);
/// # Ok(())
/// # }
/// ```
pub struct WalStream {
    vfs: VfsRef,
    file: FileRef,
    path: PathBuf,
    stream: StreamId,
    db_id: [u8; 16],
    segment_size: u64,
    spare: u32,
    /// Largest record payload a segment can hold.
    max_payload: usize,
    /// Epoch of the segment each slot holds; [`POOLED`] for a blank slot the pool owns.
    slots: Vec<u32>,
    /// Largest epoch in any header ever seen or written for this stream.
    max_epoch: u32,
    /// Slots with an epoch below this are recyclable.
    checkpoint_epoch: u32,
    /// Slot of the current segment.
    slot: usize,
    /// Epoch of the current segment.
    epoch: u32,
    enc: FrameEncoder,
    /// Segment offset past the last byte handed to the kernel; `buf` starts here.
    written_off: u64,
    /// Appended, not yet written bytes of the current segment.
    buf: Vec<u8>,
    /// Record encoding scratch, reused across appends.
    scratch: Vec<u8>,
    shared: Arc<Shared>,
}

impl fmt::Debug for WalStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalStream")
            .field("stream", &self.stream)
            .field("path", &self.path)
            .field("segment_size", &self.segment_size)
            .field("slots", &self.slots)
            .field("epoch", &self.epoch)
            .field("written", &self.written())
            .field("durable", &self.durable())
            .field("buffered", &self.buf.len())
            .field("poisoned", &self.shared.poisoned.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Where a new segment's slot came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SlotSource {
    /// A slot whose segment is below the checkpoint.
    Recycled,
    /// A zero-filled, synced spare.
    Prepared,
    /// An allocated slot never zero-filled.
    Blank,
}

impl WalStream {
    /// Creates stream `stream` for a new database, or after its recovery found nothing.
    ///
    /// An existing file at the stream's path is truncated first, so stale segments can never
    /// be mistaken for new ones. The first slot is zero-filled, its segment (epoch 1) written
    /// and synced, and the directory entry synced, before this returns. Spare slots are not
    /// prepared here: the engine runs [`SpareSegments::prepare`] on a background task.
    pub fn create(
        vfs: &VfsRef,
        db_path: &Path,
        stream: StreamId,
        db_id: [u8; 16],
        opts: WalOptions,
    ) -> Result<WalStream> {
        check_options(&opts)?;
        let path = stream_path(db_path, stream);
        let file = vfs.open(&path, OpenOptions::read_write_create())?;
        file.set_len(0)?;
        let mut s = Self::blank(
            Arc::clone(vfs),
            file,
            path,
            stream,
            db_id,
            opts.segment_size,
            opts.spare_segments,
        );
        s.open_segment(0, 0)?;
        sync_parent(&s.vfs, &s.path)?;
        Ok(s)
    }

    /// A stream over `file` with no current segment yet.
    pub(crate) fn blank(
        vfs: VfsRef,
        file: FileRef,
        path: PathBuf,
        stream: StreamId,
        db_id: [u8; 16],
        segment_size: u64,
        spare: u32,
    ) -> Self {
        let frames = segment_size / FRAME;
        let max_payload = (frames as usize - 1) * (FRAME_SIZE - FRAGMENT_HEADER_LEN);
        Self {
            vfs,
            file,
            path,
            stream,
            db_id,
            segment_size,
            spare,
            max_payload,
            slots: Vec::new(),
            max_epoch: 0,
            checkpoint_epoch: 0,
            slot: 0,
            epoch: 0,
            enc: FrameEncoder::new(0, FRAME),
            written_off: 0,
            buf: Vec::new(),
            scratch: Vec::new(),
            shared: Arc::new(Shared {
                durable: AtomicU64::new(0),
                poisoned: AtomicBool::new(false),
                pool: Mutex::new(Pool::default()),
            }),
        }
    }

    /// Adopts an existing file's slots (from recovery): `epochs` has 0 for a blank slot.
    pub(crate) fn adopt(&mut self, epochs: &[u32], max_epoch: u32, checkpoint_epoch: u32) {
        let mut pool = self.shared.pool();
        pool.total = epochs.len();
        self.slots = epochs
            .iter()
            .enumerate()
            .map(|(i, &e)| {
                if e == 0 {
                    pool.blank.push(i);
                    POOLED
                } else {
                    e
                }
            })
            .collect();
        drop(pool);
        self.max_epoch = max_epoch;
        self.checkpoint_epoch = checkpoint_epoch;
        self.publish_recyclable();
    }

    /// Starts the first segment of a fresh or reopened stream at open time: zero-fills its
    /// slot unless recycled, then writes and syncs the header.
    pub(crate) fn open_segment(&mut self, prev_epoch: u32, prev_end: u32) -> Result<()> {
        let source = self.start_segment(prev_epoch, prev_end, true)?;
        if source != SlotSource::Recycled {
            let zeros = vec![0u8; ZERO_CHUNK.min(self.segment_size as usize)];
            zero_fill(&self.file, self.segment_size, self.slot, &zeros)?;
        }
        self.sync()?;
        Ok(())
    }

    /// A handle for preparing spare slots on a background task.
    pub fn spares(&self) -> SpareSegments {
        SpareSegments {
            file: Arc::clone(&self.file),
            segment_size: self.segment_size,
            target: self.spare,
            shared: Arc::clone(&self.shared),
        }
    }

    /// Rollovers that found neither a recyclable nor a prepared slot and allocated one
    /// inline, on the shard thread (a sign that spares are not being prepared fast enough).
    pub fn inline_grows(&self) -> u64 {
        self.shared.pool().inline_grows
    }

    fn is_recyclable(&self, slot: usize) -> bool {
        let epoch = self.slots[slot];
        epoch != POOLED && epoch < self.checkpoint_epoch
    }

    fn publish_recyclable(&self) {
        let n = (0..self.slots.len())
            .filter(|&i| self.is_recyclable(i))
            .count();
        self.shared.pool().recyclable = n;
    }

    /// Picks the slot for a new segment: recyclable, then prepared, then blank, else a new
    /// slot allocated inline (counted as such unless this is the open-time segment).
    fn take_slot(&mut self, at_open: bool) -> Result<(usize, SlotSource)> {
        if let Some(i) = (0..self.slots.len()).find(|&i| self.is_recyclable(i)) {
            return Ok((i, SlotSource::Recycled));
        }
        let (slot, source) = {
            let mut pool = self.shared.pool();
            if !pool.ready.is_empty() {
                (pool.ready.remove(0), SlotSource::Prepared)
            } else if !pool.blank.is_empty() {
                (pool.blank.remove(0), SlotSource::Blank)
            } else {
                let slot = pool.total;
                pool.total += 1;
                if !at_open {
                    pool.inline_grows += 1;
                }
                drop(pool);
                self.file
                    .allocate(slot as u64 * self.segment_size, self.segment_size)?;
                self.file.sync_all()?;
                (slot, SlotSource::Blank)
            }
        };
        if slot >= self.slots.len() {
            self.slots.resize(slot + 1, POOLED);
        }
        Ok((slot, source))
    }

    /// Starts a new segment (epoch one above every epoch seen) in a free slot, buffering its
    /// header. `buf` must be empty: the previous segment has been written out.
    fn start_segment(
        &mut self,
        prev_epoch: u32,
        prev_end: u32,
        at_open: bool,
    ) -> Result<SlotSource> {
        debug_assert!(self.buf.is_empty());
        let epoch = self
            .max_epoch
            .checked_add(1)
            .filter(|&e| e != POOLED)
            .ok_or_else(|| corrupt("wal epochs exhausted"))?;
        let (slot, source) = self.take_slot(at_open)?;
        self.slots[slot] = epoch;
        self.max_epoch = epoch;
        self.slot = slot;
        self.epoch = epoch;
        self.written_off = 0;
        self.enc = FrameEncoder::new(epoch, FRAME);
        self.buf.resize(FRAME_SIZE, 0);
        let header = SegmentHeader {
            version: FormatVersion::CURRENT,
            stream: self.stream,
            epoch,
            prev_epoch,
            prev_end,
            db_id: self.db_id,
            segment_size: self.segment_size,
        };
        let frame: &mut [u8; FRAME_SIZE] = (&mut self.buf[..FRAME_SIZE])
            .try_into()
            .expect("buffer holds one frame");
        header.encode(frame);
        self.publish_recyclable();
        Ok(source)
    }

    /// The current segment is full: write and sync it, then chain a new one to its end
    /// (FORMAT §10.1 rule 1).
    fn rollover(&mut self) -> Result<()> {
        self.write_buf()?;
        self.file.sync_data()?;
        let end = self.written_off as u32;
        self.shared
            .durable
            .fetch_max(Lsn::new(self.epoch, end).0, Ordering::Release);
        self.start_segment(self.epoch, end, false)?;
        Ok(())
    }

    fn append_pos(&self) -> u64 {
        self.written_off + self.buf.len() as u64
    }

    fn check_poisoned(&self) -> Result<()> {
        if self.shared.poisoned.load(Ordering::Acquire) {
            return Err(Error::Poisoned);
        }
        Ok(())
    }

    /// Poisons the stream if `r` is an I/O or format failure (argument errors leave it usable).
    fn poison_on_err<T>(&self, r: Result<T>) -> Result<T> {
        if matches!(&r, Err(Error::Io(_) | Error::Format(_))) {
            self.shared.poisoned.store(true, Ordering::Release);
        }
        r
    }

    fn write_buf(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            let at = self.slot as u64 * self.segment_size + self.written_off;
            self.file.write_at(&self.buf, at)?;
            self.written_off += self.buf.len() as u64;
            self.buf.clear();
        }
        Ok(())
    }

    /// Frames `scratch` into the buffer, rolling over first if it does not fit.
    fn append_scratch(&mut self) -> Result<Lsn> {
        let need = self.enc.encoded_len(self.scratch.len()) as u64;
        if self.append_pos() + need > self.segment_size {
            self.rollover()?;
        }
        let end = self.enc.encode(&self.scratch, &mut self.buf);
        Ok(Lsn::new(self.epoch, end as u32))
    }
}

impl Wal for WalStream {
    fn stream(&self) -> StreamId {
        self.stream
    }

    fn append(&mut self, record: &WalRecord<'_>, durability: Durability) -> Result<CommitTicket> {
        if durability == Durability::None {
            return Err(Error::InvalidArgument {
                what: "Durability::None commits write no WAL record",
            });
        }
        self.check_poisoned()?;
        self.scratch.clear();
        record.encode(&mut self.scratch);
        if self.scratch.len() > self.max_payload {
            return Err(Error::RecordTooLarge);
        }
        let end = self.append_scratch();
        let end = self.poison_on_err(end)?;
        Ok(CommitTicket {
            stream: self.stream,
            end,
            durability,
        })
    }

    fn write(&mut self) -> Result<Lsn> {
        self.check_poisoned()?;
        let r = self.write_buf();
        self.poison_on_err(r)?;
        Ok(self.written())
    }

    fn sync(&mut self) -> Result<Lsn> {
        let lsn = self.write()?;
        let r = self.file.sync_data().map_err(Error::from);
        self.poison_on_err(r)?;
        self.shared.durable.fetch_max(lsn.0, Ordering::Release);
        Ok(lsn)
    }

    fn submit_sync(&mut self) -> Result<Completion<Lsn>> {
        let lsn = self.write()?;
        let shared = Arc::clone(&self.shared);
        Ok(self.file.submit_sync_data().map(move |r| {
            if let Err(e) = r {
                shared.poisoned.store(true, Ordering::Release);
                return Err(e);
            }
            shared.durable.fetch_max(lsn.0, Ordering::Release);
            Ok(lsn)
        }))
    }

    fn written(&self) -> Lsn {
        Lsn::new(self.epoch, self.written_off as u32)
    }

    fn durable(&self) -> Lsn {
        Lsn(self.shared.durable.load(Ordering::Acquire))
    }

    fn satisfies(&self, ticket: &CommitTicket) -> bool {
        ticket.met_by(self.stream, self.written(), self.durable())
    }

    fn checkpoint(&mut self, upto: Lsn) -> Result<()> {
        // The current segment is never recycled, so the checkpoint epoch never exceeds it.
        let epoch = upto.epoch().min(self.epoch);
        self.checkpoint_epoch = self.checkpoint_epoch.max(epoch);
        self.publish_recyclable();
        Ok(())
    }

    fn spares(&self) -> Option<SpareSegments> {
        Some(WalStream::spares(self))
    }

    fn remove(self: Box<Self>) -> Result<()> {
        let WalStream {
            vfs, file, path, ..
        } = *self;
        drop(file);
        vfs.remove(&path)?;
        sync_parent(&vfs, &path)
    }
}

//! The sidecar-file stream: one file per stream, made of preallocated slots that are recycled
//! after checkpoint.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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

pub(crate) fn corrupt(what: &'static str) -> Error {
    Error::Format(pigeonhole_format::Error::Corrupt { what })
}

pub(crate) fn check_options(opts: &WalOptions) -> Result<()> {
    let size = opts.segment_size;
    if !size.is_multiple_of(FRAME) || !(2 * FRAME..=MAX_SEGMENT_SIZE).contains(&size) {
        return Err(corrupt("wal segment size"));
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

/// The sidecar-file implementation: one file per stream, made of preallocated segments that
/// are recycled after checkpoint.
///
/// Appends are buffered in memory and framed as they arrive; [`Wal::write`] hands the buffer
/// to the kernel with one positional write. When a segment fills, the stream writes what is
/// left of it, fdatasyncs it (the one blocking sync on the shard's path, once per segment) and
/// then buffers the successor's header with `prev_end` set to where the full segment ended.
/// Slots whose epoch is below the latest checkpoint are reused before the file grows; growth
/// preallocates whole slots and syncs the new size, so later fdatasyncs touch no metadata.
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
    /// Epoch of the segment each slot holds; 0 for a blank slot.
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
    /// Durable position, updated by sync completions on the I/O thread.
    durable: Arc<AtomicU64>,
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
            .finish_non_exhaustive()
    }
}

impl WalStream {
    /// Creates stream `stream` for a new database, or after its recovery found nothing.
    ///
    /// An existing file at the stream's path is truncated first, so stale segments can never
    /// be mistaken for new ones. The first segment (epoch 1) is written and synced before
    /// this returns, with the directory entry.
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
        s.start_segment(0, 0)?;
        s.sync()?;
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
            durable: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Adopts an existing file's slots (from recovery).
    pub(crate) fn adopt(&mut self, slots: Vec<u32>, max_epoch: u32, checkpoint_epoch: u32) {
        self.slots = slots;
        self.max_epoch = max_epoch;
        self.checkpoint_epoch = checkpoint_epoch;
    }

    fn is_free(&self, slot: usize) -> bool {
        let epoch = self.slots[slot];
        epoch == 0 || epoch < self.checkpoint_epoch
    }

    /// Preallocates `n` more slots and makes the new size durable.
    fn grow(&mut self, n: usize) -> Result<()> {
        let start = self.slots.len() as u64 * self.segment_size;
        self.file.allocate(start, n as u64 * self.segment_size)?;
        self.file.sync_all()?;
        self.slots.extend(std::iter::repeat_n(0, n));
        Ok(())
    }

    /// Starts a new segment (epoch one above every epoch seen) in a free slot, buffering its
    /// header. `buf` must be empty: the previous segment has been written out.
    pub(crate) fn start_segment(&mut self, prev_epoch: u32, prev_end: u32) -> Result<()> {
        debug_assert!(self.buf.is_empty());
        let epoch = self
            .max_epoch
            .checked_add(1)
            .ok_or_else(|| corrupt("wal epochs exhausted"))?;
        let slot = match (0..self.slots.len()).find(|&i| self.is_free(i)) {
            Some(i) => i,
            None => {
                self.grow(1)?;
                self.slots.len() - 1
            }
        };
        self.slots[slot] = epoch;
        self.max_epoch = epoch;
        let free = (0..self.slots.len()).filter(|&i| self.is_free(i)).count();
        if free < self.spare as usize {
            self.grow(self.spare as usize - free)?;
        }
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
        Ok(())
    }

    /// The current segment is full: write and sync it, then chain a new one to its end
    /// (FORMAT §10.1 rule 1).
    fn rollover(&mut self) -> Result<()> {
        self.write()?;
        self.file.sync_data()?;
        let end = self.written_off as u32;
        self.durable
            .fetch_max(Lsn::new(self.epoch, end).0, Ordering::Release);
        self.start_segment(self.epoch, end)
    }

    fn append_pos(&self) -> u64 {
        self.written_off + self.buf.len() as u64
    }
}

impl Wal for WalStream {
    fn stream(&self) -> StreamId {
        self.stream
    }

    fn append(&mut self, record: &WalRecord<'_>, durability: Durability) -> Result<CommitTicket> {
        self.scratch.clear();
        record.encode(&mut self.scratch);
        if self.scratch.len() > self.max_payload {
            return Err(Error::RecordTooLarge);
        }
        let need = self.enc.encoded_len(self.scratch.len()) as u64;
        if self.append_pos() + need > self.segment_size {
            self.rollover()?;
        }
        let end = self.enc.encode(&self.scratch, &mut self.buf);
        Ok(CommitTicket {
            stream: self.stream,
            end: Lsn::new(self.epoch, end as u32),
            durability,
        })
    }

    fn write(&mut self) -> Result<Lsn> {
        if !self.buf.is_empty() {
            let at = self.slot as u64 * self.segment_size + self.written_off;
            self.file.write_at(&self.buf, at)?;
            self.written_off += self.buf.len() as u64;
            self.buf.clear();
        }
        Ok(self.written())
    }

    fn sync(&mut self) -> Result<Lsn> {
        let lsn = self.write()?;
        self.file.sync_data()?;
        self.durable.fetch_max(lsn.0, Ordering::Release);
        Ok(lsn)
    }

    fn submit_sync(&mut self) -> Result<Completion<Lsn>> {
        let lsn = self.write()?;
        let durable = Arc::clone(&self.durable);
        Ok(self.file.submit_sync_data().map(move |r| {
            r?;
            durable.fetch_max(lsn.0, Ordering::Release);
            Ok(lsn)
        }))
    }

    fn written(&self) -> Lsn {
        Lsn::new(self.epoch, self.written_off as u32)
    }

    fn durable(&self) -> Lsn {
        Lsn(self.durable.load(Ordering::Acquire))
    }

    fn satisfies(&self, ticket: &CommitTicket) -> bool {
        ticket.met_by(self.stream, self.written(), self.durable())
    }

    fn checkpoint(&mut self, upto: Lsn) -> Result<()> {
        // The current segment is never recycled, so the checkpoint epoch never exceeds it.
        let epoch = upto.epoch().min(self.epoch);
        self.checkpoint_epoch = self.checkpoint_epoch.max(epoch);
        Ok(())
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

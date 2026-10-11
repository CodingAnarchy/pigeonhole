//! Replay of one stream from its checkpoint, following segment chaining (FORMAT §10.1).

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pigeonhole_format::wal::{Decoded, FRAME_SIZE, FrameDecoder, FrameEncoder, WalRecord};
use pigeonhole_format::{Lsn, Seqno, StreamId};
use pigeonhole_io::{FileRef, VfsRef};

use crate::stream::{FRAME, Grid, check_options, corrupt, open_existing};
use crate::{Result, WalOptions, WalStream, stream_path};

/// Frames read from the file at a time.
const CHUNK_FRAMES: u64 = 32;

/// How far above the largest epoch recovery saw (every header and the replayed end) a
/// reopened stream's first segment starts (FORMAT §10.1 rule 2, D209). At most two headers
/// are ever not durable at once, because a header is written only after its predecessor is
/// durable: when segment S1 fills, its own header (written once S0's sync completed) waits on
/// S1's sync, and S2's header is held back for that same sync while S2's records are written
/// (rule 1). A power loss that keeps S2's frames and drops both headers leaves frames two
/// epochs above the largest header on disk, in a recycled slot the reopened stream may take;
/// starting three above means it never takes their epoch (#508). The reopened segment's own
/// header is synced before anything is appended, so a later recovery's max seen is at least
/// its epoch and skipped epochs are never reached again.
const REOPEN_EPOCH_GAP: u32 = 3;

/// Replays one stream from its checkpoint. A lending reader: each record borrows the reader's
/// buffer until the next call.
///
/// Within a segment, replay stops at the first fragment that is unused, stale, bad or
/// incomplete. If another segment's header names this segment and exactly this offset as its
/// predecessor, replay continues there (end of segment); otherwise the log ends (torn tail).
/// A successor naming this segment with a different offset means synced data was lost and
/// fails with [`Error::Format`](crate::Error::Format) rather than silently dropping it.
///
/// A checkpoint of epoch 0 (nothing checkpointed yet) replays from the stream's first
/// segment. A missing stream file fails with [`Error::Io`](crate::Error::Io) (`NotFound`),
/// after which the engine creates the stream.
///
/// ```
/// use std::path::Path;
/// use pigeonhole_format::wal::{StreamList, WalRecord};
/// use pigeonhole_format::{Durability, Lsn, StreamId};
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_wal::{Recovery, Wal, WalOptions, WalStream};
///
/// # fn main() -> pigeonhole_wal::Result<()> {
/// let vfs: pigeonhole_io::VfsRef = SimVfs::new(1);
/// let db = Path::new("/db/data.phdb");
/// let mut opts = WalOptions::default();
/// opts.segment_size = 2 * 32 * 1024;
/// let mut wal = WalStream::create(&vfs, db, StreamId(0), [0; 16], opts)?;
/// let mut tickets = Vec::new();
/// for seqno in 1..=3 {
///     let rec = WalRecord::Commit { seqno, participants: StreamList::new(&[0, 0]).unwrap() };
///     tickets.push(wal.append(&rec, Durability::GroupSync)?);
/// }
/// wal.sync()?;
///
/// // Replay from after the first record: the manifest recorded its end as the checkpoint.
/// let mut r = Recovery::open(&vfs, db, StreamId(0), [0; 16], tickets[0].end)?;
/// let mut seqnos = Vec::new();
/// while let Some((end, rec)) = r.next_record()? {
///     if let WalRecord::Commit { seqno, .. } = rec {
///         seqnos.push((end, seqno));
///     }
/// }
/// assert_eq!(seqnos, [(tickets[1].end, 2), (tickets[2].end, 3)]);
/// assert_eq!(r.end(), tickets[2].end);
/// assert_eq!(r.max_seqno(), 3);
/// # Ok(())
/// # }
/// ```
pub struct Recovery {
    vfs: VfsRef,
    file: FileRef,
    path: PathBuf,
    stream: StreamId,
    db_id: [u8; 16],
    grid: Option<Grid>,
    checkpoint: Lsn,
    /// The segment being replayed; `None` once the log has ended.
    cur: Option<Cursor>,
    end: Lsn,
    max_seqno: Seqno,
    /// Frames of the current segment read from the file.
    chunk: Vec<u8>,
    chunk_slot: usize,
    /// Segment offset of `chunk[0]`.
    chunk_start: u64,
}

#[derive(Debug)]
struct Cursor {
    slot: usize,
    epoch: u32,
    dec: FrameDecoder,
    /// Segment offset of the frame to feed next.
    frame: u64,
    /// Just past the last complete record (the segment's start offset if none).
    last_end: u64,
}

impl fmt::Debug for Recovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recovery")
            .field("stream", &self.stream)
            .field("path", &self.path)
            .field("checkpoint", &self.checkpoint)
            .field("end", &self.end)
            .field("max_seqno", &self.max_seqno)
            .field("cursor", &self.cur)
            .finish_non_exhaustive()
    }
}

fn seqno_of(rec: &WalRecord<'_>) -> Seqno {
    match rec {
        WalRecord::Batch { seqno, .. }
        | WalRecord::Prepare { seqno, .. }
        | WalRecord::Commit { seqno, .. } => *seqno,
    }
}

impl Recovery {
    /// Opens `stream` for replay starting at `checkpoint` (from the manifest).
    pub fn open(
        vfs: &VfsRef,
        db_path: &Path,
        stream: StreamId,
        db_id: [u8; 16],
        checkpoint: Lsn,
    ) -> Result<Recovery> {
        let path = stream_path(db_path, stream);
        let file = vfs.open(&path, open_existing())?;
        // A process that died between a slot's `allocate` and its `sync_all` left the page
        // cache's length longer than the disk's. The grid (and the blank slots the stream
        // adopts) come from that length, and appends sync with `sync_data`: make it durable.
        file.sync_all()?;
        // Likewise its directory entry: an open no longer waits for the directory sync of
        // the stream files it creates (#158, D203), so a process that died before that sync
        // finished left this file's name in the page cache only. The stream is adopted and
        // appended to, and its new commits would vanish with the name in a power loss.
        crate::sync_parent(vfs, &path)?;
        let grid = Grid::read(&file, stream, db_id)?;
        let start = match &grid {
            Some(g) if checkpoint.epoch() == 0 => g
                .headers
                .iter()
                .enumerate()
                .filter_map(|(i, h)| h.map(|h| (h.epoch, i)))
                .min()
                .map(|(epoch, slot)| (slot, epoch, FRAME)),
            Some(g) => g.slot_of(checkpoint.epoch()).map(|slot| {
                (
                    slot,
                    checkpoint.epoch(),
                    u64::from(checkpoint.offset()).max(FRAME),
                )
            }),
            None if checkpoint.epoch() == 0 => None,
            None => return Err(corrupt("wal checkpoint segment missing")),
        };
        let missing = start.is_none() && checkpoint.epoch() != 0;
        let cur = start.map(|(slot, epoch, offset)| Cursor {
            slot,
            epoch,
            dec: FrameDecoder::new(epoch, offset),
            frame: offset - offset % FRAME,
            last_end: offset,
        });
        let end = cur
            .as_ref()
            .map_or(checkpoint, |c| Lsn::new(c.epoch, c.last_end as u32));
        let mut this = Self {
            vfs: Arc::clone(vfs),
            file,
            path,
            stream,
            db_id,
            grid,
            checkpoint,
            cur,
            end,
            max_seqno: 0,
            chunk: Vec::new(),
            chunk_slot: usize::MAX,
            chunk_start: 0,
        };
        if missing {
            // The checkpoint's segment never reached the disk: everything before the
            // checkpoint was flushed, so nothing in it is needed. Treat it as a segment
            // that stopped exactly at the checkpoint: a post-recovery successor chained
            // there continues the log; otherwise the log ends at the checkpoint, unless a
            // later segment exists that nothing reaches (lost synced data).
            this.chain(checkpoint.epoch(), u64::from(checkpoint.offset()))?;
            let grid = this.grid.as_ref().expect("checked above");
            let orphaned = grid
                .headers
                .iter()
                .flatten()
                .any(|h| h.epoch > checkpoint.epoch());
            if this.cur.is_none() && orphaned {
                return Err(corrupt("wal checkpoint segment missing"));
            }
        }
        Ok(this)
    }

    fn segment_size(&self) -> u64 {
        self.grid.as_ref().map_or(0, |g| g.segment_size)
    }

    /// Makes sure `chunk` holds the frame at `frame` of `slot`.
    fn load(&mut self, slot: usize, frame: u64) -> Result<()> {
        let have = self.chunk_slot == slot
            && frame >= self.chunk_start
            && frame + FRAME <= self.chunk_start + self.chunk.len() as u64;
        if have {
            return Ok(());
        }
        let segment_size = self.segment_size();
        let len = (CHUNK_FRAMES * FRAME).min(segment_size - frame) as usize;
        self.chunk.resize(len, 0);
        self.file
            .read_at(&mut self.chunk, slot as u64 * segment_size + frame)?;
        self.chunk_slot = slot;
        self.chunk_start = frame;
        Ok(())
    }

    /// The segment with `epoch` stopped at `stop`: continue in its successor or end the log.
    fn chain(&mut self, epoch: u32, stop: u64) -> Result<()> {
        let grid = self.grid.as_ref().expect("a segment was being replayed");
        let mut successors = grid
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, h)| h.filter(|h| h.prev_epoch == epoch).map(|h| (i, h)));
        match (successors.next(), successors.next()) {
            (None, _) => {
                self.end = Lsn::new(epoch, stop as u32);
                self.cur = None;
            }
            (Some((slot, h)), None) => {
                if u64::from(h.prev_end) != stop {
                    return Err(corrupt("wal segment chain: predecessor end mismatch"));
                }
                self.cur = Some(Cursor {
                    slot,
                    epoch: h.epoch,
                    dec: FrameDecoder::new(h.epoch, FRAME),
                    frame: FRAME,
                    last_end: FRAME,
                });
                self.end = Lsn::new(h.epoch, FRAME_SIZE as u32);
            }
            (Some(_), Some(_)) => {
                return Err(corrupt("wal segment chain: several successors"));
            }
        }
        Ok(())
    }

    /// The next record and its end position, or `None` at the end of the log.
    pub fn next_record(&mut self) -> Result<Option<(Lsn, WalRecord<'_>)>> {
        loop {
            let Some(cur) = &self.cur else {
                return Ok(None);
            };
            let (slot, epoch, frame, last_end) = (cur.slot, cur.epoch, cur.frame, cur.last_end);
            if frame >= self.segment_size() {
                // The segment ran out of frames: its data stops after its last record.
                self.chain(epoch, last_end)?;
                continue;
            }
            self.load(slot, frame)?;
            let at = (frame - self.chunk_start) as usize;
            let bytes = &self.chunk[at..at + FRAME_SIZE];
            let cur = self.cur.as_mut().expect("checked above");
            match cur.dec.decode(bytes)? {
                Some(Decoded::Record { offset }) => {
                    let len = cur.dec.record().len();
                    let end = offset + FrameEncoder::new(epoch, offset).encoded_len(len) as u64;
                    cur.last_end = end;
                    self.end = Lsn::new(epoch, end as u32);
                    break;
                }
                Some(Decoded::Stop { offset }) => self.chain(epoch, offset)?,
                None => cur.frame += FRAME,
            }
        }
        let cur = self.cur.as_ref().expect("a record was just decoded");
        let record = WalRecord::decode(cur.dec.record())?;
        self.max_seqno = self.max_seqno.max(seqno_of(&record));
        Ok(Some((self.end, record)))
    }

    /// Where the valid log ends (after `next_record` returned `None`).
    pub fn end(&self) -> Lsn {
        self.end
    }

    /// Every seqno seen in any record, including PREPAREs later discarded, is at most this;
    /// the engine starts `next_seqno` above it so a seqno is never reused.
    pub fn max_seqno(&self) -> Seqno {
        self.max_seqno
    }

    /// Reopens the stream for appending: starts a fresh segment with epoch one above the
    /// largest epoch in any segment header, chained (`prev_epoch`, `prev_end`) to where replay
    /// ended. The torn segment is never appended to; its slot is recycled after checkpoint.
    ///
    /// The new segment's header is written and synced before this returns, so a checkpoint
    /// taken at the new position names a segment that exists. A recycled slot (lower epochs
    /// only) or a slot allocated past the file's end (never written: zeros) is used as is; a blank one is
    /// zero-filled first, since frames of a segment whose header write was torn may carry
    /// the epoch the new segment takes. `opts.segment_size` is used only if the file holds no
    /// segment at all; otherwise the file's slot size is kept. Spare slots are not prepared
    /// here: the engine runs [`SpareSegments::prepare`](crate::SpareSegments::prepare) on a
    /// background task.
    pub fn into_stream(self, opts: WalOptions) -> Result<WalStream> {
        check_options(&opts)?;
        let Recovery {
            vfs,
            file,
            path,
            stream,
            db_id,
            grid,
            checkpoint,
            end,
            ..
        } = self;
        let s = match grid {
            Some(grid) => {
                let epochs: Vec<u32> = grid
                    .headers
                    .iter()
                    .map(|h| h.map_or(0, |h| h.epoch))
                    .collect();
                // Above every header, and above the recovered end even when its segment
                // never reached the disk, so the new segment never shares its epoch with the
                // position it chains to; and above every epoch whose frames a power loss may
                // have kept without their header (FORMAT §10.1 rule 2, D209, #508).
                let max_epoch = grid
                    .max_epoch()
                    .max(end.epoch())
                    .checked_add(REOPEN_EPOCH_GAP - 1)
                    .ok_or_else(|| crate::stream::corrupt("wal epochs exhausted"))?;
                let mut s = WalStream::blank(
                    vfs,
                    file,
                    path,
                    stream,
                    db_id,
                    grid.segment_size,
                    opts.spare_segments,
                );
                s.adopt(&epochs, max_epoch, checkpoint.epoch());
                s.open_segment(end.epoch(), end.offset())?;
                s
            }
            None => {
                file.set_len(0)?;
                let mut s = WalStream::blank(
                    vfs,
                    file,
                    path,
                    stream,
                    db_id,
                    opts.segment_size,
                    opts.spare_segments,
                );
                s.open_segment(0, 0)?;
                s
            }
        };
        Ok(s)
    }
}

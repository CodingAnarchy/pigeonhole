//! The change feed's reader of one stream (D310 PR 3, #1248): a live, read-only cursor over
//! the stream file, following segment chaining like [`Recovery`](crate::Recovery), that
//! never mistakes a recycled slot for a short log.

use std::fmt;
use std::path::{Path, PathBuf};

use pigeonhole_format::wal::{Decoded, FRAME_SIZE, FrameDecoder, FrameEncoder, WalRecord};
use pigeonhole_format::{Lsn, StreamId};
use pigeonhole_io::{FileRef, OpenOptions, VfsRef};

use crate::stream::{FRAME, Grid};
use crate::{Result, stream_path};

/// Frames read from the file at a time.
const CHUNK_FRAMES: u64 = 32;

/// What [`FeedCursor::step`] found.
#[derive(Debug)]
pub enum Step<'a> {
    /// A complete record.
    Record {
        /// Where the record starts (its first fragment).
        start: Lsn,
        /// Just past the record: where the next one starts.
        end: Lsn,
        /// The `feed_session` of the segment holding it (FORMAT §10.2): 0 for a segment a
        /// session without the feed wrote.
        session: u64,
        /// The record.
        record: WalRecord<'a>,
    },
    /// The stream's readable end, for now: the cursor stays just past the last complete
    /// record, and the next call reads the file (and its segment headers) again. A
    /// successor whose header is not on disk yet (FORMAT §10.1 rule 1 holds it back until
    /// its predecessor is synced) also reads as this: the caller compares
    /// [`FeedCursor::epoch`] with the stream's [`FeedMarks::head`](crate::FeedMarks::head) to
    /// tell "more is coming" from "caught up".
    End,
    /// The cursor's segment is no longer in the file: its slot was recycled (a newer epoch's
    /// header is there now), or it was never written. Its records are gone; a feed reading
    /// them has expired. Never reported for a short read: a recycled slot shows as a newer
    /// epoch, in its header or in its frames.
    Gone,
    /// The records the cursor read past some point of its segment are gone: the successor
    /// names an earlier end for it, so a recovery cut the segment there (a power loss took
    /// the unsynced records the cursor had read; the next writer replayed only up to the
    /// cut). A feed that delivered any of them has lost them; one resuming past the cut
    /// expires. Distinct from a successor naming a later end, which is corruption.
    Cut,
}

/// Where the successor of segment `epoch` in `grid` says it ends, if exactly one is there
/// (several are corruption, which [`FeedCursor::chain`] reports at the stop).
fn cut_of_grid(grid: &Grid, epoch: u32) -> Option<u64> {
    let mut successors = grid
        .headers
        .iter()
        .flatten()
        .filter(|h| h.prev_epoch == epoch);
    match (successors.next(), successors.next()) {
        (Some(h), None) => Some(u64::from(h.prev_end)),
        _ => None,
    }
}

/// Whether [`FeedCursor::chain`] moved to the successor.
enum Chained {
    Moved,
    /// No successor on disk yet.
    No,
    /// The successor names an earlier end than the cursor read to ([`Step::Cut`]).
    Cut,
}

/// A live, read-only reader of one WAL stream from a record boundary (D310).
///
/// Unlike [`Recovery`](crate::Recovery), which reads the segment headers once, the cursor
/// reads them again whenever it stops, so it follows the stream as the writer appends and
/// rolls over. A record it returns is always the record that was written there: every frame
/// carries its segment's epoch, so frames of a newer segment in a recycled slot end the
/// segment's data rather than being read as part of it.
///
/// ```
/// use std::path::Path;
/// use pigeonhole_format::wal::{StreamList, WalRecord};
/// use pigeonhole_format::{Durability, Lsn, StreamId};
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_wal::{FeedCursor, Step, Wal, WalOptions, WalStream};
///
/// # fn main() -> pigeonhole_wal::Result<()> {
/// let vfs: pigeonhole_io::VfsRef = SimVfs::new(1);
/// let db = Path::new("/db/data.phdb");
/// let mut opts = WalOptions::default();
/// opts.segment_size = 2 * 32 * 1024;
/// let mut wal = WalStream::create(&vfs, db, StreamId(0), [0; 16], opts)?;
/// let commit = |seqno| WalRecord::Commit { seqno, participants: StreamList::new(&[0, 0]).unwrap() };
/// wal.append(&commit(1), Durability::GroupSync)?;
/// wal.sync()?;
///
/// let mut cursor = FeedCursor::open(&vfs, db, StreamId(0), [0; 16], Lsn::default())?;
/// assert!(matches!(cursor.step()?, Step::Record { record: WalRecord::Commit { seqno: 1, .. }, .. }));
/// assert!(matches!(cursor.step()?, Step::End));
/// // The writer goes on; the cursor picks up from where it stopped.
/// wal.append(&commit(2), Durability::GroupSync)?;
/// wal.sync()?;
/// assert!(matches!(cursor.step()?, Step::Record { record: WalRecord::Commit { seqno: 2, .. }, .. }));
/// # Ok(())
/// # }
/// ```
pub struct FeedCursor {
    file: FileRef,
    path: PathBuf,
    stream: StreamId,
    db_id: [u8; 16],
    /// The segment headers as last read; `None` while the file holds no segment.
    grid: Option<Grid>,
    /// The segment being read; `None` once it is gone.
    cur: Option<Cursor>,
    /// Whether the last call stopped (the next one reads the headers again first).
    stopped: bool,
    /// Whether the headers were read in this call and no frame was decoded since: a stop
    /// found then is where the cursor resumed, so it needs no second read (an idle stream
    /// reads its headers once per stop, not twice).
    fresh: bool,
    /// Whether the next `load` follows a header read: it reads one frame, not a chunk. An
    /// idle stream's resume needs only the frame it stopped in; a busy one reads full
    /// chunks after it.
    short_load: bool,
    /// Header reads and bytes of frames read (an inspection count for tests).
    #[cfg(test)]
    reads: (u64, u64),
    chunk: Vec<u8>,
    chunk_slot: usize,
    /// Segment offset of `chunk[0]`.
    chunk_start: u64,
    /// A stop short of its successor's recorded end, deferred once (#1370): `(epoch, stop)`.
    deferred: Option<(u32, u64)>,
}

#[derive(Debug)]
struct Cursor {
    slot: usize,
    epoch: u32,
    session: u64,
    dec: FrameDecoder,
    /// Segment offset of the frame to feed next.
    frame: u64,
    /// Just past the last complete record (where reading resumes after a stop).
    last_end: u64,
}

impl fmt::Debug for FeedCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FeedCursor")
            .field("stream", &self.stream)
            .field("path", &self.path)
            .field("cursor", &self.cur)
            .finish_non_exhaustive()
    }
}

impl FeedCursor {
    /// Opens `stream` read-only, positioned at `at`: a record boundary (a record's start or
    /// end, as [`Step::Record`] reports them), or [`Lsn::default`] for the oldest segment in
    /// the file. A position whose segment is not in the file reads as [`Step::Gone`].
    pub fn open(
        vfs: &VfsRef,
        db_path: &Path,
        stream: StreamId,
        db_id: [u8; 16],
        at: Lsn,
    ) -> Result<FeedCursor> {
        let path = stream_path(db_path, stream);
        let file = vfs.open(&path, OpenOptions::read())?;
        let grid = Grid::read(&file, stream, db_id)?;
        let mut this = FeedCursor {
            file,
            path,
            stream,
            db_id,
            grid,
            cur: None,
            stopped: false,
            fresh: false,
            short_load: false,
            #[cfg(test)]
            reads: (0, 0),
            chunk: Vec::new(),
            chunk_slot: usize::MAX,
            chunk_start: 0,
            deferred: None,
        };
        this.cur = this.place(at);
        Ok(this)
    }

    /// The cursor at `at` in the grid as read, or `None` where its segment isn't there.
    fn place(&self, at: Lsn) -> Option<Cursor> {
        let grid = self.grid.as_ref()?;
        let (slot, epoch, offset) = grid.start(at)?;
        let session = grid.headers[slot].map_or(0, |h| h.feed_session);
        Some(Cursor {
            slot,
            epoch,
            session,
            dec: FrameDecoder::new(epoch, offset),
            frame: offset - offset % FRAME,
            last_end: offset,
        })
    }

    /// The epoch of the segment the cursor is in (0 once it is gone).
    pub fn epoch(&self) -> u32 {
        self.cur.as_ref().map_or(0, |c| c.epoch)
    }

    /// Where the cursor is: just past the last record it returned (or where it opened).
    pub fn position(&self) -> Lsn {
        self.cur
            .as_ref()
            .map_or(Lsn::default(), |c| Lsn::new(c.epoch, c.last_end as u32))
    }

    /// Where the successor of segment `epoch` says it ends, as the headers were last read.
    fn cut_of(&self, epoch: u32) -> Option<u64> {
        self.grid.as_ref().and_then(|g| cut_of_grid(g, epoch))
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
        let frames = if std::mem::take(&mut self.short_load) {
            1
        } else {
            CHUNK_FRAMES
        };
        let len = (frames * FRAME).min(segment_size - frame) as usize;
        self.chunk.resize(len, 0);
        self.file
            .read_at(&mut self.chunk, slot as u64 * segment_size + frame)?;
        #[cfg(test)]
        {
            self.reads.1 += len as u64;
        }
        self.chunk_slot = slot;
        self.chunk_start = frame;
        Ok(())
    }

    /// After a stop: reads the headers again and checks the cursor's segment is still where
    /// it was. Returns `false` once it is gone.
    fn refresh(&mut self) -> Result<bool> {
        let Some(cur) = &self.cur else {
            return Ok(false);
        };
        let (slot, epoch) = (cur.slot, cur.epoch);
        self.grid = Grid::read(&self.file, self.stream, self.db_id)?;
        #[cfg(test)]
        {
            self.reads.0 += 1;
        }
        // Cached frames may predate a rewrite of the slot.
        self.chunk_slot = usize::MAX;
        self.short_load = true;
        let here = self
            .grid
            .as_ref()
            .and_then(|g| g.headers.get(slot).copied().flatten())
            .is_some_and(|h| h.epoch == epoch);
        if !here {
            self.cur = None;
        }
        Ok(here)
    }

    /// The segment stopped at `stop`: moves to its successor if one is on disk. A successor
    /// naming an earlier end means a recovery cut the segment before `stop` ([`Step::Cut`]);
    /// a later one, that synced data was lost (corrupt).
    fn chain(&mut self, epoch: u32, stop: u64) -> Result<Chained> {
        let Some(grid) = self.grid.as_ref() else {
            return Ok(Chained::No);
        };
        let mut successors = grid
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, h)| h.filter(|h| h.prev_epoch == epoch).map(|h| (i, h)));
        let (slot, h) = match (successors.next(), successors.next()) {
            (None, _) => return Ok(Chained::No),
            (Some(s), None) => s,
            (Some(_), Some(_)) => {
                return Err(crate::stream::corrupt(
                    "wal segment chain: several successors",
                ));
            }
        };
        if u64::from(h.prev_end) < stop {
            return Ok(Chained::Cut);
        }
        if u64::from(h.prev_end) != stop {
            // Short of the successor's end: the frames past `stop` may have been read while
            // they were being written (a torn read; the successor landed in between). Read
            // again from here first, once (#1370). By M1 a torn read only hits records past
            // the frontier the feed loaded before reading, so stopping here skips nothing.
            // The same short stop on the second look is data a sync covered and lost.
            if self.deferred != Some((epoch, stop)) {
                self.deferred = Some((epoch, stop));
                return Ok(Chained::No);
            }
            return Err(crate::stream::corrupt(
                "wal segment chain: predecessor end mismatch",
            ));
        }
        self.deferred = None;
        self.cur = Some(Cursor {
            slot,
            epoch: h.epoch,
            session: h.feed_session,
            dec: FrameDecoder::new(h.epoch, FRAME),
            frame: FRAME,
            last_end: FRAME,
        });
        Ok(Chained::Moved)
    }

    /// The next record, or where reading stops for now ([`Step::End`]) or for good
    /// ([`Step::Gone`]).
    pub fn step(&mut self) -> Result<Step<'_>> {
        if self.stopped {
            self.stopped = false;
            if !self.refresh()? {
                return Ok(Step::Gone);
            }
            self.fresh = true;
        }
        loop {
            let Some(cur) = &self.cur else {
                return Ok(Step::Gone);
            };
            let (slot, epoch, frame, last_end) = (cur.slot, cur.epoch, cur.frame, cur.last_end);
            // A successor already on disk names where this segment ends. Past it, any frame
            // still here is one a power loss left after the recovery cut it (simp724's C1 on
            // #1328): CRC-valid, but discarded, and its seqnos may be reused. A cursor there
            // (a token resumed past the cut) has lost what it read, so it is cut before
            // reading anything more.
            let cut = self.cut_of(epoch);
            if cut.is_some_and(|c| last_end > c) {
                self.cur = None;
                return Ok(Step::Cut);
            }
            let stop = if frame >= self.segment_size() {
                Some(last_end)
            } else {
                self.load(slot, frame)?;
                let at = (frame - self.chunk_start) as usize;
                let bytes = &self.chunk[at..at + FRAME_SIZE];
                let cur = self.cur.as_mut().expect("checked above");
                match cur.dec.decode(bytes)? {
                    Some(Decoded::Record { offset }) if cut.is_some_and(|c| offset >= c) => {
                        // A frame past the cut (above): never delivered.
                        self.cur = None;
                        return Ok(Step::Cut);
                    }
                    Some(Decoded::Record { offset }) => {
                        self.fresh = false;
                        let len = cur.dec.record().len();
                        let end = offset + FrameEncoder::new(epoch, offset).encoded_len(len) as u64;
                        cur.last_end = end;
                        let start = Lsn::new(epoch, offset as u32);
                        let session = cur.session;
                        let cur = self.cur.as_ref().expect("checked above");
                        let record = WalRecord::decode(cur.dec.record())?;
                        return Ok(Step::Record {
                            start,
                            end: Lsn::new(epoch, end as u32),
                            session,
                            record,
                        });
                    }
                    Some(Decoded::Stop { offset }) => Some(offset),
                    None => {
                        cur.frame += FRAME;
                        None
                    }
                }
            };
            let Some(stop) = stop else { continue };
            // The segment's valid data ends at `stop` for now: a successor on disk takes
            // over, else this is the readable end. Read the headers first: the successor
            // may have been written since they were read, or the slot recycled. Unless this
            // call read them and decoded nothing since (`fresh`, an idle stream): chaining
            // then goes by headers read just before these frames, as a poll a moment earlier
            // would have, a short stop is deferred once as below (#1370), and the next call
            // reads the headers again before reading on.
            if !std::mem::take(&mut self.fresh) && !self.refresh()? {
                return Ok(Step::Gone);
            }
            match self.chain(epoch, stop)? {
                Chained::Moved => continue,
                Chained::Cut => {
                    self.cur = None;
                    return Ok(Step::Cut);
                }
                Chained::No => {}
            }
            // Not moved: resume just past the last complete record next time.
            let cur = self.cur.as_mut().expect("refresh kept it");
            cur.dec = FrameDecoder::new(epoch, cur.last_end);
            cur.frame = cur.last_end - cur.last_end % FRAME;
            self.stopped = true;
            return Ok(Step::End);
        }
    }
}

#[cfg(test)]
mod read_tests {
    use super::*;
    use crate::{Wal, WalOptions, WalStream};
    use pigeonhole_format::wal::BatchBuilder;
    use pigeonhole_format::{Durability, FamilyId, Kind, TableId};
    use pigeonhole_io::sim::SimVfs;

    const DB_ID: [u8; 16] = [0xAB; 16];

    fn append(wal: &mut WalStream, seqno: u64) {
        let mut b = BatchBuilder::new();
        b.push(
            TableId(1),
            FamilyId(2),
            Kind::Put,
            b"row",
            b"q",
            Some(seqno),
            b"v",
        )
        .unwrap();
        let record = WalRecord::Batch {
            seqno,
            commit_ts: seqno,
            batch: b.batch(),
        };
        wal.append(&record, Durability::GroupSync).unwrap();
        wal.sync().unwrap();
    }

    fn drain(c: &mut FeedCursor) -> usize {
        let mut n = 0;
        loop {
            match c.step().unwrap() {
                Step::Record { .. } => n += 1,
                Step::End => return n,
                other => panic!("{other:?}"),
            }
        }
    }

    /// D310 PR 5b (the subscriber cost on the AX102): an idle stream's poll reads its
    /// headers once and one frame, not twice and a 1 MiB chunk; a stream with new records
    /// still reads them all, its headers again at its stop.
    #[test]
    fn an_idle_stop_reads_the_headers_once_and_one_frame() {
        let vfs: VfsRef = SimVfs::new(5);
        let db = Path::new("/db/data.phdb");
        let o = WalOptions {
            segment_size: 64 * FRAME,
            retain_segments: 4,
            feed_session: 0xfeed,
            ..WalOptions::default()
        };
        let mut wal = WalStream::create(&vfs, db, StreamId(0), DB_ID, o).unwrap();
        append(&mut wal, 1);
        let mut c = FeedCursor::open(&vfs, db, StreamId(0), DB_ID, Lsn::default()).unwrap();
        assert_eq!(drain(&mut c), 1);
        for _ in 0..10 {
            let before = c.reads;
            assert_eq!(drain(&mut c), 0);
            assert_eq!(c.reads.0 - before.0, 1, "one header read per idle stop");
            assert_eq!(c.reads.1 - before.1, FRAME, "one frame per idle stop");
        }
        // New records: delivered, with the headers read at the resume and again at the stop.
        for s in 2..=4 {
            append(&mut wal, s);
        }
        let before = c.reads;
        assert_eq!(drain(&mut c), 3);
        assert_eq!(
            c.reads.0 - before.0,
            2,
            "a stop after records reads the headers"
        );
    }
}

//! The in-memory mock stream.

use pigeonhole_format::wal::WalRecord;
use pigeonhole_format::wal::{FRAGMENT_HEADER_LEN, FRAME_SIZE};
use pigeonhole_format::{Durability, Lsn, StreamId};
use pigeonhole_io::Completion;

use crate::{CommitTicket, Error, Result, Wal};

/// In-memory mock of a stream for engine tests: keeps appended, written and synced records
/// separately so a test can drop what a crash would lose.
///
/// Positions are plain `Lsn`s in one simulated segment per "incarnation": a
/// [`MemWal::crash`] starts a new epoch, as a real stream would after recovery.
///
/// ```
/// use pigeonhole_format::wal::{StreamList, WalRecord};
/// use pigeonhole_format::{Durability, StreamId};
/// use pigeonhole_wal::{MemWal, Wal};
///
/// let mut wal = MemWal::new(StreamId(0));
/// let rec = |seqno| WalRecord::Commit { seqno, participants: StreamList::new(&[0, 0]).unwrap() };
/// wal.append(&rec(1), Durability::GroupSync).unwrap();
/// wal.sync().unwrap();
/// wal.append(&rec(2), Durability::Buffered).unwrap();
/// wal.write().unwrap();
/// wal.append(&rec(3), Durability::Buffered).unwrap();
///
/// wal.crash(false); // process crash: written records survive
/// assert_eq!(wal.records().len(), 2);
/// wal.crash(true); // power loss: only synced records survive
/// assert_eq!(wal.records().len(), 1);
/// ```
#[derive(Debug)]
pub struct MemWal {
    stream: StreamId,
    epoch: u32,
    /// Segment offset of the next record.
    pos: u64,
    /// Every record still held, with its end position.
    records: Vec<(Lsn, Vec<u8>)>,
    /// Records `..written` have been "handed to the kernel"; `..durable` are synced.
    written: usize,
    durable: usize,
    written_lsn: Lsn,
    durable_lsn: Lsn,
}

impl Default for MemWal {
    fn default() -> Self {
        Self::new(StreamId(0))
    }
}

impl MemWal {
    /// An empty in-memory stream.
    pub fn new(stream: StreamId) -> Self {
        let start = Lsn::new(1, FRAME_SIZE as u32);
        Self {
            stream,
            epoch: 1,
            pos: FRAME_SIZE as u64,
            records: Vec::new(),
            written: 0,
            durable: 0,
            written_lsn: start,
            durable_lsn: start,
        }
    }

    /// Simulates a crash: keeps written records for a process crash, synced records for
    /// power loss.
    pub fn crash(&mut self, power_loss: bool) {
        // Written records survive a process crash but stay unsynced: a later power loss
        // still drops them.
        let keep = if power_loss {
            self.durable
        } else {
            self.written
        };
        self.records.truncate(keep);
        self.written = keep;
        self.epoch += 1;
        self.pos = FRAME_SIZE as u64;
        self.written_lsn = Lsn::new(self.epoch, FRAME_SIZE as u32);
        if power_loss {
            self.durable_lsn = self.written_lsn;
        }
    }

    /// The surviving records, encoded, in order (feed them to the engine's replay).
    pub fn records(&self) -> Vec<(Lsn, Vec<u8>)> {
        self.records.clone()
    }
}

impl Wal for MemWal {
    fn stream(&self) -> StreamId {
        self.stream
    }

    fn append(&mut self, record: &WalRecord<'_>, durability: Durability) -> Result<CommitTicket> {
        if durability == Durability::None {
            return Err(Error::InvalidArgument {
                what: "Durability::None commits write no WAL record",
            });
        }
        let mut bytes = Vec::new();
        record.encode(&mut bytes);
        let len = (bytes.len() + FRAGMENT_HEADER_LEN) as u64;
        if self.pos + len > u64::from(u32::MAX) {
            self.epoch += 1;
            self.pos = FRAME_SIZE as u64;
        }
        self.pos += len;
        let end = Lsn::new(self.epoch, self.pos as u32);
        self.records.push((end, bytes));
        Ok(CommitTicket {
            stream: self.stream,
            end,
            durability,
        })
    }

    fn write(&mut self) -> Result<Lsn> {
        self.written = self.records.len();
        if let Some((end, _)) = self.records.last() {
            self.written_lsn = self.written_lsn.max(*end);
        }
        Ok(self.written_lsn)
    }

    fn sync(&mut self) -> Result<Lsn> {
        let lsn = self.write()?;
        self.durable = self.written;
        self.durable_lsn = lsn;
        Ok(lsn)
    }

    fn submit_sync(&mut self) -> Result<Completion<Lsn>> {
        Ok(Completion::ready(Ok(self.sync()?)))
    }

    fn written(&self) -> Lsn {
        self.written_lsn
    }

    fn durable(&self) -> Lsn {
        self.durable_lsn
    }

    fn satisfies(&self, ticket: &CommitTicket) -> bool {
        ticket.met_by(self.stream, self.written_lsn, self.durable_lsn)
    }

    fn checkpoint(&mut self, upto: Lsn) -> Result<()> {
        let drop = self
            .records
            .iter()
            .take_while(|(end, _)| *end <= upto)
            .count();
        self.records.drain(..drop);
        self.written = self.written.saturating_sub(drop);
        self.durable = self.durable.saturating_sub(drop);
        Ok(())
    }

    fn remove(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

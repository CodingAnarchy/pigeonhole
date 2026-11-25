//! The change feed's reader in the writer process (D310 PR 3, #1248; rev 3 §2.4–§2.6).
//!
//! A [`FeedReader`] merges every WAL stream's records into whole commits in seqno order, up
//! to the feed's frontier ([`Engine::feed_frontier`](crate::Engine::feed_frontier)). It
//! reads the stream files (through [`FeedCursor`]), never the writer's memory: by the M1
//! invariant (concurrency.md, the written-hold protocol) every record at or below the
//! frontier is in its file, so a poll that loads the frontier and then reads every stream to
//! its end has every commit up to it.
//!
//! - **Keys.** A single is ordered by its seqno, a cross-shard commit by its Decision's apply
//!   seqno; its PREPAREs are held by id until the Decision is read (FORMAT §10.3, D263).
//! - **An aborted round's PREPARE** is dropped once its own stream's checkpoint passed it
//!   with no Decision read (simp724's rule): a checkpoint passes a PREPARE only once it is
//!   aborted, or decided and settled, and then its Decision was in its file before the
//!   checkpoint moved. The checkpoints are loaded before the frontier. The stream's floor
//!   is the backstop.
//! - **The mark rule** (§2.2): a record counts only in a segment whose session fed every
//!   table the feed follows, within sessions that chain with no other root commit between
//!   them (the sidecar, FORMAT §10.4).
//! - **Tokens** fall between seqnos (§2.5): the last delivered seqno, and per stream where to
//!   read again and how far the delivered records reached.
//! - **Expiry** ([`Expiry`]) is sticky.
//! - **The read-ahead cap** (D320): a pass keeps only records with keys at or below a
//!   cutoff, which falls once what's kept passes the budget, and delivers up to the smaller
//!   of the frontier and the cutoff; then it reads again from the token's hints. A stream
//!   isn't in key order (a Decision can follow Singles with larger seqnos in its group), so
//!   every pass still reads every stream to its end.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pigeonhole_compaction::blob_pointer;
use pigeonhole_format::feed::{FeedEntry, FeedHeader, FeedTables, decode_entries_at};
use pigeonhole_format::wal::{BatchRef, WalRecord};
use pigeonhole_format::{FamilyId, Kind, Lsn, Seqno, StreamId, TableId, Timestamp};
use pigeonhole_io::OpenOptions;
use pigeonhole_wal::{FeedCursor, Step};

use crate::shard::{Shared, Wake};
use crate::{Error, Result};
use pigeonhole_shm::ShmRegion;

mod catalog;
mod history;
mod read_ahead;
mod token;

pub use catalog::CatalogEvent;
pub(crate) use catalog::catalog_digest_of;
pub use token::all_tables_token_position;
#[allow(clippy::wildcard_imports)]
use {catalog::*, history::*, read_ahead::*};

/// Where a feed starts.
#[derive(Debug, Clone)]
pub enum FeedStart {
    /// With the commits after the frontier at subscription.
    Now,
    /// With the oldest commit retained history still holds for the feed's tables. Under load
    /// that history may be recycled before the reader reaches it, which expires the feed at
    /// once; [`FeedStart::After`] from a snapshot is the way to bootstrap.
    Oldest,
    /// With the commits after `seqno` (a snapshot's seqno). A feed of every table delivers
    /// the catalog changes with a ceiling above it ([`FeedStart::Snapshot`] knows which the
    /// snapshot saw).
    After(Seqno),
    /// With the commits after `snapshot`'s seqno, and in a feed of every table the catalog
    /// changes its catalog doesn't reflect yet (D315): read the snapshot, then follow the
    /// feed, and no commit or catalog change is missed or seen twice.
    Snapshot(crate::Snapshot),
    /// Where a token left off ([`FeedReader::token`]).
    Token(Vec<u8>),
}

/// What a feed follows.
#[derive(Debug, Clone)]
pub struct FeedSpec {
    /// The tables, each fed by the session. Empty: every table (D315), including tables
    /// created after the feed opened. An empty list is accepted only when the session feeds
    /// every table (the change feed on without a table list); every session of its history
    /// must have too, and it resumes only from a token of a feed of every table.
    pub tables: Vec<TableId>,
    /// Only mutations of rows starting with these bytes (empty: every row).
    pub prefix: Vec<u8>,
    /// The durable frontier (the default, §2.4 S3) rather than the visible one.
    pub durable: bool,
    /// Where to start.
    pub start: FeedStart,
}

/// One mutation of a delivered commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// Its table.
    pub table: TableId,
    /// Its family.
    pub family: FamilyId,
    /// Put, Merge, or one of the delete kinds.
    pub kind: Kind,
    /// Its timestamp: explicit, or the commit's.
    pub ts: Timestamp,
    /// The row.
    pub row: Vec<u8>,
    /// The qualifier (empty for a family delete).
    pub qualifier: Vec<u8>,
    /// The stored value (FORMAT §3), with a separated value resolved; empty for deletes.
    pub value: Vec<u8>,
}

/// One whole commit (§2.5): every mutation of it the feed follows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Commit {
    /// The seqno it applied at.
    pub seqno: Seqno,
    /// Its commit timestamp.
    pub ts: Timestamp,
    /// Its mutations in the feed's tables and prefix, in record order (shares in their
    /// participants' order).
    pub changes: Vec<Change>,
}

/// Why a feed ended (§2.6): the feed can't deliver what follows its position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expiry {
    /// What happened, for people.
    pub reason: String,
    /// The oldest seqno history still holds, when known.
    pub oldest_retained: Option<Seqno>,
}

/// What [`FeedReader::try_next`] found.
#[derive(Debug)]
pub enum Next<'a> {
    /// The next whole commit.
    Commit(&'a Commit),
    /// Nothing past the frontier yet.
    Pending,
    /// The feed has expired (and stays so).
    Expired(&'a Expiry),
    /// A catalog change (D315), in a feed of every table only: between every commit below
    /// its ceiling and every commit at or above it.
    Catalog(&'a CatalogEvent),
}

/// A range drop as a feed that includes feed-silent records delivers it (D331): as an empty
/// [`Next::Commit`] at the drop's seqno, which [`FeedReader::range_drop`] names: rows
/// `[start, end)` of `table` (`end` `None`: to the table's end) dropped at `seqno`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RangeDropEvent {
    /// The drop's seqno, which applies nothing.
    pub seqno: Seqno,
    /// The table.
    pub table: TableId,
    /// The range's first row.
    pub start: Vec<u8>,
    /// The row after the range; `None`: to the table's end.
    pub end: Option<Vec<u8>>,
}

/// How far a feed has covered its history ([`FeedReader::position`], ICR 0114 amendment):
/// a seqno for its commits, and in a feed of every table a sequence for its catalog changes.
/// Each part only grows, and they are independent: one position covers another iff both
/// parts are at least as large ([`FeedPosition::covers`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct FeedPosition {
    /// Every commit of the spec at or below it is covered.
    pub seqno: Seqno,
    /// A feed of every table: every catalog change with a root sequence at or below it was
    /// delivered or passed over (0 for none, and in a feed of some tables).
    pub catalog: u64,
}

impl FeedPosition {
    /// Whether this position covers everything `other` does: both parts at least as large.
    pub fn covers(&self, other: &FeedPosition) -> bool {
        self.seqno >= other.seqno && self.catalog >= other.catalog
    }

    /// The position covering what both do, part by part.
    pub fn join(self, other: FeedPosition) -> FeedPosition {
        FeedPosition {
            seqno: self.seqno.max(other.seqno),
            catalog: self.catalog.max(other.catalog),
        }
    }
}

/// A mutation as read, before its commit's timestamp is known: its fixed fields, and its
/// bytes' lengths in its record's buffer ([`Raws`]).
#[derive(Debug, Clone, Copy)]
struct RawHead {
    table: TableId,
    family: FamilyId,
    kind: Kind,
    ts: Option<Timestamp>,
    row: u32,
    qualifier: u32,
    value: u32,
}

/// A record's followed mutations, packed into two allocations sized exactly: their fixed
/// fields, and their rows, qualifiers and values back to back (#1637). A reader keeps up to
/// its read-ahead budget of these, so one allocation per mutation field (and a spine grown
/// to four) made a small commit cost about three times what the cap counted.
#[derive(Debug, Default)]
struct Raws {
    heads: Box<[RawHead]>,
    bytes: Box<[u8]>,
}

/// A mutation of a [`Raws`].
struct RawRef<'a> {
    table: TableId,
    family: FamilyId,
    kind: Kind,
    ts: Option<Timestamp>,
    row: &'a [u8],
    qualifier: &'a [u8],
    value: &'a [u8],
}

impl Raws {
    fn iter(&self) -> impl Iterator<Item = RawRef<'_>> {
        let mut at = 0usize;
        self.heads.iter().map(move |h| {
            let (r, q, v) = (h.row as usize, h.qualifier as usize, h.value as usize);
            let row = &self.bytes[at..at + r];
            let qualifier = &self.bytes[at + r..at + r + q];
            let value = &self.bytes[at + r + q..at + r + q + v];
            at += r + q + v;
            RawRef {
                table: h.table,
                family: h.family,
                kind: h.kind,
                ts: h.ts,
                row,
                qualifier,
                value,
            }
        })
    }

    /// The heap memory it holds: its two allocations, each rounded to the allocator's
    /// smallest size class.
    fn memory(&self) -> u64 {
        alloc_size(std::mem::size_of_val(&*self.heads)) + alloc_size(self.bytes.len())
    }
}

/// An allocation of `n` bytes, rounded up to a 16-byte size class (none for 0).
fn alloc_size(n: usize) -> u64 {
    (n as u64).next_multiple_of(16)
}

/// Packs `batch`'s mutations of `tables` (every table but a replica's state table, if
/// empty) and rows starting with `prefix`. Two passes over the batch: the first sizes the
/// result's two allocations, the second fills them, so each is made once at its exact size
/// and nothing outlives the call (a scratch buffer would keep the largest batch's size,
/// outside the read-ahead's count: simp724's S1 on #1667).
fn pack(batch: BatchRef<'_>, tables: &[TableId], prefix: &[u8]) -> Result<Raws> {
    let len = |b: &[u8]| {
        u32::try_from(b.len())
            .map_err(|_| Error::Corruption("a logged mutation's field is over 4 GiB".into()))
    };
    // The table first, then the row: a mutation outside is skipped by its lengths, never
    // copied (§2.7). An empty list is every table (D315), but a replica's state table: a
    // commit only to it is passed over (D315 PR 3).
    let followed = |m: &pigeonhole_format::wal::Mutation<'_>| {
        let table = if tables.is_empty() {
            m.table != pigeonhole_format::manifest::REPLICA_TABLE
        } else {
            tables.binary_search(&m.table).is_ok()
        };
        table && m.row.starts_with(prefix)
    };
    let (mut n, mut size) = (0usize, 0usize);
    for m in batch.iter() {
        let m = m?;
        if followed(&m) {
            n += 1;
            size += m.row.len() + m.qualifier.len() + m.value.len();
        }
    }
    let mut heads = Vec::with_capacity(n);
    let mut bytes = Vec::with_capacity(size);
    for m in batch.iter() {
        let m = m?;
        if !followed(&m) {
            continue;
        }
        heads.push(RawHead {
            table: m.table,
            family: m.family,
            kind: m.kind,
            ts: m.ts,
            row: len(m.row)?,
            qualifier: len(m.qualifier)?,
            value: len(m.value)?,
        });
        bytes.extend_from_slice(m.row);
        bytes.extend_from_slice(m.qualifier);
        bytes.extend_from_slice(m.value);
    }
    // Full to capacity, so neither conversion reallocates.
    debug_assert!(heads.len() == n && bytes.len() == size);
    Ok(Raws {
        heads: heads.into_boxed_slice(),
        bytes: bytes.into_boxed_slice(),
    })
}

/// A record as read, owned.
enum Item {
    Single {
        seqno: Seqno,
        ts: Timestamp,
        changes: Raws,
        /// Feed-silent, for a reader that includes them (D331): noted once it's kept.
        silent: bool,
    },
    Prepare {
        id: Seqno,
        changes: Raws,
        /// An external share (D329): see [`Held::external`].
        external: bool,
        silent: bool,
    },
    Decision {
        id: Seqno,
        apply: Seqno,
        ts: Timestamp,
        participants: Vec<StreamId>,
    },
    /// Past the read-ahead's cutoff (D320): passed over, read again after a rewind.
    Skipped,
    /// A Decision past the cutoff: its round is passed over, its shares too.
    SkippedRound {
        id: Seqno,
    },
    /// An external transaction's abort (D329, type 6): its held shares go, and nothing is
    /// delivered at its own seqno.
    Abort {
        id: Seqno,
    },
    /// A range drop (D331, type 7), at a seqno of its own, which applies nothing: the
    /// frontier passes it. A reader that includes feed-silent records (a replica's
    /// follower) is delivered it, in seqno order; its range is read only then.
    RangeDrop {
        seqno: Seqno,
        event: Option<Box<RangeDropEvent>>,
    },
    Other,
}

/// A record read but not yet delivered, by key.
#[derive(Debug)]
enum Pending {
    Single {
        ts: Timestamp,
        changes: Raws,
        stream: usize,
        start: Lsn,
        end: Lsn,
    },
    Decided {
        id: Seqno,
        ts: Timestamp,
        participants: Vec<StreamId>,
        stream: usize,
        start: Lsn,
        end: Lsn,
        /// The session that wrote the Decision.
        session: u64,
    },
}

impl Pending {
    /// The stream and start of its record.
    fn at(&self) -> (usize, Lsn) {
        match self {
            Pending::Single { stream, start, .. } | Pending::Decided { stream, start, .. } => {
                (*stream, *start)
            }
        }
    }
}

/// A PREPARE waiting for its Decision.
#[derive(Debug)]
struct Held {
    stream: usize,
    start: Lsn,
    end: Lsn,
    changes: Raws,
    /// Read in a segment outside the feed's history (the mark rule): delivering it expires.
    outside: bool,
    /// An external share (D329), which may be held as long as its caller takes to decide:
    /// outside the read-ahead budget and never what trimming evicts, so a long hold can't
    /// lower the cutoff and stall delivery (perf2's E3 on ICR 0140). Bounded instead by the
    /// writer's in-doubt limits.
    external: bool,
}

/// One stream's reading state.
#[derive(Debug)]
struct StreamRead {
    cursor: FeedCursor,
    /// Just past the last delivered (or skipped) record of this stream.
    delivered_end: Lsn,
    /// From a token: the delivered records reached here, so the stream must too.
    must_reach: Option<Lsn>,
    /// What a capped reader learned of the stream past its hint (#1827).
    index: SkipIndex,
}

/// #1827: a run of a stream's records in one segment, read while the reader was capped
/// (D320), so a later pass can jump over it when it would only pass every one over.
#[derive(Debug, Clone, Copy)]
struct Chunk {
    /// Its first record's start: where a jump leaves the stream's hint.
    start: Lsn,
    /// Just past its last record: where a jump moves the cursor.
    end: Lsn,
    /// The lowest key a record in it can matter at, by `read_one`'s tests for passing a
    /// record over: a Single's seqno, and a Prepare's or a Decision's round id + 1. A
    /// Decision is keyed by its round, not its apply seqno: passing it over drops its round's
    /// held shares, which matters only for a round whose id is below the cutoff, and a chunk
    /// keyed that way is never jumped while such a share may be held (rule C would otherwise
    /// drop it as aborted). A record of any other type is 0, never jumped.
    min_key: Seqno,
    /// Its segment's feed session.
    session: u64,
}

/// #1827: the most chunks a stream's skip index keeps (perf2's S1 on #1922). Past it, adjacent
/// chunks of a segment are merged pairwise, so a capped reader's index takes a constant
/// memory, under 64 B a chunk: about 256 KiB a stream, outside the read-ahead budget.
const SKIP_INDEX_CHUNKS: usize = 4096;

/// #1827: one stream's chunks, by the cursor's position just before each one's first
/// record. Built while the reader is capped, at the stream's indexed end only (a pass that
/// reads inside the index adds nothing); trimmed to the hint at each rewind and dropped once
/// a pass reads to the end without passing anything over.
///
/// At most `cap` chunks (perf2's S1 on #1922): once over it, adjacent chunks of one segment
/// are merged pairwise. A merged chunk's key is the lower of the two lowest keys, so a jump
/// over it is a jump over both, exactly; it still never crosses a segment, and its key is
/// still a cursor position. If no pair can merge (every chunk alone in its segment), the
/// index stops growing until a rewind trims it: the tail past it is stepped, as without one.
#[derive(Debug)]
struct SkipIndex {
    chunks: BTreeMap<Lsn, Chunk>,
    /// The chunk being built, its key, and the log bytes in it.
    open: Option<(Lsn, Chunk, u64)>,
    cap: usize,
    /// At the cap with nothing left to merge: nothing is added until a trim.
    full: bool,
    /// Merging passes run (an inspection count for tests).
    #[cfg(any(test, feature = "test-hooks"))]
    coarsenings: u64,
}

impl SkipIndex {
    fn new(cap: usize) -> Self {
        Self {
            chunks: BTreeMap::new(),
            open: None,
            cap: cap.max(2),
            full: false,
            #[cfg(any(test, feature = "test-hooks"))]
            coarsenings: 0,
        }
    }

    /// Notes a record read at the cursor's position `before`, `start..end`, keyed `key`, in
    /// a segment of `session`: it extends the open chunk when it's the next record after it
    /// in the same segment, and begins one past the index's end. One inside the index adds
    /// nothing, nor does any while the index is full.
    fn note(&mut self, before: Lsn, start: Lsn, end: Lsn, key: Seqno, session: u64, size: u64) {
        if self.full {
            return;
        }
        let len = u64::from(end.offset().saturating_sub(start.offset()));
        if let Some((_, c, bytes)) = &mut self.open
            && c.end == before
            && c.start.epoch() == start.epoch()
        {
            c.end = end;
            c.min_key = c.min_key.min(key);
            *bytes += len;
            if *bytes >= size {
                self.close();
            }
            return;
        }
        let indexed_end = match &self.open {
            Some((_, c, _)) => Some(c.end),
            None => self.chunks.last_key_value().map(|(_, c)| c.end),
        };
        if indexed_end.is_some_and(|e| before < e) {
            return;
        }
        self.close();
        if self.full {
            return;
        }
        let c = Chunk {
            start,
            end,
            min_key: key,
            session,
        };
        self.open = Some((before, c, len));
        if len >= size {
            self.close();
        }
    }

    /// Closes the open chunk, merging pairwise once over the cap.
    fn close(&mut self) {
        if let Some((key, c, _)) = self.open.take() {
            self.chunks.insert(key, c);
        }
        if self.chunks.len() > self.cap {
            self.coarsen();
            // Nothing merged: the newest goes, and the tail past the index is stepped.
            while self.chunks.len() > self.cap {
                self.chunks.pop_last();
                self.full = true;
            }
        }
    }

    /// Merges each chunk with the next where the next starts where it ends, in its segment.
    fn coarsen(&mut self) {
        #[cfg(any(test, feature = "test-hooks"))]
        {
            self.coarsenings += 1;
        }
        let mut old = std::mem::take(&mut self.chunks).into_iter().peekable();
        while let Some((key, mut c)) = old.next() {
            if let Some((next_key, next)) = old.peek()
                && *next_key == c.end
                && next.start.epoch() == c.start.epoch()
            {
                c.end = next.end;
                c.min_key = c.min_key.min(next.min_key);
                old.next();
            }
            self.chunks.insert(key, c);
        }
    }

    /// The closed chunk the cursor at `at` is just before: keyed there, or starting there (a
    /// cursor reopened at a hint is at the chunk's first record, past a key at the previous
    /// segment's end: perf2's N1 on #1922).
    fn at(&self, at: Lsn) -> Option<Chunk> {
        let (&key, c) = self.chunks.range(..=at).next_back()?;
        (key == at || c.start == at).then_some(*c)
    }

    /// Drops the chunks with a record before `hint`: everything there was delivered. A chunk
    /// keyed before the hint but starting at or after it stays.
    fn trim(&mut self, hint: Lsn) {
        let mut kept = self.chunks.split_off(&hint);
        if let Some((&k, &c)) = self.chunks.last_key_value()
            && c.start >= hint
        {
            kept.insert(k, c);
        }
        self.chunks = kept;
        if self.open.as_ref().is_some_and(|(_, c, _)| c.start < hint) {
            self.open = None;
        }
        self.full = false;
    }

    fn clear(&mut self) {
        self.chunks.clear();
        self.open = None;
        self.full = false;
    }

    /// Its chunks, the open one too (an inspection count for tests).
    #[cfg(feature = "test-hooks")]
    fn len(&self) -> usize {
        self.chunks.len() + usize::from(self.open.is_some())
    }
}

/// Each stream's starts of the records read but not delivered (`ready` and `held`), as a
/// count per start, so [`FeedReader::token`]'s hint per stream is its first key rather than
/// a scan of every pending record (cli692 on ticket 5: a hub behind by 10K commits spent
/// milliseconds per token). Kept in step wherever `ready` or `held` gains or loses one.
#[derive(Debug, Default)]
struct Starts(Vec<BTreeMap<Lsn, u32>>);

impl Starts {
    fn add(&mut self, stream: usize, start: Lsn) {
        if self.0.len() <= stream {
            self.0.resize_with(stream + 1, BTreeMap::new);
        }
        *self.0[stream].entry(start).or_insert(0) += 1;
    }

    fn remove(&mut self, stream: usize, start: Lsn) {
        let starts = &mut self.0[stream];
        let n = starts.get_mut(&start).expect("a pending start is counted");
        *n -= 1;
        if *n == 0 {
            starts.remove(&start);
        }
    }

    fn first(&self, stream: usize) -> Option<Lsn> {
        self.0.get(stream)?.first_key_value().map(|(&l, _)| l)
    }
}

/// Distinguishes feed readers' entries in the watermark registries from commits' and
/// shards' (`watermark_waiters`): the top two bits set.
static NEXT_KEY: AtomicU64 = AtomicU64::new(0);

/// A thread-unparking waker for a blocking wait.
struct Unpark(std::thread::Thread);

impl std::task::Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// A subscription to the committed changes of some tables (D310): see the module docs.
pub struct FeedReader {
    shared: Arc<Shared>,
    /// The shared-memory region the frontier and the streams' marks are read from: the
    /// writer's own, or a reader process's attachment (D310 PR 4). Stale once the writer
    /// that made it is gone: the feed then fails with `SnapshotExpired` and the caller
    /// subscribes again from its token.
    shm: ShmRegion,
    /// The writer's feed, in the writer process (its checkpoint history serves start
    /// positions and its intervals bound waits); `None` in a reader process.
    writer: Option<Arc<crate::feed::Feed>>,
    /// The view blob values are resolved through: the writer's current one, or a reader
    /// process's view of its attachment.
    view: ViewFn,
    /// A reader process's wait between polls, doubling while nothing arrives.
    backoff: Duration,
    /// Since when a reader process has waited for a view listing the next commit's blob
    /// files.
    blob_wait: Option<Instant>,
    tables: Vec<TableId>,
    prefix: Vec<u8>,
    durable: bool,
    streams: Vec<StreamRead>,
    history: History,
    /// Every commit with a key at or below this was delivered, skipped or precedes the
    /// start.
    delivered: Seqno,
    /// Where [`position`](Self::position)'s seqno is published, for a metrics exporter on
    /// another thread (`set_progress`, D316 A4, ICR 0116): a `fetch_max` at the end of every
    /// poll, on the feed's own thread. `None` until one's set.
    progress: Option<Arc<AtomicU64>>,
    /// The frontier as of the last poll.
    frontier: Seqno,
    /// The frontier the streams were last read to their ends under (#1448): while the
    /// frontier stays there, no commit happened since, and every record at or below it was
    /// read then, so a poll reads nothing.
    read_through: Option<Seqno>,
    /// Polls that read the streams (an inspection count for tests).
    stream_reads: u64,
    /// Records the stream cursors stepped (an inspection count for tests, #1827).
    #[cfg(feature = "test-hooks")]
    record_steps: u64,
    /// #1827: whether the streams' skip indexes are built. Set once a pass is capped (D320)
    /// and cleared, with the indexes, once a pass reads to the end passing nothing over: a
    /// reader that keeps up never builds one.
    indexing: bool,
    /// Chunks jumped over (an inspection count for tests, #1827).
    chunk_skips: u64,
    /// Polls of any kind (an inspection count for tests, #1417).
    polls: u64,
    /// [`wait`](Self::wait)'s re-polls without parking: its waker's registration found the
    /// wake already due (an inspection count for tests).
    unparked_repolls: u64,
    /// [`wait`](Self::wait)'s parks, each returned by a wake, the backstop or spuriously (an
    /// inspection count for tests).
    parks: u64,
    /// The newest session of any record read (the token's anchor).
    session: u64,
    ready: BTreeMap<Seqno, Pending>,
    /// The PREPAREs read and waiting for their Decision, by round id and stream: one entry
    /// per share, so a share costs its entry and no spine (#1637).
    held: BTreeMap<(Seqno, usize), Held>,
    /// The starts of `ready`'s and `held`'s records, per stream.
    starts: Starts,
    /// The read-ahead cap (D320).
    ahead: ReadAhead,
    /// Rounds whose shares rule C dropped (no Decision read, the checkpoint passed): id →
    /// (stream, epoch) of a dropped share. A Decision for one later expires the feed rather
    /// than passing over its commit as recovery's all-or-nothing (simp724's H1 on #1316).
    /// An entry goes once the floor passes its segment.
    dropped: HashMap<Seqno, (usize, u32)>,
    out: Commit,
    /// Feed-silent commits are delivered (D331: a replica's follower only), and whether the
    /// last commit delivered was one.
    include_silent: bool,
    out_silent: bool,
    /// The xids of the spanning commits read and not yet delivered: a single commit's by its
    /// seqno, a round's by its id. Beside the records, not in them, as `silent_seen`: a
    /// commit without one pays a bit test when read and an emptiness test when delivered.
    xids: HashMap<Seqno, Arc<[u8]>>,
    /// The xid of the record just read, if it's a spanning commit's share: set as the record
    /// is read and taken by the item that keeps it, not carried in the item, which every
    /// record read moves (perf2's N2 on #2076).
    read_xid: Option<Arc<[u8]>>,
    /// The xid of the commit last delivered ([`xid`](Self::xid)): beside the commit, not in
    /// it, as `out_silent` is, so `Commit` keeps its fields (semver: an exhaustively
    /// constructible public struct can't gain one).
    out_xid: Option<Arc<[u8]>>,
    /// With `include_silent`, the silent commits read, kept and not yet delivered: a single
    /// commit's seqno, a round's id. Kept beside the records, not in them, so a reader that
    /// doesn't include them carries nothing for it (#2055's feed shapes). Noted only where a
    /// commit is kept (ready or held) and removed with it, so it's bounded by what's kept.
    silent_seen: HashSet<Seqno>,
    /// A feed of every table (D315): the catalog changes read from the sidecar and not yet
    /// delivered, in order; the last one delivered (its root sequence, in the token); how far
    /// the sidecar is read; and the tables dropped by delivered changes, whose later changes
    /// are passed over (a commit routed before a drop can be logged after it). Each change
    /// is queued with the writer session that made it: delivering it moves the token's
    /// session as a record of that session does (audit120's P2-C5). Beside the event, not in
    /// it: `CatalogEvent` is what a replica receives and encodes, and has no session.
    catalog: VecDeque<(CatalogEvent, u64)>,
    catalog_seq: u64,
    /// A `Bound` start's bound (`Oldest`, `After`; 0 for `Now` and `Snapshot`, and a token's
    /// own): every catalog change with a ceiling at or below it is before the start, in every
    /// load, not only in the entries already in the file at open. The hold doesn't bound
    /// visible seqnos, so a start past a change's ceiling can come before its entry is
    /// appended (simp724's F1 on #1515). A token carries it, so a resume passes over what its
    /// start did (F1b). Not the token's delivered seqno: a `Now` start can be past a change's
    /// ceiling that its view doesn't reflect, and must still deliver that change.
    catalog_bound: Seqno,
    sidecar_pos: u64,
    /// The writer's `Feed::catalog_roots` as of the last sidecar load: a change means a
    /// catalog change's entry is rooted that this feed hasn't loaded, though no seqno moved.
    catalog_roots_seen: u64,
    /// The sidecar, once opened ([`FeedReader::load_catalog`]).
    sidecar_file: Option<pigeonhole_io::FileRef>,
    dropped_tables: HashSet<TableId>,
    /// Every table a catalog change in the sidecar drops, read so far: rooted or not yet,
    /// delivered, passed over or queued (#1853). A feed of every table delivering a change to
    /// a table the catalog no longer has, and none of these drops, expires.
    recorded_drops: HashSet<TableId>,
    /// Every table a catalog change in the sidecar creates, read so far: a reader process's
    /// view can lag the writer's manifest, and miss a table this feed already has a commit to.
    recorded_creates: HashSet<TableId>,
    /// A feed of every table: the catalog at its position (D325), which its tokens carry.
    /// `None` until the open works it out, and for a feed of some tables.
    model: Option<CatalogModel>,
    /// What a version 3 token says the catalog was at its position, checked at open.
    claim: Option<CatalogModel>,
    /// A token's last catalog change (its `catalog_seq`), for the check at open.
    claim_known: Option<u64>,
    /// At open, until the first sidecar load: the catalog the start sees.
    deriving: Option<Arc<crate::catalog::Catalog>>,
    /// A feed of every table: the tables seen present in `present_of`, the catalog of the
    /// view they were checked against (#1853's guard, without a lookup per commit).
    present: pigeonhole_format::hash::FastSet<TableId>,
    present_of: Option<Arc<crate::catalog::Catalog>>,
    /// The table the last commit's changes ended with, present in `present_of`.
    present_last: Option<TableId>,
    out_catalog: CatalogEvent,
    /// The range drop the commit last delivered stands for (D331), at `drop_seqno`.
    out_drop: Option<RangeDropEvent>,
    drop_seqno: Seqno,
    /// Range drops queued in `ready` as empty commits, by seqno (D331; only with
    /// `include_silent`).
    drops: BTreeMap<Seqno, RangeDropEvent>,
    expired: Option<Expiry>,
    /// An error that ended the reader once a commit was already taken (a blob value that
    /// couldn't be read): every later call fails, since going on would skip that commit.
    failed: Option<String>,
    key: u64,
    /// This open's session.
    current: u64,
}

impl Drop for FeedReader {
    fn drop(&mut self) {
        // An async reader may have left a waker for the feed's holds.
        if let Some(feed) = &self.writer {
            feed.forget_hold_waiter(self.key);
        }
    }
}

impl std::fmt::Debug for FeedReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeedReader")
            .field("tables", &self.tables)
            .field("durable", &self.durable)
            .field("delivered", &self.delivered)
            .field("frontier", &self.frontier)
            .field("expired", &self.expired)
            .finish_non_exhaustive()
    }
}

/// The current view, for resolving blob values: the writer's, or a reader process's view of
/// its attachment.
pub(crate) type ViewFn = Box<dyn Fn() -> Result<Arc<crate::snapshot::View>> + Send + Sync>;

/// Where a feed reads from (D310 PR 4): the shared-memory region of the writer it follows,
/// how to get the current view, and the writer's feed when this is the writer process.
pub(crate) struct Origin {
    pub(crate) shm: ShmRegion,
    pub(crate) view: ViewFn,
    pub(crate) writer: Option<Arc<crate::feed::Feed>>,
}

/// A reader process's first and longest waits between polls: it watches the writer's
/// watermarks in shared memory, so the writer does no per-commit work for it.
const READER_BACKOFF_MIN: Duration = Duration::from_micros(50);
const READER_BACKOFF_MAX: Duration = Duration::from_millis(5);
/// How long a reader process waits for a view that lists a commit's blob files before it
/// takes them as removed (the bound a reader snapshot waits for its view too).
const READER_BLOB_WAIT: Duration = Duration::from_secs(1);

/// A record [`FeedReader::read_one`] read: its start and end, its segment's feed session,
/// the lowest key it can matter at (`Chunk::min_key`, #1827), and what it is.
type ReadRecord = (Lsn, Lsn, u64, Seqno, Item);

/// A delivered commit's packed changes: a single's record, or a round's shares.
enum Packs {
    One(Raws),
    Many(Vec<Raws>),
}

/// What [`FeedReader::take_round`] found of a decided round.
enum Round {
    /// Every share's changes, in stream order.
    Whole(Vec<Raws>),
    /// Dropped whole by an earlier session's recovery: never happened.
    Dropped,
    /// A share is missing or outside the history: the feed expires.
    Gap,
}

/// What `emit` delivered.
enum Emitted {
    Nothing,
    Commit,
    Catalog,
}

impl Emitted {
    fn polled(self) -> Option<Polled> {
        match self {
            Emitted::Nothing => None,
            Emitted::Commit => Some(Polled::Commit),
            Emitted::Catalog => Some(Polled::Catalog),
        }
    }
}

/// What one poll left.
enum Polled {
    Commit,
    Catalog,
    Pending { held: bool },
    Expired,
}

fn expiry(reason: impl Into<String>) -> Expiry {
    Expiry {
        reason: reason.into(),
        oldest_retained: None,
    }
}

impl FeedReader {
    pub(crate) fn open(shared: Arc<Shared>, spec: FeedSpec, origin: Origin) -> Result<FeedReader> {
        // The session and fed tables of the writer followed: the writer's own feed, or in a
        // reader process the writer's session entry, the sidecar's last (it is written before
        // the writer's shared memory exists, so the attachment's writer wrote it).
        let (session, fed) = match &origin.writer {
            Some(feed) => (feed.session, feed.tables.clone()),
            None => writer_session(&shared)?,
        };
        let mut tables = spec.tables;
        tables.sort_unstable();
        tables.dedup();
        // A replica's state table (D315 PR 3; simp724's S2) is no feed's, at every layer.
        if tables.contains(&pigeonhole_format::manifest::REPLICA_TABLE) {
            return Err(Error::InvalidArgument(
                "a replica's state table can't be fed".into(),
            ));
        }
        // Every table (D315): the session must feed every table, not a list of them.
        if tables.is_empty() && !matches!(fed, FeedTables::All) {
            return Err(Error::InvalidArgument(
                "a feed of every table needs the change feed on for every table: open \
                 without a feed table list"
                    .into(),
            ));
        }
        if let Some(t) = tables.iter().find(|&&t| !fed.contains(t)) {
            return Err(Error::InvalidArgument(format!(
                "table {} is not fed: name it in the feed's tables at open",
                t.0
            )));
        }
        let history = read_history(&shared, session, &tables)?;
        let key = (3 << 62) | NEXT_KEY.fetch_add(1, Ordering::Relaxed);
        let db_id = shared.pager.db_id();
        let mut this = FeedReader {
            tables,
            prefix: spec.prefix,
            durable: spec.durable,
            streams: Vec::with_capacity(shared.shards),
            history,
            delivered: 0,
            progress: None,
            frontier: 0,
            read_through: None,
            stream_reads: 0,
            #[cfg(feature = "test-hooks")]
            record_steps: 0,
            indexing: false,
            chunk_skips: 0,
            polls: 0,
            unparked_repolls: 0,
            parks: 0,
            session: 0,
            ready: BTreeMap::new(),
            held: BTreeMap::new(),
            starts: Starts::default(),
            ahead: ReadAhead::new(shared.shards, READ_AHEAD_BYTES),
            dropped: HashMap::new(),
            out: Commit::default(),
            include_silent: false,
            out_silent: false,
            xids: HashMap::new(),
            read_xid: None,
            out_xid: None,
            silent_seen: HashSet::new(),
            expired: None,
            failed: None,
            key,
            current: session,
            shared,
            shm: origin.shm,
            writer: origin.writer,
            view: origin.view,
            backoff: READER_BACKOFF_MIN,
            blob_wait: None,
            catalog: VecDeque::new(),
            catalog_seq: 0,
            catalog_bound: 0,
            sidecar_pos: 0,
            catalog_roots_seen: 0,
            sidecar_file: None,
            dropped_tables: HashSet::new(),
            recorded_drops: HashSet::new(),
            recorded_creates: HashSet::new(),
            model: None,
            claim: None,
            claim_known: None,
            deriving: None,
            present: Default::default(),
            present_of: None,
            present_last: None,
            out_catalog: CatalogEvent::default(),
            out_drop: None,
            drop_seqno: 0,
            drops: BTreeMap::new(),
        };
        let token_start = matches!(spec.start, FeedStart::Token(_));
        // A feed of every table: the catalog its start reflects, for `Now` and a snapshot
        // (D315). `Now`'s is the view loaded before the sidecar is read, so every change it
        // reflects already has its entry there (the entry is appended before the change is
        // published).
        let start_catalog = match &spec.start {
            _ if !this.tables.is_empty() => None,
            FeedStart::Now => Some(Arc::clone(&(this.view)()?.catalog)),
            FeedStart::Snapshot(snap) => Some(Arc::clone(&snap.view.catalog)),
            _ => None,
        };
        // `Oldest` from an earlier session still kept whole (#1465), when there is one.
        let earlier = match spec.start {
            FeedStart::Oldest => this.earlier_start(db_id),
            _ => None,
        };
        // Where each stream starts, and the bound below which nothing is delivered.
        let starts: Vec<(Lsn, Option<Lsn>)> = match (spec.start, earlier) {
            (_, Some((bound, at, session))) => {
                this.delivered = bound;
                // The earlier session it starts in, as a non-token start names its own
                // (P2-C6): with 0 a token taken before any record skipped the history check.
                // Records of later sessions move it on (`raise_session`).
                this.session = session;
                at.into_iter().map(|a| (a, None)).collect()
            }
            (FeedStart::Token(bytes), None) => match this.decode_token(&bytes, db_id)? {
                Ok(starts) => starts,
                Err(e) => {
                    this.expired = Some(e);
                    return Ok(this);
                }
            },
            (start, None) => {
                let mut reader_starts: Vec<Lsn> = Vec::new();
                // A reader process's view of each stream's floor and lost bound, loaded once
                // (the floor first, so the bound is at least that floor's).
                let marks: Vec<pigeonhole_shm::FeedMarksView> = (0..this.shared.shards)
                    .map(|s| this.shm.feed_marks(s as u32))
                    .collect();
                let bound = match start {
                    FeedStart::Now => {
                        // A reader process starts each stream at its checkpoint, loaded
                        // first: every record before it was applied and visible when the
                        // checkpoint moved, so its key is at or below the bound loaded next.
                        if this.writer.is_none() {
                            reader_starts = (0..this.shared.shards)
                                .map(|s| Lsn(this.shm.feed_marks(s as u32).checkpoint))
                                .collect();
                        }
                        this.shm
                            .visible_seqno()
                            .max(this.shm.written_seqno(this.durable, this.tables.is_empty()))
                            .max(this.writer.as_ref().map_or(0, |f| f.checkpoint_key()))
                    }
                    FeedStart::After(seqno) => seqno,
                    FeedStart::Snapshot(ref snap) => snap.seqno,
                    FeedStart::Oldest => {
                        // The largest bound every stream's retained history serves: from the
                        // writer's checkpoint history, or in a reader process from the
                        // bounds it mirrors (`FEED_BOUND`).
                        match &this.writer {
                            Some(feed) => (0..this.shared.shards)
                                .map(|s| feed.oldest_bound(s, this.floor(s)))
                                .max()
                                .unwrap_or(0),
                            None => marks.iter().map(|m| m.bound).max().unwrap_or(0),
                        }
                    }
                    FeedStart::Token(_) => unreachable!("handled above"),
                };
                this.delivered = bound;
                // The bound and the starts are this session's state, so a token taken
                // before any record is read names it: with 0 a resume skipped the history
                // check (audit120's P2-C6).
                this.session = this.current;
                let mut starts = Vec::with_capacity(this.shared.shards);
                for (s, m) in marks.iter().enumerate() {
                    // A reader process has no checkpoint history: from its checkpoint for
                    // `Now`, else from the stream's floor, skipping keys at or below the
                    // bound (correct, and slower to start).
                    let at = match &this.writer {
                        Some(feed) => feed.start_at(s, bound, this.floor(s)),
                        // A record dropped below the floor may have a key above the bound:
                        // that history is gone (simp724's B1), as `start_at` says for the
                        // writer.
                        None if reader_starts.is_empty() && m.bound > bound => None,
                        None => Some(
                            reader_starts
                                .get(s)
                                .copied()
                                .unwrap_or(Lsn::new(m.floor, 0)),
                        ),
                    };
                    match at {
                        Some(at) => starts.push((at, None)),
                        None => {
                            this.expired = Some(expiry(format!(
                                "history past seqno {bound} is no longer retained"
                            )));
                            return Ok(this);
                        }
                    }
                }
                starts
            }
        };
        for (s, (at, must_reach)) in starts.into_iter().enumerate() {
            let cursor = this.open_cursor(s, at)?;
            this.streams.push(StreamRead {
                cursor,
                delivered_end: at,
                must_reach,
                index: SkipIndex::new(SKIP_INDEX_CHUNKS),
            });
        }
        // A feed of every table: the catalog changes before its start are passed over, their
        // drops kept (D315). A token says the last one delivered; another start, its bound.
        if this.tables.is_empty() && this.expired.is_none() {
            // The catalog the start sees, before the sidecar is read (D325).
            this.deriving = Some(match &start_catalog {
                Some(catalog) => Arc::clone(catalog),
                None => Arc::clone(&(this.view)()?.catalog),
            });
            let start = match &start_catalog {
                Some(catalog) => Some(&**catalog),
                None if token_start => None,
                None => {
                    this.catalog_bound = this.delivered;
                    None
                }
            };
            this.load_catalog(start)?;
        }
        Ok(this)
    }

    /// A cursor on stream `s` from `at`.
    fn open_cursor(&self, s: usize, at: Lsn) -> Result<FeedCursor> {
        let mut cursor = FeedCursor::open(
            &self.shared.vfs,
            &self.shared.path,
            StreamId(s as u32),
            self.shared.pager.db_id(),
            at,
        )?;
        // In the writer's process a stop in the head segment reads no segment headers
        // (D310 PR 5b's subscriber cost; a reader process has no marks to follow).
        if let Some(marks) = self.writer.as_ref().and_then(|f| f.marks(s)) {
            cursor.follow_head(Arc::clone(marks));
        }
        // The mark is decoded here and checked against `incompat` bit 3 by `read_one`'s filter.
        cursor.accept_silent(true);
        Ok(cursor)
    }

    /// The next whole commit, without waiting.
    pub fn try_next(&mut self) -> Result<Next<'_>> {
        let polled = self.poll()?;
        Ok(self.lend(polled))
    }

    /// The next whole commit, waiting up to `timeout` for one. Waits on the watermark the
    /// feed follows (never spins): a commit wakes it once its seqno passes; where the feed's
    /// own holds, or a catalog change's, keep the frontier back, it waits on the feed's hold
    /// list, which their release wakes (#1417), bounded by the hold's interval. A reader
    /// process, with no hold list, backs off from 50 µs to 5 ms.
    pub fn wait(&mut self, timeout: Duration) -> Result<Next<'_>> {
        let deadline = Instant::now() + timeout;
        // Whether this call left a waker on the feed's hold list: taken back on the way out,
        // so releases don't keep waking a waiter that isn't waiting (as `poll_next` does).
        let mut on_hold_list = false;
        loop {
            let polled = match self.poll() {
                Ok(p) => p,
                Err(e) => {
                    self.leave_hold_list(on_hold_list);
                    return Err(e);
                }
            };
            let Polled::Pending { held } = polled else {
                self.leave_hold_list(on_hold_list);
                self.backoff = READER_BACKOFF_MIN;
                return Ok(self.lend(polled));
            };
            let now = Instant::now();
            if now >= deadline {
                self.leave_hold_list(on_hold_list);
                return Ok(Next::Pending);
            }
            let mut park = deadline - now;
            match &self.writer {
                // A reader process watches shared memory: a bounded backoff, never a spin.
                None => {
                    park = park.min(self.backoff);
                    self.backoff = (self.backoff * 2).min(READER_BACKOFF_MAX);
                }
                Some(feed) => {
                    let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
                    if self.register_wake(feed, held, &waker, &mut on_hold_list) {
                        self.unparked_repolls += 1;
                        continue;
                    }
                }
            }
            if let Some(feed) = &self.writer {
                // A backstop, either way: a hold may pass the watermark later than the
                // publish that wakes this, and a wake may be lost to a racing drain. Bound
                // the sleep by the hold's interval.
                let hold = if self.durable {
                    feed.sync_interval_nanos
                } else {
                    feed.flush_interval_nanos
                };
                park = park.min(Duration::from_nanos(hold));
            }
            self.parks += 1;
            std::thread::park_timeout(park);
        }
    }

    /// Whether the writer's session stopped its sidecar appends (D315): the writer's own
    /// flag in its process, or the stop it published in shared memory in a reader process
    /// (D315 amendment, #1869).
    fn appends_stopped(&self) -> bool {
        match &self.writer {
            Some(feed) => feed.appends_stopped(),
            None => self.shm.feed_appends_stopped(),
        }
    }

    /// Whether a catalog change may have been rooted since this feed last loaded the sidecar:
    /// the writer's count moved, or (a reader process) unknown.
    fn catalog_moved(&self) -> bool {
        self.writer
            .as_ref()
            .is_none_or(|feed| feed.catalog_roots() != self.catalog_roots_seen)
    }

    /// Whether a feed of every table, just registered on the hold list, must poll again
    /// rather than park: a frozen catalog hold (simp724's W2 on #1533) or a catalog change
    /// rooted since its last load, each stored before `release_catalog`'s wake, re-checked
    /// after the registration's fence.
    fn catalog_wake_missed(&self, feed: &crate::feed::Feed) -> bool {
        self.tables.is_empty()
            && (feed.appends_stopped() || feed.catalog_roots() != self.catalog_roots_seen)
    }

    /// Takes this reader's waker off the feed's hold list, if `on` (it left one there).
    fn leave_hold_list(&self, on: bool) {
        if on && let Some(feed) = &self.writer {
            feed.forget_hold_waiter(self.key);
        }
    }

    /// The next whole commit, for an async caller (D310 PR 5a): `Poll::Ready` with what
    /// [`try_next`](Self::try_next) would return once there is a commit or an expiry,
    /// `Poll::Pending` otherwise, with `cx`'s waker registered so the task is woken (never
    /// spinning) when the frontier can move: by the watermark the feed follows, or, where
    /// the feed's own holds keep it back, by the shard as it lets a hold go.
    ///
    /// The writer process only: in a reader process nothing in this process learns of the
    /// writer's progress, so it is `Unsupported` there; use [`wait`](Self::wait) on a thread
    /// of its own (#1407 tracks an async form).
    pub fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Next<'_>>> {
        use std::task::Poll;
        let Some(feed) = self.writer.clone() else {
            return Poll::Ready(Err(Error::Unsupported(
                "an async change feed in a reader process: call the feed's blocking `next` on a \
                 thread of its own (#1407)",
            )));
        };
        // Whether this call left a waker on the hold list: a poll that then finds a commit
        // takes it back, so releases don't keep paying for a waiter that isn't waiting
        // (simp724's P1 on #1408).
        let mut on_hold_list = false;
        loop {
            let polled = match self.poll() {
                Ok(p) => p,
                Err(e) => {
                    self.leave_hold_list(on_hold_list);
                    return Poll::Ready(Err(e));
                }
            };
            let Polled::Pending { held } = polled else {
                self.leave_hold_list(on_hold_list);
                return Poll::Ready(Ok(self.lend(polled)));
            };
            if !self.register_wake(&feed, held, cx.waker(), &mut on_hold_list) {
                return Poll::Pending;
            }
        }
    }

    /// Leaves `waker` where the frontier's next move wakes it, for [`wait`](Self::wait) and
    /// [`poll_next`](Self::poll_next) in the writer process; `true` to poll again instead, as
    /// something that wakes it may have come before the registration. `on_hold_list` notes
    /// a waker left on the feed's hold list.
    fn register_wake(
        &self,
        feed: &crate::feed::Feed,
        held: bool,
        waker: &std::task::Waker,
        on_hold_list: &mut bool,
    ) -> bool {
        let target = self.frontier.saturating_add(1);
        if held {
            // The feed's own holds (or a catalog change's) keep the frontier below the
            // watermark: park on the hold list, which their release wakes, rather than poll
            // every millisecond (#1417).
            feed.wait_for_hold(self.key, waker);
            *on_hold_list = true;
        } else {
            let waiters = if self.durable {
                &self.shared.durable_waiters
            } else {
                &self.shared.waiters
            };
            waiters.register(self.key, target, Wake::Feed(waker.clone()));
            // A feed of every table waits for catalog changes too, which move no seqno:
            // their release wakes the catalog list.
            if self.tables.is_empty() {
                feed.wait_for_catalog(self.key, waker);
                *on_hold_list = true;
            }
        }
        // A releaser's store before its wake, re-checked after the registration's fence, so
        // a wake that found the list empty is seen here (simp724's W2 on #1533); then a
        // publish that missed the registration.
        self.catalog_wake_missed(feed)
            || self.shm.written_seqno(self.durable, self.tables.is_empty()) >= target
    }

    fn lend(&self, polled: Polled) -> Next<'_> {
        match polled {
            Polled::Commit => Next::Commit(&self.out),
            Polled::Catalog => Next::Catalog(&self.out_catalog),
            Polled::Pending { .. } => Next::Pending,
            Polled::Expired => Next::Expired(self.expired.as_ref().expect("set")),
        }
    }

    /// PREPAREs read and held for their Decision (an inspection helper for tests).
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn held_prepares(&self) -> usize {
        self.held.len()
    }

    /// The read-ahead kept now (D320): the bytes counted over the records read but not
    /// delivered, checked against a scan of them (an inspection helper for tests).
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn read_ahead_bytes(&self) -> u64 {
        let scanned = self
            .ready
            .values()
            .map(pending_bytes)
            .chain(self.held.values().map(held_bytes))
            .sum::<u64>();
        assert_eq!(self.ahead.bytes, scanned, "the read-ahead's tracked bytes");
        self.ahead.bytes
    }

    /// The rounds the read-ahead cap passes over by id until the next rewind (D320; an
    /// inspection count for tests, simp724's S1 on #1622).
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn skipped_rounds(&self) -> usize {
        self.ahead.skipped_rounds.len()
    }

    /// Sets the read-ahead budget (D320) in place of the 64 MiB default: a seam for tests
    /// that need the cap to bite on a few commits.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn set_read_ahead(&mut self, bytes: u64) {
        self.ahead.base = bytes;
        self.ahead.budget = bytes;
    }

    /// The frontier as of the last poll.
    pub fn frontier(&self) -> Seqno {
        self.frontier
    }

    /// Delivers feed-silent commits too (D331, ICR 0141), each with [`silent`](Self::silent)
    /// set: for a replica's follower only, which applies them silent, so the replica stays a
    /// copy of its primary and its own feed matches the primary's. Never reachable from a
    /// client's subscription: the public API's feed options have no such field. Not part of
    /// the stable API.
    #[doc(hidden)]
    pub fn include_silent(&mut self) {
        self.include_silent = true;
    }

    /// Whether the commit last delivered is feed-silent (only ever, with
    /// [`include_silent`](Self::include_silent)). Not part of the stable API.
    #[doc(hidden)]
    pub fn silent(&self) -> bool {
        self.out_silent
    }

    /// The range drop the commit last delivered stands for (D331; only ever with
    /// [`include_silent`](Self::include_silent)): an empty commit at the drop's seqno. Not
    /// part of the stable API.
    #[doc(hidden)]
    #[inline]
    pub fn range_drop(&self) -> Option<&RangeDropEvent> {
        // One compare for a commit (#2089's callgrind): `drop_seqno` is set only when a
        // drop's empty commit is delivered, and a seqno is never delivered twice.
        if self.drop_seqno != self.out.seqno {
            return None;
        }
        self.out_drop.as_ref()
    }

    /// Delivers the range drop at `key` (D331), if one is there: an empty commit at its
    /// seqno, which [`range_drop`](Self::range_drop) names (simp724's M1 on #2089: `Next`
    /// stays as it is). Every stream logs the drop, and their copies share one key in
    /// `ready`: the one kept advances its stream, and the others pass as records behind
    /// `delivered` (feed1248's note).
    #[cold]
    #[inline(never)]
    fn emit_drop(&mut self, key: Seqno) -> bool {
        // Ones behind `delivered` were passed over (read again past a cutoff).
        self.drops.retain(|&k, _| k >= key);
        let Some(event) = self.drops.remove(&key) else {
            return false;
        };
        self.out.seqno = key;
        self.out.ts = 0;
        self.out.changes.clear();
        self.out_silent = false;
        // Nor a transaction's share (D329 2b's xid): the drop isn't one, whatever the
        // commit before it was (#2089 landing after #2076).
        self.out_xid = None;
        self.out_drop = Some(event);
        self.drop_seqno = key;
        true
    }

    /// The transaction the commit last delivered is a share of, when it's a spanning
    /// commit's (D329 2b): every share of one commit has the same. Not part of the stable
    /// API.
    #[doc(hidden)]
    #[inline]
    pub fn xid(&self) -> Option<&Arc<[u8]>> {
        self.out_xid.as_ref()
    }

    /// The silent and xid notes held now (test hook), each checked against what the reader
    /// keeps: a single commit's in `ready`, a round's among the held shares. So empty once a
    /// feed has drained.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn notes(&self) -> usize {
        for &k in self.silent_seen.iter().chain(self.xids.keys()) {
            let kept = self.ready.contains_key(&k)
                || self
                    .held
                    .range((k, 0)..(k.saturating_add(1), 0))
                    .next()
                    .is_some();
            assert!(kept, "a note for {k}, a commit the reader doesn't keep");
        }
        self.silent_seen.len() + self.xids.len()
    }

    /// The last delivered seqno: every commit at or below it was delivered (or precedes the
    /// feed's start).
    pub fn delivered(&self) -> Seqno {
        self.delivered
    }

    /// How far this feed has covered its history (ICR 0114 amendment; simp724's F1 on #1518):
    /// every commit of its spec at or below [`FeedPosition::seqno`] was delivered, passed
    /// over or precedes its start, and in a feed of every table a catalog change was
    /// delivered or passed over iff its sequence is at most [`FeedPosition::catalog`]: changes
    /// go in sequence order, and those passed over at the start are a prefix. The seqno says
    /// nothing about catalog changes (simp724's P1 on #1520): a `Now` or snapshot start can be
    /// past the ceiling of a change not yet published, which it still delivers.
    ///
    /// It is at least [`delivered`](Self::delivered), which follows every record of the
    /// feed's streams, other tables' included (passed over with no changes), and it also
    /// covers the seqnos no record holds: once a whole read pass under a frontier F finds
    /// nothing left to deliver below it, every commit up to F is covered (M1: every record at
    /// or below F was read). An undecided round needs no term: the frontier never passes an
    /// undecided apply seqno. It never goes back: a record read later is above the frontier
    /// of every earlier pass. Before the first poll it is the start's bound, or a token's
    /// delivered seqno. Under the read-ahead cap (D320) a pass covers only up to its cutoff
    /// as well: every record at or below both was kept.
    pub fn position(&self) -> FeedPosition {
        let mut seqno = self.read_through.unwrap_or(0).min(self.ahead.cutoff);
        if let Some((&key, _)) = self.ready.first_key_value() {
            seqno = seqno.min(key.saturating_sub(1));
        }
        if self.tables.is_empty()
            && let Some((ev, _)) = self.catalog.front()
        {
            seqno = seqno.min(ev.seqno_ceiling.saturating_sub(1));
        }
        FeedPosition {
            seqno: seqno.max(self.delivered),
            catalog: if self.tables.is_empty() {
                self.catalog_seq
            } else {
                0
            },
        }
    }

    /// Publishes [`position`](Self::position)'s seqno into `sink` from now on, as a
    /// `fetch_max` at the end of every poll (D316 A4, ICR 0116): it moves past commits that
    /// carry nothing for the feed and past seqnos no record holds, even while the feed waits,
    /// so a caught-up feed reads as caught up. `fetch_max`, so a resubscribe's new reader,
    /// whose position can start lower, never moves it back. Stores the current one at once.
    pub fn set_progress(&mut self, sink: Arc<AtomicU64>) {
        sink.fetch_max(self.position().seqno, Ordering::Relaxed);
        self.progress = Some(sink);
    }

    /// A feed of every table: the digest of the catalog at its position (D325's model, #1853
    /// part C2), which a replica that applied everything this feed delivered must have too
    /// ([`Engine::catalog_digest`](crate::Engine::catalog_digest)). `None` for a feed of some
    /// tables, and for one that expired at its open.
    pub fn catalog_digest(&self) -> Option<u64> {
        self.model.as_ref().map(CatalogModel::digest)
    }

    /// One poll ([`poll_inner`](Self::poll_inner)), then the position published to the
    /// progress sink, if one is set ([`set_progress`](Self::set_progress), D316 A4): on
    /// every path out, a commit, `Pending`, an expiry or an error, so a waiting feed's
    /// position keeps up with the frontier it passes. One `fetch_max` a poll, on the feed's
    /// own thread; nothing on the commit path.
    fn poll(&mut self) -> Result<Polled> {
        self.polls += 1;
        let polled = self.poll_inner();
        if let Some(p) = &self.progress {
            p.fetch_max(self.position().seqno, Ordering::Relaxed);
        }
        polled
    }

    fn poll_inner(&mut self) -> Result<Polled> {
        if let Some(e) = &self.failed {
            return Err(Error::Corruption(format!(
                "the change feed stopped at an earlier error: {e}"
            )));
        }
        if self.expired.is_some() {
            return Ok(Polled::Expired);
        }
        // The writer this region belongs to is gone (a reader process's attachment after a
        // writer restart, D310 M2): seqnos may be reissued, so nothing more is read here.
        // The caller subscribes again from its token, which the new writer checks.
        if self.shm.is_stale() {
            return Err(Error::SnapshotExpired);
        }
        // A commit (or catalog change) already read and below the frontier goes first.
        if let Some(p) = self.emit()?.polled() {
            return Ok(p);
        }
        if self.expired.is_some() {
            return Ok(Polled::Expired);
        }
        // A feed of every table, once the session's sidecar appends stopped: no later catalog
        // change can reach it, and the frozen catalog hold keeps the frontier from moving, so
        // this is checked before the frontier is (simp724 on #1493). In a reader process, from
        // the stop the writer published in shared memory (D315 amendment, #1869), not by
        // waiting at the hold until the writer restarts.
        if self.tables.is_empty() && self.appends_stopped() {
            self.expired = Some(expiry(
                "the change feed's sidecar can no longer be written: catalog changes after this point can't be delivered",
            ));
            return Ok(Polled::Expired);
        }
        // simp724's order: the checkpoints, then the frontier, each Acquire from the
        // shared-memory marks the writer mirrors (layout version 8), then every stream read
        // to its readable end. By M1 every record at or below F is then read: F never passes
        // a record behind a held segment header (FORMAT §10.1 rule 1), since D200 hides its
        // group until a sync writes that header and the feed's holds settle on
        // `Wal::written` (which excludes it). `it::feed_head` pins this; no head mark is
        // needed (D310 PR 4).
        let checkpoints: Vec<Lsn> = (0..self.streams.len())
            .map(|s| Lsn(self.shm.feed_marks(s as u32).checkpoint))
            .collect();
        let frontier = self.shm.written_seqno(self.durable, self.tables.is_empty());
        // Reader processes: where a test restarts the writer between the stale check above
        // and the reads below (simp724's B2).
        #[cfg(feature = "test-hooks")]
        self.shared
            .hooks
            .sites
            .run(crate::engine::hooks::HookSite::ReaderFeedReads);
        // D320: the read-ahead cap passed records over, and the frontier is past its cutoff
        // with nothing kept left to deliver below it: the streams are read again from the
        // first record of each not delivered (the token's hints), as a resume from the
        // token would be.
        if self.ahead.capped()
            && self.ahead.cutoff < frontier
            && self
                .ready
                .first_key_value()
                .is_none_or(|(&k, _)| k > self.ahead.cutoff)
            && !self.rewind()?
        {
            return Ok(Polled::Expired);
        }
        // Nothing committed since the streams were last read to their ends under this
        // frontier (it rises with every commit): by M1 every record at or below it was read
        // then, and `emit` above found none left to deliver. Reading again would only find
        // the same ends, which costs a stream's segment headers and a chunk of frames each
        // (#1448: an idle reader process spent 1-1.7 ms of CPU per poll there). Not while a
        // PREPARE is held: a checkpoint or floor can move without a commit (a flush), and
        // `drop_aborted` (rule C) must see it, or the commits behind an aborted round wait
        // for an unrelated one (feed1248's P1 on #1449). An external share (D329) is settled
        // here: it ends only by a record of its own (its Decision, or its abort's type 6),
        // whose seqno moves the frontier, so it never needs rule C, and a share in doubt for
        // hours keeps the shortcut (ICR 0140's E3). A writer restart since the stale check
        // above is seen here, as after the reads below.
        if self.read_through == Some(frontier) && self.held.values().all(|h| h.external) {
            if self.shm.is_stale() {
                return Err(Error::SnapshotExpired);
            }
            // A catalog change moves no seqno: a feed of every table loads the sidecar here
            // too, when the writer counted a new one (in a reader process, which has no count
            // to look at, on each of its backoff's polls), and delivers it if it's due.
            if self.tables.is_empty() && self.catalog_moved() {
                self.load_catalog(None)?;
                if let Some(p) = self.emit()?.polled() {
                    return Ok(p);
                }
            }
            return Ok(self.pending_state());
        }
        loop {
            self.stream_reads += 1;
            for s in 0..self.streams.len() {
                if !self.read_stream(s)? {
                    return Ok(Polled::Expired);
                }
            }
            // The writer may have restarted while the streams were read (simp724's B2): the new
            // session's records are then in, read as outside the history, and the frontier is
            // the old one's. Nothing read here is delivered: the caller resubscribes from the
            // token. A new writer's segments are written after its shared memory exists, so this
            // check sees every such restart.
            if self.shm.is_stale() {
                return Err(Error::SnapshotExpired);
            }
            // Only now: simp724's rule holds once every stream was read to its end after the
            // checkpoints were loaded.
            let floors: Vec<u32> = (0..self.streams.len()).map(|s| self.floor(s)).collect();
            if let Some(e) = self.drop_aborted(&checkpoints, &floors) {
                self.expired = Some(e);
                return Ok(Polled::Expired);
            }
            // #1827: a pass that passed nothing over keeps everything past the hints; the
            // skip indexes have nothing left to jump.
            if self.indexing && !self.ahead.capped() {
                self.indexing = false;
                self.streams.iter_mut().for_each(|st| st.index.clear());
            }
            self.frontier = self.frontier.max(frontier);
            self.read_through = Some(frontier);
            // A feed of every table (D315): the catalog changes up to the frontier, after it was
            // loaded. In the writer's process, once the session's sidecar appends stopped no
            // later change can reach the feed: it expires (simp724 on #1493).
            if self.tables.is_empty() {
                self.load_catalog(None)?;
            }
            if let Some(p) = self.emit()?.polled() {
                return Ok(p);
            }
            if self.expired.is_some() {
                return Ok(Polled::Expired);
            }
            // D320's liveness: a capped pass left nothing to deliver below a cutoff under the
            // frontier (shares of rounds in flight around the next commit filled the budget).
            // It reads again at once, from a rewind, keeping twice as much (a delivery restores
            // the budget): `Pending` still says every commit up to the frontier was delivered.
            if self.ahead.cutoff < self.frontier
                && self
                    .ready
                    .first_key_value()
                    .is_none_or(|(&k, _)| k > self.ahead.cutoff)
            {
                self.ahead.budget = self.ahead.budget.saturating_mul(2);
                if !self.rewind()? {
                    return Ok(Polled::Expired);
                }
                continue;
            }
            return Ok(self.pending_state());
        }
    }

    /// Nothing to deliver now: whether the feed's own holds keep the frontier below the
    /// watermark it follows (the visible watermark may be past the frontier).
    fn pending_state(&self) -> Polled {
        let watermark = if self.durable {
            self.shm.durable_seqno()
        } else {
            self.shm.visible_seqno()
        };
        Polled::Pending {
            held: watermark > self.frontier,
        }
    }

    /// Polls that read the streams to their ends since the reader opened (#1448).
    #[cfg(feature = "test-hooks")]
    pub fn stream_reads(&self) -> u64 {
        self.stream_reads
    }

    /// Records the stream cursors stepped since the reader opened (#1827).
    #[cfg(feature = "test-hooks")]
    pub fn record_steps(&self) -> u64 {
        self.record_steps
    }

    /// Indexed chunks jumped over since the reader opened (#1827).
    #[cfg(feature = "test-hooks")]
    pub fn chunk_skips(&self) -> u64 {
        self.chunk_skips
    }

    /// The chunks in the streams' skip indexes now (#1827).
    #[cfg(feature = "test-hooks")]
    pub fn indexed_chunks(&self) -> usize {
        self.streams.iter().map(|st| st.index.len()).sum()
    }

    /// The most chunks one stream's skip index keeps (#1827, perf2's S1 on #1922).
    #[cfg(feature = "test-hooks")]
    pub fn skip_index_cap() -> usize {
        SKIP_INDEX_CHUNKS
    }

    /// The largest skip index of any stream now (#1827).
    #[cfg(feature = "test-hooks")]
    pub fn largest_skip_index(&self) -> usize {
        self.streams
            .iter()
            .map(|st| st.index.len())
            .max()
            .unwrap_or(0)
    }

    /// The skip indexes' merging passes since the reader opened (#1827).
    #[cfg(feature = "test-hooks")]
    pub fn skip_index_coarsenings(&self) -> u64 {
        self.streams.iter().map(|st| st.index.coarsenings).sum()
    }

    /// Polls of any kind since the reader opened (#1417).
    #[cfg(feature = "test-hooks")]
    pub fn polls(&self) -> u64 {
        self.polls
    }

    /// [`wait`](Self::wait)'s re-polls that skipped parking since the reader opened: a spin
    /// shows here, while a spurious park return does not (an inspection count for tests).
    #[cfg(feature = "test-hooks")]
    pub fn unparked_repolls(&self) -> u64 {
        self.unparked_repolls
    }

    /// [`wait`](Self::wait)'s parks since the reader opened (an inspection count for tests).
    #[cfg(feature = "test-hooks")]
    pub fn parks(&self) -> u64 {
        self.parks
    }

    /// Reads stream `s` to its readable end. Returns `false` once the feed expired.
    fn read_stream(&mut self, s: usize) -> Result<bool> {
        if self.indexing {
            self.read_stream_as::<true>(s)
        } else {
            self.read_stream_as::<false>(s)
        }
    }

    /// [`read_stream`](Self::read_stream), with the skip indexes built and used (#1827) or,
    /// for a reader that has never been capped since the indexes were last dropped, not:
    /// that instance is main's loop, with nothing of the index in it per record (callgrind's
    /// feed-drain). An eviction that turns indexing on mid-stream hands the rest of the
    /// stream to the indexing instance, so the index has no hole.
    fn read_stream_as<const INDEXING: bool>(&mut self, s: usize) -> Result<bool> {
        let chunk_bytes = (self.ahead.base / 16).clamp(256, 1 << 20);
        loop {
            let before = self.streams[s].cursor.position();
            // #1827: a chunk an earlier capped pass indexed whose every record this pass
            // would pass over (keys past the cutoff, which is at least `delivered`): jumped
            // whole, as passing each over would leave it (the hint at its start, its session
            // raised), without stepping its records. Not while a token's delivered records
            // must still be reached: they're at or below `delivered`. A reader that isn't
            // indexing has no chunks, and pays only the flag's test (callgrind's feed-drain).
            if INDEXING
                && self.streams[s].must_reach.is_none()
                && let Some(c) = self.streams[s].index.at(before)
                && c.min_key > self.ahead.cutoff
            {
                debug_assert!(c.min_key > self.delivered, "a chunk at or below delivered");
                self.ahead.skip(s, c.start);
                if self.history.holds(c.session) {
                    self.raise_session(c.session);
                }
                self.chunk_skips += 1;
                if !self.streams[s].cursor.skip_to(c.end)? {
                    self.expired = Some(expiry(
                        "retention passed the feed: a segment it was reading was recycled",
                    ));
                    return Ok(false);
                }
                continue;
            }
            let Some((start, end, session, key, item)) = self.read_one::<INDEXING>(s)? else {
                return Ok(self.expired.is_none());
            };
            if INDEXING {
                self.streams[s]
                    .index
                    .note(before, start, end, key, session, chunk_bytes);
            }
            // A token's delivered records must still be there (a torn tail or a restart
            // that lost them expires it, M2).
            if let Some(m) = self.streams[s].must_reach {
                if start.epoch() != m.epoch() && before.epoch() == m.epoch() && before < m {
                    self.expired = Some(expiry(
                        "records the feed delivered were lost (a torn tail or a writer restart)",
                    ));
                    return Ok(false);
                }
                if end >= m {
                    self.streams[s].must_reach = None;
                }
            }
            let inside = self.history.holds(session);
            // The token names the latest session read through, by the history's order: a
            // resume on a copy without it must expire (#1810).
            // Compared only when it differs: once per session crossed, and per record only
            // for a stream still in the older session while streams straddle a restart (a
            // scan of the history's few entries).
            if inside {
                self.raise_session(session);
            }
            match item {
                Item::Single {
                    seqno,
                    ts,
                    changes,
                    silent,
                } => {
                    if seqno <= self.delivered {
                        // A re-read: its xid, if any, isn't kept, nor left for the next record.
                        if self.read_xid.is_some() {
                            self.read_xid = None;
                        }
                        let st = &mut self.streams[s];
                        st.delivered_end = st.delivered_end.max(end);
                        continue;
                    }
                    if !inside {
                        // A writer restart, not a gap: the caller resubscribes (B2).
                        if self.shm.is_stale() {
                            return Err(Error::SnapshotExpired);
                        }
                        self.expired = Some(gap(seqno));
                        return Ok(false);
                    }
                    // Noted only once it's kept: a commit re-read at or below what was
                    // delivered, or skipped, leaves no note behind (feed1248's condition on 2b).
                    if silent {
                        self.silent_seen.insert(seqno);
                    }
                    // One test for an untagged commit; the note out of line (#2076's feed
                    // shapes).
                    if self.read_xid.is_some() {
                        take_read_xid(&mut self.read_xid, &mut self.xids, seqno);
                    }
                    self.insert_ready(
                        seqno,
                        Pending::Single {
                            ts,
                            changes,
                            stream: s,
                            start,
                            end,
                        },
                    );
                }
                Item::Skipped => {
                    self.ahead.skip(s, start);
                    continue;
                }
                Item::SkippedRound { id } => {
                    self.ahead.skip(s, start);
                    self.evict_round(id);
                    continue;
                }
                Item::Prepare {
                    id,
                    changes,
                    external,
                    silent,
                } => {
                    if silent {
                        self.silent_seen.insert(id);
                    }
                    match self.read_xid.take() {
                        Some(xid) => {
                            // A round's shares are all tagged or all not: a tagged share with
                            // an untagged one held first (feed1248's check, both orders).
                            debug_assert!(
                                self.xids.contains_key(&id)
                                    || self
                                        .held
                                        .range((id, 0)..(id.saturating_add(1), 0))
                                        .next()
                                        .is_none(),
                                "a tagged share of a round with an untagged one"
                            );
                            note_xid(&mut self.xids, id, xid);
                        }
                        // A round's shares are all tagged or all not (feed1248's check).
                        None => debug_assert!(
                            !self.xids.contains_key(&id),
                            "an untagged share of a tagged round"
                        ),
                    }
                    let h = Held {
                        stream: s,
                        start,
                        end,
                        changes,
                        outside: !inside,
                        external,
                    };
                    self.starts.add(s, start);
                    self.ahead.bytes += held_bytes(&h);
                    // One share per stream: one already there (never expected) gives up its
                    // start and bytes with it.
                    if let Some(old) = self.held.insert((id, s), h) {
                        self.starts.remove(old.stream, old.start);
                        self.ahead.bytes -= held_bytes(&old);
                    }
                }
                Item::Decision {
                    id,
                    apply,
                    ts,
                    participants,
                } => {
                    if apply <= self.delivered {
                        self.remove_held(id);
                        let st = &mut self.streams[s];
                        st.delivered_end = st.delivered_end.max(end);
                        continue;
                    }
                    if !inside {
                        // A writer restart, not a gap: the caller resubscribes (B2).
                        if self.shm.is_stale() {
                            return Err(Error::SnapshotExpired);
                        }
                        self.expired = Some(gap(apply));
                        return Ok(false);
                    }
                    self.insert_ready(
                        apply,
                        Pending::Decided {
                            id,
                            ts,
                            participants,
                            stream: s,
                            start,
                            end,
                            session,
                        },
                    );
                }
                Item::Abort { id } => {
                    // Its shares are never delivered; the abort's own seqno holds nothing, so
                    // the frontier passes it as any seqno with no event (ICR 0140's E1).
                    // Nor is it a silent commit to report (simp724's N2 on #2055): its note
                    // goes with its shares.
                    self.remove_held(id);
                }
                // Feed-silent (D331): a client's feed passes its seqno as one with no event.
                // A replica's follower takes it in seqno order; every stream holds it, and
                // the copies share one key in `ready`.
                Item::RangeDrop { seqno, event } => {
                    let Some(event) = event else { continue };
                    if seqno <= self.delivered {
                        let st = &mut self.streams[s];
                        st.delivered_end = st.delivered_end.max(end);
                        continue;
                    }
                    // An empty commit at its seqno in `ready` (no variant of its own: the
                    // commits' matches stay as they were, #2089's callgrind), its range
                    // aside until it's delivered.
                    self.drops.insert(seqno, *event);
                    self.insert_ready(
                        seqno,
                        Pending::Single {
                            ts: 0,
                            changes: Raws {
                                heads: Box::new([]),
                                bytes: Box::new([]),
                            },
                            stream: s,
                            start,
                            end,
                        },
                    );
                }
                Item::Other => {
                    // A format 2 COMMIT is never in a fed session's segments (only this
                    // build writes feed marks); any other type is newer than this build.
                    if inside {
                        return Err(Error::Corruption(
                            "a change feed segment holds a record this build does not write".into(),
                        ));
                    }
                }
            }
            if self.trim() && !INDEXING {
                return self.read_stream_as::<true>(s);
            }
        }
    }

    /// Stream `s`'s next record as an owned item, its mutations filtered to the feed's
    /// tables and prefix; `None` at the readable end, or once its segment is gone (which
    /// expires the feed).
    fn read_one<const INDEXING: bool>(&mut self, s: usize) -> Result<Option<ReadRecord>> {
        let (tables, prefix, include_silent) = (&self.tables, &self.prefix, self.include_silent);
        let shared = &self.shared;
        // D320: a record past the read-ahead's cutoff is passed over before its mutations
        // are filtered or copied. One at or below `delivered` takes its usual path.
        let (cutoff, delivered) = (self.ahead.cutoff, self.delivered);
        let skipped_rounds = &self.ahead.skipped_rounds;
        // A feed-silent batch (D331, ICR 0141): checked against `incompat` bit 3 and passed
        // over for a subscription, out of line. The cursors take the mark, so this one test
        // per batch is the feed path's whole cost (#2055's feed shapes); `pack` is as it was.
        // `silent` from the batch's count flags, which the caller read once for this and the
        // xid's bit (#2076's feed shapes).
        let filter = |batch: BatchRef<'_>, silent: bool| {
            if silent {
                silent_batch(shared, batch, tables, prefix, include_silent)
            } else {
                pack(batch, tables, prefix)
            }
        };
        let (start, end, session, record) = match self.streams[s].cursor.step()? {
            Step::Record {
                start,
                end,
                session,
                record,
            } => {
                #[cfg(feature = "test-hooks")]
                {
                    self.record_steps += 1;
                }
                (start, end, session, record)
            }
            Step::End => return Ok(None),
            Step::Gone => {
                self.expired = Some(expiry(
                    "retention passed the feed: a segment it was reading was recycled",
                ));
                return Ok(None);
            }
            Step::Cut => {
                self.expired = Some(expiry(
                    "records the feed read were lost: a power loss cut the log before them",
                ));
                return Ok(None);
            }
        };
        // The lowest key it can matter at (`Chunk::min_key`, #1827), needed only while the
        // skip indexes are built.
        let key = match &record {
            _ if !INDEXING => 0,
            WalRecord::Batch { seqno, .. } => *seqno,
            WalRecord::Prepare { seqno, .. }
            | WalRecord::Decision { seqno, .. }
            | WalRecord::ExternalPrepare { seqno, .. }
            | WalRecord::ExternalAbort { seqno, .. }
            | WalRecord::RangeDrop { seqno, .. } => seqno.saturating_add(1),
            _ => 0,
        };
        let item = match record {
            WalRecord::Batch { seqno, .. } if seqno > cutoff && seqno > delivered => Item::Skipped,
            // Its round's apply seqno is above its id.
            WalRecord::Prepare { seqno, .. } | WalRecord::ExternalPrepare { seqno, .. }
                if seqno >= cutoff || skipped_rounds.contains(&seqno) =>
            {
                Item::Skipped
            }
            WalRecord::Decision {
                seqno, apply_seqno, ..
            } if apply_seqno > cutoff && apply_seqno > delivered => {
                Item::SkippedRound { id: seqno }
            }
            WalRecord::Batch {
                seqno,
                commit_ts,
                batch,
            } => {
                let flags = batch.count_flags();
                if flags & XID_FLAG != 0 {
                    set_read_xid_of(&mut self.read_xid, batch);
                }
                let silent = flags & SILENT_FLAG != 0;
                Item::Single {
                    seqno,
                    ts: commit_ts,
                    changes: filter(batch, silent)?,
                    silent: include_silent && silent,
                }
            }
            WalRecord::Prepare { seqno, batch, .. } => {
                let flags = batch.count_flags();
                if flags & XID_FLAG != 0 {
                    set_read_xid_of(&mut self.read_xid, batch);
                }
                let silent = flags & SILENT_FLAG != 0;
                Item::Prepare {
                    id: seqno,
                    changes: filter(batch, silent)?,
                    external: false,
                    silent: include_silent && silent,
                }
            }
            // D329: an external share is held as a D263 one is, delivered at its Decision's
            // apply seqno, or dropped by its abort.
            WalRecord::ExternalPrepare { seqno, share, .. } => match share.batch() {
                Some(batch) => Item::Prepare {
                    id: {
                        set_read_xid(&mut self.read_xid, share.xid());
                        seqno
                    },
                    changes: filter(batch, batch.is_silent())?,
                    external: true,
                    silent: include_silent && batch.is_silent(),
                },
                None => Item::Other,
            },
            WalRecord::ExternalAbort { seqno, .. } => Item::Abort { id: seqno },
            WalRecord::RangeDrop { seqno, range } => Item::RangeDrop {
                seqno,
                event: include_silent.then(|| {
                    let (table, start, end) = range.fields();
                    Box::new(RangeDropEvent {
                        seqno,
                        table,
                        start: start.to_vec(),
                        end: end.map(<[u8]>::to_vec),
                    })
                }),
            },
            WalRecord::Decision {
                seqno,
                apply_seqno,
                commit_ts,
                participants,
            } => Item::Decision {
                id: seqno,
                apply: apply_seqno,
                ts: commit_ts,
                participants: participants.iter().collect(),
            },
            _ => Item::Other,
        };
        Ok(Some((start, end, session, key, item)))
    }

    /// Adds a record read to `ready`, counting its start. A key already there (never expected:
    /// one record per seqno) gives up its start with its record.
    fn insert_ready(&mut self, key: Seqno, p: Pending) {
        let (stream, start) = p.at();
        self.starts.add(stream, start);
        self.ahead.bytes += pending_bytes(&p);
        if let Pending::Decided { id, .. } = p {
            self.ahead.decided.insert(id, key);
        }
        if let Some(old) = self.ready.insert(key, p) {
            self.forget_ready(&old);
        }
    }

    /// Uncounts a record taken out of `ready`: its start, bytes and decided round.
    fn forget_ready(&mut self, p: &Pending) {
        let (stream, start) = p.at();
        self.starts.remove(stream, start);
        self.ahead.bytes -= pending_bytes(p);
        if let Pending::Decided { id, .. } = p {
            self.ahead.decided.remove(id);
        }
    }

    /// Takes round `id`'s held shares, uncounting their starts and bytes.
    fn remove_held(&mut self, id: Seqno) -> Vec<Held> {
        let mut shares = Vec::new();
        for (_, h) in self
            .held
            .extract_if((id, 0)..=(id, usize::MAX), |_, _| true)
        {
            self.starts.remove(h.stream, h.start);
            self.ahead.bytes -= held_bytes(&h);
            shares.push(h);
        }
        // Its notes go with its shares: a round dropped, evicted or decided below what was
        // delivered leaves nothing behind.
        self.silent_seen.remove(&id);
        self.xids.remove(&id);
        shares
    }

    /// Drops held PREPAREs of rounds that ended without a Decision: their stream's
    /// checkpoint (loaded before the frontier) passed them and no Decision for them was read
    /// (rule C). A held share the floor passed but the checkpoint didn't expires the feed:
    /// the floor never passes the checkpoint's segment, so that is a bound broken, and
    /// dropping it could pass over a committed round (fail closed, simp724's H1 on #1316).
    fn drop_aborted(&mut self, checkpoints: &[Lsn], floors: &[u32]) -> Option<Expiry> {
        let decided: std::collections::HashSet<Seqno> = self
            .ready
            .values()
            .filter_map(|p| match p {
                Pending::Decided { id, .. } => Some(*id),
                Pending::Single { .. } => None,
            })
            .collect();
        drop_undecided(
            &mut self.held,
            &mut self.starts,
            &mut self.ahead.bytes,
            &mut self.dropped,
            &decided,
            checkpoints,
            floors,
        )
    }

    /// A decided round's shares, taken from those held: whole, or dropped whole by an
    /// earlier session's recovery (a power loss took a share of its `Buffered` commit, and
    /// recovery replays all or nothing), or a gap that expires the feed. Each share's stream
    /// is past it.
    fn take_round(&mut self, id: Seqno, participants: &[StreamId], session: u64) -> Round {
        let shares = self.remove_held(id);
        // A round rule C dropped a share of: never passed over silently.
        let dropped = self.dropped.remove(&id).is_some();
        // Every participant's PREPARE was in its file before the Decision was written, so it
        // has been read; one missing was lost under the reader.
        let complete = participants
            .iter()
            .all(|p| shares.iter().any(|h| h.stream == p.0 as usize));
        if !complete && session != self.current && !dropped {
            for h in &shares {
                let st = &mut self.streams[h.stream];
                st.delivered_end = st.delivered_end.max(h.end);
            }
            return Round::Dropped;
        }
        if !complete || shares.iter().any(|h| h.outside) {
            // The feed expires on it (#2196): each share it held is a resume point again, so
            // the hint a token is built from stays at or before it.
            for h in &shares {
                self.ahead.skip(h.stream, h.start);
            }
            return Round::Gap;
        }
        let mut raws = Vec::with_capacity(shares.len());
        for h in shares {
            let st = &mut self.streams[h.stream];
            st.delivered_end = st.delivered_end.max(h.end);
            raws.push(h.changes);
        }
        Round::Whole(raws)
    }

    /// Delivers the smallest pending commit at or below the frontier into `out`. Returns
    /// whether it did; a commit with nothing the feed follows is passed over.
    fn emit(&mut self) -> Result<Emitted> {
        loop {
            // A catalog change first, once every commit below its ceiling is delivered: the
            // frontier is past them (F + 1 >= the ceiling), and none of them is still ready.
            // Up to the frontier, and under the read-ahead cap its cutoff (D320): every
            // record at or below both was kept.
            let limit = self.frontier.min(self.ahead.cutoff);
            if let Some((ev, _)) = self.catalog.front()
                && ev.seqno_ceiling <= limit.saturating_add(1)
                && self
                    .ready
                    .first_key_value()
                    .is_none_or(|(&k, _)| k >= ev.seqno_ceiling)
            {
                let (ev, session) = self.catalog.pop_front().expect("checked");
                if let Some(m) = &mut self.model {
                    m.apply(&ev.edits);
                }
                self.note_drops(&ev.edits);
                self.catalog_seq = ev.sequence;
                self.raise_session(session);
                self.out_catalog = ev;
                // No commit is delivered with it, so no drop either (feed1248 on #2089).
                self.drop_seqno = 0;
                return Ok(Emitted::Catalog);
            }
            let Some((&key, _)) = self.ready.first_key_value() else {
                return Ok(Emitted::Nothing);
            };
            if key > limit {
                return Ok(Emitted::Nothing);
            }
            // Before anything is taken: the view can fail (a reader process's attachment
            // whose writer is gone, or whose view is being republished), and a failure here
            // leaves the commit to the next call.
            let view = (self.view)()?;
            // A reader process's view can lag the writer's manifest: a value separated at
            // commit time names a blob file the manifest committed before the commit was
            // logged, which a view record of an older manifest version doesn't list yet
            // (simp724's question on #1352). Wait for a newer view, up to a bound, rather
            // than take the commit and expire; past the bound, the take below expires as for
            // a file retention removed.
            if self.writer.is_none() {
                let listed = self
                    .ready
                    .first_key_value()
                    .is_some_and(|(_, p)| self.blobs_listed(&view, p));
                if !listed {
                    let since = *self.blob_wait.get_or_insert_with(Instant::now);
                    if since.elapsed() < READER_BLOB_WAIT {
                        return Ok(Emitted::Nothing);
                    }
                }
            }
            self.blob_wait = None;
            let (_, p) = self.ready.pop_first().expect("checked");
            self.forget_ready(&p);
            self.ahead.budget = self.ahead.base;
            let (ts, raws) = match p {
                Pending::Single {
                    ts,
                    changes,
                    stream,
                    end,
                    ..
                } => {
                    let st = &mut self.streams[stream];
                    st.delivered_end = st.delivered_end.max(end);
                    if self.include_silent {
                        self.out_silent = self.silent_seen.remove(&key);
                    }
                    // One test for a commit that isn't a spanning one's share, as `out_silent`
                    // is (#2076's feed shapes).
                    if !self.xids.is_empty() || self.out_xid.is_some() {
                        set_out_xid(&mut self.out_xid, &mut self.xids, key);
                    }
                    (ts, Packs::One(changes))
                }
                Pending::Decided {
                    id,
                    ts,
                    participants,
                    stream,
                    start,
                    end,
                    session,
                    ..
                } => {
                    let st = &mut self.streams[stream];
                    let was = st.delivered_end;
                    st.delivered_end = st.delivered_end.max(end);
                    if self.include_silent {
                        self.out_silent = self.silent_seen.remove(&id);
                    }
                    if !self.xids.is_empty() || self.out_xid.is_some() {
                        set_out_xid(&mut self.out_xid, &mut self.xids, id);
                    }
                    match self.take_round(id, &participants, session) {
                        Round::Whole(raws) => (ts, Packs::Many(raws)),
                        Round::Dropped => {
                            self.delivered = key;
                            continue;
                        }
                        Round::Gap => {
                            // #2196: the position stays before the round, so a token taken
                            // from the expired feed resumes into the same gap, never past
                            // it: the Decision is a resume point again (its shares are, in
                            // `take_round`), and its stream's end delivered goes back.
                            self.streams[stream].delivered_end = was;
                            self.ahead.skip(stream, start);
                            self.expired = Some(gap(key));
                            return Ok(Emitted::Nothing);
                        }
                    }
                }
            };
            self.delivered = key;
            // A dropped table's changes, in a commit routed before its drop and logged after
            // it (a feed of every table, D315): the drop discarded them.
            let packs: &[Raws] = match &raws {
                Packs::One(r) => std::slice::from_ref(r),
                Packs::Many(v) => v,
            };
            // A feed of every table (#1853): the tables seen present in the catalog, kept
            // while the catalog is the same one (a view swap with a new catalog forgets
            // them), so a commit to tables seen before costs no lookup (perf287 on #1857).
            let every = self.tables.is_empty();
            if every
                && !self
                    .present_of
                    .as_ref()
                    .is_some_and(|c| Arc::ptr_eq(c, &view.catalog))
            {
                self.present.clear();
                self.present_last = None;
                self.present_of = Some(Arc::clone(&view.catalog));
            }
            let dropped = &self.dropped_tables;
            let kept = |r: &RawRef<'_>| dropped.is_empty() || !dropped.contains(&r.table);
            if !packs.iter().flat_map(Raws::iter).any(|r| kept(&r)) {
                // A range drop's empty commit (D331), only ever with `include_silent`.
                if !self.drops.is_empty() && self.emit_drop(key) {
                    return Ok(Emitted::Commit);
                }
                continue;
            }
            self.out.seqno = key;
            self.out.ts = ts;
            self.out.changes.clear();
            // Takes change `$r` into the commit, or ends the reader (a blob gone, or an error).
            // One copy of the code in each loop below, so a feed of some tables runs exactly
            // the loop it ran before #1857's check.
            macro_rules! take {
                ($r:expr) => {{
                    let r = $r;
                    let value = match blob_pointer(r.value) {
                        None => r.value.to_vec(),
                        // The blob GC horizon follows the oldest retained segment, not readers: a
                        // file the view no longer lists under a slow reader means retention
                        // passed it (S1). Any other failure, a checksum's included, is an error.
                        Some(ptr) if view.ssts.blob_reader(ptr.blob_file).is_none() => {
                            self.expired = Some(expiry(
                                "a blob value the feed was delivering is gone: retention passed the feed",
                            ));
                            return Ok(Emitted::Nothing);
                        }
                        Some(ptr) => match view.ssts.read_pointer(&ptr) {
                            Ok(cell) => cell.to_vec(),
                            // The commit is taken: going on would skip it, so the reader stops
                            // here for good.
                            Err(e) => {
                                self.failed = Some(e.to_string());
                                return Err(e);
                            }
                        },
                    };
                    self.out.changes.push(Change {
                        table: r.table,
                        family: r.family,
                        kind: r.kind,
                        ts: r.ts.unwrap_or(ts),
                        row: r.row.to_vec(),
                        qualifier: r.qualifier.to_vec(),
                        value,
                    });
                }};
            }
            // A table of this commit the catalog doesn't have (#1853), checked in this pass:
            // a commit's changes run by table, so each run is checked once, and a run of the
            // table the last one ended with (`present_last`) costs no lookup at all.
            // The order is deliberate (feed1248's N1 on #1874): the changes are taken in the
            // same pass, so a commit with a missing table may end on its values first, a blob
            // gone (`FeedExpired` too, worded for retention) or a read error. Checking before
            // the values would bring back the second pass this replaces.
            let mut missing = false;
            if every {
                let mut last = self.present_last;
                for r in packs.iter().flat_map(Raws::iter).filter(kept) {
                    if last != Some(r.table) {
                        last = Some(r.table);
                        if !self.present.contains(&r.table) {
                            if view.catalog.table(r.table).is_some() {
                                self.present.insert(r.table);
                            } else {
                                missing = true;
                            }
                        }
                    }
                    take!(r);
                }
                if !missing {
                    self.present_last = last;
                }
            } else {
                for r in packs.iter().flat_map(Raws::iter).filter(kept) {
                    take!(r);
                }
            }
            // A table the catalog no longer has was dropped, and every drop appends its
            // catalog change to the sidecar before the catalog loses the table. If no recorded
            // drop explains a table missing from the view, even after reading the sidecar
            // again (a drop can race the view), the table was dropped by a session that
            // records no catalog changes: a 1.1.x session after a rollback. Catalog changes
            // are then missing from the history (that session's creates too), so the feed
            // expires rather than deliver a commit it can't name and go on without them. In
            // a reader process, whose view can lag the writer's manifest, a table a recorded
            // change created may be missing from the view without a drop.
            if missing {
                let lagging = self.writer.is_none();
                let unexplained = |this: &Self| {
                    packs
                        .iter()
                        .flat_map(Raws::iter)
                        .map(|r| r.table)
                        .find(|&t| {
                            view.catalog.table(t).is_none()
                                && !this.recorded_drops.contains(&t)
                                && !(lagging && this.recorded_creates.contains(&t))
                        })
                };
                // Most missing tables are explained by a drop already read: the sidecar is
                // read again only when one isn't.
                if unexplained(self).is_some() {
                    self.load_catalog(None)?;
                    // The re-read finds only a change that raced the view, whose ceiling is
                    // above this commit: the catalog hold (D315) kept this commit from being
                    // emittable while any change at or below it was unloaded (feed1248's Q1
                    // on #1857).
                    debug_assert!(
                        self.catalog
                            .front()
                            .is_none_or(|(ev, _)| ev.seqno_ceiling > key),
                        "a catalog change at or below seqno {key} was loaded after it was popped"
                    );
                }
                if let Some(table) = unexplained(self) {
                    self.expired = Some(expiry(format!(
                        "seqno {key} changes table id {}, which the catalog no longer has and \
                         no catalog change in the feed's history dropped: a session that \
                         records no catalog changes (a 1.1.x build's, after a rollback) \
                         changed the tables, so the feed can't deliver those changes",
                        table.0
                    )));
                    return Ok(Emitted::Nothing);
                }
            }
            return Ok(Emitted::Commit);
        }
    }
}

fn gap(key: Seqno) -> Expiry {
    expiry(format!(
        "history has a gap before seqno {key}: a session that did not feed these tables wrote the database"
    ))
}

/// [`FeedReader::drop_aborted`]'s rule over the held shares: drops each undecided share its
/// stream's checkpoint passed, noting its round in `dropped`, and expires on one only the
/// floor passed. Forgets dropped rounds whose segment is below the floor.
fn drop_undecided(
    held: &mut BTreeMap<(Seqno, usize), Held>,
    starts: &mut Starts,
    bytes: &mut u64,
    dropped: &mut HashMap<Seqno, (usize, u32)>,
    decided: &std::collections::HashSet<Seqno>,
    checkpoints: &[Lsn],
    floors: &[u32],
) -> Option<Expiry> {
    let floor_of = |s: usize| floors.get(s).copied().unwrap_or(0);
    for (&(id, _), h) in held.iter() {
        if decided.contains(&id) {
            continue;
        }
        let passed = checkpoints.get(h.stream).is_some_and(|&c| c > h.start);
        if !passed && h.start.epoch() < floor_of(h.stream) {
            return Some(expiry(
                "retention passed a held cross-shard share the checkpoint had not",
            ));
        }
    }
    held.retain(|&(id, _), h| {
        if decided.contains(&id) {
            return true;
        }
        let passed = checkpoints.get(h.stream).is_some_and(|&c| c > h.start);
        if passed {
            dropped.insert(id, (h.stream, h.start.epoch()));
            starts.remove(h.stream, h.start);
            *bytes -= held_bytes(h);
        }
        !passed
    });
    dropped.retain(|_, &mut (s, epoch)| epoch >= floor_of(s));
    None
}

impl FeedReader {
    /// Whether `view` lists every blob file pending commit `p`'s values point into.
    fn blobs_listed(&self, view: &crate::snapshot::View, p: &Pending) -> bool {
        let listed = |raws: &Raws| {
            raws.iter().all(|r| {
                blob_pointer(r.value)
                    .is_none_or(|ptr| view.ssts.blob_reader(ptr.blob_file).is_some())
            })
        };
        match p {
            Pending::Single { changes, .. } => listed(changes),
            Pending::Decided { id, .. } => self
                .held
                .range((*id, 0)..=(*id, usize::MAX))
                .all(|(_, h)| listed(&h.changes)),
        }
    }

    /// `FeedStart::Oldest` from before this open (#1465): the oldest earlier session of the
    /// feed's history whose first segment every stream still keeps, and the bound to start
    /// there. The writer's checkpoint history (`Feed::oldest_bound`) starts at this open,
    /// since it is rebuilt at each one, so without this `Oldest` after a reopen was `Now`.
    ///
    /// The bound is `K = m - 1`, `m` the smallest id in any stream's first record of the
    /// session: a single's seqno, a round's id, or a decision's id or apply seqno, the lower.
    /// One bound for every stream, so none starts past another's earliest commit.
    /// - **It bounds what is before the start positions.** Every record before the
    ///   session's first segment was logged before the session opened, so its key is at
    ///   most the allocator's start then. Every id in the session's records was allocated
    ///   in it, above that start: replay applies a round only whole and writes nothing, and
    ///   nothing re-logs a record. So `K` is at least the start (the rule `start_at` keeps
    ///   for the writer's history). One exception: an external share still in doubt when
    ///   the session opened (D329) is before its first segment, and its key, its Decision's
    ///   apply seqno, is allocated later. Such a session isn't started in (#2190, from
    ///   [`external_rounds`](Self::external_rounds)): a later one or the current history,
    ///   whose `start_at` starts before such a share (#2189), decides.
    /// - **It skips none of the session's commits.** A single's seqno is allocated in the
    ///   group that logs it, after that stream's first record. A cross-shard commit is keyed
    ///   by its apply seqno, reserved once every participant logged its PREPARE: after each
    ///   participant's first record. This assumes a participant reports PREPARED only after
    ///   its PREPARE was appended to its stream (the group settles after its WAL write); a
    ///   change that answers before the append breaks this bound. So every commit's key is
    ///   at least `m`. Taking only
    ///   singles' seqnos would skip that commit when a session opens with a cross-shard one
    ///   (simp724).
    ///
    /// A session's first segment is the one an open started (`prev_epoch` 0, or an epoch
    /// more than one above it: FORMAT §10.1 rule 2), so a session partly recycled at a floor
    /// is passed over, not started in its middle.
    ///
    /// `None` (the current history decides) when no earlier session qualifies or anything
    /// read fails: a later start only has a larger bound, so it is always safe (simp724).
    /// With the bound and the starts, the session they are in: a token taken before any
    /// record is read names it (audit120's P2-C6, perf2's remainder).
    fn earlier_start(&self, db_id: [u8; 16]) -> Option<(Seqno, Vec<Lsn>, u64)> {
        let shards = self.shared.shards;
        let open = |s: usize, at: Lsn| {
            let mut cursor = FeedCursor::open(
                &self.shared.vfs,
                &self.shared.path,
                StreamId(s as u32),
                db_id,
                at,
            )
            .ok()?;
            cursor.accept_silent(true);
            Some(cursor)
        };
        // Each stream's first retained segment of every earlier session in the history.
        let mut firsts: Vec<HashMap<u64, u32>> = Vec::with_capacity(shards);
        for s in 0..shards {
            let floor = self.floor(s);
            let mut f = HashMap::new();
            for (epoch, prev, session) in open(s, Lsn::default())?.segments() {
                let opened = prev == 0 || epoch != prev.wrapping_add(1);
                if opened
                    && epoch >= floor
                    && session != self.current
                    && self.history.holds(session)
                {
                    f.entry(session).or_insert(epoch);
                }
            }
            firsts.push(f);
        }
        let mut sessions: Vec<(u32, u64)> = firsts
            .first()?
            .iter()
            .filter(|(id, _)| firsts.iter().all(|f| f.contains_key(id)))
            .map(|(&id, &epoch)| (epoch, id))
            .collect();
        sessions.sort_unstable();
        if sessions.is_empty() {
            return None;
        }
        // #2190: every stream's external PREPAREs, and every Decision and abort, from its
        // floor: a session that opened with a share in doubt isn't started in (below).
        //
        // Only once the database has logged an external PREPARE: `incompat` bit 2 is set
        // before the first and never cleared (ICR 0140), so with it clear there is no share
        // to find, and every database without spanning transactions skips the read (audit120
        // on #2193). Read from disk too when the pager's copy lacks it (a reader process's
        // pager sees it only once it reloads); a failed read scans.
        let bit = pigeonhole_format::wal::INCOMPAT_EXTERNAL_PREPARE;
        let externals = self.shared.pager.features().incompat & bit != 0
            || self
                .shared
                .pager
                .features_on_disk()
                .map_or(true, |f| f.incompat & bit != 0);
        let rounds = if externals {
            self.external_rounds(&open, &firsts)?
        } else {
            ExternalRounds::default()
        };
        'sessions: for (_, session) in sessions {
            let mut first = None::<Seqno>;
            // The smallest seqno the session's first records were allocated in it: a round
            // with a smaller id was prepared before it.
            let mut alloc = Seqno::MAX;
            for (s, f) in firsts.iter().enumerate() {
                let Some(mut c) = open(s, Lsn::new(f[&session], 0)) else {
                    continue 'sessions;
                };
                let (id, own) = match c.step() {
                    // The stream logged nothing in the session: no id from it.
                    Ok(Step::End) => continue,
                    Ok(Step::Record {
                        session: of,
                        record,
                        ..
                    }) if of == session => match record {
                        WalRecord::Batch { seqno, .. }
                        | WalRecord::Prepare { seqno, .. }
                        | WalRecord::Commit { seqno, .. }
                        | WalRecord::ExternalPrepare { seqno, .. } => (seqno, seqno),
                        WalRecord::Decision {
                            seqno, apply_seqno, ..
                        } => (seqno.min(apply_seqno), apply_seqno),
                        WalRecord::ExternalAbort {
                            seqno, abort_seqno, ..
                        } => (seqno.min(abort_seqno), abort_seqno),
                        WalRecord::RangeDrop { seqno, .. } => (seqno, seqno),
                        // A kind this build can't key: the session can't be started in.
                        _ => continue 'sessions,
                    },
                    // The segment gone (recycled since the headers were read) or cut,
                    // another session's record there, or a read that failed: this session
                    // can't be started in (simp724's B1), so a later one or the history
                    // decides. Dropping only this stream would start it at a gone or foreign
                    // segment, or take `m` too high and skip its earliest commits.
                    _ => continue 'sessions,
                };
                first = Some(first.map_or(id, |f| f.min(id)));
                alloc = alloc.min(own);
            }
            // A session that logged nothing: the next one decides.
            let Some(first) = first else { continue };
            // #2190: a share in doubt when the session opened is before its first segments,
            // and its key, its Decision's apply seqno, was allocated after the session's
            // start: a start here would pass the share over, and its Decision would be a gap
            // or, read in a session before the current one, a round recovery dropped,
            // passed over silently. A later session or the current history (`start_at`,
            // which starts before such a share, #2189) decides instead: a later start, never
            // a skip. Such a share shows as a PREPARE kept before the session with no
            // Decision or abort before it, or, once retention recycled the PREPARE, as a
            // Decision or abort in the session or after for a round older than it.
            let before = |s: usize, epoch: u32| epoch < firsts[s][&session];
            let open_doubt = rounds.prepared.iter().any(|&(s, epoch, id)| {
                before(s, epoch)
                    && !rounds
                        .resolved
                        .iter()
                        .any(|&(c, at, r)| r == id && before(c, at))
            });
            let decided_after = rounds
                .resolved
                .iter()
                .any(|&(c, at, id)| !before(c, at) && id < alloc);
            if open_doubt || decided_after {
                continue;
            }
            let at = firsts.iter().map(|f| Lsn::new(f[&session], 0)).collect();
            return Some((first.saturating_sub(1), at, session));
        }
        None
    }

    /// [`earlier_start`](Self::earlier_start)'s scan (#2190): each stream's external PREPAREs
    /// (D329), and every Decision and external abort, from its floor to its end, each with
    /// its segment's epoch. `None` (the current history decides) when a stream can't be read
    /// through: a segment gone or cut, or a read that failed.
    ///
    /// A share recovery dropped as aborted (a participant's PREPARE torn) leaves no record,
    /// so it counts as in doubt: the start is then later than it could be, never past a
    /// share whose round commits after it. The read is of what an `Oldest` start reads
    /// anyway, and only when an earlier session could be started in.
    fn external_rounds(
        &self,
        open: &impl Fn(usize, Lsn) -> Option<FeedCursor>,
        firsts: &[HashMap<u64, u32>],
    ) -> Option<ExternalRounds> {
        let mut rounds = ExternalRounds::default();
        for s in 0..firsts.len() {
            let mut c = open(s, Lsn::new(self.floor(s), 0))?;
            loop {
                match c.step() {
                    Ok(Step::Record { end, record, .. }) => {
                        // The record's segment: `end` is in it (`start` may be in the one
                        // before, past a pad).
                        let epoch = end.epoch();
                        match record {
                            WalRecord::ExternalPrepare { seqno, .. } => {
                                rounds.prepared.push((s, epoch, seqno));
                            }
                            WalRecord::Decision { seqno, .. }
                            | WalRecord::ExternalAbort { seqno, .. } => {
                                rounds.resolved.push((s, epoch, seqno));
                            }
                            _ => {}
                        }
                    }
                    Ok(Step::End) => break,
                    _ => return None,
                }
            }
        }
        Some(rounds)
    }

    /// The token's session from now on: `session`'s, if it came later in the history than
    /// the one it names (#1810; by the chain's order, never by value). For a record, a
    /// catalog change delivered, and one passed over (audit120's P2-C5): each puts the
    /// position in its writer's session.
    fn raise_session(&mut self, session: u64) {
        if session != self.session && self.history.later(session, self.session) {
            self.session = session;
        }
    }

    /// Stream `s`'s floor: no segment of a smaller epoch is kept (the writer's shared-memory
    /// mark).
    fn floor(&self, s: usize) -> u32 {
        self.shm.feed_marks(s as u32).floor
    }
}

/// [`FeedReader::external_rounds`]'s scan (#2190).
#[derive(Default)]
struct ExternalRounds {
    /// Each external PREPARE: its stream, its segment's epoch, and its round's id.
    prepared: Vec<(usize, u32, Seqno)>,
    /// Each Decision or external abort: its stream, its segment's epoch, and its round's id.
    resolved: Vec<(usize, u32, Seqno)>,
}

/// A feed-silent batch a feed read (D331, ICR 0141): `Corruption` unless the database has
/// `incompat` bit 3, which a writer sets before appending the first (a stray bit is never
/// read as the mark); then nothing for a subscription, whose commit is passed over as one
/// to other tables is, and the batch's mutations for a reader that includes them. A reader
/// process's pager doesn't see the bit until it reloads, and setting it commits no
/// manifest, so the superblocks on disk are read again (simp724's S1 on #2055); a `true`
/// is kept in `silent_flagged`.
#[cold]
#[inline(never)]
fn silent_batch(
    shared: &Shared,
    batch: BatchRef<'_>,
    tables: &[TableId],
    prefix: &[u8],
    include_silent: bool,
) -> Result<Raws> {
    if !shared.silent_flagged.load(Ordering::Acquire) {
        let on_disk = shared
            .pager
            .features_on_disk()
            .is_ok_and(|f| f.incompat & pigeonhole_format::wal::INCOMPAT_FEED_SILENT != 0);
        if !on_disk {
            return Err(Error::Corruption(
                pigeonhole_format::wal::SILENT_WITHOUT_BIT.to_owned(),
            ));
        }
        shared.silent_flagged.store(true, Ordering::Release);
    }
    if include_silent {
        pack(batch, tables, prefix)
    } else {
        Ok(Raws::default())
    }
}

/// The record just read's xid into its note under `key` (D329 2b), out of line: only for a
/// spanning commit's share.
#[cold]
#[inline(never)]
fn take_read_xid(slot: &mut Option<Arc<[u8]>>, xids: &mut HashMap<Seqno, Arc<[u8]>>, key: Seqno) {
    if let Some(xid) = slot.take() {
        note_xid(xids, key, xid);
    }
}

/// The delivered commit's xid: its note under `key`, or none. Out of line: only once a
/// spanning commit's share has been read.
#[cold]
#[inline(never)]
fn set_out_xid(out: &mut Option<Arc<[u8]>>, xids: &mut HashMap<Seqno, Arc<[u8]>>, key: Seqno) {
    *out = xids.remove(&key);
}

/// The count's flags (`BatchRef::count_flags`): the silent mark and the xid trailer's bit.
const SILENT_FLAG: u8 = (pigeonhole_format::wal::SILENT_BATCH >> 24) as u8;
const XID_FLAG: u8 = (pigeonhole_format::wal::XID_BATCH >> 24) as u8;

/// Sets the reader's slot for the record just read to `batch`'s xid (D329 2b), out of
/// line: only a spanning commit's share has one.
#[cold]
#[inline(never)]
fn set_read_xid_of(slot: &mut Option<Arc<[u8]>>, batch: BatchRef<'_>) {
    if let Some(x) = batch.xid() {
        *slot = Some(Arc::from(x));
    }
}

/// Sets the reader's slot to an external share's xid (D329 2b), out of line.
#[cold]
#[inline(never)]
fn set_read_xid(slot: &mut Option<Arc<[u8]>>, xid: &[u8]) {
    *slot = Some(Arc::from(xid));
}

/// Notes `xid` under `key` for delivery. Every share of a round has the same xid, and a
/// round is never part tagged (feed1248's assert on 2b): a split that lost the tag on one
/// shard's share would show here.
#[cold]
#[inline(never)]
fn note_xid(xids: &mut HashMap<Seqno, Arc<[u8]>>, key: Seqno, xid: Arc<[u8]>) {
    let old = xids.insert(key, Arc::clone(&xid));
    debug_assert!(
        old.as_deref().is_none_or(|o| *o == *xid),
        "two shares of one round carry different xids"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_format::manifest::Edit;

    /// perf2's S1 on #1922: a skip index over its cap merges adjacent chunks of a segment
    /// pairwise and stays at the cap; each merged chunk's key is the lowest of the records it
    /// covers (so a jump over it is exact), it covers them contiguously, never across a
    /// segment; and at the cap with nothing to merge it stops growing until a trim.
    #[test]
    fn a_skip_index_over_its_cap_merges_exactly_within_segments() {
        const CAP: usize = 8;
        // 3 segments of 40 records, 100 bytes each, every record its own chunk (size 100);
        // keys falling and rising so a merge's minimum isn't its first record's.
        let mut index = SkipIndex::new(CAP);
        let mut records = Vec::new();
        let mut before = Lsn::new(1, 4096);
        for epoch in 1..=3u32 {
            for i in 0..40u32 {
                let start = Lsn::new(epoch, 4096 + i * 100);
                let end = Lsn::new(epoch, 4096 + (i + 1) * 100);
                let key = 1000 + u64::from(epoch) * 100 + u64::from((i * 7) % 40);
                index.note(before, start, end, key, u64::from(epoch), 100);
                records.push((start, end, key));
                before = end;
                assert!(index.chunks.len() <= CAP, "{} chunks", index.chunks.len());
            }
        }
        assert!(index.coarsenings > 0, "the cap bit");
        assert!(!index.full, "pairs were left to merge");
        let mut covered = 0;
        for (&key, c) in &index.chunks {
            assert_eq!(c.start.epoch(), c.end.epoch(), "a chunk crossed a segment");
            assert_eq!(u64::from(c.start.epoch()), c.session);
            assert!(
                key <= c.start,
                "the key is a cursor position before the chunk"
            );
            let inside: Vec<_> = records
                .iter()
                .filter(|(s, e, _)| *s >= c.start && *e <= c.end)
                .collect();
            assert!(!inside.is_empty());
            assert_eq!(
                inside.first().unwrap().0,
                c.start,
                "contiguous from its start"
            );
            assert_eq!(inside.last().unwrap().1, c.end, "contiguous to its end");
            covered += inside.len();
            let min = inside.iter().map(|(_, _, k)| *k).min().unwrap();
            assert_eq!(
                c.min_key, min,
                "a merged chunk's key is its records' lowest"
            );
            assert!(index.at(key).is_some() && index.at(c.start).is_some());
        }
        assert_eq!(covered + usize::from(index.open.is_some()), records.len());

        // Every chunk alone in its segment, more segments than the cap: full, then a trim
        // lets it grow again.
        let mut index = SkipIndex::new(2);
        let mut before = Lsn::new(1, 4096);
        for epoch in 1..=4u32 {
            let start = Lsn::new(epoch, 4096);
            let end = Lsn::new(epoch, 4196);
            index.note(before, start, end, u64::from(epoch), 0, 100);
            before = end;
        }
        assert!(index.full, "nothing left to merge");
        assert!(index.chunks.len() <= 2);
        index.trim(Lsn::new(2, 4096));
        assert!(!index.full);
        // A chunk keyed at segment 1's end but starting in segment 2 survives a trim to its
        // start, and is found there (perf2's N1).
        assert!(index.at(Lsn::new(2, 4096)).is_some());
    }

    /// A catalog with tables `ids` and the counters given (feed1248's S2 on #1870: `Derive`
    /// on hand-built catalogs).
    fn catalog(ids: &[u32], next_table: u32, next_family: u32) -> Arc<crate::catalog::Catalog> {
        let mut c = crate::catalog::Catalog::default();
        for &id in ids {
            c.apply(
                &Edit::CreateTable {
                    table: TableId(id),
                    name: format!("t{id}"),
                },
                1,
            )
            .unwrap();
        }
        c.counters.next_table = next_table;
        c.counters.next_family = next_family;
        Arc::new(c)
    }

    fn create(id: u32, family: u32) -> Vec<Edit> {
        vec![
            Edit::CreateTable {
                table: TableId(id),
                name: format!("t{id}"),
            },
            Edit::PutFamily {
                table: TableId(id),
                family: FamilyId(family),
                name: "f".into(),
                options: Default::default(),
            },
        ]
    }

    fn drop_table(id: u32) -> Vec<Edit> {
        vec![Edit::DropTable { table: TableId(id) }]
    }

    #[test]
    fn derive_puts_in_what_a_lagging_view_lacks_before_the_start() {
        // The start's catalog: tables 1 and 2, counters past both. The view lags table 2's
        // create, which is before the start.
        let start = catalog(&[1, 2], 3, 3);
        let mut d = Derive::new(catalog(&[1], 2, 2), None);
        d.before(&create(1, 1));
        d.before(&create(2, 2));
        assert_eq!(d.finish(None).unwrap(), CatalogModel::of(&start));
    }

    #[test]
    fn derive_leaves_out_a_create_and_drop_a_lagging_view_never_saw() {
        // S1: the view lags table 2's create and its drop, both before the start. The drop
        // isn't reflected by a view that never counted table 2.
        let start = catalog(&[1], 3, 3);
        let mut d = Derive::new(catalog(&[1], 2, 2), None);
        d.before(&create(2, 2));
        d.before(&drop_table(2));
        let model = d.finish(None).unwrap();
        assert_eq!(model.tables, CatalogModel::of(&start).tables);
        // With the start's claim, no false expiry.
        let mut d = Derive::new(catalog(&[1], 2, 2), None);
        d.before(&create(2, 2));
        d.before(&drop_table(2));
        assert_eq!(
            d.finish(Some(CatalogModel::of(&start))),
            Ok(CatalogModel::of(&start))
        );
    }

    #[test]
    fn derive_takes_out_a_change_after_the_start_the_view_reflects_in_order() {
        let at_start = catalog(&[1], 2, 2);
        // A pending change the view already reflects is taken out (the `Rooted::Pending`
        // arm's call).
        let mut d = Derive::new(catalog(&[1, 2], 3, 3), None);
        d.after(&create(2, 2));
        assert_eq!(d.finish(None).unwrap(), CatalogModel::of(&at_start));
        // The prefix stops at the first change the view doesn't reflect: a later one isn't
        // taken out, though on its own it looks reflected (table 3 counted and absent).
        let mut d = Derive::new(catalog(&[1], 4, 4), None);
        d.after(&create(5, 5));
        d.after(&drop_table(3));
        let model = d.finish(None).unwrap();
        assert_eq!(model.tables, CatalogModel::of(&at_start).tables);
    }

    #[test]
    fn derive_checks_a_claim() {
        let claim = CatalogModel::of(&catalog(&[1], 2, 2));
        // Equal, with a recorded create after the start the view reflects.
        let mut d = Derive::new(catalog(&[1, 2], 3, 3), None);
        d.after(&create(2, 2));
        assert_eq!(d.check(Some(claim)), Ok(claim));
        // An extra table no change recorded.
        let d = Derive::new(catalog(&[1, 2], 3, 3), None);
        assert!(d.check(Some(claim)).unwrap_err().contains("tables"));
        // A table counter no recorded change accounts for (a create and drop unrecorded).
        let d = Derive::new(catalog(&[1], 3, 2), None);
        assert!(
            d.check(Some(claim))
                .unwrap_err()
                .contains("table was created")
        );
        // A family counter, likewise (a family added unrecorded).
        let d = Derive::new(catalog(&[1], 2, 3), None);
        assert!(d.check(Some(claim)).unwrap_err().contains("family"));
    }

    /// `held`'s starts, counted as the reader counts them.
    fn starts_of(held: &BTreeMap<(Seqno, usize), Held>) -> Starts {
        let mut starts = Starts::default();
        for h in held.values() {
            starts.add(h.stream, h.start);
        }
        starts
    }

    fn held(stream: usize, epoch: u32) -> Held {
        Held {
            stream,
            start: Lsn::new(epoch, 64),
            end: Lsn::new(epoch, 128),
            changes: Raws::default(),
            outside: false,
            external: false,
        }
    }

    /// simp724's H1 on #1316: rule C drops an undecided share its checkpoint passed and
    /// remembers its round; one only the floor passed expires the feed, never a drop.
    #[test]
    fn a_share_only_the_floor_passed_expires_and_never_drops() {
        let decided = std::collections::HashSet::from([7]);
        // Stream 0's checkpoint is in epoch 5, stream 1's in epoch 3; the floors are 4 and 2.
        let checkpoints = [Lsn::new(5, 0), Lsn::new(3, 0)];
        let floors = [4, 2];
        let mut held_map = BTreeMap::from([
            // Undecided, passed on stream 0: dropped, and remembered.
            ((1, 0), held(0, 4)),
            // Undecided, not passed (epoch 5 at offset 64 is after the checkpoint): kept.
            ((2, 0), held(0, 5)),
            // Decided: kept whatever the marks say.
            ((7, 0), held(0, 1)),
            ((7, 1), held(1, 1)),
        ]);
        let mut dropped = HashMap::new();
        let mut starts = starts_of(&held_map);
        // Each share's read-ahead bytes (D320): four shares with no changes.
        let mut bytes = 4 * HELD_ENTRY;
        let e = drop_undecided(
            &mut held_map,
            &mut starts,
            &mut bytes,
            &mut dropped,
            &decided,
            &checkpoints,
            &floors,
        );
        assert!(e.is_none());
        assert!(!held_map.contains_key(&(1, 0)));
        assert!(held_map.contains_key(&(2, 0)) && held_map.contains_key(&(7, 1)));
        assert_eq!(dropped.get(&1), Some(&(0, 4)));
        assert_eq!(
            bytes,
            3 * HELD_ENTRY,
            "the dropped share's bytes went with it"
        );
        // The dropped share's start went with it: stream 0's earliest is the decided round's.
        assert_eq!(starts.0, starts_of(&held_map).0);
        assert_eq!(starts.first(0), Some(Lsn::new(1, 64)));

        // A share below its stream's floor that the checkpoint didn't pass: a bound broken,
        // so the feed expires and nothing is dropped.
        let mut held_map = BTreeMap::from([((3, 1), held(1, 1))]);
        let mut dropped = HashMap::new();
        let checkpoints = [Lsn::new(5, 0), Lsn::new(1, 0)];
        let mut starts = starts_of(&held_map);
        let mut bytes = HELD_ENTRY;
        let e = drop_undecided(
            &mut held_map,
            &mut starts,
            &mut bytes,
            &mut dropped,
            &decided,
            &checkpoints,
            &floors,
        );
        assert!(e.is_some_and(|e| e.reason.contains("retention passed a held")));
        assert!(held_map.contains_key(&(3, 1)) && dropped.is_empty());
        assert_eq!(bytes, HELD_ENTRY);

        // A remembered round is forgotten once the floor passes its segment.
        let mut dropped = HashMap::from([(1, (0, 3)), (2, (0, 4))]);
        let e = drop_undecided(
            &mut BTreeMap::new(),
            &mut Starts::default(),
            &mut 0,
            &mut dropped,
            &decided,
            &checkpoints,
            &floors,
        );
        assert!(e.is_none());
        assert_eq!(dropped.keys().copied().collect::<Vec<_>>(), [2]);
    }
}

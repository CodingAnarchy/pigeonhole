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

/// The token's format version.
const TOKEN_VERSION: u8 = 1;
/// A token from a feed of every table (D315; simp724's S1 on #1487). Only it resumes such a
/// feed: a feed of some tables' token sits past commits of the others it never followed, so
/// resuming every table from it would skip them. A build that doesn't know this version
/// refuses the token as not one (`InvalidArgument`), never misreading it.
const TOKEN_VERSION_ALL: u8 = 2;

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

/// A catalog change a feed of every table delivers (D315): one manifest commit's catalog
/// edits, from its `Catalog` sidecar entry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CatalogEvent {
    /// The commit's `seqno_ceiling`: every commit below it comes before the change, every
    /// commit at or above it after.
    pub seqno_ceiling: Seqno,
    /// The commit's `ts_floor`: every default timestamp before the change is at most this.
    pub timestamp: u64,
    /// The root commit's sequence: it orders catalog changes, and a token records the last
    /// one delivered.
    pub sequence: u64,
    /// Its edits, in the order they applied: `CreateTable`, `PutFamily`, `DropTable`.
    pub edits: Vec<pigeonhole_format::manifest::Edit>,
    /// The name of each table the edits name without a `CreateTable` among them, as the
    /// catalog held it before them: every table an event names is named in the event.
    pub names: Vec<(TableId, String)>,
}

/// A mutation as read, before its commit's timestamp is known.
#[derive(Debug, Clone)]
struct Raw {
    table: TableId,
    family: FamilyId,
    kind: Kind,
    ts: Option<Timestamp>,
    row: Vec<u8>,
    qualifier: Vec<u8>,
    value: Vec<u8>,
}

/// A record as read, owned.
enum Item {
    Single {
        seqno: Seqno,
        ts: Timestamp,
        changes: Vec<Raw>,
    },
    Prepare {
        id: Seqno,
        changes: Vec<Raw>,
    },
    Decision {
        id: Seqno,
        apply: Seqno,
        ts: Timestamp,
        participants: Vec<StreamId>,
    },
    Other,
}

/// A record read but not yet delivered, by key.
#[derive(Debug)]
enum Pending {
    Single {
        ts: Timestamp,
        changes: Vec<Raw>,
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

/// A PREPARE waiting for its Decision.
#[derive(Debug)]
struct Held {
    stream: usize,
    start: Lsn,
    end: Lsn,
    changes: Vec<Raw>,
    /// Read in a segment outside the feed's history (the mark rule): delivering it expires.
    outside: bool,
}

/// One stream's reading state.
#[derive(Debug)]
struct StreamRead {
    cursor: FeedCursor,
    /// Just past the last delivered (or skipped) record of this stream.
    delivered_end: Lsn,
    /// From a token: the delivered records reached here, so the stream must too.
    must_reach: Option<Lsn>,
}

/// The sessions whose segments the feed's history runs through (the mark rule).
#[derive(Debug)]
struct History {
    sessions: Vec<u64>,
}

impl History {
    fn holds(&self, session: u64) -> bool {
        session != 0 && self.sessions.contains(&session)
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
    /// Polls of any kind (an inspection count for tests, #1417).
    polls: u64,
    /// The newest session of any record read (the token's anchor).
    session: u64,
    ready: BTreeMap<Seqno, Pending>,
    held: HashMap<Seqno, Vec<Held>>,
    /// Rounds whose shares rule C dropped (no Decision read, the checkpoint passed): id →
    /// (stream, epoch) of a dropped share. A Decision for one later expires the feed rather
    /// than passing over its commit as recovery's all-or-nothing (simp724's H1 on #1316).
    /// An entry goes once the floor passes its segment.
    dropped: HashMap<Seqno, (usize, u32)>,
    out: Commit,
    /// A feed of every table (D315): the catalog changes read from the sidecar and not yet
    /// delivered, in order; the last one delivered (its root sequence, in the token); how far
    /// the sidecar is read; and the tables dropped by delivered changes, whose later changes
    /// are passed over (a commit routed before a drop can be logged after it).
    catalog: VecDeque<CatalogEvent>,
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
    out_catalog: CatalogEvent,
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

/// Whether a `Catalog` sidecar entry's root landed, as the entries after it say (D315
/// amendment): its Root entry follows (`Yes`), its intent follows or nothing does yet
/// (`Pending`: read it again next time), or something else follows (`Never`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Rooted {
    Yes,
    Pending,
    Never,
}

/// Which catalog changes are before a feed of every table's start, at open (D315), beside
/// the reader's `catalog_bound` for `Oldest` and `After`.
#[derive(Clone, Copy)]
enum CatalogStart<'a> {
    /// Those the start's catalog reflects: `Now` (the view's), `Snapshot` (its view's). Its
    /// view was loaded before the sidecar was read, so no later entry can be reflected.
    Reflected(&'a crate::catalog::Catalog),
}

/// Whether `catalog` reflects every one of a catalog change's `edits`. Ids only grow and are
/// never reused, and an id is counted only once its change is published: a created table or
/// family is reflected once the counter passed its id, a drop once the table is gone. That
/// holds for a prefix of the changes, checked in order.
fn reflects(
    catalog: &crate::catalog::Catalog,
    edits: &[pigeonhole_format::manifest::Edit],
) -> bool {
    use pigeonhole_format::manifest::Edit;
    edits.iter().all(|e| match e {
        Edit::CreateTable { table, .. } => table.0 < catalog.counters.next_table,
        Edit::PutFamily { family, .. } => family.0 < catalog.counters.next_family,
        Edit::DropTable { table } => catalog.table(*table).is_none(),
        _ => true,
    })
}

/// A reader process's first and longest waits between polls: it watches the writer's
/// watermarks in shared memory, so the writer does no per-commit work for it.
const READER_BACKOFF_MIN: Duration = Duration::from_micros(50);
const READER_BACKOFF_MAX: Duration = Duration::from_millis(5);
/// How long a reader process waits for a view that lists a commit's blob files before it
/// takes them as removed (the bound a reader snapshot waits for its view too).
const READER_BLOB_WAIT: Duration = Duration::from_secs(1);

/// What [`FeedReader::take_round`] found of a decided round.
enum Round {
    /// Every share, its changes in stream order.
    Whole(Vec<Raw>),
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
            polls: 0,
            session: 0,
            ready: BTreeMap::new(),
            held: HashMap::new(),
            dropped: HashMap::new(),
            out: Commit::default(),
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
            out_catalog: CatalogEvent::default(),
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
            (_, Some((bound, at))) => {
                this.delivered = bound;
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
            let mut cursor = FeedCursor::open(
                &this.shared.vfs,
                &this.shared.path,
                StreamId(s as u32),
                db_id,
                at,
            )?;
            // In the writer's process a stop in the head segment reads no segment headers
            // (D310 PR 5b's subscriber cost; a reader process has no marks to follow).
            if let Some(marks) = this.writer.as_ref().and_then(|f| f.marks(s)) {
                cursor.follow_head(Arc::clone(marks));
            }
            this.streams.push(StreamRead {
                cursor,
                delivered_end: at,
                must_reach,
            });
        }
        // A feed of every table: the catalog changes before its start are passed over, their
        // drops kept (D315). A token says the last one delivered; another start, its bound.
        if this.tables.is_empty() && this.expired.is_none() {
            let start = match &start_catalog {
                Some(catalog) => Some(CatalogStart::Reflected(catalog)),
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
            std::thread::park_timeout(park);
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
            waiters.register(self.key, target, Wake::Task(waker.clone()));
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
        self.held.values().map(Vec::len).sum()
    }

    /// The frontier as of the last poll.
    pub fn frontier(&self) -> Seqno {
        self.frontier
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
    /// delivered seqno.
    pub fn position(&self) -> FeedPosition {
        let mut seqno = self.read_through.unwrap_or(0);
        if let Some((&key, _)) = self.ready.first_key_value() {
            seqno = seqno.min(key.saturating_sub(1));
        }
        if self.tables.is_empty()
            && let Some(ev) = self.catalog.front()
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

    /// Where this feed is, to resume it with [`FeedStart::Token`]: just past the last
    /// commit [`FeedReader::try_next`] returned, never inside one (§2.5).
    pub fn token(&self) -> Vec<u8> {
        {
            use std::sync::atomic::AtomicU64 as A;
            static N: A = A::new(0);
            let held: usize = self.held.values().map(Vec::len).sum();
            if self.ready.len() + held > 1000 {
                let n = N.fetch_add(1, Ordering::Relaxed);
                if n % 512 == 0 {
                    eprintln!(
                        "DBG token: streams {} ready {} held {} (big call #{n})",
                        self.streams.len(),
                        self.ready.len(),
                        held
                    );
                }
            }
        }
        let mut t = Vec::with_capacity(1 + 16 + 4 + 8 + 8 + 16 * self.streams.len() + 16);
        t.push(if self.tables.is_empty() {
            TOKEN_VERSION_ALL
        } else {
            TOKEN_VERSION
        });
        t.extend_from_slice(&self.shared.pager.db_id());
        t.extend_from_slice(&(self.streams.len() as u32).to_le_bytes());
        t.extend_from_slice(&self.delivered.to_le_bytes());
        t.extend_from_slice(&self.session.to_le_bytes());
        for (s, st) in self.streams.iter().enumerate() {
            t.extend_from_slice(&self.hint(s).0.to_le_bytes());
            t.extend_from_slice(&st.delivered_end.0.to_le_bytes());
        }
        // A feed of every table: the last catalog change delivered, and its start's bound
        // (D315; simp724's F1b on #1515: a resume passes over what the start did).
        if self.tables.is_empty() {
            t.extend_from_slice(&self.catalog_seq.to_le_bytes());
            t.extend_from_slice(&self.catalog_bound.to_le_bytes());
        }
        t
    }

    /// Where stream `s` must be read again from to deliver everything after `delivered`:
    /// the earliest record of it read but not delivered (held or pending), else the
    /// cursor.
    fn hint(&self, s: usize) -> Lsn {
        let mut at = self.streams[s].cursor.position();
        for p in self.ready.values() {
            let (stream, start) = match p {
                Pending::Single { stream, start, .. } | Pending::Decided { stream, start, .. } => {
                    (*stream, *start)
                }
            };
            if stream == s {
                at = at.min(start);
            }
        }
        for h in self.held.values().flatten() {
            if h.stream == s {
                at = at.min(h.start);
            }
        }
        at
    }

    /// Decodes a token into each stream's start and must-reach position, or the reason it
    /// can't resume.
    #[allow(clippy::type_complexity)]
    fn decode_token(
        &mut self,
        bytes: &[u8],
        db_id: [u8; 16],
    ) -> Result<std::result::Result<Vec<(Lsn, Option<Lsn>)>, Expiry>> {
        let h = TokenHeader::parse(bytes)?;
        // A feed of every table resumes only from a token of one (simp724's S1 on #1487). A
        // feed of some tables may resume from an all-tables token: it follows a subset of
        // what that feed did, from the same position.
        if self.tables.is_empty() && !h.all_tables {
            return Err(Error::InvalidArgument(
                "a feed of every table can't resume from a token of a feed of some tables: \
                 the other tables' commits before it would be skipped"
                    .into(),
            ));
        }
        let u64_at = |b: &[u8], at: usize| u64::from_le_bytes(b[at..at + 8].try_into().unwrap());
        if h.db_id != db_id {
            return Err(Error::InvalidArgument(
                "the token is another database's".into(),
            ));
        }
        let (streams, delivered, session, per) = (h.streams, h.delivered, h.session, h.per);
        if h.all_tables {
            self.catalog_seq = h.catalog_seq;
            self.catalog_bound = h.catalog_bound;
        }
        self.delivered = delivered;
        self.session = session;
        if streams != self.shared.shards {
            return Ok(Err(expiry(format!(
                "the shard count changed ({streams} to {})",
                self.shared.shards
            ))));
        }
        if session != 0 && !self.history.holds(session) {
            return Ok(Err(expiry(
                "another session wrote without feeding these tables since the token",
            )));
        }
        let mut starts = Vec::with_capacity(streams);
        for s in 0..streams {
            let hint = Lsn(u64_at(per, s * 16));
            let reached = Lsn(u64_at(per, s * 16 + 8));
            if hint.epoch() != 0 && hint.epoch() < self.floor(s) {
                return Ok(Err(expiry("retention passed the token")));
            }
            starts.push((hint, (reached > hint).then_some(reached)));
        }
        Ok(Ok(starts))
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
        // A feed of every table in the writer's process, once the session's sidecar appends
        // stopped: no later catalog change can reach it, and the frozen catalog hold keeps
        // the frontier from moving, so this is checked before the frontier is (simp724 on
        // #1493).
        if self.tables.is_empty() && self.writer.as_ref().is_some_and(|f| f.appends_stopped()) {
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
        // Nothing committed since the streams were last read to their ends under this
        // frontier (it rises with every commit): by M1 every record at or below it was read
        // then, and `emit` above found none left to deliver. Reading again would only find
        // the same ends, which costs a stream's segment headers and a chunk of frames each
        // (#1448: an idle reader process spent 1-1.7 ms of CPU per poll there). Not while a
        // PREPARE is held: a checkpoint or floor can move without a commit (a flush), and
        // `drop_aborted` (rule C) must see it, or the commits behind an aborted round wait
        // for an unrelated one (feed1248's P1 on #1449). A writer restart since the stale
        // check above is seen here, as after the reads below.
        if self.read_through == Some(frontier) && self.held.is_empty() {
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
        Ok(self.pending_state())
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

    /// Polls of any kind since the reader opened (#1417).
    #[cfg(feature = "test-hooks")]
    pub fn polls(&self) -> u64 {
        self.polls
    }

    /// Reads stream `s` to its readable end. Returns `false` once the feed expired.
    fn read_stream(&mut self, s: usize) -> Result<bool> {
        loop {
            let before = self.streams[s].cursor.position();
            let Some((start, end, session, item)) = self.read_one(s)? else {
                return Ok(self.expired.is_none());
            };
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
            if inside {
                self.session = self.session.max(session);
            }
            match item {
                Item::Single { seqno, ts, changes } => {
                    if seqno <= self.delivered {
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
                    self.ready.insert(
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
                Item::Prepare { id, changes } => {
                    self.held.entry(id).or_default().push(Held {
                        stream: s,
                        start,
                        end,
                        changes,
                        outside: !inside,
                    });
                }
                Item::Decision {
                    id,
                    apply,
                    ts,
                    participants,
                } => {
                    if apply <= self.delivered {
                        self.held.remove(&id);
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
                    self.ready.insert(
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
        }
    }

    /// Stream `s`'s next record as an owned item, its mutations filtered to the feed's
    /// tables and prefix; `None` at the readable end, or once its segment is gone (which
    /// expires the feed).
    fn read_one(&mut self, s: usize) -> Result<Option<(Lsn, Lsn, u64, Item)>> {
        let (tables, prefix) = (&self.tables, &self.prefix);
        let filter = |batch: BatchRef<'_>| -> Result<Vec<Raw>> {
            let mut out = Vec::new();
            for m in batch.iter() {
                let m = m?;
                // The table first, then the row: a mutation outside is skipped by its
                // lengths, never copied (§2.7).
                // An empty list is every table (D315), but a replica's state table: a
                // commit only to it is passed over (D315 PR 3).
                let followed = if tables.is_empty() {
                    m.table != pigeonhole_format::manifest::REPLICA_TABLE
                } else {
                    tables.binary_search(&m.table).is_ok()
                };
                if !followed || !m.row.starts_with(prefix) {
                    continue;
                }
                out.push(Raw {
                    table: m.table,
                    family: m.family,
                    kind: m.kind,
                    ts: m.ts,
                    row: m.row.to_vec(),
                    qualifier: m.qualifier.to_vec(),
                    value: m.value.to_vec(),
                });
            }
            Ok(out)
        };
        let (start, end, session, record) = match self.streams[s].cursor.step()? {
            Step::Record {
                start,
                end,
                session,
                record,
            } => (start, end, session, record),
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
        let item = match record {
            WalRecord::Batch {
                seqno,
                commit_ts,
                batch,
            } => Item::Single {
                seqno,
                ts: commit_ts,
                changes: filter(batch)?,
            },
            WalRecord::Prepare { seqno, batch, .. } => Item::Prepare {
                id: seqno,
                changes: filter(batch)?,
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
        Ok(Some((start, end, session, item)))
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
        let shares = self.held.remove(&id).unwrap_or_default();
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
            return Round::Gap;
        }
        let mut raws = Vec::new();
        for h in shares {
            let st = &mut self.streams[h.stream];
            st.delivered_end = st.delivered_end.max(h.end);
            raws.extend(h.changes);
        }
        Round::Whole(raws)
    }

    /// Delivers the smallest pending commit at or below the frontier into `out`. Returns
    /// whether it did; a commit with nothing the feed follows is passed over.
    fn emit(&mut self) -> Result<Emitted> {
        loop {
            // A catalog change first, once every commit below its ceiling is delivered: the
            // frontier is past them (F + 1 >= the ceiling), and none of them is still ready.
            if let Some(ev) = self.catalog.front()
                && ev.seqno_ceiling <= self.frontier.saturating_add(1)
                && self
                    .ready
                    .first_key_value()
                    .is_none_or(|(&k, _)| k >= ev.seqno_ceiling)
            {
                let ev = self.catalog.pop_front().expect("checked");
                self.note_drops(&ev.edits);
                self.catalog_seq = ev.sequence;
                self.out_catalog = ev;
                return Ok(Emitted::Catalog);
            }
            let Some((&key, _)) = self.ready.first_key_value() else {
                return Ok(Emitted::Nothing);
            };
            if key > self.frontier {
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
                    (ts, changes)
                }
                Pending::Decided {
                    id,
                    ts,
                    participants,
                    stream,
                    end,
                    session,
                    ..
                } => {
                    let st = &mut self.streams[stream];
                    st.delivered_end = st.delivered_end.max(end);
                    match self.take_round(id, &participants, session) {
                        Round::Whole(raws) => (ts, raws),
                        Round::Dropped => {
                            self.delivered = key;
                            continue;
                        }
                        Round::Gap => {
                            self.expired = Some(gap(key));
                            return Ok(Emitted::Nothing);
                        }
                    }
                }
            };
            self.delivered = key;
            // A dropped table's changes, in a commit routed before its drop and logged after
            // it (a feed of every table, D315): the drop discarded them.
            let mut raws = raws;
            if !self.dropped_tables.is_empty() {
                raws.retain(|r| !self.dropped_tables.contains(&r.table));
            }
            if raws.is_empty() {
                continue;
            }
            self.out.seqno = key;
            self.out.ts = ts;
            self.out.changes.clear();
            for r in raws {
                let value = match blob_pointer(&r.value) {
                    None => r.value,
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
                    row: r.row,
                    qualifier: r.qualifier,
                    value,
                });
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
    held: &mut HashMap<Seqno, Vec<Held>>,
    dropped: &mut HashMap<Seqno, (usize, u32)>,
    decided: &std::collections::HashSet<Seqno>,
    checkpoints: &[Lsn],
    floors: &[u32],
) -> Option<Expiry> {
    let floor_of = |s: usize| floors.get(s).copied().unwrap_or(0);
    for (id, shares) in held.iter() {
        if decided.contains(id) {
            continue;
        }
        for h in shares {
            let passed = checkpoints.get(h.stream).is_some_and(|&c| c > h.start);
            if !passed && h.start.epoch() < floor_of(h.stream) {
                return Some(expiry(
                    "retention passed a held cross-shard share the checkpoint had not",
                ));
            }
        }
    }
    held.retain(|id, shares| {
        if decided.contains(id) {
            return true;
        }
        shares.retain(|h| {
            let passed = checkpoints.get(h.stream).is_some_and(|&c| c > h.start);
            if passed {
                dropped.insert(*id, (h.stream, h.start.epoch()));
            }
            !passed
        });
        !shares.is_empty()
    });
    dropped.retain(|_, &mut (s, epoch)| epoch >= floor_of(s));
    None
}

impl FeedReader {
    /// Whether `view` lists every blob file pending commit `p`'s values point into.
    fn blobs_listed(&self, view: &crate::snapshot::View, p: &Pending) -> bool {
        let listed = |raws: &[Raw]| {
            raws.iter().all(|r| {
                blob_pointer(&r.value)
                    .is_none_or(|ptr| view.ssts.blob_reader(ptr.blob_file).is_some())
            })
        };
        match p {
            Pending::Single { changes, .. } => listed(changes),
            Pending::Decided { id, .. } => self
                .held
                .get(id)
                .is_none_or(|shares| shares.iter().all(|h| listed(&h.changes))),
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
    ///   for the writer's history).
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
    fn earlier_start(&self, db_id: [u8; 16]) -> Option<(Seqno, Vec<Lsn>)> {
        let shards = self.shared.shards;
        let open = |s: usize, at: Lsn| {
            FeedCursor::open(
                &self.shared.vfs,
                &self.shared.path,
                StreamId(s as u32),
                db_id,
                at,
            )
            .ok()
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
        'sessions: for (_, session) in sessions {
            let mut first = None::<Seqno>;
            for (s, f) in firsts.iter().enumerate() {
                let Some(mut c) = open(s, Lsn::new(f[&session], 0)) else {
                    continue 'sessions;
                };
                let id = match c.step() {
                    // The stream logged nothing in the session: no id from it.
                    Ok(Step::End) => continue,
                    Ok(Step::Record {
                        session: of,
                        record,
                        ..
                    }) if of == session => match record {
                        WalRecord::Batch { seqno, .. }
                        | WalRecord::Prepare { seqno, .. }
                        | WalRecord::Commit { seqno, .. } => seqno,
                        WalRecord::Decision {
                            seqno, apply_seqno, ..
                        } => seqno.min(apply_seqno),
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
            }
            // A session that logged nothing: the next one decides.
            let Some(first) = first else { continue };
            let at = firsts.iter().map(|f| Lsn::new(f[&session], 0)).collect();
            return Some((first.saturating_sub(1), at));
        }
        None
    }

    /// A feed of every table (D315): reads the sidecar's new entries and queues the catalog
    /// changes of the feed's history not yet delivered. Called after the frontier F is
    /// loaded: the catalog hold keeps F below a change's ceiling until its entry is appended,
    /// so every change with a ceiling at most F + 1 is in the file by then. At open, `start`
    /// says which changes are before a non-token start (a token says the last one delivered
    /// instead). A passed-over or delivered change's drops are kept, so a dropped table's
    /// later changes are passed over.
    fn load_catalog(&mut self, start: Option<CatalogStart<'_>>) -> Result<()> {
        // Before the file is read: a change counted after this is loaded next time.
        if let Some(feed) = &self.writer {
            self.catalog_roots_seen = feed.catalog_roots();
        }
        // Opened once and kept: an idle reader process's poll then costs one length check
        // (simp724's N1 on #1539). The sidecar is only ever appended to, cut or overwritten in
        // place, never replaced by another file.
        let file = match &self.sidecar_file {
            Some(f) => Arc::clone(f),
            None => {
                let path = crate::feed::sidecar_path(&self.shared.path);
                match self.shared.vfs.open(&path, OpenOptions::read()) {
                    Ok(f) => {
                        self.sidecar_file = Some(Arc::clone(&f));
                        f
                    }
                    Err(e) if e.kind == pigeonhole_io::ErrorKind::NotFound => return Ok(()),
                    Err(e) => return Err(e.into()),
                }
            }
        };
        let len = file.len()?;
        let from = self.sidecar_pos.max(FeedHeader::LEN as u64);
        if len <= from {
            return Ok(());
        }
        let mut bytes = vec![0u8; (len - from) as usize];
        file.read_at(&mut bytes, from)?;
        if self.sidecar_pos == 0 {
            let mut head = [0u8; FeedHeader::LEN];
            file.read_at(&mut head, 0)?;
            match FeedHeader::decode(&head) {
                Ok(h) if h.db_id == self.shared.pager.db_id() => {}
                _ => return Ok(()),
            }
        }
        // Up to the first torn entry: the rest is read again once it's whole.
        let (entries, used) = decode_entries_at(&bytes);
        self.sidecar_pos = from + used as u64;
        // A catalog change counts once its root commit's Root entry follows it (D315
        // amendment): its entry is appended with the commit's intent, before the commit
        // writes, and a commit that then fails is cut off at the next open, so until the
        // Root is there, it may never have happened. Read from it again next time.
        // A Catalog entry with neither its Root nor its own intent after it can never get its
        // Root: its commit didn't land, and no open resolved it (simp724's F1 on #1526).
        // Waiting there would deliver later commits without the changes after it: expire.
        let rooted: Vec<Rooted> = (0..entries.len())
            .map(
                |i| match (&entries[i].1, entries.get(i + 1).map(|e| &e.1)) {
                    (
                        FeedEntry::Catalog {
                            session, sequence, ..
                        },
                        Some(FeedEntry::Root {
                            session: s,
                            sequence: q,
                        }),
                    ) if s == session && q == sequence => Rooted::Yes,
                    (
                        FeedEntry::Catalog { sequence, .. },
                        Some(FeedEntry::RootIntent { sequence: q, .. }),
                    ) if q == sequence => Rooted::Pending,
                    // The end of what's whole so far: its intent may still be on its way.
                    (FeedEntry::Catalog { .. }, None) => Rooted::Pending,
                    _ => Rooted::Never,
                },
            )
            .collect();
        // The start's catalog reflects a prefix of the changes, in order: once one isn't
        // reflected, no later one is.
        let mut reflecting = true;
        for (i, (at, e)) in entries.into_iter().enumerate() {
            let FeedEntry::Catalog {
                session,
                sequence,
                seqno_ceiling,
                timestamp,
                edits,
                names,
            } = e
            else {
                continue;
            };
            if !self.history.holds(session) {
                continue;
            }
            match rooted[i] {
                Rooted::Yes => {}
                Rooted::Pending => {
                    self.sidecar_pos = from + at as u64;
                    break;
                }
                Rooted::Never => {
                    self.expired = Some(expiry(format!(
                        "the catalog change of root {sequence} never got its Root entry, and \
                         a later entry follows it: later catalog changes can't be delivered"
                    )));
                    break;
                }
            }
            let before_start = seqno_ceiling <= self.catalog_bound
                || match start {
                    Some(CatalogStart::Reflected(catalog)) => {
                        reflecting = reflecting && reflects(catalog, &edits);
                        reflecting
                    }
                    None => sequence <= self.catalog_seq,
                };
            if before_start {
                self.note_drops(&edits);
                self.catalog_seq = self.catalog_seq.max(sequence);
                continue;
            }
            self.catalog.push_back(CatalogEvent {
                seqno_ceiling,
                timestamp,
                sequence,
                edits,
                names,
            });
        }
        Ok(())
    }

    /// The tables `edits` drop: their later changes are passed over.
    fn note_drops(&mut self, edits: &[pigeonhole_format::manifest::Edit]) {
        for e in edits {
            if let pigeonhole_format::manifest::Edit::DropTable { table } = e {
                self.dropped_tables.insert(*table);
            }
        }
    }

    /// Stream `s`'s floor: no segment of a smaller epoch is kept (the writer's shared-memory
    /// mark).
    fn floor(&self, s: usize) -> u32 {
        self.shm.feed_marks(s as u32).floor
    }
}

/// In a reader process: the writer's session and fed tables, from its entry in the sidecar
/// (the last session entry: a writer writes it before its shared memory exists).
/// `InvalidArgument` when the writer doesn't feed (its open recorded `fed: none`).
fn writer_session(shared: &Shared) -> Result<(u64, FeedTables)> {
    let off = || {
        Error::InvalidArgument(
            "the writer's change feed is off: subscribe to a writer that feeds".into(),
        )
    };
    let entries = match crate::feed::read_sidecar(&shared.vfs, &shared.path, shared.pager.db_id()) {
        Ok(Some(entries)) => entries,
        Ok(None) => return Err(off()),
        Err(Error::Io(e)) if e.kind == pigeonhole_io::ErrorKind::NotFound => return Err(off()),
        Err(e) => return Err(e),
    };
    for e in entries.iter().rev() {
        match e {
            FeedEntry::Session {
                session,
                streams,
                tables,
                ..
            } => {
                if *streams as usize != shared.shards {
                    // A new writer with another shard count is starting: this attachment is
                    // the old one's (simp724's nit). Subscribe again once it runs.
                    return Err(Error::SnapshotExpired);
                }
                return Ok((*session, tables.clone()));
            }
            FeedEntry::Unfed { .. } => return Err(off()),
            _ => {}
        }
    }
    Err(off())
}

/// The sessions whose segments hold the feed's history: the current one, and each one
/// before it while it fed every table in `tables`, had the same stream count, and chains.
/// A session chains when its `opened_at` is the previous session's last recorded
/// `sequence` and every sequence between was recorded: the previous session's Root
/// entries run contiguously from its own `opened_at` (simp724 on fix A: a lost middle
/// entry could hide another session's root commits, so a gap breaks the chain), with no
/// feed-off open between.
fn read_history(shared: &Shared, current: u64, tables: &[TableId]) -> Result<History> {
    let entries = crate::feed::read_sidecar(&shared.vfs, &shared.path, shared.pager.db_id())?
        .unwrap_or_default();
    Ok(History {
        sessions: chain(&entries, current, tables),
    })
}

/// [`read_history`]'s rule over the sidecar's entries.
fn chain(entries: &[FeedEntry], current: u64, tables: &[TableId]) -> Vec<u64> {
    let mut sessions: Vec<Link> = Vec::new();
    let mut last_seq: Option<u64> = None;
    let mut broken = true;
    for e in entries {
        match e {
            FeedEntry::Session {
                session,
                opened_at,
                streams,
                tables: fed,
                ..
            } => {
                // A feed of every table (an empty list, D315) needs a session that fed every
                // table: one with a list may have left out any table created since.
                let feeds = if tables.is_empty() {
                    matches!(fed, FeedTables::All)
                } else {
                    tables.iter().all(|&t| fed.contains(t))
                };
                let chained = !broken && last_seq == Some(*opened_at);
                sessions.push(Link {
                    session: *session,
                    feeds,
                    streams: *streams,
                    chained,
                });
                last_seq = Some(*opened_at);
                broken = false;
            }
            FeedEntry::Root { session, sequence } => {
                let ours = sessions.last().is_some_and(|s| s.session == *session);
                if !ours || last_seq.map(|l| l + 1) != Some(*sequence) {
                    broken = true;
                }
                last_seq = Some(*sequence);
            }
            FeedEntry::Unfed { .. } => broken = true,
            // A catalog change (D315) is appended just before its root commit's Root entry,
            // which carries the chain: one of another session breaks it.
            FeedEntry::Catalog { session, .. } => {
                if sessions.last().is_none_or(|s| s.session != *session) {
                    broken = true;
                }
            }
            // A root commit's intent (D315 amendment) is overwritten by its Root entry once
            // the commit is durable, and an open resolves one a crash left at the end. One
            // still here is either the live session's commit in progress, after everything
            // the chain reads, or an older one no open could decide: fail closed.
            FeedEntry::RootIntent { .. } => broken = true,
            // A newer build's entry may mean a break: fail closed (simp724's nit on #1316).
            FeedEntry::Unknown(_) | _ => broken = true,
        }
    }
    let Some(pos) = sessions.iter().rposition(|s| s.session == current) else {
        return vec![current];
    };
    let streams = sessions[pos].streams;
    let mut run = Vec::new();
    for (i, s) in sessions[..=pos].iter().enumerate().rev() {
        if !s.feeds || s.streams != streams {
            break;
        }
        run.push(s.session);
        if !s.chained || i == 0 {
            break;
        }
    }
    run
}

/// One session in [`chain`]'s walk of the sidecar.
struct Link {
    session: u64,
    /// It fed every table the feed follows.
    feeds: bool,
    streams: u32,
    /// It chains to the session before it.
    chained: bool,
}

/// A token's header, as encoded by [`FeedReader::token`]: the one decoder of its layout.
struct TokenHeader<'a> {
    all_tables: bool,
    db_id: [u8; 16],
    streams: usize,
    delivered: u64,
    session: u64,
    /// Each stream's position (16 bytes each), checked to be `streams` of them.
    per: &'a [u8],
    /// A feed of every table's last catalog change delivered, and its start's bound (D315).
    catalog_seq: u64,
    catalog_bound: u64,
}

impl<'a> TokenHeader<'a> {
    fn parse(bytes: &'a [u8]) -> Result<Self> {
        let bad = || Error::InvalidArgument("not a change feed token".into());
        let (&version, rest) = bytes.split_first().ok_or_else(bad)?;
        if !matches!(version, TOKEN_VERSION | TOKEN_VERSION_ALL) || rest.len() < 16 + 4 + 8 + 8 {
            return Err(bad());
        }
        let u64_at = |b: &[u8], at: usize| u64::from_le_bytes(b[at..at + 8].try_into().unwrap());
        let all_tables = version == TOKEN_VERSION_ALL;
        let streams = u32::from_le_bytes(rest[16..20].try_into().unwrap()) as usize;
        let mut per = &rest[36..];
        let (mut catalog_seq, mut catalog_bound) = (0, 0);
        if all_tables {
            // The last catalog change delivered and the start's bound, after the streams'
            // positions (D315).
            let Some(split) = per.len().checked_sub(16) else {
                return Err(bad());
            };
            catalog_seq = u64_at(per, split);
            catalog_bound = u64_at(per, split + 8);
            per = &per[..split];
        }
        if per.len() != streams * 16 {
            return Err(bad());
        }
        Ok(Self {
            all_tables,
            db_id: rest[..16].try_into().unwrap(),
            streams,
            delivered: u64_at(rest, 20),
            session: u64_at(rest, 28),
            per,
            catalog_seq,
            catalog_bound,
        })
    }
}

/// Where a token of a feed of every table stands (#1436, ICR 0125): its database, the last
/// seqno delivered (or passed over), and the last catalog change delivered. A replica stores
/// this as where its bootstrap copy starts. `InvalidArgument` for anything but such a token.
pub fn all_tables_token_position(token: &[u8]) -> Result<([u8; 16], u64, u64)> {
    let h = TokenHeader::parse(token)?;
    if !h.all_tables {
        return Err(Error::InvalidArgument(
            "not a token of a feed of every table".into(),
        ));
    }
    Ok((h.db_id, h.delivered, h.catalog_seq))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(session: u64, opened_at: u64) -> FeedEntry {
        FeedEntry::Session {
            session,
            opened_at,
            retention_bytes: 1 << 20,
            retention_age_secs: 0,
            streams: 2,
            tables: FeedTables::All,
        }
    }

    fn root(session: u64, sequence: u64) -> FeedEntry {
        FeedEntry::Root { session, sequence }
    }

    #[test]
    fn sessions_chain_only_through_contiguous_sequences() {
        let t = [TableId(1)];
        let whole = [
            session(1, 1),
            root(1, 2),
            root(1, 3),
            root(1, 4),
            session(2, 4),
            root(2, 5),
        ];
        assert_eq!(chain(&whole, 2, &t), [2, 1]);
        // A lost middle entry: the last still matches the next open, but the root commit at
        // 3 is unaccounted for, so session 1's history ends there (simp724 on fix A).
        let gap = [
            session(1, 1),
            root(1, 2),
            root(1, 4),
            session(2, 4),
            root(2, 5),
        ];
        assert_eq!(chain(&gap, 2, &t), [2]);
        // A lost last entry: the next open is ahead.
        let ahead = [session(1, 1), root(1, 2), session(2, 3)];
        assert_eq!(chain(&ahead, 2, &t), [2]);
        // A feed-off open between.
        let unfed = [
            session(1, 1),
            root(1, 2),
            FeedEntry::Unfed { sequence: 2 },
            session(2, 2),
        ];
        assert_eq!(chain(&unfed, 2, &t), [2]);
        // A session with another stream count, or not feeding the table, ends it too.
        let narrower = [
            FeedEntry::Session {
                session: 1,
                opened_at: 1,
                retention_bytes: 1 << 20,
                retention_age_secs: 0,
                streams: 2,
                tables: FeedTables::Only(vec![TableId(9)]),
            },
            session(2, 1),
        ];
        assert_eq!(chain(&narrower, 2, &t), [2]);
        // An entry this build doesn't know may be a break: it ends the chain too.
        let unknown = [
            session(1, 1),
            root(1, 2),
            FeedEntry::Unknown(0xee),
            session(2, 2),
        ];
        assert_eq!(chain(&unknown, 2, &t), [2]);
    }

    fn held(stream: usize, epoch: u32) -> Held {
        Held {
            stream,
            start: Lsn::new(epoch, 64),
            end: Lsn::new(epoch, 128),
            changes: Vec::new(),
            outside: false,
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
        let mut held_map = HashMap::from([
            // Undecided, passed on stream 0: dropped, and remembered.
            (1, vec![held(0, 4)]),
            // Undecided, not passed (epoch 5 at offset 64 is after the checkpoint): kept.
            (2, vec![held(0, 5)]),
            // Decided: kept whatever the marks say.
            (7, vec![held(0, 1), held(1, 1)]),
        ]);
        let mut dropped = HashMap::new();
        let e = drop_undecided(&mut held_map, &mut dropped, &decided, &checkpoints, &floors);
        assert!(e.is_none());
        assert!(!held_map.contains_key(&1));
        assert!(held_map.contains_key(&2) && held_map.contains_key(&7));
        assert_eq!(dropped.get(&1), Some(&(0, 4)));

        // A share below its stream's floor that the checkpoint didn't pass: a bound broken,
        // so the feed expires and nothing is dropped.
        let mut held_map = HashMap::from([(3, vec![held(1, 1)])]);
        let mut dropped = HashMap::new();
        let checkpoints = [Lsn::new(5, 0), Lsn::new(1, 0)];
        let e = drop_undecided(&mut held_map, &mut dropped, &decided, &checkpoints, &floors);
        assert!(e.is_some_and(|e| e.reason.contains("retention passed a held")));
        assert!(held_map.contains_key(&3) && dropped.is_empty());

        // A remembered round is forgotten once the floor passes its segment.
        let mut dropped = HashMap::from([(1, (0, 3)), (2, (0, 4))]);
        let e = drop_undecided(
            &mut HashMap::new(),
            &mut dropped,
            &decided,
            &checkpoints,
            &floors,
        );
        assert!(e.is_none());
        assert_eq!(dropped.keys().copied().collect::<Vec<_>>(), [2]);
    }
}

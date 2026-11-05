//! The change feed (D310): the committed changes of some tables, in commit order, from the
//! writer, resumable by token.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pigeonhole_engine::{CatalogEvent, FamilyId, FeedReader, FeedSpec, FeedStart, Next, TableId};
use pigeonhole_format::key::Kind;
use pigeonhole_format::manifest::Edit;
use pigeonhole_format::value::decode_value;

use crate::db::{Db, Snapshot};
use crate::table::TableCore;
use crate::{Error, ErrorCode, FamilyInfo, ReadTable, Result, Table, Value};

/// Table and family names by id, as the feed's changes carry them.
type Names = HashMap<(TableId, FamilyId), (Arc<str>, Arc<str>)>;

/// What a [`Change`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ChangeKind {
    /// A value for one cell version ([`RowMutation::put`](crate::RowMutation::put) and its
    /// typed forms).
    Put,
    /// A merge operand, or an aggregate family's input
    /// ([`RowMutation::merge`](crate::RowMutation::merge), `incr`, `aggregate`).
    Merge,
    /// Deleted exactly the version at [`Change::timestamp`]
    /// ([`RowMutation::delete_cell`](crate::RowMutation::delete_cell)).
    DeleteCell,
    /// Deleted every version of the column at or below [`Change::timestamp`]
    /// ([`RowMutation::delete_column`](crate::RowMutation::delete_column)).
    DeleteColumn,
    /// Deleted every cell of the row in the family at or below [`Change::timestamp`]
    /// ([`RowMutation::delete_family`](crate::RowMutation::delete_family); a
    /// [`delete_row`](crate::RowMutation::delete_row) is one per family).
    DeleteFamily,
}

impl ChangeKind {
    fn of(kind: Kind) -> Self {
        match kind {
            Kind::Put => ChangeKind::Put,
            Kind::Merge => ChangeKind::Merge,
            Kind::CellDelete => ChangeKind::DeleteCell,
            Kind::ColumnDelete => ChangeKind::DeleteColumn,
            Kind::FamilyDelete => ChangeKind::DeleteFamily,
        }
    }

    /// Whether it is one of the deletes (it has no value).
    pub fn is_delete(self) -> bool {
        matches!(
            self,
            ChangeKind::DeleteCell | ChangeKind::DeleteColumn | ChangeKind::DeleteFamily
        )
    }
}

/// One mutation of a delivered [`Commit`].
#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    table: Arc<str>,
    family: Arc<str>,
    kind: ChangeKind,
    ts: u64,
    row: Vec<u8>,
    qualifier: Vec<u8>,
    /// The stored value (its tag byte first); empty for deletes.
    stored: Vec<u8>,
}

impl Change {
    /// The table's name. Empty only for a family created after the feed opened whose table
    /// was dropped before the feed reached this change: its name went with the table.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The family's name (empty in the same case as [`table`](Self::table)).
    pub fn family(&self) -> &str {
        &self.family
    }

    /// What it did.
    pub fn kind(&self) -> ChangeKind {
        self.kind
    }

    /// The row key.
    pub fn row(&self) -> &[u8] {
        &self.row
    }

    /// The qualifier (empty for [`ChangeKind::DeleteFamily`]).
    pub fn qualifier(&self) -> &[u8] {
        &self.qualifier
    }

    /// Its timestamp: the one the write gave, or else its commit's.
    pub fn timestamp(&self) -> u64 {
        self.ts
    }

    /// The value written or merged, as the read API types it; `None` for the deletes.
    pub fn value(&self) -> Option<Value<'_>> {
        if self.kind.is_delete() {
            return None;
        }
        // Checked when the commit was delivered (`fill`): a value that doesn't decode fails
        // `next` with `Corruption` instead of reaching here.
        decode_value(&self.stored)
            .ok()
            .map(crate::cell::typed_value)
    }
}

/// One whole commit: every change of it in the feed's tables and row prefix, never part of
/// one. Commits arrive once each, in commit order ([`Commit::seqno`]).
#[derive(Debug, Clone, PartialEq)]
pub struct Commit {
    seqno: u64,
    ts: u64,
    changes: Vec<Change>,
}

impl Commit {
    /// Its sequence number: the order commits arrive in, and what a [`Snapshot::seqno`]
    /// counts.
    pub fn seqno(&self) -> u64 {
        self.seqno
    }

    /// Its commit timestamp.
    pub fn timestamp(&self) -> u64 {
        self.ts
    }

    /// Its changes the feed follows, in the order the commit wrote them.
    pub fn changes(&self) -> &[Change] {
        &self.changes
    }
}

impl Commit {
    /// A commit with nothing in it yet: the buffer the next one is read into.
    fn empty() -> Self {
        Commit {
            seqno: 0,
            ts: 0,
            changes: Vec::new(),
        }
    }
}

/// What a feed delivers ([`Feed::next_event`]): a commit, or in a feed of every table
/// ([`Pigeonhole::feed`](crate::Pigeonhole::feed)) a change to its tables too (D315), each
/// in its place in commit order.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum FeedEvent {
    /// One whole commit, as [`Feed::next`] delivers it.
    Commit(Commit),
    /// Tables created or dropped, or families added: every commit before it saw the old
    /// tables, every commit after it the new ones.
    Catalog(CatalogChange),
}

/// One change to the database's tables, as a feed of every table delivers it
/// ([`FeedEvent::Catalog`]): one or more edits that applied together.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogChange {
    seqno: u64,
    ts: u64,
    edits: Vec<CatalogEdit>,
}

impl CatalogChange {
    /// Where it falls in commit order: every commit with a lower [`Commit::seqno`] came
    /// before it, every commit at or above it comes after.
    pub fn seqno(&self) -> u64 {
        self.seqno
    }

    /// The timestamp floor when it applied: every commit before it that took the default
    /// timestamp has one at most this.
    pub fn timestamp(&self) -> u64 {
        self.ts
    }

    /// Its edits, in the order they applied.
    pub fn edits(&self) -> &[CatalogEdit] {
        &self.edits
    }
}

/// One edit of a [`CatalogChange`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum CatalogEdit {
    /// A table was created, with these families.
    #[non_exhaustive]
    TableCreated {
        /// Its name.
        table: String,
        /// Its families, with their options.
        families: Vec<FamilyInfo>,
    },
    /// A family was added to an existing table.
    #[non_exhaustive]
    FamilyAdded {
        /// The table's name.
        table: String,
        /// The family, with its options.
        family: FamilyInfo,
    },
    /// A table was dropped, with all its data.
    #[non_exhaustive]
    TableDropped {
        /// Its name.
        table: String,
    },
}

/// Where a [`Feed`] left off: [`FeedBuilder::resume`] continues after its last delivered
/// commit, in this or a later open of the database. Opaque bytes, to store with the work
/// the commits fed (`as_bytes`, `from_bytes`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FeedToken(Vec<u8>);

impl FeedToken {
    /// The token's bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// A token from bytes [`as_bytes`](Self::as_bytes) gave. Checked when a feed resumes
    /// from it: bytes from another database or not a token are `InvalidArgument`.
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        FeedToken(bytes.into())
    }
}

/// Builds a [`Feed`] ([`Table::feed`], or [`ReadTable::feed`] in a reader process).
#[derive(Debug)]
#[must_use = "a builder does nothing until `open`"]
pub struct FeedBuilder {
    db: Arc<Db>,
    tables: Vec<Arc<TableCore>>,
    /// Every table, including those created later ([`crate::Pigeonhole::feed`], D315).
    all: bool,
    prefix: Vec<u8>,
    durable: bool,
    start: FeedStart,
    /// The first refusal (a handle from another database), returned by `open`.
    error: Option<Error>,
}

impl FeedBuilder {
    pub(crate) fn new(core: &Arc<TableCore>) -> Self {
        FeedBuilder {
            db: Arc::clone(&core.db),
            tables: vec![Arc::clone(core)],
            all: false,
            prefix: Vec::new(),
            durable: true,
            start: FeedStart::Now,
            error: None,
        }
    }

    /// A feed of every table of `db` (D315, ICR 0114).
    pub(crate) fn all_tables(db: &Arc<Db>) -> Self {
        FeedBuilder {
            db: Arc::clone(db),
            tables: Vec::new(),
            all: true,
            prefix: Vec::new(),
            durable: true,
            start: FeedStart::Now,
            error: None,
        }
    }

    /// Follows `table` as well: one feed, the commits of all its tables in one order, each
    /// commit whole across them.
    pub fn table(self, table: &Table) -> Self {
        self.add(&table.core)
    }

    /// Follows `table` as well, in a reader process ([`table`](Self::table) for a
    /// [`ReadTable`]).
    pub fn read_table(self, table: &ReadTable) -> Self {
        self.add(&table.core)
    }

    fn add(mut self, core: &Arc<TableCore>) -> Self {
        if self.all {
            self.error.get_or_insert(Error::new(
                ErrorCode::InvalidArgument,
                format!(
                    "a feed of every table already follows table {:?}",
                    core.info.name
                ),
            ));
            return self;
        }
        if !Arc::ptr_eq(&self.db, &core.db) {
            self.error.get_or_insert(Error::new(
                ErrorCode::InvalidArgument,
                format!("table {:?} belongs to another database", core.info.name),
            ));
            return self;
        }
        if !self.tables.iter().any(|t| t.info.id == core.info.id) {
            self.tables.push(Arc::clone(core));
        }
        self
    }

    /// Only the changes to rows starting with `prefix`. A commit with none is passed over.
    pub fn prefix(mut self, prefix: &[u8]) -> Self {
        self.prefix = prefix.to_vec();
        self
    }

    /// Delivers a commit once it is visible to reads, not only once it is durable at its
    /// level (the default). Earlier, by up to a sync; but a power loss can undo a
    /// `Buffered` or `None` commit it delivered, and then the feed's next resume expires.
    pub fn visible(mut self) -> Self {
        self.durable = false;
        self
    }

    /// Starts after the commits `snapshot` sees: read the snapshot, then follow the feed,
    /// and nothing is missed or seen twice.
    pub fn from_snapshot(mut self, snapshot: &Snapshot) -> Self {
        if let Err(e) = self.db.check_snapshot(snapshot) {
            self.error.get_or_insert(e);
            return self;
        }
        // The snapshot itself, so a feed of every table knows which catalog changes it saw.
        self.start = FeedStart::Snapshot(snapshot.inner.clone());
        self
    }

    /// Starts with the oldest commit the database's feed history still holds
    /// ([`Options::change_feed`](crate::Options::change_feed)), across reopens too: from the
    /// oldest earlier open whose history every shard still keeps whole. Under load that
    /// history can be recycled before the feed reaches it, which expires the feed at once;
    /// [`from_snapshot`](Self::from_snapshot) is the way to start from a known state.
    pub fn from_oldest(mut self) -> Self {
        self.start = FeedStart::Oldest;
        self
    }

    /// Continues after the last commit a feed delivered before `token` was taken.
    pub fn resume(mut self, token: &FeedToken) -> Self {
        self.start = FeedStart::Token(token.0.clone());
        self
    }

    /// Subscribes. With none of `from_snapshot`, `from_oldest` and `resume`, the feed starts
    /// with the commits after this call; with several, the last one called wins.
    ///
    /// A table or snapshot from another database is `InvalidArgument`, as everywhere else,
    /// and a snapshot from before an automatic recovery is `SnapshotExpired`, as reading it
    /// would be.
    ///
    /// Fails with `InvalidArgument` if the database's change feed is off or doesn't follow
    /// one of the tables ([`Options::feed_tables`](crate::Options::feed_tables)), or if a
    /// token is not one of this database's. A feed of every table
    /// ([`Pigeonhole::feed`](crate::Pigeonhole::feed)) needs the change feed on for every
    /// table, with no `feed_tables` list. A start whose history is gone shows at the first
    /// [`Feed::next`], as `FeedExpired`.
    pub fn open(self) -> Result<Feed> {
        if let Some(e) = self.error {
            return Err(e);
        }
        let spec = FeedSpec {
            tables: self.tables.iter().map(|t| t.info.id).collect(),
            prefix: self.prefix,
            durable: self.durable,
            start: self.start,
        };
        let mut names = HashMap::new();
        for t in &self.tables {
            note_names(&mut names, &t.info);
        }
        let (reader, generation) = subscribe(&self.db, spec.clone())?;
        Ok(Feed {
            db: self.db,
            // Only ever resubscribed from its own token: the start isn't kept, so a snapshot
            // start doesn't stay pinned for the feed's life.
            spec: FeedSpec {
                start: FeedStart::Now,
                ..spec
            },
            reader,
            generation,
            out: Delivery {
                names,
                commit: Commit::empty(),
                event: None,
            },
            oldest_retained: None,
            failed: None,
        })
    }
}

/// The table and family names of `info`, by id.
fn note_names(names: &mut Names, info: &pigeonhole_engine::TableInfo) {
    let table: Arc<str> = Arc::from(info.name.as_str());
    for f in &info.families {
        names
            .entry((info.id, f.id))
            .or_insert_with(|| (Arc::clone(&table), Arc::from(f.name.as_str())));
    }
}

/// The engine's reader for `spec` on the current engine, and that engine's generation.
fn subscribe(db: &Db, spec: FeedSpec) -> Result<(FeedReader, u64)> {
    loop {
        let generation = db.generation();
        let reader = db.engine().feed(spec.clone())?;
        // A recovery's swap between the two loads: subscribe again on the new engine.
        if db.generation() == generation {
            return Ok((reader, generation));
        }
    }
}

/// A subscription to the committed changes of some tables ([`Table::feed`]): whole commits,
/// once each, in commit order, from the writer.
///
/// By default a commit arrives once it is durable at its level, so nothing delivered is
/// ever undone ([`FeedBuilder::visible`] for sooner). Take a [`token`](Self::token) after
/// handling a commit to continue there later, in this open or the next.
///
/// When the history a feed needs is gone, it expires: [`next`](Self::next) fails with
/// [`ErrorCode::FeedExpired`] from then on. That happens when retention
/// ([`Options::change_feed`](crate::Options::change_feed)) passed it, when the database
/// was opened since with the feed turned off, without these tables, or with another
/// explicit shard count (an open that mentions neither inherits them and keeps it), or when
/// a power loss took commits it delivered. Start again from a snapshot.
///
/// ```
/// use std::time::Duration;
/// use pigeonhole::{Family, FeedRetention, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let options = Options::default().change_feed(FeedRetention::bytes(64 << 20));
/// let db = Pigeonhole::open(dir.join("app.phdb"), options)?;
/// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// let mut feed = t.feed().open()?;
/// t.mutate(b"row").put("f", b"q", b"v").commit()?;
/// let commit = feed.next(Duration::from_secs(5))?.expect("the commit");
/// let change = &commit.changes()[0];
/// assert_eq!((change.row(), change.qualifier()), (&b"row"[..], &b"q"[..]));
/// let token = feed.token(); // store it with what the commit fed
/// drop(feed);
/// let mut feed = t.feed().resume(&token).open()?;
/// assert!(feed.try_next()?.is_none());
/// # drop(feed);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Feed {
    db: Arc<Db>,
    spec: FeedSpec,
    reader: FeedReader,
    /// The engine generation `reader` reads (D265).
    generation: u64,
    /// What the last call delivered, and the names that fill it.
    out: Delivery,
    oldest_retained: Option<u64>,
    /// The error that ended the feed, returned by every later call (simp724 on #1321): the
    /// reader had already moved past the commit it was delivering, so going on would skip
    /// it.
    failed: Option<Error>,
}

/// The longest single wait handed to the engine; a longer `next` waits again.
const WAIT_SLICE: Duration = Duration::from_secs(3600);

impl Feed {
    /// The next commit, waiting up to `timeout` for one; `None` if none arrived in time. A
    /// feed of every table passes its catalog changes over ([`next_event`](Self::next_event)
    /// delivers them too).
    ///
    /// An error reading the feed ends it: every later call returns the same error, as an
    /// expired feed does, so no commit is ever skipped. A value that doesn't decode is
    /// `Corruption`; the feed's [`token`](Self::token) then points past that commit, so
    /// resuming from it skips the damaged one.
    pub fn next(&mut self, timeout: Duration) -> Result<Option<&Commit>> {
        Ok(self.wait(timeout, false)?.then_some(&self.out.commit))
    }

    /// The next commit if one is ready; `None` otherwise. Never waits. Errors end the feed
    /// as [`next`](Self::next)'s do.
    pub fn try_next(&mut self) -> Result<Option<&Commit>> {
        Ok(self.try_step(false)?.then_some(&self.out.commit))
    }

    /// The next commit, for an async caller (D310 PR 5a): `Poll::Ready` with it, or with an
    /// error as [`next`](Self::next) would end with (they end the feed the same way), and
    /// `Poll::Pending` until there is one, with `cx`'s waker registered so the task is woken,
    /// never spinning, when the frontier can move. A feed doesn't end on its own, so there is
    /// no `None`. It follows an automatic recovery as `next` does.
    ///
    /// The writer process only: in a reader process it is
    /// [`ErrorCode::Unsupported`]; call the blocking [`next`](Self::next) on a thread of its
    /// own (#1407). [`nonblocking::FeedStream`](crate::nonblocking::FeedStream) wraps this
    /// as a `Stream` of owned commits.
    pub fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<&Commit>> {
        self.poll_step(cx, false).map_ok(|()| &self.out.commit)
    }

    /// The next event, waiting up to `timeout` for one; `None` if none arrived in time: a
    /// commit, or in a feed of every table ([`Pigeonhole::feed`](crate::Pigeonhole::feed))
    /// a catalog change too (D315), each in its place in commit order. Errors end the feed as
    /// [`next`](Self::next)'s do, and the [`token`](Self::token) after an event resumes just
    /// past it.
    pub fn next_event(&mut self, timeout: Duration) -> Result<Option<&FeedEvent>> {
        Ok(if self.wait(timeout, true)? {
            self.out.event.as_ref()
        } else {
            None
        })
    }

    /// The next event if one is ready; `None` otherwise. Never waits. As
    /// [`next_event`](Self::next_event) otherwise.
    pub fn try_next_event(&mut self) -> Result<Option<&FeedEvent>> {
        Ok(if self.try_step(true)? {
            self.out.event.as_ref()
        } else {
            None
        })
    }

    /// The next event, for an async caller: [`poll_next`](Self::poll_next) for
    /// [`next_event`](Self::next_event). The writer process only, as `poll_next`.
    /// [`nonblocking::FeedEventStream`](crate::nonblocking::FeedEventStream) wraps it as a
    /// `Stream` of owned events.
    pub fn poll_next_event(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<&FeedEvent>> {
        use std::task::Poll;
        match self.poll_step(cx, true) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => match &self.out.event {
                Some(event) => Poll::Ready(Ok(event)),
                None => unreachable!("a delivered step leaves its event"),
            },
        }
    }

    /// Waits up to `timeout` for the next delivery into `self.out` (catalog changes too, if
    /// `events`): whether one arrived.
    fn wait(&mut self, timeout: Duration, events: bool) -> Result<bool> {
        self.check_failed()?;
        let deadline = Instant::now().checked_add(timeout);
        loop {
            self.follow_recovery()?;
            let left = deadline.map_or(WAIT_SLICE, |d| {
                d.saturating_duration_since(Instant::now()).min(WAIT_SLICE)
            });
            let next = match self.reader.wait(left) {
                Ok(next) => next,
                // A reader process's writer restarted: follow it from the token.
                Err(pigeonhole_engine::Error::SnapshotExpired) => {
                    self.resubscribe()?;
                    continue;
                }
                // The old engine failing under a recovery's swap: follow the recovery.
                Err(_) if swapped(&self.db, self.generation) => continue,
                Err(e) => return Err(fail(&mut self.failed, e.into())),
            };
            match step(&mut self.out, &self.db, self.generation, next, events) {
                Ok(Step::Ready) => return Ok(true),
                // A catalog change a commits-only call passes over, or a superseded
                // reader's expiry: look again (the engine returns at once if more is ready).
                Ok(Step::Again) => continue,
                Ok(Step::Pending) => {}
                Ok(Step::Expired(oldest, e)) => {
                    self.oldest_retained = oldest;
                    return Err(e);
                }
                Err(e) => return Err(fail(&mut self.failed, e)),
            }
            // Out of time, unless a recovery's swap ended the old engine's wait early (look
            // again on the new one) or the wait was one slice of a longer one.
            let more = deadline.is_none_or(|d| Instant::now() < d);
            if !more {
                return Ok(false);
            }
        }
    }

    /// The next delivery into `self.out` if one is ready, as [`wait`](Self::wait) without
    /// waiting.
    fn try_step(&mut self, events: bool) -> Result<bool> {
        self.check_failed()?;
        loop {
            self.follow_recovery()?;
            let next = match self.reader.try_next() {
                Ok(next) => next,
                // A reader process's writer restarted: follow it from the token.
                Err(pigeonhole_engine::Error::SnapshotExpired) => {
                    self.resubscribe()?;
                    continue;
                }
                // The old engine failing under a recovery's swap: follow it and look again
                // (a swap is rare, so this goes round once or twice).
                Err(_) if swapped(&self.db, self.generation) => continue,
                Err(e) => return Err(fail(&mut self.failed, e.into())),
            };
            return match step(&mut self.out, &self.db, self.generation, next, events) {
                Ok(Step::Ready) => Ok(true),
                Ok(Step::Again) => continue,
                Ok(Step::Pending) => Ok(false),
                Ok(Step::Expired(oldest, e)) => {
                    self.oldest_retained = oldest;
                    Err(e)
                }
                Err(e) => Err(fail(&mut self.failed, e)),
            };
        }
    }

    /// The next delivery into `self.out`, for an async caller, as [`wait`](Self::wait).
    fn poll_step(
        &mut self,
        cx: &mut std::task::Context<'_>,
        events: bool,
    ) -> std::task::Poll<Result<()>> {
        use std::task::Poll;
        if let Err(e) = self.check_failed() {
            return Poll::Ready(Err(e));
        }
        loop {
            if let Err(e) = self.follow_recovery() {
                return Poll::Ready(Err(e));
            }
            let next = match self.reader.poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(next)) => next,
                // The old engine failing under a recovery's swap: follow it and look again.
                Poll::Ready(Err(_)) if swapped(&self.db, self.generation) => continue,
                // `Unsupported` in a reader process: not the feed's failure, so not kept.
                Poll::Ready(Err(e @ pigeonhole_engine::Error::Unsupported(_))) => {
                    return Poll::Ready(Err(e.into()));
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(fail(&mut self.failed, e.into()))),
            };
            return match step(&mut self.out, &self.db, self.generation, next, events) {
                Ok(Step::Ready) => Poll::Ready(Ok(())),
                Ok(Step::Again) => continue,
                // The engine's `poll_next` returns `Poll::Pending` instead.
                Ok(Step::Pending) => Poll::Pending,
                Ok(Step::Expired(oldest, e)) => {
                    self.oldest_retained = oldest;
                    Poll::Ready(Err(e))
                }
                Err(e) => Poll::Ready(Err(fail(&mut self.failed, e))),
            };
        }
    }

    /// The error that ended the feed, again.
    fn check_failed(&self) -> Result<()> {
        match &self.failed {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    /// Where the feed is: [`FeedBuilder::resume`] continues after the last commit delivered.
    pub fn token(&self) -> FeedToken {
        FeedToken(self.reader.token())
    }

    /// Once the feed expired: the oldest commit seqno its history still holds, when known.
    pub fn oldest_retained(&self) -> Option<u64> {
        self.oldest_retained
    }

    /// `Closed` once the database closed. After an automatic recovery onto a new engine
    /// (D265, [`Options::auto_recover`](crate::Options::auto_recover)) it resumes on that
    /// engine from the feed's own token, which the new engine checks: a durable feed
    /// continues, and a visible one that delivered what the failure lost expires. One load
    /// otherwise.
    fn follow_recovery(&mut self) -> Result<()> {
        match self.db.check_generation(self.generation) {
            Ok(()) => return Ok(()),
            Err(e) if e.code() == ErrorCode::SnapshotExpired => {}
            Err(e) => return Err(e),
        }
        self.resubscribe()
    }

    /// Subscribes again from the feed's own token, on the current engine (after a recovery)
    /// or a fresh attachment (a reader process after its writer restarted, D310 PR 4): the
    /// engine checks the token, so a durable feed continues and a visible one that delivered
    /// what was lost expires.
    fn resubscribe(&mut self) -> Result<()> {
        let mut spec = self.spec.clone();
        spec.start = FeedStart::Token(self.reader.token());
        let (reader, generation) = subscribe(&self.db, spec)?;
        self.reader = reader;
        self.generation = generation;
        Ok(())
    }
}

/// What the last call delivered, and the names that fill it.
#[derive(Debug)]
struct Delivery {
    names: Names,
    /// The last commit [`Feed::next`] delivered, or the buffer the next is read into.
    commit: Commit,
    /// The last event [`Feed::next_event`] delivered. A commit's buffer comes back to
    /// `commit` for the next one.
    event: Option<FeedEvent>,
}

/// What one of the engine's answers comes to.
enum Step {
    /// Delivered into the [`Delivery`].
    Ready,
    /// Nothing for this call; ask the engine again at once.
    Again,
    /// Nothing ready.
    Pending,
    /// The feed expired, with the oldest seqno still held.
    Expired(Option<u64>, Error),
}

/// `next` into `out`: catalog changes only if `events`, though their names are always
/// noted. An error is the feed's failure.
fn step(
    out: &mut Delivery,
    db: &Db,
    generation: u64,
    next: Next<'_>,
    events: bool,
) -> Result<Step> {
    // The last event's commit buffer, for this one.
    if let Some(FeedEvent::Commit(c)) = out.event.take() {
        out.commit = c;
    }
    match next {
        Next::Commit(c) => {
            fill(&mut out.commit, &mut out.names, db, c)?;
            if events {
                let commit = std::mem::replace(&mut out.commit, Commit::empty());
                out.event = Some(FeedEvent::Commit(commit));
            }
            Ok(Step::Ready)
        }
        Next::Catalog(ev) => {
            let change = catalog_change(&mut out.names, ev);
            if !events {
                return Ok(Step::Again);
            }
            out.event = Some(FeedEvent::Catalog(change));
            Ok(Step::Ready)
        }
        // The old engine's reader, still waiting as a recovery swapped in a new engine,
        // reads the new session's records as outside its history: the new engine's reader
        // decides from the token instead (cli692's S2 on #1321).
        Next::Expired(_) if swapped(db, generation) => Ok(Step::Again),
        Next::Expired(e) => Ok(Step::Expired(e.oldest_retained, expired(e))),
        Next::Pending => Ok(Step::Pending),
    }
}

/// `ev` as the public [`CatalogChange`], noting the names of the families it adds and
/// forgetting those of the tables it drops. A `CreateTable` and the `PutFamily` edits of
/// its table are one [`CatalogEdit::TableCreated`].
fn catalog_change(names: &mut Names, ev: &CatalogEvent) -> CatalogChange {
    // Every table the edits name: created among them, or named by the event (the
    // writer's rule, D315 2b).
    let mut tables: HashMap<TableId, Arc<str>> = ev
        .names
        .iter()
        .map(|(t, n)| (*t, Arc::from(n.as_str())))
        .collect();
    // Where each table created here sits in `edits`.
    let mut created: HashMap<TableId, usize> = HashMap::new();
    let mut edits = Vec::with_capacity(ev.edits.len());
    for e in &ev.edits {
        match e {
            Edit::CreateTable { table, name } => {
                tables.insert(*table, Arc::from(name.as_str()));
                created.insert(*table, edits.len());
                edits.push(CatalogEdit::TableCreated {
                    table: name.clone(),
                    families: Vec::new(),
                });
            }
            Edit::PutFamily {
                table,
                family,
                name,
                options,
            } => {
                let table_name = tables.get(table).cloned().unwrap_or_else(|| Arc::from(""));
                names.insert(
                    (*table, *family),
                    (Arc::clone(&table_name), Arc::from(name.as_str())),
                );
                let info = FamilyInfo::from_parts(name, options);
                match created.get(table).and_then(|&i| edits.get_mut(i)) {
                    Some(CatalogEdit::TableCreated { families, .. }) => families.push(info),
                    _ => edits.push(CatalogEdit::FamilyAdded {
                        table: table_name.to_string(),
                        family: info,
                    }),
                }
            }
            Edit::DropTable { table } => {
                names.retain(|(t, _), _| t != table);
                created.remove(table);
                let name = tables
                    .get(table)
                    .map_or_else(String::new, |n| n.to_string());
                edits.push(CatalogEdit::TableDropped { table: name });
            }
            // The writer stages only the three above (D315).
            _ => {}
        }
    }
    CatalogChange {
        seqno: ev.seqno_ceiling,
        ts: ev.timestamp,
        edits,
    }
}

/// `c` into `out`, with its tables' and families' names.
fn fill(out: &mut Commit, names: &mut Names, db: &Db, c: &pigeonhole_engine::Commit) -> Result<()> {
    out.seqno = c.seqno;
    out.ts = c.ts;
    // The commit's `Change`s are reused: their buffers keep their capacity across commits.
    out.changes.truncate(c.changes.len());
    for (i, ch) in c.changes.iter().enumerate() {
        let key = (ch.table, ch.family);
        if !names.contains_key(&key) {
            // A family created after the feed opened.
            if let Some(info) = db.engine().table_by_id(ch.table) {
                note_names(names, &info);
            }
        }
        // Unnamed only if the family was created after the feed opened and its table was
        // dropped before the feed reached this commit: the names went with the table.
        let (table, family) = names
            .get(&key)
            .cloned()
            .unwrap_or_else(|| (Arc::from(""), Arc::from("")));
        let kind = ChangeKind::of(ch.kind);
        // Admission decoded every value before it was logged; one that doesn't decode now is
        // damage, never an empty value (simp724 on #1321).
        if !kind.is_delete()
            && let Err(e) = decode_value(&ch.value)
        {
            out.changes.clear();
            return Err(Error::new(
                ErrorCode::Corruption,
                format!(
                    "a change feed value at seqno {} doesn't decode: {e}",
                    c.seqno
                ),
            ));
        }
        if let Some(slot) = out.changes.get_mut(i) {
            slot.table = table;
            slot.family = family;
            slot.kind = kind;
            slot.ts = ch.ts;
            for (to, from) in [
                (&mut slot.row, &ch.row),
                (&mut slot.qualifier, &ch.qualifier),
                (&mut slot.stored, &ch.value),
            ] {
                to.clear();
                to.extend_from_slice(from);
            }
        } else {
            out.changes.push(Change {
                table,
                family,
                kind,
                ts: ch.ts,
                row: ch.row.clone(),
                qualifier: ch.qualifier.clone(),
                stored: ch.value.clone(),
            });
        }
    }
    Ok(())
}

/// Whether the engine of `generation` was replaced by a recovery (or the database closed)
/// since the feed's reader subscribed: an error from that reader then is the old engine's,
/// not the feed's (simp724 on #1321), and `Feed::follow_recovery` decides what follows.
fn swapped(db: &Db, generation: u64) -> bool {
    db.check_generation(generation).is_err()
}

/// Ends the feed with `e`: kept for every later call, and returned now.
fn fail(failed: &mut Option<Error>, e: Error) -> Error {
    failed.get_or_insert(e).clone()
}

fn expired(e: &pigeonhole_engine::Expiry) -> Error {
    Error::new(
        ErrorCode::FeedExpired,
        format!("the change feed expired: {}", e.reason),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Family, FeedRetention, Options, Pigeonhole};

    /// simp724 on #1321: a value that doesn't decode is `Corruption`, never an empty value,
    /// and it ends the feed: every later call returns it, so the damaged commit is never
    /// passed over.
    #[test]
    fn an_undecodable_value_fails_the_feed_for_good() {
        let dir = crate::doc_support::temp_dir();
        let db = Pigeonhole::open(
            dir.join("db.phdb"),
            Options::default().change_feed(FeedRetention::bytes(16 << 20)),
        )
        .unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("f", Family::default())
            .create_if_missing()
            .unwrap();
        let mut feed = t.feed().open().unwrap();
        let info = &t.core.info;
        let bad = pigeonhole_engine::Commit {
            seqno: 9,
            ts: 1,
            changes: vec![pigeonhole_engine::Change {
                table: info.id,
                family: info.families[0].id,
                kind: Kind::Put,
                ts: 1,
                row: b"r".to_vec(),
                qualifier: b"q".to_vec(),
                // A tag byte no build writes.
                value: vec![0xEE, 1, 2],
            }],
        };
        let e = fill(&mut feed.out.commit, &mut feed.out.names, &feed.db, &bad).unwrap_err();
        assert_eq!(e.code(), ErrorCode::Corruption, "{e}");
        assert!(feed.out.commit.changes.is_empty());
        // What `next` does with it: kept, and returned by every later call.
        let e = fail(&mut feed.failed, e);
        t.mutate(b"after").put("f", b"q", b"v").commit().unwrap();
        for _ in 0..2 {
            assert_eq!(feed.next(Duration::from_secs(5)).unwrap_err(), e);
            assert_eq!(feed.try_next().unwrap_err(), e);
        }
        drop(feed);
        db.close().unwrap();
    }
}

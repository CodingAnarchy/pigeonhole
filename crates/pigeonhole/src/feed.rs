//! The change feed (D310): the committed changes of some tables, in commit order, from the
//! writer, resumable by token.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pigeonhole_engine::{FamilyId, FeedReader, FeedSpec, FeedStart, Next, TableId};
use pigeonhole_format::key::Kind;
use pigeonhole_format::value::decode_value;

use crate::db::{Db, Snapshot};
use crate::{Error, ErrorCode, Result, Table, Value};

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
    /// The table's name.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The family's name.
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
        Some(crate::cell::typed_value(
            decode_value(&self.stored).unwrap_or(pigeonhole_engine::ValueRef::Bytes(&[])),
        ))
    }
}

/// One whole commit: every change of it in the feed's tables and row prefix, never part of
/// one. Commits arrive once each, in commit order ([`Commit::seqno`]).
#[derive(Debug, Clone, Default, PartialEq)]
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

/// Builds a [`Feed`] ([`Table::feed`]).
#[derive(Debug)]
#[must_use = "a builder does nothing until `open`"]
pub struct FeedBuilder {
    db: Arc<Db>,
    tables: Vec<Table>,
    prefix: Vec<u8>,
    durable: bool,
    start: FeedStart,
}

impl FeedBuilder {
    pub(crate) fn new(table: &Table) -> Self {
        FeedBuilder {
            db: Arc::clone(&table.core.db),
            tables: vec![table.clone()],
            prefix: Vec::new(),
            durable: true,
            start: FeedStart::Now,
        }
    }

    /// Follows `table` as well: one feed, the commits of all its tables in one order, each
    /// commit whole across them.
    pub fn table(mut self, table: &Table) -> Self {
        if !self
            .tables
            .iter()
            .any(|t| t.core.info.id == table.core.info.id)
        {
            self.tables.push(table.clone());
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
        self.start = FeedStart::After(snapshot.seqno());
        self
    }

    /// Starts with the oldest commit the database's feed history still holds
    /// ([`Options::change_feed`](crate::Options::change_feed)). Under load that history can
    /// be recycled before the feed reaches it, which expires the feed at once;
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
    /// with the commits after this call.
    ///
    /// Fails with `InvalidArgument` if the database's change feed is off or doesn't follow
    /// one of the tables ([`Options::feed_tables`](crate::Options::feed_tables)), or if a
    /// token is not one of this database's; `Unsupported` in a reader process (for now). A
    /// start whose history is gone shows at the first [`Feed::next`], as `FeedExpired`.
    pub fn open(self) -> Result<Feed> {
        let spec = FeedSpec {
            tables: self.tables.iter().map(|t| t.core.info.id).collect(),
            prefix: self.prefix,
            durable: self.durable,
            start: self.start,
        };
        let mut names = HashMap::new();
        for t in &self.tables {
            note_names(&mut names, &t.core.info);
        }
        let (reader, generation) = subscribe(&self.db, spec.clone())?;
        Ok(Feed {
            db: self.db,
            spec,
            reader,
            generation,
            names,
            commit: Commit::default(),
            oldest_retained: None,
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
/// was opened without the feed for these tables or with another shard count since, or when
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
    names: Names,
    commit: Commit,
    oldest_retained: Option<u64>,
}

/// The longest single wait handed to the engine; a longer `next` waits again.
const WAIT_SLICE: Duration = Duration::from_secs(3600);

impl Feed {
    /// The next commit, waiting up to `timeout` for one; `None` if none arrived in time.
    pub fn next(&mut self, timeout: Duration) -> Result<Option<&Commit>> {
        let deadline = Instant::now().checked_add(timeout);
        loop {
            self.follow_recovery()?;
            let left = deadline.map_or(WAIT_SLICE, |d| {
                d.saturating_duration_since(Instant::now()).min(WAIT_SLICE)
            });
            match self.reader.wait(left)? {
                Next::Commit(c) => {
                    fill(&mut self.commit, &mut self.names, &self.db, c);
                    return Ok(Some(&self.commit));
                }
                Next::Expired(e) => {
                    self.oldest_retained = e.oldest_retained;
                    return Err(expired(e));
                }
                Next::Pending => {}
            }
            // Out of time, unless a recovery's swap ended the old engine's wait early (look
            // again on the new one) or the wait was one slice of a longer one.
            let more = deadline.is_none_or(|d| Instant::now() < d);
            if !more {
                return Ok(None);
            }
        }
    }

    /// The next commit if one is ready; `None` otherwise. Never waits.
    pub fn try_next(&mut self) -> Result<Option<&Commit>> {
        self.follow_recovery()?;
        match self.reader.try_next()? {
            Next::Commit(c) => {
                fill(&mut self.commit, &mut self.names, &self.db, c);
                Ok(Some(&self.commit))
            }
            Next::Expired(e) => {
                self.oldest_retained = e.oldest_retained;
                Err(expired(e))
            }
            Next::Pending => Ok(None),
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
        let mut spec = self.spec.clone();
        spec.start = FeedStart::Token(self.reader.token());
        let (reader, generation) = subscribe(&self.db, spec)?;
        self.reader = reader;
        self.generation = generation;
        Ok(())
    }
}

/// `c` into `out`, with its tables' and families' names.
fn fill(out: &mut Commit, names: &mut Names, db: &Db, c: &pigeonhole_engine::Commit) {
    out.seqno = c.seqno;
    out.ts = c.ts;
    out.changes.clear();
    for ch in &c.changes {
        let key = (ch.table, ch.family);
        if !names.contains_key(&key) {
            // A family created after the feed opened.
            if let Some(info) = db.engine().table_by_id(ch.table) {
                note_names(names, &info);
            }
        }
        let (table, family) = names
            .get(&key)
            .cloned()
            .unwrap_or_else(|| (Arc::from(""), Arc::from("")));
        out.changes.push(Change {
            table,
            family,
            kind: ChangeKind::of(ch.kind),
            ts: ch.ts,
            row: ch.row.clone(),
            qualifier: ch.qualifier.clone(),
            stored: ch.value.clone(),
        });
    }
}

fn expired(e: &pigeonhole_engine::Expiry) -> Error {
    Error::new(
        ErrorCode::FeedExpired,
        format!("the change feed expired: {}", e.reason),
    )
}

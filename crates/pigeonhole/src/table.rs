use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use pigeonhole_engine::{FamilyId, TableInfo};

use crate::db::Db;

use crate::cell::CellRef;
use crate::{Error, ErrorCode, Family, Pigeonhole, Result, RowMutation, RowRead, Scan, Snapshot};

/// Defines or opens a table. Returned by [`Pigeonhole::table`].
///
/// ```
/// use pigeonhole::{days, ErrorCode, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let events = db
///     .table("events")?
///     .family("ev", Family::default().ttl(days(7)))
///     .create()?;
/// assert_eq!(events.families(), ["ev"]);
///
/// // Creating it again fails; opening it (and adding a family) works.
/// let again = db.table("events")?.family("ev", Family::default()).create();
/// assert_eq!(again.unwrap_err().code(), ErrorCode::TableExists);
/// let events = db.table("events")?.family("meta", Family::default()).open()?;
/// assert_eq!(events.families(), ["ev", "meta"]);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct TableBuilder<'db> {
    db: &'db Pigeonhole,
    name: String,
    families: Vec<(String, Family)>,
}

impl<'db> TableBuilder<'db> {
    pub(crate) fn new(db: &'db Pigeonhole, name: &str) -> Self {
        Self {
            db,
            name: name.to_owned(),
            families: Vec::new(),
        }
    }
}

/// What a builder does when the table exists or is missing.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Finish {
    CreateIfMissing,
    Create,
    Open,
}

impl TableBuilder<'_> {
    /// Declares a family. On an existing table, a family not yet present is added (cheap);
    /// one that exists keeps its stored options.
    pub fn family(mut self, name: &str, family: Family) -> Self {
        self.families.push((name.to_owned(), family));
        self
    }

    /// Opens the table, creating it (and any missing declared families) if needed. Creating
    /// a table needs at least one declared family; an empty name or a new table without one
    /// fails with [`ErrorCode::InvalidArgument`].
    pub fn create_if_missing(self) -> Result<Table> {
        self.finish(Finish::CreateIfMissing)
    }

    /// Creates the table; fails with `TableExists` if it exists, and with `InvalidArgument`
    /// for an empty name or no declared family.
    pub fn create(self) -> Result<Table> {
        self.finish(Finish::Create)
    }

    /// Opens an existing table; fails with `TableNotFound`. Declared families that the table
    /// does not have yet are added.
    pub fn open(self) -> Result<Table> {
        self.finish(Finish::Open)
    }

    fn finish(self, how: Finish) -> Result<Table> {
        let db = &self.db.db;
        db.check_open()?;
        if self.name.is_empty() {
            return Err(Error::new(ErrorCode::InvalidArgument, "empty table name"));
        }
        let engine = self.db.engine();
        let mut defs = Vec::with_capacity(self.families.len());
        for (name, family) in &self.families {
            if !defs.iter().any(|(n, _)| n == name) {
                defs.push((name.clone(), family.to_engine()));
            }
        }
        let mut info = match engine.table(&self.name) {
            Some(info) if how == Finish::Create => {
                return Err(pigeonhole_engine::Error::TableExists(info.name.clone()).into());
            }
            Some(info) => info,
            None if how == Finish::Open => {
                return Err(pigeonhole_engine::Error::TableNotFound(self.name).into());
            }
            None if defs.is_empty() => {
                return Err(Error::new(
                    ErrorCode::InvalidArgument,
                    format!(
                        "table {:?} needs at least one family: declare one with .family(..)",
                        self.name
                    ),
                ));
            }
            None => match db.retry_closed(|| {
                self.db
                    .engine()
                    .create_table(&self.name, &defs)
                    .map_err(Error::from)
            }) {
                Ok(info) => info,
                // Another thread created it first: open it as it is.
                Err(e) if e.code() == ErrorCode::TableExists && how != Finish::Create => engine
                    .table(&self.name)
                    .ok_or_else(|| pigeonhole_engine::Error::TableNotFound(self.name.clone()))?,
                Err(e) => return Err(e),
            },
        };
        for (name, options) in defs {
            if info.family(&name).is_some() {
                continue;
            }
            let id = info.id;
            info = match db.retry_closed(|| {
                self.db
                    .engine()
                    .add_family(id, &name, options.clone())
                    .map_err(Error::from)
            }) {
                Ok(info) => info,
                // Added concurrently: it keeps the options it was created with.
                Err(e) if e.code() == ErrorCode::FamilyExists => engine
                    .table(&self.name)
                    .ok_or_else(|| pigeonhole_engine::Error::TableNotFound(self.name.clone()))?,
                Err(e) => return Err(e),
            };
        }
        Ok(Table {
            core: Arc::new(TableCore::new(
                Arc::clone(db),
                info,
                self.db.cached.fresh(db),
            )),
        })
    }
}

/// What every table handle shares: the database and the catalog entry it was opened with.
#[derive(Debug)]
pub(crate) struct TableCore {
    pub(crate) db: Arc<Db>,
    pub(crate) info: Arc<TableInfo>,
    pub(crate) cached: crate::db::Cached,
}

impl TableCore {
    pub(crate) fn new(db: Arc<Db>, info: Arc<TableInfo>, cached: crate::db::Cached) -> Self {
        Self { db, info, cached }
    }

    /// The engine for one operation: the handle's cached one while it is current.
    #[inline]
    pub(crate) fn engine(&self) -> crate::db::EngineRef<'_> {
        self.cached.get(&self.db)
    }

    /// `f` on the engine for one operation (the hot paths: no guard materialized).
    #[inline(always)]
    pub(crate) fn with<R>(&self, f: impl FnOnce(&pigeonhole_engine::Engine) -> R) -> R {
        self.cached.with(&self.db, f)
    }

    fn family_info(&self, family: &str) -> Option<crate::FamilyInfo> {
        self.info.family(family).map(crate::FamilyInfo::from)
    }

    /// The catalog entry as of now: a family added through another handle shows up here.
    /// Falls back to the handle's own entry if the table was dropped or replaced.
    pub(crate) fn current_info(&self) -> Arc<TableInfo> {
        match self.engine().table(&self.info.name) {
            Some(info) if info.id == self.info.id => info,
            _ => Arc::clone(&self.info),
        }
    }

    /// Resolves a family name: from the handle's entry (no allocation), else from the
    /// current catalog.
    #[inline]
    pub(crate) fn family_id(&self, name: &str) -> Result<FamilyId> {
        if let Some(f) = self.info.family(name) {
            return Ok(f.id);
        }
        self.current_info()
            .family(name)
            .map(|f| f.id)
            .ok_or_else(|| family_not_found(&self.info.name, name))
    }

    /// `get` as of now with a supervisor: run again on the recovered engine if it got
    /// `Closed`. Out of line, so the plain arm of `get` keeps main's codegen (inlined beside
    /// it, the two arms shared a temporary for the engine's result and moved it once more:
    /// about 50 instructions per get on callgrind, #836 round 7).
    #[cold]
    #[inline(never)]
    fn get_latest_supervised(
        &self,
        table: pigeonhole_engine::TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> Result<Option<pigeonhole_engine::CellData>> {
        self.db.retry_closed(|| {
            self.with(|e| e.get_latest(table, family, row, qualifier))
                .map_err(Error::from)
        })
    }

    /// A snapshot with a supervisor (see `get_latest_supervised`).
    #[cold]
    #[inline(never)]
    pub(crate) fn snapshot_supervised(&self) -> Result<pigeonhole_engine::Snapshot> {
        self.db
            .retry_closed(|| self.engine().snapshot().map_err(Error::from))
    }

    /// The newest version of one cell, as of `snapshot` or now.
    #[inline]
    pub(crate) fn get(
        &self,
        snapshot: Option<&Snapshot>,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> Result<Option<CellRef<'_>>> {
        self.db.check_open()?;
        let family = self.family_id(family)?;
        let table = self.info.id;
        let data = match snapshot {
            // With a supervisor, the read is run again on the recovered engine if it got
            // `Closed`; without one, the call as before the option (`Db::supervised`).
            None if self.db.supervised() => {
                self.get_latest_supervised(table, family, row, qualifier)?
            }
            None => self.with(|e| e.get_latest(table, family, row, qualifier))?,
            Some(s) => {
                self.db.check_snapshot(s)?;
                self.with(|e| e.get(&s.inner, table, family, row, qualifier))?
            }
        };
        Ok(data.map(CellRef::owned))
    }

    /// A get as a future; errors found before any read (closed, unknown family, a snapshot
    /// of another database) resolve on the first poll.
    #[cfg(feature = "async")]
    pub(crate) fn get_async(
        &self,
        snapshot: Option<&Snapshot>,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> crate::nonblocking::GetFuture {
        let started = self.db.check_open().and_then(|()| {
            let family = self.family_id(family)?;
            let table = self.info.id;
            Ok(match snapshot {
                None => self.with(|e| e.get_latest_async(table, family, row, qualifier)),
                Some(s) => {
                    self.db.check_snapshot(s)?;
                    self.with(|e| e.get_async(&s.inner, table, family, row, qualifier))
                }
            })
        });
        crate::nonblocking::GetFuture::new(started)
    }

    fn scan(self: &Arc<Self>, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Scan<'_> {
        Scan::new(self, start.map(<[u8]>::to_vec), end.map(<[u8]>::to_vec))
    }

    fn scan_range<'k, K: AsRef<[u8]> + ?Sized + 'k>(
        self: &Arc<Self>,
        range: impl RangeBounds<&'k K>,
    ) -> Scan<'_> {
        self.scan(
            range.start_bound().map(|k| k.as_ref()),
            range.end_bound().map(|k| k.as_ref()),
        )
    }

    fn scan_prefix(self: &Arc<Self>, prefix: &[u8]) -> Scan<'_> {
        let end = match prefix_end(prefix) {
            Some(end) => Bound::Excluded(end),
            None => Bound::Unbounded,
        };
        Scan::new(self, Bound::Included(prefix.to_vec()), end)
    }
}

pub(crate) fn family_not_found(table: &str, family: &str) -> Error {
    Error::new(
        ErrorCode::FamilyNotFound,
        format!("no such family {family:?} in table {table:?}"),
    )
}

/// The smallest key greater than every key starting with `prefix`, or `None` if there is
/// none (`prefix` is empty or all `0xff`).
pub(crate) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let keep = prefix.iter().rposition(|&b| b != 0xff)?;
    let mut end = prefix[..=keep].to_vec();
    end[keep] += 1;
    Some(end)
}

/// A table handle for reads and writes. Cheap to clone; `Send + Sync`.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let pages = db
///     .table("pages")?
///     .family("meta", Family::default().max_versions(1))
///     .family("links", Family::default())
///     .family("stats", Family::counter())
///     .create_if_missing()?;
///
/// pages
///     .mutate(b"com.example/a")
///     .put("meta", b"status", b"200")
///     .put("links", b"com.example/b", b"")
///     .incr("stats", b"hits", 1)
///     .commit()?;
///
/// let status = pages.get(b"com.example/a", "meta", b"status")?.unwrap();
/// assert_eq!(status.value(), b"200");
/// let hits = pages.get(b"com.example/a", "stats", b"hits")?.and_then(|c| c.as_i64());
/// assert_eq!(hits, Some(1));
///
/// let row = pages.row(b"com.example/a").family("links").read()?.unwrap();
/// assert_eq!(row.len(), 1);
///
/// let keys: Vec<Vec<u8>> = pages
///     .scan_prefix(b"com.example/")
///     .iter()?
///     .map(|row| row.map(|r| r.key().to_vec()))
///     .collect::<pigeonhole::Result<_>>()?;
/// assert_eq!(keys, [b"com.example/a".to_vec()]);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Table {
    pub(crate) core: Arc<TableCore>,
}

impl Table {
    /// The table name.
    pub fn name(&self) -> &str {
        &self.core.info.name
    }

    /// Family names, in creation order, as of when this handle was opened.
    pub fn families(&self) -> Vec<&str> {
        self.core
            .info
            .families
            .iter()
            .map(|f| f.name.as_str())
            .collect()
    }

    /// A family's stored options, as of when this handle was opened (ICR 0047); `None` if
    /// the table has no such family.
    pub fn family_info(&self, family: &str) -> Option<crate::FamilyInfo> {
        self.core.family_info(family)
    }

    /// Starts a single-row atomic mutation.
    pub fn mutate<'t>(&'t self, row: &[u8]) -> RowMutation<'t> {
        RowMutation::new(self, row)
    }

    /// The newest version of one cell. Borrows from the cache: no allocation.
    #[inline]
    pub fn get(&self, row: &[u8], family: &str, qualifier: &[u8]) -> Result<Option<CellRef<'_>>> {
        self.core.get(None, row, family, qualifier)
    }

    /// The newest version of one cell as of `snapshot`.
    pub fn get_at(
        &self,
        snapshot: &Snapshot,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> Result<Option<CellRef<'_>>> {
        self.core.get(Some(snapshot), row, family, qualifier)
    }

    /// The shard that owns `row` right now ([`Shard::index`](crate::Shard::index)), for
    /// application-owned mode: a thread driving a shard can commit the rows that shard owns,
    /// so its writes run on its own shard with no handoff (the spec's inline local writes).
    /// A routing hint only (ICR 0022):
    /// - it reflects the current tablet map, which can change at any time after a split,
    ///   merge or move;
    /// - a commit from any thread is correct whatever shard owns its rows, merely routed
    ///   there.
    ///
    /// `None` if the table was dropped.
    ///
    /// ```
    /// use pigeonhole::{Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(2))?;
    /// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
    /// let shard = t.shard_of(b"user:1").expect("the table exists");
    /// assert!(shard < 2);
    /// # drop(t);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn shard_of(&self, row: &[u8]) -> Option<usize> {
        self.core.engine().shard_of(self.core.info.id, row)
    }

    /// Splits this table's row space at `keys` (sorted) and spreads the pieces over the
    /// shards, piece `i` to shard `i % shards`, waiting until done. A bench hook (D213): the
    /// gate's group-commit table starts on every shard instead of waiting for the balancer.
    /// The balancer may still move the pieces later.
    #[doc(hidden)]
    pub fn split_over_shards(&self, keys: Vec<Vec<u8>>) -> Result<()> {
        let shards = self.core.engine().shard_stats().len().max(1);
        let owners: Vec<u16> = (0..=keys.len()).map(|i| (i % shards) as u16).collect();
        self.core
            .db
            .engine()
            .split_table(self.core.info.id, keys, &owners)
            .map_err(Into::into)
    }

    /// [`get`](Self::get) as a future resolving to an owned [`Cell`](crate::Cell): memtable
    /// and cache hits resolve on the first poll, and a block the cache does not hold is read
    /// asynchronously (see [`nonblocking`](crate::nonblocking)).
    #[cfg(feature = "async")]
    pub fn get_async(
        &self,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> crate::nonblocking::GetFuture {
        self.core.get_async(None, row, family, qualifier)
    }

    /// [`get_at`](Self::get_at) as a future, as [`get_async`](Self::get_async).
    #[cfg(feature = "async")]
    pub fn get_at_async(
        &self,
        snapshot: &Snapshot,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> crate::nonblocking::GetFuture {
        self.core.get_async(Some(snapshot), row, family, qualifier)
    }

    /// Starts a row read.
    pub fn row<'t>(&'t self, row: &[u8]) -> RowRead<'t> {
        RowRead::new(&self.core, row)
    }

    /// Starts an ordered scan over a row range: `table.scan(b"a".."b")`.
    pub fn scan<'k, K: AsRef<[u8]> + ?Sized + 'k>(
        &self,
        range: impl RangeBounds<&'k K>,
    ) -> Scan<'_> {
        self.core.scan_range(range)
    }

    /// Starts a scan of every row starting with `prefix`.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Scan<'_> {
        self.core.scan_prefix(prefix)
    }

    /// Starts a scan from explicit bounds (the form a C ABI exports).
    pub fn scan_bounds(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Scan<'_> {
        self.core.scan(start, end)
    }
}

/// A table handle on a [`PigeonholeReader`](crate::PigeonholeReader): reads only.
///
/// See [`PigeonholeReader`](crate::PigeonholeReader) for an example.
#[derive(Debug, Clone)]
pub struct ReadTable {
    pub(crate) core: Arc<TableCore>,
}

impl ReadTable {
    /// The table name.
    pub fn name(&self) -> &str {
        &self.core.info.name
    }

    /// Family names, in creation order, as of when this handle was opened (ICR 0047).
    pub fn families(&self) -> Vec<&str> {
        self.core
            .info
            .families
            .iter()
            .map(|f| f.name.as_str())
            .collect()
    }

    /// As [`Table::family_info`].
    pub fn family_info(&self, family: &str) -> Option<crate::FamilyInfo> {
        self.core.family_info(family)
    }

    /// The newest version of one cell.
    pub fn get(&self, row: &[u8], family: &str, qualifier: &[u8]) -> Result<Option<CellRef<'_>>> {
        self.core.get(None, row, family, qualifier)
    }

    /// The newest version of one cell as of `snapshot`.
    pub fn get_at(
        &self,
        snapshot: &Snapshot,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> Result<Option<CellRef<'_>>> {
        self.core.get(Some(snapshot), row, family, qualifier)
    }

    /// [`get`](Self::get) as a future resolving to an owned [`Cell`](crate::Cell): memtable
    /// and cache hits resolve on the first poll, and a block the cache does not hold is read
    /// asynchronously (see [`nonblocking`](crate::nonblocking)).
    #[cfg(feature = "async")]
    pub fn get_async(
        &self,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> crate::nonblocking::GetFuture {
        self.core.get_async(None, row, family, qualifier)
    }

    /// [`get_at`](Self::get_at) as a future, as [`get_async`](Self::get_async).
    #[cfg(feature = "async")]
    pub fn get_at_async(
        &self,
        snapshot: &Snapshot,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> crate::nonblocking::GetFuture {
        self.core.get_async(Some(snapshot), row, family, qualifier)
    }

    /// Starts a row read.
    pub fn row<'t>(&'t self, row: &[u8]) -> RowRead<'t> {
        RowRead::new(&self.core, row)
    }

    /// Starts an ordered scan over a row range.
    pub fn scan<'k, K: AsRef<[u8]> + ?Sized + 'k>(
        &self,
        range: impl RangeBounds<&'k K>,
    ) -> Scan<'_> {
        self.core.scan_range(range)
    }

    /// Starts a scan of every row starting with `prefix`.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Scan<'_> {
        self.core.scan_prefix(prefix)
    }

    /// Starts a scan from explicit bounds.
    pub fn scan_bounds(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Scan<'_> {
        self.core.scan(start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::prefix_end;

    #[test]
    fn prefix_end_is_the_next_key_after_the_prefix() {
        assert_eq!(prefix_end(b"user:"), Some(b"user;".to_vec()));
        assert_eq!(prefix_end(b"a\xff\xff"), Some(b"b".to_vec()));
        assert_eq!(prefix_end(b"\xff\xff"), None);
        assert_eq!(prefix_end(b""), None);
    }
}

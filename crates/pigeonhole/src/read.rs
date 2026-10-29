use std::borrow::Cow;
use std::ops::{Bound, Range, RangeBounds};
use std::sync::Arc;

use pigeonhole_engine::{
    FamilyId, QualifierFilter, ReadSpec, ScanCursor, ScanSpec, TableInfo, ValuePredicate,
};

use smallvec::SmallVec;

use crate::cell::RowBuf;
use crate::table::TableCore;
use crate::{Result, Row, RowRef, Snapshot};

/// A predicate on a cell value, evaluated inside the read path before materialization.
///
/// Byte predicates compare the value bytes (what [`CellRef::value`](crate::CellRef::value)
/// returns); `I64` matches `i64` (and varint) values only (decision D77).
///
/// ```
/// use std::cmp::Ordering;
/// use pigeonhole::{Family, Options, Pigeonhole, ValueFilter};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// t.mutate(b"a").put_i64("f", b"score", 10).commit()?;
/// t.mutate(b"b").put_i64("f", b"score", 200).commit()?;
///
/// let high: Vec<Vec<u8>> = t
///     .scan_prefix(b"")
///     .value_filter(ValueFilter::I64(Ordering::Greater, 100))
///     .iter()?
///     .map(|r| r.map(|r| r.key().to_vec()))
///     .collect::<pigeonhole::Result<_>>()?;
/// assert_eq!(high, [b"b".to_vec()]);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ValueFilter {
    /// Value bytes equal.
    Equals(Vec<u8>),
    /// Value bytes start with.
    Prefix(Vec<u8>),
    /// The value is an `i64` that compares to the operand as given.
    I64(std::cmp::Ordering, i64),
}

impl ValueFilter {
    pub(crate) fn to_engine(&self) -> ValuePredicate {
        self.clone().into_engine()
    }

    /// [`ValueFilter::to_engine`] without the copy, for a filter the caller owns.
    pub(crate) fn into_engine(self) -> ValuePredicate {
        match self {
            ValueFilter::Equals(v) => ValuePredicate::Equals(v),
            ValueFilter::Prefix(v) => ValuePredicate::Prefix(v),
            ValueFilter::I64(o, v) => ValuePredicate::I64(o, v),
        }
    }
}

/// A condition for a conditional mutation ([`RowMutation::commit_if`](crate::RowMutation::commit_if)).
///
/// See [`RowMutation`](crate::RowMutation) for an example.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Condition {
    /// The column has a visible version.
    Exists {
        /// Family.
        family: String,
        /// Qualifier.
        qualifier: Vec<u8>,
    },
    /// The column has no visible version.
    Absent {
        /// Family.
        family: String,
        /// Qualifier.
        qualifier: Vec<u8>,
    },
    /// The column's newest value matches.
    Value {
        /// Family.
        family: String,
        /// Qualifier.
        qualifier: Vec<u8>,
        /// Condition on the value.
        filter: ValueFilter,
    },
}

/// A read's selected families, resolved: inline for up to four.
pub(crate) type FamilyIds = SmallVec<[FamilyId; 4]>;

/// The selection shared by row reads and scans, with family names kept until the read
/// starts (builders never fail; errors surface at `read`/`iter`).
#[derive(Debug, Clone)]
struct Selection {
    /// The selected families' names, one after another (`ends` marks where each ends):
    /// inline for a couple of short names, so a read allocates nothing for them (#287).
    names: SmallVec<[u8; 32]>,
    ends: SmallVec<[u32; 4]>,
    spec: ReadSpec,
    snapshot: Option<Snapshot>,
}

impl Selection {
    fn new() -> Self {
        let mut spec = ReadSpec::default();
        spec.versions = 1;
        Self {
            names: SmallVec::new(),
            ends: SmallVec::new(),
            spec,
            snapshot: None,
        }
    }

    fn push_family(&mut self, name: &str) {
        self.names.extend_from_slice(name.as_bytes());
        let end = u32::try_from(self.names.len()).expect("family names under 4 GiB");
        self.ends.push(end);
    }

    /// The selected families' names, in order.
    fn family_names(&self) -> impl Iterator<Item = &str> {
        let mut start = 0;
        self.ends.iter().map(move |&end| {
            let name = &self.names[start..end as usize];
            start = end as usize;
            std::str::from_utf8(name).expect("pushed from a str")
        })
    }

    fn qualifier_bounds(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) {
        self.spec.qualifiers =
            QualifierFilter::Range(start.map(<[u8]>::to_vec), end.map(<[u8]>::to_vec));
    }

    fn time_range(&mut self, range: Range<u64>) {
        self.spec.time_range = Some((range.start, range.end));
    }

    /// For a read as of now (no snapshot given): the table's info and the families to read,
    /// resolved from the current catalog before the engine loads its view, so the view sees
    /// every one of them. With no families selected, that is every family of this catalog
    /// entry, named explicitly: a family added before the view loads is not read, as the
    /// entry could not name its cells. `None` when a name does not resolve (or the table has
    /// no family): the caller then takes the snapshot path, which reports it exactly as
    /// before (a family created meanwhile is found there).
    fn start_latest(&self, core: &TableCore) -> Result<Option<(Arc<TableInfo>, FamilyIds)>> {
        core.db.check_open()?;
        let info = core.current_info();
        let mut ids = SmallVec::new();
        for name in self.family_names() {
            let Some(family) = info.family(name) else {
                return Ok(None);
            };
            if !ids.contains(&family.id) {
                ids.push(family.id);
            }
        }
        if self.ends.is_empty() {
            ids.extend(info.families.iter().map(|f| f.id));
        }
        if ids.is_empty() {
            return Ok(None);
        }
        Ok(Some((info, ids)))
    }

    /// Takes the snapshot, then resolves the projection against the catalog as of then: the
    /// table's info, which names every family the read can return, the snapshot, and the
    /// selected families' ids (empty: every family).
    fn start(
        &mut self,
        core: &TableCore,
    ) -> Result<(Arc<TableInfo>, pigeonhole_engine::Snapshot, FamilyIds)> {
        core.db.check_open()?;
        let snapshot = match self.snapshot.take() {
            Some(s) => {
                core.db.check_snapshot(&s)?;
                s.inner
            }
            // The retry only with a supervisor (`Db::supervised`).
            None if core.db.supervised() => core.snapshot_supervised()?,
            None => core.with(pigeonhole_engine::Engine::snapshot)?,
        };
        // The catalog as of the snapshot names every family it can return, and resolves a
        // family dropped (or dropped and created again) since as the snapshot saw it; a name
        // only the current catalog knows resolves there and reads nothing (#892).
        let info = core.info_at(&snapshot);
        let mut ids = SmallVec::new();
        for name in self.family_names() {
            let id = match info.family(name) {
                Some(f) => f.id,
                None => core.family_id(name)?,
            };
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        Ok((info, snapshot, ids))
    }
}

/// Implements the builder methods `RowRead` and `Scan` share.
macro_rules! selection_methods {
    () => {
        /// Read only these families (default: all), in this order.
        pub fn families<'f>(mut self, families: impl IntoIterator<Item = &'f str>) -> Self {
            self.sel.names.clear();
            self.sel.ends.clear();
            for name in families {
                self.sel.push_family(name);
            }
            self
        }

        /// Add one family to the projection.
        pub fn family(mut self, family: &str) -> Self {
            self.sel.push_family(family);
            self
        }

        /// Only qualifiers starting with `prefix`.
        pub fn qualifier_prefix(mut self, prefix: &[u8]) -> Self {
            self.sel.spec.qualifiers = QualifierFilter::Prefix(prefix.to_vec());
            self
        }

        /// Only qualifiers within `range`.
        pub fn qualifier_range<'k, K: AsRef<[u8]> + ?Sized + 'k>(
            mut self,
            range: impl RangeBounds<&'k K>,
        ) -> Self {
            self.sel.qualifier_bounds(
                range.start_bound().map(|k| k.as_ref()),
                range.end_bound().map(|k| k.as_ref()),
            );
            self
        }

        /// Only qualifiers within explicit bounds (the non-generic form a C ABI exports).
        pub fn qualifier_bounds(mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self {
            self.sel.qualifier_bounds(start, end);
            self
        }

        /// Only the newest version of each column (the default).
        pub fn latest(mut self) -> Self {
            self.sel.spec.versions = 1;
            self
        }

        /// Only versions with timestamps in `range`.
        pub fn time_range(mut self, range: Range<u64>) -> Self {
            self.sel.time_range(range);
            self
        }

        /// Only cells whose value matches.
        pub fn value_filter(mut self, filter: ValueFilter) -> Self {
            self.sel.spec.value = Some(filter.into_engine());
            self
        }

        /// Read as of `snapshot` instead of now.
        pub fn snapshot(mut self, snapshot: &Snapshot) -> Self {
            self.sel.snapshot = Some(snapshot.clone());
            self
        }
    };
}

/// A row read under construction. Finish with [`RowRead::read`].
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let t = db.table("t")?.family("temp", Family::default()).create_if_missing()?;
/// for (ts, v) in [(10u64, b"20.5"), (20, b"21.0"), (30, b"21.5")] {
///     t.mutate(b"sensor:7").put_at("temp", b"c", ts, v).commit()?;
/// }
///
/// let row = t.row(b"sensor:7").family("temp").versions(2).read()?.unwrap();
/// let versions: Vec<u64> = row.iter().map(|e| e.cell.timestamp()).collect();
/// assert_eq!(versions, [30, 20]);
///
/// let window = t.row(b"sensor:7").versions(0).time_range(10..30).read()?.unwrap();
/// assert_eq!(window.len(), 2);
/// assert!(t.row(b"sensor:8").read()?.is_none());
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[must_use = "a row read does nothing until .read()"]
pub struct RowRead<'t> {
    core: &'t TableCore,
    row: Vec<u8>,
    sel: Selection,
}

impl<'t> RowRead<'t> {
    /// The latest-version read with a supervisor: run again on the recovered engine if it
    /// got `Closed`. Out of line, so the plain arm keeps main's codegen (`TableCore::get_latest_supervised`).
    #[cold]
    #[inline(never)]
    fn read_latest_supervised(
        &self,
        table: pigeonhole_engine::TableId,
        families: &[pigeonhole_engine::FamilyId],
        buf: &mut RowBuf,
    ) -> Result<bool> {
        self.core.db.retry_closed(|| {
            self.core
                .engine()
                .read_row_latest_into(table, &self.row, families, &self.sel.spec, buf)
                .map_err(crate::Error::from)
        })
    }

    pub(crate) fn new(core: &'t TableCore, row: &[u8]) -> Self {
        Self {
            core,
            row: row.to_vec(),
            sel: Selection::new(),
        }
    }

    selection_methods!();

    /// Up to `n` versions of each column (0: all retained).
    pub fn versions(mut self, n: u32) -> Self {
        self.sel.spec.versions = n;
        self
    }

    /// At most `n` columns per family. The same limit as [`Scan::column_limit`], which
    /// applies it to each row.
    pub fn column_limit(mut self, n: u32) -> Self {
        self.sel.spec.columns_per_row = n;
        self
    }

    /// Performs the read. `None` if the row has no matching cell.
    pub fn read(mut self) -> Result<Option<RowRef<'t>>> {
        // As of now, a read takes no snapshot to create and drop (#287); one given, or a
        // family the current catalog does not know yet, goes through a snapshot.
        let latest = match self.sel.snapshot {
            None => self.sel.start_latest(self.core)?,
            Some(_) => None,
        };
        let (info, snapshot, families) = match latest {
            Some((info, families)) => (info, None, families),
            None => {
                let (info, snapshot, families) = self.sel.start(self.core)?;
                (info, Some(snapshot), families)
            }
        };
        // The engine resolves the cells straight into the row this returns (#287): no
        // intermediate row and no second copy of each cell.
        let mut buf = RowBuf::new(info, 0);
        let table = self.core.info.id;
        let any = match &snapshot {
            // The retry only with a supervisor (`Db::supervised`).
            None if self.core.db.supervised() => {
                self.read_latest_supervised(table, &families, &mut buf)?
            }
            None => self.core.with(|e| {
                e.read_row_latest_into(table, &self.row, &families, &self.sel.spec, &mut buf)
            })?,
            Some(snapshot) => self.core.with(|e| {
                e.read_row_into_families(
                    snapshot,
                    table,
                    &self.row,
                    &families,
                    &self.sel.spec,
                    &mut buf,
                )
            })?,
        };
        if !any {
            return Ok(None);
        }
        buf.key = self.row;
        Ok(Some(RowRef::owned(buf)))
    }
}

#[cfg(feature = "async")]
impl RowRead<'_> {
    /// [`read`](RowRead::read) as a future resolving to an owned [`Row`]: a memtable or cache
    /// hit is read at the call and resolves on the first poll, and a block the cache does
    /// not hold is read asynchronously (see [`nonblocking`](crate::nonblocking)).
    pub fn read_async(mut self) -> crate::nonblocking::RowFuture {
        let started = (|| {
            let latest = match self.sel.snapshot {
                None => self.sel.start_latest(self.core)?,
                Some(_) => None,
            };
            let (engine, table) = (&self.core.engine(), self.core.info.id);
            Ok(Ok(match latest {
                Some((info, families)) => {
                    // First at the call, as the sync read (#709): a hit needs no future, so
                    // concurrent readers share no reference count beyond the sync read's.
                    let mut buf = RowBuf::new(info, 0);
                    let missed = match engine.try_read_row_latest(
                        table,
                        &self.row,
                        &families,
                        &self.sel.spec,
                        &mut buf,
                    ) {
                        Ok(r) => {
                            return Ok(Err(r.map_err(crate::Error::from).map(|any| {
                                any.then(|| {
                                    buf.key = std::mem::take(&mut self.row);
                                    Row::new(buf)
                                })
                            })));
                        }
                        Err(missed) => missed,
                    };
                    // A miss may have left cells in `buf`: the future starts from an empty
                    // row for the same table entry, waiting on the read the miss submitted.
                    let info = buf.info.take().expect("set by RowBuf::new");
                    engine.read_row_latest_async_after(
                        missed,
                        table,
                        &self.row,
                        &families,
                        &self.sel.spec,
                        RowBuf::new(info, 0),
                    )
                }
                None => {
                    let (info, snapshot, families) = self.sel.start(self.core)?;
                    engine.read_row_async(
                        &snapshot,
                        table,
                        &self.row,
                        &families,
                        &self.sel.spec,
                        RowBuf::new(info, 0),
                    )
                }
            }))
        })();
        match started {
            Ok(Err(done)) => crate::nonblocking::RowFuture::done(done),
            Ok(Ok(f)) => crate::nonblocking::RowFuture::new(Ok(f), self.row),
            Err(e) => crate::nonblocking::RowFuture::new(Err(e), self.row),
        }
    }
}

/// An ordered scan under construction. Finish with [`Scan::iter`].
///
/// ```
/// use std::ops::Bound;
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// for k in [&b"user:1"[..], b"user:2", b"user:3", b"vendor:1"] {
///     t.mutate(k).put("f", b"q", b"v").commit()?;
/// }
///
/// let mut it = t.scan_prefix(b"user:").limit(2).iter()?;
/// let mut keys = Vec::new();
/// while let Some(row) = it.next_ref()? {
///     keys.push(row.key().to_vec());
/// }
/// assert_eq!(keys, [b"user:1".to_vec(), b"user:2".to_vec()]);
///
/// // Resume after the last key.
/// let rest = t
///     .scan_bounds(Bound::Excluded(b"user:2"), Bound::Excluded(b"user;"))
///     .iter()?
///     .count();
/// assert_eq!(rest, 1);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[must_use = "a scan does nothing until .iter()"]
pub struct Scan<'t> {
    /// Borrowed from the table handle, or held ([`Scan::into_owned`]).
    core: Cow<'t, Arc<TableCore>>,
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    sel: Selection,
    limit: Option<u64>,
}

impl<'t> Scan<'t> {
    pub(crate) fn new(
        core: &'t Arc<TableCore>,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Self {
        Self {
            core: Cow::Borrowed(core),
            start,
            end,
            sel: Selection::new(),
            limit: None,
        }
    }

    selection_methods!();

    /// Up to `n` versions of each column.
    pub fn versions(mut self, n: u32) -> Self {
        self.sel.spec.versions = n;
        self
    }

    /// At most `n` columns per family in each row; the rest of each row is skipped without
    /// decoding. The same limit as [`RowRead::column_limit`] (ICR 0086).
    pub fn column_limit(mut self, n: u32) -> Self {
        self.sel.spec.columns_per_row = n;
        self
    }

    /// Stop after `n` rows.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// The scan, holding its table instead of borrowing it (ICR 0052): its [`RowIter`] is
    /// `Send + 'static`, so it can move to another thread or into a consumer that keeps it,
    /// such as an Arrow stream handed to DuckDB or Polars. It holds the table as a clone of
    /// the [`Table`](crate::Table) handle would, and keeps the database open as long.
    ///
    /// ```
    /// use pigeonhole::{Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
    /// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
    /// t.mutate(b"a").put("f", b"q", b"v").commit()?;
    ///
    /// let it = t.scan_prefix(b"").into_owned().iter()?;
    /// drop(t);
    /// let rows = std::thread::spawn(move || it.count()).join().unwrap();
    /// assert_eq!(rows, 1);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_owned(self) -> Scan<'static> {
        Scan {
            core: Cow::Owned(self.core.into_owned()),
            start: self.start,
            end: self.end,
            sel: self.sel,
            limit: self.limit,
        }
    }

    /// Starts the scan.
    pub fn iter(mut self) -> Result<RowIter<'t>> {
        let generation = self.core.db.generation();
        let (info, snapshot, families) = self.sel.start(&self.core)?;
        let mut spec = ScanSpec::new(self.start, self.end);
        spec.read = self.sel.spec;
        spec.read.families = families.into_vec();
        // `ScanSpec::limit` uses 0 for "unlimited"; `limit(0)` asks for no rows.
        spec.limit = self.limit.unwrap_or(0);
        let cursor = self
            .core
            .with(|e| e.scan(&snapshot, self.core.info.id, spec))?;
        Ok(RowIter {
            cursor,
            buf: RowBuf::new(info, 0),
            done: self.limit == Some(0),
            table: self.core,
            generation,
        })
    }

    /// The scan as a [`Stream`](futures_core::Stream) of owned [`Row`]s, in key order (see
    /// [`nonblocking`](crate::nonblocking)): each tablet is positioned without blocking, and
    /// the blocks each step will read are fetched asynchronously first, only as the stream
    /// is polled. A block a step reads that was not predicted is read synchronously and
    /// counted ([`EngineMetrics::async_sync_reads`](crate::EngineMetrics::async_sync_reads); D196,
    /// #398). An error setting the scan up is the stream's first item.
    #[cfg(feature = "async")]
    pub fn stream(mut self) -> crate::nonblocking::RowStream<'t> {
        let limit_zero = self.limit == Some(0);
        let generation = self.core.db.generation();
        let started = (|| {
            let (info, snapshot, families) = self.sel.start(&self.core)?;
            let mut spec = ScanSpec::new(self.start, self.end);
            spec.read = self.sel.spec;
            spec.read.families = families.into_vec();
            spec.limit = self.limit.unwrap_or(0);
            let cursor = self
                .core
                .with(|e| e.scan_async(&snapshot, self.core.info.id, spec))?;
            Ok((cursor, RowBuf::new(info, 0)))
        })();
        crate::nonblocking::RowStream::new(started, limit_zero, self.core, generation)
    }
}

/// Reads the cells of the row `cursor` is on into `buf` (cleared first).
#[cfg(feature = "async")]
pub(crate) fn fill_row(cursor: &mut ScanCursor, buf: &mut RowBuf) -> Result<()> {
    buf.clear();
    buf.key.extend_from_slice(cursor.row());
    while cursor.push_next_cell(buf)? {}
    Ok(())
}

/// Rows of a scan, in key order. As an [`Iterator`] it yields owned [`Row`]s (cheap: values
/// stay pinned, not copied); [`RowIter::next_ref`] lends zero-copy [`RowRef`]s instead.
///
/// Once the database is closed through any handle, the next step fails with `Closed` and
/// the iterator is finished (#826), owned ones ([`Scan::into_owned`]) included.
///
/// See [`Scan`] for an example.
#[derive(Debug)]
pub struct RowIter<'t> {
    cursor: ScanCursor,
    buf: RowBuf,
    done: bool,
    /// The table: borrowed, or held by an owned scan. Its database's closed flag stops
    /// the iterator (#826).
    table: Cow<'t, Arc<TableCore>>,
    /// The engine generation the scan started in (D265): a step after the database
    /// recovered onto a newer engine fails with `SnapshotExpired`.
    generation: u64,
}

impl RowIter<'_> {
    /// Reads the next row into `buf`. Returns `false` at the end. After an error the
    /// iterator is finished.
    fn fill(&mut self) -> Result<bool> {
        if self.done {
            return Ok(false);
        }
        // After `close` the engine's files may already belong to a new writer, which can
        // reuse the pages this cursor would read next (#826): fail with `Closed`, as every
        // handle does; after a recovery onto a newer engine, `SnapshotExpired` (D265). One
        // check, one load: the closed flag lives in the generation word.
        let r = self
            .table
            .db
            .check_generation(self.generation)
            .and_then(|()| self.fill_inner());
        if !matches!(r, Ok(true)) {
            self.done = true;
        }
        r
    }

    fn fill_inner(&mut self) -> Result<bool> {
        self.buf.clear();
        if !self.cursor.next_row()? {
            return Ok(false);
        }
        self.buf.key.extend_from_slice(self.cursor.row());
        while self.cursor.push_next_cell(&mut self.buf)? {}
        Ok(true)
    }

    /// The next row, borrowed from the iterator until the next call.
    pub fn next_ref(&mut self) -> Result<Option<RowRef<'_>>> {
        Ok(if self.fill()? {
            Some(RowRef::borrowed(&self.buf))
        } else {
            None
        })
    }
}

impl Iterator for RowIter<'_> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.fill() {
            Ok(true) => {
                let next = self.buf.empty_like();
                Some(Ok(Row::new(std::mem::replace(&mut self.buf, next))))
            }
            Ok(false) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pigeonhole_engine::{FamilyOptions, ValueRef, WriteBatch};

    use super::Selection;
    use crate::{Family, Options, Pigeonhole};

    /// #392 review: a read as of now resolves its families from the catalog before the
    /// engine loads its view. A family added (and written) in between must not come back as
    /// a cell the catalog entry cannot name (family `""`): without a selection, the read
    /// names the entry's families.
    #[test]
    fn a_family_added_before_the_view_loads_is_not_read_unnamed() {
        let dir = crate::doc_support::temp_dir();
        let db = Pigeonhole::open(dir.join("t.phdb"), Options::default().shards(1)).unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("f", Family::default())
            .create_if_missing()
            .unwrap();
        t.mutate(b"r").put("f", b"q", b"v").commit().unwrap();
        let engine = t.core.engine().to_arc();
        let table = t.core.info.id;
        engine.on_next(
            pigeonhole_engine::HookSite::LatestViewLoad,
            Box::new({
                let engine = Arc::clone(&engine);
                move || {
                    let info = engine
                        .add_family(table, "x", FamilyOptions::default())
                        .unwrap();
                    let x = info.family("x").unwrap().id;
                    let mut wb = WriteBatch::new();
                    wb.put(table, x, b"r", b"q", None, ValueRef::Bytes(b"new"))
                        .unwrap();
                    engine.commit(wb, None).unwrap();
                }
            }),
        );
        let row = t.row(b"r").read().unwrap().unwrap();
        let families: Vec<&str> = row.iter().map(|e| e.family).collect();
        assert_eq!(
            families,
            ["f"],
            "a cell of a family the read could not name"
        );
        // The next read's catalog has `x`.
        let row = t.row(b"r").read().unwrap().unwrap();
        let families: Vec<&str> = row.iter().map(|e| e.family).collect();
        assert_eq!(families, ["f", "x"]);
        db.close().unwrap();
    }

    #[test]
    fn family_names_come_back_in_order_inline_or_spilled() {
        let mut sel = Selection::new();
        assert_eq!(sel.family_names().count(), 0);
        sel.push_family("f");
        sel.push_family("");
        sel.push_family("été");
        assert_eq!(sel.family_names().collect::<Vec<_>>(), ["f", "", "été"]);
        assert!(!sel.names.spilled());
        // Past the inline bytes and the inline count: the same names, now on the heap.
        let long = "a-family-name-longer-than-the-inline-buffer";
        for _ in 0..5 {
            sel.push_family(long);
        }
        assert!(sel.names.spilled());
        let names: Vec<&str> = sel.family_names().collect();
        assert_eq!(names.len(), 8);
        assert_eq!(&names[..3], ["f", "", "été"]);
        assert!(names[3..].iter().all(|n| *n == long));
        // `families` replaces the selection.
        sel.names.clear();
        sel.ends.clear();
        sel.push_family("g");
        assert_eq!(sel.family_names().collect::<Vec<_>>(), ["g"]);
    }
}

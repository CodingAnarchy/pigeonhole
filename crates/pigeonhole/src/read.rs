use std::borrow::Cow;
use std::ops::{Bound, Range, RangeBounds};
use std::sync::Arc;

use pigeonhole_engine::{
    CellData, ColumnPosition, FamilyId, KeyCursor, KeySet, KnnSkips, KnnVisitor, QualifierFilter,
    ReadInput, ReadSpec, ReduceSpec, ScanCursor, ScanSpec, TableInfo, TopK, ValuePredicate,
};

use pigeonhole_format::vector::QueryBuf;
use smallvec::SmallVec;

use crate::cell::RowBuf;
use crate::table::TableCore;
use crate::{CellEntry, CellRef, Result, Row, RowRef, Snapshot};

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
    /// Where an earlier walk of the row stopped ([`RowRead::resume_after`]).
    resume: Option<ColumnPosition>,
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
            resume: None,
        }
    }

    selection_methods!();

    /// Resume after `position`, the column an earlier walk of a row of this table stopped
    /// at ([`RowPages::position`], [`RowCells::position`]; D297): the families the selection
    /// names before the position's family are skipped, the position's family starts after
    /// its column (all its versions), and later families start at the selection's own
    /// qualifier bound. A position whose family the read does not select (one dropped
    /// since, or not named) fails the read with `InvalidArgument`.
    ///
    /// A chain of calls is one consistent read of the row only under one
    /// [`snapshot`](RowRead::snapshot); without one, each call reads as of its own start,
    /// so a column written or deleted between calls may be seen twice or not at all. A
    /// position from another table fails the read with `InvalidArgument`; one from another
    /// row of the same table positions this row's walk at the same column (the caller's
    /// responsibility).
    pub fn resume_after(mut self, position: &RowPosition) -> Self {
        self.resume = Some(position.0.clone());
        self
    }

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
        if self.resume.is_some() {
            return self.read_resumed();
        }
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
        // A resumed read is a walk of the row's cells (D297), read at the call.
        if self.resume.is_some() {
            return crate::nonblocking::RowFuture::done(
                self.read_resumed().map(|r| r.map(|r| r.to_owned())),
            );
        }
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

/// A multi-get under construction ([`Table::keys`](crate::Table::keys), D299). Finish with
/// [`KeysRead::read`].
///
/// It reads the rows of a [`KeySet`] in key order, under one snapshot (the one given, or one
/// taken at `read`), with the selection applied to every row. Each tablet's read is set up
/// once for all of its keys, and each row's sources move forward from the previous key's:
/// sorted keys that share blocks share their decode. Keys whose row has no cell the
/// selection keeps are skipped. The row cache is not consulted.
///
/// ```
/// use pigeonhole::{Family, KeySet, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let g = db.table("g")?.family("out", Family::default()).create_if_missing()?;
/// g.mutate(b"v:1").put("out", b"v:2", b"").put("out", b"v:3", b"").commit()?;
/// g.mutate(b"v:3").put("out", b"v:1", b"").commit()?;
///
/// // One hop of a BFS: the neighbours of every node in the frontier.
/// let frontier = KeySet::from_unsorted([&b"v:3"[..], b"v:1", b"v:9"]);
/// let mut next = Vec::new();
/// let mut rows = g.keys(&frontier).family("out").read()?;
/// while let Some((i, row)) = rows.next()? {
///     assert!(frontier.get(i) != b"v:9"); // no row: skipped
///     next.extend(row.iter().map(|e| e.qualifier.to_vec()));
/// }
/// assert_eq!(next.len(), 3);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[must_use = "a multi-get does nothing until .read()"]
pub struct KeysRead<'t> {
    core: &'t Arc<TableCore>,
    keys: KeySet,
    sel: Selection,
}

impl<'t> KeysRead<'t> {
    /// The `k` vectors nearest to the `f32` `query` in this read's one vector family (D296,
    /// D303), as a [`Nearest`] to run: exact, ranked by the family's metric, ties by row
    /// then qualifier. The read's selection applies (qualifiers, time range, value filter),
    /// with the newest version of each cell.
    pub fn nearest_f32(self, query: &[f32], k: usize) -> Nearest<'t> {
        self.nearest(QueryBuf::F32(query.into()), k)
    }

    /// [`nearest_f32`](Self::nearest_f32) for an `i8` vector family.
    pub fn nearest_i8(self, query: &[i8], k: usize) -> Nearest<'t> {
        self.nearest(QueryBuf::I8(query.into()), k)
    }

    /// [`nearest_f32`](Self::nearest_f32) for a bit vector family (`query` packed eight bits
    /// to a byte, least significant first).
    pub fn nearest_bits(self, query: &[u8], k: usize) -> Nearest<'t> {
        self.nearest(QueryBuf::Bits(query.into()), k)
    }

    fn nearest(self, query: QueryBuf, k: usize) -> Nearest<'t> {
        Nearest {
            core: Cow::Borrowed(self.core),
            input: NearestInput::Keys(Arc::new(self.keys)),
            sel: self.sel,
            query,
            k,
            parallelism: 1,
        }
    }

    pub(crate) fn new(core: &'t Arc<TableCore>, keys: &KeySet) -> Self {
        Self {
            core,
            keys: keys.clone(),
            sel: Selection::new(),
        }
    }

    selection_methods!();

    /// Up to `n` versions of each column (0: all retained).
    pub fn versions(mut self, n: u32) -> Self {
        self.sel.spec.versions = n;
        self
    }

    /// At most `n` columns per family in each row (a hub in a frontier stays a page).
    pub fn column_limit(mut self, n: u32) -> Self {
        self.sel.spec.columns_per_row = n;
        self
    }

    /// Starts the read.
    pub fn read(mut self) -> Result<KeyRows<'t>> {
        let generation = self.core.db.generation();
        let (info, snapshot, families) = self.sel.start(self.core)?;
        let mut spec = self.sel.spec;
        spec.families = families.into_vec();
        let cursor = self
            .core
            .with(|e| e.multi_get(&snapshot, self.core.info.id, self.keys, spec))?;
        Ok(KeyRows {
            cursor,
            buf: RowBuf::new(info, 0),
            core: Cow::Borrowed(self.core),
            generation,
            done: false,
        })
    }
}

/// The rows of a multi-get, in key order ([`KeysRead::read`]): each lent with its index in
/// the [`KeySet`] until the next call. One buffer is reused for every row.
///
/// With the `async` feature, [`KeyRows::next_async`] and [`KeyRows::poll_next`] read the
/// same rows without blocking on the device, and [`KeyRows::into_owned_stream`] makes a
/// [`Stream`](futures_core::Stream) of owned rows. Each key is positioned from the block
/// cache only; the blocks it lacks, and those the next few keys' reads start from, are
/// fetched asynchronously, in flight together.
#[derive(Debug)]
pub struct KeyRows<'t> {
    cursor: KeyCursor,
    buf: RowBuf,
    /// The table: borrowed, or held by an owned stream. Its database's closed flag stops
    /// the read (#826).
    core: Cow<'t, Arc<TableCore>>,
    generation: u64,
    done: bool,
}

impl<'t> KeyRows<'t> {
    /// The next row with a cell the selection keeps, and its key's index in the key set;
    /// `None` after the last. After an error the read is finished.
    #[allow(clippy::should_implement_trait)] // A lending step: an `Iterator` cannot lend.
    pub fn next(&mut self) -> Result<Option<(usize, RowRef<'_>)>> {
        if self.done {
            return Ok(None);
        }
        let r = self
            .core
            .db
            .check_generation(self.generation)
            .and_then(|()| self.fill());
        self.finish(r)
    }

    /// The row a step read, lent; the read is finished at its end or an error.
    fn finish(&mut self, r: Result<Option<usize>>) -> Result<Option<(usize, RowRef<'_>)>> {
        match r {
            Ok(Some(i)) => Ok(Some((i, RowRef::borrowed(&self.buf)))),
            Ok(None) => {
                self.done = true;
                Ok(None)
            }
            Err(e) => {
                self.done = true;
                Err(e)
            }
        }
    }

    fn fill(&mut self) -> Result<Option<usize>> {
        let Some(i) = self.cursor.next_key()? else {
            return Ok(None);
        };
        fill_key(&mut self.cursor, &mut self.buf)?;
        Ok(Some(i))
    }

    /// [`KeyRows::next`] without blocking: `Pending` while a block the next row needs is
    /// being fetched.
    #[cfg(feature = "async")]
    pub fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<(usize, RowRef<'_>)>>> {
        match self.poll_fill(cx) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(r) => std::task::Poll::Ready(self.finish(r)),
        }
    }

    /// [`KeyRows::next`] as a future that never blocks on the device
    /// ([`KeyRows::poll_next`]).
    ///
    /// ```
    /// use pigeonhole::{Family, KeySet, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
    /// let g = db.table("g")?.family("out", Family::default()).create_if_missing()?;
    /// g.mutate(b"v:1").put("out", b"v:2", b"").commit()?;
    ///
    /// let frontier = KeySet::from_unsorted([&b"v:1"[..], b"v:2"]);
    /// let mut rows = g.keys(&frontier).read()?;
    /// let mut seen = 0;
    /// # pigeonhole::doc_support::block_on(async {
    /// while let Some((i, row)) = rows.next_async().await? {
    ///     assert_eq!(frontier.get(i), b"v:1");
    ///     seen += row.iter().count();
    /// }
    /// # Ok::<(), pigeonhole::Error>(())
    /// # })?;
    /// assert_eq!(seen, 1);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "async")]
    pub async fn next_async(&mut self) -> Result<Option<(usize, RowRef<'_>)>> {
        let r = std::future::poll_fn(|cx| self.poll_fill(cx)).await;
        self.finish(r)
    }

    /// The rows as a [`Stream`](futures_core::Stream) of owned [`Row`]s with their keys'
    /// indexes, holding the table ([`KeyRows::poll_next`] reads them).
    #[cfg(feature = "async")]
    pub fn into_owned_stream(self) -> crate::nonblocking::KeyStream {
        crate::nonblocking::KeyStream::new(KeyRows {
            cursor: self.cursor,
            buf: self.buf,
            core: Cow::Owned(self.core.into_owned()),
            generation: self.generation,
            done: self.done,
        })
    }

    /// Polls the next row into the buffer: its key's index, or `None` after the last.
    #[cfg(feature = "async")]
    fn poll_fill(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<usize>>> {
        use std::task::Poll;
        if self.done {
            return Poll::Ready(Ok(None));
        }
        // As `next`: nothing is read after `close` (#826), nor after a recovery onto a newer
        // engine (D265).
        if let Err(e) = self.core.db.check_generation(self.generation) {
            return Poll::Ready(Err(e));
        }
        Poll::Ready(match self.cursor.poll_next_key(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(Some(i))) => {
                let buf = &mut self.buf;
                self.cursor.counted(|c| fill_key(c, buf)).map(|()| Some(i))
            }
            Poll::Ready(Ok(None)) => Ok(None),
            Poll::Ready(Err(e)) => Err(e.into()),
        })
    }

    /// The next row, owned, for [`KeyStream`](crate::nonblocking::KeyStream).
    #[cfg(feature = "async")]
    pub(crate) fn poll_owned(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<(usize, Row)>>> {
        use std::task::Poll;
        let r = match self.poll_fill(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(r) => r,
        };
        Poll::Ready(match r {
            Ok(Some(i)) => {
                let next = self.buf.empty_like();
                Some(Ok((i, Row::new(std::mem::replace(&mut self.buf, next)))))
            }
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        })
    }
}

/// A breadth-first traversal under construction ([`Table::traverse`](crate::Table::traverse),
/// D304). Finish with [`Traverse::run`].
///
/// A node is a row; its edges are the columns of one family, and an edge's qualifier is the
/// destination's row key. From the seeds, each hop reads the frontier's edges in key order
/// (by source row, then qualifier, the newest version of each), with the selection applied
/// to the edges, and keeps every destination not visited before, once. One snapshot covers
/// every hop. The hops run in the engine: no row is materialised, duplicates and visited
/// nodes never leave it, and a capped hub costs its first `per_node_limit` edges, not the
/// row.
///
/// - [`per_node_limit(m)`](Traverse::per_node_limit): at most the first `m` edges of each
///   node, in qualifier order (edges to visited nodes count); the rest of the row is skipped
///   with a seek.
/// - [`hop_limit(n)`](Traverse::hop_limit): at most `n` new nodes per hop, the ones whose
///   first edge comes first in that order. Deterministic, not a sample.
/// - [`max_nodes(t)`](Traverse::max_nodes): at most `t` nodes visited in all, the seeds
///   included; 10,000,000 by default. The visited set is held in memory, about (key length
///   + 48) bytes per node.
///
/// ```
/// use pigeonhole::{Family, KeySet, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let g = db.table("g")?.family("out", Family::default()).create_if_missing()?;
/// g.mutate(b"a").put("out", b"b", b"").put("out", b"c", b"").commit()?;
/// g.mutate(b"b").put("out", b"a", b"").put("out", b"d", b"").commit()?;
/// g.mutate(b"c").put("out", b"d", b"").commit()?;
///
/// let t = g.traverse(&KeySet::from_unsorted([b"a"])).family("out").hops(2).parents(true).run()?;
/// let hops = t.hops();
/// assert_eq!(hops[0].nodes().iter().collect::<Vec<_>>(), [&b"b"[..], b"c"]);
/// // `a` was visited, and `d` is reached from `b` first (b sorts before c).
/// assert_eq!(hops[1].nodes().iter().collect::<Vec<_>>(), [&b"d"[..]]);
/// assert_eq!(hops[1].parents(), Some(&[0][..]));
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[must_use = "a traversal does nothing until .run()"]
pub struct Traverse<'t> {
    core: &'t TableCore,
    seeds: KeySet,
    sel: Selection,
    hops: u32,
    per_node_limit: u32,
    hop_limit: u64,
    max_nodes: u64,
    parents: bool,
    parallelism: usize,
}

impl<'t> Traverse<'t> {
    pub(crate) fn new(core: &'t TableCore, seeds: &KeySet) -> Self {
        Self {
            core,
            seeds: seeds.clone(),
            sel: Selection::new(),
            hops: 1,
            per_node_limit: 0,
            hop_limit: 0,
            max_nodes: pigeonhole_engine::DEFAULT_MAX_NODES,
            parents: false,
            parallelism: 1,
        }
    }

    selection_methods!();

    /// Hops to take, 1 to 64 (default 1).
    pub fn hops(mut self, n: u32) -> Self {
        self.hops = n;
        self
    }

    /// At most `m` edges read from each node per hop, in qualifier order (0: all, the
    /// default).
    pub fn per_node_limit(mut self, m: u32) -> Self {
        self.per_node_limit = m;
        self
    }

    /// At most `n` new nodes per hop (0: all, the default).
    pub fn hop_limit(mut self, n: u64) -> Self {
        self.hop_limit = n;
        self
    }

    /// At most `t` nodes visited in all, the seeds included (default 10,000,000;
    /// `u64::MAX` for no limit).
    pub fn max_nodes(mut self, t: u64) -> Self {
        self.max_nodes = t;
        self
    }

    /// Record, for each node, which node of the previous hop reached it ([`Hop::parents`]).
    pub fn parents(mut self, on: bool) -> Self {
        self.parents = on;
        self
    }

    /// Read each hop on up to `n` threads owned by the call, when the frontier spans
    /// several tablets and has at least 1,000 keys per thread (default 1).
    pub fn parallel(mut self, n: usize) -> Self {
        self.parallelism = n.max(1);
        self
    }

    /// Runs the traversal. The edge family must be selected, and only it
    /// (`InvalidArgument` otherwise); empty seeds give no hops.
    pub fn run(mut self) -> Result<Traversal> {
        let (snapshot, seeds, spec) = self.start()?;
        let t = self
            .core
            .with(|e| e.traverse(&snapshot, self.core.info.id, seeds, &spec))?;
        Ok(Traversal::from_engine(t, self.parents))
    }

    /// [`Traverse::run`] as a future that never blocks on the device: each hop's keys are
    /// positioned from the block cache, with the blocks they and the next keys need fetched
    /// asynchronously, and a poll yields after 4,096 edges read, so a hub never holds the
    /// executor's thread for long (edges the selection drops on the way are not counted) (cap hubs with [`per_node_limit`](Traverse::per_node_limit)
    /// all the same: edges past a key's first block are read synchronously where the cache
    /// misses). Hops run on the polling task, never fanned out; the result equals `run`'s.
    ///
    /// ```
    /// use pigeonhole::{Family, KeySet, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
    /// let g = db.table("g")?.family("out", Family::default()).create_if_missing()?;
    /// g.mutate(b"a").put("out", b"b", b"").commit()?;
    /// g.mutate(b"b").put("out", b"c", b"").commit()?;
    ///
    /// let seeds = KeySet::from_unsorted([b"a"]);
    /// let t = pigeonhole::doc_support::block_on(g.traverse(&seeds).family("out").hops(2).run_async())?;
    /// assert_eq!(t.hops().len(), 2);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "async")]
    pub async fn run_async(mut self) -> Result<Traversal> {
        let (snapshot, seeds, spec) = self.start()?;
        let pending = self
            .core
            .with(|e| e.traverse_async(&snapshot, self.core.info.id, seeds, spec))?;
        Ok(Traversal::from_engine(pending.await?, self.parents))
    }

    /// The snapshot, the seeds and the engine's spec.
    fn start(
        &mut self,
    ) -> Result<(
        pigeonhole_engine::Snapshot,
        Arc<KeySet>,
        pigeonhole_engine::TraverseSpec,
    )> {
        if self.sel.ends.len() != 1 {
            return Err(crate::Error::new(
                crate::ErrorCode::InvalidArgument,
                "a traversal reads exactly one edge family: select it with .family()",
            ));
        }
        let (_, snapshot, families) = self.sel.start(self.core)?;
        let seeds = Arc::new(std::mem::take(&mut self.seeds));
        let mut spec = pigeonhole_engine::TraverseSpec::new(families[0]);
        spec.hops = self.hops;
        spec.per_node_limit = self.per_node_limit;
        spec.hop_limit = self.hop_limit;
        spec.max_nodes = self.max_nodes;
        spec.read = self.sel.spec.clone();
        spec.parallelism = self.parallelism;
        Ok((snapshot, seeds, spec))
    }
}

impl Traversal {
    fn from_engine(t: pigeonhole_engine::Traversal, parents: bool) -> Self {
        Traversal {
            hops: t
                .hops
                .into_iter()
                .map(|h| Hop {
                    nodes: h.nodes,
                    parents: parents.then_some(h.parents),
                })
                .collect(),
            stats: TraverseStats {
                rows: t.stats.rows,
                edges: t.stats.edges,
                hops_cut: t.stats.hops_cut,
                cut_by_max_nodes: t.stats.cut_by_max_nodes,
            },
        }
    }
}

/// A traversal's result ([`Traverse::run`]): its hops, in order. There are fewer than asked
/// when a frontier ran empty or `max_nodes` was reached.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Traversal {
    hops: Vec<Hop>,
    stats: TraverseStats,
}

impl Traversal {
    /// The hops: hop 1 first.
    pub fn hops(&self) -> &[Hop] {
        &self.hops
    }

    /// Counts.
    pub fn stats(&self) -> TraverseStats {
        self.stats
    }
}

/// One hop's new nodes ([`Traversal::hops`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Hop {
    nodes: KeySet,
    parents: Option<Vec<u32>>,
}

impl Hop {
    /// The nodes first reached at this hop, sorted: ready for the next read
    /// ([`Table::keys`](crate::Table::keys), say).
    pub fn nodes(&self) -> &KeySet {
        &self.nodes
    }

    /// With [`Traverse::parents`]: for each node, the index in the previous hop's nodes (the
    /// seeds for the first hop) of the source of the first edge that reached it, enough to
    /// rebuild one shortest path per node.
    pub fn parents(&self) -> Option<&[u32]> {
        self.parents.as_deref()
    }
}

/// Counts of a traversal ([`Traversal::stats`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TraverseStats {
    /// Frontier rows with an edge the selection keeps.
    pub rows: u64,
    /// Edges read.
    pub edges: u64,
    /// Hops that `hop_limit` or `max_nodes` cut short.
    pub hops_cut: u32,
    /// Whether `max_nodes` was reached with hops left to take: a hop cut short by it, or the
    /// remaining hops skipped (they may have found nothing new).
    pub cut_by_max_nodes: bool,
}

/// Reads the cells of the key `cursor` is on into `buf` (cleared first).
fn fill_key(cursor: &mut KeyCursor, buf: &mut RowBuf) -> Result<()> {
    buf.clear();
    buf.key.extend_from_slice(cursor.row());
    while cursor.push_next_cell(buf)? {}
    Ok(())
}

/// A position within a row: the column (family and qualifier) a walk of the row handed out
/// last ([`RowPages::position`], [`RowCells::position`]), from which a later read of a row of
/// the same table resumes ([`RowRead::resume_after`], D297).
///
/// It names its table but not its row. [`RowPosition::to_bytes`] is a stable form (its first
/// byte is the encoding version, 1) to keep a position across processes or hand it over an
/// API boundary.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole, RowPosition};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let g = db.table("g")?.family("out", Family::default()).create_if_missing()?;
/// let mut m = g.mutate(b"hub");
/// for i in 0u32..10 {
///     m = m.put("out", &i.to_be_bytes(), b"");
/// }
/// m.commit()?;
///
/// // The first page, then the rest in a later call.
/// let mut pages = g.row(b"hub").family("out").pages(4)?;
/// assert_eq!(pages.next()?.unwrap().len(), 4);
/// let at = pages.position().unwrap();
/// drop(pages);
/// let token = at.to_bytes();
///
/// let at = RowPosition::from_bytes(&token)?;
/// let rest = g.row(b"hub").family("out").resume_after(&at).read()?.unwrap();
/// assert_eq!(rest.len(), 6);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowPosition(ColumnPosition);

impl RowPosition {
    /// The stable byte form: the encoding version (1), the table and family ids, the
    /// qualifier.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    /// Decodes [`RowPosition::to_bytes`]'s form; anything else fails with
    /// `InvalidArgument`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(Self(ColumnPosition::from_bytes(bytes)?))
    }
}

/// The scans one walk of a row reads, in order, from one snapshot: one over every selected
/// family, or for a resumed walk one over the position's family (past its column) and one
/// over the families after it.
fn row_walk(
    core: &TableCore,
    row: &[u8],
    mut sel: Selection,
    resume: Option<&ColumnPosition>,
) -> Result<(Arc<TableInfo>, Vec<ScanCursor>)> {
    let (info, snapshot, families) = sel.start(core)?;
    let table = core.info.id;
    let scan = |families: Vec<FamilyId>, qualifiers: QualifierFilter| -> Result<ScanCursor> {
        let mut spec = ScanSpec::new(Bound::Included(row.to_vec()), Bound::Included(row.to_vec()));
        spec.read = sel.spec.clone();
        spec.read.families = families;
        spec.read.qualifiers = qualifiers;
        spec.limit = 1;
        core.with(|e| e.scan(&snapshot, table, spec))
            .map_err(crate::Error::from)
    };
    let Some(at) = resume else {
        return Ok((
            info,
            vec![scan(families.into_vec(), sel.spec.qualifiers.clone())?],
        ));
    };
    if at.table() != table {
        return Err(crate::Error::from(
            pigeonhole_engine::Error::InvalidArgument(
                "the row position is from another table".to_owned(),
            ),
        ));
    }
    // The families in the order the walk reads them.
    let order: Vec<FamilyId> = if families.is_empty() {
        info.families.iter().map(|f| f.id).collect()
    } else {
        families.into_vec()
    };
    let mut cursors = Vec::with_capacity(2);
    let rest = match order.iter().position(|&f| f == at.family()) {
        Some(i) => {
            let past = past_column(&sel.spec.qualifiers, at.qualifier());
            cursors.push(scan(vec![at.family()], past)?);
            i + 1
        }
        // The walk's order is the selection's (or the table's), not the ids': a position
        // in a family this read does not select has no place in it (cli692's B1 on #1210).
        None => {
            return Err(crate::Error::from(
                pigeonhole_engine::Error::InvalidArgument(
                    "the row position's family is not one this read selects".to_owned(),
                ),
            ));
        }
    };
    if rest < order.len() {
        cursors.push(scan(order[rest..].to_vec(), sel.spec.qualifiers.clone())?);
    }
    Ok((info, cursors))
}

/// `selection` with its lower bound raised past `qualifier`: whichever starts later.
fn past_column(selection: &QualifierFilter, qualifier: &[u8]) -> QualifierFilter {
    let (lo, hi) = match selection {
        QualifierFilter::Range(lo, hi) => (lo.clone(), hi.clone()),
        QualifierFilter::Prefix(p) => (Bound::Included(p.clone()), prefix_end(p)),
        _ => (Bound::Unbounded, Bound::Unbounded),
    };
    let after = Bound::Excluded(qualifier.to_vec());
    let lo = match &lo {
        Bound::Included(b) if b.as_slice() > qualifier => lo,
        Bound::Excluded(b) if b.as_slice() >= qualifier => lo,
        _ => after,
    };
    QualifierFilter::Range(lo, hi)
}

/// The first qualifier past every one starting with `prefix` (unbounded when none is).
fn prefix_end(prefix: &[u8]) -> Bound<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Bound::Excluded(end);
        }
    }
    Bound::Unbounded
}

/// The cell-by-cell walk behind [`RowCells`] and [`RowPages`]: the walk's scans in order,
/// the current cell's family and qualifier, and its version in one reused cell.
#[derive(Debug)]
struct CellWalk<'t> {
    core: &'t TableCore,
    generation: u64,
    info: Arc<TableInfo>,
    /// Scans still to start, in reverse order (the next is the last).
    pending: Vec<ScanCursor>,
    /// The scan on the row, if one is.
    cursor: Option<ScanCursor>,
    qualifier: Vec<u8>,
    data: CellData,
    family: Option<FamilyId>,
    /// The index in `info` of `family` (cells come grouped by family).
    family_index: Option<(FamilyId, usize)>,
    done: bool,
}

impl<'t> CellWalk<'t> {
    fn new(
        core: &'t TableCore,
        row: &[u8],
        sel: Selection,
        resume: Option<&ColumnPosition>,
    ) -> Result<Self> {
        let generation = core.db.generation();
        let (info, mut cursors) = row_walk(core, row, sel, resume)?;
        cursors.reverse();
        Ok(Self {
            core,
            generation,
            info,
            pending: cursors,
            cursor: None,
            qualifier: Vec::new(),
            data: CellData::EMPTY,
            family: None,
            family_index: None,
            done: false,
        })
    }

    /// Steps to the next cell: `false` at the end of the walk. After an error the walk is
    /// done.
    fn step(&mut self) -> Result<bool> {
        if self.done {
            return Ok(false);
        }
        let r = self
            .core
            .db
            .check_generation(self.generation)
            .and_then(|()| self.step_inner());
        if !matches!(r, Ok(true)) {
            self.done = true;
        }
        r
    }

    fn step_inner(&mut self) -> Result<bool> {
        loop {
            let Some(cursor) = self.cursor.as_mut() else {
                let Some(mut next) = self.pending.pop() else {
                    return Ok(false);
                };
                if next.next_row()? {
                    self.cursor = Some(next);
                }
                continue;
            };
            let Some(cell) = cursor.next_cell()? else {
                self.cursor = None;
                continue;
            };
            self.qualifier.clear();
            self.qualifier.extend_from_slice(cell.qualifier);
            self.family = Some(cell.family);
            // A small value is copied into the reused cell; a large one stays pinned.
            let small = cell.stored.len() <= CellData::INLINE_MAX;
            if small {
                self.data.set_inline(cell.ts, cell.stored);
            } else {
                self.data = cursor.current_data();
            }
            return Ok(true);
        }
    }

    /// [`CellWalk::step`] straight into `buf` (the cell's qualifier unescaped into its
    /// qualifiers, a small value copied, a large one pinned): `false` at the end of the walk.
    /// `qualifier`, `data` and `family` are not set.
    fn push_step(&mut self, buf: &mut RowBuf) -> Result<bool> {
        if self.done {
            return Ok(false);
        }
        let r = self
            .core
            .db
            .check_generation(self.generation)
            .and_then(|()| {
                loop {
                    let Some(cursor) = self.cursor.as_mut() else {
                        let Some(mut next) = self.pending.pop() else {
                            return Ok(false);
                        };
                        if next.next_row()? {
                            self.cursor = Some(next);
                        }
                        continue;
                    };
                    // `next_cell` and the sink's pushes, not `ScanCursor::push_next_cell`: a
                    // third caller of that changed how it inlines into the scan's row fill
                    // (+2-3% on the scan shapes, #1227's first callgrind run).
                    let Some(cell) = cursor.next_cell()? else {
                        self.cursor = None;
                        continue;
                    };
                    let start = buf.qualifiers.len();
                    buf.qualifiers.extend_from_slice(cell.qualifier);
                    let qualifier = start..buf.qualifiers.len();
                    if cell.stored.len() <= CellData::INLINE_MAX {
                        pigeonhole_engine::RowSink::push_inline(
                            buf,
                            cell.family,
                            qualifier,
                            cell.ts,
                            cell.stored,
                        );
                    } else {
                        let family = cell.family;
                        buf.push(family, qualifier, cursor.current_data());
                    }
                    return Ok(true);
                }
            });
        if !matches!(r, Ok(true)) {
            self.done = true;
        }
        r
    }

    /// The current cell's family name.
    fn family_name(&mut self) -> &str {
        let Some(id) = self.family else {
            return "";
        };
        let i = match self.family_index {
            Some((f, i)) if f == id => i,
            _ => {
                let i = self
                    .info
                    .families
                    .iter()
                    .position(|f| f.id == id)
                    .unwrap_or(usize::MAX);
                self.family_index = Some((id, i));
                i
            }
        };
        self.info.families.get(i).map_or("", |f| f.name.as_str())
    }

    fn position(&self) -> Option<RowPosition> {
        self.family.map(|f| {
            RowPosition(ColumnPosition::new(
                self.core.info.id,
                f,
                self.qualifier.clone(),
            ))
        })
    }
}

/// The cells of one row, one at a time ([`RowRead::cells`], D297): for a row too wide to
/// read whole, such as a graph hub with millions of edges. Each cell is lent until the next
/// call, so the walk allocates nothing per cell, and a large value stays pinned rather than
/// copied. Cells come in [`RowRead::read`]'s order: by family, then qualifier, then newest
/// version first.
///
/// The walk holds its snapshot, and with it the files it reads, until it is dropped. After
/// the database is closed, the next step fails with `Closed` (#826).
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let g = db.table("g")?.family("out", Family::default()).create_if_missing()?;
/// let mut m = g.mutate(b"hub");
/// for i in 0u64..1000 {
///     m = m.put("out", &i.to_be_bytes(), b"");
/// }
/// m.commit()?;
///
/// let mut degree = 0;
/// let mut cells = g.row(b"hub").family("out").cells()?;
/// while let Some(e) = cells.next()? {
///     assert_eq!(e.family, "out");
///     degree += 1;
/// }
/// assert_eq!(degree, 1000);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[must_use = "a cell walk reads nothing until .next()"]
pub struct RowCells<'t> {
    walk: CellWalk<'t>,
}

impl RowCells<'_> {
    /// The next cell, lent until the next call; `None` at the end of the row. After an
    /// error the walk is finished.
    #[allow(clippy::should_implement_trait)] // A lending step: an `Iterator` cannot lend.
    pub fn next(&mut self) -> Result<Option<CellEntry<'_>>> {
        if !self.walk.step()? {
            return Ok(None);
        }
        let walk = &mut self.walk;
        let _ = walk.family_name();
        let family = walk
            .family_index
            .and_then(|(_, i)| walk.info.families.get(i))
            .map_or("", |f| f.name.as_str());
        Ok(Some(CellEntry {
            family,
            qualifier: &walk.qualifier,
            cell: CellRef::borrowed(&walk.data),
        }))
    }

    /// The column of the cell handed out last, to resume after
    /// ([`RowRead::resume_after`]). Resuming after it skips that column's remaining
    /// versions: [`RowRead::pages`] ends its pages on column boundaries for that reason.
    pub fn position(&self) -> Option<RowPosition> {
        self.walk.position()
    }
}

/// Pages of one row's cells, of at most `n` cells each ([`RowRead::pages`], D297), from one
/// walk under one snapshot. A page holds whole columns only, so all the versions of a column
/// the selection keeps are on one page; a column whose versions alone exceed `n` is a page
/// of its own. One buffer is reused for every page: past the first page a page
/// allocates nothing.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let g = db.table("g")?.family("out", Family::default()).create_if_missing()?;
/// let mut m = g.mutate(b"hub");
/// for i in 0u64..2500 {
///     m = m.put("out", &i.to_be_bytes(), b"");
/// }
/// m.commit()?;
///
/// let mut sizes = Vec::new();
/// let mut pages = g.row(b"hub").family("out").pages(1000)?;
/// while let Some(page) = pages.next()? {
///     sizes.push(page.len());
/// }
/// assert_eq!(sizes, [1000, 1000, 500]);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[must_use = "a page walk reads nothing until .next()"]
pub struct RowPages<'t> {
    walk: CellWalk<'t>,
    row: Vec<u8>,
    n: usize,
    buf: RowBuf,
    /// The versions of the column that starts the next page (its family as an index into
    /// the table's families) and its qualifier: a column that did not fit this one.
    carry: Vec<(usize, CellData)>,
    carry_qualifier: Vec<u8>,
    /// The last column on the pages handed out.
    last: Option<FamilyId>,
    last_qualifier: Vec<u8>,
}

impl RowPages<'_> {
    /// The next page, lent until the next call; `None` at the end of the row. After an
    /// error the walk is finished.
    #[allow(clippy::should_implement_trait)] // A lending step: an `Iterator` cannot lend.
    pub fn next(&mut self) -> Result<Option<RowRef<'_>>> {
        self.buf.clear();
        self.buf.key.extend_from_slice(&self.row);
        self.buf.push_column(&mut self.carry, &self.carry_qualifier);
        // Cells go straight into the page. A column boundary shows as a cell whose column
        // differs from the one before; the column it starts goes to the next page when this
        // one is full, and so does a column that grows past `n` on a page it did not start.
        let mut col_start = 0;
        loop {
            let before = self.buf.len();
            if !self.walk.push_step(&mut self.buf)? {
                break;
            }
            if before == 0 {
                continue;
            }
            if !self.buf.same_column(before - 1, before) {
                if before >= self.n {
                    self.buf
                        .split_off_column(before, &mut self.carry, &mut self.carry_qualifier);
                    break;
                }
                col_start = before;
            } else if col_start > 0 && self.buf.len() > self.n {
                self.buf
                    .split_off_column(col_start, &mut self.carry, &mut self.carry_qualifier);
                break;
            }
        }
        if let Some((family, qualifier)) = self.buf.last_column() {
            self.last = Some(family);
            self.last_qualifier.clear();
            self.last_qualifier.extend_from_slice(qualifier);
        }
        Ok((self.buf.len() > 0).then(|| RowRef::borrowed(&self.buf)))
    }

    /// The last column on the pages handed out so far, to resume after in a later read
    /// ([`RowRead::resume_after`]).
    pub fn position(&self) -> Option<RowPosition> {
        self.last.map(|f| {
            RowPosition(ColumnPosition::new(
                self.walk.core.info.id,
                f,
                self.last_qualifier.clone(),
            ))
        })
    }
}

fn push_cell(buf: &mut RowBuf, family: FamilyId, qualifier: &[u8], data: CellData) {
    let start = buf.qualifiers.len();
    buf.qualifiers.extend_from_slice(qualifier);
    let end = buf.qualifiers.len();
    buf.push(family, start..end, data);
}

impl<'t> RowRead<'t> {
    /// The row's cells, one at a time ([`RowCells`], D297): for a row too wide to read
    /// whole. It reads like a scan of one row (the row cache is not consulted), with this
    /// read's selection, versions and column limit. The column limit counts columns per
    /// family, per call: a walk resumed in a later call counts afresh.
    pub fn cells(self) -> Result<RowCells<'t>> {
        let walk = CellWalk::new(self.core, &self.row, self.sel, self.resume.as_ref())?;
        Ok(RowCells { walk })
    }

    /// The row's cells in pages of at most `n` cells, each ending at a column boundary
    /// ([`RowPages`], D297). `n` of 0 is taken as 1. The column limit counts columns per
    /// family over the whole walk (not per page), and afresh in each resumed call.
    pub fn pages(self, n: usize) -> Result<RowPages<'t>> {
        let walk = CellWalk::new(self.core, &self.row, self.sel, self.resume.as_ref())?;
        let buf = RowBuf::new(Arc::clone(&walk.info), 0);
        Ok(RowPages {
            walk,
            row: self.row,
            n: n.max(1),
            buf,
            carry: Vec::new(),
            carry_qualifier: Vec::new(),
            last: None,
            last_qualifier: Vec::new(),
        })
    }

    /// [`RowRead::read`] after [`RowRead::resume_after`]: the walk's cells into one row.
    fn read_resumed(self) -> Result<Option<RowRef<'t>>> {
        let row = self.row.clone();
        let mut walk = CellWalk::new(self.core, &self.row, self.sel, self.resume.as_ref())?;
        let mut buf = RowBuf::new(Arc::clone(&walk.info), 0);
        while walk.step()? {
            let family = walk.family.expect("set by a step");
            let data = std::mem::replace(&mut walk.data, CellData::EMPTY);
            push_cell(&mut buf, family, &walk.qualifier, data);
        }
        if buf.len() == 0 {
            return Ok(None);
        }
        buf.key = row;
        Ok(Some(RowRef::owned(buf)))
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
    /// The `k` vectors nearest to the `f32` `query` in this read's one vector family (D296,
    /// D303), as a [`Nearest`] to run: exact, ranked by the family's metric, ties by row
    /// then qualifier. The read's selection applies (qualifiers, time range, value filter),
    /// with the newest version of each cell.
    pub fn nearest_f32(self, query: &[f32], k: usize) -> Nearest<'t> {
        self.nearest(QueryBuf::F32(query.into()), k)
    }

    /// [`nearest_f32`](Self::nearest_f32) for an `i8` vector family.
    pub fn nearest_i8(self, query: &[i8], k: usize) -> Nearest<'t> {
        self.nearest(QueryBuf::I8(query.into()), k)
    }

    /// [`nearest_f32`](Self::nearest_f32) for a bit vector family (`query` packed eight bits
    /// to a byte, least significant first).
    pub fn nearest_bits(self, query: &[u8], k: usize) -> Nearest<'t> {
        self.nearest(QueryBuf::Bits(query.into()), k)
    }

    fn nearest(self, query: QueryBuf, k: usize) -> Nearest<'t> {
        Nearest {
            core: self.core,
            input: NearestInput::Range(self.start, self.end),
            sel: self.sel,
            query,
            k,
            parallelism: 1,
        }
    }

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

    /// The scan's cells one at a time, row after row ([`ScanCells`], D297): a scan over rows
    /// that may be too wide to read whole. Each cell is lent until the next call.
    pub fn cells(mut self) -> Result<ScanCells<'t>> {
        let generation = self.core.db.generation();
        let (info, snapshot, families) = self.sel.start(&self.core)?;
        let mut spec = ScanSpec::new(self.start, self.end);
        spec.read = self.sel.spec;
        spec.read.families = families.into_vec();
        spec.limit = self.limit.unwrap_or(0);
        let cursor = self
            .core
            .with(|e| e.scan(&snapshot, self.core.info.id, spec))?;
        Ok(ScanCells {
            cursor,
            info,
            table: self.core,
            generation,
            in_row: false,
            done: self.limit == Some(0),
            qualifier: Vec::new(),
            data: CellData::EMPTY,
            family: None,
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

/// A scan's cells one at a time, row after row ([`Scan::cells`], D297). Each cell and its
/// row key are lent until the next call, so the walk allocates nothing per cell; a large
/// value stays pinned rather than copied. Rows come in key order; within a row, cells come
/// by family, then qualifier, then newest version first.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let g = db.table("g")?.family("out", Family::default()).create_if_missing()?;
/// g.mutate(b"a").put("out", b"b", b"").put("out", b"c", b"").commit()?;
/// g.mutate(b"b").put("out", b"c", b"").commit()?;
///
/// let mut edges = Vec::new();
/// let mut cells = g.scan_prefix(b"").family("out").cells()?;
/// while let Some((row, e)) = cells.next()? {
///     edges.push((row.to_vec(), e.qualifier.to_vec()));
/// }
/// assert_eq!(edges.len(), 3);
/// assert_eq!(edges[2], (b"b".to_vec(), b"c".to_vec()));
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[must_use = "a cell walk reads nothing until .next()"]
pub struct ScanCells<'t> {
    cursor: ScanCursor,
    info: Arc<TableInfo>,
    table: Cow<'t, Arc<TableCore>>,
    generation: u64,
    in_row: bool,
    done: bool,
    qualifier: Vec<u8>,
    data: CellData,
    /// The current cell's family, and its index in `info`.
    family: Option<(FamilyId, usize)>,
}

impl ScanCells<'_> {
    /// The next cell and its row key, lent until the next call; `None` at the end of the
    /// scan. After an error the walk is finished.
    #[allow(clippy::should_implement_trait)] // A lending step: an `Iterator` cannot lend.
    pub fn next(&mut self) -> Result<Option<(&[u8], CellEntry<'_>)>> {
        if self.done {
            return Ok(None);
        }
        let r = self
            .table
            .db
            .check_generation(self.generation)
            .and_then(|()| self.step());
        if !matches!(r, Ok(true)) {
            self.done = true;
        }
        if !r? {
            return Ok(None);
        }
        let family = self
            .family
            .and_then(|(_, i)| self.info.families.get(i))
            .map_or("", |f| f.name.as_str());
        Ok(Some((
            self.cursor.row(),
            CellEntry {
                family,
                qualifier: &self.qualifier,
                cell: CellRef::borrowed(&self.data),
            },
        )))
    }

    fn step(&mut self) -> Result<bool> {
        loop {
            if !self.in_row {
                if !self.cursor.next_row()? {
                    return Ok(false);
                }
                self.in_row = true;
            }
            let Some(cell) = self.cursor.next_cell()? else {
                self.in_row = false;
                continue;
            };
            self.qualifier.clear();
            self.qualifier.extend_from_slice(cell.qualifier);
            let id = cell.family;
            let small = cell.stored.len() <= CellData::INLINE_MAX;
            if small {
                self.data.set_inline(cell.ts, cell.stored);
            } else {
                self.data = self.cursor.current_data();
            }
            if self.family.is_none_or(|(f, _)| f != id) {
                let i = self
                    .info
                    .families
                    .iter()
                    .position(|f| f.id == id)
                    .unwrap_or(usize::MAX);
                self.family = Some((id, i));
            }
            return Ok(true);
        }
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

/// One vector of a nearest-neighbour search ([`Nearest::run`]): the cell's row, qualifier
/// and timestamp, and its distance to the query under the family's metric (smaller is
/// nearer; exact for `i8` and bit vectors).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Neighbour {
    /// The row key.
    pub row: Vec<u8>,
    /// The qualifier.
    pub qualifier: Vec<u8>,
    /// The distance, as [`VectorSpec::distance_f32`](crate::VectorSpec::distance_f32) and
    /// the other forms give it.
    pub distance: f64,
    /// The version's timestamp.
    pub ts: u64,
}

/// What a nearest-neighbour search reads.
#[derive(Debug)]
enum NearestInput {
    Range(Bound<Vec<u8>>, Bound<Vec<u8>>),
    Keys(Arc<KeySet>),
}

/// An exact nearest-neighbour search under construction ([`Scan::nearest_f32`],
/// [`KeysRead::nearest_f32`] and their `i8` and bit forms; D296 PR 4, D303). Finish with
/// [`run`](Nearest::run).
///
/// It reads every selected cell of the one vector family the read names and keeps the `k`
/// nearest, in the engine, without materialising rows. The order is total: distance, then
/// row key, then qualifier, the same at any [`parallel`](Nearest::parallel). The query
/// must have the family's element type and dimension, with finite components (at most
/// 2^56 in magnitude) and, under cosine, a nonzero norm; otherwise `InvalidArgument`.
///
/// ```
/// use pigeonhole::{Element, Family, KeySet, Metric, Options, Pigeonhole, VectorSpec};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
/// let spec = VectorSpec::new(2, Element::F32, Metric::L2)?;
/// let t = db.table("docs")?.family("emb", Family::vector(spec)).create_if_missing()?;
/// for (row, v) in [(b"a", [0.0, 0.0]), (b"b", [1.0, 0.0]), (b"c", [5.0, 5.0])] {
///     t.mutate(row).put_vector_f32("emb", b"", &v).commit()?;
/// }
/// let near = t.scan_prefix(b"").family("emb").nearest_f32(&[0.9, 0.0], 2).run()?;
/// let rows: Vec<&[u8]> = near.iter().map(|n| n.row.as_slice()).collect();
/// assert_eq!(rows, [&b"b"[..], b"a"]);
///
/// // Over chosen rows: a key-set read.
/// let some = KeySet::from_unsorted([b"a", b"c"]);
/// let near = t.keys(&some).family("emb").nearest_f32(&[4.0, 4.0], 1).run()?;
/// assert_eq!(near[0].row, b"c");
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[must_use = "a search does nothing until .run()"]
pub struct Nearest<'t> {
    core: Cow<'t, Arc<TableCore>>,
    input: NearestInput,
    sel: Selection,
    query: QueryBuf,
    k: usize,
    parallelism: usize,
}

impl Nearest<'_> {
    /// Search on up to `n` threads owned by the call (default 1), over the input's tablets:
    /// a range fans out past the engine's work floor, and a key set (a few wide rows, as an
    /// IVF index's lists) whenever its keys span more than one tablet. The result is the
    /// same at any `n`.
    pub fn parallel(mut self, n: usize) -> Self {
        self.parallelism = n.max(1);
        self
    }

    /// Runs the search: at most `k` neighbours, nearest first. The read must name exactly
    /// one family, a vector family, and the query must fit it (`InvalidArgument`).
    pub fn run(mut self) -> Result<Vec<Neighbour>> {
        let invalid = |m: String| crate::Error::new(crate::ErrorCode::InvalidArgument, m);
        if self.sel.ends.len() != 1 {
            return Err(invalid(
                "nearest reads exactly one vector family: select it with .family()".into(),
            ));
        }
        let core: &TableCore = &self.core;
        let (info, snapshot, families) = self.sel.start(core)?;
        let family = families[0];
        let options = &info
            .families
            .iter()
            .find(|f| f.id == family)
            .ok_or_else(|| invalid("the family is not in the table".into()))?
            .options;
        let name = self
            .sel
            .family_names()
            .next()
            .unwrap_or_default()
            .to_owned();
        let params = (options.kind == pigeonhole_engine::FamilyKind::Vector)
            .then(|| pigeonhole_format::vector::Params::parse(&options.kind_params).ok())
            .flatten()
            .ok_or_else(|| invalid(format!("family {name:?} is not a vector family")))?;
        let skips = Arc::new(KnnSkips::default());
        // Checked and prepared once (the query's norm too); every group's visitor shares it.
        let prepared = params
            .prepare(self.query.clone())
            .map_err(|e| invalid(format!("family {name:?}: the query: {e}")))?;
        if self.k == 0 {
            return Ok(Vec::new());
        }
        let keys = matches!(self.input, NearestInput::Keys(_));
        let input = match std::mem::replace(&mut self.input, NearestInput::Keys(Arc::default())) {
            NearestInput::Range(lo, hi) => ReadInput::Ranges(vec![(lo, hi)]),
            NearestInput::Keys(k) => ReadInput::Keys(k),
        };
        let mut spec = ReduceSpec::new(input);
        let mut read = self.sel.spec.clone();
        read.families = vec![family];
        read.versions = 1;
        spec.read = read;
        spec.parallelism = self.parallelism;
        if keys && self.parallelism > 1 {
            // A key set's floor counts keys; an IVF query reads a few wide rows (#490 D).
            spec.parallel_floor = 0;
        }
        let k = self.k;
        let reduced = core.with(|e| {
            e.reduce(
                &snapshot,
                core.info.id,
                &spec,
                || KnnVisitor::typed(prepared.clone(), Arc::clone(&skips)),
                || TopK::new(k),
            )
        })?;
        Ok(reduced
            .items
            .into_iter()
            .map(|(cell, score)| Neighbour {
                row: cell.row,
                qualifier: cell.qualifier,
                distance: score.0,
                ts: cell.ts,
            })
            .collect())
    }
}

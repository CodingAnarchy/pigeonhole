use std::ops::{Bound, Range, RangeBounds};
use std::sync::Arc;

use pigeonhole_engine::{
    FamilyId, QualifierFilter, ReadSpec, ScanCursor, ScanSpec, TableInfo, ValuePredicate,
};

use smallvec::SmallVec;

use crate::cell::RowBuf;
use crate::table::{TableCore, family_not_found};
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
        match self {
            ValueFilter::Equals(v) => ValuePredicate::Equals(v.clone()),
            ValueFilter::Prefix(v) => ValuePredicate::Prefix(v.clone()),
            ValueFilter::I64(o, v) => ValuePredicate::I64(*o, *v),
        }
    }
}

/// A condition for a conditional mutation ([`RowMutation::commit_if`](crate::RowMutation::commit_if)).
///
/// See [`RowMutation`](crate::RowMutation) for an example.
#[derive(Debug, Clone, PartialEq)]
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

    /// Resolves the projection against the catalog as of now and takes the snapshot. Returns
    /// the catalog entry, which names every family the read can return.
    /// The snapshot, the table's info, and the selected families' ids (empty: every family).
    fn start(
        &mut self,
        core: &TableCore,
    ) -> Result<(
        Arc<TableInfo>,
        pigeonhole_engine::Snapshot,
        SmallVec<[FamilyId; 4]>,
    )> {
        core.db.check_open()?;
        // The snapshot first: the catalog read afterwards includes every family it can see.
        let snapshot = match self.snapshot.take() {
            Some(s) => {
                core.db.check_snapshot(&s)?;
                s.inner
            }
            None => core.db.engine.snapshot()?,
        };
        let info = core.current_info();
        let mut ids = SmallVec::new();
        for name in self.family_names() {
            let id = info
                .family(name)
                .ok_or_else(|| family_not_found(&info.name, name))?
                .id;
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
            self.sel.spec.value = Some(filter.to_engine());
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

    /// At most `n` columns per family.
    pub fn column_limit(mut self, n: u32) -> Self {
        self.sel.spec.columns_per_row = n;
        self
    }

    /// Performs the read. `None` if the row has no matching cell.
    pub fn read(mut self) -> Result<Option<RowRef<'t>>> {
        let (info, snapshot, families) = self.sel.start(self.core)?;
        // The engine resolves the cells straight into the row this returns (#287): no
        // intermediate row and no second copy of each cell.
        let mut buf = RowBuf::new(info, 0);
        let any = self.core.db.engine.read_row_into_families(
            &snapshot,
            self.core.info.id,
            &self.row,
            &families,
            &self.sel.spec,
            &mut buf,
        )?;
        if !any {
            return Ok(None);
        }
        buf.key = self.row;
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
    core: &'t TableCore,
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    sel: Selection,
    limit: Option<u64>,
}

impl<'t> Scan<'t> {
    pub(crate) fn new(core: &'t TableCore, start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Self {
        Self {
            core,
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

    /// At most `n` columns per family per row; the rest of each row is skipped without
    /// decoding.
    pub fn columns_per_row(mut self, n: u32) -> Self {
        self.sel.spec.columns_per_row = n;
        self
    }

    /// Stop after `n` rows.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// Starts the scan.
    pub fn iter(mut self) -> Result<RowIter<'t>> {
        let (info, snapshot, families) = self.sel.start(self.core)?;
        let mut spec = ScanSpec::new(self.start, self.end);
        spec.read = self.sel.spec;
        spec.read.families = families.into_vec();
        // `ScanSpec::limit` uses 0 for "unlimited"; `limit(0)` asks for no rows.
        spec.limit = self.limit.unwrap_or(0);
        let cursor = self
            .core
            .db
            .engine
            .scan(&snapshot, self.core.info.id, spec)?;
        Ok(RowIter {
            cursor,
            buf: RowBuf::new(info, 0),
            done: self.limit == Some(0),
            _table: std::marker::PhantomData,
        })
    }
}

/// Rows of a scan, in key order. As an [`Iterator`] it yields owned [`Row`]s (cheap: values
/// stay pinned, not copied); [`RowIter::next_ref`] lends zero-copy [`RowRef`]s instead.
///
/// See [`Scan`] for an example.
#[derive(Debug)]
pub struct RowIter<'t> {
    cursor: ScanCursor,
    buf: RowBuf,
    done: bool,
    _table: std::marker::PhantomData<&'t TableCore>,
}

impl RowIter<'_> {
    /// Reads the next row into `buf`. Returns `false` at the end. After an error the
    /// iterator is finished.
    fn fill(&mut self) -> Result<bool> {
        if self.done {
            return Ok(false);
        }
        let r = self.fill_inner();
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
        loop {
            let start = self.buf.qualifiers.len();
            let Some(family) = self.cursor.next_cell_into(&mut self.buf.qualifiers)? else {
                break;
            };
            let end = self.buf.qualifiers.len();
            self.cursor.push_current(family, start..end, &mut self.buf);
        }
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
    use super::Selection;

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

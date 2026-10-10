use std::borrow::Cow;
use std::ops::Range;
use std::sync::Arc;

use pigeonhole_engine::{CellData, FamilyId, TableInfo, ValueRef};

/// A typed view of a value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value<'a> {
    /// Opaque bytes.
    Bytes(&'a [u8]),
    /// A signed 64-bit integer (counters).
    I64(i64),
    /// A 64-bit float.
    F64(f64),
    /// A varint-encoded signed integer.
    Varint(i64),
}

/// The value bytes of a stored value: everything after the tag byte.
#[inline]
fn payload(data: &CellData) -> &[u8] {
    data.stored().get(1..).unwrap_or_default()
}

fn typed(data: &CellData) -> Value<'_> {
    match data.value() {
        ValueRef::Bytes(b) => Value::Bytes(b),
        ValueRef::I64(v) => Value::I64(v),
        ValueRef::F64(v) => Value::F64(v),
        ValueRef::Varint(v) => Value::Varint(v),
        // The engine resolves blob pointers on read; never reached.
        ValueRef::Blob(_) => Value::Bytes(&[]),
    }
}

fn as_i64(data: &CellData) -> Option<i64> {
    match data.value() {
        ValueRef::I64(v) | ValueRef::Varint(v) => Some(v),
        _ => None,
    }
}

/// A borrowed cell version, tied to the table handle that read it. Reading it allocates
/// nothing; the bytes stay pinned until it drops. [`CellRef::to_owned`] keeps them longer.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole, Value};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// t.mutate(b"row").put("f", b"name", b"Ada").put_i64("f", b"age", 36).commit()?;
///
/// let name = t.get(b"row", "f", b"name")?.unwrap();
/// assert_eq!(name.value(), b"Ada");
/// assert_eq!(name.typed(), Value::Bytes(b"Ada"));
/// let age = t.get(b"row", "f", b"age")?.unwrap();
/// assert_eq!(age.as_i64(), Some(36));
/// assert_eq!(age.value(), 36i64.to_le_bytes());
///
/// // Keep a cell past the borrow of the table.
/// let owned = name.to_owned();
/// drop(t);
/// assert_eq!(owned.value(), b"Ada");
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct CellRef<'a> {
    data: Cow<'a, CellData>,
}

impl<'a> CellRef<'a> {
    #[inline]
    pub(crate) fn owned(data: CellData) -> Self {
        Self {
            data: Cow::Owned(data),
        }
    }

    pub(crate) fn borrowed(data: &'a CellData) -> Self {
        Self {
            data: Cow::Borrowed(data),
        }
    }

    /// The value bytes (for typed values, their encoding without the tag).
    #[inline]
    pub fn value(&self) -> &[u8] {
        payload(&self.data)
    }

    /// The typed value.
    pub fn typed(&self) -> Value<'_> {
        typed(&self.data)
    }

    /// The value as an `i64`, if it is one.
    pub fn as_i64(&self) -> Option<i64> {
        as_i64(&self.data)
    }

    /// The version's timestamp (microseconds since the Unix epoch by default).
    pub fn timestamp(&self) -> u64 {
        self.data.timestamp()
    }

    /// An owned handle that can outlive the table borrow (no copy of the value).
    pub fn to_owned(&self) -> Cell {
        Cell {
            data: CellData::clone(&self.data),
        }
    }
}

/// An owned cell version: a cheap ref-counted handle that pins its cache block. `Send`,
/// `Sync`, `'static`; safe to hold across `.await`.
///
/// ```
/// # fn assert_owned<T: Send + Sync + 'static>() {}
/// assert_owned::<pigeonhole::Cell>();
/// ```
#[derive(Debug, Clone)]
pub struct Cell {
    data: CellData,
}

impl Cell {
    #[cfg(feature = "async")]
    pub(crate) fn from_data(data: CellData) -> Self {
        Self { data }
    }

    /// The value bytes.
    #[inline]
    pub fn value(&self) -> &[u8] {
        payload(&self.data)
    }

    /// The typed value.
    pub fn typed(&self) -> Value<'_> {
        typed(&self.data)
    }

    /// The value as an `i64`, if it is one.
    pub fn as_i64(&self) -> Option<i64> {
        as_i64(&self.data)
    }

    /// The version's timestamp.
    pub fn timestamp(&self) -> u64 {
        self.data.timestamp()
    }
}

/// One entry of a row: which column, and which version.
#[derive(Debug, Clone)]
pub struct CellEntry<'a> {
    /// Family name.
    pub family: &'a str,
    /// Qualifier.
    pub qualifier: &'a [u8],
    /// The version.
    pub cell: CellRef<'a>,
}

/// One cell of a row: its family (an index into the row's catalog entry), its qualifier as
/// a range of the row's qualifier buffer, and the version.
#[derive(Debug, Clone)]
pub(crate) struct RowCell {
    family: usize,
    qualifier: Range<usize>,
    cell: Cell,
}

/// The storage behind [`Row`] and [`RowRef`]: a few buffers per row, none per cell.
#[derive(Debug, Clone, Default)]
pub(crate) struct RowBuf {
    /// Names the families (a catalog entry that includes every family read).
    pub(crate) info: Option<Arc<TableInfo>>,
    pub(crate) key: Vec<u8>,
    /// Every qualifier of the row, concatenated.
    pub(crate) qualifiers: Vec<u8>,
    cells: Vec<RowCell>,
    /// The family pushed last and its index in `info`: cells come grouped by family, so
    /// resolving one is a lookup per family run, not per cell.
    last_family: Option<(FamilyId, usize)>,
}

impl pigeonhole_engine::RowSink for RowBuf {
    fn qualifiers(&mut self) -> &mut Vec<u8> {
        &mut self.qualifiers
    }

    fn push(&mut self, family: FamilyId, qualifier: Range<usize>, data: CellData) {
        RowBuf::push(self, family, qualifier, data);
    }

    // Inlined into the row read's per-cell loop: with the row cache's hit path as a second
    // caller, a plain `#[inline]` stopped being honored there (#404, +0.7% on ycsb-a).
    #[inline(always)]
    fn push_inline(&mut self, family: FamilyId, qualifier: Range<usize>, ts: u64, stored: &[u8]) {
        let family = self.family_index(family);
        self.cells.push(RowCell {
            family,
            qualifier,
            cell: Cell {
                data: CellData::EMPTY,
            },
        });
        if let Some(c) = self.cells.last_mut() {
            c.cell.data.set_inline(ts, stored);
        }
    }

    fn cell_count(&self) -> Option<usize> {
        Some(self.cells.len())
    }

    fn cell(&self, i: usize) -> Option<(&[u8], &CellData)> {
        let c = self.cells.get(i)?;
        Some((self.qualifiers.get(c.qualifier.clone())?, &c.cell.data))
    }
}

impl RowBuf {
    /// An empty row whose families are named by `info`, with room for `cells` cells.
    pub(crate) fn new(info: Arc<TableInfo>, cells: usize) -> Self {
        Self {
            info: Some(info),
            cells: Vec::with_capacity(cells),
            ..Self::default()
        }
    }

    /// An empty row for the same families, with this row's capacities (the next row of a
    /// scan is likely shaped like the last).
    pub(crate) fn empty_like(&self) -> Self {
        Self {
            info: self.info.clone(),
            key: Vec::with_capacity(self.key.len()),
            qualifiers: Vec::with_capacity(self.qualifiers.len()),
            cells: Vec::with_capacity(self.cells.len()),
            last_family: self.last_family,
        }
    }

    /// Empties the buffers, keeping their capacity.
    pub(crate) fn clear(&mut self) {
        self.key.clear();
        self.qualifiers.clear();
        self.cells.clear();
    }

    /// Number of cells.
    pub(crate) fn len(&self) -> usize {
        self.cells.len()
    }

    /// Appends a cell whose qualifier is `qualifier` within `qualifiers`.
    pub(crate) fn push(&mut self, family: FamilyId, qualifier: Range<usize>, data: CellData) {
        let family = self.family_index(family);
        self.cells.push(RowCell {
            family,
            qualifier,
            cell: Cell { data },
        });
    }

    /// The index of family `id` in `info` (`usize::MAX` if it is not there, which names it
    /// `""`).
    fn family_index(&mut self, id: FamilyId) -> usize {
        if let Some((last, i)) = self.last_family
            && last == id
        {
            return i;
        }
        let i = self
            .info
            .as_deref()
            .and_then(|info| info.families.iter().position(|f| f.id == id))
            .unwrap_or(usize::MAX);
        self.last_family = Some((id, i));
        i
    }

    fn family_name(&self, i: usize) -> &str {
        self.info
            .as_deref()
            .and_then(|info| info.families.get(i))
            .map_or("", |f| f.name.as_str())
    }

    fn qualifier(&self, cell: &RowCell) -> &[u8] {
        &self.qualifiers[cell.qualifier.clone()]
    }

    fn entry(&self, i: usize) -> Option<(&str, &[u8], &Cell)> {
        let c = self.cells.get(i)?;
        Some((self.family_name(c.family), self.qualifier(c), &c.cell))
    }

    /// The newest version of one column: the first cell of it, since versions come newest
    /// first.
    fn get(&self, family: &str, qualifier: &[u8]) -> Option<&Cell> {
        let i = self
            .info
            .as_deref()?
            .families
            .iter()
            .position(|f| f.name == family)?;
        self.cells
            .iter()
            .find(|c| c.family == i && self.qualifier(c) == qualifier)
            .map(|c| &c.cell)
    }
}

/// Where a [`RowRef`] gets its cells.
#[derive(Debug, Clone)]
enum RowSrc<'a> {
    Owned(Arc<RowBuf>),
    Borrowed(&'a RowBuf),
}

/// A borrowed row: its key and its cells ordered by family, qualifier, newest version first.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let t = db
///     .table("t")?
///     .family("a", Family::default())
///     .family("b", Family::default())
///     .create_if_missing()?;
/// t.mutate(b"row").put("b", b"x", b"1").put("a", b"z", b"2").put("a", b"y", b"3").commit()?;
///
/// let row = t.row(b"row").read()?.unwrap();
/// assert_eq!(row.key(), b"row");
/// let cells: Vec<(&str, &[u8])> = row.iter().map(|e| (e.family, e.qualifier)).collect();
/// assert_eq!(cells, [("a", &b"y"[..]), ("a", b"z"), ("b", b"x")]);
/// assert_eq!(row.get("a", b"z").unwrap().value(), b"2");
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct RowRef<'a> {
    src: RowSrc<'a>,
}

impl<'a> RowRef<'a> {
    pub(crate) fn owned(buf: RowBuf) -> Self {
        Self {
            src: RowSrc::Owned(Arc::new(buf)),
        }
    }

    pub(crate) fn borrowed(buf: &'a RowBuf) -> Self {
        Self {
            src: RowSrc::Borrowed(buf),
        }
    }

    fn buf(&self) -> &RowBuf {
        match &self.src {
            RowSrc::Owned(b) => b,
            RowSrc::Borrowed(b) => b,
        }
    }

    /// The row key.
    pub fn key(&self) -> &[u8] {
        &self.buf().key
    }

    /// Number of cells.
    pub fn len(&self) -> usize {
        self.buf().len()
    }

    /// Whether the row has no cells.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The `i`th cell.
    pub fn entry(&self, i: usize) -> Option<CellEntry<'_>> {
        let (family, qualifier, cell) = self.buf().entry(i)?;
        Some(CellEntry {
            family,
            qualifier,
            cell: CellRef::borrowed(&cell.data),
        })
    }

    /// Iterates the cells.
    pub fn iter(&self) -> impl Iterator<Item = CellEntry<'_>> + '_ {
        (0..self.len()).filter_map(move |i| self.entry(i))
    }

    /// The newest version of one column.
    pub fn get(&self, family: &str, qualifier: &[u8]) -> Option<CellRef<'_>> {
        self.buf()
            .get(family, qualifier)
            .map(|c| CellRef::borrowed(&c.data))
    }

    /// An owned copy of the row's structure; values stay pinned, not copied.
    pub fn to_owned(&self) -> Row {
        let buf = match &self.src {
            RowSrc::Owned(b) => Arc::clone(b),
            RowSrc::Borrowed(b) => Arc::new(RowBuf::clone(b)),
        };
        Row { buf }
    }
}

/// An owned row. Cheap to clone.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole, Row};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// t.mutate(b"a").put("f", b"q", b"1").commit()?;
/// t.mutate(b"b").put("f", b"q", b"2").commit()?;
///
/// let rows: Vec<Row> = t.scan_prefix(b"").iter()?.collect::<pigeonhole::Result<_>>()?;
/// assert_eq!(rows.len(), 2);
/// let (family, qualifier, cell) = rows[1].entry(0).unwrap();
/// assert_eq!((family, qualifier, cell.value()), ("f", &b"q"[..], &b"2"[..]));
/// assert_eq!(rows[0].get("f", b"q").unwrap().value(), b"1");
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Row {
    buf: Arc<RowBuf>,
}

impl Row {
    pub(crate) fn new(buf: RowBuf) -> Self {
        Self { buf: Arc::new(buf) }
    }

    /// The row key.
    pub fn key(&self) -> &[u8] {
        &self.buf.key
    }

    /// Number of cells.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether the row has no cells.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The `i`th cell: family name, qualifier and version.
    pub fn entry(&self, i: usize) -> Option<(&str, &[u8], &Cell)> {
        self.buf.entry(i)
    }

    /// The newest version of one column.
    pub fn get(&self, family: &str, qualifier: &[u8]) -> Option<&Cell> {
        self.buf.get(family, qualifier)
    }

    /// A borrowed view.
    pub fn view(&self) -> RowRef<'_> {
        RowRef::borrowed(&self.buf)
    }
}

use std::marker::PhantomData;

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

/// A borrowed cell version, tied to the table handle that read it. Reading it allocates
/// nothing; the bytes stay pinned until it drops. [`CellRef::to_owned`] keeps them longer.
#[derive(Debug, Clone)]
pub struct CellRef<'a> {
    _priv: PhantomData<&'a ()>,
}

impl<'a> CellRef<'a> {
    /// The value bytes (for typed values, their encoding without the tag).
    pub fn value(&self) -> &[u8] {
        todo!()
    }

    /// The typed value.
    pub fn typed(&self) -> Value<'_> {
        todo!()
    }

    /// The value as an `i64`, if it is one.
    pub fn as_i64(&self) -> Option<i64> {
        todo!()
    }

    /// The version's timestamp (microseconds since the Unix epoch by default).
    pub fn timestamp(&self) -> u64 {
        todo!()
    }

    /// An owned handle that can outlive the table borrow (no copy of the value).
    pub fn to_owned(&self) -> Cell {
        todo!()
    }
}

/// An owned cell version: a cheap ref-counted handle that pins its cache block. `Send`,
/// `Sync`, `'static`; safe to hold across `.await`.
#[derive(Debug, Clone)]
pub struct Cell {
    _priv: (),
}

impl Cell {
    /// The value bytes.
    pub fn value(&self) -> &[u8] {
        todo!()
    }

    /// The typed value.
    pub fn typed(&self) -> Value<'_> {
        todo!()
    }

    /// The value as an `i64`, if it is one.
    pub fn as_i64(&self) -> Option<i64> {
        todo!()
    }

    /// The version's timestamp.
    pub fn timestamp(&self) -> u64 {
        todo!()
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

/// A borrowed row: its key and its cells ordered by family, qualifier, newest version first.
#[derive(Debug, Clone)]
pub struct RowRef<'a> {
    _priv: PhantomData<&'a ()>,
}

impl<'a> RowRef<'a> {
    /// The row key.
    pub fn key(&self) -> &[u8] {
        todo!()
    }

    /// Number of cells.
    pub fn len(&self) -> usize {
        todo!()
    }

    /// Whether the row has no cells.
    pub fn is_empty(&self) -> bool {
        todo!()
    }

    /// The `i`th cell.
    pub fn entry(&self, i: usize) -> Option<CellEntry<'_>> {
        todo!()
    }

    /// Iterates the cells.
    pub fn iter(&self) -> impl Iterator<Item = CellEntry<'_>> + '_ {
        std::iter::empty()
    }

    /// The newest version of one column.
    pub fn get(&self, family: &str, qualifier: &[u8]) -> Option<CellRef<'_>> {
        todo!()
    }

    /// An owned copy of the row's structure; values stay pinned, not copied.
    pub fn to_owned(&self) -> Row {
        todo!()
    }
}

/// An owned row. Cheap to clone.
#[derive(Debug, Clone)]
pub struct Row {
    _priv: (),
}

impl Row {
    /// The row key.
    pub fn key(&self) -> &[u8] {
        todo!()
    }

    /// Number of cells.
    pub fn len(&self) -> usize {
        todo!()
    }

    /// Whether the row has no cells.
    pub fn is_empty(&self) -> bool {
        todo!()
    }

    /// The `i`th cell: family name, qualifier and version.
    pub fn entry(&self, i: usize) -> Option<(&str, &[u8], &Cell)> {
        todo!()
    }

    /// The newest version of one column.
    pub fn get(&self, family: &str, qualifier: &[u8]) -> Option<&Cell> {
        todo!()
    }

    /// A borrowed view.
    pub fn view(&self) -> RowRef<'_> {
        todo!()
    }
}

use std::marker::PhantomData;
use std::ops::{Bound, Range, RangeBounds};

use crate::{Result, Row, RowRef, Snapshot};

/// A predicate on a cell value, evaluated inside the read path before materialization.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueFilter {
    /// Value bytes equal.
    Equals(Vec<u8>),
    /// Value bytes start with.
    Prefix(Vec<u8>),
    /// The value is an `i64` that compares to the operand as given.
    I64(std::cmp::Ordering, i64),
}

/// A condition for a conditional mutation ([`RowMutation::commit_if`](crate::RowMutation::commit_if)).
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

/// A row read under construction. Finish with [`RowRead::read`].
#[derive(Debug)]
#[must_use = "a row read does nothing until .read()"]
pub struct RowRead<'t> {
    _priv: PhantomData<&'t ()>,
}

impl<'t> RowRead<'t> {
    /// Read only these families (default: all), in this order.
    pub fn families<'f>(self, families: impl IntoIterator<Item = &'f str>) -> Self {
        todo!()
    }

    /// Add one family to the projection.
    pub fn family(self, family: &str) -> Self {
        todo!()
    }

    /// Only qualifiers starting with `prefix`.
    pub fn qualifier_prefix(self, prefix: &[u8]) -> Self {
        todo!()
    }

    /// Only qualifiers within `range`.
    pub fn qualifier_range<'k, K: AsRef<[u8]> + ?Sized + 'k>(
        self,
        range: impl RangeBounds<&'k K>,
    ) -> Self {
        todo!()
    }

    /// Only qualifiers within explicit bounds (the non-generic form a C ABI exports).
    pub fn qualifier_bounds(self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self {
        todo!()
    }

    /// Only the newest version of each column (the default).
    pub fn latest(self) -> Self {
        todo!()
    }

    /// Up to `n` versions of each column (0: all retained).
    pub fn versions(self, n: u32) -> Self {
        todo!()
    }

    /// Only versions with timestamps in `range`.
    pub fn time_range(self, range: Range<u64>) -> Self {
        todo!()
    }

    /// At most `n` columns per family.
    pub fn column_limit(self, n: u32) -> Self {
        todo!()
    }

    /// Only cells whose value matches.
    pub fn value_filter(self, filter: ValueFilter) -> Self {
        todo!()
    }

    /// Read as of `snapshot` instead of now.
    pub fn snapshot(self, snapshot: &Snapshot) -> Self {
        todo!()
    }

    /// Performs the read. `None` if the row has no matching cell.
    pub fn read(self) -> Result<Option<RowRef<'t>>> {
        todo!()
    }
}

/// An ordered scan under construction. Finish with [`Scan::iter`].
#[derive(Debug)]
#[must_use = "a scan does nothing until .iter()"]
pub struct Scan<'t> {
    _priv: PhantomData<&'t ()>,
}

impl<'t> Scan<'t> {
    /// Read only these families (default: all), in this order.
    pub fn families<'f>(self, families: impl IntoIterator<Item = &'f str>) -> Self {
        todo!()
    }

    /// Add one family to the projection.
    pub fn family(self, family: &str) -> Self {
        todo!()
    }

    /// Only qualifiers starting with `prefix`.
    pub fn qualifier_prefix(self, prefix: &[u8]) -> Self {
        todo!()
    }

    /// Only qualifiers within `range`.
    pub fn qualifier_range<'k, K: AsRef<[u8]> + ?Sized + 'k>(
        self,
        range: impl RangeBounds<&'k K>,
    ) -> Self {
        todo!()
    }

    /// Only qualifiers within explicit bounds (the non-generic form a C ABI exports).
    pub fn qualifier_bounds(self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self {
        todo!()
    }

    /// Only the newest version of each column (the default).
    pub fn latest(self) -> Self {
        todo!()
    }

    /// Up to `n` versions of each column.
    pub fn versions(self, n: u32) -> Self {
        todo!()
    }

    /// Only versions with timestamps in `range`.
    pub fn time_range(self, range: Range<u64>) -> Self {
        todo!()
    }

    /// At most `n` columns per family per row; the rest of each row is skipped without
    /// decoding.
    pub fn columns_per_row(self, n: u32) -> Self {
        todo!()
    }

    /// Only cells whose value matches.
    pub fn value_filter(self, filter: ValueFilter) -> Self {
        todo!()
    }

    /// Stop after `n` rows.
    pub fn limit(self, n: u64) -> Self {
        todo!()
    }

    /// Read as of `snapshot` instead of now.
    pub fn snapshot(self, snapshot: &Snapshot) -> Self {
        todo!()
    }

    /// Starts the scan.
    pub fn iter(self) -> Result<RowIter<'t>> {
        todo!()
    }
}

/// Rows of a scan, in key order. As an [`Iterator`] it yields owned [`Row`]s (cheap: values
/// stay pinned, not copied); [`RowIter::next_ref`] lends zero-copy [`RowRef`]s instead.
#[derive(Debug)]
pub struct RowIter<'t> {
    _priv: PhantomData<&'t ()>,
}

impl RowIter<'_> {
    /// The next row, borrowed from the iterator until the next call.
    pub fn next_ref(&mut self) -> Result<Option<RowRef<'_>>> {
        todo!()
    }
}

impl Iterator for RowIter<'_> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

use std::ops::{Bound, RangeBounds};

use crate::{CellRef, Family, Pigeonhole, Result, RowMutation, RowRead, Scan, Snapshot};

/// Defines or opens a table. Returned by [`Pigeonhole::table`].
#[derive(Debug)]
pub struct TableBuilder<'db> {
    _db: &'db Pigeonhole,
}

impl TableBuilder<'_> {
    /// Declares a family. On an existing table, a family not yet present is added (cheap);
    /// one that exists keeps its stored options.
    pub fn family(self, name: &str, family: Family) -> Self {
        todo!()
    }

    /// Opens the table, creating it (and any missing declared families) if needed.
    pub fn create_if_missing(self) -> Result<Table> {
        todo!()
    }

    /// Creates the table; fails with `TableExists` if it exists.
    pub fn create(self) -> Result<Table> {
        todo!()
    }

    /// Opens an existing table; fails with `TableNotFound`.
    pub fn open(self) -> Result<Table> {
        todo!()
    }
}

/// A table handle for reads and writes. Cheap to clone; `Send + Sync`.
#[derive(Debug, Clone)]
pub struct Table {
    _priv: (),
}

impl Table {
    /// The table name.
    pub fn name(&self) -> &str {
        todo!()
    }

    /// Family names.
    pub fn families(&self) -> Vec<&str> {
        todo!()
    }

    /// Starts a single-row atomic mutation.
    pub fn mutate<'t>(&'t self, row: &[u8]) -> RowMutation<'t> {
        todo!()
    }

    /// The newest version of one cell. Borrows from the cache: no allocation.
    pub fn get(&self, row: &[u8], family: &str, qualifier: &[u8]) -> Result<Option<CellRef<'_>>> {
        todo!()
    }

    /// The newest version of one cell as of `snapshot`.
    pub fn get_at(
        &self,
        snapshot: &Snapshot,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> Result<Option<CellRef<'_>>> {
        todo!()
    }

    /// Starts a row read.
    pub fn row<'t>(&'t self, row: &[u8]) -> RowRead<'t> {
        todo!()
    }

    /// Starts an ordered scan over a row range: `table.scan(b"a".."b")`.
    pub fn scan<'k, K: AsRef<[u8]> + ?Sized + 'k>(
        &self,
        range: impl RangeBounds<&'k K>,
    ) -> Scan<'_> {
        todo!()
    }

    /// Starts a scan of every row starting with `prefix`.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Scan<'_> {
        todo!()
    }

    /// Starts a scan from explicit bounds (the form a C ABI exports).
    pub fn scan_bounds(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Scan<'_> {
        todo!()
    }
}

/// A table handle on a [`PigeonholeReader`](crate::PigeonholeReader): reads only.
#[derive(Debug, Clone)]
pub struct ReadTable {
    _priv: (),
}

impl ReadTable {
    /// The table name.
    pub fn name(&self) -> &str {
        todo!()
    }

    /// The newest version of one cell.
    pub fn get(&self, row: &[u8], family: &str, qualifier: &[u8]) -> Result<Option<CellRef<'_>>> {
        todo!()
    }

    /// The newest version of one cell as of `snapshot`.
    pub fn get_at(
        &self,
        snapshot: &Snapshot,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> Result<Option<CellRef<'_>>> {
        todo!()
    }

    /// Starts a row read.
    pub fn row<'t>(&'t self, row: &[u8]) -> RowRead<'t> {
        todo!()
    }

    /// Starts an ordered scan over a row range.
    pub fn scan<'k, K: AsRef<[u8]> + ?Sized + 'k>(
        &self,
        range: impl RangeBounds<&'k K>,
    ) -> Scan<'_> {
        todo!()
    }

    /// Starts a scan of every row starting with `prefix`.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Scan<'_> {
        todo!()
    }

    /// Starts a scan from explicit bounds.
    pub fn scan_bounds(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Scan<'_> {
        todo!()
    }
}

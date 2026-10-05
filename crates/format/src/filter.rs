//! Per-SST filters: a cache-line-blocked bloom filter over row keys and another over
//! row+qualifier. Pinned in memory while the SST is open. See `FORMAT.md` §5.
//!
//! The filter block's first byte names the filter kind so a ribbon filter can be added later
//! without breaking old files.

/// Filter kinds. Numbers are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FilterKind {
    /// 512-bit blocked bloom filter with double hashing inside one cache line.
    BlockedBloom = 1,
}

/// Hash of a row key for the row filter: xxh3-64 of the escaped row bytes.
pub fn row_hash(escaped_row: &[u8]) -> u64 {
    todo!()
}

/// Hash of a column for the cell filter: xxh3-64 of the column prefix
/// (escaped row, terminator, escaped qualifier, terminator).
pub fn column_hash(column_prefix: &[u8]) -> u64 {
    todo!()
}

/// Builds a filter from hashes.
#[derive(Debug, Default)]
pub struct FilterBuilder {
    _priv: (),
}

impl FilterBuilder {
    /// A builder targeting `bits_per_key` bits per distinct key.
    pub fn new(bits_per_key: u8) -> Self {
        todo!()
    }

    /// Adds a key hash. Duplicate consecutive hashes are ignored.
    pub fn add_hash(&mut self, hash: u64) {
        todo!()
    }

    /// Appends the logical filter block to `out` and resets the builder.
    pub fn finish(&mut self, out: &mut Vec<u8>) {
        todo!()
    }
}

/// A filter read from its logical block bytes. Zero-copy.
#[derive(Debug, Clone, Copy)]
pub struct Filter<'a> {
    _bytes: &'a [u8],
}

impl<'a> Filter<'a> {
    /// Parses a logical filter block. Never panics.
    pub fn new(logical: &'a [u8]) -> crate::Result<Self> {
        todo!()
    }

    /// `false` means the key is definitely absent; `true` means it may be present.
    pub fn may_contain(&self, hash: u64) -> bool {
        todo!()
    }
}

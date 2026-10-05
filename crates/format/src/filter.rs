//! Per-SST filters: a cache-line-blocked bloom filter over row keys and another over
//! row+qualifier. Pinned in memory while the SST is open. See `FORMAT.md` §6.
//!
//! The filter block's first byte names the filter kind so a ribbon filter can be added later
//! without breaking old files.

use std::ops::Deref;

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

/// Hash of a column for the column filter: xxh3-64 of the column prefix (escaped row,
/// terminator, escaped qualifier, terminator). For a row's family markers the key is the
/// marker prefix ([`crate::key::encode_marker_prefix`]); a point get probes both.
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

/// A filter over its logical block bytes, held by any byte owner (zero-copy; an SST reader
/// keeps it pinned beside the rest of its state).
#[derive(Debug, Clone)]
pub struct Filter<B> {
    _bytes: B,
}

impl<B: Deref<Target = [u8]>> Filter<B> {
    /// Parses a logical filter block. Never panics.
    pub fn new(logical: B) -> crate::Result<Self> {
        todo!()
    }

    /// `false` means the key is definitely absent; `true` means it may be present.
    pub fn may_contain(&self, hash: u64) -> bool {
        todo!()
    }
}

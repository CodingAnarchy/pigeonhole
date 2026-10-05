//! The entry-level scan filter, shared by every source so pushdown semantics are uniform.

use std::ops::Bound;

use crate::Timestamp;

/// Conditions that are safe to apply to individual entries before MVCC resolution
/// (decision D22). `pigeonhole-sst` applies it inside the block decoder (and may skip whole
/// blocks); memtable sources are wrapped in `pigeonhole_compaction::FilteredCursor`. Both use
/// [`ScanFilter::admits`], so a pushed-down scan equals an unfiltered one filtered afterwards.
///
/// Always admitted: delete entries and family markers (hiding them could resurrect older
/// versions) and merge operands (dropping some would produce partial counters).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ScanFilter {
    /// Which qualifiers to keep.
    pub qualifiers: QualifierFilter,
    /// Keep puts with `min <= ts < max`.
    pub time_range: Option<(Timestamp, Timestamp)>,
}

/// Qualifier selection.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum QualifierFilter {
    /// Every qualifier.
    #[default]
    All,
    /// Qualifiers starting with these (unescaped) bytes.
    Prefix(Vec<u8>),
    /// Qualifiers within the range (unescaped).
    Range(Bound<Vec<u8>>, Bound<Vec<u8>>),
}

impl ScanFilter {
    /// Admits everything.
    pub fn all() -> Self {
        Self::default()
    }

    /// Whether the filter admits every entry (lets sources skip the check).
    pub fn is_all(&self) -> bool {
        todo!()
    }

    /// Whether to keep the entry with internal key `key`. Allocation-free.
    pub fn admits(&self, key: &[u8]) -> bool {
        todo!()
    }

    /// The first key at or after `key` that could be admitted within the same row (a seek
    /// hint for skipping excluded qualifiers), appended to `out`. Returns false if none.
    pub fn next_admissible(&self, key: &[u8], out: &mut Vec<u8>) -> bool {
        todo!()
    }
}

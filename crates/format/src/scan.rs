//! The entry-level scan filter, shared by every source so pushdown semantics are uniform.

use std::ops::Bound;

use crate::Timestamp;
use crate::key::{Escaped, Kind, TERMINATOR, decode_key, escape_into, row_prefix_len};

/// Conditions that are safe to apply to individual entries before MVCC resolution
/// (decision D22). `pigeonhole-sst` applies it inside the block decoder (and may skip whole
/// blocks); memtable sources are wrapped in `pigeonhole_compaction::FilteredCursor`. Both use
/// [`ScanFilter::admits`], so a pushed-down scan equals an unfiltered one filtered afterwards.
///
/// The time range applies to puts only: delete entries, family markers and merge operands
/// always pass it (hiding a delete could resurrect older versions; dropping some operands
/// would produce partial counters). The qualifier selection applies to every cell entry,
/// deletes and merges included, because nothing of an excluded column is ever returned, so
/// its deletes and operands cannot change a result; family markers have no qualifier and
/// always pass. A malformed key is admitted, so filtering never hides a decoding error.
///
/// ```
/// use pigeonhole_format::key::{Kind, encode_key};
/// use pigeonhole_format::scan::{QualifierFilter, ScanFilter};
///
/// let mut f = ScanFilter::all();
/// f.qualifiers = QualifierFilter::Prefix(b"meta:".to_vec());
/// let mut key = Vec::new();
/// encode_key(&mut key, b"row", b"body", 5, 1, Kind::Put).unwrap();
/// assert!(!f.admits(&key));
///
/// // A seek hint jumps over the excluded qualifiers.
/// let mut hint = Vec::new();
/// assert!(f.next_admissible(&key, &mut hint));
/// assert!(hint.as_slice() > key.as_slice());
/// ```
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

impl QualifierFilter {
    /// Where a qualifier stands relative to the selection.
    fn place(&self, q: Escaped<'_>) -> Place {
        use std::cmp::Ordering::*;
        match self {
            Self::All => Place::In,
            Self::Prefix(p) => {
                if q.starts_with_raw(p) {
                    Place::In
                } else if q.cmp_raw(p) == Less {
                    Place::Before
                } else {
                    Place::After
                }
            }
            Self::Range(lo, hi) => {
                let below = match lo {
                    Bound::Included(l) => q.cmp_raw(l) == Less,
                    Bound::Excluded(l) => q.cmp_raw(l) != Greater,
                    Bound::Unbounded => false,
                };
                let above = match hi {
                    Bound::Included(h) => q.cmp_raw(h) == Greater,
                    Bound::Excluded(h) => q.cmp_raw(h) != Less,
                    Bound::Unbounded => false,
                };
                if below {
                    Place::Before
                } else if above {
                    Place::After
                } else {
                    Place::In
                }
            }
        }
    }
}

enum Place {
    Before,
    In,
    After,
}

impl ScanFilter {
    /// Admits everything.
    pub fn all() -> Self {
        Self::default()
    }

    /// Whether the filter admits every entry (lets sources skip the check).
    pub fn is_all(&self) -> bool {
        self.qualifiers == QualifierFilter::All && self.time_range.is_none()
    }

    /// Whether to keep the entry with internal key `key`. Allocation-free.
    pub fn admits(&self, key: &[u8]) -> bool {
        if self.is_all() {
            return true;
        }
        let Ok(parts) = decode_key(key) else {
            return true;
        };
        let Some(q) = parts.qualifier else {
            return true;
        };
        if !matches!(self.qualifiers.place(q), Place::In) {
            return false;
        }
        match self.time_range {
            Some((min, max)) if parts.kind == Kind::Put => min <= parts.ts && parts.ts < max,
            _ => true,
        }
    }

    /// The first key at or after `key` that could be admitted within the same row (a seek
    /// hint for skipping excluded qualifiers), appended to `out`. Returns false if none.
    ///
    /// Only the qualifier selection produces skips: inside an admitted column, deletes and
    /// merge operands pass whatever their timestamp, so the hint is `key` itself.
    pub fn next_admissible(&self, key: &[u8], out: &mut Vec<u8>) -> bool {
        let parts = match decode_key(key) {
            Ok(p) => p,
            Err(_) => {
                out.extend_from_slice(key);
                return true;
            }
        };
        let Some(q) = parts.qualifier else {
            out.extend_from_slice(key);
            return true;
        };
        match self.qualifiers.place(q) {
            Place::In => {
                out.extend_from_slice(key);
                true
            }
            Place::After => false,
            Place::Before => {
                // Every key of the row whose qualifier is >= `start` (or > it, for an
                // excluded bound) sorts at or after this hint.
                let Ok(row_len) = row_prefix_len(key) else {
                    out.extend_from_slice(key);
                    return true;
                };
                out.extend_from_slice(&key[..row_len]);
                match &self.qualifiers {
                    QualifierFilter::Prefix(p) => escape_into(out, p),
                    QualifierFilter::Range(Bound::Included(l), _) => escape_into(out, l),
                    QualifierFilter::Range(Bound::Excluded(l), _) => {
                        // After every key of qualifier `l` (`l`, terminator, suffix), before
                        // any longer qualifier (`l`, then `00 FF` or a byte >= 01).
                        escape_into(out, l);
                        out.extend_from_slice(&[TERMINATOR[0], TERMINATOR[1] + 1]);
                    }
                    _ => out.extend_from_slice(&key[row_len..]),
                }
                true
            }
        }
    }
}

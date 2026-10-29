//! The entry-level scan filter, shared by every source so pushdown semantics are uniform.

use std::ops::Bound;

use crate::Timestamp;
use crate::key::{Escaped, Kind, TERMINATOR, decode_key, escape_into};

/// Conditions that are safe to apply to individual entries before MVCC resolution
/// (decision D22). `pigeonhole-sst` applies it inside the block decoder (and may skip whole
/// blocks); memtable sources are wrapped in `pigeonhole_compaction::FilteredCursor`. Both use
/// [`CompiledFilter::check`], so a pushed-down scan equals an unfiltered one filtered
/// afterwards.
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
/// use pigeonhole_format::scan::{QualifierFilter, ScanFilter, Verdict};
///
/// let mut f = ScanFilter::all();
/// f.qualifiers = QualifierFilter::Prefix(b"meta:".to_vec());
/// let mut key = Vec::new();
/// encode_key(&mut key, b"row", b"meta:a", 5, 1, Kind::Put).unwrap();
/// let mut hint = Vec::new();
/// assert_eq!(f.compile().check(&key, &mut hint), Verdict::Admit);
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
}

/// What a cursor does with an entry, by [`CompiledFilter::check`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Return it.
    Admit,
    /// Step to the next entry.
    Next,
    /// Seek to the hint written to `out`, which sorts after the entry.
    Seek,
    /// Nothing later in this row can be admitted: skip to the next row.
    SkipRow,
}

/// A [`ScanFilter`] for a cursor's per-entry checks (ICR 0030): one call decodes the key once
/// for both the admission and the seek hint, and compares the key's escaped qualifier bytes
/// with the bounds directly. Escaping preserves order and prefixes (`0x00` becomes `00 FF`,
/// below every other byte), and a bound without a `0x00` is its own escaped form; a bound
/// with one (rare) is compared through the unescaping path.
///
/// [`CompiledFilter::check`] applies [`ScanFilter`]'s rules (D22), malformed keys included
/// (admitted); the crate's tests check it against those rules written out on unescaped
/// qualifiers.
///
/// ```
/// use pigeonhole_format::key::{Kind, encode_key};
/// use pigeonhole_format::scan::{QualifierFilter, ScanFilter, Verdict};
///
/// let mut f = ScanFilter::all();
/// f.qualifiers = QualifierFilter::Prefix(b"meta:".to_vec());
/// let c = f.compile();
/// let mut key = Vec::new();
/// encode_key(&mut key, b"row", b"body", 5, 1, Kind::Put).unwrap();
/// let mut hint = Vec::new();
/// assert_eq!(c.check(&key, &mut hint), Verdict::Seek);
/// assert!(hint.as_slice() > key.as_slice());
/// ```
#[derive(Debug, Clone)]
pub struct CompiledFilter(ScanFilter);

impl ScanFilter {
    /// This filter compiled for per-entry checks.
    pub fn compile(&self) -> CompiledFilter {
        self.clone().into_compiled()
    }

    /// [`ScanFilter::compile`], consuming the filter: a move, nothing computed or allocated
    /// (every source of every read compiles its filter, most of them admitting everything).
    #[inline(always)]
    pub fn into_compiled(self) -> CompiledFilter {
        CompiledFilter(self)
    }
}

impl CompiledFilter {
    /// Whether the filter admits every entry (lets sources skip the check).
    #[inline]
    pub fn is_all(&self) -> bool {
        self.0.is_all()
    }

    /// Where a qualifier stands relative to the selection: its escaped bytes compared with
    /// the bounds in one pass ([`cmp_plain`]), else, for a bound with a `0x00` where it
    /// matters, through [`QualifierFilter::place`].
    #[inline]
    fn place(&self, q: Escaped<'_>) -> Place {
        use PlainCmp::*;
        let e = q.as_escaped();
        let placed = match &self.0.qualifiers {
            QualifierFilter::All => Some(Place::In),
            QualifierFilter::Prefix(p) => cmp_plain(e, p).map(|c| match c {
                Less => Place::Before,
                Equal | Extends => Place::In,
                Greater => Place::After,
            }),
            QualifierFilter::Range(lo, hi) => (|| {
                let below = match lo {
                    Bound::Included(l) => cmp_plain(e, l)? == Less,
                    Bound::Excluded(l) => matches!(cmp_plain(e, l)?, Less | Equal),
                    Bound::Unbounded => false,
                };
                if below {
                    return Some(Place::Before);
                }
                let above = match hi {
                    Bound::Included(h) => matches!(cmp_plain(e, h)?, Greater | Extends),
                    Bound::Excluded(h) => !matches!(cmp_plain(e, h)?, Less),
                    Bound::Unbounded => false,
                };
                Some(if above { Place::After } else { Place::In })
            })(),
        };
        match placed {
            Some(p) => p,
            None => self.0.qualifiers.place(q),
        }
    }

    /// What to do with the entry with internal key `key`. On [`Verdict::Seek`], the hint is
    /// appended to `out` (cleared first); `out` is otherwise left empty. Allocation-free once
    /// `out` has grown to a row prefix plus the selection's start.
    pub fn check(&self, key: &[u8], out: &mut Vec<u8>) -> Verdict {
        out.clear();
        if self.is_all() {
            return Verdict::Admit;
        }
        let Ok(parts) = decode_key(key) else {
            return Verdict::Admit;
        };
        let Some(q) = parts.qualifier else {
            return Verdict::Admit;
        };
        match self.place(q) {
            Place::In => match self.0.time_range {
                Some((min, max))
                    if parts.kind == Kind::Put && !(min <= parts.ts && parts.ts < max) =>
                {
                    // Inside an admitted column, the next entry may pass (a delete or an
                    // operand, or another timestamp): step, as a hint of `key` itself would.
                    Verdict::Next
                }
                _ => Verdict::Admit,
            },
            Place::After => Verdict::SkipRow,
            Place::Before => {
                let row_len = parts.row.as_escaped().len() + TERMINATOR.len();
                out.extend_from_slice(&key[..row_len]);
                match &self.0.qualifiers {
                    QualifierFilter::Prefix(p) => escape_into(out, p),
                    QualifierFilter::Range(Bound::Included(l), _) => escape_into(out, l),
                    QualifierFilter::Range(Bound::Excluded(l), _) => {
                        // After every key of qualifier `l`, before any longer qualifier.
                        escape_into(out, l);
                        out.extend_from_slice(&[TERMINATOR[0], TERMINATOR[1] + 1]);
                    }
                    // Unbounded below is never Before.
                    _ => {
                        out.clear();
                        return Verdict::Next;
                    }
                }
                if out.as_slice() > key {
                    Verdict::Seek
                } else {
                    out.clear();
                    Verdict::Next
                }
            }
        }
    }
}

/// How escaped qualifier bytes order against a raw bound, by [`cmp_plain`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum PlainCmp {
    Less,
    Equal,
    /// Starts with the bound and is longer.
    Extends,
    Greater,
}

/// Orders the escaped bytes `e` of a valid qualifier against the raw bound `b` as their raw
/// forms order, in one pass with no call: `None` if `b` holds a `0x00` before the order is
/// decided. Up to there the bound is its own escaped form, and an escaped `0x00` (`00 FF`)
/// orders below every other byte as a raw `0x00` does.
#[inline]
fn cmp_plain(e: &[u8], b: &[u8]) -> Option<PlainCmp> {
    for (i, &c) in b.iter().enumerate() {
        if c == 0 {
            return None;
        }
        match e.get(i) {
            None => return Some(PlainCmp::Less),
            Some(&x) if x != c => {
                return Some(if x < c {
                    PlainCmp::Less
                } else {
                    PlainCmp::Greater
                });
            }
            Some(_) => {}
        }
    }
    Some(if e.len() == b.len() {
        PlainCmp::Equal
    } else {
        PlainCmp::Extends
    })
}

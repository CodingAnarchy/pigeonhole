//! The internal key: how `(row, qualifier, timestamp, seqno, kind)` becomes bytes whose
//! lexicographic order is the logical order.
//!
//! ```text
//! cell:   [row, escaped][00 01][qualifier, escaped][00 01][!ts u64 BE][!seqno u64 BE][kind u8]
//! marker: [row, escaped][00 01][00 00]                    [!ts u64 BE][!seqno u64 BE][kind u8]
//! ```
//!
//! Escaping replaces `0x00` with `0x00 0xFF`. The terminator `0x00 0x01` therefore sorts
//! before any continuation, so a shorter row sorts first. A family-in-row marker uses
//! `0x00 0x00` in place of the qualifier, which sorts before every qualifier (including the
//! empty one), so a reader meets a row's markers before its cells. See `FORMAT.md` §2.

use crate::{Seqno, Timestamp};

/// Maximum length, in unescaped bytes, of a row key or a qualifier (64 KiB).
pub const MAX_KEY_PART: usize = 64 * 1024;

/// Length of the fixed suffix: inverted timestamp, inverted seqno, kind.
pub const SUFFIX_LEN: usize = 17;

/// Terminator after an escaped row or qualifier.
pub const TERMINATOR: [u8; 2] = [0x00, 0x01];

/// Stands in for the qualifier in a family-in-row marker.
pub const MARKER_QUALIFIER: [u8; 2] = [0x00, 0x00];

/// What an internal key records. Values are frozen; new kinds take new numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Kind {
    /// A value for one cell version. The value may be inline or a blob pointer.
    Put = 0x01,
    /// A merge operand, resolved at read and compaction time by the family's merge operator.
    Merge = 0x02,
    /// Deletes exactly the version at this key's timestamp.
    CellDelete = 0x03,
    /// Deletes every version of the column with timestamp `<=` this key's timestamp.
    ColumnDelete = 0x04,
    /// Family-in-row marker: deletes every cell of this row in this family with timestamp
    /// `<=` this key's timestamp. Only valid with the marker qualifier.
    FamilyDelete = 0x05,
}

impl Kind {
    /// Parses a kind byte.
    pub fn from_u8(b: u8) -> crate::Result<Self> {
        todo!()
    }

    /// Whether this kind is a deletion of any granularity.
    pub fn is_delete(self) -> bool {
        todo!()
    }
}

/// Byte placed in the kind position of a seek key; sorts before every real [`Kind`].
pub const SEEK_KIND: u8 = 0x00;

/// Appends the internal key for one cell to `out`. `out` is not cleared, so callers can
/// reuse one buffer per thread and stay allocation-free.
pub fn encode_key(
    out: &mut Vec<u8>,
    row: &[u8],
    qualifier: &[u8],
    ts: Timestamp,
    seqno: Seqno,
    kind: Kind,
) -> crate::Result<()> {
    todo!()
}

/// Appends a family-in-row marker key ([`Kind::FamilyDelete`]) for `row`.
pub fn encode_marker_key(
    out: &mut Vec<u8>,
    row: &[u8],
    ts: Timestamp,
    seqno: Seqno,
) -> crate::Result<()> {
    todo!()
}

/// Appends the escaped row and its terminator: a prefix shared by every key of `row`.
pub fn encode_row_prefix(out: &mut Vec<u8>, row: &[u8]) -> crate::Result<()> {
    todo!()
}

/// Appends the escaped row, escaped qualifier and both terminators: a prefix shared by every
/// version of one column.
pub fn encode_column_prefix(out: &mut Vec<u8>, row: &[u8], qualifier: &[u8]) -> crate::Result<()> {
    todo!()
}

/// Appends a key that sorts immediately before the newest version of `(row, qualifier)`
/// visible at `(ts, seqno)`: seeking to it lands on the first entry with timestamp `<= ts`
/// and, at equal timestamp, seqno `<= seqno`.
pub fn encode_seek_key(
    out: &mut Vec<u8>,
    row: &[u8],
    qualifier: &[u8],
    ts: Timestamp,
    seqno: Seqno,
) -> crate::Result<()> {
    todo!()
}

/// Appends the escaped form of `part` (no terminator).
pub fn escape_into(out: &mut Vec<u8>, part: &[u8]) {
    todo!()
}

/// A decoded internal key. Borrows the escaped parts from the input; nothing is copied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyParts<'a> {
    /// The row, still escaped.
    pub row: Escaped<'a>,
    /// The qualifier, still escaped; `None` for a family-in-row marker.
    pub qualifier: Option<Escaped<'a>>,
    /// The (un-inverted) timestamp.
    pub ts: Timestamp,
    /// The (un-inverted) seqno.
    pub seqno: Seqno,
    /// The kind.
    pub kind: Kind,
}

/// Decodes an internal key. Never panics, whatever the input.
pub fn decode_key(key: &[u8]) -> crate::Result<KeyParts<'_>> {
    todo!()
}

/// Splits the fixed 17-byte suffix off `key` without parsing the variable part.
pub fn split_suffix(key: &[u8]) -> crate::Result<(&[u8], Timestamp, Seqno, Kind)> {
    todo!()
}

/// Length of the row prefix of `key` (escaped row plus terminator), found by scanning for the
/// first unescaped terminator. Two keys belong to the same row iff these prefixes are equal.
pub fn row_prefix_len(key: &[u8]) -> crate::Result<usize> {
    todo!()
}

/// Length of the column prefix of `key` (row prefix, escaped qualifier and its terminator,
/// or the marker bytes). Two keys address the same column iff these prefixes are equal.
pub fn column_prefix_len(key: &[u8]) -> crate::Result<usize> {
    todo!()
}

/// An escaped byte string borrowed from an internal key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Escaped<'a>(&'a [u8]);

impl<'a> Escaped<'a> {
    /// Wraps bytes that are already escaped (no terminator).
    pub fn new(escaped: &'a [u8]) -> Self {
        Self(escaped)
    }

    /// The escaped bytes, as stored.
    pub fn as_escaped(&self) -> &'a [u8] {
        self.0
    }

    /// Whether the escaped form contains no escape sequences, so it equals the raw bytes.
    pub fn is_verbatim(&self) -> bool {
        todo!()
    }

    /// Appends the unescaped bytes to `out`.
    pub fn unescape_into(&self, out: &mut Vec<u8>) {
        todo!()
    }

    /// Compares with raw (unescaped) bytes without allocating.
    pub fn eq_raw(&self, raw: &[u8]) -> bool {
        todo!()
    }
}

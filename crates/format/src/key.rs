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
//!
//! **Delete rule** (BigTable semantics, decision D9): a `ColumnDelete` or `FamilyDelete` with
//! timestamp `T` hides every version with timestamp `<= T` in its scope, regardless of seqno,
//! so a later put with an older timestamp stays hidden. A `CellDelete` hides exactly the
//! versions with its timestamp. Seqnos decide only snapshot visibility.

use crate::{Error, Seqno, Timestamp};

/// Byte-wise comparison of two keys (or rows, or any byte strings): exactly `<[u8]>::cmp`,
/// lexicographic with a shorter prefix first, but inline. It reads eight bytes at a time as
/// big-endian words, then the tail byte by byte. Internal keys are short (a column prefix and
/// the 17-byte suffix), so a library `memcmp` call costs more than the comparison itself; two
/// keys of one column agree on their prefix words and differ in the timestamp and seqno words.
/// Every hot key comparison (merges, the memtable, routing, block building) uses it.
///
/// ```
/// use std::cmp::Ordering;
/// use pigeonhole_format::key::compare;
///
/// assert_eq!(compare(b"row:1", b"row:2"), Ordering::Less);
/// assert_eq!(compare(b"row", b"row:1"), Ordering::Less);
/// assert_eq!(compare(b"row:10", b"row:1"), Ordering::Greater);
/// ```
#[inline]
pub fn compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let n = a.len().min(b.len());
    let (mut x, mut y) = (&a[..n], &b[..n]);
    while let (Some((p, xs)), Some((q, ys))) =
        (x.split_first_chunk::<8>(), y.split_first_chunk::<8>())
    {
        if p != q {
            return u64::from_be_bytes(*p).cmp(&u64::from_be_bytes(*q));
        }
        (x, y) = (xs, ys);
    }
    for (p, q) in x.iter().zip(y) {
        if p != q {
            return p.cmp(q);
        }
    }
    a.len().cmp(&b.len())
}

/// The length of the longest common prefix of `a` and `b`, compared eight bytes at a time
/// (as [`compare`] does): the bytes a block entry shares with the previous key.
///
/// ```
/// use pigeonhole_format::key::common_prefix_len;
///
/// assert_eq!(common_prefix_len(b"row:10:a", b"row:10:b"), 7);
/// assert_eq!(common_prefix_len(b"row", b"row:1"), 3);
/// assert_eq!(common_prefix_len(b"", b"x"), 0);
/// ```
#[inline]
pub fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let (mut x, mut y) = (&a[..n], &b[..n]);
    let mut i = 0;
    while let (Some((p, xs)), Some((q, ys))) =
        (x.split_first_chunk::<8>(), y.split_first_chunk::<8>())
    {
        if p != q {
            let diff = u64::from_be_bytes(*p) ^ u64::from_be_bytes(*q);
            return i + (diff.leading_zeros() / 8) as usize;
        }
        i += 8;
        (x, y) = (xs, ys);
    }
    i + x.iter().zip(y).take_while(|(p, q)| p == q).count()
}

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
        match b {
            0x01 => Ok(Self::Put),
            0x02 => Ok(Self::Merge),
            0x03 => Ok(Self::CellDelete),
            0x04 => Ok(Self::ColumnDelete),
            0x05 => Ok(Self::FamilyDelete),
            _ => Err(Error::Corrupt { what: "key kind" }),
        }
    }

    /// Whether this kind is a deletion of any granularity.
    pub fn is_delete(self) -> bool {
        matches!(
            self,
            Self::CellDelete | Self::ColumnDelete | Self::FamilyDelete
        )
    }
}

/// Byte placed in the kind position of a seek key; sorts before every real [`Kind`].
pub const SEEK_KIND: u8 = 0x00;

fn check_part(part: &[u8]) -> crate::Result<()> {
    if part.len() > MAX_KEY_PART {
        Err(Error::KeyTooLarge)
    } else {
        Ok(())
    }
}

fn put_suffix(out: &mut Vec<u8>, ts: Timestamp, seqno: Seqno, kind: u8) {
    out.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
    out.extend_from_slice(&(u64::MAX - seqno).to_be_bytes());
    out.push(kind);
}

/// Appends the internal key for one cell to `out`. `out` is not cleared, so callers can
/// reuse one buffer per thread and stay allocation-free.
///
/// [`Kind::FamilyDelete`] is rejected here (it is only valid on a marker key; use
/// [`encode_marker_key`]).
///
/// ```
/// use pigeonhole_format::key::{Kind, decode_key, encode_key};
///
/// let mut older = Vec::new();
/// encode_key(&mut older, b"row", b"q", 10, 1, Kind::Put).unwrap();
/// let mut newer = Vec::new();
/// encode_key(&mut newer, b"row", b"q", 20, 2, Kind::Put).unwrap();
/// assert!(newer < older); // newer timestamps sort first
///
/// let parts = decode_key(&older).unwrap();
/// assert!(parts.row.eq_raw(b"row"));
/// assert_eq!((parts.ts, parts.seqno, parts.kind), (10, 1, Kind::Put));
/// ```
pub fn encode_key(
    out: &mut Vec<u8>,
    row: &[u8],
    qualifier: &[u8],
    ts: Timestamp,
    seqno: Seqno,
    kind: Kind,
) -> crate::Result<()> {
    if kind == Kind::FamilyDelete {
        return Err(Error::InvalidArgument {
            what: "FamilyDelete needs encode_marker_key",
        });
    }
    encode_column_prefix(out, row, qualifier)?;
    put_suffix(out, ts, seqno, kind as u8);
    Ok(())
}

/// Appends a family-in-row marker key ([`Kind::FamilyDelete`]) for `row`.
pub fn encode_marker_key(
    out: &mut Vec<u8>,
    row: &[u8],
    ts: Timestamp,
    seqno: Seqno,
) -> crate::Result<()> {
    encode_marker_prefix(out, row)?;
    put_suffix(out, ts, seqno, Kind::FamilyDelete as u8);
    Ok(())
}

/// Appends the marker prefix of `row` (escaped row, terminator, `00 00`): every family marker
/// of the row starts with it. A point get seeks here before seeking to the column.
pub fn encode_marker_prefix(out: &mut Vec<u8>, row: &[u8]) -> crate::Result<()> {
    encode_row_prefix(out, row)?;
    out.extend_from_slice(&MARKER_QUALIFIER);
    Ok(())
}

/// Appends the escaped row and its terminator: a prefix shared by every key of `row`.
pub fn encode_row_prefix(out: &mut Vec<u8>, row: &[u8]) -> crate::Result<()> {
    check_part(row)?;
    escape_into(out, row);
    out.extend_from_slice(&TERMINATOR);
    Ok(())
}

/// Appends the escaped row, escaped qualifier and both terminators: a prefix shared by every
/// version of one column.
pub fn encode_column_prefix(out: &mut Vec<u8>, row: &[u8], qualifier: &[u8]) -> crate::Result<()> {
    check_part(qualifier)?;
    encode_row_prefix(out, row)?;
    escape_into(out, qualifier);
    out.extend_from_slice(&TERMINATOR);
    Ok(())
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
    encode_column_prefix(out, row, qualifier)?;
    put_suffix(out, ts, seqno, SEEK_KIND);
    Ok(())
}

/// Appends the escaped form of `part` (no terminator).
pub fn escape_into(out: &mut Vec<u8>, part: &[u8]) {
    let mut rest = part;
    while let Some(i) = rest.iter().position(|&b| b == 0) {
        out.extend_from_slice(&rest[..=i]);
        out.push(0xFF);
        rest = &rest[i + 1..];
    }
    out.extend_from_slice(rest);
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

/// Scans one escaped string from the front of `b`. Returns its escaped length (the
/// terminator follows it) and its unescaped length, or `Corrupt` if a `0x00` is followed by
/// anything other than `0xFF` or the terminator.
fn scan_escaped(b: &[u8]) -> crate::Result<(usize, usize)> {
    let mut i = 0;
    let mut raw = 0;
    loop {
        let Some(z) = b[i..].iter().position(|&c| c == 0) else {
            return Err(Error::Corrupt {
                what: "key: missing terminator",
            });
        };
        raw += z;
        i += z;
        match b.get(i + 1) {
            Some(0xFF) => {
                raw += 1;
                i += 2;
            }
            Some(0x01) => return Ok((i, raw)),
            _ => {
                return Err(Error::Corrupt {
                    what: "key: bad escape",
                });
            }
        }
    }
}

/// Decodes an internal key. Never panics, whatever the input.
pub fn decode_key(key: &[u8]) -> crate::Result<KeyParts<'_>> {
    let (body, ts, seqno, kind) = split_suffix(key)?;
    let (row_len, row_raw) = scan_escaped(body)?;
    let rest = &body[row_len + 2..];
    let qualifier = if rest == MARKER_QUALIFIER {
        None
    } else {
        let (q_len, q_raw) = scan_escaped(rest)?;
        if q_len + 2 != rest.len() {
            return Err(Error::Corrupt {
                what: "key: trailing bytes",
            });
        }
        if q_raw > MAX_KEY_PART {
            return Err(Error::KeyTooLarge);
        }
        Some(Escaped(&rest[..q_len]))
    };
    if row_raw > MAX_KEY_PART {
        return Err(Error::KeyTooLarge);
    }
    if qualifier.is_none() != (kind == Kind::FamilyDelete) {
        return Err(Error::Corrupt {
            what: "key: marker kind",
        });
    }
    Ok(KeyParts {
        row: Escaped(&body[..row_len]),
        qualifier,
        ts,
        seqno,
        kind,
    })
}

/// [`decode_key`] for a key whose first `row_len + 2` bytes (an escaped row and its
/// terminator) are those of a key already decoded: the row is not scanned again, and
/// everything after it is checked as `decode_key` checks it. A writer adding a row's cells in
/// order decodes each row once.
///
/// ```
/// use pigeonhole_format::key::{Kind, decode_key, decode_key_in_row, encode_key};
///
/// let mut a = Vec::new();
/// encode_key(&mut a, b"row", b"q1", 5, 1, Kind::Put).unwrap();
/// let mut b = Vec::new();
/// encode_key(&mut b, b"row", b"q2", 5, 2, Kind::Put).unwrap();
/// let row_len = decode_key(&a).unwrap().row.as_escaped().len();
/// assert_eq!(decode_key_in_row(&b, row_len).unwrap(), decode_key(&b).unwrap());
/// ```
pub fn decode_key_in_row(key: &[u8], row_len: usize) -> crate::Result<KeyParts<'_>> {
    let (body, ts, seqno, kind) = split_suffix(key)?;
    if body.get(row_len..row_len + 2) != Some(&TERMINATOR[..]) {
        return Err(Error::Corrupt {
            what: "key: row terminator",
        });
    }
    let rest = &body[row_len + 2..];
    let qualifier = if rest == MARKER_QUALIFIER {
        None
    } else {
        let (q_len, q_raw) = scan_escaped(rest)?;
        if q_len + 2 != rest.len() {
            return Err(Error::Corrupt {
                what: "key: trailing bytes",
            });
        }
        if q_raw > MAX_KEY_PART {
            return Err(Error::KeyTooLarge);
        }
        Some(Escaped(&rest[..q_len]))
    };
    if qualifier.is_none() != (kind == Kind::FamilyDelete) {
        return Err(Error::Corrupt {
            what: "key: marker kind",
        });
    }
    Ok(KeyParts {
        row: Escaped(&body[..row_len]),
        qualifier,
        ts,
        seqno,
        kind,
    })
}

/// Splits the fixed 17-byte suffix off `key` without parsing the variable part.
pub fn split_suffix(key: &[u8]) -> crate::Result<(&[u8], Timestamp, Seqno, Kind)> {
    let Some(split) = key.len().checked_sub(SUFFIX_LEN) else {
        return Err(Error::Truncated {
            what: "internal key",
        });
    };
    let (body, suffix) = key.split_at(split);
    let mut ts = [0; 8];
    ts.copy_from_slice(&suffix[..8]);
    let mut seqno = [0; 8];
    seqno.copy_from_slice(&suffix[8..16]);
    let kind = Kind::from_u8(suffix[16])?;
    Ok((
        body,
        u64::MAX - u64::from_be_bytes(ts),
        u64::MAX - u64::from_be_bytes(seqno),
        kind,
    ))
}

/// Length of the row prefix of `key` (escaped row plus terminator), found by scanning for the
/// first unescaped terminator. Two keys belong to the same row iff these prefixes are equal.
pub fn row_prefix_len(key: &[u8]) -> crate::Result<usize> {
    Ok(scan_escaped(key)?.0 + TERMINATOR.len())
}

/// Length of the column prefix of `key` (row prefix, escaped qualifier and its terminator,
/// or the marker bytes). Two keys address the same column iff these prefixes are equal.
pub fn column_prefix_len(key: &[u8]) -> crate::Result<usize> {
    let row = row_prefix_len(key)?;
    let rest = &key[row..];
    if rest.starts_with(&MARKER_QUALIFIER) {
        return Ok(row + MARKER_QUALIFIER.len());
    }
    Ok(row + scan_escaped(rest)?.0 + TERMINATOR.len())
}

/// An escaped byte string borrowed from an internal key.
///
/// ```
/// use pigeonhole_format::key::Escaped;
///
/// let e = Escaped::new(b"a\x00\xFFb");
/// assert!(!e.is_verbatim());
/// assert!(e.eq_raw(b"a\x00b"));
/// let mut raw = Vec::new();
/// e.unescape_into(&mut raw);
/// assert_eq!(raw, b"a\x00b");
/// ```
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
        !self.0.contains(&0)
    }

    /// Appends the unescaped bytes to `out`.
    pub fn unescape_into(&self, out: &mut Vec<u8>) {
        out.extend(self.raw_bytes());
    }

    /// Compares with raw (unescaped) bytes without allocating.
    pub fn eq_raw(&self, raw: &[u8]) -> bool {
        self.raw_bytes().eq(raw.iter().copied())
    }

    /// Orders the unescaped bytes against `raw` without allocating.
    pub(crate) fn cmp_raw(&self, raw: &[u8]) -> std::cmp::Ordering {
        self.raw_bytes().cmp(raw.iter().copied())
    }

    /// Whether the unescaped bytes start with `raw`, without allocating.
    pub(crate) fn starts_with_raw(&self, raw: &[u8]) -> bool {
        let mut it = self.raw_bytes();
        raw.iter().all(|&b| it.next() == Some(b))
    }

    /// The unescaped bytes. Tolerates malformed input: a `0x00` not followed by `0xFF` is
    /// taken as is.
    fn raw_bytes(&self) -> impl Iterator<Item = u8> + 'a {
        let mut it = self.0.iter().copied().peekable();
        std::iter::from_fn(move || {
            let b = it.next()?;
            if b == 0 {
                it.next_if_eq(&0xFF);
            }
            Some(b)
        })
    }
}

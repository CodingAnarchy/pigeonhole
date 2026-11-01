//! Byte-budgeted reads (D308, ICR 0103): [`Scan::read_within`](super::Scan::read_within) and
//! [`KeysRead::read_within`](super::KeysRead::read_within) return the longest prefix of the
//! read's cells whose size is at most a budget, as owned rows, with where to resume.
//!
//! The read runs the engine's bounded read (D296) with a `ByteBudget` reducer over a visitor
//! that copies each cell's stored value: the scan's own loop is untouched (D296 R3). Sizes
//! follow D308 §2: each cell its qualifier, its value (as [`CellRef::value`](crate::CellRef)
//! returns it) and 16 bytes, and each row's key once.

use std::ops::Bound;
use std::sync::Arc;

use pigeonhole_engine::{
    BUDGET_ITEM_OVERHEAD, ByteBudget, CellData, CellKey, CellVisitor, ColumnPosition, FamilyId,
    KeySet, ReadInput, ReduceSpec, Timestamp, Visit,
};

use super::{Selection, TableCore};
use crate::cell::{Row, RowBuf};
use crate::{Error, ErrorCode, Result};

/// Where a position resumes: after a whole row, after a column inside it, or at a row (from
/// its first column: an empty page's start).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Mark {
    AfterRow,
    AfterColumn(ColumnPosition),
    AtRow,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Position {
    key: Vec<u8>,
    mark: Mark,
}

/// The byte form's version.
const VERSION: u8 = 1;
const SCAN: u8 = b'S';
const KEYS: u8 = b'K';

impl Position {
    fn to_bytes(&self, kind: u8) -> Vec<u8> {
        let mut b = vec![VERSION, kind];
        b.push(match self.mark {
            Mark::AfterRow => 0,
            Mark::AfterColumn(_) => 1,
            Mark::AtRow => 2,
        });
        b.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        b.extend_from_slice(&self.key);
        if let Mark::AfterColumn(c) = &self.mark {
            b.extend_from_slice(&c.to_bytes());
        }
        b
    }

    fn from_bytes(b: &[u8], kind: u8, what: &str) -> Result<Self> {
        let bad = || Error::new(ErrorCode::InvalidArgument, format!("not a {what}"));
        if b.len() < 7 || b[0] != VERSION || b[1] != kind {
            return Err(bad());
        }
        let len = u32::from_le_bytes(b[3..7].try_into().expect("4 bytes")) as usize;
        let key = b.get(7..7 + len).ok_or_else(bad)?.to_vec();
        let rest = &b[7 + len..];
        let mark = match (b[2], rest.is_empty()) {
            (0, true) => Mark::AfterRow,
            (2, true) => Mark::AtRow,
            (1, false) => Mark::AfterColumn(ColumnPosition::from_bytes(rest).map_err(|_| bad())?),
            _ => return Err(bad()),
        };
        Ok(Self { key, mark })
    }
}

/// Where a [`Scan::read_within`](super::Scan::read_within) page stopped, to resume from with
/// [`Scan::resume_after`](super::Scan::resume_after): after its last row, after a column
/// inside it when the page cut the row, or (an empty page) at its own start. Its bytes
/// (`to_bytes`) are versioned and stable, so a client can hand it back later.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScanPosition(Position);

impl ScanPosition {
    /// The position as bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes(SCAN)
    }

    /// The position [`to_bytes`](ScanPosition::to_bytes) wrote; anything else is
    /// `InvalidArgument`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Position::from_bytes(bytes, SCAN, "scan position").map(Self)
    }

    /// The row the position is in or after.
    pub fn row(&self) -> &[u8] {
        &self.0.key
    }
}

/// Where a [`KeysRead::read_within`](super::KeysRead::read_within) page stopped, to resume
/// from with [`KeysRead::resume_after`](super::KeysRead::resume_after). It names the last
/// key returned (and the column when the page cut its row), not an index into the key set,
/// so a set rebuilt between pages (keys added or removed) resumes at the right key
/// (cli692's S1 on #1250).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeysPosition(Position);

impl KeysPosition {
    /// The position as bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes(KEYS)
    }

    /// The position [`to_bytes`](KeysPosition::to_bytes) wrote; anything else is
    /// `InvalidArgument`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Position::from_bytes(bytes, KEYS, "multi-get position").map(Self)
    }

    /// The key the position is at or after.
    pub fn key(&self) -> &[u8] {
        &self.0.key
    }
}

macro_rules! page {
    ($(#[$doc:meta])* $name:ident, $pos:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone)]
        pub struct $name {
            rows: Vec<Row>,
            bytes: u64,
            next_bytes: Option<u64>,
            resume: Option<$pos>,
        }

        impl $name {
            /// The page's rows, in key order. A row the budget cut holds the columns that fit.
            pub fn rows(&self) -> &[Row] {
                &self.rows
            }

            /// The rows, owned.
            pub fn into_rows(self) -> Vec<Row> {
                self.rows
            }

            /// The page's size by D308's rule: each cell's qualifier, value and 16 bytes,
            /// and each row's key once. Never more than the budget.
            pub fn bytes(&self) -> u64 {
                self.bytes
            }

            /// Whether the budget stopped the read (a `limit` stopping it first does not
            /// count).
            pub fn cut_by_budget(&self) -> bool {
                self.next_bytes.is_some()
            }

            /// With [`cut_by_budget`](Self::cut_by_budget), the size of the first column that
            /// didn't fit, its row key included: a read resumed at this page's
            /// [`resume`](Self::resume) with a budget of this many bytes returns it.
            pub fn next_bytes(&self) -> Option<u64> {
                self.next_bytes
            }

            /// Where to resume: `Some` whenever the read stopped before its input ended (by
            /// the budget or by `limit`), `None` once it is exhausted. An empty page cut by
            /// the budget resumes at its own start.
            pub fn resume(&self) -> Option<&$pos> {
                self.resume.as_ref()
            }
        }
    };
}

page!(
    /// A [`Scan::read_within`](super::Scan::read_within) result (D308).
    ScanPage,
    ScanPosition
);
page!(
    /// A [`KeysRead::read_within`](super::KeysRead::read_within) result (D308).
    KeysPage,
    KeysPosition
);

/// Copies each cell's stored value (tag byte first) for the page.
struct Copy;

impl CellVisitor for Copy {
    type Item = Vec<u8>;
    fn visit(
        &mut self,
        _: &[u8],
        _: FamilyId,
        _: &[u8],
        _: Timestamp,
        value: &[u8],
    ) -> Visit<Vec<u8>> {
        Visit::Emit(value.to_vec())
    }
}

/// A cell's own size (D308 §2): its qualifier, its value as `CellRef::value` gives it (the
/// stored bytes after the tag), and the overhead. It takes `&Vec<u8>` because the reducer
/// hands its size function `&Item`, and the item is the copied value.
#[allow(clippy::ptr_arg)]
fn size(cell: &pigeonhole_engine::CellRef<'_>, stored: &Vec<u8>) -> u64 {
    cell.qualifier.len() as u64 + stored.len().saturating_sub(1) as u64 + BUDGET_ITEM_OVERHEAD
}

/// A page's parts before its resume point is set.
struct Read {
    rows: Vec<Row>,
    /// Each row's size, by D308's rule.
    row_bytes: Vec<u64>,
    last: Option<CellKey>,
    overflow: Option<pigeonhole_engine::Overflow>,
    table: pigeonhole_engine::TableId,
}

fn read(
    core: &TableCore,
    sel: &mut Selection,
    input: ReadInput,
    resume: InRow,
    budget: u64,
) -> Result<Read> {
    let (info, snapshot, families) = sel.start(core)?;
    let mut spec = ReduceSpec::new(input);
    let mut r = sel.spec.clone();
    r.families = families.into_vec();
    spec.read = r;
    spec.resume = resume;
    let table = core.info.id;
    let (acc, _) = core.with(|e| {
        e.reduce_into(
            &snapshot,
            table,
            &spec,
            || Copy,
            || ByteBudget::new(budget, size),
        )
    })?;
    let page = acc.into_page();
    let last = page.items.last().map(|(k, _)| k.clone());
    let (mut rows, mut row_bytes) = (Vec::new(), Vec::new());
    let mut buf: Option<(RowBuf, u64)> = None;
    for (key, stored) in page.items {
        let own = key.qualifier.len() as u64
            + stored.len().saturating_sub(1) as u64
            + BUDGET_ITEM_OVERHEAD;
        if buf.as_ref().is_none_or(|(b, _)| b.key != key.row) {
            if let Some((b, n)) = buf.take() {
                rows.push(Row::new(b));
                row_bytes.push(n);
            }
            let mut b = RowBuf::new(Arc::clone(&info), 1);
            b.key = key.row.clone();
            buf = Some((b, key.row.len() as u64));
        }
        let (b, n) = buf.as_mut().expect("set above");
        *n += own;
        let start = b.qualifiers.len();
        b.qualifiers.extend_from_slice(&key.qualifier);
        let range = start..b.qualifiers.len();
        b.push(key.family, range, CellData::from_stored(key.ts, stored));
    }
    if let Some((b, n)) = buf {
        rows.push(Row::new(b));
        row_bytes.push(n);
    }
    Ok(Read {
        rows,
        row_bytes,
        last,
        overflow: page.overflow,
        table,
    })
}

impl Read {
    /// The page's resume point when the budget cut it (`None` otherwise), by the pinned
    /// rules: after the last kept column when the cut is inside its row, after the last kept
    /// row when it is at a row boundary, and at the page's own start when nothing fit.
    fn resume(&self, given: Option<&Position>) -> Option<Position> {
        let o = self.overflow.as_ref()?;
        Some(match &self.last {
            Some(l) if l.row == o.key.row => Position {
                key: l.row.clone(),
                mark: Mark::AfterColumn(ColumnPosition::new(
                    self.table,
                    l.family,
                    l.qualifier.clone(),
                )),
            },
            Some(l) => Position {
                key: l.row.clone(),
                mark: Mark::AfterRow,
            },
            None => given.cloned().unwrap_or(Position {
                key: o.key.row.clone(),
                mark: Mark::AtRow,
            }),
        })
    }
}

/// A resume inside a row: the row and the column to resume after.
type InRow = Option<(Vec<u8>, ColumnPosition)>;

/// The later of two range starts.
fn later(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    use Bound::{Excluded, Included, Unbounded};
    let rank = |x: &Bound<Vec<u8>>| match x {
        Unbounded => (None, 0),
        Included(k) => (Some(k.clone()), 0),
        Excluded(k) => (Some(k.clone()), 1),
    };
    if rank(&a) >= rank(&b) { a } else { b }
}

/// A scan's range and resume after `position` (cli692's pinned rule 3): it starts at the
/// later of the range's start and the position; a position before the range starts at the
/// range, one past its end reads nothing. A mid-row position whose row the range excludes
/// resumes after that row (perf287's N1 on #1264).
fn clamp(start: Bound<Vec<u8>>, position: Option<&Position>) -> (Bound<Vec<u8>>, InRow) {
    let Some(p) = position else {
        return (start, None);
    };
    match &p.mark {
        Mark::AtRow => (later(start, Bound::Included(p.key.clone())), None),
        Mark::AfterRow => (later(start, Bound::Excluded(p.key.clone())), None),
        Mark::AfterColumn(c) => {
            let at = Bound::Included(p.key.clone());
            if later(start.clone(), at.clone()) == at {
                (at, Some((p.key.clone(), c.clone())))
            } else {
                // The range starts after the position's column (its row excluded, or a
                // later row): the position is before the range.
                (start, None)
            }
        }
    }
}

/// [`Scan::read_within`](super::Scan::read_within).
pub(super) fn scan(
    core: &TableCore,
    sel: &mut Selection,
    (start, end): (Bound<Vec<u8>>, Bound<Vec<u8>>),
    limit: Option<u64>,
    after: Option<&ScanPosition>,
    budget: u64,
) -> Result<ScanPage> {
    if limit == Some(0) {
        return Ok(ScanPage {
            rows: Vec::new(),
            bytes: 0,
            next_bytes: None,
            resume: None,
        });
    }
    let given = after.map(|p| &p.0);
    let (lo, resume) = clamp(start, given);
    let r = read(
        core,
        sel,
        ReadInput::Ranges(vec![(lo, end)]),
        resume,
        budget,
    )?;
    if let Some(n) = limit.map(|n| usize::try_from(n).unwrap_or(usize::MAX))
        && (r.rows.len() > n
            || (r.rows.len() == n
                && r.overflow.as_ref().is_some_and(|o| {
                    r.rows
                        .last()
                        .is_some_and(|l| l.key() != o.key.row.as_slice())
                })))
    {
        // `limit` stopped the read first: its rows, resumed after the last.
        let mut rows = r.rows;
        rows.truncate(n);
        let key = rows.last().map(|l| l.key().to_vec()).unwrap_or_default();
        return Ok(ScanPage {
            bytes: r.row_bytes[..n].iter().sum(),
            rows,
            next_bytes: None,
            resume: Some(ScanPosition(Position {
                key,
                mark: Mark::AfterRow,
            })),
        });
    }
    Ok(ScanPage {
        resume: r.resume(given).map(ScanPosition),
        next_bytes: r.overflow.as_ref().map(|o| o.bytes),
        bytes: r.row_bytes.iter().sum(),
        rows: r.rows,
    })
}

/// [`KeysRead::read_within`](super::KeysRead::read_within).
pub(super) fn keys(
    core: &TableCore,
    sel: &mut Selection,
    set: &KeySet,
    after: Option<&KeysPosition>,
    budget: u64,
) -> Result<KeysPage> {
    let given = after.map(|p| &p.0);
    // The keys from the position on (cli692's S1: by key, so a rebuilt set resumes at the
    // right one), and the resume inside the position's row when it names a column.
    let (from, resume) = match given {
        None => (set.clone(), None),
        Some(p) => {
            let keep = |k: &[u8]| match &p.mark {
                Mark::AfterRow => k > p.key.as_slice(),
                Mark::AfterColumn(_) | Mark::AtRow => k >= p.key.as_slice(),
            };
            let kept = KeySet::from_sorted(set.iter().filter(|k| keep(k)))
                .expect("a sorted set's subset is sorted");
            let resume = match &p.mark {
                Mark::AfterColumn(c) => Some((p.key.clone(), c.clone())),
                _ => None,
            };
            (kept, resume)
        }
    };
    if from.is_empty() {
        return Ok(KeysPage {
            rows: Vec::new(),
            bytes: 0,
            next_bytes: None,
            resume: None,
        });
    }
    let r = read(core, sel, ReadInput::Keys(Arc::new(from)), resume, budget)?;
    Ok(KeysPage {
        resume: r.resume(given).map(KeysPosition),
        next_bytes: r.overflow.as_ref().map(|o| o.bytes),
        bytes: r.row_bytes.iter().sum(),
        rows: r.rows,
    })
}

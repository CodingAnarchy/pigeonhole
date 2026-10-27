//! Lazy filtered sources under a `MergingCursor` (D288): the merge yields exactly what the
//! eager merge yields (random sources, filters and movement sequences, equal keys across
//! sources, an error partway through a settle), and a source with nothing admissible stops
//! near the merge's position instead of walking ahead to its next admitted entry.

use std::cell::Cell;
use std::collections::BTreeSet;
use std::ops::Bound;
use std::rc::Rc;

use pigeonhole_compaction::{FilteredCursor, MergingCursor, VecCursor};
use pigeonhole_format::Cursor;
use pigeonhole_format::key::{Kind, SUFFIX_LEN, encode_key};
use pigeonhole_format::scan::{QualifierFilter, ScanFilter};
use proptest::prelude::*;

type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// A source that counts its moves and can fail a move: the inner cursor of the sources
/// under test.
struct Probe {
    inner: VecCursor,
    /// Moves made (seeks and steps), shared with the test.
    moves: Rc<Cell<usize>>,
    /// Fails the move that would take `moves` past this.
    fail_after: Rc<Cell<usize>>,
}

impl Probe {
    fn new(entries: Entries) -> (Self, Rc<Cell<usize>>, Rc<Cell<usize>>) {
        let moves = Rc::new(Cell::new(0));
        let fail_after = Rc::new(Cell::new(usize::MAX));
        let probe = Self {
            inner: VecCursor::new(entries),
            moves: Rc::clone(&moves),
            fail_after: Rc::clone(&fail_after),
        };
        (probe, moves, fail_after)
    }

    fn moved(&self) -> Result<(), String> {
        let n = self.moves.get() + 1;
        if n > self.fail_after.get() {
            return Err(format!("injected failure at move {n}"));
        }
        self.moves.set(n);
        Ok(())
    }
}

impl Cursor for Probe {
    type Error = String;

    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn seek_to_first(&mut self) -> Result<(), String> {
        self.moved()?;
        self.inner.seek_to_first().map_err(|e| e.to_string())
    }

    fn seek(&mut self, target: &[u8]) -> Result<(), String> {
        self.moved()?;
        self.inner.seek(target).map_err(|e| e.to_string())
    }

    fn next(&mut self) -> Result<(), String> {
        self.moved()?;
        self.inner.next().map_err(|e| e.to_string())
    }

    /// Passes the rest of `column` in one call (as a memtable's stale-tail index does), so
    /// the merge's `skip_column -> true` path runs.
    fn skip_column(&mut self, column: &[u8]) -> Result<bool, String> {
        self.moved()?;
        self.inner.next().map_err(|e| e.to_string())?;
        while self.inner.valid() && in_column(self.inner.key(), column) {
            self.inner.next().map_err(|e| e.to_string())?;
        }
        Ok(true)
    }
}

/// Whether `key` is an entry of `column` (an internal key without its suffix).
fn in_column(key: &[u8], column: &[u8]) -> bool {
    key.len() == column.len() + SUFFIX_LEN && key.starts_with(column)
}

/// An internal key from a small space, so sources share keys: three rows, three qualifiers,
/// three timestamps, two seqnos.
fn key(n: usize) -> Vec<u8> {
    let row = [b"a", b"b", b"c"][n % 3];
    let q = [&b"x"[..], b"y", b"yz"][(n / 3) % 3];
    let ts = 1 + ((n / 9) % 3) as u64;
    let seqno = 1 + ((n / 27) % 2) as u64;
    let mut k = Vec::new();
    encode_key(&mut k, row, q, ts, seqno, Kind::Put).unwrap();
    k
}

const KEYS: usize = 54;

fn sources() -> impl Strategy<Value = Vec<Entries>> {
    prop::collection::vec(prop::collection::btree_set(0..KEYS, 0..16), 1..=5).prop_map(|srcs| {
        srcs.into_iter()
            .enumerate()
            .map(|(s, set): (usize, BTreeSet<usize>)| {
                set.into_iter()
                    .map(|n| (key(n), vec![s as u8, n as u8]))
                    .collect()
            })
            .collect()
    })
}

fn filters() -> impl Strategy<Value = ScanFilter> {
    let qualifiers = prop_oneof![
        Just(QualifierFilter::All),
        Just(QualifierFilter::Prefix(b"y".to_vec())),
        Just(QualifierFilter::Prefix(b"x".to_vec())),
        Just(QualifierFilter::Range(
            Bound::Included(b"y".to_vec()),
            Bound::Unbounded
        )),
        Just(QualifierFilter::Range(
            Bound::Excluded(b"x".to_vec()),
            Bound::Excluded(b"yz".to_vec())
        )),
    ];
    let time = prop_oneof![
        Just(None),
        (1u64..4, 1u64..5).prop_map(|(a, b)| Some((a, b.max(a)))),
    ];
    (qualifiers, time).prop_map(|(qualifiers, time_range)| {
        let mut f = ScanFilter::all();
        f.qualifiers = qualifiers;
        f.time_range = time_range;
        f
    })
}

#[derive(Debug, Clone)]
enum Op {
    First,
    Seek(usize),
    /// To the larger of this key and the current one.
    SeekForward(usize),
    Next,
    SkipRow,
    /// Past the current entry's column: only the top moves.
    SkipColumn,
}

fn ops() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(
        prop_oneof![
            1 => Just(Op::First),
            2 => (0..KEYS).prop_map(Op::Seek),
            3 => (0..KEYS).prop_map(Op::SeekForward),
            8 => Just(Op::Next),
            2 => Just(Op::SkipRow),
            3 => Just(Op::SkipColumn),
        ],
        1..60,
    )
}

type Merge = MergingCursor<FilteredCursor<Probe>>;

/// The merge of `sources` under `filter`, lazy or eager, with each source's move counter.
fn merge(sources: &[Entries], filter: &ScanFilter, lazy: bool) -> (Merge, Vec<Rc<Cell<usize>>>) {
    let mut moves = Vec::new();
    let mut fails = Vec::new();
    let cursors = sources
        .iter()
        .map(|e| {
            let (p, m, f) = Probe::new(e.clone());
            moves.push(m);
            fails.push(f);
            if lazy {
                FilteredCursor::lazy(p, filter.clone())
            } else {
                FilteredCursor::new(p, filter.clone())
            }
        })
        .collect();
    (MergingCursor::new(cursors), moves)
}

fn at(m: &Merge) -> Option<(Vec<u8>, Vec<u8>)> {
    m.valid().then(|| (m.key().to_vec(), m.value().to_vec()))
}

fn apply(m: &mut Merge, op: &Op, target_floor: Option<&[u8]>) -> Result<(), String> {
    match op {
        Op::First => m.seek_to_first(),
        Op::Seek(n) => m.seek(&key(*n)),
        Op::SeekForward(n) => {
            let mut target = key(*n);
            if let Some(k) = target_floor {
                target = target.max(k.to_vec());
            }
            m.seek_forward(&target)
        }
        Op::Next => m.next(),
        Op::SkipRow => m.skip_row(),
        // The column of the eager merge's current entry (the floor), which both merges are on.
        Op::SkipColumn => match target_floor {
            Some(k) => m
                .skip_column(&k[..k.len() - SUFFIX_LEN])
                .map(|moved| assert!(moved)),
            None => Ok(()),
        },
    }
}

fn check(sources: Vec<Entries>, filter: ScanFilter, ops: &[Op]) -> Result<(), TestCaseError> {
    let (mut eager, _) = merge(&sources, &filter, false);
    let (mut lazy, _) = merge(&sources, &filter, true);
    eager.seek_to_first().unwrap();
    lazy.seek_to_first().unwrap();
    for (step, op) in ops.iter().enumerate() {
        // Both from the eager merge's position, so the targets are the same.
        let floor = at(&eager).map(|(k, _)| k);
        apply(&mut eager, op, floor.as_deref()).unwrap();
        apply(&mut lazy, op, floor.as_deref()).unwrap();
        prop_assert_eq!(at(&lazy), at(&eager), "after step {} ({:?})", step, op);
        let current = lazy
            .current()
            .map(|c| (c.key().to_vec(), c.value().to_vec()));
        prop_assert_eq!(current, at(&lazy), "current() at step {}", step);
        if let Some(c) = lazy.current() {
            prop_assert!(c.settled(), "unsettled top at step {}", step);
        }
    }
    while eager.valid() {
        prop_assert_eq!(at(&lazy), at(&eager));
        eager.next().unwrap();
        lazy.next().unwrap();
    }
    prop_assert!(!lazy.valid());
    // An exhausted merge stays exhausted on a forward seek, as the eager one does.
    lazy.seek_forward(&key(0)).unwrap();
    eager.seek_forward(&key(0)).unwrap();
    prop_assert_eq!(at(&lazy), at(&eager));
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3_000))]

    /// Every movement after every other, over random sources (equal keys across sources
    /// included) and filters.
    #[test]
    fn a_lazy_merge_yields_what_the_eager_merge_yields(
        sources in sources(),
        filter in filters(),
        ops in ops(),
    ) {
        check(sources, filter, &ops)?;
    }

    /// A move that fails partway through settling a lazy source leaves a merge that a seek
    /// puts back on the eager merge's stream.
    #[test]
    fn an_error_mid_settle_then_a_seek_gives_the_same_stream(
        sources in sources(),
        filter in filters(),
        fail_at in 0usize..40,
        target in 0..KEYS,
    ) {
        let (mut lazy, moves) = merge(&sources, &filter, true);
        // Fail the first source's move `fail_at` moves from now, wherever that falls: on a
        // seek, a step, or inside a settle.
        let fail_after = fail_handle(&lazy, 0);
        fail_after.set(moves[0].get() + fail_at);
        let mut failed = lazy.seek_to_first().is_err();
        while !failed && lazy.valid() {
            failed = lazy.next().is_err();
        }
        fail_after.set(usize::MAX);
        let (mut eager, _) = merge(&sources, &filter, false);
        lazy.seek(&key(target)).unwrap();
        eager.seek(&key(target)).unwrap();
        while eager.valid() {
            prop_assert_eq!(at(&lazy), at(&eager), "failed: {}", failed);
            eager.next().unwrap();
            lazy.next().unwrap();
        }
        prop_assert!(!lazy.valid());
    }
}

/// The failure switch of source `i`'s probe.
fn fail_handle(m: &Merge, i: usize) -> Rc<Cell<usize>> {
    Rc::clone(&m.sources()[i].inner().fail_after)
}

/// Equal keys across lazy sources come out once per source, the earlier source first, and
/// the merge ends (a source parks only strictly past its bound, so a tie never stalls it).
#[test]
fn equal_keys_across_lazy_sources_come_out_in_source_order() {
    let mut filter = ScanFilter::all();
    filter.time_range = Some((2, 4));
    for n in [2usize, 3, 5] {
        let sources: Vec<Entries> = (0..n)
            .map(|s| {
                (0..KEYS)
                    .map(|k| (key(k), vec![s as u8, k as u8]))
                    .collect()
            })
            .collect();
        let (mut lazy, _) = merge(&sources, &filter, true);
        let (mut eager, _) = merge(&sources, &filter, false);
        lazy.seek_to_first().unwrap();
        eager.seek_to_first().unwrap();
        let mut count = 0;
        while eager.valid() {
            assert_eq!(at(&lazy), at(&eager), "{n} sources");
            eager.next().unwrap();
            lazy.next().unwrap();
            count += 1;
        }
        assert!(!lazy.valid());
        assert_eq!(count, n * KEYS * 2 / 3, "{n} sources: ts 2 and 3 of 1..=3");
    }
}

/// The #711 shape: one source holds the rows the scan wants, another holds many rows with
/// nothing admissible. Eagerly, the second walks all of them on the first seek; lazily, it
/// parks on its first entry past the merge's position and is never moved again.
#[test]
fn a_source_with_nothing_admissible_stops_at_the_merge_frontier() {
    let k = |row: &[u8], q: &[u8], ts: u64| {
        let mut k = Vec::new();
        encode_key(&mut k, row, q, ts, 1, Kind::Put).unwrap();
        k
    };
    let mut filter = ScanFilter::all();
    filter.time_range = Some((10, 20));
    // Entity 1's rows in range (more than the ten read, as a scan's limit leaves them: a
    // source alone in the merge settles fully, which is right); the other source has
    // entity 1's newer points, then 1000 rows of later entities, all newer than the range.
    let wanted: Entries = (0..12u8)
        .map(|i| (k(&[b'1', b':', b'b', i], b"v", 15), vec![]))
        .collect();
    let mut other: Entries = (0..50u8)
        .map(|i| (k(&[b'1', b':', b'a', i], b"v", 99), vec![]))
        .collect();
    other.extend((0..1000u16).map(|i| {
        let mut row = b"2:".to_vec();
        row.extend_from_slice(&i.to_be_bytes());
        (k(&row, b"v", 99), vec![])
    }));
    for lazy in [false, true] {
        let (mut m, moves) = merge(&[wanted.clone(), other.clone()], &filter, lazy);
        m.seek(b"1:").unwrap();
        let mut rows = 0;
        while m.valid() && rows < 10 {
            rows += 1;
            m.next().unwrap();
        }
        assert_eq!(rows, 10);
        let walked = moves[1].get();
        if lazy {
            // Entity 1's 50 newer points, plus one to park on entity 2.
            assert!(walked <= 52, "lazy: the other source moved {walked} times");
        } else {
            assert!(
                walked > 1000,
                "eager: the other source moved {walked} times"
            );
        }
    }
}

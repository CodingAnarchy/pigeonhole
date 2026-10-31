//! D296 (#490 A): bounded-result reads give exactly what a scan of the same snapshot gives,
//! reduced: the k best, the first n, one per key, a row's first cell, an early stop. Data
//! in SSTs and memtables, with overwrites, deletes and a counter family's merges, so the
//! visitor sees resolved cells only. Cells order as the scan hands them out (row, family,
//! qualifier, newest version first), so each brute force below is positions in the scan.
//! Fan-out (scoped threads, from a thread that drives no shard) equals the sequential read.
//! A `Keys` input (D296 PR 2) reads as a one-row scan of each key would, and as multi-get.

use crate::common;

use std::cmp::Ordering;
use std::future::Future;
use std::ops::{Bound, ControlFlow};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    BUDGET_ITEM_OVERHEAD, ByteBudget, BytePage, CellKey, CellRef, CellVisitor, Dedupe, Engine,
    EngineShard, FamilyOptions, KeySet, Limit, ReadInput, ReadSpec, ReduceSpec, Reducer, RowRange,
    ScanSpec, Snapshot, TopK, ValuePredicate, ValueRef, Visit, WriteBatch,
};
use pigeonhole_format::manifest::FamilyKind;
use pigeonhole_format::{Durability, FamilyId, TableId, Timestamp};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";
const BUDGET: u64 = 64 << 20;
const ROWS: u32 = 300;

/// A visible cell from a scan: (row, family, qualifier, ts, stored value).
type Cell = (Vec<u8>, FamilyId, Vec<u8>, Timestamp, Vec<u8>);

/// A cell's identity: (row, family, qualifier, ts).
type Id = (Vec<u8>, FamilyId, Vec<u8>, Timestamp);

fn drive<F: Future + Unpin>(shards: &mut [EngineShard], mut pending: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..1_000_000 {
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
        if let Poll::Ready(r) = Pin::new(&mut pending).poll(&mut cx) {
            return r;
        }
    }
    panic!("never resolved");
}

fn row(i: u32) -> Vec<u8> {
    format!("r{i:04}").into_bytes()
}

/// A small deterministic score (many ties on purpose: 16 values).
fn score(value: &[u8]) -> u32 {
    value
        .iter()
        .fold(7u32, |h, b| h.wrapping_mul(31).wrapping_add(u32::from(*b)))
        % 16
}

struct Db {
    engine: Arc<Engine>,
    _shards: Vec<EngineShard>,
    table: TableId,
}

/// Rows `r0000..r0299` in families `f` and `g` (bytes) and `c` (a counter), some flushed to
/// SSTs, then overwrites, deletes and more counter operands in the memtables.
fn setup(seed: u64) -> Db {
    let vfs = SimVfs::new(seed);
    let opts = || common::options(Arc::clone(&vfs), 4, BUDGET);
    let db = Engine::open(Path::new(DB), opts()).unwrap();
    let counter = FamilyOptions::default()
        .merge_operator("pigeonhole.i64_add")
        .kind(FamilyKind::Counter);
    let t = db
        .create_table(
            "t",
            &[
                ("f".into(), FamilyOptions::default()),
                ("g".into(), FamilyOptions::default()),
                ("c".into(), counter),
            ],
        )
        .unwrap();
    db.close().unwrap();
    drop(db);
    let (engine, mut shards) = Engine::open_application_owned(Path::new(DB), opts()).unwrap();
    let (f, g, c) = (t.families[0].id, t.families[1].id, t.families[2].id);
    let commit = |shards: &mut Vec<EngineShard>, wb: WriteBatch| {
        drive(
            shards,
            engine.submit(wb, Some(Durability::Buffered)).unwrap(),
        )
        .unwrap();
    };
    for i in 0..ROWS {
        let mut wb = WriteBatch::new();
        for q in 0..3u8 {
            let v = [i.to_le_bytes().as_slice(), &[q]].concat();
            wb.put(t.id, f, &row(i), &[b'a' + q], None, ValueRef::Bytes(&v))
                .unwrap();
        }
        wb.put(
            t.id,
            g,
            &row(i),
            b"x",
            None,
            ValueRef::Bytes(&i.to_be_bytes()),
        )
        .unwrap();
        if i % 10 == 0 {
            wb.merge(t.id, c, &row(i), b"n", ValueRef::I64(i64::from(i % 7)))
                .unwrap();
        }
        commit(&mut shards, wb);
    }
    drive(&mut shards, engine.submit_flush().unwrap()).unwrap();
    for i in (0..ROWS).step_by(7) {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            f,
            &row(i),
            b"a",
            None,
            ValueRef::Bytes(b"overwritten"),
        )
        .unwrap();
        if i % 3 == 0 {
            wb.delete_column(t.id, f, &row(i), b"b", None).unwrap();
        }
        if i % 5 == 0 {
            wb.delete_row(t.id, &row(i + 1), None).unwrap();
        }
        commit(&mut shards, wb);
    }
    for i in (0..ROWS).step_by(10) {
        let mut wb = WriteBatch::new();
        wb.merge(t.id, c, &row(i), b"n", ValueRef::I64(3)).unwrap();
        commit(&mut shards, wb);
    }
    Db {
        engine,
        _shards: shards,
        table: t.id,
    }
}

/// Every visible cell in `range` under `read`, from a scan, in scan order.
fn scan_cells(db: &Db, snap: &Snapshot, range: RowRange, read: &ReadSpec) -> Vec<Cell> {
    let mut spec = ScanSpec::new(range.0, range.1);
    spec.read = read.clone();
    let mut cur = db.engine.scan(snap, db.table, spec).unwrap();
    let mut out = Vec::new();
    while cur.next_row().unwrap() {
        let r = cur.row().to_vec();
        while let Some(c) = cur.next_cell().unwrap() {
            out.push((
                r.clone(),
                c.family,
                c.qualifier.to_vec(),
                c.ts,
                c.stored.to_vec(),
            ));
        }
    }
    out
}

fn ranges() -> Vec<RowRange> {
    vec![
        (Bound::Unbounded, Bound::Excluded(row(60))),
        (Bound::Excluded(row(60)), Bound::Included(row(150))),
        (Bound::Included(row(151)), Bound::Excluded(row(240))),
        (Bound::Included(row(240)), Bound::Unbounded),
    ]
}

fn cells_in_ranges(db: &Db, snap: &Snapshot) -> Vec<Cell> {
    ranges()
        .into_iter()
        .flat_map(|r| scan_cells(db, snap, r, &ReadSpec::default()))
        .collect()
}

/// A result's cells, comparable with scan cells.
fn ids<I>(items: &[(CellKey, I)]) -> Vec<Id> {
    items
        .iter()
        .map(|(k, _)| (k.row.clone(), k.family, k.qualifier.clone(), k.ts))
        .collect()
}

fn cell_ids(cells: &[Cell]) -> Vec<Id> {
    cells
        .iter()
        .map(|(r, f, q, t, _)| (r.clone(), *f, q.clone(), *t))
        .collect()
}

/// Scores each cell's stored value.
struct Score;

impl CellVisitor for Score {
    type Item = u32;
    fn visit(&mut self, _: &[u8], _: FamilyId, _: &[u8], _: Timestamp, value: &[u8]) -> Visit<u32> {
        Visit::Emit(score(value))
    }
}

/// Emits every cell.
struct Every;

impl CellVisitor for Every {
    type Item = ();
    fn visit(&mut self, _: &[u8], _: FamilyId, _: &[u8], _: Timestamp, _: &[u8]) -> Visit<()> {
        Visit::Emit(())
    }
}

/// Runs `reduce` on another thread (one that drives no shard), so it may fan out.
fn off_thread<T: Send>(f: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|s| s.spawn(f).join().unwrap())
}

fn fanned(spec: &ReduceSpec) -> ReduceSpec {
    let mut s = spec.clone();
    s.parallelism = 4;
    s.parallel_floor = 0;
    s
}

#[test]
fn top_k_equals_the_k_smallest_scored_cells_of_a_scan_ties_in_scan_order() {
    let db = setup(4901);
    let snap = db.engine.snapshot().unwrap();
    let cells = cells_in_ranges(&db, &snap);
    let mut order: Vec<(u32, usize)> = cells
        .iter()
        .enumerate()
        .map(|(i, c)| (score(&c.4), i))
        .collect();
    order.sort();
    let want: Vec<Cell> = order[..25].iter().map(|&(_, i)| cells[i].clone()).collect();

    let spec = ReduceSpec::new(ReadInput::Ranges(ranges()));
    let seq = db
        .engine
        .reduce(&snap, db.table, &spec, || Score, || TopK::new(25))
        .unwrap();
    assert_eq!(ids(&seq.items), cell_ids(&want));
    assert_eq!(seq.stats.threads, 1, "the driving thread never fans out");

    let par_spec = fanned(&spec);
    let engine = Arc::clone(&db.engine);
    let par = off_thread(|| engine.reduce(&snap, db.table, &par_spec, || Score, || TopK::new(25)))
        .unwrap();
    assert_eq!(par.items, seq.items, "fan-out equals the sequential read");
    assert!(
        par.stats.threads > 1,
        "the read fanned out: {:?}",
        par.stats
    );
    assert_eq!(par.stats.cells, seq.stats.cells);
}

#[test]
fn limit_keeps_the_first_n_cells_in_scan_order_at_any_fan_out() {
    let db = setup(4902);
    let snap = db.engine.snapshot().unwrap();
    let cells = cells_in_ranges(&db, &snap);
    for n in [1usize, 2, 17, 200] {
        let spec = ReduceSpec::new(ReadInput::Ranges(ranges()));
        let seq = db
            .engine
            .reduce(&snap, db.table, &spec, || Every, || Limit::new(n))
            .unwrap();
        assert_eq!(ids(&seq.items), cell_ids(&cells[..n]), "n = {n}");
        let par_spec = fanned(&spec);
        let engine = Arc::clone(&db.engine);
        let par =
            off_thread(|| engine.reduce(&snap, db.table, &par_spec, || Every, || Limit::new(n)))
                .unwrap();
        assert_eq!(par.items, seq.items, "n = {n}: fan-out equals sequential");
        assert!(par.stats.threads > 1, "n = {n}: the read fanned out");
    }
}

#[test]
fn dedupe_keeps_one_cell_per_item_key_the_first_in_scan_order() {
    let db = setup(4903);
    let snap = db.engine.snapshot().unwrap();
    let mut want = std::collections::BTreeMap::<u32, Cell>::new();
    for c in cells_in_ranges(&db, &snap) {
        want.entry(score(&c.4)).or_insert(c);
    }
    let exclude = Arc::new(std::collections::HashSet::from([3u32]));
    want.remove(&3);
    let make = || Dedupe::new(|s: &u32| *s, Some(Arc::clone(&exclude)));
    let spec = ReduceSpec::new(ReadInput::Ranges(ranges()));
    let got = db
        .engine
        .reduce(&snap, db.table, &spec, || Score, make)
        .unwrap();
    let mut got: Vec<(u32, Id)> = got
        .items
        .iter()
        .map(|(k, s)| (*s, (k.row.clone(), k.family, k.qualifier.clone(), k.ts)))
        .collect();
    got.sort();
    let want: Vec<(u32, Id)> = want
        .into_iter()
        .map(|(s, (r, f, q, t, _))| (s, (r, f, q, t)))
        .collect();
    assert_eq!(got, want);
}

/// Emits a row's first cell, then skips the rest of the row.
struct FirstPerRow;

impl CellVisitor for FirstPerRow {
    type Item = ();
    fn visit(&mut self, _: &[u8], _: FamilyId, _: &[u8], _: Timestamp, _: &[u8]) -> Visit<()> {
        Visit::EmitAndNextRow(())
    }
}

/// Stops the read at the first row at or past `at`.
struct StopAt(Vec<u8>);

impl CellVisitor for StopAt {
    type Item = ();
    fn visit(&mut self, row: &[u8], _: FamilyId, _: &[u8], _: Timestamp, _: &[u8]) -> Visit<()> {
        if row >= self.0.as_slice() {
            Visit::Stop
        } else {
            Visit::Emit(())
        }
    }
}

/// Keeps everything pushed.
#[derive(Default)]
struct All(Vec<(CellKey, ())>);

impl Reducer for All {
    type Item = ();
    fn push(&mut self, cell: CellRef<'_>, item: ()) -> ControlFlow<()> {
        self.0.push((cell.to_key(), item));
        ControlFlow::Continue(())
    }
    fn merge(&mut self, other: Self) {
        self.0.extend(other.0);
        self.0.sort_by(|a, b| a.0.cmp(&b.0));
    }
    fn finish(self) -> Vec<(CellKey, ())> {
        self.0
    }
}

fn whole() -> ReduceSpec {
    ReduceSpec::new(ReadInput::Ranges(vec![(
        Bound::Unbounded,
        Bound::Unbounded,
    )]))
}

#[test]
fn next_row_takes_each_rows_first_cell_and_stop_ends_the_read() {
    let db = setup(4904);
    let snap = db.engine.snapshot().unwrap();
    let cells = scan_cells(
        &db,
        &snap,
        (Bound::Unbounded, Bound::Unbounded),
        &ReadSpec::default(),
    );
    let mut want: Vec<Cell> = Vec::new();
    for c in &cells {
        if want.last().is_none_or(|l| l.0 != c.0) {
            want.push(c.clone());
        }
    }
    let got = db
        .engine
        .reduce(&snap, db.table, &whole(), || FirstPerRow, All::default)
        .unwrap();
    assert_eq!(ids(&got.items), cell_ids(&want));
    assert_eq!(
        got.stats.cells as usize,
        want.len(),
        "the rest of each row was skipped, not visited"
    );

    let at = row(100);
    let got = db
        .engine
        .reduce(
            &snap,
            db.table,
            &whole(),
            || StopAt(at.clone()),
            All::default,
        )
        .unwrap();
    let before: Vec<Cell> = cells.iter().filter(|c| c.0 < at).cloned().collect();
    assert_eq!(ids(&got.items), cell_ids(&before));
}

#[test]
fn deletes_overwrites_merges_and_value_predicates_resolve_before_the_visitor() {
    let db = setup(4905);
    let snap = db.engine.snapshot().unwrap();
    // Every cell, then a value predicate (counters above 5: merged results only).
    let mut over_five = ReadSpec::default();
    over_five.value = Some(ValuePredicate::I64(Ordering::Greater, 5));
    for read in [ReadSpec::default(), over_five] {
        let mut spec = whole();
        spec.read = read.clone();
        let got = db
            .engine
            .reduce(&snap, db.table, &spec, || Every, All::default)
            .unwrap();
        let want = scan_cells(&db, &snap, (Bound::Unbounded, Bound::Unbounded), &read);
        assert!(!want.is_empty());
        assert_eq!(ids(&got.items), cell_ids(&want), "read {:?}", read.value);
    }
    // A deleted row (r0001), a deleted column and an overwrite (r0000).
    let first_two: RowRange = (Bound::Included(row(0)), Bound::Included(row(1)));
    let spec = ReduceSpec::new(ReadInput::Ranges(vec![first_two.clone()]));
    let got = db
        .engine
        .reduce(&snap, db.table, &spec, || Every, All::default)
        .unwrap();
    let want = scan_cells(&db, &snap, first_two, &ReadSpec::default());
    assert_eq!(ids(&got.items), cell_ids(&want));
    assert!(
        got.items.iter().all(|(k, _)| k.row == row(0)),
        "row r0001 was deleted"
    );
}

#[test]
fn overlapping_or_unsorted_ranges_are_refused() {
    let db = setup(4906);
    let snap = db.engine.snapshot().unwrap();
    for ranges in [
        vec![
            (Bound::Unbounded, Bound::Included(row(10))),
            (Bound::Included(row(10)), Bound::Unbounded),
        ],
        vec![
            (Bound::Included(row(50)), Bound::Excluded(row(60))),
            (Bound::Included(row(0)), Bound::Excluded(row(10))),
        ],
        vec![
            (Bound::Unbounded, Bound::Unbounded),
            (Bound::Included(row(0)), Bound::Excluded(row(10))),
        ],
    ] {
        let spec = ReduceSpec::new(ReadInput::Ranges(ranges));
        let err = db
            .engine
            .reduce(&snap, db.table, &spec, || Every, All::default)
            .unwrap_err();
        assert!(
            matches!(err, pigeonhole_engine::Error::InvalidArgument(_)),
            "{err:?}"
        );
    }
}

/// Present rows, deleted rows (r0001 and every 35th from r0036), a row past the data and a
/// key before it.
fn key_set() -> Arc<KeySet> {
    let mut keys: Vec<Vec<u8>> = (0..ROWS).step_by(3).map(row).collect();
    keys.extend([row(1), row(36), row(9999), b"a".to_vec()]);
    Arc::new(KeySet::from_unsorted(keys))
}

/// Each key's cells from a one-row scan, in key order.
fn cells_of_keys(db: &Db, snap: &Snapshot, keys: &KeySet, read: &ReadSpec) -> Vec<Cell> {
    keys.iter()
        .flat_map(|k| {
            let one = (Bound::Included(k.to_vec()), Bound::Included(k.to_vec()));
            scan_cells(db, snap, one, read)
        })
        .collect()
}

#[test]
fn keys_input_reads_each_key_as_a_one_row_scan_and_as_multi_get() {
    let db = setup(4907);
    let snap = db.engine.snapshot().unwrap();
    let keys = key_set();
    let mut over_five = ReadSpec::default();
    over_five.value = Some(ValuePredicate::I64(Ordering::Greater, 5));
    for read in [ReadSpec::default(), over_five] {
        let want = cells_of_keys(&db, &snap, &keys, &read);
        assert!(!want.is_empty());
        let mut spec = ReduceSpec::new(ReadInput::Keys(Arc::clone(&keys)));
        spec.read = read.clone();
        let got = db
            .engine
            .reduce(&snap, db.table, &spec, || Every, All::default)
            .unwrap();
        assert_eq!(ids(&got.items), cell_ids(&want), "read {:?}", read.value);
        assert_eq!(got.stats.cells as usize, want.len());

        let mut cur = db
            .engine
            .multi_get(&snap, db.table, (*keys).clone(), read.clone())
            .unwrap();
        let mut multi = Vec::new();
        while let Some(i) = cur.next_key().unwrap() {
            let r = keys.get(i).to_vec();
            while let Some(c) = cur.next_cell().unwrap() {
                multi.push((r.clone(), c.family, c.qualifier.to_vec(), c.ts));
            }
        }
        assert_eq!(multi, cell_ids(&want), "multi-get agrees");
    }
}

#[test]
fn keys_input_fans_out_per_tablet_and_equals_the_sequential_read() {
    let mut db = setup(4908);
    // Key groups are per tablet: three tablets, so the read has three groups to share.
    for at in [row(100), row(200)] {
        let split = db.engine.split_tablet_pending(db.table, &at).unwrap();
        drive(&mut db._shards, split).unwrap();
    }
    let snap = db.engine.snapshot().unwrap();
    let keys = key_set();
    let cells = cells_of_keys(&db, &snap, &keys, &ReadSpec::default());
    let spec = ReduceSpec::new(ReadInput::Keys(Arc::clone(&keys)));
    for n in [1usize, 17, 100_000] {
        let seq = db
            .engine
            .reduce(&snap, db.table, &spec, || Every, || Limit::new(n))
            .unwrap();
        assert_eq!(ids(&seq.items), cell_ids(&cells[..n.min(cells.len())]));
        let par_spec = fanned(&spec);
        let engine = Arc::clone(&db.engine);
        let par =
            off_thread(|| engine.reduce(&snap, db.table, &par_spec, || Every, || Limit::new(n)))
                .unwrap();
        assert_eq!(par.items, seq.items, "n = {n}: fan-out equals sequential");
        assert!(par.stats.threads > 1, "n = {n}: {:?}", par.stats);
        assert_eq!(par.stats.groups, 3, "n = {n}: one group per tablet");
        // Sequentially, one accumulator takes every group's cells (D308 B2): a full `Limit`
        // ends the read, so the groups after the one that filled it aren't read.
        if n >= cells.len() {
            assert_eq!(seq.stats.groups, 3, "n = {n}: every group read");
        } else {
            assert!(seq.stats.groups <= 3, "n = {n}: {:?}", seq.stats);
            assert_eq!(
                seq.stats.cells, n as u64,
                "n = {n}: nothing read past the n-th cell"
            );
        }
    }
    let seq = db
        .engine
        .reduce(&snap, db.table, &spec, || Score, || TopK::new(25))
        .unwrap();
    let par_spec = fanned(&spec);
    let engine = Arc::clone(&db.engine);
    let par = off_thread(|| engine.reduce(&snap, db.table, &par_spec, || Score, || TopK::new(25)))
        .unwrap();
    assert_eq!(par.items, seq.items);
}

#[test]
fn keys_input_next_row_skips_the_rest_of_a_key_and_stop_ends_the_group() {
    let db = setup(4909);
    let snap = db.engine.snapshot().unwrap();
    let keys = key_set();
    let cells = cells_of_keys(&db, &snap, &keys, &ReadSpec::default());
    let mut want: Vec<Cell> = Vec::new();
    for c in &cells {
        if want.last().is_none_or(|l| l.0 != c.0) {
            want.push(c.clone());
        }
    }
    let spec = ReduceSpec::new(ReadInput::Keys(Arc::clone(&keys)));
    let got = db
        .engine
        .reduce(&snap, db.table, &spec, || FirstPerRow, All::default)
        .unwrap();
    assert_eq!(ids(&got.items), cell_ids(&want));
    assert_eq!(
        got.stats.cells as usize,
        want.len(),
        "the rest of each key was skipped, not visited"
    );

    // `Stop` ends only its own group (one per tablet): the groups before `at` read in full,
    // and every group from the one holding `at` stops at its first key at or past it.
    let at = row(100);
    let got = db
        .engine
        .reduce(&snap, db.table, &spec, || StopAt(at.clone()), All::default)
        .unwrap();
    let before: Vec<Cell> = cells.iter().filter(|c| c.0 < at).cloned().collect();
    assert_eq!(ids(&got.items), cell_ids(&before));
}

// D308: byte budgets.

/// Emits each cell's stored length (its value with the tag byte).
struct Len;

impl CellVisitor for Len {
    type Item = u64;
    fn visit(&mut self, _: &[u8], _: FamilyId, _: &[u8], _: Timestamp, value: &[u8]) -> Visit<u64> {
        Visit::Emit(value.len() as u64)
    }
}

/// An item's own size by D308's rule, with the stored length as the value's.
fn own_size(cell: &CellRef<'_>, len: &u64) -> u64 {
    cell.qualifier.len() as u64 + len + BUDGET_ITEM_OVERHEAD
}

/// The in-budget prefix of `cells`, cut by hand: columns whole while they fit, each row's
/// key once; the cells kept, their size, and the first column left out (its first cell and
/// its size with the row key).
fn by_hand(cells: &[Cell], budget: u64) -> (Vec<Id>, u64, Option<(Id, u64)>) {
    let ids = cell_ids(cells);
    let own = |c: &Cell| c.2.len() as u64 + c.4.len() as u64 + BUDGET_ITEM_OVERHEAD;
    let (mut used, mut keep, mut i) = (0u64, 0usize, 0usize);
    while i < cells.len() {
        let mut j = i + 1;
        while j < cells.len()
            && (&cells[j].0, cells[j].1, &cells[j].2) == (&cells[i].0, cells[i].1, &cells[i].2)
        {
            j += 1;
        }
        let col: u64 = cells[i..j].iter().map(own).sum();
        let row = cells[i].0.len() as u64;
        let charge = if keep == 0 || cells[keep - 1].0 != cells[i].0 {
            row
        } else {
            0
        };
        if used + col + charge > budget {
            return (
                ids[..keep].to_vec(),
                used,
                Some((ids[i].clone(), col + row)),
            );
        }
        used += col + charge;
        keep = j;
        i = j;
    }
    (ids, used, None)
}

fn page_of(page: BytePage<u64>) -> (Vec<Id>, u64, Option<(Id, u64)>) {
    let over = page.overflow.map(|o| {
        (
            (
                o.key.row.clone(),
                o.key.family,
                o.key.qualifier.clone(),
                o.key.ts,
            ),
            o.bytes,
        )
    });
    (ids(&page.items), page.bytes, over)
}

/// Budgets at 0, at every one of the first `n` column boundaries and one byte either side,
/// and past the whole read.
fn budgets(cells: &[Cell], n: usize) -> Vec<u64> {
    let mut out = vec![0, 1, u64::MAX];
    let mut used = 0u64;
    for (i, c) in cells.iter().enumerate().take(n) {
        used += c.2.len() as u64 + c.4.len() as u64 + BUDGET_ITEM_OVERHEAD;
        if i == 0 || cells[i - 1].0 != c.0 {
            used += c.0.len() as u64;
        }
        out.extend([used.saturating_sub(1), used, used + 1]);
    }
    out
}

fn run_budget(
    db: &Db,
    snap: &Snapshot,
    spec: &ReduceSpec,
    budget: u64,
) -> (BytePage<u64>, pigeonhole_engine::ReduceStats) {
    let (acc, stats) = db
        .engine
        .reduce_into(
            snap,
            db.table,
            spec,
            || Len,
            || ByteBudget::new(budget, own_size),
        )
        .unwrap();
    (acc.into_page(), stats)
}

#[test]
fn a_byte_budget_keeps_the_longest_in_budget_prefix_at_any_fan_out() {
    let db = setup(4911);
    let snap = db.engine.snapshot().unwrap();
    let cells = cells_in_ranges(&db, &snap);
    let spec = ReduceSpec::new(ReadInput::Ranges(ranges()));
    let par_spec = fanned(&spec);
    for budget in budgets(&cells, 40) {
        let (seq, _) = run_budget(&db, &snap, &spec, budget);
        let seq = page_of(seq);
        assert_eq!(seq, by_hand(&cells, budget), "budget {budget}");
        assert!(
            seq.1 <= budget,
            "budget {budget}: never more than the budget"
        );
        let engine = Arc::clone(&db.engine);
        let (par, pstats) = off_thread(|| {
            let (acc, stats) = engine
                .reduce_into(
                    &snap,
                    db.table,
                    &par_spec,
                    || Len,
                    || ByteBudget::new(budget, own_size),
                )
                .unwrap();
            (page_of(acc.into_page()), stats)
        });
        assert_eq!(
            par, seq,
            "budget {budget}: fan-out equals the sequential read"
        );
        assert!(pstats.threads > 1, "budget {budget}: the read fanned out");
    }
}

#[test]
fn a_budget_admits_or_refuses_a_column_with_all_its_versions() {
    let db = setup(4912);
    let snap = db.engine.snapshot().unwrap();
    let mut read = ReadSpec::default();
    read.versions = 3;
    let cells: Vec<Cell> = ranges()
        .into_iter()
        .flat_map(|r| scan_cells(&db, &snap, r, &read))
        .collect();
    let multi = cells
        .windows(2)
        .position(|w| (&w[0].0, w[0].1, &w[0].2) == (&w[1].0, w[1].1, &w[1].2))
        .expect("an overwritten column has two versions");
    let mut spec = ReduceSpec::new(ReadInput::Ranges(ranges()));
    spec.read = read;
    // Budgets that cut on each version of the first multi-version column, and around it.
    for budget in budgets(&cells, multi + 4) {
        let (page, _) = run_budget(&db, &snap, &spec, budget);
        let got = page_of(page);
        assert_eq!(got, by_hand(&cells, budget), "budget {budget}");
        if let Some((first, _)) = &got.2 {
            assert!(
                got.0
                    .iter()
                    .all(|k| (&k.0, k.1, &k.2) != (&first.0, first.1, &first.2)),
                "budget {budget}: no version of the cut column is kept"
            );
        }
    }
}

#[test]
fn a_sequential_budgeted_read_stops_at_its_cut() {
    let db = setup(4913);
    let snap = db.engine.snapshot().unwrap();
    let spec = ReduceSpec::new(ReadInput::Ranges(ranges()));
    let all = db
        .engine
        .reduce(&snap, db.table, &spec, || Every, All::default)
        .unwrap();
    let (page, stats) = run_budget(&db, &snap, &spec, 512);
    assert!(page.overflow.is_some());
    assert!(
        stats.groups < all.stats.groups,
        "the groups after the cut weren't read: {stats:?} of {:?}",
        all.stats
    );
    // Past the kept prefix: the cut column's one version (`versions` 1), then the next
    // column's first cell, where the read breaks.
    assert!(
        stats.cells <= page.items.len() as u64 + 2,
        "a column and one cell past the cut at most: {stats:?}, {} kept",
        page.items.len()
    );
}

#[test]
fn a_budget_on_a_keys_input_cuts_the_multi_get_by_hand() {
    let db = setup(4914);
    let snap = db.engine.snapshot().unwrap();
    let keys = key_set();
    let cells = cells_of_keys(&db, &snap, &keys, &ReadSpec::default());
    let spec = ReduceSpec::new(ReadInput::Keys(Arc::clone(&keys)));
    for budget in budgets(&cells, 25) {
        let (page, _) = run_budget(&db, &snap, &spec, budget);
        assert_eq!(page_of(page), by_hand(&cells, budget), "budget {budget}");
    }
}

#[test]
fn a_blob_separated_value_counts_its_whole_length() {
    let vfs = SimVfs::new(4915);
    let opts = || common::options(Arc::clone(&vfs), 1, BUDGET);
    let (engine, mut shards) = Engine::open_application_owned(Path::new(DB), opts()).unwrap();
    let t = engine
        .create_table(
            "b",
            &[("v".into(), FamilyOptions::default().blob_threshold(64))],
        )
        .unwrap();
    let big = vec![7u8; 4096];
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        b"r",
        b"q",
        None,
        ValueRef::Bytes(&big),
    )
    .unwrap();
    drive(
        &mut shards,
        engine.submit(wb, Some(Durability::Buffered)).unwrap(),
    )
    .unwrap();
    drive(&mut shards, engine.submit_flush().unwrap()).unwrap();
    let snap = engine.snapshot().unwrap();
    let spec = whole();
    let (acc, _) = engine
        .reduce_into(
            &snap,
            t.id,
            &spec,
            || Len,
            || ByteBudget::new(u64::MAX, own_size),
        )
        .unwrap();
    let page = acc.into_page();
    // The stored value is the tag byte and the whole value, resolved through its pointer.
    assert_eq!(page.items[0].1, 1 + big.len() as u64);
    assert_eq!(
        page.bytes,
        1 + 1 + big.len() as u64 + BUDGET_ITEM_OVERHEAD + 1
    );
}

/// The cells of `cells` after the last version of the column `(row, family, qualifier)`.
fn after_column(cells: &[Cell], row: &[u8], family: FamilyId, qualifier: &[u8]) -> Vec<Cell> {
    let last = cells
        .iter()
        .rposition(|c| c.0 == row && c.1 == family && c.2 == qualifier)
        .expect("the column is in the read");
    cells[last + 1..].to_vec()
}

#[test]
fn a_read_resumes_after_each_column_of_a_row_for_ranges_and_keys() {
    let db = setup(4916);
    let snap = db.engine.snapshot().unwrap();
    let all_families: Vec<FamilyId> = {
        let cells = scan_cells(
            &db,
            &snap,
            (Bound::Unbounded, Bound::Unbounded),
            &ReadSpec::default(),
        );
        let mut f: Vec<FamilyId> = cells.iter().map(|c| c.1).collect();
        f.sort();
        f.dedup();
        f
    };
    let mut reversed = ReadSpec::default();
    reversed.families = all_families.iter().rev().copied().collect();
    reversed.versions = 2;
    for read in [ReadSpec::default(), reversed] {
        for r in [0u32, 7, 10, 140, 299] {
            let start = row(r);
            let range = (Bound::Included(start.clone()), Bound::Unbounded);
            let cells = scan_cells(&db, &snap, range.clone(), &read);
            let keys: Arc<KeySet> = Arc::new(KeySet::from_unsorted((r..ROWS).step_by(3).map(row)));
            let key_cells = cells_of_keys(&db, &snap, &keys, &read);
            let columns: Vec<(FamilyId, Vec<u8>)> = {
                let mut c: Vec<(FamilyId, Vec<u8>)> = cells
                    .iter()
                    .filter(|c| c.0 == start)
                    .map(|c| (c.1, c.2.clone()))
                    .collect();
                c.dedup();
                c
            };
            assert!(!columns.is_empty(), "row {r} has cells");
            for (family, qualifier) in columns {
                let pos =
                    pigeonhole_engine::ColumnPosition::new(db.table, family, qualifier.clone());
                let mut spec = ReduceSpec::new(ReadInput::Ranges(vec![range.clone()]));
                spec.read = read.clone();
                spec.resume = Some((start.clone(), pos.clone()));
                let got = db
                    .engine
                    .reduce(&snap, db.table, &spec, || Every, All::default)
                    .unwrap();
                let want = after_column(&cells, &start, family, &qualifier);
                assert_eq!(
                    ids(&got.items),
                    cell_ids(&want),
                    "row {r}, after {family:?}/{qualifier:?}"
                );
                let mut kspec = ReduceSpec::new(ReadInput::Keys(Arc::clone(&keys)));
                kspec.read = read.clone();
                kspec.resume = Some((start.clone(), pos));
                let got = db
                    .engine
                    .reduce(&snap, db.table, &kspec, || Every, All::default)
                    .unwrap();
                let want = after_column(&key_cells, &start, family, &qualifier);
                assert_eq!(
                    ids(&got.items),
                    cell_ids(&want),
                    "keys: row {r}, after {family:?}/{qualifier:?}"
                );
            }
        }
    }
}

#[test]
fn a_resume_position_from_another_table_or_an_unselected_family_is_refused() {
    let db = setup(4917);
    let snap = db.engine.snapshot().unwrap();
    let cells = scan_cells(
        &db,
        &snap,
        (Bound::Unbounded, Bound::Unbounded),
        &ReadSpec::default(),
    );
    let (f, g) = (
        cells[0].1,
        cells.iter().find(|c| c.1 != cells[0].1).unwrap().1,
    );
    let mut read = ReadSpec::default();
    read.families = vec![f];
    let mut spec = whole();
    spec.read = read;
    spec.resume = Some((
        row(0),
        pigeonhole_engine::ColumnPosition::new(db.table, g, b"x".to_vec()),
    ));
    let e = db
        .engine
        .reduce(&snap, db.table, &spec, || Every, All::default)
        .unwrap_err();
    assert!(
        matches!(e, pigeonhole_engine::Error::InvalidArgument(_)),
        "{e:?}"
    );
    let mut spec = whole();
    spec.resume = Some((
        row(0),
        pigeonhole_engine::ColumnPosition::new(TableId(db.table.0 + 1), f, b"a".to_vec()),
    ));
    let e = db
        .engine
        .reduce(&snap, db.table, &spec, || Every, All::default)
        .unwrap_err();
    assert!(
        matches!(e, pigeonhole_engine::Error::InvalidArgument(_)),
        "{e:?}"
    );
}

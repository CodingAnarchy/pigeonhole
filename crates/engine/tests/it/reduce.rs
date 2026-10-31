//! D296 (#490 A): bounded-result reads give exactly what a scan of the same snapshot gives,
//! reduced: the k best, the first n, one per key, a row's first cell, an early stop. Data
//! in SSTs and memtables, with overwrites and deletes, so the visitor sees resolved cells only.
//! Fan-out (scoped threads, from a thread that drives no shard) equals the sequential read.

use crate::common;

use std::future::Future;
use std::ops::{Bound, ControlFlow};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::RowRange;
use pigeonhole_engine::{
    CellVisitor, Dedupe, Engine, EngineShard, FamilyOptions, Limit, ReadInput, ReduceSpec, Reducer,
    ScanSpec, Snapshot, TopK, ValueRef, Visit, WriteBatch,
};
use pigeonhole_format::{Durability, FamilyId, TableId, Timestamp};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";
const BUDGET: u64 = 64 << 20;
const ROWS: u32 = 300;

/// A visible cell from a scan: (row, family, qualifier, stored value).
type Cell = (Vec<u8>, FamilyId, Vec<u8>, Vec<u8>);

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
    f: FamilyId,
    g: FamilyId,
}

/// Rows `r0000..r0299` in families `f` and `g`, some flushed to SSTs, then overwrites and
/// deletes in the memtables.
fn setup(seed: u64) -> Db {
    let vfs = SimVfs::new(seed);
    let opts = || common::options(Arc::clone(&vfs), 4, BUDGET);
    let db = Engine::open(Path::new(DB), opts()).unwrap();
    let t = db
        .create_table(
            "t",
            &[
                ("f".into(), FamilyOptions::default()),
                ("g".into(), FamilyOptions::default()),
            ],
        )
        .unwrap();
    db.close().unwrap();
    drop(db);
    let (engine, mut shards) = Engine::open_application_owned(Path::new(DB), opts()).unwrap();
    let (f, g) = (t.families[0].id, t.families[1].id);
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
    Db {
        engine,
        _shards: shards,
        table: t.id,
        f,
        g,
    }
}

/// Every visible cell of the snapshot in `[start, end)`, from a scan: (row, family,
/// qualifier, stored value).
fn scan_cells(db: &Db, snap: &Snapshot, start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Vec<Cell> {
    let mut cur = db
        .engine
        .scan(snap, db.table, ScanSpec::new(start, end))
        .unwrap();
    let mut out = Vec::new();
    while cur.next_row().unwrap() {
        let r = cur.row().to_vec();
        while let Some(c) = cur.next_cell().unwrap() {
            out.push((r.clone(), c.family, c.qualifier.to_vec(), c.stored.to_vec()));
        }
    }
    out
}

/// Scores each cell's stored value.
struct Score;

impl CellVisitor for Score {
    type Item = u32;
    fn visit(&mut self, _: &[u8], _: FamilyId, _: &[u8], _: Timestamp, value: &[u8]) -> Visit<u32> {
        Visit::Emit(score(value))
    }
}

/// Emits each cell's family id (as `u32`), so results compare against the scan.
struct Families;

impl CellVisitor for Families {
    type Item = u32;
    fn visit(
        &mut self,
        _: &[u8],
        family: FamilyId,
        _: &[u8],
        _: Timestamp,
        _: &[u8],
    ) -> Visit<u32> {
        Visit::Emit(family.0)
    }
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
        .flat_map(|(s, e)| scan_cells(db, snap, s, e))
        .collect()
}

/// Runs `reduce` on another thread (one that drives no shard), so it may fan out.
fn off_thread<T: Send>(f: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|s| s.spawn(f).join().unwrap())
}

#[test]
fn top_k_equals_the_k_smallest_scored_cells_of_a_scan_ties_by_row_then_qualifier() {
    let db = setup(4901);
    let snap = db.engine.snapshot().unwrap();
    let mut want: Vec<(u32, Vec<u8>, Vec<u8>)> = cells_in_ranges(&db, &snap)
        .into_iter()
        .map(|(r, _, q, v)| (score(&v), r, q))
        .collect();
    want.sort();
    want.truncate(25);
    let want: Vec<(Vec<u8>, Vec<u8>, u32)> = want.into_iter().map(|(s, r, q)| (r, q, s)).collect();

    let spec = ReduceSpec::new(ReadInput::Ranges(ranges()));
    let seq = db
        .engine
        .reduce(&snap, db.table, &spec, || Score, || TopK::new(25))
        .unwrap();
    assert_eq!(seq.items, want);
    assert_eq!(seq.stats.threads, 1, "the driving thread never fans out");

    let mut par_spec = spec.clone();
    par_spec.parallelism = 4;
    par_spec.parallel_floor = 0;
    let engine = Arc::clone(&db.engine);
    let par = off_thread(|| engine.reduce(&snap, db.table, &par_spec, || Score, || TopK::new(25)))
        .unwrap();
    assert_eq!(par.items, want, "fan-out equals the sequential read");
    assert!(
        par.stats.threads > 1,
        "the read fanned out: {:?}",
        par.stats
    );
    assert_eq!(par.stats.cells, seq.stats.cells);
}

#[test]
fn limit_keeps_the_first_n_cells_in_key_order_at_any_fan_out() {
    let db = setup(4902);
    let snap = db.engine.snapshot().unwrap();
    let all: Vec<(Vec<u8>, Vec<u8>)> = cells_in_ranges(&db, &snap)
        .into_iter()
        .map(|(r, _, q, _)| (r, q))
        .collect();
    for n in [1usize, 17, 200] {
        // The n smallest (row, qualifier) of the scan.
        let mut keys = all.clone();
        keys.sort();
        keys.truncate(n);
        let mut spec = ReduceSpec::new(ReadInput::Ranges(ranges()));
        let seq = db
            .engine
            .reduce(&snap, db.table, &spec, || Families, || Limit::new(n))
            .unwrap();
        let kept: Vec<(Vec<u8>, Vec<u8>)> = seq
            .items
            .iter()
            .map(|(r, q, _)| (r.clone(), q.clone()))
            .collect();
        assert_eq!(kept, keys, "n = {n}");
        spec.parallelism = 4;
        spec.parallel_floor = 0;
        let engine = Arc::clone(&db.engine);
        let par =
            off_thread(|| engine.reduce(&snap, db.table, &spec, || Families, || Limit::new(n)))
                .unwrap();
        assert_eq!(par.items, seq.items, "n = {n}: fan-out equals sequential");
        assert!(par.stats.threads > 1, "n = {n}: the read fanned out");
    }
}

#[test]
fn dedupe_keeps_one_cell_per_item_key_from_the_smallest_row_and_qualifier() {
    let db = setup(4903);
    let snap = db.engine.snapshot().unwrap();
    // Key: the score (16 values), so many cells share each key.
    let mut want = std::collections::BTreeMap::<u32, (Vec<u8>, Vec<u8>)>::new();
    for (r, _, q, v) in cells_in_ranges(&db, &snap) {
        let e = want.entry(score(&v)).or_insert((r.clone(), q.clone()));
        if (r.clone(), q.clone()) < *e {
            *e = (r, q);
        }
    }
    let exclude = Arc::new(std::collections::HashSet::from([3u32]));
    want.remove(&3);
    let make = || Dedupe::new(|s: &u32| *s, Some(Arc::clone(&exclude)));
    let spec = ReduceSpec::new(ReadInput::Ranges(ranges()));
    let got = db
        .engine
        .reduce(&snap, db.table, &spec, || Score, make)
        .unwrap();
    let mut got: Vec<(u32, Vec<u8>, Vec<u8>)> =
        got.items.into_iter().map(|(r, q, s)| (s, r, q)).collect();
    got.sort();
    let want: Vec<(u32, Vec<u8>, Vec<u8>)> =
        want.into_iter().map(|(s, (r, q))| (s, r, q)).collect();
    assert_eq!(got, want);
}

/// Emits a row's first cell, then skips the rest of the row.
struct FirstPerRow;

impl CellVisitor for FirstPerRow {
    type Item = u32;
    fn visit(
        &mut self,
        _: &[u8],
        family: FamilyId,
        _: &[u8],
        _: Timestamp,
        _: &[u8],
    ) -> Visit<u32> {
        Visit::EmitAndNextRow(family.0)
    }
}

/// Stops the read at the first row at or past `at`.
struct StopAt(Vec<u8>);

impl CellVisitor for StopAt {
    type Item = u32;
    fn visit(&mut self, row: &[u8], _: FamilyId, _: &[u8], _: Timestamp, _: &[u8]) -> Visit<u32> {
        if row >= self.0.as_slice() {
            Visit::Stop
        } else {
            Visit::Emit(0)
        }
    }
}

/// Keeps everything pushed, in order.
#[derive(Default)]
struct All(Vec<(Vec<u8>, Vec<u8>, u32)>);

impl Reducer for All {
    type Item = u32;
    fn push(&mut self, row: &[u8], qualifier: &[u8], item: u32) -> ControlFlow<()> {
        self.0.push((row.to_vec(), qualifier.to_vec(), item));
        ControlFlow::Continue(())
    }
    fn merge(&mut self, other: Self) {
        self.0.extend(other.0);
        self.0.sort();
    }
    fn finish(self) -> Vec<(Vec<u8>, Vec<u8>, u32)> {
        self.0
    }
}

#[test]
fn next_row_takes_each_rows_first_cell_and_stop_ends_the_read() {
    let db = setup(4904);
    let snap = db.engine.snapshot().unwrap();
    let cells = scan_cells(&db, &snap, Bound::Unbounded, Bound::Unbounded);
    let mut want = Vec::new();
    for (r, fam, q, _) in &cells {
        if want
            .last()
            .is_none_or(|(lr, _, _): &(Vec<u8>, Vec<u8>, u32)| lr != r)
        {
            want.push((r.clone(), q.clone(), fam.0));
        }
    }
    let spec = ReduceSpec::new(ReadInput::Ranges(vec![(
        Bound::Unbounded,
        Bound::Unbounded,
    )]));
    let got = db
        .engine
        .reduce(&snap, db.table, &spec, || FirstPerRow, All::default)
        .unwrap();
    assert_eq!(got.items, want);
    assert_eq!(
        got.stats.cells as usize,
        want.len(),
        "the rest of each row was skipped, not visited"
    );

    let at = row(100);
    let got = db
        .engine
        .reduce(&snap, db.table, &spec, || StopAt(at.clone()), All::default)
        .unwrap();
    let want: Vec<_> = cells
        .iter()
        .filter(|(r, ..)| *r < at)
        .map(|(r, _, q, _)| (r.clone(), q.clone(), 0))
        .collect();
    assert_eq!(got.items, want);
}

#[test]
fn only_resolved_cells_are_visited() {
    let db = setup(4905);
    let snap = db.engine.snapshot().unwrap();
    // Row r0001 was deleted, r0000's "b" column too, and r0000's "a" overwritten.
    let spec = ReduceSpec::new(ReadInput::Ranges(vec![(
        Bound::Included(row(0)),
        Bound::Included(row(1)),
    )]));
    let got = db
        .engine
        .reduce(&snap, db.table, &spec, || Families, All::default)
        .unwrap();
    let want: Vec<_> = scan_cells(&db, &snap, Bound::Included(row(0)), Bound::Included(row(1)))
        .into_iter()
        .map(|(r, fam, q, _)| (r, q, fam.0))
        .collect();
    assert_eq!(got.items, want);
    assert!(
        got.items.iter().all(|(r, ..)| *r == row(0)),
        "{:?}",
        got.items
    );
    assert!(!got.items.iter().any(|(_, q, f)| q == b"b" && *f == db.f.0));
    let _ = db.g;
}

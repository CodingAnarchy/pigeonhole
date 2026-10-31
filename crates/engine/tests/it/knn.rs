//! D296 PR 3 (#490 A, C): exact k-nearest-neighbour search through `Engine::reduce` with a
//! `KnnVisitor` and a `TopK<Score>` gives exactly the k nearest cells a scan of the same
//! snapshot gives, scored by the same kernels (D296 R2), for every metric, sequentially and
//! fanned out, with a qualifier selection (filtered KNN), over vectors in SSTs and memtables
//! with overwrites, deleted rows, malformed values and zero vectors.

use crate::common;

use std::future::Future;
use std::ops::Bound;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineShard, FamilyOptions, KnnSkips, KnnVisitor, Metric, QualifierFilter, ReadInput,
    ReadSpec, ReduceSpec, ScanSpec, Score, Snapshot, TopK, ValueRef, WriteBatch,
};
use pigeonhole_format::{Durability, TableId};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";
const BUDGET: u64 = 64 << 20;
const ROWS: u32 = 2000;
const DIM: usize = 32;

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
    format!("v{i:05}").into_bytes()
}

struct Rng(u64);

impl Rng {
    fn f(&mut self) -> f32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        ((self.0 >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    }
    fn vector(&mut self) -> Vec<f32> {
        (0..DIM).map(|_| self.f()).collect()
    }
}

fn le(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
}

struct Db {
    engine: Arc<Engine>,
    _shards: Vec<EngineShard>,
    table: TableId,
}

/// Two vectors per row (qualifiers `a` and `b`) in family `e`, half flushed; then overwrites,
/// deleted rows, malformed values (wrong length, an `i64`) and zero vectors.
fn setup(seed: u64) -> Db {
    let vfs = SimVfs::new(seed);
    let opts = || common::options(Arc::clone(&vfs), 4, BUDGET);
    let db = Engine::open(Path::new(DB), opts()).unwrap();
    let t = db
        .create_table("t", &[("e".into(), FamilyOptions::default())])
        .unwrap();
    db.close().unwrap();
    drop(db);
    let (engine, mut shards) = Engine::open_application_owned(Path::new(DB), opts()).unwrap();
    let e = t.families[0].id;
    let mut rng = Rng(seed | 1);
    let commit = |shards: &mut Vec<EngineShard>, wb: WriteBatch| {
        drive(
            shards,
            engine.submit(wb, Some(Durability::Buffered)).unwrap(),
        )
        .unwrap();
    };
    for chunk in 0..ROWS / 100 {
        let mut wb = WriteBatch::new();
        for i in chunk * 100..(chunk + 1) * 100 {
            for q in [b"a", b"b"] {
                wb.put(
                    t.id,
                    e,
                    &row(i),
                    q,
                    None,
                    ValueRef::Bytes(&le(&rng.vector())),
                )
                .unwrap();
            }
        }
        commit(&mut shards, wb);
        if chunk == ROWS / 200 {
            drive(&mut shards, engine.submit_flush().unwrap()).unwrap();
        }
    }
    let mut wb = WriteBatch::new();
    for i in (0..ROWS).step_by(13) {
        wb.put(
            t.id,
            e,
            &row(i),
            b"a",
            None,
            ValueRef::Bytes(&le(&rng.vector())),
        )
        .unwrap();
    }
    for i in (5..ROWS).step_by(97) {
        wb.delete_row(t.id, &row(i), None).unwrap();
    }
    for i in (7..ROWS).step_by(211) {
        wb.put(t.id, e, &row(i), b"b", None, ValueRef::Bytes(b"short"))
            .unwrap();
        wb.put(t.id, e, &row(i + 1), b"b", None, ValueRef::I64(7))
            .unwrap();
        wb.put(
            t.id,
            e,
            &row(i + 2),
            b"a",
            None,
            ValueRef::Bytes(&le(&[0.0; DIM])),
        )
        .unwrap();
    }
    commit(&mut shards, wb);
    Db {
        engine,
        _shards: shards,
        table: t.id,
    }
}

/// The k nearest cells by brute force over a scan, scored with the visitor's own kernels.
fn brute(
    db: &Db,
    snap: &Snapshot,
    read: &ReadSpec,
    visitor: &KnnVisitor,
    k: usize,
) -> Vec<(Vec<u8>, Vec<u8>, f64)> {
    let mut spec = ScanSpec::new(Bound::Unbounded, Bound::Unbounded);
    spec.read = read.clone();
    let mut cur = db.engine.scan(snap, db.table, spec).unwrap();
    // (score, scan position) orders as TopK does: score, then cell (scan) order.
    let mut all = Vec::new();
    let mut n = 0usize;
    while cur.next_row().unwrap() {
        let r = cur.row().to_vec();
        while let Some(c) = cur.next_cell().unwrap() {
            if let Some(d) = visitor.distance(c.stored).filter(|d| !d.is_nan()) {
                all.push((Score(d), n, r.clone(), c.qualifier.to_vec()));
            }
            n += 1;
        }
    }
    all.sort_by_key(|a| (a.0, a.1));
    all.into_iter()
        .take(k)
        .map(|(s, _, r, q)| (r, q, s.0))
        .collect()
}

fn knn(
    db: &Db,
    snap: &Snapshot,
    spec: &ReduceSpec,
    query: &Arc<[f32]>,
    metric: Metric,
    k: usize,
    skips: &Arc<KnnSkips>,
) -> Vec<(Vec<u8>, Vec<u8>, f64)> {
    db.engine
        .reduce(
            snap,
            db.table,
            spec,
            || KnnVisitor::new(Arc::clone(query), metric, Arc::clone(skips)),
            || TopK::new(k),
        )
        .unwrap()
        .items
        .into_iter()
        .map(|(c, s)| (c.row, c.qualifier, s.0))
        .collect()
}

#[test]
fn exact_knn_equals_a_brute_force_scan_for_every_metric_and_any_fan_out() {
    let db = setup(4911);
    let snap = db.engine.snapshot().unwrap();
    let query: Arc<[f32]> = Rng(77).vector().into();
    for metric in [Metric::L2, Metric::InnerProduct, Metric::Cosine] {
        for k in [1usize, 10, 100] {
            let skips = Arc::new(KnnSkips::default());
            let probe = KnnVisitor::new(Arc::clone(&query), metric, Arc::new(KnnSkips::default()));
            let want = brute(&db, &snap, &ReadSpec::default(), &probe, k);
            assert_eq!(want.len(), k);
            let mut spec = ReduceSpec::new(ReadInput::Ranges(vec![
                (Bound::Unbounded, Bound::Excluded(row(700))),
                (Bound::Included(row(700)), Bound::Unbounded),
            ]));
            let got = knn(&db, &snap, &spec, &query, metric, k, &skips);
            assert_eq!(got, want, "{metric:?} k = {k}");
            // Malformed values: "short" and an i64, every 211th row from 7 and 8.
            let (shape, nan) = skips.counts();
            assert!(
                shape > 0,
                "{metric:?}: malformed values were skipped and counted"
            );
            assert_eq!(
                nan > 0,
                metric == Metric::Cosine,
                "zero vectors are NaN under cosine only"
            );

            spec.parallelism = 4;
            spec.parallel_floor = 0;
            let engine = Arc::clone(&db.engine);
            let par = std::thread::scope(|s| {
                s.spawn(|| {
                    engine
                        .reduce(
                            &snap,
                            db.table,
                            &spec,
                            || {
                                KnnVisitor::new(
                                    Arc::clone(&query),
                                    metric,
                                    Arc::new(KnnSkips::default()),
                                )
                            },
                            || TopK::new(k),
                        )
                        .unwrap()
                })
                .join()
                .unwrap()
            });
            assert!(par.stats.threads > 1);
            let par: Vec<_> = par
                .items
                .into_iter()
                .map(|(c, s)| (c.row, c.qualifier, s.0))
                .collect();
            assert_eq!(par, want, "{metric:?} k = {k}: fan-out equals sequential");
        }
    }
}

#[test]
fn filtered_knn_ranks_only_the_selected_cells() {
    let db = setup(4912);
    let snap = db.engine.snapshot().unwrap();
    let query: Arc<[f32]> = Rng(99).vector().into();
    let mut read = ReadSpec::default();
    read.qualifiers = QualifierFilter::Prefix(b"b".to_vec());
    let probe = KnnVisitor::new(
        Arc::clone(&query),
        Metric::L2,
        Arc::new(KnnSkips::default()),
    );
    let want = brute(&db, &snap, &read, &probe, 20);
    assert!(want.iter().all(|(_, q, _)| q == b"b"));
    let mut spec = ReduceSpec::new(ReadInput::Ranges(vec![(
        Bound::Unbounded,
        Bound::Unbounded,
    )]));
    spec.read = read;
    let got = knn(
        &db,
        &snap,
        &spec,
        &query,
        Metric::L2,
        20,
        &Arc::new(KnnSkips::default()),
    );
    assert_eq!(got, want);
}

/// D296 PR 4 (D303): a typed visitor over a vector family ranks exactly as the family's
/// `Params::distance` (the function `VectorSpec::distance_*` uses), for `f32` and `i8`
/// vectors and every metric they take, over ranges and key sets, sequentially and fanned
/// out over three tablets (asserted). Ties, frequent with `i8`, go by row.
#[test]
fn typed_knn_ranks_as_the_familys_distance_at_any_fan_out() {
    use pigeonhole_engine::KeySet;
    use pigeonhole_format::vector::{Element, Params, Query, QueryBuf, VectorRef};
    let vfs = SimVfs::new(4913);
    let opts = || common::options(Arc::clone(&vfs), 4, BUDGET);
    let db = Engine::open(Path::new(DB), opts()).unwrap();
    let metrics = [
        pigeonhole_format::vector::Metric::L2,
        pigeonhole_format::vector::Metric::InnerProduct,
        pigeonhole_format::vector::Metric::Cosine,
    ];
    let mut families = Vec::new();
    for (e, el) in [("f", Element::F32), ("i", Element::I8)] {
        for (m, metric) in metrics.iter().enumerate() {
            let p = Params::new(el, 4, *metric).unwrap();
            families.push((
                format!("{e}{m}"),
                FamilyOptions::default()
                    .kind(pigeonhole_format::manifest::FamilyKind::Vector)
                    .kind_params(p.encode()),
                p,
            ));
        }
    }
    let t = db
        .create_table(
            "t",
            &families
                .iter()
                .map(|(n, o, _)| (n.clone(), o.clone()))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    db.close().unwrap();
    drop(db);
    let (engine, mut shards) = Engine::open_application_owned(Path::new(DB), opts()).unwrap();
    let mut rng = Rng(4913);
    let mut wb = WriteBatch::new();
    for i in 0..ROWS {
        for (name, _, p) in &families {
            let f = t.family(name).unwrap().id;
            let payload: Vec<u8> = match p.element {
                Element::F32 => le(&rng.vector()[..4]),
                // Small i8 components: many ties. Never all zeros (cosine refuses that).
                _ => {
                    let mut v: Vec<u8> =
                        (0..4).map(|_| ((rng.f() * 3.0) as i8 + 1) as u8).collect();
                    if v.iter().all(|b| *b == 0) {
                        v[0] = 1;
                    }
                    v
                }
            };
            wb.put(
                t.id,
                f,
                &row(i),
                b"",
                None,
                ValueRef::Vector(VectorRef::new(p.element, 4, &payload).unwrap()),
            )
            .unwrap();
        }
        if i % 200 == 199 {
            drive(
                &mut shards,
                engine
                    .submit(std::mem::take(&mut wb), Some(Durability::Buffered))
                    .unwrap(),
            )
            .unwrap();
        }
    }
    drive(&mut shards, engine.submit_flush().unwrap()).unwrap();
    for at in [row(ROWS / 3), row(2 * ROWS / 3)] {
        let split = engine.split_tablet_pending(t.id, &at).unwrap();
        drive(&mut shards, split).unwrap();
    }
    let snap = engine.snapshot().unwrap();
    let qf: Arc<[f32]> = vec![0.3, -0.7, 0.2, 0.9].into();
    let qi: Arc<[i8]> = vec![1, -1, 2, 0].into();
    let keys = Arc::new(KeySet::from_unsorted((0..ROWS).step_by(3).map(row)));
    for (name, _, p) in &families {
        let f = t.family(name).unwrap().id;
        let (query, q) = match p.element {
            Element::F32 => (QueryBuf::F32(Arc::clone(&qf)), Query::F32(&qf)),
            _ => (QueryBuf::I8(Arc::clone(&qi)), Query::I8(&qi)),
        };
        // Brute force over a scan, ranked by (distance, row).
        let mut spec = ScanSpec::new(Bound::Unbounded, Bound::Unbounded);
        spec.read.families = vec![f];
        let mut cur = engine.scan(&snap, t.id, spec).unwrap();
        let mut all = Vec::new();
        while cur.next_row().unwrap() {
            let r = cur.row().to_vec();
            while let Some(c) = cur.next_cell().unwrap() {
                let ValueRef::Vector(v) = pigeonhole_format::value::decode_value(c.stored).unwrap()
                else {
                    panic!("a vector");
                };
                all.push((Score(p.distance(q, &v)), r.clone()));
            }
        }
        all.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let in_keys = |r: &Vec<u8>| keys.iter().any(|k| k == r.as_slice());
        for (input, want) in [
            (
                ReadInput::Ranges(vec![(Bound::Unbounded, Bound::Unbounded)]),
                all.iter().take(25).cloned().collect::<Vec<_>>(),
            ),
            (
                ReadInput::Keys(Arc::clone(&keys)),
                all.iter()
                    .filter(|(_, r)| in_keys(r))
                    .take(25)
                    .cloned()
                    .collect(),
            ),
        ] {
            let want: Vec<(Vec<u8>, f64)> = want.into_iter().map(|(s, r)| (r, s.0)).collect();
            let mut rspec = ReduceSpec::new(input);
            rspec.read.families = vec![f];
            for (threads, floor) in [(1, rspec.parallel_floor), (4, 0)] {
                rspec.parallelism = threads;
                rspec.parallel_floor = floor;
                let skips = Arc::new(KnnSkips::default());
                let engine = Arc::clone(&engine);
                let got = std::thread::scope(|s| {
                    s.spawn(|| {
                        engine
                            .reduce(
                                &snap,
                                t.id,
                                &rspec,
                                || {
                                    KnnVisitor::typed(
                                        p.prepare(query.clone()).unwrap(),
                                        Arc::clone(&skips),
                                    )
                                },
                                || TopK::new(25),
                            )
                            .unwrap()
                    })
                    .join()
                    .unwrap()
                });
                if threads > 1 {
                    assert!(got.stats.threads > 1, "{name}: fanned out");
                }
                let got: Vec<(Vec<u8>, f64)> =
                    got.items.into_iter().map(|(c, s)| (c.row, s.0)).collect();
                assert_eq!(got, want, "{name}, {threads} threads");
                assert_eq!(skips.counts(), (0, 0), "{name}: nothing skipped");
            }
        }
    }
}

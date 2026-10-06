//! The public layer's overhead over the engine: point gets on memtable-resident data (small
//! values copied inline, large ones pinned) through `Table::get` versus
//! `Engine::get_latest`, and a full scan through `RowIter::next_ref` versus the engine's
//! `ScanCursor`. Both run on the in-memory `SimVfs`, so no disk is involved.
//!
//! Run with `cargo bench -p pigeonhole`.

use std::hint::black_box;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pigeonhole::{Family, Options, Pigeonhole, Table};
use pigeonhole_engine::{
    Engine, EngineOptions, FamilyId, FamilyOptions, ScanSpec, TableId, ValueRef, WriteBatch,
};
use pigeonhole_io::sim::SimVfs;

const ROWS: usize = 1000;

struct Fixture {
    db: Pigeonhole,
    table: Table,
    engine: Arc<Engine>,
    ids: (TableId, FamilyId),
}

/// The same `ROWS` rows (`cells` columns of `len` bytes each) in a public database and in an
/// engine opened directly.
fn fixture(len: usize, cells: usize) -> Fixture {
    let db = Pigeonhole::open(
        "/bench/public.phdb",
        Options::default()
            .vfs(SimVfs::new(1) as _)
            .shards(1)
            .memtable_budget(256 << 20),
    )
    .expect("open");
    let table = db
        .table("t")
        .expect("table")
        .family("f", Family::default())
        .create_if_missing()
        .expect("create");
    let mut o = EngineOptions::new(SimVfs::new(1));
    o.create_if_missing = true;
    o.shards = 1;
    o.memtable_budget = 256 << 20;
    let engine = Engine::open(Path::new("/bench/engine.phdb"), o).expect("open engine");
    let info = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .expect("create");
    let ids = (info.id, info.families[0].id);
    let value = vec![7u8; len];
    for r in 0..ROWS {
        let row = format!("row{r:06}");
        let mut m = table.mutate(row.as_bytes());
        let mut wb = WriteBatch::new();
        for c in 0..cells {
            let q = format!("q{c:03}");
            m = m.put("f", q.as_bytes(), &value);
            wb.put(
                ids.0,
                ids.1,
                row.as_bytes(),
                q.as_bytes(),
                None,
                ValueRef::Bytes(&value),
            )
            .expect("put");
        }
        m.commit().expect("commit");
        engine.commit(wb, None).expect("commit");
    }
    Fixture {
        db,
        table,
        engine,
        ids,
    }
}

fn gets(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(1));
    for len in [16usize, 256] {
        let fx = fixture(len, 1);
        let keys: Vec<Vec<u8>> = (0..ROWS)
            .map(|r| format!("row{r:06}").into_bytes())
            .collect();
        let mut i = 0;
        group.bench_with_input(BenchmarkId::new("public", len), &len, |b, _| {
            b.iter(|| {
                i = (i + 7919) % ROWS;
                let cell = fx.table.get(&keys[i], "f", b"q000").expect("get");
                black_box(cell.map(|c| c.value()[0]))
            })
        });
        group.bench_with_input(BenchmarkId::new("engine", len), &len, |b, _| {
            b.iter(|| {
                i = (i + 7919) % ROWS;
                let cell = fx
                    .engine
                    .get_latest(fx.ids.0, fx.ids.1, &keys[i], b"q000")
                    .expect("get");
                black_box(cell.map(|c| c.stored()[1]))
            })
        });
        drop(fx.table);
        fx.db.close().expect("close");
        fx.engine.close().expect("close");
    }
    group.finish();
}

fn scans(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    let cells = 8;
    group.throughput(Throughput::Elements((ROWS * cells) as u64));
    let fx = fixture(32, cells);
    group.bench_function("public_next_ref", |b| {
        b.iter(|| {
            let mut it = fx.table.scan_prefix(b"").iter().expect("iter");
            let mut n = 0usize;
            while let Some(row) = it.next_ref().expect("next") {
                for e in row.iter() {
                    n += e.cell.value().len();
                }
            }
            black_box(n)
        })
    });
    group.bench_function("public_iterator", |b| {
        b.iter(|| {
            let mut n = 0usize;
            for row in fx.table.scan_prefix(b"").iter().expect("iter") {
                n += row.expect("row").len();
            }
            black_box(n)
        })
    });
    group.bench_function("engine_cursor", |b| {
        b.iter(|| {
            let snap = fx.engine.snapshot().expect("snapshot");
            let spec = ScanSpec::new(Bound::Unbounded, Bound::Unbounded);
            let mut cur = fx.engine.scan(&snap, fx.ids.0, spec).expect("scan");
            let mut n = 0usize;
            while cur.next_row().expect("row") {
                while let Some(len) = cur
                    .next_cell()
                    .expect("cell")
                    .map(|c| c.stored.len() + c.qualifier.len())
                {
                    // The pinned value, as the public layer takes it per cell.
                    n += len + cur.current_data().stored().len();
                }
            }
            black_box(n)
        })
    });
    group.finish();
    drop(fx.table);
    fx.db.close().expect("close");
    fx.engine.close().expect("close");
}

criterion_group!(benches, gets, scans);
criterion_main!(benches);

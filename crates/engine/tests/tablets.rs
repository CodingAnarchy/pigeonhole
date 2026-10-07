//! Tablet splits, merges and moves (issue #38): the model checker with tablet changes
//! interleaved with writes, two-phase commits, flushes, compactions and crashes, for every
//! shard count; identical results across shard counts with changes mid-run; a crash at
//! every write point of runs full of changes; and targeted checks of the balancer, reads
//! during a change and the default-timestamp floor travelling with a tablet.

mod common;

use std::ops::Bound;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use common::{Config, final_dump, run};
use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, PendingMaintenance, ReadSpec, ScanSpec,
    TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

fn seeds() -> Vec<u64> {
    let n: u64 = std::env::var("PIGEONHOLE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let base: u64 = std::env::var("PIGEONHOLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    (base..base + n).collect()
}

fn check(seed: u64, cfg: &Config) -> common::Stats {
    match run(seed, cfg) {
        Ok(stats) => {
            eprintln!("seed {seed}: {stats:?}");
            stats
        }
        Err(f) => panic!("{f}"),
    }
}

/// Tablet changes requested often, and the balancer acting on its own.
fn churn(cfg: &mut Config) {
    cfg.tablet_changes = true;
    cfg.tablet_ops_ppm = 150_000;
    cfg.balance_fast = true;
}

#[test]
fn tablet_changes_match_the_model_for_every_shard_count() {
    let mut changes = 0;
    for shards in 1..=8 {
        let mut cfg = Config::quiet(300);
        churn(&mut cfg);
        cfg.shards = shards;
        for seed in seeds() {
            let stats = check(seed, &cfg);
            changes += stats.tablet_changes;
        }
    }
    assert!(changes > 50, "only {changes} tablet changes completed");
}

#[test]
fn tablet_changes_under_faults_and_crashes() {
    let mut totals = (0, 0, 0);
    for shards in [1, 2, 3, 5, 8] {
        let mut cfg = Config::standard(300);
        churn(&mut cfg);
        cfg.shards = shards;
        for seed in seeds() {
            let s = check(seed, &cfg).engine_tablet_changes;
            totals = (totals.0 + s.0, totals.1 + s.1, totals.2 + s.2);
        }
    }
    eprintln!("splits, merges, moves: {totals:?}");
    assert!(totals.0 > 0 && totals.1 > 0 && totals.2 > 0, "{totals:?}");
}

#[test]
fn tablet_changes_with_a_changed_shard_count() {
    for seed in seeds() {
        let mut cfg = Config::standard(300);
        churn(&mut cfg);
        cfg.shards = 3;
        cfg.reopen_shards = vec![1, 5, 2, 8, 4];
        check(seed, &cfg);
    }
}

#[test]
fn results_are_identical_across_shard_counts_with_tablet_changes() {
    for seed in seeds() {
        let mut cfg = Config::quiet(250);
        // Purges at fixed points (decision D74), as in the model_check equivalent.
        cfg.compaction.l0_trigger = u32::MAX;
        cfg.compaction.level_base_bytes = u64::MAX;
        cfg.compact_every = Some(50);
        cfg.tablet_changes = true;
        cfg.tablet_every = Some(9);
        cfg.balance_fast = true;
        cfg.shards = 1;
        let reference = final_dump(seed, &cfg);
        for shards in 2..=8 {
            cfg.shards = shards;
            let dump = final_dump(seed, &cfg);
            assert_eq!(
                dump, reference,
                "seed {seed}: {shards} shards differ from 1 shard"
            );
        }
    }
}

#[test]
fn crash_at_every_write_point_of_tablet_changes() {
    // Short runs full of splits, merges and moves (requested and the balancer's) among
    // cross-shard batches, tiny memtables and frequent flushes: every mutating operation is a
    // crash point, so the sweep crosses every step of a change (the freeze and flush, the
    // manifest commit retiring and adding tablets) and of the commits around it.
    let mut cfg = Config::quiet(25);
    cfg.shards = 4;
    cfg.spec.read_fraction = 0.2;
    cfg.spec.max_batch = 6;
    cfg.spec.rows = 12;
    cfg.faults.torn_writes = true;
    cfg.memtable_freeze_bytes = 2 << 10;
    cfg.tablet_changes = true;
    cfg.tablet_ops_ppm = 400_000;
    cfg.balance_fast = true;
    let step: u64 = std::env::var("PIGEONHOLE_SWEEP_STEP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    for seed in seeds() {
        let base = run(seed, &cfg).unwrap_or_else(|f| panic!("seed {seed} without a crash: {f}"));
        eprintln!(
            "seed {seed}: sweeping {} crash points ({} tablet changes)",
            base.mutating_ops, base.tablet_changes
        );
        let mut n = 1;
        while n <= base.mutating_ops {
            let mut c = cfg.clone();
            c.crash_at = Some(n);
            if let Err(f) = run(seed, &c) {
                panic!("seed {seed}, crash after mutating op {n}: {f}");
            }
            n += step;
        }
    }
}

// ---------------------------------------------------------------------------------------
// Targeted checks on one engine
// ---------------------------------------------------------------------------------------

struct Db {
    vfs: Arc<SimVfs>,
    engine: Arc<Engine>,
    shards: Vec<EngineShard>,
    table: Arc<TableInfo>,
}

fn open(shards: usize, tweak: impl FnOnce(&mut EngineOptions)) -> Db {
    let vfs = SimVfs::new(7);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = shards;
    o.pin_threads = false;
    o.memtable_budget = 4 << 20;
    o.reader_slots = 4;
    o.tablet_changes = true;
    o.balance_interval_nanos = 0;
    tweak(&mut o);
    let (engine, shards) =
        Engine::open_application_owned(Path::new("/db/data.phdb"), o).expect("open");
    let table = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .expect("table");
    Db {
        vfs,
        engine,
        shards,
        table,
    }
}

impl Db {
    fn step(&mut self) {
        let now = pigeonhole_io::Vfs::monotonic_nanos(&*self.vfs);
        for s in &mut self.shards {
            s.run_once(now + 1_000);
        }
    }

    fn drive(&mut self, mut m: PendingMaintenance) -> pigeonhole_engine::Result<()> {
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..100_000 {
            if let Poll::Ready(r) = Pin::new(&mut m).poll(&mut cx) {
                return r;
            }
            self.step();
        }
        panic!("maintenance did not finish")
    }

    fn put(&mut self, row: &[u8], value: &[u8]) -> u64 {
        let f = self.table.families[0].id;
        let mut wb = WriteBatch::new();
        wb.put(self.table.id, f, row, b"q", None, ValueRef::Bytes(value))
            .unwrap();
        let mut pc = self
            .engine
            .submit(wb, Some(Durability::Buffered))
            .expect("submit");
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(r) = Pin::new(&mut pc).poll(&mut cx) {
                return r.expect("commit").seqno;
            }
            self.step();
        }
    }

    fn get(&self, row: &[u8]) -> Option<(u64, Vec<u8>)> {
        let f = self.table.families[0].id;
        self.engine
            .get_latest(self.table.id, f, row, b"q")
            .expect("get")
            .map(|c| {
                let ValueRef::Bytes(v) = c.value() else {
                    panic!("bytes")
                };
                (c.timestamp(), v.to_vec())
            })
    }

    fn ranges(&self) -> Vec<pigeonhole_engine::TabletOwner> {
        self.engine
            .snapshot()
            .unwrap()
            .view()
            .tablets()
            .ranges(self.table.id)
    }

    fn scan_rows(&self) -> Vec<Vec<u8>> {
        let snap = self.engine.snapshot().unwrap();
        let mut c = self
            .engine
            .scan(
                &snap,
                self.table.id,
                ScanSpec::new(Bound::Unbounded, Bound::Unbounded),
            )
            .unwrap();
        let mut out = Vec::new();
        while c.next_row().unwrap() {
            out.push(c.row().to_vec());
        }
        out
    }
}

fn key(i: u64) -> Vec<u8> {
    // Spread over the key space, like the bench's skewed workload.
    format!("k{:016x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15)).into_bytes()
}

#[test]
fn a_tables_writes_spread_across_shards_after_splits() {
    let shards = 4;
    let mut db = open(shards, |o| {
        o.balance_interval_nanos = u64::MAX; // only the explicit balancer passes below
        o.balance_min_writes = 10;
    });
    let mut written = 0u64;
    // One table, one hot tablet on one shard: the balancer splits it over the idle shards.
    for round in 0..4 {
        for _ in 0..300 {
            db.put(&key(written), b"v");
            written += 1;
        }
        let m = db.engine.balance_pending().unwrap();
        db.drive(m).unwrap();
        eprintln!("round {round}: {:?}", db.ranges());
    }
    let owners: std::collections::BTreeSet<u16> = db.ranges().iter().map(|(_, s, ..)| *s).collect();
    assert_eq!(owners.len(), shards, "tablets on {owners:?}");
    // New writes land on every shard's WAL stream.
    db.engine.take_appended();
    for i in 0..400 {
        db.put(&key(written + i), b"w");
    }
    let streams: std::collections::BTreeSet<u16> =
        db.engine.take_appended().iter().map(|r| r.stream).collect();
    assert_eq!(streams.len(), shards, "writes went to streams {streams:?}");
    // Everything is still there, in order.
    let rows = db.scan_rows();
    assert_eq!(rows.len() as u64, written + 400);
    assert!(rows.windows(2).all(|w| w[0] < w[1]));
    for i in 0..written + 400 {
        assert!(db.get(&key(i)).is_some(), "row {i} lost");
    }
    let (splits, _, _) = db.engine.tablet_changes();
    assert!(splits > 0);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
fn a_tablet_splits_at_the_size_threshold_and_merges_when_small_and_cold() {
    let mut db = open(2, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.tablet_split_bytes = 16 << 10;
        o.memtable_freeze_bytes = 32 << 10;
    });
    let value = vec![0x5A; 200];
    for i in 0..1_500 {
        db.put(&key(i), &value);
    }
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    let m = db.engine.compact_pending(None).unwrap();
    db.drive(m).unwrap();
    assert_eq!(db.ranges().len(), 1);
    // Big enough: the balancer splits it (in two, on the same shard).
    let m = db.engine.balance_pending().unwrap();
    db.drive(m).unwrap();
    let ranges = db.ranges();
    assert_eq!(ranges.len(), 2, "{ranges:?}");
    assert_eq!(ranges[0].1, ranges[1].1);
    assert_eq!(db.scan_rows().len(), 1_500);
    // The children share the parent's SSTs until compaction rewrites them (D13).
    let m = db.engine.compact_pending(None).unwrap();
    db.drive(m).unwrap();
    assert_eq!(db.scan_rows().len(), 1_500);
    for i in (0..1_500).step_by(37) {
        assert_eq!(db.get(&key(i)).map(|(_, v)| v), Some(value.clone()));
    }
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }

    // Small and cold neighbours on one shard merge back.
    let mut db = open(2, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.tablet_split_bytes = 64 << 20;
    });
    for i in 0..50 {
        db.put(&key(i), b"x");
    }
    let id = db.table.id;
    let m = db.engine.split_tablet_pending(id, &key(3)).unwrap();
    db.drive(m).unwrap();
    assert_eq!(db.ranges().len(), 2);
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    // Two balancer passes without writes: cold.
    for _ in 0..2 {
        let m = db.engine.balance_pending().unwrap();
        db.drive(m).unwrap();
    }
    assert_eq!(db.ranges().len(), 1, "{:?}", db.ranges());
    assert_eq!(db.scan_rows().len(), 50);
    let (_, merges, _) = db.engine.tablet_changes();
    assert_eq!(merges, 1);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
fn reads_are_never_blocked_by_a_move() {
    let mut db = open(2, |_| {});
    for i in 0..100 {
        db.put(&key(i), b"before");
    }
    let snap = db.engine.snapshot().unwrap();
    let id = db.table.id;
    let ranges = db.ranges();
    let to = 1 - ranges[0].1;
    // Requested but not yet run: reads in the middle of the move see everything.
    let m = db.engine.move_tablet_pending(id, &key(0), to).unwrap();
    db.step();
    for i in 0..100 {
        assert_eq!(db.get(&key(i)).map(|(_, v)| v), Some(b"before".to_vec()));
    }
    db.drive(m).unwrap();
    assert_eq!(db.ranges()[0].1, to);
    // The old snapshot still reads through its own view.
    let f = db.table.families[0].id;
    for i in 0..100 {
        let c = db.engine.get(&snap, id, f, &key(i), b"q").unwrap();
        assert!(c.is_some());
    }
    let row = db
        .engine
        .read_row(&snap, id, &key(5), &ReadSpec::default())
        .unwrap();
    assert!(row.is_some());
    // Writes after the move go to the new owner.
    db.engine.take_appended();
    db.put(&key(5), b"after");
    assert!(db.engine.take_appended().iter().all(|r| r.stream == to));
    assert_eq!(db.get(&key(5)).map(|(_, v)| v), Some(b"after".to_vec()));
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
fn the_default_timestamp_floor_travels_with_a_tablet() {
    // The clock never moves: every default timestamp comes from the floor (D11).
    let mut db = open(2, |_| {});
    let id = db.table.id;
    let mut last = 0;
    for i in 0..20u8 {
        db.put(b"row", &[i]);
        let (ts, v) = db.get(b"row").unwrap();
        assert!(ts > last && v == [i]);
        last = ts;
    }
    let from = db.ranges()[0].1;
    let to = 1 - from;
    let m = db.engine.move_tablet_pending(id, b"row", to).unwrap();
    db.drive(m).unwrap();
    assert_eq!(db.ranges()[0].1, to);
    // The new owner never assigned a timestamp; without the carried floor its first one
    // would be older than the tablet's last.
    db.put(b"row", b"newest");
    let (ts, v) = db.get(b"row").unwrap();
    assert_eq!(v, b"newest");
    assert!(ts > last, "{ts} <= {last}");
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
fn invalid_tablet_changes_are_refused() {
    let mut db = open(2, |_| {});
    for i in 0..10 {
        db.put(&key(i), b"v");
    }
    let id = db.table.id;
    // Splitting at the tablet's first row, merging without a neighbour, moving to itself
    // or to a shard that does not exist.
    let m = db.engine.split_tablet_pending(id, b"").unwrap();
    assert!(db.drive(m).is_err());
    let m = db.engine.merge_tablets_pending(id, b"x");
    assert!(m.is_err());
    let own = db.ranges()[0].1;
    let m = db.engine.move_tablet_pending(id, b"x", own).unwrap();
    assert!(db.drive(m).is_err());
    let m = db.engine.move_tablet_pending(id, b"x", 9).unwrap();
    assert!(db.drive(m).is_err());
    // Merging tablets on different shards is refused; after a move back it works.
    let m = db.engine.split_tablet_pending(id, &key(4)).unwrap();
    db.drive(m).unwrap();
    let left_row = db.ranges()[0].2.clone();
    let m = db.engine.move_tablet_pending(id, &key(4), 1 - own).unwrap();
    db.drive(m).unwrap();
    let m = db.engine.merge_tablets_pending(id, &left_row).unwrap();
    assert!(db.drive(m).is_err());
    let m = db.engine.move_tablet_pending(id, &key(4), own).unwrap();
    db.drive(m).unwrap();
    let m = db.engine.merge_tablets_pending(id, &left_row).unwrap();
    db.drive(m).unwrap();
    assert_eq!(db.ranges().len(), 1);
    assert_eq!(db.scan_rows().len(), 10);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

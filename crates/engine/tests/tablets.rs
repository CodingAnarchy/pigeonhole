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

use common::{Config, final_dump, run, tablet_changes};
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

/// Reopens with 1, 5, 2, 8 and 4 shards after starting on 3, tablet changes churning.
fn changed_shard_count(seed: u64) {
    let mut cfg = Config::standard(300);
    churn(&mut cfg);
    cfg.shards = 3;
    cfg.reopen_shards = vec![1, 5, 2, 8, 4];
    check(seed, &cfg);
}

#[test]
fn tablet_changes_with_a_changed_shard_count() {
    for seed in seeds() {
        changed_shard_count(seed);
    }
}

#[test]
fn tablet_changes_with_a_changed_shard_count_regressions() {
    // A participant compacted a slot while it held a prepared share with an explicit
    // timestamp below a row delete in the inputs (#132).
    changed_shard_count(183);
}

/// The final dump of `seed` matches the 1-shard run for 2..=8 shards, with tablet changes.
fn identical_across_shard_counts(seed: u64) {
    let mut cfg = Config::quiet(250);
    // Purges at fixed points (decision D74), as in the model_check equivalent.
    cfg.compaction.l0_trigger = u32::MAX;
    cfg.compaction.tiered_max_space_amp_percent = u32::MAX;
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

#[test]
fn results_are_identical_across_shard_counts_with_tablet_changes() {
    for seed in seeds() {
        identical_across_shard_counts(seed);
    }
}

#[test]
fn results_identical_across_shard_counts_regressions() {
    // Seeds of the 1–300 sweep where a multi-shard run missed a cell the 1-shard run had
    // (#94): the balancer moved a tablet past the shards' full-compaction rounds (13), and a
    // lone SST was moved to the last level, keeping a delete a rewrite purged (106).
    for seed in [13, 106] {
        identical_across_shard_counts(seed);
    }
}

/// Crashes `seed` after every `PIGEONHOLE_SWEEP_STEP`-th (default 2nd) mutating operation.
fn crash_sweep(seed: u64) {
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

#[test]
fn crash_at_every_write_point_of_tablet_changes() {
    for seed in seeds() {
        crash_sweep(seed);
    }
}

#[test]
fn crash_at_every_write_point_of_tablet_changes_regressions() {
    // #98: a transaction's helper thread reported its seqno after a compaction that took
    // the commit as input was drained, and the model applied the purge without it. The
    // race needs the helper thread to lag, so it is not deterministic: before the fix about
    // a third of the sweeps failed under parallel load.
    for _ in 0..8 {
        crash_sweep(170);
    }
}

// ---------------------------------------------------------------------------------------
// Targeted checks on one engine
// ---------------------------------------------------------------------------------------

struct Db {
    vfs: Arc<SimVfs>,
    /// The engine's clock (`vfs`, or a wrapper of it).
    clock: pigeonhole_io::VfsRef,
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
    let clock = Arc::clone(&o.vfs);
    let (engine, shards) =
        Engine::open_application_owned(Path::new("/db/data.phdb"), o).expect("open");
    let table = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .expect("table");
    Db {
        vfs,
        clock,
        engine,
        shards,
        table,
    }
}

impl Db {
    fn step(&mut self) {
        let now = self.clock.monotonic_nanos();
        for s in &mut self.shards {
            s.run_once(now + 1_000);
        }
    }

    /// Steps until no shard has work left (queued messages or runnable background work).
    fn settle(&mut self) {
        for _ in 0..100_000 {
            let now = self.clock.monotonic_nanos();
            let mut busy = false;
            for s in &mut self.shards {
                busy |= s.run_once(now + 1_000);
            }
            if !busy {
                return;
            }
        }
        panic!("the shards never went idle")
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
        self.commit(wb)
    }

    fn commit(&mut self, wb: WriteBatch) -> u64 {
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
    for _ in 0..4 {
        for _ in 0..300 {
            db.put(&key(written), b"v");
            written += 1;
        }
        let m = db.engine.balance_pending().unwrap();
        db.drive(m).unwrap();
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
    let (splits, _, _) = tablet_changes(&db.engine);
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
    let (_, merges, _) = tablet_changes(&db.engine);
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
fn commit_order_across_a_move() {
    // Issue #105, the guarantee `Engine::submit` documents: a commit submitted after an
    // earlier one was acknowledged is applied after it, also when the earlier one waited
    // (parked) on a tablet's old owner during a move. Commits in flight together are all
    // applied, in either order (D132).
    let mut db = open(2, |_| {});
    let (id, f) = (db.table.id, db.table.families[0].id);
    db.put(b"row", b"before");
    let to = 1 - db.ranges()[0].1;
    let submit = |db: &Db, v: &[u8]| {
        let mut wb = WriteBatch::new();
        wb.put(id, f, b"row", b"q", None, ValueRef::Bytes(v))
            .unwrap();
        db.engine.submit(wb, Some(Durability::Buffered)).unwrap()
    };
    let wait = |db: &mut Db, mut pc: pigeonhole_engine::PendingCommit| {
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(r) = Pin::new(&mut pc).poll(&mut cx) {
                return r.expect("commit").seqno;
            }
            db.step();
        }
    };
    // Acknowledged, then the next: the order holds across the move.
    let m = db.engine.move_tablet_pending(id, b"row", to).unwrap();
    db.step();
    let parked = submit(&db, b"parked");
    db.drive(m).unwrap();
    assert_eq!(db.ranges()[0].1, to);
    let first = wait(&mut db, parked);
    let second = db.put(b"row", b"after");
    assert!(second > first);
    assert_eq!(db.get(b"row").map(|(_, v)| v), Some(b"after".to_vec()));
    // In flight together across a move back: both are applied, in some order.
    let m = db.engine.move_tablet_pending(id, b"row", 1 - to).unwrap();
    db.step();
    let a = submit(&db, b"a");
    db.drive(m).unwrap();
    let b = submit(&db, b"b");
    let (a, b) = (wait(&mut db, a), wait(&mut db, b));
    assert_ne!(a, b);
    let newest = if a > b { b"a".to_vec() } else { b"b".to_vec() };
    assert_eq!(db.get(b"row").map(|(_, v)| v), Some(newest));
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
fn a_full_compaction_rewrites_a_lone_sst() {
    // Issue #94: with tablet changes on, how many SSTs a slot holds depends on when splits
    // and moves flushed it, so a full compaction rewrites a lone L0 SST rather than moving
    // it to the last level, and purges the same deletes (D74) whatever the layout.
    let mut db = open(1, |_| {});
    let (id, f) = (db.table.id, db.table.families[0].id);
    db.put(b"row", b"old");
    let (ts, _) = db.get(b"row").unwrap();
    let mut wb = WriteBatch::new();
    wb.delete_row(id, b"row", Some(ts + 10)).unwrap();
    db.commit(wb);
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    let m = db.engine.compact_pending(None).unwrap();
    db.drive(m).unwrap();
    // The purged delete no longer hides a write below it (D74).
    let mut wb = WriteBatch::new();
    wb.put(id, f, b"row", b"q", Some(ts + 5), ValueRef::Bytes(b"below"))
        .unwrap();
    db.commit(wb);
    assert_eq!(db.get(b"row"), Some((ts + 5, b"below".to_vec())));
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
fn a_bottommost_compaction_counts_a_prepared_share_above_its_inputs() {
    // Issue #132 (seed 183): a participant compacted a slot while it held a cross-shard
    // share, prepared and undecided, with an explicit timestamp below a row delete in the
    // inputs. `min_ts_above` saw only the memtables, so the delete was purged, and the share
    // applied after it became visible. A prepared share is above the inputs (D70). Tablet
    // changes off: two tables on two shards take the same two-phase path.
    let mut db = open(2, |o| o.tablet_changes = false);
    let owner = |db: &Db, table| db.engine.snapshot().unwrap().view().tablets().ranges(table)[0].1;
    let participant = usize::from(owner(&db, db.table.id));
    let other = (0..8)
        .map(|i| {
            db.engine
                .create_table(&format!("u{i}"), &[("f".into(), FamilyOptions::default())])
                .unwrap()
        })
        .find(|u| usize::from(owner(&db, u.id)) != participant)
        .expect("a table on the other shard");
    let (id, f) = (db.table.id, db.table.families[0].id);
    // Two L0 SSTs, so the full compaction rewrites them at the bottom (and may purge).
    db.put(b"row", b"old");
    let (ts, _) = db.get(b"row").unwrap();
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    let mut wb = WriteBatch::new();
    wb.delete_row(id, b"row", Some(ts + 10)).unwrap();
    db.commit(wb);
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();

    // The other table's row first: its shard coordinates.
    let mut wb = WriteBatch::new();
    wb.put(
        other.id,
        other.families[0].id,
        b"x",
        b"q",
        None,
        ValueRef::Bytes(b"x"),
    )
    .unwrap();
    wb.put(id, f, b"row", b"q", Some(ts + 5), ValueRef::Bytes(b"below"))
        .unwrap();
    let mut pc = db
        .engine
        .submit(wb, Some(Durability::Buffered))
        .expect("submit");
    let coordinator = 1 - participant;
    let now = db.clock.monotonic_nanos();
    db.shards[coordinator].run_once(now + 1_000);
    // The participant prepares its share; the coordinator does not run, so no decision.
    for _ in 0..8 {
        db.shards[participant].run_once(now + 1_000);
    }
    let m = db.engine.compact_pending(None).unwrap();
    for _ in 0..64 {
        db.shards[participant].run_once(now + 1_000);
    }
    step_until_done(&mut db, &mut pc);
    db.drive(m).unwrap();
    db.settle();
    // The delete was above the share's timestamp, so it was kept: the share stays hidden.
    assert_eq!(db.get(b"row"), None);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
fn a_participant_refuses_a_commit_timestamp_below_its_floor() {
    // Issue #105: a coordinator picks a cross-shard commit's timestamp above the floors it
    // reads, but a participant may assign higher default timestamps before the PREPARE
    // arrives (another thread published its floor just after the coordinator read it; the
    // hook replays that read). The participant refuses, and the retry takes a fresh
    // timestamp above everything it assigned, so its tablet's timestamps never go back.
    let mut db = open(2, |_| {});
    let id = db.table.id;
    let f = db.table.families[0].id;
    let m = db.engine.split_tablet_pending(id, b"m").unwrap();
    db.drive(m).unwrap();
    let low = db.ranges()[0].1;
    let high = 1 - low;
    if db.ranges()[1].1 != high {
        let m = db.engine.move_tablet_pending(id, b"z", high).unwrap();
        db.drive(m).unwrap();
    }
    // The frozen clock: each default timestamp is the shard's floor plus one.
    let mut last = 0;
    for i in 0..10u8 {
        db.put(b"z", &[i]);
        last = db.get(b"z").unwrap().0;
    }
    assert!(db.get(b"a").is_none());
    db.engine.publish_stale_ts_floor(high, 0);
    let mut wb = WriteBatch::new();
    wb.put(id, f, b"a", b"q", None, ValueRef::Bytes(b"low"))
        .unwrap();
    wb.put(id, f, b"z", b"q", None, ValueRef::Bytes(b"both"))
        .unwrap();
    db.commit(wb);
    let (ts, v) = db.get(b"z").unwrap();
    assert_eq!(v, b"both");
    assert!(
        ts > last,
        "{ts} <= {last}: the commit went below the participant's floor"
    );
    assert_eq!(db.get(b"a").unwrap().0, ts);
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

#[test]
fn a_merge_waits_until_both_siblings_compacted_a_shared_sst() {
    // After a split both children reference the parent's SSTs (D13). Once one child has
    // compacted its copy and the other has not, the other's SST still holds the first
    // child's rows: a merged tablet would read them twice (and resurrect what the first
    // child's compaction dropped), so the merge is refused until both compacted.
    let mut db = open(1, |o| {
        o.balance_interval_nanos = 0;
        o.compaction.l0_trigger = 2;
    });
    let id = db.table.id;
    let mut rows: Vec<Vec<u8>> = (0..40).map(key).collect();
    rows.sort();
    for r in &rows {
        db.put(r, b"old");
    }
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    let mid = rows[20].clone();
    let m = db.engine.split_tablet_pending(id, &mid).unwrap();
    db.drive(m).unwrap();
    let ranges = db.ranges();
    assert_eq!(ranges.len(), 2, "{ranges:?}");
    let left = ranges[0].0;
    // Only the left child reaches the L0 trigger: it rewrites its copy of the shared SST.
    db.engine.take_compactions();
    for r in &rows[..20] {
        db.put(r, b"new");
    }
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    let mut compacted = Vec::new();
    for _ in 0..1_000 {
        compacted.extend(db.engine.take_compactions());
        if !compacted.is_empty() {
            break;
        }
        db.step();
    }
    assert!(
        !compacted.is_empty() && compacted.iter().all(|c| c.tablet == left),
        "{compacted:?}"
    );
    let m = db.engine.merge_tablets_pending(id, &rows[0]).unwrap();
    let e = db
        .drive(m)
        .expect_err("merged while a sibling still shares an SST");
    assert!(e.to_string().contains("shared SST"), "{e}");
    assert_eq!(db.ranges().len(), 2);
    // Once both children compacted, the merge goes through and every row reads once.
    let m = db.engine.compact_pending(None).unwrap();
    db.drive(m).unwrap();
    let m = db.engine.merge_tablets_pending(id, &rows[0]).unwrap();
    db.drive(m).unwrap();
    assert_eq!(db.ranges().len(), 1);
    assert_eq!(db.scan_rows(), rows);
    for (i, r) in rows.iter().enumerate() {
        let want: &[u8] = if i < 20 { b"new" } else { b"old" };
        assert_eq!(db.get(r).map(|(_, v)| v).as_deref(), Some(want), "row {i}");
    }
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
fn tablet_changes_are_refused_when_switched_off() {
    let mut db = open(2, |o| o.tablet_changes = false);
    for i in 0..10 {
        db.put(&key(i), b"v");
    }
    let id = db.table.id;
    let refused = |r: pigeonhole_engine::Result<()>| {
        assert!(
            matches!(r, Err(pigeonhole_engine::Error::Unsupported(_))),
            "{r:?}"
        );
    };
    let m = db.engine.split_tablet_pending(id, &key(4)).unwrap();
    refused(db.drive(m));
    let to = 1 - db.ranges()[0].1;
    let m = db.engine.move_tablet_pending(id, &key(4), to).unwrap();
    refused(db.drive(m));
    let m = db.engine.balance_pending().unwrap();
    refused(db.drive(m));
    assert_eq!(db.ranges().len(), 1);
    assert_eq!(tablet_changes(&db.engine), (0, 0, 0));
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
fn the_balancer_merges_cold_siblings_once_one_compacted_a_shared_sst() {
    // After a split, the left child compacts its copy of the shared SST and the right child
    // never reaches its L0 trigger: its inherited SST still holds the left child's rows, so
    // the merge is refused (correctly), and the balancer has to ask for a rewrite of the
    // right child's SST before it can merge (#95). Every step waits for an event rather
    // than counting on time or a fixed number of passes.
    let mut db = open(1, |o| {
        o.balance_interval_nanos = u64::MAX; // only the explicit passes below
        o.tablet_split_bytes = 64 << 20;
        o.compaction.l0_trigger = 2;
    });
    let id = db.table.id;
    let mut rows: Vec<Vec<u8>> = (0..40).map(key).collect();
    rows.sort();
    for r in &rows {
        db.put(r, b"old");
    }
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    let m = db.engine.split_tablet_pending(id, &rows[20]).unwrap();
    db.drive(m).unwrap();
    let ranges = db.ranges();
    assert_eq!(ranges.len(), 2, "{ranges:?}");
    let (left, right) = (ranges[0].0, ranges[1].0);
    // Only the left child reaches its L0 trigger and rewrites its copy of the shared SST.
    db.engine.take_compactions();
    for r in &rows[..20] {
        db.put(r, b"new");
    }
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    let mut compacted = Vec::new();
    for _ in 0..100_000 {
        compacted.extend(db.engine.take_compactions());
        if !compacted.is_empty() {
            break;
        }
        db.step();
    }
    assert!(
        !compacted.is_empty() && compacted.iter().all(|c| c.tablet == left),
        "the left child never compacted on its own: {compacted:?}"
    );
    // Explicit balancer passes, each driven to completion, until the merge commits. The
    // tablets turn cold after two passes without writes; then the balancer asks for the
    // right child's rewrite (a background compaction, stepped to completion) and merges
    // on a later pass.
    let mut rewrote_right = false;
    for pass in 0..32 {
        let m = db.engine.balance_pending().unwrap();
        db.drive(m).unwrap();
        db.settle();
        rewrote_right |= db
            .engine
            .take_compactions()
            .iter()
            .any(|c| c.tablet == right);
        if db.ranges().len() == 1 {
            eprintln!("merged after {} passes", pass + 1);
            break;
        }
    }
    assert!(
        rewrote_right,
        "the balancer never rewrote the right child's inherited SST"
    );
    assert_eq!(db.ranges().len(), 1, "never merged: {:?}", db.ranges());
    assert_eq!(db.scan_rows(), rows);
    for (i, r) in rows.iter().enumerate() {
        let want: &[u8] = if i < 20 { b"new" } else { b"old" };
        assert_eq!(db.get(r).map(|(_, v)| v).as_deref(), Some(want), "row {i}");
    }
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// A cross-shard commit that touches a tablet just moved to another shard gets a timestamp
/// above the tablet's earlier writes, so it is the newest version.
#[test]
fn a_cross_shard_commit_after_a_move_is_the_newest_version() {
    // 3 shards; T's tablet moves A -> B; a cross-shard commit coordinated by C touches T.
    let mut db = open(3, |_| {});
    let t = db.table.id;
    let ft = db.table.families[0].id;
    let from = db.ranges()[0].1;
    let u = db
        .engine
        .create_table("u", &[("g".into(), FamilyOptions::default())])
        .unwrap();
    let snap = db.engine.snapshot().unwrap();
    let ushard = snap.view().tablets().ranges(u.id)[0].1;
    drop(snap);
    let mut last = 0;
    for i in 0..50u8 {
        db.put(b"row", &[i]);
        let (ts, _) = db.get(b"row").unwrap();
        last = ts;
    }
    let to = (0..3u16)
        .find(|s| *s != from && *s != ushard)
        .unwrap_or((from + 1) % 3);
    let m = db.engine.move_tablet_pending(t, b"row", to).unwrap();
    db.drive(m).unwrap();
    // Cross-shard: first row on u (coordinator = u's shard), then t/row.
    let mut wb = WriteBatch::new();
    wb.put(
        u.id,
        u.families[0].id,
        b"x",
        b"q",
        None,
        ValueRef::Bytes(b"ux"),
    )
    .unwrap();
    wb.put(t, ft, b"row", b"q", None, ValueRef::Bytes(b"newest"))
        .unwrap();
    let mut pc = db.engine.submit(wb, Some(Durability::Buffered)).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(r) = Pin::new(&mut pc).poll(&mut cx) {
            r.expect("commit");
            break;
        }
        db.step();
    }
    let (ts, v) = db.get(b"row").unwrap();
    assert_eq!(v, b"newest", "a newer commit is hidden: ts {ts} <= {last}");
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// Opens `/db/data.phdb` on `vfs` (application-owned, tablet changes on, no balancer) and
/// table `t` with `families` families, creating it if missing.
fn open_wide(
    vfs: &Arc<SimVfs>,
    shards: usize,
    families: usize,
    tweak: impl FnOnce(&mut EngineOptions),
) -> Db {
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = shards;
    o.pin_threads = false;
    o.memtable_budget = 4 << 20;
    o.reader_slots = 4;
    o.tablet_changes = true;
    o.balance_interval_nanos = 0;
    tweak(&mut o);
    let clock = Arc::clone(&o.vfs);
    let (engine, shards) =
        Engine::open_application_owned(Path::new("/db/data.phdb"), o).expect("open");
    let table = engine.table("t").unwrap_or_else(|| {
        let fams: Vec<(String, FamilyOptions)> = (0..families)
            .map(|i| (format!("f{i:02}"), FamilyOptions::default()))
            .collect();
        engine.create_table("t", &fams).expect("table")
    });
    Db {
        vfs: Arc::clone(vfs),
        clock,
        engine,
        shards,
        table,
    }
}

impl Db {
    fn close(mut self) {
        self.engine.close().unwrap();
        for _ in 0..8 {
            self.step();
        }
    }

    fn flush(&mut self) {
        let m = self.engine.flush_pending().unwrap();
        self.drive(m).unwrap();
    }

    /// One commit writing `value` to `rows` in every family of the table.
    fn put_wide(&mut self, rows: &[Vec<u8>], value: &[u8]) -> pigeonhole_engine::Result<()> {
        let mut wb = WriteBatch::new();
        for f in &self.table.families {
            for row in rows {
                wb.put(self.table.id, f.id, row, b"q", None, ValueRef::Bytes(value))
                    .unwrap();
            }
        }
        let mut pc = self.engine.submit(wb, Some(Durability::Buffered))?;
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..1_000_000 {
            if let Poll::Ready(r) = Pin::new(&mut pc).poll(&mut cx) {
                return r.map(|_| ());
            }
            self.step();
        }
        panic!("a wide commit never resolved")
    }

    fn get_in(&self, family: usize, row: &[u8]) -> Option<Vec<u8>> {
        let f = self.table.families[family].id;
        self.engine
            .get_latest(self.table.id, f, row, b"q")
            .expect("get")
            .map(|c| {
                let ValueRef::Bytes(v) = c.value() else {
                    panic!("bytes")
                };
                v.to_vec()
            })
    }

    fn assert_wide(&self, rows: &[Vec<u8>], value: &[u8]) {
        for (i, _) in self.table.families.iter().enumerate() {
            for row in rows {
                assert_eq!(self.get_in(i, row).as_deref(), Some(value), "family {i}");
            }
        }
    }
}

#[test]
fn a_reopen_with_fewer_shards_after_splits_keeps_writes_flowing() {
    // Issue #104: a 20-family table split into four tablets, one per shard, is 80
    // `(tablet, family)` slots. Owners are not persisted (D130), so a reopen on one shard
    // puts all 80 on it: more slots than its arena had chunks, so a commit writing them all
    // was refused with `Busy` for ever. The arena's chunks now shrink to serve the slots
    // placed at open, and a reopen on two shards spreads the tablets within each arena.
    let vfs = SimVfs::new(104);
    let mut db = open_wide(&vfs, 4, 20, |_| {});
    let mut rows: Vec<Vec<u8>> = (0..64).map(key).collect();
    rows.sort();
    db.put_wide(&rows, b"a").unwrap();
    // A shard holding more than 16 families splits (it used to be capped at a quarter of
    // 64 chunks for budgets up to 16 MiB).
    let id = db.table.id;
    let home = db.ranges()[0].1;
    let others: Vec<u16> = (0..4).filter(|s| *s != home).collect();
    for (at, to) in [(32, others[0]), (16, others[1]), (48, others[2])] {
        let m = db.engine.split_tablet_pending(id, &rows[at]).unwrap();
        db.drive(m).unwrap();
        let m = db.engine.move_tablet_pending(id, &rows[at], to).unwrap();
        db.drive(m).unwrap();
    }
    let owners: std::collections::BTreeSet<u16> = db.ranges().iter().map(|r| r.1).collect();
    assert_eq!(owners.len(), 4, "{:?}", db.ranges());
    db.put_wide(&rows, b"b").unwrap();
    db.flush();
    db.close();

    // One shard: all 80 slots. Wide commits and flushes of every slot keep working.
    let mut db = open_wide(&vfs, 1, 20, |_| {});
    assert_eq!(db.ranges().len(), 4);
    db.assert_wide(&rows, b"b");
    for v in [b"c", b"d", b"e"] {
        db.put_wide(&rows, v).unwrap();
        db.flush();
    }
    db.assert_wide(&rows, b"e");
    db.close();

    // Two shards: 40 slots each, both take writes.
    let mut db = open_wide(&vfs, 2, 20, |_| {});
    let owners: Vec<u16> = db.ranges().iter().map(|r| r.1).collect();
    assert_eq!(owners.iter().filter(|s| **s == 0).count(), 2, "{owners:?}");
    db.put_wide(&rows, b"f").unwrap();
    db.flush();
    db.assert_wide(&rows, b"f");
    db.close();
}

/// Commits (one row in family 0, then a flush) until one waits for arena room, with a
/// reader process pinning its first view the whole time (so every retired chunk stays).
fn cycles_until_a_stall_under_a_reader_pin(tablet_changes: bool) -> u32 {
    use pigeonhole_io::ProcessId;
    let writer = ProcessId {
        pid: 1,
        start_time: 1,
    };
    let reader = ProcessId {
        pid: 2,
        start_time: 1,
    };
    let options = |vfs: &Arc<SimVfs>| {
        let mut o = EngineOptions::new(vfs.clone());
        o.create_if_missing = true;
        o.shards = 1;
        o.pin_threads = false;
        // A 512 KiB budget in a 2 MiB arena: 8 KiB chunks with tablet changes on or off.
        o.memtable_budget = 512 << 10;
        o.reader_slots = 4;
        o.tablet_changes = tablet_changes;
        o.balance_interval_nanos = 0;
        o
    };
    let vfs = SimVfs::new(96);
    vfs.enter_process(writer);
    let mut db = open_wide(&vfs, 1, 4, |o| *o = options(&vfs));
    db.put_wide(&[b"r".to_vec()], b"v").unwrap();
    db.flush();
    vfs.enter_process(reader);
    let r = Engine::open_reader(Path::new("/db/data.phdb"), options(&vfs)).unwrap();
    let pin = r.snapshot().unwrap();
    vfs.enter_process(writer);
    let f = db.table.families[0].id;
    let mut cycles = 0;
    let mut cx = Context::from_waker(Waker::noop());
    let mut pc = loop {
        assert!(cycles < 10_000, "never stalled");
        let mut wb = WriteBatch::new();
        let row = format!("r{cycles:05}");
        wb.put(
            db.table.id,
            f,
            row.as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(b"v"),
        )
        .unwrap();
        let mut pc = db.engine.submit(wb, Some(Durability::Buffered)).unwrap();
        let mut done = false;
        for _ in 0..1_000 {
            if let Poll::Ready(r) = Pin::new(&mut pc).poll(&mut cx) {
                r.unwrap();
                done = true;
                break;
            }
            db.step();
        }
        if !done {
            assert!(db.engine.metrics().stalls.0 > 0, "cycle {cycles} hangs");
            break pc;
        }
        cycles += 1;
        db.flush();
    };
    // Releasing the pin frees the room the stalled commit waits for.
    vfs.enter_process(reader);
    drop(pin);
    r.close().unwrap();
    vfs.enter_process(writer);
    for _ in 0..100_000 {
        if let Poll::Ready(r) = Pin::new(&mut pc).poll(&mut cx) {
            r.unwrap();
            break;
        }
        db.step();
    }
    db.close();
    cycles
}

#[test]
fn idle_slots_do_not_churn_chunks_under_a_reader_pin() {
    // Issue #104: retiring every idle slot after each flush gave a slot written once per
    // flush a fresh memtable each cycle, and a reader process's pin keeps every retired
    // chunk: the arena filled twice as fast as with tablet changes off. Idle slots now
    // retire only when a commit waits for arena room.
    let off = cycles_until_a_stall_under_a_reader_pin(false);
    let on = cycles_until_a_stall_under_a_reader_pin(true);
    eprintln!("commits before a stall: off {off}, on {on}");
    assert!(off > 50, "{off}");
    assert!(on + 2 >= off, "on {on}, off {off}");
}

#[test]
fn a_busy_shard_still_rewrites_the_sst_blocking_a_cold_merge() {
    // As above, but another table on the shard keeps reaching its L0 trigger: some slot is
    // always due for compaction when the balancer asks for the rewrite. Cleanups alternate
    // with due compactions, so the cold pair still merges.
    let mut db = open(1, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.tablet_split_bytes = 64 << 20;
        o.compaction.l0_trigger = 2;
    });
    let id = db.table.id;
    let w = db
        .engine
        .create_table("w", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let mut rows: Vec<Vec<u8>> = (0..40).map(key).collect();
    rows.sort();
    for r in &rows {
        db.put(r, b"old");
    }
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    let m = db.engine.split_tablet_pending(id, &rows[20]).unwrap();
    db.drive(m).unwrap();
    for r in &rows[..20] {
        db.put(r, b"new");
    }
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    db.settle();
    let mut n = 0u64;
    for pass in 0..32 {
        // Two flushes of `w` each pass: it is due again whenever the balancer runs.
        for _ in 0..2 {
            let mut wb = WriteBatch::new();
            for _ in 0..8 {
                wb.put(
                    w.id,
                    w.families[0].id,
                    &key(n),
                    b"q",
                    None,
                    ValueRef::Bytes(b"w"),
                )
                .unwrap();
                n += 1;
            }
            let mut pc = db.engine.submit(wb, Some(Durability::Buffered)).unwrap();
            step_until_done(&mut db, &mut pc);
            let m = db.engine.flush_pending().unwrap();
            db.drive(m).unwrap();
        }
        let m = db.engine.balance_pending().unwrap();
        db.drive(m).unwrap();
        db.settle();
        if db.ranges().len() == 1 {
            eprintln!("merged after {} passes", pass + 1);
            break;
        }
    }
    assert_eq!(db.ranges().len(), 1, "never merged: {:?}", db.ranges());
    assert_eq!(db.scan_rows(), rows);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

fn step_until_done(db: &mut Db, pc: &mut pigeonhole_engine::PendingCommit) {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..100_000 {
        if let Poll::Ready(r) = Pin::new(&mut *pc).poll(&mut cx) {
            r.expect("commit");
            return;
        }
        db.step();
    }
    panic!("a commit never resolved");
}

// ---- liveness of commits that meet a tablet change (#102) ----

/// A VFS whose clock moves a nanosecond each time it is read, so timers see a moving
/// clock (`SimVfs`'s only moves when the test advances it).
#[derive(Debug)]
struct Ticking {
    inner: pigeonhole_io::VfsRef,
    ticks: std::sync::atomic::AtomicU64,
}

impl Ticking {
    fn tick(&self) -> u64 {
        self.ticks
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

fn ticking(o: &mut EngineOptions) {
    o.vfs = Arc::new(Ticking {
        inner: Arc::clone(&o.vfs),
        ticks: std::sync::atomic::AtomicU64::new(0),
    });
}

impl pigeonhole_io::Vfs for Ticking {
    fn open(
        &self,
        path: &Path,
        opts: pigeonhole_io::OpenOptions,
    ) -> pigeonhole_io::Result<pigeonhole_io::FileRef> {
        self.inner.open(path, opts)
    }
    fn remove(&self, path: &Path) -> pigeonhole_io::Result<()> {
        self.inner.remove(path)
    }
    fn exists(&self, path: &Path) -> pigeonhole_io::Result<bool> {
        self.inner.exists(path)
    }
    fn list_dir(&self, dir: &Path) -> pigeonhole_io::Result<Vec<std::path::PathBuf>> {
        self.inner.list_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> pigeonhole_io::Result<()> {
        self.inner.sync_dir(dir)
    }
    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: pigeonhole_io::SharedOpen,
    ) -> pigeonhole_io::Result<pigeonhole_io::SharedRegion> {
        self.inner.open_shared(name, dir, len, mode)
    }
    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> pigeonhole_io::Result<()> {
        self.inner.remove_shared(name, dir)
    }
    fn now_micros(&self) -> u64 {
        self.inner.now_micros() + self.tick() / 1_000
    }
    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos() + self.tick()
    }
    fn current_process(&self) -> pigeonhole_io::ProcessId {
        self.inner.current_process()
    }
    fn process_alive(&self, process: pigeonhole_io::ProcessId) -> bool {
        self.inner.process_alive(process)
    }
}

/// One shard holding `t`'s only tablet, with a compaction of it parked before its manifest
/// commit: a change started now waits for that compaction, so it stays in its drain.
fn hold_a_compaction(db: &mut Db) {
    for i in 0..20 {
        db.put(&key(i), b"v");
    }
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    db.engine.park_manifest_commits(true);
    let _compact = db.engine.compact_pending(None).unwrap();
    for _ in 0..1_000 {
        if db.engine.manifest_commit_parked() {
            return;
        }
        db.step();
    }
    panic!("the compaction never reached its manifest commit");
}

fn submit_put(db: &Db, table: &TableInfo, row: &[u8]) -> pigeonhole_engine::PendingCommit {
    let mut wb = WriteBatch::new();
    wb.put(
        table.id,
        table.families[0].id,
        row,
        b"q",
        None,
        ValueRef::Bytes(b"w"),
    )
    .unwrap();
    db.engine.submit(wb, Some(Durability::Buffered)).unwrap()
}

fn poll_commit(
    pc: &mut pigeonhole_engine::PendingCommit,
) -> Option<pigeonhole_engine::Result<pigeonhole_engine::CommitInfo>> {
    let mut cx = Context::from_waker(Waker::noop());
    match Pin::new(pc).poll(&mut cx) {
        Poll::Ready(r) => Some(r),
        Poll::Pending => None,
    }
}

/// Steps with the clock moving `nanos` per step until `pc` resolves.
fn step_until(
    db: &mut Db,
    pc: &mut pigeonhole_engine::PendingCommit,
    nanos: u64,
) -> pigeonhole_engine::Result<pigeonhole_engine::CommitInfo> {
    for _ in 0..10_000 {
        if let Some(r) = poll_commit(pc) {
            return r;
        }
        db.vfs.advance(nanos);
        db.step();
    }
    panic!("the commit never resolved");
}

fn close(mut db: Db) {
    db.engine.park_manifest_commits(false);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
fn a_commit_parked_behind_a_change_fails_busy_after_the_write_stall_timeout() {
    let mut db = open(1, |o| {
        o.write_stall_timeout_nanos = 50_000_000;
        ticking(o);
    });
    hold_a_compaction(&mut db);
    let id = db.table.id;
    let split = db.engine.split_tablet_pending(id, &key(3)).unwrap();
    for _ in 0..50 {
        db.vfs.advance(1_000);
        db.step();
    }
    let table = Arc::clone(&db.table);
    let mut pc = submit_put(&db, &table, &key(1));
    for _ in 0..50 {
        db.vfs.advance(1_000);
        db.step();
    }
    assert!(
        poll_commit(&mut pc).is_none(),
        "the commit should be parked"
    );
    let r = step_until(&mut db, &mut pc, 1_000_000);
    assert!(matches!(r, Err(pigeonhole_engine::Error::Busy)), "{r:?}");
    // The change itself goes through once the compaction commits.
    db.engine.park_manifest_commits(false);
    db.drive(split).unwrap();
    assert_eq!(db.ranges().len(), 2);
    db.put(&key(1), b"after");
    assert_eq!(db.get(&key(1)).map(|(_, v)| v), Some(b"after".to_vec()));
    close(db);
}

#[test]
fn close_gives_up_a_draining_change_and_its_parked_commits() {
    let mut db = open(1, |_| {});
    hold_a_compaction(&mut db);
    let id = db.table.id;
    let mut split = db.engine.split_tablet_pending(id, &key(3)).unwrap();
    for _ in 0..50 {
        db.vfs.advance(1_000);
        db.step();
    }
    let table = Arc::clone(&db.table);
    let mut pc = submit_put(&db, &table, &key(1));
    for _ in 0..50 {
        db.vfs.advance(1_000);
        db.step();
    }
    assert!(
        poll_commit(&mut pc).is_none(),
        "the commit should be parked"
    );
    db.engine.close().unwrap();
    for _ in 0..50 {
        db.vfs.advance(1_000);
        db.step();
    }
    // Both resolve while the compaction is still parked: close waits on neither.
    let r = poll_commit(&mut pc).expect("the parked commit resolved at close");
    assert!(matches!(r, Err(pigeonhole_engine::Error::Closed)), "{r:?}");
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(r) = Pin::new(&mut split).poll(&mut cx) else {
        panic!("the change is still running")
    };
    assert!(matches!(r, Err(pigeonhole_engine::Error::Closed)), "{r:?}");
    db.engine.park_manifest_commits(false);
    for _ in 0..1_000 {
        db.step();
    }
    assert_eq!(db.ranges().len(), 1);
}

#[test]
fn the_balancer_leaves_a_compacting_tablet_alone() {
    let mut db = open(1, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.tablet_split_bytes = 1;
    });
    hold_a_compaction(&mut db);
    // Big enough to split by size, but compacting: the balancer waits.
    let m = db.engine.balance_pending().unwrap();
    db.drive(m).unwrap();
    assert_eq!(tablet_changes(&db.engine), (0, 0, 0));
    assert_eq!(db.ranges().len(), 1);
    db.engine.park_manifest_commits(false);
    for _ in 0..1_000 {
        db.step();
    }
    let m = db.engine.balance_pending().unwrap();
    db.drive(m).unwrap();
    assert_eq!(tablet_changes(&db.engine).0, 1);
    close(db);
}

/// Two shards: `t` split with its right child on shard 1 and its left child on shard 0
/// (which therefore lost a tablet), and table `u` on shard 1.
fn two_shards_with_a_lost_tablet(db: &mut Db) -> Arc<TableInfo> {
    let t = db.table.id;
    let mut rows: Vec<Vec<u8>> = (0..20).map(key).collect();
    rows.sort();
    for r in &rows {
        db.put(r, b"v");
    }
    if db.ranges()[0].1 != 0 {
        let m = db.engine.move_tablet_pending(t, &rows[0], 0).unwrap();
        db.drive(m).unwrap();
    }
    let m = db.engine.split_tablet_pending(t, &rows[10]).unwrap();
    db.drive(m).unwrap();
    let m = db.engine.move_tablet_pending(t, &rows[10], 1).unwrap();
    db.drive(m).unwrap();
    let u = db
        .engine
        .create_table("u", &[("g".into(), FamilyOptions::default())])
        .unwrap();
    let snap = db.engine.snapshot().unwrap();
    let ushard = snap.view().tablets().ranges(u.id)[0].1;
    drop(snap);
    if ushard != 1 {
        let m = db.engine.move_tablet_pending(u.id, b"x", 1).unwrap();
        db.drive(m).unwrap();
    }
    u
}

#[test]
fn a_prepare_routed_with_an_older_map_runs_when_none_of_its_rows_moved() {
    let mut db = open(2, |_| {});
    let u = two_shards_with_a_lost_tablet(&mut db);
    let t = Arc::clone(&db.table);
    // Coordinated by shard 1 (u's row first); shard 0's share is the left child of t. The
    // tablet map changes between every step (a new table each time), so every PREPARE
    // reaches shard 0 routed with an older map, though no row of the commit moved. Each one
    // used to be refused with `Moved` and retried, for ever.
    let mut wb = WriteBatch::new();
    wb.put(
        u.id,
        u.families[0].id,
        b"x",
        b"q",
        None,
        ValueRef::Bytes(b"u"),
    )
    .unwrap();
    wb.put(
        t.id,
        t.families[0].id,
        &key(0),
        b"q",
        None,
        ValueRef::Bytes(b"t"),
    )
    .unwrap();
    let mut pc = db.engine.submit(wb, Some(Durability::Buffered)).unwrap();
    let mut done = None;
    for i in 0..200 {
        db.engine
            .create_table(
                &format!("bump{i}"),
                &[("f".into(), FamilyOptions::default())],
            )
            .unwrap();
        if let Some(r) = poll_commit(&mut pc) {
            done = Some(r);
            break;
        }
        db.step();
    }
    let r = done.expect("the commit never finished");
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(db.get(&key(0)).map(|(_, v)| v), Some(b"t".to_vec()));
    close(db);
}

#[test]
fn a_commit_refused_while_a_change_waits_fails_busy_after_the_write_stall_timeout() {
    // Shard 0 holds a split of t's left child in its drain (the child's compaction is
    // parked). A cross-shard commit touching that child is refused with `Moved` and waits
    // for the tablet map to change; it gives up with `Busy` once the write stall timeout
    // has passed rather than waiting for ever.
    let mut db = open(2, |o| {
        o.write_stall_timeout_nanos = 50_000_000;
        ticking(o);
    });
    let u = two_shards_with_a_lost_tablet(&mut db);
    let t = Arc::clone(&db.table);
    let mut rows: Vec<Vec<u8>> = (0..20).map(key).collect();
    rows.sort();
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    db.engine.park_manifest_commits(true);
    let _compact = db.engine.compact_pending(Some(t.id)).unwrap();
    for _ in 0..1_000 {
        if db.engine.manifest_commit_parked() {
            break;
        }
        db.step();
    }
    assert!(db.engine.manifest_commit_parked());
    let compacting = db.engine.take_compactions();
    let left = db.ranges()[0].0;
    assert!(compacting.iter().any(|c| c.tablet == left) || compacting.is_empty());
    let split = db.engine.split_tablet_pending(t.id, &rows[5]).unwrap();
    for _ in 0..50 {
        db.vfs.advance(1_000);
        db.step();
    }
    let mut wb = WriteBatch::new();
    wb.put(
        u.id,
        u.families[0].id,
        b"x",
        b"q",
        None,
        ValueRef::Bytes(b"u"),
    )
    .unwrap();
    wb.put(
        t.id,
        t.families[0].id,
        &rows[1],
        b"q",
        None,
        ValueRef::Bytes(b"t"),
    )
    .unwrap();
    let mut pc = db.engine.submit(wb, Some(Durability::Buffered)).unwrap();
    for _ in 0..50 {
        db.vfs.advance(1_000);
        db.step();
    }
    assert!(
        poll_commit(&mut pc).is_none(),
        "the commit should wait to retry"
    );
    let r = step_until(&mut db, &mut pc, 1_000_000);
    assert!(matches!(r, Err(pigeonhole_engine::Error::Busy)), "{r:?}");
    db.engine.park_manifest_commits(false);
    db.drive(split).unwrap();
    close(db);
}

// ---- balancer stability (#103) ----

/// `t` split at `keys` and its tablets handed to `owners` in order.
fn lay_out(db: &mut Db, keys: &[&[u8]], owners: &[u16]) {
    let id = db.table.id;
    for k in keys {
        let m = db.engine.split_tablet_pending(id, k).unwrap();
        db.drive(m).unwrap();
    }
    for (i, owner) in owners.iter().enumerate() {
        let r = db.ranges()[i].clone();
        if r.1 != *owner {
            let m = db.engine.move_tablet_pending(id, &r.2, *owner).unwrap();
            db.drive(m).unwrap();
        }
    }
    let got: Vec<u16> = db.ranges().iter().map(|r| r.1).collect();
    assert_eq!(got, owners);
}

fn changes(db: &Db) -> u64 {
    let (s, m, v) = tablet_changes(&db.engine);
    s + m + v
}

#[test]
fn the_balancer_goes_quiet_under_uniform_load() {
    // Four shards, one tablet each over a quarter of a uniformly written key space. Every
    // interval each shard writes about the same rows, give or take sampling noise, and
    // memtables flush at different times. Nothing is skewed, so nothing should move.
    let mut db = open(4, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
        o.memtable_freeze_bytes = 16 << 10;
    });
    lay_out(&mut db, &[b"k4", b"k8", b"kc"], &[0, 1, 2, 3]);
    let before = changes(&db);
    let value = vec![7u8; 200];
    let mut written = 0u64;
    for pass in 0..30 {
        for _ in 0..200 {
            db.put(&key(written), &value);
            written += 1;
        }
        let m = db.engine.balance_pending().unwrap();
        db.drive(m).unwrap();
        eprintln!("pass {pass}: {} changes", changes(&db) - before);
    }
    assert_eq!(changes(&db), before, "{:?}", db.ranges());
    assert_eq!(db.scan_rows().len() as u64, written);
    close(db);
}

#[test]
fn concurrent_moves_never_take_a_shard_past_its_slots() {
    // A shard holds at most a quarter of its arena's chunks in `(tablet, family)` slots: 64
    // here. With 21 families a tablet takes 21 slots, so shard 2 holding two tablets (42)
    // has room for one more. Shards 0 and 1 each move a tablet to it at the same time:
    // only one may land.
    let vfs = SimVfs::new(7);
    let mut db = open_wide(&vfs, 3, 21, |_| {});
    let id = db.table.id;
    if db.ranges()[0].1 != 0 {
        let m = db.engine.move_tablet_pending(id, b"", 0).unwrap();
        db.drive(m).unwrap();
    }
    let step = |db: &mut Db, at: &[u8], piece: &[u8], to: u16| {
        let m = db.engine.split_tablet_pending(id, at).unwrap();
        db.drive(m).unwrap();
        let m = db.engine.move_tablet_pending(id, piece, to).unwrap();
        db.drive(m).unwrap();
    };
    step(&mut db, b"k4", b"", 2);
    step(&mut db, b"k8", b"k4", 2);
    step(&mut db, b"kc", b"kc", 1);
    let on = |db: &Db, s: u16| db.ranges().iter().filter(|r| r.1 == s).count();
    assert_eq!(
        (on(&db, 0), on(&db, 1), on(&db, 2)),
        (1, 1, 2),
        "{:?}",
        db.ranges()
    );
    // Hold the manifest queue (a compaction's commit is parked), so both moves start, and
    // check the slots, before either commits.
    for i in 0..40 {
        db.put(&key(i), b"v");
    }
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    db.engine.park_manifest_commits(true);
    let _compact = db.engine.compact_pending(None).unwrap();
    for _ in 0..1_000 {
        if db.engine.manifest_commit_parked() {
            break;
        }
        db.step();
    }
    assert!(db.engine.manifest_commit_parked());
    let a = db.engine.move_tablet_pending(id, b"k8", 2).unwrap();
    let b = db.engine.move_tablet_pending(id, b"kc", 2).unwrap();
    for _ in 0..50 {
        db.step();
    }
    db.engine.park_manifest_commits(false);
    let ra = db.drive(a);
    let rb = db.drive(b);
    assert!(ra.is_ok() != rb.is_ok(), "{ra:?} {rb:?}");
    for e in [ra, rb].into_iter().filter_map(Result::err) {
        assert!(matches!(e, pigeonhole_engine::Error::Unsupported(_)), "{e}");
    }
    assert_eq!(on(&db, 2), 3, "{:?}", db.ranges());
    close(db);
}

#[test]
fn cold_tablets_spread_over_shards_consolidate_into_one() {
    // A table left in four small tablets alternating between two shards: neighbours never
    // share a shard, so merges alone never fire. Cold tablets move to their left
    // neighbour's shard and merge, until one tablet remains.
    let mut db = open(2, |o| o.balance_interval_nanos = u64::MAX);
    for i in 0..40 {
        db.put(&key(i), b"v");
    }
    lay_out(&mut db, &[b"k4", b"k8", b"kc"], &[0, 1, 0, 1]);
    for pass in 0..40 {
        let m = db.engine.balance_pending().unwrap();
        db.drive(m).unwrap();
        if db.ranges().len() == 1 {
            eprintln!("one tablet after {pass} passes");
            break;
        }
    }
    assert_eq!(db.ranges().len(), 1, "{:?}", db.ranges());
    assert_eq!(db.scan_rows().len(), 40);
    close(db);
}

#[test]
fn an_idle_shard_still_merges_cold_tablets() {
    // No message reaches the shard after the split: its balancer's own wakeup runs the
    // passes that merge the two cold halves.
    let mut db = open(1, |o| {
        o.balance_interval_nanos = 1_000_000;
        ticking(o);
    });
    for i in 0..40 {
        db.put(&key(i), b"v");
    }
    let id = db.table.id;
    let m = db.engine.split_tablet_pending(id, b"k8").unwrap();
    db.drive(m).unwrap();
    assert_eq!(db.ranges().len(), 2);
    for _ in 0..1_000 {
        if db.ranges().len() == 1 {
            break;
        }
        db.vfs.advance(100_000);
        db.step();
    }
    assert_eq!(db.ranges().len(), 1, "{:?}", db.ranges());
    assert_eq!(db.scan_rows().len(), 40);
    close(db);
}

#[test]
fn an_idle_shard_backs_off_its_balancer_and_a_write_resets_it() {
    // With tablet changes on by default every idle shard wakes for its balancer pass: a pass
    // that finds nothing to do after no writes doubles the interval, up to 10 s, and the next
    // write brings it back to the base interval.
    const BASE: u64 = 1_000_000;
    const CAP: u64 = 10_000_000_000;
    let mut db = open(1, |o| {
        o.balance_interval_nanos = BASE;
        ticking(o);
    });
    db.step();
    let deadline = |db: &Db| {
        db.shards[0]
            .next_deadline()
            .expect("a balancer pass is pending")
    };
    // Run each pass when it is due and record when the next one is due.
    let mut due = vec![deadline(&db)];
    for _ in 0..24 {
        let now = db.clock.monotonic_nanos();
        db.vfs.advance(due.last().unwrap().saturating_sub(now) + 1);
        db.step();
        due.push(deadline(&db));
    }
    let gaps: Vec<u64> = due.windows(2).map(|w| w[1] - w[0]).collect();
    assert!(gaps[0] <= 8 * BASE, "{gaps:?}");
    assert!(
        gaps.windows(2).all(|w| w[1] + BASE / 2 >= w[0]),
        "the interval shrank while idle: {gaps:?}"
    );
    let last = *gaps.last().unwrap();
    assert!(
        (CAP..CAP + BASE).contains(&last),
        "not capped at 10 s: {gaps:?}"
    );
    db.put(b"row", b"v");
    db.step();
    let now = db.clock.monotonic_nanos();
    let gap = deadline(&db).saturating_sub(now);
    assert!(gap <= BASE, "a write did not reset the interval: {gap}");
    close(db);
}

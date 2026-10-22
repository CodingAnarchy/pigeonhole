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
    CompactionStyle, Engine, EngineOptions, EngineShard, FamilyOptions, PendingMaintenance,
    ReadSpec, ScanSpec, TableInfo, ValueRef, WriteBatch,
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
    crash_sweep_with(seed, None);
}

/// [`crash_sweep`], with deferred I/O forced on or off (`None`: `PIGEONHOLE_DEFERRED_IO`).
fn crash_sweep_with(seed: u64, deferred_io: Option<bool>) {
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
    if let Some(d) = deferred_io {
        cfg.deferred_io = d;
    }
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
    // #520: with deferred I/O, a checkpoint was committed before the new stream files'
    // directory sync completed (D203); a power loss took the files' names, the reopen created
    // stream 0 afresh under the stale checkpoint, and a later crash lost an acknowledged
    // commit. No checkpoint names a stream before its creation is durable now (D211).
    crash_sweep_with(273, Some(true));
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
    open_on(&SimVfs::new(7), shards, tweak)
}

/// [`open`] on a given simulated disk (a reopen finds the table there).
fn open_on(vfs: &Arc<SimVfs>, shards: usize, tweak: impl FnOnce(&mut EngineOptions)) -> Db {
    let vfs = Arc::clone(vfs);
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
    engine.record_history(true);
    let table = engine.table("t").unwrap_or_else(|| {
        engine
            .create_table(
                "t",
                &[
                    ("f".into(), FamilyOptions::default()),
                    // Read by `overlap_txn` beside the rows it writes (D271).
                    ("g".into(), FamilyOptions::default()),
                ],
            )
            .expect("table")
    });
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
    // One table, one hot tablet on one shard: once the skew has held for 10 passes (D214),
    // the balancer splits it over the idle shards.
    for _ in 0..14 {
        for _ in 0..100 {
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

/// Row `i` of tablet `t` in [`tablets_on`]'s layout (split at `t01`, `t02`, ...).
fn tablet_row(t: usize, i: u64) -> Vec<u8> {
    format!("t{t:02}-{i:08}").into_bytes()
}

/// Splits the table into `n` tablets (`tablet_row(t, _)` lands in tablet `t`) and moves
/// tablet `t` to `owner(t)`.
fn tablets_on(db: &mut Db, n: usize, owner: impl Fn(usize) -> u16) {
    let id = db.table.id;
    for t in 1..n {
        let m = db
            .engine
            .split_tablet_pending(id, format!("t{t:02}").as_bytes())
            .unwrap();
        db.drive(m).unwrap();
    }
    for t in 0..n {
        let row = tablet_row(t, 0);
        let from = db
            .ranges()
            .into_iter()
            .find(|(_, _, start, end)| {
                row.as_slice() >= start.as_slice()
                    && end.as_deref().is_none_or(|e| row.as_slice() < e)
            })
            .unwrap()
            .1;
        if from != owner(t) {
            let m = db.engine.move_tablet_pending(id, &row, owner(t)).unwrap();
            db.drive(m).unwrap();
        }
    }
    assert_eq!(db.ranges().len(), n);
}

/// Tablets per shard.
fn tablets_per_shard(db: &Db, shards: usize) -> Vec<usize> {
    let mut out = vec![0; shards];
    for (_, s, ..) in db.ranges() {
        out[usize::from(s)] += 1;
    }
    out
}

/// One balancer pass, driven to completion, after `rows(t)` writes to each tablet `t`.
fn write_and_balance(db: &mut Db, n: usize, rows: impl Fn(usize) -> u64, written: &mut u64) {
    for t in 0..n {
        for _ in 0..rows(t) {
            db.put(&tablet_row(t, *written), b"v");
            *written += 1;
        }
    }
    let m = db.engine.balance_pending().unwrap();
    db.drive(m).unwrap();
}

/// A shard at about 3x the mean with several tablets keeps shedding them, one move per dwell,
/// until it is below 1.1x the mean, then the balancer stops (D214's hysteresis: a first
/// move that leaves the shard skewed does not end the balancing).
#[test]
fn a_skewed_shard_keeps_shedding_tablets_until_balanced() {
    let shards = 4;
    let mut db = open(shards, |o| {
        o.balance_interval_nanos = u64::MAX; // only the explicit balancer passes below
        o.balance_min_writes = 10;
    });
    // 12 equally written tablets: 9 on shard 0, one on each other shard.
    let n = 12;
    tablets_on(&mut db, n, |t| if t < 9 { 0 } else { (t - 8) as u16 });
    assert_eq!(tablets_per_shard(&db, shards), vec![9, 1, 1, 1]);
    let setup = tablet_changes(&db.engine);
    let mut written = 0;
    let mut passes = 0;
    while tablets_per_shard(&db, shards) != vec![3, 3, 3, 3] && passes < 300 {
        write_and_balance(&mut db, n, |_| 20, &mut written);
        passes += 1;
    }
    let after = tablet_changes(&db.engine);
    let changes = (after.0 - setup.0, after.1 - setup.1, after.2 - setup.2);
    assert_eq!(
        tablets_per_shard(&db, shards),
        vec![3, 3, 3, 3],
        "after {passes} passes, (splits, merges, moves) {changes:?}"
    );
    // Six moves, the first after the 10-pass persistence and one per dwell after it.
    assert_eq!(changes, (0, 0, 6), "after {passes} passes");
    // Balanced (every shard at the mean): nothing changes any more.
    for _ in 0..60 {
        write_and_balance(&mut db, n, |_| 20, &mut written);
    }
    assert_eq!(tablet_changes(&db.engine), after);
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// One phase of a closed-loop load, as the scaling gate's driver runs it (D204): tablet `t`
/// gets `demand[t]` new rows, each assigned at the start to the shard owning it, and every
/// shard writes 20 of its rows per balancer pass until it is done, then idles until every
/// shard is. Returns the passes the phase took.
fn closed_loop_phase(db: &mut Db, demand: &[u64], written: &mut u64) -> usize {
    let ranges = db.ranges();
    let owner = |row: &[u8]| {
        ranges
            .iter()
            .find(|(_, _, start, end)| {
                row >= start.as_slice() && end.as_deref().is_none_or(|e| row < e)
            })
            .unwrap()
            .1
    };
    let mut rows: Vec<Vec<Vec<u8>>> = vec![Vec::new(); 4];
    for (t, d) in demand.iter().enumerate() {
        for _ in 0..*d {
            let row = tablet_row(t, *written);
            *written += 1;
            rows[usize::from(owner(&row))].push(row);
        }
    }
    let mut passes = 0;
    while rows.iter().any(|r| !r.is_empty()) {
        for mine in &mut rows {
            for row in mine.drain(..mine.len().min(20)) {
                db.put(&row, b"v");
            }
        }
        let m = db.engine.balance_pending().unwrap();
        db.drive(m).unwrap();
        passes += 1;
    }
    passes
}

/// A closed-loop load settles (D224): every shard writes at the same rate while it has work,
/// so skew shows only in each phase's tail, after the lighter shards finish. There, the busy
/// shards read about 2x the mean only because the mean fell: that alone moves nothing. The
/// real imbalance (shards 0 and 1 hold tablets with twice the work) is fixed on the slow
/// averages, and then the placement stays put. Before, the busy shards shed tablets in every
/// phase's tail.
#[test]
fn a_closed_loop_load_settles_instead_of_churning_every_tail() {
    let shards = 4;
    let mut db = open(shards, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
    });
    // Two tablets per shard; tablets 0..4 (on shards 0 and 1) carry twice the work.
    let n = 8;
    tablets_on(&mut db, n, |t| (t / 2) as u16);
    let demand: Vec<u64> = (0..n).map(|t| if t < 4 { 600 } else { 300 }).collect();
    let mut written = 0;
    let mut changes = Vec::new();
    for _ in 0..12 {
        let before = tablet_changes(&db.engine);
        closed_loop_phase(&mut db, &demand, &mut written);
        let after = tablet_changes(&db.engine);
        changes.push(after.1 - before.1 + after.2 - before.2 + after.0 - before.0);
    }
    // Settled: nothing changes in the last six phases.
    assert!(
        changes[6..].iter().all(|c| *c == 0),
        "changes per phase {changes:?}"
    );
    // Settled where the shards share the work evenly: each owns at most 1.25x the mean of the
    // next phase's rows (the scaling gate's bound, D223).
    let ranges = db.ranges();
    let mut per = [0u64; 4];
    for (t, d) in demand.iter().enumerate() {
        for i in 0..*d {
            let row = tablet_row(t, written + i);
            let (_, s, ..) = ranges
                .iter()
                .find(|(_, _, start, end)| {
                    row.as_slice() >= start.as_slice()
                        && end.as_deref().is_none_or(|e| row.as_slice() < e)
                })
                .unwrap();
            per[usize::from(*s)] += 1;
        }
    }
    let mean = per.iter().sum::<u64>() as f64 / 4.0;
    assert!(
        per.iter().all(|p| *p as f64 <= 1.25 * mean),
        "rows per shard {per:?}, changes per phase {changes:?}"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// A standing imbalance still moves a tablet (D224): one shard at a steady 2x the mean from
/// the start. The mean never falls, so the skew counts as D214 has it.
#[test]
fn a_standing_imbalance_still_moves_a_tablet() {
    let shards = 4;
    let mut db = open(shards, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
    });
    // Shard 0 holds four tablets, the others one each, all written alike: shard 0 carries
    // 4/7 of the writes, about 2.3x the mean.
    let n = 7;
    tablets_on(&mut db, n, |t| if t < 4 { 0 } else { (t - 3) as u16 });
    let before = tablet_changes(&db.engine);
    let mut written = 0;
    let mut passes = 0;
    while tablet_changes(&db.engine) == before && passes < 100 {
        write_and_balance(&mut db, n, |_| 20, &mut written);
        passes += 1;
    }
    let after = tablet_changes(&db.engine);
    assert_eq!(after.2 - before.2, 1, "a move after {passes} passes");
    // The 10-pass persistence, as D214 has it: no later because of #582.
    assert!(passes <= 12, "moved after {passes} passes");
    assert_eq!(tablets_per_shard(&db, shards)[0], 3);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// Two skewed shards acting on the same pass never both shed into the same cold shard
/// (D224): the first claims it, and the second picks the next coldest. Before, both moved a
/// tablet to the coldest shard, which became the hottest.
#[test]
fn skewed_shards_never_shed_into_the_same_cold_shard_at_once() {
    let shards = 4;
    let mut db = open(shards, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
    });
    // Shards 0 and 1 hold three written tablets each; shard 2 one barely written, shard 3 one
    // written like the others.
    let n = 8;
    tablets_on(&mut db, n, |t| match t {
        0..3 => 0,
        3..6 => 1,
        6 => 2,
        _ => 3,
    });
    let rows = |t: usize| if t == 6 { 2 } else { 20 };
    let mut written = 0;
    let before = tablets_per_shard(&db, shards);
    let mut passes = 0;
    while tablets_per_shard(&db, shards) == before && passes < 100 {
        write_and_balance(&mut db, n, rows, &mut written);
        passes += 1;
    }
    assert_eq!(
        tablets_per_shard(&db, shards),
        vec![2, 2, 2, 2],
        "the first moves after {passes} passes"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// Three skewed shards with one tablet each and a single cold shard: the first splits into
/// the cold shard and claims it, and the others, finding every shard below the mean claimed,
/// wait for a later pass instead of splitting with nowhere to go (D224).
#[test]
fn skewed_shards_wait_when_every_cold_shard_is_claimed() {
    let shards = 4;
    let mut db = open(shards, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
    });
    let n = 4;
    tablets_on(&mut db, n, |t| t as u16);
    let rows = |t: usize| if t == 3 { 2 } else { 40 };
    let mut written = 0;
    let before = tablet_changes(&db.engine);
    let mut passes = 0;
    while tablet_changes(&db.engine) == before && passes < 100 {
        write_and_balance(&mut db, n, rows, &mut written);
        passes += 1;
    }
    let after = tablet_changes(&db.engine);
    assert_eq!(
        (after.0 - before.0, after.2 - before.2),
        (1, 0),
        "one split into the cold shard after {passes} passes"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// A packing move leaves a shard a skew change claimed alone (D270), as a skew change does
/// (D224): with shard 0, the only active shard once light writes pack, claimed for 400 of
/// its passes, shards 1-3 keep their tablets past the shrink to one active shard, and hand
/// them over once the claim runs out. Packing moves claim nothing themselves, so the three
/// then land together, as before.
#[test]
fn a_packing_move_waits_for_a_claimed_shard() {
    let mut db = spread_and_busy(0.1, true);
    assert!(db.engine.claim_shard(0, 400));
    let mut written = 0;
    let mut packed_k = None;
    for pass in 0..390 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        if packed_k.is_none() && db.engine.pack_ks()[0] == Some(1) {
            packed_k = Some(pass);
        }
        assert_eq!(
            tablets_per_shard(&db, 4),
            vec![1, 1, 1, 1],
            "a tablet moved into the claimed shard at pass {pass}"
        );
    }
    assert!(packed_k.is_some(), "the active shards never shrank to one");
    let mut passes = 390;
    while tablets_per_shard(&db, 4) != vec![4, 0, 0, 0] && passes < 600 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(
        tablets_per_shard(&db, 4),
        vec![4, 0, 0, 0],
        "after {passes} passes (shrank at pass {packed_k:?})"
    );
    assert!(
        passes <= 430,
        "packed {passes} passes in, long after the claim ran out"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// A skew change that waits on a compaction of its tablet claims no shard (D270): shard 0's
/// one tablet is hot, so it would split into the cold shards, but it is compacting (#102).
/// While it waits, no cold shard is claimed: a claim would hold every other shard's skew
/// change off it for nothing. Once the compaction ends, the split claims and starts.
#[test]
fn a_change_that_waits_on_a_compaction_claims_no_shard() {
    let shards = 4;
    let mut db = open(shards, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
    });
    let n = 4;
    tablets_on(&mut db, n, |t| t as u16);
    // `key` rows sort before `t01`: they are tablet 0's, on shard 0.
    hold_a_compaction(&mut db);
    let before = tablet_changes(&db.engine);
    let mut written = 0;
    for pass in 0..40 {
        write_and_balance(&mut db, n, |t| if t == 0 { 40 } else { 2 }, &mut written);
        assert_eq!(
            tablet_changes(&db.engine),
            before,
            "a change at pass {pass}"
        );
        for s in 1..shards {
            assert!(
                !db.engine.shard_claimed(s),
                "shard {s} claimed at pass {pass} by a change that waits"
            );
        }
    }
    db.engine.park_manifest_commits(false);
    for _ in 0..1_000 {
        db.step();
    }
    let mut passes = 0;
    while tablet_changes(&db.engine) == before && passes < 100 {
        write_and_balance(&mut db, n, |t| if t == 0 { 40 } else { 2 }, &mut written);
        passes += 1;
    }
    assert_ne!(
        tablet_changes(&db.engine),
        before,
        "the hot tablet never split after the compaction"
    );
    // And `hold_a_compaction`'s 20 rows.
    assert_eq!(db.scan_rows().len() as u64, written + 20);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// A burst of skew shorter than the persistence (10 passes, D214) moves and splits nothing:
/// before D214 one skewed pass split the hot shard's tablet over the others.
#[test]
fn a_short_burst_of_skew_changes_no_tablet() {
    let shards = 4;
    let mut db = open(shards, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
    });
    let n = 4;
    tablets_on(&mut db, n, |t| t as u16);
    let before = tablet_changes(&db.engine);
    let mut written = 0;
    for pass in 0..40 {
        // Passes 10..14: tablet 0 (shard 0) takes 3x the writes of the others.
        let burst = (10..14).contains(&pass);
        write_and_balance(
            &mut db,
            n,
            |t| if burst && t == 0 { 60 } else { 20 },
            &mut written,
        );
    }
    assert_eq!(tablet_changes(&db.engine), before);
    assert_eq!(tablets_per_shard(&db, shards), vec![1, 1, 1, 1]);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// Four shards, one written tablet each, every shard measuring itself `busy` (the test hook
/// stands in for the threads' busy time, which application-owned shards do not count).
fn spread_and_busy(busy: f64, pack: bool) -> Db {
    spread_and_busy_with(busy, pack, |_| {})
}

/// [`spread_and_busy`] with more options.
fn spread_and_busy_with(busy: f64, pack: bool, tweak: impl FnOnce(&mut EngineOptions)) -> Db {
    let shards = 4;
    let mut db = open(shards, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
        o.balance_pack = pack;
        tweak(o);
    });
    tablets_on(&mut db, shards, |t| t as u16);
    for s in 0..shards {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(busy));
    }
    db
}

/// Light writes (D217): four shards each 10% busy (B = 0.4) need one shard. After the 50-pass
/// shrink persistence, shards 1-3 hand their written tablets to shard 0, one per dwell, and
/// then nothing changes.
#[test]
fn light_writes_pack_tablets_onto_fewer_shards() {
    let mut db = spread_and_busy(0.1, true);
    let setup = tablet_changes(&db.engine);
    let mut written = 0;
    let mut passes = 0;
    while tablets_per_shard(&db, 4) != vec![4, 0, 0, 0] && passes < 600 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    let after = tablet_changes(&db.engine);
    let changes = (after.0 - setup.0, after.1 - setup.1, after.2 - setup.2);
    assert_eq!(
        tablets_per_shard(&db, 4),
        vec![4, 0, 0, 0],
        "after {passes} passes, (splits, merges, moves) {changes:?}"
    );
    assert_eq!(changes, (0, 0, 3), "three moves, one per tablet");
    assert!(
        passes >= 50,
        "packed after {passes} passes, before the persistence"
    );
    for _ in 0..60 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
    }
    assert_eq!(
        tablet_changes(&db.engine),
        after,
        "packed: nothing more changes"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// Every shard uses the same active shard count `k` (D217), at every balancer pass, across
/// a switch from light to heavy writes: packed onto shard 0, which then saturates, `k` grows
/// and shard 0 sheds tablets. Each shard used to keep its own `k` (its own hysteresis
/// counters over loads published at different passes), so shards disagreed while it changed
/// and, on the AX102 and locally, for good (one kept 16, others 1 to 3).
#[test]
fn every_shard_uses_the_same_active_shard_count_from_light_to_heavy_writes() {
    let mut db = spread_and_busy(0.1, true);
    let mut written = 0;
    let agree = |db: &Db, when: &str| {
        let ks = db.engine.pack_ks();
        assert!(
            ks.windows(2).all(|w| w[0] == w[1]),
            "{when}: the shards use different k: {ks:?}"
        );
        ks[0].expect("a balancer pass ran")
    };
    let mut passes = 0;
    while tablets_per_shard(&db, 4) != vec![4, 0, 0, 0] && passes < 600 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        agree(&db, &format!("light, pass {passes}"));
        passes += 1;
    }
    assert_eq!(tablets_per_shard(&db, 4), vec![4, 0, 0, 0]);
    assert_eq!(agree(&db, "packed"), 1);
    // Shard 0 now does all the work and saturates; the others idle.
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Busy, Some(0.95));
    for s in 1..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.0));
    }
    let mut grew = None;
    for pass in 0..120 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        let k = agree(&db, &format!("heavy, pass {pass}"));
        if k > 1 && grew.is_none() {
            grew = Some(pass);
        }
    }
    assert!(grew.is_some(), "k never grew from 1");
    let tablets = tablets_per_shard(&db, 4);
    assert!(tablets[0] < 4, "shard 0 shed no tablet: {tablets:?}");
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// One balancer pass of a durable load for D248 (deferred I/O on): every tablet commits 20
/// rows at `durability`, then the clock moves `in_flight` nanoseconds while the group syncs
/// are in flight and `idle` more once they are done, and the balancer runs.
fn sync_pass(
    db: &mut Db,
    n: usize,
    durability: Durability,
    in_flight: u64,
    idle: u64,
    written: &mut u64,
) {
    let f = db.table.families[0].id;
    let mut pending: Vec<_> = (0..n)
        .map(|t| {
            let mut wb = WriteBatch::new();
            for _ in 0..20 {
                wb.put(
                    db.table.id,
                    f,
                    &tablet_row(t, *written),
                    b"q",
                    None,
                    ValueRef::Bytes(b"v"),
                )
                .unwrap();
                *written += 1;
            }
            db.engine.submit(wb, Some(durability)).expect("submit")
        })
        .collect();
    db.step();
    db.vfs.advance(in_flight);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..10_000 {
        db.vfs.complete_all_io();
        db.step();
        pending.retain_mut(|pc| match Pin::new(pc).poll(&mut cx) {
            Poll::Ready(r) => {
                r.expect("commit");
                false
            }
            Poll::Pending => true,
        });
        if pending.is_empty() {
            break;
        }
    }
    assert!(pending.is_empty(), "commits did not finish");
    db.vfs.advance(idle);
    let mut m = db.engine.balance_pending().unwrap();
    for _ in 0..100_000 {
        if let Poll::Ready(r) = Pin::new(&mut m).poll(&mut cx) {
            return r.unwrap();
        }
        db.vfs.complete_all_io();
        db.step();
    }
    panic!("balancer pass did not finish");
}

/// D248: packing counts group-sync utilization. Four shards whose threads are only 5% busy
/// (B = 0.2, one shard's worth) but whose durable commits keep a group sync in flight
/// through the whole pass read about 1 busy each, so `k` stays 4 and
/// the table stays spread. Counting only the threads' busy time, they packed onto one shard
/// (perf287, AX102: 36% of ycsb-a's throughput at GroupSync).
#[test]
fn a_sync_bound_load_keeps_its_shards_active() {
    let mut db = spread_and_busy(0.05, true);
    db.vfs.set_deferred_io(true);
    let mut written = 0;
    for _ in 0..120 {
        sync_pass(
            &mut db,
            4,
            Durability::GroupSync,
            1_000_000,
            0,
            &mut written,
        );
    }
    assert_eq!(tablets_per_shard(&db, 4), vec![1, 1, 1, 1]);
    assert_eq!(db.engine.pack_ks(), vec![Some(4); 4]);
    let synced: Vec<u64> = db
        .engine
        .shard_stats()
        .iter()
        .map(|s| s.sync_busy_nanos)
        .collect();
    assert!(
        synced.iter().all(|&n| n >= 100 * 1_000_000),
        "every shard's syncs were in flight about 120 ms: {synced:?}"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.vfs.set_deferred_io(false);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// D248's other side: the same load packs as before (D217) when its syncs are not what the
/// shards spend their time on. At `Buffered` (no group syncs), and as a durable trickle
/// (syncs in flight 5% of the time), four shards 5% busy pack onto one.
#[test]
fn buffered_and_trickling_durable_loads_still_pack() {
    for (durability, idle) in [
        (Durability::Buffered, 0),
        (Durability::GroupSync, 19_000_000),
    ] {
        let mut db = spread_and_busy(0.05, true);
        db.vfs.set_deferred_io(true);
        let mut written = 0;
        let mut passes = 0;
        while tablets_per_shard(&db, 4) != vec![4, 0, 0, 0] && passes < 600 {
            sync_pass(&mut db, 4, durability, 1_000_000, idle, &mut written);
            passes += 1;
        }
        assert_eq!(
            tablets_per_shard(&db, 4),
            vec![4, 0, 0, 0],
            "{durability:?} did not pack"
        );
        assert_eq!(db.scan_rows().len() as u64, written);
        db.vfs.set_deferred_io(false);
        db.engine.close().unwrap();
        for _ in 0..8 {
            db.step();
        }
    }
}

/// D248 changes nothing for application-owned shards: their threads' busy time is not
/// counted, so packing does not act, and their group syncs do not start it. (A durable load
/// with syncs in flight a fifth of the time: counted, it would pack.)
#[test]
fn an_application_owned_durable_load_is_not_packed() {
    let mut db = spread_and_busy(0.05, true);
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, None);
    }
    db.vfs.set_deferred_io(true);
    let setup = tablet_changes(&db.engine);
    let mut written = 0;
    for _ in 0..150 {
        sync_pass(
            &mut db,
            4,
            Durability::GroupSync,
            1_000_000,
            4_000_000,
            &mut written,
        );
    }
    assert_eq!(tablet_changes(&db.engine), setup);
    assert_eq!(tablets_per_shard(&db, 4), vec![1, 1, 1, 1]);
    db.vfs.set_deferred_io(false);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// As `tablets_on`, for a table whose tablets several shards' slots must share: it splits from
/// the top and moves each new tablet to its owner at once, so no shard ever holds more
/// tablets than its memtable slots serve.
fn tablets_on_wide(db: &mut Db, n: usize, owner: impl Fn(usize) -> u16) {
    let id = db.table.id;
    let owner_of = |db: &Db, row: &[u8]| {
        db.ranges()
            .into_iter()
            .find(|(_, _, start, end)| {
                row >= start.as_slice() && end.as_deref().is_none_or(|e| row < e)
            })
            .unwrap()
            .1
    };
    for t in (1..n).rev() {
        let m = db
            .engine
            .split_tablet_pending(id, format!("t{t:02}").as_bytes())
            .unwrap();
        db.drive(m).unwrap();
        let row = tablet_row(t, 0);
        if owner_of(db, &row) != owner(t) {
            let m = db.engine.move_tablet_pending(id, &row, owner(t)).unwrap();
            db.drive(m).unwrap();
        }
    }
    if owner_of(db, b"") != owner(0) {
        let m = db.engine.move_tablet_pending(id, b"", owner(0)).unwrap();
        db.drive(m).unwrap();
    }
    assert_eq!(db.ranges().len(), n);
}

/// D251 (amends D214): a shard full of equally written small tablets (16 of a 4-family table
/// fill its 64 slots) beside a lightly written one. No tablet carries the eighth of the gap a
/// move used to need, and no split fits the full shard, so it never shed (the pre-release
/// gate's cross-shard-contention, 2026-10-14). The largest that fits moves now, one per dwell.
#[test]
fn a_full_shard_of_equal_small_tablets_sheds_them() {
    let mut db = open_wide(&SimVfs::new(7), 2, 4, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
        o.balance_pack = false;
    });
    tablets_on_wide(&mut db, 17, |t| if t < 16 { 0 } else { 1 });
    assert_eq!(tablets_per_shard(&db, 2), vec![16, 1]);
    let before = tablet_changes(&db.engine);
    let mut written = 0;
    let mut passes = 0;
    while tablets_per_shard(&db, 2)[0] > 11 && passes < 600 {
        write_and_balance(&mut db, 17, |t| if t < 16 { 20 } else { 50 }, &mut written);
        passes += 1;
    }
    let after = tablet_changes(&db.engine);
    let per_shard = tablets_per_shard(&db, 2);
    assert!(
        per_shard[0] <= 11,
        "the full shard shed {} tablets in {passes} passes: {per_shard:?}",
        16 - per_shard[0].min(16)
    );
    assert!(
        after.2 > before.2,
        "no tablet moved: {before:?} -> {after:?}"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// D251 (amends D217): packed onto two shards by the slot floor (20 tablets of a 4-family
/// table need 80 slots), then shard 0 saturates (0.98 busy) beside shard 1 at 0.31. `B / k`
/// is 0.645, inside D217's band (grow above 0.75, shrink below 0.5), so `k` stayed 2 for good.
/// A shard above `PACK_HOT` grows `k` now; and while it stays hot, `k` never shrinks back,
/// although one fewer shard would hold `B` under the target (no shrink-grow oscillation).
/// Once the load drops, `k` shrinks back to the floor as before.
#[test]
fn a_saturated_shard_grows_the_active_shards_holds_them_while_hot_and_lets_go_after() {
    let shards = 4;
    let mut db = open_wide(&SimVfs::new(7), shards, 4, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
        o.balance_pack = true;
    });
    let n = 20;
    tablets_on_wide(&mut db, n, |t| (t % shards) as u16);
    for s in 0..shards {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.05));
    }
    let mut written = 0;
    let mut passes = 0;
    while db.engine.pack_ks()[0] != Some(2) && passes < 600 {
        write_and_balance(&mut db, n, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(db.engine.pack_ks()[0], Some(2), "light writes did not pack");
    for (s, busy) in [0.98, 0.31, 0.0, 0.0].into_iter().enumerate() {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(busy));
    }
    let mut grew = None;
    for pass in 0..60 {
        write_and_balance(&mut db, n, |_| 20, &mut written);
        if db.engine.pack_ks()[0].is_some_and(|k| k > 2) {
            grew = Some(pass);
            break;
        }
    }
    assert!(grew.is_some(), "k stayed 2 beside a saturated shard");
    let mut k_max = 0;
    for pass in 0..300 {
        write_and_balance(&mut db, n, |_| 20, &mut written);
        let k = db.engine.pack_ks()[0].expect("decided");
        assert!(
            k >= k_max,
            "k shrank from {k_max} to {k} at pass {pass} while shard 0 was hot"
        );
        k_max = k_max.max(k);
    }
    // Once the load drops, `k` shrinks back to the slot floor as before D251.
    for s in 0..shards {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.05));
    }
    let mut passes = 0;
    while db.engine.pack_ks()[0] != Some(2) && passes < 600 {
        write_and_balance(&mut db, n, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(
        db.engine.pack_ks()[0],
        Some(2),
        "k did not shrink back after the load dropped"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// D251's flap guard (perf287's review): one hot tablet written on a single row, which a split
/// cannot divide and a move would only carry elsewhere, keeps its shard saturated. A hot
/// shard grows `k` once; with nothing shed since, it never grows again, and while it stays
/// hot `k` never shrinks: over 300 passes `k` changes at most once.
#[test]
fn an_unsplittable_hot_tablet_grows_k_at_most_once_and_does_not_flap() {
    let shards = 4;
    let mut db = open(shards, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
        o.balance_pack = true;
    });
    tablets_on(&mut db, shards, |t| t as u16);
    for s in 0..shards {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.05));
    }
    let mut written = 0;
    let mut passes = 0;
    while db.engine.pack_ks()[0] != Some(1) && passes < 600 {
        write_and_balance(&mut db, shards, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(db.engine.pack_ks()[0], Some(1), "light writes did not pack");
    // Shard 0's thread saturates on one row of tablet 0; nothing else is written.
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Busy, Some(0.95));
    let row = tablet_row(0, 0);
    let mut ks = vec![db.engine.pack_ks()[0].expect("decided")];
    for _ in 0..300 {
        for _ in 0..20 {
            db.put(&row, b"v");
        }
        let m = db.engine.balance_pending().unwrap();
        db.drive(m).unwrap();
        let k = db.engine.pack_ks()[0].expect("decided");
        if *ks.last().unwrap() != k {
            ks.push(k);
        }
    }
    assert!(ks.len() <= 2, "k changed more than once: {ks:?}");
    assert!(ks.windows(2).all(|w| w[1] > w[0]), "k shrank back: {ks:?}");
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// D251's shrink guard: spread over four shards, shard 0's thread saturates (0.95) on one
/// row and the others idle. B is about 0.95, so D217 alone would shrink `k` (B / (k - 1) is
/// 0.32), and the hot shard would then grow it back: shrink, grow, shrink. While a shard is
/// hot, `k` never shrinks.
#[test]
fn a_hot_shard_holds_off_the_shrink() {
    let mut db = spread_and_busy(0.0, true);
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Busy, Some(0.95));
    let row = tablet_row(0, 0);
    let mut ks = vec![];
    for _ in 0..300 {
        for _ in 0..20 {
            db.put(&row, b"v");
        }
        let m = db.engine.balance_pending().unwrap();
        db.drive(m).unwrap();
        let k = db.engine.pack_ks()[0].expect("decided");
        if ks.last() != Some(&k) {
            ks.push(k);
        }
    }
    assert_eq!(ks, vec![4], "k changed while shard 0 was hot");
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// D251 and D248 together (perf287's review): a single durable writer whose group syncs stay
/// in flight all the time reads busy (D248's sync term) though its thread is nearly idle.
/// That is not D251's hot shard (turn busy only): it neither grows `k` past D217's rule nor
/// holds off its shrink, and the store settles at `k` <= 2 without flapping.
#[test]
fn a_sync_saturated_single_writer_settles_at_two_shards_or_fewer() {
    // Turn busy 0.05 on the writer's shard, none elsewhere; its syncs in flight 95% of the
    // time: B is about 0.95, D217 wants 2, and the sync term alone is above `PACK_HOT`. Write
    // skew is off (D214 would split the writer's tablet over the shards): packing alone.
    let shards = 4;
    let mut db = open(shards, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
        o.balance_pack = true;
        o.balance_skew = 1e9;
    });
    tablets_on(&mut db, shards, |t| t as u16);
    for s in 0..shards {
        db.engine.set_shard_load(
            s,
            pigeonhole_engine::LoadTerm::Busy,
            Some(if s == 0 { 0.05 } else { 0.0 }),
        );
    }
    db.vfs.set_deferred_io(true);
    let mut written = 0;
    let mut ks = vec![];
    for _ in 0..600 {
        sync_pass(
            &mut db,
            1,
            Durability::GroupSync,
            950_000,
            50_000,
            &mut written,
        );
        let k = db.engine.pack_ks()[0].expect("decided");
        if ks.last() != Some(&k) {
            ks.push(k);
        }
    }
    assert!(*ks.last().unwrap() <= 2, "settled at k {ks:?}");
    assert!(ks.windows(2).all(|w| w[1] < w[0]), "k grew back: {ks:?}");
    db.vfs.set_deferred_io(false);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// Packing never strands tablets (D217): six tablets of a 20-family table need 120 memtable
/// slots, and a shard serves 64 (three tablets). Light writes (B = 0.3, one shard's worth)
/// pack them onto two shards, not three onto one shard and the rest left where they were.
#[test]
fn packing_keeps_as_many_shards_as_the_tablets_slots_need() {
    let shards = 6;
    let mut db = open_wide(&SimVfs::new(7), shards, 20, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
    });
    // One tablet per shard. Each split is moved off before the next (all six on one shard
    // would not fit).
    let id = db.table.id;
    let owner_of = |db: &Db, t: usize| {
        let row = tablet_row(t, 0);
        db.ranges()
            .into_iter()
            .find(|(_, _, start, end)| {
                row.as_slice() >= start.as_slice()
                    && end.as_deref().is_none_or(|e| row.as_slice() < e)
            })
            .unwrap()
            .1
    };
    for t in 1..shards {
        let m = db
            .engine
            .split_tablet_pending(id, format!("t{t:02}").as_bytes())
            .unwrap();
        db.drive(m).unwrap();
        for u in 0..=t {
            if owner_of(&db, u) != u as u16 {
                let m = db
                    .engine
                    .move_tablet_pending(id, &tablet_row(u, 0), u as u16)
                    .unwrap();
                db.drive(m).unwrap();
            }
        }
    }
    assert_eq!(tablets_per_shard(&db, shards), vec![1; shards]);
    for s in 0..shards {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.05));
    }
    let mut written = 0;
    let mut passes = 0;
    let packed = |db: &Db| tablets_per_shard(db, shards)[2..].iter().all(|&n| n == 0);
    while !packed(&db) && passes < 600 {
        write_and_balance(&mut db, shards, |_| 20, &mut written);
        passes += 1;
    }
    assert!(
        packed(&db),
        "after {passes} passes: {:?}",
        tablets_per_shard(&db, shards)
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// Once packed, a shard that turns 90% busy (B / k = 0.9, past 0.75 for 10 passes) brings a
/// second shard back, and write skew spreads the tablets over the two active shards only.
#[test]
fn packed_tablets_spread_again_when_the_shard_turns_busy() {
    let mut db = spread_and_busy(0.1, true);
    let mut written = 0;
    let mut passes = 0;
    while tablets_per_shard(&db, 4) != vec![4, 0, 0, 0] && passes < 600 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(tablets_per_shard(&db, 4), vec![4, 0, 0, 0]);
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Busy, Some(0.9));
    for s in 1..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.0));
    }
    let packed = tablet_changes(&db.engine);
    passes = 0;
    while tablets_per_shard(&db, 4) != vec![2, 2, 0, 0] && passes < 300 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    let after = tablet_changes(&db.engine);
    assert_eq!(
        tablets_per_shard(&db, 4),
        vec![2, 2, 0, 0],
        "after {passes} passes, changes {:?}",
        (after.0 - packed.0, after.1 - packed.1, after.2 - packed.2)
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// One pass of a write load that overflows shard 0's arena (D255's test): a batch per
/// tablet, each past a quarter of the 4 MiB arena, submitted together with the simulator's
/// I/O deferred, so the first flush stays in flight and the later batches wait for room;
/// the clock then advances `stalled` while they wait, the I/O completes, every commit
/// finishes, and the balancer runs a pass. Every tablet gets the same writes, so only the
/// placement decides which shard stalls.
fn stall_pass(db: &mut Db, _n: usize, stalled: u64, written: &mut u64) {
    stall_pass_with(db, |_| 20, 64, stalled, false, written);
}

/// [`stall_pass`] with `rows(t)` rows of `value_kib` KiB per tablet `t` (0: none). With
/// `measure_open` the balancer pass runs while the commits still wait for room (the I/O
/// completes only after it), and the shard's published stall fraction after that pass is
/// returned.
fn stall_pass_with(
    db: &mut Db,
    rows: impl Fn(usize) -> usize,
    value_kib: usize,
    stalled: u64,
    measure_open: bool,
    written: &mut u64,
) -> Option<f64> {
    let n = 4;
    let f = db.table.families[0].id;
    let value = vec![7u8; value_kib << 10];
    let mut pending: Vec<_> = (0..n)
        .filter(|&t| rows(t) > 0)
        .map(|t| {
            let mut wb = WriteBatch::new();
            for _ in 0..rows(t) {
                wb.put(
                    db.table.id,
                    f,
                    &tablet_row(t, *written),
                    b"q",
                    None,
                    ValueRef::Bytes(&value),
                )
                .unwrap();
                *written += 1;
            }
            db.engine
                .submit(wb, Some(Durability::Buffered))
                .expect("submit")
        })
        .collect();
    // The shard admits what fits, freezes and starts a flush (its I/O stays in flight), and
    // the rest wait for room.
    for _ in 0..20 {
        db.vfs.advance(2_000_000);
        db.step();
    }
    db.vfs.advance(stalled);
    let mut cx = Context::from_waker(Waker::noop());
    let mut measured = None;
    let m = if measure_open {
        // The wait stays open through 26 balancer passes before the reading: nothing has
        // completed, and the event that began the wait is well outside the stall window,
        // so only the open span can keep the shard reading stalled.
        let mut open = None;
        for _ in 0..26 {
            let mut m = db.engine.balance_pending().unwrap();
            let mut done = false;
            for _ in 0..50 {
                db.vfs.advance(2_000_000);
                db.step();
                if let Poll::Ready(r) = Pin::new(&mut m).poll(&mut cx) {
                    r.unwrap();
                    done = true;
                    break;
                }
            }
            if !done {
                open = Some(m);
            }
        }
        measured = db.engine.shard_stall_busy(0);
        open
    } else {
        None
    };
    for _ in 0..10_000 {
        // The clock keeps moving while the flushes complete, as a real one would, past the
        // room wait's re-check interval each step: a wait for room on a stopped clock with
        // nothing in flight is refused (issue #70).
        db.vfs.advance(2_000_000);
        db.vfs.complete_all_io();
        db.step();
        pending.retain_mut(|pc| match Pin::new(pc).poll(&mut cx) {
            Poll::Ready(r) => {
                r.expect("commit");
                false
            }
            Poll::Pending => true,
        });
        if pending.is_empty() {
            break;
        }
    }
    assert!(pending.is_empty(), "commits did not finish");
    let mut m = m.unwrap_or_else(|| db.engine.balance_pending().unwrap());
    for _ in 0..100_000 {
        if let Poll::Ready(r) = Pin::new(&mut m).poll(&mut cx) {
            r.unwrap();
            return measured;
        }
        db.vfs.advance(2_000_000);
        db.vfs.complete_all_io();
        db.step();
    }
    panic!("the balancer pass did not finish")
}

/// The shard owning tablet `t`.
fn owner_of(db: &Db, t: usize) -> u16 {
    let row = tablet_row(t, 0);
    db.ranges()
        .into_iter()
        .find(|(_, _, start, end)| {
            row.as_slice() >= start.as_slice() && end.as_deref().is_none_or(|e| row.as_slice() < e)
        })
        .expect("a tablet holds the row")
        .1
}

/// Once packed, a shard whose thread reads idle but whose writers wait for arena room
/// reads hot all the same (D255: the stall time counts as busy, beside the turn and sync
/// terms), so a second shard comes back and the tablets spread over the two. With flush and
/// compaction on dedicated threads (D252) a loaded shard's turns are cheap: counting only
/// them kept whole tables on one shard, whose arena then forced tiny flushes (45x write
/// amplification on a bulk load).
#[test]
fn packed_tablets_spread_again_when_the_shard_stalls_for_room() {
    // No compaction: a job keeps the view it started with, whose readers pin the chunks of
    // every memtable flushed meanwhile, and on the simulator's stopped clock a wait for
    // room with nothing but that in flight is refused (issue #70).
    let mut db = spread_and_busy_with(0.1, true, |o| o.compaction.l0_trigger = 1 << 20);
    let mut written = 0;
    let mut passes = 0;
    while tablets_per_shard(&db, 4) != vec![4, 0, 0, 0] && passes < 600 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(tablets_per_shard(&db, 4), vec![4, 0, 0, 0]);
    // Every thread idle: only the stall can make a shard hot.
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.0));
    }
    db.vfs.set_deferred_io(true);
    let packed = tablet_changes(&db.engine);
    let stalls_before = db.engine.metrics().stalls.0;
    passes = 0;
    let spread = |db: &Db| {
        let per = tablets_per_shard(db, 4);
        per[0] < 4 && per.iter().filter(|&&n| n > 0).count() >= 2
    };
    while !spread(&db) && passes < 300 {
        stall_pass(&mut db, 4, 1_000_000_000, &mut written);
        passes += 1;
    }
    let after = tablet_changes(&db.engine);
    assert!(
        spread(&db),
        "after {passes} passes shard 0 still holds every tablet: {:?}, changes {:?}",
        tablets_per_shard(&db, 4),
        (after.0 - packed.0, after.1 - packed.1, after.2 - packed.2)
    );
    assert!(
        db.engine.pack_ks()[0].is_some_and(|k| k >= 2),
        "the active shards stayed at {:?}",
        db.engine.pack_ks()[0]
    );
    assert!(
        db.engine.metrics().stalls.0 > stalls_before,
        "the load never waited for room"
    );
    db.vfs.complete_all_io();
    db.vfs.set_deferred_io(false);
    db.settle();
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// Packs four tablets onto shard 0 (threads 10% busy), with compaction off: a job keeps the
/// view it started with, whose readers pin the chunks of every memtable flushed meanwhile.
fn packed_without_compaction() -> (Db, u64) {
    let mut db = spread_and_busy_with(0.1, true, |o| o.compaction.l0_trigger = 1 << 20);
    let mut written = 0;
    let mut passes = 0;
    while tablets_per_shard(&db, 4) != vec![4, 0, 0, 0] && passes < 600 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(tablets_per_shard(&db, 4), vec![4, 0, 0, 0]);
    (db, written)
}

/// The hot test alone (D251's rule with D255's term): two active shards whose busy sum is
/// below the spread threshold, one of them with idle turns but stalling writers, grow the
/// active shards by one. Shard 0 gets to 80% busy, which grows the active shards to two by
/// the busy sum (not hot: under 90%) and spreads the tablets; then its thread reads idle and
/// shard 1 30% busy, so the sum over the two is 0.65, under the 0.75 spread threshold, and
/// only shard 0's writers stalling (its tablets get the heavy writes) can grow `k` further.
#[test]
fn a_stalling_shard_is_hot_even_when_the_busy_sum_is_low() {
    let (mut db, mut written) = packed_without_compaction();
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Busy, Some(0.8));
    for s in 1..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.0));
    }
    let mut passes = 0;
    while tablets_per_shard(&db, 4) != vec![2, 2, 0, 0] && passes < 300 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(
        tablets_per_shard(&db, 4),
        vec![2, 2, 0, 0],
        "after {passes} passes"
    );
    assert_eq!(db.engine.pack_ks()[0], Some(2));
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Busy, Some(0.0));
    db.engine
        .set_shard_load(1, pigeonhole_engine::LoadTerm::Busy, Some(0.3));
    db.vfs.set_deferred_io(true);
    passes = 0;
    while db.engine.pack_ks()[0] == Some(2) && passes < 100 {
        // The heavy writes follow shard 0's tablets, whichever the balancer leaves it: only
        // shard 0 ever stalls.
        let heavy: Vec<usize> = (0..4).filter(|&t| owner_of(&db, t) == 0).collect();
        assert!(
            !heavy.is_empty(),
            "shard 0 holds no tablet after {passes} passes"
        );
        stall_pass_with(
            &mut db,
            |t| if heavy.contains(&t) { 40 } else { 1 },
            64,
            1_000_000_000,
            false,
            &mut written,
        );
        passes += 1;
    }
    assert!(
        db.engine.pack_ks()[0].is_some_and(|k| k >= 3),
        "after {passes} passes the active shards are {:?}: the stalling shard did not read hot",
        db.engine.pack_ks()[0]
    );
    db.vfs.complete_all_io();
    db.vfs.set_deferred_io(false);
    db.settle();
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// A wait for room still open when the balancer measures counts (D255): the pass taken
/// while the commits wait reads a stall fraction above zero. Crediting the wait only once it
/// ends would read zero here, a pass late.
#[test]
fn a_stall_still_open_at_the_balancer_pass_counts() {
    let (mut db, mut written) = packed_without_compaction();
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.0));
    }
    db.vfs.set_deferred_io(true);
    assert_eq!(db.engine.shard_stall_busy(0), Some(0.0));
    let measured = stall_pass_with(&mut db, |_| 20, 64, 1_000_000_000, true, &mut written);
    assert!(
        measured.is_some_and(|f| f > 0.5),
        "the open wait was not counted: {measured:?}"
    );
    db.vfs.complete_all_io();
    db.vfs.set_deferred_io(false);
    db.settle();
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// Once a stall-driven growth has spread the tablets and the stalls stop, the active shards
/// hold while the store keeps writing (D255's floor): the shards it spread to run cheap
/// turns, so by the busy sum alone they would shrink after 50 passes, the stalls would
/// return, and `k` would swing between the two.
#[test]
fn the_active_shards_a_stall_spread_to_hold_while_the_writes_go_on() {
    let (mut db, mut written) = packed_without_compaction();
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.05));
    }
    db.vfs.set_deferred_io(true);
    let mut passes = 0;
    while !db.engine.pack_ks()[0].is_some_and(|k| k >= 2) && passes < 300 {
        stall_pass(&mut db, 4, 1_000_000_000, &mut written);
        passes += 1;
    }
    let k = db.engine.pack_ks()[0].expect("decided");
    assert!(k >= 2, "after {passes} passes");
    // The same row rate with 1 KiB rows (20 KiB per tablet per pass: no shard waits for
    // room, on one shard or four), so the floor's write-rate condition holds while nothing
    // is hot; every thread reads 5% busy.
    let stalls = db.engine.metrics().stalls.0;
    let mut ks = Vec::new();
    for _ in 0..120 {
        stall_pass_with(&mut db, |_| 20, 1, 1_000_000_000, false, &mut written);
        ks.push(db.engine.pack_ks()[0].expect("decided"));
    }
    assert!(
        ks.iter().all(|&now| now >= 2),
        "the active shards shrank back: {ks:?} (stalls {} -> {})",
        stalls,
        db.engine.metrics().stalls.0
    );
    db.vfs.complete_all_io();
    db.vfs.set_deferred_io(false);
    db.settle();
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// L0 pacing with its paced commits taken away by a tablet change ends its stall span
/// (D255, ctr274's review of #730): with the span left open the shard would read stalled,
/// and so hot, with no writer at all. Two tablets on one shard: a held compaction keeps
/// tablet 0's L0 at the pacing depth (2 × the trigger of 2), which paces every commit of the
/// shard; the ninth commit to tablet 1 (the bucket holds 8 tokens) is paced; a move of
/// tablet 1 parks its commits off this shard's queue while tablet 0 keeps the pacing score.
#[test]
fn pacing_ends_when_a_tablet_change_takes_its_commits() {
    let mut db = open(2, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.compaction.l0_trigger = 2;
        o.memtable_freeze_bytes = 32 << 10;
        o.tablet_split_bytes = u64::MAX;
    });
    let from = 0u16;
    let to = 1u16;
    tablets_on(&mut db, 2, |_| from);
    db.engine.hold_compactions(true);
    let value = vec![0x5A; 200];
    let mut i = 0u64;
    // Four L0 files on tablet 0 (one memtable each: 40 rows of 200 bytes, about 20 KiB with
    // their keys, stay under the 32 KiB freeze size): the pacing depth, with the compaction
    // held. From here every commit on the shard is paced, and a blocking put on a stopped
    // clock would never get its token, so the rest is submitted and stepped by hand.
    for _ in 0..4 {
        for _ in 0..40 {
            db.put(&tablet_row(0, i), &value);
            i += 1;
        }
        let m = db.engine.flush_pending().unwrap();
        db.drive(m).unwrap();
    }
    assert!(db.engine.compaction_held(), "no compaction started");
    // Paced commits to tablet 1: after the bucket's eight tokens a commit stays pending on
    // a clock that does not move.
    let f = db.table.families[0].id;
    let mut cx = Context::from_waker(Waker::noop());
    let mut pending = Vec::new();
    for _ in 0..12 {
        let mut wb = WriteBatch::new();
        wb.put(
            db.table.id,
            f,
            &tablet_row(1, i),
            b"q",
            None,
            ValueRef::Bytes(&value),
        )
        .unwrap();
        i += 1;
        pending.push(db.engine.submit(wb, Some(Durability::Buffered)).unwrap());
        for _ in 0..3 {
            db.step();
        }
    }
    pending.retain_mut(|pc| Pin::new(pc).poll(&mut cx).is_pending());
    assert!(!pending.is_empty(), "no commit was paced");
    assert!(db.engine.metrics().stalls.0 > 0, "no stall was counted");
    // Tablet 1 moves to the other shard: its paced commits leave this shard's queue while
    // tablet 0's held compaction keeps the pacing score at 1.
    let m = db
        .engine
        .move_tablet_pending(db.table.id, &tablet_row(1, 0), to)
        .unwrap();
    db.drive(m).unwrap();
    for pc in &mut pending {
        for _ in 0..100_000 {
            if let Poll::Ready(r) = Pin::new(&mut *pc).poll(&mut cx) {
                r.expect("commit");
                break;
            }
            db.vfs.advance(2_000_000);
            db.step();
        }
    }
    assert_eq!(tablets_per_shard(&db, 2), vec![1, 1]);
    db.engine.hold_compactions(false);
    db.settle();
    // Ten balancer passes a second apart with no writer on this shard: its stall fraction
    // must fall to nothing. Left open, the pacing span would read as a full stall.
    for _ in 0..10 {
        db.vfs.advance(1_000_000_000);
        let m = db.engine.balance_pending().unwrap();
        db.drive(m).unwrap();
    }
    let frac = db.engine.shard_stall_busy(usize::from(from));
    assert!(
        frac.is_some_and(|f| f < 0.1),
        "shard {from} still reads stalled after its commits moved away: {frac:?}"
    );
    assert_eq!(db.scan_rows().len() as u64, i);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// No shrink while the load is still ramping (D255, #734): four shards settled at 30% busy
/// (past the first-shrink window) would pack onto three after 50 passes, but with the rows
/// per pass rising the fast write average stays above the slow one and the active shards
/// hold; once the rows hold steady long enough for the slow average to catch up, the shrink
/// comes.
#[test]
fn no_shrink_while_the_load_is_ramping() {
    let (mut db, mut written) = spread_and_settled(0.3);
    for pass in 0..80u64 {
        write_and_balance(&mut db, 4, |_| 20 + 4 * pass, &mut written);
        let k = db.engine.pack_ks()[0];
        assert!(
            k.is_none_or(|k| k == 4),
            "the active shards shrank to {k:?} at pass {pass} of the ramp"
        );
    }
    let mut passes = 0;
    while db.engine.pack_ks()[0].is_none_or(|k| k == 4) && passes < 600 {
        write_and_balance(&mut db, 4, |_| 340, &mut written);
        passes += 1;
    }
    assert!(
        db.engine.pack_ks()[0].is_some_and(|k| k < 4),
        "the active shards never shrank once the load held steady ({passes} passes)"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// Packs four tablets onto shard 0 (threads 10% busy): the start of the D256 tests.
fn packed_onto_shard_0() -> (Db, u64) {
    let mut db = spread_and_busy(0.1, true);
    let mut written = 0;
    let mut passes = 0;
    while tablets_per_shard(&db, 4) != vec![4, 0, 0, 0] && passes < 600 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(tablets_per_shard(&db, 4), vec![4, 0, 0, 0]);
    (db, written)
}

/// The pool's work for a shard counts toward the busy sum (D256): packed onto shard 0 with
/// every thread 10% busy, a pool term of 1.5 on shard 0 (a flush and a compaction for it
/// on two pool threads at once) sizes the active shards to three or more within 100
/// passes; without it the sum reads 0.1 and the store stays packed.
#[test]
fn pool_work_counts_toward_the_busy_sum() {
    let (mut db, mut written) = packed_onto_shard_0();
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Pool, Some(1.5));
    let mut passes = 0;
    while !db.engine.pack_ks()[0].is_some_and(|k| k >= 3) && passes < 100 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert!(
        db.engine.pack_ks()[0].is_some_and(|k| k >= 3),
        "the pool term did not grow the active shards in {passes} passes: {:?}",
        db.engine.pack_ks()[0]
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.close();
}

/// Concurrent commits hold the active shards (D257): four shards settled at 60% busy, then
/// each thread 5% in turns with three commits in flight over the store (0.75 at each
/// shard: `round(3 / 0.75)` = 4), hold at four through 150 passes; with the commits in
/// flight gone the same turns pack them. Without the concurrency term the busy sum reads
/// 0.2 and they pack after 50 passes.
#[test]
fn concurrent_commits_hold_the_active_shards() {
    let (mut db, mut written) = spread_and_settled(0.05);
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Inflight, Some(0.75));
    }
    for pass in 0..150 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        let k = db.engine.pack_ks()[0];
        assert!(
            k.is_none_or(|k| k == 4),
            "the active shards shrank to {k:?} at pass {pass} with three commits in flight"
        );
    }
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Inflight, Some(0.0));
    }
    let mut passes = 0;
    while db.engine.pack_ks()[0].is_none_or(|k| k == 4) && passes < 300 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert!(
        db.engine.pack_ks()[0].is_some_and(|k| k < 4),
        "the store never packed once the commits in flight stopped ({passes} passes)"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.close();
}

/// One client's worth of commits in flight packs to one shard however the threads spin
/// (D257, the reverse of D256's kept-awake rule): four shards settled, each thread 5% in
/// turns with a lone writer's commits in flight spread over them (0.25 each, 1.0 in all:
/// `round(1 / 0.75)` = 1, where a ceiling would keep two), pack onto one shard. The spin
/// the writer causes on each shard is not measured by the override: on the AX102 it read
/// 0.5 per shard at one client over 13 shards, which D256 counted as 6.5 shards' worth.
#[test]
fn a_lone_writers_commits_in_flight_pack_to_one_shard() {
    let (mut db, mut written) = spread_and_settled(0.05);
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Inflight, Some(0.25));
    }
    let mut passes = 0;
    while db.engine.pack_ks()[0] != Some(1) && passes < 300 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(
        db.engine.pack_ks()[0],
        Some(1),
        "one writer's commits in flight kept more than one shard active after {passes} passes"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.close();
}

/// Commits in flight grow a packed store (D257): packed onto shard 0 with every thread 10%
/// busy, three commits in flight at shard 0 (three clients' worth; the busy sum alone
/// reads 0.1 and wants one shard) take the active shards to `round(3 / 0.75)` = 4 within
/// 100 passes. Without the `k < k_c` growth the store stays packed.
#[test]
fn concurrent_commits_grow_a_packed_store() {
    let (mut db, mut written) = packed_onto_shard_0();
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Inflight, Some(3.0));
    let mut passes = 0;
    while db.engine.pack_ks()[0] != Some(4) && passes < 100 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(
        db.engine.pack_ks()[0],
        Some(4),
        "three commits in flight did not spread the store in {passes} passes"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.close();
}

/// A growth publishes where it is heading while its passes accrue (D242's amendment, the
/// bench's warm-up): packed onto one shard, three commits in flight call for four, and
/// `pack_target` reads a target above 1 on the passes before the growth decides (rising
/// with the concurrency's moving average), then (4, 4).
#[test]
fn a_pending_growth_publishes_the_count_it_is_heading_for() {
    let (mut db, mut written) = packed_onto_shard_0();
    assert_eq!(db.engine.pack_target(), Some((1, 1)));
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Inflight, Some(3.0));
    let mut pending = 0;
    let mut passes = 0;
    while db.engine.pack_ks()[0] != Some(4) && passes < 100 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
        // The target rises with the concurrency's moving average: 2, 3, then 4.
        if db
            .engine
            .pack_target()
            .is_some_and(|(k, target)| k == 1 && target > 1)
        {
            pending += 1;
        }
    }
    assert_eq!(
        db.engine.pack_ks()[0],
        Some(4),
        "no growth in {passes} passes"
    );
    assert!(
        pending >= 5,
        "the pending growth published its target on {pending} of {passes} passes"
    );
    assert_eq!(db.engine.pack_target(), Some((4, 4)));
    assert_eq!(db.scan_rows().len() as u64, written);
    db.close();
}

/// A concurrency-driven first shrink does not wait for D255's window, its 50 passes or the
/// write ramp (D257): four shards spread at 10% busy with one client's worth of commits in
/// flight (0.25 each) pack within 80 passes (50 of agreement and the 10-pass count), where the
/// busy sum alone holds them to 300
/// (`a_light_store_packs_after_the_first_window`) and the harness's write ramp (the slow
/// average from 0) reads as ramping for about 160: once the fast and the slow concurrency
/// have agreed on one shard for 50 passes, the growth count decides. One client's gate row
/// measures ten seconds after its load and a 2 s warm-up; each of the three put the pack
/// inside the measured phase.
#[test]
fn a_concurrency_driven_first_shrink_does_not_wait_for_the_window() {
    let mut db = spread_and_busy(0.1, true);
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Inflight, Some(0.25));
    }
    let mut written = 0;
    let mut passes = 0;
    while db.engine.pack_ks()[0].is_none_or(|k| k == 4) && passes < 80 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert!(
        db.engine.pack_ks()[0].is_some_and(|k| k < 4),
        "one client's commits in flight waited for the window ({passes} passes)"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.close();
}

/// A concurrency dip after a heavy phase still waits for the window (D257 keeps D255's
/// guard): four shards spread at 10% busy with three commits in flight (0.75 each) for 200
/// passes, then 0.4 in all for 50 passes, hold at four: the slow concurrency still reads
/// the heavy phase (about 2 after 50 passes), so the fast one's call for one shard is a
/// dip, not a settled load. With the dip test on the fast concurrency alone they pack
/// after 50.
#[test]
fn a_concurrency_dip_after_a_heavy_phase_does_not_pack_an_unpacked_store() {
    let mut db = spread_and_busy(0.1, true);
    let mut written = 0;
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Inflight, Some(0.75));
    }
    for _ in 0..200 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
    }
    assert!(db.engine.pack_ks()[0].is_none_or(|k| k == 4));
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Inflight, Some(0.1));
    }
    for pass in 0..50 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        let k = db.engine.pack_ks()[0];
        assert!(
            k.is_none_or(|k| k == 4),
            "the active shards shrank to {k:?} at pass {pass} of the dip"
        );
    }
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Inflight, Some(0.75));
    }
    for pass in 0..50 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        let k = db.engine.pack_ks()[0];
        assert!(
            k.is_none_or(|k| k == 4),
            "the active shards shrank to {k:?} at pass {pass} after the dip"
        );
    }
    assert_eq!(db.scan_rows().len() as u64, written);
    db.close();
}

/// A stall-driven growth is the capacity side and is not held to the concurrency (D257
/// with D255): packed onto shard 0, one writer's worth of commits in flight (0.9, which
/// calls for one shard) whose writers stall grows the active shards past one all the same:
/// a single-threaded loader waiting for arena room still gets more arenas.
#[test]
fn a_stall_driven_growth_is_not_held_to_the_commits_in_flight() {
    let (mut db, mut written) = packed_with_shard_0_stalled();
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Inflight, Some(0.9));
    let mut passes = 0;
    while !db.engine.pack_ks()[0].is_some_and(|k| k >= 2) && passes < 120 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert!(
        db.engine.pack_ks()[0].is_some_and(|k| k >= 2),
        "a stalling shard with one writer's commits in flight did not grow the active shards in {passes} passes: {:?}",
        db.engine.pack_ks()[0]
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.close();
}

/// The count a thread-saturated growth (D251) reached is a floor while the store keeps
/// writing at least half its rate then (D256, as D255's stall floor): packed onto shard 0,
/// its thread saturated grows the active shards; with every thread 5% busy after that they
/// hold for 100 passes, and at a quarter of the rate they pack again. Without the floor
/// the busy sum packs them after 50.
#[test]
fn a_saturated_growth_holds_while_the_writes_go_on() {
    let (mut db, mut written) = packed_onto_shard_0();
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Busy, Some(1.0));
    let mut passes = 0;
    while !db.engine.pack_ks()[0].is_some_and(|k| k >= 2) && passes < 120 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    let k = db.engine.pack_ks()[0].expect("decided");
    assert!(k >= 2, "after {passes} passes");
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.05));
    }
    for pass in 0..100 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        let now = db.engine.pack_ks()[0].expect("decided");
        assert!(now >= k, "the floor gave way at pass {pass}: {now} < {k}");
    }
    passes = 0;
    while db.engine.pack_ks()[0] != Some(1) && passes < 400 {
        write_and_balance(&mut db, 4, |_| 5, &mut written);
        passes += 1;
    }
    assert_eq!(
        db.engine.pack_ks()[0],
        Some(1),
        "after {passes} passes at a quarter of the rate, tablets {:?}",
        tablets_per_shard(&db, 4)
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.close();
}

/// Packs four tablets onto shard 0 (threads 10% busy) and then makes shard 0 read fully
/// stalled through the test hook, every thread idle.
fn packed_with_shard_0_stalled() -> (Db, u64) {
    let mut db = spread_and_busy(0.1, true);
    let mut written = 0;
    let mut passes = 0;
    while tablets_per_shard(&db, 4) != vec![4, 0, 0, 0] && passes < 600 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(tablets_per_shard(&db, 4), vec![4, 0, 0, 0]);
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.0));
    }
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Stalled, Some(1.0));
    (db, written)
}

/// As [`packed_with_shard_0_stalled`], with 650 light passes between the packing and the
/// stall, past the window in which a stall undoes the shrink: the growth from there is the
/// rules' own, one shard at a time.
fn packed_long_ago_with_shard_0_stalled() -> (Db, u64) {
    let mut db = spread_and_busy(0.1, true);
    let mut written = 0;
    let mut passes = 0;
    while tablets_per_shard(&db, 4) != vec![4, 0, 0, 0] && passes < 600 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(tablets_per_shard(&db, 4), vec![4, 0, 0, 0]);
    for _ in 0..650 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
    }
    assert_eq!(db.engine.pack_ks()[0], Some(1));
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.0));
    }
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Stalled, Some(1.0));
    (db, written)
}

/// A stall-driven hot shard grows the active shards again while it still has tablets to
/// shed (D255): its stall fraction stays at 1 however much it sheds, so D251's cooled check
/// alone would hold it to one growth, at two shards (the busy sum over two is 0.5, under
/// the spread threshold).
#[test]
fn a_stall_driven_growth_repeats_while_the_shard_has_tablets_to_shed() {
    let (mut db, mut written) = packed_long_ago_with_shard_0_stalled();
    let mut passes = 0;
    while !db.engine.pack_ks()[0].is_some_and(|k| k >= 3) && passes < 120 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert!(
        db.engine.pack_ks()[0].is_some_and(|k| k >= 3),
        "after {passes} passes the active shards are {:?}, tablets {:?}",
        db.engine.pack_ks()[0],
        tablets_per_shard(&db, 4)
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// The floor a stall-driven growth set holds under the same write rate once the stall is
/// gone and every thread reads 5% busy (ctr274's review of #730), and releases when the
/// rate drops below half of what it was: the shards then pack again.
#[test]
fn the_stall_floor_holds_under_the_same_rate_and_releases_when_it_drops() {
    let (mut db, mut written) = packed_with_shard_0_stalled();
    let mut passes = 0;
    while !db.engine.pack_ks()[0].is_some_and(|k| k >= 3) && passes < 120 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    let k = db.engine.pack_ks()[0].expect("decided");
    assert!(k >= 3, "after {passes} passes");
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Stalled, Some(0.0));
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.05));
    }
    for pass in 0..100 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        let now = db.engine.pack_ks()[0].expect("decided");
        assert!(now >= k, "the floor gave way at pass {pass}: {now} < {k}");
    }
    // The rate drops to a quarter: the floor releases and the shrink follows (the stalled
    // phase split tablets, and the moves back take a dwell each, so the count is the check).
    passes = 0;
    while db.engine.pack_ks()[0] != Some(1) && passes < 400 {
        write_and_balance(&mut db, 4, |_| 5, &mut written);
        passes += 1;
    }
    assert_eq!(
        db.engine.pack_ks()[0],
        Some(1),
        "after {passes} passes at a quarter of the rate, tablets {:?}",
        tablets_per_shard(&db, 4)
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// The floor also expires after a quiet window with no active shard stalling (D255,
/// ctr274's follow-up): the stall's cause gone with the rate unchanged, the shards pack
/// again after 300 quiet passes, not never.
#[test]
fn the_stall_floor_expires_after_a_quiet_window() {
    let (mut db, mut written) = packed_with_shard_0_stalled();
    let mut passes = 0;
    while !db.engine.pack_ks()[0].is_some_and(|k| k >= 3) && passes < 120 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    let k = db.engine.pack_ks()[0].expect("decided");
    assert!(k >= 3, "after {passes} passes");
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Stalled, Some(0.0));
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.05));
    }
    for pass in 0..250 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        let now = db.engine.pack_ks()[0].expect("decided");
        assert!(now >= k, "the floor gave way at pass {pass}: {now} < {k}");
    }
    passes = 0;
    while db.engine.pack_ks()[0] != Some(1) && passes < 400 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(
        db.engine.pack_ks()[0],
        Some(1),
        "after 250 + {passes} quiet passes, tablets {:?}",
        tablets_per_shard(&db, 4)
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// A periodic batch writer packs (D255, ctr274's review of #730): four shards 10% busy, a
/// batch every fifth pass and nothing between. Right after each batch the fast write
/// average is far above the slow one, so the pass reads as ramping; a shrink count that
/// reset on every such pass never reached 50 and `k` stayed at four for good. A ramping
/// pass only pauses the count, so the batches' quiet passes carry it.
#[test]
fn periodic_batch_writers_still_pack() {
    let mut db = spread_and_busy(0.1, true);
    let mut written = 0;
    let mut passes = 0u64;
    while tablets_per_shard(&db, 4) != vec![4, 0, 0, 0] && passes < 600 {
        let batch = passes.is_multiple_of(5);
        write_and_balance(&mut db, 4, |_| if batch { 100 } else { 0 }, &mut written);
        passes += 1;
    }
    assert_eq!(
        tablets_per_shard(&db, 4),
        vec![4, 0, 0, 0],
        "not packed after {passes} passes, k {:?}",
        db.engine.pack_ks()[0]
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// The floor's quiet window doubles only when a stall comes back within one window of the
/// last expiry (D255, ctr274's review): a second episode well after the first releases
/// after the base window again, not the doubled one, so a store's ordinary episodes never
/// pin a later transient stall for minutes.
#[test]
fn the_floor_backoff_resets_after_a_quiet_episode() {
    let (mut db, mut written) = packed_with_shard_0_stalled();
    // Episode 1: the stall-driven growth, then the stall gone; the floor expires after the
    // base window.
    let episode = |db: &mut Db, written: &mut u64| -> usize {
        db.engine
            .set_shard_load(0, pigeonhole_engine::LoadTerm::Stalled, Some(1.0));
        let mut passes = 0;
        while !db.engine.pack_ks()[0].is_some_and(|k| k >= 2) && passes < 120 {
            write_and_balance(db, 4, |_| 20, written);
            passes += 1;
        }
        assert!(
            db.engine.pack_ks()[0].is_some_and(|k| k >= 2),
            "no stall-driven growth after {passes} passes"
        );
        db.engine
            .set_shard_load(0, pigeonhole_engine::LoadTerm::Stalled, Some(0.0));
        for s in 0..4 {
            db.engine
                .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.05));
        }
        passes = 0;
        while db.engine.pack_ks()[0] != Some(1) && passes < 1000 {
            write_and_balance(db, 4, |_| 20, written);
            passes += 1;
        }
        assert_eq!(
            db.engine.pack_ks()[0],
            Some(1),
            "no release after {passes} passes"
        );
        passes
    };
    let first = episode(&mut db, &mut written);
    assert!(
        first < 600,
        "the first episode's window was not the base one: {first} passes"
    );
    // More than a window later, with the stall gone throughout.
    for _ in 0..400 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
    }
    let second = episode(&mut db, &mut written);
    assert!(
        second < 600,
        "the second episode's window doubled although the first expired {first} passes in and 400 quiet passes passed since: {second} passes"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// Short stalls a few times a second read as a stalled shard (D255): a packed shard whose
/// writers wait for room on every third pass stalled on at least 3 of the last 10 passes,
/// reads stalled, and grows the active shards. A moving average of such stalls peaked near
/// 0.56 and never reached the hot test.
#[test]
fn stalls_on_three_of_ten_passes_read_as_a_stalled_shard() {
    let (mut db, mut written) = packed_without_compaction();
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.0));
    }
    db.vfs.set_deferred_io(true);
    let mut passes = 0u64;
    while !db.engine.pack_ks()[0].is_some_and(|k| k >= 2) && passes < 150 {
        if passes.is_multiple_of(3) {
            stall_pass(&mut db, 4, 1_000_000_000, &mut written);
        } else {
            stall_pass_with(&mut db, |_| 1, 8, 1_000_000_000, false, &mut written);
        }
        passes += 1;
    }
    assert!(
        db.engine.pack_ks()[0].is_some_and(|k| k >= 2),
        "after {passes} passes the active shards are {:?}: stalls on every third pass did not read hot",
        db.engine.pack_ks()[0]
    );
    db.vfs.complete_all_io();
    db.vfs.set_deferred_io(false);
    db.settle();
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// One stall every ten passes is not capacity (D255, ctr274's review): a packed shard
/// whose writers wait for room on every tenth pass stays packed through 150 passes.
#[test]
fn a_stall_every_ten_passes_does_not_read_as_a_stalled_shard() {
    let (mut db, mut written) = packed_without_compaction();
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.0));
    }
    db.vfs.set_deferred_io(true);
    for pass in 0..150u64 {
        if pass.is_multiple_of(10) {
            stall_pass(&mut db, 4, 1_000_000_000, &mut written);
        } else {
            stall_pass_with(&mut db, |_| 1, 8, 1_000_000_000, false, &mut written);
        }
        assert_eq!(
            db.engine.pack_ks()[0],
            Some(1),
            "a stall every ten passes spread the store at pass {pass}"
        );
    }
    db.vfs.complete_all_io();
    db.vfs.set_deferred_io(false);
    db.settle();
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// Four shards spread with a settled steady load: 300 passes at 60% busy (no shrink count
/// runs: the busy sum over three is 0.8), long enough for the slow write average to meet
/// the fast one, so the passes that follow do not read as ramping; then every thread at
/// `busy`.
fn spread_and_settled(busy: f64) -> (Db, u64) {
    let mut db = spread_and_busy(0.6, true);
    let mut written = 0;
    for _ in 0..300 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
    }
    assert!(db.engine.pack_ks()[0].is_none_or(|k| k == 4));
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(busy));
    }
    (db, written)
}

/// A busy pass resets the shrink count ramping or not (D255, ctr274's review of build 4):
/// four shards settled at 30% busy have 40 light passes counted, then three passes that are both busy
/// (every shard at 60%, past the shrink target over three shards) and ramping (a burst of
/// rows); the count must start over, so 40 more light passes leave the active shards at
/// four. Pausing the reset too would carry the 40 across and shrink about 13 passes in.
#[test]
fn a_busy_pass_during_a_ramp_still_resets_the_shrink_count() {
    let (mut db, mut written) = spread_and_settled(0.3);
    for _ in 0..40 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
    }
    assert!(db.engine.pack_ks()[0].is_none_or(|k| k == 4));
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.6));
    }
    for _ in 0..3 {
        write_and_balance(&mut db, 4, |_| 400, &mut written);
    }
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.3));
    }
    for pass in 0..40 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        let k = db.engine.pack_ks()[0];
        assert!(
            k.is_none_or(|k| k == 4),
            "the active shards shrank to {k:?} {pass} passes after the busy burst"
        );
    }
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// A pass on which an active shard's writers stall resets the shrink count (D255): four
/// shards settled at 30% busy have 40 light passes counted, then one pass on which shard 0
/// reads stalled (a stall term of 1, which reads hot); 40 more light passes leave the active
/// shards at four. Counting the stalled pass as light would carry the 40 across and shrink
/// after 10 (a write lull whose writers waited on room shrank a 16 GiB load from 16 shards
/// to 2 on the AX102).
#[test]
fn a_stalled_pass_resets_the_shrink_count() {
    let (mut db, mut written) = spread_and_settled(0.3);
    for _ in 0..40 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
    }
    assert!(db.engine.pack_ks()[0].is_none_or(|k| k == 4));
    // Stalled for one pass: one hot pass, too few for a growth.
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Stalled, Some(1.0));
    write_and_balance(&mut db, 4, |_| 20, &mut written);
    db.engine
        .set_shard_load(0, pigeonhole_engine::LoadTerm::Stalled, Some(0.0));
    for pass in 0..40 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        let k = db.engine.pack_ks()[0];
        assert!(
            k.is_none_or(|k| k == 4),
            "the active shards shrank to {k:?} {pass} passes after the stalled pass"
        );
    }
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// A shrink that stalls writers soon after, with the store writing twice what it did at
/// the shrink, was wrong (the load came back), and the first stall-driven growth undoes it
/// in one step (D255): packed from four shards onto one at 80 rows a pass, a shard reading
/// fully stalled at 240 rows a pass (the fast write average passes twice the rate at the
/// shrink by the growth's tenth pass) takes the active shards straight back to four, not two.
#[test]
fn a_stall_soon_after_a_shrink_undoes_it_in_one_step() {
    let (mut db, mut written) = packed_with_shard_0_stalled();
    let mut passes = 0;
    while db.engine.pack_ks()[0] == Some(1) && passes < 60 {
        write_and_balance(&mut db, 4, |_| 60, &mut written);
        passes += 1;
    }
    assert_eq!(
        db.engine.pack_ks()[0],
        Some(4),
        "the first growth after {passes} passes did not undo the shrink"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// The undo needs the load back (D255, ctr274's review): a stall at the same write rate as
/// the shrink grows the active shards one step, to two, not back to four.
#[test]
fn a_stall_at_the_same_rate_does_not_undo_the_shrink() {
    let (mut db, mut written) = packed_with_shard_0_stalled();
    let mut passes = 0;
    while db.engine.pack_ks()[0] == Some(1) && passes < 60 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(
        db.engine.pack_ks()[0],
        Some(2),
        "the first growth after {passes} passes was not one step"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// The stall term weighs in the busy sum (D255): two shards reading fully stalled make a
/// busy sum near 2, so the first growth from one active shard goes to four at once
/// (`ceil(B / 0.5)`), not to two.
#[test]
fn stalled_shards_weigh_in_the_busy_sum() {
    let (mut db, mut written) = packed_long_ago_with_shard_0_stalled();
    db.engine
        .set_shard_load(1, pigeonhole_engine::LoadTerm::Stalled, Some(1.0));
    let mut passes = 0;
    while db.engine.pack_ks()[0] == Some(1) && passes < 60 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(
        db.engine.pack_ks()[0],
        Some(4),
        "the first growth after {passes} passes did not size by the stalled shards"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// A load that only ever ramps still packs (D255): four shards 30% busy under rows rising
/// 0.4% a pass read as ramping throughout (the fast write average stays about 1.4× the
/// slow one), hold at four for 600 passes, and then the shrink count runs anyway.
#[test]
fn a_load_that_only_ramps_still_packs_after_the_cap() {
    let mut db = spread_and_busy(0.3, true);
    let mut written = 0;
    let rows = |pass: u64| (20.0 * 1.004f64.powi(pass as i32)).round() as u64;
    for pass in 0..580u64 {
        write_and_balance(&mut db, 4, |_| rows(pass), &mut written);
        let k = db.engine.pack_ks()[0];
        assert!(
            k.is_none_or(|k| k == 4),
            "the active shards shrank to {k:?} at pass {pass} of the ramp"
        );
    }
    let mut pass = 580u64;
    while db.engine.pack_ks()[0].is_none_or(|k| k == 4) && pass < 720 {
        write_and_balance(&mut db, 4, |_| rows(pass), &mut written);
        pass += 1;
    }
    assert!(
        db.engine.pack_ks()[0].is_some_and(|k| k < 4),
        "the active shards never shrank under a ramp past the cap ({pass} passes)"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// The first shrink from the unpacked start needs a slow-average window of the light
/// condition (D255, #734): four shards 60% busy for 100 passes, 10% for 100 (a dip between
/// a load's phases, twice the usual 50-pass persistence), 60% again: the active shards stay
/// at four throughout. With the usual persistence the dip packed the store, and the load's
/// return then moved every tablet back out while it was measured.
#[test]
fn a_dip_between_phases_does_not_pack_an_unpacked_store() {
    let mut db = spread_and_busy(0.6, true);
    let mut written = 0;
    let phases: [(f64, usize); 3] = [(0.6, 100), (0.1, 100), (0.6, 100)];
    for (phase, &(busy, passes)) in phases.iter().enumerate() {
        for s in 0..4 {
            db.engine
                .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(busy));
        }
        for pass in 0..passes {
            write_and_balance(&mut db, 4, |_| 20, &mut written);
            let k = db.engine.pack_ks()[0];
            assert!(
                k.is_none_or(|k| k == 4),
                "the active shards shrank to {k:?} at pass {pass} of phase {phase}"
            );
        }
    }
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// A light store still packs, after the window (D255, #734): four shards 10% busy hold at
/// four for 250 passes and have packed by 400; the shrinks after the first keep D217's 50.
#[test]
fn a_light_store_packs_after_the_first_window() {
    let mut db = spread_and_busy(0.1, true);
    let mut written = 0;
    for pass in 0..250 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        let k = db.engine.pack_ks()[0];
        assert!(
            k.is_none_or(|k| k == 4),
            "the active shards shrank to {k:?} at pass {pass}, before the window"
        );
    }
    let mut passes = 250;
    while db.engine.pack_ks()[0].is_none_or(|k| k == 4) && passes < 400 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert!(
        db.engine.pack_ks()[0].is_some_and(|k| k < 4),
        "a light store never packed ({passes} passes)"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// The first-shrink window restarts on a reopen (D255, #734, ctr274's review): a light store
/// packed onto one shard, closed and reopened on the same four shards comes back unpacked
/// (owners are not persisted, D130) and holds at four through 250 writing passes, then packs
/// by 400. Persisting the active count or the counter across a reopen would pack it after
/// 50.
#[test]
fn the_first_shrink_window_restarts_on_a_reopen() {
    let vfs = SimVfs::new(7);
    let options = |o: &mut EngineOptions| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_min_writes = 10;
        o.balance_pack = true;
    };
    let mut db = open_on(&vfs, 4, options);
    tablets_on(&mut db, 4, |t| t as u16);
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.1));
    }
    let mut written = 0;
    let mut passes = 0;
    while tablets_per_shard(&db, 4) != vec![4, 0, 0, 0] && passes < 600 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert_eq!(tablets_per_shard(&db, 4), vec![4, 0, 0, 0]);
    db.close();

    let mut db = open_on(&vfs, 4, options);
    assert_eq!(db.scan_rows().len() as u64, written);
    let placed = tablets_per_shard(&db, 4);
    assert!(
        placed.iter().filter(|n| **n > 0).count() > 1,
        "placed unpacked at open: {placed:?}"
    );
    for s in 0..4 {
        db.engine
            .set_shard_load(s, pigeonhole_engine::LoadTerm::Busy, Some(0.1));
    }
    for pass in 0..250 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        let k = db.engine.pack_ks()[0];
        assert!(
            k.is_none_or(|k| k == 4),
            "the active shards shrank to {k:?} at pass {pass} after the reopen"
        );
    }
    let mut passes = 250;
    while db.engine.pack_ks()[0].is_none_or(|k| k == 4) && passes < 400 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert!(
        db.engine.pack_ks()[0].is_some_and(|k| k < 4),
        "the reopened store never packed ({passes} passes)"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.close();
}

/// An idle store never packs and its passes do not count toward the first-shrink window
/// (D255, #734, ctr274's review): four shards 10% busy with writes under
/// `balance_min_writes` (a row a tablet a pass) for 1,000 passes keep every tablet where it
/// is and the active shards at four; the writes then hold them at four for 250 passes and
/// pack by 400, as from open. Counting every pass would pack it within 50 of the first
/// write. (A tablet written on no pass at all goes cold after 100 and D224's consolidation
/// moves it, its own rule with its own tests; the active count stays at four there too.)
#[test]
fn an_idle_store_stays_unpacked_and_does_not_count_toward_the_window() {
    let mut db = spread_and_busy(0.1, true);
    let before = tablet_changes(&db.engine);
    let mut written = 0;
    for pass in 0..1_000 {
        write_and_balance(&mut db, 4, |_| 1, &mut written);
        let k = db.engine.pack_ks()[0];
        assert!(
            k.is_none_or(|k| k == 4),
            "the active shards shrank to {k:?} at idle pass {pass}"
        );
    }
    assert_eq!(tablet_changes(&db.engine), before);
    assert_eq!(tablets_per_shard(&db, 4), vec![1, 1, 1, 1]);
    for pass in 0..250 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        let k = db.engine.pack_ks()[0];
        assert!(
            k.is_none_or(|k| k == 4),
            "the active shards shrank to {k:?} at writing pass {pass}"
        );
    }
    let mut passes = 250;
    while db.engine.pack_ks()[0].is_none_or(|k| k == 4) && passes < 400 {
        write_and_balance(&mut db, 4, |_| 20, &mut written);
        passes += 1;
    }
    assert!(
        db.engine.pack_ks()[0].is_some_and(|k| k < 4),
        "the store never packed once written ({passes} passes)"
    );
    assert_eq!(db.scan_rows().len() as u64, written);
    db.close();
}

/// With packing off (`balance_pack = false`), or an idle store (writes below
/// `balance_min_writes`), the same light load changes no tablet.
#[test]
fn packing_off_or_an_idle_store_keeps_the_placement() {
    for (pack, rows) in [(false, 20), (true, 1)] {
        let mut db = spread_and_busy(0.1, pack);
        let before = tablet_changes(&db.engine);
        let mut written = 0;
        for _ in 0..120 {
            write_and_balance(&mut db, 4, |_| rows, &mut written);
        }
        assert_eq!(
            tablet_changes(&db.engine),
            before,
            "pack {pack}, {rows} rows per tablet"
        );
        assert_eq!(tablets_per_shard(&db, 4), vec![1, 1, 1, 1]);
        db.engine.close().unwrap();
        for _ in 0..8 {
            db.step();
        }
    }
}

#[test]
fn a_split_due_on_a_compacting_tablet_starts_when_the_compaction_ends() {
    // The balancer leaves a compacting tablet alone (#102). Under a load whose compactions
    // run back to back no pass lands between two, so the split it chose starts as soon as
    // the running compaction ends, before the next one is picked and with no further
    // balancer pass.
    let mut db = open(1, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.tablet_split_bytes = 8 << 10;
        o.memtable_freeze_bytes = 32 << 10;
    });
    db.engine.hold_compactions(true);
    let value = vec![0x5A; 200];
    let mut i = 0;
    while !db.engine.compaction_held() {
        assert!(i < 20_000, "no compaction started");
        for _ in 0..250 {
            db.put(&key(i), &value);
            i += 1;
        }
        let m = db.engine.flush_pending().unwrap();
        db.drive(m).unwrap();
    }
    // Due a split, but compacting: the pass holds the tablet.
    let m = db.engine.balance_pending().unwrap();
    db.drive(m).unwrap();
    assert_eq!(db.ranges().len(), 1);
    db.engine.hold_compactions(false);
    db.settle();
    let ranges = db.ranges();
    assert_eq!(ranges.len(), 2, "{ranges:?}");
    assert_eq!(db.scan_rows().len(), i as usize);
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
        // Cold as soon as a pass went by with no writes since the split arrived.
        o.balance_cold_passes = Some(1);
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
fn a_single_reserved_after_a_decision_stamps_above_it_before_the_share_is_applied() {
    // D245, D264 (perf287): on a frozen clock every default timestamp comes from a floor.
    // The coordinator's floor runs well above the participant's; it decides a cross-shard
    // commit X at (S, T) and raises the participant to T. While the DECISION's sync is in
    // flight the participant has no `Decide` yet (it raises its own floor to T only when it
    // applies the share), so a single it reserves after S stamps above T only through the
    // folded raise. Without the fold it would stamp at its own floor, below T, at a later
    // seqno.
    let mut db = open(2, |o| o.balance_interval_nanos = u64::MAX);
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
    for i in 0..20u8 {
        db.put(&[b'x', i], b"v");
    }
    db.engine.record_history(true);
    db.engine.take_appended();
    db.vfs.set_deferred_io(true);
    // X: `x` first, so `high` coordinates; its share on `low` writes `a`.
    let mut wb = WriteBatch::new();
    wb.put(id, f, b"x", b"q", None, ValueRef::Bytes(b"X"))
        .unwrap();
    wb.put(id, f, b"a", b"q", None, ValueRef::Bytes(b"X"))
        .unwrap();
    let mut x = db.engine.submit(wb, Some(Durability::Sync)).unwrap();
    let mut decision = None;
    for _ in 0..1_000 {
        db.step();
        decision = db
            .engine
            .take_appended()
            .into_iter()
            .find(|r| r.kind == pigeonhole_engine::AppendedKind::Commit);
        if decision.is_some() {
            break;
        }
        db.vfs.complete_all_io();
    }
    let decision = decision.expect("X was never decided");
    let s = decision.apply_seqno;
    // The DECISION's sync is still in flight: `low` reserves and stamps a single on another
    // row before it hears X's decision.
    let mut wb = WriteBatch::new();
    wb.put(id, f, b"b", b"q", None, ValueRef::Bytes(b"Y"))
        .unwrap();
    let mut y = db.engine.submit(wb, Some(Durability::None)).unwrap();
    let mut stamped = None;
    for _ in 0..100 {
        let now = db.clock.monotonic_nanos();
        db.shards[usize::from(low)].run_once(now + 1_000);
        stamped = db
            .engine
            .take_appended()
            .into_iter()
            .find(|r| r.kind == pigeonhole_engine::AppendedKind::Batch);
        if stamped.is_some() {
            break;
        }
    }
    let stamped = stamped.expect("the single was never stamped");
    assert!(
        stamped.seqno > s,
        "single at {} not after S {s}",
        stamped.seqno
    );
    db.vfs.set_deferred_io(false);
    db.vfs.complete_all_io();
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..10_000 {
        let done_x = matches!(Pin::new(&mut x).poll(&mut cx), Poll::Ready(_));
        let done_y = matches!(Pin::new(&mut y).poll(&mut cx), Poll::Ready(_));
        if done_x && done_y {
            break;
        }
        db.step();
    }
    let (t, _) = db.get(b"a").expect("X committed");
    assert!(
        stamped.commit_ts > t,
        "the single at seqno {} (after S {s}) stamped {} at or below X's timestamp {t}",
        stamped.seqno,
        stamped.commit_ts
    );
    assert_eq!(db.get(b"b"), Some((stamped.commit_ts, b"Y".to_vec())));
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
fn a_round_refused_for_an_overlap_and_for_room_retries_on_the_overlap_wake_only() {
    // #724 PR 4 (perf287): a round refused for an overlap at P and for room at Q waits for
    // P's overlap wake only (`RetryWait`'s precedence), even when Q's room wake comes first.
    // Three shards: Q (shard 0, the manifest pump's) with a full arena, P, and R. Z, older,
    // has an undecided share on P's row `n` while R, its other participant, is held back. X
    // (Q coordinates) is refused for room at Q and behind Z at P, which acknowledges X's
    // abort without waking it. Q alone then frees room and wakes itself: no retry runs. Once
    // R runs, Z commits, P wakes X, and X commits.
    let mut db = open(3, |o| {
        o.memtable_budget = 4 << 20;
        // No memtable freezes by size: only the wait for room frees the arena.
        o.memtable_freeze_bytes = 64 << 20;
        o.balance_interval_nanos = u64::MAX;
    });
    let id = db.table.id;
    let f = db.table.families[0].id;
    for key in [b"m", b"t"] {
        let m = db.engine.split_tablet_pending(id, key).unwrap();
        db.drive(m).unwrap();
    }
    let (q, p, r) = (0u16, 1u16, 2u16);
    for (i, key, to) in [(0, &b"a"[..], q), (1, &b"n"[..], p), (2, &b"u"[..], r)] {
        if db.ranges()[i].1 != to {
            let m = db.engine.move_tablet_pending(id, key, to).unwrap();
            db.drive(m).unwrap();
        }
    }
    assert_eq!(
        db.ranges().iter().map(|t| t.1).collect::<Vec<_>>(),
        [q, p, r]
    );
    let value = vec![7u8; 64 << 10];
    for i in 0..1_000u32 {
        let (free, largest, _) = db.engine.arena_free(usize::from(q));
        if free < 4 * value.len() as u64 || largest < 4 * value.len() as u64 {
            break;
        }
        let mut wb = WriteBatch::new();
        wb.put(
            id,
            f,
            format!("a{i:04}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&value),
        )
        .unwrap();
        db.commit(wb);
    }
    let put = |rows: &[(&[u8], &[u8])]| {
        let mut wb = WriteBatch::new();
        for (row, v) in rows {
            wb.put(id, f, row, b"q", None, ValueRef::Bytes(v)).unwrap();
        }
        wb
    };
    let run = |db: &mut Db, shard: u16| {
        let now = db.clock.monotonic_nanos();
        db.shards[usize::from(shard)].run_once(now + 1_000);
    };
    db.engine.record_history(true);
    db.engine.take_appended();
    // Z: P coordinates (`n` first); its share on P is admitted, R never answers yet.
    let mut z = db
        .engine
        .submit(
            put(&[(b"n", b"Z"), (b"u", b"Z")]),
            Some(Durability::Buffered),
        )
        .unwrap();
    for _ in 0..4 {
        run(&mut db, p);
    }
    // X: Q coordinates (`b` first, a 1 MiB share that does not fit there) and writes `n`.
    let big = vec![9u8; 1 << 20];
    let before = db.engine.room_refusals();
    let refused_before = db.engine.metrics().cross_shard.refused_rounds;
    // X reads `n` (family `g`): a write over Z's undecided write no longer overlaps it
    // (D271), a read of its row does.
    let g = db.table.families[1].id;
    let mut tx = db.engine.begin().unwrap();
    tx.get(id, g, b"n", b"q").unwrap();
    tx.batch()
        .put(id, f, b"b", b"q", None, ValueRef::Bytes(&big))
        .unwrap();
    tx.batch()
        .put(id, f, b"n", b"q", None, ValueRef::Bytes(b"X"))
        .unwrap();
    let mut x = tx.submit(Some(Durability::Buffered)).unwrap();
    // Q and P only: Q refuses its own share for room, P refuses X behind Z, Q decides the
    // abort, P acknowledges it (no wake: Z has not moved), and Q queues the retry.
    for _ in 0..6 {
        run(&mut db, q);
        run(&mut db, p);
    }
    assert!(
        db.engine.room_refusals() > before,
        "Q never refused for room"
    );
    // Q alone: whatever room its wait has not freed yet frees, and its room wake reaches
    // the coordinator (Q itself), here or already during the abort round. The retry still
    // waits for P's overlap wake.
    for _ in 0..5_000 {
        run(&mut db, q);
    }
    let (free, _, _) = db.engine.arena_free(usize::from(q));
    assert!(free >= big.len() as u64, "Q never freed room: {free} free");
    assert!(
        poll_commit(&mut x).is_none(),
        "X resolved before P's overlap wake"
    );
    // R answers: Z commits, P wakes X, and X's retry commits.
    let rz = poll_until(&mut db, &mut z, 100_000).expect("Z resolved");
    rz.expect("Z commits");
    let rx = poll_until(&mut db, &mut x, 100_000).expect("X resolved");
    rx.expect("X commits");
    assert_eq!(db.get(b"n").map(|(_, v)| v), Some(b"X".to_vec()));
    // One refused round: a retry run on Q's room wake would have met Z's share at P again.
    let refused = db.engine.metrics().cross_shard.refused_rounds - refused_before;
    assert_eq!(refused, 1, "X's retry ran before P's overlap wake");
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
#[allow(deprecated)] // the D263-deprecated refusal counters stay 0
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
    let before = db.engine.metrics().cross_shard;
    let mut wb = WriteBatch::new();
    wb.put(id, f, b"a", b"q", None, ValueRef::Bytes(b"low"))
        .unwrap();
    wb.put(id, f, b"z", b"q", None, ValueRef::Bytes(b"both"))
        .unwrap();
    db.commit(wb);
    // D263: the timestamp is picked at the decision, above the floor the
    // participant reported with its `Prepared` (taken on its own thread), so the stale
    // published floor refuses nothing: one round, and the commit lands above the floor.
    let after = db.engine.metrics().cross_shard;
    assert_eq!(after.refused_below_floor, before.refused_below_floor);
    assert_eq!(after.refused_rounds, before.refused_rounds);
    assert_eq!(after.rounds - before.rounds, 1);
    assert_eq!(after.refused_moved, before.refused_moved);
    assert_eq!(after.refused_move_raise, before.refused_move_raise);
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
fn a_participant_never_stamps_a_later_seqno_below_a_prepared_commit_timestamp() {
    // D142's order, the other way round from the test above. `high` coordinates a
    // cross-shard commit B: it reserves B's seqno, then picks B's timestamp above every
    // floor, its own here, which ran ahead of `low`'s on the frozen clock. A single A,
    // queued on the participant `low` ahead of B's PREPARE, is admitted in the same group,
    // so it gets a later seqno; it must not get a lower default timestamp than B's (D11: a
    // tablet's default timestamps never go backwards in seqno order).
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
    // The frozen clock: each default timestamp is the shard's floor plus one, so `high`'s
    // floor climbs ten above `low`'s.
    for i in 0..10u8 {
        db.put(b"z", &[i]);
    }
    // A, a single on `low`, then B, coordinated by `high` (its first row is there); neither
    // has run yet.
    let mut wb = WriteBatch::new();
    wb.put(id, f, b"a", b"s", None, ValueRef::Bytes(b"single"))
        .unwrap();
    let mut a = db
        .engine
        .submit(wb, Some(Durability::Buffered))
        .expect("submit");
    let mut wb = WriteBatch::new();
    wb.put(id, f, b"z", b"q", None, ValueRef::Bytes(b"both"))
        .unwrap();
    wb.put(id, f, b"a", b"q", None, ValueRef::Bytes(b"both"))
        .unwrap();
    let mut b = db
        .engine
        .submit(wb, Some(Durability::Buffered))
        .expect("submit");
    // `high` reserves B's seqno, picks its timestamp and sends `low` the PREPARE, which
    // queues behind A; then `low` admits both in one group.
    let now = db.clock.monotonic_nanos();
    db.shards[usize::from(high)].run_once(now + 1_000);
    db.shards[usize::from(low)].run_once(now + 1_000);
    let mut cx = Context::from_waker(Waker::noop());
    let mut seqno = |pc: &mut pigeonhole_engine::PendingCommit, db: &mut Db| {
        for _ in 0..100_000 {
            if let Poll::Ready(r) = Pin::new(&mut *pc).poll(&mut cx) {
                return r.expect("commit").seqno;
            }
            db.step();
        }
        panic!("a commit never resolved");
    };
    let seqno_a = seqno(&mut a, &mut db);
    let seqno_b = seqno(&mut b, &mut db);
    let ts_a = db
        .engine
        .get_latest(id, f, b"a", b"s")
        .unwrap()
        .expect("A")
        .timestamp();
    let (ts_b, _) = db.get(b"z").unwrap();
    assert_eq!(db.get(b"a").unwrap().0, ts_b);
    eprintln!("A: seqno {seqno_a} ts {ts_a}; B: seqno {seqno_b} ts {ts_b}");
    let (first, second) = if seqno_a < seqno_b {
        ((seqno_a, ts_a), (seqno_b, ts_b))
    } else {
        ((seqno_b, ts_b), (seqno_a, ts_a))
    };
    assert!(
        second.1 >= first.1,
        "seqno {} has timestamp {} below seqno {}'s {} on one tablet (D11, D142)",
        second.0,
        second.1,
        first.0,
        first.1
    );
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
fn singles_stamped_ahead_of_a_prepare_in_an_earlier_group_land_above_it() {
    // #653 across groups: A is stamped in an earlier group than the one that admits B's
    // PREPARE, at a later seqno. B's coordinator raised `low`'s floor to B's timestamp
    // before reserving B's seqno (D245), so A lands above it and nothing is refused for
    // order; B is refused only for arena room, and commits once room frees.
    let (attempts, refusals) = stamped_in_an_earlier_group(false, 64 << 20);
    assert_eq!(attempts as u64, 1 + refusals, "{refusals:?}");
}

#[test]
fn singles_stamped_ahead_of_a_prepare_land_above_it_with_a_moving_clock() {
    // The same on a clock that moves (each read a nanosecond).
    let (attempts, refusals) = stamped_in_an_earlier_group(true, 64 << 20);
    assert_eq!(attempts as u64, 1 + refusals, "{refusals:?}");
}

#[test]
fn a_prepare_short_of_arena_room_is_refused_and_commits_once_room_frees() {
    // #655: with a 4 MiB arena, B's PREPARE finds no room behind the queued singles. Waiting
    // would hold B's seqno, and with it the durable watermark below the memtables the
    // freeze needs durable (D227): no room ever frees, on a frozen clock for good. It is
    // refused instead (D245, ICR 0042), and B commits once `low` reports room freed.
    let (attempts, refusals) = stamped_in_an_earlier_group(false, 4 << 20);
    assert!(refusals > 0, "no room refusal: {refusals:?}");
    assert_eq!(attempts as u64, 1 + refusals, "{refusals:?}");
}

#[test]
fn a_lone_prepare_against_a_full_arena_commits_on_a_frozen_clock() {
    // D245 (perf287's check 2): no single waits, so only the PREPARE's own refusal for room
    // can start the freeze and flush that frees it (D124's room wait); `RoomFreed` follows
    // that flush, and the retry commits. On the frozen clock no timeout would end a loop.
    let (mut db, mut pc, before) = prepare_against_a_full_arena(false, false);
    let r = poll_until(&mut db, &mut pc, 100_000).expect("resolved");
    r.expect("commit");
    let after = db.engine.room_refusals();
    assert!(after > before, "never refused for room: {after}");
    assert_eq!(db.get(b"b").map(|(_, v)| v.len()), Some(1 << 20));
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
fn a_retry_waiting_for_room_fails_busy_at_the_write_stall_timeout() {
    // D245 (perf287's check 3): a snapshot pins the arena, so no flush frees room for the
    // refused share. Its retry waits on the participant's room generation, and like a
    // commit parked behind a tablet change (D241) it fails `Busy` at the timeout.
    let (mut db, mut pc, before) = prepare_against_a_full_arena(true, true);
    let r = poll_until(&mut db, &mut pc, 1_000_000).expect("resolved");
    assert!(matches!(r, Err(pigeonhole_engine::Error::Busy)), "{r:?}");
    assert!(db.engine.room_refusals() > before, "never refused for room");
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
fn a_retry_waiting_for_room_ends_closed_at_close() {
    // The same retry on the frozen clock, which never reaches a timeout: the close ends it
    // with `Closed` (not committed).
    let (mut db, mut pc, before) = prepare_against_a_full_arena(false, true);
    for _ in 0..1_000 {
        db.step();
    }
    assert!(poll_commit(&mut pc).is_none(), "resolved before the close");
    assert!(db.engine.room_refusals() > before, "never refused for room");
    db.engine.close().unwrap();
    let r = poll_until(&mut db, &mut pc, 100_000).expect("resolved");
    assert!(matches!(r, Err(pigeonhole_engine::Error::Closed)), "{r:?}");
}

#[test]
fn a_moved_retry_keeping_its_timestamp_still_raises_its_new_participant_first() {
    // D245 (perf287's check 1): a cross-shard commit refused `Moved` while its tablet moves
    // to a shard with a lower floor is retried with its first timestamp (it still fits).
    // The retry must raise the new owner before it reserves its seqno, like a fresh pick:
    // 1,024 singles queued there ahead of the retry's PREPARE are stamped in an earlier
    // turn than the one that reads the PREPARE, at later seqnos, and must land above it.
    let mut db = open(3, |o| o.memtable_budget = 64 << 20);
    let (t, f) = (db.table.id, db.table.families[0].id);
    let m = db.engine.split_tablet_pending(t, b"m").unwrap();
    db.drive(m).unwrap();
    // `t`: "a".. on shard 0, "m".. on shard 2; `u` alone on shard 1.
    for (row, to) in [(&b"a"[..], 0u16), (b"m", 2)] {
        let owner = db
            .ranges()
            .iter()
            .rfind(|r| r.2.as_slice() <= row)
            .map(|r| r.1)
            .unwrap();
        if owner != to {
            let m = db.engine.move_tablet_pending(t, row, to).unwrap();
            db.drive(m).unwrap();
        }
    }
    let u = db
        .engine
        .create_table("u", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let uf = u.families[0].id;
    let u_owner = db.engine.snapshot().unwrap().view().tablets().ranges(u.id)[0].1;
    if u_owner != 1 {
        let m = db.engine.move_tablet_pending(u.id, b"", 1).unwrap();
        db.drive(m).unwrap();
    }
    // Shard 0's floor runs 3,000 ahead of shard 2's.
    for i in 0..3_000u32 {
        db.put(b"a", &i.to_le_bytes());
    }
    // `u`'s compaction holds at its manifest commit, so the move of `u` to shard 2 waits in
    // its drain (D241); the commit over "a" (0) and `u` (1) meets it, is refused `Moved` and
    // waits for the map to change.
    for round in 0..2u8 {
        for i in 0..20u8 {
            let mut wb = WriteBatch::new();
            wb.put(u.id, uf, &[b'r', i], b"q", None, ValueRef::Bytes(&[round]))
                .unwrap();
            db.commit(wb);
        }
        let m = db.engine.flush_pending().unwrap();
        db.drive(m).unwrap();
    }
    db.engine.take_appended();
    db.engine.park_manifest_commits(true);
    let _compact = db.engine.compact_pending(Some(u.id)).unwrap();
    for _ in 0..1_000 {
        if db.engine.manifest_commit_parked() {
            break;
        }
        db.step();
    }
    assert!(db.engine.manifest_commit_parked(), "no compaction held");
    let mut mv = db.engine.move_tablet_pending(u.id, b"", 2).unwrap();
    for _ in 0..20 {
        db.step();
    }
    let mut wb = WriteBatch::new();
    wb.put(t, f, b"a", b"q", None, ValueRef::Bytes(b"c"))
        .unwrap();
    wb.put(u.id, uf, b"r0", b"c", None, ValueRef::Bytes(b"c"))
        .unwrap();
    let mut c = db.engine.submit(wb, Some(Durability::Buffered)).unwrap();
    for _ in 0..50 {
        db.step();
    }
    assert!(poll_commit(&mut c).is_none(), "committed during the move");
    // The compaction and the move finish on shards 1 and 2 only, so the retry waits on 0.
    db.engine.park_manifest_commits(false);
    let mut cx = Context::from_waker(Waker::noop());
    let mut moved = false;
    for _ in 0..10_000 {
        let now = db.clock.monotonic_nanos();
        db.shards[1].run_once(now + 1_000);
        db.shards[2].run_once(now + 1_000);
        if Pin::new(&mut mv).poll(&mut cx).is_ready() {
            moved = true;
            break;
        }
    }
    assert!(moved, "the move never finished");
    // 1,024 singles queued on 2; then 0 retries (raise, reserve, PREPARE behind them), and
    // 2's next turn stamps only the singles.
    let mut singles: Vec<_> = (0..1_024u32)
        .map(|i| {
            let mut wb = WriteBatch::new();
            wb.put(
                t,
                f,
                format!("p{i:04}").as_bytes(),
                b"s",
                None,
                ValueRef::Bytes(b"s"),
            )
            .unwrap();
            db.engine.submit(wb, Some(Durability::Buffered)).unwrap()
        })
        .collect();
    let now = db.clock.monotonic_nanos();
    db.shards[0].run_once(now + 1_000);
    db.shards[2].run_once(now + 1_000);
    step_until_done(&mut db, &mut c);
    for s in &mut singles {
        step_until_done(&mut db, s);
    }
    let records = db.engine.take_appended();
    let prepares: Vec<_> = records
        .iter()
        .filter(|r| r.stream == 0 && r.kind == pigeonhole_engine::AppendedKind::Prepare)
        .collect();
    let (ts_c, _) = db.get(b"a").unwrap();
    assert!(
        prepares.len() >= 2,
        "the commit was never refused: {prepares:?}"
    );
    // The `Moved` retry kept its first timestamp.
    let (first, retry) = (prepares[0], prepares[1]);
    assert_eq!(retry.commit_ts, first.commit_ts, "{prepares:?}");
    // Every single on 2 reserved after the retry's seqno was stamped above its kept
    // timestamp: the retry raised 2 before reserving. (A later attempt refused for room
    // behind them takes a fresh timestamp, since 2 stamped since.)
    let singles_on_2: Vec<_> = records
        .iter()
        .filter(|r| r.stream == 2 && r.kind == pigeonhole_engine::AppendedKind::Batch)
        .collect();
    let after: Vec<_> = singles_on_2
        .iter()
        .filter(|r| r.seqno > retry.seqno)
        .collect();
    assert!(
        !after.is_empty(),
        "no single reserved after the retry's seqno"
    );
    for r in &after {
        assert!(
            r.commit_ts > retry.commit_ts,
            "single ({}, {}) below the retry ({}, {})",
            r.seqno,
            r.commit_ts,
            retry.seqno,
            retry.commit_ts
        );
    }
    // And D11's order with the share that landed.
    let seqno_c = prepares.last().unwrap().seqno;
    for r in &singles_on_2 {
        assert!(
            (r.seqno < seqno_c) == (r.commit_ts <= ts_c),
            "single ({}, {}) against commit ({seqno_c}, {ts_c})",
            r.seqno,
            r.commit_ts
        );
    }
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// Two shards: `low`'s arena (4 MiB, nothing frozen by size) filled with committed singles
/// until less than four 64 KiB values are free, then a cross-shard commit whose 1 MiB share
/// on `low` can't fit, submitted (not yet run). With `pin`, a snapshot taken first holds
/// every memtable, so no flush frees room. Returns the store, the commit and the room
/// refusals before it.
fn prepare_against_a_full_arena(
    moving_clock: bool,
    pin: bool,
) -> (Db, pigeonhole_engine::PendingCommit, u64) {
    let mut db = open(2, |o| {
        o.memtable_budget = 4 << 20;
        // No memtable freezes by size: only a wait for room frees the arena.
        o.memtable_freeze_bytes = 64 << 20;
        if moving_clock {
            o.write_stall_timeout_nanos = 200_000;
            ticking(o);
        }
    });
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
    let value = vec![7u8; 64 << 10];
    for i in 0..1_000u32 {
        let (free, largest, _) = db.engine.arena_free(usize::from(low));
        if free < 4 * value.len() as u64 || largest < 4 * value.len() as u64 {
            break;
        }
        let mut wb = WriteBatch::new();
        wb.put(
            id,
            f,
            format!("a{i:04}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&value),
        )
        .unwrap();
        db.commit(wb);
    }
    if pin {
        std::mem::forget(db.engine.snapshot().unwrap());
    }
    let before = db.engine.room_refusals();
    let big = vec![9u8; 1 << 20];
    let mut wb = WriteBatch::new();
    wb.put(id, f, b"z", b"q", None, ValueRef::Bytes(b"z"))
        .unwrap();
    wb.put(id, f, b"b", b"q", None, ValueRef::Bytes(&big))
        .unwrap();
    let pc = db.engine.submit(wb, Some(Durability::Buffered)).unwrap();
    (db, pc, before)
}

/// Steps `db` until `pc` resolves, at most `steps` times.
fn poll_until(
    db: &mut Db,
    pc: &mut pigeonhole_engine::PendingCommit,
    steps: usize,
) -> Option<pigeonhole_engine::Result<pigeonhole_engine::CommitInfo>> {
    for _ in 0..steps {
        if let Some(r) = poll_commit(pc) {
            return Some(r);
        }
        db.step();
    }
    None
}

/// `low`'s queue holds A and 1,023 more singles ahead of cross-shard commit B's PREPARE, so
/// its first turn (up to 1,024 messages) stamps them, at later seqnos than B's, before the
/// PREPARE is read; `high`'s floor, which B's timestamp comes from, runs 3,000 ahead of
/// `low`'s. Checks D11's order between A and B, and returns the coordination attempts B took
/// (its coordinator logs a PREPARE of its own for each) and the refusals for room.
fn stamped_in_an_earlier_group(moving_clock: bool, budget: u64) -> (usize, u64) {
    let mut db = open(2, |o| {
        o.memtable_budget = budget;
        if moving_clock {
            o.write_stall_timeout_nanos = 50_000_000;
            ticking(o);
        }
    });
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
    // `high`'s floor climbs 3,000 above `low`'s (the clock moves a microsecond per
    // thousand reads at most).
    for i in 0..3_000u32 {
        db.put(b"z", &i.to_le_bytes());
    }
    let single = |db: &mut Db, row: &[u8]| {
        let mut wb = WriteBatch::new();
        wb.put(id, f, row, b"s", None, ValueRef::Bytes(b"single"))
            .unwrap();
        db.engine
            .submit(wb, Some(Durability::Buffered))
            .expect("submit")
    };
    let mut a = single(&mut db, b"a");
    let mut rest: Vec<_> = (0..1_023u32)
        .map(|i| single(&mut db, format!("b{i:04}").as_bytes()))
        .collect();
    db.engine.take_appended();
    let mut wb = WriteBatch::new();
    wb.put(id, f, b"z", b"q", None, ValueRef::Bytes(b"both"))
        .unwrap();
    wb.put(id, f, b"a", b"q", None, ValueRef::Bytes(b"both"))
        .unwrap();
    let mut b = db
        .engine
        .submit(wb, Some(Durability::Buffered))
        .expect("submit");
    // `high` reserves B's seqno, picks its timestamp and queues the PREPARE behind the
    // 1,024 singles; `low`'s first turn stamps only those.
    let now = db.clock.monotonic_nanos();
    db.shards[usize::from(high)].run_once(now + 1_000);
    db.shards[usize::from(low)].run_once(now + 1_000);
    let mut cx = Context::from_waker(Waker::noop());
    let mut seqno = |pc: &mut pigeonhole_engine::PendingCommit, db: &mut Db| {
        for _ in 0..100_000 {
            if let Poll::Ready(r) = Pin::new(&mut *pc).poll(&mut cx) {
                return r.expect("commit").seqno;
            }
            db.step();
        }
        panic!("a commit never resolved");
    };
    let seqno_a = seqno(&mut a, &mut db);
    for pc in &mut rest {
        seqno(pc, &mut db);
    }
    let seqno_b = seqno(&mut b, &mut db);
    let ts_a = db
        .engine
        .get_latest(id, f, b"a", b"s")
        .unwrap()
        .expect("A")
        .timestamp();
    let (ts_b, _) = db.get(b"z").unwrap();
    assert_eq!(db.get(b"a").unwrap().0, ts_b);
    eprintln!("A: seqno {seqno_a} ts {ts_a}; B: seqno {seqno_b} ts {ts_b}");
    assert!(
        (seqno_a < seqno_b) == (ts_a <= ts_b),
        "A (seqno {seqno_a}, ts {ts_a}) and B (seqno {seqno_b}, ts {ts_b}) are in opposite \
         orders on one tablet (D11, D142)"
    );
    let attempts = db
        .engine
        .take_appended()
        .iter()
        .filter(|r| r.stream == high && r.kind == pigeonhole_engine::AppendedKind::Prepare)
        .count();
    let refusals = db.engine.room_refusals();
    eprintln!("attempts {attempts}, refusals {refusals:?}");
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
    (attempts, refusals)
}

#[test]
fn an_aborted_prepare_does_not_keep_refusing_later_commits() {
    // D244, perf287's review: a PREPARE is kept from its routing on. One whose transaction
    // then fails validation on the participant `p` is aborted, and its entry, at a timestamp
    // above `p`'s floor, must go with it. Otherwise a later commit over shards with lower
    // floors, at a later seqno and a lower timestamp, is refused by it on every retry (its
    // fresh timestamp never clears a floor nobody published), on a frozen clock for ever.
    let mut db = open(3, |o| o.memtable_budget = 64 << 20);
    let id = db.table.id;
    let f = db.table.families[0].id;
    for k in [b"h", b"p"] {
        let m = db.engine.split_tablet_pending(id, k).unwrap();
        db.drive(m).unwrap();
    }
    // Tablets "a".., "h".., "p".. on shards 0, 1 and 2.
    for (row, to) in [(b"a", 0u16), (b"h", 1), (b"p", 2)] {
        let owner = db
            .ranges()
            .iter()
            .rfind(|r| r.2.as_slice() <= &row[..])
            .map(|r| r.1)
            .unwrap();
        if owner != to {
            let m = db.engine.move_tablet_pending(id, row, to).unwrap();
            db.drive(m).unwrap();
        }
    }
    // Shard 0's floor runs 50 ahead.
    for i in 0..50u8 {
        db.put(b"a", &[i]);
    }
    // A transaction reads "p" (on shard 2), then "p" changes: its commit over shards 0 and
    // 2 (coordinated by 0, at a timestamp above shard 2's floor) fails validation on 2.
    let mut txn = db.engine.begin().unwrap();
    txn.get(id, f, b"p", b"q").unwrap();
    db.put(b"p", b"changed");
    txn.batch()
        .put(id, f, b"a", b"q", None, ValueRef::Bytes(b"t"))
        .unwrap();
    txn.batch()
        .put(id, f, b"p", b"q", None, ValueRef::Bytes(b"t"))
        .unwrap();
    let mut t = txn.submit(Some(Durability::Buffered)).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let mut outcome = None;
    for _ in 0..10_000 {
        if let Poll::Ready(r) = Pin::new(&mut t).poll(&mut cx) {
            outcome = Some(r);
            break;
        }
        db.step();
    }
    assert!(
        matches!(outcome, Some(Err(pigeonhole_engine::Error::Conflict))),
        "{outcome:?}"
    );
    db.settle();
    // A commit over shards 1 and 2 (coordinated by 1): a later seqno, and a timestamp
    // below the aborted one's, since neither floor includes it.
    db.engine.take_appended();
    let mut wb = WriteBatch::new();
    wb.put(id, f, b"h", b"q", None, ValueRef::Bytes(b"c"))
        .unwrap();
    wb.put(id, f, b"p", b"q", None, ValueRef::Bytes(b"c"))
        .unwrap();
    let mut c = db.engine.submit(wb, Some(Durability::Buffered)).unwrap();
    let mut done = false;
    for _ in 0..10_000 {
        if let Poll::Ready(r) = Pin::new(&mut c).poll(&mut cx) {
            r.expect("commit");
            done = true;
            break;
        }
        db.step();
    }
    assert!(done, "the commit never resolved");
    let attempts = db
        .engine
        .take_appended()
        .iter()
        .filter(|r| r.stream == 1 && r.kind == pigeonhole_engine::AppendedKind::Prepare)
        .count();
    assert_eq!(attempts, 1, "the commit took {attempts} attempts");
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

/// D244's sweep (issue #653): seeded interleavings of single and cross-shard commits over
/// tablets spread on three shards, with bursts that run one shard's floor ahead and moves
/// mid-run. Each shard's WAL records of committed writes must have default timestamps that
/// never go backwards in seqno order (D11, D142): every tablet a shard owns is covered.
#[test]
fn default_timestamps_keep_seqno_order_on_every_shard_under_cross_shard_commits() {
    for seed in seeds() {
        d142_sweep(seed, true, true);
    }
}

/// D245's retry convergence on the sim's frozen clock, with the default write-stall
/// timeout: every commit commits (no `Busy`, no hang), and no PREPARE is refused for order.
/// Each shard step is atomic here, so no write can straddle a coordinator's floor read and
/// its reservation; the move-raise check is counted apart, being seqno-blind (D142).
#[test]
fn cross_shard_commits_converge_on_a_frozen_clock() {
    for seed in seeds() {
        d142_sweep(seed, true, false);
        d142_sweep(seed, false, false);
    }
}

#[test]
fn crossing_commits_regressions_from_the_seed_sweep() {
    // Seed 277 (#774's sweep): two crossing commits re-crossed on every round. The older one
    // was refused at the younger one's coordinator over its own share; the younger one,
    // refused behind the older one, was woken when the older one's attempt aborted there,
    // and both retried at once. A commit behind an older one now waits for that commit's
    // next attempt to commit, or its last (D263).
    d142_sweep(277, true, false);
    d142_sweep(277, false, false);
}

/// The same with tablet changes off (D244): three tables, one per shard (placement is
/// round-robin), and no moves.
#[test]
fn default_timestamps_keep_seqno_order_with_tablet_changes_off() {
    for seed in seeds() {
        d142_sweep(seed, false, true);
    }
}

fn d142_sweep(seed: u64, tablet_changes: bool, moving_clock: bool) {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut rng = move |n: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % n
    };
    // With `moving_clock`, a clock that moves (each read is a nanosecond) and a short
    // write-stall timeout, so `Busy` is possible; without, the sim's frozen clock and the
    // default timeout, so every commit must commit.
    let mut db = open(3, |o| {
        o.memtable_budget = 64 << 20;
        if moving_clock {
            o.write_stall_timeout_nanos = 200_000;
            ticking(o);
        }
        o.tablet_changes = tablet_changes;
    });
    let starts = *b"acegik";
    // Six targets, `(table, first row byte)`: with tablet changes on, six tablets of one
    // table ("a".., "c".., …), placed at random; off, three tables, one per shard.
    let mut tables = vec![Arc::clone(&db.table)];
    if tablet_changes {
        let id = db.table.id;
        for k in [b"c", b"e", b"g", b"i", b"k"] {
            let m = db.engine.split_tablet_pending(id, k).unwrap();
            db.drive(m).unwrap();
        }
        for s in starts {
            let to = rng(3) as u16;
            if let Ok(m) = db.engine.move_tablet_pending(id, &[s], to) {
                let _ = db.drive(m);
            }
        }
    } else {
        for i in 0..2 {
            let fam = [("f".into(), FamilyOptions::default())];
            tables.push(db.engine.create_table(&format!("t{i}"), &fam).unwrap());
        }
    }
    let target = |i: u64| -> (pigeonhole_format::TableId, pigeonhole_format::FamilyId, u8) {
        let t = &tables[if tablet_changes { 0 } else { i as usize % 3 }];
        (t.id, t.families[0].id, starts[i as usize])
    };
    let row = |rng: &mut dyn FnMut(u64) -> u64| {
        let (t, f, s) = target(rng(6));
        (t, f, vec![s, b'0' + rng(10) as u8])
    };
    let mut pending: Vec<pigeonhole_engine::PendingCommit> = Vec::new();
    let mut busy = 0;
    let mut moves: Vec<PendingMaintenance> = Vec::new();
    let mut cx = Context::from_waker(Waker::noop());
    db.engine.take_appended();
    for _ in 0..400 {
        match rng(10) {
            0..=3 => {
                let (id, f, r) = row(&mut rng);
                let mut wb = WriteBatch::new();
                wb.put(id, f, &r, b"q", None, ValueRef::Bytes(b"s"))
                    .unwrap();
                pending.push(db.engine.submit(wb, Some(Durability::Buffered)).unwrap());
            }
            4..=6 => {
                let mut wb = WriteBatch::new();
                for _ in 0..2 + rng(2) {
                    let (id, f, r) = row(&mut rng);
                    wb.put(id, f, &r, b"q", None, ValueRef::Bytes(b"x"))
                        .unwrap();
                }
                pending.push(db.engine.submit(wb, Some(Durability::Buffered)).unwrap());
            }
            7 => {
                // A burst on one tablet's rows: its shard's floor runs ahead of the others'.
                let (id, f, s) = target(rng(6));
                for i in 0..20 + rng(40) {
                    let mut wb = WriteBatch::new();
                    wb.put(
                        id,
                        f,
                        &[s, b'b', i as u8],
                        b"q",
                        None,
                        ValueRef::Bytes(b"b"),
                    )
                    .unwrap();
                    pending.push(db.engine.submit(wb, Some(Durability::Buffered)).unwrap());
                }
            }
            8 if tablet_changes && rng(8) == 0 && moves.is_empty() => {
                let (id, _, s) = target(rng(6));
                if let Ok(m) = db.engine.move_tablet_pending(id, &[s], rng(3) as u16) {
                    moves.push(m);
                }
            }
            _ => {
                // Some shards run, in a random order; the rest keep their queues.
                let now = db.clock.monotonic_nanos();
                for _ in 0..1 + rng(3) {
                    db.shards[rng(3) as usize].run_once(now + 1_000);
                }
            }
        }
        pending.retain_mut(|pc| match Pin::new(&mut *pc).poll(&mut cx) {
            Poll::Ready(r) => {
                match r {
                    Ok(_) => {}
                    Err(pigeonhole_engine::Error::Busy) => busy += 1,
                    Err(e) => panic!("seed {seed}: commit failed: {e}"),
                }
                false
            }
            Poll::Pending => true,
        });
        moves.retain_mut(|m| Pin::new(&mut *m).poll(&mut cx).is_pending());
    }
    for _ in 0..100_000 {
        if pending.is_empty() && moves.is_empty() {
            break;
        }
        db.step();
        pending.retain_mut(|pc| match Pin::new(&mut *pc).poll(&mut cx) {
            Poll::Ready(r) => {
                match r {
                    Ok(_) => {}
                    Err(pigeonhole_engine::Error::Busy) => busy += 1,
                    Err(e) => panic!("seed {seed}: commit failed: {e}"),
                }
                false
            }
            Poll::Pending => true,
        });
        moves.retain_mut(|m| Pin::new(&mut *m).poll(&mut cx).is_pending());
    }
    assert!(
        pending.is_empty(),
        "seed {seed}: {} commits never resolved ({} moves pending)",
        pending.len(),
        moves.len()
    );
    db.settle();
    // The writes that landed: singles, and the shares of committed cross-shard commits.
    let records = db.engine.take_appended();
    // D263: a COMMIT (DECISION) names the commit's id, which its PREPAREs
    // carry, and the seqno and timestamp the shares land at.
    let committed: std::collections::HashMap<u64, (u64, u64)> = records
        .iter()
        .filter(|r| r.kind == pigeonhole_engine::AppendedKind::Commit)
        .map(|r| (r.seqno, (r.apply_seqno, r.commit_ts)))
        .collect();
    let mut shares = 0;
    for stream in 0..3u16 {
        let mut writes: Vec<(u64, u64)> = records
            .iter()
            .filter(|r| r.stream == stream)
            .filter_map(|r| match r.kind {
                pigeonhole_engine::AppendedKind::Batch => Some((r.seqno, r.commit_ts)),
                pigeonhole_engine::AppendedKind::Prepare => committed.get(&r.seqno).copied(),
                pigeonhole_engine::AppendedKind::Commit => None,
            })
            .collect();
        shares += records
            .iter()
            .filter(|r| r.stream == stream && r.kind == pigeonhole_engine::AppendedKind::Prepare)
            .count();
        writes.sort_unstable();
        for w in writes.windows(2) {
            assert!(
                w[1].1 >= w[0].1,
                "seed {seed}: shard {stream}: seqno {} at ts {} after seqno {} at ts {} (D11, D142)",
                w[1].0,
                w[1].1,
                w[0].0,
                w[0].1
            );
        }
    }
    assert!(shares > 0, "seed {seed}: no cross-shard commit");
    if !moving_clock {
        assert_eq!(busy, 0, "seed {seed}: {busy} commits gave up");
    }
    eprintln!(
        "seed {seed}: {} records, {shares} shares, {busy} busy",
        records.len()
    );
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
}

#[test]
#[allow(deprecated)] // the D263-deprecated refusal counters stay 0
fn a_cross_shard_commit_no_participant_refuses_counts_one_round() {
    // ICR 0043: `Metrics::cross_shard` counts the coordinator's decided rounds, and only
    // refused ones as refused.
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
    let before = db.engine.metrics().cross_shard;
    db.put(b"a", b"single");
    assert_eq!(
        db.engine.metrics().cross_shard,
        before,
        "a one-shard commit"
    );
    let mut wb = WriteBatch::new();
    wb.put(id, f, b"a", b"q", None, ValueRef::Bytes(b"x"))
        .unwrap();
    wb.put(id, f, b"z", b"q", None, ValueRef::Bytes(b"x"))
        .unwrap();
    db.commit(wb);
    let after = db.engine.metrics().cross_shard;
    assert_eq!(after.rounds - before.rounds, 1);
    assert_eq!(after.refused_rounds, before.refused_rounds);
    assert_eq!(
        (
            after.refused_below_floor,
            after.refused_moved,
            after.refused_room
        ),
        (
            before.refused_below_floor,
            before.refused_moved,
            before.refused_room
        )
    );
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
        o.balance_cold_passes = Some(2); // cold after two unwritten passes
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
    engine.record_history(true);
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
        o.balance_cold_passes = Some(2); // cold after two unwritten passes
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
    fn clock_is_simulated(&self) -> bool {
        self.inner.clock_is_simulated()
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
#[allow(deprecated)] // the D263-deprecated refusal counters stay 0
fn a_commit_refused_many_times_before_the_timeout_still_commits() {
    // As below, but other tablet changes complete many times while the split waits: each
    // one retries the commit, and shard 0 refuses it again. It used to
    // give up with `Busy` after 16 refusals, long before the write-stall timeout; it now
    // waits for the split and commits (D241).
    let mut db = open(2, ticking);
    let u = two_shards_with_a_lost_tablet(&mut db);
    let t = Arc::clone(&db.table);
    let mut rows: Vec<Vec<u8>> = (0..20).map(key).collect();
    rows.sort();
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    // The split waits on a compaction of t held before its first slice (catalog changes
    // still commit meanwhile).
    db.engine.hold_compactions(true);
    let compact = db.engine.compact_pending(Some(t.id)).unwrap();
    for _ in 0..1_000 {
        if db.engine.compaction_held() {
            break;
        }
        db.step();
    }
    assert!(db.engine.compaction_held());
    let split = db.engine.split_tablet_pending(t.id, &rows[5]).unwrap();
    for _ in 0..50 {
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
    let before = db.engine.metrics().cross_shard;
    let mut pc = db.engine.submit(wb, Some(Durability::Buffered)).unwrap();
    // Shard 1 splits u's tablet again and again: each change retries the commit.
    for i in 0..48 {
        let m = db
            .engine
            .split_tablet_pending(u.id, format!("k{i:02}").as_bytes())
            .unwrap();
        db.drive(m).unwrap();
        assert!(poll_commit(&mut pc).is_none(), "resolved after {i} changes");
    }
    db.engine.hold_compactions(false);
    db.drive(compact).unwrap();
    db.drive(split).unwrap();
    let r = step_until(&mut db, &mut pc, 1_000);
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(db.get(&rows[1]).map(|(_, v)| v), Some(b"t".to_vec()));
    // Every refused round holds one `Moved` refusal per participant changing a tablet
    // (shard 0, and shard 1 while its split of u runs), and the last round commits
    // (ICR 0043).
    let after = db.engine.metrics().cross_shard;
    let refused = after.refused_rounds - before.refused_rounds;
    assert!(refused >= 20, "{refused} refused rounds");
    let moved = after.refused_moved - before.refused_moved;
    assert!(
        (refused..=2 * refused).contains(&moved),
        "{moved} refusals in {refused} rounds"
    );
    assert_eq!(after.rounds - before.rounds, refused + 1);
    assert_eq!(after.refused_below_floor, before.refused_below_floor);
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
    // #122: the refused move is counted as a slot-budget refusal, and so is a split on the
    // now full shard 2 (its tablet `["", "k4")` would add 21 slots to 63).
    let (slots, view) = db.engine.tablet_refusals();
    assert!(slots >= 1, "{:?}", db.engine.metrics());
    assert_eq!(view, 0);
    let m = db.engine.split_tablet_pending(id, b"k2").unwrap();
    let e = db.drive(m).unwrap_err();
    assert!(matches!(e, pigeonhole_engine::Error::Unsupported(_)), "{e}");
    assert_eq!(db.engine.tablet_refusals(), (slots + 1, 0));
    assert_eq!(on(&db, 2), 3, "{:?}", db.ranges());
    close(db);
}

#[test]
fn cold_tablets_spread_over_shards_consolidate_into_one() {
    // A table left in four small tablets alternating between two shards: neighbours never
    // share a shard, so merges alone never fire. Cold tablets move to their left
    // neighbour's shard and merge, until one tablet remains.
    let mut db = open(2, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.balance_cold_passes = Some(2); // cold after two unwritten passes
    });
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
        o.balance_cold_passes = Some(2); // cold after two unwritten passes
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

impl Db {
    /// The rows of a scan of `[start, end)` that stops after `limit` rows (0 = no limit).
    fn scan_limited(&self, start: &[u8], end: Option<&[u8]>, limit: u64) -> Vec<Vec<u8>> {
        let snap = self.engine.snapshot().unwrap();
        let end = end.map_or(Bound::Unbounded, |e| Bound::Excluded(e.to_vec()));
        let mut spec = ScanSpec::new(Bound::Included(start.to_vec()), end);
        spec.limit = limit;
        let mut c = self.engine.scan(&snap, self.table.id, spec).unwrap();
        let mut out = Vec::new();
        while c.next_row().unwrap() {
            out.push(c.row().to_vec());
        }
        out
    }

    /// Checks scans from many starts, with limits and end bounds, against `rows` (sorted).
    fn check_scans(&self, rows: &[Vec<u8>], when: &str) {
        let mut x = 0x2545_F491_4F6C_DD1Du64;
        for _ in 0..60 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let i = (x % rows.len() as u64) as usize;
            // An existing row, or a key just before it (no such row).
            let start = if x & 1 == 0 {
                rows[i].clone()
            } else {
                let mut k = rows[i].clone();
                k.pop();
                k
            };
            let from = rows.partition_point(|r| *r < start);
            for limit in [1u64, 3, 10, 0] {
                let take = if limit == 0 {
                    rows.len()
                } else {
                    limit as usize
                };
                let want: Vec<_> = rows[from..].iter().take(take).cloned().collect();
                assert_eq!(
                    self.scan_limited(&start, None, limit),
                    want,
                    "{when}: limit {limit}"
                );
            }
            let end = &rows[(i + 40).min(rows.len() - 1)];
            let to = rows.partition_point(|r| r < end);
            let want: Vec<_> = rows[from..to.max(from)].to_vec();
            assert_eq!(
                self.scan_limited(&start, Some(end), 0),
                want,
                "{when}: to {end:?}"
            );
        }
    }
}

/// #380: scans read each level below 0 through a cursor that opens its SSTs as it reaches
/// them. Limited and bounded scans from many starts match the rows over levels of several
/// SSTs, leveled and tiered, before and after a split (whose children share the parent's
/// SSTs until compaction rewrites them).
#[test]
fn limited_scans_over_levels_of_several_ssts_match_across_a_split() {
    for style in [CompactionStyle::Leveled, CompactionStyle::Tiered] {
        let mut db = open(1, |o| {
            o.balance_interval_nanos = u64::MAX;
            o.tablet_split_bytes = 64 << 20;
        });
        db.table = db
            .engine
            .create_table(
                "u",
                &[("f".into(), FamilyOptions::default().compaction(style))],
            )
            .expect("table");
        let value = vec![0x5A; 200];
        let mut rows: Vec<Vec<u8>> = (0..2_000).map(key).collect();
        for r in &rows {
            db.put(r, &value);
        }
        rows.sort();
        let m = db.engine.flush_pending().unwrap();
        db.drive(m).unwrap();
        let m = db.engine.compact_pending(None).unwrap();
        db.drive(m).unwrap();
        let table = db.table.id;
        let deepest_level_ssts = |db: &Db| {
            let levels = db.engine.sst_levels();
            (1..8u8)
                .map(|l| {
                    levels
                        .iter()
                        .filter(|(t, lv, _)| *t == table && *lv == l)
                        .count()
                })
                .max()
                .unwrap_or(0)
        };
        assert!(
            deepest_level_ssts(&db) >= 2,
            "{style:?}: no level below 0 with several SSTs: {:?}",
            db.engine.sst_levels()
        );
        db.check_scans(&rows, &format!("{style:?} compacted"));
        let m = db
            .engine
            .split_tablet_pending(table, &rows[rows.len() / 2])
            .unwrap();
        db.drive(m).unwrap();
        assert_eq!(db.ranges().len(), 2);
        db.check_scans(&rows, &format!("{style:?} split"));
        let m = db.engine.compact_pending(None).unwrap();
        db.drive(m).unwrap();
        db.check_scans(&rows, &format!("{style:?} split and compacted"));
        db.engine.close().unwrap();
        for _ in 0..8 {
            db.step();
        }
    }
}

/// An async transaction's snapshot must stay pinned until its validation runs. Here a
/// single-shard transaction reads row `a`, a delete of `a` commits after its snapshot, and
/// the transaction then waits behind an undecided cross-shard share on `a` while a full
/// compaction runs on its shard. Unpinned, the compaction purges the delete with the value
/// it hides (no live snapshot needs them), and validation no longer sees a write above the
/// snapshot: the transaction commits a lost update instead of failing with `Conflict`.
fn async_txn_after_a_purged_delete(
    pin: bool,
    compact: bool,
) -> pigeonhole_engine::Result<pigeonhole_engine::CommitInfo> {
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
    let (low, high) = (usize::from(low), usize::from(high));
    db.put(b"a", b"v0");
    db.put(b"z", b"z0");
    let fl = db.engine.submit_flush().unwrap();
    db.drive(fl).unwrap();

    // T reads `a` at its snapshot and writes it.
    let mut t = db.engine.begin().unwrap();
    assert!(t.get(id, f, b"a", b"q").unwrap().is_some());
    t.batch()
        .put(id, f, b"a", b"q", None, ValueRef::Bytes(b"t"))
        .unwrap();
    let held = pin.then(|| t.snapshot().clone());

    // A delete of `a` commits after T's snapshot, and reaches an SST.
    let mut wb = WriteBatch::new();
    wb.delete_family(id, f, b"a", None).unwrap();
    db.commit(wb);
    let fl = db.engine.submit_flush().unwrap();
    db.drive(fl).unwrap();

    // X: cross-shard (z first, so `high` coordinates), writing `a`, and doomed: its read of
    // `z` is overwritten after its snapshot, so `high` refuses its own share. `low` prepares
    // X's share on `a`, which stays undecided while `high` does not run.
    let mut x = db.engine.begin().unwrap();
    x.get(id, f, b"z", b"q").unwrap();
    x.batch()
        .put(id, f, b"z", b"q", None, ValueRef::Bytes(b"x"))
        .unwrap();
    x.batch()
        .put(id, f, b"a", b"q", None, ValueRef::Bytes(b"x"))
        .unwrap();
    db.put(b"z", b"z1");
    let mut px = x.submit(Some(Durability::Buffered)).unwrap();
    let now = db.clock.monotonic_nanos();
    db.shards[high].run_once(now + 1_000);
    db.shards[low].run_once(now + 1_000);

    // T is submitted (its snapshot handle goes with `t` unless `held` keeps it) and waits on
    // X's share in `low`'s pending queue.
    let t_seqno = t.snapshot().seqno();
    let mut pt = t.submit(Some(Durability::Buffered)).unwrap();
    // The waiting commit holds T's snapshot (#672).
    assert!(db.engine.live_snapshot_seqnos().contains(&t_seqno));
    // A full compaction runs on `low` alone, while X stays undecided.
    let _compact = compact.then(|| db.engine.submit_compact(Some(id)).unwrap());
    for _ in 0..10_000 {
        let now = db.clock.monotonic_nanos();
        if !db.shards[low].run_once(now + 1_000) {
            break;
        }
    }
    // Now everything runs: X aborts, T is admitted and validated.
    let mut cx = Context::from_waker(Waker::noop());
    let r = loop {
        if let Poll::Ready(r) = Pin::new(&mut pt).poll(&mut cx) {
            break r;
        }
        db.step();
    };
    while Pin::new(&mut px).poll(&mut cx).is_pending() {
        db.step();
    }
    drop(held);
    r
}

#[test]
fn an_async_transaction_conflicts_with_a_delete_purged_before_its_validation() {
    // The control: with the snapshot held, the delete survives the compaction.
    assert!(
        matches!(
            async_txn_after_a_purged_delete(true, true),
            Err(pigeonhole_engine::Error::Conflict)
        ),
        "control: a pinned snapshot must see the delete as a conflict"
    );
    // The second control: unpinned but with no compaction, the delete is still there.
    assert!(
        matches!(
            async_txn_after_a_purged_delete(false, false),
            Err(pigeonhole_engine::Error::Conflict)
        ),
        "control: without the compaction the delete must still conflict"
    );
    let r = async_txn_after_a_purged_delete(false, true);
    assert!(
        matches!(r, Err(pigeonhole_engine::Error::Conflict)),
        "an unpinned async transaction committed past a delete above its snapshot: {r:?}"
    );
}

/// Polls `pc` to its outcome, driving every shard.
fn resolve(
    db: &mut Db,
    pc: &mut pigeonhole_engine::PendingCommit,
) -> pigeonhole_engine::Result<pigeonhole_engine::CommitInfo> {
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(r) = Pin::new(&mut *pc).poll(&mut cx) {
            return r;
        }
        db.step();
    }
}

/// A two-shard store with rows below `m` on one shard and rows from `m` on the other.
fn split_db() -> Db {
    let mut db = open(2, |_| {});
    let id = db.table.id;
    let m = db.engine.split_tablet_pending(id, b"m").unwrap();
    db.drive(m).unwrap();
    let low = db.ranges()[0].1;
    let high = 1 - low;
    if db.ranges()[1].1 != high {
        let m = db.engine.move_tablet_pending(id, b"z", high).unwrap();
        db.drive(m).unwrap();
    }
    db
}

#[test]
fn a_transactions_commit_releases_its_snapshot_on_every_outcome() {
    // #672: the commit owns the transaction's snapshot until it resolves, whatever the
    // outcome, and whether or not anyone still polls it.
    let mut db = split_db();
    let id = db.table.id;
    let f = db.table.families[0].id;
    db.put(b"a", b"v0");
    db.put(b"z", b"z0");
    assert!(db.engine.live_snapshot_seqnos().is_empty());
    let put = |t: &mut pigeonhole_engine::Txn, row: &[u8]| {
        t.batch()
            .put(id, f, row, b"q", None, ValueRef::Bytes(b"t"))
            .unwrap();
    };

    // Committed, single-shard.
    let mut t = db.engine.begin().unwrap();
    t.get(id, f, b"a", b"q").unwrap();
    put(&mut t, b"a");
    let mut pc = t.submit(Some(Durability::Buffered)).unwrap();
    assert!(resolve(&mut db, &mut pc).is_ok());
    db.settle();
    assert_eq!(db.engine.live_snapshot_seqnos(), vec![], "committed");

    // Committed, cross-shard (the coordinator keeps a copy for retries).
    let mut t = db.engine.begin().unwrap();
    t.get(id, f, b"a", b"q").unwrap();
    t.get(id, f, b"z", b"q").unwrap();
    put(&mut t, b"z");
    put(&mut t, b"a");
    let mut pc = t.submit(Some(Durability::Buffered)).unwrap();
    assert!(resolve(&mut db, &mut pc).is_ok());
    db.settle();
    assert_eq!(
        db.engine.live_snapshot_seqnos(),
        vec![],
        "cross-shard committed"
    );

    // A real conflict, cross-shard.
    let mut t = db.engine.begin().unwrap();
    t.get(id, f, b"z", b"q").unwrap();
    put(&mut t, b"z");
    put(&mut t, b"a");
    db.put(b"z", b"z1");
    let mut pc = t.submit(Some(Durability::Buffered)).unwrap();
    assert!(matches!(
        resolve(&mut db, &mut pc),
        Err(pigeonhole_engine::Error::Conflict)
    ));
    db.settle();
    assert_eq!(db.engine.live_snapshot_seqnos(), vec![], "conflict");

    // The future dropped right after submitting: the commit still holds the snapshot until
    // it is validated, then lets it go.
    let mut t = db.engine.begin().unwrap();
    t.get(id, f, b"a", b"q").unwrap();
    put(&mut t, b"a");
    let seqno = t.snapshot().seqno();
    let pc = t.submit(Some(Durability::Buffered)).unwrap();
    assert_eq!(
        db.engine.live_snapshot_seqnos(),
        vec![seqno],
        "held while submitted"
    );
    drop(pc);
    assert_eq!(
        db.engine.live_snapshot_seqnos(),
        vec![seqno],
        "held after the drop"
    );
    db.settle();
    assert_eq!(db.engine.live_snapshot_seqnos(), vec![], "dropped future");

    // The async path, dropped before it was ever polled.
    let mut t = db.engine.begin().unwrap();
    t.get(id, f, b"a", b"q").unwrap();
    put(&mut t, b"a");
    drop(t.submit_async(Some(Durability::Buffered)).unwrap());
    db.settle();
    assert_eq!(
        db.engine.live_snapshot_seqnos(),
        vec![],
        "dropped async future"
    );
}

/// D249: a two-shard store with rows below `m` on one shard and rows from `m` on the other,
/// and the two shard indexes `(low, high)`.
fn overlap_db() -> (Db, usize, usize) {
    let mut db = open(2, |_| {});
    let id = db.table.id;
    let m = db.engine.split_tablet_pending(id, b"m").unwrap();
    db.drive(m).unwrap();
    let low = db.ranges()[0].1;
    let high = 1 - low;
    if db.ranges()[1].1 != high {
        let m = db.engine.move_tablet_pending(id, b"z", high).unwrap();
        db.drive(m).unwrap();
    }
    db.put(b"a", b"a0");
    db.put(b"y", b"y0");
    db.put(b"z", b"z0");
    (db, usize::from(low), usize::from(high))
}

/// A cross-shard transaction reading `reads` and writing `writes` (in that order, so the
/// first written row's shard coordinates).
fn overlap_txn(db: &Db, reads: &[&[u8]], writes: &[&[u8]]) -> pigeonhole_engine::Txn {
    let (id, f) = (db.table.id, db.table.families[0].id);
    let g = db.table.families[1].id;
    let mut t = db.engine.begin().unwrap();
    for r in reads {
        t.get(id, f, r, b"q").unwrap();
    }
    // D271: a write over an undecided share's write no longer overlaps it, so the rows it
    // writes are read too, in family `g`: an overlap on the row (refused over an undecided
    // write, D249) that never conflicts with the other commit's writes to `f`.
    for w in writes {
        t.get(id, g, w, b"q").unwrap();
    }
    for w in writes {
        t.batch()
            .put(id, f, w, b"q", None, ValueRef::Bytes(b"t"))
            .unwrap();
    }
    t
}

/// Runs one turn of shard `i` alone.
fn turn(db: &mut Db, i: usize) {
    let now = db.clock.monotonic_nanos();
    db.shards[i].run_once(now + 1_000);
}

#[test]
fn a_cross_shard_transaction_overlapping_an_undecided_share_retries_instead_of_conflicting() {
    // D249: B's PREPARE meets A's undecided share on `a`. It used to fail at once with
    // `Conflict` although nothing was written after B's snapshot; now it is refused, waits
    // for A's decision there, and commits. A aborts on its own real conflict.
    let (mut db, low, high) = overlap_db();
    let before = db.engine.metrics().cross_shard.refused_pending_overlap;
    // A: coordinated by `high` (`y` first), writes `a` on `low`; doomed, since its read of
    // `y` is overwritten after its snapshot.
    let a = overlap_txn(&db, &[b"y"], &[b"y", b"a"]);
    db.put(b"y", b"y1");
    let mut pa = a.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, high);
    // B: also coordinated by `high`, reads and writes `a`.
    let b = overlap_txn(&db, &[b"a"], &[b"z", b"a"]);
    let mut pb = b.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, high);
    // `low` gets A's PREPARE, then B's: one group, so B is cut to the next and refused there
    // while A is undecided.
    turn(&mut db, low);
    turn(&mut db, low);
    let rb = poll_until(&mut db, &mut pb, 100_000).expect("B resolved");
    let ra = poll_until(&mut db, &mut pa, 100_000).expect("A resolved");
    assert!(
        matches!(ra, Err(pigeonhole_engine::Error::Conflict)),
        "A: {ra:?}"
    );
    rb.expect("B commits: it only met an undecided share");
    assert!(
        db.engine.metrics().cross_shard.refused_pending_overlap > before,
        "B was never refused over the overlap"
    );
    // B, a commit that reads, committed on a retry; A failed, and no blind commit ran.
    let x = db.engine.metrics().cross_shard;
    assert!(x.max_attempts_reading >= 2, "{x:?}");
    assert_eq!(x.max_attempts_blind, 0, "{x:?}");
    assert_eq!(db.get(b"a").map(|(_, v)| v), Some(b"t".to_vec()));
}

#[test]
fn a_cross_shard_transaction_still_conflicts_with_a_write_after_its_snapshot() {
    // D249: the same overlap, but A commits a write to `a` after B's snapshot. B's retry
    // validates against it: a real conflict, `Conflict`.
    let (mut db, low, high) = overlap_db();
    let b = overlap_txn(&db, &[b"a"], &[b"z", b"a"]);
    let a = overlap_txn(&db, &[], &[b"y", b"a"]);
    let mut pa = a.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, high);
    let mut pb = b.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, high);
    turn(&mut db, low);
    turn(&mut db, low);
    let ra = poll_until(&mut db, &mut pa, 100_000).expect("A resolved");
    ra.expect("A commits");
    let rb = poll_until(&mut db, &mut pb, 100_000).expect("B resolved");
    assert!(
        matches!(rb, Err(pigeonhole_engine::Error::Conflict)),
        "B: {rb:?}"
    );
}

/// Polls both commits, stepping every shard, until both resolve.
fn resolve_both(
    db: &mut Db,
    pa: &mut pigeonhole_engine::PendingCommit,
    pb: &mut pigeonhole_engine::PendingCommit,
) -> (
    pigeonhole_engine::Result<pigeonhole_engine::CommitInfo>,
    pigeonhole_engine::Result<pigeonhole_engine::CommitInfo>,
) {
    let mut ra = None;
    let mut rb = None;
    for _ in 0..200_000 {
        if ra.is_none() {
            ra = poll_commit(pa);
        }
        if rb.is_none() {
            rb = poll_commit(pb);
        }
        if ra.is_some() && rb.is_some() {
            break;
        }
        db.step();
    }
    (
        ra.expect("A never resolved (deadlock or livelock)"),
        rb.expect("B never resolved (deadlock or livelock)"),
    )
}

#[test]
fn two_crossing_cross_shard_prepares_neither_deadlock_nor_livelock() {
    // D249: A (coordinated by `low`) and B (by `high`) both read and write `a` and `z`; each
    // prepares its own share and meets the other's undecided one. No PREPARE waits, so no
    // deadlock; on a frozen clock (no timer breaks a tie) both still resolve: at least one
    // commits, and the other commits or gets a real `Conflict` (the first one wrote a row it
    // read after its snapshot).
    let (mut db, low, high) = overlap_db();
    let before = db.engine.metrics().cross_shard.refused_pending_overlap;
    let a = overlap_txn(&db, &[b"a"], &[b"a", b"z"]);
    let b = overlap_txn(&db, &[b"z"], &[b"z", b"a"]);
    let mut pa = a.submit(Some(Durability::Buffered)).unwrap();
    let mut pb = b.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, low);
    turn(&mut db, high);
    let (ra, rb) = resolve_both(&mut db, &mut pa, &mut pb);
    let ok = |r: &pigeonhole_engine::Result<pigeonhole_engine::CommitInfo>| r.is_ok();
    assert!(ok(&ra) || ok(&rb), "neither committed: {ra:?} {rb:?}");
    for r in [&ra, &rb] {
        assert!(
            matches!(r, Ok(_) | Err(pigeonhole_engine::Error::Conflict)),
            "{r:?}"
        );
    }
    assert!(
        db.engine.metrics().cross_shard.refused_pending_overlap > before,
        "no overlap refusal: the crossing never happened"
    );
}

/// D249: three shards (`a` on 0, `k` on 1, `z` on 2). A (older, coordinated by shard 2)
/// writes `z`, `a`, `k`; B (younger, coordinated by shard 1) writes `k`, `a`. Driven in
/// `CROSSING` order, each shard admits one's PREPARE and refuses the other's: shard 1 has
/// B's own share first, shard 0 A's PREPARE first. Returns them submitted and crossed once.
fn crossed_pair() -> (
    Db,
    pigeonhole_engine::PendingCommit,
    pigeonhole_engine::PendingCommit,
    u64,
) {
    let mut db = open(3, |_| {});
    let id = db.table.id;
    for key in [b"h".as_slice(), b"p".as_slice()] {
        let m = db.engine.split_tablet_pending(id, key).unwrap();
        db.drive(m).unwrap();
    }
    for (row, shard) in [
        (b"a".as_slice(), 0u16),
        (b"k".as_slice(), 1),
        (b"z".as_slice(), 2),
    ] {
        let owner = db
            .ranges()
            .into_iter()
            .find(|r| r.2.as_slice() <= row && r.3.as_deref().is_none_or(|e| row < e))
            .map(|r| r.1)
            .unwrap();
        if owner != shard {
            let m = db.engine.move_tablet_pending(id, row, shard).unwrap();
            db.drive(m).unwrap();
        }
    }
    for row in [b"a".as_slice(), b"k", b"q", b"x", b"z"] {
        db.put(row, b"0");
    }
    let before = db.engine.metrics().cross_shard.refused_pending_overlap;
    // A: coordinated by shard 2 (`z` first), writes `a` (shard 0) and `k` (shard 1).
    let a = overlap_txn(&db, &[b"x"], &[b"z", b"a", b"k"]);
    // B: coordinated by shard 1 (`k` first), writes `a` too; its read (`q`) A never writes.
    let b = overlap_txn(&db, &[b"q"], &[b"k", b"a"]);
    let pa = a.submit(Some(Durability::Buffered)).unwrap();
    let pb = b.submit(Some(Durability::Buffered)).unwrap();
    // A's PREPAREs go out first; shard 1 then coordinates B (its share on `k`) ahead of A's
    // PREPARE there, and shard 0 gets A's PREPARE ahead of B's: each shard admits its first
    // one and refuses the other (cut, then refused while the first is undecided).
    turn(&mut db, 2);
    turn(&mut db, 1);
    turn(&mut db, 1);
    turn(&mut db, 0);
    turn(&mut db, 0);
    assert!(
        db.engine.metrics().cross_shard.refused_pending_overlap >= before + 2,
        "both were not refused: {}",
        db.engine.metrics().cross_shard.refused_pending_overlap - before
    );
    (db, pa, pb, before)
}

/// The turn order that crosses A and B: A's coordinator, then B's (its own share first, then
/// A's PREPARE cut and refused), then shard 0 (A's PREPARE first, B's cut and refused).
const CROSSING: [usize; 5] = [2, 1, 1, 0, 0];

#[test]
fn crossing_prepares_both_refused_resolve_older_first_on_a_frozen_clock() {
    // D249's older-first rule: A (older) and B (younger) are each refused at the other's
    // shard over the other's undecided share. B waits at A's shard until A's next attempt
    // there is decided, so A commits first, then B.
    let (mut db, mut pa, mut pb, _) = crossed_pair();
    let (ra, rb) = resolve_both(&mut db, &mut pa, &mut pb);
    let (a_info, b_info) = (ra.expect("A commits"), rb.expect("B commits"));
    assert!(
        a_info.seqno < b_info.seqno,
        "the older committed first: A {} B {}",
        a_info.seqno,
        b_info.seqno
    );
}

#[test]
fn crossing_prepares_driven_to_recross_every_retry_still_resolve_older_first() {
    // D249: the same pair, but every retry is driven in `CROSSING` order too, so whenever
    // both retry together they cross again. With older-first, B does not retry until A's
    // next attempt at shard 0 is decided, so they cross at most once more and both resolve.
    // Without it, both are woken together each time and cross on every retry: on a frozen
    // clock no timeout ends it, and neither ever resolves.
    let (mut db, mut pa, mut pb, before) = crossed_pair();
    let mut ra = None;
    let mut rb = None;
    for _ in 0..2_000 {
        for &i in &CROSSING {
            turn(&mut db, i);
        }
        if ra.is_none() {
            ra = poll_commit(&mut pa);
        }
        if rb.is_none() {
            rb = poll_commit(&mut pb);
        }
        if ra.is_some() && rb.is_some() {
            break;
        }
    }
    let refused = db.engine.metrics().cross_shard.refused_pending_overlap - before;
    let a_info = ra.expect("A never resolved: the pair re-crossed on every retry");
    let b_info = rb.expect("B never resolved: the pair re-crossed on every retry");
    let (a_info, b_info) = (a_info.expect("A commits"), b_info.expect("B commits"));
    assert!(a_info.seqno < b_info.seqno, "the older committed first");
    assert!(
        refused <= 4,
        "{refused} overlap refusals: they kept crossing"
    );
}

// D249's older-first guard, as ctr274 wrote it in their review of #681.
#[test]
fn a_younger_commit_refused_behind_an_older_one_waits_for_the_older_ones_next_attempt() {
    // D249's older-first rule, isolated: B (younger) is refused at shard 0 over A's (older)
    // undecided share. A's first round aborts; while A's coordinator (shard 2) is not run, A
    // cannot run its next attempt, so B must not retry, even though A's share at shard 0 is
    // decided and the overlap there is gone. Without the rule B is woken by that decision,
    // prepares and is decided first, so its seqno precedes A's (it cannot become visible
    // while shard 2 publishes no watermark, so the order of the commits is the check).
    let mut db = open(3, |_| {});
    let id = db.table.id;
    for key in [b"h".as_slice(), b"p".as_slice()] {
        let m = db.engine.split_tablet_pending(id, key).unwrap();
        db.drive(m).unwrap();
    }
    for (row, shard) in [
        (b"a".as_slice(), 0u16),
        (b"k".as_slice(), 1),
        (b"z".as_slice(), 2),
    ] {
        let owner = db
            .ranges()
            .into_iter()
            .find(|r| r.2.as_slice() <= row && r.3.as_deref().is_none_or(|e| row < e))
            .map(|r| r.1)
            .unwrap();
        if owner != shard {
            let m = db.engine.move_tablet_pending(id, row, shard).unwrap();
            db.drive(m).unwrap();
        }
    }
    for row in [b"a".as_slice(), b"j", b"k", b"x", b"z"] {
        db.put(row, b"0");
    }
    let before = db.engine.metrics().cross_shard.refused_pending_overlap;
    // A: coordinated by shard 2 (`z` first), writes `a` (shard 0) and `k` (shard 1).
    let a = overlap_txn(&db, &[b"x"], &[b"z", b"a", b"k"]);
    // B: coordinated by shard 1 (`k` first), writes `a`; it reads only on shard 1 (`j`), so
    // its rounds never need shard 2.
    let b = overlap_txn(&db, &[b"j"], &[b"k", b"a"]);
    let mut pa = a.submit(Some(Durability::Buffered)).unwrap();
    let mut pb = b.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, 2);
    turn(&mut db, 1);
    turn(&mut db, 1);
    turn(&mut db, 0);
    turn(&mut db, 0);
    assert!(
        db.engine.metrics().cross_shard.refused_pending_overlap >= before + 2,
        "both were not refused"
    );
    // Shard 2 decides A's first round (abort) and sends its decisions; then it stops.
    turn(&mut db, 2);
    for _ in 0..500 {
        turn(&mut db, 0);
        turn(&mut db, 1);
    }
    let (ra, rb) = resolve_both(&mut db, &mut pa, &mut pb);
    let (a_info, b_info) = (ra.expect("A commits"), rb.expect("B commits"));
    assert!(
        a_info.seqno < b_info.seqno,
        "B went ahead of the older A: A {} B {}",
        a_info.seqno,
        b_info.seqno
    );
}

/// This thread's CPU time in seconds. The shards are application-owned and driven here, so it
/// counts their work; unlike wall-clock time, other tests running beside it do not inflate
/// it (#795).
#[cfg(unix)]
fn thread_cpu_secs() -> f64 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    t.tv_sec as f64 + t.tv_nsec as f64 * 1e-9
}

/// CPU seconds a cross-shard commit takes whose first `aborts` rounds the participant refuses
/// (`refuse_prepares`, as overlapping an undecided share), on a two-shard store whose refusing
/// shard first logs `backlog` single-shard commits that its checkpoint cannot pass (nothing
/// flushes).
#[cfg(unix)]
fn refused_rounds_secs(backlog: usize, aborts: usize) -> f64 {
    let mut db = open(2, |o| {
        o.balance_interval_nanos = u64::MAX;
        o.memtable_budget = 256 << 20;
        o.memtable_freeze_bytes = 128 << 20;
        o.wal_pin_bytes = u64::MAX;
    });
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
    for i in 0..backlog {
        db.put(format!("z{i:08}").as_bytes(), b"v");
    }
    // One cross-shard commit coordinated by `high` (`x` first), which refuses its own first
    // `aborts` PREPAREs: each round aborts with no record of it in `high`'s log, the case
    // the scan read to its end, and the coordinator retries on the abort's wake.
    db.engine.refuse_prepares(usize::from(high), aborts as u64);
    let mut wb = WriteBatch::new();
    wb.put(id, f, b"x", b"q", None, ValueRef::Bytes(b"v"))
        .unwrap();
    wb.put(id, f, b"a", b"q", None, ValueRef::Bytes(b"v"))
        .unwrap();
    let start = thread_cpu_secs();
    db.commit(wb);
    let secs = thread_cpu_secs() - start;
    let refused = db.engine.metrics().cross_shard.refused_pending_overlap;
    assert!(
        refused >= aborts as u64,
        "only {refused} of {aborts} rounds were refused"
    );
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step();
    }
    secs
}

#[test]
#[cfg(unix)]
fn an_aborted_round_costs_the_same_whatever_the_log_holds() {
    // A refused round aborts, and each shard records the abort only if the commit's record is
    // in its log (`mark_aborted`). That check used to scan the log, which holds every
    // single-shard record since the checkpoint: a storm of refused rounds (application-owned
    // cross-shard batches, 2026-10-13) scanned it whole once per abort and saturated the
    // shards. It is a lookup now: the same refused rounds take about as long behind 100K
    // logged records as behind none. Measured in this thread's CPU time, not wall-clock
    // time, which a parallel test run inflated past the bound (#795).
    let aborts = 2_000;
    let small = refused_rounds_secs(0, aborts);
    let big = refused_rounds_secs(100_000, aborts);
    assert!(
        big < small * 3.0 + 0.02,
        "{aborts} refused rounds took {big:.3} CPU s behind 100K logged records, {small:.3} behind none"
    );
}

/// #772: D249's older-first rule leaves a younger commit refused behind an older one waiting
/// at that participant for the older one's next attempt there, its last, or `Released`. The
/// older commit A is refused with `Moved` (a move of its row `a` is held mid-flight by a held
/// compaction) while the younger transaction B is refused at shard 2 behind A's undecided
/// share. The move then lands `a` on shard 2, so A's retry is a single-shard commit there: no
/// further two-phase attempt reaches shard 2. It must still release B there, or B waits
/// until the write-stall timeout's `Busy`, which this test's clock never reaches.
#[test]
fn a_retry_that_lands_on_one_shard_releases_commits_waiting_behind_it() {
    // A clock that moves a nanosecond per read: the held compaction's retry timer needs it,
    // and the write-stall timeout (30 s) is never reached.
    let mut db = open(3, ticking);
    let id = db.table.id;
    for key in [b"h".as_slice(), b"p".as_slice()] {
        let m = db.engine.split_tablet_pending(id, key).unwrap();
        db.drive(m).unwrap();
    }
    for (row, shard) in [
        (b"a".as_slice(), 0u16),
        (b"k".as_slice(), 1),
        (b"z".as_slice(), 2),
    ] {
        let owner = db
            .ranges()
            .into_iter()
            .find(|r| r.2.as_slice() <= row && r.3.as_deref().is_none_or(|e| row < e))
            .map(|r| r.1)
            .unwrap();
        if owner != shard {
            let m = db.engine.move_tablet_pending(id, row, shard).unwrap();
            db.drive(m).unwrap();
        }
    }
    for row in [b"a".as_slice(), b"j", b"k", b"z"] {
        db.put(row, b"0");
    }
    let m = db.engine.flush_pending().unwrap();
    db.drive(m).unwrap();
    // The move of `a`'s tablet to shard 2 waits behind a held compaction: shard 0 refuses
    // PREPAREs touching `a` with `Moved` meanwhile.
    db.engine.hold_compactions(true);
    let compact = db.engine.compact_pending(Some(id)).unwrap();
    for _ in 0..1_000 {
        if db.engine.compaction_held() {
            break;
        }
        db.step();
    }
    assert!(db.engine.compaction_held());
    let mv = db.engine.move_tablet_pending(id, b"a", 2).unwrap();
    for _ in 0..50 {
        db.step();
    }
    let before = db.engine.metrics().cross_shard;
    // A (older): coordinated by shard 0 (`a` first), writes `z` on shard 2. B (younger):
    // coordinated by shard 1 (`k` first), reads `j` there, writes `z`.
    let a = overlap_txn(&db, &[], &[b"a", b"z"]);
    let b = overlap_txn(&db, &[b"j"], &[b"k", b"z"]);
    let mut pa = a.submit(Some(Durability::Buffered)).unwrap();
    let mut pb = b.submit(Some(Durability::Buffered)).unwrap();
    // Shard 0 refuses A's own share (`Moved`) and sends A's PREPARE to shard 2; shard 1
    // sends B's; shard 2 admits A's share, then refuses B behind it.
    turn(&mut db, 0);
    turn(&mut db, 1);
    turn(&mut db, 2);
    let mid = db.engine.metrics().cross_shard;
    assert!(
        mid.refused_moved > before.refused_moved,
        "A was not refused"
    );
    assert!(
        mid.refused_pending_overlap > before.refused_pending_overlap,
        "B was not refused behind A"
    );
    for _ in 0..200 {
        db.step();
    }
    assert!(poll_commit(&mut pa).is_none() && poll_commit(&mut pb).is_none());
    // The move lands: A's retry runs on shard 2 alone.
    db.engine.hold_compactions(false);
    db.drive(compact).unwrap();
    db.drive(mv).unwrap();
    assert_eq!(
        db.ranges()
            .into_iter()
            .find(|r| r.2.is_empty())
            .map(|r| r.1),
        Some(2),
        "`a` did not move to shard 2"
    );
    let (ra, rb) = resolve_both(&mut db, &mut pa, &mut pb);
    let (a_info, b_info) = (ra.expect("A commits"), rb.expect("B commits"));
    assert!(
        a_info.seqno < b_info.seqno,
        "B went ahead of the older A: A {} B {}",
        a_info.seqno,
        b_info.seqno
    );
    assert_eq!(db.get(b"z").map(|(_, v)| v), Some(b"t".to_vec()));
    close(db);
}

// ---------------------------------------------------------------------------------------
// D263: one round. A PREPARE carries an id; the seqno and timestamp are
// picked at the decision.
// ---------------------------------------------------------------------------------------

/// A blind single-shard put (one `low` shard turn after the PREPARE answered).
fn submit_blind(db: &Db, row: &[u8], value: &[u8]) -> pigeonhole_engine::PendingCommit {
    let (id, f) = (db.table.id, db.table.families[0].id);
    let mut wb = WriteBatch::new();
    wb.put(id, f, row, b"q", None, ValueRef::Bytes(value))
        .unwrap();
    db.engine.submit(wb, Some(Durability::Buffered)).unwrap()
}

/// Transaction C reads `a` on `low`, where it writes nothing (a validation-only
/// participant), and writes `z` on `high`, its coordinator. A blind single on `a` reaches
/// `low` after C's validation there answered `Prepared`, before the decision. C's seqno is
/// reserved at the decision: admitted then, the single would land below C while C never saw
/// it (write skew). It waits for the decision instead and lands above C.
#[test]
fn a_blind_single_after_a_validation_only_prepare_lands_above_the_commit() {
    let (mut db, low, high) = overlap_db();
    let c = overlap_txn(&db, &[b"a"], &[b"z"]);
    let mut pc = c.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, high);
    turn(&mut db, low);
    let mut ps = submit_blind(&db, b"a", b"blind");
    turn(&mut db, low);
    let rs = poll_until(&mut db, &mut ps, 100_000)
        .expect("the single resolved")
        .expect("the single commits");
    let rc = poll_until(&mut db, &mut pc, 100_000)
        .expect("C resolved")
        .expect("C commits: nothing was written to `a` before its decision");
    assert!(
        rc.seqno < rs.seqno,
        "the blind single landed at {} below C at {}, which validated `a` without it",
        rs.seqno,
        rc.seqno
    );
    assert_eq!(db.get(b"a").map(|(_, v)| v), Some(b"blind".to_vec()));
}

/// A single-shard transaction T on `low` reads `a` at a snapshot before cross-shard commit C
/// (which writes `a`) and commits while C's share there is undecided. T must not commit on
/// that stale read: it waits for the decision, then fails validation.
#[test]
fn a_single_shard_transaction_reading_an_undecided_shares_row_waits_and_conflicts() {
    let (mut db, low, high) = overlap_db();
    let (id, f) = (db.table.id, db.table.families[0].id);
    let mut t = db.engine.begin().unwrap();
    t.get(id, f, b"a", b"q").unwrap();
    t.batch()
        .put(id, f, b"b", b"q", None, ValueRef::Bytes(b"t"))
        .unwrap();
    let c = overlap_txn(&db, &[], &[b"z", b"a"]);
    let mut pc = c.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, high);
    turn(&mut db, low);
    let mut pt = t.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, low);
    poll_until(&mut db, &mut pc, 100_000)
        .expect("C resolved")
        .expect("C commits");
    let rt = poll_until(&mut db, &mut pt, 100_000).expect("T resolved");
    assert!(
        matches!(rt, Err(pigeonhole_engine::Error::Conflict)),
        "T committed on a read C overwrote: {rt:?}"
    );
}

/// (a)'s claim: a cross-shard commit holds back other commits' visibility only from its
/// decision until every participant applied, not during its PREPARE round. With C's PREPAREs
/// answered and its coordinator not yet run again, a single on another row of a participant
/// becomes visible on that participant's turns alone.
#[test]
fn a_single_is_not_held_back_by_a_commit_still_in_its_prepare_round() {
    let (mut db, low, high) = overlap_db();
    let c = overlap_txn(&db, &[], &[b"z", b"a"]);
    let mut pc = c.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, high);
    turn(&mut db, low);
    let mut ps = submit_blind(&db, b"b", b"free");
    let mut visible = None;
    for _ in 0..16 {
        turn(&mut db, low);
        if let Some(r) = poll_commit(&mut ps) {
            visible = Some(r);
            break;
        }
    }
    let r = visible.expect("the single waited for C's PREPARE round");
    r.expect("the single commits");
    poll_until(&mut db, &mut pc, 100_000)
        .expect("C resolved")
        .expect("C commits");
}

/// D26's never-reuse with provisional ids (perf287's point 3): a PREPARE that a crash left
/// undecided stays in its stream; the reopen raises the next seqno above its id, so no later
/// commit's DECISION can name it. Commits after the reopen, a second crash and reopen: the
/// stale share is never applied.
#[test]
fn an_undecided_prepare_left_by_a_crash_is_never_applied_after_more_commits_and_a_crash() {
    let vfs = SimVfs::new(717);
    let mut db = open_on(&vfs, 2, |_| {});
    let id = db.table.id;
    let m = db.engine.split_tablet_pending(id, b"m").unwrap();
    db.drive(m).unwrap();
    let low = usize::from(db.ranges()[0].1);
    let high = 1 - low;
    if usize::from(db.ranges()[1].1) != high {
        let m = db
            .engine
            .move_tablet_pending(id, b"z", high as u16)
            .unwrap();
        db.drive(m).unwrap();
    }
    db.put(b"a", b"a0");
    db.put(b"z", b"z0");
    db.engine.take_appended();
    // C: `low` logs its PREPARE (Buffered: in the file), the coordinator never decides.
    let c = overlap_txn(&db, &[], &[b"z", b"a"]);
    let _pc = c.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, high);
    turn(&mut db, low);
    let stale = db
        .engine
        .take_appended()
        .iter()
        .find(|r| {
            r.kind == pigeonhole_engine::AppendedKind::Prepare && usize::from(r.stream) == low
        })
        .map(|r| r.seqno)
        .expect("low logged C's PREPARE");
    vfs.crash(pigeonhole_io::sim::CrashKind::Process);
    drop(db);

    let mut db = open_on(&vfs, 2, |_| {});
    assert!(
        db.engine.next_seqno() > stale,
        "the reopen's next seqno {} does not pass the stale id {stale}",
        db.engine.next_seqno()
    );
    assert_eq!(
        db.get(b"a").map(|(_, v)| v),
        Some(b"a0".to_vec()),
        "C's share applied"
    );
    for i in 0..32u8 {
        let mut wb = WriteBatch::new();
        let f = db.table.families[0].id;
        for row in [&b"a"[..], b"z"] {
            wb.put(id, f, row, b"q", None, ValueRef::Bytes(&[b'n', i]))
                .unwrap();
        }
        db.commit(wb);
    }
    db.engine.take_appended();
    vfs.crash(pigeonhole_io::sim::CrashKind::Process);
    drop(db);

    let db = open_on(&vfs, 2, |_| {});
    for row in [&b"a"[..], b"z"] {
        assert_eq!(
            db.get(row).map(|(_, v)| v),
            Some(vec![b'n', 31]),
            "row {:?}",
            String::from_utf8_lossy(row)
        );
    }
}

/// Old files (perf287's point 4): a WAL written by 0.3.0 holds PREPAREs at the commit's own
/// seqno and timestamp and a type-3 COMMIT. Written here by hand after a crash (two
/// participant streams, the coordinator's COMMIT), this build replays the commit at that
/// seqno and timestamp.
#[test]
fn a_cross_shard_commit_in_a_0_3_0_wal_replays() {
    use pigeonhole_format::wal::{BatchBuilder, StreamList, WalRecord};
    use pigeonhole_format::{Kind, StreamId};
    use pigeonhole_wal::Wal as _;

    let vfs = SimVfs::new(718);
    let mut db = open_on(&vfs, 2, |_| {});
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
    db.put(b"a", b"a0");
    db.put(b"z", b"z0");
    let seqno = db.engine.next_seqno() + 10;
    let ts = db.get(b"z").unwrap().0.max(db.get(b"a").unwrap().0) + 1_000;
    vfs.crash(pigeonhole_io::sim::CrashKind::Process);
    drop(db);

    let path = Path::new("/db/data.phdb");
    let vfs_ref: pigeonhole_io::VfsRef = Arc::clone(&vfs) as pigeonhole_io::VfsRef;
    let manifest = Engine::inspect_manifest(&vfs_ref, path).unwrap();
    let db_id = pigeonhole_pager::Pager::open(&vfs_ref, path, false)
        .unwrap()
        .db_id();
    let share = |row: &[u8]| {
        let mut b = BatchBuilder::new();
        b.push(id, f, Kind::Put, row, b"q", None, b"\x00old")
            .unwrap();
        b
    };
    let coordinator = StreamId(u32::from(high));
    for (stream, row) in [(low, &b"a"[..]), (high, b"z")] {
        let stream = StreamId(u32::from(stream));
        let checkpoint = manifest
            .checkpoints
            .get(&stream)
            .copied()
            .unwrap_or_default();
        let mut rec =
            pigeonhole_wal::Recovery::open(&vfs_ref, path, stream, db_id, checkpoint).unwrap();
        while rec.next_record().unwrap().is_some() {}
        let mut wal = rec
            .into_stream(pigeonhole_wal::WalOptions::default())
            .unwrap();
        let b = share(row);
        wal.append(
            &WalRecord::Prepare {
                seqno,
                commit_ts: ts,
                coordinator,
                batch: b.batch(),
            },
            Durability::Sync,
        )
        .unwrap();
        if stream == coordinator {
            let mut list = Vec::new();
            StreamList::encode(
                &[StreamId(u32::from(low)), StreamId(u32::from(high))],
                &mut list,
            )
            .unwrap();
            wal.append(
                &WalRecord::Commit {
                    seqno,
                    participants: StreamList::new(&list).unwrap(),
                },
                Durability::Sync,
            )
            .unwrap();
        }
        wal.sync().unwrap();
    }

    let db = open_on(&vfs, 2, |_| {});
    for row in [&b"a"[..], b"z"] {
        assert_eq!(
            db.get(row),
            Some((ts, b"old".to_vec())),
            "row {:?}",
            String::from_utf8_lossy(row)
        );
    }
    assert!(db.engine.next_seqno() > seqno);
}

/// A blind cross-shard batch writing `value` to each of `rows` (the first row's shard
/// coordinates).
fn blind_txn(db: &Db, rows: &[&[u8]], value: &[u8]) -> pigeonhole_engine::Txn {
    let (id, f) = (db.table.id, db.table.families[0].id);
    let mut t = db.engine.begin().unwrap();
    for w in rows {
        t.batch()
            .put(id, f, w, b"q", None, ValueRef::Bytes(value))
            .unwrap();
    }
    t
}

#[test]
fn a_blind_prepare_over_an_undecided_share_is_admitted_and_answers_at_once() {
    // D271: B's PREPARE meets A's undecided share on `a`. Neither reads `a`, so B is admitted
    // and answers at once (D263 refused it, D267 chained it): each commit decides on its own,
    // and the one decided later lands above, in seqno and in timestamp (the one allocator).
    let (mut db, low, high) = overlap_db();
    let before = db.engine.metrics().cross_shard;
    let a = blind_txn(&db, &[b"y", b"a"], b"from-a");
    let mut pa = a.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, high);
    let b = blind_txn(&db, &[b"z", b"a"], b"from-b");
    let mut pb = b.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, high);
    turn(&mut db, low);
    turn(&mut db, low);
    let ra = poll_until(&mut db, &mut pa, 100_000).expect("A resolved");
    let rb = poll_until(&mut db, &mut pb, 100_000).expect("B resolved");
    let (ra, rb) = (ra.expect("A commits"), rb.expect("B commits"));
    let after = db.engine.metrics().cross_shard;
    assert_eq!(
        after.refused_pending_overlap, before.refused_pending_overlap,
        "B was refused"
    );
    assert_eq!(after.refused_rounds, before.refused_rounds);
    assert_eq!(after.chained, 0);
    // Both blind commits, each committed on its first attempt.
    assert_eq!(
        (after.max_attempts_blind, after.max_attempts_reading),
        (1, 0)
    );
    let latest: &[u8] = if rb.seqno > ra.seqno {
        b"from-b"
    } else {
        b"from-a"
    };
    assert_eq!(db.get(b"a").map(|(_, v)| v), Some(latest.to_vec()));
}

/// Turns `high`, then `low`.
fn turn_both(db: &mut Db, low: usize, high: usize) {
    turn(db, high);
    turn(db, low);
}

/// Polls `pc` once; `Some` once it resolved.
fn resolved(pc: &mut pigeonhole_engine::PendingCommit) -> bool {
    poll_commit(pc).is_some_and(|r| {
        r.expect("commits");
        true
    })
}

/// Lets every commit in `stream` resolve; each commits.
fn drain(db: &mut Db, stream: Vec<pigeonhole_engine::PendingCommit>) {
    for mut pc in stream {
        poll_until(db, &mut pc, 200_000)
            .expect("resolved")
            .expect("commits");
    }
}

#[test]
fn a_blind_single_on_an_undecided_shares_row_goes_ahead() {
    // D271: a single writing `a` while cross-shard shares on `a` are undecided goes ahead:
    // it lands below them (its seqno and timestamp come before their decisions'), so it need
    // not wait (D263 held it until each decision). The shares' coordinator, `high`, never runs
    // while the single resolves.
    let (mut db, low, high) = overlap_db();
    let mut stream = Vec::new();
    for i in 0..4u8 {
        let t = blind_txn(&db, &[b"y", b"a"], &[b's', i]);
        stream.push(t.submit(Some(Durability::Buffered)).unwrap());
    }
    turn(&mut db, high);
    turn(&mut db, low);
    let before = db.engine.metrics().cross_shard.share_waits;
    let table = db.table.clone();
    let mut single = submit_put(&db, &table, b"a");
    let mut went_in = false;
    for _ in 0..8 {
        turn(&mut db, low);
        if resolved(&mut single) {
            went_in = true;
            break;
        }
    }
    assert!(went_in, "the single waited for the undecided shares");
    assert_eq!(db.engine.metrics().cross_shard.share_waits, before);
    drain(&mut db, stream);
}

#[test]
fn a_reading_commit_gets_in_against_a_stream_of_blind_commits() {
    // D267 (perf287's R2): a commit that reads cannot chain. Refused on `a`, which a stream
    // of blind commits keeps undecided, and older than every share there, it fences `a`:
    // younger blind commits are refused there, the row drains and its retry gets in.
    let (mut db, low, high) = overlap_db();
    let mut stream = Vec::new();
    for i in 0..4u8 {
        let t = blind_txn(&db, &[b"a", b"z"], &[b's', i]);
        stream.push(t.submit(Some(Durability::Buffered)).unwrap());
        turn_both(&mut db, low, high);
    }
    let reader = overlap_txn(&db, &[b"b"], &[b"y", b"a"]);
    let mut reader = reader.submit(Some(Durability::Buffered)).unwrap();
    let mut went_in = None;
    for i in 0..60u8 {
        for j in 0..2u8 {
            let t = blind_txn(&db, &[b"a", b"z"], &[b'n', i, j]);
            stream.push(t.submit(Some(Durability::Buffered)).unwrap());
        }
        turn_both(&mut db, low, high);
        if went_in.is_none() && resolved(&mut reader) {
            went_in = Some(i);
        }
    }
    let went_in = went_in.expect("the reading commit never went in");
    assert!(
        went_in <= 30,
        "the reading commit went in only after {went_in} rounds"
    );
    drain(&mut db, stream);
}

#[test]
fn a_crash_with_two_shares_on_one_row_applies_the_decided_one_and_drops_the_other() {
    // D271: two undecided PREPAREs on `a` in `low`'s stream (D263 allowed one, D267 a chain):
    // A, coordinated by `high`, is decided (its DECISION logged there) but not applied on
    // `low`; B, coordinated by `low`, is never decided. A crash: the reopen applies A from its
    // DECISION and drops B.
    let vfs = SimVfs::new(267);
    let mut db = open_on(&vfs, 2, |_| {});
    let id = db.table.id;
    let m = db.engine.split_tablet_pending(id, b"m").unwrap();
    db.drive(m).unwrap();
    let low = usize::from(db.ranges()[0].1);
    let high = 1 - low;
    if usize::from(db.ranges()[1].1) != high {
        let m = db
            .engine
            .move_tablet_pending(id, b"z", high as u16)
            .unwrap();
        db.drive(m).unwrap();
    }
    db.put(b"a", b"a0");
    db.put(b"y", b"y0");
    db.put(b"z", b"z0");
    db.engine.take_appended();
    let a = blind_txn(&db, &[b"y", b"a"], b"from-a");
    let _pa = a.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, high);
    let b = blind_txn(&db, &[b"a", b"z"], b"from-b");
    let _pb = b.submit(Some(Durability::Buffered)).unwrap();
    // `low` logs A's PREPARE and B's own (B's decision waits for `high`'s answer).
    turn(&mut db, low);
    turn(&mut db, low);
    // `high` decides A and logs its DECISION, and answers B; `low` never runs again.
    let mut decided = false;
    for _ in 0..8 {
        turn(&mut db, high);
        decided |= db.engine.take_appended().iter().any(|r| {
            r.kind == pigeonhole_engine::AppendedKind::Commit && usize::from(r.stream) == high
        });
        if decided {
            break;
        }
    }
    assert!(decided, "A's DECISION was never logged");
    vfs.crash(pigeonhole_io::sim::CrashKind::Process);
    drop(db);

    let db = open_on(&vfs, 2, |_| {});
    assert_eq!(db.get(b"a").map(|(_, v)| v), Some(b"from-a".to_vec()));
    assert_eq!(db.get(b"y").map(|(_, v)| v), Some(b"from-a".to_vec()));
    assert_eq!(db.get(b"z").map(|(_, v)| v), Some(b"z0".to_vec()));
}

/// Three shards, one tablet each: `a` on shard 0, `n` on shard 1, `z` on shard 2.
fn three_tablet_db() -> Db {
    let mut db = open(3, |_| {});
    let id = db.table.id;
    for key in [&b"m"[..], b"t"] {
        let m = db.engine.split_tablet_pending(id, key).unwrap();
        db.drive(m).unwrap();
    }
    // `ranges` lists the tablets in key order: [.., m), [m, t), [t, ..).
    for (i, key) in [&b"a"[..], b"n", b"z"].into_iter().enumerate() {
        let shard = i as u16;
        if db.ranges()[i].1 != shard {
            let m = db.engine.move_tablet_pending(id, key, shard).unwrap();
            db.drive(m).unwrap();
        }
    }
    for row in [&b"a"[..], b"n", b"z"] {
        db.put(row, b"0");
    }
    db
}

#[test]
fn a_prepare_refused_by_a_fence_waits_behind_the_reader() {
    // D267 (perf287's R4): a refused reading commit, the oldest on `a`, fences it. A
    // younger blind commit refused there by the fence waits behind the reader (D249's
    // older-first wake), not for `a`'s shares to clear: with the reader's retry held back
    // (its coordinator, shard 1, does not run) and `a` clear, it would be woken at once and
    // refused again every round.
    let mut db = three_tablet_db();
    // The blind commits (coordinated by shard 0, `a` then `z`) are queued at shard 0 first;
    // the reader (coordinated by shard 1) takes the oldest id there, and its PREPARE reaches
    // shard 0 after them, so it meets their undecided shares, younger, and fences `a`.
    let mut stream = Vec::new();
    for i in 0..2u8 {
        let t = blind_txn(&db, &[b"a", b"z"], &[b's', i]);
        stream.push(t.submit(Some(Durability::Buffered)).unwrap());
    }
    let reader = overlap_txn(&db, &[b"b"], &[b"n", b"a"]);
    let mut reader = reader.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, 1);
    turn(&mut db, 0);
    // Shard 1 stands still: the reader's retry cannot run, and the fence stands. The chain
    // on `a` drains (shards 0 and 2), and a younger blind commit arrives every round.
    let before = db.engine.metrics().cross_shard.refused_pending_overlap;
    let rounds = 30;
    for i in 0..rounds as u8 {
        let t = blind_txn(&db, &[b"a", b"z"], &[b'n', i]);
        stream.push(t.submit(Some(Durability::Buffered)).unwrap());
        turn(&mut db, 0);
        turn(&mut db, 2);
    }
    let refused = db.engine.metrics().cross_shard.refused_pending_overlap - before;
    assert!(
        refused <= rounds + 2,
        "{refused} refusals for {rounds} fenced commits: refused ones are woken and refused again"
    );
    // Shard 1 runs again: the reader commits, then the fenced commits.
    poll_until(&mut db, &mut reader, 200_000)
        .expect("reader resolved")
        .expect("reader commits");
    drain(&mut db, stream);
}

#[test]
fn a_younger_reading_commit_is_not_held_by_a_fence() {
    // D267: a fence keeps younger blind commits from chaining on a row an older refused
    // reading commit waits for. A younger reading commit cannot chain, and goes in on the
    // row as under D249. Held by the fence too, each reading commit on a hot row waited out
    // an older one's whole retry, and the bench's transfer row (reading commits only) ran
    // at half main's rate (#830's box A/B).
    let mut db = three_tablet_db();
    let mut stream = Vec::new();
    for i in 0..2u8 {
        let t = blind_txn(&db, &[b"a", b"z"], &[b's', i]);
        stream.push(t.submit(Some(Durability::Buffered)).unwrap());
    }
    let reader = overlap_txn(&db, &[b"b"], &[b"n", b"a"]);
    let mut reader = reader.submit(Some(Durability::Buffered)).unwrap();
    turn(&mut db, 1);
    turn(&mut db, 0);
    // Shard 1 stands still: the reader's retry cannot run, and its fence on `a` stands. The
    // chain on `a` drains, and a younger reading commit on `a` goes in and commits.
    let younger = overlap_txn(&db, &[b"c"], &[b"a", b"z"]);
    let mut younger = younger.submit(Some(Durability::Buffered)).unwrap();
    let mut committed = false;
    for _ in 0..200 {
        turn(&mut db, 0);
        turn(&mut db, 2);
        if let Some(r) = poll_commit(&mut younger) {
            r.expect("the younger reading commit commits");
            committed = true;
            break;
        }
    }
    assert!(committed, "the younger reading commit is held by the fence");
    poll_until(&mut db, &mut reader, 200_000)
        .expect("reader resolved")
        .expect("reader commits");
    drain(&mut db, stream);
}

#[test]
fn a_fence_ends_with_its_readers_last_attempt_even_with_nothing_waiting() {
    // D267: a reading commit R (reads `b`, writes `y` and `a`), the oldest on `a`, is
    // refused on `low` over a younger blind commit's share there and fences `a`. `b` is then
    // written, so R's retry fails validation on `low` itself: never tracked there, its last
    // attempt is decided while `low` holds no refusal waiting (R's own was woken when `a`
    // cleared). The fence must still end; it ended only when a refusal was waiting, and
    // later blind commits on `a` were refused behind a commit that was gone (the bench's
    // transfer smoke stalled for the write-stall timeout).
    let (mut db, low, high) = overlap_db();
    let b1 = blind_txn(&db, &[b"a", b"z"], b"b1");
    let mut b1 = b1.submit(Some(Durability::Buffered)).unwrap();
    let r = overlap_txn(&db, &[b"b"], &[b"y", b"a"]);
    let mut r = r.submit(Some(Durability::Buffered)).unwrap();
    // `high` gives R the oldest id; `low` admits B1's share, then refuses R's PREPARE.
    turn(&mut db, high);
    turn(&mut db, low);
    assert!(
        db.engine.metrics().cross_shard.refused_pending_overlap > 0,
        "R was not refused on `a`"
    );
    // A write to `b` after R's snapshot, admitted on `low` before R's retry.
    let table = db.table.clone();
    let mut put = submit_put(&db, &table, b"b");
    turn(&mut db, low);
    poll_until(&mut db, &mut put, 100_000)
        .expect("the put resolved")
        .expect("the put commits");
    let rr = poll_until(&mut db, &mut r, 100_000).expect("R resolved");
    assert!(
        matches!(rr, Err(pigeonhole_engine::Error::Conflict)),
        "R: {rr:?}"
    );
    poll_until(&mut db, &mut b1, 100_000)
        .expect("B1 resolved")
        .expect("B1 commits");
    // A later blind commit on `a` is not fenced by R, which is gone.
    let before = db.engine.metrics().cross_shard.refused_pending_overlap;
    let b2 = blind_txn(&db, &[b"a", b"z"], b"b2");
    let mut b2 = b2.submit(Some(Durability::Buffered)).unwrap();
    poll_until(&mut db, &mut b2, 100_000)
        .expect("B2 resolved: refused behind a fence nothing ends")
        .expect("B2 commits");
    assert_eq!(
        db.engine.metrics().cross_shard.refused_pending_overlap,
        before
    );
}

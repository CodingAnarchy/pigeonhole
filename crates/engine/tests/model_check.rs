//! The model-checked simulation suite: the engine under `Sim` against the reference model,
//! with fault injection, crashes between and inside commits, every shard count from 1 to 8
//! (64 behind `PIGEONHOLE_SHARDS_64`), shard-count changes across reopens, and a crash at
//! every write point of a short run (including every step of a cross-shard commit).

mod common;

use common::{Config, final_dump, run};
use pigeonhole_format::Durability;
use pigeonhole_io::Vfs;

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

fn check(seed: u64, cfg: &Config) {
    match run(seed, cfg) {
        Ok(stats) => eprintln!("seed {seed}: {stats:?}"),
        Err(f) => panic!("{f}"),
    }
}

#[test]
fn quiet_runs_match_the_model_for_every_shard_count() {
    for shards in 1..=8 {
        let mut cfg = Config::quiet(300);
        cfg.shards = shards;
        for seed in seeds() {
            check(seed, &cfg);
        }
    }
}

#[test]
fn faults_and_crashes_for_every_shard_count() {
    for shards in 1..=8 {
        let mut cfg = Config::standard(250);
        cfg.shards = shards;
        for seed in seeds() {
            check(seed, &cfg);
        }
    }
}

#[test]
fn sixty_four_shards_behind_env_var() {
    if std::env::var("PIGEONHOLE_SHARDS_64").is_err() {
        eprintln!("skipped: set PIGEONHOLE_SHARDS_64=1");
        return;
    }
    let mut cfg = Config::standard(400);
    cfg.shards = 64;
    cfg.memtable_budget = 1 << 20;
    for seed in seeds() {
        check(seed, &cfg);
    }
}

#[test]
fn results_are_identical_across_shard_counts() {
    for seed in seeds() {
        let mut cfg = Config::quiet(250);
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
fn recovery_with_a_changed_shard_count() {
    for seed in seeds() {
        let mut cfg = Config::standard(400);
        cfg.shards = 3;
        cfg.reopen_shards = vec![1, 5, 2, 8, 4];
        check(seed, &cfg);
    }
}

#[test]
fn durability_matrix() {
    // Each level under process kills and power losses (the fault plan tears and reorders).
    for level in [
        Durability::None,
        Durability::Buffered,
        Durability::GroupSync,
        Durability::Sync,
    ] {
        let mut cfg = Config::standard(200);
        cfg.durability = Some(level);
        cfg.crash_ppm = 40_000;
        cfg.mid_commit_crash_ppm = 60_000;
        cfg.shards = 3;
        for seed in seeds() {
            check(seed, &cfg);
        }
    }
}

#[test]
fn crash_at_every_write_point() {
    // Short runs whose batches span shards (rows spread over 4 shards, big batches, groups
    // of concurrent commits), so the sweep crosses every step of two-phase commits: PREPARE
    // writes, syncs, the COMMIT record, and the applies after it. Every mutating operation
    // of each run is a crash point.
    let mut cfg = Config::quiet(30);
    cfg.shards = 4;
    cfg.spec.read_fraction = 0.2;
    cfg.spec.max_batch = 6;
    cfg.spec.rows = 12;
    cfg.faults.torn_writes = true;
    let step: u64 = std::env::var("PIGEONHOLE_SWEEP_STEP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    for seed in seeds() {
        let total = count_ops(seed, &cfg);
        eprintln!("seed {seed}: sweeping {total} crash points");
        let mut n = 1;
        while n <= total {
            let mut c = cfg.clone();
            c.crash_at = Some(n);
            if let Err(f) = run(seed, &c) {
                panic!("seed {seed}, crash after mutating op {n}: {f}");
            }
            n += step;
        }
    }
}

#[test]
fn io_errors_poison_shards_and_recover_on_reopen() {
    // Random read and write failures: a failed write or sync poisons the stream, commits
    // on it fail until the reopen, and whatever was acknowledged survives.
    let mut cfg = Config::standard(250);
    cfg.faults.io_error_ppm = 20_000;
    cfg.crash_ppm = 5_000;
    cfg.mid_commit_crash_ppm = 5_000;
    cfg.shards = 3;
    for seed in seeds() {
        let stats = run(seed, &cfg).unwrap_or_else(|f| panic!("{f}"));
        eprintln!("seed {seed}: {stats:?}");
        assert!(
            stats.io_errors > 0 || stats.crashes > 1,
            "seed {seed}: no I/O error was injected"
        );
    }
}

#[test]
fn results_are_identical_across_shard_counts_under_faults() {
    // Torn and reordered unsynced writes plus a process crash and reopen every 60 ops:
    // everything written survives a process crash, so every shard count recovers the same
    // commits and reads the same results.
    for seed in seeds() {
        let mut cfg = Config::standard(250);
        cfg.crash_every = Some(60);
        cfg.shards = 1;
        let reference = final_dump(seed, &cfg);
        for shards in [2, 3, 5, 8] {
            cfg.shards = shards;
            let dump = final_dump(seed, &cfg);
            assert_eq!(
                dump, reference,
                "seed {seed}: {shards} shards differ from 1 shard"
            );
        }
    }
}

/// Mutating operations a run performs before its final crash.
fn count_ops(seed: u64, cfg: &Config) -> u64 {
    let sim = pigeonhole_sim::Sim::new(seed);
    let vfs = sim.vfs();
    let mut store =
        common::Store::open(&vfs, cfg.shards, cfg.memtable_budget, &common::families()).unwrap();
    let base = vfs.now_micros();
    use pigeonhole_sim::{ModelOp, Op, Workload};
    for op in Workload::new(seed ^ 0x5eed, common::TABLE, cfg.spec.clone()).take(cfg.ops) {
        vfs.advance(1_000);
        if let Op::Commit(mut ops, durability) = op {
            for o in &mut ops {
                common::place(o);
                match o {
                    ModelOp::Put { ts: Some(t), .. } | ModelOp::DeleteCell { ts: t, .. } => {
                        *t += base
                    }
                    _ => {}
                }
            }
            let batch = store.batch(&ops).unwrap();
            let mut pc = store.engine.submit(batch, Some(durability)).unwrap();
            loop {
                match common::poll_commit(&mut pc) {
                    std::task::Poll::Ready(r) => {
                        r.unwrap();
                        break;
                    }
                    std::task::Poll::Pending => store.step_shards(vfs.monotonic_nanos()),
                }
            }
        }
    }
    let n = vfs.mutating_ops();
    let _ = store.engine.close();
    for _ in 0..4 {
        store.step_shards(vfs.monotonic_nanos());
    }
    drop(sim);
    n
}

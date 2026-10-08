//! The model-checked simulation suite: the engine under `Sim` against the reference model,
//! with fault injection, crashes between and inside commits, every shard count from 1 to 8
//! (64 behind `PIGEONHOLE_SHARDS_64`), shard-count changes across reopens, and a crash at
//! every write point of a short run (including every step of a cross-shard commit).

mod common;

use std::path::Path;

use common::{Config, final_dump, read_after_background_crash, run, run_traced};

use pigeonhole_format::Durability;
use pigeonhole_io::sim::{SimOp, SimVfs};
use pigeonhole_io::{OpenOptions, Vfs};
use pigeonhole_sim::Op;

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
fn runs_with_tablet_changes_off_match_the_model() {
    // Tablet changes are on by default (D129 as amended); the off path keeps its coverage.
    for shards in [1, 3] {
        for mut cfg in [Config::quiet(300), Config::standard(250)] {
            cfg.shards = shards;
            cfg.tablet_changes = false;
            cfg.balance_fast = false;
            for seed in seeds() {
                check(seed, &cfg);
            }
        }
    }
}

#[test]
fn harness_regressions_from_the_seed_sweep() {
    // Seed 248: an armed crash fired on background I/O and the held snapshots' re-check
    // after a commit found the store dead (issue #62's pattern). Seed 288: a surviving
    // share fit two in-flight commits (a family delete inside another commit's row
    // delete) and the greedy matcher gave it to the wrong one.
    for shards in 1..=8 {
        let mut cfg = Config::standard(250);
        cfg.shards = shards;
        for seed in [248, 288] {
            check(seed, &cfg);
        }
    }
    // Seeds 114 and 274 (issue #162): a refused attempt's PREPARE, settled at one recovery,
    // was still in the WAL at the next, fit an unacknowledged commit made in between (a row
    // delete of the same row) and took it from the commit whose share really survived.
    for seed in [114, 274] {
        check(seed, &read_error_txn_config());
    }
}

/// Transactions under frequent injected read errors, no crash armed.
fn read_error_txn_config() -> Config {
    let mut cfg = Config::standard(250);
    cfg.faults.io_error_ppm = 20_000;
    cfg.txn_ppm = 600_000;
    cfg.crash_ppm = 0;
    cfg.mid_commit_crash_ppm = 0;
    cfg
}

#[test]
fn failed_compactions_back_off_after_a_crash() {
    // Issue #79: after a power loss (seed 19) or an injected I/O error (seed 83) killed the
    // file handles, a failed compaction was retried at once and for ever with the L0 score
    // at 1.0; on the simulated clock `run_once` never returned, and the client never ran.
    let mut cfg = Config::standard(400);
    cfg.shards = 3;
    cfg.reopen_shards = vec![1, 5, 2, 8, 4];
    check(19, &cfg);
    let mut cfg = Config::standard(250);
    cfg.faults.io_error_ppm = 8_000;
    cfg.crash_ppm = 5_000;
    cfg.mid_commit_crash_ppm = 5_000;
    cfg.shards = 3;
    check(83, &cfg);
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
fn a_seed_replays_the_same_io_trace() {
    // Issue #61: every mutating SimVfs operation, background work included (WAL spare
    // preparation, flushes, compactions, the manifest pump), is scheduled by the simulator,
    // so a seed replays the same I/O in the same order. Twice in a row, then on several
    // threads at once (OS scheduling must not leak into a run). The harness runs
    // `check_and_mutate` and transactions (blocking calls) on threads of its own, so those
    // are off here.
    let mut cfg = Config::standard(250);
    cfg.shards = 3;
    (cfg.cas_ppm, cfg.txn_ppm) = (0, 0);
    // Spares are prepared only once a stream is half way through the segment it opened in,
    // and only where no checkpointed slot can be recycled (#143): one stream that grows
    // without crashes or size-triggered flushes.
    let mut spares = Config::standard(1000);
    spares.shards = 1;
    (spares.cas_ppm, spares.txn_ppm) = (0, 0);
    (spares.crash_ppm, spares.mid_commit_crash_ppm) = (0, 0);
    (spares.flush_ppm, spares.compact_ppm) = (0, 0);
    // The first freeze comes after half the arena of data, far past half a segment of WAL.
    spares.memtable_freeze_bytes = spares.memtable_budget / 2;
    // `SpareSegments::prepare`'s I/O: grow the file by a slot, zero-fill it (one write: the
    // harness's segments are 256 KiB), then `sync_all`, with nothing in between.
    let zeros = {
        let vfs = SimVfs::new(0);
        let f = vfs
            .open(Path::new("/zeros"), OpenOptions::read_write_create())
            .unwrap();
        vfs.record_ops();
        f.write_at(&[0; 8 * 32 * 1024], 0).unwrap();
        vfs.recorded_ops().pop().expect("recorded")
    };
    let prepared_a_spare = |ops: &[SimOp]| {
        ops.windows(3).any(|w| match (&w[0], &w[1], &w[2], &zeros) {
            (
                SimOp::SetLen { node, len },
                SimOp::Write {
                    node: n1,
                    offset,
                    len: l,
                    digest,
                },
                SimOp::Sync {
                    node: n2,
                    metadata: true,
                },
                SimOp::Write {
                    len: zl,
                    digest: zd,
                    ..
                },
            ) => node == n1 && node == n2 && offset + l == *len && (l, digest) == (zl, zd),
            _ => false,
        })
    };
    for (seed, cfg, spare) in seeds()
        .into_iter()
        .flat_map(|seed| [(seed, &cfg, false), (seed, &spares, true)])
    {
        let traced = |cfg: &Config| {
            let (result, ops) = run_traced(seed, cfg);
            if let Err(f) = result {
                panic!("{f}");
            }
            ops
        };
        let reference = traced(cfg);
        assert!(
            !spare || prepared_a_spare(&reference),
            "seed {seed}: no WAL spare segment was zero-filled"
        );
        let same = |ops: &[SimOp], run: &str| {
            if let Some(i) =
                (0..reference.len().max(ops.len())).find(|&i| reference.get(i) != ops.get(i))
            {
                panic!(
                    "seed {seed}: {run} diverged at op {i} of {}: {:?} vs {:?}",
                    reference.len(),
                    reference.get(i),
                    ops.get(i)
                );
            }
        };
        same(&traced(cfg), "the second run");
        std::thread::scope(|scope| {
            let runs: Vec<_> = (0..8).map(|_| scope.spawn(|| traced(cfg))).collect();
            for (t, run) in runs.into_iter().enumerate() {
                same(&run.join().unwrap(), &format!("parallel run {t}"));
            }
        });
    }
}

/// Background compaction off and a full compaction at fixed points: a bottommost compaction
/// may purge deletes (decision D74), after which a write with an older explicit timestamp
/// reads differently, so purges must happen at the same points for every shard count.
fn deterministic_compactions(cfg: &mut Config) {
    cfg.compaction.l0_trigger = u32::MAX;
    cfg.compaction.level_base_bytes = u64::MAX;
    cfg.compact_every = Some(50);
}

#[test]
fn results_are_identical_across_shard_counts() {
    for seed in seeds() {
        let mut cfg = Config::quiet(250);
        deterministic_compactions(&mut cfg);
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
    // The same, flush-heavy: tiny memtables, frequent flushes and compactions, so the
    // sweep crosses SST writes, manifest commits, checkpoints and compaction outputs.
    let mut heavy = Config::quiet(20);
    heavy.shards = 4;
    heavy.spec.read_fraction = 0.2;
    heavy.spec.max_batch = 6;
    heavy.spec.rows = 12;
    heavy.faults.torn_writes = true;
    heavy.memtable_freeze_bytes = 2 << 10;
    heavy.flush_ppm = 150_000;
    heavy.compact_ppm = 100_000;
    // Every point of the commit-only runs; every third of the flush-heavy ones by default
    // (about 1,300 points, a few minutes in CI); `PIGEONHOLE_SWEEP_STEP` sets both.
    let env_step: Option<u64> = std::env::var("PIGEONHOLE_SWEEP_STEP")
        .ok()
        .and_then(|s| s.parse().ok());
    for (name, cfg, step) in [
        ("commits", &cfg, env_step.unwrap_or(1)),
        ("flush-heavy", &heavy, env_step.unwrap_or(3)),
    ] {
        for seed in seeds() {
            // The crash points are exactly the mutating operations of the uncrashed run
            // of this seed and config (the same workload, flushes and compactions).
            let total = run(seed, cfg)
                .unwrap_or_else(|f| panic!("{name} seed {seed} without a crash: {f}"))
                .mutating_ops;
            eprintln!("{name} seed {seed}: sweeping {total} crash points");
            let mut n = 1;
            while n <= total {
                let mut c = cfg.clone();
                c.crash_at = Some(n);
                if let Err(f) = run(seed, &c) {
                    panic!("{name} seed {seed}, crash after mutating op {n}: {f}");
                }
                n += step;
            }
        }
    }
}

#[test]
fn io_errors_poison_shards_and_recover_on_reopen() {
    // Random read and write failures: a failed write or sync poisons the stream, commits
    // on it fail until the reopen, and whatever was acknowledged survives.
    let mut cfg = Config::standard(250);
    cfg.faults.io_error_ppm = 8_000;
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
fn undecided_prepares_pass_the_checkpoint() {
    // Issue #181: a cross-shard commit whose stream failed before its coordinator logged a
    // COMMIT is aborted, and a participant's checkpoint passes its PREPARE at once (D116)
    // while the other participants' PREPAREs survive. The checker took that hole for a lost
    // commit. No CAS or transaction runs on a thread here, so these seeds replay exactly
    // (found by a seed search over 1..=3000).
    for seed in [328, 422] {
        let stats = run(seed, &undecided_prepares_config()).unwrap_or_else(|f| panic!("{f}"));
        assert!(
            stats.undecided_prepares_passed > 0,
            "seed {seed} no longer reaches the case: {stats:?}"
        );
    }
}

/// I/O errors and crashes on three shards, every commit on the test's thread.
fn undecided_prepares_config() -> Config {
    let mut cfg = Config::standard(250);
    cfg.faults.io_error_ppm = 8_000;
    cfg.crash_ppm = 5_000;
    cfg.mid_commit_crash_ppm = 5_000;
    cfg.shards = 3;
    cfg.cas_ppm = 0;
    cfg.txn_ppm = 0;
    // The pinned seeds reach the case with tablet changes off; a sweep's
    // `PIGEONHOLE_TABLET_CHANGES=1` must not change their workload.
    cfg.tablet_changes = false;
    cfg.balance_fast = false;
    cfg
}

#[test]
fn an_injected_read_error_in_a_transaction_read_abandons_the_transaction() {
    // Issue #99: a transaction's read can hit an injected read error. That is an expected
    // `Io` error (nothing was submitted), not a divergence from the model. Many steps are
    // transactions and reads fail often, so the sweep reaches it; no crash is armed, so
    // D127's probe rule is not in play.
    let cfg = read_error_txn_config();
    let mut io_errors = 0;
    for seed in seeds() {
        let stats = run(seed, &cfg).unwrap_or_else(|f| panic!("{f}"));
        io_errors += stats.io_errors;
    }
    assert!(io_errors > 0, "no I/O error was injected");
}

#[test]
fn results_are_identical_across_shard_counts_under_faults() {
    // Torn and reordered unsynced writes plus a process crash and reopen every 60 ops:
    // everything written survives a process crash, so every shard count recovers the same
    // commits and reads the same results. Every commit is at least `Buffered`: a `None`
    // commit survives only if a stronger commit on its own stream writes it or a flush
    // persists it, which depends on the shard count (decision #50).
    for seed in seeds() {
        let mut cfg = Config::standard(250);
        deterministic_compactions(&mut cfg);
        cfg.crash_every = Some(60);
        cfg.durability = Some(Durability::Buffered);
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

/// A get of every row, then a scan: the steps that meet a crash fired in the background.
fn reads_of_every_row(cfg: &Config) -> Vec<Op> {
    (0..cfg.spec.rows)
        .map(|i| Op::Get {
            row: format!("row{i:06}").into_bytes(),
            family: "f".into(),
            qualifier: b"q0".to_vec(),
        })
        .chain([Op::Scan {
            start: b"row".to_vec(),
            end: b"rox".to_vec(),
        }])
        .collect()
}

#[test]
fn a_checkpointed_commit_keeps_its_default_timestamp_beside_an_explicit_one() {
    // #163: a `Durability::None` commit put one column twice, at its default timestamp and
    // at an explicit one just above it. With tablet changes on, the checker reads such a
    // commit's timestamp from its entries, and took the explicit entry's: the rebuilt model
    // collapsed the default put onto it (D34) and lost a version the engine kept.
    let mut cfg = Config::quiet(30);
    (cfg.flush_ppm, cfg.compact_ppm) = (0, 0);
    (cfg.cas_ppm, cfg.txn_ppm, cfg.tasks) = (0, 0, 1);
    cfg.tablet_changes = true;
    cfg.balance_fast = false;
    for seed in [10, 84, 263] {
        let stats = read_after_background_crash(seed, &cfg, &reads_of_every_row(&cfg))
            .unwrap_or_else(|f| panic!("{f}"));
        assert_eq!(stats.background_crashes, 1, "seed {seed}");
    }
}

#[test]
fn a_read_after_a_background_fired_crash_recovers() {
    // Issue #62: an armed power loss fires on a shard's background flush with nothing in
    // flight; the next read step meets the dead store (its SSTs' handles died with the
    // crash) and must recover from that crash, not report a read mismatch.
    let mut cfg = Config::quiet(30);
    // Nothing random between the steps: no explicit maintenance, one plain commit at a time.
    (cfg.flush_ppm, cfg.compact_ppm) = (0, 0);
    (cfg.cas_ppm, cfg.txn_ppm, cfg.tasks) = (0, 0, 1);
    let reads = reads_of_every_row(&cfg);
    for seed in seeds() {
        let stats =
            read_after_background_crash(seed, &cfg, &reads).unwrap_or_else(|f| panic!("{f}"));
        eprintln!("seed {seed}: {stats:?}");
        assert_eq!(
            stats.background_crashes, 1,
            "seed {seed}: no read step met the background-fired crash"
        );
    }
}

//! The model-checked simulation suite: the engine under `Sim` against the reference model,
//! with fault injection, crashes between and inside commits, every shard count from 1 to 8
//! (64 behind `PIGEONHOLE_SHARDS_64`), shard-count changes across reopens, and a crash at
//! every write point of a short run (including every step of a cross-shard commit).

mod common;

use std::path::Path;

use common::{
    Config, FailureClass, final_dump, lost_decision_reuses_its_seqno, read_after_background_crash,
    run, run_traced, undecided_prepare_passes,
};

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
        Ok(stats) => {
            eprintln!("seed {seed}: {stats:?}");
            // On a fault-free run (no I/O fault, no crash) the clock moves 1 µs per operation
            // and nothing can stall a commit for the write-stall timeout, so a commit that
            // gave up with `Busy` waited for something that never came: a liveness bug, such
            // as D267's fence that outlived its reader (#830). Fail it, not count it.
            if fault_free(cfg) {
                assert_eq!(
                    stats.busy, 0,
                    "seed {seed}: {} commit(s) gave up with Busy on a fault-free run: {stats:?}",
                    stats.busy
                );
            }
        }
        Err(f) => panic!("{f}"),
    }
}

/// No injected I/O fault and no crash of any kind.
fn fault_free(cfg: &Config) -> bool {
    cfg.faults == pigeonhole_io::sim::FaultPlan::none()
        && cfg.crash_ppm == 0
        && cfg.mid_commit_crash_ppm == 0
        && cfg.observed_crash_ppm == 0
        && cfg.crash_at.is_none()
        && cfg.crash_every.is_none()
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

/// D225 (#585): a participant trims a PREPARE once its commit is settled (every share in
/// SSTs), not once the coordinator checkpointed the COMMIT. Many cross-shard commits in
/// flight together (crossing each other's records), frequent flushes (so commits settle
/// early and participants trim before coordinators) and crashes between and inside commits:
/// a replay that finds a COMMIT whose participant trimmed its PREPARE must lose nothing and
/// apply nothing twice.
#[test]
fn settled_commits_trimmed_by_participants_survive_crashes() {
    for shards in 2..=4 {
        let mut cfg = Config::standard(400);
        cfg.shards = shards;
        cfg.tasks = 16;
        cfg.spec.max_batch = 8;
        cfg.flush_ppm = 100_000;
        cfg.crash_ppm = 20_000;
        cfg.mid_commit_crash_ppm = 30_000;
        for seed in seeds() {
            check(seed, &cfg);
        }
        // The same with tablets splitting, merging and moving while the commits run: a slot
        // that moved, or a tablet retired by a split or merge, reads as flushed through the
        // published catalog, and a participant may trim its PREPARE on that.
        cfg.tablet_changes = true;
        cfg.tablet_ops_ppm = 20_000;
        cfg.balance_fast = true;
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
fn a_snapshot_taken_before_a_compaction_publishes_reads_its_own_view() {
    // Issue #204 (seed 193 of the deferred-I/O sweep): a bottommost compaction decided to
    // purge a row delete, then a commit applied a put at an older explicit timestamp, then
    // a snapshot was taken, all before the compaction's root commit was durable and its view
    // published. The put is a later write (D74, D147): visible once the purge is published.
    // The snapshot reads the view it was taken in, where the delete still hides it, and
    // that read must never change. The harness took the purge at its `max_seqno` for every
    // read point, the snapshot's included, and reported `SnapshotChanged`.
    let mut cfg = Config::standard(250);
    cfg.shards = 1;
    (cfg.tablet_changes, cfg.balance_fast) = (false, false);
    cfg.deferred_io = true;
    let stats = run(193, &cfg).unwrap_or_else(|f| panic!("{f}"));
    assert!(
        stats.reads_at_older_views > 0,
        "seed 193 no longer reaches the case: {stats:?}"
    );
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
    // Seeds 99, 132 and 164 with deferred I/O (#454's first sweep): an open left its new
    // stream files' directory sync in flight (D203), a process crash dropped it, the next
    // open adopted the files and acknowledged commits in them, and a power loss then took
    // the files' names. Recovery now syncs an adopted stream's directory entry.
    for shards in 1..=8 {
        let mut cfg = Config::standard(250);
        cfg.shards = shards;
        cfg.deferred_io = true;
        for seed in [132, 164] {
            check(seed, &cfg);
        }
    }
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
        cfg.deferred_io = true;
        for seed in [99, 132] {
            check(seed, &cfg);
        }
    }
    // Seed 213 with deferred I/O (#454's second sweep): closing after the last reopen, a
    // shard's final sync waited, inside the harness's own turn, for cross-shard barrier
    // syncs that only the harness completes. The simulator's device now runs for the
    // harness thread's blocked waits (ICR 0028).
    identical_across_shard_counts_under_faults(213, Some(true));
    // Seed 41 with deferred I/O (#508): a power loss during a rollover left a successor's
    // records in a recycled slot without their header; the 8 -> 4 shard reopen took that
    // header's epoch in the same slot, and the records (a PREPARE of a lost commit) replayed
    // at the next check. Recovery now skips that epoch (D209). The failure depended on real
    // threads' timing, so the seed reproduces it only sometimes; the WAL's own crash tests
    // reproduce it deterministically.
    let mut cfg = Config::standard(400);
    cfg.shards = 3;
    cfg.reopen_shards = vec![1, 5, 2, 8, 4];
    cfg.deferred_io = true;
    check(41, &cfg);
    // #774's sweep (D263): a retried cross-shard commit whose coordinator's stream had
    // failed was answered with the I/O error before any further attempt, and the commit
    // refused behind it at its earlier participants (its coordinator among them) waited for
    // ever. Its coordinator now sends them `Released`. Seed 266 without deferred I/O, and
    // seeds 19, 88 and 240 of the read-error runs with it.
    let mut cfg = io_errors_config();
    cfg.deferred_io = false;
    check(266, &cfg);
    let mut cfg = read_error_txn_config();
    cfg.deferred_io = true;
    for seed in [19, 88, 240] {
        check(seed, &cfg);
    }
}

#[test]
fn deferred_io_runs_take_the_commit_time_separation_path() {
    // #306: with deferred I/O the harness thread is the device, and a commit separating a
    // large value (#230) waits on the manifest writer; that wait runs the device (ICR 0028)
    // instead of parking, so deferred runs separate values as plain runs do.
    for seed in seeds() {
        let share = |deferred: bool| {
            let mut cfg = Config::standard(250);
            cfg.shards = 3;
            cfg.deferred_io = deferred;
            let s = run(seed, &cfg).unwrap_or_else(|f| panic!("{f}"));
            assert!(s.large_puts > 0, "seed {seed}: no large puts");
            eprintln!(
                "seed {seed} deferred={deferred}: {} blob files for {} large puts",
                s.blob_files, s.large_puts
            );
            s.blob_files as f64 / s.large_puts as f64
        };
        let (plain, deferred) = (share(false), share(true));
        assert!(
            plain > 0.0 && deferred >= plain / 2.0 && deferred <= plain * 2.0,
            "seed {seed}: blob files per large put {deferred:.2} deferred, {plain:.2} plain"
        );
    }
}

/// Transactions under frequent injected read errors, no crash armed.
fn read_error_txn_config() -> Config {
    let mut cfg = Config::standard(250);
    cfg.faults.io_error_ppm = 20_000;
    cfg.txn_ppm = 600_000;
    cfg.crash_ppm = 0;
    cfg.mid_commit_crash_ppm = 120_000;
    cfg
}

/// #308: with tablet changes off, seed 266 crashes after three cross-shard commits failed
/// at a poisoned stream, each leaving only a PREPARE. An earlier refused attempt's PREPARE
/// held a row-delete marker that one of those commits also writes. The recovery check
/// took that commit for the refused attempt's seqno and then found the commit's own PREPARE
/// unexplained (`RecoveredFromTheFuture`). The engine recovered neither (no COMMIT, D83).
#[test]
fn a_refused_attempts_prepare_does_not_take_an_unacknowledged_commit() {
    let mut cfg = read_error_txn_config();
    cfg.tablet_changes = false;
    cfg.balance_fast = false;
    if let Err(f) = run(266, &cfg) {
        panic!("{f}");
    }
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
    // A spare's preparation: grow the file by a slot, zero-fill it (one write: the
    // harness's segments are 256 KiB), then `sync_all`, in that order. It runs in steps on
    // the shard's background task (D35), so other I/O may come in between.
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
        let SimOp::Write {
            len: zl,
            digest: zd,
            ..
        } = zeros
        else {
            unreachable!("a write was recorded")
        };
        ops.iter().enumerate().any(|(i, op)| {
            let SimOp::SetLen { node, len } = *op else {
                return false;
            };
            let filled = ops[i + 1..].iter().position(|w| {
                matches!(*w, SimOp::Write { node: n, offset, len: l, digest }
                    if n == node && offset + l == len && (l, digest) == (zl, zd))
            });
            filled.is_some_and(|j| {
                ops[i + 1 + j + 1..]
                    .iter()
                    .any(|s| matches!(*s, SimOp::Sync { node: n, metadata: true } if n == node))
            })
        })
    };
    // And with deferred I/O, whose device the harness thread runs, including in the waits
    // of commits that separate large values (#306).
    let mut deferred = cfg.clone();
    deferred.deferred_io = true;
    for (seed, cfg, spare) in seeds().into_iter().flat_map(|seed| {
        [
            (seed, &cfg, false),
            (seed, &spares, true),
            (seed, &deferred, false),
        ]
    }) {
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

#[test]
fn deferred_io_stays_in_flight_and_a_seed_replays() {
    // Issue #145: with deferred I/O, WAL group syncs and root commits stay in flight across
    // shard slices and client steps until the scheduler completes them, in an order the
    // seed picks, and a seed still replays the same I/O. No helper-thread commits (they
    // would let the OS order a run, see above).
    let mut cfg = Config::quiet(200);
    cfg.shards = 3;
    (cfg.cas_ppm, cfg.txn_ppm) = (0, 0);
    cfg.deferred_io = true;
    for seed in seeds() {
        let (result, ops) = run_traced(seed, &cfg);
        let stats = result.unwrap_or_else(|f| panic!("{f}"));
        eprintln!("seed {seed}: {stats:?}");
        assert!(
            stats.io_in_flight_steps > 0,
            "seed {seed}: no client step saw I/O in flight"
        );
        let (again, replayed) = run_traced(seed, &cfg);
        again.unwrap_or_else(|f| panic!("{f}"));
        assert!(
            ops == replayed,
            "seed {seed}: the second run's I/O diverged"
        );
    }
}

/// Background compaction off and a full compaction at fixed points: a bottommost compaction
/// may purge deletes (decision D74), after which a write with an older explicit timestamp
/// reads differently, so purges must happen at the same points for every shard count.
fn deterministic_compactions(cfg: &mut Config) {
    cfg.compaction.l0_trigger = u32::MAX;
    cfg.compaction.tiered_max_space_amp_percent = u32::MAX;
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
fn crashes_check_every_shards_flushed_through_seqno() {
    // #724: at every crash the checker asserts that each acknowledged write at or below its
    // shard's published flushed-through seqno is in an SST the durable manifest names
    // (`check_flushed_through`). With tablet changes on, so moves and splits are covered
    // (perf287). The check must have had writes to look at, or it shows nothing.
    let mut cfg = Config::standard(250);
    cfg.shards = 3;
    cfg.tablet_changes = true;
    cfg.flush_ppm = 100_000;
    let mut checked = 0;
    for seed in seeds() {
        let stats = run(seed, &cfg).unwrap_or_else(|f| panic!("{f}"));
        checked += stats.flushed_through_checks;
    }
    assert!(
        checked > 0,
        "no write was ever at or below a published flushed-through seqno"
    );
}

#[test]
fn io_errors_poison_shards_and_recover_on_reopen() {
    // Random read and write failures: a failed write or sync poisons the stream, commits
    // on it fail until the reopen, and whatever was acknowledged survives.
    let cfg = io_errors_config();
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
fn io_errors_with_deferred_io_keep_every_record_the_crash_poll_and_the_close_leave() {
    // #860, two seeds of `io_errors_poison_shards_and_recover_on_reopen` with deferred I/O:
    // - 107: a commit resolved `OutcomeUnknown` in the same poll as, and after, one that met
    //   the crash; the checker dropped it, and its surviving record read as a commit the
    //   client never made;
    // - 245: the dead store's close logged a decision it had been waiting for (the armed
    //   crash fired later in the close); the checker drained the append order before the
    //   close, so the WAL's record of it read as out of order.
    // D267 (#830) changed the timing that reaches both. Each seed fails with its half of
    // the fix reverted.
    let mut cfg = io_errors_config();
    cfg.deferred_io = true;
    for seed in [107, 245] {
        let stats = run(seed, &cfg).unwrap_or_else(|f| panic!("{f}"));
        if seed == 107 && stats.crash_poll_unacked == 0 {
            // Not reached with this target's families (`counters.rs` runs this file with
            // others): the seed still runs as a regression check.
            eprintln!("seed 107 resolved no commit beside a crash here");
        }
    }
}

#[test]
fn acknowledged_commits_resolved_beside_a_crash_are_checked() {
    // #860, the dangerous half: commits acknowledged in the same poll as one that met a
    // crash were dropped from the model, so losing one would go unseen (and a surviving one
    // read as a commit the client never made). The generator never resolves one there on its
    // own; holding acknowledgements back until a failure (`Config::hold_acks`) does. Every
    // seed must pass, and some must reach the case. With that branch of the fix reverted,
    // the acknowledged commits are dropped and the check fails.
    let mut cfg = io_errors_config();
    cfg.deferred_io = true;
    cfg.hold_acks = true;
    // Durable acknowledgements: a crash may drop a `Buffered` one, so losing it from the
    // model would show nowhere.
    cfg.durability = Some(Durability::Sync);
    cfg.crash_ppm = 100_000;
    cfg.mid_commit_crash_ppm = 100_000;
    cfg.tasks = 8;
    let mut reached = 0;
    for seed in 1..=200u64 {
        let stats = run(seed, &cfg).unwrap_or_else(|f| panic!("{f}"));
        reached += stats.crash_poll_acked;
    }
    assert!(
        reached > 0,
        "no seed resolved an acknowledged commit beside a crash"
    );
}

/// [`io_errors_poison_shards_and_recover_on_reopen`]'s configuration.
fn io_errors_config() -> Config {
    let mut cfg = Config::standard(250);
    cfg.faults.io_error_ppm = 8_000;
    cfg.crash_ppm = 5_000;
    cfg.mid_commit_crash_ppm = 5_000;
    cfg.shards = 3;
    cfg
}

#[test]
fn undecided_prepares_pass_the_checkpoint() {
    // Issue #181: a cross-shard commit whose coordinator's stream failed before it logged a
    // COMMIT is aborted, and a participant's checkpoint passes its PREPARE at once (D116)
    // while the coordinator's PREPARE survives. The checker took that hole for a lost
    // commit. The scenario is driven step by step, so it does not depend on seed luck.
    let cfg = undecided_prepares_config();
    for seed in seeds() {
        let stats = undecided_prepare_passes(seed, &cfg, false).unwrap_or_else(|f| panic!("{f}"));
        assert!(
            stats.undecided_prepares_passed > 0,
            "seed {seed}: the checker did not meet the passed PREPARE: {stats:?}"
        );
    }
}

#[test]
fn an_acknowledged_commit_without_a_commit_record_is_a_protocol_failure() {
    // The same shape, but the client was told the commit succeeded: an acknowledged
    // cross-shard commit always has a COMMIT, so the checker must not excuse the hole.
    let cfg = undecided_prepares_config();
    for seed in seeds() {
        match undecided_prepare_passes(seed, &cfg, true) {
            Err(f) if f.class == FailureClass::Protocol && f.message.contains("acknowledged") => {}
            other => panic!("seed {seed}: expected a protocol failure, got {other:?}"),
        }
    }
}

#[test]
fn a_decision_lost_by_a_crash_is_known_by_its_id_again() {
    // D263 (perf287): a decided commit whose DECISION a crash lost, its applied seqno at or
    // above the recovered next seqno, is known by its id again, and the commits that take
    // its seqnos after the reopen check cleanly.
    let cfg = undecided_prepares_config();
    for seed in seeds() {
        let stats =
            lost_decision_reuses_its_seqno(seed, &cfg, true).unwrap_or_else(|f| panic!("{f}"));
        assert!(
            stats.aliases_reverted > 0,
            "seed {seed}: no alias was reverted: {stats:?}"
        );
    }
}

#[test]
fn without_the_alias_revert_a_reused_seqno_fails_the_check() {
    // The same scenario with the revert off: X is settled under its applied seqno, and the
    // single acknowledged under it fails the check, or the test above shows nothing.
    let cfg = undecided_prepares_config();
    for seed in seeds() {
        match lost_decision_reuses_its_seqno(seed, &cfg, false) {
            Err(f)
                if f.class == FailureClass::Protocol
                    && f.message
                        .contains("which a recovery settled as never committed") => {}
            other => panic!("seed {seed}: expected the reused-seqno failure, got {other:?}"),
        }
    }
}

/// Two shards, one plain commit at a time, no random faults, crashes or maintenance.
/// `io_error_ppm` only marks the scenario's injected write error as expected.
fn undecided_prepares_config() -> Config {
    let mut cfg = Config::quiet(0);
    cfg.faults.io_error_ppm = 1;
    cfg.shards = 2;
    (cfg.flush_ppm, cfg.compact_ppm) = (0, 0);
    (cfg.cas_ppm, cfg.txn_ppm, cfg.tasks) = (0, 0, 1);
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
        identical_across_shard_counts_under_faults(seed, None);
    }
}

/// [`results_are_identical_across_shard_counts_under_faults`] for one seed, with deferred I/O
/// forced on or off (`None`: as `PIGEONHOLE_DEFERRED_IO` says).
fn identical_across_shard_counts_under_faults(seed: u64, deferred_io: Option<bool>) {
    let mut cfg = Config::standard(250);
    deterministic_compactions(&mut cfg);
    cfg.crash_every = Some(60);
    cfg.durability = Some(Durability::Buffered);
    if let Some(on) = deferred_io {
        cfg.deferred_io = on;
    }
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

#[test]
fn reads_beside_in_flight_commits_and_crashes_inside_them_keep_every_invariant() {
    // D227: a commit is visible once written and applied, before its sync returns. Reads
    // run while commits are in flight (`flight_read_ppm`), with deferred I/O keeping syncs
    // pending across steps, and power losses land inside commits more often. Acknowledged
    // commits must survive at their level and reads must match the model; a commit readers
    // may have seen but nobody was told committed may be lost to a power loss, never to a
    // process crash.
    let mut cfg = Config::standard(250);
    cfg.shards = 3;
    cfg.deferred_io = true;
    cfg.flight_read_ppm = 300_000;
    cfg.mid_commit_crash_ppm = 120_000;
    // Reads of in-flight commits' own cells (visible whole or not at all), and a process
    // crash after some of them: recovery must keep what a read saw. (Crashes this frequent
    // leave no segment time to fill, so the D200 held-header window is covered by
    // `held_header_visibility.rs` instead.)
    cfg.observed_crash_ppm = 200_000;
    let (mut reads, mut observed, mut crashes) = (0, 0, 0);
    for seed in seeds() {
        let s = run(seed, &cfg).unwrap_or_else(|f| panic!("{f}"));
        reads += s.flight_reads;
        observed += s.observed_in_flight;
        crashes += s.observed_crashes;
    }
    assert!(reads > 0, "no read ran beside an in-flight commit");
    assert!(observed > 0, "no read saw an in-flight commit whole");
    eprintln!("{reads} reads beside commits, {observed} saw one whole, {crashes} crashes after");
}

#[test]
fn hot_rows_blind_overlaps_across_many_shards() {
    // D271: blind batches on a few Zipfian rows over many shards, the shape of the
    // app-owned bench row (#816): blind PREPAREs over undecided shares are admitted at
    // once, and the one clock orders their default timestamps with their seqnos. The
    // checker holds every cell's versions to seqno and timestamp order, and a hung commit
    // fails the run.
    for shards in [3usize, 6, 10] {
        let mut cfg = Config::quiet(600);
        cfg.shards = shards;
        cfg.tasks = 64;
        (cfg.cas_ppm, cfg.txn_ppm) = (0, 0);
        cfg.spec.zipf_theta = 0.99;
        cfg.spec.rows = 200;
        cfg.spec.read_fraction = 0.1;
        cfg.spec.delete_fraction = 0.0;
        cfg.spec.max_batch = 4;
        for seed in seeds() {
            check(seed, &cfg);
        }
    }
}

#[test]
fn hot_rows_blind_overlaps_through_crashes() {
    // D267 (perf287's R3), kept for D271: the hot-row shape with crashes and mid-commit
    // crashes on, so several undecided shares on one row sit in a stream at a crash.
    let mut cfg = Config::standard(600);
    cfg.shards = 4;
    cfg.tasks = 64;
    (cfg.cas_ppm, cfg.txn_ppm) = (0, 0);
    cfg.spec.zipf_theta = 0.99;
    cfg.spec.rows = 200;
    cfg.spec.read_fraction = 0.1;
    cfg.spec.delete_fraction = 0.0;
    cfg.spec.max_batch = 4;
    for seed in seeds() {
        check(seed, &cfg);
    }
}

#[test]
fn hot_rows_conditional_singles_beside_blind_cross_shard_writers() {
    // D271 (perf287's M1): conditional singles on a few Zipfian rows beside blind batches
    // across shards, with deferred I/O so decisions land while singles wait. A blind PREPARE
    // writing a row a waiting single reads must land after the single, or the single's
    // predicate reads a state its seqno says is stale (the checker replays in seqno order).
    for shards in [2usize, 4] {
        let mut cfg = Config::quiet(600);
        cfg.shards = shards;
        cfg.tasks = 64;
        (cfg.cas_ppm, cfg.txn_ppm) = (300_000, 0);
        cfg.deferred_io = true;
        cfg.spec.zipf_theta = 0.99;
        cfg.spec.rows = 50;
        cfg.spec.read_fraction = 0.1;
        cfg.spec.delete_fraction = 0.0;
        cfg.spec.max_batch = 4;
        for seed in seeds() {
            check(seed, &cfg);
        }
    }
}

#[test]
fn hot_rows_mix_reading_commits_and_blind_writers() {
    // D267, kept for D271: transactions and conditional writes (commits that read) beside
    // blind batches on a few Zipfian rows: fences, and refusals of reads over undecided
    // writes and of writes over undecided reads. A fence or a refusal nothing ends leaves a commit
    // to give up with `Busy`, which `check` fails on a fault-free run.
    for shards in [3usize, 6] {
        let mut cfg = Config::quiet(600);
        cfg.shards = shards;
        cfg.tasks = 64;
        (cfg.cas_ppm, cfg.txn_ppm) = (100_000, 250_000);
        cfg.spec.zipf_theta = 0.99;
        cfg.spec.rows = 200;
        cfg.spec.read_fraction = 0.1;
        cfg.spec.delete_fraction = 0.0;
        cfg.spec.max_batch = 4;
        for seed in seeds() {
            check(seed, &cfg);
        }
    }
}

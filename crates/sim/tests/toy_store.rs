//! Acceptance tests for `pigeonhole-sim`: a toy store on `pigeonhole_io` passes randomized
//! crash/replay runs checked against the model, and the checker catches every deliberately
//! broken variant, each for the right reason.

mod common;

use std::time::Instant;

use common::toy::Variant;
use common::{Config, FailureClass, run};

/// Seeds to run: full strength in release builds, a fifth in debug builds to keep
/// `cargo test` quick.
fn seeds(n: u64) -> u64 {
    if cfg!(debug_assertions) { n / 5 } else { n }
}

#[test]
fn correct_store_passes_many_seeds() {
    let cfg = Config::standard(400);
    let mut crashes = 0;
    let mut mid = 0;
    for seed in 0..seeds(300) {
        let stats = run(seed, Variant::Correct, &cfg).unwrap_or_else(|f| panic!("{f}"));
        crashes += stats.crashes;
        mid += stats.mid_commit_crashes;
    }
    // The run must actually exercise crashes, including ones in the middle of commits.
    let scale = seeds(300) as usize / 30;
    assert!(
        crashes > 100 * scale && mid > 10 * scale,
        "crashes={crashes} mid={mid}"
    );
}

#[test]
fn correct_store_passes_without_faults_and_with_heavy_crashing() {
    let mut cfg = Config::standard(300);
    cfg.faults = pigeonhole_io::sim::FaultPlan::none();
    for seed in 1_000..1_000 + seeds(100) {
        run(seed, Variant::Correct, &cfg).unwrap_or_else(|f| panic!("{f}"));
    }
    let mut cfg = Config::standard(300);
    cfg.crash_ppm = 200_000;
    cfg.mid_commit_crash_ppm = 200_000;
    for seed in 2_000..2_000 + seeds(100) {
        run(seed, Variant::Correct, &cfg).unwrap_or_else(|f| panic!("{f}"));
    }
}

/// The failure classes a broken variant may legitimately be caught as. Pinning them means a
/// variant that starts failing for an unrelated reason (a checker bug, say) is noticed.
fn expected(variant: Variant) -> &'static [FailureClass] {
    use FailureClass::*;
    match variant {
        Variant::NoFsync
        | Variant::SyncBeforeWrite
        | Variant::BufferedNotWritten
        | Variant::NoDirSync => &[LostAckedCommit],
        Variant::NoChecksum | Variant::LosesMiddleCommit | Variant::PartialCommit => {
            &[RecoveredStateMismatch]
        }
        Variant::ReorderedFlush => &[RecoveredStateMismatch, LostAckedCommit],
        // Semantic bugs show up wherever the store is next compared: a live read, or the
        // full-state comparison after the next recovery.
        Variant::IgnoresDeletes | Variant::WrongVersionOrder => {
            &[LiveReadMismatch, RecoveredStateMismatch]
        }
        Variant::Correct => &[],
    }
}

fn catch_cfg() -> Config {
    let mut cfg = Config::standard(400);
    // Records spanning several sectors, so torn writes can leave interior holes.
    cfg.spec.max_value_len = 600;
    cfg.crash_ppm = 40_000;
    cfg.mid_commit_crash_ppm = 60_000;
    cfg
}

#[test]
fn checker_catches_every_broken_variant() {
    // Release builds allow 500 seeds; debug builds 250 (the slowest variant is caught near
    // seed 85).
    let n = if cfg!(debug_assertions) { 250 } else { 500 };
    let cfg = catch_cfg();
    // Control: the correct store passes the very same configuration.
    for seed in 0..seeds(200) {
        run(seed, Variant::Correct, &cfg).unwrap_or_else(|f| panic!("{f}"));
    }
    let mut report = Vec::new();
    for variant in Variant::BROKEN {
        let f = (0..n)
            .find_map(|seed| run(seed, variant, &cfg).err())
            .unwrap_or_else(|| panic!("checker missed {variant:?} in {n} seeds"));
        assert!(!f.trace.is_empty());
        assert!(
            expected(variant).contains(&f.class),
            "{variant:?} was caught for the wrong reason ({:?}, expected {:?}):\n{f}",
            f.class,
            expected(variant)
        );
        report.push(format!(
            "{variant:?}: {:?} at seed {} op #{}",
            f.class, f.seed, f.op_index
        ));
    }
    println!("{}", report.join("\n"));
}

#[test]
fn scheduler_interleaves_several_clients() {
    // Three client tasks share the store; the scheduler picks which one steps next.
    let mut cfg = catch_cfg();
    cfg.tasks = 3;
    for seed in 0..seeds(100) {
        run(seed, Variant::Correct, &cfg).unwrap_or_else(|f| panic!("{f}"));
    }
    for variant in [
        Variant::NoFsync,
        Variant::IgnoresDeletes,
        Variant::LosesMiddleCommit,
    ] {
        let f = (0..seeds(250))
            .find_map(|seed| run(seed, variant, &cfg).err())
            .unwrap_or_else(|| panic!("checker missed {variant:?} with 3 tasks"));
        assert!(expected(variant).contains(&f.class), "{f}");
    }
    // Different seeds interleave differently: the traces diverge.
    let trace = |seed| run(seed, Variant::NoFsync, &cfg).err().map(|f| f.trace);
    let traces: Vec<_> = (0..10).map(trace).collect();
    assert!(traces.windows(2).any(|w| w[0] != w[1]));
}

#[test]
fn failures_print_seed_and_trace() {
    let cfg = Config::standard(400);
    let f = (0..200)
        .find_map(|seed| run(seed, Variant::NoFsync, &cfg).err())
        .expect("NoFsync is caught");
    let text = f.to_string();
    assert!(text.contains(&format!("seed={}", f.seed)));
    assert!(text.contains("commit"), "{text}");
    assert!(text.contains("CRASH"), "{text}");
    // Replaying the seed reproduces the same failure.
    let again = run(f.seed, Variant::NoFsync, &cfg).expect_err("replays");
    assert_eq!((again.op_index, again.message), (f.op_index, f.message));
}

#[test]
fn ten_thousand_ops_run_well_under_a_second() {
    let mut cfg = Config::standard(10_000);
    cfg.crash_ppm = 1_000;
    cfg.mid_commit_crash_ppm = 1_000;
    let start = Instant::now();
    run(77, Variant::Correct, &cfg).unwrap_or_else(|f| panic!("{f}"));
    let took = start.elapsed();
    println!("10k ops in {took:?}");
    // Generous bound so debug builds on loaded CI hosts pass; release is far below it.
    assert!(
        took.as_secs_f64() < if cfg!(debug_assertions) { 5.0 } else { 1.0 },
        "{took:?}"
    );
}

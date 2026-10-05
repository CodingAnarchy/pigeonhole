//! Acceptance tests for `pigeonhole-sim`: a toy store on `pigeonhole_io` passes randomized
//! crash/replay runs checked against the model, and the checker catches every deliberately
//! broken variant.

mod common;

use std::time::Instant;

use common::toy::Variant;
use common::{Config, run};

#[test]
fn correct_store_passes_many_seeds() {
    let cfg = Config::standard(400);
    let mut crashes = 0;
    let mut mid = 0;
    for seed in 0..300 {
        let stats = run(seed, Variant::Correct, &cfg).unwrap_or_else(|f| panic!("{f}"));
        crashes += stats.crashes;
        mid += stats.mid_commit_crashes;
    }
    // The run must actually exercise crashes, including ones in the middle of commits.
    assert!(crashes > 1_000 && mid > 100, "crashes={crashes} mid={mid}");
}

#[test]
fn correct_store_passes_without_faults_and_with_heavy_crashing() {
    let mut cfg = Config::standard(300);
    cfg.faults = pigeonhole_io::sim::FaultPlan::none();
    for seed in 1_000..1_100 {
        run(seed, Variant::Correct, &cfg).unwrap_or_else(|f| panic!("{f}"));
    }
    let mut cfg = Config::standard(300);
    cfg.crash_ppm = 200_000;
    cfg.mid_commit_crash_ppm = 200_000;
    for seed in 2_000..2_100 {
        run(seed, Variant::Correct, &cfg).unwrap_or_else(|f| panic!("{f}"));
    }
}

#[test]
fn checker_catches_every_broken_variant() {
    const SEEDS: u64 = 500;
    let mut cfg = Config::standard(400);
    // Records spanning several sectors, so torn writes can leave interior holes.
    cfg.spec.max_value_len = 600;
    cfg.crash_ppm = 40_000;
    cfg.mid_commit_crash_ppm = 60_000;
    // Control: the correct store passes the very same seeds and configuration.
    for seed in 0..200 {
        run(seed, Variant::Correct, &cfg).unwrap_or_else(|f| panic!("{f}"));
    }
    let mut report = Vec::new();
    for variant in Variant::BROKEN {
        let found = (0..SEEDS).find_map(|seed| run(seed, variant, &cfg).err());
        match found {
            Some(f) => {
                assert!(!f.trace.is_empty());
                report.push(format!(
                    "{variant:?}: caught at seed {} op #{}",
                    f.seed, f.op_index
                ));
            }
            None => panic!("checker missed {variant:?} in {SEEDS} seeds"),
        }
    }
    println!("{}", report.join("\n"));
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

//! #283: with tablet changes off, one shard holding every table of six families has 24
//! `(tablet, family)` slots. Its arena used to be cut into 64 chunks, enough for 16 slots, so
//! a flush that froze every slot found no chunk for the fresh memtables and writes stalled
//! until `Busy` (seed 5 below). `common::families` gives this target six families.

mod common;

use common::{Config, run};

#[test]
fn one_tablet_off_shard_serves_six_families_on_four_tables() {
    let n: u64 = std::env::var("PIGEONHOLE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let first: u64 = std::env::var("PIGEONHOLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    for mut cfg in [Config::quiet(300), Config::standard(250)] {
        cfg.shards = 1;
        cfg.tablet_changes = false;
        cfg.balance_fast = false;
        for seed in first..first + n {
            if let Err(f) = run(seed, &cfg) {
                panic!("{f}");
            }
        }
    }
}

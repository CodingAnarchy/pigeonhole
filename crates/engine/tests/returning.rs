//! D314's returning increments (#1440) under the model-checked simulation: commits that
//! return their counters' values, some of them members of concurrent batches beside blind
//! cross-shard writers, each value checked against the model's fold at its seqno.
//! Half of them also ask for the values the columns they write held just before them
//! (get-and-set, D314 amendment 1), each checked against the model's newest version at the
//! seqno below. `common::families` gives this target a plain `sum`, a `sum_v2` keeping 2
//! versions, a `sum_ttl` and `f`.

mod common;

use common::{Config, Stats, run};

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

fn check(seed: u64, cfg: &Config) -> Stats {
    match run(seed, cfg) {
        Ok(stats) => {
            eprintln!("seed {seed}: {stats:?}");
            assert_eq!(
                stats.busy, 0,
                "seed {seed}: a commit gave up with Busy on a quiet run"
            );
            stats
        }
        Err(f) => panic!("{f}"),
    }
}

#[test]
fn hot_rows_returning_increments_beside_blind_cross_shard_writers() {
    // D314 (simp724's Q2 on #1440): returning increments on a few Zipfian rows beside blind
    // batches across shards, with deferred I/O so decisions land while singles wait. A share
    // deciding below a returning member must apply before the member folds, or the value
    // misses its operand and the member's checkpoint hides it from every later read.
    for shards in [2usize, 4] {
        let mut cfg = Config::quiet(600);
        cfg.shards = shards;
        cfg.tasks = 64;
        (cfg.cas_ppm, cfg.txn_ppm, cfg.returning_ppm) = (0, 0, 600_000);
        cfg.deferred_io = true;
        cfg.spec.zipf_theta = 0.99;
        cfg.spec.rows = 4;
        cfg.spec.qualifiers = 1;
        cfg.spec.read_fraction = 0.1;
        cfg.spec.delete_fraction = 0.0;
        cfg.spec.max_batch = 4;
        // The ordering rule's two halves each have a hand-driven test that fails without
        // them (it/returning_order.rs); random seeds reach the rule's window only rarely.
        let (mut fast, mut full, mut previous) = (0, 0, 0);
        for seed in seeds() {
            let st = check(seed, &cfg);
            (fast, full) = (fast + st.returning_fast, full + st.returning_full);
            previous += st.previous_checked;
        }
        // Half the returning commits ask for previous values (D314 amendment 1).
        assert!(
            previous > 0,
            "{shards} shards: no previous value was checked"
        );
        // The fast path must run, or the checks above would pass on the full read alone
        // (simp724 on #1440). This mix sends most folds to the full read on purpose (`sum_v2`
        // keeps 2 versions, a new TTL bucket has no base yet, row deletes are frequent, and
        // the 16 KiB memtables flush often), so the floor is a tenth: about a quarter ran
        // fast at seeds 1-3.
        assert!(
            fast > 0 && fast * 10 >= fast + full,
            "{shards} shards: {fast} fast folds, {full} full reads: the fast path barely ran"
        );
    }
    // With deletes too (`delete_column`, `delete_family`, `delete_row` beside returning
    // increments): a delete marker between the fast walk's base and the commit sends the
    // fold to the full read.
    let mut cfg = Config::quiet(600);
    cfg.shards = 2;
    cfg.tasks = 64;
    (cfg.cas_ppm, cfg.txn_ppm, cfg.returning_ppm) = (0, 0, 600_000);
    cfg.deferred_io = true;
    cfg.spec.zipf_theta = 0.99;
    cfg.spec.rows = 4;
    cfg.spec.qualifiers = 1;
    cfg.spec.read_fraction = 0.1;
    cfg.spec.delete_fraction = 0.15;
    cfg.spec.max_batch = 4;
    for seed in seeds() {
        check(seed, &cfg);
    }
}

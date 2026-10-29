//! Every workload at smoke size against Pigeonhole and each enabled comparison engine.

use std::path::PathBuf;

use pigeonhole_bench::{PigeonholeRunner, Runner, WorkloadConfig, WorkloadKind, run};

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("phdb-bench-smoke-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn smoke_all(tag: &str, make: impl FnMut(WorkloadKind) -> Box<dyn Runner>) {
    smoke_kinds(tag, &WorkloadKind::EVERY, make);
}

/// [`smoke_all`] over `kinds` only; `make` builds the runner for each workload (D274: RocksDB
/// runs the transactional ones on its transactions).
fn smoke_kinds(
    tag: &str,
    kinds: &[WorkloadKind],
    mut make: impl FnMut(WorkloadKind) -> Box<dyn Runner>,
) {
    let root = temp_dir(tag);
    for &kind in kinds {
        let config = WorkloadConfig::smoke(kind);
        let dir = root.join(kind.name());
        std::fs::create_dir_all(&dir).unwrap();
        let mut runner = make(kind);
        let report = run(runner.as_mut(), &config, &dir)
            .unwrap_or_else(|e| panic!("{kind:?} on {tag} (seed {}): {e}", config.seed));
        assert_eq!(report.workload, kind);
        assert_eq!(report.seed, config.seed);
        assert!(report.throughput > 0.0, "{kind:?} on {tag}");
        assert!(
            report.p50 <= report.p99 && report.p99 <= report.p999,
            "{kind:?} on {tag}"
        );
        assert!(
            report.hardware.contains("non-reference") || report.hardware.contains("[reference]")
        );
    }
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn every_workload_on_pigeonhole() {
    smoke_all("pigeonhole", |_| {
        Box::new(
            PigeonholeRunner::default()
                .shards(2)
                .memtable_budget(32 << 20),
        )
    });
}

#[test]
fn pigeonhole_sync_durability() {
    let root = temp_dir("pigeonhole-sync");
    let mut config = WorkloadConfig::smoke(WorkloadKind::YcsbA);
    config.operations = 200;
    let mut r = PigeonholeRunner::default()
        .shards(1)
        .memtable_budget(16 << 20)
        .sync(true);
    assert!(r.describe().contains("sync"));
    run(&mut r, &config, &root).unwrap();
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn pigeonhole_loads_more_than_the_memtable_budget() {
    // The engine flushes to SSTs: a data set many times the memtable budget loads and
    // runs (the `full` preset relies on it).
    let root = temp_dir("pigeonhole-bound");
    let mut config = WorkloadConfig::smoke(WorkloadKind::YcsbA);
    config.records = 20_000;
    let mut r = PigeonholeRunner::default()
        .shards(1)
        .memtable_budget(1 << 20);
    let report = run(&mut r, &config, &root).unwrap_or_else(|e| panic!("{e}"));
    assert!(report.throughput > 0.0, "{report:?}");
    std::fs::remove_dir_all(&root).ok();
}

#[cfg(feature = "rocksdb")]
#[test]
fn every_workload_on_rocksdb() {
    use pigeonhole_bench::{RocksDbRunner, RocksDbTxnRunner, TxnMode};
    // D274: as `phdb-bench --engine rocksdb` does, the transactional workloads run on
    // RocksDB's optimistic transactions, every other on the plain DB.
    smoke_all("rocksdb", |kind| match kind {
        WorkloadKind::Transfer | WorkloadKind::YcsbF => {
            Box::new(RocksDbTxnRunner::new(TxnMode::Optimistic))
        }
        _ => Box::new(RocksDbRunner::default()),
    });
}

/// D274: `rocksdb-txn`, RocksDB's pessimistic transactions, on the transactional workloads.
#[cfg(feature = "rocksdb")]
#[test]
fn transactional_workloads_on_rocksdb_txn() {
    use pigeonhole_bench::{RocksDbTxnRunner, TxnMode};
    smoke_kinds(
        "rocksdb-txn",
        &[WorkloadKind::Transfer, WorkloadKind::YcsbF],
        |_| Box::new(RocksDbTxnRunner::new(TxnMode::Pessimistic)),
    );
}

#[cfg(feature = "sqlite")]
#[test]
fn every_workload_on_sqlite() {
    smoke_all("sqlite", |_| {
        Box::new(pigeonhole_bench::SqliteRunner::default())
    });
}

#[test]
fn warmup_is_run_but_not_recorded() {
    use pigeonhole_bench::{RunOptions, run_detailed};
    let root = temp_dir("warmup");
    let config = WorkloadConfig::smoke(WorkloadKind::YcsbA);
    let mut r = PigeonholeRunner::default()
        .shards(1)
        .memtable_budget(16 << 20);
    let rec = run_detailed(
        &mut r,
        &config,
        &root,
        &RunOptions {
            warmup: 0.25,
            ..RunOptions::default()
        },
    )
    .unwrap();
    assert_eq!(rec.operations, config.operations);
    assert_eq!(rec.warmup_ops, config.operations / 4);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn ycsb_f_runs_on_concurrent_clients() {
    // D247: read-modify-write is atomic in every runner, so ycsb-f takes any thread count.
    let root = temp_dir("ycsb-f-threads");
    let mut config = WorkloadConfig::smoke(WorkloadKind::YcsbF);
    config.threads = 4;
    let mut r = PigeonholeRunner::default()
        .shards(2)
        .memtable_budget(16 << 20);
    let report = run(&mut r, &config, &root).unwrap();
    assert!(report.throughput > 0.0);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn describe_reports_memory_budget() {
    use pigeonhole_bench::MemoryBudget;
    let m = MemoryBudget {
        write_buffer: 32 << 20,
        cache: 8 << 20,
        ..MemoryBudget::default()
    };
    let d = PigeonholeRunner::default().memory(m).describe();
    assert!(d.contains("memtable=32MiB cache=8MiB bloom=10"), "{d}");
    #[cfg(feature = "rocksdb")]
    assert!(
        pigeonhole_bench::RocksDbRunner::default()
            .memory(m)
            .describe()
            .contains("write_buffer=32MiB cache=8MiB bloom=10")
    );
    #[cfg(feature = "sqlite")]
    assert!(
        pigeonhole_bench::SqliteRunner::default()
            .memory(m)
            .describe()
            .contains("page_cache=40MiB")
    );
}

#[test]
fn larger_than_ram_preset_splits_cold_and_hot_gets() {
    use pigeonhole_bench::{MemoryBudget, RunOptions, run_detailed};

    let root = temp_dir("cold");
    let mut config = WorkloadConfig::smoke(WorkloadKind::YcsbC);
    config.records = 2_000;
    config.operations = 4_000;
    // The preset's budget is far below the data set even at this size when shrunk further.
    let budget = MemoryBudget {
        write_buffer: 1 << 20,
        cache: 1 << 20,
        ..MemoryBudget::default()
    };
    assert!(MemoryBudget::larger_than_ram().cache < MemoryBudget::default().cache);
    let mut r = PigeonholeRunner::default().shards(1).memory(budget);
    let rec = run_detailed(
        &mut r,
        &config,
        &root,
        &RunOptions {
            warmup: 0.0,
            ..RunOptions::default()
        },
    )
    .unwrap_or_else(|e| panic!("seed {}: {e}", config.seed));
    let reads = rec.detail.reads.expect("a read workload reports the split");
    // Every measured op is a get: each is cold (first touch of its row) or hot.
    assert_eq!(reads.cold.count + reads.hot.count, rec.operations);
    assert!(reads.cold.count > 0 && reads.hot.count > 0, "{reads:?}");
    assert!(reads.cold.count <= config.records);
    assert!(rec.detail.store_bytes > 0);
    assert_eq!(rec.detail.busy_retries, 0);
    // A write-only workload has no gets to split.
    let config = WorkloadConfig::smoke(WorkloadKind::SkewedMultiShard);
    let rec = run_detailed(&mut r, &config, &root, &RunOptions::default()).unwrap();
    assert!(rec.detail.reads.is_none());
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn pigeonhole_transfers_are_split_by_shard_locality() {
    // #708: each transfer records under whether its two accounts were on one shard at the
    // start of the measured pass (`Runner::op_label`), never under the plain type name.
    use pigeonhole_bench::{RunOptions, TRANSFER_CROSS_SHARD, TRANSFER_ONE_SHARD, run_detailed};
    let root = temp_dir("pigeonhole-transfer-locality");
    let mut config = WorkloadConfig::smoke(WorkloadKind::Transfer);
    config.operations = 400;
    let mut r = PigeonholeRunner::default()
        .shards(2)
        .memtable_budget(16 << 20);
    let rec = run_detailed(&mut r, &config, &root, &RunOptions::default()).unwrap();
    let by_type = &rec.detail.by_type;
    assert!(
        by_type
            .iter()
            .all(|t| t.op == TRANSFER_ONE_SHARD || t.op == TRANSFER_CROSS_SHARD),
        "{by_type:?}"
    );
    let n: u64 = by_type.iter().map(|t| t.stats.count).sum();
    assert_eq!(n, rec.operations, "every measured transfer is labelled");
    std::fs::remove_dir_all(&root).ok();
}

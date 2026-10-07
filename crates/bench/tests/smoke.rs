//! Every workload at smoke size against Pigeonhole and each enabled comparison engine.

use std::path::PathBuf;

use pigeonhole_bench::{PigeonholeRunner, Runner, WorkloadConfig, WorkloadKind, run};

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("phdb-bench-smoke-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn smoke_all(tag: &str, mut make: impl FnMut() -> Box<dyn Runner>) {
    let root = temp_dir(tag);
    for kind in WorkloadKind::ALL {
        let config = WorkloadConfig::smoke(kind);
        let dir = root.join(kind.name());
        std::fs::create_dir_all(&dir).unwrap();
        let mut runner = make();
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
    smoke_all("pigeonhole", || {
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
    smoke_all("rocksdb", || {
        Box::new(pigeonhole_bench::RocksDbRunner::default())
    });
}

#[cfg(feature = "sqlite")]
#[test]
fn every_workload_on_sqlite() {
    smoke_all("sqlite", || {
        Box::new(pigeonhole_bench::SqliteRunner::default())
    });
}

#[cfg(feature = "fjall")]
#[test]
fn every_workload_on_fjall() {
    smoke_all("fjall", || {
        Box::new(pigeonhole_bench::FjallRunner::default())
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
    let rec = run_detailed(&mut r, &config, &root, &RunOptions { warmup: 0.25 }).unwrap();
    assert_eq!(rec.operations, config.operations);
    assert_eq!(rec.warmup_ops, config.operations / 4);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn ycsb_f_refuses_concurrent_clients() {
    let root = temp_dir("ycsb-f-threads");
    let mut config = WorkloadConfig::smoke(WorkloadKind::YcsbF);
    config.threads = 2;
    let mut r = PigeonholeRunner::default()
        .shards(1)
        .memtable_budget(16 << 20);
    let err = run(&mut r, &config, &root).unwrap_err();
    assert!(err.contains("one thread"), "{err}");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn describe_reports_memory_budget() {
    use pigeonhole_bench::MemoryBudget;
    let m = MemoryBudget {
        write_buffer: 32 << 20,
        cache: 8 << 20,
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
    #[cfg(feature = "fjall")]
    assert!(
        pigeonhole_bench::FjallRunner::default()
            .memory(m)
            .describe()
            .contains("write_buffer=32MiB cache=8MiB")
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
    };
    assert!(MemoryBudget::larger_than_ram().cache < MemoryBudget::default().cache);
    let mut r = PigeonholeRunner::default().shards(1).memory(budget);
    let rec = run_detailed(&mut r, &config, &root, &RunOptions { warmup: 0.0 })
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

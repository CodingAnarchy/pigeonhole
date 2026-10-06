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
fn pigeonhole_reports_memory_bound() {
    // Until the engine flushes to SSTs (#37), a data set larger than the memtable
    // budget fails with a clear error instead of a bogus number.
    let root = temp_dir("pigeonhole-bound");
    let mut config = WorkloadConfig::smoke(WorkloadKind::YcsbA);
    config.records = 20_000;
    let mut r = PigeonholeRunner::default()
        .shards(1)
        .memtable_budget(1 << 20);
    let err = run(&mut r, &config, &root).unwrap_err();
    assert!(err.contains("load"), "{err}");
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

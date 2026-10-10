//! Block-cache hits and misses in the engine's metrics (#57, ICR 0024).

use std::sync::Arc;

use pigeonhole::{Family, Options, Pigeonhole};
use pigeonhole_io::sim::SimVfs;

const PATH: &str = "/db/bc.phdb";

fn options(vfs: &Arc<SimVfs>, cache: usize) -> Options {
    Options::default()
        .vfs(Arc::clone(vfs) as _)
        .shards(1)
        .memtable_budget(4 << 20)
        .wal_segment_size(256 << 10)
        .block_cache(cache)
}

/// A store whose rows are all in SSTs, reopened with a block cache of `cache` bytes.
fn reopened(cache: usize) -> Pigeonhole {
    let vfs = SimVfs::new(57);
    let db = Pigeonhole::open(PATH, options(&vfs, 1 << 20)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    for i in 0..100u32 {
        t.mutate(format!("r{i:03}").as_bytes())
            .put("f", b"q", b"v")
            .commit()
            .unwrap();
    }
    drop(t);
    db.flush().unwrap();
    db.close().unwrap();
    Pigeonhole::open(PATH, options(&vfs, cache)).unwrap()
}

fn read(db: &Pigeonhole) {
    let t = db.table("t").unwrap().open().unwrap();
    assert!(t.row(b"r050").family("f").read().unwrap().is_some());
}

#[test]
fn a_cold_read_misses_and_the_same_read_then_hits() {
    let db = reopened(1 << 20);
    let (_, m0) = db.engine_metrics().block_cache;
    read(&db);
    let (h1, m1) = db.engine_metrics().block_cache;
    assert!(m1 > m0, "the first read of a reopened store misses");
    read(&db);
    let (h2, m2) = db.engine_metrics().block_cache;
    assert_eq!(m2, m1, "the same read again misses nothing");
    assert!(h2 > h1, "and hits");
}

#[test]
fn a_disabled_block_cache_counts_nothing() {
    let db = reopened(0);
    read(&db);
    read(&db);
    assert_eq!(db.engine_metrics().block_cache, (0, 0));
}

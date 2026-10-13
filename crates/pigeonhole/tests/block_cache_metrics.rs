//! Block-cache hits and misses (#57, ICR 0024) and pinned SST metadata (D215, ICR 0032) in the
//! engine's metrics.

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
    let m0 = db.engine_metrics().block_cache_misses;
    read(&db);
    let m = db.engine_metrics();
    let (h1, m1) = (m.block_cache_hits, m.block_cache_misses);
    assert!(m1 > m0, "the first read of a reopened store misses");
    read(&db);
    let m = db.engine_metrics();
    let (h2, m2) = (m.block_cache_hits, m.block_cache_misses);
    assert_eq!(m2, m1, "the same read again misses nothing");
    assert!(h2 > h1, "and hits");
}

#[test]
fn a_disabled_block_cache_counts_nothing() {
    let db = reopened(0);
    read(&db);
    read(&db);
    let m = db.engine_metrics();
    assert_eq!((m.block_cache_hits, m.block_cache_misses), (0, 0));
    assert_eq!(
        m.block_cache_pinned_warnings, 0,
        "no capacity to warn about"
    );
}

/// The bytes the open SST readers pin (top index, filters): none before any read opens one.
fn pinned_after_a_read(db: &Pigeonhole) -> u64 {
    assert_eq!(db.engine_metrics().block_cache_pinned_bytes, 0);
    read(db);
    db.engine_metrics().block_cache_pinned_bytes
}

#[test]
fn open_ssts_count_their_pinned_metadata() {
    let db = reopened(1 << 20);
    let pinned = pinned_after_a_read(&db);
    let m = db.engine_metrics();
    assert!(pinned > 0, "the read opened an SST reader");
    assert_eq!(m.block_cache_capacity_bytes, 1 << 20);
    assert_eq!(m.block_cache_pinned_warnings, 0, "far below half the cache");
}

#[test]
fn a_cache_too_small_for_the_metadata_warns_once() {
    let pinned = pinned_after_a_read(&reopened(1 << 20));
    // Under twice the metadata: opening the readers takes the pinned bytes past half.
    let db = reopened(pinned as usize * 3 / 2);
    assert_eq!(pinned_after_a_read(&db), pinned);
    read(&db);
    assert_eq!(db.engine_metrics().block_cache_pinned_warnings, 1);
}

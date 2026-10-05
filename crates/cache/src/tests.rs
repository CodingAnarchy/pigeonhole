//! Policy tests that need the crate's internals; behavior tests live in `tests/`.

use super::*;

/// Loop length, shortened under Miri.
const fn n(full: u32) -> u32 {
    if cfg!(miri) { full / 20 } else { full }
}

fn key(offset: u64) -> BlockKey {
    BlockKey { file: 1, offset }
}

fn block(len: usize) -> BlockData {
    vec![0xab; len].into()
}

/// One shard holding ten 100-byte blocks.
fn small_cache() -> BlockCache {
    BlockCache::new(1000, 1)
}

#[test]
fn high_priority_outlives_low_under_normal_churn() {
    let c = small_cache();
    drop(c.insert(key(0), block(100), Priority::High));
    drop(c.insert(key(1), block(100), Priority::Low));
    for k in 100..100 + u64::from(n(1000)) {
        drop(c.insert(key(k), block(100), Priority::Normal));
        assert!(c.usage() <= c.capacity());
    }
    assert!(
        c.get(key(0)).is_some(),
        "High block evicted by unaccessed Normal blocks"
    );
    assert!(c.get(key(1)).is_none(), "Low block survived");
}

#[test]
fn low_priority_scan_does_not_flush_the_working_set() {
    let c = small_cache();
    for k in 0..6 {
        drop(c.insert(key(k), block(100), Priority::Normal));
        drop(c.get(key(k)));
    }
    for k in 1000..1000 + u64::from(n(10_000)) {
        drop(c.insert(key(k), block(100), Priority::Low));
    }
    for k in 0..6 {
        assert!(
            c.get(key(k)).is_some(),
            "working-set block {k} flushed by a Low scan"
        );
    }
}

#[test]
fn eviction_skips_pins() {
    let c = small_cache();
    let pins: Vec<_> = (0..10)
        .map(|k| c.insert(key(k), block(100), Priority::Low))
        .collect();
    for k in 100..200 {
        drop(c.insert(key(k), block(100), Priority::High));
    }
    for (k, pin) in pins.iter().enumerate() {
        let hit = c.get(key(k as u64)).expect("pinned block evicted");
        assert_eq!(hit.bytes(), pin.bytes());
    }
    drop(pins);
    drop(c.insert(key(500), block(100), Priority::Normal));
    assert!(c.usage() <= c.capacity());
}

#[test]
fn row_cache_hash_collision_is_a_miss() {
    let rows = RowCache::new(1 << 16);
    drop(rows.insert(1, b"a", 0, b"A".to_vec()));
    // Plant row "b" under row "a"'s hash, as a colliding row would be.
    let h = RowCache::hash(1, b"a");
    let entry = Arc::new(RowEntry {
        family: 1,
        epoch: 0,
        row: b"b"[..].into(),
        encoded: b"B".to_vec(),
    });
    let shard = rows.shards.shard(h).expect("shard");
    lock(shard).insert(h, entry, 10, Priority::Normal);
    assert!(rows.get(1, b"a", 0).is_none());
    // Invalidating "a" must not drop "b".
    rows.invalidate(1, b"a");
    assert!(lock(shard).peek(&h).is_some());
}

#[test]
fn row_cache_charges_and_evicts() {
    let rows = RowCache::new(64 << 10);
    for i in 0..n(10_000) {
        drop(rows.insert(i as u64 % 3, &i.to_be_bytes(), 0, vec![0; 100]));
        assert!(rows.usage() <= 64 << 10);
    }
    assert!(rows.usage() > 32 << 10);
}

#[test]
fn row_cache_new_epoch_replaces() {
    let rows = RowCache::new(1 << 16);
    drop(rows.insert(1, b"r", 1, b"old".to_vec()));
    drop(rows.insert(1, b"r", 2, b"new".to_vec()));
    assert!(rows.get(1, b"r", 1).is_none());
    assert_eq!(&rows.get(1, b"r", 2).expect("hit")[..], b"new");
    let one = rows.usage();
    drop(rows.insert(1, b"r", 3, b"newer".to_vec()));
    assert_eq!(rows.usage(), one + 2);
}

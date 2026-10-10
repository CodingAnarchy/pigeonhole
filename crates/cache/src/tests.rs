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

/// Whether `k` is cached, without counting a hit.
fn cached(c: &BlockCache, k: BlockKey) -> bool {
    let shard = c.shards.shard(hash::hash_of(&k)).expect("shard");
    lock(shard).peek(&k).is_some()
}

/// Whether `k` is cached in the main queue.
fn in_main(c: &BlockCache, k: BlockKey) -> bool {
    let shard = c.shards.shard(hash::hash_of(&k)).expect("shard");
    lock(shard).in_main(&k)
}

#[test]
fn high_outlives_equally_hot_normal_in_main() {
    // Twenty 100-byte blocks. Five High and five Normal blocks are hit equally until both
    // sets sit in main with saturated hit counts; then newcomers that are hit once (so they
    // are promoted to main too) push main to evict, with no further hits on either set.
    let c = BlockCache::new(2000, 1);
    let high: Vec<BlockKey> = (0..5).map(key).collect();
    let normal: Vec<BlockKey> = (100..105).map(key).collect();
    for k in &high {
        drop(c.insert(*k, block(100), Priority::High));
    }
    for k in &normal {
        drop(c.insert(*k, block(100), Priority::Normal));
    }
    let mut next = 1000;
    let mut newcomer = |c: &BlockCache| {
        drop(c.insert(key(next), block(100), Priority::Normal));
        drop(c.get(key(next)));
        next += 1;
    };
    let hot = |c: &BlockCache| {
        for k in high.iter().chain(&normal) {
            for _ in 0..8 {
                drop(c.get(*k));
            }
        }
    };
    for round in 0.. {
        if normal.iter().all(|k| in_main(&c, *k)) {
            break;
        }
        assert!(round < 100, "Normal blocks never promoted");
        hot(&c);
        newcomer(&c);
    }
    // Both sets are in main now; saturate them equally once more (promotion cost the Normal
    // blocks a life). High sits ahead of Normal in main, so at equal lives it would go first.
    assert!(high.iter().chain(&normal).all(|k| in_main(&c, *k)));
    hot(&c);
    while normal.iter().any(|k| cached(&c, *k)) {
        newcomer(&c);
    }
    let left = high.iter().filter(|k| cached(&c, **k)).count();
    assert_eq!(
        left, 5,
        "only {left} of 5 High blocks outlived the equally hot Normal ones"
    );
}

#[test]
fn erase_files_batches() {
    let c = BlockCache::new(1 << 20, 4);
    for file in 0..20 {
        for off in 0..4 {
            drop(c.insert(BlockKey { file, offset: off }, block(10), Priority::Normal));
        }
    }
    let odd: Vec<u64> = (0..20).filter(|f| f % 2 == 1).collect();
    c.erase_files(&odd);
    c.erase_files(&[0, 2]);
    for file in 0..20 {
        let hit = c.get(BlockKey { file, offset: 3 }).is_some();
        assert_eq!(hit, file % 2 == 0 && file > 2, "file {file}");
    }
    assert_eq!(c.usage(), 8 * 4 * 10);
}

#[test]
fn lookups_are_counted_across_shards_and_threads() {
    let c = Arc::new(BlockCache::new(1 << 20, 8));
    for off in 0..64 {
        drop(c.insert(key(off), block(10), Priority::Normal));
    }
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let c = Arc::clone(&c);
            std::thread::spawn(move || {
                // Offsets 0..64 are cached, 64..128 are not: half hit, half miss.
                for i in 0..n(1000) {
                    drop(c.get(key(u64::from(i) % 128)));
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let lookups = u64::from(4 * n(1000));
    let (hits, misses) = c.hits_and_misses();
    assert_eq!(hits + misses, lookups);
    assert_eq!(
        hits,
        (0..u64::from(n(1000))).filter(|i| i % 128 < 64).count() as u64 * 4
    );
}

#[test]
fn a_disabled_cache_counts_nothing() {
    let c = BlockCache::disabled();
    drop(c.insert(key(0), block(10), Priority::Normal));
    assert!(c.get(key(0)).is_none());
    assert_eq!(c.hits_and_misses(), (0, 0));
}

//! Behavior tests for the block cache, row cache and cells, plus a model-checked property test.

use std::collections::HashMap;

use pigeonhole_cache::{BlockCache, BlockData, BlockHandle, BlockKey, Cell, Priority, RowCache};
use pigeonhole_io::IoBuf;
use proptest::prelude::*;

fn key(file: u64, offset: u64) -> BlockKey {
    BlockKey { file, offset }
}

#[test]
fn insert_get_replace() {
    let c = BlockCache::new(1 << 20, 4);
    let k = key(1, 0);
    assert!(c.get(k).is_none());
    let old = c.insert(k, b"one".to_vec().into(), Priority::Normal);
    assert_eq!(c.get(k).expect("hit").bytes(), b"one");
    drop(c.insert(k, b"two!".to_vec().into(), Priority::Normal));
    assert_eq!(&c.get(k).expect("hit")[..], b"two!");
    assert_eq!(old.bytes(), b"one", "replacement changed a pinned block");
    drop(old);
    assert_eq!(c.usage(), 4);
    assert_eq!(c.capacity(), 1 << 20);
}

#[test]
fn io_buffers_are_charged_their_capacity() {
    let c = BlockCache::new(1 << 20, 1);
    let mut buf = IoBuf::zeroed(100);
    buf[..3].copy_from_slice(b"abc");
    let h = c.insert(key(1, 0), buf.into(), Priority::Normal);
    assert_eq!(h.len(), 100);
    assert_eq!(&h[..3], b"abc");
    assert_eq!(c.usage(), IoBuf::ALIGN);
}

#[test]
fn disabled_cache_stores_nothing() {
    let c = BlockCache::disabled();
    let h = c.insert(key(1, 0), b"x".to_vec().into(), Priority::High);
    assert_eq!(h.bytes(), b"x");
    assert!(c.get(key(1, 0)).is_none());
    assert_eq!(c.usage(), 0);
    assert_eq!(c.capacity(), 0);
    c.erase_file(1);
}

#[test]
fn oversized_block_is_returned_but_not_cached() {
    let c = BlockCache::new(1000, 1);
    let h = c.insert(key(1, 0), vec![1; 2000].into(), Priority::High);
    assert_eq!(h.len(), 2000);
    assert!(c.get(key(1, 0)).is_none());
    assert_eq!(c.usage(), 0);
}

#[test]
fn erase_file_drops_only_that_files_unpinned_blocks() {
    let c = BlockCache::new(1 << 20, 4);
    for off in 0..50 {
        drop(c.insert(key(1, off * 4096), vec![1; 10].into(), Priority::Normal));
        drop(c.insert(key(2, off * 4096), vec![2; 10].into(), Priority::Normal));
    }
    let pinned = c.get(key(1, 0)).expect("hit");
    c.erase_file(1);
    assert_eq!(c.usage(), 50 * 10 + 10);
    for off in 1..50 {
        assert!(c.get(key(1, off * 4096)).is_none());
        assert!(c.get(key(2, off * 4096)).is_some());
    }
    assert_eq!(
        c.get(key(1, 0)).expect("pinned block erased").bytes(),
        pinned.bytes()
    );
    drop(pinned);
    c.erase_file(1);
    assert!(c.get(key(1, 0)).is_none());
    assert_eq!(c.usage(), 50 * 10);
}

#[test]
fn usage_stays_within_capacity_across_shards() {
    let c = BlockCache::new(64 << 10, 8);
    for off in 0..if cfg!(miri) { 500 } else { 10_000u64 } {
        drop(c.insert(
            key(off % 7, off * 4096),
            vec![0; 512].into(),
            Priority::Normal,
        ));
        if off % 3 == 0 {
            drop(c.get(key(off % 7, (off / 2) * 4096)));
        }
    }
    assert!(c.usage() <= c.capacity());
    assert!(
        c.usage() > c.capacity() / 2,
        "cache barely used: {}",
        c.usage()
    );
}

#[test]
fn cells_pin_their_block() {
    let c = BlockCache::new(100, 1);
    let cell = Cell::in_block(
        c.insert(key(1, 0), b"0123456789".to_vec().into(), Priority::Low),
        2..5,
    );
    for off in 1..100 {
        drop(c.insert(key(1, off), vec![0; 10].into(), Priority::High));
    }
    assert_eq!(cell.bytes(), b"234");
    assert!(
        c.get(key(1, 0)).is_some(),
        "a block pinned by a cell was evicted"
    );
    let clone = cell.clone();
    drop(cell);
    assert_eq!(&clone[..], b"234");
    assert_eq!(Cell::in_block(clone_handle(&c), 0..0).bytes(), b"");
}

fn clone_handle(c: &BlockCache) -> BlockHandle {
    c.get(key(1, 0)).expect("hit")
}

#[test]
#[should_panic(expected = "outside a block")]
fn cell_range_past_block_panics() {
    let h = BlockCache::disabled().insert(key(1, 0), vec![0; 4].into(), Priority::Normal);
    let _ = Cell::in_block(h, 2..5);
}

#[test]
fn owned_cell() {
    let c = Cell::owned(vec![1, 2, 3]);
    assert_eq!(&c.clone()[..], &[1, 2, 3]);
    assert!(format!("{c:?}").contains("owned"));
}

#[test]
fn row_cache_keys_by_family_row_and_epoch() {
    let rows = RowCache::new(1 << 20);
    drop(rows.insert(1, b"row", 5, b"f1".to_vec()));
    drop(rows.insert(2, b"row", 5, b"f2".to_vec()));
    assert_eq!(&rows.get(1, b"row", 5).expect("hit")[..], b"f1");
    assert_eq!(&rows.get(2, b"row", 5).expect("hit")[..], b"f2");
    assert!(rows.get(1, b"row", 6).is_none());
    assert!(rows.get(1, b"ro", 5).is_none());
    assert!(rows.get(3, b"row", 5).is_none());
    rows.invalidate(1, b"row");
    assert!(rows.get(1, b"row", 5).is_none());
    assert!(rows.get(2, b"row", 5).is_some());
}

#[test]
fn zero_capacity_row_cache_stores_nothing() {
    let rows = RowCache::new(0);
    let h = rows.insert(1, b"r", 1, vec![1]);
    assert_eq!(&h[..], &[1]);
    assert!(rows.get(1, b"r", 1).is_none());
}

#[derive(Debug, Clone)]
enum Op {
    Insert {
        file: u64,
        off: u64,
        len: usize,
        prio: Priority,
    },
    Get {
        file: u64,
        off: u64,
    },
    Pin {
        file: u64,
        off: u64,
    },
    Unpin(usize),
    Erase(u64),
}

fn op() -> impl Strategy<Value = Op> {
    let prio = prop_oneof![
        Just(Priority::Low),
        Just(Priority::Normal),
        Just(Priority::High)
    ];
    prop_oneof![
        4 => (0..3u64, 0..12u64, 8..300usize, prio)
            .prop_map(|(file, off, len, prio)| Op::Insert { file, off, len, prio }),
        3 => (0..3u64, 0..12u64).prop_map(|(file, off)| Op::Get { file, off }),
        1 => (0..3u64, 0..12u64).prop_map(|(file, off)| Op::Pin { file, off }),
        1 => (0..8usize).prop_map(Op::Unpin),
        1 => (0..3u64).prop_map(Op::Erase),
    ]
}

/// Block contents unique to one insert: the generation, then filler.
fn contents(generation: u64, len: usize) -> Vec<u8> {
    let mut v = generation.to_le_bytes().to_vec();
    v.resize(len, generation as u8);
    v
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) { 4 } else { ProptestConfig::default().cases },
        ..ProptestConfig::default()
    })]

    /// Against a model of "the latest insert per key": a hit always returns the latest
    /// contents, a pinned latest block is always a hit, held handles never change, and with
    /// nothing pinned the cache is within capacity.
    #[test]
    fn model(ops in prop::collection::vec(op(), 1..200), shards in 1..3usize) {
        let cache = BlockCache::new(1500, shards);
        let mut latest: HashMap<BlockKey, Vec<u8>> = HashMap::new();
        let mut pins: Vec<(BlockKey, Vec<u8>, BlockHandle)> = Vec::new();
        let mut ever_pinned = false;
        for (generation, op) in ops.into_iter().enumerate() {
            match op {
                Op::Insert { file, off, len, prio } => {
                    let k = key(file, off);
                    let v = contents(generation as u64, len);
                    let h = cache.insert(k, BlockData::from(v.clone()), prio);
                    prop_assert_eq!(h.bytes(), &v[..]);
                    latest.insert(k, v);
                }
                Op::Get { file, off } => {
                    if let Some(h) = cache.get(key(file, off)) {
                        prop_assert_eq!(Some(h.bytes()), latest.get(&key(file, off)).map(|v| &v[..]));
                    }
                }
                Op::Pin { file, off } => {
                    let k = key(file, off);
                    if let Some(h) = cache.get(k) {
                        pins.push((k, h.to_vec(), h));
                        ever_pinned = true;
                    }
                }
                Op::Unpin(i) => {
                    if !pins.is_empty() {
                        pins.swap_remove(i % pins.len());
                    }
                }
                Op::Erase(file) => {
                    cache.erase_file(file);
                    latest.retain(|k, v| {
                        k.file != file || pins.iter().any(|(pk, pv, _)| pk == k && pv == v)
                    });
                }
            }
            for (k, v, h) in &pins {
                prop_assert_eq!(h.bytes(), &v[..]);
                if latest.get(k) == Some(v) {
                    prop_assert!(cache.get(*k).is_some(), "pinned block {:?} evicted", k);
                }
            }
            // A shard pushed over capacity by pins shrinks on its next insert.
            if !ever_pinned {
                prop_assert!(cache.usage() <= cache.capacity());
            }
        }
        drop(pins);
        for off in 0..64 {
            drop(cache.insert(key(9, off), vec![0; 8].into(), Priority::Low));
        }
        prop_assert!(cache.usage() <= cache.capacity());
    }
}

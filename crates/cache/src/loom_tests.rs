//! Loom models: a pinned `BlockHandle` or `Cell` outlives eviction, replacement and
//! `erase_file` racing on other threads, and its bytes never change underneath it.
//!
//! Memory safety itself (no use-after-free of evicted bytes) comes from `Arc` ownership:
//! eviction only drops the shard's reference. What these models check is the protocol on
//! top: a handle is never handed out for an entry that eviction or `erase_file` has already
//! removed from the shard's index and accounting, pinned entries stay indexed and charged,
//! and the bytes a handle sees are the ones that were inserted.
//!
//! Run with `RUSTFLAGS="--cfg loom" cargo test --release -p pigeonhole-cache --lib loom`.

use loom::thread;

use super::*;

fn key(offset: u64) -> BlockKey {
    BlockKey { file: 1, offset }
}

fn block(byte: u8) -> BlockData {
    vec![byte; 64].into()
}

/// One shard that fits one block, so every insert evicts.
fn tiny() -> Arc<BlockCache> {
    Arc::new(BlockCache::new(64, 1))
}

#[test]
fn loom_get_races_evicting_insert() {
    loom::model(|| {
        let cache = tiny();
        drop(cache.insert(key(0), block(1), Priority::Normal));

        let c = Arc::clone(&cache);
        let reader = thread::spawn(move || {
            if let Some(h) = c.get(key(0)) {
                let cell = Cell::in_block(h, 8..16);
                thread::yield_now();
                assert_eq!(cell.bytes(), &[1; 8]);
            }
        });
        let c = Arc::clone(&cache);
        let writer = thread::spawn(move || {
            drop(c.insert(key(1), block(2), Priority::High));
        });
        reader.join().unwrap();
        writer.join().unwrap();
        assert!(cache.usage() <= 128);
    });
}

#[test]
fn loom_pinned_handle_survives_replace_and_erase() {
    loom::model(|| {
        let cache = tiny();
        let pinned = cache.insert(key(0), block(1), Priority::Low);

        let c = Arc::clone(&cache);
        let replacer = thread::spawn(move || {
            drop(c.insert(key(0), block(2), Priority::Normal));
        });
        let c = Arc::clone(&cache);
        let eraser = thread::spawn(move || c.erase_file(1));

        let cell = Cell::in_block(pinned.clone(), 0..64);
        drop(pinned);
        assert_eq!(cell.bytes(), &[1; 64]);
        replacer.join().unwrap();
        eraser.join().unwrap();
        assert_eq!(cell.bytes(), &[1; 64]);
        if let Some(h) = cache.get(key(0)) {
            assert_eq!(h.bytes(), &[2; 64]);
        }
    });
}

#[test]
fn loom_handle_dropped_while_evicting() {
    loom::model(|| {
        let cache = tiny();
        let pinned = cache.insert(key(0), block(1), Priority::Normal);

        let dropper = thread::spawn(move || {
            assert_eq!(pinned.bytes(), &[1; 64]);
            drop(pinned);
        });
        let c = Arc::clone(&cache);
        let writer = thread::spawn(move || {
            let h = c.insert(key(1), block(2), Priority::Normal);
            assert_eq!(h.bytes(), &[2; 64]);
        });
        dropper.join().unwrap();
        writer.join().unwrap();
        // Both are unpinned now; the next insert brings the shard back to capacity.
        drop(cache.insert(key(2), block(3), Priority::Normal));
        assert_eq!(cache.usage(), 64);
    });
}

#[test]
fn loom_row_handle_survives_invalidate() {
    loom::model(|| {
        let rows = Arc::new(RowCache::new(1 << 16));
        drop(rows.insert(1, b"r", 1, vec![7; 16]));

        let r = Arc::clone(&rows);
        let reader = thread::spawn(move || {
            if let Some(h) = r.get(1, b"r", 1) {
                thread::yield_now();
                assert_eq!(&h[..], &[7; 16]);
            }
        });
        let r = Arc::clone(&rows);
        let writer = thread::spawn(move || {
            r.invalidate(1, b"r");
            drop(r.insert(1, b"r", 2, vec![8; 16]));
        });
        reader.join().unwrap();
        writer.join().unwrap();
        assert!(rows.get(1, b"r", 1).is_none());
    });
}

#[test]
fn loom_get_races_erase_file_and_eviction() {
    loom::model(|| {
        // Room for two blocks: the evicting insert must pick a victim among key(0) and
        // key(1), which `get` may be pinning at the same moment.
        let cache = Arc::new(BlockCache::new(128, 1));
        drop(cache.insert(key(0), block(1), Priority::Normal));
        drop(cache.insert(BlockKey { file: 2, offset: 0 }, block(2), Priority::Normal));

        let c = Arc::clone(&cache);
        let reader = thread::spawn(move || {
            if let Some(h) = c.get(key(0)) {
                // Pinned: neither the eraser nor the evictor may unindex or uncharge it.
                thread::yield_now();
                assert_eq!(h.bytes(), &[1; 64]);
                let again = c.get(key(0)).expect("pinned block left the index");
                assert!(
                    Arc::ptr_eq(&again.block, &h.block),
                    "handle for an evicted entry"
                );
                assert!(c.usage() >= 64, "pinned block uncharged");
            }
        });
        let c = Arc::clone(&cache);
        let eraser = thread::spawn(move || c.erase_file(1));
        let c = Arc::clone(&cache);
        let evictor = thread::spawn(move || {
            drop(c.insert(BlockKey { file: 3, offset: 0 }, block(3), Priority::High));
        });
        reader.join().unwrap();
        eraser.join().unwrap();
        evictor.join().unwrap();
        assert!(cache.usage() <= 128 + 64);
        if let Some(h) = cache.get(key(0)) {
            assert_eq!(h.bytes(), &[1; 64]);
        }
    });
}

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

/// The row cache's epoch protocol (D201): a writer raises the row's watermark, then writes,
/// then publishes visibility; a filler stores the row it read at its read point under the
/// epoch it loaded after that point; a reader hits only on an equal epoch at or below its
/// own read point. Whatever the interleaving, a hit returns the row as of the reader's read
/// point. The row's state at seqno `s` is `s` itself (the write at seqno 2 replaces the
/// state at 1), so a stale hit would return 1 to a reader at 2.
///
/// The entry stands for `RowCache`'s (one entry per row, exact epoch match), packed into one
/// atomic `epoch << 32 | state` that is replaced whole: the cache's own concurrency is
/// modelled above, and its shards and locks would only multiply the interleavings.
#[test]
fn loom_row_epochs_never_serve_a_stale_row() {
    use loom::sync::atomic::{AtomicU64, Ordering};

    // Bounded as the memtable's models are: the stale-hit schedules need two preemptions.
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(|| {
        let epochs = Arc::new(RowEpochs::new(1));
        // The cached entry: `(epoch, row state)`. The state at seqno 1 is cached already.
        let entry = Arc::new(AtomicU64::new(1 << 32 | 1));
        let unpack = |v: u64| (v >> 32, v & 0xffff_ffff);
        let visible = Arc::new(AtomicU64::new(1));
        let h = 7;
        epochs.note_write(h, 1);

        let writer = {
            let (epochs, visible) = (Arc::clone(&epochs), Arc::clone(&visible));
            thread::spawn(move || {
                epochs.note_write(h, 2);
                visible.store(2, Ordering::Release);
            })
        };
        let filler = {
            let (epochs, entry, visible) = (
                Arc::clone(&epochs),
                Arc::clone(&entry),
                Arc::clone(&visible),
            );
            thread::spawn(move || {
                let s = visible.load(Ordering::Acquire);
                if let Some(e) = epochs.epoch(h, s) {
                    entry.store(e << 32 | s, Ordering::SeqCst);
                }
            })
        };
        let reader = {
            let (epochs, entry, visible) = (
                Arc::clone(&epochs),
                Arc::clone(&entry),
                Arc::clone(&visible),
            );
            thread::spawn(move || {
                let s = visible.load(Ordering::Acquire);
                if let Some(e) = epochs.epoch(h, s) {
                    let (epoch, state) = unpack(entry.load(Ordering::SeqCst));
                    if epoch == e {
                        assert_eq!(state, s, "a stale row served at read point {s}");
                    }
                }
            })
        };
        writer.join().unwrap();
        filler.join().unwrap();
        reader.join().unwrap();
        // Once the write is visible, a hit is the new state.
        let s = visible.load(Ordering::Acquire);
        let e = epochs.epoch(h, s).expect("visible");
        let (epoch, state) = unpack(entry.load(Ordering::SeqCst));
        if epoch == e {
            assert_eq!(state, 2);
        }
    });
}

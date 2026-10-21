//! Loom model of the flushed-through seqno a peer's checkpoint reads (D266;
//! `RUSTFLAGS="--cfg engine_loom" cargo test --release -p pigeonhole-engine --lib loom`).
//!
//! The protocol's shared state, with the orderings the engine uses:
//! - `flushed_through[p]` (`Shared`): shard p's `store(Release)` (`store_flushed_through`)
//!   of `min(oldest unflushed − 1, visible)`, after a flush's root commit; a peer's
//!   checkpoint `load(Acquire)` (`peers_behind`).
//! - the root commit (`ManifestPump`): durable before p hears `Flushed` and publishes. The
//!   model stores it on p's thread, Relaxed: only the publish orders it for the peer.
//! - the visible watermark (`shm`): the coordinator raises it past a cross share's seqno S
//!   (Release) only once every participant applied (`Applied`, modelled as a flag stored
//!   Release after p applies and loaded Acquire by the coordinator); p loads it Acquire.
//!
//! The claim: a peer that reads `flushed_through[p] ≥ S` sees the root commit that put p's
//! share at S in an SST, even when p published before the share reached it (the cap).

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use loom::thread;

/// The share's seqno.
const S: u64 = 1;

/// `flushed_through` from `oldest` (p's oldest unflushed entry) and the visible watermark.
fn bound(oldest: Option<u64>, visible: &AtomicU64) -> u64 {
    let vis = visible.load(Ordering::Acquire);
    oldest.map_or(vis, |o| (o - 1).min(vis))
}

#[test]
fn loom_a_peer_that_reads_the_bound_past_a_share_sees_its_flush() {
    loom::model(|| {
        let visible = Arc::new(AtomicU64::new(0));
        let applied = Arc::new(AtomicBool::new(false));
        let root = Arc::new(AtomicU64::new(0));
        let f = Arc::new(AtomicU64::new(0));

        // Shard p: publishes with nothing unflushed (an idle participant asked early), then
        // applies the share at S, answers `Applied`, flushes it and publishes again.
        let p = {
            let (visible, applied, root, f) = (
                Arc::clone(&visible),
                Arc::clone(&applied),
                Arc::clone(&root),
                Arc::clone(&f),
            );
            thread::spawn(move || {
                f.store(bound(None, &visible), Ordering::Release);
                let oldest = Some(S);
                applied.store(true, Ordering::Release);
                // Unflushed: the bound stays below S.
                f.store(bound(oldest, &visible), Ordering::Release);
                root.store(S, Ordering::Relaxed);
                f.store(bound(None, &visible), Ordering::Release);
            })
        };
        // The coordinator: S becomes visible once p applied.
        let c = {
            let (visible, applied) = (Arc::clone(&visible), Arc::clone(&applied));
            thread::spawn(move || {
                if applied.load(Ordering::Acquire) {
                    visible.store(S, Ordering::Release);
                }
            })
        };
        // A peer's checkpoint: past S only if p's share at S is in an SST.
        if f.load(Ordering::Acquire) >= S {
            assert_eq!(
                root.load(Ordering::Relaxed),
                S,
                "a peer read the bound past S before p's flush"
            );
        }
        p.join().unwrap();
        c.join().unwrap();
    });
}

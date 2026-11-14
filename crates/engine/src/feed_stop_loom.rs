//! Loom model of the sidecar stop's publication to reader processes (D315 amendment, #1869;
//! `RUSTFLAGS="--cfg engine_loom" cargo test --release -p pigeonhole-engine --lib loom`).
//!
//! `Feed::stop_appends` and `Feed::publish_to`, operation for operation. Loom can't
//! instrument the shipped `OnceLock<ShmRegion>` or the mapped header word, so:
//! - `Feed::appends_stopped` is the `AtomicBool` it is: stored SeqCst by `stop_appends`,
//!   loaded SeqCst by `publish_to`.
//! - `Feed::shm` (`OnceLock`) is an `AtomicBool`: `set` publishes with Release, `get` loads
//!   with Acquire, as `OnceLock`'s state does.
//! - shm `feed_flags` is an `AtomicU32`: `fetch_or(FEED_APPENDS_STOPPED, Release)`.
//! - Each side has a SeqCst fence between its store and its load.
//!
//! The claim: whichever comes first, a stop (one of the three stop sites) or the open
//! handing the session its region, once both have run the region says the appends
//! stopped. Without the fences the store-then-load pair can miss on both sides.

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicU32, Ordering, fence};
use loom::thread;

/// shm `FEED_APPENDS_STOPPED`.
const STOPPED: u32 = 1;

/// The shared state the two sides touch.
#[derive(Default)]
struct Session {
    /// `Feed::appends_stopped`.
    stopped: AtomicBool,
    /// `Feed::shm` set (the `OnceLock`'s state).
    region: AtomicBool,
    /// shm `feed_flags`.
    flags: AtomicU32,
}

impl Session {
    /// `Feed::stop_appends`.
    fn stop_appends(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if self.region.load(Ordering::Acquire) {
            self.flags.fetch_or(STOPPED, Ordering::Release);
        }
    }

    /// `Feed::publish_to`.
    fn publish_to(&self) {
        self.region.store(true, Ordering::Release);
        fence(Ordering::SeqCst);
        if self.stopped.load(Ordering::SeqCst) && self.region.load(Ordering::Acquire) {
            self.flags.fetch_or(STOPPED, Ordering::Release);
        }
    }
}

#[test]
fn loom_a_stop_reaches_the_region_whichever_comes_first() {
    loom::model(|| {
        let s = Arc::new(Session::default());
        // A stop site (an append, a Root write or the sync's completion) on its own thread.
        let stopper = {
            let s = Arc::clone(&s);
            thread::spawn(move || s.stop_appends())
        };
        // The open, handing the session its region.
        s.publish_to();
        stopper.join().unwrap();
        assert_eq!(
            s.flags.load(Ordering::Acquire),
            STOPPED,
            "both ran, and the region doesn't say the appends stopped"
        );
    });
}

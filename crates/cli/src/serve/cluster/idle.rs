//! A cluster subscription's idle limit once its merge is the client's own stream (#1758's
//! option B, simp724's R1 on its plan).
//!
//! A stream is polled only while its client reads: once the client stops and its HTTP/2
//! window fills, nothing polls it, so a timer inside it never fires. Yet a stuck client must
//! not keep what its subscription holds: its `--max-streams` slot, its parts (their streams
//! and the subscription's peer leases). [`Watched`] keeps that state where a small task can
//! take it: past `--stream-idle-ms` from the client's last take, the task drops it, and the
//! stream, if it's ever polled again, ends with the idle error. Only the HTTP/2 stream's
//! shell is left, until the connection goes.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::serve::feed::TakeClock;

/// A stream's state, dropped by its watchdog once the client hasn't taken anything for the
/// idle limit.
pub(crate) struct Watched<T> {
    state: Arc<Mutex<Option<T>>>,
    clock: Arc<TakeClock>,
    /// Dropped with the stream: the watchdog ends.
    _stop: tokio::sync::oneshot::Sender<()>,
}

impl<T: Send + 'static> Watched<T> {
    /// `state`, watched from now: the client has until `idle` after its last take (or after
    /// now, before the first). Spawns the watchdog on the current runtime.
    pub(crate) fn new(state: T, idle: Duration) -> Self {
        let state = Arc::new(Mutex::new(Some(state)));
        let clock = Arc::new(TakeClock::new());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        tokio::spawn(watch(Arc::clone(&state), Arc::clone(&clock), idle, stopped));
        Self {
            state,
            clock,
            _stop: stop,
        }
    }
}

impl<T> Watched<T> {
    /// The client took something now.
    pub(crate) fn took(&self) {
        self.clock.took();
    }

    /// The state: `None` once the watchdog dropped it.
    pub(crate) fn lock(&self) -> MutexGuard<'_, Option<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The watchdog: sleeps until the last take plus `idle`, again while takes move that on, and
/// drops the state once it passes with none. Ends with the stream (`stopped`). One timer per
/// idle period, not per take.
async fn watch<T>(
    state: Arc<Mutex<Option<T>>>,
    clock: Arc<TakeClock>,
    idle: Duration,
    mut stopped: tokio::sync::oneshot::Receiver<()>,
) {
    let begun = tokio::time::Instant::now();
    loop {
        let due = clock.last().unwrap_or(begun).max(begun) + idle;
        tokio::select! {
            _ = &mut stopped => return,
            () = tokio::time::sleep_until(due) => {
                let due = clock.last().unwrap_or(begun).max(begun) + idle;
                if tokio::time::Instant::now() >= due {
                    // Out of the lock first, then dropped: a part's drop may take a while.
                    let gone = state.lock().unwrap_or_else(PoisonError::into_inner).take();
                    drop(gone);
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    /// What a subscription holds, as the test sees it: a slot, and a flag set when dropped.
    struct Held {
        _slot: tokio::sync::OwnedSemaphorePermit,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for Held {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    fn paused() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap()
    }

    fn held(slots: &Arc<tokio::sync::Semaphore>) -> (Held, Arc<AtomicBool>) {
        let dropped = Arc::new(AtomicBool::new(false));
        let h = Held {
            _slot: Arc::clone(slots).try_acquire_owned().unwrap(),
            dropped: Arc::clone(&dropped),
        };
        (h, dropped)
    }

    /// Lets the watchdog run up to `d` from now.
    async fn after(d: Duration) {
        tokio::time::sleep(d).await;
        tokio::task::yield_now().await;
    }

    #[test]
    fn a_client_that_never_takes_loses_its_slot_and_state_at_the_idle_limit() {
        paused().block_on(async {
            let idle = Duration::from_secs(5);
            let slots = Arc::new(tokio::sync::Semaphore::new(1));
            let (h, dropped) = held(&slots);
            let w = Watched::new(h, idle);
            // Nothing polls the stream: only the watchdog runs.
            after(idle - Duration::from_millis(1)).await;
            assert!(w.lock().is_some(), "not yet");
            assert_eq!(slots.available_permits(), 0);
            after(Duration::from_millis(2)).await;
            assert!(w.lock().is_none(), "dropped at the limit");
            assert!(dropped.load(Ordering::SeqCst));
            assert_eq!(slots.available_permits(), 1, "its slot is free");
        });
    }

    #[test]
    fn takes_move_the_limit_on_and_it_counts_from_the_last() {
        paused().block_on(async {
            let idle = Duration::from_secs(5);
            let slots = Arc::new(tokio::sync::Semaphore::new(1));
            let (h, dropped) = held(&slots);
            let w = Watched::new(h, idle);
            // A take every half limit, for four limits: never idle.
            for _ in 0..8 {
                after(idle / 2).await;
                w.took();
            }
            assert!(w.lock().is_some());
            after(idle - Duration::from_millis(1)).await;
            assert!(w.lock().is_some(), "the limit counts from the last take");
            after(Duration::from_millis(2)).await;
            assert!(w.lock().is_none());
            assert!(dropped.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn a_stream_dropped_ends_its_watchdog_and_frees_its_slot() {
        paused().block_on(async {
            let slots = Arc::new(tokio::sync::Semaphore::new(1));
            let (h, dropped) = held(&slots);
            let w = Watched::new(h, Duration::from_secs(5));
            drop(w);
            after(Duration::from_millis(1)).await;
            assert!(
                dropped.load(Ordering::SeqCst),
                "the state went with the stream"
            );
            assert_eq!(slots.available_permits(), 1);
        });
    }
}

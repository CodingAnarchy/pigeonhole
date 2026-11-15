//! A crate's background loop run by the server under a cluster partition's write gate
//! (D317 ticket 6): a vector index's rebalancer, and a memory store's forgetter and IVF
//! rebalancer (#1915). The crates' own threads would write past the gate, so a fenced
//! partition (an operator's fence, or one waiting for its peers' maps) would keep changing.
//!
//! Each slice of work enters the gate first and holds it only for that slice, so a fence's
//! wait for its writes to drain waits for one slice at most (simp724's (b) on #1915). A
//! fenced gate skips the slice and waits. The loop ends once its index or store closes.

use std::sync::Arc;
use std::time::Duration;

/// How long a loop works between looks at the gate and at whether it closed.
pub(crate) const SLICE: Duration = Duration::from_millis(50);

/// How long a loop with nothing to do, or a fenced one, waits before it looks again.
pub(crate) const IDLE: Duration = Duration::from_secs(1);

/// A crate's loop of bounded slices: the shape `Rebalancer` and `Forgetter` share.
pub(crate) trait Slices: Send + 'static {
    /// Whether what it works on closed: the loop ends.
    fn closed(&self) -> bool;
    /// Works for about `budget`: whether to call again soon.
    fn run_once(&mut self, budget: Duration) -> Result<bool, String>;
    /// Waits until there's work, it closes, or `timeout` passes.
    fn wait(&mut self, timeout: Duration);
}

impl Slices for pigeonhole_vector::Rebalancer {
    fn closed(&self) -> bool {
        Self::closed(self)
    }
    fn run_once(&mut self, budget: Duration) -> Result<bool, String> {
        Self::run_once(self, budget).map_err(|e| e.to_string())
    }
    fn wait(&mut self, timeout: Duration) {
        Self::wait(self, timeout);
    }
}

/// A forget pass deletes one memory per transaction, up to the policy's `max_per_pass`: the
/// gate is entered again before each `run_once`.
impl Slices for pigeonhole_memory::Forgetter {
    fn closed(&self) -> bool {
        Self::closed(self)
    }
    fn run_once(&mut self, budget: Duration) -> Result<bool, String> {
        Self::run_once(self, budget).map_err(|e| e.to_string())
    }
    fn wait(&mut self, timeout: Duration) {
        Self::wait(self, timeout);
    }
}

/// Runs `l` until it closes, each slice under `gate` when there is one. `what` names it in
/// the log line of a failed slice, which leaves its work for a later one.
pub(crate) fn run(mut l: impl Slices, what: &str, gate: Option<&Arc<super::gate::WriteGate>>) {
    while !l.closed() {
        let held = match gate.map(|g| g.enter()) {
            Some(Err(_)) => {
                l.wait(IDLE);
                continue;
            }
            Some(Ok(h)) => Some(h),
            None => None,
        };
        let worked = l.run_once(SLICE);
        drop(held);
        match worked {
            Ok(true) => {}
            Ok(false) => l.wait(IDLE),
            Err(e) => {
                eprintln!("phdb serve: {what}: {e}");
                l.wait(IDLE);
            }
        }
    }
}

/// [`run`] on a thread of its own named `thread`, entering `placement`'s CPUs first.
pub(crate) fn spawn(
    l: impl Slices,
    thread: &str,
    what: String,
    gate: Option<Arc<super::gate::WriteGate>>,
    placement: Option<Arc<super::placement::Placement>>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name(thread.to_owned())
        .spawn(move || {
            if let Some(p) = placement {
                p.enter();
            }
            run(l, &what, gate.as_ref());
        })
}

#[cfg(test)]
mod tests {
    use super::{Slices, run};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// A loop that counts its slices and waits, and closes after `waits` waits.
    struct Counting {
        slices: Arc<AtomicUsize>,
        waits: usize,
    }

    impl Slices for Counting {
        fn closed(&self) -> bool {
            self.waits == 0
        }
        fn run_once(&mut self, _: Duration) -> Result<bool, String> {
            self.slices.fetch_add(1, Ordering::Relaxed);
            Ok(false)
        }
        fn wait(&mut self, _: Duration) {
            self.waits -= 1;
        }
    }

    /// #1915 (M2): a fenced partition's background loop (a memory store's forgetter, an
    /// index's rebalancer) runs no slice; once unfenced, it does, and each slice is counted
    /// on the gate, so a fence waits for one slice at most.
    #[test]
    fn a_fenced_partitions_loop_runs_no_slice() {
        let gate = crate::serve::gate::WriteGate::opened("p1", true);
        let slices = Arc::new(AtomicUsize::new(0));
        let l = || Counting {
            slices: Arc::clone(&slices),
            waits: 3,
        };
        run(l(), "test", Some(&gate));
        assert_eq!(slices.load(Ordering::Relaxed), 0);
        gate.unfence();
        run(l(), "test", Some(&gate));
        assert_eq!(slices.load(Ordering::Relaxed), 3);
        assert_eq!(gate.inflight(), 0, "each slice's hold ended with it");
        // Without a gate (a single node), every slice runs.
        run(l(), "test", None);
        assert_eq!(slices.load(Ordering::Relaxed), 6);
    }
}

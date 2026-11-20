use super::*;

/// Where a client is, for the feed dump ([`Feeds::dump`], `phdb serve` on SIGUSR1): two
/// relaxed stores per batch, read only by the dump.
#[derive(Debug, Default)]
pub(crate) struct Tracker {
    /// [`ON_RING`], [`ON_OWN`] or [`WAITING`].
    pub(crate) state: AtomicU64,
    /// On the ring, its next index; on its own stream, the seqno it covered.
    at: AtomicU64,
    /// The hub it's counted under (its address): a client that moved to a failed hub's
    /// replacement is counted there only (perf287's N1 on #1643).
    hub: AtomicUsize,
}

pub(crate) const ON_RING: u64 = 0;
pub(crate) const ON_OWN: u64 = 1;
/// Waiting for a `--max-catch-up` slot, delivering nothing.
pub(crate) const WAITING: u64 = 2;

impl Tracker {
    pub(crate) fn set(&self, state: u64, at: u64) {
        self.state.store(state, Ordering::Relaxed);
        self.at.store(at, Ordering::Relaxed);
    }
}

/// When a client last took an event, for a send waiting for room: its `--stream-idle-ms`
/// counts from the last take (simp724's I1 on #1536). A take stores the time; it wakes no
/// one. A waiting send's one timer, when it fires, reads it and waits on if the client took
/// something meanwhile (perf287's profile on #1397: a wake and a new timer per take, per
/// subscriber, were `Client::send`'s 9.6% self at 16 hubs × 16).
///
/// A batch ([`Sink::put_all`]) is stamped when the connection takes it from the channel and
/// at its last event; its events in between are stamped only while a send waits
/// ([`TakeClock::watch`]). A clock read per event was 6% of the network workers' cycles at
/// saturation (g129, #1737), and without a waiting send no one reads the time between. So
/// a waiting send still sees every take, and a client draining a long batch slowly is slow,
/// not idle.
#[derive(Debug)]
pub(crate) struct TakeClock {
    /// On Tokio's clock, so a paused test clock pauses it too.
    base: tokio::time::Instant,
    /// Nanoseconds from `base` to the last take; 0 before the first.
    last: AtomicU64,
    /// Sends waiting for room now ([`TakeClock::watch`]).
    watchers: AtomicUsize,
}

impl TakeClock {
    pub(crate) fn new() -> Self {
        Self {
            base: tokio::time::Instant::now(),
            last: AtomicU64::new(0),
            watchers: AtomicUsize::new(0),
        }
    }

    /// A take, now.
    pub(crate) fn took(&self) {
        let nanos = u64::try_from(self.base.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.last.store(nanos.max(1), Ordering::Relaxed);
    }

    /// A take within a batch: stamped only while a send waits for room. Sequentially
    /// consistent with [`TakeClock::watch`]'s count, which a waiting send raises before it
    /// reads the time its wait began: a take that reads 0 was ordered before the raise, so
    /// at most one event at the wait's start goes unstamped, and the wait counts from its
    /// start anyway (simp724's N1 on #2012).
    fn took_within(&self) {
        if self.watchers.load(Ordering::SeqCst) != 0 {
            self.took();
        }
    }

    /// While the guard lives, every take is stamped, not just a batch's first and last: a
    /// send waiting for room counts its idle limit from the client's last take.
    fn watch(&self) -> Watch<'_> {
        self.watchers.fetch_add(1, Ordering::SeqCst);
        Watch(self)
    }

    /// The last take, or the clock's start (the stream's) before the first: what an idle
    /// limit counts from (#1798 (a)'s merge task).
    pub(crate) fn last_or_start(&self) -> tokio::time::Instant {
        self.last().unwrap_or(self.base)
    }

    /// When the last take was, or `None` before the first.
    pub(crate) fn last(&self) -> Option<tokio::time::Instant> {
        match self.last.load(Ordering::Relaxed) {
            0 => None,
            n => Some(self.base + Duration::from_nanos(n)),
        }
    }
}

/// A send waiting for room ([`TakeClock::watch`]).
struct Watch<'a>(&'a TakeClock);

impl Drop for Watch<'_> {
    fn drop(&mut self) {
        self.0.watchers.fetch_sub(1, Ordering::SeqCst);
    }
}

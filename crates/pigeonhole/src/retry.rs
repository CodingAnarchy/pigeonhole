//! Retrying optimistic transactions (#694, ICR 0060), and the transaction counters behind
//! [`EngineMetrics`](crate::EngineMetrics)' transaction fields (ICR 0061).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::{CommitInfo, Durability, Error, ErrorCode, Pigeonhole, Result, Transaction};

/// How [`Pigeonhole::transact`] retries a transaction (ICR 0060).
///
/// An attempt that fails with [`ErrorCode::Conflict`] (something it read changed before it
/// committed) or [`ErrorCode::SnapshotExpired`] (a recovery replaced the engine it began on)
/// is retried after a pause; with [`retry_busy`](Self::retry_busy), so is one that fails
/// with [`ErrorCode::Busy`]. Pauses grow exponentially from
/// [`backoff`](Self::backoff)'s base to its cap, each drawn uniformly below its bound ("full
/// jitter"), so competing transactions spread out instead of colliding again. Retrying stops
/// at [`max_attempts`](Self::max_attempts) or past [`deadline`](Self::deadline), whichever
/// comes first, returning the last error.
///
/// The defaults: no attempt cap, a 30 s deadline (the default write-stall timeout), pauses
/// from 100 µs up to 100 ms, `Busy` not retried, the writer's default durability.
///
/// A plain value: numbers, durations and flags, no closure or trait object.
/// [`Pigeonhole::transact`] is a Rust-only convenience (a closure and a generic error) over
/// it; everything it decides is reachable without it, so an async caller with its own timer
/// (or a future C ABI) drives the same loop with [`Pigeonhole::transaction`] and
/// [`next_pause`](Self::next_pause) (or [`retries_code`](Self::retries_code) and
/// [`delay`](Self::delay)).
///
/// ```
/// use std::time::Duration;
/// use pigeonhole::RetryPolicy;
///
/// let policy = RetryPolicy::default()
///     .max_attempts(10)
///     .deadline(Duration::from_secs(2))
///     .backoff(Duration::from_micros(50), Duration::from_millis(20));
/// assert!(policy.delay(1) <= Duration::from_micros(50));
/// assert!(policy.delay(30) <= Duration::from_millis(20));
/// ```
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    max_attempts: Option<u32>,
    deadline: Option<Duration>,
    base: Duration,
    cap: Duration,
    retry_busy: bool,
    durability: Option<Durability>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: None,
            deadline: Some(Duration::from_secs(30)),
            base: Duration::from_micros(100),
            cap: Duration::from_millis(100),
            retry_busy: false,
            durability: None,
        }
    }
}

impl RetryPolicy {
    /// At most `n` attempts in all (at least 1).
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = Some(n.max(1));
        self
    }

    /// No attempt cap (the default): only the deadline stops retrying.
    pub fn unlimited_attempts(mut self) -> Self {
        self.max_attempts = None;
        self
    }

    /// No new attempt starts once `d` has passed since the first began (default 30 s). An
    /// attempt already running finishes, so the whole call can take longer: a commit
    /// waiting for room returns `Busy` only after `Options::write_stall_timeout` (also 30 s
    /// by default).
    pub fn deadline(mut self, d: Duration) -> Self {
        self.deadline = Some(d);
        self
    }

    /// No deadline: only the attempt cap stops retrying. With neither, a transaction that
    /// keeps conflicting is retried for ever.
    pub fn no_deadline(mut self) -> Self {
        self.deadline = None;
        self
    }

    /// The pause before the second attempt is drawn below `base`, and the bound doubles per
    /// attempt up to `cap` (defaults 100 µs and 100 ms).
    pub fn backoff(mut self, base: Duration, cap: Duration) -> Self {
        self.base = base;
        self.cap = cap.max(base);
        self
    }

    /// Also retry an attempt that fails with [`ErrorCode::Busy`] (default off): the
    /// database could not make room within `Options::write_stall_timeout`.
    pub fn retry_busy(mut self, yes: bool) -> Self {
        self.retry_busy = yes;
        self
    }

    /// Commit each attempt with `durability` (default: the writer's default durability).
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = Some(durability);
        self
    }

    /// Whether an attempt that failed with `error` is retried.
    pub fn retries(&self, error: &Error) -> bool {
        self.retries_code(error.code())
    }

    /// [`retries`](Self::retries) by error code alone: `Conflict` and `SnapshotExpired`
    /// always, `Busy` with [`retry_busy`](Self::retry_busy).
    pub fn retries_code(&self, code: ErrorCode) -> bool {
        match code {
            ErrorCode::Conflict | ErrorCode::SnapshotExpired => true,
            ErrorCode::Busy => self.retry_busy,
            _ => false,
        }
    }

    /// The whole retry decision after attempt `attempt` (counting from 1) failed with
    /// `code`, `elapsed` after the first attempt began: `None` to give up (the code is not
    /// retried, the attempt cap is reached, or the deadline passed), else the pause before
    /// the next attempt, cut short at the deadline. With it and
    /// [`Pigeonhole::transaction`] a caller drives exactly [`Pigeonhole::transact`]'s loop
    /// without its closure: an async caller with its own timer, or a C ABI.
    pub fn next_pause(&self, code: ErrorCode, attempt: u32, elapsed: Duration) -> Option<Duration> {
        let more = self.max_attempts.is_none_or(|n| attempt < n)
            && self.deadline.is_none_or(|d| elapsed < d);
        if !more || !self.retries_code(code) {
            return None;
        }
        let pause = self.delay(attempt + 1);
        Some(match self.deadline {
            Some(d) => pause.min(d.saturating_sub(elapsed)),
            None => pause,
        })
    }

    /// The pause before attempt `attempt` (counting from 1; none before the first): drawn
    /// uniformly below `min(base × 2^(attempt−2), cap)`.
    pub fn delay(&self, attempt: u32) -> Duration {
        if attempt <= 1 {
            return Duration::ZERO;
        }
        let shift = (attempt - 2).min(32);
        let bound = self
            .base
            .checked_mul(1u32 << shift.min(31))
            .unwrap_or(self.cap)
            .min(self.cap);
        let nanos = bound.as_nanos() as u64;
        if nanos == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos(jitter() % nanos)
    }
}

/// A cheap per-thread random number for the jitter: no dependency, no lock.
fn jitter() -> u64 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = Cell::new({
            let t = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos() as u64);
            let here = &t as *const u64 as u64;
            (t ^ here.rotate_left(32)) | 1
        });
    }
    STATE.with(|s| {
        // xorshift64*
        let mut x = s.get();
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        s.set(x);
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    })
}

impl Pigeonhole {
    /// Runs `body` in a transaction and commits it, retrying as `policy` says (ICR 0060):
    /// the usual way to use [`Transaction`]. Each attempt starts a fresh transaction, so
    /// `body` reads current data; it may run several times, so it should do nothing outside
    /// the transaction that must not be repeated. Returns what `body` returned and the
    /// commit.
    ///
    /// `body` returns any error type that a [`pigeonhole::Error`](Error) converts into, so
    /// it can give up with the application's own error (nothing is committed then). If any
    /// of the transaction's reads failed with an error the policy retries (a recovery
    /// expiring its snapshot), the attempt is retried whatever `body` returned, even if it
    /// swallowed that error and gave up for another reason: the snapshot is gone either way.
    /// [`Transaction::get`] and [`Transaction::get_row`] reads count; a failed
    /// [`get_async`](Transaction::get_async) does not. A commit that fails with a retried
    /// error (a `Conflict`, or `SnapshotExpired` when a recovery refused it) is retried;
    /// anything else is returned as is.
    ///
    /// ```
    /// use pigeonhole::{Family, Options, Pigeonhole, RetryPolicy};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(2))?;
    /// let bank = db.table("bank")?.family("acct", Family::default()).create_if_missing()?;
    /// bank.mutate(b"alice").put("acct", b"balance", b"10").commit()?;
    /// bank.mutate(b"bob").put("acct", b"balance", b"0").commit()?;
    ///
    /// // Move 5 from alice to bob, retrying on conflicts with other transfers.
    /// let balance = |txn: &mut pigeonhole::Transaction, who: &[u8]| -> pigeonhole::Result<u64> {
    ///     let cell = txn.get(&bank, who, "acct", b"balance")?;
    ///     Ok(cell.map_or(0, |c| std::str::from_utf8(c.value()).unwrap().parse().unwrap()))
    /// };
    /// let (moved, _commit) = db.transact(&RetryPolicy::default(), |txn| {
    ///     let (a, b) = (balance(txn, b"alice")?, balance(txn, b"bob")?);
    ///     let amount = a.min(5);
    ///     txn.put(&bank, b"alice", "acct", b"balance", (a - amount).to_string().as_bytes())
    ///         .put(&bank, b"bob", "acct", b"balance", (b + amount).to_string().as_bytes());
    ///     Ok::<_, pigeonhole::Error>(amount)
    /// })?;
    /// assert_eq!(moved, 5);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn transact<T, E>(
        &self,
        policy: &RetryPolicy,
        mut body: impl FnMut(&mut Transaction) -> std::result::Result<T, E>,
    ) -> std::result::Result<(T, CommitInfo), E>
    where
        E: From<Error>,
    {
        let started = Instant::now();
        let mut attempt = 1u32;
        loop {
            // `retryable`: a pigeonhole error the policy may retry; `given_up`: the
            // application's own error, returned as is.
            let mut given_up = None;
            let retryable = match self.transaction() {
                Err(e) => Some(e),
                Ok(mut txn) => match body(&mut txn) {
                    Err(app) => match txn.failed_read() {
                        Some(e) if policy.retries(&e) => Some(e),
                        _ => {
                            given_up = Some(app);
                            None
                        }
                    },
                    Ok(value) => {
                        let commit = match policy.durability {
                            Some(d) => txn.commit_with(d),
                            None => txn.commit(),
                        };
                        match commit {
                            Ok(info) => return Ok((value, info)),
                            Err(e) => Some(e),
                        }
                    }
                },
            };
            if let Some(app) = given_up {
                return Err(app);
            }
            let error = retryable.expect("an error when nothing was given up");
            let Some(pause) = policy.next_pause(error.code(), attempt, started.elapsed()) else {
                return Err(E::from(error));
            };
            attempt += 1;
            self.db.txn_counts.retries.fetch_add(1, Ordering::Relaxed);
            if !pause.is_zero() {
                std::thread::sleep(pause);
            }
        }
    }
}

/// Transaction outcomes since open (ICR 0061), across automatic recoveries.
#[derive(Debug, Default)]
pub(crate) struct TxnCounts {
    pub(crate) commits: AtomicU64,
    pub(crate) conflicts: AtomicU64,
    pub(crate) retries: AtomicU64,
}

impl TxnCounts {
    /// Counts one transaction commit's outcome.
    pub(crate) fn count(&self, result: &Result<CommitInfo>) {
        match result {
            Ok(_) => self.commits.fetch_add(1, Ordering::Relaxed),
            Err(e) if e.code() == ErrorCode::Conflict => {
                self.conflicts.fetch_add(1, Ordering::Relaxed)
            }
            Err(_) => return,
        };
    }

    /// Fills the transaction fields of `m`.
    pub(crate) fn fill(&self, m: &mut crate::EngineMetrics) {
        m.transactions = self.commits.load(Ordering::Relaxed);
        m.transaction_conflicts = self.conflicts.load(Ordering::Relaxed);
        m.transaction_retries = self.retries.load(Ordering::Relaxed);
    }
}

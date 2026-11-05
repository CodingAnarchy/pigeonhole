//! Forgetting (D311 §4, T2): a retention policy and TTL per kind, re-evaluated in each delete's
//! transaction (M2, M3); bounded, resumable passes; the `Forgetter`, which runs them on the
//! crate's thread or the application's (as D284 and `pigeonhole-vector`'s `Rebalancer` do).

use std::sync::{Arc, Condvar, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use pigeonhole::{RetryPolicy, Transaction};

use crate::error::{Error, Result, corrupt};
use crate::layout::*;
use crate::store::{Kind, LinkType, Store};

/// What to forget (D311 §4): memories whose retention fell below a threshold, and memories
/// past a kind's maximum age. Protected memories are never forgotten: pinned ones, and those
/// a live semantic memory was derived from.
///
/// **Retention:** `importance · exp(−since_last_use / half_life) + use_weight · ln(1 + uses)`,
/// with `since_last_use` measured from the last use, or from creation if never used. The use
/// term doesn't decay and grows without bound, so a memory used often enough is never
/// forgotten by retention (only by [`max_age`](Self::max_age)).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ForgetPolicy {
    min_retention: Option<f64>,
    max_age: Vec<(Kind, Duration)>,
    half_life: Duration,
    use_weight: f64,
    max_per_pass: usize,
    interval: Duration,
}

impl Default for ForgetPolicy {
    fn default() -> Self {
        ForgetPolicy {
            min_retention: None,
            max_age: Vec::new(),
            half_life: Duration::from_secs(7 * 24 * 3600),
            use_weight: 0.1,
            max_per_pass: 1000,
            interval: Duration::from_secs(3600),
        }
    }
}

impl ForgetPolicy {
    /// A policy that forgets nothing yet: add [`min_retention`](Self::min_retention) or
    /// [`max_age`](Self::max_age).
    pub fn new() -> Self {
        Self::default()
    }

    /// Forgets memories whose retention is below `r`.
    pub fn min_retention(mut self, r: f64) -> Self {
        self.min_retention = Some(r);
        self
    }

    /// Forgets memories of `kind` older than `age`, counted from their creation
    /// ([`MemoryId::created_us`]), not their last use: a TTL as a forget policy (M3), so a
    /// memory goes whole, never cell by cell. Setting a kind again replaces its age.
    pub fn max_age(mut self, kind: Kind, age: Duration) -> Self {
        self.max_age.retain(|(k, _)| *k != kind);
        self.max_age.push((kind, age));
        self
    }

    /// Retention's half-life: 7 days by default.
    pub fn half_life(mut self, d: Duration) -> Self {
        self.half_life = d;
        self
    }

    /// The weight of `ln(1 + uses)` in retention: 0.1 by default.
    pub fn use_weight(mut self, w: f64) -> Self {
        self.use_weight = w;
        self
    }

    /// The most memories a pass deletes: 1,000 by default.
    pub fn max_per_pass(mut self, n: usize) -> Self {
        self.max_per_pass = n;
        self
    }

    /// How often the [`Forgetter`] runs a pass when nothing else wakes it: 1 hour by default,
    /// at least 1 s.
    pub fn interval(mut self, d: Duration) -> Self {
        self.interval = d;
        self
    }

    /// The retention below which a memory is forgotten, if any.
    pub fn get_min_retention(&self) -> Option<f64> {
        self.min_retention
    }

    /// The maximum age set for `kind`, if any.
    pub fn get_max_age(&self, kind: Kind) -> Option<Duration> {
        self.max_age
            .iter()
            .find(|(k, _)| *k == kind)
            .map(|(_, a)| *a)
    }

    /// Retention's half-life.
    pub fn get_half_life(&self) -> Duration {
        self.half_life
    }

    /// The weight of `ln(1 + uses)` in retention.
    pub fn get_use_weight(&self) -> f64 {
        self.use_weight
    }

    /// The most memories a pass deletes.
    pub fn get_max_per_pass(&self) -> usize {
        self.max_per_pass
    }

    /// How often the [`Forgetter`] runs a pass.
    pub fn get_interval(&self) -> Duration {
        self.interval
    }

    pub(crate) fn check(&self) -> Result<()> {
        let bad = |m: &str| Err(Error::InvalidArgument(format!("a forget policy's {m}")));
        if let Some(r) = self.min_retention
            && !r.is_finite()
        {
            return bad("min_retention must be finite");
        }
        if !self.use_weight.is_finite() || self.use_weight < 0.0 {
            return bad("use_weight must be finite and not negative");
        }
        if self.half_life.is_zero() {
            return bad("half_life must be positive");
        }
        if self.max_per_pass == 0 {
            return bad("max_per_pass must be at least 1");
        }
        // The floor between passes, and the cap on a failed pass's backoff: below it a
        // forgetter would spin (simp724 on #1380).
        if self.interval < Duration::from_secs(1) {
            return bad("interval must be at least 1 s");
        }
        Ok(())
    }

    /// The policy as `G` stores it: a version byte, then little-endian fields.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut b = vec![1u8];
        match self.min_retention {
            Some(r) => {
                b.push(1);
                b.extend_from_slice(&r.to_le_bytes());
            }
            None => b.push(0),
        }
        b.extend_from_slice(&(self.half_life.as_micros() as u64).to_le_bytes());
        b.extend_from_slice(&self.use_weight.to_le_bytes());
        b.extend_from_slice(&(self.max_per_pass as u64).to_le_bytes());
        b.extend_from_slice(&(self.interval.as_micros() as u64).to_le_bytes());
        b.push(self.max_age.len() as u8);
        for (k, age) in &self.max_age {
            b.push(k.code());
            b.extend_from_slice(&(age.as_micros() as u64).to_le_bytes());
        }
        b
    }

    pub(crate) fn decode(b: &[u8]) -> Result<Self> {
        let bad = || corrupt("the store's forget policy");
        let mut at = 0usize;
        let mut take = |n: usize| -> Result<&[u8]> {
            let s = b.get(at..at + n).ok_or_else(bad)?;
            at += n;
            Ok(s)
        };
        if take(1)? != [1] {
            return Err(bad());
        }
        let u64_ = |s: &[u8]| u64::from_le_bytes(s.try_into().expect("8"));
        let min_retention = match take(1)?[0] {
            0 => None,
            1 => Some(f64::from_le_bytes(take(8)?.try_into().expect("8"))),
            _ => return Err(bad()),
        };
        let half_life = Duration::from_micros(u64_(take(8)?));
        let use_weight = f64::from_le_bytes(take(8)?.try_into().expect("8"));
        let max_per_pass = u64_(take(8)?) as usize;
        let interval = Duration::from_micros(u64_(take(8)?));
        let n = take(1)?[0];
        let mut max_age = Vec::new();
        for _ in 0..n {
            let k = Kind::from_code(take(1)?[0])?;
            max_age.push((k, Duration::from_micros(u64_(take(8)?))));
        }
        let p = ForgetPolicy {
            min_retention,
            max_age,
            half_life,
            use_weight,
            max_per_pass,
            interval,
        };
        p.check().map_err(|_| bad())?;
        Ok(p)
    }

    /// Whether a memory is due, from what was read of it.
    fn due(&self, m: &Facts, now: u64) -> bool {
        let age = now.saturating_sub(m.created) as f64;
        if self
            .max_age
            .iter()
            .any(|(k, a)| *k == m.kind && age > a.as_micros() as f64)
        {
            return true;
        }
        let Some(min) = self.min_retention else {
            return false;
        };
        let since = now.saturating_sub(m.last_used.unwrap_or(m.created)) as f64;
        let retention = m.importance * (-since / self.half_life.as_micros() as f64).exp()
            + self.use_weight * (1.0 + m.uses as f64).ln();
        retention < min
    }
}

/// What forgetting reads of a memory, in its delete's transaction.
struct Facts {
    created: u64,
    kind: Kind,
    importance: f64,
    uses: u64,
    last_used: Option<u64>,
}

/// What a forget pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ForgetPass {
    /// Memories looked at.
    pub examined: u64,
    /// Memories deleted.
    pub deleted: u64,
    /// Memories that were due but protected (pinned, or a live semantic memory's source).
    pub protected: u64,
    /// Where to resume ([`Forget::after`]): `None` once the pass reached the store's end.
    pub resume: Option<MemoryId>,
}

/// A forget pass to run ([`Store::forget`]).
#[must_use = "a forget pass does nothing until it runs"]
#[derive(Debug)]
pub struct Forget<'a> {
    store: &'a Store,
    policy: Option<&'a ForgetPolicy>,
    after: Option<MemoryId>,
    now_us: Option<u64>,
}

impl<'a> Forget<'a> {
    pub(crate) fn new(store: &'a Store) -> Self {
        Forget {
            store,
            policy: None,
            after: None,
            now_us: None,
        }
    }

    /// Runs this pass under `policy` instead of the store's own (a one-off).
    pub fn policy(mut self, policy: &'a ForgetPolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Resumes after `id`: a previous pass's [`ForgetPass::resume`].
    pub fn after(mut self, id: &MemoryId) -> Self {
        self.after = Some(id.clone());
        self
    }

    /// Ages are measured from `us` (µs since the Unix epoch) rather than the wall clock.
    pub fn now_us(mut self, us: u64) -> Self {
        self.now_us = Some(us);
        self
    }

    /// Runs the pass: scans memories in key order from the resume point, nominating the due
    /// ones, and deletes each in its own transaction after re-evaluating it there
    /// ([`Store::forget_in`]). It stops after [`ForgetPolicy::max_per_pass`] deletions.
    /// `InvalidArgument` if the store has no policy and none was given.
    ///
    /// A pass goes in id order, which is newest first within a namespace, so a resumed pass
    /// doesn't see memories created behind its position until the next pass from the start.
    pub fn run(self) -> Result<ForgetPass> {
        let store = self.store;
        let r = self.run_inner();
        // The pass's IVF writes, after its transactions (D311 T8).
        store.ivf_settle();
        r
    }

    fn run_inner(self) -> Result<ForgetPass> {
        let persisted;
        let policy = match self.policy {
            Some(p) => p,
            None => {
                persisted = self.store.forget_policy()?.ok_or_else(|| {
                    Error::InvalidArgument(
                        "the store has no forget policy: set one, or pass one with .policy".into(),
                    )
                })?;
                &persisted
            }
        };
        policy.check()?;
        let now = self.now_us.unwrap_or_else(now_us);
        let mut pass = ForgetPass::default();
        let mut nominated: Vec<MemoryId> = Vec::new();
        let mut last: Option<MemoryId> = None;
        let after = self.after.as_ref().map(MemoryId::row);
        let mut full = false;
        self.store.scan_memories(after.as_deref(), |id, facts| {
            pass.examined += 1;
            last = Some(id.clone());
            if policy.due(facts, now) {
                nominated.push(id.clone());
                if nominated.len() >= policy.max_per_pass {
                    full = true;
                    return Ok(false);
                }
            }
            Ok(true)
        })?;
        for id in &nominated {
            let (deleted, _) = self.store.db().transact(&RetryPolicy::default(), |txn| {
                self.store.forget_with(txn, id, policy, now)
            })?;
            match deleted {
                Forgot::Deleted => pass.deleted += 1,
                Forgot::Protected => pass.protected += 1,
                Forgot::NotDue | Forgot::Gone => {}
            }
        }
        pass.resume = if full { last } else { None };
        Ok(pass)
    }
}

/// What [`Store::forget_in`] did with one memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Forgot {
    /// Deleted.
    Deleted,
    /// Due, but protected: pinned, or a live semantic memory's source.
    Protected,
    /// Not due (any more: a use, a raised importance, landed since it was nominated).
    NotDue,
    /// Already gone.
    Gone,
}

impl Store {
    /// A forget pass under the store's policy ([`forget_policy`](Store::forget_policy)), or
    /// another with [`policy`](Forget::policy): set [`after`](Forget::after) to resume, then
    /// [`run`](Forget::run). A forgotten memory is deleted as [`delete`](Store::delete)
    /// deletes, so the change feed (D310) shows it as deletes.
    pub fn forget(&self) -> Forget<'_> {
        Forget::new(self)
    }

    /// Re-evaluates the memory `id` under the store's policy at `now_us`, inside the caller's
    /// transaction (M2), and deletes it ([`delete_in`](Store::delete_in)) if it's still due
    /// and not protected. It reads `stats`, `peak`, `meta` (kind, pinned) and `backlinks`
    /// here, so a `mark_used`, a merge raising its importance, or a consolidation adding a
    /// `derived_from` link, committed since it was nominated, either shows in the read or
    /// conflicts with it. `InvalidArgument` if the store has no policy.
    pub fn forget_in(&self, txn: &mut Transaction, id: &MemoryId, now_us: u64) -> Result<Forgot> {
        let policy = self
            .forget_policy()?
            .ok_or_else(|| Error::InvalidArgument("the store has no forget policy".into()))?;
        self.forget_with(txn, id, &policy, now_us)
    }

    pub(crate) fn forget_with(
        &self,
        txn: &mut Transaction,
        id: &MemoryId,
        policy: &ForgetPolicy,
        now_us: u64,
    ) -> Result<Forgot> {
        let t = self.table();
        let row = id.row();
        if txn.get(t, &row, CONTENT, TEXT)?.is_none() {
            return Ok(Forgot::Gone);
        }
        let kind = match txn
            .get(t, &row, META, KIND)?
            .and_then(|c| c.value().first().copied())
        {
            Some(c) => Kind::from_code(c)?,
            None => Kind::Episodic,
        };
        let pinned = txn.get(t, &row, META, PINNED)?.is_some();
        let importance = txn
            .get(t, &row, PEAK, IMPORTANCE)?
            .and_then(|c| c.as_i64())
            .map_or(0.0, |p| p as f64 / PPM);
        let uses = txn
            .get(t, &row, STATS, USES)?
            .and_then(|c| c.as_i64())
            .unwrap_or(0)
            .max(0) as u64;
        let last_used = txn
            .get(t, &row, PEAK, LAST_USED)?
            .and_then(|c| c.as_i64())
            .map(|v| v.max(0) as u64);
        let facts = Facts {
            created: id.created_us(),
            kind,
            importance,
            uses,
            last_used,
        };
        if !policy.due(&facts, now_us) {
            return Ok(Forgot::NotDue);
        }
        if pinned || self.derived_by_live_semantic(txn, &row)? {
            return Ok(Forgot::Protected);
        }
        self.delete_in(txn, id)?;
        Ok(Forgot::Deleted)
    }

    /// Whether a live semantic memory links to `row` with `DERIVED_FROM`: its backlinks, and
    /// each such source's `content:text` and kind, read in the transaction.
    fn derived_by_live_semantic(&self, txn: &mut Transaction, row: &[u8]) -> Result<bool> {
        let t = self.table();
        let sources: Vec<Vec<u8>> = match txn.get_row(t, row, &[BACKLINKS])? {
            Some(r) => r
                .iter()
                .filter(|e| e.cell.value().first() == Some(&LinkType::DERIVED_FROM.get()))
                .map(|e| e.qualifier.to_vec())
                .collect(),
            None => Vec::new(),
        };
        for s in sources {
            if txn.get(t, &s, CONTENT, TEXT)?.is_none() {
                continue;
            }
            let kind = txn
                .get(t, &s, META, KIND)?
                .and_then(|c| c.value().first().copied());
            if kind == Some(Kind::Semantic.code()) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Calls `f` with each memory after `after` (all, from the start, with `None`), in key
    /// order, with what forgetting reads of it, until `f` returns `false`.
    ///
    /// It reads in pages of [`SCAN_PAGE`] rows, each with its own iterator, dropped before
    /// the next page starts after the last row read (#1507). An iterator holds its view
    /// while it lives, and a view pins the memtables it can read. One iterator for a whole
    /// pass over a large store kept flushed memtables from being reused, so writers waited
    /// for room (#1477, #1506). A pass needs no single snapshot: each nominated memory is
    /// evaluated again in its own transaction before it's deleted ([`Store::forget_with`]).
    ///
    /// **`f` must not commit.** A page's iterator, and so its view, is alive while `f` runs:
    /// a commit there could wait for memtable room that only this page's view holds, until
    /// the write-stall timeout (#1497's trap, perf287 on #1519). `f` only nominates; the
    /// deletions run after the scan returns.
    fn scan_memories(
        &self,
        after: Option<&[u8]>,
        mut f: impl FnMut(&MemoryId, &Facts) -> Result<bool>,
    ) -> Result<()> {
        use std::ops::Bound;
        let mut resume: Option<Vec<u8>> = after.map(<[u8]>::to_vec);
        loop {
            let start = resume.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
            let mut it = self
                .table()
                .scan_bounds(start, Bound::Unbounded)
                .families([CONTENT, META, STATS, PEAK])
                .limit(SCAN_PAGE)
                .iter()?;
            let mut rows = 0;
            while let Some(r) = it.next_ref()? {
                rows += 1;
                resume = Some(r.key().to_vec());
                let Some(id) = MemoryId::from_row(r.key()) else {
                    continue;
                };
                if r.get(CONTENT, TEXT).is_none() {
                    continue;
                }
                let kind = match r.get(META, KIND).and_then(|c| c.value().first().copied()) {
                    Some(c) => Kind::from_code(c)?,
                    None => Kind::Episodic,
                };
                let facts = Facts {
                    created: id.created_us(),
                    kind,
                    importance: r
                        .get(PEAK, IMPORTANCE)
                        .and_then(|c| c.as_i64())
                        .map_or(0.0, |p| p as f64 / PPM),
                    uses: r
                        .get(STATS, USES)
                        .and_then(|c| c.as_i64())
                        .unwrap_or(0)
                        .max(0) as u64,
                    last_used: r
                        .get(PEAK, LAST_USED)
                        .and_then(|c| c.as_i64())
                        .map(|v| v.max(0) as u64),
                };
                if !f(&id, &facts)? {
                    return Ok(());
                }
            }
            if rows < SCAN_PAGE {
                return Ok(());
            }
        }
    }
}

/// Rows one page of a forgetting pass's scan reads with one iterator (#1507): enough that a
/// page's setup is noise beside its rows, few enough that its view lives a few
/// milliseconds.
const SCAN_PAGE: u64 = 256;

#[derive(Debug, Default)]
struct State {
    notified: bool,
    closed: bool,
}

/// Between a store's handles and its forgetter: the last handle says when it's gone.
#[derive(Default)]
pub(crate) struct Signal {
    state: Mutex<State>,
    cv: Condvar,
    wakeup: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl Signal {
    fn wait(&self, timeout: Duration) {
        let st = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (mut st, _) = self
            .cv
            .wait_timeout_while(st, timeout, |s| !s.notified && !s.closed)
            .unwrap_or_else(PoisonError::into_inner);
        st.notified = false;
    }

    /// Wakes the forgetter now (a policy was set).
    pub(crate) fn notify(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .notified = true;
        self.cv.notify_all();
    }

    pub(crate) fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed = true;
        self.cv.notify_all();
        let w = self
            .wakeup
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(w) = w {
            w();
        }
    }
}

/// The application's handles to a store: dropping the last one closes its forgetter.
pub(crate) struct UserToken(pub(crate) Arc<Signal>);

impl Drop for UserToken {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Runs a store's forget policy ([`StoreOptions::forget`](crate::StoreOptions::forget)) in
/// bounded passes. [`Store::open`] runs one on the crate's thread;
/// [`Store::take_forgetter`] hands it to an application that opened the store with
/// [`OpenOptions::own_forgetter`](crate::OpenOptions::own_forgetter) and runs its threads
/// itself (an application-owned database, D284). Run it in a loop:
///
/// ```
/// # use std::time::Duration;
/// # fn run(mut f: pigeonhole_memory::Forgetter) -> pigeonhole_memory::Result<()> {
/// while !f.closed() {
///     if !f.run_once(Duration::from_millis(50))? {
///         f.wait(Duration::from_secs(60));
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// It watches the application's handles: once every [`Store`] handle is gone,
/// [`closed`](Self::closed) is true and the loop ends. Each deletion is its own transaction,
/// so stopping between calls is always safe.
///
/// [`Pigeonhole::close`](pigeonhole::Pigeonhole::close) doesn't wait for a pass: a pass in
/// flight fails with [`Closed`](pigeonhole::ErrorCode::Closed) at its next read or commit, and
/// the deletion it had open is dropped whole, so the store stays consistent. To let the
/// forgetter finish instead, drop the [`Store`] handles and wait for it to stop (on the crate's
/// thread, it stops within its current pass) before closing.
pub struct Forgetter {
    store: Store,
    user: Weak<UserToken>,
    signal: Arc<Signal>,
    resume: Option<MemoryId>,
    next_pass: Instant,
    /// Passes failed in a row: each waits twice as long, from 5 s up to `interval`.
    failures: u32,
    /// The interval of the policy last read (the default before one is).
    interval: Duration,
}

impl std::fmt::Debug for Forgetter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Forgetter")
            .field("closed", &self.closed())
            .finish_non_exhaustive()
    }
}

impl Forgetter {
    pub(crate) fn new(store: Store, user: Weak<UserToken>, signal: Arc<Signal>) -> Self {
        Forgetter {
            store,
            user,
            signal,
            resume: None,
            next_pass: Instant::now(),
            failures: 0,
            interval: ForgetPolicy::default().interval,
        }
    }

    /// Runs forget passes under the store's current policy (read each pass, so
    /// [`Store::set_forget_policy`] takes effect) until the store is through or `budget` has
    /// passed, always running at least one pass when one is due. Returns whether to call
    /// again soon (a pass stopped at `max_per_pass` with more to look at). When it returns
    /// `false`, the next pass is due after the policy's interval: [`wait`](Self::wait).
    /// Without a policy it does nothing, and checks again after an hour or when one is set.
    ///
    /// A failed pass (the database closed, retries used up) returns its error, and the next
    /// pass waits: 5 s after the first failure, doubling with each failure in a row up to the
    /// policy's interval. A pass that succeeds resets it. Calls before then return `false`.
    #[must_use = "`true` means call again soon; `false` means wait"]
    pub fn run_once(&mut self, budget: Duration) -> Result<bool> {
        if self.closed() || Instant::now() < self.next_pass {
            return Ok(false);
        }
        let r = self.passes(budget);
        if r.is_err() {
            self.failures = self.failures.saturating_add(1);
            let backoff = Duration::from_secs(5)
                .saturating_mul(1 << (self.failures - 1).min(20))
                .min(self.interval);
            self.next_pass = Instant::now() + backoff;
        } else {
            self.failures = 0;
        }
        r
    }

    fn passes(&mut self, budget: Duration) -> Result<bool> {
        let start = Instant::now();
        loop {
            let Some(policy) = self.store.forget_policy()? else {
                self.next_pass = Instant::now() + ForgetPolicy::default().interval;
                return Ok(false);
            };
            self.interval = policy.interval;
            let mut f = self.store.forget().policy(&policy);
            if let Some(r) = &self.resume {
                f = f.after(r);
            }
            let pass = f.run()?;
            self.resume = pass.resume;
            if self.resume.is_none() {
                self.next_pass = Instant::now() + policy.interval;
                return Ok(false);
            }
            if self.closed() || start.elapsed() >= budget {
                return Ok(!self.closed());
            }
        }
    }

    /// Waits until the next pass is due, a policy is set, the store's last handle drops, or
    /// `timeout` passes.
    pub fn wait(&mut self, timeout: Duration) {
        let until = self.next_pass.saturating_duration_since(Instant::now());
        self.signal
            .wait(timeout.min(until.max(Duration::from_millis(1))));
    }

    /// Whether every [`Store`] handle is gone: nothing is left to do.
    pub fn closed(&self) -> bool {
        self.user.strong_count() == 0
    }

    /// Calls `wakeup` once when the store's last handle drops, for an application whose
    /// threads wait in an event loop rather than in [`wait`](Self::wait).
    pub fn set_wakeup(&mut self, wakeup: Box<dyn Fn() + Send + Sync>) {
        *self
            .signal
            .wakeup
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Arc::from(wakeup));
    }
}

/// Runs `f` on a thread of the crate's own until its store closes.
pub(crate) fn spawn(mut f: Forgetter) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("pigeonhole-memory-forgetter".into())
        .spawn(move || {
            while !f.closed() {
                // A failed pass (the database closing, say) is retried after run_once's backoff.
                if !matches!(f.run_once(Duration::from_millis(100)), Ok(true)) {
                    f.wait(Duration::from_secs(60));
                }
            }
        })
        .map(|_| ())
}

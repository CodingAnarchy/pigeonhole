//! Feed-driven consolidation (D311 T9 part 2, #1343, ICR 0107): a [`ConsolidateLoop`]
//! watches a store's new memories ([`Store::watch`]) and runs a consolidation pass for each
//! namespace once its oldest new memory is `min_age` old, resuming from a position it keeps in
//! the store's `G` row.
//!
//! The reviews on #1343 shaped it:
//! - Only `Remembered` marks a namespace (cli692's A1, simp724's R3): a pass's own summaries
//!   (`Summarized`) and `mark_used`/merges (`Updated`) never re-arm one.
//! - Two passes over one group conflict in the group's transaction (it reads each source's
//!   `meta:consolidated`), and the later one skips it (simp724's R1); a group's summary,
//!   marks and deletes commit together (R2), so a crash between groups re-runs only what
//!   didn't commit.
//! - The catch-up scan marks only namespaces with unconsolidated memories (R4).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pigeonhole::{ErrorCode, Snapshot};

use crate::consolidate::{ConsolidatePass, ConsolidatePolicy, Consolidator};
use crate::error::{Error, Result, corrupt};
use crate::layout::{
    CONSOLIDATED, GLOBAL, MEMORY_TAG, META, MemoryId, SUMMARY, check_namespace, now_us,
};
use crate::store::Store;
use crate::watch::{MemoryEvent, Watch, WatchToken};

/// `G:meta:loop:{name}`: a loop's position.
fn state_key(name: &str) -> Vec<u8> {
    [b"loop:".as_slice(), name.as_bytes()].concat()
}

/// What one [`ConsolidateLoop::run_once`] did.
#[derive(Debug)]
#[non_exhaustive]
pub struct LoopStep {
    /// Memory events read (one per memory per commit, every kind).
    pub events: usize,
    /// The passes run, by namespace.
    pub passes: Vec<(String, ConsolidatePass)>,
    /// The passes that failed, by namespace: the consolidator's error (`Error::Consolidate`,
    /// with the pass so far). The namespace stays pending, retried after `min_age`.
    pub failed: Vec<(String, Error)>,
    /// The passes the consolidator stopped ([`ConsolidateStopped`](crate::ConsolidateStopped)),
    /// by namespace, each with the pass so far (the groups before the stop are committed).
    /// The namespaces keep their pending time, so they're due again at the next step, not
    /// after `min_age`.
    pub stopped: Vec<(String, ConsolidatePass)>,
    /// Whether a [`LoopInterrupter`] ended this step's wait early (the step then ran the
    /// passes due and saved, as at the end of its timeout).
    pub interrupted: bool,
    /// When the next pass is due, µs since the Unix epoch, if any namespace is pending.
    pub next_due: Option<u64>,
    /// Where the loop is, as saved: the watch's position, for callers that checkpoint
    /// elsewhere too.
    pub token: WatchToken,
}

/// The persisted part: the watch's position, the pending namespaces, and a generation that
/// tells two loops of one name apart.
#[derive(Debug, Default)]
struct Saved {
    generation: u64,
    token: Vec<u8>,
    /// Namespace → when its oldest unconsolidated new memory was made (µs).
    pending: BTreeMap<String, u64>,
}

impl Saved {
    /// When the first pending namespace falls due (µs), given `min_age`.
    fn next_due(&self, min_age: u64) -> Option<u64> {
        self.pending
            .values()
            .map(|t| t.saturating_add(min_age))
            .min()
    }

    /// The namespaces due at `now`.
    fn due(&self, min_age: u64, now: u64) -> Vec<String> {
        self.pending
            .iter()
            .filter(|(_, at)| at.saturating_add(min_age) <= now)
            .map(|(ns, _)| ns.clone())
            .collect()
    }

    /// Notes a commit's new memories: each namespace keeps its oldest pending time.
    fn note(&mut self, events: &[MemoryEvent]) {
        for e in events {
            if let MemoryEvent::Remembered(id) = e {
                let at = self
                    .pending
                    .entry(id.namespace().to_owned())
                    .or_insert(id.created_us());
                *at = (*at).min(id.created_us());
            }
        }
    }

    /// Version 1: a version byte, the generation (8), the token (len u32 + bytes), the count
    /// (u32), then each (len u16 + namespace, µs u64). Little endian.
    fn encode_at(&self, generation: u64) -> Vec<u8> {
        let mut b = vec![1u8];
        b.extend_from_slice(&generation.to_le_bytes());
        b.extend_from_slice(&(self.token.len() as u32).to_le_bytes());
        b.extend_from_slice(&self.token);
        b.extend_from_slice(&(self.pending.len() as u32).to_le_bytes());
        for (ns, at) in &self.pending {
            b.extend_from_slice(&(ns.len() as u16).to_le_bytes());
            b.extend_from_slice(ns.as_bytes());
            b.extend_from_slice(&at.to_le_bytes());
        }
        b
    }

    fn decode(b: &[u8]) -> Result<Saved> {
        let bad = || corrupt("a consolidation loop's saved position");
        let mut at = 0usize;
        let mut take = |n: usize| -> Result<&[u8]> {
            let s = b.get(at..at + n).ok_or_else(bad)?;
            at += n;
            Ok(s)
        };
        if take(1)? != [1] {
            return Err(bad());
        }
        let generation = u64::from_le_bytes(take(8)?.try_into().map_err(|_| bad())?);
        let len = u32::from_le_bytes(take(4)?.try_into().map_err(|_| bad())?) as usize;
        let token = take(len)?.to_vec();
        let count = u32::from_le_bytes(take(4)?.try_into().map_err(|_| bad())?);
        let mut pending = BTreeMap::new();
        for _ in 0..count {
            let n = u16::from_le_bytes(take(2)?.try_into().map_err(|_| bad())?) as usize;
            let ns = std::str::from_utf8(take(n)?).map_err(|_| bad())?.to_owned();
            let us = u64::from_le_bytes(take(8)?.try_into().map_err(|_| bad())?);
            pending.insert(ns, us);
        }
        Ok(Saved {
            generation,
            token,
            pending,
        })
    }
}

/// A feed-driven consolidation loop (D311 T9; [`Store::consolidate_loop`]): it reads the
/// store's new memories from its change feed and runs [`Store::consolidate`] for a namespace
/// once its oldest new memory is the policy's `min_age` old. Writer process only.
///
/// Its position (the watch's token and the pending namespaces) is kept in the store under the
/// loop's name, saved after each step, so a loop that stops resumes where it was, a pending
/// namespace included.
///
/// **One loop per name.** A loop that starts takes the name over (a restarted process must,
/// from one that died), and a loop whose name was taken over fails its next step with
/// [`Error::LoopTakenOver`] (code `Conflict`). While two run, a pass they race on conflicts group by group, so no
/// group is consolidated twice (each group's transaction reads its sources'
/// `meta:consolidated`; simp724's R1 on #1343). It's at least once: a stop between a pass and
/// the save re-runs the pass, which skips what's already consolidated.
///
/// The database's change feed must be on and follow the store's table (as for
/// [`Store::watch`]).
pub struct ConsolidateLoop {
    store: Store,
    name: String,
    consolidator: Arc<dyn Consolidator>,
    policy: ConsolidatePolicy,
    namespaces: Vec<String>,
    clock: Box<dyn Fn() -> u64 + Send + Sync>,
    state: Option<(Watch, Saved)>,
    /// Set by a [`LoopInterrupter`]; taken by the step whose wait it ends.
    interrupt: Arc<AtomicBool>,
}

/// How often a step's wait checks for an interrupt.
const INTERRUPT_SLICE: Duration = Duration::from_millis(50);

/// Ends a [`ConsolidateLoop`]'s current wait early, from another thread
/// ([`ConsolidateLoop::interrupter`]): the step stops waiting for new memories, runs the
/// passes that are due, saves its position and returns, with
/// [`LoopStep::interrupted`] set. If no step is waiting, the next one returns at once. It
/// can't cut a pass short: a consolidator call runs to its end. For a binding whose caller
/// was interrupted, or a shutdown (perf2 on #1441).
#[derive(Debug, Clone)]
pub struct LoopInterrupter(Arc<AtomicBool>);

impl LoopInterrupter {
    /// Ends the current or next step's wait (within about 50 ms).
    ///
    /// The flag stays set until a step sees it, so an interrupt sent just as a step ends makes
    /// the next step return at once, after running what's due and saving. That's one short step,
    /// and it keeps an interrupt sent just before a step from being lost. There's no `clear()`
    /// for that reason: clearing at the start of a step would race with that interrupt.
    pub fn interrupt(&self) {
        self.0.store(true, Ordering::Release);
    }
}

// Owned and `Send`, so a binding can keep one across calls or move it to a worker thread
// (cli692's B2 on #1432).
const _: fn() = || {
    fn send<T: Send + 'static>() {}
    send::<ConsolidateLoop>();
};

impl std::fmt::Debug for ConsolidateLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsolidateLoop")
            .field("name", &self.name)
            .field("namespaces", &self.namespaces)
            .finish_non_exhaustive()
    }
}

impl ConsolidateLoop {
    pub(crate) fn new(
        store: &Store,
        name: &str,
        consolidator: Arc<dyn Consolidator>,
    ) -> ConsolidateLoop {
        ConsolidateLoop {
            store: store.clone(),
            name: name.to_owned(),
            consolidator,
            policy: ConsolidatePolicy::default(),
            namespaces: Vec::new(),
            clock: Box::new(now_us),
            state: None,
            interrupt: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The passes' policy ([`ConsolidatePolicy::new`]'s defaults if not set). Its `min_age`
    /// also says when a namespace is due.
    pub fn policy(mut self, policy: &ConsolidatePolicy) -> Self {
        self.policy = policy.clone();
        self
    }

    /// Only these namespaces (every namespace by default).
    pub fn namespaces(mut self, namespaces: &[&str]) -> Self {
        self.namespaces = namespaces.iter().map(|s| (*s).to_owned()).collect();
        self
    }

    /// The clock `min_age` is measured on, µs since the Unix epoch: the system's by default
    /// (cli692's A3 on #1343; for tests and simulations).
    ///
    /// Only for `min_age` and when a pass is due: `run_once`'s timeout is real time, since
    /// it bounds a real wait for the feed (simp724's L4 on #1432).
    pub fn clock(mut self, now_us: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        self.clock = Box::new(now_us);
        self
    }

    /// One step: reads the store's new memories for up to `timeout` (less if a pass falls
    /// due sooner), runs the passes that are due, and saves the loop's position. `timeout`
    /// bounds the wait, not the passes, which take as long as the consolidator does (cli692's
    /// A5).
    ///
    /// [`Error::LoopTakenOver`] means another loop of this name now runs: stop this one, and
    /// don't recreate it.
    ///
    /// The first step of a loop with no saved position, and one after its feed history was
    /// recycled (`FeedExpired`), scans the store once at a snapshot for namespaces with
    /// unconsolidated memories, then follows the feed from that snapshot.
    pub fn run_once(&mut self, timeout: Duration) -> Result<LoopStep> {
        self.check()?;
        if self.state.is_none() {
            self.state = Some(self.start()?);
            // Claims the name now, so a loop already running fails its next save, whatever
            // order the two save in.
            self.save()?;
        }
        let (events, interrupted) = self.read_events(timeout)?;
        let (passes, failed, stopped) = self.run_due_passes((self.clock)())?;
        let token = self.save()?;
        Ok(LoopStep {
            events,
            passes,
            failed,
            stopped,
            interrupted,
            next_due: self.next_due(),
            token,
        })
    }

    /// Step 1: reads events until `timeout` (real time), until the first pending namespace
    /// falls due (on the loop's clock), or until an interrupt. How many events it read, and
    /// whether an interrupt ended it.
    fn read_events(&mut self, timeout: Duration) -> Result<(usize, bool)> {
        let min_age = self.min_age_us();
        let mut events = 0usize;
        let started = std::time::Instant::now();
        loop {
            let (_, saved) = self.state.as_ref().expect("started");
            let mut wait = timeout.saturating_sub(started.elapsed());
            if let Some(due) = saved.next_due(min_age) {
                wait = wait.min(Duration::from_micros(due.saturating_sub((self.clock)())));
            }
            // In slices, so an interrupt (`LoopInterrupter`) ends the wait within one.
            if self.interrupt.swap(false, Ordering::AcqRel) {
                return Ok((events, true));
            }
            let sliced = wait > INTERRUPT_SLICE;
            let (watch, _) = self.state.as_mut().expect("started");
            let commit = match watch.next(wait.min(INTERRUPT_SLICE)) {
                Ok(c) => c,
                Err(Error::Db(e)) if e.code() == ErrorCode::FeedExpired => {
                    // Its history is gone: catch up by a scan, keeping the generation.
                    let generation = self.state.take().map_or(0, |(_, s)| s.generation);
                    let (w, mut s) = self.catch_up()?;
                    s.generation = generation;
                    self.state = Some((w, s));
                    continue;
                }
                Err(e) => return Err(e),
            };
            let Some(commit) = commit else {
                if sliced {
                    continue;
                }
                return Ok((events, false));
            };
            events += commit.events.len();
            let (_, saved) = self.state.as_mut().expect("started");
            saved.note(&commit.events);
            if started.elapsed() >= timeout {
                return Ok((events, false));
            }
        }
    }

    /// Step 2: runs the passes due at `now`, and moves each namespace's pending time on.
    /// The passes that ran, failed and stopped.
    #[allow(clippy::type_complexity)]
    fn run_due_passes(
        &mut self,
        now: u64,
    ) -> Result<(
        Vec<(String, ConsolidatePass)>,
        Vec<(String, Error)>,
        Vec<(String, ConsolidatePass)>,
    )> {
        let (_, saved) = self.state.as_ref().expect("started");
        let due = saved.due(self.min_age_us(), now);
        let (mut passes, mut failed, mut stopped) = (Vec::new(), Vec::new(), Vec::new());
        for ns in due {
            let r = self
                .store
                .consolidate(&ns, &*self.consolidator)
                .policy(&self.policy)
                .now_us(now)
                .run();
            let next = match r {
                Ok(pass) => {
                    passes.push((ns.clone(), pass));
                    self.oldest_pending(&ns, now)?
                }
                // The consolidator stopped rather than failed (an interrupted binding, say):
                // the namespace keeps its pending time, so it's due again at the next step
                // (cli692 on #1441).
                Err(Error::Consolidate { error, pass })
                    if error.is::<crate::consolidate::ConsolidateStopped>() =>
                {
                    stopped.push((ns, pass));
                    continue;
                }
                Err(e @ Error::Consolidate { .. }) => {
                    failed.push((ns.clone(), e));
                    // Retried after `min_age`, not at once.
                    Some(now)
                }
                Err(e) => return Err(e),
            };
            let (_, saved) = self.state.as_mut().expect("started");
            match next {
                Some(at) => saved.pending.insert(ns, at),
                None => saved.pending.remove(&ns),
            };
        }
        Ok((passes, failed, stopped))
    }

    /// A handle that ends this loop's waits early, from another thread: see
    /// [`LoopInterrupter`].
    pub fn interrupter(&self) -> LoopInterrupter {
        LoopInterrupter(Arc::clone(&self.interrupt))
    }

    /// When the next pass is due (µs since the Unix epoch), as of the last step; `None`
    /// before the first step or when nothing is pending.
    pub fn next_due(&self) -> Option<u64> {
        let min_age = self.min_age_us();
        self.state.as_ref().and_then(|(_, s)| s.next_due(min_age))
    }

    /// Runs steps until `stop` is set, each waiting up to a second for new memories.
    pub fn run(&mut self, stop: &AtomicBool) -> Result<()> {
        while !stop.load(Ordering::Acquire) {
            self.run_once(Duration::from_secs(1))?;
        }
        Ok(())
    }

    fn check(&self) -> Result<()> {
        if self.name.is_empty() || self.name.contains(':') {
            return Err(Error::InvalidArgument(format!(
                "a consolidation loop's name is non-empty and has no ':', not {:?}",
                self.name
            )));
        }
        for ns in &self.namespaces {
            check_namespace(ns)?;
        }
        Ok(())
    }

    fn min_age_us(&self) -> u64 {
        u64::try_from(self.policy.get_min_age().as_micros()).unwrap_or(u64::MAX)
    }

    fn namespace_refs(&self) -> Vec<&str> {
        self.namespaces.iter().map(String::as_str).collect()
    }

    /// The saved position, or a catch-up when there's none.
    fn start(&self) -> Result<(Watch, Saved)> {
        let t = self.store.table();
        match t.get(GLOBAL, META, &state_key(&self.name))? {
            Some(c) => {
                let saved = Saved::decode(c.value())?;
                let opened = self
                    .store
                    .watch(&self.namespace_refs())
                    .resume(&WatchToken::from_bytes(saved.token.clone()))
                    .open();
                match opened {
                    Ok(watch) => Ok((watch, saved)),
                    // The saved position's history is gone (cli692's B3 on #1432; the first
                    // `next` reports it too, which `run_once` handles).
                    Err(Error::Db(e)) if e.code() == ErrorCode::FeedExpired => {
                        let (w, mut s) = self.catch_up()?;
                        s.generation = saved.generation;
                        Ok((w, s))
                    }
                    Err(e) => Err(e),
                }
            }
            None => self.catch_up(),
        }
    }

    /// A snapshot, the namespaces with unconsolidated memories at it (each from its oldest
    /// such memory), and a watch from it: nothing missed, nothing twice (simp724's R4).
    fn catch_up(&self) -> Result<(Watch, Saved)> {
        let s = self.store.db().snapshot()?;
        let mut pending: BTreeMap<String, u64> = BTreeMap::new();
        let mut it = self
            .store
            .table()
            .scan_prefix(b"")
            .family(META)
            .snapshot(&s)
            .iter()?;
        while let Some(r) = it.next_ref()? {
            let Some(id) = MemoryId::from_row(r.key()) else {
                continue;
            };
            if !self.watched(id.namespace()) || !unconsolidated(&r) {
                continue;
            }
            let at = pending
                .entry(id.namespace().to_owned())
                .or_insert(id.created_us());
            *at = (*at).min(id.created_us());
        }
        drop(it);
        let watch = self
            .store
            .watch(&self.namespace_refs())
            .from_snapshot(&s)
            .open()?;
        let generation = self
            .store
            .table()
            .get(GLOBAL, META, &state_key(&self.name))?
            .map(|c| Saved::decode(c.value()).map(|s| s.generation))
            .transpose()?
            .unwrap_or(0);
        Ok((
            watch,
            Saved {
                generation,
                token: Vec::new(),
                pending,
            },
        ))
    }

    fn watched(&self, ns: &str) -> bool {
        self.namespaces.is_empty() || self.namespaces.iter().any(|n| n == ns)
    }

    /// After a pass at `now`: the oldest unconsolidated memory of `ns` made after
    /// `now − min_age` (too young for that pass), if any. Rids sort newest first, so the scan
    /// stops at the first memory older than that.
    fn oldest_pending(&self, ns: &str, now: u64) -> Result<Option<u64>> {
        let cutoff = now.saturating_sub(self.min_age_us());
        let s: Snapshot = self.store.db().snapshot()?;
        let prefix = [ns.as_bytes(), MEMORY_TAG].concat();
        let mut it = self
            .store
            .table()
            .scan_prefix(&prefix)
            .family(META)
            .snapshot(&s)
            .iter()?;
        let mut oldest = None;
        while let Some(r) = it.next_ref()? {
            let Some(id) = MemoryId::from_row(r.key()) else {
                continue;
            };
            if id.created_us() <= cutoff {
                break;
            }
            if unconsolidated(&r) {
                oldest = Some(id.created_us());
            }
        }
        Ok(oldest)
    }

    /// Saves the position, if no other loop of this name saved since this one last did.
    fn save(&mut self) -> Result<WatchToken> {
        let (watch, saved) = self.state.as_mut().expect("started");
        let token = watch.token();
        saved.token = token.as_bytes().to_vec();
        let key = state_key(&self.name);
        let t = self.store.table();
        let mut txn = self.store.db().transaction()?;
        let theirs = match txn.get(t, GLOBAL, META, &key)? {
            Some(c) => Saved::decode(c.value())?.generation,
            None => 0,
        };
        if theirs != saved.generation {
            return Err(Error::LoopTakenOver(self.name.clone()));
        }
        // The generation moves only once the commit is in (simp724's L1 on #1432): a save
        // that fails otherwise leaves the loop and the store agreeing, and the next save
        // works.
        let next = saved.generation + 1;
        txn.put(t, GLOBAL, META, &key, &saved.encode_at(next));
        #[cfg(feature = "__unstable-hooks")]
        if crate::hooks::take_fail_loop_save(&self.name) {
            drop(txn);
            return Err(corrupt(
                "an injected save failure (hooks::fail_next_loop_save)",
            ));
        }
        match txn.commit() {
            Ok(_) => {
                saved.generation = next;
                Ok(token)
            }
            // The read above is of this one cell (D275: a transaction's `get` conflicts only
            // with a write to that cell, not to the rest of `G`), so a conflict is another
            // loop of this name saving meanwhile (simp724's L2).
            Err(e) if e.code() == ErrorCode::Conflict => {
                Err(Error::LoopTakenOver(self.name.clone()))
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// A memory a pass could still fold: not consolidated, not a summary. Read from `meta`
/// alone (simp724's L3 on #1432): a memory row that's there is live, since a delete removes
/// the whole row, and every memory has `meta:kind`.
fn unconsolidated(r: &pigeonhole::RowRef<'_>) -> bool {
    r.get(META, CONSOLIDATED).is_none() && r.get(META, SUMMARY).is_none()
}

//! `Text`'s BM25 indexes (D313 ticket 7c, D309, ICR 0105): the open handles, by name, and
//! the requests' query and option mapping.
//!
//! - **One handle per index**, opened on first use (on the blocking pool: an open reads the
//!   index's metadata) and kept until the server stops. An index has no thread of its own.
//! - **A rebuild stops at a fence** (#1756): it runs a document at a time under the
//!   partition's write gate ([`Steps`]), so a fence's drain waits for one document, not the
//!   whole rebuild. A rebuild stopped that way (or by a restart) leaves its cursor in the
//!   index, and resumes on its own once the partition takes writes again
//!   ([`Texts::resume_soon`]): at the partition's open, after a restart or a move, and after
//!   the stop. A client's `Text.Rebuild` always starts over (ai1249 on #1756).
//! - **`--text-pack` packs the indexes in the background** (#1585, ICR 0143): off by default,
//!   since a packed index can't be read by a 1.2 build. With it, a thread per partition
//!   ([`Texts::pack_soon`]) packs each index a chunk at a time, under the write gate and the
//!   node's rebuild slot as a resume is: a fence stops it at its next chunk, and it yields
//!   to a client's `Text.Rebuild`.
//! - **Clauses map by field name** (ICR 0105's amendment for 7c): a `term` field is matched as
//!   given (`Query::term`, `Query::fuzzy`); a `text` field is tokenised as documents are
//!   (`Query::text`, and a phrase's text as `Query::quoted_phrase`, the syntax's `"..."~N`).

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use pigeonhole::Pigeonhole;
use pigeonhole_text::{Fuzziness, Index, IndexOptions, Normalization, PackOptions, Query, Source};

use super::errors::ApiError;
use super::proto;

#[derive(Debug)]
pub(crate) struct Texts {
    db: Pigeonhole,
    open: Mutex<super::handles::Handles<Index>>,
    /// The running (or last) `Text.Rebuild`'s thread, for `close` to wait for.
    rebuild: Mutex<Option<JoinHandle<()>>>,
    /// Set by `close` under `rebuild`'s lock: no rebuild starts after it, and a running one
    /// stops at its next document.
    closed: AtomicBool,
    /// The partition's write gate: a rebuild enters it for each document (#1756).
    gate: Option<Arc<super::gate::WriteGate>>,
    /// Where the resume thread runs: the free cores, as a rebuild's.
    placement: Option<Arc<super::placement::Placement>>,
    /// The resume thread ([`Texts::resume_soon`]).
    resume: Mutex<Resume>,
    /// The source tables of `Text.CreateIndex` calls in flight, once per call (#1916). Added
    /// to under the partition's `schema` lock, so a `DropTable` holding it sees every create
    /// that could still write an index's source after the drop's check.
    creating: Mutex<Vec<String>>,
    /// The pack thread ([`Texts::pack_soon`]), with `--text-pack`.
    pack: Mutex<Resume>,
    /// The pack thread's counters, for `/metrics`.
    pub(crate) packs: PackCounters,
}

/// What a partition's background pack has done (`--text-pack`), for `/metrics`.
#[derive(Debug, Default)]
pub(crate) struct PackCounters {
    /// Passes over the partition's indexes that ran to their end.
    pub(crate) passes: AtomicU64,
    /// Postings moved into blocks.
    pub(crate) postings: AtomicU64,
    /// Indexes whose pack was refused or failed (a first pack with reader processes
    /// attached, or an error): one for an index each time it starts failing, not one a pass.
    pub(crate) refused: AtomicU64,
    /// Packs that stopped because a chunk kept conflicting with writers: the next pass
    /// carries on. One that grows every pass is a pack that never gets through (ai1249).
    pub(crate) conflicts: AtomicU64,
}

/// How long the pack thread waits between passes. A pass over indexes with nothing new
/// reads their few unpacked cells and writes nothing.
const PACK_EVERY: Duration = Duration::from_secs(60);

/// The resume thread's state, under [`Texts::resume`]'s lock.
#[derive(Debug, Default)]
struct Resume {
    /// Its thread, for `close` to wait for.
    thread: Option<JoinHandle<()>>,
    /// Whether it runs: a `resume_soon` meanwhile sets `again` instead of starting another.
    running: bool,
    /// Asked again while running: it looks once more before it ends.
    again: bool,
}

/// How long the resume thread waits after a pass that failed, before it tries again.
const RESUME_RETRY: Duration = Duration::from_secs(30);

/// What a resume pass found.
enum Pass {
    /// Nothing more is pending.
    Done,
    /// The partition takes no writes, the slot is busy, or a rebuild stopped: look again
    /// soon.
    Wait,
    /// A look or a rebuild failed: look again later.
    Failed,
    /// The texts closed.
    Closed,
}

/// The node's one rebuild at a time (`Text.Rebuild`s and resumes, across partitions), and
/// whether a resume holds it: a client's rebuild then waits for it rather than being
/// refused, and the resume stops at its next document to let it in (ai1249 on #1756).
#[derive(Debug)]
pub(crate) struct RebuildSlot {
    place: Arc<Semaphore>,
    /// `--text-pack`: the node's partitions pack their text indexes in the background.
    pack: AtomicBool,
    /// A resume holds the place.
    resuming: AtomicBool,
    /// Clients waiting for the place: a resume stops for them.
    waiting: AtomicUsize,
}

impl RebuildSlot {
    pub(crate) fn new() -> Self {
        Self {
            place: Arc::new(Semaphore::new(1)),
            pack: AtomicBool::new(false),
            resuming: AtomicBool::new(false),
            waiting: AtomicUsize::new(0),
        }
    }

    /// Turns the background pack on (`--text-pack`), for every partition sharing the slot.
    pub(crate) fn pack_on(&self) {
        self.pack.store(true, Ordering::Release);
    }

    /// Whether the node packs its text indexes in the background.
    pub(crate) fn packs(&self) -> bool {
        self.pack.load(Ordering::Acquire)
    }

    /// A client's place: now; or, while a resume holds it, once the resume has stopped at its
    /// next document, within `wait`. `Overloaded` while another client's rebuild runs (as
    /// before #1756), or if the place doesn't come within `wait`.
    pub(crate) async fn client(&self, wait: Duration) -> Result<OwnedSemaphorePermit, ApiError> {
        let busy = || ApiError::overloaded("the server is already rebuilding a text index");
        if let Ok(p) = Arc::clone(&self.place).try_acquire_owned() {
            return Ok(p);
        }
        if !self.resuming.load(Ordering::SeqCst) {
            return Err(busy());
        }
        self.waiting.fetch_add(1, Ordering::SeqCst);
        let got = tokio::time::timeout(wait, Arc::clone(&self.place).acquire_owned()).await;
        self.waiting.fetch_sub(1, Ordering::SeqCst);
        match got {
            Ok(Ok(p)) => Ok(p),
            _ => Err(busy()),
        }
    }

    /// A resume's place, if it's free and no client waits for it.
    fn resume(self: &Arc<Self>) -> Option<ResumeHold> {
        if self.waiting.load(Ordering::SeqCst) > 0 {
            return None;
        }
        let place = Arc::clone(&self.place).try_acquire_owned().ok()?;
        self.resuming.store(true, Ordering::SeqCst);
        Some(ResumeHold {
            slot: Arc::clone(self),
            _place: place,
        })
    }

    /// Whether a client waits: a resume stops at its next document.
    fn wanted(&self) -> bool {
        self.waiting.load(Ordering::SeqCst) > 0
    }
}

/// A resume's hold on the [`RebuildSlot`].
struct ResumeHold {
    slot: Arc<RebuildSlot>,
    _place: OwnedSemaphorePermit,
}

impl Drop for ResumeHold {
    fn drop(&mut self) {
        // Before the place goes (fields drop after this): a client that finds the place
        // taken and this flag clear is refused, so it's cleared only as the place frees.
        self.slot.resuming.store(false, Ordering::SeqCst);
    }
}

/// Why a rebuild's [`Steps`] stopped it.
#[derive(Debug)]
pub(crate) enum Stop {
    /// The server or the partition closes: the next open resumes it.
    Closed,
    /// A client's `Text.Rebuild` waits for the slot (a resume's only).
    Yielded,
    /// The partition's gate refused the next document: a fence, or no lease.
    Fenced(ApiError),
}

/// A hold seam (tests): milliseconds a rebuild waits after each document, so a fence lands
/// mid-rebuild.
#[cfg(test)]
pub(crate) static STEP_DELAY_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// An inspection counter (tests): resumes that stopped for a client's `Text.Rebuild`, so a
/// test can tell a resume that yielded the slot from one that finished (#1995).
#[cfg(test)]
pub(crate) static YIELDED: AtomicUsize = AtomicUsize::new(0);

/// A rebuild's step (#1756), run after each document's transaction: the gate's hold for the
/// next document, as `gated`'s slices take one each (so a fence's drain waits for one
/// document), or a stop.
pub(crate) struct Steps {
    texts: Arc<Texts>,
    /// The hold the next document runs under.
    held: Option<super::gate::Held>,
    /// A resume's slot: it yields to a waiting client.
    slot: Option<Arc<RebuildSlot>>,
    stop: Option<Stop>,
}

impl Steps {
    /// Steps for a rebuild whose first document runs under `held` (the request's admission,
    /// or the resume's own `enter`).
    pub(crate) fn new(
        texts: Arc<Texts>,
        held: Option<super::gate::Held>,
        slot: Option<Arc<RebuildSlot>>,
    ) -> Self {
        Self {
            texts,
            held,
            slot,
            stop: None,
        }
    }

    pub(crate) fn step(&mut self) -> ControlFlow<()> {
        #[cfg(test)]
        {
            let ms = STEP_DELAY_MS.load(Ordering::Relaxed);
            if ms > 0 {
                std::thread::sleep(Duration::from_millis(ms));
            }
        }
        // The document just done: a commit that returned past the lease's end is logged, as
        // `gated::run` logs a slice's (its work is in this node's file, kept only if the
        // node gets the lease back).
        if let Some(h) = self.held.take()
            && let Err(e) = h.committed()
        {
            eprintln!("phdb serve: a text index rebuild or pack: {}", e.message);
        }
        if self.texts.closed.load(Ordering::Acquire) {
            return self.stopped(Stop::Closed);
        }
        if self.slot.as_ref().is_some_and(|s| s.wanted()) {
            return self.stopped(Stop::Yielded);
        }
        if let Some(gate) = &self.texts.gate {
            match gate.enter() {
                Ok(h) => self.held = Some(h),
                Err(e) => return self.stopped(Stop::Fenced(e)),
            }
        }
        ControlFlow::Continue(())
    }

    fn stopped(&mut self, why: Stop) -> ControlFlow<()> {
        self.stop = Some(why);
        ControlFlow::Break(())
    }

    /// Why it stopped, if it did; the last hold goes.
    pub(crate) fn finish(mut self) -> Option<Stop> {
        if let Some(h) = self.held.take()
            && let Err(e) = h.committed()
        {
            eprintln!("phdb serve: a text index rebuild or pack: {}", e.message);
        }
        self.stop.take()
    }
}

/// A hold seam (tests): a `Text.CreateIndex` of an index named here meets the test at the
/// first barrier, then waits at the second before it creates anything (#1916).
#[cfg(test)]
pub(crate) static HOLD_CREATE: Mutex<Vec<HeldCreate>> = Mutex::new(Vec::new());

/// An entry of [`HOLD_CREATE`]: the index's name, the barrier it meets the test at, and the
/// one it waits at.
#[cfg(test)]
pub(crate) type HeldCreate = (String, Arc<std::sync::Barrier>, Arc<std::sync::Barrier>);

#[cfg(test)]
fn hold_create(name: &str) {
    let held = HOLD_CREATE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .find(|(n, _, _)| n == name)
        .map(|(_, a, b)| (Arc::clone(a), Arc::clone(b)));
    if let Some((reached, release)) = held {
        reached.wait();
        release.wait();
    }
}

/// A `Text.CreateIndex` in flight over a source table ([`Texts::creating`]); dropping it
/// removes the entry.
pub(crate) struct Creating {
    texts: Arc<Texts>,
    source: String,
}

impl Drop for Creating {
    fn drop(&mut self) {
        let mut creating = self
            .texts
            .creating
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(i) = creating.iter().position(|s| *s == self.source) {
            creating.swap_remove(i);
        }
    }
}

impl Texts {
    pub(crate) fn new(
        db: Pigeonhole,
        gate: Option<Arc<super::gate::WriteGate>>,
        placement: Option<Arc<super::placement::Placement>>,
    ) -> Self {
        Self {
            db,
            open: Mutex::new(super::handles::Handles::default()),
            rebuild: Mutex::new(None),
            closed: AtomicBool::new(false),
            gate,
            placement,
            resume: Mutex::new(Resume::default()),
            creating: Mutex::new(Vec::new()),
            pack: Mutex::new(Resume::default()),
            packs: PackCounters::default(),
        }
    }

    /// Notes a `Text.CreateIndex` over `source` until the guard drops. Call it holding the
    /// partition's `schema` lock (#1916).
    pub(crate) fn creating(self: &Arc<Self>, source: &str) -> Creating {
        self.creating
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(source.to_owned());
        Creating {
            texts: Arc::clone(self),
            source: source.to_owned(),
        }
    }

    /// Whether a `Text.CreateIndex` over `source` is in flight ([`Texts::creating`]).
    pub(crate) fn creating_over(&self, source: &str) -> bool {
        self.creating
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|s| s == source)
    }

    /// Resumes this partition's stopped rebuilds (#1756), on a thread of its own
    /// (`phdb-serve-text-resume`) that waits for the partition to take writes, then for the
    /// node's rebuild slot, and ends once none is pending. Called at the partition's open
    /// (after a restart or a move), after a promote, and when a rebuild stops at a fence.
    /// A call while it runs makes it look once more before it ends.
    pub(crate) fn resume_soon(self: &Arc<Self>, slot: &Arc<RebuildSlot>) {
        let mut r = self.resume.lock().unwrap_or_else(PoisonError::into_inner);
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        if r.running {
            r.again = true;
            return;
        }
        let me = Arc::clone(self);
        let slot = Arc::clone(slot);
        let spawned = std::thread::Builder::new()
            .name("phdb-serve-text-resume".into())
            .spawn(move || me.resume_loop(&slot));
        match spawned {
            Ok(h) => {
                r.running = true;
                r.again = false;
                // The last one ended (it cleared `running`): nothing to wait for.
                r.thread = Some(h);
            }
            Err(e) => eprintln!("phdb serve: no thread to resume text index rebuilds: {e}"),
        }
    }

    fn resume_loop(self: Arc<Self>, slot: &Arc<RebuildSlot>) {
        if let Some(p) = &self.placement {
            p.enter();
        }
        // Indexes this run couldn't open or resume: passed over until the next `resume_soon`,
        // so one bad index never holds the rest back (ai1249 on #1975).
        let mut failed = std::collections::HashSet::new();
        loop {
            {
                let mut r = self.resume.lock().unwrap_or_else(PoisonError::into_inner);
                if std::mem::take(&mut r.again) {
                    failed.clear();
                }
            }
            let wait = match self.resume_pass(slot, &mut failed) {
                Pass::Done => {
                    let mut r = self.resume.lock().unwrap_or_else(PoisonError::into_inner);
                    if r.again && !self.closed.load(Ordering::Acquire) {
                        continue;
                    }
                    r.running = false;
                    return;
                }
                Pass::Closed => {
                    self.resume
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .running = false;
                    return;
                }
                Pass::Wait => super::gated::IDLE,
                Pass::Failed => RESUME_RETRY,
            };
            self.idle(wait, |_| false);
        }
    }

    /// Waits `wait` in steps, so a close isn't kept waiting; ends early once `woken` says
    /// so.
    fn idle(&self, wait: Duration, woken: impl Fn(&Self) -> bool) {
        let until = std::time::Instant::now() + wait;
        while std::time::Instant::now() < until {
            if self.closed.load(Ordering::Acquire) || woken(self) {
                break;
            }
            std::thread::sleep(Duration::from_millis(50).min(wait));
        }
    }

    /// Starts this partition's pack thread (`phdb-serve-text-pack`, #1585) if the node runs
    /// with `--text-pack`, once: it packs the partition's text indexes, then again every
    /// [`PACK_EVERY`], until the texts close. Called where [`resume_soon`](Self::resume_soon)
    /// is. A call while it runs makes it pass again at once (a promote: the partition takes
    /// writes now).
    pub(crate) fn pack_soon(self: &Arc<Self>, slot: &Arc<RebuildSlot>) {
        if !slot.packs() {
            return;
        }
        let mut p = self.pack.lock().unwrap_or_else(PoisonError::into_inner);
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        if p.running {
            p.again = true;
            return;
        }
        let me = Arc::clone(self);
        let slot = Arc::clone(slot);
        let spawned = std::thread::Builder::new()
            .name("phdb-serve-text-pack".into())
            .spawn(move || me.pack_loop(&slot));
        match spawned {
            Ok(h) => {
                p.running = true;
                p.again = false;
                p.thread = Some(h);
            }
            Err(e) => eprintln!("phdb serve: no thread to pack text indexes: {e}"),
        }
    }

    fn pack_loop(self: Arc<Self>, slot: &Arc<RebuildSlot>) {
        if let Some(p) = &self.placement {
            p.enter();
        }
        // Indexes whose pack was refused or failed, logged once each: a later pass tries
        // them again without saying so again, and says when one goes through.
        let mut refused = std::collections::HashSet::new();
        loop {
            self.pack
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .again = false;
            let wait = match self.pack_pass(slot, &mut refused) {
                Pass::Closed => {
                    self.pack
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .running = false;
                    return;
                }
                Pass::Done => {
                    self.packs.passes.fetch_add(1, Ordering::Relaxed);
                    PACK_EVERY
                }
                Pass::Wait => super::gated::IDLE,
                Pass::Failed => RESUME_RETRY,
            };
            self.idle(wait, |me| {
                me.pack.lock().unwrap_or_else(PoisonError::into_inner).again
            });
        }
    }

    /// One pass over the partition's text indexes: each packed in turn, a chunk at a time
    /// under the gate and the slot. An index with a rebuild pending is left to the resume
    /// thread, and packed by a later pass.
    fn pack_pass(
        self: &Arc<Self>,
        slot: &Arc<RebuildSlot>,
        refused: &mut std::collections::HashSet<String>,
    ) -> Pass {
        if self.closed.load(Ordering::Acquire) {
            return Pass::Closed;
        }
        // Writes first: a fenced partition (or one without its lease) waits.
        if let Some(gate) = &self.gate
            && gate.fenced().is_some()
        {
            return Pass::Wait;
        }
        let names = match Index::list(&self.db) {
            Ok(n) => n,
            Err(e) => {
                eprintln!("phdb serve: listing the text indexes to pack: {e}");
                return Pass::Failed;
            }
        };
        if names.is_empty() {
            return Pass::Done;
        }
        let Some(_hold) = slot.resume() else {
            return Pass::Wait;
        };
        for name in names {
            let fail = |refused: &mut std::collections::HashSet<String>, why: String| {
                // Once for the index, until a pack of it goes through: a pass comes every
                // minute, and a table that never packs would otherwise count for ever
                // (ai1249 on #2018).
                if refused.insert(name.clone()) {
                    self.packs.refused.fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "phdb serve: text index {name}: not packed, and tried again each \
                         pass: {why}"
                    );
                }
            };
            let index = match self.get(&name) {
                Ok(i) => i,
                Err(e) => {
                    fail(refused, e.message);
                    continue;
                }
            };
            match index.rebuild_pending() {
                Ok(false) => {}
                // The resume thread's, or a client's: packed once it is done.
                Ok(true) => continue,
                Err(e) => {
                    fail(refused, e.to_string());
                    continue;
                }
            }
            let held = match self.gate.as_ref().map(super::gate::WriteGate::enter) {
                Some(Err(_)) => return Pass::Wait,
                Some(Ok(h)) => Some(h),
                None => None,
            };
            let mut steps = Steps::new(Arc::clone(self), held, Some(Arc::clone(slot)));
            let done = index.pack_with(PackOptions::new(), |_| steps.step());
            let stop = steps.finish();
            match done {
                // A chunk that kept conflicting with writers: the cursor stays, and the next
                // pass carries on from it (ai1249's S2 on #2009). Not a failure.
                Err(e) if e.code() == pigeonhole::ErrorCode::Conflict => {
                    self.packs.conflicts.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => fail(refused, e.to_string()),
                Ok(s) => {
                    self.packs.postings.fetch_add(s.postings, Ordering::Relaxed);
                    if refused.remove(&name) {
                        eprintln!("phdb serve: text index {name}: packed now");
                    }
                    if s.stopped {
                        return match stop {
                            Some(Stop::Closed) => Pass::Closed,
                            _ => Pass::Wait,
                        };
                    }
                }
            }
        }
        Pass::Done
    }

    /// One look at the pending rebuilds: each resumed in turn, under the gate and the slot.
    /// One that can't be opened or resumed (a custom tokeniser, a repair a crash left, a
    /// source gone) is logged once, added to `failed`, and passed over by this run.
    fn resume_pass(
        self: &Arc<Self>,
        slot: &Arc<RebuildSlot>,
        failed: &mut std::collections::HashSet<String>,
    ) -> Pass {
        if self.closed.load(Ordering::Acquire) {
            return Pass::Closed;
        }
        // Writes first: a fenced partition (or one without its lease) waits.
        if let Some(gate) = &self.gate
            && gate.fenced().is_some()
        {
            return Pass::Wait;
        }
        let names = match Index::pending_rebuilds(&self.db) {
            Ok(n) => n,
            Err(e) => {
                eprintln!("phdb serve: looking for stopped text index rebuilds: {e}");
                return Pass::Failed;
            }
        };
        let names: Vec<String> = names.into_iter().filter(|n| !failed.contains(n)).collect();
        if names.is_empty() {
            return Pass::Done;
        }
        let Some(_hold) = slot.resume() else {
            return Pass::Wait;
        };
        for name in names {
            let index = match self.get(&name) {
                Ok(i) => i,
                Err(e) => {
                    eprintln!(
                        "phdb serve: text index {name}: its stopped rebuild can't resume, and \
                         stays stopped until the partition opens again or a Text.Rebuild of it: \
                         {}",
                        e.message
                    );
                    failed.insert(name);
                    continue;
                }
            };
            let held = match self.gate.as_ref().map(super::gate::WriteGate::enter) {
                Some(Err(_)) => return Pass::Wait,
                Some(Ok(h)) => Some(h),
                None => None,
            };
            eprintln!("phdb serve: text index {name}: resuming its stopped rebuild");
            let mut steps = Steps::new(Arc::clone(self), held, Some(Arc::clone(slot)));
            let done = index.resume_rebuild_with(|_| steps.step());
            let stop = steps.finish();
            match done {
                Err(e) => {
                    eprintln!(
                        "phdb serve: text index {name}: its resumed rebuild failed, and stays \
                         stopped until the partition opens again or a Text.Rebuild of it: {e}"
                    );
                    failed.insert(name);
                    continue;
                }
                Ok(s) if !s.stopped => {
                    eprintln!(
                        "phdb serve: text index {name}: its rebuild finished ({} documents, {} \
                         changed)",
                        s.documents, s.changed
                    );
                }
                Ok(s) => {
                    #[cfg(test)]
                    if matches!(stop, Some(Stop::Yielded)) {
                        YIELDED.fetch_add(1, Ordering::SeqCst);
                    }
                    eprintln!(
                        "phdb serve: text index {name}: its resumed rebuild stopped again after \
                         {} documents ({})",
                        s.documents,
                        match &stop {
                            Some(Stop::Closed) => "closing".to_owned(),
                            Some(Stop::Yielded) => "a client's Text.Rebuild runs".to_owned(),
                            Some(Stop::Fenced(e)) => e.message.clone(),
                            None => "stopped".to_owned(),
                        }
                    );
                    return match stop {
                        Some(Stop::Closed) => Pass::Closed,
                        _ => Pass::Wait,
                    };
                }
            }
        }
        // Another may have stopped meanwhile: look again.
        Pass::Wait
    }

    /// Runs `rebuild` on a thread of its own (`phdb-serve-text-rebuild`), which `close`
    /// waits for: a shutdown never closes the database under a rebuild (ai1249's S1 on
    /// #1587). `Closed` once the server is closing.
    pub(crate) fn spawn_rebuild(
        &self,
        rebuild: impl FnOnce() + Send + 'static,
    ) -> Result<(), ApiError> {
        let mut thread = self.rebuild.lock().unwrap_or_else(PoisonError::into_inner);
        if self.closed.load(Ordering::Acquire) {
            return Err(ApiError::from_code(
                pigeonhole::ErrorCode::Closed,
                "the server is shutting down",
            ));
        }
        // The last rebuild's thread is done: the caller holds the one rebuild slot, which
        // that thread released last.
        *thread = Some(
            std::thread::Builder::new()
                .name("phdb-serve-text-rebuild".into())
                .spawn(rebuild)
                .map_err(|e| ApiError::internal(format!("a rebuild thread: {e}")))?,
        );
        Ok(())
    }

    /// Refuses new rebuilds, stops a running one and the resume thread, and waits for them:
    /// called at shutdown or at the partition's close, before the database closes. A rebuild
    /// stops at its next document, keeping its cursor, so the next open resumes it (#1756).
    pub(crate) fn close(&self) {
        let thread = {
            let mut thread = self.rebuild.lock().unwrap_or_else(PoisonError::into_inner);
            self.closed.store(true, Ordering::Release);
            thread.take()
        };
        // A rebuild stops at its next document (#1756), and the resume thread at its next
        // look: neither holds the close for a whole rebuild.
        let resume = self
            .resume
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .thread
            .take();
        let pack = self
            .pack
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .thread
            .take();
        for t in [thread, resume, pack].into_iter().flatten() {
            let _ = t.join();
        }
    }

    fn handles(&self) -> std::sync::MutexGuard<'_, super::handles::Handles<Index>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The index `name` if this server has it open: no I/O, so on any thread.
    pub(crate) fn cached(&self, name: &str) -> Option<Arc<Index>> {
        self.handles().get(name).cloned()
    }

    /// The index `name`, opened if this server hasn't yet. Sync: call it on the blocking
    /// pool. The open runs outside the cache's lock; two first calls racing both open it,
    /// and the first to finish is kept. An open that a [`forget`](Self::forget) overtook
    /// opens again, since it may have read the catalog from before it (#1632).
    pub(crate) fn get(&self, name: &str) -> Result<Arc<Index>, ApiError> {
        loop {
            let generation = {
                let open = self.handles();
                if let Some(i) = open.get(name) {
                    return Ok(Arc::clone(i));
                }
                open.generation()
            };
            let index = Index::open(&self.db, name)?;
            if let Some(kept) = self.keep(name, index, generation) {
                return Ok(kept);
            }
        }
    }

    /// Creates the index `name` over `source`, or with `if_missing` opens an existing one,
    /// whatever its source and options: `if_missing` compares neither. Sync, as `get`.
    pub(crate) fn create(
        &self,
        name: &str,
        source: Source,
        options: IndexOptions,
        if_missing: bool,
    ) -> Result<(), ApiError> {
        if if_missing && self.cached(name).is_some() {
            return Ok(());
        }
        #[cfg(test)]
        hold_create(name);
        let generation = self.handles().generation();
        let index = match Index::create(&self.db, name, source, options) {
            Ok(i) => i,
            Err(e) if if_missing && e.code() == pigeonhole::ErrorCode::TableExists => {
                Index::open(&self.db, name)?
            }
            Err(e) => return Err(e.into()),
        };
        // Overtaken by a forget: the next call opens it afresh.
        self.keep(name, index, generation);
        Ok(())
    }

    /// Drops every handle: the catalog changed under the server (a replica applied a
    /// primary's change, or a re-bootstrap swapped the database), so the next call on each
    /// name opens it again (#1436). A call holding a handle finishes on it. An index has no
    /// thread, so nothing else is left. An open running across it keeps nothing, and opens
    /// again (#1632).
    pub(crate) fn forget(&self) {
        self.handles().forget();
    }

    /// Keeps `index` under `name` unless another call kept one first (then that one is
    /// returned). `None`, keeping nothing, if a [`forget`](Self::forget) came after
    /// `generation`, the one the open started at (#1632).
    fn keep(&self, name: &str, index: Index, generation: u64) -> Option<Arc<Index>> {
        self.handles().keep(generation, name, Arc::new(index))
    }
}

/// A request's index options: unset ones take the library's defaults.
pub(crate) fn options(r: &proto::CreateTextIndexRequest) -> Result<IndexOptions, ApiError> {
    let mut o = IndexOptions::new();
    if let Some(k1) = r.k1 {
        o = o.k1(k1);
    }
    if let Some(b) = r.b {
        o = o.b(b);
    }
    if let Some(on) = r.positions {
        o = o.positions(on);
    }
    if let Some(on) = r.stop_words {
        o = o.stop_words(on);
    }
    if let Some(n) = r.max_terms {
        o = o.max_terms(n);
    }
    if let Some(n) = r.max_term_bytes {
        o = o.max_term_bytes(n);
    }
    o = match proto::Normalization::try_from(r.normalization) {
        Ok(proto::Normalization::Unspecified) => o,
        Ok(proto::Normalization::Nfc) => o.normalization(Normalization::Nfc),
        Ok(proto::Normalization::Nfkc) => o.normalization(Normalization::Nfkc),
        Ok(proto::Normalization::None) => o.normalization(Normalization::None),
        Err(_) => return Err(ApiError::invalid("an unknown normalization")),
    };
    Ok(o)
}

/// A search's query: the syntax, parsed at once so a bad one is `InvalidArgument` before the
/// search, or the clauses in the request's order. A repeated clause is refused: the library
/// counts it once, which would shift `TextMatch.clause`, the clause's index in the request
/// (ai1249's D1 on 7c).
pub(crate) fn query(r: &proto::TextSearchRequest) -> Result<Query, ApiError> {
    use proto::text_clause::Clause as C;
    use proto::text_search_request::Query as Q;
    match &r.query {
        None => Err(ApiError::invalid(
            "a search needs a query: `syntax` or `clauses`",
        )),
        Some(Q::Syntax(s)) => Ok(Query::parse(s)?),
        Some(Q::Clauses(q)) => {
            let mut out = Query::new();
            for (i, c) in q.clauses.iter().enumerate() {
                let before = out.clauses().len();
                out = match &c.clause {
                    Some(C::Term(t)) => out.term(t.as_str()),
                    Some(C::Text(t)) => out.text(t.as_str()),
                    Some(C::Phrase(p)) => {
                        let slop = u16::try_from(p.slop)
                            .map_err(|_| ApiError::invalid("a phrase's slop is at most 65,535"))?;
                        out.quoted_phrase(p.text.as_str(), slop)
                    }
                    Some(C::Fuzzy(f)) => out.fuzzy(f.term.as_str(), fuzziness(f.fuzziness)?),
                    None => return Err(ApiError::invalid(format!("clause {i} is empty"))),
                };
                if out.clauses().len() == before {
                    return Err(ApiError::invalid(format!(
                        "clause {i} repeats an earlier clause"
                    )));
                }
            }
            Ok(out)
        }
    }
}

fn fuzziness(f: i32) -> Result<Fuzziness, ApiError> {
    Ok(match proto::Fuzziness::try_from(f) {
        Ok(proto::Fuzziness::Unspecified | proto::Fuzziness::Auto) => Fuzziness::Auto,
        Ok(proto::Fuzziness::One) => Fuzziness::One,
        Ok(proto::Fuzziness::Two) => Fuzziness::Two,
        Err(_) => return Err(ApiError::invalid("an unknown fuzziness")),
    })
}

/// A hit as the proto has it.
pub(crate) fn hit(h: pigeonhole_text::Hit) -> proto::TextHit {
    proto::TextHit {
        id: h.id.into(),
        score: h.score,
        text: h.text.unwrap_or_default(),
        matches: h
            .matches
            .into_iter()
            .map(|m| proto::TextMatch {
                clause: u32::try_from(m.clause).unwrap_or(u32::MAX),
                occurrence: m.occurrence,
                start: m.range.start as u64,
                end: m.range.end as u64,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::Texts;
    use pigeonhole::{Family, Options, Pigeonhole};
    use pigeonhole_text::{Index, IndexOptions, Source};

    /// #1632: an open that a `forget` overtook (a replica's catalog change, or a swap)
    /// keeps nothing, and the next call opens the index afresh.
    #[test]
    fn an_open_overtaken_by_a_forget_keeps_nothing() {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = Pigeonhole::open(dir.join("t.phdb"), Options::default().shards(1)).unwrap();
        let table = db
            .table("docs")
            .unwrap()
            .family("f", Family::default())
            .create()
            .unwrap();
        drop(
            Index::create(
                &db,
                "x",
                Source::new(&table, "f", b"body"),
                IndexOptions::new(),
            )
            .unwrap(),
        );
        let texts = Texts::new(db.clone(), None, None);
        // The steps of `get`, with a forget between the open and the keep.
        let started = texts.handles().generation();
        let index = Index::open(&db, "x").unwrap();
        texts.forget();
        assert!(
            texts.keep("x", index, started).is_none(),
            "a stale open is not kept"
        );
        assert!(texts.cached("x").is_none());
        // Opened afresh, it is kept.
        let kept = texts.get("x").unwrap();
        assert!(std::sync::Arc::ptr_eq(&kept, &texts.cached("x").unwrap()));
        drop((kept, table));
        db.close().unwrap();
    }
}

//! The pump's manifest commit in flight (#135, #166), and the threads blocked on the
//! writer's exclusion (#134 step 2, item 3: loom-modelled with the exclusion protocol).
//!
//! The commit is held here rather than by the pump, so that a thread blocked on the
//! exclusion can finish it once its root commit completes. Otherwise the exclusion would
//! stay held until the pump's shard runs again, which never happens when the blocked thread
//! is the one that drives it (application-owned mode, issue #135).
//!
//! Generic over the commit `C` and the root commit's result `R`, so the loom models below
//! run it alone.

#[cfg(engine_loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(engine_loom)]
use loom::sync::{Mutex, MutexGuard};
#[cfg(not(engine_loom))]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(engine_loom))]
use std::sync::{Mutex, MutexGuard};

use std::sync::PoisonError;

pub(crate) struct Flight<C, R> {
    state: Mutex<FlightState<C, R>>,
    /// Threads blocked in `commit_req_from_thread` (woken by a root commit's completion
    /// and by every release of the exclusion).
    threads: Mutex<Vec<std::task::Waker>>,
    thread_count: AtomicUsize,
}

struct FlightState<C, R> {
    /// Bumped per commit started, so a pump recognizes its own.
    generation: u64,
    commit: Option<C>,
    result: Option<R>,
}

/// Where a pump's commit is.
pub(crate) enum Stage<C, R> {
    /// The root commit has not completed.
    Pending,
    /// It completed: the taker finishes it.
    Done(Box<C>, R),
    /// Someone else finished it.
    Gone,
}

impl<C, R> Default for Flight<C, R> {
    fn default() -> Self {
        Self {
            state: Mutex::new(FlightState {
                generation: 0,
                commit: None,
                result: None,
            }),
            threads: Mutex::new(Vec::new()),
            thread_count: AtomicUsize::new(0),
        }
    }
}

impl<C, R> std::fmt::Debug for Flight<C, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Flight").finish_non_exhaustive()
    }
}

impl<C, R> Flight<C, R> {
    fn lock(&self) -> MutexGuard<'_, FlightState<C, R>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records `commit` (whose root commit is about to be submitted); returns its generation.
    pub(crate) fn start(&self, commit: C) -> u64 {
        let mut st = self.lock();
        debug_assert!(
            st.commit.is_none(),
            "one manifest commit in flight at a time"
        );
        st.generation += 1;
        st.commit = Some(commit);
        st.result = None;
        st.generation
    }

    /// The root commit of `generation` completed.
    pub(crate) fn complete(&self, generation: u64, result: R) {
        {
            let mut st = self.lock();
            if st.generation == generation && st.commit.is_some() {
                st.result = Some(result);
            }
        }
        self.wake_threads();
    }

    /// Takes the pump's commit `generation` if its root commit completed.
    pub(crate) fn stage(&self, generation: u64) -> Stage<C, R> {
        let mut st = self.lock();
        if st.generation != generation || st.commit.is_none() {
            return Stage::Gone;
        }
        match st.result.take() {
            Some(r) => Stage::Done(Box::new(st.commit.take().expect("checked")), r),
            None => Stage::Pending,
        }
    }

    /// Takes whatever commit is in flight if its root commit completed and `held` does not
    /// hold it back (a test hook): a blocked thread finishing it for a pump that has not
    /// run. Taking it hands the pump's exclusion over to the taker.
    pub(crate) fn take_done(&self, held: impl FnOnce(&C) -> bool) -> Option<(C, R)> {
        let mut st = self.lock();
        let commit = st.commit.as_ref()?;
        st.result.as_ref()?;
        if held(commit) {
            return None;
        }
        Some((
            st.commit.take().expect("checked"),
            st.result.take().expect("checked"),
        ))
    }

    /// Takes the pump's commit `generation` whatever its state (the pump is being dropped).
    pub(crate) fn abandon(&self, generation: u64) -> Option<(C, Option<R>)> {
        let mut st = self.lock();
        if st.generation != generation {
            return None;
        }
        let commit = st.commit.take()?;
        Some((commit, st.result.take()))
    }

    /// Registers a blocked thread's waker. The caller then re-checks the exclusion and
    /// [`Flight::ready`]: a release or completion before the registration is seen there.
    pub(crate) fn register(&self, waker: &std::task::Waker) {
        let mut list = self.threads.lock().unwrap_or_else(PoisonError::into_inner);
        if !list.iter().any(|w| w.will_wake(waker)) {
            list.push(waker.clone());
        }
        self.thread_count.store(list.len(), Ordering::SeqCst);
    }

    /// Wakes (and forgets) every registered thread.
    pub(crate) fn wake_threads(&self) {
        if self.thread_count.load(Ordering::SeqCst) == 0 {
            return;
        }
        let woken = {
            let mut list = self.threads.lock().unwrap_or_else(PoisonError::into_inner);
            self.thread_count.store(0, Ordering::SeqCst);
            std::mem::take(&mut *list)
        };
        for w in woken {
            w.wake();
        }
    }

    /// Whether a completed commit waits to be finished.
    pub(crate) fn ready(&self) -> bool {
        let st = self.lock();
        st.commit.is_some() && st.result.is_some()
    }

    /// Whether commit `generation`'s root commit completed and `f` holds for it (test hook).
    #[cfg(feature = "test-hooks")]
    pub(crate) fn completed(&self, generation: u64, f: impl FnOnce(&C) -> bool) -> bool {
        let st = self.lock();
        st.generation == generation && st.result.is_some() && st.commit.as_ref().is_some_and(f)
    }
}

/// The exclusion protocol (manifest.rs) around the real [`Flight`]: `claim`/`release`,
/// `begin`/`end` over the request queue, `ManifestPump::run` and `commit_req_from_thread`,
/// each mirrored line for line minus the I/O. Keep them in step with manifest.rs.
#[cfg(all(test, engine_loom))]
mod loom_tests {
    use super::{Flight, Stage};
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::{Arc, Condvar, Mutex};
    use loom::thread;
    use std::task::{Wake, Waker};

    /// Requests in a model (at most); each is answered (`end`) exactly once.
    const REQS: usize = 3;

    /// A request's `Waiter`: `end` answers it and wakes the thread that polled it, as the
    /// engine's notifier does.
    #[derive(Default)]
    struct Reply {
        answers: usize,
        waker: Option<Waker>,
    }

    /// `Shared`, as far as the exclusion protocol goes.
    struct M {
        /// `manifest_busy`, an atomic try-lock in the engine. A `Mutex<bool>` here: loom 0.7
        /// lets an RMW read a value older than the latest store (tokio-rs/loom#254), fixed by
        /// tokio-rs/loom#391; #533 tracks going back to the atomic once loom releases it.
        busy: Mutex<bool>,
        /// `manifest_queue`.
        queue: Mutex<Vec<usize>>,
        /// A commit carries the requests `begin` took; its root commit cannot fail here.
        flight: Flight<Vec<usize>, ()>,
        replies: [Mutex<Reply>; REQS],
        /// Threads holding the exclusion (at most one).
        holders: AtomicUsize,
        /// Root-commit completions (the I/O), joined at the end.
        io: Mutex<Vec<thread::JoinHandle<()>>>,
    }

    impl M {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                busy: Mutex::new(false),
                queue: Mutex::new(Vec::new()),
                flight: Flight::default(),
                replies: std::array::from_fn(|_| Mutex::new(Reply::default())),
                holders: AtomicUsize::new(0),
                io: Mutex::new(Vec::new()),
            })
        }

        fn claim(&self) -> bool {
            let won = !std::mem::replace(&mut *self.busy.lock().unwrap(), true);
            if won {
                let before = self.holders.fetch_add(1, Ordering::Relaxed);
                assert_eq!(before, 0, "two threads hold the exclusion");
            }
            won
        }

        /// `release` (the final close's look is `close_gate`'s model).
        fn release(&self) {
            self.holders.fetch_sub(1, Ordering::Relaxed);
            *self.busy.lock().unwrap() = false;
            self.flight.wake_threads();
        }

        fn busy(&self) -> bool {
            *self.busy.lock().unwrap()
        }

        fn push(&self, req: usize) {
            self.queue.lock().unwrap().push(req);
        }

        fn queue_is_empty(&self) -> bool {
            self.queue.lock().unwrap().is_empty()
        }

        /// `begin`: takes the whole queue. Must hold the exclusion.
        fn begin(&self) -> Option<Vec<usize>> {
            let held = self.holders.load(Ordering::Relaxed);
            assert_eq!(held, 1, "begin without the exclusion");
            let reqs = std::mem::take(&mut *self.queue.lock().unwrap());
            (!reqs.is_empty()).then_some(reqs)
        }

        /// `end`: answers the commit's requests. Must hold the exclusion.
        fn end(&self, commit: Vec<usize>) {
            let held = self.holders.load(Ordering::Relaxed);
            assert_eq!(held, 1, "end without the exclusion");
            for r in commit {
                let waker = {
                    let mut reply = self.replies[r].lock().unwrap();
                    reply.answers += 1;
                    reply.waker.take()
                };
                if let Some(w) = waker {
                    w.wake();
                }
            }
        }

        /// Polls request `req`'s `Waiter`: answered, or `waker` is woken when it is.
        fn poll(&self, req: usize, waker: &Waker) -> bool {
            let mut reply = self.replies[req].lock().unwrap();
            if reply.answers > 0 {
                return true;
            }
            reply.waker = Some(waker.clone());
            false
        }

        /// `drain_sync`: the root commit runs inline (blocking).
        fn drain_sync(&self) {
            while let Some(commit) = self.begin() {
                self.end(commit);
            }
        }

        /// `commit_req_from_thread`, with its I/O reaping left out: there is no I/O only
        /// this thread completes.
        fn commit_from_thread(&self, req: usize) {
            self.push(req);
            let parker = Parker::new();
            let waker = Waker::from(std::sync::Arc::clone(&parker));
            loop {
                if self.claim() {
                    loop {
                        self.drain_sync();
                        self.release();
                        if self.queue_is_empty() || !self.claim() {
                            break;
                        }
                    }
                } else if let Some((commit, ())) = self.flight.take_done(|_| false) {
                    self.end(commit);
                    self.release();
                    continue;
                }
                if self.poll(req, &waker) {
                    return;
                }
                self.flight.register(&waker);
                if !self.busy() || self.flight.ready() {
                    continue;
                }
                parker.park();
            }
        }
    }

    /// `std::thread::park`/`unpark` (the thread's waker, `waker::thread_waker`, and a
    /// shard waiting for its task's waker): a token, so a wake before the park is kept.
    /// Loom's own park drops an unpark that reaches a thread blocked on a loom `Mutex`.
    struct Parker {
        token: Mutex<bool>,
        cv: Condvar,
    }

    impl Parker {
        fn new() -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                token: Mutex::new(false),
                cv: Condvar::new(),
            })
        }

        fn park(&self) {
            let mut token = self.token.lock().unwrap();
            while !*token {
                token = self.cv.wait(token).unwrap();
            }
            *token = false;
        }
    }

    impl Wake for Parker {
        fn wake(self: std::sync::Arc<Self>) {
            *self.token.lock().unwrap() = true;
            self.cv.notify_one();
        }
    }

    enum Poll {
        Done,
        Blocked,
    }

    /// `ManifestPump`, with its shard's parker as the task's waker.
    struct Pump {
        m: Arc<M>,
        inflight: Option<u64>,
        task: std::sync::Arc<Parker>,
    }

    impl Pump {
        /// `ManifestPump::start`: the root commit completes on another thread.
        fn start(&mut self, commit: Vec<usize>) {
            let generation = self.m.flight.start(commit);
            self.inflight = Some(generation);
            let (m, task) = (
                Arc::clone(&self.m),
                Waker::from(std::sync::Arc::clone(&self.task)),
            );
            let io = thread::spawn(move || {
                m.flight.complete(generation, ());
                task.wake();
            });
            self.m.io.lock().unwrap().push(io);
        }

        /// `ManifestPump::run`.
        fn run(&mut self) -> Poll {
            loop {
                if let Some(generation) = self.inflight {
                    match self.m.flight.stage(generation) {
                        Stage::Pending => return Poll::Blocked,
                        Stage::Done(commit, ()) => {
                            self.inflight = None;
                            self.m.end(*commit);
                            self.m.release();
                        }
                        Stage::Gone => self.inflight = None,
                    }
                    if self.m.queue_is_empty() {
                        return Poll::Done;
                    }
                }
                if !self.m.claim() {
                    return Poll::Done;
                }
                match self.m.begin() {
                    Some(commit) => {
                        self.start(commit);
                        return Poll::Blocked;
                    }
                    None => {
                        self.m.release();
                        if self.m.queue_is_empty() {
                            return Poll::Done;
                        }
                    }
                }
            }
        }

        /// The shard runs the task until it is done, parked while it is blocked.
        fn drive(&mut self) {
            while let Poll::Blocked = self.run() {
                self.task.park();
            }
        }
    }

    /// `manifest::submit` on a shard, and the pump it starts there.
    fn submit(m: &Arc<M>, req: usize) -> Pump {
        m.push(req);
        Pump {
            m: Arc::clone(m),
            inflight: None,
            task: Parker::new(),
        }
    }

    fn model(f: impl Fn() + Sync + Send + 'static) {
        let mut b = loom::model::Builder::new();
        b.preemption_bound = Some(2);
        b.check(f);
    }

    /// Every request answered once (the first `reqs`), nothing left queued or in flight,
    /// the exclusion free.
    fn settled(m: &M, reqs: usize) {
        for io in std::mem::take(&mut *m.io.lock().unwrap()) {
            io.join().unwrap();
        }
        for (r, reply) in m.replies.iter().enumerate().take(reqs) {
            assert_eq!(
                reply.lock().unwrap().answers,
                1,
                "request {r} answered once"
            );
        }
        assert!(m.queue_is_empty(), "a request was stranded in the queue");
        assert!(!m.flight.ready(), "a completed commit was never finished");
        assert!(!m.busy(), "the exclusion is still held");
    }

    #[test]
    fn loom_a_shard_pump_and_a_thread_commit_every_request_once() {
        // A shard submits and runs its pump to the end. An application thread submits on
        // the shard it drives and runs that pump once (it leaves if the exclusion is held),
        // then commits from the thread, then drives the pump on. In every interleaving: one
        // holder at a time, and every request answered once (whoever releases looks again).
        model(|| {
            let m = M::new();
            let shard = {
                let m = Arc::clone(&m);
                thread::spawn(move || submit(&m, 0).drive())
            };
            let mut pump = submit(&m, 2);
            let first = pump.run();
            m.commit_from_thread(1);
            if let Poll::Blocked = first {
                pump.drive();
            }
            shard.join().unwrap();
            settled(&m, 3);
        });
    }

    #[test]
    fn loom_a_request_queued_while_a_thread_holds_the_exclusion_is_not_stranded() {
        // A shard submits while an application thread drains the queue: its pump finds the
        // exclusion held and leaves, and runs again only if it started a commit. The thread
        // looks at the queue again after it releases, so the request is still answered
        // (`race_window` forces the engine's version of this).
        model(|| {
            let m = M::new();
            let shard = {
                let m = Arc::clone(&m);
                thread::spawn(move || submit(&m, 0).drive())
            };
            m.commit_from_thread(1);
            shard.join().unwrap();
            settled(&m, 2);
        });
    }

    #[test]
    fn loom_a_thread_driving_the_pumps_shard_finishes_its_commit() {
        // Application-owned (#135, #166): the thread that drives the pump's shard runs the
        // pump once, then commits from the thread; the pump does not run again until that
        // returns. Its commit's root completes on the I/O thread: the blocked thread takes
        // it over, so neither waits on the other. Another thread commits meanwhile.
        model(|| {
            let m = M::new();
            let other = {
                let m = Arc::clone(&m);
                thread::spawn(move || m.commit_from_thread(2))
            };
            let mut pump = submit(&m, 0);
            let first = pump.run();
            m.commit_from_thread(1);
            if let Poll::Blocked = first {
                pump.drive();
            }
            other.join().unwrap();
            settled(&m, 3);
        });
    }
}

#[cfg(all(test, not(engine_loom)))]
mod tests {
    use super::{Flight, Stage};

    #[test]
    fn a_completed_commit_is_finished_once_by_the_pump_or_a_taker() {
        let f: Flight<&str, u8> = Flight::default();
        let g = f.start("a");
        assert!(matches!(f.stage(g), Stage::Pending));
        assert!(f.take_done(|_| false).is_none(), "not completed");
        f.complete(g, 7);
        assert!(f.ready());
        assert!(f.take_done(|_| true).is_none(), "held back by the hook");
        assert_eq!(f.take_done(|_| false), Some(("a", 7)));
        assert!(matches!(f.stage(g), Stage::Gone), "the pump finds it gone");
        assert!(!f.ready());

        let g = f.start("b");
        f.complete(g - 1, 1);
        assert!(!f.ready(), "a stale generation's completion is ignored");
        f.complete(g, 2);
        assert!(matches!(f.stage(g), Stage::Done(c, 2) if *c == "b"));
        assert!(f.take_done(|_| false).is_none());

        let g = f.start("c");
        assert_eq!(f.abandon(g + 1), None);
        assert_eq!(f.abandon(g), Some(("c", None)));
    }
}

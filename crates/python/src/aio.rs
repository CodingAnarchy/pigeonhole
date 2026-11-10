//! Native `asyncio` over the async front door (#1479, ICR 0121; D196, D210).
//!
//! An `*_async` call returns an `asyncio.Future` of the running loop. Its Rust future (a
//! `CommitFuture`, `GetFuture`, a feed wait) is driven without the GIL:
//!
//! - **The call** creates the asyncio future, then, with the GIL released, builds the Rust
//!   future (a `commit_async` submits right there when its shard has room, D210) and polls it
//!   once. Ready already: the result is set at once, on the loop thread. Pending: the task is
//!   parked under its own lock, which it holds from the first poll to the store (perf2's R5).
//! - **The waker** runs on whatever thread resolves the future (a shard or group-sync
//!   thread). It never touches Python: it sets the task's `queued` flag and, if it wasn't set,
//!   pushes the task onto the process's queue, and signals the bridge only if the bridge is
//!   parked (R2), so a busy bridge costs a shard thread a short push and one atomic load.
//! - **The bridge thread** (`pigeonhole-asyncio`, one per process, started on the first
//!   parked await) drains the queue. Per task: clear `queued` before the poll, so a wake
//!   during it requeues the task (R1); skip a cancelled one (C1); poll without the GIL, inside
//!   `catch_unwind` (R3, R4). Then it takes the GIL once for the whole drain, converts every
//!   result, and hands each event loop its batch through one `call_soon_threadsafe` (B2: a
//!   future completes on the loop it was created on).
//! - **The poll pool** (`pigeonhole-asyncio-poll-N`, 2 to 16 threads by the machine's
//!   parallelism, or `PIGEONHOLE_ASYNCIO_POOL_THREADS`; started on the first call that uses
//!   it): the same loop on a second queue, each thread taking one task at a time, for
//!   futures whose polls are long or block. `start_on_bridge` starts them there without the
//!   call's inline poll (a memory recall, which runs the sync recall in one poll: ai1249's B1
//!   on #1560, and g45's offload), so they hold up neither the event loop nor the bridge
//!   thread that commits complete on. A task stays on the queue
//!   it started on; a task woken during a poll may be picked up by a second thread, which
//!   waits on its lock and polls it again, or finds it done.
//! - **Cancellation:** the asyncio future's done callback, on the loop thread, marks a
//!   cancelled task and drops its Rust future at once if no poll holds it, else wakes the
//!   bridge to drop it (C2). A commit waiting for admission (D210) thus never holds its
//!   shard's queue after its cancel, and one not yet submitted is never submitted.
//! - **Exit and fork:** the bridge attaches with `Python::try_attach`, so it never hangs on a
//!   finalizing interpreter (it drops what's left). A forked child starts a fresh bridge on
//!   its first parked await; an await started before `fork()` never completes in the child
//!   (its task belongs to the parent's bridge), so start new ones there.
//! - **A panic** in a poll, a result's conversion or a delivery fails that task with an
//!   internal `RuntimeError` and never stops the bridge, which every await in the process
//!   relies on (perf2's F1).

use std::collections::VecDeque;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PyTuple};

/// What a finished task hands to its asyncio future, built on the bridge with the GIL held.
pub(crate) enum Entry {
    /// `set_result(value)`.
    Value(Py<PyAny>),
    /// `set_exception(error)`.
    Error(PyErr),
    /// `claim(True)` on the loop gives the result (`None`: nothing to set); `claim(False)`
    /// releases it if the future was cancelled meanwhile (the feed's hand-back, B5).
    Claim(Py<PyAny>),
}

/// A future's result, converted to Python later, under the GIL.
pub(crate) type Output = Box<dyn FnOnce(Python<'_>) -> Entry + Send>;

type BoxFuture = Pin<Box<dyn Future<Output = Output> + Send>>;

/// `r` as an [`Output`]: its value converted by `f`, or its error as the sync forms raise it.
pub(crate) fn output<T, F>(r: pigeonhole::Result<T>, f: F) -> Output
where
    T: Send + 'static,
    F: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
{
    Box::new(
        move |py| match crate::error::OrPy::or_py(r).and_then(|v| f(py, v)) {
            Ok(v) => Entry::Value(v),
            Err(e) => Entry::Error(e),
        },
    )
}

struct Bridge {
    pid: u32,
    queue: Mutex<VecDeque<Arc<Task>>>,
    ready: Condvar,
    /// How many of the bridge's threads wait on `ready`, changed under `queue`'s lock: a
    /// waker signals only when one does.
    idle: AtomicUsize,
    /// Tasks a thread takes per drain: all of them for the bridge (one thread), one for the
    /// poll pool, so its threads share the work.
    take: usize,
}

static BRIDGE: Mutex<Option<Arc<Bridge>>> = Mutex::new(None);
static POOL: Mutex<Option<Arc<Bridge>>> = Mutex::new(None);

thread_local! {
    /// This thread's handle on the bridge, so a loop's parked awaits don't take `BRIDGE`'s
    /// lock (perf2's F3). Checked against the pid: a forked child starts its own.
    static LOCAL: std::cell::RefCell<Option<Arc<Bridge>>> = const { std::cell::RefCell::new(None) };
    /// The same for the poll pool.
    static LOCAL_POOL: std::cell::RefCell<Option<Arc<Bridge>>> = const { std::cell::RefCell::new(None) };
}

/// The process's bridge, started on the first parked await (and again in a forked child).
fn bridge() -> Arc<Bridge> {
    cached(&LOCAL, || {
        shared(&BRIDGE, 1, usize::MAX, "pigeonhole-asyncio")
    })
}

/// The poll pool (ai1249's B1 on #1560): a bridge of [`pool_threads`] threads, each taking
/// one task at a time, for the futures [`start_on_bridge`] starts, whose polls are long or
/// block (a memory recall runs the sync recall in its one poll). On the one bridge thread,
/// 16 concurrent recalls ran at a fifth of `to_thread`'s rate and held commits' completions
/// up behind them. Commits, gets and feed waits stay on the bridge.
fn pool() -> Arc<Bridge> {
    cached(&LOCAL_POOL, || {
        shared(&POOL, pool_threads(), 1, "pigeonhole-asyncio-poll")
    })
}

/// The pool's threads: `PIGEONHOLE_ASYNCIO_POOL_THREADS` if it's set to a whole number from
/// 1 to 256 (read once, when the pool starts), else the machine's parallelism, from 2 to 16.
/// A recall runs on one of them from start to finish, its reads included, so they bound how
/// many recalls run at once: at 8 threads, 16 and 32 callers both ran at about 127
/// recalls/s on the 16-core reference machine (g45).
fn pool_threads() -> usize {
    std::env::var("PIGEONHOLE_ASYNCIO_POOL_THREADS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| (1..=256).contains(n))
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map_or(2, std::num::NonZeroUsize::get)
                .clamp(2, 16)
        })
}

fn cached(
    local: &'static std::thread::LocalKey<std::cell::RefCell<Option<Arc<Bridge>>>>,
    start: impl FnOnce() -> Arc<Bridge>,
) -> Arc<Bridge> {
    let pid = std::process::id();
    local.with(|l| {
        let mut l = l.borrow_mut();
        if let Some(b) = l.as_ref().filter(|b| b.pid == pid) {
            return Arc::clone(b);
        }
        let b = start();
        *l = Some(Arc::clone(&b));
        b
    })
}

/// `slot`'s bridge for this process, started with `threads` threads named `name` if there's
/// none yet (or only a forked parent's).
fn shared(
    slot: &Mutex<Option<Arc<Bridge>>>,
    threads: usize,
    take: usize,
    name: &str,
) -> Arc<Bridge> {
    let mut b = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(b) = b.as_ref().filter(|b| b.pid == std::process::id()) {
        return Arc::clone(b);
    }
    let new = Arc::new(Bridge {
        pid: std::process::id(),
        queue: Mutex::new(VecDeque::new()),
        ready: Condvar::new(),
        idle: AtomicUsize::new(0),
        take,
    });
    for i in 0..threads {
        let run = Arc::clone(&new);
        let name = if threads == 1 {
            name.to_owned()
        } else {
            format!("{name}-{i}")
        };
        std::thread::Builder::new()
            .name(name)
            .spawn(move || run.run())
            .expect("spawn an asyncio bridge thread");
    }
    *b = Some(Arc::clone(&new));
    new
}

impl Bridge {
    fn run(&self) {
        // F2: the queue and this spare swap under the lock, so neither the shard threads'
        // pushes nor this drain allocate once both have grown.
        let mut batch: VecDeque<Arc<Task>> = VecDeque::new();
        loop {
            {
                let mut q = self.queue.lock().expect("not poisoned");
                while q.is_empty() {
                    self.idle.fetch_add(1, Ordering::SeqCst);
                    q = self.ready.wait(q).expect("not poisoned");
                    self.idle.fetch_sub(1, Ordering::SeqCst);
                }
                if self.take >= q.len() {
                    std::mem::swap(&mut *q, &mut batch);
                } else {
                    batch.extend(q.drain(..self.take));
                    if self.idle.load(Ordering::SeqCst) > 0 {
                        // More than this thread takes: another waiting thread takes some.
                        self.ready.notify_one();
                    }
                }
            }
            let done: Vec<(Arc<Task>, Result<Output, String>)> = batch
                .drain(..)
                .filter_map(|t| {
                    // R1: cleared before the poll, so a wake during it queues the task again.
                    t.queued.store(false, Ordering::SeqCst);
                    t.poll().map(|r| (t, r))
                })
                .collect();
            if !done.is_empty() {
                // F1: a drain that panics past its per-task guards is reported, and the
                // bridge goes on.
                if let Err(p) = catch_unwind(AssertUnwindSafe(|| deliver(done))) {
                    eprintln!("pigeonhole-asyncio: {}", panic_message(&p));
                }
            }
        }
    }
}

struct Task {
    /// The Rust future: `None` before the first poll stores it, once done, or once dropped.
    fut: Mutex<Option<BoxFuture>>,
    queued: AtomicBool,
    cancelled: AtomicBool,
    /// The asyncio future and its loop: taken out under the GIL on delivery or cancel, so
    /// the last `Arc<Task>`, often a stale waker the engine drops on a shard thread, holds
    /// no Python object (cli692's P2).
    py: Mutex<Option<(Py<PyAny>, Py<PyAny>)>>,
    /// Set once the first poll is `Pending` (F3). A wake before that only sets `queued`, and
    /// `start` queues the task itself.
    bridge: std::sync::OnceLock<Arc<Bridge>>,
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.queued.swap(true, Ordering::SeqCst) {
            return;
        }
        let Some(b) = self.bridge.get() else {
            return;
        };
        b.queue
            .lock()
            .expect("not poisoned")
            .push_back(Arc::clone(self));
        if b.idle.load(Ordering::SeqCst) > 0 {
            b.ready.notify_one();
        }
    }
}

impl Task {
    /// One poll, without the GIL. `Some` once the future finished (or panicked).
    fn poll(self: &Arc<Self>) -> Option<Result<Output, String>> {
        let mut slot = self.fut.lock().expect("not poisoned");
        if self.cancelled.load(Ordering::SeqCst) {
            // C1: dropped, never polled again, so a waiter admitted after its cancel never
            // submits.
            let fut = slot.take();
            drop(slot);
            drop(fut);
            return None;
        }
        let fut = slot.as_mut()?;
        let r = poll_once(self, fut);
        if r.is_some() {
            *slot = None;
        }
        r
    }
}

fn poll_once(task: &Arc<Task>, fut: &mut BoxFuture) -> Option<Result<Output, String>> {
    let waker = Waker::from(Arc::clone(task));
    let mut cx = Context::from_waker(&waker);
    match catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(&mut cx))) {
        Ok(Poll::Pending) => None,
        Ok(Poll::Ready(out)) => Some(Ok(out)),
        Err(panic) => Some(Err(panic_message(&panic))),
    }
}

fn panic_message(p: &Box<dyn std::any::Any + Send>) -> String {
    let what = p
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown".into());
    format!("internal error in a pigeonhole future: {what}")
}

fn entry(py: Python<'_>, r: Result<Output, String>) -> Entry {
    match r {
        Ok(out) => out(py),
        Err(msg) => Entry::Error(PyRuntimeError::new_err(msg)),
    }
}

/// `(future, kind, value)` for `_complete`: kind 0 a result, 1 an exception, 2 a claim.
fn batch_item<'py>(py: Python<'py>, fut: &Py<PyAny>, e: Entry) -> PyResult<Bound<'py, PyTuple>> {
    let (kind, value) = match e {
        Entry::Value(v) => (0u8, v),
        Entry::Error(err) => (1, err.into_value(py).into_any()),
        Entry::Claim(c) => (2, c),
    };
    PyTuple::new(
        py,
        [
            fut.clone_ref(py),
            kind.into_pyobject(py)?.into_any().unbind(),
            value,
        ],
    )
}

/// Converts a drain's results under one GIL take, and gives each loop its batch.
fn deliver(done: Vec<(Arc<Task>, Result<Output, String>)>) {
    // A finalizing interpreter: drop everything, unconverted (never block on the GIL).
    let _ = Python::try_attach(|py| {
        let complete = match complete_fn(py) {
            Ok(f) => f,
            Err(e) => return e.print(py),
        };
        let mut loops: Vec<(Py<PyAny>, Vec<Bound<'_, PyTuple>>)> = Vec::new();
        for (task, r) in done {
            let converted = catch_unwind(AssertUnwindSafe(|| entry(py, r)))
                .unwrap_or_else(|p| Entry::Error(PyRuntimeError::new_err(panic_message(&p))));
            // Taken by a cancel already: nothing to complete.
            let Some((fut, event_loop)) = task.py.lock().expect("not poisoned").take() else {
                continue;
            };
            let item = match batch_item(py, &fut, converted) {
                Ok(i) => i,
                Err(e) => {
                    e.print(py);
                    continue;
                }
            };
            match loops.iter_mut().find(|(l, _)| l.is(&event_loop)) {
                Some((_, items)) => items.push(item),
                None => loops.push((event_loop, vec![item])),
            }
        }
        for (l, items) in loops {
            let Ok(list) = PyList::new(py, items) else {
                continue;
            };
            // A closed loop raises: its futures can never be awaited, so they're dropped.
            let _ = l
                .bind(py)
                .call_method1("call_soon_threadsafe", (complete, list));
        }
    });
}

/// `_complete(batch)`, cached: runs on the loop, setting each future unless cancelled.
fn complete_fn(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static COMPLETE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    COMPLETE
        .get_or_try_init(py, || {
            Ok::<_, PyErr>(wrap_pyfunction!(complete, py)?.into_any().unbind())
        })
        .map(|f| f.bind(py))
}

/// On the loop: each `(future, kind, value)` of a drain. A cancelled future is skipped (a
/// claim is released instead); an error in one doesn't stop the rest.
#[pyfunction]
fn complete(py: Python<'_>, batch: &Bound<'_, PyList>) {
    for item in batch.iter() {
        if let Err(e) = complete_one(&item) {
            e.print(py);
        }
    }
}

fn complete_one(item: &Bound<'_, PyAny>) -> PyResult<()> {
    let (fut, kind, value): (Bound<'_, PyAny>, u8, Bound<'_, PyAny>) = item.extract()?;
    let cancelled = fut.call_method0("cancelled")?.is_truthy()?;
    match kind {
        2 if cancelled => {
            value.call1((false,))?;
        }
        2 => {
            let r = value.call1((true,))?;
            if r.is_none() {
                // F4: can't happen for a live future; fail loudly rather than hang.
                let e = PyRuntimeError::new_err(
                    "internal error: a feed wait's commit was lost before it was delivered",
                );
                fut.call_method1("set_exception", (e,))?;
            } else {
                fut.call_method1("set_result", (r,))?;
            }
        }
        _ if cancelled => {}
        0 => {
            fut.call_method1("set_result", (value,))?;
        }
        _ => {
            fut.call_method1("set_exception", (value,))?;
        }
    }
    Ok(())
}

/// The asyncio future's done callback: a cancel marks the task and drops its Rust future
/// (C2), here if no poll holds it, else on the bridge, which it wakes.
#[pyclass(module = "pigeonhole", frozen)]
struct CancelHook(Arc<Task>);

#[pymethods]
impl CancelHook {
    fn __call__(&self, fut: &Bound<'_, PyAny>) -> PyResult<()> {
        if !fut.call_method0("cancelled")?.is_truthy()? {
            return Ok(());
        }
        let task = &self.0;
        task.cancelled.store(true, Ordering::SeqCst);
        // P2: the Python objects go here, under the GIL.
        let objects = task.py.lock().expect("not poisoned").take();
        drop(objects);
        let dropped = match task.fut.try_lock() {
            Ok(mut slot) => {
                let fut = slot.take();
                drop(slot);
                drop(fut);
                true
            }
            Err(_) => false,
        };
        if !dropped {
            task.wake_by_ref();
        }
        Ok(())
    }
}

/// The running loop and a new future on it: `RuntimeError` with no running loop.
pub(crate) fn new_future(py: Python<'_>) -> PyResult<(Bound<'_, PyAny>, Bound<'_, PyAny>)> {
    static GET_LOOP: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let get_loop = GET_LOOP.get_or_try_init(py, || {
        Ok::<_, PyErr>(py.import("asyncio")?.getattr("get_running_loop")?.unbind())
    })?;
    let event_loop = get_loop.bind(py).call0()?;
    let fut = event_loop.call_method0("create_future")?;
    Ok((event_loop, fut))
}

/// Starts `make`'s future for `fut`, a future of `event_loop` ([`new_future`]): builds it and
/// polls it once with the GIL released, then either sets `fut` now or parks the task.
pub(crate) fn start<'py, M, F>(
    py: Python<'py>,
    event_loop: &Bound<'py, PyAny>,
    fut: &Bound<'py, PyAny>,
    make: M,
) -> PyResult<()>
where
    M: FnOnce() -> F + Send,
    F: Future<Output = Output> + Send + 'static,
{
    let task = Arc::new(Task {
        fut: Mutex::new(None),
        queued: AtomicBool::new(false),
        cancelled: AtomicBool::new(false),
        py: Mutex::new(Some((fut.clone().unbind(), event_loop.clone().unbind()))),
        bridge: std::sync::OnceLock::new(),
    });
    let first = py.detach(|| {
        // R5: the slot stays locked from the first poll to the store, so a wake that comes
        // before the store makes the bridge wait for it.
        let mut slot = task.fut.lock().expect("not poisoned");
        let made = catch_unwind(AssertUnwindSafe(|| -> BoxFuture { Box::pin(make()) }));
        let mut f = match made {
            Ok(f) => f,
            Err(panic) => return Some(Err(panic_message(&panic))),
        };
        let r = poll_once(&task, &mut f);
        if r.is_none() {
            *slot = Some(f);
        }
        r
    });
    match first {
        Some(r) => {
            // Done at once: the task's Python objects go now, under the GIL (P2).
            drop(task.py.lock().expect("not poisoned").take());
            complete_now(py, fut, entry(py, r))
        }
        None => {
            let _ = task.bridge.set(bridge());
            // A wake during the first poll found no bridge and only set `queued`: queue the
            // task now (a duplicate push is harmless, R1). Both sides must stay `SeqCst`
            // swaps on `queued` (the waker swaps, then reads `bridge`; this sets `bridge`,
            // then swaps): a plain store/load pair would be a store-buffering race in which
            // neither side sees the other, and the wake is lost (perf2 on #1547).
            if task.queued.swap(false, Ordering::SeqCst) {
                task.wake_by_ref();
            }
            fut.call_method1("add_done_callback", (CancelHook(task),))?;
            Ok(())
        }
    }
}

/// [`start`] without its inline first poll (ai1249's B1 on #1560): the future is built here
/// and polled first on the poll pool, so a call whose polls are long or block (a memory
/// recall) never runs that work on the loop thread, and the call returns at once.
/// Commits keep `start`'s inline poll, which submits them at the call (D210's ordering).
pub(crate) fn start_on_bridge<'py, M, F>(
    py: Python<'py>,
    event_loop: &Bound<'py, PyAny>,
    fut: &Bound<'py, PyAny>,
    make: M,
) -> PyResult<()>
where
    M: FnOnce() -> F + Send,
    F: Future<Output = Output> + Send + 'static,
{
    let made = py.detach(|| catch_unwind(AssertUnwindSafe(|| -> BoxFuture { Box::pin(make()) })));
    let f = match made {
        Ok(f) => f,
        Err(panic) => return complete_now(py, fut, entry(py, Err(panic_message(&panic)))),
    };
    let task = Arc::new(Task {
        fut: Mutex::new(Some(f)),
        queued: AtomicBool::new(false),
        cancelled: AtomicBool::new(false),
        py: Mutex::new(Some((fut.clone().unbind(), event_loop.clone().unbind()))),
        bridge: std::sync::OnceLock::new(),
    });
    let _ = task.bridge.set(pool());
    task.wake_by_ref();
    fut.call_method1("add_done_callback", (CancelHook(task),))?;
    Ok(())
}

/// Sets `fut` on the loop thread (a future ready at its first poll, or a value known at
/// the call).
pub(crate) fn complete_now(py: Python<'_>, fut: &Bound<'_, PyAny>, e: Entry) -> PyResult<()> {
    complete_one(batch_item(py, &fut.clone().unbind(), e)?.as_any())
}

/// The retry state of one `Database.transact_async` (private, ICR 0121 B7): the sync
/// `transact`'s `RetryPolicy`, asked for each pause, so the Python retry loop has the same
/// attempts, backoff, deadline and retried codes.
#[pyclass(module = "pigeonhole._native", name = "_Retry")]
pub(crate) struct Retry {
    db: pigeonhole::Pigeonhole,
    policy: pigeonhole::RetryPolicy,
    started: std::time::Instant,
    attempt: u32,
}

#[pymethods]
impl Retry {
    #[new]
    #[pyo3(signature = (db, max_attempts=None, deadline=None))]
    fn new(
        db: &crate::db::Database,
        max_attempts: Option<u32>,
        deadline: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        use crate::Maybe;
        let mut policy = pigeonhole::RetryPolicy::default();
        policy = policy.maybe(max_attempts, |p, n| p.max_attempts(n));
        if let Some(d) = deadline {
            policy = policy.deadline(crate::convert::duration(d)?);
        }
        Ok(Self {
            db: db.db()?,
            policy,
            started: std::time::Instant::now(),
            attempt: 1,
        })
    }

    /// The pause in seconds before the next attempt after `error`, or `None` to give up
    /// (an error the policy doesn't retry, or its attempts or deadline spent).
    fn next_pause(&mut self, error: &Bound<'_, PyAny>) -> Option<f64> {
        let code = if error.is_instance_of::<crate::error::Conflict>() {
            pigeonhole::ErrorCode::Conflict
        } else if error.is_instance_of::<crate::error::Busy>() {
            pigeonhole::ErrorCode::Busy
        } else {
            return None;
        };
        if !self.policy.retries_code(code) {
            return None;
        }
        let pause = self
            .policy
            .next_pause(code, self.attempt, self.started.elapsed())?;
        self.attempt += 1;
        // Counted as the library's `transact` counts its retries (cli692's N1).
        self.db.note_transaction_retry();
        Some(pause.as_secs_f64())
    }
}

/// A hold seam for the bridge's cancellation tests (feature `test-hooks`, perf2's C1 and
/// C2 on #1479): a future that stays pending until `open()`, counting its polls and drops,
/// driven by the same `start` and bridge as every `*_async` call.
#[cfg(feature = "test-hooks")]
pub(crate) mod hooks {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    use pyo3::prelude::*;

    #[derive(Default)]
    struct State {
        open: AtomicBool,
        polls: AtomicU64,
        drops: AtomicU64,
        waker: Mutex<Option<Waker>>,
    }

    #[pyclass(module = "pigeonhole._native", name = "_Gate", frozen)]
    pub(crate) struct Gate(Arc<State>);

    struct Held(Arc<State>);

    impl Future for Held {
        type Output = super::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let s = &self.0;
            s.polls.fetch_add(1, Ordering::SeqCst);
            if s.open.load(Ordering::SeqCst) {
                return Poll::Ready(Box::new(|py| super::Entry::Value(py.None())));
            }
            *s.waker.lock().expect("not poisoned") = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    struct Panics(Held);

    impl Future for Panics {
        type Output = super::Output;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match Pin::new(&mut self.0).poll(cx) {
                Poll::Ready(_) => Poll::Ready(Box::new(|_| panic!("a test conversion panic"))),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl Drop for Held {
        fn drop(&mut self) {
            self.0.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[pymethods]
    impl Gate {
        #[new]
        fn new() -> Self {
            Self(Arc::default())
        }

        /// A future of the running loop, pending until `open()`.
        fn future<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            let (event_loop, fut) = super::new_future(py)?;
            let s = Arc::clone(&self.0);
            super::start(py, &event_loop, &fut, move || Held(s))?;
            Ok(fut)
        }

        /// A future that panics converting its result once the gate opens (perf2's F1): it
        /// fails with `RuntimeError`, and the bridge goes on serving others.
        fn panicking_future<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            let (event_loop, fut) = super::new_future(py)?;
            let s = Arc::clone(&self.0);
            super::start(py, &event_loop, &fut, move || Panics(Held(s)))?;
            Ok(fut)
        }

        /// Opens the gate and wakes its future (from any thread, like an engine wake).
        fn open(&self) {
            self.0.open.store(true, Ordering::SeqCst);
            if let Some(w) = self.0.waker.lock().expect("not poisoned").take() {
                w.wake();
            }
        }

        #[getter]
        fn polls(&self) -> u64 {
            self.0.polls.load(Ordering::SeqCst)
        }

        #[getter]
        fn drops(&self) -> u64 {
            self.0.drops.load(Ordering::SeqCst)
        }
    }

    /// The database's `EngineMetrics::transaction_retries` (an inspection helper, for the
    /// test that the Python transacts count their retries: cli692's N1).
    #[pyfunction]
    #[pyo3(name = "_transaction_retries")]
    pub(crate) fn transaction_retries(db: &crate::db::Database) -> PyResult<u64> {
        Ok(db.db()?.engine_metrics().transaction_retries)
    }
}

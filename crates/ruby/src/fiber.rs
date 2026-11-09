//! The waiting calls under a fiber scheduler (#1574, ICR 0126): the native half of
//! `Pigeonhole::Native::FiberBridge` (`lib/pigeonhole/fiber.rb`).
//!
//! A waiting call (a commit, say) builds the engine's future and polls it once, at the call,
//! so it submits there (D210). If it isn't ready, the calling fiber parks, and the engine's
//! waker, on a shard or group-sync thread, may not take the GVL or call Ruby (D196). So the
//! waker only queues the op's id and writes one byte to the thread's pipe, at most once per
//! drain (`armed`). One of the parked fibers holds the drainer role: it waits on the pipe's
//! read end through the scheduler (`IO#wait_readable`), then [`FiberBridge::drain`] reads the
//! pipe empty, polls every woken op in one call without the GVL, and returns the ids of those
//! now done, whose fibers it resumes. All the scheduler calls happen on the fibers' own
//! thread, so they can't race each other (§3 of the design note on #1574).
//!
//! Unix only: on Windows the waiting calls block as before (ICR 0126).

use std::collections::HashMap;
use std::future::Future;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use magnus::{Error, Ruby, Value};

use crate::convert::{arg_error, lock};
use crate::fork::ForkSafe;
use crate::gvl::without_gvl;

/// What a done op gives the fiber: converted to Ruby with the GVL held, after the resume.
pub(crate) type Out = Box<dyn FnOnce(&Ruby) -> Result<Value, Error> + Send>;
/// An op's future.
pub(crate) type Fut = Pin<Box<dyn Future<Output = Out> + Send>>;

/// One thread's bridge state, shared with the engine's wakers.
struct Shared {
    /// Ids woken since the last drain.
    ready: Mutex<Vec<u64>>,
    /// Set by the first wake after a drain, which writes the byte. The drain reads the pipe
    /// empty, then clears it, then takes `ready`: a wake before the clear is in `ready`, and
    /// one after it writes a byte again. (Cleared before the read instead, a wake between
    /// the two had its byte read and later wakes wrote none: a lost wake.)
    armed: AtomicBool,
    /// The pipe, both ends non-blocking (cli692's R4). Both live here, so the write end never
    /// outlives the read end.
    reader: std::io::PipeReader,
    writer: std::io::PipeWriter,
    /// The ops not yet done, by id: the drain polls these.
    ops: Mutex<HashMap<u64, Arc<OpInner>>>,
    next: AtomicU64,
}

/// An op's future, then its result.
enum OpState {
    Pending(Fut),
    Done(Out),
    /// Taken by the fiber, or dropped by a cancel.
    Gone,
}

struct OpInner {
    id: u64,
    state: Mutex<OpState>,
}

/// The waker the engine calls: queue the id, and write a byte unless one is already
/// waiting to be drained. Never blocks (the pipe is non-blocking and holds at most two
/// bytes) and never takes the GVL.
struct OpWaker {
    shared: Arc<Shared>,
    id: u64,
}

impl Wake for OpWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        lock(&self.shared.ready).push(self.id);
        if !self.shared.armed.swap(true, Ordering::SeqCst) {
            // A full pipe (impossible: `armed` bounds it) or a closed one: either way the
            // drain that matters will still run.
            let _ = (&self.shared.writer).write(&[1]);
        }
    }
}

impl OpInner {
    /// Polls the future once (if still pending): `true` once it's done. Run without the GVL.
    fn poll(&self, shared: &Arc<Shared>) -> bool {
        let mut st = lock(&self.state);
        let OpState::Pending(fut) = &mut *st else {
            return true;
        };
        let waker = Waker::from(Arc::new(OpWaker {
            shared: Arc::clone(shared),
            id: self.id,
        }));
        match fut.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(out) => {
                *st = OpState::Done(out);
                true
            }
            Poll::Pending => false,
        }
    }
}

/// `Pigeonhole::FiberBridge`'s native state: one per Ruby thread and process.
#[magnus::wrap(class = "Pigeonhole::Native::FiberBridgeCore", free_immediately, size)]
pub(crate) struct FiberBridge(ForkSafe<Arc<Shared>>);

/// One waiting call: its future until it's done, then its result.
#[magnus::wrap(class = "Pigeonhole::Native::FiberOp", free_immediately, size)]
pub(crate) struct FiberOp(ForkSafe<(Arc<Shared>, Arc<OpInner>)>);

fn io_error(ruby: &Ruby, what: &str, e: &std::io::Error) -> Error {
    crate::error::uncoded(
        ruby,
        "Error",
        &format!("the fiber bridge's pipe: {what}: {e}"),
    )
}

impl FiberBridge {
    /// `Pigeonhole.native_fiber_bridge`: a new bridge, with its pipe.
    pub(crate) fn new(ruby: &Ruby) -> Result<Self, Error> {
        use nix::fcntl::{FcntlArg, OFlag, fcntl};
        let (reader, writer) = std::io::pipe().map_err(|e| io_error(ruby, "pipe", &e))?;
        // `std::io::pipe` makes both ends close-on-exec but blocking.
        for fd in [reader.as_raw_fd(), writer.as_raw_fd()] {
            let flags = fcntl(fd, FcntlArg::F_GETFL)
                .map_err(|e| io_error(ruby, "F_GETFL", &std::io::Error::from(e)))?;
            let flags = OFlag::from_bits_truncate(flags) | OFlag::O_NONBLOCK;
            fcntl(fd, FcntlArg::F_SETFL(flags))
                .map_err(|e| io_error(ruby, "F_SETFL", &std::io::Error::from(e)))?;
        }
        Ok(Self(ForkSafe::new(Arc::new(Shared {
            ready: Mutex::new(Vec::new()),
            armed: AtomicBool::new(false),
            reader,
            writer,
            ops: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
        }))))
    }

    /// `native_fd`: the read end, for `IO.for_fd(fd, autoclose: false)`.
    pub(crate) fn fd(ruby: &Ruby, rb_self: &Self) -> Result<i32, Error> {
        Ok(rb_self.0.get(ruby)?.reader.as_raw_fd())
    }

    /// `native_drain`: reads the pipe empty, polls every woken op in one call without the
    /// GVL, and returns the ids of those now done. The order is read, clear `armed`, take
    /// `ready`, so no wake is lost (see `armed`); a wake after the clear makes the next wait
    /// return at once, at worst for nothing.
    pub(crate) fn drain(ruby: &Ruby, rb_self: &Self) -> Result<Vec<u64>, Error> {
        let shared = Arc::clone(rb_self.0.get(ruby)?);
        without_gvl(ruby, move || {
            let mut buf = [0u8; 64];
            // Non-blocking: `WouldBlock` (or anything else, `EINTR` included) ends it. A byte
            // left behind only makes the next wait return at once, for nothing (cli692's N2).
            while matches!((&shared.reader).read(&mut buf), Ok(n) if n > 0) {}
            shared.armed.store(false, Ordering::SeqCst);
            let mut woken = std::mem::take(&mut *lock(&shared.ready));
            woken.sort_unstable();
            woken.dedup();
            let mut done = Vec::new();
            for id in woken {
                let op = lock(&shared.ops).get(&id).cloned();
                if let Some(op) = op
                    && op.poll(&shared)
                {
                    lock(&shared.ops).remove(&id);
                    done.push(id);
                }
            }
            done
        })
    }

    /// The number of ops not yet done: the Ruby half's drainer exits when its own op is done,
    /// and tests check nothing is left behind.
    pub(crate) fn pending(ruby: &Ruby, rb_self: &Self) -> Result<usize, Error> {
        Ok(lock(&rb_self.0.get(ruby)?.ops).len())
    }

    /// Starts an op on this bridge: builds its future with `make` and polls it once, both
    /// without the GVL, so the call submits here (D210) exactly as the blocking path does.
    pub(crate) fn start(
        ruby: &Ruby,
        bridge: &Self,
        make: impl FnOnce() -> Fut + Send,
    ) -> Result<FiberOp, Error> {
        let shared = Arc::clone(bridge.0.get(ruby)?);
        let s = Arc::clone(&shared);
        let op = without_gvl(ruby, move || {
            let fut = make();
            let id = s.next.fetch_add(1, Ordering::Relaxed);
            let op = Arc::new(OpInner {
                id,
                state: Mutex::new(OpState::Pending(fut)),
            });
            // Registered before the first poll, so a wake during it finds the op.
            lock(&s.ops).insert(id, Arc::clone(&op));
            if op.poll(&s) {
                lock(&s.ops).remove(&id);
            }
            op
        })?;
        Ok(FiberOp(ForkSafe::new((shared, op))))
    }
}

impl FiberOp {
    /// `native_id`.
    pub(crate) fn id(ruby: &Ruby, rb_self: &Self) -> Result<u64, Error> {
        Ok(rb_self.0.get(ruby)?.1.id)
    }

    /// `native_done?`: whether its result is waiting to be taken.
    pub(crate) fn is_done(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        Ok(matches!(
            *lock(&rb_self.0.get(ruby)?.1.state),
            OpState::Done(_)
        ))
    }

    /// `native_take`: the result, converted (or raised). Once.
    pub(crate) fn take(ruby: &Ruby, rb_self: &Self) -> Result<Value, Error> {
        let (_, op) = rb_self.0.get(ruby)?;
        let out = match std::mem::replace(&mut *lock(&op.state), OpState::Gone) {
            OpState::Done(out) => out,
            other => {
                *lock(&op.state) = other;
                return Err(arg_error(ruby, "the operation isn't done"));
            }
        };
        out(ruby)
    }

    /// `native_cancel`: drops the op now, without the GVL, and forgets it: a pending future
    /// (a commit still waiting for room writes nothing; one submitted may still land, D210),
    /// or a done result its fiber won't take (a stream's item then stays in its slot for the
    /// next caller). Nothing once taken.
    pub(crate) fn cancel(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        let (shared, op) = rb_self.0.get(ruby)?;
        let (shared, op) = (Arc::clone(shared), Arc::clone(op));
        without_gvl(ruby, move || {
            lock(&shared.ops).remove(&op.id);
            let gone = std::mem::replace(&mut *lock(&op.state), OpState::Gone);
            drop(gone);
        })
    }
}

/// An op that's ready at its first poll (an argument the call refuses, say).
pub(crate) fn ready(out: Out) -> Fut {
    Box::pin(std::future::ready(out))
}

/// `fut`, its output converted by `conv` once the fiber has the GVL back.
pub(crate) fn map<F, T>(
    fut: F,
    conv: impl FnOnce(&Ruby, T) -> Result<Value, Error> + Send + 'static,
) -> Fut
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    Box::pin(async move {
        let t = fut.await;
        Box::new(move |ruby: &Ruby| conv(ruby, t)) as Out
    })
}

/// A hold seam for the bridge's cancellation tests (feature `test-hooks`, as #1547's Python
/// `_Gate`): an op whose future stays pending until `open`, counting its polls and drops,
/// driven by the same `start`, waker and drain as every waiting call. A poll once open
/// stands for the submission of a commit that was waiting for room.
#[cfg(feature = "test-hooks")]
pub(crate) mod hooks {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    use magnus::{Error, IntoValue, Ruby};

    use super::{FiberBridge, FiberOp, Out};
    use crate::convert::lock;

    #[derive(Default)]
    struct State {
        open: AtomicBool,
        polls: AtomicU64,
        submits: AtomicU64,
        drops: AtomicU64,
        waker: Mutex<Option<Waker>>,
    }

    /// `Pigeonhole::Native::Gate`.
    #[magnus::wrap(class = "Pigeonhole::Native::Gate", free_immediately, size)]
    pub(crate) struct Gate(Arc<State>);

    struct Held(Arc<State>);

    impl Future for Held {
        type Output = Out;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Out> {
            let s = &self.0;
            s.polls.fetch_add(1, Ordering::SeqCst);
            if s.open.load(Ordering::SeqCst) {
                s.submits.fetch_add(1, Ordering::SeqCst);
                return Poll::Ready(Box::new(|ruby: &Ruby| {
                    Ok(ruby.qnil().into_value_with(ruby))
                }));
            }
            *lock(&s.waker) = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    impl Drop for Held {
        fn drop(&mut self) {
            self.0.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Gate {
        /// `Gate.new`.
        pub(crate) fn new() -> Self {
            Self(Arc::default())
        }

        /// `op(bridge)`: an op on `bridge`, pending until `open`.
        pub(crate) fn op(
            ruby: &Ruby,
            rb_self: &Self,
            bridge: &FiberBridge,
        ) -> Result<FiberOp, Error> {
            let s = Arc::clone(&rb_self.0);
            FiberBridge::start(ruby, bridge, move || Box::pin(Held(s)))
        }

        /// `open`: opens the gate and wakes its op, as an engine thread would.
        pub(crate) fn open(&self) {
            self.0.open.store(true, Ordering::SeqCst);
            if let Some(w) = lock(&self.0.waker).take() {
                w.wake();
            }
        }

        /// `[polls, submits, drops]`.
        pub(crate) fn counts(&self) -> (u64, u64, u64) {
            let s = &self.0;
            (
                s.polls.load(Ordering::SeqCst),
                s.submits.load(Ordering::SeqCst),
                s.drops.load(Ordering::SeqCst),
            )
        }
    }
}

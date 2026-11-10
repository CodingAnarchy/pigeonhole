//! The memory store under a fiber scheduler (#1574 PR 3, ICR 0126 amendment 2): the
//! natives `lib/pigeonhole/memory.rb` uses when `Native::FiberBridge.current` is set.
//!
//! - **Writes** (`remember`, `delete`, `link`, `supersede`, `mark_used`) run as the crate's
//!   own do, step by step (ICR 0123): the store's `*_in` form in a `Database#transact`, whose
//!   commit yields through the bridge, then `settle` on the offload pool. `remember`'s
//!   near-dedup lookup, a read outside the transaction, runs on the pool before it.
//! - **`recall`** runs on the offload pool: measured, it keeps about 3.6× the throughput of
//!   `Recall::run_async` polled on the scheduler's thread (ICR 0126 amendment 3).
//! - **`ConsolidateLoop#run_once`** waits for new memories with `ConsolidateLoop::poll_wait`
//!   through the bridge, then runs its passes on the step's worker, waiting for the groups
//!   through the bridge too, so a consolidator block that itself yields (an HTTP call under
//!   Async) doesn't stall the thread.
//!
//! Unix only, as the bridge is.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use magnus::{Error, IntoValue, RArray, Ruby, Value, prelude::*};
use pigeonhole_memory as mem;

use super::{
    Groups, LoopSlot, LoopState, OrMem, SLICE, Waiter, build, call, hits, id_arg, ids_arg, in_txn,
    link_type, recall_args, remembered,
};
use crate::classes::{ConsolidateLoop, ReadStore, Store, Transaction};
use crate::convert::lock;
use crate::fiber::{FiberBridge, FiberOp, Fut, Out};
use crate::gvl::without_gvl;
use crate::offload;

/// `Pigeonhole::Native::PreparedMemory`: a memory `prepare_remember` checked and placed,
/// for `remember`'s attempts under a scheduler (internal).
#[magnus::wrap(class = "Pigeonhole::Native::PreparedMemory", free_immediately, size)]
pub(crate) struct PreparedMemory(Arc<mem::PreparedMemory>);

/// `out` as a fiber op's result: its value converted by `conv`, or its error raised.
fn out<T: Send + 'static>(
    r: mem::Result<T>,
    conv: impl FnOnce(&Ruby, T) -> Result<Value, Error> + Send + 'static,
) -> Out {
    Box::new(move |ruby: &Ruby| conv(ruby, r.or_mem(ruby)?))
}

/// An offload job's outcome as a fiber op's result, as [`out`]: a panic in the job raises
/// `Pigeonhole::Error` in the fiber, as a panic on a blocking path does (cli692's R1 on
/// #1614).
fn joined<T: Send + 'static>(
    r: std::thread::Result<mem::Result<T>>,
    conv: impl FnOnce(&Ruby, T) -> Result<Value, Error> + Send + 'static,
) -> Out {
    match r {
        Ok(r) => out(r, conv),
        Err(panic) => {
            let why = panic
                .downcast_ref::<&str>()
                .map(|s| (*s).to_owned())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            drop(panic);
            Box::new(move |ruby: &Ruby| {
                Err(crate::error::uncoded(
                    ruby,
                    "Error",
                    &format!("a pigeonhole-ruby-offload job panicked: {why}"),
                ))
            })
        }
    }
}

impl Store {
    /// `native_prepare_remember(ns, text, embedding, kind, importance, source, session,
    /// links, pinned)`: `remember`'s part before its transaction, here (no near dedup to
    /// look up).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_remember(
        ruby: &Ruby,
        rb_self: &Self,
        ns: String,
        text_: String,
        emb: Option<RArray>,
        kind_: Option<Value>,
        importance: Option<f64>,
        source: Option<String>,
        session: Option<String>,
        links: Option<RArray>,
        pinned: bool,
    ) -> Result<PreparedMemory, Error> {
        let m = build(
            ruby, text_, emb, kind_, importance, source, session, links, pinned,
        )?;
        let s = rb_self.handle(ruby)?;
        let p = call(ruby, move || s.prepare_remember(&ns, m))?;
        Ok(PreparedMemory(Arc::new(p)))
    }

    /// `native_prepare_remember_op(bridge, ns, text, ...)`: the same on the offload pool,
    /// for a store with near dedup (its lookup is a vector search).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_remember_op(
        ruby: &Ruby,
        rb_self: &Self,
        bridge: &FiberBridge,
        ns: String,
        text_: String,
        emb: Option<RArray>,
        kind_: Option<Value>,
        importance: Option<f64>,
        source: Option<String>,
        session: Option<String>,
        links: Option<RArray>,
        pinned: bool,
    ) -> Result<FiberOp, Error> {
        let m = build(
            ruby, text_, emb, kind_, importance, source, session, links, pinned,
        )?;
        let s = rb_self.handle(ruby)?;
        FiberBridge::start(ruby, bridge, move || {
            let job = offload::job(move || s.prepare_remember(&ns, m));
            Box::pin(async move {
                joined(job.await, |ruby, p| {
                    Ok(PreparedMemory(Arc::new(p)).into_value_with(ruby))
                })
            }) as Fut
        })
    }

    /// `native_remember_prepared_in(txn, prepared)`: one attempt of `remember`'s
    /// transaction.
    pub(crate) fn remember_prepared_in(
        ruby: &Ruby,
        rb_self: &Self,
        txn: &Transaction,
        prepared: &PreparedMemory,
    ) -> Result<RArray, Error> {
        let s = rb_self.handle(ruby)?;
        let p = Arc::clone(&prepared.0);
        let r = in_txn(ruby, txn, move |t| s.remember_prepared_in(t, &p))?;
        remembered(ruby, r)
    }

    /// `native_link_in(txn, from, to, link_type, weight)`.
    pub(crate) fn link_in(
        ruby: &Ruby,
        rb_self: &Self,
        txn: &Transaction,
        from: Value,
        to: Value,
        ty: Value,
        weight: f32,
    ) -> Result<(), Error> {
        let (a, b, ty) = (id_arg(ruby, from)?, id_arg(ruby, to)?, link_type(ruby, ty)?);
        let s = rb_self.handle(ruby)?;
        in_txn(ruby, txn, move |t| s.link_in(t, &a, &b, ty, weight))
    }

    /// `native_supersede_in(txn, old, new)`.
    pub(crate) fn supersede_in(
        ruby: &Ruby,
        rb_self: &Self,
        txn: &Transaction,
        old: Value,
        new: Value,
    ) -> Result<(), Error> {
        let (a, b) = (id_arg(ruby, old)?, id_arg(ruby, new)?);
        let s = rb_self.handle(ruby)?;
        in_txn(ruby, txn, move |t| s.supersede_in(t, &a, &b))
    }

    /// `native_mark_used_in(txn, ids)`: `[marked, [missing id, ...]]`.
    pub(crate) fn mark_used_in(
        ruby: &Ruby,
        rb_self: &Self,
        txn: &Transaction,
        ids: RArray,
    ) -> Result<RArray, Error> {
        let ids = ids_arg(ruby, ids)?;
        let s = rb_self.handle(ruby)?;
        let m = in_txn(ruby, txn, move |t| s.mark_used_in(t, &ids))?;
        super::marked(ruby, m)
    }

    /// `native_near_dedup_on`: whether `remember`'s near-dedup lookup runs.
    pub(crate) fn near_dedup_on(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        let s = rb_self.handle(ruby)?;
        Ok(call(ruby, move || s.near_dedup())?.is_some())
    }

    /// `native_has_ivf`: whether the store has an IVF index to settle.
    pub(crate) fn has_ivf(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        Ok(rb_self.handle(ruby)?.ivf().is_some())
    }

    /// `native_settle_op(bridge)`: `settle` on the offload pool (with inline rebalance it
    /// may commit splits). Dropped before it starts, it's skipped: the parked writes wait for
    /// the next write's settle, which is harmless for recall (see `Store::settle`).
    pub(crate) fn settle_op(
        ruby: &Ruby,
        rb_self: &Self,
        bridge: &FiberBridge,
    ) -> Result<FiberOp, Error> {
        let s = rb_self.handle(ruby)?;
        FiberBridge::start(ruby, bridge, move || {
            let job = offload::job(move || s.settle());
            Box::pin(
                async move { joined(job.await.map(Ok), |ruby, ()| Ok(ruby.qnil().as_value())) },
            ) as Fut
        })
    }

    /// `native_recall_op(bridge, args)`: `native_recall` on the offload pool.
    pub(crate) fn recall_op(
        ruby: &Ruby,
        rb_self: &Self,
        bridge: &FiberBridge,
        args: RArray,
    ) -> Result<FiberOp, Error> {
        let a = recall_args(ruby, args)?;
        let s = rb_self.handle(ruby)?;
        recall_job(ruby, bridge, move || {
            let ns: Vec<&str> = a.namespaces.iter().map(String::as_str).collect();
            let r = s.recall(&ns);
            a.run(r)
        })
    }
}

impl ReadStore {
    /// `native_recall_op(bridge, args)`: `native_recall` on the offload pool.
    pub(crate) fn recall_op(
        ruby: &Ruby,
        rb_self: &Self,
        bridge: &FiberBridge,
        args: RArray,
    ) -> Result<FiberOp, Error> {
        let a = recall_args(ruby, args)?;
        let s = rb_self.store(ruby)?;
        recall_job(ruby, bridge, move || {
            let ns: Vec<&str> = a.namespaces.iter().map(String::as_str).collect();
            let r = s.recall(&ns);
            a.run(r)
        })
    }
}

/// A recall as an op: `run` on the offload pool, its hits converted once the fiber resumes.
fn recall_job(
    ruby: &Ruby,
    bridge: &FiberBridge,
    run: impl FnOnce() -> mem::Result<(Vec<mem::RecallHit>, mem::RecallStats)> + Send + 'static,
) -> Result<FiberOp, Error> {
    FiberBridge::start(ruby, bridge, move || {
        let job = offload::job(run);
        Box::pin(async move { joined(job.await, |ruby, h| Ok(hits(ruby, h)?.as_value())) }) as Fut
    })
}

// ---- The consolidation loop -------------------------------------------------------------

/// A fiber's wait for the loop's new memories (`ConsolidateLoop::poll_wait`): it holds the
/// loop meanwhile (the slot says `Waiting`, with its waker for `close`) and puts it back
/// when it ends, however it ends, unless the loop was closed meanwhile.
struct LoopWait {
    home: Arc<Mutex<LoopSlot>>,
    state: Option<Box<LoopState>>,
}

impl Future for LoopWait {
    type Output = Out;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Out> {
        let this = &mut *self;
        let mut g = lock(&this.home);
        if matches!(*g, LoopSlot::Closed) {
            let gone = this.state.take();
            drop(g);
            drop(gone);
            return Poll::Ready(Box::new(|ruby: &Ruby| {
                Err(crate::error::spent(ruby, LoopSlot::Closed.why()))
            }));
        }
        let st = this.state.as_mut().expect("held until ready");
        // Polled with the slot's lock held, so a `close` either comes first (above) or finds
        // this poll's waker.
        match st.lp.poll_wait(cx) {
            Poll::Pending => {
                *g = LoopSlot::Waiting(Some(cx.waker().clone()));
                Poll::Pending
            }
            Poll::Ready(r) => {
                *g = LoopSlot::Idle(this.state.take().expect("held until ready"));
                Poll::Ready(out(r, |ruby, n| Ok(n.into_value_with(ruby))))
            }
        }
    }
}

impl Drop for LoopWait {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        // Cancel-safe: a commit is noted in the poll that takes it (W6 on #1563).
        let mut g = lock(&self.home);
        if matches!(*g, LoopSlot::Closed) {
            drop(g);
            drop(state);
        } else {
            *g = LoopSlot::Idle(state);
        }
    }
}

impl ConsolidateLoop {
    /// `native_wait_op(bridge)`: waits for the next commit with memory events in the loop's
    /// namespaces, and records it; the op's result is the number of events. 0 at once for
    /// a loop that hasn't stepped yet (its first step starts it).
    pub(crate) fn wait_op(
        ruby: &Ruby,
        rb_self: &Self,
        bridge: &FiberBridge,
    ) -> Result<FiberOp, Error> {
        let home = Arc::clone(rb_self.0.get(ruby)?);
        let state = {
            let mut g = lock(&home);
            let s = g.take(ruby)?;
            *g = LoopSlot::Waiting(None);
            Box::new(s)
        };
        FiberBridge::start(ruby, bridge, move || {
            Box::pin(LoopWait {
                home,
                state: Some(state),
            }) as Fut
        })
    }

    /// `native_run_once_fiber(bridge, waited, block)`: the step after a fiber's waits that
    /// read `waited` events: it reads what's already there, then runs the due passes on the
    /// worker, waiting for their groups through `bridge` (the Ruby `FiberBridge`).
    pub(crate) fn run_once_fiber(
        ruby: &Ruby,
        rb_self: &Self,
        bridge: Value,
        waited: usize,
        block: magnus::block::Proc,
    ) -> Result<RArray, Error> {
        let core: Value = bridge.funcall("native", ())?;
        let core = <&FiberBridge>::try_convert(core)?;
        Self::step(
            ruby,
            rb_self,
            std::time::Duration::ZERO,
            waited,
            block,
            Waiter::Fiber(bridge, core),
        )
    }
}

/// Ready once a group or the close is there to take: taken by `drive` itself.
struct GroupsReady(Arc<Groups>);

impl Future for GroupsReady {
    type Output = Out;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Out> {
        if self.0.poll_ready(cx) {
            Poll::Ready(Box::new(|ruby: &Ruby| Ok(ruby.qnil().as_value())))
        } else {
            Poll::Pending
        }
    }
}

/// Parks the calling fiber on `bridge` until `groups` has a group or is closed. Its error
/// is whatever raised into the fiber meanwhile. With no bridge to park on (the op can't
/// start), it waits one slice with the GVL released instead, so `drive` still drains.
pub(super) fn await_groups(
    ruby: &Ruby,
    bridge: Value,
    core: &FiberBridge,
    groups: &Arc<Groups>,
) -> Result<(), Error> {
    let g = Arc::clone(groups);
    let op = match FiberBridge::start(ruby, core, move || Box::pin(GroupsReady(g)) as Fut) {
        Ok(op) => op,
        Err(_) => {
            let g = Arc::clone(groups);
            return without_gvl(ruby, move || g.wait_ready(SLICE));
        }
    };
    bridge.funcall::<_, _, Value>("await", (op,)).map(|_| ())
}

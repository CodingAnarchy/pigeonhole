//! Backpressure for async commits (D210): a per-shard bound on the bytes of async commits
//! submitted and not yet resolved. An async commit over it waits, before it submits, until
//! the shard's earlier commits resolve; waiters are admitted in arrival order. Blocking
//! commits are neither charged nor delayed (their threads bound them).
//!
//! **Fast path (D236).** A commit is charged to its shard's own count with one
//! compare-and-add when the shard has room and no one waits there. Each shard's count sits on
//! cache lines of its own, and a charge returns to that shard alone: commits to different
//! shards share no written memory. (D210's engine-wide count was written by every async
//! commit on every shard; at 16 shards it was most of the cross-core traffic.)

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::write::{PendingCheck, PendingCommit, WriteBatch};

/// The engine's admission state: one queue and counter per shard.
#[derive(Debug)]
pub(crate) struct Admission {
    shards: Vec<Arc<ShardAdmission>>,
    /// The engine is closing: every waiter is admitted, and its submission fails `Closed`.
    closed: AtomicBool,
}

/// One shard's admission. Aligned so that no two shards' counters share a cache line (nor
/// their reference counts, in the `Arc` header before it): a commit charges and returns
/// only its own shard's.
#[derive(Debug)]
#[repr(align(128))]
struct ShardAdmission {
    /// Bytes the shard may hold of async commits submitted and not yet resolved.
    bound: u64,
    /// Charged bytes of async commits submitted and not yet resolved.
    inflight: AtomicU64,
    /// Waiters queued (read without the lock: a commit with room and no one ahead of it
    /// takes the fast path).
    waiting: AtomicUsize,
    queue: Mutex<Queue>,
}

#[derive(Debug, Default)]
struct Queue {
    next_id: u64,
    /// In arrival order, so sorted by id: an id and the waker of the task that last polled
    /// it.
    waiters: VecDeque<(u64, Option<Waker>)>,
}

impl Queue {
    /// The waiter `id`'s position (ids rise in arrival order).
    fn find(&self, id: u64) -> Option<usize> {
        self.waiters.binary_search_by_key(&id, |(w, _)| *w).ok()
    }
}

impl Admission {
    pub(crate) fn new(shards: usize, bound: u64) -> Self {
        let bound = bound.max(1);
        Self {
            shards: (0..shards.max(1))
                .map(|_| {
                    Arc::new(ShardAdmission {
                        bound,
                        inflight: AtomicU64::new(0),
                        waiting: AtomicUsize::new(0),
                        queue: Mutex::new(Queue::default()),
                    })
                })
                .collect(),
            closed: AtomicBool::new(false),
        }
    }

    /// Admits every waiter (they then fail to submit with `Closed`).
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        for s in &self.shards {
            let q = s.queue.lock().unwrap_or_else(PoisonError::into_inner);
            for (_, w) in &q.waiters {
                if let Some(w) = w {
                    w.wake_by_ref();
                }
            }
        }
    }

    /// The fast path: charges `bytes` to `shard` if no one waits there and it has room.
    #[inline]
    pub(crate) fn try_fast(&self, shard: usize, bytes: u64) -> Option<Charge> {
        let s = &self.shards[shard];
        if s.waiting.load(Ordering::Acquire) != 0 || self.closed.load(Ordering::Relaxed) {
            return None;
        }
        s.try_charge(bytes)
            .then(|| Charge(Some((Arc::clone(s), bytes))))
    }

    /// Bytes charged to `shard` and not yet returned (tests).
    pub(crate) fn inflight(&self, shard: usize) -> u64 {
        self.shards[shard].inflight.load(Ordering::Acquire)
    }

    /// Whether more than one shard can be charged (else every commit is shard 0's).
    pub(crate) fn sharded(&self) -> bool {
        self.shards.len() > 1
    }
}

impl ShardAdmission {
    /// Charges `bytes` if they fit: the shard holds less than the bound with them, or holds
    /// nothing (a commit larger than the bound goes alone).
    fn try_charge(&self, bytes: u64) -> bool {
        let mut cur = self.inflight.load(Ordering::Acquire);
        loop {
            if cur != 0 && cur + bytes > self.bound {
                return false;
            }
            match self.inflight.compare_exchange_weak(
                cur,
                cur + bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(now) => cur = now,
            }
        }
    }

    /// Returns a charge of `bytes` and, if anyone waits, wakes the first waiter.
    fn release(&self, bytes: u64) {
        self.inflight.fetch_sub(bytes, Ordering::AcqRel);
        if self.waiting.load(Ordering::Acquire) != 0 {
            let q = self.queue.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some((_, Some(w))) = q.waiters.front() {
                w.wake_by_ref();
            }
        }
    }
}

/// A resolved commit returns its charge of `arg` bytes to its shard.
impl pigeonhole_runtime::Settle for ShardAdmission {
    fn settled(&self, arg: u64) {
        self.release(arg);
    }
}

/// A fast-path charge on its way into a commit's completion: returned when the completion
/// resolves, or here when the submission fails before it has one.
pub(crate) struct Charge(Option<(Arc<ShardAdmission>, u64)>);

impl Charge {
    /// Nothing charged (a blocking commit, or no admission).
    pub(crate) fn none() -> Self {
        Self(None)
    }

    /// Hands the charge to a submitted commit's completion, which returns it when it
    /// resolves.
    pub(crate) fn attach<T: Send>(&mut self, waiter: &pigeonhole_runtime::Waiter<T>) {
        if let Some((s, bytes)) = self.0.take() {
            waiter.on_settled(s, bytes);
        }
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        if let Some((s, bytes)) = self.0.take() {
            s.release(bytes);
        }
    }
}

/// An async commit's place in its shard's admission: charged at once when it has room and
/// no one waits ahead of it, otherwise queued until [`Admit::poll_admit`] finds it first
/// with room. Dropped while queued, it leaves the queue; dropped charged and unsubmitted,
/// it returns the charge.
#[derive(Debug)]
pub struct Admit {
    admission: Option<Arc<Admission>>,
    shard: usize,
    bytes: u64,
    state: AdmitState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmitState {
    Queued(u64),
    Charged,
    /// Handed to a submitted commit, or not charged at all (closed, or no admission).
    Done,
}

impl Admit {
    pub(crate) fn new(admission: Option<&Arc<Admission>>, shard: usize, bytes: u64) -> Self {
        let Some(a) = admission else {
            return Self::unbounded();
        };
        let shard = shard.min(a.shards.len() - 1);
        let s = &a.shards[shard];
        let state = if a.closed.load(Ordering::Acquire) {
            AdmitState::Done
        } else if s.waiting.load(Ordering::Acquire) == 0 && s.try_charge(bytes) {
            AdmitState::Charged
        } else {
            let mut q = s.queue.lock().unwrap_or_else(PoisonError::into_inner);
            let id = q.next_id;
            q.next_id += 1;
            q.waiters.push_back((id, None));
            s.waiting.fetch_add(1, Ordering::AcqRel);
            AdmitState::Queued(id)
        };
        Self {
            admission: Some(Arc::clone(a)),
            shard,
            bytes,
            state,
        }
    }

    fn unbounded() -> Self {
        Self {
            admission: None,
            shard: 0,
            bytes: 0,
            state: AdmitState::Done,
        }
    }

    /// The shard this admission charges.
    pub(crate) fn shard(&self) -> usize {
        self.shard
    }

    /// Whether the commit may submit now (charged, or nothing to charge).
    pub fn is_admitted(&self) -> bool {
        !matches!(self.state, AdmitState::Queued(_))
    }

    /// Ready once the commit may submit: it is first in its shard's queue and fits, or the
    /// engine is closing. Wakes the next waiter when admitted, in case it fits too.
    pub fn poll_admit(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let AdmitState::Queued(id) = self.state else {
            return Poll::Ready(());
        };
        let a = self
            .admission
            .as_ref()
            .expect("a queued admit has its admission");
        let s = &a.shards[self.shard];
        let mut q = s.queue.lock().unwrap_or_else(PoisonError::into_inner);
        let closed = a.closed.load(Ordering::Acquire);
        let first = q.waiters.front().is_some_and(|(f, _)| *f == id);
        if closed || (first && s.try_charge(self.bytes)) {
            if let Some(i) = q.find(id) {
                q.waiters.remove(i);
            }
            s.waiting.fetch_sub(1, Ordering::AcqRel);
            if let Some((_, Some(w))) = q.waiters.front() {
                w.wake_by_ref();
            }
            self.state = if closed {
                AdmitState::Done
            } else {
                AdmitState::Charged
            };
            return Poll::Ready(());
        }
        if let Some((_, w)) = q.find(id).and_then(|i| q.waiters.get_mut(i)) {
            match w {
                Some(w) if w.will_wake(cx.waker()) => {}
                _ => *w = Some(cx.waker().clone()),
            }
        }
        Poll::Pending
    }

    /// The charge, if any, to return once the commit resolves: the shard's admission and
    /// the bytes, for its [`Settle`](pigeonhole_runtime::Settle).
    fn take_charge(&mut self) -> Option<(Arc<ShardAdmission>, u64)> {
        if self.state != AdmitState::Charged {
            return None;
        }
        self.state = AdmitState::Done;
        let a = self.admission.take()?;
        Some((Arc::clone(&a.shards[self.shard]), self.bytes))
    }

    /// Hands the charge to a submitted commit: returned when the shard resolves it.
    pub fn attach(mut self, pending: &PendingCommit) {
        if let Some((a, arg)) = self.take_charge() {
            pending.sub.waiter.on_settled(a, arg);
        }
    }

    /// [`attach`](Self::attach) for a conditional commit.
    pub fn attach_check(mut self, pending: &PendingCheck) {
        if let Some((a, arg)) = self.take_charge() {
            pending.waiter.on_settled(a, arg);
        }
    }
}

/// Nothing to charge: admitted, and returns nothing.
impl Default for Admit {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl Drop for Admit {
    fn drop(&mut self) {
        let Some(a) = self.admission.take() else {
            return;
        };
        match self.state {
            AdmitState::Charged => a.shards[self.shard].release(self.bytes),
            AdmitState::Queued(id) => {
                let s = &a.shards[self.shard];
                let mut q = s.queue.lock().unwrap_or_else(PoisonError::into_inner);
                let was_first = q.waiters.front().is_some_and(|(f, _)| *f == id);
                if let Some(i) = q.find(id) {
                    q.waiters.remove(i);
                }
                s.waiting.fetch_sub(1, Ordering::AcqRel);
                if was_first && let Some((_, Some(w))) = q.waiters.front() {
                    w.wake_by_ref();
                }
            }
            AdmitState::Done => {}
        }
    }
}

/// The bytes an async commit is charged: its encoded batch and row deletes.
pub(crate) fn batch_bytes(batch: &WriteBatch) -> u64 {
    let rows: usize = batch.row_deletes.iter().map(|d| d.row.len() + 16).sum();
    (batch.builder.batch().as_bytes().len() + rows) as u64
}

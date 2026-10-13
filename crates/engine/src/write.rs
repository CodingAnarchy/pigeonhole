use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use pigeonhole_compaction::ValuePredicate;
use pigeonhole_format::Kind;
use pigeonhole_format::value::{ValueRef, ValueTag, encode_value};
use pigeonhole_format::wal::{BatchBuilder, BatchRef};
use pigeonhole_format::{Durability, FamilyId, TableId, Timestamp};
use pigeonhole_runtime::{Notifier, SlotCache, Waiter, completion, completion_from};

use crate::engine::Inner;
use crate::{CellData, CommitInfo, Error, Snapshot};

/// A whole-row delete recorded in a batch, expanded to one family marker per family of the
/// table when the batch is submitted (decision D10; the batch does not know the catalog).
#[derive(Debug, Clone)]
pub(crate) struct RowDelete {
    pub table: TableId,
    pub row: Vec<u8>,
    pub ts: Option<Timestamp>,
}

/// The timestamp of a counter family's counter (decision D179): a put or merge operand
/// without an explicit timestamp in a `FamilyKind::Counter` family is written here, so all
/// of a counter's increments share one `(column, timestamp)` and combine.
pub const COUNTER_TS: Timestamp = 0;

/// A multi-row write with one durability point. Mutations are encoded on insert in the WAL
/// batch encoding (`pigeonhole_format::wal::BatchBuilder`), so commit never re-encodes.
/// Commit is atomic per row always; across rows it is atomic too (two-phase commit across
/// shards when rows span shards).
///
/// ```
/// use pigeonhole_engine::{FamilyId, TableId, ValueRef, WriteBatch};
///
/// let mut wb = WriteBatch::new();
/// wb.put(TableId(1), FamilyId(1), b"row", b"q", None, ValueRef::Bytes(b"v")).unwrap();
/// wb.merge(TableId(1), FamilyId(2), b"row", b"hits", ValueRef::I64(1)).unwrap();
/// wb.delete_column(TableId(1), FamilyId(1), b"row", b"old", None).unwrap();
/// assert_eq!(wb.len(), 3);
/// wb.clear();
/// assert!(wb.is_empty());
/// ```
#[derive(Debug, Clone)]
pub struct WriteBatch {
    pub(crate) builder: BatchBuilder,
    pub(crate) row_deletes: Vec<RowDelete>,
    /// Scratch for value encoding (kept between calls).
    value_buf: Vec<u8>,
}

/// Bytes a new batch has room for before it grows: a few small mutations (#320).
const INITIAL_BATCH_BYTES: usize = 256;

/// The largest batch whose buffer a thread keeps for its next batch (#320): a thread that
/// once committed a large batch does not hold that much memory for good.
const RECYCLED_BATCH_MAX: usize = 64 << 10;

thread_local! {
    /// A committed batch's buffer, handed back by its shard with the reply ([`Settled`]), for
    /// this thread's next [`WriteBatch`]: a commit's buffer is then neither allocated per
    /// commit nor freed on the shard's thread (#320).
    static RECYCLED_BATCH: std::cell::Cell<Option<BatchBuilder>> =
        const { std::cell::Cell::new(None) };
}

/// Keeps `batch`'s buffer for this thread's next batch, unless it grew large.
fn recycle(batch: Option<BatchBuilder>) {
    if let Some(batch) = batch
        && batch.batch().as_bytes().len() <= RECYCLED_BATCH_MAX
    {
        RECYCLED_BATCH.with(|slot| slot.set(Some(batch)));
    }
}

/// A shard's reply to a single-shard commit: its outcome, and the batch's buffer handed back
/// to the committing side for reuse ([`recycle`]). Cross-shard and internal commits return
/// none.
#[derive(Debug)]
pub(crate) struct Settled {
    pub(crate) info: CommitInfo,
    pub(crate) batch: Option<BatchBuilder>,
}

impl From<CommitInfo> for Settled {
    fn from(info: CommitInfo) -> Self {
        Self { info, batch: None }
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        let builder = match RECYCLED_BATCH.with(std::cell::Cell::take) {
            Some(mut b) => {
                b.clear();
                b
            }
            None => BatchBuilder::with_capacity(INITIAL_BATCH_BYTES),
        };
        Self {
            builder,
            row_deletes: Vec::new(),
            value_buf: Vec::new(),
        }
    }
}

impl WriteBatch {
    /// An empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Puts a value. `ts = None` uses the commit timestamp, or [`COUNTER_TS`] in a counter
    /// family (which holds only `i64` values).
    pub fn put(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
        ts: Option<Timestamp>,
        value: ValueRef<'_>,
    ) -> crate::Result<()> {
        if matches!(value, ValueRef::Blob(_)) {
            return Err(Error::InvalidArgument(
                "a blob pointer cannot be written directly".to_owned(),
            ));
        }
        self.push_value(table, family, Kind::Put, row, qualifier, ts, value)
    }

    /// Appends a put or merge operand. A `Bytes` value is written straight into the batch
    /// (its only copy until the commit routes it, #320); the small typed ones are encoded
    /// first.
    #[allow(clippy::too_many_arguments)]
    fn push_value(
        &mut self,
        table: TableId,
        family: FamilyId,
        kind: Kind,
        row: &[u8],
        qualifier: &[u8],
        ts: Option<Timestamp>,
        value: ValueRef<'_>,
    ) -> crate::Result<()> {
        if let ValueRef::Bytes(payload) = value {
            self.builder.push_tagged(
                table,
                family,
                kind,
                row,
                qualifier,
                ts,
                ValueTag::Bytes as u8,
                payload,
            )?;
            return Ok(());
        }
        self.value_buf.clear();
        encode_value(&mut self.value_buf, value);
        self.builder
            .push(table, family, kind, row, qualifier, ts, &self.value_buf)?;
        Ok(())
    }

    /// Writes a merge operand. In a counter family (`FamilyKind::Counter`) it lands at
    /// [`COUNTER_TS`]; elsewhere at the commit timestamp.
    pub fn merge(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
        operand: ValueRef<'_>,
    ) -> crate::Result<()> {
        if matches!(operand, ValueRef::Blob(_)) {
            return Err(Error::InvalidArgument(
                "a blob pointer cannot be a merge operand".to_owned(),
            ));
        }
        self.push_value(table, family, Kind::Merge, row, qualifier, None, operand)
    }

    /// Writes a merge operand at timestamp `ts`: a bucket of a counter family (decision
    /// D179). Other families refuse it at commit with `InvalidArgument`.
    pub fn merge_at(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
        ts: Timestamp,
        operand: ValueRef<'_>,
    ) -> crate::Result<()> {
        if matches!(operand, ValueRef::Blob(_)) {
            return Err(Error::InvalidArgument(
                "a blob pointer cannot be a merge operand".to_owned(),
            ));
        }
        self.push_value(
            table,
            family,
            Kind::Merge,
            row,
            qualifier,
            Some(ts),
            operand,
        )
    }

    /// Deletes one version.
    pub fn delete_cell(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
        ts: Timestamp,
    ) -> crate::Result<()> {
        self.builder.push(
            table,
            family,
            Kind::CellDelete,
            row,
            qualifier,
            Some(ts),
            &[],
        )?;
        Ok(())
    }

    /// Deletes all versions of a column at or below `ts` (`None`: the commit timestamp).
    pub fn delete_column(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
        ts: Option<Timestamp>,
    ) -> crate::Result<()> {
        self.builder
            .push(table, family, Kind::ColumnDelete, row, qualifier, ts, &[])?;
        Ok(())
    }

    /// Deletes a family within a row at or below `ts`.
    pub fn delete_family(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        ts: Option<Timestamp>,
    ) -> crate::Result<()> {
        self.builder
            .push(table, family, Kind::FamilyDelete, row, &[], ts, &[])?;
        Ok(())
    }

    /// Deletes a whole row: one family marker per family of the table (decision D10).
    pub fn delete_row(
        &mut self,
        table: TableId,
        row: &[u8],
        ts: Option<Timestamp>,
    ) -> crate::Result<()> {
        if row.len() > pigeonhole_format::key::MAX_KEY_PART {
            return Err(Error::KeyTooLarge);
        }
        self.row_deletes.push(RowDelete {
            table,
            row: row.to_vec(),
            ts,
        });
        Ok(())
    }

    /// Mutations so far.
    pub fn len(&self) -> usize {
        self.builder.len() + self.row_deletes.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears for reuse, keeping allocations.
    pub fn clear(&mut self) {
        self.builder.clear();
        self.row_deletes.clear();
    }

    /// The encoded mutations (row deletes excluded until expanded).
    pub(crate) fn batch(&self) -> BatchRef<'_> {
        self.builder.batch()
    }
}

/// A condition for `Engine::check_and_mutate`, evaluated on the owning shard at the latest
/// seqno.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// The column has a visible version.
    Exists {
        /// Family.
        family: FamilyId,
        /// Qualifier.
        qualifier: Vec<u8>,
    },
    /// The column has no visible version.
    Absent {
        /// Family.
        family: FamilyId,
        /// Qualifier.
        qualifier: Vec<u8>,
    },
    /// The column's latest value matches.
    Value {
        /// Family.
        family: FamilyId,
        /// Qualifier.
        qualifier: Vec<u8>,
        /// Condition.
        predicate: ValuePredicate,
    },
}

/// A commit submitted to its shard(s): what the engine's own blocking commits wait on
/// ([`Submission::wait`]). [`PendingCommit`] wraps it for tickets and async commits, so the
/// blocking path carries nothing of theirs (#523).
#[derive(Debug)]
pub(crate) struct Submission {
    pub(crate) waiter: Waiter<crate::Result<Settled>>,
    /// The engine, for registering an async waker on the global watermark: reached through
    /// the commit's shard's handle, so commits to different shards share no reference count
    /// (D236).
    pub(crate) shared: Arc<crate::engine::ShardRef>,
    /// Whether `wait` may poll before it parks (D198): not for a durable commit, which
    /// waits for a sync. The window is the engine's `commit_spin_nanos`.
    pub(crate) spins: bool,
}

/// A commit submitted to its shard(s) and not yet resolved, or an async commit still
/// waiting for room to submit (D210). The basis of ticket-style commits
/// ([`PendingCommit::wait`]) and async commits (`.await`). Dropping it does not roll back:
/// a submitted commit lands or fails atomically either way; one still waiting writes
/// nothing.
#[derive(Debug)]
#[must_use = "dropping a pending commit does not cancel it, but its result is lost"]
pub struct PendingCommit {
    pub(crate) sub: Submission,
    /// Resolved by the shard; waiting for visibility (async polling).
    pub(crate) resolved: Option<CommitInfo>,
    /// An async commit still waiting for room on its shard (D210): submitted once
    /// admitted. `sub` holds a placeholder waiter meanwhile.
    pub(crate) deferred: Option<Box<Deferred<PendingCommit, crate::Result<Settled>>>>,
}

impl From<Submission> for PendingCommit {
    fn from(sub: Submission) -> Self {
        Self {
            sub,
            resolved: None,
            deferred: None,
        }
    }
}

/// A conditional commit's reply: whether the batch applied, and its commit when it did.
type CheckReply = crate::Result<(bool, Option<CommitInfo>)>;

/// An async commit still waiting for room on its shard (D210): its admission and the
/// submission to run once admitted. The waiting [`PendingCommit`] or [`PendingCheck`] holds
/// it, beside a placeholder waiter whose notifier this keeps unresolved.
pub(crate) struct Deferred<P, R> {
    admit: crate::Admit,
    submit: Box<dyn FnOnce() -> crate::Result<P> + Send + Sync>,
    placeholder: Notifier<R>,
}

impl<P, R> std::fmt::Debug for Deferred<P, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deferred")
            .field("admit", &self.admit)
            .finish_non_exhaustive()
    }
}

impl<P, R: Send> Deferred<P, R> {
    /// A deferral and the placeholder waiter its pending commit holds until it submits.
    fn new(
        admit: crate::Admit,
        submit: Box<dyn FnOnce() -> crate::Result<P> + Send + Sync>,
    ) -> (Box<Self>, Waiter<R>) {
        let (placeholder, waiter) = completion();
        (
            Box::new(Self {
                admit,
                submit,
                placeholder,
            }),
            waiter,
        )
    }

    /// Submits once admitted: the submission and its admission, whose charge goes to the
    /// submitted commit.
    fn poll_submit(
        slot: &mut Option<Box<Self>>,
        cx: &mut Context<'_>,
    ) -> Poll<(crate::Admit, crate::Result<P>)> {
        let d = slot.as_mut().expect("a deferred commit");
        if d.admit.poll_admit(cx).is_pending() {
            return Poll::Pending;
        }
        let Self {
            admit,
            submit,
            placeholder,
        } = *slot.take().expect("a deferred commit");
        let submitted = submit();
        // Dropped after the submission replaced it: nobody waits on the placeholder.
        drop(placeholder);
        Poll::Ready((admit, submitted))
    }

    /// [`poll_submit`](Self::poll_submit), parking the thread until admitted (a blocking
    /// wait on an async commit that is still waiting for room).
    fn submit_blocking(slot: &mut Option<Box<Self>>) -> (crate::Admit, crate::Result<P>) {
        struct Unpark(std::thread::Thread);
        impl std::task::Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut cx = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(r) = Self::poll_submit(slot, &mut cx) {
                return r;
            }
            std::thread::park();
        }
    }
}

thread_local! {
    /// This thread's spare completion slots for its commits (#320): a commit reuses one
    /// instead of allocating its reply slot (and, on macOS, the slot's lock). Freed when the
    /// thread exits.
    static COMMIT_SLOTS: SlotCache<crate::Result<Settled>> = const { SlotCache::new() };
}

/// A commit's completion, reusing one of this thread's spare slots when it has one (#320).
pub(crate) fn commit_completion() -> (
    Notifier<crate::Result<Settled>>,
    Waiter<crate::Result<Settled>>,
) {
    COMMIT_SLOTS
        .try_with(completion_from)
        .unwrap_or_else(|_| completion())
}

impl Drop for Submission {
    fn drop(&mut self) {
        // The shard has released the slot once it replied (or dropped the reply): it goes
        // back to the thread this commit is dropped on, reset.
        let _ = COMMIT_SLOTS.try_with(|c| self.waiter.recycle_into(c));
    }
}

impl PendingCommit {
    /// Blocks until the commit meets its durability level and is visible. The thread parks
    /// while it waits. Fails with [`Error::Closed`] if the database closed before the commit
    /// was written, and with [`Error::OutcomeUnknown`] if it closed, or a shard died (its
    /// thread panicked, or its `EngineShard` was dropped), after the commit may have reached
    /// the WAL but before it was visible: the next open decides it (D227, ICR 0038).
    ///
    /// In application-owned mode a thread that drives a shard must not block: the commit
    /// may need that shard to run (its own group, or any group holding the global
    /// watermark below it, decision D88). On such a thread this returns the result if the
    /// commit is already done and visible, and otherwise fails at once with
    /// [`Error::WouldDeadlock`]: the commit was submitted and will apply, but its outcome
    /// must be awaited from the event loop (poll this future there) or another thread.
    pub fn wait(mut self) -> crate::Result<CommitInfo> {
        if self.deferred.is_some() {
            self = self.submit_deferred_blocking()?;
        }
        self.sub.wait()
    }

    /// A blocking wait on an async commit still waiting for room: parks until admitted,
    /// then submits. Out of line: tickets never wait for room.
    #[cold]
    #[inline(never)]
    fn submit_deferred_blocking(mut self) -> crate::Result<PendingCommit> {
        let (admit, submitted) = Deferred::submit_blocking(&mut self.deferred);
        let pending = submitted?;
        admit.attach(&pending);
        Ok(pending)
    }
}

impl Submission {
    /// [`PendingCommit::wait`] for a commit the engine submitted itself (a blocking
    /// commit): never waiting for room.
    pub(crate) fn wait(mut self) -> crate::Result<CommitInfo> {
        wait_body(&self.shared, &mut self.waiter, self.spins)
    }
}

/// A blocking commit's wait on the engine's own borrowed state (D236): the caller holds
/// the engine for the whole wait, so the commit takes no reference on it. Returns the
/// completion slot to this thread's cache, as dropping a [`Submission`] does.
#[inline]
pub(crate) fn wait_blocking(
    shared: &crate::shard::Shared,
    waiter: &mut Waiter<crate::Result<Settled>>,
    spins: bool,
) -> crate::Result<CommitInfo> {
    let r = wait_body(shared, waiter, spins);
    let _ = COMMIT_SLOTS.try_with(|c| waiter.recycle_into(c));
    r
}

/// The wait of a submitted commit: its reply, then visibility (D19), spinning first for a
/// commit that may (D198).
#[inline]
fn wait_body(
    shared: &crate::shard::Shared,
    waiter: &mut Waiter<crate::Result<Settled>>,
    spins: bool,
) -> crate::Result<CommitInfo> {
    let window = if spins { shared.commit_spin_nanos } else { 0 };
    if window == 0 {
        // No spin: exactly the parking wait.
        let Settled { info, batch } = wait_reply(shared, waiter)?;
        recycle(batch);
        wait_visible(shared, info.seqno)?;
        return Ok(info);
    }
    let mut spin = SpinWait::new(window);
    let Settled { info, batch } = wait_reply_spinning(shared, waiter, &mut spin)?;
    recycle(batch);
    wait_visible_spinning(shared, info.seqno, &mut spin)?;
    Ok(info)
}

/// A blocking wait's poll before it parks (D198): up to its window, unless this thread's
/// recent polls kept finding nothing (then it skips the next 1, 2, 4, ... up to 64 polls).
/// A shard usually answers a buffered commit within a few microseconds, and a parked
/// thread's wakeup costs that again or more.
struct SpinWait {
    window: u64,
    end: Option<std::time::Instant>,
    hit: Option<bool>,
}

thread_local! {
    /// `(polls to skip, backoff exponent)` for this thread's commit waits.
    static SPIN_BACKOFF: std::cell::Cell<(u32, u32)> = const { std::cell::Cell::new((0, 0)) };
}

impl SpinWait {
    fn new(window: u64) -> Self {
        let window = if window == 0 {
            0
        } else {
            SPIN_BACKOFF.with(|b| {
                let (skip, level) = b.get();
                if skip > 0 {
                    b.set((skip - 1, level));
                    0
                } else {
                    window
                }
            })
        };
        Self {
            window,
            end: None,
            hit: None,
        }
    }

    /// Polls `ready` until it is true or the window ends. Returns whether it became true.
    fn poll(&mut self, mut ready: impl FnMut() -> bool) -> bool {
        if self.window == 0 {
            return false;
        }
        let end = *self.end.get_or_insert_with(|| {
            std::time::Instant::now() + std::time::Duration::from_nanos(self.window)
        });
        let mut polls = 0u32;
        loop {
            if ready() {
                self.hit.get_or_insert(true);
                return true;
            }
            polls = polls.wrapping_add(1);
            if polls.is_multiple_of(64) {
                if std::time::Instant::now() >= end {
                    self.hit = Some(false);
                    self.window = 0;
                    return false;
                }
                std::thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }
    }
}

impl Drop for SpinWait {
    fn drop(&mut self) {
        let Some(hit) = self.hit else { return };
        SPIN_BACKOFF.with(|b| {
            let (_, level) = b.get();
            b.set(if hit {
                (0, 0)
            } else {
                let level = (level + 1).min(6);
                (1 << (level - 1), level)
            });
        });
    }
}

/// Blocks for a shard's reply, or (on a thread that drives a shard) takes it only if it is
/// already there.
pub(crate) fn wait_reply<T: Send>(
    shared: &crate::shard::Shared,
    waiter: &mut Waiter<crate::Result<T>>,
) -> crate::Result<T> {
    if shared.drivers.current_drives() {
        let mut cx = Context::from_waker(std::task::Waker::noop());
        return match Pin::new(waiter).poll(&mut cx) {
            Poll::Ready(Some(r)) => r,
            Poll::Ready(None) => Err(dropped_reply_now(shared)),
            Poll::Pending => Err(Error::WouldDeadlock),
        };
    }
    park_for_reply(shared, waiter)
}

/// Parks until the shard's reply arrives, counting the wait in `Metrics::commit_parks` if it
/// parks at all (ICR 0027).
fn park_for_reply<T: Send>(
    shared: &crate::shard::Shared,
    waiter: &mut Waiter<crate::Result<T>>,
) -> crate::Result<T> {
    let waker = crate::waker::thread_waker();
    let mut cx = Context::from_waker(&waker);
    let mut parked = false;
    loop {
        match Pin::new(&mut *waiter).poll(&mut cx) {
            Poll::Ready(Some(r)) => return r,
            Poll::Ready(None) => return Err(dropped_reply(shared)),
            Poll::Pending => {
                if !parked {
                    parked = true;
                    shared.commit_parks.fetch_add(1, Ordering::Relaxed);
                }
                std::thread::park();
            }
        }
    }
}

/// The error for a commit whose shard dropped its reply unanswered: it stopped first. A
/// shard the close stopped drops only commits it never wrote: `Closed`. A failed shard (a
/// panic, a dropped driver) may have written the commit's record before it stopped:
/// `OutcomeUnknown`, which the next open settles (ICR 0038, ICR 0040). Any recorded failure
/// counts, even another shard's: then a commit dropped unwritten also reads `OutcomeUnknown`,
/// which is safe (reading it back finds it missing). A panicking shard records its failure as
/// its thread unwinds, after it dropped the replies in flight, so this waits briefly for that
/// record, sleeping between looks (blocking waits only; never on the commit path).
#[cold]
fn dropped_reply(shared: &crate::shard::Shared) -> Error {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
    loop {
        if shared.failure.get().is_some() {
            return Error::OutcomeUnknown;
        }
        if shared.closed.load(Ordering::Acquire) || std::time::Instant::now() >= deadline {
            return Error::Closed;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// As [`dropped_reply`] without waiting (a poll, or a thread that drives a shard): a failure
/// already recorded makes it `OutcomeUnknown`.
#[cold]
fn dropped_reply_now(shared: &crate::shard::Shared) -> Error {
    if shared.failure.get().is_some() {
        Error::OutcomeUnknown
    } else {
        Error::Closed
    }
}

/// [`wait_reply`], polling first for up to `spin`'s window.
fn wait_reply_spinning<T: Send>(
    shared: &crate::shard::Shared,
    waiter: &mut Waiter<crate::Result<T>>,
    spin: &mut SpinWait,
) -> crate::Result<T> {
    if shared.drivers.current_drives() {
        let mut cx = Context::from_waker(std::task::Waker::noop());
        return match Pin::new(waiter).poll(&mut cx) {
            Poll::Ready(Some(r)) => r,
            Poll::Ready(None) => Err(dropped_reply_now(shared)),
            Poll::Pending => Err(Error::WouldDeadlock),
        };
    }
    let mut polled = None;
    spin.poll(|| {
        let mut cx = Context::from_waker(std::task::Waker::noop());
        match Pin::new(&mut *waiter).poll(&mut cx) {
            Poll::Pending => false,
            ready => {
                polled = Some(ready);
                true
            }
        }
    });
    match polled {
        Some(Poll::Ready(Some(r))) => return r,
        Some(Poll::Ready(None)) => return Err(dropped_reply(shared)),
        _ => {}
    }
    park_for_reply(shared, waiter)
}

/// Blocks until `seqno` is visible (D19), parked on the shards' watermark publishes. Fails
/// instead on a thread that drives a shard, which may be the one holding the watermark, and
/// with `OutcomeUnknown` once visibility can no longer advance (the close finished, or a
/// shard died): the commit was acknowledged by its shard, but never became visible here.
pub(crate) fn wait_visible(
    shared: &crate::shard::Shared,
    seqno: pigeonhole_format::Seqno,
) -> crate::Result<()> {
    if shared.shm.visible_seqno() >= seqno {
        return Ok(());
    }
    if shared.drivers.current_drives() {
        return Err(Error::WouldDeadlock);
    }
    let waker = crate::waker::thread_waker();
    while !shared.wait_visible(seqno, &waker) {
        if shared.visibility_ended() {
            return Err(Error::OutcomeUnknown);
        }
        std::thread::park();
    }
    Ok(())
}

/// [`wait_visible`], polling first for what is left of `spin`'s window.
fn wait_visible_spinning(
    shared: &crate::shard::Shared,
    seqno: pigeonhole_format::Seqno,
    spin: &mut SpinWait,
) -> crate::Result<()> {
    if shared.shm.visible_seqno() >= seqno {
        return Ok(());
    }
    if shared.drivers.current_drives() {
        return Err(Error::WouldDeadlock);
    }
    if spin.poll(|| shared.shm.visible_seqno() >= seqno) {
        return Ok(());
    }
    let waker = crate::waker::thread_waker();
    while !shared.wait_visible(seqno, &waker) {
        if shared.visibility_ended() {
            return Err(Error::OutcomeUnknown);
        }
        std::thread::park();
    }
    Ok(())
}

impl PendingCommit {
    /// An async commit that waits for room on its shard (D210) and runs `submit` once
    /// admitted.
    pub(crate) fn waiting(
        shared: Arc<crate::engine::ShardRef>,
        admit: crate::Admit,
        submit: Box<dyn FnOnce() -> crate::Result<PendingCommit> + Send + Sync>,
    ) -> Self {
        let (deferred, waiter) = Deferred::new(admit, submit);
        Self {
            sub: Submission {
                waiter,
                shared,
                spins: false,
            },
            resolved: None,
            deferred: Some(deferred),
        }
    }

    /// Polls a commit still waiting for room: once admitted, it submits and goes on as the
    /// submitted commit. Out of line: the uncommon state.
    #[cold]
    #[inline(never)]
    fn poll_deferred(&mut self, cx: &mut Context<'_>) -> Poll<crate::Result<CommitInfo>> {
        let Poll::Ready((admit, submitted)) = Deferred::poll_submit(&mut self.deferred, cx) else {
            return Poll::Pending;
        };
        match submitted {
            Ok(pending) => {
                admit.attach(&pending);
                *self = pending;
                Pin::new(self).poll(cx)
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl Future for PendingCommit {
    type Output = crate::Result<CommitInfo>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if this.deferred.is_some() {
            return this.poll_deferred(cx);
        }
        let info = match this.resolved {
            Some(info) => info,
            None => match Pin::new(&mut this.sub.waiter).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Err(dropped_reply_now(&this.sub.shared))),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Ready(Some(Ok(Settled { info, batch }))) => {
                    recycle(batch);
                    this.resolved = Some(info);
                    info
                }
            },
        };
        // Durable and applied on its shard; another shard's in-flight group may still hold
        // the global watermark below it (D19). The shards wake registered waiters when they
        // publish a watermark, so this never spins.
        if this.sub.shared.wait_visible(info.seqno, cx.waker()) {
            Poll::Ready(Ok(info))
        } else if this.sub.shared.visibility_ended() {
            Poll::Ready(Err(Error::OutcomeUnknown))
        } else {
            Poll::Pending
        }
    }
}

/// A [`check_and_mutate`](crate::Engine::check_and_mutate) submitted to its shard and not
/// yet resolved ([`Engine::submit_check_and_mutate`](crate::Engine::submit_check_and_mutate)):
/// resolves to whether the batch applied, and its commit when it did, once that commit is
/// durable at its level and visible. Dropping it does not roll back.
#[derive(Debug)]
#[must_use = "dropping a pending check does not cancel it, but its result is lost"]
pub struct PendingCheck {
    pub(crate) waiter: Waiter<crate::Result<(bool, Option<CommitInfo>)>>,
    pub(crate) shared: Arc<crate::engine::ShardRef>,
    /// Resolved by the shard; waiting for visibility (async polling).
    pub(crate) resolved: Option<(bool, Option<CommitInfo>)>,
    /// As [`PendingCommit`]'s: still waiting for room (D210).
    pub(crate) deferred: Option<Box<Deferred<PendingCheck, CheckReply>>>,
}

impl PendingCheck {
    /// Blocks until the check resolved and an applied commit is visible, as
    /// [`PendingCommit::wait`] does (including its refusal on a thread that drives a shard).
    pub fn wait(mut self) -> crate::Result<(bool, Option<CommitInfo>)> {
        if self.deferred.is_some() {
            let (admit, submitted) = Deferred::submit_blocking(&mut self.deferred);
            self = submitted?;
            admit.attach_check(&self);
        }
        let (applied, info) = wait_reply(&self.shared, &mut self.waiter)?;
        if let Some(info) = info {
            wait_visible(&self.shared, info.seqno)?;
        }
        Ok((applied, info))
    }
}

impl PendingCheck {
    /// [`PendingCommit::waiting`] for a conditional commit.
    pub(crate) fn waiting(
        shared: Arc<crate::engine::ShardRef>,
        admit: crate::Admit,
        submit: Box<dyn FnOnce() -> crate::Result<PendingCheck> + Send + Sync>,
    ) -> Self {
        let (deferred, waiter) = Deferred::new(admit, submit);
        Self {
            waiter,
            shared,
            resolved: None,
            deferred: Some(deferred),
        }
    }

    /// [`PendingCommit::poll_deferred`] for a conditional commit.
    #[cold]
    #[inline(never)]
    fn poll_deferred(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<crate::Result<(bool, Option<CommitInfo>)>> {
        let Poll::Ready((admit, submitted)) = Deferred::poll_submit(&mut self.deferred, cx) else {
            return Poll::Pending;
        };
        match submitted {
            Ok(pending) => {
                admit.attach_check(&pending);
                *self = pending;
                Pin::new(self).poll(cx)
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl Future for PendingCheck {
    type Output = crate::Result<(bool, Option<CommitInfo>)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if this.deferred.is_some() {
            return this.poll_deferred(cx);
        }
        let outcome = match this.resolved {
            Some(outcome) => outcome,
            None => match Pin::new(&mut this.waiter).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Err(dropped_reply_now(&this.shared))),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Ready(Some(Ok(outcome))) => {
                    this.resolved = Some(outcome);
                    outcome
                }
            },
        };
        // As a commit: an applied batch is visible once the global watermark covers it (D19).
        let Some(info) = outcome.1 else {
            return Poll::Ready(Ok(outcome));
        };
        if this.shared.wait_visible(info.seqno, cx.waker()) {
            Poll::Ready(Ok(outcome))
        } else if this.shared.visibility_ended() {
            Poll::Ready(Err(Error::OutcomeUnknown))
        } else {
            Poll::Pending
        }
    }
}

/// One `(table, row, family)` a transaction read; validated at commit.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ReadKey {
    pub table: TableId,
    pub row: Vec<u8>,
    pub family: FamilyId,
}

/// An optimistic multi-row transaction (Phase 4). Reads record the `(row, family)` ranges
/// they touch; at commit, each participant shard checks its tablets for conflicting commits
/// since the snapshot during PREPARE, and any conflict aborts with `Error::Conflict`.
#[derive(Debug)]
pub struct Txn {
    pub(crate) engine: Arc<Inner>,
    pub(crate) snapshot: Snapshot,
    pub(crate) reads: Vec<ReadKey>,
    pub(crate) batch: WriteBatch,
}

impl Txn {
    /// The transaction's snapshot seqno.
    pub fn snapshot(&self) -> &crate::Snapshot {
        &self.snapshot
    }

    /// Reads a cell and records the read.
    pub fn get(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> crate::Result<Option<CellData>> {
        let key = ReadKey {
            table,
            row: row.to_vec(),
            family,
        };
        if !self.reads.contains(&key) {
            self.reads.push(key);
        }
        self.engine
            .get(&self.snapshot, table, family, row, qualifier)
    }

    /// [`get`](Txn::get) as a future: records the read at the call, then reads at the
    /// transaction's snapshot as [`Engine::get_async`](crate::Engine::get_async) does. A
    /// read recorded and then dropped unpolled still counts at validation (conservative).
    pub fn get_async(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> crate::GetFuture {
        let key = ReadKey {
            table,
            row: row.to_vec(),
            family,
        };
        if !self.reads.contains(&key) {
            self.reads.push(key);
        }
        crate::GetFuture::new(
            Arc::clone(&self.engine),
            Some(self.snapshot.clone()),
            table,
            family,
            row,
            qualifier,
        )
    }

    /// The buffered writes.
    pub fn batch(&mut self) -> &mut WriteBatch {
        &mut self.batch
    }

    /// [`submit`](Self::submit) for an async commit (D210), as
    /// [`Engine::submit_async`](crate::Engine::submit_async): when it has to wait for room,
    /// the pending commit submits the transaction once admitted.
    pub fn submit_async(self, durability: Option<Durability>) -> crate::Result<PendingCommit> {
        let mut charge = match self.engine.fast_admit(None, &self.batch) {
            crate::engine::FastAdmit::Wait(admit) => {
                let shared = self
                    .engine
                    .shard_ref(pigeonhole_runtime::ShardId(admit.shard() as u16));
                return Ok(PendingCommit::waiting(
                    shared,
                    admit,
                    Box::new(move || self.submit(durability)),
                ));
            }
            crate::engine::FastAdmit::Charged(charge) => charge,
        };
        let pending = PendingCommit::from(self.submission(durability)?);
        charge.attach(&pending.sub.waiter);
        Ok(pending)
    }

    /// Submits the transaction for validation and commit without waiting: the
    /// [`PendingCommit`] resolves as [`Txn::commit`] returns (an async caller polls it). It
    /// never blocks, so a thread that drives a shard may call it (D88).
    pub fn submit(self, durability: Option<Durability>) -> crate::Result<PendingCommit> {
        self.submission(durability).map(PendingCommit::from)
    }

    /// [`submit`](Self::submit), as the engine's own [`Submission`].
    pub(crate) fn submission(self, durability: Option<Durability>) -> crate::Result<Submission> {
        let Txn {
            engine,
            snapshot,
            reads,
            batch,
        } = self;
        engine.submit(batch, durability, Some((snapshot.seqno, reads)), None)
    }

    /// Validates and commits.
    pub fn commit(self, durability: Option<Durability>) -> crate::Result<CommitInfo> {
        let Txn {
            engine,
            snapshot,
            reads,
            batch,
        } = self;
        engine.shared.refuse_blocking_on_driver("Txn::commit")?;
        engine
            .submit(batch, durability, Some((snapshot.seqno, reads)), None)?
            .wait()
    }
}

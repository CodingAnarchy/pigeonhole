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
use pigeonhole_runtime::Waiter;

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

/// A commit submitted to its shard(s) and not yet resolved. The basis of sync commits
/// ([`PendingCommit::wait`]), async commits (`.await`) and ticket-style commits. Dropping it
/// does not roll back: the commit lands or fails atomically either way.
#[derive(Debug)]
#[must_use = "dropping a pending commit does not cancel it, but its result is lost"]
pub struct PendingCommit {
    pub(crate) waiter: Waiter<crate::Result<Settled>>,
    /// The engine, for registering an async waker on the global watermark.
    pub(crate) shared: Arc<crate::shard::Shared>,
    /// Resolved by the shard; waiting for visibility (async polling).
    pub(crate) resolved: Option<CommitInfo>,
    /// Whether `wait` may poll before it parks (D198): not for a durable commit, which
    /// waits for a sync. The window is the engine's `commit_spin_nanos`.
    pub(crate) spins: bool,
}

impl PendingCommit {
    /// Blocks until the commit meets its durability level and is visible. The thread parks
    /// while it waits. Fails with [`Error::Closed`] if the database closes, or a shard dies
    /// (its thread panicked, or its `EngineShard` was dropped), before the commit is visible.
    ///
    /// In application-owned mode a thread that drives a shard must not block: the commit
    /// may need that shard to run (its own group, or any group holding the global
    /// watermark below it, decision D88). On such a thread this returns the result if the
    /// commit is already done and visible, and otherwise fails at once with
    /// [`Error::WouldDeadlock`]: the commit was submitted and will apply, but its outcome
    /// must be awaited from the event loop (poll this future there) or another thread.
    pub fn wait(mut self) -> crate::Result<CommitInfo> {
        let window = if self.spins {
            self.shared.commit_spin_nanos
        } else {
            0
        };
        if window == 0 {
            // No spin: exactly the parking wait.
            let Settled { info, batch } = wait_reply(&self.shared, &mut self.waiter)?;
            recycle(batch);
            wait_visible(&self.shared, info.seqno)?;
            return Ok(info);
        }
        let mut spin = SpinWait::new(window);
        let Settled { info, batch } =
            wait_reply_spinning(&self.shared, &mut self.waiter, &mut spin)?;
        recycle(batch);
        wait_visible_spinning(&self.shared, info.seqno, &mut spin)?;
        Ok(info)
    }
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
            Poll::Ready(None) => Err(Error::Closed),
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
            Poll::Ready(None) => return Err(Error::Closed),
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
            Poll::Ready(None) => Err(Error::Closed),
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
        Some(Poll::Ready(None)) => return Err(Error::Closed),
        _ => {}
    }
    park_for_reply(shared, waiter)
}

/// Blocks until `seqno` is visible (D19), parked on the shards' watermark publishes. Fails
/// instead on a thread that drives a shard, which may be the one holding the watermark, and
/// with `Closed` once visibility can no longer advance (the close finished, or a shard died).
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
            return Err(Error::Closed);
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
            return Err(Error::Closed);
        }
        std::thread::park();
    }
    Ok(())
}

impl Future for PendingCommit {
    type Output = crate::Result<CommitInfo>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let info = match this.resolved {
            Some(info) => info,
            None => match Pin::new(&mut this.waiter).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Err(Error::Closed)),
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
        if this.shared.wait_visible(info.seqno, cx.waker()) {
            Poll::Ready(Ok(info))
        } else if this.shared.visibility_ended() {
            Poll::Ready(Err(Error::Closed))
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
    pub(crate) shared: Arc<crate::shard::Shared>,
    /// Resolved by the shard; waiting for visibility (async polling).
    pub(crate) resolved: Option<(bool, Option<CommitInfo>)>,
}

impl PendingCheck {
    /// Blocks until the check resolved and an applied commit is visible, as
    /// [`PendingCommit::wait`] does (including its refusal on a thread that drives a shard).
    pub fn wait(mut self) -> crate::Result<(bool, Option<CommitInfo>)> {
        let (applied, info) = wait_reply(&self.shared, &mut self.waiter)?;
        if let Some(info) = info {
            wait_visible(&self.shared, info.seqno)?;
        }
        Ok((applied, info))
    }
}

impl Future for PendingCheck {
    type Output = crate::Result<(bool, Option<CommitInfo>)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let outcome = match this.resolved {
            Some(outcome) => outcome,
            None => match Pin::new(&mut this.waiter).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Err(Error::Closed)),
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
            Poll::Ready(Err(Error::Closed))
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

    /// Submits the transaction for validation and commit without waiting: the
    /// [`PendingCommit`] resolves as [`Txn::commit`] returns (an async caller polls it). It
    /// never blocks, so a thread that drives a shard may call it (D88).
    pub fn submit(self, durability: Option<Durability>) -> crate::Result<PendingCommit> {
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

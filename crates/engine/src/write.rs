use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use pigeonhole_compaction::ValuePredicate;
use pigeonhole_format::Kind;
use pigeonhole_format::value::{ValueRef, encode_value};
use pigeonhole_format::wal::{BatchBuilder, BatchRef};
use pigeonhole_format::{Durability, FamilyId, TableId, Timestamp};
use pigeonhole_runtime::Waiter;
use pigeonhole_shm::ShmRegion;

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
#[derive(Debug, Default, Clone)]
pub struct WriteBatch {
    pub(crate) builder: BatchBuilder,
    pub(crate) row_deletes: Vec<RowDelete>,
    /// Scratch for value encoding (kept between calls).
    value_buf: Vec<u8>,
}

impl WriteBatch {
    /// An empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Puts a value. `ts = None` uses the commit timestamp.
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
        self.value_buf.clear();
        encode_value(&mut self.value_buf, value);
        self.builder.push(
            table,
            family,
            Kind::Put,
            row,
            qualifier,
            ts,
            &self.value_buf,
        )?;
        Ok(())
    }

    /// Writes a merge operand.
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
        self.value_buf.clear();
        encode_value(&mut self.value_buf, operand);
        self.builder.push(
            table,
            family,
            Kind::Merge,
            row,
            qualifier,
            None,
            &self.value_buf,
        )?;
        Ok(())
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
    pub(crate) waiter: Waiter<crate::Result<CommitInfo>>,
    /// For the visibility wait (decision D19).
    pub(crate) shm: ShmRegion,
    /// The engine, for registering an async waker on the global watermark.
    pub(crate) shared: Arc<crate::shard::Shared>,
    /// Resolved by the shard; waiting for visibility (async polling).
    pub(crate) resolved: Option<CommitInfo>,
}

impl PendingCommit {
    /// Blocks until the commit meets its durability level and is visible.
    ///
    /// In application-owned mode this must not be called on a thread that drives the
    /// commit's shard (it would wait for work only that thread can do): poll the future
    /// from the event loop instead, or wait on another thread.
    pub fn wait(self) -> crate::Result<CommitInfo> {
        let info = self.waiter.wait().unwrap_or(Err(Error::Closed))?;
        // The shard resolved the commit once its group was durable and its own watermark
        // published; another shard's in-flight group may still hold the global watermark
        // below it for a moment (D19).
        let mut spins = 0u32;
        while self.shm.visible_seqno() < info.seqno {
            spins += 1;
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
        Ok(info)
    }
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
                Poll::Ready(Some(Ok(info))) => {
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

    /// The buffered writes.
    pub fn batch(&mut self) -> &mut WriteBatch {
        &mut self.batch
    }

    /// Validates and commits.
    pub fn commit(self, durability: Option<Durability>) -> crate::Result<CommitInfo> {
        let Txn {
            engine,
            snapshot,
            reads,
            batch,
        } = self;
        engine
            .submit(batch, durability, Some((snapshot.seqno, reads)), None)?
            .wait()
    }
}

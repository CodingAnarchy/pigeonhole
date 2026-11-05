//! The Ruby classes' Rust data: one struct per wrapped class. Their methods live in
//! `db.rs`, `table.rs` and `read.rs`.
//!
//! **Unsafe:** this module allows `unsafe_code` only because `#[magnus::wrap]` expands to an
//! `unsafe impl magnus::TypedData` for each struct. It contains no hand-written unsafe.
#![allow(unsafe_code)]

use std::sync::{Mutex, RwLock};

use crate::fork::ForkSafe;
use crate::table::{Durability, Handle, Op};

/// `Pigeonhole::Database`: the writer process's handle; `None` once closed.
#[magnus::wrap(class = "Pigeonhole::Database", free_immediately, size)]
pub(crate) struct Database(
    pub(crate) ForkSafe<RwLock<Option<pigeonhole::Pigeonhole>>>,
    /// The memory stores opened on it (PR 3a): `close` closes them first, so their forgetter
    /// threads stop (simp724's P1 on #1340, as in Python).
    pub(crate) crate::memory::Stores<crate::memory::StoreSlot>,
);

/// `Pigeonhole::Reader`: a reader process's handle; `None` once closed.
#[magnus::wrap(class = "Pigeonhole::Reader", free_immediately, size)]
pub(crate) struct Reader(
    pub(crate) ForkSafe<RwLock<Option<pigeonhole::PigeonholeReader>>>,
    /// The memory stores opened on it: `close` closes them first.
    pub(crate) crate::memory::Stores<crate::memory::ReadSlot>,
);

/// `Pigeonhole::TableBuilder`'s state.
pub(crate) struct Builder {
    pub(crate) db: pigeonhole::Pigeonhole,
    pub(crate) name: String,
    pub(crate) families: Mutex<Vec<(String, pigeonhole::Family)>>,
}

/// `Pigeonhole::TableBuilder`.
#[magnus::wrap(class = "Pigeonhole::TableBuilder", free_immediately, size)]
pub(crate) struct TableBuilder(pub(crate) ForkSafe<Builder>);

/// `Pigeonhole::Table`: a writer or reader table handle.
#[magnus::wrap(class = "Pigeonhole::Table", free_immediately, size)]
pub(crate) struct Table(pub(crate) ForkSafe<Handle>);

/// `Pigeonhole::Mutation`'s state: writes recorded against one row.
pub(crate) struct MutationState {
    pub(crate) table: pigeonhole::Table,
    pub(crate) row: Vec<u8>,
    /// `None` once committed.
    pub(crate) ops: Mutex<Option<Vec<Op>>>,
    /// The columns `return_previous` and `return_current` asked for (ICR 0113).
    pub(crate) asked: Mutex<Asked>,
    pub(crate) durability: Mutex<Option<Durability>>,
}

/// A column: family and qualifier.
pub(crate) type Column = (String, Vec<u8>);

/// The columns a mutation's `return_previous` and `return_current` asked for (ICR 0113).
#[derive(Debug, Clone, Default)]
pub(crate) struct Asked {
    pub(crate) previous: Vec<Column>,
    pub(crate) current: Vec<Column>,
}

impl Asked {
    /// Whether `return_current` asked for the column, or it's `condition`'s (always
    /// reported).
    pub(crate) fn has_current(&self, family: &str, q: &[u8], condition: Option<&Column>) -> bool {
        self.current
            .iter()
            .chain(condition)
            .any(|(f, cq)| f == family && cq == q)
    }
}

/// `Pigeonhole::Mutation`.
#[magnus::wrap(class = "Pigeonhole::Mutation", free_immediately, size)]
pub(crate) struct Mutation(pub(crate) ForkSafe<MutationState>);

/// One recorded batch write: its table, its row and the write.
pub(crate) type BatchOp = (pigeonhole::Table, Vec<u8>, Op);

/// `Pigeonhole::WriteBatch`'s state.
pub(crate) struct BatchState {
    pub(crate) db: pigeonhole::Pigeonhole,
    /// `None` once committed or discarded.
    pub(crate) ops: Mutex<Option<Vec<BatchOp>>>,
}

/// `Pigeonhole::WriteBatch`.
#[magnus::wrap(class = "Pigeonhole::WriteBatch", free_immediately, size)]
pub(crate) struct WriteBatch(pub(crate) ForkSafe<BatchState>);

/// `Pigeonhole::Transaction`: the engine transaction behind a mutex (simp724's R4); `None`
/// once committed or rolled back.
#[magnus::wrap(class = "Pigeonhole::Transaction", free_immediately, size)]
pub(crate) struct Transaction(pub(crate) ForkSafe<Mutex<Option<pigeonhole::Transaction>>>);

/// `Pigeonhole::Snapshot`; `None` once released.
#[magnus::wrap(class = "Pigeonhole::Snapshot", free_immediately, size)]
pub(crate) struct Snapshot(pub(crate) ForkSafe<Mutex<Option<pigeonhole::Snapshot>>>);

/// `Pigeonhole::Cell`.
#[magnus::wrap(class = "Pigeonhole::Cell", free_immediately, size)]
pub(crate) struct Cell(pub(crate) pigeonhole::Cell);

/// `Pigeonhole::Incremented` (also `Pigeonhole::Returned`): the counter cells a
/// `Mutation#commit_returning` (or an applied `commit_if_returning`) wrote, after its commit
/// (D314), and the columns `return_previous` and `return_current` asked for, with those
/// requests and the condition's column. Plain data: no handle, so nothing to guard across a
/// fork.
#[magnus::wrap(class = "Pigeonhole::Incremented", free_immediately, size)]
pub(crate) struct Incremented(
    pub(crate) pigeonhole::Incremented,
    pub(crate) Asked,
    pub(crate) Option<Column>,
);

/// `Pigeonhole::Seen`: the row as a failed `Mutation#commit_if_returning`'s condition saw it
/// (D314 amendment 2). Plain data.
#[magnus::wrap(class = "Pigeonhole::Seen", free_immediately, size)]
pub(crate) struct Seen(pub(crate) pigeonhole::Seen);

/// `Pigeonhole::Row`.
#[magnus::wrap(class = "Pigeonhole::Row", free_immediately, size)]
pub(crate) struct Row(pub(crate) pigeonhole::Row);

/// `Pigeonhole::Family`: a family's options.
#[magnus::wrap(class = "Pigeonhole::Family", free_immediately, size)]
pub(crate) struct Family(pub(crate) pigeonhole::Family);

/// `Pigeonhole::FamilyInfo`: a family's stored options.
#[magnus::wrap(class = "Pigeonhole::FamilyInfo", free_immediately, size)]
pub(crate) struct FamilyInfo(pub(crate) pigeonhole::FamilyInfo);

/// `Pigeonhole::Condition`.
#[magnus::wrap(class = "Pigeonhole::Condition", free_immediately, size)]
pub(crate) struct Condition(pub(crate) pigeonhole::Condition);

/// What a scan reads, converted and checked when `Table#scan` is called; each enumeration
/// starts its own cursor from it.
pub(crate) struct ScanArgs {
    pub(crate) table: Handle,
    pub(crate) prefix: Option<Vec<u8>>,
    pub(crate) start: Option<Vec<u8>>,
    pub(crate) end: Option<Vec<u8>>,
    pub(crate) families: Option<Vec<String>>,
    pub(crate) qualifier_prefix: Option<Vec<u8>>,
    pub(crate) versions: u32,
    pub(crate) limit: Option<u64>,
    pub(crate) column_limit: Option<u32>,
    pub(crate) chunk: usize,
}

/// `Pigeonhole::Native::ScanSpec` (internal): a scan's arguments.
#[magnus::wrap(class = "Pigeonhole::Native::ScanSpec", free_immediately, size)]
pub(crate) struct ScanSpec(pub(crate) ForkSafe<ScanArgs>);

/// `Pigeonhole::Native::ScanCursor` (internal): one enumeration's rows, read at one
/// snapshot (simp724's R3); `None` once finished or closed.
#[magnus::wrap(class = "Pigeonhole::Native::ScanCursor", free_immediately, size)]
pub(crate) struct ScanCursor {
    pub(crate) rows: ForkSafe<Mutex<Option<pigeonhole::RowIter<'static>>>>,
    pub(crate) chunk: usize,
}

/// `Pigeonhole::VectorSpec`: a vector family's dimension, element type and metric.
#[magnus::wrap(class = "Pigeonhole::VectorSpec", free_immediately, size)]
pub(crate) struct VectorSpec(pub(crate) pigeonhole::VectorSpec);

/// `Pigeonhole::RowPosition`: where a walk of a row's pages stopped.
#[magnus::wrap(class = "Pigeonhole::RowPosition", free_immediately, size)]
pub(crate) struct RowPosition(pub(crate) pigeonhole::RowPosition);

/// `Pigeonhole::ScanPosition`: where a `scan_within` page stopped.
#[magnus::wrap(class = "Pigeonhole::ScanPosition", free_immediately, size)]
pub(crate) struct ScanPosition(pub(crate) pigeonhole::ScanPosition);

/// `Pigeonhole::KeysPosition`: where a `multi_get_within` page stopped.
#[magnus::wrap(class = "Pigeonhole::KeysPosition", free_immediately, size)]
pub(crate) struct KeysPosition(pub(crate) pigeonhole::KeysPosition);

/// What a walk of a row's pages reads, and where it is.
pub(crate) struct PagesState {
    pub(crate) table: Handle,
    pub(crate) row: Vec<u8>,
    pub(crate) size: usize,
    pub(crate) families: Option<Vec<String>>,
    pub(crate) qualifier_prefix: Option<Vec<u8>>,
    pub(crate) versions: u32,
    pub(crate) snapshot: Option<pigeonhole::Snapshot>,
    /// Where the last page ended.
    pub(crate) position: Option<pigeonhole::RowPosition>,
    pub(crate) done: bool,
}

/// `Pigeonhole::Pages`: a wide row's pages, each read when it is asked for.
#[magnus::wrap(class = "Pigeonhole::Pages", free_immediately, size)]
pub(crate) struct Pages(pub(crate) ForkSafe<Mutex<PagesState>>);

/// `Pigeonhole::Feed`: a change feed subscription (its state is `crate::feed::FeedState`).
#[magnus::wrap(class = "Pigeonhole::Feed", free_immediately, size)]
pub(crate) struct Feed(pub(crate) ForkSafe<crate::feed::FeedState>);

// ---- Pigeonhole::Memory (PR 3a; methods in memory.rs) ------------------------------

/// `Pigeonhole::Memory::MemoryId`: a memory's id. Its text form is RFC 4648 base32 of its
/// byte form (ICR 0107).
#[magnus::wrap(class = "Pigeonhole::Memory::MemoryId", free_immediately, size)]
#[derive(Clone)]
pub(crate) struct MemoryId(pub(crate) pigeonhole_memory::MemoryId);

/// `Pigeonhole::Memory::EmbeddingModel`: the model a store is pinned to.
#[magnus::wrap(class = "Pigeonhole::Memory::EmbeddingModel", free_immediately, size)]
#[derive(Clone)]
pub(crate) struct EmbeddingModel(pub(crate) pigeonhole_memory::EmbeddingModel);

/// `Pigeonhole::Memory::IvfOptions`: a store's IVF index options (D311 T8).
#[magnus::wrap(class = "Pigeonhole::Memory::IvfOptions", free_immediately, size)]
#[derive(Clone, Copy)]
pub(crate) struct IvfOptions(pub(crate) pigeonhole_memory::IvfOptions);

/// `Pigeonhole::Memory::Weights`: recall's signal weights (1 by default; `recency` 0.25,
/// `graph` and `consolidated` 0.5).
#[magnus::wrap(class = "Pigeonhole::Memory::Weights", free_immediately, size)]
#[derive(Clone)]
pub(crate) struct Weights(pub(crate) pigeonhole_memory::Weights);

/// `Pigeonhole::Memory::Expand`: a recall's graph expansion (T4).
#[magnus::wrap(class = "Pigeonhole::Memory::Expand", free_immediately, size)]
#[derive(Clone)]
pub(crate) struct Expand(pub(crate) pigeonhole_memory::Expand);

/// `Pigeonhole::Memory::ForgetPolicy`: a store's forget policy (T2).
#[magnus::wrap(class = "Pigeonhole::Memory::ForgetPolicy", free_immediately, size)]
#[derive(Clone)]
pub(crate) struct ForgetPolicy(pub(crate) pigeonhole_memory::ForgetPolicy);

/// `Pigeonhole::Memory::ConsolidatePolicy`: how `consolidate` picks and folds groups (T3).
#[magnus::wrap(
    class = "Pigeonhole::Memory::ConsolidatePolicy",
    free_immediately,
    size
)]
#[derive(Clone)]
pub(crate) struct ConsolidatePolicy(pub(crate) pigeonhole_memory::ConsolidatePolicy);

/// `Pigeonhole::Memory::NearDedup`: a store's near-duplicate dedup (T3).
#[magnus::wrap(class = "Pigeonhole::Memory::NearDedup", free_immediately, size)]
#[derive(Clone, Copy)]
pub(crate) struct NearDedup(pub(crate) pigeonhole_memory::NearDedup);

/// `Pigeonhole::Memory::Memory`: a memory to write, what a consolidator returns.
#[magnus::wrap(class = "Pigeonhole::Memory::Memory", free_immediately, size)]
#[derive(Clone)]
pub(crate) struct Memory {
    pub(crate) text: String,
    pub(crate) inner: pigeonhole_memory::Memory,
}

/// `Pigeonhole::Memory::Store`: a memory store in the writer process.
#[magnus::wrap(class = "Pigeonhole::Memory::Store", free_immediately, size)]
pub(crate) struct Store(pub(crate) ForkSafe<std::sync::Arc<crate::memory::StoreSlot>>);

/// `Pigeonhole::Memory::ReadStore`: a memory store in a reader process.
#[magnus::wrap(class = "Pigeonhole::Memory::ReadStore", free_immediately, size)]
pub(crate) struct ReadStore(pub(crate) ForkSafe<std::sync::Arc<crate::memory::ReadSlot>>);

// ---- Pigeonhole::Text and Pigeonhole::Vector (PR 3b; methods in index.rs) ----------------

/// `Pigeonhole::Text::Query`: a full-text query.
#[magnus::wrap(class = "Pigeonhole::Text::Query", free_immediately, size)]
pub(crate) struct TextQuery(pub(crate) pigeonhole_text::Query);

/// `Pigeonhole::Text::Index`: a full-text index in the writer process.
#[magnus::wrap(class = "Pigeonhole::Text::Index", free_immediately, size)]
pub(crate) struct TextIndex(pub(crate) ForkSafe<pigeonhole_text::Index>);

/// `Pigeonhole::Text::ReadIndex`: a full-text index in a reader process.
#[magnus::wrap(class = "Pigeonhole::Text::ReadIndex", free_immediately, size)]
pub(crate) struct TextReadIndex(pub(crate) ForkSafe<pigeonhole_text::ReadIndex>);

/// `Pigeonhole::Vector::Index`: a vector index in the writer process.
#[magnus::wrap(class = "Pigeonhole::Vector::Index", free_immediately, size)]
pub(crate) struct VectorIndex(pub(crate) ForkSafe<pigeonhole_vector::Index>);

/// `Pigeonhole::Vector::ReadIndex`: a vector index in a reader process.
#[magnus::wrap(class = "Pigeonhole::Vector::ReadIndex", free_immediately, size)]
pub(crate) struct VectorReadIndex(pub(crate) ForkSafe<pigeonhole_vector::ReadIndex>);

/// `Pigeonhole::Vector::Placed`: one transaction's writes to one index; `None` once passed
/// to `after_commit`.
#[magnus::wrap(class = "Pigeonhole::Vector::Placed", free_immediately, size)]
pub(crate) struct Placed(pub(crate) ForkSafe<Mutex<Option<pigeonhole_vector::Placed>>>);

/// `Pigeonhole::Vector::Rebalancer`: a background-mode index's rebalancing, for a Ruby
/// thread to drive.
#[magnus::wrap(class = "Pigeonhole::Vector::Rebalancer", free_immediately, size)]
pub(crate) struct Rebalancer(
    pub(crate) ForkSafe<crate::index::Fair<pigeonhole_vector::Rebalancer>>,
);

/// `Pigeonhole::Vector::BulkLoad`: a bulk load in progress; `None` once finished.
#[magnus::wrap(class = "Pigeonhole::Vector::BulkLoad", free_immediately, size)]
pub(crate) struct VectorBulkLoad(pub(crate) ForkSafe<Mutex<Option<crate::index::BulkState>>>);

/// `Pigeonhole::Memory::Watch`: a store's memory events (T9); `None` once closed.
#[magnus::wrap(class = "Pigeonhole::Memory::Watch", free_immediately, size)]
pub(crate) struct Watch(pub(crate) ForkSafe<crate::index::Fair<Option<pigeonhole_memory::Watch>>>);

/// `Pigeonhole::Memory::ConsolidateLoop`: feed-driven consolidation (T9).
#[magnus::wrap(class = "Pigeonhole::Memory::ConsolidateLoop", free_immediately, size)]
pub(crate) struct ConsolidateLoop(pub(crate) ForkSafe<Mutex<crate::memory::LoopSlot>>);

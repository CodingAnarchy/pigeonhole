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
pub(crate) struct Database(pub(crate) ForkSafe<RwLock<Option<pigeonhole::Pigeonhole>>>);

/// `Pigeonhole::Reader`: a reader process's handle; `None` once closed.
#[magnus::wrap(class = "Pigeonhole::Reader", free_immediately, size)]
pub(crate) struct Reader(pub(crate) ForkSafe<RwLock<Option<pigeonhole::PigeonholeReader>>>);

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
    pub(crate) durability: Mutex<Option<Durability>>,
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

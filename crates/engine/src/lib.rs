//! The Pigeonhole storage engine: tablets, shards, MVCC, WAL, flush, compaction.
//!
//! [`Engine`] assembles every lower crate into a thread-per-core database. It owns the
//! catalog (tables, families), tablets and routing, the single manifest writer, views and
//! snapshots, global seqno reservation and the snapshot watermark, cross-shard two-phase
//! commit, durability resolution, background scheduling and online backup.
//!
//! This is the internal API the public `pigeonhole` crate wraps: ids instead of names,
//! explicit snapshots, no ergonomics. Data flow through the lower crates is described in
//! `docs/design/interfaces.md`.
//!
//! Reads and compaction share `pigeonhole-compaction`'s merge and resolver, so delete, TTL,
//! version and merge semantics have one implementation. Frozen memtables are flushed to SSTs
//! by a cooperative task per shard, WAL streams are checkpointed per decision D24 and
//! removed at the last clean close (one file at rest), compaction runs as cooperative tasks
//! (or on `compaction_threads`), and writes stall on L0 depth through a per-shard token
//! bucket. With `EngineOptions::tablet_changes` on (the default),
//! tablets split at a size threshold or under write skew, merge when small and cold, and
//! move between shards when load is skewed; reads are never blocked by any of it.
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

mod admission;
mod catalog;
mod check;
mod clock;
#[cfg(all(test, engine_loom))]
mod clock_loom;
mod close_gate;
mod compact;
mod engine;
mod error;
mod flush;
mod flushed_roots;
#[cfg(all(test, engine_loom))]
mod flushed_through_loom;
mod health;
mod large;
mod large_settle;
mod load_slot;
mod maintenance;
mod manifest;
mod nonblocking;
mod options;
mod position;
mod purge_guard;
mod read;
mod row_cache;
mod shard;
mod snapshot;
mod source;
mod timer_state;
mod waker;
mod watermark_waiters;
mod write;

pub use admission::Admit;
pub use check::{
    CheckCounts, CheckOptions, CheckReport, Extent, ExtentKind, Finding, Severity, Structure,
    WalFile,
};
#[doc(hidden)]
pub use engine::Handover;
pub use engine::PendingMaintenance;
pub use engine::{
    AppOwned, BackgroundWorker, CommitInfo, CrossShardMetrics, DbInfo, Engine, EngineShard,
    FamilyInfo, FamilyStorage, LevelStorage, Metrics, Role, ShardStats, TableInfo, TableStorage,
};
pub use error::{Error, Result};
pub use health::{Failure, FailureCause};
pub use nonblocking::{GetFuture, MissedRead, RowFuture};
pub use options::EngineOptions;
pub use position::ColumnPosition;
pub use read::keys::{KeyCursor, KeySet};
pub use read::reduce::{
    CellKey, CellRef, CellVisitor, Dedupe, Limit, PARALLEL_FLOOR_ENTRIES, ReadInput, ReduceSpec,
    ReduceStats, Reduced, Reducer, RowRange, TopK, Visit,
};
pub use read::{CellData, ReadSpec, RowCell, RowData, RowSink, ScanCell, ScanCursor, ScanSpec};
pub use row_cache::{RowCacheStats, table_slots as row_epoch_slots};
#[doc(hidden)]
pub use shard::{group_sync_depth, set_tracing};
pub use snapshot::{Snapshot, TabletMap, View};
pub use write::{COUNTER_TS, PendingCheck, PendingCommit, Predicate, Txn, WriteBatch};

/// What the engine's own tests reach in with (the `test-hooks` feature, `engine::hooks`).
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub use engine::hooks::{
    AppendedKind, AppendedRecord, ArenaDiscard, CompactionRecord, Fault, FlushGcMutation, HookSite,
    LoadTerm, ManifestInfo, MemoryRelease, RawEntry, TabletOwner, TabletRange,
};

pub use pigeonhole_compaction::{
    I64Add, MergeError, MergeOperator, MergeRegistry, PickerOptions, ValuePredicate,
};
pub use pigeonhole_format::compress::Compression;
pub use pigeonhole_format::manifest::{CachePriority, CompactionStyle, FamilyKind, FamilyOptions};
pub use pigeonhole_format::scan::QualifierFilter;
pub use pigeonhole_format::value::ValueRef;
pub use pigeonhole_format::{Durability, FamilyId, Seqno, TableId, Timestamp};

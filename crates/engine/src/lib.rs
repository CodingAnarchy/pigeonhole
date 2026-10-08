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

mod catalog;
mod compact;
mod engine;
mod error;
mod flush;
mod maintenance;
mod manifest;
mod options;
mod read;
mod shard;
mod snapshot;
mod source;
mod waker;
mod write;

#[cfg(feature = "test-hooks")]
pub use compact::CompactionRecord;
pub use engine::{
    CommitInfo, Engine, EngineShard, FamilyInfo, Metrics, Role, ShardStats, TableInfo,
};
#[cfg(feature = "test-hooks")]
pub use engine::{ManifestInfo, PendingMaintenance, RawEntry, TabletRange};
pub use error::{Error, Result};
pub use options::EngineOptions;
pub use read::{CellData, ReadSpec, RowCell, RowData, ScanCell, ScanCursor, ScanSpec};
#[cfg(feature = "test-hooks")]
pub use shard::{AppendedKind, AppendedRecord};
#[cfg(feature = "test-hooks")]
pub use snapshot::TabletOwner;
pub use snapshot::{Snapshot, TabletMap, View};
pub use write::{PendingCommit, Predicate, Txn, WriteBatch};

pub use pigeonhole_compaction::{
    I64Add, MergeError, MergeOperator, MergeRegistry, PickerOptions, ValuePredicate,
};
pub use pigeonhole_format::compress::Compression;
pub use pigeonhole_format::manifest::{CachePriority, CompactionStyle, FamilyOptions};
pub use pigeonhole_format::scan::QualifierFilter;
pub use pigeonhole_format::value::ValueRef;
pub use pigeonhole_format::{Durability, FamilyId, Seqno, TableId, Timestamp};

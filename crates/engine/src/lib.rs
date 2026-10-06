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
//! # Milestone A
//!
//! This build assembles everything that does not need SSTs: open, create and recovery,
//! the catalog, the write path (group commit, two-phase commit, `check_and_mutate`,
//! optimistic transactions), the read path over memtables, and close. Flush to SSTs,
//! compaction, backup and shrink land with `pigeonhole-sst` and `pigeonhole-compaction`
//! (Milestone B); until then frozen memtables are retained in memory, WAL streams are never
//! checkpointed or removed, and a full memtable arena turns commits into
//! [`Error::Busy`].
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

mod catalog;
mod engine;
mod error;
mod flush;
mod manifest;
mod options;
mod read;
mod resolve;
mod shard;
mod snapshot;
mod write;

pub use engine::{CommitInfo, Engine, EngineShard, FamilyInfo, Metrics, Role, TableInfo};
pub use error::{Error, Result};
pub use options::EngineOptions;
pub use read::{CellData, ReadSpec, RowCell, RowData, ScanCell, ScanCursor, ScanSpec};
pub use snapshot::{Snapshot, TabletMap, View};
pub use write::{PendingCommit, Predicate, Txn, WriteBatch};

pub use pigeonhole_compaction::{I64Add, MergeError, MergeOperator, MergeRegistry, ValuePredicate};
pub use pigeonhole_format::compress::Compression;
pub use pigeonhole_format::manifest::{CachePriority, CompactionStyle, FamilyOptions};
pub use pigeonhole_format::scan::QualifierFilter;
pub use pigeonhole_format::value::ValueRef;
pub use pigeonhole_format::{Durability, FamilyId, Seqno, TableId, Timestamp};

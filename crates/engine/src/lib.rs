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
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]
// Interface freeze: bodies are `todo!()`. Remove this allow when implementing.
#![allow(unused_variables, clippy::ptr_arg)]

mod engine;
mod error;
mod options;
mod read;
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

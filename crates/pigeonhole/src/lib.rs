//! Embedded, single-file, wide-column store: BigTable's data model with SQLite's deployment
//! model.
//!
//! A Pigeonhole database is a sorted, sparse, versioned map
//! `(table, row, family, qualifier, timestamp) → value` in one file.
//!
//! ```
//! use pigeonhole::{days, Durability, Family, Options, Pigeonhole};
//!
//! # fn main() -> pigeonhole::Result<()> {
//! # let dir = pigeonhole::doc_support::temp_dir();
//! let db = Pigeonhole::open(dir.join("crawl.phdb"), Options::default())?;
//! let pages = db
//!     .table("pages")?
//!     .family("meta", Family::default().max_versions(1))
//!     .family("links", Family::default().bloom_bits(10))
//!     .family("body", Family::default().blob_threshold(4096).ttl(days(30)))
//!     .family("stats", Family::counter())
//!     .create_if_missing()?;
//!
//! // Single-row atomic mutation.
//! pages
//!     .mutate(b"com.example/a")
//!     .put("meta", b"status", b"200")
//!     .put("links", b"com.example/b", b"")
//!     .incr("stats", b"hits", 1)
//!     .delete_column("meta", b"etag")
//!     .commit()?;
//!
//! // Point read: borrows from the cache, no allocation.
//! let status = pages.get(b"com.example/a", "meta", b"status")?;
//! assert_eq!(status.unwrap().value(), b"200");
//!
//! // Row read, projected to families.
//! let row = pages.row(b"com.example/a").families(["meta", "stats"]).latest().read()?;
//! assert_eq!(row.unwrap().get("stats", b"hits").unwrap().as_i64(), Some(1));
//!
//! // Ordered scan with filters pushed into the block decoder.
//! let snap = db.snapshot()?;
//! for row in pages
//!     .scan(b"com.example/"..b"com.example0")
//!     .family("links")
//!     .qualifier_prefix(b"org.")
//!     .snapshot(&snap)
//!     .iter()?
//! {
//!     let row = row?;
//!     assert!(row.is_empty() || row.key().starts_with(b"com.example/"));
//! }
//!
//! // Batched multi-row write with one durability point.
//! let mut wb = db.write_batch();
//! wb.put(&pages, b"com.example/c", "meta", b"status", b"404");
//! wb.commit_with(Durability::GroupSync)?;
//! db.close()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Install and status
//!
//! `cargo add pigeonhole`, or `pigeonhole = "0.3"` in `Cargo.toml`. This is a pre-1.0
//! release:
//! - **Done:** the core engine (Phase 1) is fault-tested in simulation; the wide-column model
//!   (Phase 2) shipped in 0.2.0 and the latency engine (Phase 3) in 0.3.0.
//! - **The file format is frozen for 1.x** at format 4: later 1.x releases read and write it,
//!   and the
//!   [compatibility promise](https://github.com/CodingAnarchy/pigeonhole/blob/main/FORMAT.md#13-compatibility-promise)
//!   takes effect at 1.0.0.
//! - **The API may still change before 1.0,** and Pigeonhole is not recommended for production
//!   use yet.
//!
//! See the [status page](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/status.md)
//! and the [changelog](https://github.com/CodingAnarchy/pigeonhole/blob/main/CHANGELOG.md).
//!
//! # API shape and the future C ABI
//!
//! Every zero-copy type ([`CellRef`], [`RowRef`]) has an owned, cheap, ref-counted
//! equivalent ([`Cell`], [`Row`]), and every iterator has a cursor-style method
//! ([`RowIter::next_ref`]), so a C ABI can wrap this crate later without exposing lifetimes,
//! generics or closures. Errors are a flat, stable [`ErrorCode`] plus a message.
//!
//! # Sync and async
//!
//! Every data operation has a blocking form, which needs no async runtime, and an async
//! form with the same semantics over the same engine (the [`nonblocking`] module, behind
//! the default-on `async` feature). `backup`, `shrink`, open, close and schema calls are
//! blocking only (D196).
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

mod cell;
mod check;
mod db;
mod error;
mod info;
mod merge;
mod metrics;
mod options;
mod read;
mod recovery;
mod retry;
mod table;
mod write;

#[doc(hidden)]
pub mod doc_support;

/// The user guide's and the READMEs' code samples, compiled and run as doctests.
#[cfg(doctest)]
mod guide {
    #[doc = include_str!("../../../docs/guide/getting-started.md")]
    struct GettingStarted;
    #[doc = include_str!("../../../docs/guide/durability.md")]
    struct Durability;
    #[doc = include_str!("../../../docs/guide/scans-and-filters.md")]
    struct ScansAndFilters;
    #[doc = include_str!("../../../docs/guide/data-modeling.md")]
    struct DataModeling;
    #[doc = include_str!("../../../docs/guide/errors.md")]
    struct Errors;
    #[doc = include_str!("../../../docs/guide/recovery.md")]
    struct Recovery;
    #[doc = include_str!("../../../docs/guide/agent-reference.md")]
    struct AgentReference;
    #[doc = include_str!("../../../docs/guide/concepts.md")]
    struct Concepts;
    #[doc = include_str!("../../../docs/guide/readers.md")]
    struct Readers;
    #[doc = include_str!("../../../docs/guide/transactions.md")]
    struct Transactions;
    #[cfg(feature = "async")]
    #[doc = include_str!("../../../docs/guide/async.md")]
    struct Async;
    #[doc = include_str!("../README.md")]
    struct CrateReadme;
    #[doc = include_str!("../../../README.md")]
    struct RepositoryReadme;
}

#[cfg(feature = "async")]
pub mod nonblocking;

pub use cell::{Cell, CellEntry, CellRef, Row, RowRef, Value};
pub use check::{
    CheckCounts, CheckOptions, CheckReport, Finding, Region, RegionKind, Severity, Structure,
    WalFile, check,
};
pub use db::{Pigeonhole, PigeonholeReader, Shard, Snapshot};
pub use error::{Error, ErrorCode, Result};
pub use info::{
    Compression, DatabaseInfo, FamilyInfo, FamilyStorage, FileFeatures, LevelInfo, TableInfo,
};
pub use metrics::{CommitStats, CrossShardMetrics, EngineMetrics};
pub use options::{
    Compaction, CpuProfile, Family, IoBackend, IoRings, Options, Priority, ReaderOptions, Recovery,
    days,
};
pub use read::{Condition, RowIter, RowRead, Scan, ValueFilter};
pub use recovery::RecoveryStats;
pub use retry::RetryPolicy;
pub use table::{ReadTable, Table, TableBuilder};
pub use write::{CommitInfo, CommitTicket, Durability, RowMutation, Transaction, WriteBatch};

pub use merge::{MergeError, MergeOperator};
pub use metrics::RowCacheStats;
#[doc(hidden)]
pub use metrics::ShardStats;
pub use recovery::{Failure, FailureCause};

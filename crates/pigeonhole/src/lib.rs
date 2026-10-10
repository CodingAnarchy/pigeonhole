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
//! `cargo add pigeonhole`, or `pigeonhole = "0.1"` in `Cargo.toml`. This is an experimental
//! 0.x release: the core engine (Phase 1) is complete and fault-tested in simulation, but the
//! on-disk format and the API may change before 1.0, the wide-column model (Phase 2) and the
//! latency work (Phase 3) are still to come, and it is not recommended for production use yet.
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
mod db;
mod error;
mod options;
mod read;
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
    #[doc = include_str!("../../../docs/guide/agent-reference.md")]
    struct AgentReference;
    #[doc = include_str!("../../../docs/guide/concepts.md")]
    struct Concepts;
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
pub use db::{Pigeonhole, PigeonholeReader, Shard, Snapshot};
pub use error::{Error, ErrorCode, Result};
pub use options::{Compaction, Family, IoBackend, IoRings, Options, Priority, ReaderOptions, days};
pub use read::{Condition, RowIter, RowRead, Scan, ValueFilter};
pub use table::{ReadTable, Table, TableBuilder};
pub use write::{CommitInfo, CommitTicket, RowMutation, Transaction, WriteBatch};

/// The engine's counters, for [`Pigeonhole::engine_metrics`] (a bench hook, ICR 0015).
#[doc(hidden)]
pub use pigeonhole_engine::Metrics as EngineMetrics;
#[doc(hidden)]
pub use pigeonhole_engine::ShardStats;
pub use pigeonhole_engine::{MergeError, MergeOperator};
pub use pigeonhole_format::Durability;

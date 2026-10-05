//! Embedded, single-file, wide-column store: BigTable's data model with SQLite's deployment
//! model.
//!
//! A Pigeonhole database is a sorted, sparse, versioned map
//! `(table, row, family, qualifier, timestamp) → value` in one file.
//!
//! ```no_run
//! use pigeonhole::{days, Durability, Family, Options, Pigeonhole};
//!
//! # fn main() -> pigeonhole::Result<()> {
//! let db = Pigeonhole::open("crawl.phdb", Options::default())?;
//! let pages = db
//!     .table("pages")?
//!     .family("meta", Family::default().max_versions(1))
//!     .family("links", Family::default().bloom_bits(10))
//!     .family("body", Family::default().blob_threshold(4096).zstd(3).ttl(days(30)))
//!     .create_if_missing()?;
//!
//! // Single-row atomic mutation.
//! pages
//!     .mutate(b"com.example/a")
//!     .put("meta", b"status", b"200")
//!     .put("links", b"com.example/b", b"")
//!     .incr("meta", b"hits", 1)
//!     .delete_column("meta", b"etag")
//!     .commit()?;
//!
//! // Point read: borrows from the cache, no allocation.
//! let status = pages.get(b"com.example/a", "meta", b"status")?;
//!
//! // Row read, projected to families.
//! let row = pages.row(b"com.example/a").families(["meta"]).latest().read()?;
//!
//! // Ordered scan with filters pushed into the block decoder.
//! let snap = db.snapshot();
//! for row in pages
//!     .scan(b"com.example/"..b"com.example0")
//!     .family("links")
//!     .qualifier_prefix(b"org.")
//!     .snapshot(&snap)
//!     .iter()?
//! {
//!     let row = row?;
//! }
//!
//! // Batched multi-row write with one durability point.
//! let mut wb = db.write_batch();
//! wb.put(&pages, b"com.example/c", "meta", b"status", b"404");
//! wb.commit_with(Durability::GroupSync)?;
//! # Ok(())
//! # }
//! ```
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
//! Phase 1 ships the blocking API, which needs no async runtime. The async front door
//! (Phase 3) lives behind the `async` feature in [`nonblocking`].
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]
// Interface freeze: bodies are `todo!()`. Remove this allow when implementing.
#![allow(unused_variables, clippy::ptr_arg)]

mod cell;
mod db;
mod error;
mod options;
mod read;
mod table;
mod write;

#[cfg(feature = "async")]
pub mod nonblocking;

pub use cell::{Cell, CellEntry, CellRef, Row, RowRef, Value};
pub use db::{Pigeonhole, PigeonholeReader, Shard, Snapshot};
pub use error::{Error, ErrorCode, Result};
pub use options::{Compaction, Family, Options, Priority, ReaderOptions, days};
pub use read::{Condition, RowIter, RowRead, Scan, ValueFilter};
pub use table::{ReadTable, Table, TableBuilder};
pub use write::{CommitInfo, RowMutation, Transaction, WriteBatch};

pub use pigeonhole_engine::{MergeError, MergeOperator};
pub use pigeonhole_format::Durability;

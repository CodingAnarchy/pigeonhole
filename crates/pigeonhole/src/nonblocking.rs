//! The async front door (Phase 3). **Not yet implemented.**
//!
//! This module is the reserved place for the async API described in the spec's "Sync and
//! async" section. It will add, over the same engine and with the same semantics:
//!
//! - `Table::get_async`, returning an owned [`Cell`](crate::Cell) (memtable and cache hits
//!   resolve on first poll);
//! - `Scan::stream`, a `futures_core::Stream` of rows with prefetch driven by polling;
//! - `RowMutation::commit_async`, `WriteBatch::commit_async` and `commit_with_async`,
//!   resolving when the record meets the requested durability (dropping the future after
//!   submission does not roll back);
//! - `WriteBatch::commit_with_ticket`, returning a sequence number to wait on later.
//!
//! Futures depend only on `std::task` and `futures-core`, so they run on any executor; the
//! `tokio` feature adds conveniences only. The engine side already exists:
//! `pigeonhole_engine::PendingCommit` and `pigeonhole_io::Completion` are futures.

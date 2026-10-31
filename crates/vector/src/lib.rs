//! An IVF (inverted-file) vector index for Pigeonhole, built on its public API only (decision
//! D306, #490 D; API in ICR 0100).
//!
//! An index is one table of ordinary rows:
//! - each list (a partition of the vectors around a centroid) is one wide row of a vector
//!   family, one column per vector, keyed by the caller's id;
//! - the centroids, the id → list mapping and the lists' lengths are rows beside them.
//!
//! A query finds the `nprobe` centroids nearest the query in memory, then reads those lists
//! with one multi-get and a top-k nearest search (`KeysRead::nearest_*`). Inserts and deletes
//! are optimistic transactions. A list that grows past its bound splits, and one that shrinks
//! below it merges, inside the write that caused it (a `Rebalancer` the application drives
//! comes with D306's second PR). The application never calls a rebalance itself.
//!
//! Each centroid, list, id mapping and length is an ordinary row, so the index is as
//! durable and as consistent as the database: a search reads one snapshot, and every move
//! between lists is one transaction. Distances are [`pigeonhole::VectorSpec`]'s, the ones
//! `nearest_*` ranks by, so a search that probes every list equals an exact search.
//!
//! See [`Index`] for an example.

#![forbid(unsafe_code)]

mod error;
/// Test seams (ICR 0085's convention): only with the `__unstable-hooks` feature, hidden,
/// unstable, and never needed by applications.
#[cfg(feature = "__unstable-hooks")]
#[doc(hidden)]
pub mod hooks;
mod index;
mod layout;
mod point;

pub use error::{Error, Result};
pub use index::{Hit, Index, IndexOptions, IndexStats, Search};

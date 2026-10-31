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
//! below it merges, inside the write that caused it, or in a `Rebalancer` the application
//! drives. The application never calls a rebalance itself.
//!
//! **Status:** scaffolding. The index lands once the vector family's writes (D303) and
//! `nearest_*` (D296) are in `pigeonhole`.

#![forbid(unsafe_code)]

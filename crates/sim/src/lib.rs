//! Deterministic simulation harness and BTreeMap reference model for Pigeonhole.
//!
//! - [`Sim`]: a seeded, single-threaded scheduler over [`SimVfs`](pigeonhole_io::sim::SimVfs)
//!   with simulated time and crash injection.
//! - [`Model`]: the full Pigeonhole semantics (versions, TTL, deletes, merges, snapshots,
//!   durability) as a plain `BTreeMap`. Every other crate's behavior is checked against it.
//! - [`Workload`]: seeded operation generators.
//!
//! Per decision D1 this crate depends only on `io` and `format`; full-stack suites live in
//! `crates/engine/tests` and `crates/pigeonhole/tests` and take this crate as a
//! dev-dependency.
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

mod bucket_state;
mod exact_state;
mod model;
mod sim;
mod streams;
mod workload;

pub use bucket_state::{
    MODEL_HISTOGRAM_BOUNDS, MODEL_QUANTILE_BITS, MODEL_QUANTILE_MAX, bucket_state,
    bucket_state_valid,
};
pub use exact_state::{exact_merge, exact_state, exact_state_valid};
pub use model::{
    COUNTER_TS, CrashWindow, Model, ModelAggregation, ModelCell, ModelError, ModelFamily, ModelOp,
    ModelPurge, aggregation_of_name, combine_aggregate_writes, combine_counter_writes, hll_sketch,
    hll_union,
};
pub use sim::{Rng, Sim, Step, TaskId};
pub use streams::{
    CommitStreams, StreamCommit, StreamRecord, check_acknowledged_survive, commit_records,
    recovered_commits, recovered_from_records, stream_lengths,
};
pub use workload::{Op, Workload, WorkloadSpec};

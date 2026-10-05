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
// Interface freeze: bodies are `todo!()`. Remove this allow when implementing.
#![allow(unused_variables, clippy::ptr_arg)]

mod model;
mod sim;
mod workload;

pub use model::{CrashWindow, Model, ModelCell, ModelFamily, ModelOp};
pub use sim::{Rng, Sim, Step, TaskId};
pub use workload::{Op, Workload, WorkloadSpec};

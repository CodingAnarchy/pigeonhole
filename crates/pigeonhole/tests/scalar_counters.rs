//! The full-stack model check (`model`) again, with D298's bitwise and `f64` aggregate
//! families (`bor`, `band`, `bxor_ttl`, `fmin`, `fmax_ttl`) beside `f`, `counter` and `sum`
//! (`families` picks this set by the test target's name). Eight families, as in `counters`,
//! so a one-shard run with tablet changes off stays at 32 slots (#1212); the families join
//! `counters` once that is fixed (#1214).

#[path = "model.rs"]
mod model;

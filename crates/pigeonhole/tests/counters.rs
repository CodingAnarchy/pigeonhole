//! The full-stack model check (`model`) again, with aggregate families (decisions D179,
//! D294): every table has `f`, `counter` (a family as 0.1.0 created them, with the `i64` add
//! operator), the counter families `sum` and `sum_ttl`, and `min`, `max_ttl`, `hll` and
//! `hll_ttl` (`families` picks this set by the test target's name). Eight families, 32 slots
//! on a shard with tablet changes off; more stall (#1212), so D298's families run in
//! `scalar_counters` until that is fixed (#1214).

#[path = "model.rs"]
mod model;

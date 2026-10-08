//! The full-stack model check (`model`) again, with counter families (decision D179): every
//! table has `f`, `counter` (a family as 0.1.0 created them, with the `i64` add operator),
//! and the counter families `sum` and `sum_ttl` (`families` picks this set by the test
//! target's name). Four families, as in `model`, so an arena with tablet changes off still
//! serves every slot of a shard (16).

#[path = "model.rs"]
mod model;

//! The model-checked simulation suite (`model_check`) again, with counter families
//! (decision D179): every table has `f`, `counter` (a 0.1.0-style family with the `i64` add
//! operator), and the counter families `sum` and `sum_ttl` (`common::families` picks this
//! set by the test target's name). Four families, as in `model_check`, so an arena with
//! tablet changes off still serves every slot of a shard (16).

#[path = "model_check.rs"]
mod model_check;

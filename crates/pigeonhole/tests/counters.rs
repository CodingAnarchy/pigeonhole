//! The full-stack model check (`model`) again, with every aggregate kind (decisions D179, D294,
//! D298): every table has `f`, `counter` (a family as 0.1.0 created them, with the `i64` add
//! operator), and the aggregate families `sum`, `sum_ttl`, `min`, `max_ttl`, `hll`, `hll_ttl`,
//! `bor`, `band`, `bxor_ttl`, `fmin`, `fmax_ttl`, `fsum`, `fmv_ttl`, `fhist`, `fq_ttl` and
//! `bloom` (`families` picks this set by the test target's name). Eighteen families, 72 slots
//! on a shard with tablet changes off: the 64-slot geometry, about 14 chunks each, until a
//! reopen sizes the chunks for them (#1212, D302, #1214).

#[path = "model.rs"]
mod model;

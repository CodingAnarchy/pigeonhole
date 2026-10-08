# 0011: `BenchOp::PutAt` and `BenchOp::GetRow`

**Status:** Approved (issue #54, coordinator task 2026-10-08).

## Change

`pigeonhole-bench` only. `BenchOp` (D105's frozen op set) gains two variants, and
`WorkloadConfig` one field:

```rust
pub enum BenchOp {
    // ... Get, Put, Scan, ReadModifyWrite unchanged ...
    /// Every cell of one row's family.
    GetRow { row: Vec<u8>, family: &'static str },
    /// Cells written at an explicit event time (µs since the Unix epoch).
    PutAt { row: Vec<u8>, family: &'static str, ts: u64, cells: Vec<(Vec<u8>, Vec<u8>)> },
}

pub struct WorkloadConfig {
    // ...
    /// The workload's "now" in µs; 0 = the wall clock when the `Workload` is created.
    pub epoch_micros: u64,
}
```

`epoch_micros` exists because event times are absolute while TTL is judged against the wall
clock: the generator must place points relative to "now", and tests need to pin it.

## Callers

- `Workload` generators (`workload.rs`): YCSB A–D and F reads become `GetRow`
  (`readallfields=true`, closes D112); sparse-wide gains a `GetRow` share; time-series
  loads and appends with `PutAt` (closes D113).
- Every runner (`pigeonhole`, `sqlite`, `rocksdb`, `fjall`) and the read classifier in
  `lib.rs` match on `BenchOp`; all are updated in the same change.
- `BenchOp` is `Clone + PartialEq` and is not matched outside this crate. `WorkloadConfig`
  literals exist only in this crate; the presets set `epoch_micros: 0`.

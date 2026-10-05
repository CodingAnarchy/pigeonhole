//! Arrow RecordBatch export for Pigeonhole scans.
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

// Phase 4: `scan_to_arrow`, exporting a `pigeonhole::Scan` as Arrow `RecordBatch`es with
// columns (row, family, qualifier, ts, value), without per-cell allocation. Nothing is frozen
// here yet; it builds on the public API only.

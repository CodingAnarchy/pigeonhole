# pigeonhole-format fuzz targets

cargo-fuzz targets for the decoders: `key` (keys, values, scan filter), `block` (logical and
physical blocks, filters, SST footer and properties), `wal` (segment headers, frames,
records, batches) and `manifest` (edits and blocks, superblocks, blob headers, shared-memory
structures). Each runs the same never-panic harness as `tests/arbitrary.rs`
(`tests/common/harness.rs`). This crate is its own workspace, excluded from the main one.

```sh
cargo install cargo-fuzz
cd crates/format
cargo +nightly fuzz run block -- -max_total_time=60
```

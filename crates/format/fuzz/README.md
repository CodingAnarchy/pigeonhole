# pigeonhole-format fuzz targets

cargo-fuzz targets for the decoders: `key` (keys, in-row key decoding, key order, values,
scan filters plain and compiled), `block` (logical data and index blocks with every cursor
move, entries decoded as keys, values and block addresses, physical blocks, filters, SST
footer and properties), `wal` (segment headers, frames, records, batches) and `manifest`
(edits and blocks, superblocks with their feature checks, blob headers, shared-memory
structures). Each runs the same never-panic harness as `tests/arbitrary.rs`
(`tests/common/harness.rs`). This crate is its own workspace, excluded from the main one.

```sh
cargo install cargo-fuzz
cd crates/format
cargo +nightly fuzz run block -- -max_total_time=60
```

Seed each corpus with the golden encodings, so the checksummed structures start valid:

```sh
mkdir -p fuzz/corpus/block && cp tests/golden/*.bin fuzz/corpus/block/
cargo +nightly fuzz run -O -a block fuzz/corpus/block -- -max_len=70000
```

`-a` keeps overflow checks on, as in test builds. A crash's minimized input goes in
`tests/fuzz-regressions/<issue>-<slug>.bin`; `tests/fuzz_regressions.rs` replays every file
there through all the harnesses.

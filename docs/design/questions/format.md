# Format questions (Phase 2)

## Proposed decision: the zstd codec, its library and its level (#44; amends D168)
FORMAT.md already reserved codec 2 for zstd and `FamilyOptions::compression_level` (i8, default 3). D168 kept `Family::zstd` refused until a codec existed.

**Interim behavior:**
- **Encoding.** A zstd block's payload is one standard zstd frame (content size included, no dictionary), written at the family's `compression_level` with libzstd's meaning: 1–22, negative for faster, 0 for its default, out-of-range values clamped. As for LZ4, a block stays uncompressed when zstd saves less than 1/8. Decoding goes into a buffer of exactly `uncompressed_len` bytes, so a frame that would decode to more or less is `Corrupt`. The level is a writer setting and is not stored per block. FORMAT.md §4.1 says so.
- **API (additive).** `compress::compress_with_level`, `block::seal_with_level` and `compress::DEFAULT_ZSTD_LEVEL` are new. `compress` and `seal` keep their signatures and use level 3. `SstWriterOptions::compression_level` (the struct is `#[non_exhaustive]`) is set by `for_family`.
- **Library: the `zstd` crate (libzstd via `zstd-sys`; MIT/BSD-3; `cargo deny` passes), not the pure-Rust `ruzstd`.** `ruzstd` 0.9's encoder implements only its fastest level (about zstd level 1), so `zstd(level)` would mean nothing. libzstd gives every level, faster decoding on the read path, and the trained dictionaries the spec mentions for later. Costs: a C toolchain at build time (via `cc`, already in the tree), and Miri cannot run the codec. The format and SST tests skip zstd under `cfg(miri)`, and CI's Miri job does not cover these crates anyway. The FFI's `unsafe` stays inside that crate, so `pigeonhole-format` keeps `#![forbid(unsafe_code)]`.
- **Public crate.** `Family::zstd(level)` is accepted, so `Family::to_engine` refuses nothing and is now infallible. The engine and public model harnesses store family `f` with zstd, so the seed sweeps cover it.
- **Not yet.** Trained dictionaries: FORMAT.md's reserved "compression dictionary address" stays absent.

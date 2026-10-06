# 0005: `pigeonhole::Options::wal_segment_size` — hidden test hook

**Status:** Approved (coordinator, 2026-10-06). Implemented in PR #47.

## Change

Add one hidden builder method next to the existing hidden `Options::vfs` hook:

```rust
impl Options {
    #[doc(hidden)]
    pub fn wal_segment_size(self, bytes: u64) -> Self;
}
```

It sets `EngineOptions::wal.segment_size` (a multiple of 32 KiB, at most 4 GiB − 32 KiB,
decision D43; validated by the engine at open). Unset keeps the 64 MiB default. Like the
segment size itself, it also lowers the largest value a commit accepts (decision D16), and
`ValueTooLarge` messages report that limit.

## Why

The frozen API's only simulation hook is `Options::vfs`. With 64 MiB segments, every open
on `SimVfs` zero-fills segment files in memory: about 0.5 s per open and 0.9 s for the first
table in a debug build, against 2 ms and 6 ms with the 256 KiB segments the engine's suites
use. The public model suite reopens after every crash, so it spent most of its ~70 s there.

## Callers

- `crates/pigeonhole/tests/model.rs`, `tests/api.rs`, `tests/alloc.rs`: the simulation
  suites (256 KiB segments).
- No application or other crate. Hidden from rustdoc; `cargo-semver-checks` ignores
  `#[doc(hidden)]` items.

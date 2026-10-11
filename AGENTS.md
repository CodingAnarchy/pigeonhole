# Agent guide

This file orients coding agents (Claude, Codex, Gemini, and others) working **on** this repository. If you want to **use** Pigeonhole from your own project, read [`docs/guide/README.md`](docs/guide/README.md) instead.

## What this is
Pigeonhole is an embedded, single-file, wide-column store in Rust: BigTable's data model with SQLite's deployment model. The design lives in [`docs/design/spec.md`](docs/design/spec.md). The per-crate work packages are in [`docs/design/task-briefs.md`](docs/design/task-briefs.md). Decisions that refine the spec are in [`docs/design/decisions/`](docs/design/decisions/README.md).

## Before you write code
1. Read [`CONTRIBUTING.md`](CONTRIBUTING.md) — the workspace and agent rules are binding.
2. Read your crate's brief and the spec sections it names.
3. Read the [decisions index](docs/design/decisions/README.md), then the decisions that touch your crate (they win over the spec). Don't read every decision file.
4. Read [`FORMAT.md`](FORMAT.md) if you touch bytes on disk or in shared memory.
5. Check [`docs/status.md`](docs/status.md) for what exists and what is in flight.

## Commands
Builds go through [`cargo-quota`](https://crates.io/crates/cargo-quota), which keeps every worktree's `target/` under one shared disk budget (see `CONTRIBUTING.md`):
```sh
cargo quota build --workspace
cargo quota test --workspace --all-features
cargo quota clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all
cargo +nightly miri test -p <unsafe-crate>                 # io, cache, memtable
RUSTFLAGS="--cfg loom" cargo test --release -p <crate> --lib loom
cargo deny check
```

## Conventions
- Rust 2024 edition, MSRV 1.96.
- Errors: each crate has its own `Error` enum; the public crate exposes a flat, stable `ErrorCode` enum plus message (C-ABI friendly).
- Integers on disk are little-endian except ordering fields inside internal keys, which are big-endian.
- Tests that use randomness take a seed and print it on failure.
- Keep Rust-only types (borrowed lifetimes, generics, closures, trait objects) out of anything that will become the C ABI boundary.

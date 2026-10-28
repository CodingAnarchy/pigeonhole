# 0085: `pigeonhole`'s test and bench hooks move behind a non-default `__unstable-hooks` feature; `allow_unregistered_merge_operators` and `PIGEONHOLE_TAIL_INDEX` go

**Status:** Approved (owner via the coordinator, 2026-10-28; #695: API audit 1a and 1b, c6480352190, options inventory R1, R2, R3 and R5, c6480419472, and the owner's addition of `tablet_changes` and `tablet_packing`). Breaking before the 1.0 freeze. No file-format or behaviour change at the defaults.

## Change
**The new feature.** `pigeonhole` gains `__unstable-hooks`, off by default. The double underscore follows the usual "not a feature" convention, and `cargo semver-checks` leaves it out. Only with the feature do these exist:

| Item | Before | Users |
|---|---|---|
| `Options::{vfs, wal_segment_size, tablet_balance, counter_checkpoint_steps, nowait_reads}`, `ReaderOptions::vfs` | `#[doc(hidden)]` | simulation, model and api tests; arrow and cli tests; bench |
| `Options::{commit_spin, shard_spin}` (R2) | public | bench |
| `Options::adaptive_shard_threads` (R3) | public, experimental | bench |
| `Options::{tablet_changes, tablet_packing}` (owner) | public, default on | bench, api/backpressure/serializability/model tests |
| `Pigeonhole::{shard_stats, pack_target, driver_target, force_driver_target, driver_trace, commit_latency_buckets, commit_latency_floor, tablet_refusals, async_commit_inflight, nowait_reads}`, `ShardStats` | `#[doc(hidden)]` | bench, tests |
| `Table::split_over_shards` | `#[doc(hidden)]` | bench, app-owned test |
| `hooks::with_watch_registry_locked_elsewhere` (1b) | `doc_support::with_watch_registry_locked_elsewhere` | fork_registry test |

**Who turns the feature on.**
- `pigeonhole`'s own tests, through a self dev-dependency (`pigeonhole = { path = ".", features = ["__unstable-hooks"] }`).
- The arrow and cli tests, through their dev-dependency.
- The bench, through its dependency.
- The callgrind and gate builds (`-p pigeonhole-bench`) therefore have it, as before.

**Environment variables (R5).**
- `PIGEONHOLE_IO` and `PIGEONHOLE_DIRECT` are read only with the feature. Without it, an unset `io_backend` is `Auto` and an unset `direct_io` is off, whatever the environment says. CI's suites and the bench have the feature, so they read the variables as before.
- `PIGEONHOLE_TAIL_INDEX` is gone. The stale-tail index has been on by default since D199, and `EngineOptions::memtable_tail_index` still turns it off in engine tests.
- `PIGEONHOLE_TRACE` stays, and CONTRIBUTING documents it.

**Tablet changes and packing are always on.** Their defaults become the standard. The bench keeps both switches through the feature. `phdb` (`pigeonhole-cli`) used `tablet_changes(false)` in its writer options. It now drops the call rather than turn hooks on in the shipped binary: it opens one shard, so tablets never move, and its small one-shot sessions never reach a size split.

**Removed (R1).** `Options::allow_unregistered_merge_operators` is removed. It was deprecated, and its body was `self.read_only_fallback(yes)`; use `read_only_fallback`.

**Left as they are (1c).** The `doc_support` helpers stay `#[doc(hidden)]` in every build, because the guide's doctests and the READMEs use them. `impl From<pigeonhole_engine::Error> for Error` stays too: a trait impl can't be gated without rewriting every `?` in the crate, and users can't name the engine's type without depending on the engine directly.

## Why
A default build exported 18 test and bench hooks, and three of them named internal-crate types (`VfsRef`, `NowaitReads` and the engine's `Error`). concepts.md said hidden items were outside the promise, but they still compiled into every application and `cargo semver-checks` guarded them.

The spin windows and the adaptive controller are polling and driver internals. Freezing them would freeze the mechanism: #749 measured `shard_spin` at 200, 50, 10 and 0 µs within noise on the gate's async shape, and D289's controller is still being tuned (#1015).

A process that inherited `PIGEONHOLE_IO` or `PIGEONHOLE_DIRECT` silently switched backend.

## Callers changed
- **Tests:** `crates/pigeonhole/tests/fork_registry.rs`, for the module move.
- **Manifests:** `crates/{pigeonhole, arrow, cli, bench}/Cargo.toml`.
- **Docs:** the agent reference, getting-started, async.md, concepts.md, the crate README and CONTRIBUTING's environment table.

The applications this breaks are those that called `commit_spin`, `shard_spin`, `adaptive_shard_threads`, `tablet_changes`, `tablet_packing` or `allow_unregistered_merge_operators`. They drop the call: the defaults stand, and `read_only_fallback` replaces the alias. The upgrade notes in release-1.0 say so.

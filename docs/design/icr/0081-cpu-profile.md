# 0081: the CPU budget's profile (D289)

**Status:** Approved (coordinator, 2026-10-27, for the owner's D289 scope: #760 comments 6443979201, 6444135824 and 6446722456). It lands in D289 PR 2.

## Change

All additions; nothing is removed or renamed.

- **`pigeonhole`:**
  - `#[non_exhaustive] pub enum CpuProfile { Embedded, Server }`; `Embedded` is the default.
  - `Options::cpu_profile(CpuProfile) -> Self`.
  - The profile selects the share of the physical cores (after the affinity mask and a container's CPU quota) that flush and compaction get by default:
    - `Embedded`: a quarter;
    - `Server`: a half;
    - either way at least one, and at most one per shard.
  - The driver ceiling is one per core in both. Explicit `shard_threads` and `compaction_cores` win.
  - Application-owned opens ignore it (D40).
- **`pigeonhole-bench`** (not published): `PigeonholeRunner::compaction_cores(c)` and `server_profile()`; `--compaction-cores C` and `--server-profile`.

The thread counts per core count, for both profiles, are in D289 ("The CPU budget") and the guide.

## Why

The owner's D289 scope: one budget function derives every thread default from the CPU the process really runs on. Shard drivers and compaction get fractions with caps that scale (D252's compaction was fixed at four), and the application keeps explicit headroom. A database that has its container to itself, with its clients elsewhere (the owner's server mode), can give the engine most of it. The profile is that choice, as one option, `#[non_exhaustive]` for later profiles.

## Callers

- **`pigeonhole`:** `options.rs` (`CpuProfile`, `thread_budget`, `to_engine_for`; unit tables for 1–128 cores and the explicit cases); `lib.rs` re-exports `CpuProfile`; tests `api.rs` (`engine_owned_mode_sizes_its_threads_from_the_cpu_budget`).
- **`pigeonhole-bench`:** `runners/pigeonhole.rs`, `main.rs`.

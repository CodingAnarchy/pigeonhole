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
  - The driver count is a thread per shard in both (one per core with the default shard count). That's the load controller's ceiling when it is opted in (`Options::adaptive_shard_threads`, ICR 0079; off by default). Explicit `shard_threads` and `compaction_cores` win.
  - Application-owned opens ignore it (D40).
- **`pigeonhole-bench`** (not published): `PigeonholeRunner::compaction_cores(c)` and `server_profile()`; `--compaction-cores C` and `--server-profile`.

The thread counts per core count, for both profiles, are in D289 ("The CPU budget") and the guide.

## Why

The owner's D289 scope: one budget function derives every thread default from the CPU the process really runs on. Shard drivers and compaction get fractions with caps that scale (D252's compaction was fixed at four), and the application keeps explicit headroom. A database that has its container to itself, with its clients elsewhere (the owner's server mode), can give the engine most of it. The profile is that choice, as one option, `#[non_exhaustive]` for later profiles.

## Callers

- **`pigeonhole`:** `options.rs` (`CpuProfile`, `thread_budget`, `to_engine_for`; unit tables for 1–128 cores and the explicit cases); `lib.rs` re-exports `CpuProfile`; tests `api.rs` (`engine_owned_mode_sizes_its_threads_from_the_cpu_budget`).
- **`pigeonhole-bench`:** `runners/pigeonhole.rs`, `main.rs`.

## Amendment (owner, 2026-10-28; #1015, ICR 0085's amendment)
The load controller is on by default (`Options::adaptive_shard_threads`, now public). The engine-owned driver count above, a thread per shard, is the controller's ceiling by default rather than only when it is opted in. `adaptive_shard_threads(false)` keeps that count fixed, as does a set `shard_threads`.

## Amendment (owner, 2026-10-30; #760, D198's 2026-10-30 amendment)
`CpuProfile::Server` also sets the shard's spin before parking to 1 ms; `Embedded` keeps the engine's 200 µs. The documented behaviour of `CpuProfile::Server` and `Options::cpu_profile` grows by that sentence; no signature changes. The window's own setter, `Options::shard_spin`, stays a hidden `__unstable-hooks` hook (ICR 0085), and an explicit window wins over the profile's.

# 0142: what a cluster balancer measures: tablets, device bytes, a thread's CPU time (D332 PR 1)

**Status:** Approved (owner, 2026-11-16, D332: "an ICR line for the unstable `tablets()`, with sampled per-tablet read counts"; the shape below as amended by perf287's review of the PR 1 plan, 2026-11-18).

## Why

D332's balancer (#1485) compares partitions' loads across a cluster's nodes and, for a partition too big or too hot, splits it at a tablet boundary that halves it. Its measures (perf287's L1, `.coord/cli692-design-1485.md` Amendment 2):

- a partition's CPU share, `(max(B, sync_busy) + sampled request CPU) ÷ cores`;
- its device share, device bytes ÷ the node's device ceiling;
- for a split key, each tablet's range, bytes, write load and reads.

B and sync_busy are already in `Metrics` (`shard_busy_nanos`, `group_sync_nanos`). This request adds the rest. Everything is doc-hidden and unstable, for serve's cluster mode only.

## Change

**`pigeonhole-engine`:**

```rust
impl Engine {
    /// Every tablet's row range, SST bytes (a shared SST's divided among its tablets) and
    /// write load (its shard's moving average of rows written per balancer interval, as
    /// published at its last pass). Not part of the stable API.
    #[doc(hidden)]
    pub fn tablets(&self) -> Vec<TabletStats>;
}

#[doc(hidden)] #[non_exhaustive]
pub struct TabletStats { pub table: String, pub start: Vec<u8>, pub end: Option<Vec<u8>>, pub bytes: u64, pub write_load: f64 }

pub struct Metrics {
    // ...
    /// Bytes the database's files read since open: on-disk sizes, readahead included.
    pub device_read_bytes: u64,
    /// Bytes they wrote since open: the WAL's, flushes' and compactions'.
    pub device_write_bytes: u64,
}
```

- **Tablet loads:** each shard stores its per-tablet averages, whole, in `Shared::tablet_loads` (`ArcSwap`, one per shard) at each balancer pass. `tablets()` loads them beside the view's tablet map. That's in the concurrency inventory.
- **Device bytes:** `Engine::open` wraps its `EngineOptions::vfs` in a `CountingVfs`, which forwards every `Vfs` method, the defaulted ones included. Every file it opens is the backend's own, given the database's counters (`File::count_into`, below; `Shared::io`): the backend adds the buffer length of each `read_at`, `submit_read`, `submit_reads_ahead`, `write_at` and `submit_write`. (Amended on #2063; see "Amendment" below.)
  - Syncs, writeback hints, allocations and punches count nothing.
  - A read counts the length asked for at submission, so a short or failed read counts in full.
  - Reads the OS page cache served count as well. These are the engine's estimate, for comparing databases on one node, not a disk utilisation figure.
  - What a file moved before it got the counters (an open's own reads) isn't counted.

**`pigeonhole-io`:**

```rust
/// CPU time the calling thread has run, in nanoseconds since an arbitrary start.
pub fn sys::thread_cpu_nanos() -> u64;
```

- Unix uses `CLOCK_THREAD_CPUTIME_ID`, a system call on Linux.
- Windows uses `QueryThreadCycleTime`, scaled by a cycle rate measured once against `Instant` (QPC) (perf287's S1, over `GetThreadTimes`' 15.6 ms ticks; simp724's S4 on #2063).
- `Vfs` gains a doc line: a new method must also be forwarded by `CountingVfs`.

```rust
/// Bytes read and written by the files given it: two counters, each on its own cache line.
pub struct IoCounters { /* ... */ }
impl IoCounters {
    pub fn new() -> Self;
    pub fn totals(&self) -> (u64, u64);
}

pub trait File {
    // ...
    /// Counts this file's I/O into `counters` from now on: the buffer length of each read and
    /// write as it's submitted. A second call with other counters is a bug (debug-asserted).
    /// The default counts nothing; a wrapping `File` must forward it.
    fn count_into(&self, counters: Arc<IoCounters>) { let _ = counters; }
}
```

- pread, io_uring (`UringFile`, whose `submit_reads_ahead` is its own) and the simulator count at their read and write entry points, through one helper each, so no I/O counts twice. Windows is pread's.

**`pigeonhole`** (`__unstable-replication`, doc-hidden, in `replication.rs`):

```rust
impl Pigeonhole {
    pub fn device_bytes(&self) -> (u64, u64);
    pub fn tablets(&self) -> Vec<TabletLoad>;
}
#[non_exhaustive]
pub struct TabletLoad { pub table: String, pub start: Vec<u8>, pub end: Option<Vec<u8>>, pub bytes: u64, pub write_load: f64 }
```

- No stable `Metrics` field is added: serve reads the device bytes through `device_bytes()`.

**Reads per tablet aren't counted in the engine** (perf287, 2026-11-18). Serve samples one read in 64 per thread and keeps its table and first key in the partition's reservoir. It matches the keys to `tablets()`' bounds only when the balancer asks, so the engine's read path doesn't change, and bounds that change mid-window lose nothing.

## Callers

- `phdb serve`, in cluster mode: `serve/cluster/load.rs`, the per-partition load behind `LocalPartition.load_cpu = 29`, `.load_device = 30` and `.load = 31`, and `--balance-window`.
- D332 PR 3's `Forward` `tablets` arm (51 and 29).
- Nothing else.

## Tests

- `crates/engine/tests/tablets.rs`: `tablets()`'s ranges tile the table, and the loads published after balancer passes.
- `crates/engine/tests/it/device_bytes.rs`: write bytes rise under commits and a flush, read bytes under uncached reads (block cache 0), and neither when idle.
- `crates/engine/src/io_count.rs`: the wrapper's defaulted `Vfs` getters answer as the backend's, and a file it opens counts exactly the bytes of each read and write entry point, once, with readahead and a read past the end counted and syncs not, on `SimVfs`, pread and (on Linux) io_uring (perf287's musts).
- `crates/io/src/sys.rs` doctest: the thread CPU clock advances with work.
- `serve/cluster/load.rs`: the sampling countdowns, the fold, the reservoir, the per-poll timer and the ceiling.

## Amendment (#2063, 2026-11-18; coordinator; perf287 agreed)
**Counting moved from a wrapping `File` into the io backends.** PR 1 first counted in `CountingFile`, a `File` around every file the engine opened. The database is one file, WAL included, so every commit's WAL write and sync took an extra dynamic call through it. x86 callgrind put seven commit shapes over their D193 ceilings, with commit-overwrite +0.6% against base.

Now:
- `CountingVfs` gives each file it opens the database's `IoCounters` (`File::count_into`), and returns the backend's file itself.
  - It stays for opens: the database's files are opened in the engine, pigeonhole-wal (its streams, recovery and feed) and pigeonhole-pager. One wrapper gives every open its counters, where an open added later without a `count_into` would go uncounted silently (the coordinator's call).
- The backends count at their entry points: a load, a branch and an add per I/O.
- The counters are two plain atomics, not per-thread stripes (perf287).
- **Clock reads go to the backend itself:**
  - `Shared::clock_vfs`, and the runtime's `RuntimeConfig::vfs`, are the unwrapped VFS. Every `monotonic_nanos` and `now_micros` the engine reads goes through them.
  - `Shared::vfs`, the counting wrapper, is for opens and file-system calls.
  - A scratch x86 callgrind A/B (run 44337403193) found 40 of the wrapper's 61 instructions a commit in clock reads through it, and 13 in the per-file wrapper.
- **The test that a missed open would fail:** on the simulator, a database working through four WAL streams, flushes and compaction, and the feed's sidecar (written, and read by a subscription) counts exactly the bytes the simulator's files moved (`SimVfs::bytes_moved`).

This reverses perf287's earlier choice of one wrapper and no backend change. It also removes that wrapper's hazard, a defaulted `File` method it didn't forward.

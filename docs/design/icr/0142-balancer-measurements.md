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
- **Device bytes:** `Engine::open` wraps its `EngineOptions::vfs` in a `CountingVfs`. Every file it opens is a `CountingFile` that forwards every `File` method, the defaulted ones included, and adds the buffer length of each `read_at`, `submit_read`, `submit_reads_ahead`, `write_at` and `submit_write` into the database's striped counters (`Shared::io`).
  - Syncs, writeback hints, allocations and punches count nothing.
  - A read counts the length asked for at submission, so a short or failed read counts in full.
  - Reads the OS page cache served count as well. These are the engine's estimate, for comparing databases on one node, not a disk utilisation figure.
  - perf287's choice over counting in each I/O backend: one point, with no unsafe or backend change.

**`pigeonhole-io`:**

```rust
/// CPU time the calling thread has run, in nanoseconds since an arbitrary start.
pub fn sys::thread_cpu_nanos() -> u64;
```

- Unix uses `CLOCK_THREAD_CPUTIME_ID`, a system call on Linux.
- Windows uses `QueryThreadCycleTime`, scaled by a cycle rate measured once against `Instant` (QPC) (perf287's S1, over `GetThreadTimes`' 15.6 ms ticks; simp724's S4 on #2063).
- `File` and `Vfs` each gain a doc line: a new method must also be forwarded by `CountingFile`/`CountingVfs`.

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
- `crates/engine/src/io_count.rs`: every defaulted `Vfs`/`File` getter answers through the wrapper as the backend does, on `SimVfs` and on pread (perf287's must), and reads and writes are counted, syncs not.
- `crates/io/src/sys.rs` doctest: the thread CPU clock advances with work.
- `serve/cluster/load.rs`: the sampling countdowns, the fold, the reservoir, the per-poll timer and the ceiling.

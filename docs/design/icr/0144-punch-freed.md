# 0144: punching the data file's free space

**Status:** Approved (coordinator, 2026-11-17; #2013, D333).

## Change

Three crates, all additive. PR 1 is `pigeonhole-io` and `pigeonhole-pager`; PR 2 is the engine's background pass and the option.

**`pigeonhole-io`:** a defaulted trait method.

```rust
pub trait File {
    /// Gives the filesystem back the blocks under `[offset, offset + len)`, keeping the
    /// file's length: the range then reads back as zeros.
    fn punch_hole(&self, offset: u64, len: u64) -> Result<()> {
        Err(Error::new(ErrorKind::Unsupported, "punch_hole: not supported here"))
    }
}
```

- **Linux** (`PreadFile`, and `UringFile` through it): `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)`. `EOPNOTSUPP` or `ENOSYS` is `ErrorKind::Unsupported`.
- **macOS:** `fcntl(F_PUNCHHOLE)`. `ENOTSUP` is `Unsupported`. Its offset and length must be multiples of the filesystem's block size; `EINVAL` from a misaligned range is an error, not `Unsupported` (simp724's S1). The pager only punches whole 64 KiB units.
- **Windows and other platforms:** the default, `Unsupported`. Punching there needs a sparse file (`FSCTL_SET_SPARSE`, then `FSCTL_SET_ZERO_DATA`), which nothing sets today.
- **`SimVfs`:** zeroes the range, clamped to the file's length, as an unsynced write of zeros. After a power loss the range holds its old bytes or the zeros, sector by sector under `torn_writes` (simp724's S3). It draws nothing from the fault RNG and injects no error, so turning punching on leaves a seed's faults where they were. It's recorded as a `SimOp::Write` of zeros, so `SimOp` gains no variant.
- **Wrappers** that forward `allocate` forward it too (the bench's `InlineFile` and `TimedFile`). Test wrappers keep the default, which only means punching is refused under them.

**`pigeonhole-pager`:**

```rust
impl Pager {
    /// Gives one block of free space back to the filesystem: of the free blocks past the
    /// lowest `reserve` bytes of free space, the largest with space not yet punched.
    /// `None` if there is nothing to punch, or if another call holds a block right now.
    pub fn punch_free(&self, reserve: u64) -> Result<Option<u64>>;
}

#[non_exhaustive]
pub struct PagerStats {
    // ...
    pub punched_bytes: u64, // free bytes now known to be holes
    pub punches: u64,       // blocks punched since open
    pub punch_bytes: u64,   // bytes given back since open
    pub punch_nanos: u64,   // the punches' wall-clock time
}
```

**`pigeonhole-engine` and `pigeonhole` (PR 2):**

```rust
impl pigeonhole::Options {
    pub fn punch_freed(self, yes: bool) -> Self; // on by default (D333, after g153)
}

pub struct EngineOptions {
    // ...
    pub punch_freed: bool,
    #[doc(hidden)]
    pub punch_tuning: Option<PunchTuning>, // tests: smaller reserve and trigger
}

#[non_exhaustive]
pub struct PunchTuning {
    pub min_reserve: u64,   // 64 MiB
    pub pass_bytes: u64,    // 256 MiB
    pub bytes_per_sec: u64, // 64 MiB/s
    pub trigger_bytes: u64, // 64 MiB
}
```

- **Metrics** (engine `Metrics`, and public `EngineMetrics` fields, so operators see them; the coordinator, 2026-11-17): `file_punches`, `file_punch_nanos`, `file_punch_bytes`, `file_punched_bytes` (a gauge), `file_punch_passes` and `file_punch_refusals`. `pigeonhole-otel` (and so `phdb serve`'s `/metrics`) exports them as `pigeonhole.file.punches`, `.punch.time`, `.punched` (bytes), `.holes` (the gauge), `.punch.passes` and `.punch.refusals`.
- **When a pass runs:** after a manifest commit's reclaim (or a shrink's), if the free space that isn't a hole yet is at least the reserve plus `trigger_bytes`, and no pass is running. A pass that finds nothing sets an idle mark: the next waits until that free space grows by `trigger_bytes` (simp724, on #2033).
- **A pass:** a background task, offloaded to a worker when there is one. It punches one block per slice and stops at `pass_bytes` or at the first `None`, sleeping between blocks to hold `bytes_per_sec` (a simulated clock does not pace). `Unsupported` stops passes until reopen; any other error ends the pass, counted as a refusal.

## Why

#2013: a long-lived database's data file keeps its high-water size. Space freed by compaction is reused, but never given back.
- `truncate_tail` returns only a free tail; free space in the middle of the file stays allocated on disk.
- After a large delete or a TTL expiry, the file can hold several times its live data.

## Semantics

- **Only free space is punched.** That's space the allocator would already hand out and overwrite: no root that may be recovered names it (`reclaim` clamps to the durable root, D61), and no live view or reader-process pin reaches it. Punching needs nothing that reuse doesn't already need.
- **One block at a time.** The pass takes one free block out of the free map (held as a placeholder), punches it with the allocator lock released, and returns it.
  - While it's held, no allocation lands there, no relocation or truncation moves it, and no region is cleared around it (simp724's C2).
  - The next large allocation does not grow the file for want of it: `wants_ahead` counts it as free.
- **The reserve:** the lowest `reserve` bytes of free space are never punched. The allocator hands out low space first, and a write into a hole allocates on the filesystem again.
- **Holes are tracked** as free-unit ranges, so a later pass skips them. An allocation takes its range out, and so does a truncation.
- **Not durable or ordered:** a power loss may leave a punched range zeroed or not. Either is fine, since nothing recoverable names it. The holes aren't persisted; after a reopen the first passes punch the same space again, which changes nothing on disk.
- **Failures:** a failed punch records nothing and returns the error. `Unsupported` means the filesystem can't punch, and the engine stops trying until the next open.

## Open questions
_None._

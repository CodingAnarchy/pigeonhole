# 0070: `RingStats` and `IoRings` count failed ring attaches and inline operations

**Status:** Approved (coordinator, 2026-10-26; perf287's review of #929's fix). Additive: both structs are `#[non_exhaustive]`.

## Change

**`pigeonhole-io`:** `RingStats` gains two fields:
- `attach_failures: u64`: times a thread could not get a ring of its own, because the kernel refused one (`ENOMEM`) or lacks a feature the ring needs. It's counted per attempt; the thread tries again later.
- `inline: u64`: operations such a thread ran inline (`pread`/`pwrite`/`fdatasync`, as the pread backend does) on a backend with no reaper thread (application-owned mode).

**`pigeonhole`:** `IoRings` gains the same two fields, mapped from `RingStats`.

**Test hook:** `pigeonhole_io::uring::fail_ring_attaches(n)` (`#[doc(hidden)]`, not part of the API) makes the next `n` attaches fail through the same path as a missing kernel feature. `usize::MAX` fails every attach.

## Why

#929: in application-owned mode a thread whose ring attach failed sent its operations to the shared ring, which nothing reaps in that mode. Its WAL syncs completed in the kernel but never resolved, a flush's durability barrier waited forever, and the shards livelocked. Such a thread now runs its operations inline. Both counters make a run that degraded this way visible (the bench JSON records them).

## Callers

- **`pigeonhole-io`:**
  - `uring.rs`: `ThreadRing::attach` counts failures and schedules a retry; `UringFile::inline_here` counts inline operations; `UringVfs::ring_stats` reports both.
  - `tests/uring.rs`: the attach-failure, every-attach-fails, retry and inline-sync tests.
- **`pigeonhole`:**
  - `db.rs`: `Db::io_rings` maps both fields.
  - `tests/app_owned_uring_no_rings.rs`: asserts both counters.
- **`pigeonhole-bench`:** `Stalls::{ring_attach_failures, ring_inline}`, from `Pigeonhole::io_rings`.

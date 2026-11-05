# 0116: the engine metric getters behind D316's A2–A4

**Status:** Approved. The coordinator assigned the number (2026-11-04), and the owner's Q2 on #1458 the same day put A2–A4 in 1.2.0. The design is D316 rev 2, reviewed by perf287 (hot path) and cli692 (API). Each getter lands in its own PR.

## The change
- **A2, memtable bytes** (this ICR's first PR). In pigeonhole (semver, additive), two `EngineMetrics` fields:
  - `memtable_bytes` (gauge): the arena bytes memtables take now, summed over shards. It counts whole chunks, partly filled ones included, so it reads high by up to one chunk per live memtable. It also counts flushed memtables whose chunks aren't reusable yet, because a snapshot or an in-flight read still holds them (#1477, #1506). That's right for arena pressure, which they add to as surely as live ones (perf287).
  - `memtable_limit_bytes` (gauge): the arenas' size, summed over shards. That's `Options::memtable_budget`, which is per shard, rounded up to 2 MiB, times the shard count.
  - **Approximate against stalls (cli692):** a commit can wait for room while `memtable_bytes` reads well under the limit. Reserved room, a share held until its decision, and a too-long contiguous run all do it, so `room_stalls` stays the ground truth.
  - **How it's published, at no cost on the commit path:** each shard's `ShardArena` mirrors its free bytes into a shared `AtomicU64` (`ShardArena::set_free_sink`, the shard's `ShardMetrics::memtable_free`). It does one relaxed store in the two places its free count changes: `allocate`, when a memtable's current chunk can't fit an entry (about once per 256 KiB of writes), and `free_runs`, when a retired memtable's chunks come back. `Engine::metrics` reads it and subtracts it from the arena's size, which is stored once when the shard starts. An insert that fits in its chunk stores nothing, and nothing runs per batch.
  - **The first version** published at the end of every batch (perf287's option (a)). It cost 10–13 instructions per commit on the single-commit callgrind shapes, which put `commit-one` and `commit-async` over their ceilings, since `commit-one`'s ceiling was base + 1. The coordinator asked for this cold-path version instead (#1512).
  - **pigeonhole-engine** (internal, additive): `Metrics::memtable_bytes` and `Metrics::memtable_limit`.
  - **pigeonhole-otel:** `pigeonhole.memtable.size` and `pigeonhole.memtable.limit` (D316).
- **A3, group-sync time:** `EngineMetrics::group_sync_nanos`, named to pair with `group_syncs` (D316 rev 2 called it `wal_sync_nanos`). It sums the shards' existing `sync_busy_nanos` (D248), with no new atomic and no new clock read, so the commit path is unchanged. A sync still in flight shows up when it finishes. It's exported as `pigeonhole.wal.group_sync.time`. The engine adds `Metrics::group_sync_nanos`.
- **A4, feed lag** (a later PR): `Feed::progress() -> FeedProgress`, a cheap `Clone + Send` handle to the last delivered seqno, stored once per delivered batch, and `Pigeonhole::visible_seqno()`. `pigeonhole-otel` gains `Registration::watch_feed(name, progress)`, exported as `pigeonhole.feed.lag`.

## Why
#1458 lists memtable bytes, WAL sync time and feed lag among the signals operators need, and the engine exported none of them (D316's gaps). Memtable bytes against the limit is the early warning for `room` stalls, before the commits wait.

## Callers changed
None: everything is additive. `pigeonhole-otel` is the first caller.

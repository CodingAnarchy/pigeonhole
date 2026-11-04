# 0116: the engine metric getters behind D316's A2–A4

**Status:** Approved. The coordinator assigned the number (2026-11-04), and the owner's Q2 on #1458 the same day put A2–A4 in 1.2.0. The design is D316 rev 2, reviewed by perf287 (hot path) and cli692 (API). Each getter lands in its own PR.

## The change
- **A2, memtable bytes** (this ICR's first PR). In pigeonhole (semver, additive), two `EngineMetrics` fields:
  - `memtable_bytes` (gauge): the arena bytes memtables take now, summed over shards. It counts whole chunks, partly filled ones included, so it reads high by up to one chunk per live memtable.
  - `memtable_limit_bytes` (gauge): the arenas' size, summed over shards.
  - **How it's published:** each shard publishes at the end of every batch, in `ShardState::publish_arena` (perf287's option (a)). It compares the bytes with the last value it published, kept in a plain shard field, and does a relaxed store into its `ShardMetrics::memtable_used` only when they differ. That's one compare per group, never per commit, with no new synchronization; the memtable crate takes no atomic. The limit is stored once, at the shard's first publish.
  - **pigeonhole-engine** (internal, additive): `Metrics::memtable_bytes` and `Metrics::memtable_limit`.
  - **pigeonhole-otel:** `pigeonhole.memtable.size` and `pigeonhole.memtable.limit` (D316).
- **A3, group-sync time** (a later PR): `EngineMetrics::wal_sync_nanos`, summing the shards' existing `sync_busy_nanos` (D248) with no new atomic. It's exported as `pigeonhole.wal.group_sync.time`.
- **A4, feed lag** (a later PR): `Feed::progress() -> FeedProgress`, a cheap `Clone + Send` handle to the last delivered seqno, stored once per delivered batch, and `Pigeonhole::visible_seqno()`. `pigeonhole-otel` gains `Registration::watch_feed(name, progress)`, exported as `pigeonhole.feed.lag`.

## Why
#1458 lists memtable bytes, WAL sync time and feed lag among the signals operators need, and the engine exported none of them (D316's gaps). Memtable bytes against the limit is the early warning for `room` stalls, before the commits wait.

## Callers changed
None: everything is additive. `pigeonhole-otel` is the first caller.

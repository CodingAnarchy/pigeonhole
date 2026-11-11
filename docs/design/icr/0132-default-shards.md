# 0132: `Options::default_shards`, an application's shard count for opens that set none

**Status:** Approved. The coordinator assigned the number and chose this fix (2026-11-10), after perf287's g70d on #1697 found `phdb serve` reopening with the library's count.

## The change
- **New public name:** `pigeonhole::Options::default_shards(n)`, default `0`. It's the shard count an open uses when `shards` is unset and the database has no count of its own: a new file, or an existing one without a change feed. `0` keeps the library's one shard per physical core (D220).
- **Internal name (not semver-checked):** `pigeonhole_engine::EngineOptions::default_shards: usize`, default `0`. `EngineOptions` is `#[non_exhaustive]`. The engine's resolution of an unset count becomes, in order:
  1. a change feed's recorded count (D310's amendment), as before;
  2. `default_shards`, when it's above 0;
  3. one per physical core.
- **The bound** on `shards` (at most 65,536) applies to `default_shards` too.
- **Compaction threads:** with `shards` unset, the CPU budget clamps them to `default_shards`, as it would an explicit count (at most one per shard; perf2 on #1699). The drivers' ceiling is unchanged, a thread per resolved shard, since a feed's recorded count can exceed the default.
- **An existing database without a change feed, opened with neither count, takes `default_shards`.** Since the count isn't stored, that's a reshard whenever the file was last opened with another count. It's harmless without a feed, as opening with another explicit count is today.
- **`phdb serve`** sets it to its default (half the physical cores) on every open, a cluster node's partitions included. `--shards` still sets `shards`.
- **No format change.**

## Why
- **The shard count isn't stored in the file.** Only a database with a change feed records one, the feed's stream count, so that an unset count doesn't end the feed's history.
- **`phdb serve` applied its default only when it created the file.** A restart, or any existing file, opened with the library's one shard per core.
  - With D289's driver per shard, every core then had a pinned driver, so serve's CPU mask fell back to "off the shards' CPUs". That put the network threads on the drivers' SMT siblings (#1244's case).
  - On the reference machine under a 12-core set: 12 drivers pinned 0-11, against the 8 shards serve created the file with.
- **Why not `shards(default)` in serve:** an explicit count that differs from a feed's recorded count reshards and ends the feed's history. Serve can't learn the recorded count before the open. A default the recorded count overrides is the one-open fix.

## Callers changed
- `phdb serve` (`crates/cli/src/serve/mod.rs` `open`, `cluster/mod.rs` `partition_options`).
- Nothing else: the default `0` keeps every other caller's behaviour.

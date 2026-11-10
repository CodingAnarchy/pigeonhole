# 0130: `wal_preallocate`, a WAL ring sized at the open

**Status:** Approved. The coordinator assigned the number and agreed to the names (2026-11-10). This is #1621's growth half, decided by perf287's g47 and g52 box runs, and recorded as a D157 amendment. perf287 reviews.

## The change
- **New public names:**
  - `pigeonhole::Options::wal_preallocate(bool)`, default `false`;
  - `phdb serve --no-wal-preallocate`. `serve` turns the option on unless this flag is given.
- **Internal names (not semver-checked):**
  - `pigeonhole_engine::EngineOptions::wal_preallocate: bool`, default `false`. `EngineOptions` is `#[non_exhaustive]`.
  - `pigeonhole_wal::SpareSegments::prepare_ring(slots) -> Result<u32>`. It tops a stream's file up to `slots` slots, zero-filling every blank one, then syncs once.
- **Behaviour, with the option on:**
  - **Before the shards take writes,** a writable open sizes each WAL stream's ring to what it holds in steady state: the slot in use, ⌈`wal_pin_bytes` / slot⌉ for the checkpoint's lag (D229; the value in effect, so `wal_pin_bytes = 0`, the default, counts as 2 × `memtable_budget`), `spare_segments` (2) and the change feed's retention in segments (D310).
  - **Every slot is zero-filled and synced,** each stream on a thread of its own, all in parallel.
  - **Slots a reopened stream already has count toward the ring,** so a reopen adds none. Blank slots recovery finds (prepared but never used) are zero-filled again, as they would be before their first use anyway (D157).
  - **A ring that outgrows the estimate** still prepares more on demand, as before.
  - **Off,** nothing changes: a new stream starts at one slot and grows on demand (D157).
- **No format change.** A prepared slot is an existing state (zeros, synced) and the file is just longer. Older builds open it as they open any stream with spare slots.

## Why
- **The cost being removed:** on a busy store, growing the ring zero-fills slots in the background on the same device as the log's own syncs. Every in-run fill shape g47 measured traded #1621's Mutate tail against group-commit's p99.9. With 256 KiB to 64 MiB chunks, or O_DIRECT, one got worse as the other got better.
- **What g52 found:**
  - Off-CPU, all of fsync's blocked time is in `folio_wait_writeback`: the zero writes delay the log's own write-back in the device queue.
  - Only "no prep during the run" met both bars: g47's pre arm, group-commit 0.80 ms against main's 0.88, and no Mutate over 25 ms.
- **The estimate against g52** (perf287, #1681): at 64 MiB retention the ring is 6 slots and g52 measured 6. At 1 GiB it's 9 and g52 measured 8.
- **Why not the library default:** at the defaults that's 5 slots × 64 MiB per shard. Without the change feed the logs are removed at the last clean close, so every session would write them again. That reverses D157's "a tiny database writes almost nothing at open". So it's opt-in in the library and on in `phdb serve`, a long-running server on a dedicated disk.

## Callers changed
- `phdb serve` (`crates/cli/src/serve/mod.rs`) turns it on.
- The CLI's integration tests that spawn `serve` (`tests/mcp.rs`, `tests/promote.rs`) pass `--no-wal-preallocate`, so each test's server doesn't write hundreds of MiB.
- No other caller changes: the default is off.

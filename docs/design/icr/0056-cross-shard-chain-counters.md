# 0056: cross-shard chain, wound and max-attempts counters

**Status:** Approved (owner, via the coordinator, 2026-10-21; with D267, #830). Additive.

## Change

**Public, additive** (`pigeonhole-engine` and `pigeonhole` `CrossShardMetrics`, both `#[non_exhaustive]`):
- `chained: u64`: PREPAREs a participant admitted behind another commit's undecided share or a waiting single, instead of refusing them (D267). Summed over shards.
- `wounds: u64`: younger undecided shares wounded, so an older commit chained behind them goes first (D267). Each wounded attempt is aborted and retried. Summed over shards.
- `max_attempts_blind: u64`: the most attempts (two-phase rounds) one committed cross-shard commit that only writes took since open. A high-water mark, not a count: the largest over the shards' coordinators.
- `max_attempts_reading: u64`: the same for commits that read (a transaction's validated reads).

The two `max_attempts` fields count only commits that committed. A commit that gave up with `Busy` at the write-stall timeout never shows there, so read them together with the `Busy` failures (perf287's review).

**Internal** (`pigeonhole-engine`):
- `CrossShardCounters` gains `chained`, `wounds` and `max_attempts: [AtomicU64; 2]` (blind, reading). They are written by the shard's own thread (Relaxed; a `fetch_max` at a commit decision), and `Engine::metrics` reads them. They are listed in the concurrency inventory.
- `CoordinateReq::attempts`, `Retried::attempts` and `Coord::attempt` carry the attempt number across retries and re-routes.

**Bench** (`pigeonhole-bench`, not published): `Stalls::{xshard_chained, xshard_wounds, xshard_max_attempts_blind, xshard_max_attempts_reading}`. The max-attempts fields are taken from the end reading, not differenced.

## Why

- **D267 chains blind PREPAREs and wounds younger shares** (#816). `chained` and `wounds` show how often that happens on a workload.
- **Reading commits keep D249/D263's refusal order with no hard bound,** as on main (perf287's review of the reader-fence fix). `max_attempts_reading` reports the worst case actually seen. If it grows with run length, that is the signal for wound-wait for readers. `max_attempts_blind` covers the cap and wound path the same way.
- **Before this change,** only the mean was visible (`refused_rounds` over the commits).

## Callers

- `pigeonhole-engine`:
  - `shard.rs`: the counters; `Coord::attempt`; `start_coordination`; the retry in `on_applied`; the record at `committing`;
  - `shard/tablets.rs`: `chain_behind_undecided` counts `chained` and `wounds`; the re-route carries `attempts`;
  - `engine.rs`: `CrossShardMetrics` and `Engine::metrics`; the first `CoordinateReq`.
- `pigeonhole`: `metrics.rs` (the public fields and the `From` conversion).
- `pigeonhole-bench`: `report.rs` (`Stalls`, `Stalls::between`) and `runners/pigeonhole.rs` (`stalls`).
- **Tests** (`crates/engine/tests/tablets.rs`):
  - `a_cross_shard_transaction_overlapping_an_undecided_share_retries_instead_of_conflicting`: a reading commit's max is at least 2, the blind max 0;
  - `a_blind_prepare_over_an_undecided_share_chains_instead_of_refusing`: blind 1, reading 0;
  - `a_single_waiting_on_a_hot_row_goes_in_while_commits_keep_chaining` and `a_crash_mid_chain_applies_the_decided_share_and_drops_the_one_behind_it`: `chained`;
  - `the_oldest_blind_commit_chains_past_a_full_row_and_wounds_it`: `wounds`.
- **Docs:** D267, `docs/design/concurrency.md` (the inventory rows and count), the changelog.

# Engine: tablet changes on by default (#38)

## Proposed decision: amend D129, tablet changes are on by default
D129 kept `EngineOptions::tablet_changes` off until splits, merges, moves and the balancer were hardened. The bugs it named (#94, #95, #98, #102–#105) and the ones the first default-on seed sweep found (#131, #132) are fixed. The sweeps of this branch then found #163, a model-harness oracle bug (fixed in the PR below this one in the stack); the sweep results are in the PR.

**Proposed amendment to D129:** "`EngineOptions::tablet_changes` and `pigeonhole::Options::tablet_changes` default to `true`. Setting them to `false` keeps every table one tablet on shard `tablet % shards`, as before #38; everything D129 gates still runs only when they are on."

**Interim behavior (this PR):** both default to `true`. `false` turns them off, and `phdb-bench --no-tablet-changes` does the same for the bench. The engine harness (`Config::standard`) and the public model suite follow the default; `PIGEONHOLE_TABLET_CHANGES=0` runs them off, and `=1` adds the fast balancer to the engine harness (the public suite always uses it). New tests keep the off path covered: `model_check::runs_with_tablet_changes_off_match_the_model`, and the public suite's `quiet_runs_with_tablet_changes_off_match_the_model` and `crashes_with_tablet_changes_off_match_a_durable_prefix`.

## Q: Should an idle application-owned shard wake every 100 ms for the balancer?
D146 has each shard arm a clock timer for its next balancer pass, so the balancer runs without messages and idle shards merge cold tablets. With tablet changes on by default, `Shard::next_wakeup` is never `None`: an idle application-owned shard is woken every `balance_interval_nanos` (100 ms), and an engine-owned one runs a short pass on the same schedule. The `idle_cpu` tests still pass.

**Interim behavior:** unchanged. `Shard::next_wakeup` documents the pass, and its doc example checks for at most 100 ms instead of `None`. Alternatives: stop arming the timer once a pass finds nothing to do and no writes arrived since the last one (re-arm on the next write), or back off the interval while idle.

## Q: Two milestone_b tests pin the arena layout without tablet changes
`flush_and_compact_are_busy_*_when_snapshots_hold_the_arena` (#116, D138) starve an arena of 64 chunks of 32 KiB through chunk rounding. With tablet changes on, D140 cuts the arena into 256 chunks of 8 KiB, and the same rows leave room, so `flush` succeeds. The scenario tests D138's `flush`/`compact` semantics, which do not depend on tablets.

**Interim behavior:** both tests set `tablet_changes = false`. `large_memtable_values_are_pinned_not_copied` (D29) now uses a 4 MiB budget: with 1 MiB and tablet changes on, chunks are 4 KiB, its 8 KiB value spans chunks, and the memtable sometimes froze and flushed before the read. A starvation case for the tablet-changes layout could be added if wanted.

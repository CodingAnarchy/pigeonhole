# Engine: tablet changes on by default (#38)

## Proposed decision: amend D129, tablet changes are on by default
D129 kept `EngineOptions::tablet_changes` off until splits, merges, moves and the balancer were hardened. The bugs it named (#94, #95, #98, #102–#105) and the ones the first default-on seed sweep found (#131, #132) are fixed. The sweeps of this branch then found #163, a model-harness oracle bug (fixed in the PR below this one in the stack); the sweep results are in the PR.

**Proposed amendment to D129:** "`EngineOptions::tablet_changes` and `pigeonhole::Options::tablet_changes` default to `true`. Setting them to `false` keeps every table one tablet on shard `tablet % shards`, as before #38; everything D129 gates still runs only when they are on."

**Interim behavior (this PR):** both default to `true`. `false` turns them off, and `phdb-bench --no-tablet-changes` does the same for the bench. The engine harness (`Config::standard`) and the public model suite follow the default; `PIGEONHOLE_TABLET_CHANGES=0` runs them off, and `=1` adds the fast balancer to the engine harness (the public suite always uses it). New tests keep the off path covered: `model_check::runs_with_tablet_changes_off_match_the_model`, and the public suite's `quiet_runs_with_tablet_changes_off_match_the_model` and `crashes_with_tablet_changes_off_match_a_durable_prefix`.

## Proposed decision: an idle shard's balancer backs off (amends D146)
D146 has each shard arm a clock timer for its next balancer pass, so idle shards still merge cold tablets. With tablet changes on by default, that is every user's cost: 10 wakeups a second per shard, for ever, on an idle database.

**Decision (this PR):** a pass that finds nothing to do (no change started, no cleanup queued, no change running or queued) after no writes and no tablet change on any shard since the last pass (`Shared::tablet_epoch` unchanged) doubles the shard's interval, up to 10 s (`BALANCE_IDLE_CAP_NANOS`; never below `balance_interval_nanos`). A write, a queued or running change, or a moved `tablet_epoch` returns it to `balance_interval_nanos` and pulls the next pass forward to one base interval from then. `Shard::next_wakeup` is bounded by the current interval: 100 ms after activity, up to 10 s while idle. Test: `tablets::an_idle_shard_backs_off_its_balancer_and_a_write_resets_it`.

Proposed amendment to D146, "Idle shards": add "…and backs off while idle: each pass that finds nothing to do after no writes and no tablet change doubles the interval, up to 10 s; a write or a tablet change returns it to `balance_interval_nanos`."

Consequence: an idle shard notices skew published by other shards, or cold tablets to consolidate, up to 10 s later. Merges of cold tablets wait for idle passes anyway, so they finish later on an idle database, which costs nothing.

## Q: Two milestone_b tests pin the arena layout without tablet changes
`flush_and_compact_are_busy_*_when_snapshots_hold_the_arena` (#116, D138) starve an arena of 64 chunks of 32 KiB through chunk rounding. With tablet changes on, D140 cuts the arena into 256 chunks of 8 KiB, and the same rows leave room, so `flush` succeeds. The scenario tests D138's `flush`/`compact` semantics, which do not depend on tablets.

**Interim behavior:** both tests set `tablet_changes = false`. `large_memtable_values_are_pinned_not_copied` (D29) now uses a 4 MiB budget: with 1 MiB and tablet changes on, chunks are 4 KiB, its 8 KiB value spans chunks, and the memtable sometimes froze and flushed before the read. A starvation case for the tablet-changes layout could be added if wanted.

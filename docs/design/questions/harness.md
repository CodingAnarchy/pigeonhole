## Proposed decision: the engine and public model suites use the sim's record-level oracle on every crash (amends D123; closes #48)
D123 left the engine harness applying its own record-level prefix rule and cross-checking `recovered_commits` only where the streams could be represented. With `recovered_from_records` (D114) both suites now call the sim oracle on every crash, and neither keeps its own copy of the rule:

- **Engine (`crates/engine/tests/common`).** The engine's append order (`Engine::take_appended`) becomes `StreamRecord`s per stream (a COMMIT names the participants whose PREPAREs the harness saw), the surviving prefix per stream comes from the WAL and SSTs as before, and `recovered_from_records` decides which commits survived; commits fully flushed below a checkpoint are added from the manifest. `sim_helper_recovered` and `Stats::helper_checked` are gone.
- **Public (`crates/pigeonhole/tests/model.rs`).** Crash runs use one shard, so every commit is one record in one stream. The public API does not show how many records survived, so each prefix, longest first, goes through `recovered_from_records`, `check_acknowledged_survive` and `Model::from_commits`; recovery must match one allowed prefix.

**Interim behavior:** as described.

## Proposed decision: a model harness attributes a step's error to an armed power loss only once the crash has fired (closes #62)
An armed power loss (`FaultPlan::crash_after_ops`) can fire on a shard's background I/O (a flush or a compaction) with no client call in progress. Both harnesses keep a liveness probe, a file opened alongside the store whose handle dies with every other one at a crash. When a step (a read, a scan, a snapshot or a commit) fails and the probe is dead, the failure is that power loss and the harness recovers from it (`crash_and_recover(Power, already = true)`). An error while a crash is armed but has not fired is no longer attributed to it: it fails the run.

**Interim behavior:** as described; each suite has a deterministic test that fires an armed crash on a background flush and then reads (`a_read_after_a_background_fired_crash_*`).

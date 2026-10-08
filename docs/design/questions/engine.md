# Engine questions

## Proposed decision: the engine's test hooks live in one module, and a public seam beats a hook
Phase 1 left about a hundred `#[cfg(feature = "test-hooks")]` sites spread over `engine.rs`, `shard.rs`, `manifest.rs`, `maintenance.rs` and `snapshot.rs`. Issue #184 moved them into `crates/engine/src/engine/hooks.rs`: the `#[doc(hidden)]` `Engine` hook methods, the types they return, and the state they keep (`Shared::hooks`, `ShardMetrics::hooks`, `ReaderState::hooks`). The module docs list the rules: every hook has a committed test that uses it, a hook does nothing until a test sets it, and a public or application-owned seam is preferred to a new hook.

D90 and D134 name hooks individually. D134's `tablet_changes` hook is gone: tests sum `Engine::shard_stats` instead, the per-shard counters the bench added for #51.

**Interim behavior:** as described; the `test-hooks` feature and every remaining hook behave as before.

## Q: should a `test-hooks` build that no test drives stop recording?
`take_appended` and `take_compactions` read back records that every `test-hooks` build keeps (every WAL record appended, every rewrite compaction), and the shard counters are stored after every batch. Because of workspace feature unification (`cargo test --workspace --all-features`, #148 1-2 F10), the public crate's suites run against such an engine too, so those two vectors grow for the length of a run that never drains them. Nothing reads them there, so results do not change, but memory use does. Making recording opt-in (a test turns it on before it reads it) would change the harness, so it is left to #148.

**Interim behavior:** recording stays on in every `test-hooks` build.

# Engine questions (Phase 2)

## Proposed decision: test-hook recording is opt-in, and the test-hooks wait matches production (#148; D164's follow-up, review 1-2 F10)
**Interim behavior:**
- **Opt-in recording.** `take_appended` and `take_compactions` record only after `Engine::record_history(true)`; recording is off at open. Compaction records are not even built while it is off. Because of workspace feature unification, every `test-hooks` build (the public crate's suites included) used to grow both vectors for a whole run nobody read. The tests that read them turn recording on: the model harness at each open, the tablets helpers, and the few direct readers. The `ShardCounters` stay always-on plain counters.
- **One wait semantics (F10).** The `test-hooks` `PendingMaintenance` future used to resolve at the first failed shard reply, while other shards still worked. It now resolves only once every shard has replied, with the failure if there was one, as production's blocking `wait` does. The harness therefore never sees a `flush`/`compact` result production cannot produce.
- **Production waits under test.** CI's Linux job also runs `cargo test -p pigeonhole --all-features` on its own. That run builds the engine without `test-hooks` (feature unification adds it only for the workspace run), so the public suites cover the production wait path too.

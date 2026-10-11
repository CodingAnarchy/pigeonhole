### Changed
- **Async commits wait for room before they submit** (D210, #149). Each shard holds at most half `memtable_budget` of async commits submitted and not yet resolved (32 MiB at the defaults). Over that, `commit_async`, `commit_with_async`, `commit_if_async` and `Transaction::commit_async`/`commit_with_async` wait in arrival order and submit once earlier commits resolve; a commit larger than the bound goes alone. Before, a producer that outran its shard queued without limit inside the engine.
  - **A commit future dropped while it still waits for room writes nothing.** Once submitted, dropping still does not roll the commit back. `let _ = t.mutate(..).commit_async();` can therefore lose the commit when its shard is over the bound: await the future, or use `WriteBatch::commit_with_ticket`, which always submits.
  - Blocking commits and `commit_with_ticket` are neither counted nor held.

### Added
- `pigeonhole-runtime`: `Waiter::on_settled`, a callback when a completion resolves, set from the waiting side. `pigeonhole-engine`: `Engine::admit`, `Txn::admit`, `Admit` and `EngineOptions::async_commit_bound` (D210).

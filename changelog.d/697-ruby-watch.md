### Added
- Ruby bindings, PR 3c (#697, ICR 0111): T9 in `Pigeonhole::Memory`.
  - **`Store#watch`/`ReadStore#watch`:** a `Watch` of memory events (`MemoryCommit`, `MemoryEvent`) through the change feed.
  - **`Store#consolidate_loop`:** a feed-driven `ConsolidateLoop` with the block as its consolidator, run one step at a time with `run_once`. A newer loop of the name takes over (`LoopTakenOver`).
  - **`StoredMemory#summary?`:** true for a summary a consolidation wrote.

### Changed
- **Ruby: the memory store's writes, recall and consolidation-loop step yield to a fiber scheduler** (#1574, ICR 0126 amendment 2). Under Async, Falcon or any `Fiber::Scheduler`, these let the thread's other fibers run while they wait:
  - `Pigeonhole::Memory::Store#remember`, `delete`, `link`, `supersede` and `mark_used`;
  - `recall` and `recall_with_stats`;
  - `ConsolidateLoop#run_once`, whose consolidator block runs on the calling fiber and may itself yield.

  Recall, the near-dedup lookup and an IVF store's settle run on a small worker pool meanwhile. The same opt-outs apply: `Fiber.blocking { }` and `Pigeonhole.fiber_scheduler = false`.

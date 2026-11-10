### Changed
- **Ruby: the change feed's and memory watch's waits yield to a fiber scheduler** (#1574, ICR 0126 amendment 1). Under Async, Falcon or any `Fiber::Scheduler`, `Feed#next` and `each`, and `Pigeonhole::Memory::Watch#next` and `each`, let the thread's other fibers run while they wait, as commits already do.
  - A fiber stopped while it waits loses no commit: the next call returns it.
  - One consumer may wait on a feed or watch at a time.
  - The same opt-outs apply: `Fiber.blocking { }` and `Pigeonhole.fiber_scheduler = false`.

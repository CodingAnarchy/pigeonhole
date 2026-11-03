### Added
- Ruby bindings, PR 3a (#697, ICR 0111): agent memory, `Pigeonhole::Memory`, as Python's `pigeonhole.memory` binds `pigeonhole-memory`.
  - **Stores:** `Store.create` (with `normalization:` from the start), `Store.open` and `Store.open_reader`.
  - **Methods:** remember (in a transaction too), delete, link, supersede, mark_used, get, verify, recall (with stats and graph expansion), and forget passes and policies.
  - **Consolidation** with a Ruby block as the consolidator. The block runs on the caller's thread while the pass runs on its own. A `StandardError` from the block becomes `ConsolidateError`; `break`, `throw` and interrupts stop the pass cleanly and take effect.
  - **Closing:** `Database#close` closes the stores opened on it first.

### Changed
- Ruby: `Feed#next(timeout:)` no longer waits past a short timeout while it gives way to another thread (ai1249's nit on #1411).

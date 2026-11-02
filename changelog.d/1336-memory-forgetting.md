### Added
- `pigeonhole-memory` (D311 T2): forgetting.
  - A store's `ForgetPolicy` forgets by retention (importance and uses, decaying with time) and by maximum age per kind.
  - Pinned memories, and those a semantic memory was derived from, are never forgotten.
  - A `Forgetter` runs the policy on the crate's thread or the application's, and `Store::forget` runs a pass now.

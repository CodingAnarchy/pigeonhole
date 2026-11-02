### Added
- `pigeonhole-memory` (D311, T1's first PR): long-term agent memory.
  - Memories live in exact namespaces, with an embedding from a pinned model.
  - Each write is a transaction, with exact dedup.
  - Links go both ways, and `supersede`, `mark_used` and `verify` are available.

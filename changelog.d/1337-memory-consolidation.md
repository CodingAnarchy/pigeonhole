### Added
- `pigeonhole-memory`: consolidation (D311 T3, #1337).
  - An application `Consolidator` folds groups of episodic memories into semantic ones.
  - Groups form by session and by embedding similarity, and each group commits in its own conflict-checked transaction, with `DERIVED_FROM` links and the group's provenance.
  - The group's memories are kept (marked consolidated), deleted or superseded.
  - Recall weights a consolidated memory's importance by `Weights::consolidated`.
- `pigeonhole-memory`: near-duplicate dedup (`NearDedup`, `DedupPolicy::{Link, Skip, Merge}`, `Remembered::Similar`, `Store::near_duplicate`).

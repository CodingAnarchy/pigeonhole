### Added
- `pigeonhole-text` (D309 PR 3a):
  - **MaxScore:** searches read only the postings that can still change the top `k`, with the same results and scores as before.
  - **`Search::id_prefix`:** restricts a search to document ids with a prefix (a namespace).

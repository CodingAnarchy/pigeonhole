### Added
- `pigeonhole-text` (D309 PR 2): phrase slop.
  - `"a phrase"~N` and `Query::phrase_with_slop(words, n)` match a phrase's words within `N` position moves, with Lucene's sloppy-phrase semantics.
  - Closer matches score higher: each match adds `1 / (distance + 1)` to the phrase's frequency.
  - An exact phrase scores as before.

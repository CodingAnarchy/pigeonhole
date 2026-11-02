### Added
- `pigeonhole-text` (D309 PR 5): highlighting.
  - `Search::highlight(true)` fills each hit's `text` and `matches`: byte ranges, each with its clause and occurrence.
  - `snippet()` picks a window of the text around the best matches.

### Added
- `pigeonhole-text` (D309 PR 4): fuzzy terms.
  - `word~`, `word~1` and `word~2` in a query, or `Query::fuzzy`. These match terms within 1 or 2 edits, with a transposition counting as one.
  - `Search::max_expansions` and `Search::prefix_length` tune them, and `SearchStats::fuzzy` reports each fuzzy term's expansions.

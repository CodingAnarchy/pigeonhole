### Changed
- Filtered scans seek to a filter's hint within a data block by searching forward from the current entry instead of from the block's start, so skipping excluded qualifiers in a wide row no longer rescans the entries already passed (#29).

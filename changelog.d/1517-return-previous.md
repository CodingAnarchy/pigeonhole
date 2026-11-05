### Added
- `RowMutation::return_previous(family, qualifier)` and `Returned::previous(family, qualifier)`: get-and-set. `commit_returning` also returns a column's newest version from just before the commit. Concurrent swaps of one cell each get the one before (D314 amendment 1, ICR 0113, #1445).

### Changed
- `commit_returning`'s result is now named `Returned`. `Incremented` remains as an alias, so 1.1.0 code compiles unchanged.

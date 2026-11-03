### Changed
- A read that skips the rest of a wide row still in the memtable (a qualifier prefix or range that ends mid-row, or a column limit) seeks past it instead of stepping through it (#1426). A prefixed read in the middle of a 100,000-cell memtable row went from 171 µs to 7 µs.

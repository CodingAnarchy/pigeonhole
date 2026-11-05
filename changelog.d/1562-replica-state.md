### Changed
- Table names starting with `__pigeonhole.` are reserved: creating one fails with `InvalidArgument` (D315 PR 3). The engine keeps a replica's state in such a table, hidden from `tables()`, `info()`, lookups by name and every feed.

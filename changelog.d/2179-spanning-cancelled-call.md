### Fixed

- `phdb serve` cluster: a spanning transaction whose `Commit` or `TxnWrite` is cancelled after it sent rows to another partition (the client went away, or its deadline passed) now ends there, and the session answers `Conflict`. Before this the session stayed open, and a retry of the call sent the rows again, so an increment could be applied twice (#2157).

### Added
- `Options::read_only(true)` opens a database read-only whatever the file needs (ICR 0109). It writes nothing to the database's files: no format upgrade, change-feed session, log segment, flush or clean-close commit. Writes fail with `ReadOnly`.

### Changed
- `phdb info`, `dump`, `backup` and `shell --read-only` open read-only when no writer runs, so they leave the database exactly as they found it. An application's change-feed tokens stay valid across them (D310). A crashed writer's log is replayed into memory and left for the application's next open, rather than checkpointed by phdb.

### Changed
- `phdb serve` runs its shard threads without polling for work before they sleep. On a busy server the poll was a quarter to half of the CPU per put, and the wakeup it saves is small next to a network round trip (D198 amendment 4). `--shard-spin US` sets a fixed poll, and `--shard-spin adaptive` restores the library's default.

### Added
- `Options::shard_spin`, public and documented (ICR 0129): how long a shard thread polls for the next commit before it sleeps. `Duration::ZERO` sleeps at once.

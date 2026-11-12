### Added
- `Options::shard_spin`, public and documented (ICR 0129): how long a shard thread polls for the next commit before it sleeps. `Duration::ZERO` sleeps at once.
- `phdb serve` runs its shard threads without that poll: on a busy server it was a quarter to half of the CPU per put, and the wakeup it saves is small next to a network round trip (D198). `--shard-spin US` sets a fixed poll, and `--shard-spin adaptive` the library's default.

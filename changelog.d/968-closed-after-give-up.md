### Fixed
- With `Options::auto_recover`, once the supervisor gave up (`recovery_stats().gave_up`), a commit could fail with `Io` instead of `Closed` when it reached the failed shard just before the database was marked dead (#966). It now gets `Closed`, as the recovery guide says; it was not committed either way.

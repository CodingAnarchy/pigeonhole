### Fixed
- `phdb-bench`'s inline runs (the `scaling` gate and the D213 group commit) no longer hang when a shard thread panics: the phase stops at once and the run exits with `shard thread N panicked: <message>` (exit code 2). Before, the other threads waited for ever for the dead one, and a gate step sat until its timeout.

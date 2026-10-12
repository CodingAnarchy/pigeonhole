### Changed
- `phdb-bench scaling` fails a run as skewed when one shard took more than 1.25/N of the measured commits, down from 2/N (D223). Its shard threads now pick their rows in parallel and start together, and the phase clock starts when they do, so efficiency no longer counts the picks.

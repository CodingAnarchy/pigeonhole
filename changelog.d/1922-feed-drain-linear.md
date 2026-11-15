### Fixed
- Change feed: a subscriber far behind (a new one from the oldest commit, or a resume of an old token over a large retention) now drains its backlog in time linear in it. Each read-ahead pass used to step through every remaining record again, so with small commits the drain slowed as the backlog grew: 1 GiB of 64-byte commits took 10 s where it now takes under 4 (#1827).

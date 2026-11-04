### Changed
- An in-process change-feed subscriber costs the writer less: at each poll, a stream with nothing new reads its WAL segment headers once instead of twice, and 32 KiB instead of 1 MiB (D310). With one subscriber, ycsb-a at a single client ran at 0.83–0.87x the feed-off throughput.

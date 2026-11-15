### Changed
- Change feed: a subscription that keeps up costs less CPU per commit. On each wake, a feed reader re-read the whole 32 KiB write-ahead-log frame it had stopped in, for every shard, to find a few hundred new bytes. It now reads only past where it stopped. In `phdb serve` that was about half of each feed hub thread's time under steady writes (#1901).

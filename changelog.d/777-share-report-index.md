### Fixed
- A flush on a store running many cross-shard commits no longer holds its shard for 17-21 ms while it reports the flushed shares to their coordinators: the shares are indexed by the slots they write, and a shard reports at most 1024 per batch (#777).

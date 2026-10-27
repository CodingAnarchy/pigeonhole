### Added
- `Options::shard_threads(k)` runs the shards on `k` threads rather than one each (D289, ICR 0079, #760). Shard `i` runs on thread `i % k`, which takes its shards' turns one after another and parks only when all of them are idle. Fewer, busier threads sleep less, so a commit waits less often for its shard's thread to be woken. The default, 0, keeps one thread per shard.

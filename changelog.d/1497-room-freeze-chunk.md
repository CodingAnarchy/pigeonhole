### Fixed
- A write could wait the whole write-stall timeout for memtable room, then succeed or fail with `Busy`, under cross-shard commits, when the memtable arena's last free space was held and nothing flushed. Some room per shard now stays free, so a wait for room always starts a flush (#1497).

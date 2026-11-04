### Fixed
- A write could wait the whole write-stall timeout for memtable room, then succeed or fail with `Busy`, when undecided cross-shard commits held the last free arena chunk: the room wait's freezes found no chunk for a fresh memtable and nothing flushed. One chunk per shard now stays free beyond every reservation, so a wait for room always starts a flush (#1497).

### Changed
- A commit no longer writes engine-wide memory on its way (D236, amends D210): an async commit is charged on the admission count of the shard it is routed to (routed once), and a pending commit holds its shard's handle instead of a reference on the engine's shared state. 16 pinned shards commit 11.6% faster on the reference machine.

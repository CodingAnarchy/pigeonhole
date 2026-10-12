### Changed
- Once an application thread waits on its shard (`Shard::wait`) and no `set_wakeup` callback is registered, a commit wakes that thread directly instead of through a composite callback: one indirect call fewer per commit.

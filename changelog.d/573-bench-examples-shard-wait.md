### Changed
- The `writepath` and `ycsb` instruction examples' application-owned shard threads wait with `Shard::wait` and are woken with `Shard::waker` (ICR 0035), in place of their own `set_wakeup` unpark and `next_wakeup` park.

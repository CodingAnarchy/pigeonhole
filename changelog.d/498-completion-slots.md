### Changed
- A commit no longer allocates its completion slot (or, on macOS, the slot's lock): each thread reuses up to 8 finished slots. Allocations per commit drop from 4.25 to 2.25 with one cell on macOS (#320). `pigeonhole-runtime` adds `SlotCache`, `completion_from` and `Waiter::recycle_into`.

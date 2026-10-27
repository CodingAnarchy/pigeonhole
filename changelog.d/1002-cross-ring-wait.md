### Fixed
- io_uring: a thread blocked on an operation in flight on another thread's ring now keeps completing its own ring's operations while it waits (#1002). Before, it only slept, so two threads each waiting on I/O in the other's ring could hang for good. No path that does this is known in 0.3.x, but D289's shared shard drivers would have made it routine.

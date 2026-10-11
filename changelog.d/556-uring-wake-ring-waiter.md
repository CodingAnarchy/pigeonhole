### Fixed
- With the io_uring backend, waking a shard thread writes its ring's eventfd only while the thread waits in the ring. Before, every wake paid that write, and the woken ring then drained the eventfd and re-armed its poll: about three system calls per write on a busy shard.

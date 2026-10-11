### Fixed
- With the io_uring backend, a shard turn that has nothing to reap no longer makes two system calls (a read of the ring's notify eventfd and an `io_uring_enter`). On the Phase 3 reference machine those cost write-heavy workloads 15–20% against pread.

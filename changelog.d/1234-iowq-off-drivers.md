### Changed
- On Linux with io_uring, pinned shard threads keep the kernel's io_uring workers off their CPUs (D305): each pinned shard thread's workers run on the rest of the process's affinity set, so they no longer preempt it. On the AX102, sparse-wide `Buffered` put p99.9 fell from 737–901 µs to 183–406 µs. Unpinned threads and other platforms are unchanged.

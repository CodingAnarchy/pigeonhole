# io questions

## Proposed decision: a new shared-memory region reserves its storage at open (#142)
The spec (§ Location and size) says "opening fails up front if the region can't be allocated", but the region was sized sparsely (`set_len`/`ftruncate`). On a small tmpfs, such as the 64 MiB `/dev/shm` Docker and Kubernetes give a container, opening succeeded and the first store into an unbacked page killed the process with `SIGBUS` (edge-case review 3-4 §4.1, 8-9 F5).

**Interim behavior:** creating a region reserves it, per platform:
- **Linux, Android, FreeBSD** (`/dev/shm` and any `shm_dir` file): `posix_fallocate` over the whole region. `ENOSPC` is `NoSpace`, which `pigeonhole-shm` maps to `Unavailable` (`ShmUnavailable`). A filesystem that cannot reserve (`EOPNOTSUPP`, `EINVAL`, `ENOSYS`) falls back to the sparse `set_len`. On tmpfs this commits the region's memory, `memtable_budget × shards` plus a few pages, at open rather than as memtables fill.
- **macOS** (`shm_dir` file): `F_PREALLOCATE` (`F_ALLOCATEALL`), then `set_len`; `ENOSPC` is `NoSpace`, other failures fall back to sparse.
- **macOS and BSD default** (`shm_open` object): unchanged `ftruncate`. The object is anonymous swap-backed memory, not a size-capped filesystem, so there is nothing to run out of later; macOS has no call to reserve it.
- **Windows**: unchanged. A pagefile-backed mapping is committed at `CreateFileMappingW` (`SEC_COMMIT`), and `set_len` on a `shm_dir` file allocates NTFS clusters.

A failed reservation removes the file. `pigeonhole-shm` also maps `NotFound` while creating a region (`/dev/shm` missing in a distroless or Lambda image, or a `shm_dir` that does not exist) to `Unavailable`. The public `ShmUnavailable` message names the size (`budget × shards`), the location and the remedies (enlarge `/dev/shm`, `shm_dir`, lower `memtable_budget` or `shards`); `pigeonhole` builds it at open because the lower error types carry no detail and changing them needs an ICR.

# io questions

## Proposed decision: a new shared-memory region checks free space at open and stays sparse (#142; owner, 2026-10-07)
The spec (§ Location and size) says "opening fails up front if the region can't be allocated", but the region was sized with a sparse `set_len`/`ftruncate` and nothing else. On a small tmpfs, such as the 64 MiB `/dev/shm` Docker and Kubernetes give a container, opening succeeded and the first store into an unbacked page killed the process with `SIGBUS` (edge-case review 3-4 §4.1, 8-9 F5).

**Owner decision (2026-10-07): a free-space check, not a reservation.** Reserving the region (`posix_fallocate`, `F_PREALLOCATE`) would commit `memtable_budget × shards` of RAM at open, which is 640 MiB at the defaults on 10 cores, even for a tiny database. Instead:
- **Region files** (`/dev/shm` on Linux and Android, and any `shm_dir` file on Unix): after creating the file, `fstatvfs` it; if `f_bavail × f_frsize` is below the region length, remove the file and fail with `NoSpace`, which `pigeonhole-shm` maps to `Unavailable` (`ShmUnavailable`). Otherwise size it sparsely with `set_len`, so memory is used only as memtables fill.
- **macOS and BSD `shm_open` objects**: no check (they are swap-backed anonymous memory, not a size-capped filesystem); `ftruncate` as before.
- **Windows**: unchanged. A pagefile-backed mapping is committed at `CreateFileMappingW` (`SEC_COMMIT`). `set_len` on a `shm_dir` file allocates NTFS clusters, and a failure now removes the file.

**Residual risk (documented in the guide's errors.md and getting-started):** the check does not hold the space. If another process fills the same tmpfs after the open, a store into a new region page can still raise `SIGBUS`.

`pigeonhole-shm` also maps `NotFound` while creating a region (no `/dev/shm` in a distroless or Lambda image, or a missing `shm_dir`) to `Unavailable`. The public `ShmUnavailable` message names the size (`budget × shards`), the location and the remedies (enlarge `/dev/shm`, `shm_dir`, lower `memtable_budget` or `shards`). `pigeonhole` builds it at open because the lower error types carry no detail, and changing them needs an ICR.

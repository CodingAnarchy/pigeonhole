# I/O questions (Phase 2)

## Proposed decision: FUSE and GPFS count as network filesystems; the local check runs before the writer lock (#147)
The review of #90 (3-4 4.3) found two gaps in network-filesystem detection.
- On Linux, `is_local` did not list FUSE or GPFS. Locks on sshfs, s3fs or gcsfuse may be local to one host, so two hosts could both be writers.
- The writer took its lock before the check. On NFS without lockd (or mounted `nolock`), `F_OFD_SETLK` failed with `ENOLCK`, and the open reported `Io("lock")` instead of `NetworkFilesystem`.

**Interim behavior:**
- Linux treats FUSE (`0x65735546`) and GPFS (`0x47504653`) as non-local. That includes local FUSE filesystems such as ntfs-3g, which are refused until an explicit opt-in exists. None is planned; it is a follow-up if anyone asks.
- `Engine::open` checks `is_local` (a read-only `fstatfs`) before `WriterLock::acquire`. D37 orders the presence lock and shared memory, not this check. Reader processes already checked first.
- Regression test: `engine/tests/network_fs.rs` uses a VFS whose files are remote and whose locks fail like `ENOLCK`.

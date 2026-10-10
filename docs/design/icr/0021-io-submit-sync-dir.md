# 0021: `Vfs::submit_sync_dir`

**Status:** Approved (coordinator, 2026-10-10; #158, D203).

## Change

`pigeonhole-io`, additive (a defaulted trait method):

```rust
pub trait Vfs {
    /// [`Vfs::sync_dir`], submitted: the completion resolves once the directory's entries
    /// are durable. The default runs `sync_dir` on the calling thread and returns a
    /// completion that is already resolved.
    fn submit_sync_dir(&self, dir: &Path) -> Completion<()> {
        Completion::ready(self.sync_dir(dir))
    }
}
```

`PreadVfs` runs it on its I/O pool. `UringVfs` opens the directory and submits an `fsync` of it through the ring. `SimVfs` defers it like its other submitted operations (with deferred I/O on, the directory's entries become durable only when the simulated device completes it, and a crash before then means it never happened), and adds `SimVfs::hold_dir_syncs` (a test hook: the device leaves held directory syncs in flight). Other wrappers keep the default.

## Why

#158: on a clean reopen, the open waited for one `sync_dir` (the new WAL stream files' directory entries), about 7 ms on macOS, where it is a full drive-cache flush. A read never needs that. The first durable commit does, and it already waits for every earlier sync of its stream (D58 ordering): with the directory sync submitted and taken into each new stream's sync ordering, open no longer waits for it (D203).

## Semantics

- **The completion resolves once the directory's entries are durable**, or with the error of the sync. On pread it is the same `fsync` of the directory as `sync_dir`, on a pool thread. On io_uring it is `IORING_OP_FSYNC` on the directory's descriptor, which stays open until the completion resolves.
- **Ordering is the caller's.** The WAL takes one directory sync per open, fans its outcome out to every stream it created, and registers it in each stream's sync ordering (`Shared::submit_durable`). So a stream's group sync counts only once the directory sync has finished, and a failed directory sync poisons those streams.

## Callers

- `pigeonhole-wal`: `WalStream::create_all` (stream files created at open) submits the directory sync instead of waiting for it.
- No other caller changes: `sync_dir` stays, for blocking callers (`WalStream::create`, the engine's stream removal at open, backups).

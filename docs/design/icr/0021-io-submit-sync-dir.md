# 0021: `Vfs::submit_sync_dir`

**Status:** Approved (coordinator, 2026-10-10; #158, D203). `Completion::fan_out` added the same day, after the first sweep of #454 hung (pending the coordinator's OK).

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

And on `Completion<()>`, additive:

```rust
impl Completion<()> {
    /// `n` completions that each resolve with this one's outcome. A blocking `wait` on any
    /// of them makes progress on this one's I/O as a wait on it would (its drive). A failure
    /// reaches the first with its OS error, and the others as an error of the same kind and
    /// context.
    pub fn fan_out(self, n: usize) -> Vec<Completion<()>>;
}
```

`PreadVfs` runs `submit_sync_dir` on its I/O pool. `UringVfs` opens the directory and submits an `fsync` of it through the ring. `SimVfs` defers it like its other submitted operations (with deferred I/O on, the directory's entries become durable only when the simulated device completes it, and a crash before then means it never happened), and adds `SimVfs::hold_dir_syncs` (a test hook: the device leaves held directory syncs in flight). `SimFile` now also defers `submit_sync_all` (it ran inline before, through the trait default), so a simulated open has its stream files' syncs in flight too. Other wrappers keep the default.

## Why

#158: on a clean reopen, the open waited for one `sync_dir` (the new WAL stream files' directory entries), about 7 ms on macOS, where it is a full drive-cache flush. A read never needs that. The first durable commit does, and it already waits for every earlier sync of its stream (D58 ordering): with the directory sync submitted and taken into each new stream's sync ordering, open no longer waits for it (D203).

## Semantics

- **The completion resolves once the directory's entries are durable**, or with the error of the sync. On pread it is the same `fsync` of the directory as `sync_dir`, on a pool thread. On io_uring it is `IORING_OP_FSYNC` on the directory's descriptor, which stays open until the completion resolves.
- **Ordering is the caller's.** The WAL takes one directory sync per open, fans its outcome out to every stream it created (`Completion::fan_out`), and registers it in each stream's sync ordering (`Shared::submit_durable`). So a stream's group sync counts only once the directory sync has finished, and a failed directory sync poisons those streams.
- **Fanned-out handles keep the drive.** A blocking wait on a stream's ordering sleeps on a condvar, so each stream also keeps a fanned-out handle on its open-time file and directory syncs and waits on those first (`Shared::drive_open_io`, in its blocking sync and its held-header waits). Where only a waiter completes I/O (the simulator's deferred device, driven by a single-threaded test harness), a plain fanned-out pair would wait for ever: the first sweep of #454 hung in a shard's final sync at close, right after a reopen.

## Callers

- `pigeonhole-wal`: `WalStream::create_all` (stream files created at open) submits the directory sync instead of waiting for it.
- No other caller changes: `sync_dir` stays, for blocking callers (`WalStream::create`, the engine's stream removal at open, backups).

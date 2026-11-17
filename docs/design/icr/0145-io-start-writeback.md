# 0145: `File::start_writeback`

**Status:** Approved (coordinator, 2026-11-17; #1991, D334).

## Change

`pigeonhole-io`, additive (a defaulted trait method):

```rust
pub trait File {
    /// Starts writing `len` bytes at `offset` back to the device, without waiting for them:
    /// a hint that they are complete and a sync will want them. Never a durability step.
    fn start_writeback(&self, _offset: u64, _len: u64) -> Result<()> {
        Ok(())
    }
}
```

- `PreadFile`, on Linux: `sync_file_range(fd, offset, len, SYNC_FILE_RANGE_WRITE)` on the calling thread. That's a submission, which returns once the range's writeback is queued. On a direct-I/O handle, or for an empty range, it does nothing.
- `UringFile` delegates to its `PreadFile`.
- **macOS and Windows: a no-op** (the default).
  - Neither has a range writeback that returns without waiting.
  - `F_FULLFSYNC` and `FlushFileBuffers` flush the whole file and wait, which is the opposite of a hint.
- **`SimVfs`: the default no-op.** The simulator keeps the bytes unsynced until a sync, so crash tests see the same states with or without the hint.
- Other wrappers keep the default.

## Why

#1991: in a sustained GroupSync session, puts stalled in rare events that grew to 24–58 ms.
- **The cause** (g130–g143): a WAL group sync's small write waiting about 12 ms inside the NVMe drive.
  - The drive was busy with a burst of the data file's 128 KiB writes.
  - SST output is written buffered and made durable by the pager's root commit, which syncs the whole data file. So each flush or compaction install sent its whole output to the device at once.
- **D334's fix** has the SST writer start each 1 MiB chunk's writeback as it writes it.

## Semantics

- **A hint only:** `sync_data` and `sync_all` still decide durability. A caller may ignore the result, and the SST writer does.
- **Bounded:** the range need not be written yet, or inside the file; the call still succeeds and affects nothing it doesn't cover.
- **Not ordered against later writes** to the range: they dirty the pages again, and the next sync writes them.

## Open questions
_None._

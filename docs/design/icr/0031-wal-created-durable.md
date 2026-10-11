# 0031: `Wal::created_durable` / `Wal::notify_created`

**Status:** Approved (coordinator, 2026-10-11; #520, D211).

## Change

`pigeonhole-wal`, additive (defaulted trait methods):

```rust
pub trait Wal {
    /// Whether this stream's file and its directory entry are durable: `None` while a new
    /// stream's creation syncs are in flight, `Some(false)` if one failed. `Some(true)` by
    /// default.
    fn created_durable(&self) -> Option<bool>;

    /// Calls `wake` (from any thread) once `created_durable` is settled: at once if it is,
    /// and by default.
    fn notify_created(&self, wake: Box<dyn FnOnce() + Send>);
}
```

`WalStream` implements both. A stream from `WalStream::create_all` is pending until its file sync and its directory sync (submitted, not waited for, D203) have both finished. Streams from `WalStream::create` and `Recovery::into_stream` are durable at once.

## Why

D211's invariant: no manifest checkpoint may name a stream whose file creation is not durable (#520). The engine's shard must know when a new stream's creation becomes durable, and be woken then, to hold its checkpoint back until then without polling. The WAL owns both syncs, so it reports their outcome.

## Callers

- `pigeonhole-engine`, `ShardState::advance_checkpoint`:
  - pending: it holds the checkpoint back and arms one wake (`ShardMsg::Created`);
  - failed: it never checkpoints;
  - durable: as before.

  A `debug_assert!` where the `WalCheckpoint` edit is built checks the invariant.
- Other `Wal` implementations (test doubles) keep the defaults.

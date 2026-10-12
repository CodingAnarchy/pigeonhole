# 0037: durable snapshots and durable backups (D227 part 2), shared-memory layout 2

**Status:** Approved (owner, via the coordinator, 2026-10-12; D227 Q2 (a)).

## Change

Additions only, in `pigeonhole`, `pigeonhole-engine` and `pigeonhole-shm`. The shared-memory layout version goes from 1 to 2. That isn't an API change, but a reader process built before this change can't attach to a writer built after it (and the other way round) until every process runs the same build. The layout check refuses the mismatch with `ShmVersionMismatch`, as for any layout bump.

```rust
// pigeonhole
impl Pigeonhole {
    /// A snapshot that sees a commit only once it is durable at its level.
    pub fn durable_snapshot(&self) -> Result<Snapshot>;
    /// `backup`, at a durable snapshot.
    pub fn backup_durable(&self, dest: impl AsRef<Path>) -> Result<()>;
}
impl PigeonholeReader {
    pub fn durable_snapshot(&self) -> Result<Snapshot>;
}

// pigeonhole-engine (writer and reader-process engines)
impl Engine {
    pub fn durable_snapshot(&self) -> Result<Snapshot>;
    pub fn backup_durable(&self, dest: &Path) -> Result<()>;
}

// pigeonhole-shm
impl ShmRegion {
    /// Publishes a shard's durable watermark (before its pending one).
    pub fn publish_durable(&self, shard: u32, durable: Seqno);
    /// min(next, every shard's pending, every shard's durable) - 1.
    pub fn durable_seqno(&self) -> Seqno;
}
impl ReaderSlot {
    /// Pins the durable seqno and view, as `pin` does for the visible one.
    pub fn pin_durable(&self, seqno: Seqno, view_version: u64) -> (Seqno, u64);
    /// Lowers the pinned seqno (`fetch_min`), keeping the pinned view.
    pub fn lower_pin(&self, seqno: Seqno);
}
```

**Shared memory (FORMAT.md §11.2, §11.3):** each shard's 64-byte watermark line gains `durable` (u64 at offset 8, `watermark::DURABLE`), initialized to `u64::MAX`. It's the lowest seqno the shard holds that isn't yet durable at its level, covering:
- every unresolved group;
- the cross-shard seqnos it coordinates and has not yet applied everywhere.

A shard publishes `durable` before `pending`. Readers load `pending` before `durable`.

**Engine:** memtables freeze only once their newest seqno is at or below the durable seqno. A flush's same-timestamp collapse could otherwise drop a version a durable snapshot still reads. Freezes wait on a second watermark-waiter registry (`durable_waiters`).

## Why

The owner's answer to D227 Q2: A1 makes visibility apply-time by default. A reader that must never see data a power loss removes needs D19's old view:
- deriving a write on another shard from what it read;
- persisting a cursor;
- taking a backup that never holds a commit the source then loses.

A second published watermark keeps that view in every process, readers included, at no cost to the default path.

## Callers

- `pigeonhole` `db.rs`: `Pigeonhole::durable_snapshot`, `backup_durable`, `PigeonholeReader::durable_snapshot` (doc examples).
- `pigeonhole-engine`:
  - `engine.rs`: `snapshot_of`, `reader_snapshot`, `durable_snapshot`, `backup_durable`, `backup_of`;
  - `shard.rs`: `durable_watermark`, `publish_watermark`, `durable_waiters`, `wake_visible`, the freeze rule.
- `pigeonhole-shm` `region.rs`: the new methods and the `DURABLE` initialization.
- `pigeonhole-format`: `watermark::DURABLE`, `ShmLayoutVersion::CURRENT = 2`; the golden header and the version tests.
- Tests: `durable_snapshot.rs` (writer and reader process), `durable_freeze.rs` (the freeze rule; fails if memtables freeze at the visible seqno).
- Docs: FORMAT.md, the durability guide, the agent reference, D227, D117, `concurrency.md`, changelog.

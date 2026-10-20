# Durability

> **Status:** this guide describes `main`; the current release on crates.io is 0.3.0, and the [changelog](../../CHANGELOG.md) and [`changelog.d/`](../../changelog.d/README.md) list what `main` adds. Semantics here come from the spec and decisions D12 and D19. The async commit forms are new in 0.3.0 ([Async](async.md)). Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup).

Every commit says how durable it must be before it returns. The default is the strongest batched level, so a commit that returns is on disk unless you asked for less.

## The four levels
`pigeonhole::Durability`, ordered weakest to strongest:

| Level | What happened before `commit` returned | Survives | Doesn't survive | Typical use |
|---|---|---|---|---|
| `None` | Applied to the memtable only. No I/O. | Nothing past the last flush or stronger commit | Any crash before then | Rebuildable caches, derived data |
| `Buffered` | Handed to the kernel with `write()`, no fsync. | Process crash, panic, `kill -9` | OS crash, power loss | Ingest with an upstream source of truth |
| `GroupSync` (default) | The WAL group containing it was fsynced; one fsync is shared by all committers in the group. | Power loss | Nothing, once returned | Systems of record |
| `Sync` | A dedicated fsync, never batched. | Power loss | Nothing, once returned | Rare latency-isolated critical writes |

In **every** level a crash never loses part of a row or part of a batch, and never loses a commit from the middle of a shard's write stream: each shard keeps a prefix of its own WAL stream (decision D84). A commit that touches only one shard is lost or kept whole. A cross-shard batch is kept only if every participant and the coordinator's decision survive, and is otherwise lost as a whole. There is **no single global order** across shards: after a power loss, a weaker commit on one shard can be gone while a later weaker commit on another shard survives. Commits acknowledged at `GroupSync` or `Sync` always survive.

## Resolution order
For each commit, the level is the first of:

1. the **per-call override**: `RowMutation::durability(d)`, `WriteBatch::commit_with(d)`, `Transaction::commit_with(d)`;
2. the **writer default**: `Options::durability(d)` at open, or `Pigeonhole::set_default_durability(d)` at runtime;
3. `Durability::GroupSync`.

```rust
use pigeonhole::{Durability, Options, Pigeonhole};

# let dir = pigeonhole::doc_support::temp_dir();
// Writer default, set at open.
let db = Pigeonhole::open(dir.join("ingest.phdb"), Options::default().durability(Durability::Buffered))?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta"])?;

// Changed at runtime; applies to commits that start afterwards.
db.set_default_durability(Durability::GroupSync);
let current = db.default_durability();
# assert_eq!(current, Durability::GroupSync);

// Per-call override.
let mut wb = db.write_batch();
// ... wb.put(..) ...
# wb.put(&pages, b"row", "meta", b"k", b"v");
wb.commit()?;                         // writer default
// (a batch is consumed by commit; build another for the next line)
let mut wb = db.write_batch();
# wb.put(&pages, b"row", "meta", b"k", b"v");
wb.commit_with(Durability::Sync)?;    // this commit only

pages.mutate(b"row").put("meta", b"k", b"v").durability(Durability::Buffered).commit()?;
# Ok::<(), pigeonhole::Error>(())
```

The writer default is process-local and **not stored in the file**. Reopening with different options changes it.

## Mixed levels
Commits at different levels share each shard's WAL stream, which is ordered.

- A `GroupSync` or `Sync` commit also makes every **earlier** `Buffered` or `None` record in that stream durable.
- A weaker commit never weakens a stronger one in the same group: the group is written to the strongest level any member requested.
- A `None` commit followed by a `GroupSync` commit on the same shard is therefore durable after the second returns. The reverse is not true: a later `None` commit is not durable. A `None` commit buffers its WAL record without writing it, so the next stronger commit on that shard writes it (decision D94). A `flush()`, a background flush or a clean close also makes it durable, because the data then lives in the file.
- Each shard has its own stream. A `GroupSync` commit on one shard does not make an earlier `Buffered` commit on another shard durable; after a power loss, every commit acknowledged at `GroupSync` or `Sync` survives, and weaker ones may or may not.

So you can run a mostly-`Buffered` ingest path and put a periodic `GroupSync` commit on it as a checkpoint for that shard.

## Reporting what happened
`commit()` and `commit_with()` return `CommitInfo`:

```rust
# use pigeonhole::Durability;
pub struct CommitInfo {
    pub seqno: u64,             // the commit's sequence number
    pub durability: Durability, // the level actually applied
}
```

Log or assert on `info.durability` if your correctness depends on it.

## Visibility and durability (D227)
A commit returns only once it is durable at its level **and** visible to readers, so a thread always reads its own write immediately after `commit()` returns.

**Other readers can see a commit sooner than that.** A shard makes a commit visible once its WAL record is handed to the operating system and the commit is applied in memory. It doesn't wait for the fsync (D227, which amends D19). That keeps one shard's sync from delaying every other shard's commits. The cost is that **visible no longer means durable**, at any level, for anyone but the committing thread after its call returns.

What that means in practice:

- **A reader can see data that a power loss then removes.** Thread A commits `user:42 → "paid"` with `GroupSync`. Before the fsync finishes, thread B (or a reader process) reads `"paid"`. The power fails before the fsync completes. After the restart, `user:42` has its old value. A was never told the commit succeeded, but B acted on it. A process crash alone doesn't do this: the write has already reached the operating system. It takes an OS crash or a power loss.
- **Across shards, a later write can survive while an earlier one it depended on is lost.** Each shard recovers a prefix of its own log, so on one shard what survives is always a prefix of what readers saw. Shards are logged independently, though. Example: client P writes `order:7 = "placed"` (shard 1). Client Q reads it and writes `invoice:7 = "for order 7"` (shard 2), and Q's commit returns. Power fails before shard 1's fsync. After recovery `invoice:7` exists and `order:7` doesn't. A transaction that read `order:7` doesn't prevent this either: it validates against what was visible. If a write must never outlive what it was derived from, read through a durable snapshot (see below), or put both rows in one shard's batch.
- **A backup can include commits the database later loses.** `backup` copies exactly what is visible at its snapshot. That can include commits whose fsync hasn't returned yet. If the power then fails, the backup holds commits the database itself doesn't. To back up only durable data, take the backup from a durable snapshot (see below).
- **A sequence number or timestamp a reader saw can be handed out again after a power loss.** Seqnos and default timestamps are assigned in memory, ahead of the fsync. If a reader saw commit seqno 1042 and that commit is then lost to a power loss, the restarted database can give 1042, and the same default timestamp, to a different commit. This matters if you use seqnos or timestamps as cursors or versions. Example: a change consumer records "processed up to seqno 1042" after reading other clients' commits. After a power loss, it skips the new commit 1042, which it has never seen. Seqnos and timestamps of commits whose own `commit()` returned are durable and never reused. A cursor taken from a durable snapshot is safe too.
- **Durable snapshots (opt-in, ICR 0037).** A reader that needs every value it sees to be durable takes a durable snapshot instead: `db.durable_snapshot()` (also on a reader process's handle). It sees a commit only once its level is met on its shard, which is how every snapshot behaved before D227. Durable means durable at the commit's own level: `GroupSync` and `Sync` commits survive a power loss, `Buffered` ones a process crash, `None` ones nothing. To read only data that survives a power loss, write it with `GroupSync` or `Sync`. A failed fsync never counts as durable. It lags `snapshot()` by the fsyncs in flight, typically well under a millisecond. Use it when what you read must never be undone:
  - before writing something derived from it, on another shard;
  - for a cursor or version you persist;
  - for a backup that must hold only durable data: `db.backup_durable(path)`.

  ```rust
  # use pigeonhole::{Family, Options, Pigeonhole};
  # fn main() -> pigeonhole::Result<()> {
  # let dir = pigeonhole::doc_support::temp_dir();
  # let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
  # let orders = db.table("orders")?.family("f", Family::default()).create_if_missing()?;
  # orders.mutate(b"order:7").put("f", b"state", b"placed").commit()?;
  let durable = db.durable_snapshot()?;
  if let Some(state) = orders.get_at(&durable, b"order:7", "f", b"state")? {
      // `state` survives a power loss: safe to act on, or to derive another write from.
      # let _ = state;
  }
  # drop(durable);
  # drop(orders);
  # db.close()?;
  # Ok(())
  # }
  ```

Unchanged:
- **Acknowledged commits** are never lost at their level.
- **Read-your-writes** holds after `commit()` returns.
- **Each shard** recovers a prefix of its log.
- **A cross-shard commit** is still all or nothing, and becomes visible only after its decision record meets the requested level and every participant has applied it. So its latency includes the slowest participant's group.

## Cross-shard batches
A `WriteBatch` whose rows live on different shards is atomic across them (two-phase commit over the shards' WAL streams). It returns only when every participant's record, and the coordinator's decision record, meet the requested level. After a crash, the batch is applied in full or not at all.

A participant can refuse a round: its rows' tablet is being split, merged or moved, another cross-shard commit still undecided there writes or reads the same rows, or it has no memtable room for its share. The round then aborts everywhere and the commit runs again in a new one, until it commits or the write-stall timeout ends it with `Busy` (D241). Refused rounds cost latency and throughput but nothing else. `engine_metrics().cross_shard` counts them (ICR 0043): `refused_rounds` over the cross-shard commits in an interval is the wasted rounds per commit, and `refused_moved`, `refused_pending_overlap` and `refused_room` say why. (`refused_below_floor` and `refused_move_raise` are deprecated: they are always 0 since 0.4.0, which picks the commit's timestamp at its decision.)

## Choosing a level
| If losing the last few commits … | Use |
|---|---|
| is unacceptable | `GroupSync` (default). Batch writes with `WriteBatch` or run committers concurrently so the fsync is shared. |
| is fine if the process dies but not the machine | `Buffered` |
| is fine because you can rebuild | `None` |
| is unacceptable and you cannot wait for a group | `Sync` (rarely right; it never shares an fsync) |

Throughput note: `GroupSync` costs about one fsync per group, not per commit. Many concurrent committers, or one large `WriteBatch`, amortize it. A shard keeps only a bounded number of group syncs in flight per WAL stream; durable commits that arrive while those run batch behind them and share the next sync, so under many concurrent committers each fsync covers more commits (D207). The bound is 2 on Linux and 1 on macOS (D221): on the reference machine, 2 came within about 1.5% of no bound, while 1 lost 6% at 16 concurrent committers.

macOS note: a plain `fsync` on macOS does not survive power loss, so `GroupSync` and `Sync` use `F_FULLFSYNC`, which flushes the whole drive's write cache and takes milliseconds. Those flushes are device-wide, so every shard's syncs queue behind one another on the drive: total durable throughput stays roughly flat as you add shards (on the order of hundreds of syncs per second for the whole store, each carrying its group of commits). If that matters for your workload on a Mac, put more writes in each commit (`WriteBatch`), run more concurrent committers so each sync carries more of them, or use `Buffered` where losing the last commits on a power loss is acceptable. Linux syncs each WAL file separately and is not limited this way. Sharing one flush across shards is tracked in [#685](https://github.com/CodingAnarchy/pigeonhole/issues/685).

Right after a clean reopen, the first `GroupSync` or `Sync` commit also waits for the new WAL files' and their directory's syncs, which the open no longer waits for itself (D203).

## WAL size

Each shard's WAL sidecar holds at most twice `memtable_budget` of writes past the oldest one not yet in the file (128 MiB per shard at the defaults; D155). Past that, the shard flushes the memtables holding the old writes, small ones included, so the WAL and the next open's replay stay bounded however long the process runs.

That flushing work runs beside the commits (on the flush and compaction threads, D252, or in short slices between commits where it runs on the shard, D206), so a WAL can briefly grow past the bound by what is written while it runs: a few MB at most at the defaults. When sizing disk space for the sidecars, allow per shard the bound plus that margin, and plus a spare segment.

## Failure modes
- `ErrorCode::NoSpace`: the device filled; the commit did not apply.
- `ErrorCode::RecordTooLarge`: the commit exceeds one WAL record. Split it into several batches (they are then atomic only individually).
- `ErrorCode::Io`: the write or fsync failed; treat the commit as not durable.

See [Errors](errors.md).

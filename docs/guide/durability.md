# Durability

> **Status:** this guide describes `main`, which will be released as 0.2.0; crates.io has 0.1.0, and the [changelog](../../CHANGELOG.md) lists what changed. Semantics here come from the spec and decisions D12 and D19. Async commit forms are Phase 3. Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup).

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

Log or assert on `info.durability` if your correctness depends on it. Per-level commit counts and latencies are exposed in engine metrics.

## Read-your-writes (D19)
A commit returns only once it is durable at its level **and** visible to readers, so a thread always reads its own write immediately after `commit()` returns.

Costs and caveats:
- A **cross-shard** commit (a batch whose rows live on different shards) is two-phase. It becomes visible only after every participating shard has applied it, so its latency includes the slowest participant's group.
- Any shard's in-flight group can briefly delay visibility of newer commits on other shards.
- A reader can observe a `Buffered` or `None` commit that a later power loss then removes. That is inherent in those levels: visible does not mean durable.

## Cross-shard batches
A `WriteBatch` whose rows live on different shards is atomic across them (two-phase commit over the shards' WAL streams). It returns only when every participant's record, and the coordinator's decision record, meet the requested level. After a crash, the batch is applied in full or not at all.

## Choosing a level
| If losing the last few commits … | Use |
|---|---|
| is unacceptable | `GroupSync` (default). Batch writes with `WriteBatch` or run committers concurrently so the fsync is shared. |
| is fine if the process dies but not the machine | `Buffered` |
| is fine because you can rebuild | `None` |
| is unacceptable and you cannot wait for a group | `Sync` (rarely right; it never shares an fsync) |

Throughput note: `GroupSync` costs about one fsync per group, not per commit. Many concurrent committers, or one large `WriteBatch`, amortize it.

## Failure modes
- `ErrorCode::NoSpace`: the device filled; the commit did not apply.
- `ErrorCode::RecordTooLarge`: the commit exceeds one WAL record. Split it into several batches (they are then atomic only individually).
- `ErrorCode::Io`: the write or fsync failed; treat the commit as not durable.

See [Errors](errors.md).

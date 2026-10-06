# pigeonhole-engine: open questions and proposed decisions (Milestone A)

Recorded while implementing the engine's first milestone (everything that does not need
`pigeonhole-sst` or `pigeonhole-compaction`). Each section states the interim behavior the
code implements.

## Proposed decision: a cross-shard commit is recovered all or nothing (proposed by engine)
The spec applies a PREPARE at recovery "if its coordinator stream holds the COMMIT". Under
`Buffered` a COMMIT is written once every PREPARE has been *written* (not synced), so a
power loss can keep the COMMIT and lose one PREPARE; the spec's rule then recovers half of
the commit (the model checker found this). The COMMIT record already lists its
participant streams (FORMAT §10.3), so recovery can require all of them.

**Interim behavior:** a PREPARE is applied only if the coordinator's stream holds the COMMIT
**and** every participant stream the COMMIT names holds its PREPARE for that seqno.
This never discards a `GroupSync`/`Sync` commit (its prepares were durable before the
COMMIT was written) and drops a `Buffered`/`None` one whole instead of in part.
For Milestone B this needs a checkpoint ordering rule: a participant's checkpoint may pass a
PREPARE only once the coordinator's COMMIT for it has been checkpointed (its share and every
other participant's are flushed by then, per D24), so a present COMMIT always finds every
PREPARE still in the logs. The test harness (`crates/engine/tests/common`) applies the same
rule when it reads WAL survivors.

## Q: the durability promise across shards (D42's prefix is per stream)
D42 says survivors after a crash form a prefix and "a `GroupSync` commit also makes earlier
`Buffered` or `None` records durable". With one WAL stream per shard that holds per
stream, not across shards: a `GroupSync` commit on shard 1 does not sync an earlier
`Buffered` commit on shard 0, so after a power loss commit 7 can survive while commit 6 is
lost. The spec's durability table only promises a commit at its own level, which the engine
keeps.

**Interim behavior:** the promise checked by the engine's model checker is "every commit
acknowledged at the floor level or stronger survives" (floor `GroupSync` for power loss,
`Buffered` for a process crash); earlier weaker commits may or may not. The sim `Model`'s
`crash_window` keeps its single-stream reading; the engine suite rebuilds the model from the
WAL's actual survivors instead of truncating.

## Q: a failed WAL sync after the group was applied
A group is applied to the memtables once its records are written; if the submitted sync
then fails, the data is already visible to readers while the committers must not be told
it is durable.

**Interim behavior:** the members get an `Io` error, the stream is poisoned (every later
commit on that shard fails until reopen, D35), and the applied data stays visible (it is in
the same state as a `None` commit: present until a crash that loses it). A COMMIT record
whose sync fails still decides the commit in memory; the caller gets the error.

## Q: Milestone A has no tablet splits
A new table is one tablet (spec), tablets split only at 256 MiB or under write skew, and
splits, merges and the balancer are not in Milestone A. So every row of a table lives on
one shard; cross-shard commits (two-phase commit) arise from batches that touch several
tables, which is how the simulation suite exercises them (rows spread over four tables).

**Interim behavior:** tablets are assigned to shards as `tablet_id % shards` (deterministic,
so a reopen with the same count routes identically). Splits and rebalancing are tracked as
a Phase 1 issue; the engine's write scaling until then is across tables.

## Q: WAL streams are never checkpointed or removed before flushes exist
Without SST flushes nothing can persist a memtable, so no stream is checkpointed, the clean
close keeps the sidecar files, and after a shard-count change the streams beyond the new
count stay on disk and are replayed at every open (D20's flush-then-remove needs flushes).
A full arena turns commits into `Busy` (the spec's token-bucket stall is a flush-era
behavior).

**Interim behavior:** as described; Milestone B adds flush, checkpoints, stream removal at
the last clean close, and the stall.

## Q: the default-timestamp floor is per shard, not per tablet
D11 keeps a floor per tablet so it can travel with a tablet move. Without moves, one floor per
shard (the maximum over its tablets) gives the same guarantee with one field.

**Interim behavior:** per-shard floor, persisted as the maximum over shards in `Counters`;
to become per tablet with the balancer.

## Q: a reader process pins once per attachment
The spec records "its pinned snapshot" per reader slot; a process holding several snapshots
can only pin one pair. The oldest live snapshot is the one that matters, and tracking
snapshot lifetimes needs a reclamation hook that Milestone B's flush work adds.

**Interim behavior:** the reader pins the first snapshot's `(seqno, view)` after attaching
and keeps that pin until it closes or re-attaches; later snapshots read newer views, which
the pin also protects. Conservative (nothing newer is ever reclaimed under the reader).

## Q: application-owned `close` does not block
`Engine::close` in application-owned mode would deadlock if it waited for shards that only
the caller's own thread drives.

**Interim behavior:** it tells every shard to close and returns; the application keeps
calling `EngineShard::run_once` until it returns `false`, and the last shard records the
clean close and removes the region. Engine-owned `close` waits and returns the result.

## Q: `From<format::Error>` keeps a wildcard arm (#14 vs. ICR 0001)
Issue #14 asks for no wildcard arm so a new variant is a compile error, but `format::Error`
is `#[non_exhaustive]`, which makes the compiler require one (as ICR 0001 noted).

**Interim behavior:** every current variant is matched explicitly (`InvalidArgument` gives
code 19) and the wildcard maps to `Corruption`.

## Q: `EngineOptions::merge_operators` is unused until compaction lands
`MergeRegistry::new`/`get` are `todo!()` in `pigeonhole-compaction`, so the engine cannot
look operators up yet.

**Interim behavior:** the built-in `pigeonhole.i64_add` is recognized by name; any other
operator name fails table creation and open with `UnknownMergeOperator` (unless
`allow_unregistered_merge`, which then fails reads of merged cells). Bases and operands
must carry `ValueTag::I64`, the rule `I64Add` applies, so reads and compaction agree.

## Proposed decision: a `Snapshot::at_seqno` test hook (proposed by engine)
The recovery checker reads the reopened engine at older seqnos through the current view.
`Snapshot::at_seqno(seqno)` (hidden from docs) returns the same view at a seqno at or below
the snapshot's. Additive; not part of the stable API.

## Q: a conditional write behind a write to its row in the same group
`check_and_mutate` and transaction validation read the applied state. When an earlier
member of the same group touches the row, the conditional member and everything after it
wait for the next group, so conditions see the applied state and per-row submission order
holds.

**Interim behavior:** as described (the cut costs one extra group commit in that case).

## Measured: the D29 copy threshold and the `arc-swap` guard (#15)
`cargo bench -p pigeonhole-engine --bench engine`, Apple M5 (10 cores), macOS, memtable-resident
data (non-reference hardware, D5). Point get of one cell by value size, `get_latest`
(arc-swap guard) versus `get` (snapshot: `Arc<View>` clone):

| value bytes | 16 | 64 | 128 | 256 | 512 | 4096 |
|---|---|---|---|---|---|---|
| `get_latest` | 303 ns | 263 ns | 289 ns | 299 ns | 270 ns | 270 ns |
| `get` + snapshot | 309 ns | 272 ns | 301 ns | 307 ns | 281 ns | 280 ns |

A miss costs 194 ns. With all 10 cores reading one cell: 30.0 ns per get aggregate
(`get_latest`, 64-byte value) versus 30.3 ns (`get`); 43.6 versus 44.0 ns for a 512-byte
value. Copying at or below 128 bytes and pinning above it are indistinguishable within noise
(the two skiplist seeks of a point get dominate), and the guard saves about 3 % single-threaded
and nothing measurable under contention.

**Interim behavior:** D29 stands as written (128-byte threshold, guard on `get_latest`); the
coordinator may record these numbers in D29.

## Resolutions from the Milestone A review (coordinator, 2026-10-06; to be numbered)
- **All-or-nothing 2PC recovery: confirmed**, plus: a flush may not persist a share of a
  cross-shard commit until every PREPARE and the COMMIT are durable (in #37's checklist).
- **Cross-shard durability: confirmed**; D42 is amended to "per stream" by the coordinator.
  The engine's checker verifies the per-stream prefix for single-shard commits and the
  per-commit promise for the rest.
- **Failed sync leaves data visible: confirmed**; documented on `Engine::commit` next to
  `Durability::None`.
- **No splits: deferred** (#38). **No checkpoints: Milestone B** (#37).
- **Per-shard timestamp floor: confirmed**, seeded from every replayed commit timestamp at
  open (D11).
- **Reader pins: changed.** A reader re-pins at its next snapshot once its live-snapshot
  count drops to zero, so a long-lived reader never blocks reclamation for ever (#39 keeps
  the oldest-live-snapshot refinement).
- **Application-owned close: confirmed**; documented that `PendingCommit::wait` on the
  thread that drives the commit's shard deadlocks (poll the future from the event loop).
- **`From<format::Error>` wildcard → `Corruption`: confirmed.** **`merge_operators` unused
  until compaction: confirmed.**
- **`Snapshot::at_seqno`: kept out of the public API** behind the non-default `test-hooks`
  cargo feature, enabled only by the crate's own tests.
- **Conditional-write deferral: changed.** A conditional member's written rows, read keys
  and predicate row are all checked against the group's earlier writers (it then runs in
  the next group) and against prepared, undecided shares (a single member waits for the
  decision; a PREPARE aborts with `Conflict` rather than wait on another commit's decision).
  Every shard owning a row a transaction read is a two-phase-commit participant with an
  empty PREPARE that validates it (no cross-shard write skew). Arena room is reserved per
  group and per prepared share, and a participant's apply error reaches the coordinator, so
  a half-applied commit is never acknowledged.
- **D29 numbers: confirmed**, folded into D29 by the coordinator.
- **Resolver:** Milestone B replaces `resolve.rs` with `pigeonhole-compaction`'s shared
  resolver; where they differ, compaction's behavior wins (a time range on a family with a
  merge operator applies to resolved versions per D82, an unknown operator is an error, value
  predicates per D77). The engine's resolver already follows D82, D75, D76 and D77.

# 0147: what a partition's new owner replays an old owner's tail with

**Status:** Proposed (D336 PR 1, #2148; numbers: coordinator, 2026-11-26).

## Change

`pigeonhole-engine` (hidden, outside the stable API) and `pigeonhole`, behind the new hidden feature `__unstable-tail-replay`. That feature turns on `__unstable-replication` and `__unstable-txn-prepare`, and `phdb serve` turns it on. All of it is additive.

```rust
// pigeonhole-engine
impl WriteBatch {
    /// One change exactly as a primary logged it, beside the batch's ordinary writes.
    pub fn push_logged_beside(&mut self, table: TableId, family: FamilyId, kind: Kind,
        row: &[u8], qualifier: &[u8], ts: Timestamp, stored: &[u8]) -> Result<()>;
}
impl Engine {
    /// `commit_logged`, tagged with `xid`.
    pub fn commit_logged_with(&self, batch: WriteBatch, durability: Option<Durability>,
        xid: Option<&[u8]>) -> Result<CommitInfo>;
}
impl Txn {
    /// The row at the snapshot as stored: every version and delete marker, in logged form.
    pub fn read_row_logged(&mut self, table: TableId, row: &[u8]) -> Result<Vec<CopiedCell>>;
}

// pigeonhole, #[cfg(feature = "__unstable-tail-replay")], #[doc(hidden)]
impl LoggedBatch<'_> {
    pub fn tag_xid(&mut self, xid: &[u8]) -> &mut Self;
}
impl Transaction {
    pub fn push_logged(&mut self, table: &Table, row: &[u8], family: &str, kind: ChangeKind,
        qualifier: &[u8], ts: u64, stored: &[u8]) -> &mut Self;
    pub fn read_row_logged(&mut self, table: &Table, row: &[u8])
        -> Result<Vec<replication::CopiedCell>>;
}
```

## Why

D336 (#2148) replays an old owner's unreplicated tail into the partition's new owner after a failover or promote.
- A plain tail commit is one logged batch, tagged with its origin (`LoggedBatch::tag_xid`). The xid names the origin's history epoch and seqno, behind a version byte, so the new owner's feed delivers the origin and a later replay of the same commit skips it.
- A tail commit that wrote a text or vector index, or a D311 memory store, is replayed as **one transaction** (D336 Amendment 3):
  - the index's own writes go through the index's paths (`*_in`);
  - the plain changes go in exactly as logged (`Transaction::push_logged`);
  - the transaction reads the source row raw (`Transaction::read_row_logged`) to decide whether anything newer is there.

## Semantics

**`push_logged_beside` / `Transaction::push_logged` (simp724 on D336 PR 1):**
- **The batch stays an ordinary one.** `WriteBatch::push_logged` marks a whole batch logged, so `route` would skip the counter prepare, the HLL and bloom conversions and the row-delete expansion for every mutation in it. These changes are kept in a second section instead.
  - `logged()` still means "a logged section and no ordinary one".
  - A mixed batch takes the ordinary path, so a replica refuses it as it refuses any own write.
  - `commit_logged` and `apply_replicated` refuse it (`InvalidArgument`: not a logged batch).
  - A batch that `push_logged` marked, with changes beside it as well, is refused at commit (`InvalidArgument`).
  - `check_and_mutate` refuses a batch with changes beside it (`InvalidArgument`; simp724's P1 on #2235). Its check that the batch touches only the checked row reads the ordinary part, so it would not see them.
- **Order inside the commit:**
  - `route` rewrites the ordinary part first: row deletes expanded, counter writes prepared, the fixed counter timestamp, conversions.
  - It then appends these changes unchanged, after a logged batch's checks: each family in its table, a merge's operator known, the table's tablets present. Their shards are added to the commit's.
  - So where an ordinary write and one of these write one cell at one timestamp, this one wins (D34's last mutation). That includes a `delete_row`'s family delete at the commit's timestamp, which a logged cell at that exact timestamp survives.
  - Below `route`, the builder is all stored form. Shards, WAL replay, cross-shard shares and the feed see no difference.
- **The timestamp floor isn't raised** to these changes' timestamps, as for `commit_logged`: the caller decides. D336 §5 doesn't raise it, on purpose (the owner's decision): a replayed cell is a write with an explicit timestamp. So an ordinary write in the same transaction, or a later one, without a timestamp takes the clock. It can sort below a logged change's timestamp, and the logged change then wins whatever their order.
- **Admission** charges these changes' bytes too (`batch_bytes`). A batch of them alone picks its admission shard by its first one.
- **The caller's row checks count these rows as written.** `phdb serve`'s replay must pass them through:
  - the D331 range gate (`touches`, the move's fence, `check_keys`);
  - D329's read holds on written rows;
  - the write gate the replay runs under (D326).
  
  D336 PR 3 does this. The engine has no row check of its own to bypass: it routes these rows to their shards like any write.
- **Validation:** a transaction's validation is by its read set, so these changes conflict as any write does with another transaction's reads.

**`read_row_logged`:**
- The row at the transaction's snapshot as a range move's copy reads it (`Engine::copy_range` over `[row, row\0)`): every version and delete marker a reader there could need, in logged form, a counter or aggregate family's operands folded into their base.
- It records a read of every family of the table (as `get_row` with no families). A commit to the row after the snapshot, a delete marker included, then fails the transaction with `Conflict`, and the replay decides again on a rerun.
- **Its limit:** what compaction already dropped below every live snapshot isn't there, as for `copy_range`. A delete marker compacted away with the versions it covered no longer hides anything older, so a tail put below it is visible again, as any write at that timestamp would be.

**`commit_logged_with` / `LoggedBatch::tag_xid`:**
- The tag is a spanning commit's trailer (D329 2b), written only by the routing step that writes `Transaction::tag_xid`'s (`tag_routed`). That step sets `incompat` bit 2 first.
- So there's no new format or feature bit (the coordinator's condition). The replay's version byte is inside the tag's bytes.
- 1–255 bytes, checked when the commit is routed. `None` is `commit_logged`.

## D336 PR 3a: what the replay adds

Two more hidden calls, under the same feature, and one wire field. All additive.

```rust
// pigeonhole, #[cfg(feature = "__unstable-tail-replay")], #[doc(hidden)]
impl LoggedBatch<'_> {
    /// `commit`, at `durability` (`commit_logged_with` with the engine's durability).
    pub fn commit_with(self, durability: Durability) -> Result<CommitInfo>;
}
impl Feed {
    /// Whether the commit the last `next` delivered was feed-silent (a range move's copy
    /// or cleanup): the tail source tells the replay, which skips it by name.
    pub fn delivered_silent(&self) -> bool;
}
```

- **`commit_with`:** a replayed commit is one logged batch with its mark, at `GroupSync`, so the mark is durable with the commit it records. `commit` keeps the database's default durability.
- **`ResolveTxn.read_only` (`cluster.proto`, field 3):** the replay asks a share's home what its decision row says, and nothing more. The home answers COMMITTED or ABORTED from the row and UNKNOWN for no row, and records nothing; the resolver's own ask records an abort over an absent row (D329 2b), which must never follow from a replay.
  - **Mixed versions:** a node that doesn't know the field ignores it and runs the resolver's ask, which may record that abort. No released build has `tail_replay`: a replay runs only once the cluster adopted it, so every node of the map knows `read_only` by then.
- **The replay's rows pass the caller's checks** (above):
  - D331's range gate: each replayed row as a write (`RangeState::check_rows`), before anything is dropped or written. A refusal stops the run, which a rerun resumes from the mark. While the tail is PENDING, no move of the partition starts (D336 PR 2).
  - D329's holds: the engine's, which see a logged commit as any (`overlaps_undecided` reads the batch). A commit writing a row an undecided share read waits for the decision, and one writing a row the share writes is ordered by the decision's seqno (D271).
  - The write gate (D326): each commit enters it, and the drop of (b) below does too.
- **Amendment 5's (b), drop then apply:** a share the new owner's copy holds in doubt is first dropped (`Pigeonhole::decide(xid, false)`), then applied with the mark in one commit. The drop writes no data, no decision row and no feed event: an external share's abort delivers nothing (the engine's feed reader). A crash between the two leaves the share decided nowhere here and the mark unmoved, and the rerun asks the home (a).
  - **When D335 PR 4 lands,** a replicated prepare can be in doubt on the new owner, and "a branch keeps no record of a share it decided" stops being enough: deciding a replicated prepare that a PENDING tail names must then go through the replay's gate and write the mark in the same commit. Whichever of D336 and D335 PR 4 lands second wires it.
    - **The race it closes** (simp724's N2 on #2265): the resolver can decide X between the replay's in-doubt check and its drop. The drop then fails, and the rerun takes (a), finds COMMITTED, and applies the share a second time. In 3a a copy holds no replicated prepare, so nothing reaches it.

## Not in this ICR

- **`promoted_from`** (D336 §3) is dropped (Amendment 4). The fork token lives in the promote intent's sibling file, in a `__phdb_cluster` row written with the identity, and in the map. It's cli-only, with no format change.
- **The tail source's open** needs no engine change. The source takes no writes, so retention trims nothing between its open and the retention hold (`hold_feed_retention`, ICR 0141) the cluster layer takes right after it. D336 PR 3 takes it.

## Callers

`phdb serve`'s tail replay (D336 PR 3). Tests: `crates/pigeonhole/tests/tail_replay.rs`, `crates/engine/tests/it/logged_beside.rs`, and the cluster's `tests/fence/tail_replay.rs` and `tests/spanning.rs` (the read-only resolve). The `delete_row` tie (a logged cell at exactly the commit's timestamp) has no test, because a test can't pin the commit's timestamp.

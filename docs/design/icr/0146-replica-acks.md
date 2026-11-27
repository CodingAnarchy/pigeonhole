# 0146: replica acknowledgements (D335)

**Status:** Approved (coordinator, 2026-11-26; D335, #2149; numbers in `.coord/numbers.md`). PR 1's part is below, then [PR 2a's](#pr-2a-the-wait); D335's later PRs amend this request with theirs.

## Change

The network API (`pigeonhole.v1`), additive:

- **`common.proto`:** `message Position { uint64 seqno = 1; uint32 n = 2; }`. A position in a partition's replicated feed: the last commit at or below `seqno`, then the first `n` events after it that aren't commits (D335's replicated prepares and aborts, PR 4). A commit's position has `n` 0. Positions compare as `(seqno, n)`.
- **`replication.proto`:**
  - `rpc FollowAcked(stream FollowMessage) returns (stream ReplicationBatch)`.
  - `message FollowMessage { oneof kind { FollowStart start = 1; Ack ack = 2; } }`.
  - `message FollowStart { FollowRequest request = 1; string node = 2; string partition = 3; }`.
  - `message Ack { Position position = 1; }`.
- **`cluster.proto`, `LocalPartition`:**
  - `reserved 35` (#2150's unused `allocated_bytes`);
  - `Position acked_position = 36`, `uint32 healthy_replicas = 37` (on an owner);
  - `Position replica_applied_position = 38` (on a replica copy).
- **The map feature string `replica_acks`** in `NodeStatus.map_features`: the node serves `FollowAcked`. PR 2a's map field has its own string, `replica_ack_wait`.
- **`/metrics`, per partition on its owner:**
  - `pigeonhole_cluster_replica_ack_wait_seconds{partition}`, a histogram from a write's commit to its acknowledgement. In PR 1 it's measured to the first replica's; from PR 2, to the `k`-th.
  - `pigeonhole_cluster_replica_acked_seqno{partition,replica}`, a gauge: each counted replica's last acknowledgement.

## Semantics

- **`FollowAcked`** is `Follow` for a cluster partition's map replica:
  - The first request message is a `FollowStart` naming the follow request, the replica's node id (the same as the request's node-id header) and the partition.
  - The owner serves that partition alone, under the same checks as `Follow`: the caller is a map replica of it, under the cluster token. It sends what `Follow` sends.
  - Each later request message is an `Ack`. A different message ends the owner's reading of acknowledgements.
  - A protocol error answers `FailedPrecondition`, never `InvalidArgument`, which a replica takes as another database's token.
  - gRPC only. A single node answers `FailedPrecondition` (its `--replica-of` replicas use `Follow`), and an older build `Unimplemented`.
- **An acknowledgement** of position `p` means:
  - the copy's stored token is at `p`;
  - everything at or before `p` is applied and durable in the copy.

  A replica sends one after each apply that stored its token (all are `GroupSync` commits), never for a batch with no events, and never past what it stored.
- **On the owner:** one counted stream per replica node. A new stream from the node replaces the old one's entry, and its first acknowledgement replaces the entry even when lower. After that the entry only rises while the stream lives. A stream's end removes it. `Status` counts only the streams of the partition's map replicas now.
- **The replica:**
  - A cluster replica copy opens `FollowAcked` on every follow, and `Follow` when its owner answers `Unimplemented` (an older build). Nothing is remembered, so an owner upgraded at the same address takes acknowledgements from its next follow on.
  - With acknowledgements, it also cuts a run where the owner's sync group ends (a batch whose last event reaches the batch's `primary_seqno`), so each group is applied and acknowledged on its own.
  - A single node's `--replica-of` replica keeps `Follow`.
- **No write waits in PR 1.** [PR 2a](#pr-2a-the-wait) adds the map's `replica_acks` and the wait.

## Callers

- `pigeonhole-cli` (`serve`):
  - `grpc.rs`'s `follow_acked`;
  - `router.rs`'s `FOLLOW_PARTITION`;
  - `acks.rs`, the owner's table;
  - `replica.rs`'s follower;
  - `status.rs`;
  - `features.rs`'s `REPLICA_ACKS_FEATURE`;
  - the `/metrics` render.
- No crate outside `pigeonhole-cli` changes.

## PR 2a: the wait

### Change

The network API (`pigeonhole.v1`), additive:

- **`cluster.proto`:**
  - `Partition.replica_acks = 12` (`uint32`): k, how many of the partition's replicas acknowledge a write before its owner answers it. 0, the default, is asynchronous.
  - `ForwardedCaller.replication = 5` (`ReplicationMode`): the caller's header, carried to the owner by a node that forwards the call.
- **`common.proto`:**
  - `enum ReplicationMode { REPLICATION_MODE_DEFAULT = 0; REPLICATION_MODE_ASYNC = 1; }`.
  - `CommitInfo.replica_acks = 3` (`uint32`): how many replicas acknowledged the commit before it was answered. It's at least k for a covered commit, and 0 for an asynchronous one.
- **`data.proto` and `transactions.proto`:** `ReplicationMode replication` on `MutateRequest` (3, so `Mutate` and `MutateReturning`), `BatchRequest` (3), `TxnCommitRequest` (3) and `CheckAndMutateRequest` (4).
- **The header `x-pigeonhole-replication`:** `async` or `default`, in any case. It's read on HTTP and as gRPC metadata, on a cluster node only. Another value is `InvalidArgument`.
- **The flag `phdb serve --cluster-replica-ack-timeout-ms`:** default 5000, at least 1.
- **The map feature string `replica_ack_wait`** in `NodeStatus.map_features`: the node keeps `Partition.replica_acks`.
- **`/metrics`, per partition on its owner, each `{partition}`.** These are shown for a partition at k ≥ 1, or one that has counted something:
  - `pigeonhole_cluster_replica_ack_timeouts_total` (counter);
  - `pigeonhole_cluster_replica_ack_lease_lost_total` (counter);
  - `pigeonhole_cluster_replica_ack_short_seconds_total` (counter);
  - `pigeonhole_cluster_replica_ack_refused_total` (counter).
  
  `pigeonhole_cluster_replica_ack_wait_seconds` now runs to the k-th acknowledgement (the first at k = 0).
- **`Status`:**
  - `LocalPartition.acked_position` is the k-th highest acknowledgement of the map's replicas (the highest at k = 0).
  - `healthy_replicas` follows §7 (below).

### Semantics

- **The map:**
  - k is at most `len(replicas)`, or the map is refused (`InvalidArgument`).
  - A map sets k > 0 only once every node reports `replica_ack_wait`, the first such map, a first map included (`FailedPrecondition` naming the nodes).
  - The owner takes k and the replicas at each partition's open and at every map install, both under the node's install lock.
- **Who waits:**
  - The map's k decides, never whether a replica stream is attached at that instant.
  - A write waits unless it opted down, by the request's `replication: REPLICATION_MODE_ASYNC` or the header.
  - An older forwarder leaves `ForwardedCaller.replication` at `DEFAULT`, so a header it took is dropped and the write waits as the map says: the safe direction.
  - A write that waits has `DURABILITY_NONE` or `DURABILITY_BUFFERED` raised to `GROUP_SYNC` on `Mutate`, `MutateReturning`, `CheckAndMutate`, `Batch`, and a transaction begun with them. The server's default is left alone. `CommitInfo.durability` reports what was applied.
- **The wait:**
  - It comes after the engine's commit and before D326's after-commit lease check.
  - It lasts until k of the map's replicas acknowledged the commit's own position (`(seqno, 0)`, the seqno its `CommitInfo` names). A reply that names no seqno waits for the node's visible seqno read after the commit. That's at or past its own, so it may wait longer, never less.
  - **At the timeout,** `OutcomeUnknown` ("not acknowledged within"). The write is committed on the owner and may be covered later. A transaction's `Commit` records that outcome, so a repeated `Commit` or `Transactions.Outcome` keeps answering `OutcomeUnknown`, even once the write is covered (simp724's Q1).
  - **When the partition's lease ends first,** `OutcomeUnknown` at once: a lapse wakes it, and a lease that runs out wakes it at its end.
  - **When the partition closes here,** `OutcomeUnknown`.
- **Too few healthy replicas (REFUSE):**
  - A write that would wait is refused at admission with `Unavailable` while fewer than k of the map's replicas are healthy. It's retry-safe: nothing was written.
  - **Healthy:** a counted replica, unless the lowest uncovered write began waiting at least 2 s ago (twice the follow's heartbeat) for a position past the replica's last acknowledgement, and the replica acknowledged nothing new in that time.
  - Every write that waited stays uncovered until the frontier passes it, a timed-out one included, so a replica that stopped acknowledging reads unhealthy though no write waits any more (simp724's N1).
  - An idle partition's replicas stay healthy on their stream alone.
  - `ASYNC` (acknowledging uncovered while short) is D335 PR 2b, below.
- **Who counts:**
  - Only the map's replicas of the partition.
  - A node the map no longer lists stops counting at that install (feed1248's A1), by the count's filter. Its stream's entry is kept: nothing ends the stream at the install, and a node removed and added back by two quick installs may keep the same live stream, whose acknowledgements stay true of its copy.
  - An install that changes the partition's `history_epoch` forgets every acknowledgement, since its positions were the old history's. The streams stay attached, so each counts again from its next acknowledgement, without a reconnect (feed1248's Q1 and Q2).
  - Any other install leaves the counted streams and the waiting writes alone.
  - A stream that attaches while its node isn't listed yet is kept, and counts once the map lists it.
- **The write paths that wait:**
  - every call `Service::call` admits as a write on a cluster partition: `Data`'s `Mutate`, `MutateReturning`, `CheckAndMutate` (when it applied) and `Batch`; `Text.Put` and `Text.Delete`; `Vectors.Insert`, `Vectors.Delete` and `Vectors.BulkLoad` (once, after its last chunk); and a `Memory` write where it reaches its partition through `Service::call`;
  - a single-partition transaction's `Commit`, before its outcome is recorded, so a repeated `Commit` or `Transactions.Outcome` never reads it as answered sooner.
- **Spanning transactions (D329), until D335 PR 4:**
  - Their writes can't be covered until PR 4 replicates prepares, and no ack wait goes on a decide (simp724's S4).
  - So a spanning transaction that writes, and touches a partition whose map sets `replica_acks` (its home, a branch, or a partition its commit's rows go to), is refused at its `Commit`: `FailedPrecondition`, before any prepare. Nothing is written and the transaction stays open. This fails closed, like REFUSE (simp724's P1, the coordinator's ruling).
  - Committed opted down (`REPLICATION_MODE_ASYNC`, or the header), it commits uncovered, answered with `replica_acks` 0.
  - A read-only spanning transaction commits as before.
  - PR 4 lifts the refusal.
- **The write paths that don't wait:**
  - A spanning transaction that opted down: its home's commit, `BranchDecide`'s shares and the resolver's decisions.
  - Work no client waits on: range-move copies, catch-ups, drops and prunes (feed-silent), text index rebuilds and packs, vector rebalances, and a memory store's background loops.

### Callers

- `pigeonhole-cli` (`serve`):
  - `acks.rs`, the wait, the admission check and the counters;
  - `service/dispatch.rs`, `service/txn.rs` and `service/ops.rs`, the write paths;
  - `gate.rs`'s lease-ended `Notify`;
  - `grpc.rs` and `http.rs`, the header;
  - `cluster/forward.rs`, `ForwardedCaller`;
  - `cluster/install.rs`, `open.rs` and `start.rs`, the configuration at install and open;
  - `cluster/map.rs`'s validation and `features.rs`'s `REPLICA_ACK_WAIT_FEATURE`;
  - `config.rs`'s flag.
- No crate outside `pigeonhole-cli` changes.

## PR 2b: the ASYNC fallback

### Change

The network API (`pigeonhole.v1`), additive, all in `cluster.proto`:

- **`enum ReplicaAckPolicy`:**
  - `REPLICA_ACK_POLICY_UNSPECIFIED = 0`, read as `REFUSE`;
  - `REPLICA_ACK_POLICY_REFUSE = 1`;
  - `REPLICA_ACK_POLICY_ASYNC = 2`.
  
  `Partition.replica_ack_policy = 13`: what the owner does with fewer than k healthy replicas.
- **`LeasePart.degraded_from = 6` (`Position`) and `LeasePart.degraded_clear = 7` (`bool`):**
  - From an owner: record where its fallback began, or that it ended.
  - In a grantor's `granted` entry: echoed only once the grantor has synced that record.
- **`NodeStatus.degraded = 19`** (repeated `DegradedRecord { partition = 1; epoch = 2; state = 3; degraded_from = 4; }`): a grantor's durable records.
  - `enum DegradedState { DEGRADED_STATE_UNKNOWN = 0; DEGRADED_STATE_CLEAR = 1; DEGRADED_STATE_FLAGGED = 2; }`.
  - A partition the node has no record of is absent, which means unknown.
- **`LocalPartition.degraded_from = 41`** (`Position`): on the owner, while it's in the fallback.
- **`BranchPrepare.replication = 3`** (`ReplicationMode`): the spanning commit opted down.
- **The map feature string `replica_ack_policy`:** the node keeps `Partition.replica_ack_policy`, and records and echoes the lease's fallback marks.
- **`/metrics`, per partition on its owner, `{partition}`:**
  - `pigeonhole_cluster_replica_ack_degraded` (gauge: 1 while in the fallback);
  - `pigeonhole_cluster_replica_ack_degraded_total` (counter: entries into it).
- **The node file `cluster-degraded.json`:** a grantor's records, JSON, written whole and synced. A file that doesn't parse fails the node's start.

### Semantics

- **The map:**
  - A policy this build doesn't know is refused (`InvalidArgument`).
  - A map sets a policy (any value) only once every node reports `replica_ack_policy`: the first such map, a first map included.
  - The policy is inert at k = 0.
- **Entering (ASYNC):**
  - A write admitted while the partition is short is taken, not refused.
  - The owner enters the fallback from the position just past its acked frontier (`(seqno, n + 1)`, or `(0, 0)` with no frontier). Every write not covered then is at or past it.
  - It starts a lease round at once, rather than at the next third of a lease.
- **Recording:**
  - Every round while in the fallback carries `degraded_from`.
  - A grantor records it per partition and epoch, syncs it before answering, and echoes it in its grant.
  - When a majority of the round's map's nodes echo exactly that start (counted as the lease counts, the owner's own grant included), the start is recorded for that map's node set.
  - A pre-1.3 node's phantom grant never echoes. Neither does a grant whose record write failed.
- **Answering uncovered:**
  - While recorded for the map's current node set, the partition still short, and the fallback not leaving, a write at or past the start is answered without its acknowledgements. `CommitInfo.replica_acks` gives the count it had, usually 0.
  - Writes already waiting are answered when the record lands. That includes a write admitted under `REFUSE` before the policy changed, when its position is at or past the start (simp724's Q1).
  - Before the record lands, a write waits as in 2a and answers `OutcomeUnknown` at the timeout. Uncovered answers stop while the partition is healthy, so the last uncovered position stops moving and the frontier can catch up.
- **The node set (simp724's P2):**
  - An install whose node set differs from the one the start was recorded for drops the record.
  - Nothing more is answered uncovered until a round's majority of the new set echoes it, since a majority of the old set may share no node with one of the new.
- **A lower echo:**
  - A grantor holding an earlier start (an owner that lost its own) echoes that one.
  - The owner moves its start down to it, which only covers more, and counts it again.
  - A grantor never raises a recorded start without a clear between.
- **Leaving:**
  - Once k replicas are healthy and the frontier has passed every write answered uncovered, each round carries `degraded_clear`, and writes wait for their acknowledgements as usual.
  - Once a majority echoes the clear, the fallback is over.
  - Short again first: the clear is given up, and the start and its record are kept.
- **A grantor's records (simp724's M1, Q2):**
  - **First grant:** a grantor records `CLEAR` when it first grants a `replica_acks` partition at an epoch, so a partition with no record is only ever unknown.
  - **Order:** records compare by epoch first, then state. A lower epoch's round never changes one, so an old owner's late round can't lower what a promoted owner's rounds recorded.
  - **At grant time:** the request's map is the grantor's own, so a record of another history (a newer one: P was re-created at lower epochs; an older one: a past history) is replaced.
  - **At an install:** the records of partitions the map no longer has are dropped, and so are those of an older history than the map's. One of a newer history is kept, as this node's map may lag.
  - **Retention:** records survive the grant's expiry and restarts.
  - **Echoed unasked:** a `FLAGGED` record is echoed in every grant of the partition at that epoch, unless the round asks to clear. An owner not in the fallback that sees one resumes it (as at a reopen, below) and clears it. That covers an owner that restarted without its own record, and a grantor that missed the clearing round (one in its start grace, say), so no grantor keeps a stale flag.
- **Reopen (simp724's P1):**
  - An owner that opens a partition whose own grantor record is `FLAGGED` at the current epoch and history resumes the fallback from that start.
  - It takes every write up to the partition's visible seqno at open as possibly answered uncovered, so it leaves only once the replicas hold them all.
  - The start is recorded again by the next round, which starts at once.
  - A replica's new stream acknowledges only once something new is applied, so a reopened partition that stays idle stays in the fallback until its next write. That write waits for its acknowledgement as usual, and covers the rest.
- **Spanning transactions:**
  - The commit-time refusal is 2a's, whatever the policy.
  - A writing `BranchPrepare` carries the commit's opt-down (`replication`).
  - A branch's owner also refuses a writing prepare that didn't opt down when its own map sets `replica_acks` (`FailedPrecondition`). This covers a map that reached the branch's owner a push before the driver (simp724's note 1).
  - The driver then aborts the other branches and answers `Conflict`, retry-safe.
  - **Mixed versions:** an older driver leaves `replication` `DEFAULT`, so its opted-down prepares on such a partition are refused, failing closed.
- **Unchanged:** `REFUSE` (unset or explicit) is 2a's. Opted-down writes never wait and never enter the fallback.

### Callers

- `pigeonhole-cli` (`serve`):
  - `acks.rs`, the fallback state;
  - `cluster/degraded.rs`, the grantor's records;
  - `cluster/fence.rs`, the marks, echo counting, the kick and the phantom grant;
  - `cluster/api.rs`, the `Lease` handler's records;
  - `cluster/install.rs`, the policy gate, pruning, and `arm_acks` with P1's resume;
  - `cluster/open.rs` and `start.rs`;
  - `cluster/status.rs`;
  - `cluster/spanning.rs` and `service/spanning.rs`, the prepare's opt-down and the owner's refusal;
  - `cluster/map.rs` and `features.rs`.
- No crate outside `pigeonhole-cli` changes.

# 0146: replica acknowledgements (D335)

**Status:** Approved (coordinator, 2026-11-26; D335, #2149; numbers in `.coord/numbers.md`). PR 1's part is below; D335's later PRs amend this request with theirs.

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
- **The map feature string `replica_acks`** in `NodeStatus.map_features`: the node serves `FollowAcked`. D335's map fields (PR 2) come under the same string.
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
  - A cluster replica copy uses `FollowAcked` unless its owner answered `Unimplemented`; it then uses `Follow` until the owner's address changes.
  - With acknowledgements, it also cuts a run where the owner's sync group ends (a batch whose last event reaches the batch's `primary_seqno`), so each group is applied and acknowledged on its own.
  - A single node's `--replica-of` replica keeps `Follow`.
- **No write waits yet:** D335 PR 2 adds the map's `replica_acks` and the wait.

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

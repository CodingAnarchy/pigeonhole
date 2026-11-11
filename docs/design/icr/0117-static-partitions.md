# 0117: static partitions across `phdb serve` nodes (the partition map, `Cluster.*`, the cluster scope)

**Status:** Approved. The number is from the coordinator. The design is D317 ([#1482](https://github.com/CodingAnarchy/pigeonhole/issues/1482#issuecomment-6637596392), revision 2): perf287's and cli692's reviews are folded in, and the owner signed it off on 2026-11-04. cli692 agreed to ticket 1's serve shapes, and the coordinator chose the `SetMap` error code (2026-11-09). This file grows with D317's tickets. Ticket 1 is below; later tickets add their sections.

## Ticket 1: the partition map

**proto (`proto/pigeonhole/v1/cluster.proto`, additive in v1):**
- `ClusterMap { cluster_id, map_version, nodes, partitions, tables }`, `Node`, `Partition { id, node, epoch }`, `TablePlacement { name, group, families, splits, partitions }` and `SplitKey { oneof key { text, raw } }`.
- **Split keys:** they compare as their bytes, so the `text` and `raw` forms of one key are one key. The canonical form prints `text` when the bytes are UTF-8.
- The `Cluster` service: `GetMap`, `SetMap`, `PushMap` and `Status`.

**What a valid map is:**
- **Ids:** `cluster_id`, node ids and partition ids are `[a-z0-9_-]`, 1–64 characters, not starting with `-`. A partition id is also its file's name.
- **Versions and epochs:** `map_version` and every epoch are at least 1.
- **References:** every partition's node is listed, and every placement's partitions are listed.
- **Placements:**
  - names are unique, and none starts with `__phdb_`;
  - split keys are non-empty, distinct and increasing;
  - `partitions.len() == splits.len() + 1`;
  - families are valid `FamilySpec`s;
  - a co-location group's tables are unsplit, on one partition.
- **Plaintext clusters:** on a node without TLS (`--insecure-no-tls-no-auth`, or tokens on a loopback listener), every node address must resolve to loopback, since the node would send its cluster token to them in the clear.

**What a change may do:** these rules apply one step at a time, where a map is set (`SetMap`, against the node's current map). A node taking a map from a peer (`PushMap`, a pull, catching up at start) checks only that it's valid, of the same `cluster_id`, and newer. It may have missed several steps, and the rules don't hold across a jump: a table dropped and created again with other splits, or a partition dropped and added back at a lower epoch.
- The same `cluster_id` and a greater `map_version`.
- Existing placements keep their split keys (compared as bytes), partitions and group. Moving ranges is #1499.
- A partition that holds a placement isn't dropped.
- A partition's epoch never falls, and its node changes only with a greater epoch.

**`phdb serve --cluster DIR --node-id ID`:**
- **It replaces the positional `FILE`.**
  - `DIR/cluster-map.json` holds the node's newest map: canonical proto3 JSON, written temp → fsync → rename → directory fsync.
  - `DIR/<partition>.phdb` are the partition files.
  - `DIR/.lock` is held exclusively while the server runs, and `phdb cluster init-partition` takes it too.
- **Refused with `--cluster` at startup, until a later D317 ticket defines them:** `--replica-of`, `--memory` and `--create`.
- **Ticket 1 serves no data in cluster mode:** routing is ticket 2. The node runs the `Cluster` service and health.
- **Flags:**
  - `--cluster-token-file FILE`: the token this node presents to its peers.
  - `--cluster-push-timeout-ms` (default 2000): each push's bound.

**The `cluster` token scope:**
- **A fourth scope outside the `read < write < admin` chain.** An admin token isn't a cluster token, and the reverse holds too.
- **Where it's accepted:** `Cluster.PushMap` (only there), plus `Cluster.GetMap` and `Cluster.Status`. Later tickets add the node-to-node forwards.
- **Token file line:** `<token> cluster`.
- **Rotation:** list the old and new cluster tokens on every node, then switch each node's `--cluster-token-file`.

**The `Cluster` service:**

| RPC | Scope | HTTP | What it does |
|---|---|---|---|
| `GetMap` | read, admin, cluster | `GET /v1/cluster/map` | The node's newest map (`NotFound` before it has one). |
| `SetMap` | admin | `POST /v1/cluster/map` | See below. |
| `PushMap` | cluster | gRPC only | Node to node, see below. |
| `Status` | read, admin, cluster | `GET /v1/cluster/status` | The node's id, map version, `cluster_id`, partition files (relative names) with their identity rows, and `divergent` peers. |

**`SetMap`:**
- **First map:** a node's first map takes `expected_map_version = 0`, and must name the node it's sent to.
- **Steps:** it validates (and validates the change from the current map), persists, and pushes to every other node in parallel, each push within `--cluster-push-timeout-ms`.
- **The answer:** sent once the map is persisted here, with each push's result. A node the push missed pulls the map later (below).

**`PushMap`:**
- **Accepted:** a valid map with a greater version and the same cluster (the change rules were checked where it was set).
- **The same version with the same canonical JSON** is a no-op answering ok.
- **The same version with different content** is divergent. It's refused, and both nodes report it in `Status.divergent` until a newer map is set.

**Pull:**
- **Who triggers it:** a call from a `cluster`-scope caller that carries `x-pigeonhole-map-version` greater than the node's own, with `x-pigeonhole-node-id`.
- **What it does:** the node fetches `GetMap` from that node's address in its current map, once per version, in the background. It takes the result only through `PushMap`'s checks.
- **Other callers:** the headers are ignored.

**Errors (no new server codes):**
- **`Conflict`** (`ABORTED`, HTTP 409, not applied, retry from `Begin`): `SetMap`'s `expected_map_version` isn't the node's version. Re-read the map and try again; the message carries the node's current `map_version`.
- **`InvalidArgument`:**
  - a map that fails validation;
  - a change the map may not make;
  - a map of another cluster;
  - a pushed map not newer than the node's own;
  - a divergent map;
  - a first map that doesn't name the node;
  - a `__phdb_` table name in any schema call.
- **`NotFound`:** `GetMap` on a node with no map.
- **`PermissionDenied`:** the wrong scope (as today).

**The reserved prefix `__phdb_`:**
- **The identity row:** `phdb cluster init-partition FILE --cluster-id C --partition P [--epoch E]` (offline; it honours `DIR/.lock`) creates `__phdb_cluster` with one row: `cluster_id`, `partition_id` and `epoch`. It refuses a file whose row names another cluster or partition.
- **Hidden:** `Schema.ListTables` hides `__phdb_` tables, and every schema call refuses the prefix.
- **Excluded from user feeds:** a user `Feed.Subscribe` of every table excludes them.
- **Kept everywhere else:** backups, bootstrap copies and replication keep them, so a copy still says what it is.

**`phdb cluster` (operator commands):**
- `init-partition`, as above.
- `set-map FILE --node ADDR`: `SetMap` with the node's current version, read first.
- `show-map (--node ADDR | --file PATH)`: reads a live node or a down node's map file.
- `status --node ADDR`: `Status` on every node in the map, with their versions side by side.

## Ticket 2: routing and forwarding
**A cluster node serves data.**
- It opens the one partition its map gives it, after checking the file's identity row: this cluster, this partition, the map's epoch. Several partitions per node come with ticket 8.
- It serves that partition through the same server as a single node. The `Cluster` service is mounted on the same listener, outside the data calls' load shedding, with a small limit of its own.
- A node the map gives no partition only routes. `/v1/health` is up, and `/metrics` has the node's counters without the engine's.

**What routes:**
- **Single-key calls** route by `(table, row)` through the map: `Get`, `ReadRow`, `Mutate`, `CheckAndMutate` and `MutateReturning`, counters included.
- **A `Batch`** routes when all its rows are in one partition. Otherwise it's refused with `CrossPartition` before anything is written, naming each partition and the first row in it.
- **A table the map doesn't place** answers `NotFound`.
- **Refused with `Unsupported`, naming the ticket that adds each:** transactions and snapshots (3, including a `Get` or `ReadRow` with a `snapshot_id`; served since ticket 3, below), fan-out reads (4), schema calls and `/v1/tables` (5), and backups (8). Feeds (7) are served since ticket 7, below. Every other call is refused too, `Info` and `/mcp` included.

**`Cluster.Forward`** (node to node, `cluster` scope, gRPC only) carries a routed call, with the sender's `map_version` and the caller it's for (`ForwardedCaller`: `read` or `write` scope, the token's namespaces). The owner checks those again.

**New server codes** (`code_num` 0; each counted in `pigeonhole_serve_requests_total` under its own `outcome`):

| Code | gRPC | HTTP | Retry |
|---|---|---|---|
| `WrongPartition` | `FAILED_PRECONDITION` | 409 | yes: this node's map no longer gives it the key. The message carries its `map_version`, and the forwarding node pulls the map and forwards once more. |
| `CrossPartition` | `FAILED_PRECONDITION` | 409 | no: split the batch. |
| `PartitionUnavailable` | `UNAVAILABLE` + detail | 503 | yes: nothing was sent to the partition's node (B3). A forwarded write that fails after it was sent answers `OutcomeUnknown`. |

## Ticket 4a: fan-out scans and multi-gets
**`Data.Scan` and `Data.MultiGet` read across a table's partitions** (D317 §7), and answer what one database holding every partition would. They're no longer refused on a cluster node. A read with a `snapshot_id` reads its snapshot's partition only (ticket 3, below), and `Nearest` and `Traverse` are refused until ticket 4b.
- **A scan** reads the partitions its range meets, one at a time, in key order. It passes each partition's pages through: their rows and `bytes`, with `next` rewritten. There's no next-partition prefetch (§7). `limit` counts rows, as on a single node, and what's left of it goes on to the next partition. Each partition's part reads at its own snapshot, as each page does on a single node.
- **A multi-get** sorts and dedupes its keys, splits them by partition, and merges the partitions' pages in key order under the one `page_bytes`. Its pages are exactly a single node's:
  - a partition reads with what's left of the budget;
  - the next one is read ahead with the whole budget, and read again with what's left if its page doesn't fit;
  - so at most about twice `page_bytes` is in flight.

**Positions** (`next`, `after`):
- **The format:** on a cluster they're a cluster position: `0xC1`, the kind (`S` or `K`), the partition's id, then that partition's own position, or nothing for its start.
  - A page cut inside a partition points into it.
  - A page that ends at a partition's end, with more to read, points at the next partition's start.
  - An empty `next` still means only the read's end.
- **Why a new format:** a single node's positions name the table by its id in one file, and a table's partitions are separate files. So a position is only valid on the partition that issued it, and the cluster's names that partition.
- **Kept apart:** a single node's positions start with their version byte, 1, so the two are never taken for each other. A cluster position sent to a single node, or a single node's sent to a cluster read, is `InvalidArgument`, saying which it is.

**`allow_partial`** (new on `ScanRequest` and `MultiGetRequest`): a partition that can't be reached is skipped, not an error.
- **Without it,** the read fails with `PartitionUnavailable`. A scan that already sent pages ends with that error, and its last page's `next` resumes it.
- **With it,** a skipped partition's `[start, end)` is a `MissingRange` in the new `missing` field (`partition`, `table`, `start`, `end`):
  - a scan reports it on the next page it sends, or on a final empty page when nothing follows;
  - a multi-get reports it on its reply.
- **A scan's partition that fails partway** still ends the stream with the error.
- **A single node** ignores `allow_partial`, and its `missing` is always empty.

**`Cluster.ForwardScan`** (node to node, `cluster` scope, gRPC only): one partition's part of a scan, a server stream of that partition's pages.
- **The request** carries the sender's `map_version`, the `ForwardedCaller`, the partition, and the scan, with that partition's own `after` and what's left of `limit`.
- **The owner** serves it only if its map gives it the partition and it has the partition open. Otherwise it answers `WrongPartition`, and the entry node pulls the map and opens the stream once more.
- **Connections:** each forwarded stream has a connection of its own to its peer while it runs, apart from the unary forwards' (simp724's S1 on #1698). HTTP/2 counts a stream's unread data against its connection's window as well, so streams sharing a connection would stall one another once a few of their clients stopped reading. A connection whose stream ended cleanly is kept for the next one (at most 4 idle per peer); ticket 7's feed streams open through the same helper.

**`Cluster.Forward`** gains a `multi_get` arm: one partition's keys of a fanned-out multi-get.

**Budgets:** a fanned-out scan holds one `--max-scans` slot on its entry node for its life, and counts as one `Data.Scan`. Each page it hasn't handed to the client counts against `--inflight-bytes`, as a single node's scan pages do.

## Ticket 4b: fan-out nearest searches and traversals
**`Vectors.Nearest` and `Graph.Traverse` read across a table's partitions** (D317 §7), and answer what one database holding every partition would. A `snapshot_id` on either is `Unsupported` on a cluster for good, since a snapshot is one partition's. The index searches (`Vectors.Search`, `Text.Search`) stay refused until co-located tables (placement groups) say which partition holds an index.

**`Nearest`**, the exact top `k`:
- **Fan-out:** every partition the source meets answers its own exact top `k` under the same `page_bytes`. A `range` source is clipped to each partition's range, and a `keys` source sends each partition its keys. At most `--fanout-forwards` (8, a new flag) of these run at once.
- **The merge** keeps the first `k` by the library's total order (distance, then row key, then qualifier), then cuts the result to `page_bytes`. A distance is never NaN, and every node computes distances with the same code, so ties break the same way everywhere.
- **Exact:** a partition's answer is a prefix of its own ranking, and nothing the global prefix keeps needs more than the budget's bytes of its partition's. `cut_by_budget` is set when the merge stopped for the budget, or came up short of `k` because some partition did.
- **A forwarded `Nearest`** carries the partition's keys, or its range clipped to bounds, and its owner refuses one that reaches outside its partition.

**`Traverse`** runs every hop at the router, by the library's rules exactly (`pigeonhole::Traverse`):
- **A hop's edges** come from one fanned-out multi-get of the frontier: the edge family, the edge selection, and `column_limit` = `per_node_limit`. The traversal's limit counts the edges the selection keeps, as `column_limit` counts selected columns.
- **Ranking:** the edges arrive in (source, qualifier) order. A destination not visited before is new once, ranked by its first edge.
- **Limits:** `hop_limit`, `max_nodes` and the byte budget cut that ranking. The reading stops where a single node's would, so `stats` (`rows`, `edges`, `hops_cut`, `cut_by_max_nodes`, `cut_by_budget`) match too. A hop's nodes are sorted, and `parents` index the previous hop's.
- **One deadline** (from the request's arrival) covers the whole traversal. The visited set counts against `--inflight-bytes` (its key and 48 bytes a node).
- **Snapshots:** each hop reads at its own time, and each partition at its own snapshot, unlike a single node, whose one snapshot covers every hop.
- **Cost:** one fan-out round per hop, so a deep traversal of a graph split across nodes costs hops × the slowest partition's round trip.
- **`hop_limit` saves less here:** the router reads a hop's edges until its limit or budget stops it. A single node stops at the same point, but a hub's edges past it still cross the network in the page that held them. A dedicated expand call that returns only new destinations is a perf follow-up.

**`allow_partial`** (new on `NearestRequest` and `TraverseRequest`; `missing` on both replies):
- **`Nearest`** skips a partition that can't be reached, and its result is the exact top `k` of the partitions reached.
- **A traversal's hop** skips the frontier nodes of an unreachable partition. Their edges aren't followed, so later hops and the ranking are exact only over what was reached.
- **Without it,** either read fails with `PartitionUnavailable`.

**`Cluster.Forward`** gains a `nearest` arm (field 18, reply 11), after D317 ticket 3's arms. `MissingRange` moves to `common.proto`: the same package, so the wire format and the generated names are unchanged.

## Ticket 7: cluster feeds
**`Feed.Subscribe` on a cluster node** follows every partition the subscription covers, through any node. Covered means the partitions of its tables' placements, narrowed by its prefix to the ranges that can hold such rows. Each partition is followed by a single-node subscription on the node serving it, and the entry node merges them:
- **Order:** a partition's commits keep its order, so each row's do. There's no order across partitions.
- **`FeedEvent.partition`** (field 5) names the partition each event came from. `FeedCommit.seqno` is that partition's own seqno: it's comparable only with seqnos of the same partition.
- **Tables the map doesn't place** answer `NotFound`.
- **A start from a snapshot** is `Unsupported` (the coordinator's call; a one-partition version is #1701, Post-1.0).
- **A cluster node takes `--change-feed`** for its partition, as a single node does.

**Tokens are checkpoints** (D317 §8 as amended; option (C), the coordinator's call after perf287's numbers):
- **What a checkpoint is:** a cluster token holds every partition's position. That's its own D310 token, compacted losslessly: varints, deltas between a stream's positions, the writer session as its 8 bytes, and the original bytes whenever the compaction doesn't expand back exactly. The token also holds each partition's epoch, the map version, the cluster id and a hash of the subscription (tables, prefix, visibility).
- **Which events carry one:** a token on every event would be several times the event, so only checkpoints do:
  - the first commit;
  - then every N commits after a checkpoint, with N = ceil(the last checkpoint's size / 150 bytes), from 1 to 64, and always 1 for a one-partition subscription;
  - a progress event once the quiet interval passes with the position moved;
  - a progress event before the stream ends with an error.
- **`SubscribeRequest.checkpoint_every`** (field 9, 1–64) fixes N instead. 1 gives today's token on every event.
- **Measured cost:** a checkpoint at 16 partitions × 16 shards is about 2.4 KB, every 16 events. That's about 150 bytes of token per event on average at every shape up to 64 partitions × 16 shards, where the cap of 64 takes over.
- **Resume:** from the last checkpoint is gap-free. It may deliver again the commits after that checkpoint, at most N in all for the stream. A client drops them with `seqno <= last_seen[partition]`, or sends them back:
  - **`SubscribeRequest.skip_through`** (field 8, `repeated PartitionSeqno { partition, seqno }`): with a resume token only. Each partition's part passes over its commits after the token up to that seqno, so nothing repeats.
  - More than 64 would-be-sent commits passed over in one partition is `InvalidArgument`: a client can't have handled more after a checkpoint.
  - A partition the subscription doesn't cover is `InvalidArgument`.
  - A seqno at or below the token's position changes nothing.
- **A resume token** of another cluster or another subscription is `InvalidArgument`. So is a single node's token.
- **Expiry:**
  - one from a map that changed the subscription's partitions is `FeedExpired`. Only a table new to the subscription can do that; a subscription with an explicit table list never sees it;
  - so is one from before a partition's epoch rose. A relocation by promotion gives the partition another history; ticket 6 will let a relocation by file copy keep it.

**`allow_partial`** (`SubscribeRequest` field 7):
- **Without it,** a partition that can't be opened fails the call, and one that goes away ends the stream with `PartitionUnavailable` naming it, after a last checkpoint.
- **With it,** the stream carries on without the partition:
  - A **`PartitionState { partition, available, reason, message }`** event (`FeedEvent` oneof field 6) reports it. It's sent at the start for each partition unavailable then, and on each change. `reason` is the stable code (`PartitionUnavailable`, ...).
  - The partition is opened again every second from its own position, so its commits resume in order with none skipped.
  - A partition unavailable since a subscription from now began starts at its own "now" when it comes back. A subscription from the oldest commit starts there.
  - An expired or refused position still ends the stream.
- **A partition whose node answers `WrongPartition`** (the map moved it) is opened again where the entry node's newer map puts it.
- **A single node** refuses `allow_partial`, `skip_through` and `checkpoint_every` with `InvalidArgument`.

**`Cluster.ForwardSubscribe`** (node to node, `cluster` scope, gRPC only): `ForwardSubscribeRequest { map_version, caller, partition, request, skip_through }`, answered with a single node's events. Every event carries a token, and the first is a progress event with the start's position.
- The owner serves it only if its map gives it the partition, every table of the request is placed on it, and it has it open. Otherwise it answers `WrongPartition`, and it never forwards it on.
- Each part holds a `--max-streams` place on its partition's node. The subscription holds one more on the node it entered. The `Cluster` calls' small concurrency limit doesn't apply to it.
- **Connections (simp724's S1):** a subscription's parts on one peer share a connection of the subscription's own: a lease from the stream connections (ticket 4a's `stream_channel`), never given back, so it closes with the subscription. A subscriber that stops reading holds only its own connections' HTTP/2 windows, never a forwarded call's or another stream's. One stalled stream can't fill a connection (hyper's 2 MiB stream window against 5 MiB a connection), but three or four on one connection did in a test.

**Memory:** a subscription buffers up to the feed's client limits (64 events, 256 KiB) at its entry node, plus as much on each part's node. That's `(partitions + 1) × 256 KiB` at most, outside `--inflight-bytes`, as a single node's feed buffers are.

## Ticket 3: transactions and snapshots in one partition
**A transaction or pinned snapshot lives on the node serving its partition.** Any node routes the calls on it there, and the entry node holds no session state.
- **Where it begins:** `BeginRequest.route` and `OpenSnapshotRequest.route` (a row or a partition id) are required on every cluster, a one-partition map included, so a client keeps working when the map gains a partition. A missing `route` is `InvalidArgument`; a table or partition the map doesn't have is `NotFound`.
- **Ids name their partition** on a cluster: `<partition>:<32 hex>`. They're split at the last `:` (partition ids are `[a-z0-9_-]`). A single node's ids stay 32 hex. An id of the wrong form, or of a partition the map doesn't list, is `NotFound`, on a single node and on a cluster alike.
- **Routed by the id:** `TxnGet`, `TxnGetRow`, `TxnWrite`, `Commit`, `Abort` and `Snapshots.Release`. Also routed by the id are `Get` and `ReadRow` at a snapshot, and a `MultiGet` or `Scan` at one whose keys or range lie in its partition.
  - **A `MultiGet` at a snapshot** is routed and forwarded as a single-key call is (`Forward`'s `multi_get` arm). It pages as its partition's node does, with that node's own positions.
  - **A `Scan` at a snapshot** runs through the fan-out with the snapshot's partition alone, with cluster positions: it's served here with the snapshot pinned for the stream, or on its node through `ForwardScan`, whose owner checks the range again.
  - `Traverse` and `Nearest` at a snapshot are `Unsupported`.
- **`CrossPartition`:** any row outside the session's partition is refused before anything is buffered or written, at the entry node and again at the owner. That covers a transaction's read or write, a `Commit`'s rows, a read at the snapshot, and a scan range that reaches another partition. The message names both partitions.
- **`Cluster.Forward`'s new arms:** `begin`, `txn_get`, `txn_get_row`, `txn_write`, `commit`, `abort`, `open_snapshot` and `release_snapshot`, with the replies `begin`, `commit`, `open_snapshot` and `empty` (the reads answer `get` and `read_row`). For B3, a `Commit` or `Abort` that fails after it was sent answers `OutcomeUnknown`, as a write does. Sending `Commit` again, through any node, answers the first outcome (B1). A `Begin` or `Snapshots.Open` cut after it was sent is retry-safe, but it can leave a session on the owner until its idle lease ends, counted against the per-token cap there.
- **Sessions are bound to the token's SHA-256 digest** (its principal), on a single node too, so a reordered or reloaded token file keeps them.
  - `ForwardedCaller.token_digest` carries the digest; it's empty on a cluster without tokens. The owner resolves it against its own token file. Its own entry decides the scope (capped at `write`, as in ticket 2) and the namespaces. A digest it doesn't list, or any digest sent to a node without tokens, is `PermissionDenied`, so a cluster's nodes list the same tokens.
  - Digests never appear in logs or errors.
  - `--max-sessions` and `--max-sessions-per-token` count per node.

## Callers changed
None yet: ticket 1 adds a mode, a service and a scope, and changes nothing a single-node server answers, except that `__phdb_` table names are refused and hidden.

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
**`Vectors.Nearest` and `Graph.Traverse` read across a table's partitions** (D317 §7), and answer what one database holding every partition would. A `snapshot_id` on either is `Unsupported` on a cluster for good, since a snapshot is one partition's. The index searches (`Vectors.Search`, `Text.Search`) stay refused on a cluster: no D317 ticket routes them (the coordinator, 2026-11-11), and the refusal names no ticket. It points at `Nearest`, which fans out. Index search across partitions is #1722 (1.3.0).

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

**Charging the merge pages (#1715, 1.2.0; plan reviewed by cli692).** A fanned-out read's partition pages count against `--inflight-bytes` on its entry node, as a single node's reply does.
- **The holding:** each read reserves its share before its first partition read. No partition read then waits for budget while the request holds some (cli692's P1):
  - a multi-get: its `page_bytes`;
  - a nearest search: twice that, the result's bound plus one answer;
  - a traversal: one hop page plus the seeds' visited bytes.
- **Concurrency on spare budget:** a multi-get's read-ahead, and a nearest search's further concurrent forwards, take spare budget with `try_reserve` or don't run.
- **The reply** takes over the holding as its charge (`fit` to its encoded size, twice for HTTP/JSON), so there's no gap and no second reservation.
- **Parts are charged once, at the entry** (perf287's g78 on #1730: with every node an entry, owners' admission waited on holdings that waited on them, and nothing answered).
  - Local parts, and parts a peer forwards with `ForwardRequest.fanout_part` (field 21), are read through `Service::fanout_part`. That's the same reads, scope check included, with no admission and no reply charge, so an owner never waits on budget for one.
  - Only peers send it: `Cluster.Forward` takes cluster scope, and a client's token is refused.
  - An owner's memory for others' parts is bounded by the `Cluster` calls' concurrency limit (16) × a page (at most 8 MiB), not by its `--inflight-bytes`.
- **A traversal reads each hop in pages of its own `page_bytes`,** not 8 MiB. The router keeps `per_node_limit` per source row across pages, since a resumed row's `column_limit` starts over.
  - **The visited set's growth** takes spare budget first. Otherwise the hop holding (with the seeds' share in it) goes back while it waits. Retaking it still waits while the visited set is held. That set is the traversal's state, and has no bound worth reserving up front.
  - **When the visited set leaves less of the budget than a hop page,** the next hop can't be read charged, so the traversal ends after the hop it has. It's marked `cut_by_budget` if a hop was left to read (cli692's N2 on #1730). A smaller page isn't read instead, since it might not hold a single edge.
- **A forward's connect and call share one deadline** (cli692's F1 on #1730), the call's, at the sending node. A node that takes the connection and never answers can no longer hold a read, and its pages' budget, past its deadline. A read answers `PartitionUnavailable`; a write's outcome is unknown (B3).

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

## Ticket 5: schema in the map
**A cluster's tables and families live in the partition map.** `Schema.CreateTable`, `DropTable`, `ListTables` and `DescribeTable` (and `GET /v1/tables`) work on any cluster node, and each node reconciles its partitions' files to the map.
- **`CreateTable`** is a map change. `CreateTableRequest.placement` (field 4, a new `TablePlacementRequest { group, splits, partitions }`) gives the table's ranges as `set-map` would. A cluster refuses a create without one, unless the map already places the table (an operator can place a table with `set-map` first). A single node refuses a request that has one (`InvalidArgument`).
  - **`if_missing`** adds the families an existing table lacks. A family that exists keeps the map's options even when the request gives others: it's neither refused nor changed, as a single node's `if_missing` keeps the file's (simp724's N3 on #1720; the plan had said refused).
  - The map's validation applies as it does to `SetMap`. `validate_change` also refuses a family removed or changed, and a changed `created`.
- **`DropTable`** removes the table's placement from the map. Its partitions stay.
- **`ListTables`, `DescribeTable` and `GET /v1/tables`** answer from the map, with no fan-out. Tables the map doesn't place, a partition's own `__phdb_` tables included, aren't listed.
- **The scope** is a single node's: `write` for a create (`if_missing` included), `admin` for a drop, `read` for list and describe. It's checked at the entry node, and again at the writer (amendment 1).
- **The reply** comes once the map is set, persisted and pushed. `TableResponse.unreconciled` (field 3) lists the partitions whose node the push missed. A drop answers `Empty`, as on a single node. `Cluster.Status` shows each partition's reconcile: `LocalPartition.reconciled_version`, `reconcile_errors`, `unmapped_tables` and `adopted_tables` (fields 6–9).

**`TablePlacement.created`** (field 6) is the map version that created the table. It tells a table from one dropped and created again under its name. It's fixed for the table's life; 0 for a placement made by hand with `set-map`.

**Reconcile** runs on every map a node takes, before the map's routes serve, and at start. Each partition records each table the map made in its `__phdb_cluster` table (row `schema/<table>`, family `schema`, qualifier `created`), so reconcile needs only the map and the file (simp724's S1):
- **Placed here with `created` c:**
  - The file has no such table: it's created with the map's families, and c recorded.
  - The table is recorded with c: any missing families are added. An option mismatch is reported in `Status`, not changed.
  - The table is recorded with another c′: the map dropped it and created it again, so the old table is dropped and the new one created.
  - The table isn't recorded: it was made by hand before the map managed it. It's adopted, with missing families added, and c recorded.
- **Recorded, and not placed here:** the map dropped it. The table is dropped, then its record.
- **Neither recorded nor placed:** a hand-made table, left alone and listed in `Status` (`unmapped_tables`).
- **A crash** between a table and its record redoes the step on the next reconcile. `__phdb_` tables are never touched.
- **The window:** the map is set, pushed, and each node reconciles before its routes serve. A call routed by the new map finds its table, or answers `TableNotFound` while a node that hasn't taken the map yet catches up. That `TableNotFound` is safe to retry: nothing was applied.

**D317 amendment 1: one map writer.** Two schema changes through two entry nodes diverged in the model test: each compare-and-set ran against its own node's map, both made the same next version, and one table was lost. The coordinator approved the fix with simp724's conditions:
- **The writer** is the node with the smallest id in the current map. It alone changes the map.
- **Schema changes go to it.** Another node forwards `CreateTable` and `DropTable` to it on `Cluster.Forward` (request arms `create_table` 19 and `drop_table` 20; reply `table` 12, and `empty` for a drop). A writer whose map names another writer answers `WrongPartition`. The entry node pulls the newer map and sends once more. A writer never forwards a change on. At the writer, changes queue one at a time, so a compare-and-set conflicts only with an operator's `set-map` at the same moment, and is retried against the re-read map (S2).
- **`SetMap`** is refused at a node that isn't the writer, with `WrongPartition` naming the writer and its address. `phdb cluster set-map` reads the map from `--node` and sends the map to its writer.
- **Scope at the writer:** a forward's `ForwardedCaller` scope stays `read` or `write`. The writer resolves `token_digest` against its own token file, uncapped (`schema_caller_of`), and checks a drop's `admin` itself. A `cluster` token, or a digest it doesn't list, is refused. Without tokens anywhere, the caller is anyone, as on a single node.
- **B3:** a forwarded create or drop whose answer is lost after it was sent answers `OutcomeUnknown`. A create with `if_missing` is safe to send again, and a drop that went through answers `TableNotFound` the second time.
- **Divergence:** a same-version map with other contents is refused and reported, as in ticket 1.
- **Recovery when the writer is down:** `phdb cluster set-map --force` (`SetMapRequest.force`, field 3) at any node.
  - The CLI reads `Status` from every node it can reach and puts the map's version above all of them.
  - The map must name a writer that answered: remove the down node, or let another id be the smallest.
  - The node takes it without the writer check or the compare-and-set: it need only be the cluster's, at a version above the node's own.
  - A node that starts with a map changes none as the writer until it has taken its peers' newer maps. So a writer back after a forced recovery takes the forced map first, which no longer names it, and follows it.
  - Force is for a writer that's down. A writer that's up but cut off can still make a change, which the forced map then loses.

**Tests:**
- **Through the nodes:** a create, an added family, a drop and a create again under the same name, each through a different node. A hand-made table survives, and one the map takes over is reported as adopted.
- **Concurrency:** twelve creates at once through three nodes all land, and every node ends with the same map and no divergence. With every node writing, as before amendment 1, the test fails.
- **Recovery:** the writer stops; a forced map that still names it is refused; one without it is set above every version, and schema calls work through the new writer. The old writer restarts with its old map, takes the forced one, refuses a `set-map` naming the new writer, and forwards a schema call to it.
- **Reconcile unit tests:** a crash between a map and its reconcile; a drop and re-create while a node was away. Mutation-checked: dropping unmapped tables, or ignoring `created`, fails them.

## Ticket 8b: cluster backup, and metrics by partition
**`Admin.Backup` and `Admin.BackupDownload` on a cluster node** (D317 §9) back up one partition the node holds. They're never forwarded: a backup's bytes don't take a hop.
- **`BackupRequest.partition`** (field 3) and **`BackupDownloadRequest.partition`** (field 5) name the partition. A cluster node requires one, even when it holds just one partition (`InvalidArgument` without it). A single node refuses one (`InvalidArgument`).
- **Refusals:** a partition the map gives another node is `WrongPartition { map_version }`. One the map gives this node but that isn't open here is ticket 2's `PartitionUnavailable`. One the map doesn't have is `NotFound`.
- **What the last chunk says:** `BackupChunk.partition` (7), `cluster_id` (8) and `epoch` (9) come from the identity row (`__phdb_cluster`) of the same snapshot the copy is of. `map_version` (10) is the node's map version when the backup started. Earlier chunks leave them empty. A resume names the partition its copy is of; another is `InvalidArgument`.
- **A cluster node takes `--backup-dir` and `--max-backups`** for its partition's backups, as a single node does.

**`phdb cluster backup DIR --node URL [--durable] [--parallel N]`:**
1. It reads the map from `--node` (V).
2. It downloads every partition's copy from the node V gives it, at most N at once (default 4), resuming a broken stream, and checks each copy's SHA-256.
3. Each copy's identity must be the map's: cluster, partition, and the partition's epoch at V. Otherwise the backup fails, naming both.
4. It reads the map again (V′). V′ must have the same partitions, owners and epochs as V, and place every table both hold the same way. Otherwise it fails: "the map changed during the backup". Tables created or dropped in between are allowed.
5. It writes `DIR/map.json` (V′, so a restore reconciles the copies against the map they were taken under: simp724's A2), and `DIR/manifest.json`:
   - `cluster_id`, `map_version` (V′), `map_version_at_start` (V), `durable`, and `consistency`;
   - and per partition: `file`, `node`, `epoch`, `bytes`, `sha256` and the node's map version.

`DIR` must be new or empty. Copies are written as `<partition>.phdb.partial` and renamed once checked, and the manifest is written last, so a backup without one isn't whole. **Each partition's copy is a consistent snapshot of its own. The set is not one point in time across partitions.**

**Metrics by partition:** on a cluster node, `/metrics` and `Admin.Metrics` label every per-partition series `pigeonhole_partition="<id>"`. That covers the engine's metrics (D323, through `pigeonhole-otel`'s `Config::attribute`, ICR 0133) and the serve feed fan-out's and bulk loads' gauges. Node-wide series (connections, replies, sessions) stay unlabelled. Each family's `# HELP`/`# TYPE` comes once, whatever the number of partitions. A single node's text is unchanged. On a cluster node, `Admin.Metrics` is answered by the node itself; it was `Unsupported` before.

## Partition upload (for restore, 8b PR 2, and relocation by file copy, ticket 6 PR 2)
**`Admin.PartitionUpload`** (client streaming, gRPC only, admin scope, a cluster node only) streams one file of a partition's set into a node. **`Admin.PartitionUploadStatus { upload_id }`** says how many bytes of an upload the node holds, so a client can resume. Both answer `Unsupported` on a single node.
- **A partition is a set of files,** named by their suffix of the partition's database path: `""` for the main file, `-wal-<n>` for a WAL sidecar, `-feed` for the change feed's sidecar. Each node maps a name onto its own path. **`PartitionFile { name = 1; bytes = 2; sha256 = 3 }`** (common.proto) describes one.
- **`PartitionUploadHeader`:**
  - `partition` (1), and `cluster_id` (2) and `epoch` (3): what the main file says it is, so the epoch the copy is *of*;
  - `upload_id` (4): 32 lower-case hex, chosen by the client;
  - `purpose` (5): `RESTORE` or `RELOCATE`;
  - `file` (6): the file this upload carries;
  - `manifest` (7): every file of the set, `file` among them, with the main file required.
- **`PartitionUploadChunk { header = 1; offset = 2; data = 3 }`:** the first message carries the header. Each message's data starts where the node's bytes end, and is at most the node's `max_chunk`. That's 512 KiB, or less on a node whose `--max-request-bytes` is lower: min(512 KiB, the limit − 16 KiB), at least 1 KiB (cli692's F3 on #1728). A message that doesn't come within `--stream-idle-ms` ends the upload with `DeadlineExceeded`, which frees its place; it then resumes (cli692's F1).
- **Replies:** `PartitionUploadReply { received = 1; complete = 2 }`, and `PartitionUploadStatusReply { received = 1; complete = 2; max_chunk = 3 }`. A client asks the status first, for `max_chunk`.
- **A full disk or quota** answers `NoSpace` (`ResourceExhausted`, 507): free space, then resume from the status. Nothing written is lost (cli692's F2).
- **Accepted only while the partition isn't open on the node,** with one upload of a given file of it at a time, and at most `--max-uploads` (default 1) at once; more are `Overloaded`. A resume must send the same header (`InvalidArgument` otherwise).
- **Staged, never served:**
  - Bytes go to `<cluster DIR>/.phdb-incoming/<upload_id>.part` (the directory mode 0700, never a link).
  - When every byte has arrived and the SHA-256 matches, the file is synced and renamed `<partition>.phdb<name>.ready`. Then its header is written beside it (`….ready.header`). The header is the commit marker, written last after any older one is removed, so a crash between the steps leaves a ready file with no header, which the install never takes (feed1248's F1 on #1728).
  - A file whose bytes don't match is dropped (`InvalidArgument`): upload it again.
  - Incomplete uploads untouched for 5 minutes are deleted, and so is every incomplete one when the node starts. Ready files are kept.
- **Installed by ticket 6's stamp,** not by the upload. Relocate's first step on the new node (copy, promotion and restore alike) installs the set once every manifest file is ready with one header. It checks the header against the stamp's partition and epoch (a restore may name an older epoch, and with `restore_from_cluster_id`, another cluster), replaces the partition's whole file set, then raises the epoch. The map change opens the partition. This upload section ships the staging; the install comes with ticket 6 PR 1.

## Callers changed
None yet: ticket 1 adds a mode, a service and a scope, and changes nothing a single-node server answers, except that `__phdb_` table names are refused and hidden.

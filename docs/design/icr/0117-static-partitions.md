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
- **A node change (1.3.0, #1899, D317 amendment 3):** a non-forced map whose node ids differ from the current map's gets `previous_nodes` (`ClusterMap` field 6, repeated `Node`) set by the writer to the current map's nodes. Its `SetMap` pushes to the nodes before and after it until a majority of each holds it, for up to three push timeouts, and answers with the last pushes (nodes it removed included). While the newest map has `previous_nodes`, every non-forced `SetMap` (schema changes included) first pushes the map to both sets again, and is refused with `FailedPrecondition`, naming both counts and the nodes missing, unless a majority of each holds it. The change it takes clears `previous_nodes`, unless it changes the nodes itself. A forced map clears it. The writer ignores whatever `previous_nodes` a map sent to it says.

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
  - **Amendment (#1786, 1.3.0, the owner's call):** it's the node's total, split across its partitions by their shards (`total × shards ÷ the node's shards`), as `--engine-memory` is; 1.2 gave each partition all of it. The engine still keeps at least two WAL segments a stream. A partition opened while serving takes its share for the new set, and keeps at least the retention its feed's last session had (a moved partition's history, simp724's R1) until a restart re-splits it; the open and the start log a node over its total and a partition whose history shrank. Serve-only hooks, not stable API: `Options::change_feed_keep_last` and `Pigeonhole::feed_previous_retention` (the engine's `feed_keep_last` and `FeedStorage::previous_retention_bytes`).

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
  - so is one from before a partition's history began (`Partition.history_epoch`, ticket 6): a promotion or a restore gives the partition another history. A relocation by copy keeps it, and the position continues on the new node.

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
4. It reads the map again (V′). V′ must have the same partitions, owners and epochs as V, and place every table both hold the same way, with the same `created` (a table dropped and created again under its name fails it: simp724's N2 on #1711). Otherwise it fails: "the map changed during the backup". Tables created or dropped in between are allowed.
5. It writes `DIR/map.json` (V′, so a restore reconciles the copies against the map they were taken under: simp724's A2), and `DIR/manifest.json`:
   - `cluster_id`, `map_version` (V′), `map_version_at_start` (V), `durable`, and `consistency`;
   - and per partition: `file`, `node`, `epoch`, `bytes`, `sha256` and the node's map version.

`DIR` must be new or empty. Copies are written as `<partition>.phdb.partial` and renamed once checked, and the manifest is written last, so a backup without one isn't whole. **Each partition's copy is a consistent snapshot of its own. The set is not one point in time across partitions.**

**Metrics by partition:** on a cluster node, `/metrics` and `Admin.Metrics` label every per-partition series `pigeonhole_partition="<id>"`. That covers the engine's metrics (D323, through `pigeonhole-otel`'s `Config::attribute`, ICR 0133) and the serve feed fan-out's and bulk loads' gauges. Node-wide series (connections, replies, sessions) stay unlabelled. Each family's `# HELP`/`# TYPE` comes once, whatever the number of partitions. A single node's text is unchanged. On a cluster node, `Admin.Metrics` is answered by the node itself; it was `Unsupported` before.

**`phdb cluster restore DIR --node URL [--map FILE] [--cluster-id ID] [--old-unreachable] [--parallel N]`** (8b PR 2) puts a complete backup back:
1. **Checks, before any change:** each copy's size and SHA-256 against `manifest.json`; the map (`DIR/map.json`, or `--map`) must have the backup's partitions, and a `--map` exactly the backup map's tables. A node's live map must be of the restored cluster. On every node either map names: no restored partition `Serving` or `Opening`; on the node it's restored to, not open at all (close it); that node must answer, and so must the live map's owner, unless `--old-unreachable`.
2. **Uploads:** each copy to its node, `Admin.PartitionUpload` with `purpose = RESTORE`, the header's cluster and epoch the backup's, the manifest the main file alone. The upload id is derived from the cluster, partition, node and the copy's SHA-256, so a run again resumes it.
3. **Stamps:** `Cluster.Stamp { restore: true }` from `e` to `e + 1`, where `e` is the highest of the backup's, the backup map's and the live map's epoch for the partition, and `restore_from_cluster_id` the backup's cluster under `--cluster-id`. The install moves the partition's other files (its WAL and feed files) aside to `.replaced-<nanos>`, so a restored partition's history starts at the restore, and a feed token from before it answers `FeedExpired` (the epoch changed).
4. **The map, last:** the backup's map with those epochs, set by amendment 1's forced path (`set-map --force`): its version above every reachable node's, no `validate_change` against the live map, its writer reachable. Every table's placement and `created` stay exactly as the backup has them (ai1249's (a)): another `created` would make each node drop the table and create it again, empty. It is always the backup's map, never merged with the live one: a table created since the backup is dropped, one dropped since comes back.
5. **Served:** each partition opens on its node with the map, under the operator's persisted fence from before the restore. The restore waits (up to 60 s) for it to open, then `Cluster.Unfence`s it there.
6. **A run again** (simp724's S1): `DIR/restore-progress.json` records, under the live map's version, each partition stamped (node, new epoch) and the map's version once set; it's removed when the restore finishes. A partition recorded and whose node's `Status` shows its file at that epoch is neither uploaded nor stamped again. A recorded map that the live map still matches (same or newer version, every partition on its recorded node at its recorded epoch) leaves only step 5. A `DIR` that takes no writes (a read-only snapshot) doesn't stop the restore: the first failed write is a warning (stderr and the report's `warning`), and nothing more is recorded, so a run again after a failure redoes it (simp724's S3). The report's `close_and_drop_on` lists each node the restored map moves a partition off (simp724's S2).
7. **Amendment (#1832, 1.3.0): the epoch passes every copy a node reports.** `e` in step 3 is also at least every epoch a node's `Status` gives the partition's file, except a file this restore's own earlier run stamped (recorded in its progress), which it reuses. A move or relocation that failed after its stamp can leave a file past the map's epoch (simp724's N1 on #1832).
8. **Amendment (#1832, 1.3.0): `--partition P [--to ID]`**, one partition into the live cluster:
   - **Scope:** only P's placement changes, to `--to` (default: the live map's node) at a new epoch `e + 1`, `e` the highest of the copy's, the live map's and every node's `Status` epoch for P. The other partitions and every table stay the live map's. P must be in the backup and the live map, and the live cluster must be the backup's; `--map`, `--cluster-id` and `--parallel` are refused with it.
   - **Schema (Q1 of the design):** the live map's, reconciled onto the copy when P opens (ticket 5). A table both maps have with the same `created` must give P the same key ranges, bounded by the splits, or the restore is refused: no reconcile moves rows between partitions. Tables created or dropped since are the reconcile's (created empty, dropped), and the report names them.
   - **Steps:** the checks of step 1 for P (`Serving` or `Opening` nowhere, not open where it goes, the live map's owner reachable unless `--old-unreachable`), the upload of step 2, then relocate's step with the stamp's `restore` set: `Cluster.Stamp { restore: true }` from `e` to `e + 1` and the checked, non-forced `SetMap` relocate sends (P on its node at `e + 1`, a new history), then the wait for a majority of the nodes to hold the map (#1760), then step 5's unfence.
   - **A run again:** `DIR/restore-progress-<P>.json` records the stamp's node and epoch before it, and the map's version once set. A run again computes `e` again (its own recorded file excluded from the floor), reuses the stamp only if its epoch is still `e + 1`, and with the map set and still placing P so, only waits and unfences.

## Ticket 8a: several partitions per node
**A cluster node serves every partition the map gives it** (D317 §10). Before, a map that gave a node more than one was refused at its start.
- **One service per partition, sharing the node's.** Each partition has its own database, tables, indexes, feeds and session table (ids `<partition>:<hex>`, ticket 3).
  - **Node-wide:** the budgets and pools (`--inflight-bytes`, `--max-concurrent`, `--max-scans`, `--max-streams`, the blocking pool), connections, metrics, limits, `--max-sessions` and `--max-sessions-per-token`, and `--backup-dir`'s one `Backups`.
  - A call is admitted once, by whichever service takes it.
  - Bulk loads, text rebuilds and the SIGUSR1 dump cover every partition; shutdown closes each.
- **Schema (ticket 5):** every map a node takes reconciles each of its partitions, each through its own service and under its own schema lock, and `Cluster.Status` reports each partition's reconcile (simp724's N1 on #1720).
- **Calls** route to the partition's own service here, or to its node, as before. Two partitions on one node are still two partitions: a batch across them is `CrossPartition`, and a session id names its own.
- **Opening:** the partitions open in id order before the network side starts, each checked against the map as in ticket 2. One that fails to open (a missing file, a wrong identity) fails the node's start, naming it, after closing those already open.

**The node's budget (§10, perf287's P1):**
- **`--shards` and the new `--compaction-threads`** are the node's totals, split across its partitions in id order: equal shares, the remainder one each to the first, at least one shard each.
  - **Shards:** the share is passed as the database's default shard count (ICR 0132), so a partition whose change feed recorded a count keeps it rather than being resharded.
  - **Compaction threads:** `--compaction-threads` defaults to the library's per-database default (a quarter of the physical cores, at least 1, at most one per shard), computed once for the node. Pinned compaction threads never exceed it. With fewer than one per partition, the first get one pinned thread each, and the rest one each that is confined to the node's compaction CPUs (`Options::compaction_cpu_set`, ICR 0134) rather than pinned (perf287's A1).
  - A single node takes `--compaction-threads` too, as its database's `compaction_cores`.
- **Shards first (perf287's g75):** with several partitions, the node reserves its compaction CPUs before any engine opens: the `--compaction-threads` CPUs that come after all the partitions' shards in the engine's pin order. Every partition's compaction threads are confined to them (`compaction_cpu_set`). So each engine's pin plan holds only its shards, and the shards take the first CPUs together, as one engine's would. Opened one after another without this, each engine pinned its shards and then its compaction threads, interleaving them. The network mask was then split across the AX102's two L3 domains, and its threads ran at 0.70 IPC against 0.91 at one partition (25% less Get throughput). A partition whose feed keeps more shards than its share pins the extra ones onto the reserved CPUs.
- **The pinning check:** after each open, the node prints the partition's shard and compaction threads, pinned, confined and unpinned. On Linux, an engine with unpinned threads is a warning naming the reason. `/metrics` has `pigeonhole_serve_partition_unpinned_shard_threads` and `pigeonhole_serve_partition_unpinned_compaction_threads`, labelled `pigeonhole_partition`.
- **The network mask** (D313's R1, #1697) leaves out the shards' CPUs of every partition on the node: the union of their shard threads' CPUs and SMT siblings. The engines' compaction CPUs stay in the mask. g72 (#1703 comment 6787495064) measured leaving them out too: on one node it cost the network side about 3x its throughput, and #1703 stays open for 1.3.0 (the coordinator's call). The startup line counts the shard CPUs left out and the compaction CPUs not (the reserved ones, where compaction is confined). Ticket 6 reapplies the masks when a partition opens or closes on a running node.

**Engine memory (§10; cli692's M3, perf287's A3):**
- **`--engine-memory BYTES`** covers the memtables, block cache and row cache. Unset, every database takes the library's defaults (64 MiB of memtable per shard, a 256 MiB block cache, a 64 MiB row cache), as before.
- **How a share is spent:** the memtables first, at most 64 MiB and at least 128 KiB per shard; the rest to the block cache and row cache at the library's 4:1.
- **On a cluster node it's the node's total.**
  - The map's new **`Partition.memory_bytes`** (field 4; 0: none) gives a partition its own amount, taken off the top.
  - The rest is split equally across the node's other partitions.
  - A share that can't cover its shards' memtables at 128 KiB each, or overrides above the total, fail the node's start, naming the partition and the least it needs. That's checked against the flag, not the host's RAM.
- **Reported:** each partition's resolved numbers are on its startup line, and in `/metrics` as `pigeonhole_serve_partition_memtable_bytes`, `pigeonhole_serve_partition_block_cache_bytes` and `pigeonhole_serve_partition_row_cache_bytes`, labelled `pigeonhole_partition`.

**Not changed:** a single node answers as before, with the same mask. Its startup line adds the shard and compaction CPU counts, and it takes the two new flags.

## Ticket 6 PR 1: fencing, close, stamp and relocation
**A node writes P only while its newest map names it for P at P's file's epoch, and P isn't fenced** (D317, amendment 2).

**Proto** (numbers from the coordinator, `.coord/numbers.md`):
- **`Cluster` RPCs:**
  - `Fence`, `Unfence`, `Close` and `DropPartition` take `PartitionRequest { partition }` and answer the partition's `LocalPartition`. They're admin scope.
  - `Stamp(StampRequest)` answers `LocalPartition`, with `files` set to the set it installed. It's cluster or admin scope.
- **`LocalPartition`** gains three fields:
  - `state = 10`: a `LocalPartitionState`, one of `SERVING`, `FENCED`, `CLOSED` or `OPENING`;
  - `files = 11`: a closed partition's files once hashed, as `PartitionFile`;
  - `fence_reason = 12`: a `FenceReason`, `OPERATOR` or `AWAITING_PEER_MAP`, set only when `FENCED`.
  - `awaiting_peers = 13` (bool, 1.3.0, #1829): the peers have yet to confirm it since it opened here, whatever `fence_reason` says. With D326 (1.3.0): it holds no live lease. The gate keeps the operator's fence and the wait for the peers as two flags; `fence_reason` reports the operator's first, so an operator-fenced partition no peer has confirmed reads `OPERATOR` with `awaiting_peers` set.
- **`StampRequest`** has six fields: `cluster_id`, `partition`, `from_epoch`, `to_epoch`, `restore` and `restore_from_cluster_id`.
- **`PartitionRequest.force = 2`** (bool), read only by `Unfence` (cli692's S1): it lifts a start fence that no peer's map confirms. **1.3.0 (D326):** it lifts only the operator's fence, without asking the peers, and never gives a lease.
- **A new code, `Fenced`:** gRPC `FAILED_PRECONDITION`, HTTP 409. Nothing is written. The sender passes it through unchanged and never retries it.

**The write gate:** each partition's writes take a gate: an in-flight count, a fence flag and a `Notify`, with `SeqCst` admission (Dekker-style).
- **What takes it:**
  - every write;
  - a transaction's `Commit` when the transaction wrote or the commit carries rows;
  - bulk loads, text and vector index builds and rebalances;
  - the schema reconcile. It skips an operator-fenced partition, and its unfence runs it.
- **Lock order:** the gate comes after the service's admission (budget, scope) and is held until the write's commit ends, past its reply for a write that runs detached.
- **`Fence`** sets the flag, persists `DIR/fenced.json`, and waits up to 30 s for the writes admitted before it.
- **`Unfence`** checks that the map names the node at the file's epoch, persists `fenced.json`, reconciles the schema, and clears the flag last.
  - **Either fence** needs the peers' word too: every peer is asked at the call, and the newest map among those that answer (taken, if newer) must be at least as new as the node's own and name it at the file's epoch, or `force`. The first peer to agree isn't enough: one that missed the same move holds the same stale map (simp724's S1 on #1826). The node's own map may be stale. A cluster of one confirms on its own map. (Until audit120's P2-F1, 2026-11-13, only a start fence asked. A move fences its old node as the operator, and that node's map is the one left behind when it misses the move's map: an unfence there lifted the fence on the stale map alone and gave the partition two writers.)
  - **The accepted risk (1.2.0):** auto-unfence at start trusted one peer's map at least as new as the node's, and a peer that missed the same move held the same stale version. **1.3.0 (#1760, D317 amendment 3):** a partition is confirmed only when a majority of the map's nodes hold the newest map and it names the node; a round brings peers that are behind up to it before counting them, every open (a move's new owner too) starts unconfirmed, a map that changes the nodes can't also move a partition, and a move waits for a majority to hold its map.
- **Start:** every partition opens fenced. A peer's map at least as new as the node's own, naming the node at the file's epoch, unfences it. The sources are `sync_on_start`, a push from a peer, and a retry loop (250 ms up to 5 s). A one-node cluster unfences on its own map. An operator's fence stays.
- **A text index rebuild stops at a fence** (#1756): it runs a document at a time under the gate, so the fence's drain waits for one document. The stopped rebuild answers `Fenced`, keeps its cursor in the index, and resumes on its own once the partition takes writes again (an unfence, a lease back, an open after a move or a restart). Until #1722 a cluster node refuses `Text.Rebuild`, so today this covers a shutdown's close; the gate path is in place for #1722.

**Opening on a running node:** `Parts` is an `ArcSwap` of id to service, swapped under the node's open lock.
- **When:**
  - a map that gives the node a partition not open there opens it, then unfences it on that map;
  - `unfence` reopens a closed partition.
- **Its share** of `--shards` and `--compaction-threads` is the node's totals split for the new set. Open engines keep theirs; resharing an open engine is out of scope.
  - **Its compaction** is confined to the node's compaction CPUs (#1888, 1.3.0): the start's reserved set (ICR 0134), kept on the node's `Server`, or on a node that started with one partition the open partitions' pinned compaction CPUs. Before, a run-time open passed no set, so its threads were pinned on the free cores the network side's mask covers.
- **Its memory** is what the open partitions leave of `--engine-memory`, never past it (perf287's P2). Below the memtables' floor, the open is refused, naming the budget, and a restart of that node alone re-splits it (`start_budget`), since the map already gives it the partition. A target with no partitions gets all of it. **Amended (#1761, 1.3.0):** the open re-splits `--engine-memory` live, as a restart would. The open partitions keep their memtables (the arena is a fixed shared-memory region readers map, sized at open) and their block and row caches are resized (`Pigeonhole::set_cache_capacities`, hidden: a serve-only contract) to what their new shares leave after them (`placement::resplit`). Caches shrink before the new engine opens and grow back if the open fails; a close grows the others' after its engine closed, so the node never holds more than `--engine-memory`. The open is refused only when the open partitions' memtables leave too little for the new one's, and then a restart re-splits with smaller memtables. Shards aren't re-split (a database keeps its count), nor compaction threads (confined, above, their CPU is bounded).
- **Where it opens (the coordinator's mask directive):** on a fresh thread that first takes the narrow free-core mask (`Placement::enter`). `pin_plan` plans from the calling thread's mask, so the new shards land off every open partition's shard cores and their SMT siblings.
- **Too few free cores:** where the new partition's shards don't fit, the open is refused, never pinned onto a sibling or left unpinned.
- **The mask reapply:** each runtime's masks sit in an `ArcSwap` with a generation, published before the bump.
  - A thread records its role and reapplies its mask when the generation moved.
  - The triggers are the request, feed and hub runtimes' park and unpark hooks, each `Service::call`, and each blocking task's start, so a busy worker takes it at its next call.
  - The hub's read loop and the feed's delivery loops refresh too, once per read or batch: a busy hub may never park (perf287's P1).
  - Thread counts stay as started (Tokio can't resize a runtime), so a mask that shrank has more threads per CPU (cli692 on #1745). The log line gives each runtime's threads against its CPUs, and says so when the masks fall back to none (too few free cores).
  - **The other threads (#1762, 1.3.0):** right after the reapply, a sweep of `/proc/self/task` moves every thread that doesn't refresh itself onto the blocking mask, or the process's CPUs when there's none. That covers the engines' unpinned threads, and the server's own long-running ones: a backup, a bulk load, an index build or rebalancer, a replica's follow, and candle's pool, whose `CANDLE_CPU_MASK` is read only once.
    - It leaves alone the threads that refresh themselves (registered by thread ID as they enter: names don't tell a network worker from a Tokio blocking thread), and any thread whose affinity lies within the open partitions' pinned shard and compaction CPUs: the engines' own pins (D235, ICR 0134).
    - A thread whose affinity equals a mask in force before the reapply, or the process's CPUs, is moved even when the new pins cover it whole: on a narrow host, that's a thread still on the old free cores the new shards took (perf287's S1 on #1871).
      - That includes compaction threads confined to the node's reserved compaction CPUs (several partitions, ICR 0134) when the reserved set is the whole old blocking mask: free cores exactly as many as the compaction CPUs, without SMT. They move to the new blocking mask, which is the reserved set minus the new shard's core, and a close of that partition widens them again. `pinned_compaction_cpus()` still reports the whole reserved set. Letting the confined set win instead would leave every other thread on the new shard's core in that layout (perf287 and simp724 on #1871).
    - A thread is checked and masked under the lock a thread registers and masks itself under, so one entering mid-sweep keeps its own mask.
    - It runs on the blocking pool, awaited by the open or close, never on a request worker (simp724's S1 on #1871).
    - Two passes at most, the second only when threads started during the first. The log line counts the threads it moved. io_uring's `iou-wrk-*` workers refuse a new mask; they aren't counted, and are tried again at the next sweep.
    - Before 1.3.0 these threads kept the mask they started on, so a partition opened later could pin its shards where they ran.

**Close (amendment 2):**
- **It refuses an unfenced partition,** then waits 30 s for the fence's writes.
- **Then, in order:**
  1. it swaps the partition out of `Parts`;
  2. its feed hubs fail (their clients resume from their tokens);
  3. its bulk load, text rebuilds and index threads end;
  4. its database closes.
- **It answers `CLOSED` at once.** Each file is hashed on a blocking thread (SHA-256, then `posix_fadvise(DONTNEED)` on Linux). Each close has a generation, so a hash started for an earlier close of the same partition never lands on a later one (cli692's S2). The database's own close error shows in `error`.
  - A failed hash sets `error`, and `close` again retries it.
  - A call already running on the partition answers `PartitionUnavailable`, not the engine's `Closed`.

**Stamp:** refused while P is open here.
1. **The staged set is checked** (upload's `ready_set`): the header against the request, and each staged file's size and SHA-256 against the manifest, before any rename (feed1248's F2).
2. **The install is resumable (F1):**
   - it writes `.phdb-incoming/<P>.installing` (the header) first;
   - it moves P's files outside the manifest aside to `<file>.replaced-<nanos>`; then, for each staged file, it checks it against the manifest again, moves the file it replaces aside the same way (never overwritten, feed1248's S2), and renames it in; then it syncs the directory;
   - it removes the marker last, then P's leftover staged files (N2).
   - A stamp or an open that finds the marker rolls it forward, checking the files already renamed against the manifest. P never opens while the marker is there, and an upload of P is refused while it is (feed1248's S1).
   - The stamp opens the file for its identity row with one shard by default (a fed file keeps its recorded count), small memory and no pinned threads (feed1248's S3).
3. **The identity row is raised** from `from_epoch` to `to_epoch`.
   - Already at `to_epoch`: success, nothing done.
   - A stale file is refused unless `restore`.
   - With `restore_from_cluster_id`, the file must name that cluster, and is rewritten to this one.

**CLI:**
- **`phdb cluster fence | unfence | close | drop-partition P --node URL`.**
- **`phdb cluster relocate P --to ID --node URL [--old-unreachable]`.** The target is `--to ID`, since `--node` is the URL every `phdb cluster` command reaches a node by.
  - It checks that P is fenced or closed on its node (cli692's S7; closed for an in-place restore), unless `--old-unreachable`.
  - Then it stamps the target from `e` to `e + 1`, and sets the map through the writer.
  - Run again once done, it answers done.
  - `--copy` (keeping the feed history, `SetMapRequest.copies = 4`) is ticket 6 PR 2's (#1729, its own section).
- **`drop-partition`** moves P's files and any `.replaced` ones into `DIR/.phdb-dropped/<P>-<epoch>-<time>/`, deleting nothing, the main file last, so a drop that stopped part way finishes on a retry (cli692's N6). It refuses while the map places P on the node or P is open there.
- **`relocate --old-unreachable`** still asks the old node's `Status` for 3 s, and refuses if it answers serving P (cli692's S3).
- **`phdb cluster status`** gives each partition's state, for example `fenced (operator)`.

## Partition upload (for restore, 8b PR 2, and relocation by file copy, ticket 6 PR 2)
**`Admin.PartitionUpload`** (client streaming, gRPC only, admin scope, a cluster node only) streams one file of a partition's set into a node. **`Admin.PartitionUploadStatus { upload_id = 1; partition = 2 }`** says how many bytes of an upload the node holds, so a client can resume, and whether an install of `partition`'s set is in progress there (#1848, 1.3.0; empty `partition`, from an older client: false). Both answer `Unsupported` on a single node.
- **A partition is a set of files,** named by their suffix of the partition's database path: `""` for the main file, `-wal-<n>` for a WAL sidecar, `-feed` for the change feed's sidecar. Each node maps a name onto its own path. **`PartitionFile { name = 1; bytes = 2; sha256 = 3 }`** (common.proto) describes one.
- **`PartitionUploadHeader`:**
  - `partition` (1), and `cluster_id` (2) and `epoch` (3): what the main file says it is, so the epoch the copy is *of*;
  - `upload_id` (4): 32 lower-case hex, chosen by the client;
  - `purpose` (5): `RESTORE` or `RELOCATE`;
  - `file` (6): the file this upload carries;
  - `manifest` (7): every file of the set, `file` among them, with the main file required.
- **`PartitionUploadChunk { header = 1; offset = 2; data = 3 }`:** the first message carries the header. Each message's data starts where the node's bytes end, and is at most the node's `max_chunk`. That's 512 KiB, or less on a node whose `--max-request-bytes` is lower: min(512 KiB, the limit − 16 KiB), at least 1 KiB (cli692's F3 on #1728). A message that doesn't come within `--stream-idle-ms` ends the upload with `DeadlineExceeded`, which frees its place; it then resumes (cli692's F1).
- **Replies:** `PartitionUploadReply { received = 1; complete = 2 }`, and `PartitionUploadStatusReply { received = 1; complete = 2; max_chunk = 3; installing = 4 }`. A client asks the status first, for `max_chunk`.
  - `complete` holds only while the file is staged: an install renames it away.
  - `installing` holds while the partition's install marker is staged. The node then takes no upload of the partition, and a file the install already renamed isn't complete. A relocation or a restore run again over an interrupted install therefore sends nothing and goes on to its stamp, which finishes the install (#1848).
- **A full disk or quota** answers `NoSpace` (`ResourceExhausted`, 507): free space, then resume from the status. Nothing written is lost (cli692's F2).
- **Accepted only while the partition isn't open on the node,** with one upload of a given file of it at a time, and at most `--max-uploads` (default 1) at once; more are `Overloaded`. A resume must send the same header (`InvalidArgument` otherwise).
- **Staged, never served:**
  - Bytes go to `<cluster DIR>/.phdb-incoming/<upload_id>.part` (the directory mode 0700, never a link).
  - When every byte has arrived and the SHA-256 matches, the file is synced and renamed `<partition>.phdb<name>.ready`. Then its header is written beside it (`….ready.header`). The header is the commit marker, written last after any older one is removed, so a crash between the steps leaves a ready file with no header, which the install never takes (feed1248's F1 on #1728).
  - A file whose bytes don't match is dropped (`InvalidArgument`): upload it again.
  - Incomplete uploads untouched for 5 minutes are deleted, and so is every incomplete one when the node starts. Ready files are kept.
- **Installed by ticket 6's stamp,** not by the upload. Relocate's first step on the new node (copy, promotion and restore alike) installs the set once every manifest file is ready with one header. It checks the header against the stamp's partition and epoch (a restore may name an older epoch, and with `restore_from_cluster_id`, another cluster), replaces the partition's whole file set, then raises the epoch. The map change opens the partition. This upload section ships the staging; the install comes with ticket 6 PR 1.

## Ticket 6 PR 2: relocation by file copy
A partition moved by copying its closed files keeps its history, so feed tokens resume across the move. Fencing, closing, `relocate` and the stamp are PR 1's (its own section). PR 2 adds:

**`Partition.history_epoch`** (cluster.proto, field 5, `uint64`): the epoch P's current history began at.
- **Derived where a map is set** (`SetMap`), never taken from the map sent, so a map built by hand or from an older one can't carry a history across a move that broke it:
  - an unchanged epoch keeps the current map's value;
  - an epoch raised by a relocation by copy keeps it;
  - any other raise (a promotion, a restore) sets it to the new epoch;
  - a new partition's is its epoch;
  - a node's first map, unless a peer's, is derived over an empty map, so each history starts at its epoch. A restore's forced map onto fresh nodes doesn't carry the backup map's old values (simp724's S1 on #1729).
  - **A forced map** (`SetMapRequest.force`, amendment 1) is derived the same way against the receiving node's map, as a change that copied nothing, and can't list `copies` (`InvalidArgument`): a copy is a planned move, made through the writer. Its epochs must be new, so no history value is used twice (simp724's F1 and F2 on #1729):
    - **The node** refuses (`InvalidArgument`) a forced map that lowers a partition's epoch or moves it to another node at the same epoch, against its own map. The table rules of a change don't apply.
    - **`phdb cluster set-map --force`** checks the same against every node it reaches, since the receiving node can be behind. It refuses, naming the node, and reports the nodes it couldn't reach as `unchecked`.
    - So a token can expire across a forced map but never resumes across another history, provided no unreachable node had a newer epoch. An unchecked node is normally the down writer: check its map file (`phdb cluster show-map --file`) if it may have one.
- **Checked:** a map whose `history_epoch` is past its `epoch` is invalid.
- **Maps from before ticket 6** have 0. No epoch had moved then, so it reads as "never broken".
- **Peers take it as set:** a pushed or pulled map arrives derived.

**`SetMapRequest.copies`** (cluster.proto, field 4, `repeated string`): the partitions this change relocates by copy (`relocate --copy`, which `phdb cluster move` calls).
- It marks the change only and is never part of the map.
- Each listed partition must move to another node at exactly its epoch + 1. Otherwise the call is `InvalidArgument`. A node's first map can't list one.

**Feeds:** a cluster token's component taken at epoch `e` resumes while its partition's `history_epoch` is at most `e`. Otherwise it's `FeedExpired`. That's checked at the resume, and again when a part reopens where the map now puts its partition. A part that reopens records the epoch it opened at, so later checkpoints carry the new epoch. The token format is unchanged.

**`BackupDownloadRequest.fenced_file`** (admin.proto, field 6, `optional string`): on a cluster node, one file of `partition`, closed here after a fence, sent as it lies. No backup is taken.
- `fenced_file` names the file by its suffix after the partition's database path (`""` the main file, `-wal-<n>`, `-feed`; ICR 0135). It's `optional` so that `""` asks for the main file: set, it asks for a fenced file; unset, for a backup.
- **The file** is one the close's hash listed (PR 1's closed record, `close.rs`'s `Closed::file`), read from the partition's own path.
- **The chunks** are `BackupChunk`s of the file's bytes from `offset`. The last carries `total` and the file's `sha256`, as hashed at the close.
- **Refusals:**
  - only a partition in state `closed` (PR 1), and only once its hash is done; otherwise `FailedPrecondition` (a failed hash says to close it again);
  - a name the hash didn't list: `NotFound`, naming the files it did;
  - a file that isn't its listed size any more (it changed after the close): the download fails with `FailedPrecondition` rather than sending past or short of it. This holds for every download that streams a kept file;
  - `partition` is required;
  - `durable`, `replica` and `backup_id` must be unset (`InvalidArgument`);
  - a single node refuses `fenced_file` (`InvalidArgument`).

**`PartitionFile`** (common.proto) is the upload's (the section above). PR 1's closed-partition status (`LocalPartition.files`) lists one per file, and `fenced_file` takes its `name`. A move sends the closed partition's whole set, which the stamp installs only once complete. With a change feed, as every cluster partition has, that's the main file, every `-wal-<n>` and the feed sidecar `-feed`: a clean close keeps the WAL files as the feed's retained history (D310 §2.1), so the set can be as large as the feed's retention, and the move's downtime grows with it. That's why feed history survives: the WAL holds the retained changes, and the feed sidecar, bound to the database's id, holds the feed's sessions.

**`phdb cluster relocate P --to ID --copy`** sends PR 1's map change with `copies = [P]`. It's refused with `--old-unreachable`, since a copy is made from the old node's closed files, and a stopped node's partition is promoted instead. The CLI doesn't check that the stamp installed a copy: a rerun after a successful stamp also installs nothing. The guide says `--copy` is for files copied whole (by `move`, or by hand through the upload).

**`phdb cluster move P --to ID [--drop-source]`** (`cluster/moving.rs`) runs the whole move against the writer's map, where P is on OLD at epoch `e`:
1. **Fence and close** P on OLD (skipping what's done), then poll OLD's `Status` until P's `files` are listed. A failed hash is closed again. A close the database itself reported an error for stops the move.
2. **Copy** each file: `PartitionUploadStatus` on NEW for `received` and `max_chunk` (nothing is sent for a complete file, or while NEW reports the partition's install in progress: the stamp finishes it), then `BackupDownload{fenced_file}` from OLD at `received`, re-chunked into `PartitionUpload{RELOCATE, epoch e, manifest = OLD's files}`. The upload id is the first 16 bytes, as hex, of a SHA-256 over the cluster, the partition, `e`, the file's name and its hash. So a rerun resumes its own uploads, and a re-close with other bytes starts new ones. The download's last chunk must carry the listed size and hash.
3. **Stamp** NEW from `e` to `e + 1`. Compare the reply's `files` (the installed set, Q1) with OLD's closed `files`, by name, size and SHA-256. A mismatch stops the move before the map changes. The stamp itself refuses a staged set that doesn't match the manifest it was sent, so no test can produce a mismatch through the API: only `compare`'s unit test covers this check (simp724's N4).
4. **The map:** `relocate --copy`. Its stamp is a no-op now (the file is at `e + 1`).
5. **Serving:** poll NEW's `Status` until P is `serving`. A node serves P only at the map's epoch, so the state is enough.
6. **`--drop-source`:** `drop-partition` on OLD, only after step 3 compared and matched.

**A rerun** finds where the last run stopped:
- the map already names NEW: it's done, and nothing is compared or dropped;
- NEW's file is at `e + 1` (the stamp ran): it copies OLD's closed set and stamps again, as a first run does. The stamp installs the staged set over the earlier copy, which moves aside, and the move compares it. An earlier copy is never taken for this run's: OLD may have been unfenced and written to between the runs, and the stamp's identity row changed the main file, so the earlier copy can't be compared with what OLD closed now (audit120's F1, 2026-11-12).

Nothing is persisted client-side. The report gives the write downtime (fence to map, when this run fenced), the read downtime (close to serving), each file's size and bytes sent, whether the copy was verified, and what happened to the source.

## Amendment: fences and moves after #1800 (1.2.0 audit)
- **`Close` makes the fence the operator's, and persists it.** Whatever fenced the partition (`phdb cluster fence`, or the start fence), a close turns it into an operator's fence in `fenced.json`, so a closed partition doesn't reopen and lift its fence on a peer's map at a restart, mid-move.
- **A stamp that installs a staged set clears the operator's fence** on that node: the files are another copy, at a new epoch (a move or relocation back onto a node that once held the partition). A stamp that installs nothing keeps the fence. A restore's stamps keep it, and the restore lifts it once its map is set.
- **`move` step 1 fences P whenever it's open on OLD,** fenced already or not (a start fence isn't persisted), and refuses a partition that is still `opening` there: run the move again once it's open.
- **The report's `verified`** is `true` for every move that copied: the installed set is always compared with OLD's closed files before the map changes. Only a run that finds the move already done reports `false`, since it compared nothing.
- **`FENCE_REASON_OPERATOR`** (`cluster.proto`) says the same: set by `fence` or a `close`, lifted by `unfence` or by a stamp that installs a set.

## Callers changed
None yet: ticket 1 adds a mode, a service and a scope, and changes nothing a single-node server answers, except that `__phdb_` table names are refused and hidden.

## Amendment: `Admin.Info`, `Flush` and `Compact` on a cluster node (#1852, 1.3.0)
Additive; the numbers are the coordinator's (`.coord/numbers.md`). In 1.2.0 a cluster node answered these three `Unsupported` (the router's catch-all).
- **`Admin.Flush` and `Admin.Compact`** take `MaintenanceRequest { string partition = 1; }` in place of `Empty`, which is wire-compatible: an old client's empty message decodes as an empty request.
  - **On a cluster node:** an empty `partition` acts on every partition the map gives the node that's open, serving or fenced (a fenced partition's files still change), one after another: the set `Info` lists, so a copy the map moved away that's still open here isn't one of them. Each is tried even after one fails, and the answer is the first failure's code with every failed partition named in its message. A named one acts on that partition alone: `WrongPartition` for one another node holds, `PartitionUnavailable` for one closed or not open here, and `NotFound` for one the map doesn't list.
  - **On a single node:** `partition` must be empty (`InvalidArgument`).
  - **Never forwarded:** each node answers for its own partitions, as a backup does.
- **`Admin.Info`** gains `repeated PartitionInfo partitions = 7` and `repeated string other_partitions = 8`.
  - **On a cluster node:** `partitions` lists each partition the map gives the node, in id order. `other_partitions` lists the map's partitions other nodes hold. The top-level fields describe the node: `path` is its cluster directory, `file_bytes` its partitions' files together, `free_bytes` the free space on that directory's disk (0 when no partition is open), and `tables` the map's tables.
  - **On a single node:** both lists are empty, and the top-level fields are as before.
- **`PartitionInfo`:** `id = 1`, `path = 2` (its database file), `file_bytes = 3`, `tables = 4` (its own file's), `state = 5` (`LocalPartitionState`), `fence_reason = 6` (`FenceReason`), `awaiting_peers = 7` (bool, 1.3.0, #1829), and `lease_remaining_ms = 8` (1.3.0, D326).
  - The state, fence reason and `awaiting_peers` are as `Cluster.Status` reports them.
  - `file_bytes` and `tables` are set only for an open partition (serving or fenced).
  - A partition the map places on the node that it hasn't opened (no file, or a failed open) is `LOCAL_PARTITION_STATE_UNSPECIFIED`.

## D326 PR 1: leases (1.3.0)
**A partition serves on its node only while a majority of the map's nodes grant it a lease** (D326). Additive on the wire.

**Proto** (numbers from the coordinator, `.coord/numbers.md`, 2026-11-14):
- **`Cluster.Lease(LeaseRequest) returns (LeaseResponse)`:** node to node, cluster scope, gRPC only.
  - `LeaseRequest { cluster_id = 1; node_id = 2; map_version = 3; round = 4; repeated LeasePart parts = 5; lease_ms = 6; start = 7; }`. Another cluster's id is `InvalidArgument`. A `lease_ms` other than the grantor's `--cluster-lease-ms` is `FailedPrecondition`, naming both (audit120's M1). `start` is a random id per start of the asking node: a request with another id than the grant's replaces it whatever its round (audit120's S-b); within one start, an older round is ignored.
  - `LeasePart { partition = 1; epoch = 2; }`.
  - `LeaseResponse { newer_map_version = 1; repeated LeasePart granted = 2; repeated LeaseWithheld withheld = 3; grace_remaining_ms = 4; }`. A refusal isn't an error: the reply says what was granted and why the rest wasn't.
  - `LeaseWithheld { partition = 1; holder = 2; remaining_ms = 3; }`.
  - **`Cluster.Release(ReleaseRequest) returns (ReleaseResponse)`** (D326 PR 2): node to node, cluster scope, gRPC only. `ReleaseRequest { cluster_id = 1; node_id = 2; round = 3; repeated LeasePart parts = 4; start = 5; }`, `ReleaseResponse {}`. A node sends it to every node of its map when it closes a partition. A grantor drops the parts from its grant to that node only if the grant is of the same `start` and its round is no newer than the release's; it then ignores lease requests of an older round. Another cluster's id is `InvalidArgument`.
- **`LocalPartition.lease_remaining_ms = 14`** and **`PartitionInfo.lease_remaining_ms = 8`**: the time left on the lease, 0 without one.
- **`NodeStatus.grace_remaining_ms = 6`**: the time left on the node's start grace.
- **`FenceReason.FENCE_REASON_LEASE_LAPSED = 3`**: the lease ran out. `AWAITING_PEER_MAP` now means no lease since the partition opened here.

**Flag:** `phdb serve --cluster-lease-ms` (default 10000). Refused at start below 3 × `--cluster-push-timeout-ms`.

**Behaviour:**
- **A grantor** grants at its own map's version only, what that map gives the asker at the epochs asked, for L from receipt on its clock. It withholds a partition while its grant to another node runs, and grants nothing during its start grace, `L·(1+ρ)` (ρ = 1%), its own vote included.
- **The holder** asks every L/3 (sooner while a partition it serves has none), from the nodes that answered its round's map fetch, and holds P until `t0 + L·(1−ρ)/(1+ρ)` once a majority granted it.
- **Writes** need the lease and no operator's fence (`Fenced` otherwise, with the reason), and are checked again once the commit returns: past the lease's end, `OutcomeUnknown`. A transaction's `Commit` with writes is checked the same way, and a retry of it finds the same outcome.
- **Feeds (D326 PR 2):** a cluster partition's feed components deliver only under its lease, their catch-up included. Without one since the partition opened, while the node's last lease round reached a majority, they wait. Once the lease lapses, or a round misses a majority, they end with `PartitionUnavailable`, and a new subscribe of the partition there is refused. A quiet component ends at the lease's end too.
- **Reads** (`Get`, `MultiGet`, `ReadRow`, `Scan`, a fanned-out part, `Transactions.Begin`, `TxnGet`, `TxnGetRow`, `Snapshots.Open`, `Graph.Traverse`, `Vectors.Nearest`) need the lease when they take their snapshot, checked again after: `PartitionUnavailable` otherwise. New in 1.3.0: the fence was write-only. `Feed.Subscribe` isn't gated here (D326 PR 2).
- **`Unfence`** without `force` checks the peers as before; with `force`, it lifts the operator's fence alone. Either way the partition serves once it holds a lease.
- **A node change:** the writer takes no further change until `L·(1+ρ)` after it saw a majority of both node sets hold the change (audit120's L1); `SetMap` waits that out before it answers.
- **Two nodes:** a majority is both, so with one down the other stops serving within L.


## D328 PR 1: read replicas in the map (1.3.0)
**A node the map lists in `Partition.replicas` holds a replica copy of the partition, which follows the partition's owner** (D328, #1851). Additive on the wire. Routed reads are PR 2, `phdb cluster promote` PR 3.

**Proto** (numbers from the coordinator, `.coord/numbers.md`, 2026-11-14):
- **`Partition.replicas = 7`** (`repeated string`): node ids, in order of preference. Each a map node, none P's `node`, none twice; a map that removes a node must drop it from every list (`InvalidArgument` otherwise). A change to it alone isn't a move. The writer adds the first replicas only once every node reports the map feature `replicas` (`NodeStatus.map_features`).
- **`LocalPartition.role = 15`** (`PartitionRole`: `PARTITION_ROLE_OWNER`, `PARTITION_ROLE_REPLICA`) and, for a replica copy: **`replica_of = 16`** (the owner node it last dialled), **`replica_applied_seqno = 17`**, **`replica_primary_seqno = 18`**, **`replica_lag_ms = 19`** (since the owner last said it was caught up, on the copy's clock) and **`replica_state = 20`** (`ReplicaState`: `BOOTSTRAPPING`, `FOLLOWING`, `BEHIND`, `STOPPED`, `OWNER_FEED_OFF`). A copy's `error` is its follower's last error while it isn't following.
- **`PartitionInfo.replica = 9`** (admin.proto, the existing `ReplicaInfo`): set on each replica copy `Admin.Info` lists, beside the partitions the node owns; `primary` is the owner's address. Its `state` is unset.
- **`/metrics`:** each copy's `pigeonhole_replica_*` gauges, as a single node's replica has them, labelled `pigeonhole_partition`.

**Behaviour:**
- **The owner's side.** `Replication.Follow` on a cluster node serves the open partition it owns whose database the token names; none gives `WrongPartition` with the node's map version. The stream runs under P's lease (D326): it waits for one and ends with `PartitionUnavailable` when it lapses. `Admin.BackupDownload { partition, replica }` on a cluster node takes a durable snapshot and sends the feed token to follow from.
- **The owner's change feed must be on (S1).** Without it, both answer `FailedPrecondition` naming `--change-feed`, and the copy reports `OWNER_FEED_OFF`.
- **Cluster scope.** A cluster-scope token may make `Follow` and a `BackupDownload` with `replica` and a `partition` (the nodes' own calls to each other); every other call keeps its scope.
- **A new history bootstraps again.** When the map's `history_epoch` for P is above the epoch in the copy's identity row (a relocate without `--copy`, a restore), the copy bootstraps again from the owner rather than resuming. A copy move resumes.
- **A move onto a replica's node is refused (S4, until PR 3).** `phdb cluster move`, `relocate` and `restore --partition` with `--to` a node in `P.replicas`; and on that node, `Cluster.Stamp` for P while its replica copy is open (`FailedPrecondition`, naming the copy). `drop-partition` refuses while the copy is open. The steps: set a map without the node in P's replicas, `drop-partition` P there, run the command again.
- **Outside the lease machinery (S3).** A replica copy is never asked for, confirmed or lapsed, and `Fence`, `Unfence` and `Close` don't act on it (it isn't a partition they find).

## D328 PR 2a: single-partition reads from replicas (1.3.0)
**A `Get` or `ReadRow` sent with `x-pigeonhole-read-replica-max-lag-ms` may be served by a replica of its partition** (D328 §3). Additive on the wire.

**Headers** (not proto; an ICR line each):
- **`x-pigeonhole-read-replica-max-lag-ms: N`** (request): the staleness the read accepts. On a single node it's ignored.
- **`x-pigeonhole-min-primary-seqno: S`** (request, now also on cluster nodes): read-your-writes for the read's partition. On a read across partitions, together with the first header, it's `InvalidArgument`.
- **`x-pigeonhole-served-by: owner|replica`** and **`x-pigeonhole-served-seqno`** (response): set on a read sent with the first header.

**Proto** (numbers from the coordinator, `.coord/numbers.md`):
- `ForwardRequest.to_replica = 44`, `.replica_max_lag_ms = 42`, `.min_primary_seqno = 43`: the entry node chose this node's replica copy. The read is served from the copy, never as owner, else `ReplicaBehind` or `PartitionUnavailable`, and the entry node tries the next replica or the owner.
- `ForwardResponse.served_seqno = 28`: the seqno the serving node read at, for a single-partition read.

**Behaviour:**
- **Freshness** is the copy's own clock since the newest owner message whose durable frontier it had applied through (D330's M2: this keeps moving under sustained writes). A copy that's bootstrapping its first copy, or stopped, serves nothing.
- **Order:** the entry node tries P's replicas in the map's order, skipping one found behind or unreachable in the last 2 s, then the owner, within one deadline from the request's arrival.
- **A replica copy's reads** don't take P's lease (S3). Its writes stay refused.
- **Never routed to a replica:** writes, transactions, snapshots, `Subscribe`, schema and admin calls, and (until PR 2b) reads across partitions.

## D328 PR 2b: reads across partitions from replicas (1.3.0)
**A multi-get, scan, nearest search or traversal sent with `x-pigeonhole-read-replica-max-lag-ms` takes each partition's part from its replicas first** (D328 §3, S2). Additive on the wire.

**Proto** (numbers from the coordinator, `.coord/numbers.md`):
- `ForwardScanRequest.replica_max_lag_ms = 5`, `.min_primary_seqno = 6`, `.to_replica = 7`: the entry node chose this node's replica copy for the partition's pages.
- `ScanPage.served_seqno = 5`: the seqno a page was read at (the replica's applied seqno, or the owner's visible seqno, after the page); 0 on a scan sent without the bound.
- A multi-get's or nearest search's part to a replica is `ForwardRequest` with `fanout_part` and `to_replica` (PR 2a's fields). It holds no budget on the replica, as an owner's part holds none.

**Behaviour:**
- **Parts:** the entry node tries the partition's replicas in the map's order, skipping one backed off, then the owner. A traversal's hops are multi-get parts.
- **Scans (S2):** each page checks the copy's freshness before its snapshot and again before it's sent. Past the bound the stream ends with `ReplicaBehind`, and the entry node reopens the partition from the last page's position on the next replica or the owner: rows aren't skipped or repeated, and the row limit carries over.
- **A scan, nearest search or traversal at a snapshot, or a multi-get at one,** stays the owner's. Read-your-writes with a read across partitions stays refused (PR 2a).
- **The response headers** `served-by` and `served-seqno` aren't set on reads across partitions, whose parts can come from different places. A scan's pages carry `served_seqno` instead.

## D328 PR 3: promotion, and moves onto a replica's node (1.3.0)
**`phdb cluster promote P --to ID` makes a node holding P's replica P's owner** (D328 §6). Additive on the wire.

**Proto** (numbers from the coordinator, `.coord/numbers.md`):
- `StampRequest.promote = 7`: the file is this node's replica copy of P, at `replica_epoch`. The node makes it a primary at the last commit it applied (`Pigeonhole::promote`) if it's still a replica, then raises its epoch to `to_epoch`. Refused with a staged set, with `restore`, or while the copy is open here.
- `StampRequest.replica_epoch = 8`: the epoch the copy's identity row records, the owner's at its bootstrap (`e_b`). The file must still record it, and it must be at most `from_epoch`.

**Status:** a replica copy's `LocalPartition.epoch` is now its identity epoch (`e_b`), not the map's.

**Behaviour:**
- **Promote** checks:
  - the owner is fenced or closed, or `--old-unreachable` (refused if it answers and serves P);
  - ID's copy satisfies `history_epoch <= e_b <=` the map's epoch.
- **Promote's steps:**
  1. A map without ID in P's replicas (which closes the copy).
  2. The stamp with `promote`.
  3. The map with P on ID at the epoch + 1 and a new history.
  4. A wait until ID serves P at that epoch, as a move's (`--timeout` bounds it; #2035).
  5. The report: `applied_seqno`, `owner_durable_seqno` and `lost_commits` ("unknown" with `--old-unreachable`).
- **A rerun** finishes from the closed or stamped copy, if the map still has P where the stopped run left it. Once the map has moved on (a set-map, relocate or promote since), the stamp refuses a file that's already a primary, rather than stamp it over what P took since: the operator moves it aside.
- **Not in 1.3.0:** making the old owner a replica automatically (the owner's #1484 call: the operator rejoins it).
- **`Admin.Promote` on a cluster node** answers `FailedPrecondition`, naming `phdb cluster promote`.
- **S4, now done for you:** `move`, `relocate` and `restore --partition` with `--to` a node in `P.replicas` first set a map without it there, and wait for it to close its copy. The set installed replaces the copy's file, which goes aside. The node still refuses `Stamp` while a copy of P is open, as the safety net.

## D330 PR 1: the map's writer under a lease, with terms (1.3.0, #1484)
**The map's writer holds a lease as a partition's owner does, and maps are ordered by a writer term.** With failover on, a node is elected in a down writer's place. Additive on the wire.

**Proto** (numbers from the coordinator, `.coord/numbers.md`):
- `ClusterMap.writer = 9`, `.writer_term = 10`: the writer the map names, and the term it was elected or forced at. A map names a writer from term 1 on, and only then. At term 0 the writer is the smallest id, as before.
- `ClusterMap.failover = 11`: off by default (the owner's Q1). Turned on with `set-map`. Off, no node elects a writer.
- `LeasePart.term = 3`: the writer lease's term (`partition` `__writer`, `epoch` 0).
- `LeaseRequest.writer_term = 8`: the asking node's map term.
- `LeaseResponse.writer_term = 5`: the grantor's highest term, on an answer to a vote.
- `NodeStatus.writer = 8`, `.writer_term = 9`, `.writer_lease_remaining_ms = 10`.
  - `writer_term` is the node's highest term: its map's, or one it voted for.
  - `writer_lease_remaining_ms` is, on the writer, its own lease; elsewhere, the node's grant of it to the writer.
- **Map feature `writer_terms`:** `writer`, `writer_term` and `failover` go into a map only once every node reports it.

**Flag:** `phdb serve --cluster-failover-after-ms` (default 0, which means 2 × `--cluster-lease-ms`): the failover margin.

**Ordering:** maps are ordered by `(writer_term, map_version)`.
- A node takes a map with a higher term, whatever its version, and refuses one with a lower term.
- An equal pair with other contents is divergent. An equal pair with the same contents is a no-op install.
- A lease round orders peers' maps the same way. So does a grantor, for its version check and `newer_map_version`.

**The writer lease:**
- `__writer` is a reserved partition id. The writer asks for it in its lease rounds at the map's term.
- A grantor grants it to the node its map names as writer, at the map's term, unless it voted for a higher one. It withholds it from any other node as it withholds a partition.
- From term 1, `SetMap` needs it before the install, waiting up to `L·(1+ρ)` plus a push timeout for it, and again before the push. If it's gone by the push, the map stays installed on the writer and the answer is `OutcomeUnknown`: the map may still reach the cluster at the writer's next round, or be replaced by an elected map (simp724's S1 on #2056). At term 0 the lease isn't needed, so a cluster whose older nodes don't grant it can still change its map.
- A plain `SetMap` keeps the current map's `writer` and `writer_term`, whatever the request says.
- A node that installs a map naming another writer releases its writer lease to every node (`Cluster.Release`, keyed by the term).

**The term fence:**
- A node refuses any map below the highest term it voted for: on a push, a set, a forced set or a round's catch-up.
- The vote is synced to `cluster-writer-term` in the node's cluster directory before it's answered, so the fence survives a restart.

**An election** runs only while the map has `failover` on. It starts on a node once:
- its grant of the writer lease has been out for the failover margin;
- every node it reaches reports no writer lease;
- each smaller live id (one that answered) has had a margin to go first.

Its steps:
1. **A vote:** `Cluster.Lease` with the writer lease alone, at one above every term it has seen. A grantor grants it, whatever the request's map version, when:
   - its start grace is over;
   - no other node's writer lease runs there;
   - the term is above its map's and above any term it has voted for.

   The grantor records the term, then answers. The vote joins the candidate's grant there rather than replacing it.
2. **The newest map** by `(term, version)` that the voting majority holds.
3. **That map** at the new term, naming the candidate, at the next version. It's pushed until a majority holds it, while the vote's lease runs.

**`set-map --force`:** once every reachable node keeps `writer_terms`, the forced map takes a term above every one they know (simp724's S2), naming its writer: the map's `writer` if set, else the smallest id. It must be reachable. A node that knows a higher term refuses it. If a node knows a term but runs a build without terms, the force is refused.

**`phdb cluster status`** adds `writer`, `writer_term`, `writer_lease_remaining_ms` (the writer's own) and `failover`.

**Not yet:** the guide's failure model and timeline (PR 3). Partition failover, its events and metrics are D330 PR 2, below.

## D330 PR 2: partition failover by the map's writer (1.3.0, #1484)
**With the map's `failover` on, the writer promotes a down partition's freshest replica, through D328's promote path, and records it in the map.** Off by default. Additive on the wire.

**Proto** (numbers from the coordinator, `.coord/numbers.md`):
- `ClusterMap.failover_rejoin = 12`: reserved (the owner's Q4); a map setting it is refused.
- `ClusterMap.failovers = 13`: repeated `FailoverEvent`, a new message with its fields numbered from 1:
  - `kind` (`FailoverKind`: PARTITION or WRITER), `partition`, `from`, `to`;
  - `at_ms` (the writer's wall clock, never compared), `writer`, `writer_term`;
  - `applied_seqno` and `primary_seqno` (the promoted copy's applied seqno and the owner's durable seqno as it last heard it);
  - `in_doubt_xids` (at most 4 KiB, as of the owner's last renewal) and `in_doubt_count`;
  - `to_epoch` and `state` (`FailoverState`: PENDING, DONE, ABANDONED).

  The map keeps the last 16 finished events, and every pending one.
- `Partition.failover = 8` (`FailoverMode`: INHERIT, OFF, ON). The map's flag is the master switch: OFF opts P out, and ON is INHERIT's behaviour.
- `Partition.failover_count = 11`: P's failovers, ever; it never goes down.
- `LeaseRequest.pre_vote = 9` (bool): an election's pre-vote (Raft's). A candidate asks for real votes, and records its own, only once a majority would vote. A node answers it as the vote, recording, granting and fencing nothing, and says yes only once the writer lease has been out there for the failover margin (simp724). Without it, a node cut off from every other ran an election it couldn't win; its recorded vote then fenced the live writer's lower-term maps from it for good once it rejoined (found by §8 test 2). A build without leases answers `Unimplemented`, as to a vote: no pre-vote. A vote that's really held and lost can still strand its candidate; the fix (the live writer adopts the higher term) is a 1.3.0 follow-up.
- `LeasePart.in_doubt_xids = 4` and `.in_doubt_count = 5`: an owner's in-doubt external shares of P, sent with its renewals while the map has failover on, as the operator pastes them into `phdb cluster txn decide` (UTF-8; hex only for an xid that isn't). They're empty in an answer.
- `NodeStatus.failover_blocked = 12`: on the writer, each down partition it doesn't fail over, and why.
- `NodeStatus.grants = 13`: every partition lease the node grants that still runs (`LeaseWithheld`: partition, holder, time left). The writer's survey reads it.
- `LocalPartition.failover_blocked_reason = 26` was assigned and isn't used: the owner of a down partition can't report it, so the writer's `failover_blocked` replaces it (simp724's F6).
- **Map feature `writer_terms`** gates every field above, as it gates `failover`.

**Flag:** `phdb serve --cluster-failover-max-lag-ms` (default 0, no limit). An automatic failover refuses a replica staler than that beyond the owner's last renewal.

**Down:** the writer's own grant of P to P's owner has run out, unrenewed, the failover margin ago.
- A writer that took office since counts from the owner's first renewal it saw, or from a lease after taking office (simp724's S3).
- Down partitions are reported, not failed over, when:
  - the map's `failover` is off;
  - P is OFF;
  - P has no replicas;
  - the owner answers and serves P, or the operator fenced (`FENCE_REASON_OPERATOR`) or closed it there. An owner that answers but has lost its lease reports Fenced awaiting its peers, and counts as down;
  - any other node still grants P's lease to its owner (`NodeStatus.grants`: the owner is alive, cut off from the writer alone).
- A node that doesn't answer the survey is unknown, not granting. The grant round still keeps two owners from serving at once.
- A partition blocked after a survey is surveyed again only after the failover margin, or when the map changes (perf287 on #2069). A newly down partition is surveyed at once.

**The replica:** of P's replicas, the one with the highest applied seqno (then map order) whose copy:
- is FOLLOWING or BEHIND;
- is at the writer's own map version and term;
- is inside P's history;
- is no staler than the owner's last renewal plus a heartbeat (1 s), and within `--cluster-failover-max-lag-ms`.

The owner's last renewal is the writer's own last grant of P to it, less a lease. The writer is one of P's grantors, so it has this whatever its office time; a writer elected after the owner died still measures from the owner's real renewal (simp724's B1 on #2069). A writer that has seen no renewal of P's owner since it started has no baseline. It blocks P with a reason that says to promote by hand (`phdb cluster promote P --to <replica> --old-unreachable`).

**The promote** is `phdb cluster promote`'s, shared code, run by the writer over its peers. One runs at a time.
1. **A map** takes the copy's node out of P's replicas and, with writer terms or failover in the map, records the promote PENDING at its `to_epoch`.
   - `to_epoch` is above P's epoch and above every epoch a promote of P has recorded (simp724's S2): a copy stamped by a promote that never finished never matches a new history.
   - An older pending promote of P becomes ABANDONED.
2. **The stamp** at `to_epoch`. A promote's stamp may raise the epoch by more than one; every other stamp still raises it by exactly one.
3. **The map** gives P to the copy's node at `to_epoch`, with a new history, marks the event DONE and raises `failover_count`.
4. **It waits** for P to serve there.

A writer that stops between the stamp and the map leaves the promote PENDING. The next check finds the copy stamped at `to_epoch` and finishes it (finish only; simp724's F4). `phdb cluster promote` by hand does the same, and resumes by the pending epoch, not "the map's + 1".

**Pending promotes are kept.** A map that drops or alters a pending promote whose `to_epoch` is above P's epoch is refused, on a set, a forced set, a push or a catch-up. It may only:
- finish it, with the map giving P that node at that epoch;
- abandon it, for a later promote of P recorded above it.

No other move takes P to an epoch at or below a pending one's.

**An election** records a WRITER event in the elected map.

**The writer's duties** (an election, then failovers) run on a task of their own after each lease round, one at a time, never inline. Every survey asks all nodes at once, under one deadline: the push timeout, at most a sixth of the lease. This fixes #2056's stall, where an election's survey asked nodes in turn inside the lease round, so a black-holed peer could delay the node's own renewals.

**A map change wakes a replica copy** whose partition's owner, epoch or history it changes (D330 amendment 2). The copy looks the owner up at once, rather than after its retry pause (up to 10 s).

**Metrics:** `pigeonhole_cluster_failovers_total{partition}` (from `failover_count`) and `pigeonhole_cluster_writer_term`, from the node's map: every node shows the same.

**`phdb cluster status`** adds `failover_blocked` (the writer's) and `failovers` (the map's events). Each node's JSON has its `grants`.

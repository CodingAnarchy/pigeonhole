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
- **Refused with `Unsupported`, naming the ticket that adds each:** transactions and snapshots (3, including a `Get` or `ReadRow` with a `snapshot_id`), fan-out reads (4), schema calls and `/v1/tables` (5), feeds (7) and backups (8). Every other call is refused too, `Info` and `/mcp` included.

**`Cluster.Forward`** (node to node, `cluster` scope, gRPC only) carries a routed call, with the sender's `map_version` and the caller it's for (`ForwardedCaller`: `read` or `write` scope, the token's namespaces). The owner checks those again.

**New server codes** (`code_num` 0; each counted in `pigeonhole_serve_requests_total` under its own `outcome`):

| Code | gRPC | HTTP | Retry |
|---|---|---|---|
| `WrongPartition` | `FAILED_PRECONDITION` | 409 | yes: this node's map no longer gives it the key. The message carries its `map_version`, and the forwarding node pulls the map and forwards once more. |
| `CrossPartition` | `FAILED_PRECONDITION` | 409 | no: split the batch. |
| `PartitionUnavailable` | `UNAVAILABLE` + detail | 503 | yes: nothing was sent to the partition's node (B3). A forwarded write that fails after it was sent answers `OutcomeUnknown`. |

## Ticket 4a: fan-out scans and multi-gets
**`Data.Scan` and `Data.MultiGet` read across a table's partitions** (D317 §7), and answer what one database holding every partition would. They're no longer refused on a cluster node. A read with a `snapshot_id` is still refused until ticket 3, and `Nearest` and `Traverse` until ticket 4b.
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
- **Connections:** forwarded streams use connections of their own to each peer, apart from the unary forwards'.

**`Cluster.Forward`** gains a `multi_get` arm: one partition's keys of a fanned-out multi-get.

**Budgets:** a fanned-out scan holds one `--max-scans` slot on its entry node for its life, and counts as one `Data.Scan`. Each page it hasn't handed to the client counts against `--inflight-bytes`, as a single node's scan pages do.

## Callers changed
None yet: ticket 1 adds a mode, a service and a scope, and changes nothing a single-node server answers, except that `__phdb_` table names are refused and hidden.

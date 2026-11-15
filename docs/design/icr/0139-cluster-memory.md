# 0139: agent memory and `/mcp` on cluster nodes (`ClusterMap.memory`, memory forwards, recall's pool API)

**Status:** Approved. The numbers are from the coordinator (`.coord/numbers.md`), for #1915. The design (`.coord/ai1249-design-memory-mcp.md`) was reviewed by simp724, who confirmed the revised recall (M1) and the lease rules (M2); the coordinator chose the two-phase exact recall. This file grows with #1915's PRs: PR 1 (below) places the store and routes the calls that touch one partition; PR 2 adds the calls that span partitions (Watch in PR 2b); PR 3, `/mcp`.

## PR 1: a store per partition, and routing by namespace

**proto (`cluster.proto`, additive):**
- `MemoryPlacement { store = 1, splits = 2 (repeated SplitKey), partitions = 3 }`, and `ClusterMap.memory = 8`.
- `ForwardRequest.op` arms `remember = 34`, `recall = 35`, `mark_used = 36`, `supersede = 37`, `link = 38`, `forget = 39`, each the client's request. `ForwardResponse.reply` arms `remember = 19`, `recall = 20`, `mark_used = 21`, `forget = 22`; `supersede` and `link` answer `empty`.
- `NodeStatus.map_features = 7` (`repeated string`, ICR 0138's number, shared): the map fields the node's build keeps. This build reports `memory`.

**Why a whole store per partition:** a store is three tables (`mem_S`, `mem_S_text`, `mem_S_ivf`). Its memory, dedup and counter rows are keyed by namespace (`{ns}\0…`), but its index rows (postings, corpus statistics, IVF lists) cover every namespace in a file, and every write touches all three in one transaction. So a partition holds a whole store of its own for the namespaces in its range.

**What a valid map is (beside ICR 0117's rules):**
- the store is named, and none of its tables is placed as a table;
- split keys are non-empty, distinct, increasing and contain no `\0`. For such a key `k`, `k <= ns` exactly when `k <= ns\0…`, so a namespace's rows never straddle a range: a namespace routes by comparing it alone;
- `partitions.len() == splits.len() + 1`, each one listed.

**What a change may do:** the memory placement is fixed once written (compared with split keys as bytes): not dropped, re-split, moved or renamed (#1499). A partition it names isn't dropped.

**Adding it (mixed versions):** a 1.2 node drops `ClusterMap.memory` when it stores a map, so its copy would read as another map of the same version (divergent, and no help to a confirm, #1899). The map's writer adds a memory placement, in a node's first map or a later one, only once every node in the map answers `Status` with `memory` in `map_features`. Otherwise `SetMap` answers `FailedPrecondition`, naming each node that doesn't (an older build, or no answer). `--force` skips the check, as it skips the majority.

**`phdb serve --cluster` with `--memory NAME`:** accepted (ICR 0117 refused it).
- The node's map may place `NAME` now or later; a map placing another store refuses the start.
- Each partition the map places the store on opens it at the partition's reconcile (at start, on each map taken, and when a partition opens while the node serves), creating it with the `--memory-*` options if missing, and checks it against the node's pin, `--memory-dim` and `--memory-normalization`. A mismatch is kept as that partition's answer to its memory calls (`FailedPrecondition`), not a failed start: other partitions serve, and the partition's `Status` lists it in `reconcile_errors`. A partition the map places the store on whose node runs without `--memory` does the same, naming the flag.
- The store's forgetter and IVF rebalancer run on the server's threads, each slice under the partition's write gate (a fenced partition writes nothing in the background), not on the crate's threads. They end when the partition closes.

**Routing:** a memory call goes to the partition holding the namespace it names, served there or forwarded (`Cluster.Forward`), with the owner checking the caller's scope and namespaces again:

| Call | Partition |
|---|---|
| `Remember` | `namespace` |
| `Link` | `from`'s namespace |
| `Supersede` | `old`'s namespace |
| `MarkUsed`, `Forget` | every id's namespace |
| `Recall` | every named namespace |

- An empty namespace, or one containing `\0`, answers `InvalidArgument` before routing.
- A call whose namespaces are on more than one partition answered `Unsupported` until PR 2.
- `Memory.Watch` answered `Unsupported` until PR 2b.
- A map with no memory placement: every memory call answers `Unsupported`.

**Callers:** `phdb serve --cluster`, and clients of `Memory` through any cluster node.

## PR 2: recall, `MarkUsed` and `Forget` across partitions

**Amendments to the design, from building it** (simp724 confirmed both, `.coord/simp724-review-1915-pr2-exactness.md`):
- **The graph expansion goes hop by hop.** Its bounds (`Expand::max`, and the byte budget its links are read within) are shared by every seed, so one walk per partition isn't one store's walk once either binds. The entry node drives the walk, and each hop is one round trip to the partitions holding its memories. The cut is exact: a store's hop cuts iff its links' bytes pass the budget left, whatever their order. So a partition's part that passes it cuts, and so do parts that pass it together.
- **Each namespace doubles its own `n`** when the gather's filters leave a list short, single node included. An IVF list depends on `n`, so with one `n` for the whole recall, another namespace in the call could change a namespace's IVF candidates, which a partition can't reproduce. Recalls over one namespace, and exact lists, are unchanged.

**proto (`cluster.proto`, additive):**
- `ForwardRequest.op` arms `recall_pool = 40` (`RecallPoolRequest`) and `recall_bodies = 41` (`RecallBodiesRequest`); `ForwardResponse.reply` arms `recall_pool = 23` (`RecallPoolResponse`) and `recall_bodies = 24` (`RecallBodiesResponse`).
- `RecallPoolRequest { request = 1 (RecallRequest), now_us = 2, hop = 3 (bytes) }`: the pool, or with `hop` (a `RecallHop`'s bytes) the partition's part of that hop. Field 3 was `repeated bytes seeds` in the design and is `bytes hop` (this amendment, confirmed by the coordinator 2026-11-15 and recorded in `.coord/numbers.md`; allowed because ICR 0139 hadn't shipped).
- `RecallPoolResponse { pool = 1 }`: a `RecallPool`'s bytes, or a `RecallHopPart`'s for a hop.
- `RecallBodiesRequest { request = 1, ids = 2 }`, `RecallBodiesResponse { bodies = 1 }`.
- `ForwardMemoryWatchRequest { map_version = 1, caller = 2, partition = 3, request = 4 }` and `Cluster.ForwardMemoryWatch` are defined here and served from PR 2b (Watch).

**`pigeonhole-memory`'s public API** (additive; opaque, versioned bytes on ICR 0138's pattern; a version this build doesn't read is `InvalidArgument`, which serve answers as `FailedPrecondition`):
- `Recall::gather(self) -> Result<RecallPool>`, `Recall::hop(self, &RecallHop) -> Result<RecallHopPart>`, `Recall::bodies(self, &[MemoryId]) -> Result<RecallBodies>`. `hop` and `bodies` refuse a memory outside the recall's namespaces.
- `RecallPool`: `merge`, `len`, `is_empty`, `to_bytes`, `from_bytes`, `rank(Fusion, Weights) -> RecallRanking`. A memory in two merged pools is `InvalidArgument`.
- `RecallRanking`: `top(k)`, `walk(Expand, Option<u64>) -> RecallWalk`, `finish(RecallBodies, k, Option<u64>, with_signals) -> (Vec<RecallHit>, RecallStats)`.
- `RecallWalk`: `next_hop() -> Option<RecallHop>`, `settle(parts)`, `finish() -> RecallRanking`.
- `RecallHop`: `namespaces`, `split(by) -> BTreeMap<K, RecallHop>`, `to_bytes`, `from_bytes`. `RecallHopPart`: `to_bytes`, `from_bytes`.
- `RecallBodies`: `merge`, `len`, `is_empty`, `to_bytes`, `from_bytes`.
- In place of the design's `expand_from(seeds)` and `RecallRanking::expanded(pool)`: the walk above. `Recall::run`'s signature is unchanged, and it runs these steps in process.

**A recall across partitions:** the entry node, which needs `--memory` for this (a node without it answers `Unsupported`, though it forwards a one-partition recall), checks it (scope, `k`, the byte budget against its own `--memory-*` limits, at most 64 namespaces), embeds its text once if it has an embedder, and fixes one clock (`now_us`). Then:
1. each partition holding some of its namespaces returns its pool;
2. the entry node merges and ranks them, then walks the graph expansion hop by hop;
3. the partitions holding the best `k + 4` return their bodies, and the entry node cuts the hits to `k` and the budget, skipping any memory gone since its pool.

Each step is a fan-out part (`Service::fanout_part`): lease-gated before and after its read, counted against the entry node's `--inflight-bytes`. A partition that can't be reached fails the recall (`PartitionUnavailable`); there's no partial answer.
- **Exact against one store holding every namespace** but for the keyword signal (each partition's BM25 statistics are its own), and only when quiescent: each step reads each partition at its own snapshot.
- A recall whose namespaces are on one partition is forwarded whole, as in PR 1, with a single node's rules (no 64 cap).

**`MarkUsed` and `Forget` across partitions:** split, each partition's ids one call (served or forwarded as a write), sent together, the counts summed. A part that's refused, or unreachable before its send, fails the call with its error. A part whose outcome is unknown makes it `OutcomeUnknown`, naming the partition. Either message names the partitions that applied and their counts. Retrying the whole call is safe: `Forget` skips what's gone, and `MarkUsed` is advisory.

**Still refused:** `Memory.Watch` (`Unsupported` until PR 2b, below), and `/mcp` (PR 3).

## PR 2b: `Memory.Watch` across partitions

The design is `.coord/ai1249-1915-pr2b-watch.md`, reviewed by simp724 (`.coord/simp724-review-1915-pr2b-watch.md`). No new numbers: `Cluster.ForwardMemoryWatch` and `ForwardMemoryWatchRequest` are PR 2's, and the marks below are `MemoryEvent.progress = 3`.

**A watch on a cluster node** has one component per partition holding its namespaces (every partition of the placement for a watch of every namespace), merged on the node called:
- **Components:** a partition served there is watched there; another node's over `Cluster.ForwardMemoryWatch`, on the watch's own connection to that node. Each is told only its partition's namespaces. The owner checks the caller's scope and namespaces again, and that its map gives it the partition with those namespaces (else `WrongPartition`, never forwarded on).
- **Whole commits:** a component's stream marks commits with `Progress` events: one first, at its start position, and one after each commit with memories, with that commit's token. The merge takes the marks (the client never sees them) and moves each commit as one, so no other partition's event lands inside a commit, and resuming from any event's token never skips the rest of one. Nothing is delivered before every component's start mark, and the opens and start marks are bounded by the request timeout: a component delivers nothing before its partition's first lease, so one whose partition gets none in time fails the call with `PartitionUnavailable`, naming it (simp724's S5).
- **The token** is the cluster feed's `ClusterToken` on every event: each partition at its last whole commit delivered, the event's own at its commit's end, with its epoch. Its hash is `memory_watch_hash`: the store and the sorted namespaces behind a domain tag, with a tag of its own for a watch of every namespace, so a feed's token, another watch's (another store or namespace set) or a single node's is refused (`InvalidArgument`). A partition whose history began after the token's epoch is `FeedExpired`, as for the feed.
- **Leases (M2):** each component is delivered under its partition's lease, through `DeliveryLease` (`serve/lease.rs`), lifted out of the feed's events so both hold one. Without a lease, the stream's own end comes first, so a move's `WrongPartition` isn't reported as the lease's `PartitionUnavailable` (simp724's M1), and each look while waiting for a first lease stamps the stream's take clock, so `--stream-idle-ms` doesn't end it (M2).
- **Close and move (S2):** a component ends with its partition's `parts_ended` error, as the feed's do: `PartitionUnavailable` on a close, `WrongPartition` on a map that moves it. Any component's error ends the watch (no `allow_partial`); the client resumes from its last token.
- **`--max-streams`:** one place on the node called, and one per component on its partition's node, as a cluster subscription.
- **An older node** answers `ForwardMemoryWatch` with `Unimplemented`, which the merge answers as `Unsupported`, naming the node.

## PR 3: `/mcp` through the router, then on a cluster node

Design: `.coord/ai1249-1915-pr3-mcp.md`, reviewed by simp724 (`.coord/simp724-review-1915-pr3-mcp.md`). PR 3a sends serve's `/mcp` tools through `Router::dispatch` on one node. PR 3b lifts the cluster refusal.

**proto (additive, this amendment; numbers from the coordinator, recorded in `.coord/numbers.md`):**
- `QualifierSelection { prefix = 1 (bytes), start = 2 (Bound), end = 3 (Bound) }` in common.proto. No existing message held these: `Selection` and `TraverseRequest` carry them as inline fields.
- `NearestRequest.qualifiers = 10`: a nearest search ranks only the vectors at the qualifiers it selects, applied as a read's `Selection` applies its qualifier fields. On a cluster, each partition's part applies it. The `/mcp` `nearest` tool's `qualifier` and `qualifier_prefix` need it once the tool is a `Nearest` call.
- **`NodeStatus.map_features` widened** (the coordinator's option (a)): it lists what a build supports that its peers rely on. That's the map fields it keeps, as before, and now the request fields it applies. The first of those is `nearest_qualifiers`.
- **Mixed versions** (simp724's point; the coordinator's call):
  - A node built before `qualifiers` ignores field 10 and ranks every qualifier.
  - So a cluster nearest search with `qualifiers` asks each node that holds a part of it for its features. Answers are cached on the entry node for 30 s per peer.
  - It is refused with `FailedPrecondition`, naming each node that answers without `nearest_qualifiers`. A silent short result would be worse.
  - A node that doesn't answer is unavailable, not an older build (the coordinator's call on simp724's question): with `allow_partial` its parts are skipped and named in `missing`, otherwise `PartitionUnavailable`. Its no-answer is cached for 5 s, so searches while it's down don't each wait a push timeout.
  - The entry node also applies the selection to every part's answer again. That covers a peer whose build changed within the cache: never a neighbour at another qualifier.

**`--mcp-blocking` (default 8; the coordinator's routine call, 2026-11-15):**
- **What:** the most `/mcp` messages running at once. Each runs on a blocking-pool thread under a place of its own, not a `--blocking-threads` permit, and tokio's blocking-thread cap grows by the same count.
- **Why:** a tool's database call is now a service op through `Router::dispatch`, which takes a `--blocking-threads` permit of its own. If `/mcp` messages held those permits while waiting on their ops, a pool full of them would wait for ever. With a place of their own, they never hold a permit their ops need.
- **The conditions it rests on (simp724's review):**
  - the place is awaited on the async side, before the pool thread;
  - only `/mcp` threads wait on blocking work;
  - every other wait on the dispatch path is a try or bounded;
  - the cap leaves room for the ops.
- **Default:** 8 holds a few agents' sessions at once while keeping the extra threads small beside `--blocking-threads`.

**Still refused in PR 3a:** `/mcp` on a cluster node (PR 3b).

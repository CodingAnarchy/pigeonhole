# 0139: agent memory and `/mcp` on cluster nodes (`ClusterMap.memory`, memory forwards, recall's pool API)

**Status:** Approved. The numbers are from the coordinator (`.coord/numbers.md`), for #1915. The design (`.coord/ai1249-design-memory-mcp.md`) was reviewed by simp724, who confirmed the revised recall (M1) and the lease rules (M2); the coordinator chose the two-phase exact recall. This file grows with #1915's PRs: PR 1 (below) places the store and routes the calls that touch one partition; PR 2 adds the calls that span partitions; PR 3, `/mcp`.

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
- Each partition the map places the store on opens it at the partition's reconcile (at start, on each map taken, and when a partition opens while the node serves), creating it with the `--memory-*` options if missing, and checks it against the node's pin, `--memory-dim` and `--memory-normalization`. A mismatch is kept as that partition's answer to its memory calls (`FailedPrecondition`), not a failed start: other partitions serve.
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
- A call whose namespaces are on more than one partition answers `Unsupported` until PR 2.
- `Memory.Watch` answers `Unsupported` until PR 2.
- A map with no memory placement: every memory call answers `Unsupported`.

**Callers:** `phdb serve --cluster`, and clients of `Memory` through any cluster node.

## PR 2 and PR 3 (numbers assigned, sections to come)
- `ForwardRequest.op` `recall_pool = 40`, `recall_bodies = 41`; `ForwardResponse.reply` `recall_pool = 23`, `recall_bodies = 24`; `RecallPoolRequest`, `RecallPoolResponse`, `RecallBodiesRequest`, `RecallBodiesResponse`, `ForwardMemoryWatchRequest` (fields from 1); `Cluster.ForwardMemoryWatch`.
- `pigeonhole-memory`: the opaque `RecallPool`, `RecallRanking` and `RecallBodies`, and `Recall::gather`, `expand_from` and `bodies` (exact signatures may settle in PR 2 as an amendment).

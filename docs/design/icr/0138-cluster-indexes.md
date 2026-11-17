# 0138: vector and text indexes on cluster nodes (`ClusterMap.indexes`, index forwards, `QueryStats`)

**Status:** Approved. The numbers are from the coordinator (`.coord/numbers.md`), for #1722, and so is the feature string `indexes`. The design (`.coord/ai1249-design-1722.md`) was reviewed by audit120. The owner decided T3 (text search is strict by default, with an `allow_partial` opt-in), and perf287 measured the fan-out (g108). This file grows with #1722's PRs:
- PR 1a (below): placements, and the calls that touch one partition;
- PR 1b: the vector search fan-out and `Verify`;
- PR 2a: `pigeonhole-text`'s `QueryStats`;
- PR 2b: the two-round text search;
- PR 3: `Vectors.BulkLoad`.

## PR 1a: index placements, and writes routed by id

**proto (additive):**
- **`cluster.proto`:**
  - `IndexPlacement { name = 1, oneof kind { CreateVectorIndexRequest vector = 2; CreateTextIndexRequest text = 3 }, follows = 4, splits = 5 (repeated SplitKey), partitions = 6, created = 7 }`;
  - `ClusterMap.indexes = 7`;
  - `ForwardRequest.op` arms `create_vector_index = 22`, `vector_insert = 23`, `vector_delete = 24`, `create_text_index = 27`, `text_put = 28` and `text_delete = 29`. Each carries the client's request, and each answers `empty`.
- **`vector.proto`:** `CreateVectorIndexRequest.follows = 8` (string) and `.placement = 9` (`TablePlacementRequest`).
- **The feature:** `NodeStatus.map_features` reports `indexes`.

**Why a whole index per partition:** an index keeps its rows in one table whose keys aren't its ids:
- an IVF index's centroids, lists and id map;
- a text index's postings, forward rows and corpus counters.

Every write touches shared rows. So each partition an index is placed on holds a whole index of its own over the ids in its range, made with the options the create sent. Co-location groups don't fit: a group can't be split, and reconcile creates tables from families, not indexes.

**Where an index's ids are:**
- **A text index follows its source table:** `Text.Put` writes the source cell and the postings in one transaction, so a document must be in its source row's partition.
- **A vector index** either follows a placed table (its ids are that table's row keys) or has split keys and partitions of its own, as a table's.

**What a valid map is (beside ICR 0117's and 0139's rules):**
- the name is non-empty, outside `__phdb_`, and unique among the map's tables, its indexes and the memory store's tables;
- the placement holds its create request, which names the same index. A text one's `source_table` is the table it follows;
- `follows` names a placed table and comes with no split keys or partitions of its own. Without it, the split keys are as a table's and each partition is listed. A text index always follows.

**What a change may do:**
- An index placement is fixed once written: its request, `follows`, split keys (compared as bytes), partitions and `created`.
- Dropping it is a schema change.
- A partition an index of its own ranges names isn't dropped.
- A table an index follows can't be dropped while the index is there, since the map would then name an unplaced table. The writer's `DropTable` answers `FailedPrecondition`, naming the indexes, as a single node does since #1916.

**Adding it (mixed versions):**
- A 1.2 node drops `ClusterMap.indexes` when it stores a map, so its copy would read as divergent (as for `memory`, 0139).
- So the writer adds the first index placement, in a node's first map or a later one, only once every node answers `Status` with `indexes` in `map_features`.
- Otherwise `SetMap` answers `FailedPrecondition`, naming the nodes that don't. `--force` skips the check.

**The calls:**
- **`Vectors.CreateIndex` and `Text.CreateIndex`** on a cluster are schema changes at the map's writer, made and forwarded as `Schema.CreateTable` is (D317 ticket 5).
  - A vector create needs exactly one of `follows` and `placement`. With `placement`, its group must be empty.
  - `if_missing` answers an index the map places as it is, without comparing its options, as a single node does.
  - A single node refuses `follows` and `placement` with `InvalidArgument`.
- **Reconcile** creates each index the map places on a partition, after that partition's tables (a text index reads its source there).
  - A vector index is created empty.
  - A text index is created unfilled (`pigeonhole_text::Index::create_unfilled`, ICR 0105's amendment, the coordinator's call), and the partition's background resume (#1756) fills it from the source rows, a document at a time under its write gate.
  - So a create over a large table never holds the installing thread. PR 2's search answers `Unavailable` ("still filling") for a partition whose index is still filling. It records it in `__phdb_cluster` with `created`, as a table is recorded, and drops it when the map drops it or places another incarnation. An unrecorded index table the map places is adopted.
- **`Schema.DropTable`** of an index's name removes its placement.
- **`Vectors.Insert` and `Delete`, `Text.Put` and `Delete`** route to the partition holding the id, by the table the index follows or by its own ranges. They're served there or forwarded, as a single-row call is.
  - **Not in the table routes:** an index's own table isn't routed by its ids. Data calls on it stay as before.
- **Still `Unsupported` until the later PRs:** `Vectors.Search` (use `Vectors.Nearest`), `Verify` and `BulkLoad`, and `Text.Search`, `Rebuild` and `Verify`. The search refusals are split by kind. (PR 1b serves `Vectors.Search` and both `Verify`s, below.)

**Backups, restores and the rest (the reviews, audit120 and simp724 on #1994):**
- **A backup** is refused, as for a table, when an index is dropped and created again or placed differently while it runs (`backup::moved`): copies either side would hold two incarnations, and a restore would make the older one's partitions again, empty.
- **`restore --map`** must keep the backup's indexes exactly, as it keeps its tables.
- **A partition restored alone:** indexes count as tables do. One created since comes back empty (a text one refills), one dropped since goes, and one whose range on the partition moved refuses the restore. An index that follows a table has that table's ranges.
- **The writer** refuses a text index whose `source_family` the followed table's placement doesn't list (`InvalidArgument`; a placement with no families can't be checked). It answers `Schema.CreateTable` of an index's name with `TableExists`, as a single node does.
- **Reconcile records an index before creating it.** A create that fails part way (the text create makes its table, then commits its config) drops the table and the record, and the next reconcile tries again. A recorded index whose table is missing (a crash between the two) is created. One that's there but doesn't open gets an error in `Status` ("drop the index and create it again"), which works because it's recorded. It's never made again automatically, since a full index that stopped opening would come back empty. An unrecorded table of an index's name (made by hand) is adopted only if it opens as that index.
- **Index creates answer `Empty`,** with no `unreconciled` list: a write right after can answer `TableNotFound` from a partition that hasn't taken the map yet, and the guide says to retry. `ListTables` and `DescribeTable` show the map's tables only, and `GetMap` lists the indexes.
- **Calls not yet served** (`Vectors.Verify`, `BulkLoad`; `Text.Rebuild`, `Verify`) answer `Unsupported` "isn't supported on a cluster yet", as the searches do.

**Tests:**
- **Through three nodes:**
  - an index of each kind created through any node is made on every partition of the table it follows;
  - writes through every node land in the partition of their id, a text document beside its source row;
  - the followed table's drop is refused, naming both indexes;
  - the indexes' drops reach every partition, and the table then drops;
  - a vector index of its own ranges routes by them;
  - creates with neither or both of `follows`/`placement`, or following an unplaced table, are refused;
  - the first index waits for every node to report `indexes` (a test seam hides it on one node).
- **Unit tests:** map validation and change rules, and index routes.

## PR 1b: the vector search fan-out, and `Verify`

**proto (additive):**
- **`cluster.proto`:**
  - `ForwardRequest.op` arms `vector_search = 25`, `vector_verify = 26` and `text_verify = 31`, each the client's request;
  - `ForwardResponse.reply` arms `vector_search = 13`, `vector_verify = 14` and `text_verify = 16`;
  - `ForwardRequest.partition = 50` (string, outside the oneof). This is the partition a fanned-out part of an index's search or check is for. Its request names only the index, and a node can own several of the index's partitions, as it can a table's. The owner serves that partition if it owns it and the map places the index on it, else `WrongPartition`. It's empty on every other call. PR 2's text parts use it too.

**The calls:**
- **`Vectors.Search`:**
  - **Where it goes:** to each partition whose range meets `id_prefix` (`Routes::index_partitions`, listing a partition once however many of the index's ranges it holds). Without a prefix, it goes to every partition of the index. Each part is the client's request as it is.
  - **The merge:** the entry node keeps the first `k` by `(distance, id)`. `stats` sums `lists_probed` and `lists`. `k` and the query are checked once, at the entry node.
  - **Not a single index's answer:** each partition trained its own centroids, so `nprobe` and `max_probe` are per partition, and recall and cost rise with the partitions. perf287's g108 measured this (the guide quotes it). With every list probed, the search is exact over every row.
  - **Ties at the cut** are each partition's (list, then id). So across partitions they're ordered by id only among the hits returned.
  - **Unreachable:** a part that fails fails the search (`PartitionUnavailable` for an unreachable node). There's no `allow_partial` in v1.
- **`Vectors.Verify` and `Text.Verify`:** sent to every partition of the index, with the counts summed. A part that fails fails the call, its message prefixed with the partition's id.
- **The parts:** each is served like a nearest search's (`fanout_part`). The entry node holds the in-flight budget: two default read pages for a search (the reply's charge is then fitted to its size), and a small share for a check. The owner serves its part with no admission of its own, its scope and lease still checked. At most `--fanout-forwards` parts run at once. Parts go to the owner, never to a replica copy (D328).
- **A forwarded index search or check without `partition`** isn't routed by its content (`unrouted`).
- **Still `Unsupported`:** `Text.Search` and `Text.Rebuild` (PR 2), and `Vectors.BulkLoad` (PR 3).

**Tests:**
- **Through three nodes:**
  - a search through the routing-only node returns both partitions' hits in `(distance, id)` order, and `stats.lists` is both partitions' list counts;
  - an `id_prefix` reaches only its partition;
  - `Vectors.Verify` and `Text.Verify` sum both partitions;
  - an index not in the map answers `TableNotFound`;
  - with 300 seeded vectors over an index's own two ranges and `nprobe` covering every list, the top 10 equal a brute-force L2 ranking;
  - with one partition's node stopped, a search whose prefix lies in the other partition answers, one without a prefix answers `PartitionUnavailable`, and a check fails naming the stopped partition.
- **Unit tests:**
  - `index_partitions` by prefix, with a partition holding two ranges listed once;
  - a named part routes only to a partition the index is on (`WrongPartition` otherwise; `InvalidArgument` for a call that doesn't name one);
  - the hit merge's order and cut.

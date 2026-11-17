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
- **Still `Unsupported` after PR 1a:** `Vectors.BulkLoad`, and `Text.Search` and `Rebuild`, with the search refusals split by kind. PR 1b serves `Vectors.Search` and both `Verify`s (below).

**Backups, restores and the rest (the reviews, audit120 and simp724 on #1994):**
- **A backup** is refused, as for a table, when an index is dropped and created again or placed differently while it runs (`backup::moved`): copies either side would hold two incarnations, and a restore would make the older one's partitions again, empty.
- **`restore --map`** must keep the backup's indexes exactly, as it keeps its tables.
- **A partition restored alone:** indexes count as tables do. One created since comes back empty (a text one refills), one dropped since goes, and one whose range on the partition moved refuses the restore. An index that follows a table has that table's ranges.
- **The writer** refuses a text index whose `source_family` the followed table's placement doesn't list (`InvalidArgument`; a placement with no families can't be checked). It answers `Schema.CreateTable` of an index's name with `TableExists`, as a single node does.
- **Reconcile records an index before creating it.** A create that fails part way (the text create makes its table, then commits its config) drops the table and the record, and the next reconcile tries again. A recorded index whose table is missing (a crash between the two) is created. One that's there but doesn't open gets an error in `Status` ("drop the index and create it again"), which works because it's recorded. It's never made again automatically, since a full index that stopped opening would come back empty. An unrecorded table of an index's name (made by hand) is adopted only if it opens as that index.
- **Index creates answer `Empty`,** with no `unreconciled` list: a write right after can answer `TableNotFound` from a partition that hasn't taken the map yet, and the guide says to retry. `ListTables` and `DescribeTable` show the map's tables only, and `GetMap` lists the indexes.
- **Calls not yet served** answer `Unsupported` "isn't supported on a cluster yet", as the searches do.

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
  - **Unreachable:** a part that fails fails the search (`PartitionUnavailable` for an unreachable node), its message prefixed with the partition's id. There's no `allow_partial` in v1.
- **`Vectors.Verify` and `Text.Verify`:** sent to every partition of the index, with the counts summed. A part that fails fails the call, its message prefixed with the partition's id. The sums are per-partition counts added up: a term in two partitions counts twice in `terms`, and `lists` is every partition's lists.
- **The parts:** each is served like a nearest search's (`fanout_part`). The entry node holds the in-flight budget: two default read pages for a search (the reply's charge is then fitted to its size), and a small share for a check. The owner serves its part with no admission of its own, its scope and lease still checked. At most `--fanout-forwards` parts run at once. Parts go to the owner, never to a replica copy (D328).
- **A forwarded index search or check without `partition`** isn't routed by its content (`unrouted`).
- **A verify part is a whole index's check on its owner,** run on the owner's blocking pool with no admission there, as a nearest search's part is. The entry node's admission and `--fanout-forwards` bound how many run.
- **A part is checked by the index's name, not its incarnation** (audit120's S2 on #2016). An index dropped and created again with another placement while one search runs could give an answer that mixes the two incarnations' partitions, or misses some of the new one's. The table fan-outs have the same shape. An index placement never changes in place, so this needs a drop and a create inside one search. Not guarded in v1.
- **Mixed builds of `main`:** a node built at PR 1a that owns a partition answers a 1b part with `InvalidArgument` ("a forward carries no call"), so the search fails rather than answering short. Neither PR has shipped, and 1.3.0 carries both.
- **Still `Unsupported`:** `Text.Search` and `Text.Rebuild` (PR 2), and `Vectors.BulkLoad` (PR 3).

**Tests:**
- **Through three nodes:**
  - a search through the routing-only node returns both partitions' hits in `(distance, id)` order, and `stats.lists` is both partitions' list counts;
  - an `id_prefix` reaches only its partition;
  - `Vectors.Verify` and `Text.Verify` sum both partitions;
  - an index not in the map answers `TableNotFound`;
  - with 300 seeded vectors over an index's own two ranges and `nprobe` covering every list, the top 10 equal a brute-force L2 ranking;
  - with one partition's node stopped, a search whose prefix lies in the other partition answers, one without a prefix answers `PartitionUnavailable`, and a check fails naming the stopped partition;
  - one node owning two partitions of an index: searches and checks through it and through both other nodes see each partition once (a mutation serving the wrong named partition fails it).
- **Not tested here:** `WrongPartition` on a part mid-search. It's `Reach::part`'s map pull and one retry, shared with multi-get and nearest parts.
- **Unit tests:**
  - `index_partitions` by prefix, with a partition holding two ranges listed once;
  - a named part routes only to a partition the index is on (`WrongPartition` otherwise; `InvalidArgument` for a call that doesn't name one);
  - the hit merge's order and cut.

## PR 2a: `pigeonhole-text`'s `QueryStats`

**No proto or engine changes.** This adds a library API (in `crates/text/public-api.txt`), with the single-index search moved onto it.

**The API (T2):**
- **`QueryStats`:** opaque (a tuple struct with a private field), `Debug` and `Clone`. It holds the document count and total length, each query term's and phrase term's `df`, each fuzzy term's kept expansions (term, distance, `df`, boost) and whether it was cut, and the fuzzy options it was read under (`max_expansions`, `prefix_length`).
- **`Search::query_stats(self) -> Result<QueryStats>`,** and `query_stats_async` under the `async` feature. This is round 1, read at the search's snapshot, or one taken for it, so each part's numbers come from one snapshot. **It's on `Search`, not `Index` as the design note had it:** round 1 needs the fuzzy options (`max_expansions` and `prefix_length` decide the expansions), and the search builder carries them. `k`, `id_prefix`, `highlight` and `stats` don't change it.
- **`Search::stats(self, QueryStats) -> Self`:** round 2. The search scores with the given numbers. Each fuzzy term expands to the given expansions, whatever this index's own walk would keep. The statistics must be for the same resolved terms, phrase terms and fuzzy terms (with the same clauses and edits), read with the same fuzzy options. Otherwise the search fails with `InvalidArgument`.
- **`QueryStats::merge(impl IntoIterator<Item = QueryStats>) -> Result<QueryStats>`:**
  - the counts are summed (saturating);
  - each fuzzy term's expansions are joined by term with their `df`s summed, then cut again to `max_expansions` by the search's own order: closest, then highest `df`, then bytewise;
  - `cut` is set when any part cut or the re-cut dropped a term;
  - `InvalidArgument` for no parts, or for parts of different queries or fuzzy options.
- **`QueryStats::cut(&self, clause: usize) -> bool`:** whether a fuzzy term of that clause was cut. **`QueryStats::documents(&self) -> u64`.**
- **`QueryStats::to_bytes` and `from_bytes`:**
  - **The layout:** version byte `1`, then little-endian fixed-width fields. Terms are sorted, so equal statistics give equal bytes.
  - **Refusals:** `from_bytes` refuses another version ("query statistics of version N; this build reads version 1"), bytes that end early or run on, and out-of-range fields. Each is an `InvalidArgument`.
  - **Allocation:** nothing is preallocated from a count read off the wire.
  - **Ranges (audit120's S1 on #2025):** `docs` and `len` must be ≥ 0, each `df` (a term's or an expansion's) must be in `0..=docs`, a boost must be in (0, 1], and no term or expansion may be listed twice. A `df` above the documents makes `idf` the log of a number at or under 0, so such bytes are refused rather than ranked with NaN. A fuzzy term with no clause is refused too.

**One code path:** `run` and `run_with_stats` (and their async forms) gather the index's statistics into the same struct `query_stats` returns, then rank with it. A search given statistics replaces the gather with them, and reads only its own postings counts: each term's, each phrase term's and each given expansion's `df` here.

**Two numbers per term, kept apart:**
- **To score:** the idf, a phrase's idf, the fuzzy blended `df` (the largest among the given expansions) and BM25's `n` and average length all come from the statistics.
- **To walk:** a term's walk, an expansion's walk, a phrase's lead choice and its join's point-read or walk choice use the postings here.
- **A term with postings neither here nor elsewhere,** a fuzzy expansion with none here, and a phrase with a term missing here all add no clause, as an absent term always did.
- On one index the two numbers are the same map (`Dfs::both` looks a term up once).

**Cost on one index:** the move adds no reads. The order is unchanged: `REPAIR`, the `S` counters, each term's `df`, the fuzzy walks, then the phrases. One read moved: an empty index used to return before reading `len` and the `df`s, and now reads them. The text callgrind shapes are the check, and the PR quotes them.

**Rounds at different snapshots (audit120's S2 on #2025):** a term first written between round 1 and round 2 has postings in round 2 and a merged `df` of 0, so it scores with `idf(0)`, the largest, and `n` leaves the new documents out. The ranking is consistent with itself, but it isn't one index's at either moment. PR 2b either pins each partition's round 2 to its round 1 snapshot or keeps this as the stated limit (the design note's "the rounds run at different snapshots"). `Search::stats`'s doc and the guide state it.

**Tests (`crates/text/tests/query_stats.rs`):**
- **A split matches one index:**
  - **The setup:** 300 seeded documents in two tenants' parts, skewed one to two, and the same documents in one index.
  - **The queries:** terms, phrases, a sloppy phrase, fuzzy terms and a mix, each with no prefix, a prefix in one part, and a prefix inside a range.
  - **The checks:** with every match, and with the top 10, the merged parts' scores equal the one index's at each place (relative 1e-9), and each hit's score is that document's score in the one index.
  - **Both layouts:** the same again with every index packed.
- **Own statistics:** an index searched with its own `query_stats` returns exactly `run_with_stats`'s hits and stats. This covers highlighting, an id prefix, and the async forms of both calls.
- **A term in one part only:** the other part, given the merged statistics, finds nothing and fails nothing. This holds for a term, a phrase and a fuzzy term.
- **audit120's cut shape:** `max_expansions` 2, where each part cuts the globally most frequent term. Each part's statistics, and the merge, flag `cut` on the fuzzy clause and not on the term clause. The search runs over the re-cut set.
- **The encoding:**
  - `to_bytes` round-trips, and the search gives the same answer with the decoded statistics;
  - a newer version is refused with its number;
  - every truncation, and a trailing byte, is refused;
  - out-of-range numbers are refused: a negative count, a `df` above the documents, a boost outside (0, 1], and a repeated expansion.
- **Refusals:** statistics of another term, of a missing fuzzy term, under other fuzzy options or with other edits; a merge of different queries; and a merge of nothing.
- **Mutations, all caught:**
  - a term or a phrase scored with this part's `df`;
  - the blended fuzzy `df` taken from this part's postings;
  - a merge that doesn't sum the expansions' `df`s;
  - a merge that doesn't re-cut.

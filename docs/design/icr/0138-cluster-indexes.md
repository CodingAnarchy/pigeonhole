# 0138: vector and text indexes on cluster nodes (`ClusterMap.indexes`, index forwards, `QueryStats`)

**Status:** Approved. The numbers are from the coordinator (`.coord/numbers.md`), for #1722, and so is the feature string `indexes`. The design (`.coord/ai1249-design-1722.md`) was reviewed by audit120. The owner decided T3 (text search is strict by default, with an `allow_partial` opt-in), and perf287 measured the fan-out (g108). This file grows with #1722's PRs:
- PR 1a (below): placements, and the calls that touch one partition;
- PR 1b: the vector search fan-out and `Verify`;
- PR 2a: `pigeonhole-text`'s `QueryStats`;
- PR 2b: the two-round text search;
- PR 2c: `Text.Rebuild`'s fan-out (split from PR 2, the coordinator's call on 2026-11-17);
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
  - So a create over a large table never holds the installing thread. PR 2b's search answers `Unavailable` ("still filling") for a partition whose index is still filling. It records it in `__phdb_cluster` with `created`, as a table is recorded, and drops it when the map drops it or places another incarnation. An unrecorded index table the map places is adopted.
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
  - **Expansions (PR 2b, audit120 on #2025):** a fuzzy term with more expansions than `max_expansions` is refused, which a part's or a merge's never has. That also bounds the check for an expansion listed twice.

**One code path:** `run` and `run_with_stats` (and their async forms) gather the index's statistics into the same struct `query_stats` returns, then rank with it. A search given statistics replaces the gather with them, and reads only its own postings counts: each term's, each phrase term's and each given expansion's `df` here.

**Two numbers per term, kept apart:**
- **To score:** the idf, a phrase's idf, the fuzzy blended `df` (the largest among the given expansions) and BM25's `n` and average length all come from the statistics.
- **To walk:** a term's walk, an expansion's walk, a phrase's lead choice and its join's point-read or walk choice use the postings here.
- **A term with postings neither here nor elsewhere,** a fuzzy expansion with none here, and a phrase with a term missing here all add no clause, as an absent term always did.
- On one index the two numbers are the same map (`Dfs::both` looks a term up once).

**Cost on one index:** the move adds no reads. The order is unchanged: `REPAIR`, the `S` counters, each term's `df`, the fuzzy walks, then the phrases. One read moved: an empty index used to return before reading `len` and the `df`s, and now reads them. The text callgrind shapes are the check, and the PR quotes them.

**Rounds at different snapshots (audit120's S2 on #2025):** a term first written between round 1 and round 2 has postings in round 2 and a merged `df` of 0, so it scores with `idf(0)`, the largest, and `n` leaves the new documents out. The ranking is consistent with itself, but it isn't one index's at either moment. PR 2b keeps this as the stated limit (below). `Search::stats`'s doc and the guide state it.

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

## PR 2b: the two-round text search

**proto (additive):**
- **`text.proto`:** `TextSearchRequest.allow_partial = 7` (bool); `TextSearchResponse.partial = 2` (bool), `.missing = 3` (repeated `MissingRange`) and `.cut_clauses = 4` (repeated uint32).
- **`cluster.proto`:**
  - `ForwardRequest.op` arms `text_stats = 32` (`TextStatsRequest { TextSearchRequest search = 1 }`) and `text_search = 33` (`TextSearchPartRequest { TextSearchRequest search = 1; bytes stats = 2 }`);
  - `ForwardResponse.reply` arms `text_stats = 17` (`TextStatsResponse { bytes stats = 1 }`, `QueryStats::to_bytes`) and `text_search = 18` (`TextSearchResponse`).
  - Both parts name their partition in `ForwardRequest.partition` (PR 1b's field).

**The search, at the node called (`indexfan.rs`):**
- **Checked once there:** `k` (1 to 1,000) and the query.
- **Round 1:** every partition of the index, whatever `id_prefix` says, answers `text_stats`: `Search::query_stats` at one snapshot of its own, as bytes. The entry node reads each with `from_bytes` and merges them with `QueryStats::merge`. Bytes it can't read, or that don't merge, fail the search with `FailedPrecondition` (the nodes' builds differ).
- **Round 2:** each partition `id_prefix` meets answers `text_search` with the merged bytes: its search with `Search::stats`, its top `k` by `(score desc, id asc)`. The entry node keeps the first `k` in that order, which is the library's (`maxscore.rs`, `Held`), so the top `k` is exact.
- **One partition:** an index placed on one partition skips round 1. Its part gets empty `stats` and reads its own, which are the whole index's.
- **The parts** carry the client's request less `allow_partial`. They're served like PR 1b's (`fanout_part`, owner only, no admission there). The entry node holds two default pages of the in-flight budget, and a round 1 part beyond the first takes a 64 KiB share.
- **`cut_clauses`:** the fuzzy clauses whose terms were cut, from `SearchStats::fuzzy` (a term two clauses yield is listed under the first). On a cluster, each round 2 part scores with the merged statistics, whose `cut` is set when any partition, or the merge's re-cut, cut. So each part names the same clauses, and the entry node takes their union. A single node fills it too: it was never on the wire before.

**Unreachable and filling partitions (T3: strict by default):**
- **Strict:** a part that fails fails the search, its message naming the partition. Round 1 needs every partition, so a search fails even when `id_prefix` lies in partitions that answered.
- **A partition whose index is still filling** (PR 1a's background fill: `rebuild_pending`) answers either round with `Unavailable` ("still filling"), since its statistics and hits would be short. On a cluster no client rebuild runs yet, so a pending rebuild is the fill. A client's `Text.Rebuild` sets `rebuild_pending` too, while its index stays whole, so with this check a strict search would fail during a healthy rebuild. PR 2c, which serves `Text.Rebuild` on a cluster, tells the two apart (the coordinator's call on audit120's nit): the check reads `fill_pending` there.
- **With `allow_partial`:** a part that fails with `PartitionUnavailable` or `Unavailable` is skipped, and `partial` is set: the answer is short in its statistics, its hits or both (audit120's should on #2029: a client checking `partial` alone never takes a short answer as whole).
  - Missing from round 1, the search scores with the merge of the parts that answered. That partition isn't asked for round 2, and its ranges that `id_prefix` meets go in `missing`.
  - Missing from round 2, its ranges that `id_prefix` meets go in `missing`.
  - If no partition answers round 1, the reply is empty, with `partial` and every reached range in `missing`.
  - `MissingRange.table` is the index's name, and its range is the partition's id range.

**The rounds at different snapshots (S2): stated, not pinned (the coordinator's call, 2026-11-17).**
- **Why not pinned:** pinning would keep a snapshot on each partition's node from round 1 to round 2, a registry of held snapshots with expiry, for a gap of one round trip.
- **What a write between the rounds does:** a document written between them ranks with round 1's statistics. A term only it holds scores with `df` 0, and `n` leaves it out. Every partition scores with the same numbers, so the ranking is consistent with itself, but it isn't one index's at either moment.
- **Where it's said:** the guide (serve.md) and `Search::stats`'s doc.
- **A pin** is a follow-up only if someone asks.

**Tests:**
- **Through three nodes** (`a_text_search_across_partitions_scores_as_one_index`):
  - **The setup:** a skewed corpus over `users`' two ranges, and the same documents in a reference index on a database of its own, made with the serve's options.
  - **The queries:** terms, three terms, a phrase, a fuzzy term with a term, a term in p2 only, and a fuzzy term matching it, each with no prefix and with a prefix in each range.
  - **The check:** each answers the reference's ids and scores (relative 1e-9), with `partial`, `missing` and `cut_clauses` empty.
  - **The round matters:** p1's own statistics score its top `alpha` document otherwise.
  - **A fuzzy term over 50 one-edit terms** split between the partitions answers `cut_clauses` `[0]`, as the reference flags the cut.
  - **An index on one partition** answers its own index's ids and scores.
  - **With p2's node stopped:** a search with a prefix in p1 answers `PartitionUnavailable`, naming p2. With `allow_partial` it answers p1's own hits and scores, with `partial` set and nothing missing. Without a prefix, it names p2's range `["m", end)` of `ut` in `missing`.
- **Unit:** a partition whose index is still filling answers `Unavailable` ("still filling"), and answers once filled.
- **Mutations, each caught by the cluster test:**
  - round 1 skipped (each part scoring with its own statistics);
  - round 1 sent only to the partitions `id_prefix` meets;
  - the merge breaking ties by id descending;
  - a partition missing round 1 not setting `partial`.

## PR 2c: `Text.Rebuild` across an index's partitions

**proto (additive, numbers from `.coord/numbers.md`):** `ForwardRequest.op` arm `TextIndexRequest text_rebuild = 30`, and `ForwardResponse.reply` arm `TextRebuildResponse text_rebuild = 15`. The part names its partition (`ForwardRequest.partition`).

**The fill, told from a client's rebuild (`pigeonhole-text`):**
- **Why:** a cluster's background fill (PR 1a) and a client's `Text.Rebuild` both keep a rebuild cursor (`rebuild_pending`). During the fill the index lacks source rows; during a client's rebuild every document is indexed, some with postings it hasn't reconciled yet, as on a single node, where searches go on throughout.
- **The mark:** `create_unfilled` writes an `unfilled` column in the index's `META` row, beside the cursor. A walk that reaches the source's end clears it: the fill's resume, a client's rebuild (which starts over, so one that began during the fill fills it), or a repair. A client's rebuild never sets it, and a stopped one leaves it as it was.
- **The API:** `Index::fill_pending()`, one point read, additive. The serve's search rounds answer `Unavailable` ("still filling") on `fill_pending`, not `rebuild_pending`.
- **Indexes from before the mark:** an index created unfilled by a pre-2c build of `main` has no mark, so its partition answers searches while its fill finishes. Only unreleased builds made such indexes (cluster indexes are new in 1.3.0).

**The rebuild, at the node called (`indexfan.rs`):**
- **Every partition of the index** gets the client's request as a part, served as that partition's own `Text.Rebuild`: admitted, gated and fenced as a write (`fanout_part` hands it to `call`). The counts are summed.
- **One node's partitions in turn:** a node has one rebuild slot, and a second client's rebuild is `Overloaded` at once. So the entry node groups the partitions by the node the map names, runs each node's in the map's order, and the nodes at once (within `--fanout-forwards`).
- **Strict:** a part that fails fails the call, naming its partition. The others' work stands; parts not started don't run, and a part already running on another node goes on (a write finishes if its caller goes away). There's no `allow_partial`: a rebuild is a write, and rebuilding again is safe.
- **A part past the deadline:** the forward is a write (`writes` in `Forwarder::send`), so a part on another node that outlasts the request's deadline answers `OutcomeUnknown`, not `PartitionUnavailable`. Its rebuild goes on there. A part served on the node called has the same deadline (a timeout around the part, simp724's S1 on #2041): its rebuild runs on a thread of its own, so it goes on too. The error names the node's partitions that hadn't started (simp724's N1). A single node has no such deadline for a write once it starts; a cluster's forwarded writes all have one (B3), and rebuild parts keep it (the coordinator's call, 2026-11-17: an unbounded forward would hold a client and a connection for minutes). The guide says how to tell when it's done: a retry's part answers `Overloaded` while the node still rebuilds, and `Text.Verify`'s `source_mismatch` reaches 0. A retry is safe.
- **A map change mid-call:** a partition moved to a node already rebuilding answers `Overloaded`, naming it.

**Also in this PR:**
- **FORMAT.md 14.1** lists the `unfilled` cell, and ICR 0105's `create_unfilled` amendment the mark (audit120's S1 on #2041).
- **`from_bytes`'s repeated-expansion check** goes through a set of the terms seen, so it's linear in the bytes (audit120's nit on #2029).
- **The PR 2b test's fill waits** take 30 s, for a loaded runner (audit120's caution on #2029).

**Tests:**
- **Through three nodes** (`a_text_rebuild_runs_on_every_partition_of_its_index`): a serves p1 and p3, b serves p2. Source rows are written around two indexes, `ut` (over p1 and p2) and `tt` (over p1 and p3, both a's).
  - A rebuild of `ut` through c answers 40 documents and 40 changed, each partition then holds its 20, and a strict search finds all 40.
  - A rebuild of `tt` through b runs a's two partitions in turn (40 and 40), and again answers 0 changed.
  - With b stopped, a rebuild of `ut` answers `PartitionUnavailable`, naming p2.
- **PR 1a's test:** a rebuild through b answers the summed counts, where it answered `Unsupported`.
- **The library** (`rebuild_stop.rs`): an unfilled index is pending a fill until its resume finishes, and when opened again; a filled index's stopped rebuild leaves a rebuild pending and no fill; a client's rebuild and a repair each fill an unfilled index.
- **Unit:** a filled index's partition answers a search's round while a client's rebuild is stopped part way.
- **Mutations, all caught:**
  - the check reading `rebuild_pending` again (the unit test);
  - every partition rebuilt at once, not grouped by node (the cluster test: `Overloaded` "already rebuilding");
  - a finished walk leaving the mark (the library tests).

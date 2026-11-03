# 0107: `pigeonhole-memory`, long-term agent memory on the public API

**Status:** Approved. The number came from the coordinator.
- The owner accepted D311 revision 2's 11 calls on 2026-11-02 (#1251 c6566064718).
- The embedding-model amendment was settled with cli692 the same day (#1251 c6578450948, c6578478485).

**Delivery:** T1 (#1335) comes in two PRs: the store here, then recall and `ReadStore`. Forgetting (T2), consolidation (T3), graph expansion (T4), and the Python and MCP surfaces (T6, T7) follow.

## T1, first PR: the store
`pigeonhole-memory` is on the public 1.x line (D295). Everything here is semver-checked (`crates/memory/public-api.txt`).

- **`EmbeddingModel::new(id, dim, element) -> Result<EmbeddingModel>`**, with `id()`, `dim()` and `element()`.
  - The `id` is non-empty, and the element is `F32`, `F16` or `BF16`. `I8` and `Bit` are refused.
  - The vector family is cosine.
- **`StoreOptions`** (`#[non_exhaustive]`, `Default`): `new()` and `embedding(EmbeddingModel)`.
- **`Store`:**
  - **Opening:**
    - `create(db, name, options)` makes the table `mem_{name}` and the text index `mem_{name}_text` (positions off).
    - `open(db, name, model_id)` fails with `Mismatch` for another model, and with `InvalidArgument` for a store without one.
    - `open_without_embeddings(db, name)`. On a pinned store this handle refuses `remember` and `supersede`, and allows `delete`, `link` and `mark_used`.
  - **Writes:**
    - `remember(ns, Memory) -> Result<Remembered>` and `remember_in(txn, ns, Memory)`;
    - `delete(&id) -> Result<bool>` and `delete_in(txn, &id)`;
    - `link(&from, &to, LinkType, f32)`, with the weight from 0 to 1 (anything else, NaN included, is `InvalidArgument`);
    - `supersede(&old, &new)`: a memory is superseded once. `old`'s `superseded_by` is read in the transaction, so two concurrent supersedes conflict. The same `new` again does nothing; another is `InvalidArgument` naming the current one;
    - `mark_used(&[MemoryId]) -> Result<Marked>`.
  - **Deleting a superseder** clears the superseded memory's `superseded_by` and `valid_to`, read in the transaction, so that memory is recalled again (simp724's G1 on #1362).
  - **Reads:** `get(&id) -> Result<Option<StoredMemory>>`, `verify() -> Result<StoreStats>` and `embedding_model()`.
- **`Memory`** is a `#[non_exhaustive]` builder:
  - `Memory::new(text)`, with `.embedding(Vec<f32>)`, `.kind(Kind)`, `.importance(f64)` (clamped to 0–1, default 0.5), `.source(id)`, `.session(s)`, `.link(MemoryId, LinkType, f32)` and `.pinned(bool)`.
  - A pinned store requires the embedding, at the model's dimension. A store without a model refuses one.
  - A memory needs text or an embedding.
  - Links stay within the memory's namespace.
- **The small types:**
  - **`Kind`** (`#[non_exhaustive]`, default `Episodic`): `Episodic`, `Semantic`, `Procedural`.
  - **`LinkType`:** a `u8` newtype with a private field (cli692 on #1362).
    - The crate's types are `DERIVED_FROM` = 1, `SUPERSEDES` = 2 and `SIMILAR_TO` = 3.
    - `LinkType::custom(n)` gives the application's, and refuses `n` below `FIRST_CUSTOM` (16) with `InvalidArgument`. `get()` returns the value.
    - Reads return whatever is stored, a reserved value this build doesn't name included.
  - **`Link`** (`#[non_exhaustive]`): `id`, `ty` and `weight`.
  - **`Source`** (`#[non_exhaustive]`): `name` and `at_us`.
  - **`Marked`** (`#[non_exhaustive]`): `marked`, and `missing` (the deleted ids, never resurrected).
  - **`Remembered`** (`#[non_exhaustive]`): `New(MemoryId)` or `Merged(MemoryId)`, with `id()`.
- **`StoredMemory`** (`#[non_exhaustive]`): `id`, `text`, `kind`, `importance`, `uses`, `last_used_us`, `session`, `pinned`, `superseded_by`, `sources: Vec<Source>`, `links: Vec<Link>`, `backlinks: Vec<Link>` and `embedding`.
- **`StoreStats`** (`#[non_exhaustive]`): `memories`, `no_content`, `stale_dedup`, `broken_links`, `missing_embeddings`, `dangling_superseded` (a `superseded_by` naming a memory that's gone), `text_index_inconsistent`, and `is_consistent()`.
- **`MemoryId`** (`Clone`, `Eq`, `Ord`, `Hash`) has `namespace()`, `created_us()`, `to_bytes()`/`from_bytes()`, and `Display`/`FromStr`. The forms are frozen:
  - **The byte form:** the namespace's length as `u16` little endian, the namespace, then the 16-byte `rid`.
  - **The text form:** RFC 4648 base32 of the byte form, lowercase, without padding.
  - **The `rid`:** `(u64::MAX − created_µs)` big-endian, then 8 random bytes.
- **`normalize(&str) -> String`** is dedup's normalisation, frozen: trim, then collapse every run of Unicode whitespace to one space.
- **`Error`** (`#[non_exhaustive]`): `Db`, `Text` (passed through, including `Busy`), `InvalidArgument`, `NotFound`, `Mismatch` and `Corruption`, with `code()`, `Display`, `std::error::Error` and `From` for both crates' errors. `NotFound`'s `code()` is `InvalidArgument`, since there's no general not-found code, and its doc says so.
- **`Memory::embedding`** into an `F16` or `BF16` model is rounded to the element when stored (to nearest, ties to even).

## T1, second PR: recall and reader processes
- **`Store::recall(&[&str]) -> Recall<'_>`** and **`ReadStore::recall`.** `Recall` is a `#[must_use]` builder:
  - **The query:** `.text(t)` and `.embedding(Vec<f32>)`. At least one is needed. The embedding is checked against the dimension, and is refused on a handle without the model.
  - **Limits:** `.k(n)` (default 10), `.candidates(n)` (per signal per namespace, default 50) and `.byte_budget(bytes)`.
  - **Reproducibility:** `.snapshot(&s)` and `.now_us(us)` (the time ages are measured from, default now).
  - **Scoring:** `.half_life(Duration)` (default 7 days), `.fusion(Fusion)` and `.weights(Weights)`.
  - **Filters:** `.include_superseded(bool)` and `.session(s)`.
  - **What a hit carries:** `.with_signals(bool)` and `.with_embeddings(bool)`.
  - **Running it:** `run() -> Result<Vec<RecallHit>>` and `run_with_stats() -> Result<(Vec<RecallHit>, RecallStats)>`.
- **`Fusion`** (`#[non_exhaustive]`, default `Rrf`): `Rrf` or `Linear`.
- **`Weights`** (`#[non_exhaustive]`, `Default` with every weight 1): `vector`, `keyword`, `recency`, `importance`, `uses`. Build it from `default()` and set its fields. T4 adds `graph` (default 0.5).
- **`RecallHit`** (`#[non_exhaustive]`): `id`, `text`, `kind`, `importance`, `created_us`, `superseded_by: Option<MemoryId>` (set only under `include_superseded`; cli692's S1 on #1374), `score`, `signals: Option<Signals>` and `embedding: Option<Vec<f32>>`.
- **The signal types** are `#[non_exhaustive]`:
  - `Signals` has `vector`, `keyword`, `recency`, `importance` and `uses`, each an `Option<Signal>`;
  - `Signal` has `rank: u32` and `value: f64`.
- **`RecallStats`** (`#[non_exhaustive]`): `candidates` and `truncated`.
- **`ReadStore`** comes from `Store::open_reader(&PigeonholeReader, name, model_id)`, which fails with `Mismatch` as `open` does, or from `Store::open_reader_without_embeddings`. It has `recall`, `get`, `verify` and `embedding_model`, and no writes.

**The rules as built:**
- **Candidates, per namespace,** counted after the filters (simp724's N1 on #1374). Each signal's list reads further down, with `n` doubling, until it holds `n` memories that pass the superseded and session filters, or its namespace is exhausted. So a filtered recall still gets each signal's best `n` eligible memories. The lists are:
  - the `n` nearest by the core's exact cosine search;
  - the `n` best from the text index with `id_prefix({ns}\0m)`;
  - the `n` newest by prefix scan.
- **The signals,** read in one multi-get at the snapshot:
  - similarity, as `1 − cosine distance`;
  - BM25, for keyword hits only;
  - recency, as `exp(−age / half_life)`;
  - importance;
  - use, as `ln(1 + uses) · exp(−since_last_use / half_life)`, only when used.
- **Ranks:** competition ranking, `1 + the number of strictly greater values`, so equal values share a rank. With a tie-break by id instead, every importance at its default would have handed the newest memory an edge.
- **RRF:** `Σ w / (60 + rank)`. Linear is `Σ w · min-max-normalised value`.
- **Order:** score descending, then id.
- **A hit's size:** its text, its id's byte form, and 16, plus 72 with signals (six of them since T4, 60 before) and `4 · dim` with the embedding. Hits are whole, and at least one is returned, as with the engine's budgets (D308): the first even if it alone is larger. `truncated` is set whenever the budget cut the list, that case included (simp724's N2 on #1374).
- **Names settled with cli692 on #1374:** `now_us(us)` (not `at`), and `uses` in `Signals` and `Weights` (not `use_`).

## T2: forgetting (#1336)
The shapes were settled with cli692 on #1336 (c6589132014).
- **`ForgetPolicy`** (`#[non_exhaustive]`, `Clone`, `PartialEq`, a `Default` builder):
  - `min_retention(f64)`;
  - `max_age(Kind, Duration)`, counted from `created_us`;
  - `half_life(Duration)` (7 days), `use_weight(f64)` (0.1), `max_per_pass(usize)` (1,000), `interval(Duration)` (1 h, at least 1 s: `InvalidArgument` below, so a forgetter never spins).
  - **Getters** for every field (#1380 review): `get_min_retention`, `get_max_age(Kind)`, `get_half_life`, `get_use_weight`, `get_max_per_pass`, `get_interval`.
  - **Retention** is `importance · exp(−since_last_use / half_life) + use_weight · ln(1 + uses)`. The use term doesn't decay, so heavy use protects a memory from retention (not from `max_age`).
- **The persisted policy:** `StoreOptions::forget(ForgetPolicy)` and `StoreOptions::episodic_ttl(Duration)` (the shorthand; after `forget` it adds to that policy, before it `forget` replaces it) record it in `G` (`meta:forget`, versioned). `Store::forget_policy()` reads it, and `Store::set_forget_policy(Option<ForgetPolicy>)` changes it (`None` turns forgetting off).
- **A pass:** `Store::forget() -> Forget<'_>`, with `.policy(&p)` (a one-off override), `.after(&MemoryId)` (resume) and `.now_us(u64)`, then `.run() -> Result<ForgetPass>`.
  - `InvalidArgument` with no persisted policy and no override.
  - `ForgetPass` (`#[non_exhaustive]`): `examined`, `deleted`, `protected`, and `resume: Option<MemoryId>`.
  - A pass deletes at most `max_per_pass`. It goes in id order, so a resumed pass doesn't see memories created behind its position until the next pass from the start.
- **`Store::forget_in(txn, &id, now_us) -> Result<Forgot>`** re-evaluates one memory, under the persisted policy, in the caller's transaction (M2).
  - **What it reads there:** `content:text`, `meta:kind`/`pinned`, `peak:importance`/`last_used`, `stats:uses`, and the `backlinks` row, plus each `DERIVED_FROM` source's `content:text` and kind.
  - If the memory is still due and not protected, it deletes it through `delete_in`.
  - `Forgot` (`#[non_exhaustive]`) is `Deleted`, `Protected`, `NotDue` or `Gone`.
  - **Protected** means pinned, or a source of a live semantic memory's `DERIVED_FROM` link. Protection applies to `max_age` too.
- **Opening:**
  - `OpenOptions` (`#[non_exhaustive]`) takes `.model(id)` or `.without_embeddings()` (exactly one), and `.own_forgetter()`.
  - `Store::open_with(db, name, OpenOptions)` and `Store::create_with(db, name, StoreOptions, OpenOptions)`.
  - `open` and `open_without_embeddings` are the short forms, and `create` opens with its own model.
- **The `Forgetter`:**
  - **The crate's thread:** without `own_forgetter`, the crate runs one on its own thread when the store has a policy at open, or once `set_forget_policy` sets one.
  - **The application's:** with `own_forgetter`, `Store::take_forgetter() -> Option<Forgetter>` hands it out once.
  - **Its API:** `run_once(budget) -> Result<bool>`, `wait(timeout)`, `closed()` and `set_wakeup`, as `pigeonhole-vector`'s `Rebalancer` has. It reads the persisted policy before each pass, and it's closed once every application handle drops. `run_once` is `#[must_use]`.
  - **A failed pass** returns its error and backs off: the next pass is due 5 s later, doubling with each failure in a row up to the policy's interval (#1380 review B1).
  - **`Pigeonhole::close`** doesn't wait for a pass: the deletion in flight fails with `Closed` and is dropped whole (#1380 review Q1).
- **`Store` is `Clone`** (cheap clones), so the forgetter can watch the application's handles.
- **A forgotten memory** is deleted as `delete` deletes, so the change feed (D310) shows deletes.
- **Nothing on `ReadStore`:** forgetting writes.

## T3: consolidation and near-duplicate dedup (#1337)
The shapes were settled on #1337: cli692 on the API (c6589818220), and simp724 on the commit, adding C1–C4.
- **`Consolidator`** (`Send + Sync`): `consolidate(&self, group: &[StoredMemory]) -> Result<Vec<Memory>, Box<dyn Error + Send + Sync>>`.
  - It's called outside every transaction, with no lock held.
  - A returned memory is written as `Semantic` unless it's `Procedural`, in the group's session. In a pinned store it needs its embedding.
- **A pass:** `Store::consolidate(ns, &dyn Consolidator) -> Consolidate<'_>`, with `.policy(&p)` and `.now_us(u64)`, then `.run() -> Result<ConsolidatePass>`.
  - On demand only: there's no thread, and the policy isn't stored, unlike forgetting.
  - T9 adds feed-driven runs.
- **`ConsolidatePolicy`** (`#[non_exhaustive]`, a builder, `PartialEq`, `get_*` getters):
  - `min_age(Duration)`, default 1 h;
  - `group_similarity(f32)`, default 0.8 cosine, from −1 to 1, and ignored in a store without a model;
  - `min_group(usize)`, default 2. 1 hands single memories over for rewriting; 0 is refused;
  - `max_group(usize)`, default 16, at least `min_group`;
  - `max_groups(usize)`, default 100;
  - `sources(SourcePolicy)`.
- **`SourcePolicy`** (`#[non_exhaustive]`): `Keep` (the default), `Delete` or `Supersede`.
- **Candidates:** episodic memories of `ns`, at least `min_age` old, not consolidated, superseded or pinned, oldest first.
  - **The scan's limit:** at most `max_groups · max_group · 4` candidates.
  - **Order:** ids, which sort newest first (a rid starts with `u64::MAX − created_us`). The scan starts at the age cutoff's key, so too-young memories aren't read.
  - **Grouping:** by session, then greedily. The first left is the seed; the others join while their cosine to the seed is at least `group_similarity`, up to `max_group`. Groups under `min_group` are left alone. A group's members follow its seed in id order.
  - **Without a model:** a session's memories, in creation order.
  - Groups go in their seeds' order, across sessions, and the consolidator gets each one newest first.
  - **Resuming (cli692's A1):** `ConsolidatePass::resume` and `Consolidate::after(&MemoryId)`.
    - When `max_groups` cuts groups off, `resume` is the last kept seed. Groups left out have later seeds, and members after those, so the next pass finds them.
    - When the scan hits its limit, `resume` is the last row scanned.
    - Otherwise it's `None`. Groups never span a resume position.
    - Memories that age past `min_age` behind a resume position wait for a pass from the start.
- **The commit:** one transaction per group, never retried. A group whose memories changed, or whose commit conflicts, counts as `skipped`.
  1. **The re-reads**, per memory: `content:text`, `meta:consolidated`, `meta:superseded_by`, `meta:pinned` (C4), and its `sources` row (C1). The union of the `sources` rows moves to the new memories, so a merge that adds a source conflicts.
  2. **The new memories** go through `remember`'s path, with exact dedup and no near dedup.
     - Each gets the group's provenance, with each source's earliest first-seen time.
     - Two returned memories of one normalised text are folded into one first, since the transaction reads at its snapshot.
  3. **A returned memory that merged into one of the group's own (C2)** is the result. It's marked consolidated, and the source policy applies to the other memories only.
  4. **`DERIVED_FROM` links** from every result to the other memories, except under `Delete`.
  5. **The source policy:**
     - `Keep` marks the other memories consolidated.
     - `Supersede` supersedes them by the first result, through a `supersede_in` shared with `supersede`.
     - `Delete` deletes them through `delete_in`, and removes any link a result made to one. A memory that a deleted one superseded is superseded by the first result instead, with its `valid_to` kept (C3).
  6. **With nothing returned:**
     - `Keep` and `Supersede` mark the memories consolidated;
     - `Delete` deletes them, except one that superseded another, which is marked consolidated instead (C3).
- **`ConsolidatePass`** (`#[non_exhaustive]`): `groups`, `committed`, `skipped`, `created`, `merged` and `sources`.
- **`Error::Consolidate { error, pass }`:** the consolidator's failures stop the pass, and the groups before it stay committed. Those are its `Err`, a returned memory the store refuses (cli692's A2), and a returned link to a memory that's gone (simp724's N1). A database error mid-pass doesn't carry the pass. Summaries skip near dedup (N2). Its `code()` is `MergeFailed`, the closest code: like a merge operator, the consolidator is application code that combines values (cli692 on #1386).
- **`StoredMemory::consolidated`**, and **`Weights::consolidated`** (default 0.5): a factor on a consolidated memory's importance signal in recall.
- **Near-duplicate dedup:**
  - **The setting:** `NearDedup::new(similarity).policy(DedupPolicy)` (`#[non_exhaustive]`, `Copy`, getters), with `similarity` from −1 to 1.
    - It's set by `StoreOptions::near_dedup`, needs a model, and is persisted in `G`.
    - `Store::near_dedup()` reads it, and `set_near_dedup(Option<NearDedup>)` changes it.
  - **`DedupPolicy`** (`#[non_exhaustive]`): `Skip`, `Merge` or `Link` (the default).
  - **The check:** `remember` checks before its transaction, when the memory has an embedding.
    - It looks at the nearest of the namespace's live, unsuperseded memories: up to 8 nearest are read, and superseded ones are skipped.
    - An exact duplicate still merges.
    - `Skip` returns the new **`Remembered::Similar(id)`** and writes nothing. `Remembered::id()` is then the match (cli692's A3).
    - `Merge` merges as an exact duplicate does.
    - `Link` writes the memory with a `SIMILAR_TO` link, weighted by the similarity.
    - Every policy reads the match's `content:text` and `superseded_by` in the transaction, `Skip` included (simp724's F1). A match that's gone or superseded is ignored.
    - The similarity must be above 0 and at most 1.
  - **`remember_in` doesn't check:** the check reads outside a transaction. `Store::near_duplicate(ns, &Memory) -> Result<Option<(MemoryId, f32)>>` runs it for a caller first, and is `InvalidArgument` without a setting.
  - **The race (S2):** two concurrent near duplicates can both insert, and consolidation folds them.

## T4: graph expansion in recall (#1338)
- **`Recall::expand(Expand)`:** none by default.
  - `Expand` is `#[non_exhaustive]`, `Copy` and plain data: `Expand::hops(n).max(m)`, with `pub hops: u32` and `pub max: usize` (default 20). A value rather than `expand(hops, max)`, so the two integers can't be swapped and seeds, directions or link types can come later (cli692 on #1338).
  - `Expand::MAX_HOPS` is 4, an associated const (cli692). Hops outside 1 to 4 are `InvalidArgument` from the run. `max(0)` gives no memory a graph signal, as `k(0)` returns no hits.
- **`Weights::graph`** (default 0.5) and **`Signals::graph`**. Neither has an effect, nor a value, without `expand`.
- **`RecallStats`** gains `expanded` (the memories given a graph signal) and `expansion_truncated` (cli692's name).

**The rules as built** (the calls on #1338, with simp724's O1 and O2):
- **Seeds:** the top 10 of the fused list before the expansion. Seed `i` (1-based) has the value `1 / i`.
- **A hop:** each memory of the previous hop has a one-hop `Traverse` over `links` and then one over `backlinks`, at the recall's snapshot. Traverse reads one family, and keeps destinations in key order, so one batched call would favour the newest seeds when cut. The weights come from the same memory's cells over the range the traversal returned. `write_link` puts the same `[type, weight]` in both directions.
- **The value:** a memory first reached at hop `h` takes the largest `value(q) × weight` over the hop-`h − 1` memories `q` it shares a link with, either way round. That's the best of its shortest paths, not of every path. Seeds get no graph signal; another candidate a seed reaches does.
- **Filters:** a reached memory passes the candidates' own test (`Recall::candidate`: content, superseded, session) or it is neither scored nor walked through.
- **`max`:** each hop keeps its best-valued memories, ties by id, while there's room (O1).
- **The budget:** with `byte_budget(B)`, the walk reads at most `B` bytes of nodes besides the hits' `B`, so a recall holds up to `2B`. That's D308's count, each node read's key plus 16 (memories already visited included). Each call gets what's left, and at least 1, so that only a node that's there can cut. A hop that doesn't fit whole is dropped, and the hops before it stay exact (O2).
- **Scoring:** reached memories are read in one multi-get per hop and scored like the other candidates (no keyword score unless they were in a keyword list). Then every signal is fused again over the larger set.

## The layout (frozen)
- **Memory rows:** `{ns}\0m{rid}`, with these families:
  - `content:text`, the text index's source;
  - `emb:v`, the embedding;
  - `meta`: `kind` (a 1-byte code), `session`, `pinned`, `consolidated` (T3: `1`, once a consolidation took it in), `superseded_by` (the newer memory's row key) and `valid_to` (µs);
  - `sources:{id}`, the first-seen µs;
  - `stats:uses`, a counter;
  - `peak:importance` (parts per million) and `peak:last_used` (µs), both `max` aggregates;
  - `links:{target row}` and `backlinks:{source row}`, each `[type u8][weight f32 LE]`.
- **Dedup rows:** `{ns}\0h{xxh3_128 of the normalised text, big-endian}`, holding `meta:rid` (the memory's row key).
- **`G`'s `meta`:** `layout` (1), `text_index`, `model_id`, `model_dim` and `model_element` (`0` for F32, `1` for F16, `2` for BF16).
  - `near_dedup` (T3) holds the setting: a version byte (1), the similarity (`f32` LE), and the policy (`0` Skip, `1` Merge, `2` Link).
  - `forget` (T2) holds the policy: a version byte (1), `min_retention` (a flag, then `f64`), `half_life` µs, `use_weight`, `max_per_pass`, `interval` µs (each 8 bytes, little endian), then a count and the `(kind u8, max_age µs)` pairs.

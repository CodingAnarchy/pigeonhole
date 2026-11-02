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
  - **Reproducibility:** `.snapshot(&s)` and `.at(us)` (the time ages are measured from, default now).
  - **Scoring:** `.half_life(Duration)` (default 7 days), `.fusion(Fusion)` and `.weights(Weights)`.
  - **Filters:** `.include_superseded(bool)` and `.session(s)`.
  - **What a hit carries:** `.with_signals(bool)` and `.with_embeddings(bool)`.
  - **Running it:** `run() -> Result<Vec<RecallHit>>` and `run_with_stats() -> Result<(Vec<RecallHit>, RecallStats)>`.
- **`Fusion`** (`#[non_exhaustive]`, default `Rrf`): `Rrf` or `Linear`.
- **`Weights`** (`#[non_exhaustive]`, `Default` with every weight 1): `vector`, `keyword`, `recency`, `importance`, `use_`. Build it from `default()` and set its fields. T4 adds `graph`.
- **`RecallHit`** (`#[non_exhaustive]`): `id`, `text`, `kind`, `importance`, `created_us`, `score`, `signals: Option<Signals>` and `embedding: Option<Vec<f32>>`.
- **The signal types** are `#[non_exhaustive]`:
  - `Signals` has `vector`, `keyword`, `recency`, `importance` and `use_`, each an `Option<Signal>`;
  - `Signal` has `rank: u32` and `value: f64`.
- **`RecallStats`** (`#[non_exhaustive]`): `candidates` and `truncated`.
- **`ReadStore`** comes from `Store::open_reader(&PigeonholeReader, name, model_id)`, which fails with `Mismatch` as `open` does, or from `Store::open_reader_without_embeddings`. It has `recall`, `get`, `verify` and `embedding_model`, and no writes.

**The rules as built:**
- **Candidates, per namespace:**
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
- **A hit's size:** its text, its id's byte form, and 16, plus 60 with signals and `4 · dim` with the embedding. Hits are whole, at least one is returned, and `truncated` is set when the budget, not `k`, cut the list.

## The layout (frozen)
- **Memory rows:** `{ns}\0m{rid}`, with these families:
  - `content:text`, the text index's source;
  - `emb:v`, the embedding;
  - `meta`: `kind` (a 1-byte code), `session`, `pinned`, `superseded_by` (the newer memory's row key) and `valid_to` (µs);
  - `sources:{id}`, the first-seen µs;
  - `stats:uses`, a counter;
  - `peak:importance` (parts per million) and `peak:last_used` (µs), both `max` aggregates;
  - `links:{target row}` and `backlinks:{source row}`, each `[type u8][weight f32 LE]`.
- **Dedup rows:** `{ns}\0h{xxh3_128 of the normalised text, big-endian}`, holding `meta:rid` (the memory's row key).
- **`G`'s `meta`:** `layout` (1), `text_index`, `model_id`, `model_dim` and `model_element` (`0` for F32, `1` for F16, `2` for BF16).

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
    - `link(&from, &to, LinkType, f32)`;
    - `supersede(&old, &new)`;
    - `mark_used(&[MemoryId]) -> Result<usize>`.
  - **Reads:** `get(&id) -> Result<Option<StoredMemory>>`, `verify() -> Result<StoreStats>` and `embedding_model()`.
- **`Memory`** is a `#[non_exhaustive]` builder:
  - `Memory::new(text)`, with `.embedding(Vec<f32>)`, `.kind(Kind)`, `.importance(f64)` (clamped to 0–1, default 0.5), `.source(id)`, `.session(s)`, `.link(MemoryId, LinkType, f32)` and `.pinned(bool)`.
  - A pinned store requires the embedding, at the model's dimension. A store without a model refuses one.
  - A memory needs text or an embedding.
  - Links stay within the memory's namespace.
- **The small types:**
  - **`Kind`** (`#[non_exhaustive]`, default `Episodic`): `Episodic`, `Semantic`, `Procedural`.
  - **`LinkType(pub u8)`:** values below 16 are the crate's (`DERIVED_FROM` = 1, `SUPERSEDES` = 2, `SIMILAR_TO` = 3); the application's are 16 and up.
  - **`Remembered`** (`#[non_exhaustive]`): `New(MemoryId)` or `Merged(MemoryId)`, with `id()`.
- **`StoredMemory`** (`#[non_exhaustive]`): `id`, `text`, `kind`, `importance`, `uses`, `last_used_us`, `session`, `pinned`, `superseded_by`, `sources`, `links`, `backlinks`, `embedding`.
- **`StoreStats`** (`#[non_exhaustive]`): `memories`, `no_content`, `stale_dedup`, `broken_links`, `missing_embeddings`, `text_index_inconsistent`, and `is_consistent()`.
- **`MemoryId`** (`Clone`, `Eq`, `Ord`, `Hash`) has `namespace()`, `created_us()`, `to_bytes()`/`from_bytes()`, and `Display`/`FromStr`. The forms are frozen:
  - **The byte form:** the namespace's length as `u16` little endian, the namespace, then the 16-byte `rid`.
  - **The text form:** RFC 4648 base32 of the byte form, lowercase, without padding.
  - **The `rid`:** `(u64::MAX − created_µs)` big-endian, then 8 random bytes.
- **`normalize(&str) -> String`** is dedup's normalisation, frozen: trim, then collapse every run of Unicode whitespace to one space.
- **`Error`** (`#[non_exhaustive]`): `Db`, `Text` (passed through, including `Busy`), `InvalidArgument`, `NotFound`, `Mismatch` and `Corruption`, with `code()`, `Display`, `std::error::Error` and `From` for both crates' errors.

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

# 0119: `pigeonhole-embed`, and `phdb mcp --embed-local`

**Status:** Approved. The coordinator assigned the number (2026-11-05), and the owner settled Q1–Q4 on #1345 the same day, all as recommended. The design is D318.

## The change
- **`pigeonhole-embed`, a new crate** on its own 0.N version line, starting at 0.1.0 (D318 Q1). PR 1:
  - **`LocalEmbedder`:** `open(Model, &LoadOptions)`, `embed_passages(&[&str]) -> Result<Vec<Vec<f32>>>`, `embed_query(&str) -> Result<Vec<f32>>`, `embedding_model(Element) -> Result<EmbeddingModel>` (the store's element; `I8` and `Bit` are `InvalidArgument`), and `model()`. It's `Send` and `Sync`, and `Debug`.
  - **`Model`** (`#[non_exhaustive]`): `BgeSmallEnV15`, with `id()`, `dim()`, `max_tokens()` and `revision()`.
  - **`LoadOptions`:** `new`, `dir`, `cache_dir`, `offline`, `threads`, and `on_thread_start(Fn() + Send + Sync)`, run first on each pool thread so a server can place them (cli692's A2).
  - **`Error`** (`#[non_exhaustive]`): `Io`, `Checksum`, `Missing`, `Download`, `Model`, `Tokenizer`, `Threads` and `InvalidArgument`, with `Result<T>`, and `code() -> ErrorCode`, as the other companion crates' errors have (A3):
    - `Io` for `Io`, `Download`, `Missing` and `Threads`;
    - `Corruption` for `Checksum`, `Model` and `Tokenizer`;
    - `InvalidArgument` for `InvalidArgument`.

    It has no `From` for candle's error, which would put candle in the public API.
  - **Re-exports:** `EmbeddingModel` (from `pigeonhole-memory`), and `Element` and `ErrorCode` (from `pigeonhole`).
  - **Features:** `download` (default), the network code. Without it, files come from a directory or the cache only.
  - **Environment variables:** `PIGEONHOLE_EMBED_CACHE` and `PIGEONHOLE_EMBED_OFFLINE`. candle's own `CANDLE_NUM_THREADS` and `CANDLE_CPU_MASK` (Linux) size and place its process-wide element-wise pool, which the crate documents but doesn't set.
  - **Review changes (cli692 on #1591):**
    - `embedding_model` takes the element (A1);
    - `open_dir` was dropped as a second spelling of `LoadOptions::dir` (N1);
    - `Model::max_tokens` lets a caller warn about truncation (N2).
- **`phdb mcp` and `phdb serve --memory`** (PR 2): `--embed-local`, `--embed-model-dir` and `--embed-offline` in the shared memory flags, behind a cargo feature `embed-local`, off by default (D318 Q4).
  - **The flags:** `--embed-local` conflicts with `--embed-url` and `--memory-model`, and the other two need it. Without the feature, `--embed-local` fails as unsupported (exit 6).
  - **Embedding:** a memory is embedded as a passage and a recall's text as a query. A new store is pinned to `Model::id()` with `Model::dim()`, so no probe embedding runs.
  - **In `phdb serve`:** the embedder's pool gets `min(free CPUs, 4)` threads, and `on_thread_start` enters the network side's placement (cli692's A2 on #1591). `--embed-timeout-ms` doesn't bound a local embed; `--memory-calls` still bounds how many run at once.
  - **candle's own pool:** it reads `CANDLE_NUM_THREADS`, `CANDLE_CPU_MASK` (Linux) and `RAYON_NUM_THREADS` from the environment (candle-core 0.11.1's `utils.rs`). With the shards pinned and `CANDLE_CPU_MASK` unset, startup prints one `phdb serve: --embed-local:` line on stderr. It gives the `export CANDLE_CPU_MASK=<the placement's CPUs> CANDLE_NUM_THREADS=<their count>` to restart with, since setting them in-process needs `unsafe`. `phdb mcp` has no placement, and prints nothing.

## Why
#1345: a local agent gets semantic recall without running or trusting an embedding service, from a model whose vectors match the ones `pigeonhole-memory`'s published recall numbers were measured with (D311 T5).

## Callers changed
None. Everything is new, and `pigeonhole-memory` is unchanged and doesn't depend on this crate.

## Versions
`embedding_model` returns `pigeonhole-memory`'s `EmbeddingModel`, and `Element` and `ErrorCode` are `pigeonhole`'s. A major release of either crate is therefore a new 0.N minor here, which the crate's own line allows (cli692's N3).

## Follow-ups
- PR 3: memrecall's `--embed-local` rerun on the AX102.
- Later, separately: the Python and Ruby bindings. No second model: the owner kept bge-small alone (D318, 2026-11-09).

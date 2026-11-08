# 0119: `pigeonhole-embed`, and `phdb mcp --embed-local`

**Status:** Approved. The coordinator assigned the number (2026-11-05), and the owner settled Q1–Q4 on #1345 the same day, all as recommended. The design is D318.

## The change
- **`pigeonhole-embed`, a new crate** on its own 0.N version line, starting at 0.1.0 (D318 Q1). PR 1:
  - **`LocalEmbedder`:** `open(Model, &LoadOptions)`, `open_dir(Model, dir)`, `embed_passages(&[&str]) -> Result<Vec<Vec<f32>>>`, `embed_query(&str) -> Result<Vec<f32>>`, `embedding_model() -> EmbeddingModel`, and `model()`. It's `Send` and `Sync`, and `Debug`.
  - **`Model`** (`#[non_exhaustive]`): `BgeSmallEnV15`, with `id()`, `dim()` and `revision()`.
  - **`LoadOptions`:** `new`, `dir`, `cache_dir`, `offline`, `threads`.
  - **`Error`** (`#[non_exhaustive]`): `Io`, `Checksum`, `Missing`, `Download`, `Model`, `Tokenizer` and `Threads`, with `Result<T>`. It has no `From` for candle's error, which would put candle in the public API.
  - **A re-export:** `EmbeddingModel`, from `pigeonhole-memory`.
  - **Features:** `download` (default), the network code. Without it, files come from a directory or the cache only.
  - **Environment variables:** `PIGEONHOLE_EMBED_CACHE` and `PIGEONHOLE_EMBED_OFFLINE`.
- **`phdb mcp`** (PR 2): `--embed-local`, `--embed-model-dir` and `--embed-offline`, behind a cargo feature `embed-local`, off by default (D318 Q4).

## Why
#1345: a local agent gets semantic recall without running or trusting an embedding service, from a model whose vectors match the ones `pigeonhole-memory`'s published recall numbers were measured with (D311 T5).

## Callers changed
None. Everything is new, and `pigeonhole-memory` is unchanged and doesn't depend on this crate.

## Follow-ups
- PR 2: `phdb mcp --embed-local`.
- PR 3: memrecall's `--embed-local` rerun on the AX102.
- Later, separately: MiniLM as a second `Model` variant (D318 Q2's fallback), and the Python and Ruby bindings.

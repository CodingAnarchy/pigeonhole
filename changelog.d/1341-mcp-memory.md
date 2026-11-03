### Added
- `phdb mcp --memory NAME`: agent memory tools over a `pigeonhole-memory` store (#1341, D311 T7, ICR 0107).
  - **The tools:** `memory_recall`, `memory_remember`, `memory_mark_used`, `memory_supersede`, `memory_link` and `memory_forget`.
  - **Guidance:** the server's instructions, the tool descriptions and a `memory://guide` resource tell the agent how to use memory well.
  - **Embedding model:** a store is pinned to one, through `--memory-model`/`--memory-dim` for client-supplied embeddings or `--embed-url`/`--embed-model`/`--embed-key-env` for server-side embedding through an OpenAI-compatible endpoint. A store can also have no model, for keyword recall. Memory text goes to the configured endpoint.
  - **`embed` feature:** the endpoint client is behind `phdb`'s default `embed` feature.

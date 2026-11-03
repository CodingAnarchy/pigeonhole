### Added
- Python: `pigeonhole.memory`, bindings for the agent memory store (#1340, D311 T6, ICR 0107).
  - **The store:** `Store` (create, open, remember and `remember_in`, delete, link, supersede, mark_used, get, verify, recall, forget and `forget_in`, near dedup, consolidate) and `ReadStore`.
  - **`Memory`:** written as keyword arguments.
  - **Recall:** `Weights` and the `fusion` choice.
  - **Consolidation:** a consolidator written in Python, whose exception stops the pass as `ConsolidateError` with the pass so far.
  - **Closing:** `Store.close()`, and `Database.close()` closes the stores opened on it first.

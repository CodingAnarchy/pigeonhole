# Pigeonhole (public crate) questions (Phase 2)

## Proposed decision: registered merge operators reach the engine; unregistered ones make the handle read-only (#43; supersedes D102)
D102 kept `Options::merge_operator` / `ReaderOptions::merge_operator` registrations without passing them on, so any family naming an operator other than `pigeonhole.i64_add` was refused.

**Interim behavior:**
- Both option types register every operator in `EngineOptions::merge_operators`. The engine already resolved registered names (`MergeKind::Registered`) for reads and compaction. Registering one under `pigeonhole.i64_add` replaces the built-in, as `MergeRegistry::register` does.
- A family naming an unregistered operator is still refused at creation with `UnknownMergeOperator`.
- **Read-only, as documented.** `allow_unregistered_merge_operators(true)` promised a read-only handle with compaction off. The engine skipped compaction of those families but still accepted writes, catalog changes and transactions. A writer opened over a family whose operator is unknown now refuses them with `ReadOnly`, as a reader process does. `flush`, `compact` (which skips those families) and `close` still work.
- The operator sees stored values (a tag byte, then the payload), as the built-in does. The guide says so.
- A merge onto a base stored in a blob file passes the operator the blob pointer, not the value. That is #235's fix (blob separation); custom operators inherit it when it lands.

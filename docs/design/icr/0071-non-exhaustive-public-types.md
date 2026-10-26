# 0071: the `pigeonhole` enums and output structs that can grow become `#[non_exhaustive]` (1.0 API freeze, part 1)

**Status:** Approved (coordinator, 2026-10-26; #941, the #695 gate; owner-accepted 1.0 triage). Breaking in 0.x, by design: after 1.0 the same change would need 2.0.

## Change

**`pigeonhole`:** `#[non_exhaustive]` on

| Type | Kind | Why it may grow in 1.x |
|---|---|---|
| `Value` | enum | new value tags (FORMAT §13 decodes unknown tags to `UnsupportedFeature`); #487's aggregations |
| `Condition` | enum | new conditional-mutation predicates |
| `ValueFilter` | enum | new filters |
| `Compaction` | enum | new compaction styles |
| `Priority` | enum | a finer cache priority |
| `Recovery` | enum (`repr(C)`) | finer recovery levels (D265) |
| `CommitInfo` | output struct | a commit timestamp or other outcomes |
| `CellEntry` | output struct | per-entry metadata |

The 22 other public enums and pub-field structs of `pigeonhole` already were. The re-exported internal types (`Durability`, `RowCacheStats`, `Failure`, `FailureCause`, `MergeError`, `MergeOperator`, `ShardStats`) are part 2 (D282: wrapped in `pigeonhole`-owned types).

**What users change:** a `match` on one of the six enums outside the crate needs a `_` arm; `CommitInfo` and `CellEntry` can no longer be built with a struct literal (they are outputs).

## Why

After 1.0, adding a variant or field to an exhaustive public type is a major release, and marking a type `#[non_exhaustive]` is itself breaking, so it can only happen before 1.0. Each type above can plausibly grow during 1.x.

## Callers

The workspace crates outside `pigeonhole` that matched exhaustively get wildcard arms:
- **`pigeonhole-arrow`, `batch.rs`:** an unknown value type exports as bytes (its raw bytes are in the values column already).
- **`pigeonhole-cli`:** `dump` writes `{"unknown_type": true}` (JSON) and type `unknown` (CSV); `schema` writes `unknown` for an unknown priority or compaction style.
- **Tests:** `arrow/tests/roundtrip.rs` and `pigeonhole/tests/corpus.rs` panic naming an unknown value type.

The cost: these crates lose the compiler's exhaustiveness check on the types; a new variant added without updating them lands in their "unknown" arm.

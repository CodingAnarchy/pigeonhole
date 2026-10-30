# 0094: `Aggregation`, `Family::aggregate`, `aggregate`/`aggregate_at` and `FamilyInfo::aggregation`

**Status:** Approved (number from the coordinator, 2026-10-30; owner accepted D294's design note and revision 1 on #487). The public API change is additive only: `cargo semver-checks -p pigeonhole --baseline-version 1.0.0` reports no semver update required. The internal crates change freely under D282.

## Change
- **pigeonhole:**
  - `Aggregation`, a `#[non_exhaustive]` enum: `Sum`, `Min` and `Max`. It's non-exhaustive so #487's HLL and #488's aggregations arrive in minor releases; a variant with parameters carries a params struct.
  - `Family::aggregate(Aggregation) -> Family`. `Family::counter()` is now `Family::aggregate(Aggregation::Sum)`, with the same options as before.
  - `aggregate(family, qualifier, i64)` and `aggregate_at(family, qualifier, ts, i64)` on `RowMutation`, and the same on `WriteBatch` and `Transaction` (with `table` and `row` first, like their `incr`). They write an input to an aggregate family's cell (the same operand `incr` writes).
  - `FamilyInfo::aggregation: Option<Aggregation>` is added to a `#[non_exhaustive]` struct. `counter: bool` keeps meaning Sum.
  - **Behaviour:** `incr` and `incr_at` in a min or max family fail with `InvalidArgument` ("write its inputs with aggregate"). In a counter family, `aggregate` is `incr`.
- **phdb** (D282's CLI surface):
  - schema JSON (`info`, `dump`, `load`) gains `"aggregation": "sum" | "min" | "max" | null`. `load` reads it, and falls back to `"counter"` for files dumped before it;
  - the shell gains `aggregate <table> <row> <family>:<qualifier> <value> [--ts US]`, and `create` takes `:min` and `:max`;
  - `info` gets an `aggregation` column.
- **Internal crates** (no promise):
  - **pigeonhole-format:** `FamilyKind::{Min, Max}`, `is_aggregate`, `required_incompat`, `INCOMPAT_AGGREGATES`; `Features::KNOWN_INCOMPAT` is bit 0.
  - **pigeonhole-pager:** `Pager::set_features` comes back (ICR 0089's constraints), and `OpenedPager::features` is added.
  - **pigeonhole-compaction:** `I64Min` and `I64Max`, registered as built-ins.
  - **pigeonhole-sim:** `ModelAggregation` and `aggregation_of_name`. `combine_counter_writes` takes the family's aggregation instead of a bool.

## Why
D294 (#487): Bigtable's other `i64` aggregations, fixed per family at creation.

## Callers changed
- **phdb:** the schema, shell and `info` changes above.
- **Tests:**
  - the engine and `pigeonhole` model harnesses (families named `min*`/`max*`);
  - the compaction tests' `combine_counter_writes` call;
  - the corpus (`aggregates` entry) and its compat-reader.

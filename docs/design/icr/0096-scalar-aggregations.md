# 0096: #488's aggregations, starting with the bitwise and `f64` kinds and `kind_params`

**Status:** Approved (number from the coordinator, 2026-10-31; owner accepted D298's design note and revision 1 on #488). Each of D298's PRs adds its section here. Under `cargo semver-checks -p pigeonhole --baseline-version 1.0.0`, `Aggregation` is new since 1.0.0, so dropping `Copy` is no break. The internal crates change freely under D282 and D295.

## PR 1: `kind_params`, bitwise and `f64` min/max
- **pigeonhole:**
  - `Aggregation` gains `BitOr`, `BitAnd`, `BitXor`, `MinF64` and `MaxF64`. It drops `Copy` (keeping `Clone`, `Eq` and `Hash`), so PR 3's variants can carry parameter structs. It is unreleased (1.1.0), so no released caller copies it.
  - `aggregate_u64(family, qualifier, u64)` and `aggregate_u64_at` write a bitwise family's input (stored as the `i64` with the same bits). `aggregate_f64(family, qualifier, f64)` and `aggregate_f64_at` write an `f64` family's input. They are on `RowMutation`, and on `WriteBatch` and `Transaction` with `table` and `row` first.
  - `put_u64` and `put_u64_at` on the same three types: a put of a `u64`'s bits as an `i64`, which sets a bitwise bucket.
  - `CellRef::as_u64()` and `Cell::as_u64()` (an `i64` value's bits), `CellRef::as_f64()` and `Cell::as_f64()`.
  - **Behaviour:**
    - `incr` in any aggregate family but a counter is refused with `InvalidArgument`, naming the method that writes its inputs.
    - An `f64` family takes only `F64` puts and inputs, never NaN: `InvalidArgument` otherwise.
    - A bitwise family takes only `I64` values.
  - **Engine:** a standard family naming any built-in operator but `pigeonhole.i64_add` (D41) is refused at creation, as are a family's `kind_params` tags its kind doesn't define (none so far).
- **phdb** (D282's CLI surface):
  - `"aggregation": "bit_or" | "bit_and" | "bit_xor" | "min_f64" | "max_f64"` in the schema JSON;
  - the shell's `create` takes the same suffixes;
  - `aggregate` parses an `f64` in an `f64` family and a decimal `u64` in a bitwise one.
- **Internal crates** (no promise):
  - **pigeonhole-format:** `FamilyKind::{BitOr, BitAnd, BitXor, MinF64, MaxF64}` (5 to 9, under `INCOMPAT_AGGREGATES`); `FamilyOptions::kind_params` and its setter; the `kind_params` module (`encode`, `parse`, `must_understand`, `FIRST_SKIPPABLE`; FORMAT §9.2.1).
  - **pigeonhole-compaction:** `Bitwise` (`OR`, `AND`, `XOR`) and `F64Extreme` (`MIN`, `MAX`), registered as built-ins.
  - **pigeonhole-sim:** `ModelAggregation::{BitOr, BitAnd, BitXor, MinF64, MaxF64}` and `ModelAggregation::is_f64`.

## Why
D298 (#488): the owner's minimum set of further aggregations for 1.1.0, and the parameter field that #488's later kinds and #490's vector kind share.

## Callers changed
- **phdb:** the schema and shell changes above.
- **Tests:** the engine and `pigeonhole` model harnesses (families named `bxor*` and `fmax*`), and the corpus's `aggregates` entry.

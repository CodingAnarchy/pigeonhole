# 0096: #488's aggregations, starting with the bitwise and `f64` kinds and `kind_params`

**Status:** Approved (number from the coordinator, 2026-10-31; owner accepted D298's design note and revision 1 on #488). Each of D298's PRs adds its section here. Under `cargo semver-checks -p pigeonhole --baseline-version 1.0.0`, `Aggregation` is new since 1.0.0, so dropping `Copy` is no break. The internal crates change freely under D282 and D295.

## PR 1: `kind_params`, bitwise and `f64` min/max
- **pigeonhole:**
  - `Aggregation` gains `BitOr`, `BitAnd`, `BitXor`, `MinF64` and `MaxF64`. It drops `Copy` (keeping `Clone`, `Eq` and `Hash`), so PR 3's variants can carry parameter structs (`Histogram(HistogramParams)` holds bucket bounds). So, unlike other small public enums, `Aggregation` is not `Copy`: clone it. It is unreleased (1.1.0), so no released caller copies it.
  - `aggregate_u64(family, qualifier, u64)` and `aggregate_u64_at` write a bitwise family's input (stored as the `i64` with the same bits). `aggregate_f64(family, qualifier, f64)` and `aggregate_f64_at` write an `f64` family's input. They are on `RowMutation`, and on `WriteBatch` and `Transaction` with `table` and `row` first.
  - `put_u64` and `put_u64_at` on the same three types: a put of a `u64`'s bits as an `i64`, which sets a bitwise bucket.
  - `CellRef::as_u64()` and `Cell::as_u64()` (an `i64` value's bits), `CellRef::as_f64()` and `Cell::as_f64()`.
  - **Behaviour:**
    - A typed input of the wrong type for the family is refused with `InvalidArgument` when the mutation is built (one catalog lookup), naming the method that writes its inputs: `incr` outside a counter, `aggregate` in an `f64` family, `aggregate_u64` outside a bitwise family, `aggregate_f64` outside an `f64` family.
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
- **Tests:** the engine and `pigeonhole` model harnesses (new `scalar_counters` targets, families named `bor`, `band`, `bxor*`, `fmin`, `fmax*`), and the corpus's `aggregates` entry.

## PR 2: exact `f64` sum and mean/variance
- **pigeonhole:**
  - `Aggregation::SumF64` and `Aggregation::MeanVar` (new variants of the non-exhaustive enum). Their inputs are written with `aggregate_f64` / `aggregate_f64_at`, which already exist.
  - `CellRef::sum_f64()` and `Cell::sum_f64() -> Option<f64>` read an exact-sum state as the correctly rounded sum. The value's shape decides, as for `hll_estimate`: a `Bytes` value that is a well-formed state. `as_f64` keeps reading `F64` values only (perf287 on #1220), so no standard family's bytes read as a number.
  - `CellRef::mean_var()` and `Cell::mean_var() -> Option<MeanVar>`, and the new `pigeonhole::MeanVar`, with accessors `count`, `mean`, `variance` and `sample_variance`. It is owned by `pigeonhole`, not re-exported from the format crate.
  - **Behaviour:**
    - Inputs and `put_f64` values must be finite (NaN and ±∞: `InvalidArgument`).
    - `put` takes a well-formed state of the kind.
    - `incr` and `aggregate(i64)` are refused, naming `aggregate_f64`.
    - A `blob_threshold` below 274 (sum) or 821 (mean/variance) is refused at creation.
  - **Engine:** puts in aggregate families are never separated at commit time, whatever the inline limit.
- **phdb:** `"aggregation": "sum_f64" | "mean_var"`; the shell's `create` suffixes; `aggregate` parses an `f64` there.
- **Internal crates:**
  - **pigeonhole-format:** `exact` (sum and mean/variance states: add, merge, validate, read; `MeanVar`, `StateError`); `FamilyKind::{SumF64, MeanVar}`.
  - **pigeonhole-compaction:** `ExactF64` (`SUM`, `MEAN_VAR`).
  - **pigeonhole-sim:** `exact_state`, `exact_state_valid`, `exact_merge`; `ModelAggregation::{SumF64, MeanVar}`, `is_state`, `state_of`, `state_valid`.

## PR 3: histograms and quantiles
- **pigeonhole:**
  - `Aggregation::Histogram(HistogramParams)` and `Aggregation::Quantiles(QuantileParams)`, the first variants with parameters.
  - `HistogramParams::new(impl Into<Vec<f64>>) -> Result<Self>` (finite, strictly increasing, 1 to 1023 bounds, never `-0.0`), `bounds()` and `bucket_of(x)`.
  - `QuantileParams`: `Default` (7 mantissa bits, 2048 buckets), the builders `mantissa_bits(p)` and `max_buckets(n)`, and the getters `get_mantissa_bits()` and `get_max_buckets()`. Out-of-range values are refused when the family is created.
  - Both are `Clone`, `Eq` and `Hash`, owned by `pigeonhole`.
  - `CellRef::histogram_counts() -> Option<Vec<u64>>`, `quantile(q) -> Option<f64>` and `quantiles_count() -> Option<u64>`, and the same on `Cell`.
  - `FamilyInfo::aggregation` carries the parameters back.
  - **Behaviour:**
    - Inputs come from `aggregate_f64`, and `put_f64` sets a cell. Histograms refuse NaN; quantiles refuse any non-finite input.
    - `incr` and `aggregate(i64)` are refused.
    - These families' values are never separated into blob files, whatever their `blob_threshold`.
- **phdb:**
  - `"aggregation": "histogram" | "quantiles"` and `"aggregation_params"` (`{"bounds": […]}`, `{"mantissa_bits", "max_buckets"}`) in the schema JSON, which `load` reads back;
  - the shell's `create … name:histogram=b0,b1,…` and `name:quantiles[=p,n]`;
  - a `\schema` column for the parameters.
- **Internal crates:**
  - **pigeonhole-format:** `buckets` (histogram and quantile states and parameters); `FamilyKind::{Histogram, Quantiles}`; `FamilyOptions::separation_threshold`.
  - **pigeonhole-compaction:** `Buckets` (`histogram(bounds)`, `quantiles(params)`).
  - **pigeonhole-sim:** `bucket_state`, `bucket_state_valid`, `MODEL_HISTOGRAM_BOUNDS`, `MODEL_QUANTILE_BITS`, `MODEL_QUANTILE_MAX`, and `ModelAggregation::{Histogram, Quantiles, finite_inputs}`.

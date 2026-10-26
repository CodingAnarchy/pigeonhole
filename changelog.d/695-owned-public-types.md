### Changed
- **Breaking (before the 1.0 API freeze, D282):** `pigeonhole` no longer re-exports internal crates' types; it defines its own.
  - **Own types:** `Durability`, `Failure`, `FailureCause`, `RowCacheStats`, `MergeError` and the `MergeOperator` trait are now `pigeonhole`'s own (same names, variants and fields), and all but the trait are `#[non_exhaustive]`.
  - **What callers change:** a `match` on `Durability` or `FailureCause` needs a `_` arm. A merge operator builds its error with `MergeError::new(operator, message)`. Code that passed these values to or from `pigeonhole-engine` or `pigeonhole-format` directly must convert.
  - **Conversions removed:** the public `From` conversions from engine types (`CommitInfo`, `EngineMetrics`, `CheckReport`, `FamilyInfo`, `DatabaseInfo`) are gone.
  - **Exact pins:** `pigeonhole` pins its internal crates exactly (`=`).
- The guide's [versions and compatibility](docs/guide/concepts.md#versions-and-compatibility) section and the READMEs state the semver policy from 1.0.0:
  - `pigeonhole`, `pigeonhole-arrow` and `phdb`'s interface follow semver;
  - the internal `pigeonhole-*` crates make no stability promise.

# 0073: `pigeonhole` defines its own public types instead of re-exporting internal crates' (1.0 API freeze, part 2)

**Status:** Approved (coordinator, 2026-10-26; #942; D282, D283). Breaking in 0.x, by design.

## Change

**`pigeonhole`:**
- **Own types, replacing re-exports.** `Durability` (was `pigeonhole_format::Durability`), `Failure`, `FailureCause`, `RowCacheStats`, `ShardStats` (still `#[doc(hidden)]`), `MergeError` and the `MergeOperator` trait (were `pigeonhole_engine`'s).
  - Same names, variants, fields, discriminants and `repr`s.
  - `#[non_exhaustive]`, except the trait.
  - `MergeError::new(operator, message)` is new, because operators build it and the struct is non-exhaustive.
  - An engine `FailureCause` this crate doesn't know maps to `Internal`.
- **`RecoveryStats::last_failure`** is now `Option<pigeonhole::Failure>`.
- **Public `From` impls removed:** `From<pigeonhole_engine::{CommitInfo, Metrics, CheckReport, &FamilyInfo, DbInfo}>` for `CommitInfo`, `EngineMetrics`, `CheckReport`, `FamilyInfo` and `DatabaseInfo` become crate-private `from_engine` functions.
- **`From<pigeonhole_engine::Error> for Error` stays,** `#[doc(hidden)]` and outside the promise, because the crate's own `?` uses it.
- **Registered merge operators** reach the engine through a private adapter (`EngineMergeOperator`).

**Workspace:** the 12 internal crates are pinned `=0.4.0`.

**Docs:**
- the guide's "Versions and compatibility" section, including the MSRV policy (D283);
- the root README's crate table, with a stability column;
- `pigeonhole`'s README;
- each internal crate's README.

## Why

D282, the owner's choice of option (a) in the 1.0 triage: `pigeonhole` follows semver from 1.0, and the internal crates make no stability promise. So an internal crate may take a new major in any release. If `pigeonhole`'s public API named an internal type, through a re-export, a public field or a public trait impl, that major would break `pigeonhole` 1.x.

D283: a 1.x minor release may raise `rust-version`, a patch never does, and the changelog calls out every raise.

## Callers

- **`pigeonhole` itself:** every engine call that takes or returns a durability, failure, stats or info value converts at the call (`Durability::engine`, `from_engine`).
- **`pigeonhole/tests/model.rs`:** the simulator's model carries the engine's `Durability`; a local `public()` converts at the public API.
- **`pigeonhole-arrow`, `pigeonhole-cli`, `pigeonhole-bench`:** no change. They use only `pigeonhole`'s names.

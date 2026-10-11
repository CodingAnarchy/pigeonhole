# 0030: `ScanFilter::compile` → `CompiledFilter`

**Status:** Approved (coordinator, 2026-10-11; #29).

## Change

`pigeonhole-format` (`scan`), additive:

```rust
impl ScanFilter {
    /// This filter compiled for per-entry checks.
    pub fn compile(&self) -> CompiledFilter;
    /// The same, consuming the filter: a move, nothing computed or allocated.
    pub fn into_compiled(self) -> CompiledFilter;
}

/// A ScanFilter compiled for a cursor's per-entry checks.
pub struct CompiledFilter { /* private */ }

impl CompiledFilter {
    pub fn is_all(&self) -> bool;
    pub fn admits(&self, key: &[u8]) -> bool;
    /// What to do with this entry; on `Seek`, the hint is written to `out`.
    pub fn check(&self, key: &[u8], out: &mut Vec<u8>) -> Verdict;
}

pub enum Verdict { Admit, Next, Seek, SkipRow }
```

`ScanFilter`, `admits` and `next_admissible` are unchanged.

## Why

A filtered scan evaluates the filter on every entry it visits (D22).
- `SstIter` and `FilteredCursor` called `admits` and then `next_admissible`, so each excluded entry decoded its key twice.
- Both compared the qualifier with the raw bounds through an unescaping iterator, byte by byte.

On a scan keeping 1 column of 8 (`scanrate` `scan-narrow-q1`), the filter took 29% of all instructions (#29).

`CompiledFilter` fixes both:
- `check` compares the key's escaped qualifier bytes with the bounds directly. Escaping preserves order and prefixes (`0x00` becomes `00 FF`, which sorts below every other byte), and a bound without a `0x00` is its own escaped form. A bound with one (rare) goes through the old unescaping comparison.
- `check` decodes the key once for both the admission and the seek hint.

`ScanFilter`'s fields are public, so a compiled form can't be cached inside it: a caller may change a field after building it. Cursors therefore compile it at construction, by value (`into_compiled`): they already receive their own copy of the filter. Compiling is a move. `CompiledFilter` is a newtype over `ScanFilter`, and nothing is precomputed, so a cursor is no larger than before and builds as it did. Every source of every read compiles its filter, most of them admitting everything. Versions that escaped the bounds at construction cost unfiltered point gets 1–3% in the CI shapes: allocation, a larger cursor, or lost inlining of the constructor.

## Semantics

`check(key)` decides exactly as the two `ScanFilter` calls did together:

| Verdict | When |
|---|---|
| `Admit` | `admits(key)` holds. This includes malformed keys and family markers, as before. |
| `SkipRow` | `next_admissible` returns false: nothing later in the row can be admitted. |
| `Seek` | `next_admissible`'s hint sorts after `key`. The hint is written to `out`, byte-identical. |
| `Next` | otherwise: an admitted column whose put fails the time range, or a hint not after `key`. |

`CompiledFilter::admits` equals `ScanFilter::admits`.

## Callers

- `pigeonhole-sst`: `SstIter` (its `settle`).
- `pigeonhole-compaction`: `FilteredCursor` (memtable sources and the engine's in-memory sources).

## Tests

- **`pigeonhole-format` `tests/scan_filter.rs`:** a property test against the two `ScanFilter` calls. It covers every filter form (prefix, ranges with every bound kind, a time range) on valid, marker, one-byte-corrupted and random keys, and checks both the verdict and the hint bytes. The `Excluded` lower bound compared with `<` instead of `<=` is caught.
- **`pigeonhole-sst` `pushdown_equals_filter_after`** (D22), and the `FilteredCursor` tests.

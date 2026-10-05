# Scans and filters

> **Status: API frozen; implementation in progress (Phase 1).** Semantics come from the spec and decision D22. `Scan::stream` (async) is Phase 3.

Both `Table::row(key)` (a `RowRead`) and `Table::scan*` (a `Scan`) are builders. Nothing happens until you call `.read()` or `.iter()`. They share most of their methods.

| Method | `RowRead` | `Scan` | Effect |
|---|---|---|---|
| `families(["a", "b"])` / `family("a")` | ✓ | ✓ | Project to these families (default: all), in this order. |
| `qualifier_prefix(p)` | ✓ | ✓ | Only qualifiers starting with `p`. |
| `qualifier_range(range)` | ✓ | ✓ | Only qualifiers within a range. |
| `qualifier_bounds(start, end)` | ✓ | ✓ | Same with explicit `Bound`s. |
| `latest()` | ✓ | ✓ | Newest version of each column (default). |
| `versions(n)` | ✓ | ✓ | Up to `n` versions per column (0: all retained). |
| `time_range(a..b)` | ✓ | ✓ | Only versions with timestamp in `[a, b)` (microseconds). |
| `column_limit(n)` | ✓ |  | At most `n` columns per family. |
| `columns_per_row(n)` |  | ✓ | At most `n` columns per family per row; the rest of the row is skipped without decoding. |
| `value_filter(f)` | ✓ | ✓ | Only cells whose value matches. |
| `limit(n)` |  | ✓ | Stop after `n` rows. |
| `snapshot(&snap)` | ✓ | ✓ | Read as of a snapshot instead of now. |

## Row scans
```rust,ignore
use std::ops::Bound;

// Every row starting with a prefix.
let it = events.scan_prefix(b"user:42:").iter()?;

// A half-open key range. Both ends must be the same type: use slices.
let it = events.scan(&b"user:42:"[..]..&b"user:43:"[..]).iter()?;

// Explicit bounds (what a C ABI exports).
let it = events.scan_bounds(Bound::Included(b"a"), Bound::Unbounded).iter()?;
```
Rows come back in **byte-wise key order**; a scan walks tablets in order, so results are ordered without a merge step. A prefix scan of `user:42:` returns exactly the keys that begin with those bytes, and nothing else, so choose separators with that in mind (`user:4` also matches `user:42:`).

Consume rows as owned values (`Iterator<Item = Result<Row>>`; cheap, values stay pinned rather than copied) or zero-copy:

```rust,ignore
let mut it = events.scan_prefix(b"user:42:").iter()?;
while let Some(row) = it.next_ref()? {
    // RowRef<'_>: valid until the next next_ref() call
    let v = row.get("ev", b"2026-10-05T12");
}
```

## Family projection
```rust,ignore
events.scan_prefix(b"user:42:").family("ev").iter()?;
events.scan_prefix(b"user:42:").families(["ev", "meta"]).iter()?;
```
Each family is its own physical tree, so **unprojected families are never read at all**. Projection is the single biggest scan optimization: keep large payloads in their own family and leave them out of scans that do not need them.

## Qualifier selection
```rust,ignore
// Prefix.
scan.qualifier_prefix(b"edge:")
// Range: [2026-10-05T00, 2026-10-06T00)
scan.qualifier_range(&b"2026-10-05T00"[..]..&b"2026-10-06T00"[..])
// Explicit bounds.
scan.qualifier_bounds(Bound::Included(b"a"), Bound::Excluded(b"m"))
```
Qualifiers are sorted bytes within a family, so a prefix or range is a contiguous read inside each row, not a filter over every column.

## Versions and time ranges
By default you get the newest version of each column. Ask for more:

```rust,ignore
// Last 10 versions of each column in the row.
let row = t.row(b"sensor:7").family("temp").versions(10).read()?;

// Versions in a window. Timestamps are u64 microseconds since the Unix epoch.
let it = t.scan_prefix(b"sensor:").time_range(t0_us..t1_us).versions(0).iter()?;
```
- Within a column, versions are ordered **newest first**.
- `versions(0)` returns every retained version. What is retained depends on the family's `max_versions` and `ttl`.
- Do not rely on reading versions older than the family's TTL; expiry is applied by timestamp.
- Counters written with `incr` are resolved at read time; see the pushdown rules below before combining them with `time_range`.

## Limiting work
- `limit(n)` stops after `n` rows. Use it for pagination: remember the last key and resume with `scan_bounds(Bound::Excluded(last_key), ...)`.
- `columns_per_row(n)` caps columns per family per row and skips the rest of each row without decoding. Use it to read "the first few attributes" of very wide rows.

```rust,ignore
let page = t
    .scan_bounds(Bound::Excluded(last_key.as_slice()), Bound::Unbounded)
    .family("meta")
    .columns_per_row(5)
    .limit(100)
    .iter()?;
```

## Value predicates
```rust,ignore
use pigeonhole::ValueFilter;
use std::cmp::Ordering;

scan.value_filter(ValueFilter::Equals(b"200".to_vec()));
scan.value_filter(ValueFilter::Prefix(b"text/".to_vec()));
scan.value_filter(ValueFilter::I64(Ordering::Greater, 100));   // i64 values compared with 100
```
A value predicate tests the **newest visible value of a column**, then the cell is materialized only if it matches. It cannot use an index; it saves materialization, not block reads. For hot lookups by value, store an inverted row instead (see [Data modeling](data-modeling.md)).

## Snapshots
```rust,ignore
let snap = db.snapshot()?;               // consistent view of everything committed so far
let c = table.get_at(&snap, b"row", "meta", b"k")?;
let it = table.scan_prefix(b"user:").snapshot(&snap).iter()?;
let row = table.row(b"row").snapshot(&snap).read()?;
println!("{}", snap.seqno());
```
- A snapshot never shows part of a commit, including a cross-shard batch.
- Use one snapshot for several reads that must agree with each other.
- Without `.snapshot(..)`, a read uses the data committed as of when it starts; pass a snapshot when several reads must agree.
- **A snapshot pins what it can see. Drop it promptly**: long-lived snapshots delay reclaiming space and memory.
- `Snapshot` is cheap to clone.

## Pushdown semantics (D22)
Filters run **before values are materialized**, and every data source (memtables and SSTs) applies them identically, so the result is always the same as reading everything and filtering afterwards. How each filter is applied:

| Filter | Where it runs | Notes |
|---|---|---|
| Family projection | Choosing which trees to open | Unprojected families cost nothing. |
| Qualifier prefix / range | Inside the block decoder | |
| Time range | Inside the block decoder, on puts | |
| `versions`, `columns_per_row` / `column_limit` | In version resolution, after snapshot visibility | Counted over versions **visible** at the snapshot. |
| Value predicates | In version resolution, before materialization | Test the newest visible value of a column. |

Consequences you should know:
- **Deletes always win.** Delete markers (cell, column, family-in-row) always pass the decoder filters, even when their qualifier or timestamp is outside your filter. Hiding a marker would resurrect older versions, so a `time_range` that excludes a column delete's timestamp still honors the delete. A delete with timestamp `T` hides every version in its scope with timestamp `<= T` (decision D9).
- **Merge operands are never dropped by pushdown.** Dropping some operands would yield a partial counter, so a counter's resolved value always reflects all its operands. Do not expect `time_range` to compute a windowed sum over an `incr` counter; use time-bucketed qualifiers instead.
- **`versions(n)` and value predicates see snapshot-visible data.** `value_filter` tests the newest visible value, not any older version of the column.
- Filtering by qualifier does not change the visibility of a version of a different qualifier.

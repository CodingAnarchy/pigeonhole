# Scans and filters

> **Status:** this guide describes `main`, which will be released as 0.2.0; crates.io has 0.1.0, and the [changelog](../../CHANGELOG.md) lists what changed. Semantics come from the spec and decision D22. `Scan::stream` (async) is Phase 3. Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup).

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
| `limit(n)` |  | ✓ | Stop after `n` rows (`limit(0)` returns none). |
| `snapshot(&snap)` | ✓ | ✓ | Read as of a snapshot instead of now. |

## Row scans
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let events = pigeonhole::doc_support::table(&db, "events", &["ev", "meta"])?;
# events.mutate(b"user:42:a").put("ev", b"2026-10-05T12", b"login").put("meta", b"k", b"v").commit()?;
# events.mutate(b"user:42:b").put("ev", b"2026-10-05T13", b"logout").commit()?;
use std::ops::Bound;

// Every row starting with a prefix.
let it = events.scan_prefix(b"user:42:").iter()?;

// A half-open key range. Both ends must be the same type: use slices.
let it = events.scan(&b"user:42:"[..]..&b"user:43:"[..]).iter()?;

// Explicit bounds (what a C ABI exports).
let it = events.scan_bounds(Bound::Included(&b"a"[..]), Bound::Unbounded).iter()?;
# Ok::<(), pigeonhole::Error>(())
```
Rows come back in **byte-wise key order**; a scan walks tablets in order, so results are ordered without a merge step. A prefix scan of `user:42:` returns exactly the keys that begin with those bytes, and nothing else, so choose separators with that in mind (`user:4` also matches `user:42:`).

Consume rows as owned values (`Iterator<Item = Result<Row>>`; cheap, values stay pinned rather than copied) or zero-copy:

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let events = pigeonhole::doc_support::table(&db, "events", &["ev", "meta"])?;
# events.mutate(b"user:42:a").put("ev", b"2026-10-05T12", b"login").put("meta", b"k", b"v").commit()?;
# events.mutate(b"user:42:b").put("ev", b"2026-10-05T13", b"logout").commit()?;
let mut it = events.scan_prefix(b"user:42:").iter()?;
while let Some(row) = it.next_ref()? {
    // RowRef<'_>: valid until the next next_ref() call
    let v = row.get("ev", b"2026-10-05T12");
}
# Ok::<(), pigeonhole::Error>(())
```

## Family projection
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let events = pigeonhole::doc_support::table(&db, "events", &["ev", "meta"])?;
# events.mutate(b"user:42:a").put("ev", b"2026-10-05T12", b"login").put("meta", b"k", b"v").commit()?;
# events.mutate(b"user:42:b").put("ev", b"2026-10-05T13", b"logout").commit()?;
events.scan_prefix(b"user:42:").family("ev").iter()?;
events.scan_prefix(b"user:42:").families(["ev", "meta"]).iter()?;
# Ok::<(), pigeonhole::Error>(())
```
Each family is its own physical tree, so **unprojected families are never read at all**. Projection is the single biggest scan optimization: keep large payloads in their own family and leave them out of scans that do not need them.

## Qualifier selection
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let events = pigeonhole::doc_support::table(&db, "events", &["ev", "meta"])?;
# events.mutate(b"user:42:a").put("ev", b"2026-10-05T12", b"login").put("meta", b"k", b"v").commit()?;
# events.mutate(b"user:42:b").put("ev", b"2026-10-05T13", b"logout").commit()?;
use std::ops::Bound;

// Prefix.
let edges = events.scan_prefix(b"user:42:").qualifier_prefix(b"edge:").iter()?;
// Range: [2026-10-05T00, 2026-10-06T00)
let day = events
    .scan_prefix(b"user:42:")
    .qualifier_range(&b"2026-10-05T00"[..]..&b"2026-10-06T00"[..])
    .iter()?;
// Explicit bounds.
let a_to_m = events
    .scan_prefix(b"user:42:")
    .qualifier_bounds(Bound::Included(&b"a"[..]), Bound::Excluded(&b"m"[..]))
    .iter()?;
# assert_eq!(day.count(), 2);
# Ok::<(), pigeonhole::Error>(())
```
Qualifiers are sorted bytes within a family, so a prefix or range is a contiguous read inside each row, not a filter over every column.

## Versions and time ranges
By default you get the newest version of each column. Ask for more:

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let t = pigeonhole::doc_support::table(&db, "sensors", &["temp"])?;
# let (t0_us, t1_us) = (0u64, u64::MAX);
// Last 10 versions of each column in the row.
let row = t.row(b"sensor:7").family("temp").versions(10).read()?;

// Versions in a window. Timestamps are u64 microseconds since the Unix epoch.
let it = t.scan_prefix(b"sensor:").time_range(t0_us..t1_us).versions(0).iter()?;
# Ok::<(), pigeonhole::Error>(())
```
- Within a column, versions are ordered **newest first**.
- `versions(0)` returns every retained version. What is retained depends on the family's `max_versions` and `ttl`.
- Do not rely on reading versions older than the family's TTL; expiry is applied by timestamp.
- In a counter family each bucket (`incr_at`) is a version, so `time_range` windows buckets; see the pushdown rules below.

## Limiting work
- `limit(n)` stops after `n` rows. Use it for pagination: remember the last key and resume with `scan_bounds(Bound::Excluded(last_key), ...)`.
- `columns_per_row(n)` caps columns per family per row and skips the rest of each row without decoding. Use it to read "the first few attributes" of very wide rows.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let t = pigeonhole::doc_support::table(&db, "t", &["meta"])?;
# use std::ops::Bound;
# let last_key = b"user:41".to_vec();
let page = t
    .scan_bounds(Bound::Excluded(last_key.as_slice()), Bound::Unbounded)
    .family("meta")
    .columns_per_row(5)
    .limit(100)
    .iter()?;
# Ok::<(), pigeonhole::Error>(())
```

## Value predicates
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let events = pigeonhole::doc_support::table(&db, "events", &["ev", "meta"])?;
# events.mutate(b"user:42:a").put("ev", b"2026-10-05T12", b"login").put("meta", b"k", b"v").commit()?;
# events.mutate(b"user:42:b").put("ev", b"2026-10-05T13", b"logout").commit()?;
use pigeonhole::ValueFilter;
use std::cmp::Ordering;

let scan = || events.scan_prefix(b"user:42:");
let ok = scan().value_filter(ValueFilter::Equals(b"200".to_vec())).iter()?;
let text = scan().value_filter(ValueFilter::Prefix(b"text/".to_vec())).iter()?;
let big = scan().value_filter(ValueFilter::I64(Ordering::Greater, 100)).iter()?; // i64 values compared with 100
# Ok::<(), pigeonhole::Error>(())
```
A value predicate tests the **newest visible value of a column**, then the cell is materialized only if it matches. A value stored in a blob file is read and tested like an inline one. It cannot use an index; it saves materialization, not block reads. For hot lookups by value, store an inverted row instead (see [Data modeling](data-modeling.md)).

## Snapshots
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let table = pigeonhole::doc_support::table(&db, "table", &["meta"])?;
let snap = db.snapshot()?;               // consistent view of everything committed so far
let c = table.get_at(&snap, b"row", "meta", b"k")?;
let it = table.scan_prefix(b"user:").snapshot(&snap).iter()?;
let row = table.row(b"row").snapshot(&snap).read()?;
println!("{}", snap.seqno());
# Ok::<(), pigeonhole::Error>(())
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
- **Deletes always win.** Delete markers (cell, column, family-in-row) always pass the decoder filters, even when their qualifier or timestamp is outside your filter. Hiding a marker would resurrect older versions, so a `time_range` that excludes a column delete's timestamp still honors the delete. A delete with timestamp `T` hides every version in its scope with timestamp `<= T` (decision D9); in a counter family, only those written before it (decision D179).
- **Counters are filtered after they are resolved.** In a family with a merge operator, `time_range` applies to resolved versions (decision D82). In a counter family each bucket is resolved on its own, so a `time_range` returns the buckets inside it with their full sums; it never sums buckets for you. In a family from 0.1.0 with the `i64` operator, a run of increments resolves to one version at its newest increment's timestamp, kept if that timestamp is in the range.
- **`versions(n)` and value predicates see snapshot-visible data.** `value_filter` tests the newest visible value, not any older version of the column.
- Filtering by qualifier does not change the visibility of a version of a different qualifier.

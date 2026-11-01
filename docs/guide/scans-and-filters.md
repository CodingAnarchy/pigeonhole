# Scans and filters

> **Status:** this guide describes `main`; the current release on crates.io is 1.0.0, and [`changelog.d/`](../../changelog.d/README.md) lists what `main` adds after it. Semantics come from the spec and decision D22. `Scan::stream` (async) is new in 0.3.0 ([Async](async.md)). Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup).

Both `Table::row(key)` (a `RowRead`) and `Table::scan*` (a `Scan`) are builders. Nothing happens until you call `.read()` or `.iter()` (or `.read_async()` and `.stream()`; see [Async](async.md)). They share most of their methods.

| Method | `RowRead` | `Scan` | Effect |
|---|---|---|---|
| `families(["a", "b"])` / `family("a")` | ✓ | ✓ | Project to these families (default: all), in this order. |
| `qualifier_prefix(p)` | ✓ | ✓ | Only qualifiers starting with `p`. |
| `qualifier_range(range)` | ✓ | ✓ | Only qualifiers within a range. |
| `qualifier_bounds(start, end)` | ✓ | ✓ | Same with explicit `Bound`s. |
| `latest()` | ✓ | ✓ | Newest version of each column (default). |
| `versions(n)` | ✓ | ✓ | Up to `n` versions per column (0: all retained). |
| `time_range(a..b)` | ✓ | ✓ | Only versions with timestamp in `[a, b)` (microseconds). |
| `column_limit(n)` | ✓ | ✓ | At most `n` columns per family; a scan applies it to each row and skips the rest of the row without decoding. |
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
- `limit(n)` also sets how early a scan reads ahead. With at most 128 rows left to return, it reads blocks ahead only after 8 blocks, so a short page doesn't wait for blocks it never reaches; without a limit, it starts after 2 (D293). Set `limit` on short scans rather than dropping the iterator early.
- `column_limit(n)` caps columns per family in each row and skips the rest of each row without decoding. Use it to read "the first few attributes" of very wide rows.

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
    .column_limit(5)
    .limit(100)
    .iter()?;
# Ok::<(), pigeonhole::Error>(())
```

## Byte budgets
`read_within(bytes)` returns as much of a scan, or a multi-get, as fits in `bytes` (D308): the longest prefix of the read's cells, in order, whose size fits. Use it to fill a fixed-size buffer, such as a page of an API response or a language model's context window, without over-reading and trimming.

- **What counts:** each cell counts its qualifier, its value (as `CellRef::value` returns it) and 16 bytes, and each row counts its key once.
  - A column, with all the versions the read selects, is taken whole or not at all.
  - A row may be cut between columns.
  - The size definition is stable, so a page is the same on every run and every release.
- **Tokens:** the budget is bytes. Convert a token budget yourself, and leave a margin for your own framing: the JSON or prompt text the cells end up in.
- **The page:**
  - `rows()` are owned rows;
  - `bytes()` is their size, never more than the budget;
  - `cut_by_budget()` says whether the budget stopped the read;
  - `resume()` is where to continue. It's `Some` whenever the read stopped before its input ended, by the budget or by `limit`, which applies too, whichever stops first.
- **An item too big for the budget:** when the first column alone doesn't fit, the page is empty and cut. `next_bytes()` is that column's size, with its row key. Retry from the same `resume()` with at least that budget.
- **Resuming:** `resume_after(&position)` continues a later read, also in another process (`ScanPosition::to_bytes`/`from_bytes`).
  - It starts at the later of the range's start and the position, so a position past the range's end reads nothing.
  - A multi-get's `KeysPosition` names the last key, not an index, so a key set rebuilt between pages resumes at the right key.
  - Pass the same `snapshot` to every page for one consistent read.
- **The cost:** the read runs inside the engine's bounded-read path, and reads about the budget plus one column. A traversal takes a budget too: `Traverse::byte_budget` ([graphs](graphs.md#traversal-hops-inside-the-engine)).

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let t = pigeonhole::doc_support::table(&db, "t", &["text"])?;
# for i in 0..50u32 { t.mutate(format!("doc:{i:02}").as_bytes()).put("text", b"body", &[b'x'; 300]).commit()?; }
let snap = db.snapshot()?;
let mut at: Option<ScanPosition> = None;
let mut docs = 0;
loop {
    let mut scan = t.scan_prefix(b"doc:").snapshot(&snap);
    if let Some(p) = &at {
        scan = scan.resume_after(p);
    }
    let page = scan.read_within(4096)?;        // about a thousand tokens of text
    assert!(page.bytes() <= 4096);
    docs += page.rows().len();
    match page.resume() {
        Some(p) => at = Some(p.clone()),
        None => break,
    }
}
assert_eq!(docs, 50);
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
- **A snapshot pins what it can see. Drop it promptly**: long-lived snapshots delay reclaiming space and memory. While one is held, `shrink` gives back less; call it again after the snapshot is dropped.
- `Snapshot` is cheap to clone.

## Pushdown semantics (D22)
Filters run **before values are materialized**, and every data source (memtables and SSTs) applies them identically, so the result is always the same as reading everything and filtering afterwards. How each filter is applied:

| Filter | Where it runs | Notes |
|---|---|---|
| Family projection | Choosing which trees to open | Unprojected families cost nothing. |
| Qualifier prefix / range | Inside the block decoder | |
| Time range | Inside the block decoder, on puts | |
| `versions`, `column_limit` | In version resolution, after snapshot visibility | Counted over versions **visible** at the snapshot. |
| Value predicates | In version resolution, before materialization | Test the newest visible value of a column. |

Consequences you should know:
- **Deletes always win.** Delete markers (cell, column, family-in-row) always pass the decoder filters, even when their qualifier or timestamp is outside your filter. Hiding a marker would resurrect older versions, so a `time_range` that excludes a column delete's timestamp still honors the delete. A delete with timestamp `T` hides every version in its scope with timestamp `<= T` (decision D9); in a counter family, only those written before it (decision D179).
- **Counters are filtered after they are resolved.** In a family with a merge operator, `time_range` applies to resolved versions (decision D82). In a counter family each bucket is resolved on its own, so a `time_range` returns the buckets inside it with their full sums; it never sums buckets for you. In a family from 0.1.0 with the `i64` operator, a run of increments resolves to one version at its newest increment's timestamp, kept if that timestamp is in the range.
- **`versions(n)` and value predicates see snapshot-visible data.** `value_filter` tests the newest visible value, not any older version of the column.
- Filtering by qualifier does not change the visibility of a version of a different qualifier.

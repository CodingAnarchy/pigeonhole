# Data modeling

> **Status:** this guide describes `main`; the current release on crates.io is 1.0.0, and [`changelog.d/`](../../changelog.d/README.md) lists what `main` adds after it. Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup).

Pigeonhole is a sorted map. Good models make the reads you do most **one point get or one contiguous scan**. Everything below follows from three facts:

1. Rows are sorted by byte-wise key, and a scan over a range or prefix is contiguous.
2. A row is the unit of atomicity: all changes to one row, across families, commit all-or-nothing.
3. Each family is its own tree with its own policy, and an unprojected family is never read.

## Row-key design
**Put what you scan together under a shared prefix.**

| Access pattern | Key | Scan |
|---|---|---|
| Everything for one user | `user:<id>:...` | `scan_prefix(b"user:42:")` |
| Pages of one site | `<reversed host>/<path>`, e.g. `com.example/a` | `scan_prefix(b"com.example/")` |
| Newest events first | `<entity>:<u64::MAX - ts as big-endian>` | `scan_prefix(b"sensor:7:").limit(n)` |

Rules of thumb:
- **Encode integers big-endian** so numeric order equals byte order: `id.to_be_bytes()`. Little-endian or decimal text without padding sorts wrongly (`"10" < "9"`).
- **Use a separator that cannot appear in the parts**, or fixed-width parts. `user:4` is a prefix of `user:42:`; scan `user:4:` instead.
- **Reverse the key part you want newest-first**: store `u64::MAX - ts` big-endian.
- **Keep keys short.** Keys and qualifiers are each at most 64 KiB (`ErrorCode::KeyTooLarge`), but shorter keys mean smaller blocks and indexes.
- **Avoid hot, monotonically increasing prefixes** (a bare timestamp as the leading key bytes) when many writers are active: all writes land at the same end of the key space (tablets split under write skew, but a spread key avoids relying on it). Lead with an entity id, or a small hash bucket if you do not need cross-entity range scans.
- **Do not put mutable data in the key.** Changing a key means delete plus put.
- Keys are arbitrary bytes; zeros are fine.

## Split families by access pattern
A family is a physical tree and carries its own policy. Split when data differs in **how it is read, how big it is, or how long it lives**.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let pages = db
    .table("pages")?
    .family("meta", Family::default().max_versions(1).cache_priority(Priority::High))
    .family("links", Family::default().bloom_bits(10))
    .family("body", Family::default().ttl(days(30)).cache_priority(Priority::Low))
    .create_if_missing()?;
# Ok::<(), pigeonhole::Error>(())
```

| Family holds | Settings that help |
|---|---|
| Small, hot attributes (status) | `max_versions(1)`, `Priority::High`, `compression(Compression::None)` if tiny |
| Counters | `Family::counter()`; a `ttl` to keep only recent buckets |
| Running minimum or maximum | `Family::aggregate(Aggregation::Min)` or `Max`; buckets as for counters |
| Distinct counts | `Family::aggregate(Aggregation::Hll)`; read with `hll_estimate` |
| Flags seen, or common to all writers | `Family::aggregate(Aggregation::BitOr)` (or `BitAnd`, `BitXor`); write with `aggregate_u64`, read with `as_u64` |
| Lowest or highest reading (temperatures, latencies) | `Family::aggregate(Aggregation::MinF64)` or `MaxF64`; write with `aggregate_f64`, read with `as_f64` |
| Totals of decimals (amounts, durations) | `Family::aggregate(Aggregation::SumF64)`: an exact sum, read with `sum_f64` |
| Averages and spread (latency mean and variance) | `Family::aggregate(Aggregation::MeanVar)`; read with `mean_var` |
| Distributions in fixed buckets (response codes, latency bands) | `Family::aggregate(Aggregation::Histogram(HistogramParams::new(bounds)?))`; read with `histogram_counts` |
| Membership checks (seen this user? this URL?) | `Family::aggregate(Aggregation::Bloom(BloomParams::new(bits, hashes)?))`; write with `aggregate` or `aggregate_bytes`, read with `bloom_contains` |
| Percentiles (p50, p99 latency) | `Family::aggregate(Aggregation::Quantiles(QuantileParams::default()))`; read with `quantile(0.99)` |
| Sparse or wide sets (links, tags) | `bloom_bits(10)` |
| Large payloads read rarely | its own family; scan other families without touching it. Values over 4 KiB already go to blob files by default; lower `blob_threshold` for smaller payloads |
| Data that should expire | `ttl(days(n))`; add `Compaction::FifoByTime` to drop whole files (time-ordered data); `Compaction::Tiered` suits write-heavy families ([styles](concepts.md#compaction-styles)) |

Do not split just to organize: every family is another tree to flush and compact. A `delete_row` also writes one marker per family.

## Time series with TTL
Key by entity, qualifier by time bucket, value is the reading:

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let ts_secs = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
# let ts_micros = ts_secs * 1_000_000;
# let value_bytes = 21.5f64.to_le_bytes();
# let (t0_secs, t1_secs) = (ts_secs - 60, ts_secs + 60);
let readings = db
    .table("readings")?
    .family("temp", Family::default().max_versions(1).ttl(days(30)))
    .create_if_missing()?;

// One row per sensor; one column per second (fixed-width, big-endian => sorted by time).
let q = ts_secs.to_be_bytes();
readings.mutate(b"sensor:7").put_at("temp", &q, ts_micros, &value_bytes).commit()?;

// A time window for one sensor: contiguous qualifier range.
let lo = t0_secs.to_be_bytes();
let hi = t1_secs.to_be_bytes();
let row = readings
    .row(b"sensor:7")
    .family("temp")
    .qualifier_range(&lo[..]..&hi[..])
    .read()?;
# assert_eq!(row.unwrap().len(), 1);
# Ok::<(), pigeonhole::Error>(())
```

Notes:
- `put_at` sets **event time** as the version timestamp, in microseconds since the Unix epoch. TTL is measured against that timestamp, so supply microseconds, not seconds or milliseconds.
- A row with millions of columns is fine, but very large rows make a single-row read large. Bucket by day or hour instead: row `sensor:7:2026-10-05`, columns by second. Then a day is one row and a month is a prefix scan.
- `Compaction::FifoByTime` (with a TTL) drops expired files wholesale, the cheapest way to age out append-only data. It keeps files in arrival order and merges only small neighbours, so it suits data written roughly in time order; a write with an old explicit timestamp lands in a new file and lives until that file expires. Expiry runs on a timer set for the earliest expiry, so even an idle family drops its expired files on time (D170); until then expired cells are hidden from reads. A file is dropped only when its **newest** cell has expired. See [Compaction styles](concepts.md#compaction-styles) for when to pick `Leveled`, `Tiered` or `FifoByTime`, including the engine's lossy size cap (not exposed in `Options`).
- If you want history of a **single value**, store it as versions of one column and read with `.versions(n)`; if you want many distinct points, use distinct qualifiers. See Versions below.

## Adjacency lists (graphs)
One row per node; one column per edge, qualifier `<dst>`, in a family per edge type:

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let g = db
    .table("graph")?
    .family("out", Family::default().bloom_bits(10).max_versions(1))
    .family("in", Family::default().bloom_bits(10).max_versions(1))
    .create_if_missing()?;

// Add edge a -> b: both directions, atomic per row; use a batch to make it atomic across both.
let mut wb = db.write_batch();
wb.put(&g, b"node:a", "out", b"node:b", b"")
  .put(&g, b"node:b", "in", b"node:a", b"");
wb.commit()?;

// Neighbors of a: one row read, projected.
let row = g.row(b"node:a").family("out").read()?;

// Is there an edge a -> b? A point get; the bloom filter makes misses cheap.
let exists = g.get(b"node:a", "out", b"node:b")?.is_some();
# assert!(exists);

// The first 100 neighbors only:
let page = g.row(b"node:a").family("out").column_limit(100).read()?;
# Ok::<(), pigeonhole::Error>(())
```
- Edge properties go in the value, or as extra families keyed the same way.
- `delete_column("out", b"node:b")` removes the edge; `delete_family("out")` removes all out-edges of a node.
- Writing both directions in one `WriteBatch` makes them atomic even if the rows live on different shards.
- [Graphs](graphs.md) covers the rest: degree, hubs too wide to read whole, many nodes at once, and multi-hop traversal.

## Counters
Counters live in a **counter family**, declared with `Family::counter()` (after Bigtable's aggregate families). A counter is one kind of aggregate family; [Min and max](#min-and-max), [Distinct counts (HLL)](#distinct-counts-hll) and [Bitwise and `f64` aggregations](#bitwise-and-f64-aggregations) cover the others. `incr` adds to an `i64` **without reading it**: the write is a blind operand, so concurrent writers never lose updates. `incr` on any other family fails with `ErrorCode::InvalidArgument`.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let pages = db
    .table("pages")?
    .family("meta", Family::default())
    .family("hits", Family::counter())
    .create_if_missing()?;
pages.mutate(b"com.example/a").incr("hits", b"total", 1).commit()?;
pages.mutate(b"com.example/a").incr("hits", b"total", 2).commit()?;

let total = pages.get(b"com.example/a", "hits", b"total")?.and_then(|c| c.as_i64());
# assert_eq!(total, Some(3));
# Ok::<(), pigeonhole::Error>(())
```
- **One cell per counter.** `incr` writes at one fixed timestamp (0), so all increments of a counter are one version. A read adds up the increments not yet combined. Flushes and compactions combine them, and a read that adds up many leaves its sum in memory, so the next reads of that counter start from there (D287, D291). A hot counter stays one cell, and its reads stay fast, however often it is incremented.
- **Buckets.** `incr_at(family, qualifier, ts, delta)` adds to the version at timestamp `ts`: use it for per-period counts (hourly, daily). Each bucket is a version of its own, so read them with `.versions(n)` and window them with `.time_range(a..b)`. A TTL expires each bucket on its own; `max_versions(n)` makes reads return the newest `n` buckets but does not shrink storage, because compaction never changes what a counter family reads (deleting a newer bucket shows the older ones again). Bound the number of stored buckets with a TTL.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["hits"])?;
const DAY: u64 = 86_400_000_000; // microseconds
let today = 20_000 * DAY;
pages.mutate(b"com.example/a").incr_at("hits", b"daily", today - DAY, 4).commit()?;
pages.mutate(b"com.example/a").incr_at("hits", b"daily", today, 1).commit()?;
pages.mutate(b"com.example/a").incr_at("hits", b"daily", today, 1).commit()?;

let row = pages.row(b"com.example/a").family("hits").versions(7).read()?.unwrap();
let days: Vec<(u64, i64)> =
    row.iter().map(|e| (e.cell.timestamp(), e.cell.as_i64().unwrap())).collect();
assert_eq!(days, [(today, 2), (today - DAY, 4)]);
# Ok::<(), pigeonhole::Error>(())
```
- **Setting a counter.** `put_i64` sets the counter and `put_i64_at` a bucket; later increments add to it. A counter family holds only `i64`s: `put`, `put_at`, `put_f64` and untyped `merge` operands fail with `InvalidArgument`. Within one mutation or batch, a counter's writes apply in order: `incr(c, 1).incr(c, 2)` adds 3, `put_i64(c, 5).incr(c, 1)` leaves 6, and `incr(c, 1).put_i64(c, 5)` leaves 5. (A delete and a write of the same cell in one mutation still keep only the last; a delete never hides writes of its own commit.)
- **Deleting.** A delete removes what earlier commits wrote: after `delete_column`, `delete_family` or `delete_row`, the next `incr` starts the counter from 0 again. Writes in the same commit as the delete are not hidden, so `incr` then `delete_column` in one mutation leaves the increment visible; delete in one commit and count in the next. (In other families a delete also hides later writes with older timestamps; see [Versions](#versions).) `delete_column`, `delete_family` and `delete_row` take the commit timestamp, so they cover the counter and buckets up to now; a bucket at a later timestamp survives them. `delete_cell(family, qualifier, 0)` deletes the counter, `delete_cell(.., ts)` one bucket.
- **TTL.** With a TTL the fixed timestamp would expire at once, so a counter family with a TTL takes only buckets: `incr` and `put_i64` fail with `InvalidArgument`; use `incr_at` and `put_i64_at`.
- A missing counter counts as 0; overflow wraps.
- **Families from 0.1.0.** In 0.1.0 every family had the `pigeonhole.i64_add` operator and `incr` worked anywhere. Those families keep their 0.1.0 behavior: `incr` writes at the commit timestamp, each increment is its own operand, and runs of them fold across timestamps when read. To move a counter to a counter family, read its value and `put_i64` it there (see the changelog). `Family::default().merge_operator("pigeonhole.i64_add")` still creates such a family.
- **Min and max.** See [Min and max](#min-and-max).
- **Custom operators.** Implement `pigeonhole::MergeOperator` (an associative fold: `merge(acc, older)` then `finish(base, acc)`), register it with `Options::merge_operator(Arc::new(op))`, and name it on the family with `Family::merge_operator("name")`, then write operands with `RowMutation::merge`. The operator's name is stored in the file; opening without it registered fails with `ErrorCode::UnknownMergeOperator` unless you set `Options::read_only_fallback(true)`: the handle is then read-only (writes, table changes, `flush`, `compact` and `shrink` fail with `ReadOnly`, and nothing is written to the file), and reads of their merged cells fail with `UnknownMergeOperator`. A family naming an operator that is not registered is refused at creation. Reader processes register operators with `ReaderOptions::merge_operator`. Operators must be associative; non-associative operators are not supported. The operator sees stored values: a tag byte (`0x00` for bytes), then the payload.

## Min and max
A counter is the `Sum` case of an **aggregate family**: every cell combines the `i64` inputs written to it, with an aggregation fixed when the family is created (after Bigtable's aggregate families; D294). `Family::aggregate(Aggregation::Min)` keeps the smallest input of each cell and `Aggregation::Max` the largest; `Family::counter()` is `Family::aggregate(Aggregation::Sum)`.

```rust
use pigeonhole::{Aggregation, Family};
# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = pigeonhole::Pigeonhole::open(dir.join("app.phdb"), pigeonhole::Options::default())?;
let t = db
    .table("sensors")?
    .family("low", Family::aggregate(Aggregation::Min))
    .family("high", Family::aggregate(Aggregation::Max))
    .create_if_missing()?;
t.mutate(b"s1").aggregate("low", b"temp", 21).aggregate("high", b"temp", 21).commit()?;
t.mutate(b"s1").aggregate("low", b"temp", 17).aggregate("high", b"temp", 17).commit()?;
assert_eq!(t.get(b"s1", "low", b"temp")?.unwrap().as_i64(), Some(17));
assert_eq!(t.get(b"s1", "high", b"temp")?.unwrap().as_i64(), Some(21));
# Ok(())
# }
```

Everything in [Counters](#counters) holds, with the family's aggregation in place of the sum:
- **Writes.** `aggregate(family, qualifier, value)` writes an input without reading the cell, at the fixed timestamp 0; `aggregate_at(.., ts, value)` writes to the bucket at `ts`. Inputs combine at read and in compaction, and a read that combines many leaves its result in memory. `incr` and `incr_at` are refused in a min or max family (`InvalidArgument`); in a counter family `aggregate` is `incr`.
- **Setting and deleting.** `put_i64` sets the cell (`put_i64_at` a bucket), and later inputs combine with it: `put_i64(c, 5).aggregate(c, 9)` leaves 5 in a min family. A delete removes what earlier commits wrote, so the next input starts the cell again.
- **Missing cells.** A cell nothing was written to, or whose inputs were all deleted, reads as absent (`None`), never as 0.
- **TTL, `max_versions`, values.** As for counters: buckets only with a TTL, and only `i64`s.
- **`FamilyInfo::aggregation`** says a family's aggregation (`None` for an ordinary family; a counter family's is `Some(Sum)`).
- **The file can no longer be opened by 1.0.x.** Min and max families came after 1.0: creating the first one marks the file (a feature bit), and from then on pigeonhole 1.0.x refuses to open it (`UnsupportedFeature`), even after the family is dropped. Back up first if you may roll back ([upgrades](concepts.md#file-format-and-upgrades)).


## Distinct counts (HLL)
`Family::aggregate(Aggregation::Hll)` counts **distinct** `i64` inputs per cell with a HyperLogLog sketch (D294): unique visitors per page, distinct devices per hour. `aggregate` adds an element without reading the cell, and `CellRef::hll_estimate` reads the estimated count (4096 registers, about 1.6% standard error; small counts are exact or nearly so). Hash a string or other key to an `i64` yourself before writing it.

```rust
use pigeonhole::{Aggregation, Family};
# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = pigeonhole::Pigeonhole::open(dir.join("app.phdb"), pigeonhole::Options::default())?;
let t = db
    .table("pages")?
    .family("visitors", Family::aggregate(Aggregation::Hll))
    .create_if_missing()?;
for user in [7, 9, 7, 12, 9] {
    t.mutate(b"home").aggregate("visitors", b"all", user).commit()?;
}
let n = t.get(b"home", "visitors", b"all")?.unwrap().hll_estimate();
assert_eq!(n, Some(3));
# Ok(())
# }
```

As for counters and min/max: `aggregate_at` writes a bucket, a delete starts the cell again, and the TTL expires each bucket. Differences:
- **Size and speed:** a cell is a sketch of up to 3 KiB, against 9 bytes for a counter, and reads and the row cache carry it whole. Bound a wide row's buckets with a TTL or `max_versions`. Reading a hot HLL cell takes about 14 µs at least, the copy of its 3 KiB sketch, against about 1 µs for a counter. With the default checkpoints, its p99 stayed under 50 µs under a steady stream of inputs (D294).
- **Writes:** `incr` and `put_i64` are refused. `put` takes only a well-formed sketch, as `phdb load` writes one back.
- **Storage:** sketches are never moved to blob files (`blob_threshold` below 3076 is refused).
- **The file can no longer be opened by 1.0.x**, as with min and max ([Min and max](#min-and-max)).

## Bitwise and `f64` aggregations
These combine like min and max (buckets, puts, deletes, TTL), over other value types (D298):
- **`BitOr`, `BitAnd`, `BitXor`** fold `u64` inputs bit by bit: the flags any writer set, the flags every writer kept, or a parity. Write inputs with `aggregate_u64` and set a cell with `put_u64`; read with `CellRef::as_u64`. The value is stored as the `i64` with the same bits.
- **`MinF64`, `MaxF64`** keep the smallest or largest `f64` input. Write with `aggregate_f64` and set with `put_f64`; read with `as_f64`. They order by `f64::total_cmp`: `-0.0` is below `+0.0`, infinities are allowed, and NaN is refused (`InvalidArgument`).

`incr` is refused in all five, and each marks the file like min and max (1.0.x refuses it).

## Exact sums and mean/variance
`f64` addition rounds at every step, so a floating-point total depends on the order its inputs arrive in, and loses small terms beside large ones. These two aggregations don't:
- **`SumF64`** keeps the exact sum of its `f64` inputs (an integer in units of the smallest `f64`). `CellRef::sum_f64` reads it rounded once. The same inputs read the same total in any order: `1e300 + 1.0 - 1e300` reads 1.0.
- **`MeanVar`** keeps the count, the exact sum and the exact sum of squares. `mean_var()` reads the count, mean, population variance and sample variance, each computed exactly and rounded once.

Write inputs with `aggregate_f64`. A `put_f64` sets a cell. Inputs must be finite (NaN and infinities are refused). A cell is up to 274 bytes for a sum and 821 for mean/variance, against 9 for a counter. Like the others, these families mark the file (1.0.x refuses it).

## Histograms and percentiles
Both take `f64` inputs with `aggregate_f64`, and both have parameters, fixed when the family is created (D298):
- **`Histogram(HistogramParams)`** counts inputs in fixed buckets. `HistogramParams::new([0.0, 10.0, 100.0])` makes four: below 0, `[0, 10)`, `[10, 100)` and 100 and up. `CellRef::histogram_counts` reads one count per bucket, and the family's info gives the bounds back. Infinities fall in the end buckets; NaN is refused. A cell is about 10 bytes per bucket.
- **`Quantiles(QuantileParams)`** keeps a log-linear sketch. `quantile(0.99)` reads within a relative error of 2^-p (`mantissa_bits`, 7 by default, so 0.8%). The same inputs give the same sketch in any order. Inputs must be finite. Each sign keeps at most `max_buckets` buckets (2048 by default). Past that, the smallest magnitudes fold together, in both signs: for data spread over many orders of magnitude, the quantiles nearest zero lose precision first (not the low tail, for signed data). A cell holds about 20 bytes per bucket in use.

`n` bounds give `n + 1` counts. Zip them with the bounds from the family's info rather than indexing by hand:

```rust
use pigeonhole::{Aggregation, Family, HistogramParams};
# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = pigeonhole::Pigeonhole::open(dir.join("app.phdb"), pigeonhole::Options::default())?;
let t = db
    .table("api")?
    .family("ms", Family::aggregate(Aggregation::Histogram(HistogramParams::new([10.0, 100.0])?)))
    .create_if_missing()?;
for ms in [3.0, 42.0, 57.0, 250.0] {
    t.mutate(b"GET /").aggregate_f64("ms", b"latency", ms).commit()?;
}
let counts = t.get(b"GET /", "ms", b"latency")?.unwrap().histogram_counts().unwrap();
let Some(Aggregation::Histogram(params)) = t.family_info("ms").unwrap().aggregation else {
    unreachable!("a histogram family");
};
// Bucket i holds inputs below bounds[i] and at or above bounds[i - 1].
let bounds = params.bounds();
let lower = std::iter::once(f64::NEG_INFINITY).chain(bounds.iter().copied());
let labelled: Vec<(f64, u64)> = lower.zip(counts).collect();
assert_eq!(labelled, [(f64::NEG_INFINITY, 1), (10.0, 2), (100.0, 1)]);
# Ok(())
# }
```

## Bloom filters and string elements
`Bloom(BloomParams::new(bits, hashes)?)` answers "may this element be in the cell?" without storing the elements (D298):
- **Writing:** `aggregate(i64)` and `aggregate_bytes(&[u8])` add an element.
- **Reading:** `CellRef::bloom_contains(i64)` and `bloom_contains_bytes(&[u8])` never answer `false` for an element that was added. They answer `true` for one that wasn't at about `(1 − e^(−hashes·n/bits))^hashes` for `n` elements: for 10,000 elements, 2^18 bits (32 KiB once dense) and 7 hashes, about 1 in 26,000.
- **Size:** a filter is stored sparse while few bits are set, and up to `bits / 8` bytes once dense.

`aggregate_bytes` also feeds an HLL family. Hash a string key there with it, rather than converting it to an `i64` yourself. A bytes element and an `i64` hash apart, so `aggregate_bytes(&7i64.to_le_bytes())` and `aggregate(7)` are two elements.

Aggregate cells are never moved to blob files, whatever `blob_threshold` says, with two exceptions: a bloom filter or a set larger than its family's `blob_threshold` is, like any large value. Reads and compactions fold new elements into it there, so a large filter costs one blob read per get plus its pending elements, and compaction rewrites it once per pass. A get that folds a filter of at most 4 KiB keeps the result in memory for the next reads; a larger one relies on compaction instead.

## Sets of integers

A `Family::aggregate(Aggregation::Set)` cell holds a set of `u32`s, such as a node's neighbours or the ids of the users who did something:

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let t = db
    .table("graph")?
    .family("out", Family::aggregate(Aggregation::Set))
    .create_if_missing()?;
t.mutate(b"node:7").set_add("out", b"", 12).set_add("out", b"", 40).commit()?;
t.mutate(b"node:7").set_remove("out", b"", 12).commit()?;
let cell = t.get(b"node:7", "out", b"")?.unwrap();
let out = cell.as_set().unwrap();
assert!(out.contains(40) && !out.contains(12));
# Ok::<(), pigeonhole::Error>(())
```

- **Order:** adds and removes apply in commit order, so the last write for an element wins, even within one commit.
- **Reading:** `CellRef::as_set` reads the set where it is stored, without decoding it. `len()` comes from per-block counts. `contains(x)` is a search, and `iter()` runs in ascending order. `intersection_len`, `intersection` and `union_len` take another cell's set, so the common neighbours of two nodes come from two gets.
- **Storage:** a set is a standard 32-bit roaring bitmap (`SetRef::as_bytes`), which CRoaring and roaring-rs read as they are. Dense ranges cost about one bit per possible element, and sparse ones two bytes per element.
- **Large sets:** a set past the family's `blob_threshold` moves to a blob file. A get then reads it from there and applies the inputs written since. Compaction folds those inputs into a new copy, so a hub's set is rewritten once per compaction, not on every write.

## Versions
A cell has many versions, newest first, each with a `u64` microsecond timestamp.

- **Default:** each `put` gets the commit timestamp. Default timestamps come from one allocator per database, so a later commit never gets an earlier default timestamp on any shard, even across a clock step or restart (D271).
- **Event time:** `put_at(.., ts, ..)` for data that arrives late or out of order. A read of the newest version then returns the one with the **largest timestamp**, not the most recently written.
- **Limit retention** with `max_versions(n)` (0 keeps all) and `ttl`.
- **Read history** with `.versions(n)`, `.time_range(a..b)`.
- **Deleting:** `delete_cell(family, qualifier, ts)` removes the version at `ts`, and a put at that same `ts` committed later stays hidden too (decision D38); write the replacement at another timestamp. `delete_column` removes every version, and a later put with a timestamp at or before the delete's timestamp stays hidden (decision D9). A put with a **newer** timestamp is visible again.

**Deletes and version limits are not permanent for writes with older timestamps** (HBase semantics). Until compaction purges them, a delete keeps hiding any later write at or below its timestamp, and `max_versions` only limits what reads return. Once a compaction at the bottom of the tree has run with no open snapshot that still needs them, the delete markers and the versions beyond `max_versions` are gone for good. After that, a write with an older explicit timestamp (`put_at`, `delete_cell`) behaves as if they never existed: a `put_at` below a purged delete becomes visible, and deleting the newest version does not bring back a purged older one. Writes with default timestamps are never affected, because their timestamps are newer than anything a purge removes. If you rewrite history with explicit timestamps, write the replacement at a timestamp newer than the delete instead of relying on the delete to keep hiding it.

## Wide, overwritten rows
An overwrite adds a version; it does not replace the old one in place. Older versions stay in the tree until a flush or compaction drops them. In the writer and in reader processes alike, reads pass a column's older versions in the memtable in one jump (D194, D199, D277), but in SSTs not yet compacted a read still steps over every version it passes, and it merges the row from each memtable and SST that holds part of it. A row that is wide (thousands of columns), overwritten often and read whole therefore has a higher read tail than a store that updates in place. This is the Phase 2 gate's documented gap ([D193](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/design/decisions/phase-2.md#d193), [#387](https://github.com/CodingAnarchy/pigeonhole/issues/387)), judged against SQLite on the reference machine ([D208](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/design/decisions/phase-3.md#d208)). To keep it down:

- **Read only the columns you need.** `get` for one cell, or `qualifier_prefix` / `qualifier_range` on a row read or scan, seeks past the rest of the row instead of stepping through it.
- **Set `max_versions`.** The default, 0, keeps every version, so not even compaction drops them. `max_versions(1)` lets flushes and compactions drop the older ones (unless an open snapshot still needs them).
- **Let compaction run.** After a bulk load or a burst of overwrites, and before a read-heavy phase, `compact()` rewrites the trees so reads step over current versions only.
- **The row cache does not help wide rows by default.** It stores family rows up to `Options::row_cache_max_row` (4 KiB encoded by default), so it suits small, hot rows (see [Getting started](getting-started.md#open-a-database)), not rows of thousands of columns.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let profiles = db
    .table("profiles")?
    .family("attr", Family::default().max_versions(1))
    .create_if_missing()?;
profiles.mutate(b"user:42").put("attr", b"pref.theme", b"dark").commit()?;
// The columns this read needs, not the whole row.
let prefs = profiles.row(b"user:42").family("attr").qualifier_prefix(b"pref.").read()?;
assert!(prefs.is_some());
// After the load, before a read-heavy phase.
db.compact()?;
# Ok::<(), pigeonhole::Error>(())
```

## Anti-patterns
| Don't | Why | Instead |
|---|---|---|
| Bare timestamp or auto-increment as the leading key bytes with many writers | Every write hits the same end of the key space | Lead with entity id or a small hash bucket |
| Little-endian or unpadded decimal numbers in keys | Sorts wrongly | Big-endian fixed width |
| One huge row for unbounded data | Row reads grow without limit | Bucket rows by time or hash |
| Read-modify-write counters (`get`, add, `put`) | Races, extra reads | `incr` in a counter family |
| One family per attribute | Many trees to flush and compact | Qualifiers inside a few families by access pattern |
| Large payload in the same family as hot metadata | Scans of metadata decode payload blocks | Separate family, or a `blob_threshold` below the payload size (the default separates values over 4 KiB) |
| Value predicates as a query engine over big ranges | They save materialization, not block reads | An inverted row keyed by the value |
| Long-lived snapshots | Pin memory and space; `shrink` gives back less while one is held | Take, read, drop |
| `scan` with different-length byte-array literals (`b"a"..b"bcd"`) | Does not compile (end types differ) | Slices, `scan_prefix` or `scan_bounds` |
| Relying on cross-family or cross-row atomicity beyond a row without a batch | Only a single row, or a `WriteBatch`, is atomic | Put it in one row, or use `write_batch()` |
| Multi-row read-then-write invariants | Separate reads and writes race | `db.transact(...)` (optimistic; retries `Conflict` for you; see [Transactions](transactions.md)), or one row and `commit_if` |

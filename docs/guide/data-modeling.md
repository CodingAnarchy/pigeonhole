# Data modeling

> **Status: Phase 1 sync API implemented.** Phase 2+ features are labeled. Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup).

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
| Small, hot attributes (status) | `max_versions(1)`, `Priority::High`, `uncompressed()` if tiny |
| Counters | `Family::counter()`; a `ttl` to keep only recent buckets |
| Sparse or wide sets (links, tags) | `bloom_bits(10)` |
| Large payloads read rarely | its own family; scan other families without touching it. `blob_threshold` keeps them out of the family's tree |
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

## Counters
Counters live in a **counter family**, declared with `Family::counter()` (after Bigtable's aggregate families). `incr` adds to an `i64` **without reading it**: the write is a blind operand, so concurrent writers never lose updates. `incr` on any other family fails with `ErrorCode::InvalidArgument`.

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
- **One cell per counter.** `incr` writes at one fixed timestamp (0), so all increments of a counter are one version. Reads add up the increments not yet combined, and compaction combines them, so a hot counter stays one cell however often it is incremented.
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
- **Setting a counter.** `put_i64` sets the counter and `put_i64_at` a bucket; later increments add to it. A counter family holds only `i64`s: `put`, `put_at`, `put_f64` and untyped `merge` operands fail with `InvalidArgument`. A counter and a `put_i64` in the same mutation collapse to the last one written, as any two writes to one cell do.
- **Deleting.** A delete removes what was written before it: after `delete_column`, `delete_family` or `delete_row`, the next `incr` starts the counter from 0 again. (In other families a delete also hides later writes with older timestamps; see [Versions](#versions).) `delete_cell(family, qualifier, 0)` deletes the counter, `delete_cell(.., ts)` one bucket.
- **TTL.** With a TTL the fixed timestamp would expire at once, so a counter family with a TTL takes only buckets: `incr` and `put_i64` fail with `InvalidArgument`; use `incr_at` and `put_i64_at`.
- A missing counter counts as 0; overflow wraps.
- **Families from 0.1.0.** In 0.1.0 every family had the `pigeonhole.i64_add` operator and `incr` worked anywhere. Those families keep their 0.1.0 behavior: `incr` writes at the commit timestamp, each increment is its own operand, and runs of them fold across timestamps when read. To move a counter to a counter family, read its value and `put_i64` it there (see the changelog). `Family::default().merge_operator("pigeonhole.i64_add")` still creates such a family.
- **Custom operators.** Implement `pigeonhole::MergeOperator` (an associative fold: `merge(acc, older)` then `finish(base, acc)`), register it with `Options::merge_operator(Arc::new(op))`, and name it on the family with `Family::merge_operator("name")`, then write operands with `RowMutation::merge`. The operator's name is stored in the file; opening without it registered fails with `ErrorCode::UnknownMergeOperator` unless you set `Options::allow_unregistered_merge_operators(true)`: the handle is then read-only (writes and table changes fail with `ReadOnly`), compaction skips those families, and reads of their merged cells fail with `UnknownMergeOperator`. A family naming an operator that is not registered is refused at creation. Reader processes register operators with `ReaderOptions::merge_operator`. Operators must be associative; non-associative operators are not supported. The operator sees stored values: a tag byte (`0x00` for bytes), then the payload.

## Versions
A cell has many versions, newest first, each with a `u64` microsecond timestamp.

- **Default:** each `put` gets the commit timestamp. Default timestamps never go backwards for a tablet, even across a clock step or restart.
- **Event time:** `put_at(.., ts, ..)` for data that arrives late or out of order. A read of the newest version then returns the one with the **largest timestamp**, not the most recently written.
- **Limit retention** with `max_versions(n)` (0 keeps all) and `ttl`.
- **Read history** with `.versions(n)`, `.time_range(a..b)`.
- **Deleting:** `delete_cell(family, qualifier, ts)` removes the version at `ts`, and a put at that same `ts` committed later stays hidden too (decision D38); write the replacement at another timestamp. `delete_column` removes every version, and a later put with a timestamp at or before the delete's timestamp stays hidden (decision D9). A put with a **newer** timestamp is visible again.

**Deletes and version limits are not permanent for writes with older timestamps** (HBase semantics). Until compaction purges them, a delete keeps hiding any later write at or below its timestamp, and `max_versions` only limits what reads return. Once a compaction at the bottom of the tree has run with no open snapshot that still needs them, the delete markers and the versions beyond `max_versions` are gone for good. After that, a write with an older explicit timestamp (`put_at`, `delete_cell`) behaves as if they never existed: a `put_at` below a purged delete becomes visible, and deleting the newest version does not bring back a purged older one. Writes with default timestamps are never affected, because their timestamps are newer than anything a purge removes. If you rewrite history with explicit timestamps, write the replacement at a timestamp newer than the delete instead of relying on the delete to keep hiding it.

## Anti-patterns
| Don't | Why | Instead |
|---|---|---|
| Bare timestamp or auto-increment as the leading key bytes with many writers | Every write hits the same end of the key space | Lead with entity id or a small hash bucket |
| Little-endian or unpadded decimal numbers in keys | Sorts wrongly | Big-endian fixed width |
| One huge row for unbounded data | Row reads grow without limit | Bucket rows by time or hash |
| Read-modify-write counters (`get`, add, `put`) | Races, extra reads | `incr` in a counter family |
| One family per attribute | Many trees to flush and compact | Qualifiers inside a few families by access pattern |
| Large payload in the same family as hot metadata | Scans of metadata decode payload blocks | Separate family, or a `blob_threshold` below the payload size |
| Value predicates as a query engine over big ranges | They save materialization, not block reads | An inverted row keyed by the value |
| Long-lived snapshots | Pin memory and space | Take, read, drop |
| `scan` with different-length byte-array literals (`b"a"..b"bcd"`) | Does not compile (end types differ) | Slices, `scan_prefix` or `scan_bounds` |
| Relying on cross-family or cross-row atomicity beyond a row without a batch | Only a single row, or a `WriteBatch`, is atomic | Put it in one row, or use `write_batch()` |
| Multi-row read-then-write invariants | Separate reads and writes race | `Transaction` (optimistic, retry on `Conflict`), or one row and `commit_if` |

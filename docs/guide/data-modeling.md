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
| Small, hot attributes (status, counters) | `max_versions(1)`, `Priority::High`, `uncompressed()` if tiny |
| Sparse or wide sets (links, tags) | `bloom_bits(10)` |
| Large payloads read rarely | its own family; scan other families without touching it. Phase 2: `blob_threshold` |
| Data that should expire | `ttl(days(n))`; add `Compaction::FifoByTime` to drop whole files |

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
- `Compaction::FifoByTime` (with a TTL) drops expired files wholesale, the cheapest way to age out append-only data. It keeps files in arrival order and merges only small neighbours, so it suits data written roughly in time order; a write with an old explicit timestamp lands in a new file and lives until that file expires. Expiry is checked when the family flushes or compacts, and expired cells are hidden from reads in the meantime.
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

## Counters with merge operators
`incr` adds to an `i64` **without reading it**: the write is a blind merge operand resolved at read and compaction time. Concurrent writers never lose updates.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta"])?;
pages.mutate(b"com.example/a").incr("meta", b"hits", 1).commit()?;

let hits = pages.get(b"com.example/a", "meta", b"hits")?.and_then(|c| c.as_i64());
# assert_eq!(hits, Some(1));
# Ok::<(), pigeonhole::Error>(())
```
- The built-in `pigeonhole.i64_add` is the default operator. A missing counter counts as 0; overflow wraps.
- Write a counter column **only** with `incr` (and `put_i64` to set or reset a base). Mixing arbitrary `put` bytes into a counter column can make resolution fail with `ErrorCode::MergeFailed`.
- Counters are for totals. For per-period counts, use one column per period (`2026-10-05`) and `incr` each.
- Each `incr` is a separate operand until compaction folds the counter. A read adds up the operands it finds, at about 30 ns each (~3 µs for 100 increments, ~0.3 ms for 10,000). Compaction folds a counter's operands, and its last `put_i64`, into one value once they reach the bottom level, provided the family has no TTL and no snapshot older than them is open ([#34](https://github.com/CodingAnarchy/pigeonhole/issues/34)). A hot counter's recent increments stay separate until then. After a fold, the counter is one version at the newest increment's timestamp. A later `put_at` or `delete_cell` with an explicit timestamp inside the folded span acts on that version, not on the individual increments: `delete_cell` at the newest increment's timestamp hides the whole folded counter, at an older increment's timestamp it hides nothing, and `put_at` there writes an older version below the counter. Avoid explicit timestamps on counter columns. For a counter incremented millions of times between compactions, or in a family with a TTL (operands then expire one by one and are never folded), spread the count over time-bucketed columns.
- Do not use `time_range` to window a counter; operands are never dropped by pushdown (see [Scans and filters](scans-and-filters.md)).
- **Phase 2:** custom operators. Implement `pigeonhole::MergeOperator` (an associative fold: `merge(acc, older)` then `finish(base, acc)`), register it with `Options::merge_operator(Arc::new(op))`, and name it on the family with `Family::merge_operator("name")`, then write operands with `RowMutation::merge`. The operator's name is stored in the file; opening without it registered fails with `ErrorCode::UnknownMergeOperator` unless you set `Options::allow_unregistered_merge_operators(true)` (read-only, compaction off). Operators must be associative; non-associative operators are not supported.

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
| Read-modify-write counters (`get`, add, `put`) | Races, extra reads | `incr` |
| One family per attribute | Many trees to flush and compact | Qualifiers inside a few families by access pattern |
| Large payload in the same family as hot metadata | Scans of metadata decode payload blocks | Separate family (Phase 2: blob threshold) |
| Value predicates as a query engine over big ranges | They save materialization, not block reads | An inverted row keyed by the value |
| Long-lived snapshots | Pin memory and space | Take, read, drop |
| `scan` with different-length byte-array literals (`b"a"..b"bcd"`) | Does not compile (end types differ) | Slices, `scan_prefix` or `scan_bounds` |
| Relying on cross-family or cross-row atomicity beyond a row without a batch | Only a single row, or a `WriteBatch`, is atomic | Put it in one row, or use `write_batch()` |
| Multi-row read-then-write invariants | Separate reads and writes race | `Transaction` (optimistic, retry on `Conflict`), or one row and `commit_if` |

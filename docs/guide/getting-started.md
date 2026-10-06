# Getting started

> **Status: Phase 1 sync API implemented.** Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup). Track progress in [`../status.md`](../status.md). Features from later phases are labeled with their phase; [the last section](#what-the-current-build-does-not-do-yet) lists what the current build does not do yet.

## Install
Pigeonhole is not published to crates.io yet. Depend on it from git:

```toml
[dependencies]
pigeonhole = { git = "https://github.com/CodingAnarchy/pigeonhole" }
```

Requirements: Rust 2024 edition, MSRV 1.96. The blocking API needs no async runtime. The `async` feature (Phase 3) is off by default and currently gates an empty module.

## Open a database
```rust
use pigeonhole::{days, Durability, Family, Options, Pigeonhole};

# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("crawl.phdb"), Options::default())?;
# Ok::<(), pigeonhole::Error>(())
```

- `open` creates the file if missing (`Options::create_if_missing`, default true) and takes the **writer lock**. A second writer, in this or any other process, fails with `ErrorCode::WriterLocked`.
- `Options::default()` is a valid configuration. Options are process-local and not stored in the file, so reopening with different options changes them.
- `Pigeonhole` is cheap to clone; every clone shares the same engine. Pass clones to threads.
- Opening replays the WAL sidecar files; there is no full-file recovery scan. While the database is open you will see sidecar files next to it. When the last handle closes cleanly, only the one file remains (once SST flushes land; until then the WAL sidecars stay, see the last section).
- The database must be on a local filesystem. Network filesystems fail with `ErrorCode::NetworkFilesystem`.

Common options:

```rust
# use pigeonhole::{Durability, Options, Pigeonhole};
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(
    dir.join("ingest.phdb"),
    Options::default()
        .durability(Durability::Buffered) // writer default; see durability.md
        .shards(1)                        // shard threads; default is the CPUs available
        .block_cache(256 << 20)           // bytes
        .row_cache(0),                    // bytes; 0 (default) disables
)?;
# Ok::<(), pigeonhole::Error>(())
```

## Create a table with families
Only families are declared. Qualifiers (columns) are created on write.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let pages = db
    .table("pages")?
    .family("meta", Family::default().max_versions(1))
    .family("links", Family::default().bloom_bits(10))
    .family("body", Family::default().ttl(days(30)))
    .create_if_missing()?;
# assert_eq!(pages.families(), ["meta", "links", "body"]);
# Ok::<(), pigeonhole::Error>(())
```

`db.table(name)` returns a `TableBuilder`. Finish it with one of:

| Method | Behavior |
|---|---|
| `create_if_missing()` | Open the table, creating it and any missing declared families. |
| `create()` | Create it; `ErrorCode::TableExists` if it exists. |
| `open()` | Open an existing table; `ErrorCode::TableNotFound` if absent. |

On an existing table, a declared family that is not yet present is added (cheap). A family that already exists **keeps its stored options**; the options you pass are ignored for it.

The returned `Table` is cheap to clone and `Send + Sync`. Also available: `db.tables()`, `db.drop_table(name)`, `table.name()`, `table.families()`.

`Family` settings you will use first: `max_versions(n)` (0 keeps all), `ttl(Duration)`, `bloom_bits(u8)`, `lz4()` (default), `uncompressed()`, `block_size(u32)`, `cache_priority(Priority)`. Phase 2: `zstd(level)` and `compaction(Compaction::Tiered | FifoByTime)` are refused with `ErrorCode::Unsupported` when the table or family is created; `blob_threshold(bytes)` is stored but values stay inline; a custom `merge_operator(name)` fails with `ErrorCode::UnknownMergeOperator`.

## Write one row atomically
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body"])?;
let info = pages
    .mutate(b"com.example/a")
    .put("meta", b"status", b"200")
    .put("links", b"com.example/b", b"")
    .incr("meta", b"hits", 1)
    .delete_column("meta", b"etag")
    .commit()?;

println!("seqno {} at {:?}", info.seqno, info.durability);
# Ok::<(), pigeonhole::Error>(())
```

- A `RowMutation` is **all-or-nothing across every family of that row**.
- Builder calls cannot fail. Problems (unknown family, key over 64 KiB, value too large) surface from `commit()`.
- `commit()` returns `CommitInfo { seqno, durability }`: the sequence number, and the durability level actually applied. It returns only once your write is durable at that level **and** visible to reads, so you always read your own write.
- Variants: `put_at(family, qualifier, ts, value)` for event time (timestamps are `u64` microseconds since the Unix epoch), `put_i64`, `put_f64`, `delete_cell(family, qualifier, ts)`, `delete_family(family)`, `delete_row()`, `.durability(d)` to override durability for this commit.
- `incr(family, qualifier, delta)` adds to an `i64` counter without reading it first. The built-in `pigeonhole.i64_add` operator is the default, so no registration is needed.

## Read
### One cell
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body"])?;
# pages.mutate(b"com.example/a").put("meta", b"status", b"200").incr("meta", b"hits", 3).put("links", b"org.example/x", b"").commit()?;
# pages.mutate(b"com.example/b").put("links", b"org.example/y", b"").commit()?;
if let Some(cell) = pages.get(b"com.example/a", "meta", b"status")? {
    let bytes: &[u8] = cell.value();
    let ts: u64 = cell.timestamp();
#   assert_eq!(bytes, b"200");
#   assert!(ts > 0);
}

let hits: Option<i64> = pages
    .get(b"com.example/a", "meta", b"hits")?
    .and_then(|c| c.as_i64());
# assert_eq!(hits, Some(3));
# Ok::<(), pigeonhole::Error>(())
```
`get` returns the newest version as a `CellRef` that borrows from the cache; reading it allocates nothing. Call `cell.to_owned()` for a `Cell` that outlives the borrow (still no copy of the value).

### One row
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body"])?;
# pages.mutate(b"com.example/a").put("meta", b"status", b"200").incr("meta", b"hits", 3).put("links", b"org.example/x", b"").commit()?;
# pages.mutate(b"com.example/b").put("links", b"org.example/y", b"").commit()?;
let row = pages
    .row(b"com.example/a")
    .families(["meta"])
    .latest()
    .read()?;                      // Result<Option<RowRef<'_>>>

if let Some(row) = row {
    for e in row.iter() {
        // e.family: &str, e.qualifier: &[u8], e.cell: CellRef<'_>
#       assert_eq!(e.family, "meta");
    }
    let status = row.get("meta", b"status");
#   assert_eq!(status.unwrap().value(), b"200");
}
# Ok::<(), pigeonhole::Error>(())
```
`read()` returns `None` if the row has no matching cell. Cells are ordered by family (in the order the families were created, or the order you listed them with `family(..)`), then qualifier, then newest version first. `row.to_owned()` gives an owned `Row`.

### A range of rows
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body"])?;
# pages.mutate(b"com.example/a").put("meta", b"status", b"200").incr("meta", b"hits", 3).put("links", b"org.example/x", b"").commit()?;
# pages.mutate(b"com.example/b").put("links", b"org.example/y", b"").commit()?;
let snap = db.snapshot()?;
for row in pages
    .scan_prefix(b"com.example/")
    .family("links")
    .qualifier_prefix(b"org.")
    .snapshot(&snap)
    .iter()?
{
    let row = row?;                // Row (owned, cheap)
    println!("{:?}: {} cells", row.key(), row.len());
}
# Ok::<(), pigeonhole::Error>(())
```
The iterator yields `Result<Row>`. For zero-copy rows use the cursor form:

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body"])?;
# pages.mutate(b"com.example/a").put("meta", b"status", b"200").incr("meta", b"hits", 3).put("links", b"org.example/x", b"").commit()?;
# pages.mutate(b"com.example/b").put("links", b"org.example/y", b"").commit()?;
let mut it = pages.scan_prefix(b"com.example/").iter()?;
while let Some(row) = it.next_ref()? {
    // row: RowRef<'_>, valid until the next call
#   assert!(row.key().starts_with(b"com.example/"));
}
# Ok::<(), pigeonhole::Error>(())
```

`Table::scan` takes a range over byte strings. Both ends must have the **same type**, so `b"a"..b"bcd"` (arrays of different length) does not compile. Use slices, or the explicit-bounds form:

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body"])?;
use std::ops::Bound;
let rows = pages.scan(&b"com.example/"[..]..&b"com.example0"[..]);
let rows = pages.scan_bounds(Bound::Included(&b"a"[..]), Bound::Excluded(&b"b"[..]));
# Ok::<(), pigeonhole::Error>(())
```

See [Scans and filters](scans-and-filters.md) for everything a scan can do.

## Write many rows at once
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body"])?;
let mut wb = db.write_batch();
wb.put(&pages, b"com.example/c", "meta", b"status", b"404")
  .incr(&pages, b"com.example/c", "meta", b"hits", 1)
  .delete_row(&pages, b"com.example/old");
let info = wb.commit_with(Durability::GroupSync)?;
# Ok::<(), pigeonhole::Error>(())
```
A `WriteBatch` spans any rows and tables, is atomic across all of them, and has **one durability point**. `commit()` uses the writer default; `commit_with(d)` overrides it for this commit. Builder methods take `&mut self` and return `&mut Self`, so chain them or call them in a loop. `commit` consumes the batch.

## Close
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
db.close()?;
# Ok::<(), pigeonhole::Error>(())
```
If this is the last handle open anywhere, `close` checkpoints the WAL and removes the sidecar and shared-memory files, leaving one file (the current build keeps the WAL sidecars; see below). Dropping the last clone does the same but ignores errors, so call `close()` when you want to know about failures.

## Maintenance
`db.flush()` writes every memtable to the file. `db.compact()` compacts every table fully. `db.backup(dest)` writes a consistent single-file copy while writes continue. In the current build only `flush` works, and only partly; see below.

## What the current build does not do yet
The storage engine writes memtables to SSTs in the file in the second half of Phase 1. Until then:

| Feature | Current behavior |
|---|---|
| `db.flush()` | Freezes the memtables; nothing is written to the file. Data stays durable through the WAL. |
| `db.compact()`, `db.backup(dest)` | Fail with `ErrorCode::Unsupported`. |
| Clean close | Leaves the WAL sidecar files next to the database (the data has nowhere else to go); reopening replays them. |
| `Durability::None` commits | Lost at the next close or crash, even when a later stronger commit returned; [#50](https://github.com/CodingAnarchy/pigeonhole/issues/50) makes a later stronger commit cover them (see [Durability](durability.md#mixed-levels)). |
| Memory bound | Until the engine flushes memtables to SSTs ([#37](https://github.com/CodingAnarchy/pigeonhole/issues/37)), everything written stays in the memtable arenas: total data ≤ `memtable_budget` × shards, and less in practice, since every version and delete marker counts and each table lives on one shard until tablets split. Beyond it, commits fail with `Busy`. Reopening with a `memtable_budget` too small for the data in the WAL fails with `InvalidArgument`. Size `Options::memtable_budget` for your data. |

Later phases:

| Feature | Phase |
|---|---|
| zstd, blob separation, `Compaction::Tiered`/`FifoByTime`, custom merge operators | 2 |
| `async` front door (`get_async`, `Scan::stream`, `commit_async`) | 3 |

Available ahead of their phase: `RowMutation::commit_if` (P2), `Transaction` and reader processes (`open_reader`) (P4).

## Next
[Durability](durability.md) · [Scans and filters](scans-and-filters.md) · [Data modeling](data-modeling.md) · [Errors](errors.md) · [Agent reference](agent-reference.md)

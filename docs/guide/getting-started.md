# Getting started

> **Status: API frozen; implementation in progress (Phase 1).** Every signature below exists in the `pigeonhole` crate, but the bodies are not implemented yet, so the samples are marked `rust,ignore` and will not run until Phase 1 lands. Track progress in [`../status.md`](../status.md). Features from later phases are labeled with their phase.

## Install
Pigeonhole is not published to crates.io yet. Depend on it from git:

```toml
[dependencies]
pigeonhole = { git = "https://github.com/CodingAnarchy/pigeonhole" }
```

Requirements: Rust 2024 edition, MSRV 1.96. The blocking API needs no async runtime. The `async` feature (Phase 3) is off by default and currently gates an empty module.

## Open a database
```rust,ignore
use pigeonhole::{days, Durability, Family, Options, Pigeonhole};

let db = Pigeonhole::open("crawl.phdb", Options::default())?;
```

- `open` creates the file if missing (`Options::create_if_missing`, default true) and takes the **writer lock**. A second writer, in this or any other process, fails with `ErrorCode::WriterLocked`.
- `Options::default()` is a valid configuration. Options are process-local and not stored in the file, so reopening with different options changes them.
- `Pigeonhole` is cheap to clone; every clone shares the same engine. Pass clones to threads.
- Opening replays the WAL sidecar files; there is no full-file recovery scan. While the database is open you will see sidecar files next to it. When the last handle closes cleanly, only the one file remains.
- The database must be on a local filesystem. Network filesystems fail with `ErrorCode::NetworkFilesystem`.

Common options:

```rust,ignore
let db = Pigeonhole::open(
    "ingest.phdb",
    Options::default()
        .durability(Durability::Buffered) // writer default; see durability.md
        .shards(1)                        // shard threads; default is the CPUs available
        .block_cache(256 << 20)           // bytes
        .row_cache(0),                    // bytes; 0 (default) disables
)?;
```

## Create a table with families
Only families are declared. Qualifiers (columns) are created on write.

```rust,ignore
let pages = db
    .table("pages")?
    .family("meta", Family::default().max_versions(1))
    .family("links", Family::default().bloom_bits(10))
    .family("body", Family::default().ttl(days(30)))
    .create_if_missing()?;
```

`db.table(name)` returns a `TableBuilder`. Finish it with one of:

| Method | Behavior |
|---|---|
| `create_if_missing()` | Open the table, creating it and any missing declared families. |
| `create()` | Create it; `ErrorCode::TableExists` if it exists. |
| `open()` | Open an existing table; `ErrorCode::TableNotFound` if absent. |

On an existing table, a declared family that is not yet present is added (cheap). A family that already exists **keeps its stored options**; the options you pass are ignored for it.

The returned `Table` is cheap to clone and `Send + Sync`. Also available: `db.tables()`, `db.drop_table(name)`, `table.name()`, `table.families()`.

`Family` settings you will use first: `max_versions(n)` (0 keeps all), `ttl(Duration)`, `bloom_bits(u8)`, `lz4()` (default), `uncompressed()`, `block_size(u32)`, `cache_priority(Priority)`. Phase 2: `zstd(level)`, `blob_threshold(bytes)`, `compaction(Compaction::Tiered | FifoByTime)`, custom `merge_operator(name)`.

## Write one row atomically
```rust,ignore
let info = pages
    .mutate(b"com.example/a")
    .put("meta", b"status", b"200")
    .put("links", b"com.example/b", b"")
    .incr("meta", b"hits", 1)
    .delete_column("meta", b"etag")
    .commit()?;

println!("seqno {} at {:?}", info.seqno, info.durability);
```

- A `RowMutation` is **all-or-nothing across every family of that row**.
- Builder calls cannot fail. Problems (unknown family, key over 64 KiB, value too large) surface from `commit()`.
- `commit()` returns `CommitInfo { seqno, durability }`: the sequence number, and the durability level actually applied. It returns only once your write is durable at that level **and** visible to reads, so you always read your own write.
- Variants: `put_at(family, qualifier, ts, value)` for event time (timestamps are `u64` microseconds since the Unix epoch), `put_i64`, `put_f64`, `delete_cell(family, qualifier, ts)`, `delete_family(family)`, `delete_row()`, `.durability(d)` to override durability for this commit.
- `incr(family, qualifier, delta)` adds to an `i64` counter without reading it first. The built-in `pigeonhole.i64_add` operator is the default, so no registration is needed.

## Read
### One cell
```rust,ignore
if let Some(cell) = pages.get(b"com.example/a", "meta", b"status")? {
    let bytes: &[u8] = cell.value();
    let ts: u64 = cell.timestamp();
}

let hits: Option<i64> = pages
    .get(b"com.example/a", "meta", b"hits")?
    .and_then(|c| c.as_i64());
```
`get` returns the newest version as a `CellRef` that borrows from the cache; reading it allocates nothing. Call `cell.to_owned()` for a `Cell` that outlives the borrow (still no copy of the value).

### One row
```rust,ignore
let row = pages
    .row(b"com.example/a")
    .families(["meta"])
    .latest()
    .read()?;                      // Result<Option<RowRef<'_>>>

if let Some(row) = row {
    for e in row.iter() {
        // e.family: &str, e.qualifier: &[u8], e.cell: CellRef<'_>
    }
    let status = row.get("meta", b"status");
}
```
`read()` returns `None` if the row has no matching cell. Cells are ordered by family, qualifier, then newest version first. `row.to_owned()` gives an owned `Row`.

### A range of rows
```rust,ignore
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
```
The iterator yields `Result<Row>`. For zero-copy rows use the cursor form:

```rust,ignore
let mut it = pages.scan_prefix(b"com.example/").iter()?;
while let Some(row) = it.next_ref()? {
    // row: RowRef<'_>, valid until the next call
}
```

`Table::scan` takes a range over byte strings. Both ends must have the **same type**, so `b"a"..b"bcd"` (arrays of different length) does not compile. Use slices, or the explicit-bounds form:

```rust,ignore
use std::ops::Bound;
let rows = pages.scan(&b"com.example/"[..]..&b"com.example0"[..]);
let rows = pages.scan_bounds(Bound::Included(b"a"), Bound::Excluded(b"b"));
```

See [Scans and filters](scans-and-filters.md) for everything a scan can do.

## Write many rows at once
```rust,ignore
let mut wb = db.write_batch();
wb.put(&pages, b"com.example/c", "meta", b"status", b"404")
  .incr(&pages, b"com.example/c", "meta", b"hits", 1)
  .delete_row(&pages, b"com.example/old");
let info = wb.commit_with(Durability::GroupSync)?;
```
A `WriteBatch` spans any rows and tables, is atomic across all of them, and has **one durability point**. `commit()` uses the writer default; `commit_with(d)` overrides it for this commit. Builder methods take `&mut self` and return `&mut Self`, so chain them or call them in a loop. `commit` consumes the batch.

## Close
```rust,ignore
db.close()?;
```
If this is the last handle open anywhere, `close` checkpoints the WAL and removes the sidecar and shared-memory files, leaving one file. Dropping the last clone does the same but ignores errors, so call `close()` when you want to know about failures.

## Maintenance
`db.flush()` writes every memtable to the file. `db.compact()` compacts every table fully. `db.backup(dest)` writes a consistent single-file copy while writes continue.

## What is not available yet
| Feature | Phase |
|---|---|
| zstd, blob separation, `Compaction::Tiered`/`FifoByTime`, custom merge operators, `RowMutation::commit_if` | 2 |
| `async` front door (`get_async`, `Scan::stream`, `commit_async`) | 3 |
| Reader processes (`open_reader`), `Transaction` | 4 |

## Next
[Durability](durability.md) · [Scans and filters](scans-and-filters.md) · [Data modeling](data-modeling.md) · [Errors](errors.md) · [Agent reference](agent-reference.md)

# Getting started

> **Status: Phase 1 sync API implemented.** Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup). Track progress in [`../status.md`](../status.md). Features from later phases are labeled with their phase; [the last section](#what-the-current-build-does-not-do-yet) lists what the current build does not do yet.

## Install
Pigeonhole is published on [crates.io](https://crates.io/crates/pigeonhole). Add it with `cargo add pigeonhole`, or in `Cargo.toml`:

```toml
[dependencies]
pigeonhole = "0.1"
```

This is an experimental 0.x release: the on-disk format and the API may change before 1.0 (see [the maturity note](README.md)). The full API reference is on [docs.rs](https://docs.rs/pigeonhole); this guide covers concepts and usage, and the [agent reference](agent-reference.md) is the one-page summary.

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
- Opening replays the WAL sidecar files; there is no full-file recovery scan. Opening is cheap: a small database opens in milliseconds and writes well under a megabyte, however many shards it has, and the amount of WAL to replay is bounded because each shard flushes data that would otherwise pin its log (twice `memtable_budget` of log at most). While the database is open you will see sidecar files next to it. When the last handle closes cleanly, only the one file remains: the close flushes every memtable into the file, checkpoints the WAL and removes the sidecars.
- The database must be on a local filesystem. Network filesystems fail with `ErrorCode::NetworkFilesystem`.
- **Shared memory.** The writer creates a shared-memory region of `memtable_budget × shards` plus about 10 MiB for views and reader slots (64 MiB per shard by default, so 266 MiB on 4 CPUs). It takes memory only as the memtables fill, up to that size, and is released at close. On Linux it lives in `/dev/shm`, which Docker and Kubernetes cap at 64 MiB by default. `open` checks that the region's filesystem has that much free space; if not, it fails with `ErrorCode::ShmUnavailable`, and the message says how much it needs and where. Then enlarge `/dev/shm` (`docker run --shm-size=1g`, or in Kubernetes an `emptyDir` with `medium: Memory` mounted at `/dev/shm`), point `Options::shm_dir` at a larger tmpfs, or lower `memtable_budget` or `shards`. The check does not hold the space: if another process fills that tmpfs after the open, a write that needs more of the region can crash the process with `SIGBUS`. Give the database a `/dev/shm` (or `shm_dir`) with room to spare.
- **Threads are not pinned** to CPUs by default, so several databases or containers on one host share the CPUs cleanly. On a host or cpuset dedicated to one database, `Options::pin_threads(true)` pins shard `i` to the `i`-th CPU the opening thread may run on.

Common options:

```rust
# use pigeonhole::{Durability, Options, Pigeonhole};
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(
    dir.join("ingest.phdb"),
    Options::default()
        .durability(Durability::Buffered) // writer default; see durability.md
        .shards(1)                        // shard threads; default is the CPUs available
        .memtable_budget(16 << 20)        // write buffer per shard; shm = budget × shards
        .block_cache(256 << 20)           // bytes; 256 MiB is the default
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

`Family` settings you will use first: `max_versions(n)` (0 keeps all), `ttl(Duration)`, `bloom_bits(u8)`, `lz4()` (default), `uncompressed()`, `block_size(u32)`, `cache_priority(Priority)`, `compaction(Compaction::Leveled | Tiered | FifoByTime)`. Phase 2: `zstd(level)` is refused with `ErrorCode::Unsupported` when the table or family is created; `blob_threshold(bytes)` is stored but values stay inline; `merge_operator(name)` names an operator registered with `Options::merge_operator` (an unregistered name fails with `ErrorCode::UnknownMergeOperator`).

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
If this is the last handle open anywhere, `close` checkpoints the WAL and removes the sidecar and shared-memory files, leaving one file. A crash instead leaves the sidecars, and the next open replays them. Dropping the last clone does the same but ignores errors, so call `close()` when you want to know about failures.

## Maintenance
The database lives on disk: memtables are flushed into the file as they fill, and the size of your data is limited by the disk, not by memory. `Options::memtable_budget` (per shard, default 64 MiB) only sizes the in-memory write buffer. You rarely need to call anything below; background flushes and compactions run on their own.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta"])?;
pages.mutate(b"row").put("meta", b"k", b"v").durability(Durability::None).commit()?;

// Writes every memtable into the file and returns once the data is there.
// Even a `None` commit survives a crash from here on.
db.flush()?;

// Merges every level of every table into the last one: drops versions beyond
// `max_versions`, expired cells and covered tombstones (no snapshot can still see them).
db.compact()?;

// Returns free space at the end of the file to the filesystem; the result is the
// number of bytes released.
let released = db.shrink()?;
println!("released {released} bytes");

// A consistent single-file copy of everything committed so far; writers keep running.
// The copy opens on its own, with no sidecar files.
db.backup(dir.join("guide-backup.phdb"))?;
let copy = Pigeonhole::open(dir.join("guide-backup.phdb"), Options::default().create_if_missing(false))?;
let pages_copy = copy.table("pages")?.open()?;
assert_eq!(pages_copy.get(b"row", "meta", b"k")?.unwrap().value(), b"v");
# drop(pages_copy);
# copy.close()?;
# Ok::<(), pigeonhole::Error>(())
```

- `flush()` and `compact()` return after the work is in the file; both fail with `ErrorCode::Closed` after `close`. They report only their own failure: `compact()` returns the error of the compaction it started, and a background flush or compaction that failed earlier (it is retried after a growing backoff) is never reported to a later call. If the stall timeout (`Options::write_stall_timeout`, 30 s by default) passes while a call waits for room, it fails with the transient `ErrorCode::Busy`.
- `backup(dest)` writes a new file at `dest`, which must not exist, from a snapshot taken when you call it, so it holds exactly the commits visible at that moment. Commits that land while it runs are not in it. It holds that snapshot, memtables included, until it finishes, so on a large file under heavy writes the arena can fill and writers fail with `Busy` after the 30 s stall timeout; back up when load is light. A database whose families store blob files cannot be backed up yet (`ErrorCode::Unsupported`); in the current build values stay inline, so this does not occur.
- The file does not shrink by itself: space freed by compaction is reused by later writes, but the file keeps its length. Call `compact()` and then `shrink()` to give space back to the filesystem.
- `shrink()` returns the bytes released (`0` when there is nothing to release). It cuts off the free space at the end of the file, moves live data from the file's tail into free space nearer the start, then truncates again, so it costs a read and rewrite of that data. It runs online: other threads keep reading and writing. Space still held by an open snapshot or scan is released on a later call after you drop it. It fails with `ErrorCode::Closed` after `close`, `ErrorCode::NoSpace` if the disk is full when it moves the manifest (which needs a few KiB first), and `ErrorCode::Io` on a disk failure. Use it after a large delete, not routinely.
- The file cannot end before its live data packed toward the start. Data lives in extents of a power of two from 64 KiB to 64 MiB, each aligned to its size, after a 64 KiB header, so a file holding a 64 MiB SST stays at least 128 MiB however few rows it has. Data with no free extent of its size below it stays put; `shrink` does not report that as an error.

## What happens when writes outrun the disk
Reopening after a crash with fewer shards or a smaller `memtable_budget` than before works: if the WAL's unflushed data does not fit the memtable arenas, the open writes it to the file as it replays, which makes that open slower. A write that finds the memtable arena full waits (a write stall) while a flush frees room. `ErrorCode::Busy` means the wait ran past the stall timeout (`Options::write_stall_timeout`, 30 s by default): it is **transient**, so back off and retry. A single batch that can never fit a shard's arena (more than about half of it) fails at once with `ErrorCode::BatchTooLarge`, which never succeeds on retry: split the batch or raise `Options::memtable_budget`. See [Errors](errors.md).

## What the current build does not do yet

| Feature | Current behavior |
|---|---|
| `Durability::None` commits | Durable once flushed (`flush`, a clean close, or a background flush), or once a later stronger commit on the same shard returns (decision D94, see [Durability](durability.md#mixed-levels)). A crash before either loses them. |
| `zstd`, blob separation | Phase 2. |

Later phases:

| Feature | Phase |
|---|---|
| zstd, blob separation | 2 |
| `async` front door (`get_async`, `Scan::stream`, `commit_async`) | 3 |

Available ahead of their phase: `RowMutation::commit_if` (P2), `Transaction` and reader processes (`open_reader`) (P4).

## Next
[Durability](durability.md) · [Scans and filters](scans-and-filters.md) · [Data modeling](data-modeling.md) · [Errors](errors.md) · [Agent reference](agent-reference.md)

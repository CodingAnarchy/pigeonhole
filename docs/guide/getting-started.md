# Getting started

> **Status:** this guide describes `main`; the current release on crates.io is 0.3.4, and the [changelog](../../CHANGELOG.md) and [`changelog.d/`](../../changelog.d/README.md) list what `main` adds. Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup). Track progress in [`../status.md`](../status.md). Features from later phases are labeled with their phase; [the last section](#what-the-current-build-does-not-do-yet) lists what the current build does not do yet.

## Install
Pigeonhole is published on [crates.io](https://crates.io/crates/pigeonhole). Add it with `cargo add pigeonhole`, or in `Cargo.toml`:

```toml
[dependencies]
pigeonhole = "0.3"
```

This is an experimental 0.x release: the API may change before 1.0, and the file format is frozen for 1.x at format 4 (see [the maturity note](README.md) and [file format and upgrades](concepts.md#file-format-and-upgrades)). Upgrading from 0.2.x: the changelog's [Upgrading from 0.2.x](../../CHANGELOG.md#upgrading-from-02x) lists the behaviour changes you will notice (pinned threads, apply-time visibility, `OutcomeUnknown`, `Busy`, application-owned loops). From 0.1.0, [Migrating from 0.1.0](../../CHANGELOG.md#migrating-from-010) comes first (counter families, the version 2 file format, the FUSE opt-in). The full API reference is on [docs.rs](https://docs.rs/pigeonhole); this guide covers concepts and usage, and the [agent reference](agent-reference.md) is the one-page summary.

This guide follows `main`, whose API is frozen for 1.0 (ICR 0085–0088). The 0.3.x releases have the API from before the freeze: for example `Family::lz4()` and `zstd(level)` instead of `compression(..)`, and `open_application_owned` returning `(Pigeonhole, Vec<Shard>)` instead of `AppOwned`. The changelog lists every breaking change.

Requirements: Rust 2024 edition, MSRV 1.96. The blocking API needs no async runtime. The `async` feature is on by default (since 0.3.0; off in 0.2.x) and adds an async form of every data operation (see [Async](async.md)); `default-features = false` drops it.

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
- Opening replays the WAL sidecar files; there is no full-file recovery scan. Opening is cheap: a small database opens in milliseconds and writes well under a megabyte, however many shards it has, and the amount of WAL to replay is bounded because each shard flushes data that would otherwise pin its log (twice `memtable_budget` of log at most). After a clean close the open waits for no flush: a `GroupSync` or `Sync` commit right after it waits instead for the new WAL files' syncs, while reads and weaker commits do not (D203). While the database is open you will see sidecar files next to it. When the last handle closes cleanly, only the one file remains: the close flushes every memtable into the file, checkpoints the WAL and removes the sidecars.
- The database must be on a local filesystem. Network and cluster filesystems, and FUSE mounts on Linux (macFUSE mounts macOS does not mark local), fail with `ErrorCode::NetworkFilesystem`: their byte-range locks may not exclude a second writer on another host. For a **local** FUSE mount you trust (ntfs-3g, an encrypted home directory such as gocryptfs), `Options::allow_fuse(true)` (and `ReaderOptions::allow_fuse(true)` in reader processes) accepts FUSE, and only FUSE. You then take on its risks: its locks may be local to one host, and a sync may not reach stable storage. Never set it for network-backed FUSE (sshfs, s3fs, gcsfuse).
- **Shared memory.** The writer creates a shared-memory region of `memtable_budget × shards` plus about 12 MiB for views, reader slots and row epochs (64 MiB per shard by default, so 268 MiB with 4 shards). It takes memory only as the memtables fill, up to that size, and is released at close. On Linux it lives in `/dev/shm`, which Docker and Kubernetes cap at 64 MiB by default. `open` checks that the region's filesystem has that much free space; if not, it fails with `ErrorCode::ShmUnavailable`, and the message says how much it needs and where. Then enlarge `/dev/shm` (`docker run --shm-size=1g`, or in Kubernetes an `emptyDir` with `medium: Memory` mounted at `/dev/shm`), point `Options::shm_dir` at a larger tmpfs, or lower `memtable_budget` or `shards`. The check does not hold the space: if another process fills that tmpfs after the open, a write that needs more of the region can crash the process with `SIGBUS`. Give the database a `/dev/shm` (or `shm_dir`) with room to spare.
- **Memory at the defaults.** A writer's memory grows with the core count, because each shard has its own write buffer. With every option at its default it can use up to:
  - **write buffers:** `memtable_budget` (64 MiB) per shard, one shard per physical core, plus about 12 MiB for views, reader slots and row epochs (the shared-memory region above);
  - **block cache:** 256 MiB (`block_cache`);
  - **row cache:** 64 MiB plus about 9% for its write watermarks, about 70 MiB (`row_cache`).

  | Physical cores | Write buffers | Caches | Up to |
  |--:|--:|--:|--:|
  | 4 | 268 MiB | 326 MiB | about 0.6 GiB |
  | 16 | 1,036 MiB | 326 MiB | about 1.3 GiB |
  | 64 | 4,108 MiB | 326 MiB | about 4.3 GiB |

  These are ceilings: the write buffers fill only under write load and the caches only as reads miss, so a lightly used database stays far below them. Each reader process adds its own block and row caches. On a small device or a shared host, set `shards`, `memtable_budget`, `block_cache` and `row_cache` explicitly. A single memory budget is planned after 1.0 (#1041).
- **Threads: start with `cpu_profile`.** `Options::cpu_profile` says who the machine's (or container's) CPUs are for, and from that the engine sizes its threads: up to a shard thread per shard, as few as the load needs, and a share of the cores for flush and compaction (D289). For most databases this is the only thread setting to choose. `shards`, `shard_threads` and `compaction_cores` below override one part of it each; an explicit value always wins over the profile.
  - **`CpuProfile::Embedded` (the default)** leaves CPU for your application in the same process.
  - **`CpuProfile::Server`** is for a database whose clients are elsewhere, such as another container or over the network.
  - **Physical cores** means those the process may use: an affinity mask or a container's CPU limit narrows them.

  | Physical cores | Shard threads (default: at most, sized to the load) | Flush and compaction, Embedded | Flush and compaction, Server |
  |--:|--:|--:|--:|
  | 2 | 2 | 1 | 1 |
  | 4 | 4 | 1 | 2 |
  | 8 | 8 | 2 | 4 |
  | 16 | 16 | 4 | 8 |
  | 64 | 64 | 16 | 32 |

- **Override: how many shards (`shards`).** The default is one shard per physical core (SMT siblings count once: two shards on one core's hyperthreads slow each other down, D220), which suits many threads writing at once. Each write is applied by the shard that owns its row, so with only a few writing threads their writes spread thinly over many shards: each shard sits idle between commits, and a commit that finds its shard asleep pays to wake it (a few microseconds on bare metal and macOS, tens on some virtual machines). Engine-owned shards handle that themselves: while writes are light, the balancer keeps the written tablets on just enough shards to stay about half busy, and spreads them again within about a second once those shards pass 75% busy, or once any one of them is saturated (packing, D217, D251). A shard counts as busy while its thread works or while its group fsyncs are in flight, so durable commits that wait on the disk keep enough shards active (D248). Application-owned shards are never packed, so there set `Options::shards(n)` to about the number of threads writing concurrently (at least 1), up to the core count. Reads do not depend on the shard count (they run on the calling thread), and fewer shards also means a smaller shared-memory region.
- **Threads that run the shards (`adaptive_shard_threads`, `shard_threads`).** Engine-owned mode sizes the shard threads to the load by default (D289): it starts a thread per shard and moves a light load's shards onto fewer threads, which sleep less, so a commit waits less often for a sleeping thread; under load it spreads them out again. `Options::adaptive_shard_threads(false)` keeps a thread per shard, never resized.
  - **A fixed count:** `Options::shard_threads(k)` runs the shards on `k` threads, shard `i` on thread `i % k`, and turns the load sizing off. For a load you know is light and spread over many shards, a few busy threads sleep less, so a commit rarely finds its thread asleep and waits for the OS to wake it. On the reference machine, two threads took a light 16-shard store's put p99 from 270–550 µs to 31–35 µs (four threads didn't: 280–330 µs). A heavy load needs a thread per shard: at two threads transfer ran at about half its throughput.
- **Override: flush and compaction threads (`compaction_cores`).** Engine-owned mode runs flush and compaction on threads of their own: a quarter of the physical cores by default, at least one and at most one per shard (half with `CpuProfile::Server`; the table above; D252, D289). On a shard thread, each flush or compaction slice would hold up the commits queued behind it for up to half a millisecond, which on contended workloads is most of the write tail. They are pinned after the shards (to the shards' SMT siblings first), and run unpinned where too few CPUs are free for them as well as the shards. `Options::compaction_cores(k)` sets the count; `compaction_cores(0)` keeps the work on the shards, in short slices between commits, which saves threads on small or shared hosts at the cost of that tail. Application-owned mode starts no threads (D40), so there the work runs on the shards you drive, or on background workers you also drive (`Options::background_workers(n)`, handed out by `open_application_owned`, D284; see [Async](async.md)).
- **Shard threads are pinned** to CPUs of their own by default (D235): one per physical core first, inside the set the opening thread may run on (its affinity mask, cpuset or container), never outside it. Pinning never fails an open: where it can't help, threads run unpinned and `engine_metrics()` says how many are pinned and why not (`threads_started`, `threads_pinned`, `unpinned_reason`). That covers macOS (affinity there is only a hint), a CPU quota narrower than the affinity set, and fewer free CPUs than threads (with room for the shards but not the flush and compaction threads, only those run unpinned). Several databases in one process pin to different CPUs, and one that finds too few free runs unpinned. **Several processes on one host don't coordinate**: give each its own CPUs (an affinity mask or cpuset) or open with `Options::pin_threads(false)`. Application-owned shards run on your threads; pin those yourself.
- **Waiting for a commit.** A thread waiting for a `Buffered` or `None` commit polls for up to 50 µs before it sleeps, and an engine-owned shard that just handled a commit polls for up to 200 µs for the next one (D198, amended 2026-10-19), because a shard usually answers faster than a sleep and wake. Durable commits and idle databases do not poll, and polls that keep finding nothing poll less often.
- **I/O backend (Linux).** The default (`IoBackend::Auto`, D218) uses io_uring where the kernel offers it and a `pread` thread pool elsewhere. `IoBackend::Uring` fails the open where io_uring is unavailable; `IoBackend::Pread` forces the thread pool.
- **Direct I/O.** `Options::direct_io(true)` reads and writes SST and blob extents without the OS page cache, so the block cache is their only cache. It is off by default, and that suits most databases: buffered reads use whatever memory is free as cache, while direct reads past the block cache go to the device. Turn it on when the data is much larger than RAM *and* `block_cache` holds at least about 1/64 of the data (the SST indexes and filters it pins plus the index partitions, D215). Cold gets then cost one device read each without the page cache's copy and eviction work. On the reference machine (AX102, 64 GiB table, 1 GiB block cache), cold gets measured:

  | | direct | buffered |
  |---|---|---|
  | 1 thread, p50 / p99 | 88 / 205 µs | 92 / 264 µs |
  | 16 threads, p99 | 293 µs | 447 µs |
  | 16 threads, throughput | 130K gets/s | 122K gets/s |

  With a small block cache, direct I/O is much slower: a sparse-wide workload whose data fit in memory ran at 3.3K ops/s with direct I/O and a 16 MiB block cache, against 19K buffered.

Common options:

```rust
# use pigeonhole::{Durability, Options, Pigeonhole};
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(
    dir.join("ingest.phdb"),
    Options::default()
        .durability(Durability::Buffered) // the writer's default (GroupSync if unset); see durability.md
        .shards(1)                        // shards (data partitions); default is one per physical core
        .memtable_budget(16 << 20)        // write buffer per shard; shm = budget × shards
        .block_cache(256 << 20)           // bytes; 256 MiB is the default
        .row_cache(64 << 20),             // bytes; 64 MiB is the default, 0 disables
)?;
# Ok::<(), pigeonhole::Error>(())
```

**Row cache.** `Options::row_cache(bytes)` (64 MiB by default; 0 turns it off) keeps the newest version of each cell of small, hot family rows in memory. It serves latest row reads, and point gets at the latest point, at a snapshot or in a transaction (D290), without merging the row's memtables and SSTs. A family row is stored the second time a row read misses it, so rows read once are never stored. Point gets are served from the rows it holds but do not store rows themselves: storing a whole family row for one cell cost write-heavy point-get workloads (transfers, read-modify-writes) more at the tail than it saved (D290, amended after the A/B on #1059). It returns exactly what an uncached read would. A commit that only puts new values for cells the cached row already has moves the copy forward with them; any other write to the row (a delete, an increment, a new column) makes its copy miss until it is stored again (D290). It pays off when the same small rows are read again and again; each write pays one atomic per row it touches. Snapshot row reads, multi-version and time-range reads and scans do not use it (reader processes have a row cache of their own, see [Reader processes](readers.md)), and a family row larger than `Options::row_cache_max_row` (encoded bytes, default 4 KiB) is read as usual. `Options::row_cache_family(table, family)` limits it to the families that benefit. Check `Pigeonhole::row_cache_stats()`: if hits stay low next to misses, the cache only costs memory, and `row_cache(0)` gives it back. It is on by default since D216.

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
    .family("stats", Family::counter())
    .create_if_missing()?;
# assert_eq!(pages.families(), ["meta", "links", "body", "stats"]);
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

`Family` settings you will use first: `max_versions(n)` (0 keeps all), `ttl(Duration)`, `bloom_bits(u8)`, `compression(Compression)` (`Lz4` by default; `Zstd(level)`, smaller blocks than LZ4 for more CPU; or `None`), `block_size(u32)`, `cache_priority(Priority)`, `compaction(Compaction::Leveled | Tiered | FifoByTime)`, and `merge_operator(name)`, which names an operator registered with `Options::merge_operator` (an unregistered name fails with `ErrorCode::UnknownMergeOperator`). `Family::counter()` declares a counter family, the only kind that takes `incr` (see [Counters](data-modeling.md#counters)). `blob_threshold(bytes)` (default 4096) moves larger values to blob files.

## Write one row atomically
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body", "stats"])?;
let info = pages
    .mutate(b"com.example/a")
    .put("meta", b"status", b"200")
    .put("links", b"com.example/b", b"")
    .incr("stats", b"hits", 1)
    .delete_column("meta", b"etag")
    .commit()?;

println!("seqno {} at {:?}", info.seqno, info.durability);
# Ok::<(), pigeonhole::Error>(())
```

- A `RowMutation` is **all-or-nothing across every family of that row**.
- Builder calls cannot fail. Problems (unknown family, key over 64 KiB, value too large) surface from `commit()`.
- `commit()` returns `CommitInfo { seqno, durability }`: the sequence number, and the durability level actually applied. It returns only once your write is durable at that level **and** visible to reads, so you always read your own write.
- Variants: `put_at(family, qualifier, ts, value)` for event time (timestamps are `u64` microseconds since the Unix epoch), `put_i64`, `put_f64`, `delete_cell(family, qualifier, ts)`, `delete_family(family)`, `delete_row()`, `.durability(d)` to override durability for this commit.
- `incr(family, qualifier, delta)` adds to an `i64` counter without reading it first. It needs a counter family (`Family::counter()`); on any other family the commit fails with `ErrorCode::InvalidArgument`.

## Read
### One cell
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body", "stats"])?;
# pages.mutate(b"com.example/a").put("meta", b"status", b"200").incr("stats", b"hits", 3).put("links", b"org.example/x", b"").commit()?;
# pages.mutate(b"com.example/b").put("links", b"org.example/y", b"").commit()?;
if let Some(cell) = pages.get(b"com.example/a", "meta", b"status")? {
    let bytes: &[u8] = cell.value();
    let ts: u64 = cell.timestamp();
#   assert_eq!(bytes, b"200");
#   assert!(ts > 0);
}

let hits: Option<i64> = pages
    .get(b"com.example/a", "stats", b"hits")?
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
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body", "stats"])?;
# pages.mutate(b"com.example/a").put("meta", b"status", b"200").incr("stats", b"hits", 3).put("links", b"org.example/x", b"").commit()?;
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
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body", "stats"])?;
# pages.mutate(b"com.example/a").put("meta", b"status", b"200").incr("stats", b"hits", 3).put("links", b"org.example/x", b"").commit()?;
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
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body", "stats"])?;
# pages.mutate(b"com.example/a").put("meta", b"status", b"200").incr("stats", b"hits", 3).put("links", b"org.example/x", b"").commit()?;
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
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body", "stats"])?;
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
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta", "links", "body", "stats"])?;
let mut wb = db.write_batch();
wb.put(&pages, b"com.example/c", "meta", b"status", b"404")
  .incr(&pages, b"com.example/c", "stats", b"hits", 1)
  .delete_row(&pages, b"com.example/old");
let info = wb.commit_with(Durability::GroupSync)?;
# Ok::<(), pigeonhole::Error>(())
```
A `WriteBatch` spans any rows and tables, is atomic across all of them, and has **one durability point**. `commit()` uses the writer default; `commit_with(d)` overrides it for this commit. Builder methods take `&mut self` and return `&mut Self`, so chain them or call them in a loop. `commit` consumes the batch.

## Conditional writes and transactions
`commit_if` commits a row mutation only if a condition on that row holds, atomically (BigTable's `check_and_mutate`). It returns `None` when the condition failed and nothing was written.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let jobs = pigeonhole::doc_support::table(&db, "jobs", &["meta"])?;
let unclaimed = Condition::Absent { family: "meta".into(), qualifier: b"owner".to_vec() };
let first = jobs.mutate(b"job:1").put("meta", b"owner", b"w1").commit_if(&unclaimed)?;
let second = jobs.mutate(b"job:1").put("meta", b"owner", b"w2").commit_if(&unclaimed)?;
assert!(first.is_some() && second.is_none());

// Compare a value: `ValueFilter::Equals`, `Prefix` or `I64(ordering, n)`.
let is_w1 = Condition::Value {
    family: "meta".into(),
    qualifier: b"owner".to_vec(),
    filter: ValueFilter::Equals(b"w1".to_vec()),
};
assert!(jobs.mutate(b"job:1").put("meta", b"state", b"done").commit_if(&is_w1)?.is_some());
# Ok::<(), pigeonhole::Error>(())
```
`Condition::Exists` and `Condition::Absent` test a column, `Condition::Value` its newest value. The condition and the mutation are on one row.

For invariants across rows, a `Transaction` (P4, available now) reads, buffers writes and commits them atomically. It is optimistic: if a commit changed anything it read before it commits, `commit` fails with `ErrorCode::Conflict` and nothing is written, so retry the whole transaction. A transaction that only reads never conflicts: all its reads come from one snapshot, and its commit returns at once.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let bank = pigeonhole::doc_support::table(&db, "bank", &["acct"])?;
# bank.mutate(b"alice").put("acct", b"balance", b"10").commit()?;
let mut txn = db.transaction()?;
let alice = txn.get(&bank, b"alice", "acct", b"balance")?.map(|c| c.to_owned());
# assert_eq!(alice.unwrap().value(), b"10");
txn.put(&bank, b"alice", "acct", b"balance", b"5")
    .put(&bank, b"bob", "acct", b"balance", b"5");
match txn.commit() {
    Ok(_) => {}
    Err(e) if e.code() == ErrorCode::Conflict => { /* re-read and try again */ }
    Err(e) => return Err(e),
}
# Ok::<(), pigeonhole::Error>(())
```

[Transactions](transactions.md) covers isolation, conflicts and cost. `Pigeonhole::transact` runs that loop for you: a fresh transaction per attempt, retried on `Conflict` (and `SnapshotExpired`; `Busy` too with `RetryPolicy::retry_busy`) with exponential backoff and jitter, as a `RetryPolicy` says. The closure may run more than once, so keep side effects outside it.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let bank = pigeonhole::doc_support::table(&db, "bank", &["acct"])?;
# bank.mutate(b"alice").put("acct", b"balance", b"10").commit()?;
let (_, commit) = db.transact(&RetryPolicy::default(), |txn| {
    let alice = txn.get(&bank, b"alice", "acct", b"balance")?.map(|c| c.to_owned());
    # let _ = alice;
    txn.put(&bank, b"alice", "acct", b"balance", b"5")
        .put(&bank, b"bob", "acct", b"balance", b"5");
    Ok::<_, pigeonhole::Error>(())
})?;
# let _ = commit;
# Ok::<(), pigeonhole::Error>(())
```

## Close
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
db.close()?;
# Ok::<(), pigeonhole::Error>(())
```
If this is the last handle open anywhere, `close` checkpoints the WAL and removes the sidecar and shared-memory files, leaving one file. A crash instead leaves the sidecars, and the next open replays them. Dropping the last clone does the same but ignores errors, so call `close()` when you want to know about failures.

If a shard fails (a panic, or an I/O error on its log), `db.is_failed()` turns true and some commits fail. Close the database and open it again: [Recovering from a failed shard](recovery.md) walks through it, including what to do with the commits that were in flight. Or open with `Options::auto_recover(Recovery::Io)`: the database then recovers in place, behind the same handles. Operations during the pause wait, then get `Busy` (not committed, retry), and snapshots and transactions from before fail with `SnapshotExpired`. It is not available in application-owned mode.

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

- `flush()` and `compact()` return after the work is in the file; both fail with `ErrorCode::Closed` after `close`. They report only their own failure: `compact()` returns the error of the compaction it started, and a background flush or compaction that failed earlier (it is retried after a growing backoff) is never reported to a later call. If the stall timeout (`Options::write_stall_timeout`, 30 s by default) passes while the call waits for room, it fails with the transient `ErrorCode::Busy`; the timeout counts from when that call began waiting.
- `backup(dest)` writes a new file at `dest`, which must not exist, from a snapshot taken when you call it, so it holds exactly the commits visible at that moment. Commits that land while it runs are not in it. It holds that snapshot's memtables only while it copies them, at the start; the rest of the run holds only the snapshot's SST files, so writers are not stalled by a long backup. The SST and blob files are copied byte for byte, so the copy keeps the source's layout (its levels, and versions a compaction hasn't removed yet) and none of its free space; a blob file is copied if the snapshot references any value in it.
- The file does not shrink by itself: space freed by compaction is reused by later writes, but the file keeps its length. Call `compact()` and then `shrink()` to give space back to the filesystem.
- `shrink()` returns the bytes released (`0` when there is nothing to release). It cuts off the free space at the end of the file, moves live data from the file's tail into free space nearer the start, then truncates again, so it costs a read and rewrite of that data. It runs online: other threads keep reading and writing. Space still held by an open snapshot or scan, a backup in progress or a reader process's snapshot is released only after that is dropped; while one is held, `shrink` never grows the file but may give back less, so call it again afterwards for the rest. It fails with `ErrorCode::Closed` after `close`, `ErrorCode::ReadOnly` on a handle that cannot write, `ErrorCode::NoSpace` if the disk is full when it moves the manifest (which needs a few KiB first), and `ErrorCode::Io` on a disk failure. Use it after a large delete, not routinely.
- After `compact()` and `shrink()`, a file above a few MiB is typically 1.05–1.2× its live data: compaction cuts its outputs into power-of-two pieces that pack tightly (D183), and `shrink` moves blob files as well as SSTs (D185). A small file is dominated by fixed metadata (about 1 MiB). Data with no free extent of its size below it stays put; `shrink` does not report that as an error.
- The `phdb` command-line tool does the same from a shell: `phdb info`, `phdb check` (an offline integrity check), `phdb backup`, `phdb compact --shrink`, `phdb dump` and `phdb load`. See [Command line](cli.md). From code, `db.info()` describes what the file holds, and `pigeonhole::check(path, CheckOptions::default())` checks a database no writer has open.

## What happens when writes outrun the disk
Reopening after a crash with fewer shards or a smaller `memtable_budget` than before works: if the WAL's unflushed data does not fit the memtable arenas, the open writes it to the file as it replays, which makes that open slower. A write that finds the memtable arena full waits (a write stall) while a flush frees room. `ErrorCode::Busy` means that write's own wait ran past the stall timeout (`Options::write_stall_timeout`, 30 s by default), counted from when it began waiting (D240): it is **transient**, and the write was not committed, so back off and retry. A commit whose rows are being moved between shards gets `Busy` the same way if the move holds it up for the whole timeout (D241). When compaction falls behind instead, commits are slowed down, never refused. A single batch that can never fit a shard's arena (more than about half of it) fails at once with `ErrorCode::BatchTooLarge`, which never succeeds on retry: split the batch or raise `Options::memtable_budget`. See [Errors](errors.md).

## What the current build does not do yet

| Feature | Current behavior |
|---|---|
| `Durability::None` commits | Durable once flushed (`flush`, a clean close, or a background flush), or once a later stronger commit on the same shard returns (decision D94, see [Durability](durability.md#mixed-levels)). A crash before either loses them. |

Available ahead of their phase: `Transaction` and reader processes (`open_reader`) (P4).

## Next
[Durability](durability.md) · [Transactions](transactions.md) · [Async](async.md) · [Scans and filters](scans-and-filters.md) · [Data modeling](data-modeling.md) · [Reader processes](readers.md) · [Recovery](recovery.md) · [Errors](errors.md) · [Command line](cli.md) · [Arrow export](arrow.md) · [Performance](performance.md) · [Agent reference](agent-reference.md)

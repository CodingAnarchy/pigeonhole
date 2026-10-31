# Concepts

> **Status:** this guide describes `main`; the current release on crates.io is 1.0.0, and [`changelog.d/`](../../changelog.d/README.md) lists what `main` adds after it.

Pigeonhole stores a sorted, sparse, versioned map:

```text
(table, row, family, qualifier, timestamp) → value
```

Picture a wall of pigeonholes: labeled slots addressed by row and column, most of them empty.

## Tables
A table is a namespace with its own set of column families. One database file holds many tables.

## Rows
A row key is arbitrary bytes, up to 64 KiB, sorted lexicographically (byte-wise). A row is the **unit of atomicity**: every mutation to one row commits all-or-nothing, across all of its families. It is also the unit of locality: a row's cells are stored together within each family.

Design row keys so that rows you scan together share a prefix: `com.example/page/1`, `user:42:events`, and so on.

## Column families
Families are declared when a table is created (adding one later is cheap). Each family is its own physical LSM tree, so a scan of one family never reads bytes from another. A family carries policy:

| Setting | Effect |
|---|---|
| `max_versions(n)` | Keep at most *n* versions per column (0 keeps all). |
| `ttl(d)` | Cells whose timestamp is older than *d* expire. |
| `bloom_bits(b)` | Bloom filter bits per key, to skip files on misses. |
| `blob_threshold(bytes)` | Values larger than this (default 4096; `u32::MAX` never) move to blob files when they are flushed or compacted; the family's tree keeps a 16-byte pointer, so compaction and scans of other columns do not copy them. Blob GC rewrites a blob file once it is about half garbage, and `compact()` empties every file with garbage. Blob records are not compressed: for large compressible values, raise the threshold to keep them inline. |
| compression | `compression(Compression)`: `Lz4` by default; `None`; `Zstd(level)`: smaller blocks than LZ4 for more CPU, at libzstd levels 1–22 (higher is smaller and slower; negative is faster; default 3). |
| `cache_priority(p)` | `Priority::High`, `Normal` (default) or `Low`: how long the family's blocks stay cached. |

### Compaction styles
`Family::compaction(..)` picks how a family's files are merged. The default is right for most families.

| Style | Pick it when | Trade-off |
|---|---|---|
| `Leveled` (default) | Reads matter most: point gets, short scans, mixed read/write. | Lowest read cost and space overhead; the most rewriting of data on the write path. |
| `Tiered` | The family is write-heavy and read less often (ingest, logs, event capture). | Far less rewriting per byte written; reads may consult more files and the file can hold more stale data until runs merge (D165). |
| `FifoByTime` | Append-only data written roughly in time order that you age out with a TTL. | Cheapest way to expire data (whole files are dropped, with no rewrite), but it never merges old data into big sorted runs, so reads of old data stay as fragmented as it arrived. |

Notes:
- **Tiered depth.** The depth of the tree is engine-wide (7 levels by default) and is not a per-family setting. Keep the default: write amplification of a tiered family grows only logarithmically with the amount of data there (about 4× after 1000 flushes and 6× after 4000, insert-only, in the decision's measurements), while a shallow tree of 3 levels grows it linearly with the data (D169). If you build the engine yourself with a shallow `max_levels`, prefer `Leveled`.
- **`FifoByTime` expiry.** A file is dropped when its **newest** cell has expired (`ttl`), not cell by cell, so a file lives until its newest cell is older than the TTL. Until it is dropped, its expired cells are already hidden from reads. Expiry runs on a timer set for the earliest expiry, so an idle family drops its expired files on time, without waiting for a flush or compaction (D170). The one exception is a stopped clock, such as a simulator's, where expiry falls back to the next flush or compaction. A family with no TTL never drops anything. `Engine::compact` (`Pigeonhole::compact`) merges a FIFO family into one run, which then expires only when its newest cell does.
- **`FifoByTime` size cap.** The engine has an optional size cap that drops the oldest files even when they have not expired (`PickerOptions::fifo_max_bytes`, off by default). It is **lossy**, and the public `Options` do not expose it. With explicit timestamps it can resurrect an older version of a cell, uncover a value a dropped delete was hiding, or move a counter backwards (D167). Do not rely on it for correctness.

Put data with different access patterns in different families: small hot metadata in one, large or TTL'd payloads in another.

## Qualifiers
Qualifiers are arbitrary bytes, created on write, sorted within a family. They are the "columns", but they are not declared: a row can have zero or ten million of them, and absent ones cost nothing. Use them for sparse attributes, time buckets (`2026-10-05T12`), or adjacency lists (`edge:<dst>`).

## Timestamps and versions
Every cell version carries a `u64` timestamp in **microseconds since the Unix epoch**, sorted newest first. By default Pigeonhole assigns the commit time, and a default timestamp never goes backwards anywhere in the database (one allocator stamps every shard's commits, D271), even across a clock step or a restart. Supply your own with `put_at` for event time; TTL treats user timestamps as microseconds. Reads return the newest version unless asked for more (`versions(n)`, `time_range`).

## Values
Values are bytes, with typed forms for `i64` and `f64`. A value can be up to 4 GiB − 2 bytes: one longer than the inline limit (the smaller of the WAL segment payload, 64 MiB and half a shard's memtable arena) is written to a blob file as it is committed, so only a pointer goes through the WAL and the memtable. Such a put costs one extra manifest commit, with its file sync, even under `Durability::None`. A merge operand stays within the inline limit (`ValueTooLarge` beyond it). `incr` adds to an `i64` counter without reading it first; counters live in counter families (`Family::counter()`, see [Counters](data-modeling.md#counters)). Other aggregate families keep a minimum, a maximum or a distinct count per cell the same way (`Family::aggregate`, see [Min and max](data-modeling.md#min-and-max) and [Distinct counts](data-modeling.md#distinct-counts-hll)). Custom merge operators (append and so on) are registered with `Options::merge_operator`.

## Deletes
Deletes write markers: one cell version (`delete_cell`), a whole column (`delete_column`), a family within a row (`delete_family`), or a whole row (`delete_row`, which writes one family marker per family in the same atomic commit). A column or family delete with timestamp *T* hides every version in its scope with timestamp ≤ *T*, regardless of when it was written, so a later `put` with an older timestamp stays hidden; a `put` with a newer timestamp is visible again. A cell delete hides the version at exactly its timestamp, again regardless of when it was written: a later `put` at that same timestamp stays hidden, so write the replacement at another timestamp.

In an aggregate family (a counter, min, max or HLL family) a delete hides only what earlier commits wrote, so the cell starts again after it ([Counters](data-modeling.md#counters)).

These rules hold only until compaction purges the markers (HBase semantics, decision D74). A bottommost compaction with no open snapshot that needs them removes delete markers and versions beyond `max_versions` for good. After that, a write with an **older explicit timestamp** (`put_at`, `delete_cell`) behaves as if they never existed: a `put_at` below a purged delete becomes visible, and deleting the newest version does not bring back a purged older one. Writes with default timestamps are never affected. See [Data modeling](data-modeling.md#versions).

## Multi-process readers
One writer process and any number of reader processes on the same host can open the same `.phdb` file; readers see each commit as soon as the writer publishes it. A reader's handle (`Pigeonhole::open_reader`) has no write methods, but the reader process still opens the file **read-write** and writes nothing to it: the processes coordinate with byte-range locks on the file, and some of those locks are exclusive, which POSIX grants only on a writable file descriptor. This is the same requirement SQLite has in WAL mode. So every reader process needs write permission on the database file, and a database on read-only media cannot be opened by readers.

**Upgrades:** the writer and every reader process must run builds with the same shared-memory layout. **Stop reader processes before upgrading the writer.** A writer of a newer layout (this release's is 6) fails to open with `ShmVersionMismatch` while readers of an older build are still attached, and an older reader can't attach to a newer writer.

A reader's snapshot survives the writer closing. Once a new writer opens, though, reads through snapshots taken before the restart fail with `SnapshotExpired`, because the new writer may reuse the space those snapshots read. Take a new snapshot and redo the read. Snapshots taken after the restart read normally.

[Reader processes](readers.md) has the details: crashes on either side, long-held snapshots, the limits (`Options::reader_slots`) and the requirements.

## Platform and process notes
- **Block cache.** Each process that opens the file has its own block cache, **256 MiB by default** (`Options::block_cache`, `ReaderOptions::block_cache`). The writer and every reader process each add their own, on top of the memtable arenas (`memtable_budget` per shard). The writer's row cache (`Options::row_cache`, 64 MiB by default, D216) adds its size, plus about 9% for its per-row write watermarks, on top of that. Set them explicitly on small devices. Every open SST keeps its top index and filters pinned in the block cache (about 0.3% of the data with 1 KiB cells, more with smaller ones), and the index partitions reads need are cached beside them: give the block cache at least twice the pinned bytes, or point reads of data outside the cache cost two reads instead of one. `engine_metrics()` reports them as `block_cache_pinned_bytes` (with `block_cache_capacity_bytes`), and `block_cache_pinned_warnings` counts each time they passed half the cache.
- **Forked children.** A child process created with `fork` can't use the database its parent opened: every operation on the inherited handle fails with `Unsupported` ("opened in another process (fork); open it again here") at once, including snapshots, cursors, transactions and commits started before the fork, and closing or dropping it does nothing. In the child, a handle may only be dropped or closed: metrics and other accessors return empty values (ICR 0067). The parent's engine threads don't exist in the child, and the child shares the parent's file descriptors and shared memory. **Opening a database in a forked child is unsupported, too:** `exec` first (or spawn a new process) and open it there. That's standard advice after `fork` without `exec` in a multithreaded process: the child may block for ever on a lock one of the parent's threads held, and on macOS it aborts outright (the system's thread parking can't be used on the child side of a fork). In Python, use the `spawn` or `forkserver` start method of `multiprocessing`. Only `fork(3)` is detected, through `pthread_atfork`: `_Fork`, `vfork` and a raw `clone` skip it, so never use a handle after those without `exec` (#901; for reader processes, see [Reader processes](readers.md)).
- **Never open the file yourself on macOS or BSD.** The writer lock is a POSIX `fcntl` lock, and on those systems closing *any* descriptor of the file drops *all* of the process's locks on it. Pigeonhole guards against this between its own handles, but not against yours: a plain `std::fs::File::open` and drop of the `.phdb` inside the process (to hash it, `fs::copy` it, or from a file watcher) releases the writer lock, and a second process can then open the file as a writer and corrupt it. Use `backup` to copy a live database. On Linux the locks belong to the open file description and this does not happen.
- **Readers in other PID namespaces work.** The writer judges reader liveness by a byte-range lock each reader holds on the file, not by process id (#693), so a writer and readers in different containers that share the volume and the shared memory work like processes on one host.
- **Long backups.** `backup` writes its snapshot's memtables into the copy first (at most one arena's worth of data) and then releases them, keeping only the snapshot's SST and blob files for the long copy. Writers therefore keep the whole arena while a large backup runs; what the backup holds meanwhile is file space (old SSTs a compaction replaced are freed once it finishes). The SST and blob files are copied byte for byte, as a file-level backup would copy them, so the copy is about the size of the source's live data, with none of its free space. A `shrink` while a backup or snapshot holds space gives back less, and never grows the file; call it again afterwards for the rest.
- **Custom `Vfs` clocks (test hooks only).** `Options::vfs` exists only with the `__unstable-hooks` feature. The engine decides a clock is frozen (as in the simulator) when 1024 polls in a row read the same `monotonic_nanos` value, and then uses its simulator fallbacks: it admits writers unpaced during an L0 stall and refuses a write that waits for arena room with `Busy` at once. A custom `Vfs` must therefore advance `monotonic_nanos` at least every 10 µs of real time. A clock with 1-4 ms resolution (such as `CLOCK_MONOTONIC_COARSE`, or a cached clock) is misclassified as frozen. The built-in backend uses `Instant` and is fine.

## File format and upgrades
- **One format for 1.x.** Every 1.x release reads and writes file format 4 (`DatabaseInfo::format_version`), and opens every file written by 0.1.0 or later. Features added after 1.0 don't change the format version: a file sets a feature bit once it uses one (`DatabaseInfo::features`), and a file that uses none stays openable by every 1.x release. [`FORMAT.md` §13](../../FORMAT.md#13-compatibility-promise) is the full promise.
- **Upgrading is one-way. Back up first.** Opening an older file writable with a newer release upgrades it (to format 4 from 0.x files), and the older release then refuses it with `UnsupportedFormat`. Take a `backup` (or copy the closed file) before the first writable open with a new release if you may need to roll back. A read-only open never upgrades: reader processes, and `Options::read_only_fallback`.
- **Opening a newer file with an older release.** It works read-write unless the file uses a feature the older release lacks (for example a min or max family, which 1.0.x lacks: [Min and max](data-modeling.md#min-and-max)). Then the open fails with `UnsupportedFeature`, naming the feature bits. Some features allow reading only: `Options::read_only_fallback(true)` opens those read-only, writing nothing to the file. Upgrade the library to write.
- **Shared memory is separate.** The writer and its reader processes must run releases with the same shared-memory layout, which may change in any release (see [Multi-process readers](#multi-process-readers)): stop the readers before upgrading the writer.

## Versions and compatibility
From 1.0.0 (D282):

- **`pigeonhole` follows [semantic versioning](https://semver.org).** No 1.x release breaks code that compiles against an earlier 1.x. New API arrives in minor releases, and fixes in patch releases.
  - **Types that can grow are `#[non_exhaustive]`:** every public enum (`Value`, `Condition`, `ValueFilter`, `Compaction`, `Compression`, `Priority`, `Recovery`, `Durability`, `ErrorCode`, `FailureCause`, `CpuProfile`, `IoBackend` and the check report's enums), and the info, metrics, stats and outcome structs (ICR 0071, ICR 0073). A minor release may add a variant or a field to them, so a `match` on one of the enums needs a `_` arm, and the output structs are read, not built.
  - **Error codes are stable:** an `ErrorCode`'s number never changes; new codes may appear in minor releases.
  - **The minimum Rust version (D283):** a 1.x minor release may raise `rust-version`, a patch release never does, and the changelog calls out every raise.
  - **Hooks are outside the promise:** test and bench hooks (`Options::vfs`, `shard_stats` and the like) exist only with the `__unstable-hooks` feature (ICR 0085), which an application never turns on, and may change in any release. The few remaining `#[doc(hidden)]` items (the doc-example helpers, the trait impls that name internal crates' types) are outside the promise too.
- **`pigeonhole-arrow` follows semver too.** Its arrow-rs major is part of its API, so moving to a new arrow-rs major is a major release of `pigeonhole-arrow`.
- **`phdb` (`pigeonhole-cli`) follows semver for its commands, flags, exit statuses and output formats.** The JSON output is versioned (`"phdb": 1`); a breaking change to it takes a new version number and a major release. The promise covers the binary, not the crate's library target, which exists for in-process tests and is hidden (ICR 0086).
- **The other crates are internal:** `pigeonhole-engine`, `-format`, `-io`, `-sst`, `-wal`, `-memtable`, `-compaction`, `-runtime`, `-shm`, `-cache`, `-pager` and `-sim`.
  - They're published only because `pigeonhole` depends on them, and they make **no stability promise of their own**.
  - `pigeonhole` pins each one exactly (`=`), so a `pigeonhole` release always builds with the internal crates it was tested with.
  - An internal crate may change incompatibly in any `pigeonhole` release. Depend on `pigeonhole`, not on them.
- **Two version lines (D295).** The public crates (`pigeonhole`, `pigeonhole-arrow`, `pigeonhole-cli`) share one version, under semver. The 12 internal crates share another, which takes a new major whenever any of them changes incompatibly, while `pigeonhole` stays 1.x. The numbers don't track each other: `pigeonhole` 1.1.0 may ship with the internal crates at 2.0.0, and the release notes name both. A release's tag and changelog follow `pigeonhole`'s version.
- **The file format has its own promise,** separate from the API's: [`FORMAT.md` §13](../../FORMAT.md#13-compatibility-promise), summarized in [File format and upgrades](#file-format-and-upgrades). The shared-memory layout between a writer and its reader processes is in neither promise (stop the readers before upgrading the writer).

## Where next
[Getting started](getting-started.md) · [Data modeling](data-modeling.md) · [Agent reference](agent-reference.md)

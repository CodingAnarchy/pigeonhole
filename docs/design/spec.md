<!-- Exported from https://claude.ai/artifact/4zEQ4RyDCMoovUiUNVwrco on 2026-10-05; corrected since to match the decisions (decisions/README.md), which wins on any conflict. -->
# Pigeonhole: An Embedded Wide-Column Store in Rust

2026-10-05 · 

## Summary

Pigeonhole is a proposed embedded, single-file, wide-column store written in Rust: the BigTable data model with the SQLite deployment model. It targets point reads in single-digit microseconds, sustained writes of hundreds of thousands to low millions of cells per second per core, and ordered row scans at memory-bandwidth speed when data is hot.

SQLite owns local OLTP and DuckDB owns local OLAP. Nothing comparable owns local sparse, versioned, row-scan-heavy data: feature stores, time series keyed by entity, crawl and event caches, graph adjacency, per-user state. Today people pick one of three bad fits:
- **A raw KV engine** (RocksDB, LMDB, redb, fjall). Fast, but every user reinvents row/family/qualifier/timestamp key encoding, versioning, TTL and per-family tuning.
- **SQLite with an EAV table or JSON column.** Works, but sparse attributes bloat B-tree pages, scans of a single family read the whole row, and there are no cell versions.
- **A server** (Cassandra/Scylla, HBase, Bigtable, Tarantool). Real wide-column semantics, but a process, a network hop and an ops burden for data that lives on one machine. Tarantool and Cassandra-style schemas also fix columns up front, which defeats the point of sparse data.

Pigeonhole makes the missing quadrant a library: `cargo add`, open a file, get rows of arbitrary sparse columns grouped into families, with versions, TTLs, prefix and range scans, and no server.

The name comes from a wall of pigeonholes: labeled slots addressed by row and column, most of them empty, which is the data model in one picture.

| Artifact | Name |
|---|---|
| Rust crate | `pigeonhole` |
| Database file | `*.phdb`, with a `*.phdb-wal`-N file per shard and a shared-memory region while open |
| CLI binary | `phdb` |
| C header (future) | `pigeonhole.h` |
| Python package (future) | `pigeonholedb` (bare name taken on PyPI) |
| npm package (future) | `pigeonhole-db` (bare name taken on npm) |
| Domains (planned) | `pigeonholedb.dev` or `pigeonholedb.com` as primary, `phdb.dev` as a short alias (all unregistered as of Oct 5, 2026; bare pigeonhole.com, .dev, .io and .org are taken) |

RubyGems and trademark availability still need checking, and the planned domains need registering, before public release.

## Goals and non-goals

The design optimizes for one machine, one process, many threads, and data larger than RAM but usually hot.

| Goal | Target (NVMe, modern x86/ARM, hot cache unless noted) |
|---|---|
| Point get of one cell or one family | p50 < 2 µs, p99 < 10 µs in memory; one I/O on cold data |
| Batched writes, durable at group commit | > 1M cells/s across cores; p99 commit < 200 µs with `fsync` batching |
| Ordered row scan, single family | > 1 GB/s decoded per core from cache (row key + qualifier + value, on 100-byte values in 8-cell rows; D205) |
| Open to first read | < 5 ms, no full-file recovery scan |
| Footprint | One file at rest (plus per-shard WAL files and a shared-memory region while open); zero required config |
| Embeddability | Rust crate first, with sync and async APIs; the sync API needs no async runtime; core shaped so a C ABI can wrap it later |

Thread-per-core scaling is also a v1 goal: on workloads whose rows spread across tablets, write throughput at N shards should reach at least 0.8 × N times single-shard throughput, up to the reference hardware's core count, with no regression in single-shard p99.

**Reference hardware.** All targets and gates are measured on enterprise NVMe with power-loss protection, running Linux 6.x with io_uring available. Results on consumer SSDs, macOS and Windows are reported in every benchmark run but never gate a phase.

Non-goals for v1:
- **SQL or a query planner.** The API is get/put/delete/scan over a sorted map. A thin DataFusion table provider can come later for ad hoc analytics.
- **Distribution or replication.** Single node only; a WAL-shipping hook is the extension point.
- **Multi-process writers.** One writer process, any number of reader processes, like LMDB and DuckDB.
- **Secondary indexes.** Users build them as additional families; the engine stays a sorted map.
- **Cross-row ACID by default.** Single-row atomicity is the default, as in BigTable; multi-row transactions are an opt-in mode.

### Language scope

Rust is the principal language and the entire initial scope: the engine, public API, CLI, benchmarks and test suite are all Rust, and the Rust crate is the product.

Other languages are a later, optional layer. The future state mirrors SQLite, whose single C library underlies bindings in Python, Ruby, JavaScript, Go and many others. Pigeonhole will expose a stable C ABI (`pigeonhole``.h`), and per-language libraries will be thin wrappers over it, never reimplementations of the engine.

To keep that door open without building it now:
- Keep Rust-only types (borrowed lifetimes, generics, closures, trait objects) out of anything that will become the ABI boundary.
- Give every zero-copy Rust API an owned or cursor-style equivalent that can be exported later.
- Keep merge operators, comparators and filters identified by name in the file, so a binding in another language can open any database a Rust program wrote.
- Errors are a flat, stable code enum plus a message, which maps cleanly to any language's exceptions.

## Data model

A Pigeonhole database is a sorted, sparse, versioned map: `(table, row, family, qualifier, timestamp) → value`. Only families are declared; qualifiers are arbitrary bytes created on write, so a row can hold zero or ten million columns and absent cells cost nothing.
- **Table.** A namespace with its own set of families. Many tables share one file.
- **Row key.** Arbitrary bytes up to 64 KiB, sorted lexicographically. Row is the unit of atomicity and the unit of locality.
- **Column family.** Declared at table creation, cheap to add later. Each family is its own physical keyspace (its own LSM tree), so a scan of `meta:` never touches bytes in `blob:`. Families carry policy: compression codec, block size, bloom filter bits, max versions, TTL, value-in-key-tree vs. blob separation, and cache priority.
- **Qualifier.** Arbitrary bytes, sorted within the family. Encodes sparse columns, time buckets, or adjacency (`edge:<dst_id>`).
- **Timestamp.** u64, sorted descending so the newest version is read first. Defaults to a hybrid logical clock; users may supply their own for event time.
- **Value.** Bytes, plus optional typed tags (i64 counter, f64, varint) that enable server-side merge operators such as atomic increment and append without read-modify-write.
- **Tombstones.** Cell, column (all versions), family-in-row, and whole-row deletes, each a single marker.

Internal key layout inside a family's tree, chosen so that row prefix scans, qualifier ranges, and "latest version" are all contiguous reads:

```text
[row bytes, escaped][0x00 0x01][qualifier bytes, escaped][0x00 0x01][!timestamp: u64 BE][!seqno: u64 BE][kind: u8]
```

Escaping 0x00 as 0x00 0xFF and terminating with 0x00 0x01 keeps lexicographic order for arbitrary row and qualifier bytes (a length prefix would sort short rows first). Inverting the timestamp makes newest-first a forward seek. The inverted commit seqno follows the timestamp so that two writes with the same user timestamp never collide, the later commit sorts first, and a snapshot read skips any entry whose seqno is newer than the snapshot.

**Limits and byte order.** Row keys and qualifiers are each at most 64 KiB. Values are at most 4 GiB and are stored as blobs above the family's threshold. Ordering fields in the internal key are big-endian as shown; every other on-disk integer is little-endian.

## API surface

The API is a typed sorted-map interface with zero-copy reads; everything else is built from it.

```rust
let db = Pigeonhole::open("crawl.phdb", Options::default())?;
let pages = db.table("pages")?
    .family("meta", Family::default().max_versions(1))
    .family("links", Family::default().bloom_bits(10))
    .family("body", Family::default().blob_threshold(4096).zstd(3).ttl(days(30)))
    .create_if_missing()?;

// Single-row atomic mutation
pages.mutate(b"com.example/a")
    .put("meta", b"status", b"200")
    .put("links", b"com.example/b", b"")
    .incr("meta", b"hits", 1)
    .delete_column("meta", b"etag")
    .commit()?;

// Point read: borrows from the page cache, no allocation
let v: Option<CellRef<'_>> = pages.get(b"com.example/a", "meta", b"status")?;

// Row read, projected to families and qualifier ranges
let row = pages.row(b"com.example/a").families(["meta"]).latest().read()?;

// Ordered scan with filters pushed into the block iterator
for row in pages.scan(b"com.example/"..b"com.example0")
    .family("links")
    .qualifier_prefix(b"org.")
    .snapshot(&snap)
    .iter()? { /* ... */ }

// Batched multi-row write with one durability point
let mut wb = db.write_batch();
for e in events { wb.put(&tbl, &e.row, "ev", &e.qual, &e.val); }
wb.commit_with(Durability::GroupSync)?;   // D12: commit() uses the writer default
```

- **Reads** return borrowed `CellRef`/`RowRef` views tied to a snapshot; `.to_owned()` when needed.
- **Filters** (qualifier range, prefix, value predicate, version count, time range, column limit per row) run inside the block decoder, not after materialization.
- **Merge operators** are registered per family in Rust and identified by name in the file so a different binary can't silently misinterpret them. Opening a database that references an unregistered operator fails with a typed error by default; an explicit read-only option opens it anyway, compaction stays off, and reads of affected cells return that error instead of unresolved operands.
- **Durability** defaults to GroupSync and can be changed per writer or per call (see Durability below): `None` (memtable only), `Buffered` (WAL write, no fsync), `GroupSync` (fsync shared across concurrent committers), `Sync`.
- **Bindings:** a C ABI (`pigeonhole``.h`) and bindings for Python, Ruby, JavaScript, Go and others are future scope (see Language scope); v1 ships the Rust crate only. A CLI (`phdb`` shell`, `phdb`` dump`, `phdb`` compact`) mirrors `sqlite3`/`duckdb`.

### Durability

Every commit defaults to `GroupSync`, so a commit that returns is on disk unless the caller asks for less. The level resolves in this order: per-call override, then the writer default, then `GroupSync`.

| Mode | Survives | Cost per commit | Typical use |
|---|---|---|---|
| `None` | Nothing past the last flush | Sub-µs, no I/O | Rebuildable caches, derived data |
| `Buffered` | Process crash, panic, `kill -9`; not OS crash or power loss | ~1–2 µs | Ingest with an upstream source of truth |
| `GroupSync` (default) | Power loss, once the call returns | About one `fsync`, shared by all committers in the batch | Systems of record |
| `Sync` | Power loss, once the call returns | One dedicated `fsync`, never batched | Rare latency-isolated critical writes |

In every mode a crash loses only a suffix of recent commits, never one from the middle and never part of a row.

```rust
// Writer default, set at open (applies to every commit that doesn't override)
let db = Pigeonhole::open("ingest.phdb", Options::default().durability(Durability::Buffered))?;

// Writer default, changed at runtime; takes effect for commits that start afterward
db.set_default_durability(Durability::GroupSync);

// Per-call override, sync and async
wb.commit()?;                                  // uses the writer default
wb.commit_with(Durability::Sync)?;             // override for this commit only
txn.commit_with_async(Durability::None).await?;
pages.mutate(row).put("meta", b"k", b"v").durability(Durability::Buffered).commit()?;
```

- **Writer default** lives in `Options` and on the open database handle. It is process-local, not stored in the file, so reopening with different options changes it.
- **Per-call override** is available on every commit path: row mutations, write batches, transactions, and `check_and_mutate`, in both sync and async forms.
- **Mixed levels share ****each shard's WAL stream****.** A `GroupSync` commit also makes any earlier `Buffered` or `None` records durable, since each stream is ordered; the reverse never weakens a stronger commit.
- **Observability:** each commit result reports the level it actually used, and per-level commit counts and latencies are exposed in engine metrics.

### Sync and async

Clients choose either style per call site: every operation exists as a blocking sync method and as an `async` method with the same semantics, over one shared engine.

```rust
// Sync: no runtime involved
let v = pages.get(b"com.example/a", "meta", b"status")?;

// Async: same table handle, same snapshot rules
let v = pages.get_async(b"com.example/a", "meta", b"status").await?;
let mut rows = pages.scan(b"com.example/"..b"com.example0").family("links").stream();
while let Some(row) = rows.next().await { /* ... */ }
wb.commit_async(Durability::GroupSync).await?;
```

- **One engine, two front doors.** The core is sync and owns its threads. The async layer lives in the same crate behind an `async` feature (default-on once implemented in Phase 3; off until then, D17); disabling it removes all async dependencies.
- **Truly async I/O, not ****`spawn_blocking`****.** Async calls submit to the engine's I/O backend and register a waker; io_uring completions (or the `pread` pool on other platforms) wake the task. Memtable and cache hits return `Ready` on first poll, so hot async reads cost about the same as sync ones.
- **Runtime-agnostic.** Futures depend only on `std::task` wakers and `futures-core` traits, so they run on Tokio, smol, async-std, or a custom executor. An optional `tokio` feature adds conveniences, never requirements.
- **Owned results across ****`.await`****.** Sync reads can borrow `CellRef<'_>` from the cache; async reads return `Cell`, a cheap ref-counted handle that pins its cache block, so values can be held across await points without copying.
- **Scans as ****`Stream`****s** with the same filter pushdown and readahead as sync iterators, plus backpressure: blocks are prefetched only as the consumer polls.
- **Group commit for both.** Sync and async committers join the same commit group; an async commit future resolves when its record is durable at the requested level.
- **Cancellation.** Dropping a read or scan future is always safe. Dropping a commit future after submission does not roll it back; the write lands or fails atomically, and `commit_async` documents this. A `commit_with_ticket` variant returns a sequence number the caller can wait on or check later.
- **Parity is tested.** One test suite runs every API case through both paths and compares results, so the two styles never drift.

## Storage architecture

Pigeonhole is a per-family LSM tree whose sorted runs live as extents inside one page-structured file, with a `-wal`-N sidecar files, one per shard, folded in at checkpoint. At rest after a clean close it is a single file you can copy, just like SQLite in WAL mode.

An LSM fits this model better than a B-tree: sparse inserts land in random places, values compress far better in sorted immutable blocks, versions and tombstones are natural, and write amplification stays bounded under millions of small cells. The classic LSM weakness, read latency, is addressed directly in the performance section.

*[Diagram: architecture · write path, read path, file layout]*

Each family is its own tree inside the same file, so a hot `meta` family and a large, TTL'd `events` family compact and cache independently.

### File layout
- **Superblock pair (pages 0 and 1).** LMDB-style double-buffered roots: each holds a sequence number, checksum, and pointer to the current manifest. Commit is a write of the new manifest followed by a flip of the older superblock. No recovery scan on open.
- **Manifest.** A small copy-on-write tree listing tables, families with their options, and each family's levels and SST extents with key ranges, sizes, and filter locations.
- **Extent allocator.** 4 KiB pages, allocated in power-of-two extents (64 KiB to 64 MiB) from a free-space bitmap. Compaction writes new runs into free extents and frees old ones only once no snapshot references them, so the file never needs a vacuum to stay consistent, only to shrink.
- **SST extents.** Immutable sorted runs, one family each.
- **Blob extents.** Large values (above a per-family threshold, default 4 KiB) are separated WiscKey-style; the tree stores a 16-byte pointer. Blob GC is driven by per-extent live-byte counts from compaction.
- **WAL sidecar.** Decision: one sidecar file per shard stream (data.phdb-wal-N), each made of preallocated, recycled segments. Separate files let each shard fsync its own stream without flushing other shards' pages. Records are 32 KiB frames with CRC32C and a segment epoch, one log stream per shard covering all families, so a multi-family row mutation is one record in its owning shard's stream (each stream has its own recycled segments). Segments are preallocated at a fixed size and reused after checkpoint rather than deleted, so durable syncs are fdatasync on already-allocated blocks with no file-metadata update; the epoch lets recovery tell new records from stale ones in a reused segment. The log never has a full state: if checkpoints fall behind, new segments are allocated. The last process's clean close checkpoints and removes the sidecar files, so the database is one file at rest. An online backup API (sync and async) produces a consistent single-file copy while writers run. The WAL sits behind a trait so an in-file ring can be added later as an option for deployments that need one file at all times.

### Files and locks

While any process has `data.phdb` open, these exist:

| File | Purpose | Lifetime |
|---|---|---|
| `data.phdb` | Main page file | Permanent |
| `data.phdb-wal-0` to `data.phdb-wal-{N-1}` | One WAL file per shard stream | Created at open; checkpointed and removed at the last process's clean close |
| Shared-memory region | Header, watermarks, views, memtable arenas, reader slots | Created by the first process; removed by the last |

- **Locks are byte-range locks on a reserved lock page in the main file:** OFD locks on Linux, `fcntl` locks behind a per-process handle registry on macOS and BSD (so closing one handle never drops another handle's lock), and `LockFileEx` on Windows.
- The writer byte is held exclusively by the one writer. A second writer fails immediately with `WriterLocked`.
- The presence byte is held shared by every process with the database open. At close, a process that can upgrade it to exclusive is the last one, and removes the WAL files (after checkpoint) and the shared-memory region.
- **WAL file names carry the stream number, not the shard number,** so a reopen with a different shard count replays every existing stream before assigning tablets to the new shards.

### SST and block format
- **Data blocks** (default 16 KiB, per-family) hold prefix-compressed keys with restart points every 16 entries, plus a per-block row-start table so scans can skip whole rows without decoding cells.
- **Partitioned index:** a top-level index of index blocks, pinned in memory, so any key is at most one cached index lookup plus one data block read.
- **Filters:** a ribbon (or blocked bloom) filter on row key and another on row+qualifier, per SST, so both "does this row exist" and "does this cell exist" skip SSTs cheaply.
- **Compression:** LZ4 by default, zstd with a trained per-family dictionary optional. Hot upper levels can be stored uncompressed.
- **Checksums:** xxh3 per block, verified on read from disk, skipped on cache hits.

### Compaction, per family
- **Leveled** for read-heavy families (lowest space and read amplification).
- **Tiered/universal** for write-heavy families (lowest write amplification).
- **FIFO-by-time** for TTL'd time-series families: whole SSTs drop when their newest timestamp expires, with no rewrite.
- Version GC (max versions, TTL) and tombstone purging happen during compaction, never on the read path.

## Concurrency, transactions, durability

Pigeonhole gives lock-free snapshot reads to any number of threads and processes, single-row atomicity by default, and opt-in optimistic multi-row transactions, all on a thread-per-core engine where each core owns a slice of the data and one global MVCC sequence number orders everything.

### Thread-per-core execution

Thread-per-core is a v1 requirement: every write executes on exactly one pinned shard thread that owns its data outright, so the write path has no locks, no shared memtables and no cross-core cache-line traffic.
- **Shards.** At open, the engine starts N shard threads, one per core, pinned with CPU affinity. N defaults to the physical cores available to the process, honoring cgroup CPU quotas and affinity masks; `Options::shards(n)` overrides it, and `shards(1)` is a valid single-threaded configuration. Each shard owns its tablets' memtables, its own WAL file, its own io_uring ring (or `pread` worker), a NUMA-bound arena region in the shared-memory mapping, and the flush and compaction work for its tablets.
- **Tablets.** Each table's row-key space is split into contiguous ranges called tablets, as in BigTable. A tablet belongs to exactly one shard at a time and holds every family for its rows, so each row lives on one shard and multi-family row atomicity needs no coordination. A new table starts as one tablet. Tablets split at a size threshold (default 256 MiB of live data) or under sustained write skew, and merge when small and cold.
- **Routing.** A versioned tablet map, immutable and swapped atomically, maps key ranges to shards and is read without locks. A writer finds the owner by binary search and submits to that shard's lock-free MPSC queue; if the caller is the owning shard thread, the write runs inline with no handoff.
- **Reads run on the caller's thread.** Gets and scans are not routed: they read memtables (single-writer, multi-reader), immutable SSTs and the shared cache directly, so hot reads keep the single-digit-microsecond target with no queue hop. Scans walk tablets in key order, so results stay ordered without merging per-shard streams. An optional strict mode also routes reads to the owner, for NUMA locality on large machines.
- **Two embedding modes, both first class.** *Engine-owned:* Pigeonhole spawns and pins its shard threads, and application threads (sync or async) submit work and are woken on completion. *Application-owned:* an application that is already thread-per-core (monoio, glommio, or a custom runtime) registers each of its core threads as a shard and drives the shard loop from its own event loop, using a `ShardContext` handle for inline local writes. The engine starts no threads of its own in this mode.
- **Rebalancing.** A balancer moves tablets between shards when write load or memtable size is skewed past a threshold. A move freezes the tablet's memtable, flushes it, records the new owner in the manifest, publishes a new tablet map, and forwards the writes that queued during the move. Reads are never blocked. Splits and merges use the same path.
- **Background work on the owning core.** Each shard runs flush and compaction for its own tablets as cooperative tasks in short time slices that always yield to foreground queue work. For write-heavy deployments, `Options::compaction_cores(k)` dedicates k extra pinned threads to compaction instead.
- **Shard count can change between opens.** WAL streams are replayed per stream at recovery, independent of the current shard count, and tablets are then assigned to the new shards.

### Ordering, snapshots and cross-shard commits
- **Global seqno, reserved per group.** Each shard's group-commit leader reserves a contiguous range of global seqnos with one atomic `fetch_add` per group, not per write, so the shared counter is touched thousands of times a second rather than millions.
- **Snapshot watermark.** A snapshot may only include a seqno once every commit at or below it is applied. Each shard publishes the lowest seqno it has reserved but not yet applied; a snapshot takes the minimum across shards (one cache-line read per shard), so it never sees a partial cut.
- **Single-shard fast path.** Single-row mutations, and batches or transactions whose rows all land on one shard, are one WAL record in the owner's stream with no coordination.
- **Cross-shard batches and transactions use two-phase commit** across shard WAL streams. The coordinating shard reserves one seqno for the whole commit and holds it pending. Each participant writes a PREPARE record with the commit id; once every prepare meets the requested durability, the coordinator writes a COMMIT decision record to its own stream, and participants apply. On recovery, a prepared commit whose coordinator stream holds its COMMIT record is applied, and one without is discarded. Because the seqno stays pending until every participant has applied, snapshots see all of a cross-shard commit or none of it.
- **Durability across shards.** A cross-shard commit returns only when every participant's PREPARE and the coordinator's COMMIT meet the requested level; under `GroupSync` each stream fsyncs as part of its own group.
- **Single manifest writer.** Flushes, compactions, splits, merges and tablet moves on every shard produce manifest edits, which go on a queue to one manifest task running on shard 0. It batches edits, writes the new manifest copy-on-write, flips the superblock, and then publishes a new view. Shards free superseded extents only after the edit is durable and no snapshot still references a view that uses them.
- **Views.** A view is one immutable object combining the tablet map, each tablet's active and frozen memtables, and the SST set from one manifest version. Any change to any of these publishes a new view atomically. A snapshot pins a seqno and a view together, and reads use only their snapshot's view, so a split, move or flush mid-read can never hide a frozen memtable or a new SST. A frozen memtable stays in every new view until its flushed SST is in the manifest.

### Versions, writes and transactions
- **MVCC by sequence number.** Every commit gets a global u64 seqno, encoded in every internal key right after the user timestamp (see Data model). A snapshot is a seqno taken from the watermark plus the view current at that moment (see Views); reads ignore anything newer. Snapshots pin SST extents via epoch-based reclamation (crossbeam-epoch style), never via locks.
- **Writers.** Each write runs on its owning shard. The shard's group-commit leader issues one write() to the shard's WAL stream and, for `GroupSync`, one `fsync` for the whole group, then publishes the group's seqnos. Except under `None`, no commit returns until its record has been handed to the kernel with write(); `Buffered` skips only the fsync, so a returned `Buffered` commit survives a process crash. Merge operands such as `incr` are written blind and resolved at read and compaction time.
- **Memtables.** One per tablet and family, written only by the owning shard and read concurrently by any thread: a single-writer, multi-reader skiplist over the shard's arena, which lives in the shared-memory file and links nodes by offset so reader processes can read it too; immutable once full, then flushed by the shard. Write stalls use a per-shard token bucket on L0 depth rather than hard stops, to keep p99 smooth.
- **Conditional writes.** `check_and_mutate(row, predicate, mutation)` (the engine name; the public API spells it `RowMutation::commit_if(condition)`) gives BigTable-style compare-and-set per row without a transaction. It runs on the owning shard, which executes writes for its rows one at a time, so the check and the mutation are atomic with no latch, and later writes to the row queue behind it in order.
- **Multi-row transactions (opt-in).** Optimistic concurrency: reads record the (row, family) ranges they touched. At commit, each participant shard checks its own tablets for conflicting commits since the snapshot seqno during PREPARE; any conflict aborts the whole commit. Serializable for the touched ranges, with no lock manager.
- **Multi-process.** One writer process and any number of reader processes on the same host, coordinated through a shared-memory file; see Multi-process readers below.
- **Crash safety.** Superblock flip is the only in-place write in the main file (recycled WAL segments are overwritten too, guarded by their epoch). On open: read both superblocks and pick the valid newest, replay every shard's WAL stream past its checkpoint, then resolve prepared cross-shard commits against their coordinators' decision records. Torn WAL tails are detected by CRC and truncated.

### Multi-process readers

Multi-process readers are a v1 requirement: one writer process and any number of reader processes on the same host open the same `.phdb` file, and readers see each commit as soon as the writer publishes it, without replaying the WAL.
- **Roles.** `Pigeonhole::open` opens the writer and takes an exclusive OS lock (`flock` on Unix, `LockFileEx` on Windows); a second writer fails with a typed error. `Pigeonhole::open_reader` opens a read-only handle in any number of other processes, with the same sync and async get and scan APIs. The reader type has no write methods, so misuse fails to compile. Readers start no shard threads; every read runs on the caller's thread.
- **Shared-memory file.** While any process has the database open, every process maps a `*.phdb-shm` region, memory-backed by default (see Location and size), or a file in a configurable `shm_dir` such as `/dev/shm`. It holds:
- a header with the format version, writer generation and current manifest version;
- each shard's published snapshot watermark;
- the current tablet map;
- the memtable arenas, so readers read the writer's memtables in place;
- a reader-slot table with each reader's process id, process start time and pinned snapshot.
- **Memtables readable across processes.** Memtable arenas live in the shared-memory file and link nodes by offset, never by pointer, so any process can traverse them. The writer publishes with release ordering and readers load with acquire ordering, the same discipline as in-process readers.
- **Snapshots and reclamation.** Views are published in the shared-memory region by version. A reader takes its snapshot (a seqno from the published watermarks plus the current view version) and records both in its slot. The writer frees memtable arenas and SST extents only below the oldest snapshot pinned anywhere, in-process or in a reader slot, and keeps each view's memtables until no slot pins that view. Slots whose process is gone (process id missing, or start time changed) are reclaimed, so a crashed reader can't pin space forever.
- **SSTs and manifest.** Readers map nothing writable. They read SSTs from the main file through their own I/O backend and their own block cache, and build each view from the published view record plus the manifest catalog of that record's own manifest version, re-reading the record when the durable root has moved past it (never a catalog of another version: issue #140).
- **Writer crash and restart.** The next writer recovers from the WAL streams and rebuilds the shared-memory file under a new generation; a reader re-attaches at its next snapshot when it sees the generation change. Until that writer starts, readers keep serving their current snapshots. The new writer frees every extent its recovered root does not name, including SSTs and manifest blocks that older snapshots read (their pins live in the abandoned region), so from then on a read through a snapshot taken before the restart fails with `SnapshotExpired`. Each read checks the generation after it read, seqlock style, which is sound because the writer publishes the generation before it allocates anything.
- **Scope and limits.** Same host and a local filesystem only, as with SQLite's WAL mode; network filesystems are refused at open. When the last process closes cleanly, the `-shm` file is removed, so the database is one file at rest.
- **Location and size.** The region is memory-backed, so dirty memtable pages are never written back to disk: `/dev/shm` on Linux, a POSIX `shm_open` object on macOS and BSD, and a pagefile-backed named file mapping on Windows. Its name derives from the database file's device and inode (file ID on Windows), so every process finds the same region however it spells the path. `Options::shm_dir` overrides the location, for example to a tmpfs mount of a chosen size. Size is fixed at open: the per-shard memtable budget (default 64 MiB) times the shard count, plus header, views and reader slots. Opening fails up front if the region can't be allocated.
- **NUMA placement.** Each shard's arena is a contiguous part of the mapping. On Linux the shard binds its part to its own NUMA node with `mbind` and touches it first from the shard thread; on other platforms this is a no-op.
- **Version checks.** The header carries a shared-memory layout version, separate from the file format version. A process whose layout version differs from a live region refuses to open with `ShmVersionMismatch` rather than misread it. A writer rebuilds the region under a new layout only when no other process is attached.

## Performance design

Low latency comes from never blocking a reader and bounding a cold read to one I/O; high throughput comes from batching every expensive thing (fsync, I/O submission, compaction) and keeping cores from sharing cache lines.

### Read path
- Memtable(s) for the family, newest first: a lock-free lookup, typically under 300 ns.
- Per-SST filters, newest level first; filters and the top-level index are pinned in memory, so negative lookups cost no I/O.
- One index-block lookup (cached) and one data-block read. With the block cache hot, the whole get is a few cache misses.
- Optional **row cache** per family for small, very hot rows, keyed by (row, family, snapshot-epoch).

Supporting choices:
- **Own the buffer pool.** A sharded CLOCK-Pro (or S3-FIFO) block cache over aligned buffers, not the OS page cache, so eviction respects per-family priority and reads can be zero-copy borrows.
- **I/O backends.** `io_uring` on Linux with registered buffers and O_DIRECT for SST reads and compaction; a `pread` thread pool on macOS, Windows, and any Linux host where io_uring is unavailable. The engine probes io_uring at open and falls back automatically, since many container runtimes block it. The pread backend is first class, held to the same correctness suite, not a degraded mode. Scans issue readahead for the next N blocks as one submission.
- **mmap is optional, not default.** It is fine for read-mostly files that fit in RAM, but page-fault stalls and no control over eviction hurt p99, so it is a read-only-mode option.
- **Bounded cold read.** Pinned index and filters mean a cold point get is one data-block I/O, about 20 to 80 µs on NVMe.

### Write path
- Group commit amortizes `fsync` across all concurrent committers; `Buffered` durability skips it entirely for caches and derived data.
- Each shard owns its WAL file and memtables, so appends and inserts take no locks; seqno publication is one atomic store per shard with release ordering.
- Values above the blob threshold are written once to a blob extent, so compaction never rewrites them.

### Background work
- Flush and compaction run on the owning shard as cooperative, time-sliced tasks with I/O rate limiting, always yielding to foreground work; `compaction_cores(k)` moves them to dedicated pinned threads instead.
- Compaction picks are per tablet, per family and per key range (subcompactions), so one large family or hot tablet doesn't starve the rest.
- **Thread-per-core is the execution model**, not an option: see Thread-per-core execution under Concurrency for shards, tablets, routing and rebalancing.

### Scans
- The block iterator decodes lazily and applies qualifier, version, and time filters before materializing cells.
- Row-start tables let `columns_per_row(n)` and "latest only" skip the remainder of a wide row in O(1) per block.
- An Arrow export path yields `RecordBatch`es (row, family, qualifier, ts, value) for handoff to DuckDB or Polars without per-cell allocation.

### How it will be measured

A benchmark harness is built in the first phase, before any optimization work: YCSB A–F, a sparse-wide workload (1M rows × 0 to 10K qualifiers, Zipfian), a time-series workload with TTL, and a scan-heavy adjacency workload, each reporting p50/p99/p99.9 and throughput against RocksDB (with a hand-written wide-column key encoding), SQLite EAV, and fjall.

## Crate breakdown

The engine is one Cargo workspace of small crates with strictly downward dependencies, so agents can build components in parallel against each crate's public trait and mock the layers below.

*[Diagram: crate layers · 17 crates, 8 layers]*

Each layer may use any layer beneath it; the simulator drives the public API from above and replaces real files from below, so every crate is tested under injected faults.

### Workspace rules
- **Dependencies only point down.** No crate depends on a crate in its own layer or above; `pigeonhole-format` depends on no other workspace crate.
- **Contracts first.** Each crate's public traits and types are written and reviewed before its implementation, and every crate below the engine ships an in-memory or mock implementation for the layer above to test against.
- **`unsafe`**** is fenced.** Only `pigeonhole-io`, `pigeonhole-cache` and `pigeonhole-memtable` may contain `unsafe`, each with a documented safety argument per block; every other crate sets `#![forbid(unsafe_code)]`.
- **Every crate runs under the simulator.** All file access goes through the `Vfs` trait so `pigeonhole-sim` can inject faults and control time and scheduling.
- **Definition of done** for any crate: its acceptance tests pass, it passes the workspace's deterministic-simulation suite, and its public API has rustdoc with examples.
- **License: MIT.** Every crate is MIT-licensed, with `license = "MIT"` in each `Cargo.toml` and one `LICENSE` file at the workspace root. Dependencies must carry MIT-compatible permissive licenses (MIT, Apache-2.0, BSD, ISC, Zlib), enforced in CI with `cargo-deny`.
- **Toolchain and platforms.** MSRV is the latest stable Rust minus two releases, checked in CI. Linux, macOS and Windows are all supported targets; Windows uses the `pread` backend and `LockFileEx` for the writer lock.

### Crates

| Crate | Responsibility | Depends on | Key interfaces | Acceptance tests | Phase |
|---|---|---|---|---|---|
| `pigeonhole-format` | All on-disk encodings: internal key (escaping, inverted timestamp and seqno), data and index blocks, SST footer, superblock, manifest records, WAL frames, checksums, format version tags | none | `encode_key` / `decode_key`, block builders and readers, `FormatVersion` | Property tests that encoded byte order equals logical key order; round-trip tests; fuzzed decoders never panic; golden files frozen at 1.0 | 1 |
| `pigeonhole-io` | `Vfs` trait over files with aligned buffers and submit/complete I/O; backends: `pread` thread pool (safe reference), io_uring (Linux), and the fault-injecting simulated VFS | format | `Vfs`, `File`, `IoBuf`, `Completion` | Backend parity suite runs identical operations on every backend; Miri on the safe paths; fault-injection self-tests | 1 (pread, sim), 3 (io_uring) |
| `pigeonhole-pager` | Page file, superblock pair and flip, extent allocator and free-space bitmap, epoch-based deferred freeing, online `shrink` | io, format | `Pager`, `Extent`, `commit_root` | Crash at every write never yields an unopenable file; allocator never double-allocates; freed extents unreachable from any live snapshot | 1 |
| `pigeonhole-wal` | One stream per shard: recycled preallocated segments with epochs, group commit, PREPARE and COMMIT records for cross-shard commits, recovery reader, checkpoint | io, format | `Wal` trait, `CommitTicket`, `Recovery` | Torn-tail truncation; stale records in reused segments rejected; group commit preserves seqno order; a returned `Buffered` commit survives process kill | 1 |
| `pigeonhole-memtable` | Single-writer, multi-reader skiplist over a shard-owned arena in the shared-memory file, linked by offsets so reader processes can traverse it, one per tablet and family; freeze and iterate | format | `Memtable`, `MemIter` | loom model tests; linearizability check against a locked `BTreeMap` | 1 |
| `pigeonhole-cache` | Sharded block cache and row cache, pinning, ref-counted `Cell` handles | io | `BlockCache`, `RowCache`, `BlockHandle` | Eviction respects pins and priority; no use-after-evict under loom; hit-path allocation-free (benchmarked) | 1 (basic), 3 (tuned) |
| `pigeonhole-runtime` | Shard threads and CPU pinning, NUMA-aware arenas, per-shard MPSC submission queues, cooperative time-sliced task scheduler with foreground priority, completion wakeups for sync and async callers, ShardContext for application-owned threads | io | Shard, ShardContext, Task, Submitter | Deterministic scheduling under the simulator; foreground latency bounded while background tasks run; both embedding modes pass the same suite; no cross-shard shared mutable state outside documented queues and the seqno counter | 1 |
| `pigeonhole-shm` | Shared-memory file layout and lifecycle: header and writer generation, published watermarks, tablet map, offset-based arena regions, reader-slot table with liveness checks, writer lock, remap on generation change, memory-backed placement and naming by device and inode, per-shard NUMA binding, view publication by version, layout version checks | io, format | ShmRegion, ReaderSlot, WriterLock, Generation | Multi-process tests: readers see every commit in order and never a partial cross-shard commit; killed readers' slots are reclaimed; writer kill and restart leaves readers on a valid snapshot, then remapped; second writer refused | 1 (layout), 4 (readers) |
| `pigeonhole-sst` | SST writer and reader, ribbon or bloom filters, partitioned index, block iterator with filter pushdown and row skipping | format, io, cache | `SstWriter`, `SstReader`, `ScanFilter` | Write-then-read equivalence over random data; filters never give false negatives; pushdown results equal unfiltered-then-filtered | 1 |
| `pigeonhole-compaction` | Leveled, tiered and FIFO-by-time pickers; subcompactions; version, TTL and tombstone GC; merge-operand resolution; blob separation and blob GC | sst, pager, format | `CompactionPicker`, `MergeOperator`, `CompactionJob` | Compaction never changes the result of any read at any live snapshot (checked against the model); space reclaimed after TTL | 1 (leveled), 2 (rest) |
| `pigeonhole-engine` | Tables, families and tablets; tablet map, routing, splits, merges and rebalancing; version set and manifest with a single manifest writer; views; global seqno reservation, snapshot watermark and cross-shard two-phase commit, read and write paths, durability resolution, owner-serialized `check_and_mutate`, OCC transactions, background scheduling, online backup | all crates above | `Engine`, `Snapshot`, `WriteBatch`, `Txn` | Full model-checked simulation suite; crash-recovery suite; durability matrix (each mode × each failure type); cross-shard atomicity with a crash injected at every two-phase-commit step; identical results for every shard count from 1 to 64; throughput scaling gate | 1 to 4 |
| `pigeonhole` | Public crate: `Pigeonhole::open`, table and family builders, mutations, gets, scans, sync and async APIs, `Durability`, typed errors; features `async` (default) and `tokio` | engine | The API in this doc | Sync/async parity suite; doc examples compile and run; API semver checks in CI | 1 (sync), 3 (async) |
| `pigeonhole-arrow` | Scan export as Arrow `RecordBatch`es | pigeonhole | `scan_to_arrow` | Batches round-trip to the same cells as a plain scan | 4 |
| `pigeonhole-cli` | `phdb` binary: `shell`, `dump`, `compact`, `check`, `backup` | pigeonhole | CLI commands | Snapshot tests of CLI output; `phdb check` detects injected corruption | 4 |
| `pigeonhole-capi` | Stable C ABI and `pigeonhole.h` (future; see Language scope) | pigeonhole | `extern "C"` functions | ABI compatibility checks; C test program | Future |
| `pigeonhole-sim` | Deterministic simulation harness: seeded scheduler, simulated VFS, crash points, and the `BTreeMap` reference model | io (sim backend), pigeonhole | `Sim`, `Model`, `Workload` | Detects known bugs seeded into a test build (mutation testing of the checker) | 1 |
| `pigeonhole-bench` | Benchmark workloads and comparison runners against RocksDB, SQLite and Fjall; p50/p99/p99.9 reporting | pigeonhole | Workload definitions | Reproducible results within a set tolerance on the reference hardware | 1 |

The Phase column maps each crate to the roadmap; a crate listed in several phases gains features in each.

## Build plan

The fleet builds along one critical path, and parallel work starts only after the foundations exist and every interface is frozen. Each crate has a task brief in [task-briefs.md](task-briefs.md).

*[Diagram: build order · 5 steps, 2 gates]*

Only step 4 runs in parallel; every step before it is on the critical path, so it is staffed with one agent at a time.

### Steps
- **Bootstrap (one agent).** Workspace skeleton with every crate as an empty library; CI running tests, Miri, loom, `cargo-deny`, the MSRV check and `cargo-semver-checks`; the benchmark runner wired to the reference hardware; and a `CONTRIBUTING.md` carrying the agent rules below.
- **Interface freeze (one agent, then your review).** Every crate's public traits, types and error enums with `todo!()` bodies, plus a `FORMAT.md` that specifies every on-disk and shared-memory byte. Everything compiles; nothing works yet. Gate: you approve the interfaces.
- **Foundations, in order.** `format`, then `io` with the `pread` and simulated backends, then `sim` with the reference model. Gate: the simulator can crash and replay a toy store built on `io`.
- **Components, in parallel.** `pager`, `wal`, `memtable`, `cache`, `runtime` and `shm`, each tested against mocks of what it uses; then `sst`.
- **Assembly.** `compaction` (leveled) and `engine`, then the sync `pigeonhole` API and `bench`. Gate: the Phase 1 gate.
- **Later phases.** Phase 2 to 4 features land crate by crate on the frozen interfaces, each phase closed by its gate.

### Agent rules
- One agent owns one crate per task and edits only that crate and its tests.
- Frozen interfaces change only through an interface-change request: a short note in the repo naming the change and every caller, approved before code changes.
- No new dependency without a passing `cargo-deny` license check and a one-line justification.
- A task is done when its brief's acceptance tests and the workspace simulation suite pass in CI. Nothing merges on red.
- When the spec is silent or contradictory, the agent stops and files a question instead of guessing.

## Prior art and positioning

No shipping embedded library combines the BigTable data model, single-file deployment, and a latency-first engine; the closest pieces are fast KV engines that explicitly stop short of columns.

| System | Shape | What Pigeonhole takes | Where it falls short for this niche |
|---|---|---|---|
| Fjall | Embedded Rust LSM KV, per-keyspace trees, KV separation, compaction filters | Closest engine design; proof the Rust LSM approach works | Self-described as not a wide-column database; a directory of files, not one file |
| Smoltable | Bigtable-inspired toy wide-column DB on Fjall | Validates demand and the model | Explicitly a toy; standalone rather than a library-first, single-file design |
| RocksDB | Embedded C++ LSM KV with column families | Block format, partitioned index, filters, universal compaction | C++ FFI, hundreds of knobs, no row/qualifier/version semantics |
| LMDB / redb | Embedded B+tree KV, single file | Double-buffered meta pages, single-file ergonomics, multi-process readers | Single writer, write amplification on sparse random inserts, no versions |
| SQLite | Embedded relational, single file | File format discipline, WAL sidecar, CLI, stability promise | Sparse columns become EAV or JSON; no family locality or cell versions |
| DuckDB | Embedded columnar OLAP, single file | Product shape: zero-config, great bindings, Arrow everywhere | Built for scans of dense columns, not point reads or sparse writes |
| HBase / Cloud Bigtable / Cassandra / Scylla | Distributed wide-column servers | The data model, filters, check-and-mutate, TTL and version GC | A server and a network hop; Cassandra-style schemas predeclare columns |
| Tarantool | In-memory server with Lua, Vinyl LSM engine | Thread-per-core ideas | Server process, declared-field tuples, SQL-leaning |

The pitch in one line: **SQLite's file, RocksDB's engine, BigTable's model, Rust's safety, and DuckDB's ergonomics.**

**Decision: ****Pigeonhole**** owns its full engine.** It does not build on Fjall's `lsm-tree` or any other storage crate. The single-file layout, extent allocator, io_uring path and owned buffer pool all require control of the SST, cache and I/O layers end to end. Existing engines are reference material, not dependencies.

## Roadmap, risks, open questions

Pigeonhole is built in four gated phases, in order. Each phase is a self-contained work package with a measurable gate, and the next phase starts only when that gate passes.

*[Diagram: roadmap · 4 phases, 4 gates]*

Each phase ships only when its gate passes; the latency work deliberately follows the data model so benchmarks measure the real workload. The 1.5× RocksDB comparison is reported from Phase 1 and enforced at the Phase 3 gate.


### Phases (from the roadmap diagram)

| Phase | Name | Contents | Gate |
|---|---|---|---|
| 1 | Core engine | WAL, memtable, SSTs; single-file allocator; leveled compaction; Rust get, put, scan; shards and tablets | **Correctness:** fault-injection suite green; RocksDB gap reported, not gated |
| 2 | Wide-column model | Versions and TTL; filters, merge ops; `check_and_mutate`; blob separation; per-family compaction | **Model value:** sparse-wide bench beats hand-keyed RocksDB, and SQLite EAV on throughput, get and put p99 and p99.9; wide overwritten row reads are a documented gap (amended by D193) |
| 3 | Latency engine | io_uring, O_DIRECT; owned block cache; row cache; group commit tuning; async Rust API | **Latency:** p50 and p99 targets in the Goals table met; within 1.5× of RocksDB; every roadmap item delivered and every Goals-table target met, per the checklist in #406, on the reference hardware (amended by D197) |
| 4 | Hardening and 1.0 | Stable C ABI, Arrow; CLI and dump tools; multi-process readers; OCC transactions | **1.0 release:** file format frozen; compatibility promise |

### Risks
- **Crash consistency is the whole product.** Mitigation: a pluggable VFS with fault injection (torn writes, reordered fsyncs, ENOSPC), deterministic simulation testing of the full engine, and a model checker that compares every operation against an in-memory `BTreeMap` reference.
- **Space reclamation inside one file.** LSM churn fragments extents. Mitigation: power-of-two extents, a compaction-aware allocator that prefers freeing whole regions, and an online `shrink` that relocates tail extents.
- **`unsafe`**** surface in the I/O path.** io_uring with registered buffers and a custom cache need unsafe code. Mitigation: confine it to one crate behind a safe trait, run Miri and loom on the rest, and keep a pure-safe `pread` backend as the reference.
- **Freezing the format too early or too late.** Mitigation: version every block and the superblock from day one; promise forward compatibility only at 1.0.
- **Scope creep toward SQL.** Mitigation: keep query features in separate crates (DataFusion provider, Arrow export) that depend on the core, never the reverse.
- **Cross-shard coordination and hot tablets.** Two-phase commit and the snapshot watermark add cost that a single-log design avoids, and one hot tablet can saturate one core. Mitigation: the single-shard fast path covers all single-row writes; the balancer splits and moves hot tablets; the scaling gate and a skewed-workload benchmark run from Phase 1 so regressions surface early.
- **Cross-process shared memory.** Offset-based arenas and reader slots are easy to get subtly wrong, and a reader that dies mid-read must never corrupt anything. Mitigation: readers never write to shared memory except their own slot; the multi-process suite kills readers and the writer at random points under the simulator; offset validation in debug builds.

### Open questions
- **Naming checks.** RubyGems and trademark availability for Pigeonhole; confirm and register the planned domains in the naming table.

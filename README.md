# Pigeonhole

**An embedded, single-file, wide-column store in Rust** — BigTable's data model with SQLite's deployment model.

[![CI](https://github.com/CodingAnarchy/pigeonhole/actions/workflows/ci.yml/badge.svg)](https://github.com/CodingAnarchy/pigeonhole/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/pigeonhole.svg)](https://crates.io/crates/pigeonhole)
[![docs.rs](https://img.shields.io/docsrs/pigeonhole)](https://docs.rs/pigeonhole)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **Maturity: 1.x.** The public API of `pigeonhole`, `pigeonhole-arrow`, `pigeonhole-vector`, `pigeonhole-text`, `pigeonhole-memory` and `phdb` follows semver, from 1.0.0 or a crate's first release ([D282](docs/design/decisions/phase-3.md#d282)), and every 1.x release reads and writes file format 4 ([`FORMAT.md` §13](FORMAT.md#13-compatibility-promise) is the compatibility promise). All four phases are complete: the core engine (Phase 1), the wide-column model (Phase 2), the latency engine (Phase 3) and hardening (Phase 4), each with its performance gate on the reference machine. The 1.0 release gate passed on 2026-10-30 at `main` [`790ae3a0`](https://github.com/CodingAnarchy/pigeonhole/commit/790ae3a0): the Phase 2 floors, and within 1.5× of RocksDB on every workload. 1.0 ships with documented gaps, listed in [`docs/status.md`](docs/status.md).

> **Status: usable and disk-backed.** Memtables flush into the file and compact, so data is bounded by the disk, not memory; commits are crash-safe through the write-ahead log, and a clean close leaves one file. The current release is on the [releases page](https://github.com/CodingAnarchy/pigeonhole/releases), and the [changelog](CHANGELOG.md) says what each one changed. Every data operation has a blocking form and an async form, on by default ([async guide](docs/guide/async.md)).

SQLite owns local OLTP and DuckDB owns local OLAP. Pigeonhole targets the missing quadrant: local **sparse, versioned, row-scan-heavy** data — feature stores, time series keyed by entity, crawl and event caches, graph adjacency, per-user state. `cargo add pigeonhole`, open a file, and get rows of arbitrary sparse columns grouped into families, with versions, TTLs, prefix and range scans, and no server.

## Install
```sh
cargo add pigeonhole
```
or `pigeonhole = "1"` in `Cargo.toml`. Requires Rust 1.96 (2024 edition). API docs are on [docs.rs](https://docs.rs/pigeonhole); what changed is in the [changelog](CHANGELOG.md) and the [latest release](https://github.com/CodingAnarchy/pigeonhole/releases/latest).

```rust
use pigeonhole::{Pigeonhole, Options, Family};

# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("crawl.phdb"), Options::default())?;
let pages = db.table("pages")?
    .family("meta", Family::default().max_versions(1))
    .family("links", Family::default().bloom_bits(10))
    .create_if_missing()?;

// Single-row atomic mutation
pages.mutate(b"com.example/a")
    .put("meta", b"status", b"200")
    .put("links", b"com.example/b", b"")
    .commit()?;

// Point read
let status = pages.get(b"com.example/a", "meta", b"status")?;

// Ordered scan of one family
for row in pages.scan(b"com.example/"..b"com.example0").family("links").iter()? {
    let row = row?;
    // ...
#   assert_eq!(row.key(), b"com.example/a");
}
# assert_eq!(status.unwrap().value(), b"200");
# Ok::<(), pigeonhole::Error>(())
```

## What you can do
One line per family of operations, with the calls that do it, a guide page that explains it, and its entry in the [agent reference](docs/guide/agent-reference.md) (every signature, limit and error). [`docs/status.md`](docs/status.md#capabilities) says which release brought each one. Everything here is released, except what is marked **on `main`, 1.3.0**.

- **Read and write a row:** `Table::mutate(row)` with `put`, `put_at`, `delete_cell`, `delete_column`, `delete_family`, `delete_row`, then `commit()`, atomic per row. Read it back with `Table::get(row, family, qualifier)` or `Table::row(row).read()`. Guide: [Getting started](docs/guide/getting-started.md#write-one-row-atomically). Reference: [`RowMutation`](docs/guide/agent-reference.md#rowmutation-builder-each-self---self), [`Table`](docs/guide/agent-reference.md#tablebuilder--table--readtable).
- **Conditional writes (compare-and-set):** `RowMutation::commit_if(&Condition)`, BigTable's check-and-mutate (`Condition::Exists`, `Absent`, `Value`). `commit_if_returning` also returns the row's state whichever way the check went, and `commit_returning` with `return_previous` gives read-modify-write and get-and-set. Guide: [Getting started](docs/guide/getting-started.md#conditional-writes-and-transactions), [Data modeling](docs/guide/data-modeling.md#compare-and-set-then-read-commit_if_returning). Reference: [`RowMutation`](docs/guide/agent-reference.md#rowmutation-builder-each-self---self).
- **Batches:** `Pigeonhole::write_batch()`, then `WriteBatch::put`, `incr`, `delete_row` and the other writes, then `commit()` or `commit_with(Durability)`: many rows and tables, atomically. Guide: [Getting started](docs/guide/getting-started.md#write-many-rows-at-once). Reference: [`WriteBatch`](docs/guide/agent-reference.md#writebatch-methods-take-mut-self---mut-self).
- **Optimistic transactions:** `Pigeonhole::transaction()`, or `Pigeonhole::transact(&RetryPolicy, body)`, which retries a `Conflict` for you. They are serializable over what they read. Guide: [Transactions](docs/guide/transactions.md). Reference: [`Pigeonhole`](docs/guide/agent-reference.md#pigeonhole).
- **Multi-get:** `Table::keys(&KeySet).read()` reads many rows in key order at one snapshot. Guide: [Graphs](docs/guide/graphs.md#frontiers-many-nodes-at-once). Reference: [Wide rows, multi-get, traversal](docs/guide/agent-reference.md#wide-rows-multi-get-traversal-110).
- **Counters and aggregates:** `Family::counter()` with `incr` and `incr_at` (time buckets). `Family::aggregate(Aggregation::Min)` (also `Max`, `Hll`, `Set`, `Quantiles(..)`, `Bloom(..)` and more) with `aggregate` and `aggregate_bytes`. Every write is blind, with no read first. Guide: [Data modeling](docs/guide/data-modeling.md#counters). Reference: [`RowMutation`](docs/guide/agent-reference.md#rowmutation-builder-each-self---self), [`Family`](docs/guide/agent-reference.md#family).
- **Versions and TTL:** `Family::max_versions(n)`, `Family::ttl(duration)`, writes at your own timestamp with `put_at`, and reads with `versions(n)` and `time_range(..)`. Guide: [Data modeling](docs/guide/data-modeling.md#time-series-with-ttl), [Scans and filters](docs/guide/scans-and-filters.md#versions-and-time-ranges). Reference: [`Family`](docs/guide/agent-reference.md#family), [`RowRead` / `Scan`](docs/guide/agent-reference.md#rowread--scan-builders-self---self).
- **Scans with filters:** `Table::scan(range)`, `scan_prefix(prefix)` and `scan_bounds(..)`, with `family`, `qualifier_prefix`, `value_filter(ValueFilter)`, `limit` and `snapshot`. The filters run inside the engine. Guide: [Scans and filters](docs/guide/scans-and-filters.md). Reference: [`RowRead` / `Scan`](docs/guide/agent-reference.md#rowread--scan-builders-self---self).
- **Byte budgets:** `Scan::read_within(bytes)`, `KeysRead::read_within(bytes)` and `Nearest::run_within(bytes)` return as much as fits, and `resume()` gives the position to resume from. Guide: [Scans and filters](docs/guide/scans-and-filters.md#byte-budgets). Reference: [Byte budgets](docs/guide/agent-reference.md#byte-budgets-110-d308).
- **Wide rows and graph walks:** walk a row with `Table::row(row).cells()` or `.pages(n)`, resumable with `resume_after`. Run a breadth-first traversal in the engine with `Table::traverse(&KeySet).family(edges).hops(n).run()`. Guide: [Graphs](docs/guide/graphs.md). Reference: [Wide rows, multi-get, traversal](docs/guide/agent-reference.md#wide-rows-multi-get-traversal-110).
- **Vectors:** declare a family with `Family::vector(VectorSpec::new(..)?)`, write with `put_vector_f32`, and search exactly with `Scan::nearest_f32(query, k).run()`. For large collections, the IVF index `pigeonhole_vector::Index` (`insert_f32`, `search_f32`). Guide: [Vectors](docs/guide/vectors.md). Reference: [Vectors](docs/guide/agent-reference.md#vectors-110).
- **Full-text search:** `pigeonhole_text::Index::create(..)`, written in your own transaction with `put_in` and searched with `search(query, k).run()`: BM25, phrases, fuzzy terms and highlighting. **On `main`, 1.3.0:** `Index::pack()` moves postings into blocks, so a search of a common word reads several times less (a packed index can't be opened by 1.2, so pack only once you won't roll back); `Search::query_stats`, `QueryStats::merge` and `Search::stats` score a collection split across several indexes as one; `rebuild_with` stops and `resume_rebuild_with` resumes a rebuild. Guide: [Full-text search](docs/guide/text-search.md), [Packing an index](docs/guide/text-search.md#packing-an-index), [A collection split across indexes](docs/guide/text-search.md#a-collection-split-across-indexes). Reference: [Companion crates](docs/guide/agent-reference.md#companion-crates-110).
- **Agent memory:** `pigeonhole_memory::Store::create(..)`, then `remember`, `recall(&[ns]).text(..).run()` (hybrid recall within a byte budget), `link`, `supersede`, `forget` and `consolidate`. Guide: [Agent memory](docs/guide/agent-memory.md). Reference: [Companion crates](docs/guide/agent-reference.md#companion-crates-110).
- **Change feeds:** open with `Options::change_feed(FeedRetention::bytes(n))`. Then `Table::feed().open()`, or `Pigeonhole::feed()` for every table, gives whole commits in order through `Feed::next`, resumable from a `FeedToken`. Guide: [Change feed](docs/guide/change-feed.md). Reference: [Change feed](docs/guide/agent-reference.md#change-feed-110-d310).
- **Snapshots, backups and reader processes:** `Pigeonhole::snapshot()` and `durable_snapshot()`, `backup(dest)` while writes continue, and `Pigeonhole::open_reader` for other processes on the host. Guide: [Scans and filters](docs/guide/scans-and-filters.md#snapshots), [Reader processes](docs/guide/readers.md). Reference: [`Pigeonhole`](docs/guide/agent-reference.md#pigeonhole).
- **Durability:** each commit picks its level, `None`, `Buffered`, `GroupSync` (the default) or `Sync`, with `RowMutation::durability(d)` or `commit_with(d)`, over the default set by `Options::durability(d)`. **On `main`, 1.3.0** (Linux): SST and blob output starts its writeback as it is written, so under sustained `GroupSync` writes put latency no longer climbs through a long session (D334). Guide: [Durability](docs/guide/durability.md).
- **Disk space:** **on `main`, 1.3.0** (Linux and macOS): the data file's free space is given back to the filesystem. After compaction frees space, a paced background pass punches holes in it, so a database that shrank after large deletes or TTL expiry occupies about its live data on disk (`du`); its length (`ls -l`) stays the same. On by default; `Options::punch_freed(false)` turns it off. Reference: [`Options`](docs/guide/agent-reference.md#options-all-self---self-process-local-not-stored-in-file). Metrics: [Observability](docs/guide/observability.md).
- **Async:** every data operation has an async form, on by default (module `nonblocking`). Guide: [Async](docs/guide/async.md). Reference: [Async](docs/guide/agent-reference.md#async-feature-async-default-on-module-nonblocking).
- **Arrow, Python and Ruby:** `pigeonhole-arrow` exports a scan as Arrow record batches. The `pigeonhole-db` package (Python) and gem (Ruby) bind the API. Guides: [Arrow export](docs/guide/arrow.md), [Python](docs/guide/python.md), [Ruby](docs/guide/ruby.md). Reference: [Arrow export](docs/guide/agent-reference.md#arrow-export-pigeonhole-arrow-691), [Companion crates and bindings](docs/guide/agent-reference.md#companion-crates-and-bindings-110).
- **`phdb` and `phdb mcp`:** inspect, check, dump, load and back up a database from the shell, or give an AI agent MCP tools over one. Guides: [`phdb`](docs/guide/cli.md), [Using Pigeonhole from AI agents](docs/guide/ai-agents.md). Reference: [Command line](docs/guide/agent-reference.md#command-line-phdb-692).
- **`phdb serve`:** a server for one database, with gRPC and HTTP/JSON on one port and remote MCP at `/mcp`. It adds TLS and scoped tokens, sessions with transactions and pinned snapshots, the change feed, agent memory (`--memory`), backups, read replicas (`--replica-of`, `phdb promote`) and Prometheus `/metrics`. **On `main`, 1.3.0:** `--text-pack` packs the text indexes in the background (off by default: see full-text search above); feed subscribers get their events in batches, which costs the threads that answer writes less; `Text.Rebuild` stops at a shutdown and resumes on its own. Guide: [Serving a database](docs/guide/serve.md). Reference: [`phdb serve`](docs/guide/agent-reference.md#phdb-serve-120).
- **Clusters:** `phdb serve --cluster` splits one database across nodes by key range, with routing, transactions and snapshots within a partition, scans and feeds across partitions, schema through the map, moves, backups and `phdb cluster`. **On `main`, 1.3.0:**
  - **read replicas of a partition:** copies listed in the map's `replicas` that follow the owner's feed. `Get`, `ReadRow`, `MultiGet`, `Scan`, `Vectors.Nearest` and `Graph.Traverse` sent with `x-pigeonhole-read-replica-max-lag-ms` may be served by a replica that fresh. A replica is made its partition's owner by hand with `phdb cluster promote` (#2007). Guide: [Read replicas in a cluster](docs/guide/serve.md#read-replicas-in-a-cluster);
  - **vector and text indexes:** `Vectors.CreateIndex` and `Text.CreateIndex` place an index in the map, writes route by id, and `Vectors.Search`, `Vectors.Verify`, `Text.Verify`, `Text.Search`, `Text.Rebuild` and `Vectors.BulkLoad` run across an index's partitions; a text search is scored as one index (#2029), a rebuild's counts are summed (#2041), and a bulk load is split by id, each partition taking the load's `expected` (#2045). Guide: [Indexes on a cluster](docs/guide/serve.md#indexes-on-a-cluster);
  - **agent memory and `/mcp` on a cluster node:** `--memory` with a `memory` placement in the map; `Recall`, `Watch`, `MarkUsed` and `Forget` across partitions. Guide: [Agent memory on a cluster](docs/guide/serve.md#agent-memory-on-a-cluster);
  - **leases:** a partition serves only while a majority of the map's nodes grant it a lease (`--cluster-lease-ms`), so a node cut off from the majority stops serving. Guide: [Fencing and relocation](docs/guide/serve.md#fencing-and-relocation);
  - **`phdb cluster restore --partition`** puts one partition back from a cluster backup. Guide: [Backup and restore](docs/guide/serve.md#backup-and-restore);
  - **transactions across partitions** (D329, #1483): a transaction begun with `spans_partitions` reads and writes rows of several partitions and commits them atomically. A share left in doubt by a failure is settled by its node's resolver, `Transactions.Outcome` answers what became of a transaction, and `phdb cluster txn decide` decides a share by hand. Guide: [Transactions across partitions](docs/guide/serve.md#transactions-across-partitions);
  - **automatic failover, off by default** (D330, #1484): with the map's `failover` on, the map's writer promotes a down partition's freshest replica, and a down writer is replaced by an elected one. It can lose the commits the replica hadn't applied. Guide: [Automatic failover](docs/guide/serve.md#automatic-failover);
  - **`Admin.Info`, `Flush` and `Compact`** on a cluster node.

  Planned for 1.3.0 and not finished on `main`: online split and range move (D331, #1499) and a balancer (D332, #1485). Their map records, gates and measurements are on `main`; the commands and the balancer itself are to come. [`docs/status.md`](docs/status.md#capabilities) has each one's state.

  Guide: [Clusters](docs/guide/serve.md#clusters). Reference: [`phdb serve`](docs/guide/agent-reference.md#phdb-serve-120).

## Data model
A database is a sorted, sparse, versioned map: `(table, row, family, qualifier, timestamp) → value`.

| Concept | Meaning |
|---|---|
| **Table** | A namespace with its own families. Many tables share one file. |
| **Row key** | Arbitrary bytes (≤ 64 KiB), sorted lexicographically. Unit of atomicity and locality. |
| **Family** | Declared up front; its own physical LSM tree with its own policy (compression, bloom bits, versions, TTL, blob threshold, cache priority, compaction style). An aggregate family keeps one value per cell from blind writes: an `i64` sum (a counter), minimum or maximum, or a distinct count (HyperLogLog). |
| **Qualifier** | Arbitrary bytes created on write, sorted within the family. Absent cells cost nothing. |
| **Timestamp** | `u64`, newest first. Hybrid logical clock by default; user-supplied for event time. |
| **Value** | Bytes, up to 4 GiB − 2. Values above a family's blob threshold live in blob files; one too large for the WAL and memtable (above the smaller of 64 MiB and half a shard's memtable arena) is written to a blob file as it is committed. Optional typed merge operators. |

## Performance
Measured on the reference machine (Hetzner AX102: AMD Ryzen 9 7950X3D, 16 cores, NVMe, Linux), in the 1.0 release gate of 2026-10-30 on `main` [`790ae3a0`](https://github.com/CodingAnarchy/pigeonhole/commit/790ae3a0). 1 million rows (about 1 GB, so reads that miss the 256 MiB block cache hit the OS page cache). [Performance compared with SQLite](docs/guide/performance.md) and [`docs/bench.md`](docs/bench.md) have the full tables and method.

**One client** (`Durability::Buffered`):

| Operation | Throughput | p50 | p99 |
|---|--:|--:|--:|
| Row read (YCSB C) | 385K ops/s | 0.94 µs | 12.0 µs |
| Point get, wide rows (sparse-wide) | — | 2.5 µs | 9.6 µs |
| Put (YCSB A) | — | 5.5 µs | 8.5 µs |
| Scan of up to 100 rows (YCSB E) | 20K ops/s | 48 µs | 111 µs |
| Reads and read-modify-writes (YCSB F) | 88.9K ops/s | 12.4 µs | 27.6 µs |

**16 clients**, against RocksDB and SQLite (EAV) with the same memory. Throughput, then p99 over every operation:

| Workload | Durability | Pigeonhole | RocksDB | SQLite |
|---|---|--:|--:|--:|
| Reads (YCSB C) | Buffered | 2.42M/s, 26 µs | 1.62M/s, 21 µs | 212K/s, 174 µs |
| | GroupSync (default) | 2.46M/s, 25 µs | 1.42M/s, 23 µs | 212K/s, 177 µs |
| 50% updates (YCSB A) | Buffered | 1.55M/s, 34 µs | 698K/s, 69 µs | 86K/s, 1.1 ms |
| | GroupSync (default) | 486K/s, 98 µs | 193K/s, 214 µs | 39K/s, 82 µs |
| | Sync | 422K/s, 165 µs | 191K/s, 212 µs\* | 39K/s, 81 µs |
| Scans (YCSB E) | Buffered | 226K/s, 183 µs | 133K/s, 270 µs | 43K/s, 1.6 ms |
| | GroupSync (default) | 199K/s, 202 µs | 106K/s, 383 µs | 43K/s, 3.4 ms |
| Sparse wide rows | Buffered | 97K/s, 2.2 ms | 74K/s, 3.1 ms | 42K/s, 3.4 ms |
| | GroupSync (default) | 84K/s, 2.6 ms | 59K/s, 3.5 ms | 34K/s, 8.3 ms |

`Durability::Sync` gives each commit its own fsync, never shared; SQLite's row is `synchronous=FULL` in WAL mode, whose single writer also fsyncs each commit. \*RocksDB's `sync=true` shares one fsync across a write group, so it compares with `GroupSync`. At one client (YCSB A at `Sync`), Pigeonhole does 44K ops/s at p99 50 µs and SQLite 42K at 71 µs.

**Durable commits** (`GroupSync`, 16 shards, 16 commits in flight per shard): 11.2M cells/s, commit p50 71 µs, p99 127 µs.

## Configuration
`Options::default()` is a valid configuration. The options most users set, with their defaults:

- **`durability`**: `GroupSync`, a commit returns once it is fsynced, with concurrent commits sharing the fsync. `Buffered` skips the fsync (survives a process crash, not power loss); `Sync` fsyncs each commit alone ([Durability](docs/guide/durability.md)).
- **`block_cache`**: 256 MiB, decoded SST blocks.
- **`row_cache`**: 64 MiB, hot rows of small families (`0` turns it off).
- **`memtable_budget`**: 64 MiB of write buffer per shard.
- **`shards`**: one per physical core.
- **`adaptive_shard_threads`**: on, sizing the shard threads to the load; `shard_threads(k)` fixes the count instead.
- **`cpu_profile`**: `CpuProfile::Embedded`, leaving CPU for your application; `Server` gives flush and compaction twice the cores, and a shard thread polls 1 ms for the next commit before it sleeps instead of 200 µs (a lower commit tail for up to about two more cores under a moderate steady load).
- **Compression**, per family: `Family::compression(..)`, LZ4 by default (also `Zstd(level)` or `None`).
- **`io_backend`**: `IoBackend::Auto`, io_uring where the kernel offers it (Linux) and a `pread` pool elsewhere.
- **`direct_io`**: off; worth turning on only for data much larger than RAM with a block cache of at least about 1/64 of it.

Every option, with its default and limits, is in the agent reference's [`Options`](docs/guide/agent-reference.md#options) and [`Family`](docs/guide/agent-reference.md#family) sections; [Getting started](docs/guide/getting-started.md#open-a-database) explains the memory and thread ones.

## Documentation
- **Using Pigeonhole** (people and agents integrating it): [`docs/guide/`](docs/guide/README.md)
- **Design**: [`docs/design/spec.md`](docs/design/spec.md)
- **On-disk format**: [`FORMAT.md`](FORMAT.md)
- **Changelog**: [`CHANGELOG.md`](CHANGELOG.md)
- **Contributing** (people and agents building it): [`CONTRIBUTING.md`](CONTRIBUTING.md), [`AGENTS.md`](AGENTS.md)

## Crates
| Crate | Role | Stability (from 1.0, D282) |
|---|---|---|
| `pigeonhole` | The public API — depend on this one. | semver |
| `pigeonhole-arrow` | Arrow export. | semver |
| `pigeonhole-cli` | The `phdb` tool. | semver for commands, flags, exit statuses and output |
| `pigeonhole-vector` | An IVF vector index, built on the public API (1.1.0, D306). | semver |
| `pigeonhole-text` | Full-text search with BM25 and phrases, built on the public API (1.1.0, D309). | semver |
| `pigeonhole-memory` | Long-term memory for AI agents: dedup, links, hybrid recall within a byte budget, consolidation; built on the public API (1.1.0, D311). | semver |
| `pigeonhole-otel` | The engine's metrics through OpenTelemetry, and a Prometheus collector that `phdb serve` uses (D316, D323). | its own 0.N line, following `opentelemetry`'s minors; metric names stable (D316) |
| `pigeonhole-embed` | Local embeddings for `pigeonhole-memory`: a pinned model run on the CPU in-process (D318). | its own 0.N line (D318) |
| `pigeonhole-engine` | Tablets, shards, MVCC, recovery. | internal: none |
| `pigeonhole-{format,io,pager,wal,memtable,cache,runtime,shm,sst,compaction}` | Engine components. | internal: none |
| `pigeonhole-sim` | Deterministic simulation and reference model. | internal: none |

The internal crates are published only as `pigeonhole`'s dependencies, which pins each exactly; any release may change them incompatibly, and they have a version line of their own (D295: `pigeonhole` 1.x may depend on internal crates at 3.x). [Versions and compatibility](docs/guide/concepts.md#versions-and-compatibility) has the full policy, and [`FORMAT.md` §13](FORMAT.md#13-compatibility-promise) the file format's promise.

## License
MIT. See [LICENSE](LICENSE).

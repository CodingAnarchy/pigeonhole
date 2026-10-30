# Pigeonhole

**An embedded, single-file, wide-column store in Rust** — BigTable's data model with SQLite's deployment model.

[![CI](https://github.com/CodingAnarchy/pigeonhole/actions/workflows/ci.yml/badge.svg)](https://github.com/CodingAnarchy/pigeonhole/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/pigeonhole.svg)](https://crates.io/crates/pigeonhole)
[![docs.rs](https://img.shields.io/docsrs/pigeonhole)](https://docs.rs/pigeonhole)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **Maturity: 1.0.** The public API of `pigeonhole`, `pigeonhole-arrow` and `phdb` follows semver from 1.0.0 ([D282](docs/design/decisions/phase-3.md#d282)), and every 1.x release reads and writes file format 4 ([`FORMAT.md` §13](FORMAT.md#13-compatibility-promise) is the compatibility promise). All four phases are complete: the core engine (Phase 1), the wide-column model (Phase 2), the latency engine (Phase 3) and hardening (Phase 4), each with its performance gate on the reference machine. The 1.0 release gate passed on 2026-10-30 at `main` [`790ae3a0`](https://github.com/CodingAnarchy/pigeonhole/commit/790ae3a0): the Phase 2 floors, and within 1.5× of RocksDB on every workload. 1.0 ships with documented gaps, listed in [`docs/status.md`](docs/status.md).

> **Status: usable and disk-backed.** Memtables flush into the file and compact, so data is bounded by the disk, not memory; commits are crash-safe through the write-ahead log, and a clean close leaves one file. The current release is 1.0.0 (see the [changelog](CHANGELOG.md)): every data operation has a blocking form and an async form, on by default ([async guide](docs/guide/async.md)).

SQLite owns local OLTP and DuckDB owns local OLAP. Pigeonhole targets the missing quadrant: local **sparse, versioned, row-scan-heavy** data — feature stores, time series keyed by entity, crawl and event caches, graph adjacency, per-user state. `cargo add pigeonhole`, open a file, and get rows of arbitrary sparse columns grouped into families, with versions, TTLs, prefix and range scans, and no server.

## Install
```sh
cargo add pigeonhole
```
or `pigeonhole = "1"` in `Cargo.toml`. Requires Rust 1.96 (2024 edition). API docs are on [docs.rs](https://docs.rs/pigeonhole); what changed is in the [changelog](CHANGELOG.md) and the [1.0.0 release](https://github.com/CodingAnarchy/pigeonhole/releases/tag/v1.0.0).

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

## Data model
A database is a sorted, sparse, versioned map: `(table, row, family, qualifier, timestamp) → value`.

| Concept | Meaning |
|---|---|
| **Table** | A namespace with its own families. Many tables share one file. |
| **Row key** | Arbitrary bytes (≤ 64 KiB), sorted lexicographically. Unit of atomicity and locality. |
| **Family** | Declared up front; its own physical LSM tree with its own policy (compression, bloom bits, versions, TTL, blob threshold, cache priority, compaction style). A counter family holds `i64` sums. |
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
| `pigeonhole-engine` | Tablets, shards, MVCC, recovery. | internal: none |
| `pigeonhole-{format,io,pager,wal,memtable,cache,runtime,shm,sst,compaction}` | Engine components. | internal: none |
| `pigeonhole-sim` | Deterministic simulation and reference model. | internal: none |

The internal crates are published only as `pigeonhole`'s dependencies, which pins each exactly; any release may change them incompatibly. [Versions and compatibility](docs/guide/concepts.md#versions-and-compatibility) has the full policy, and [`FORMAT.md` §13](FORMAT.md#13-compatibility-promise) the file format's promise.

## License
MIT. See [LICENSE](LICENSE).

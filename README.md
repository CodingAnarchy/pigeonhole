# Pigeonhole

**An embedded, single-file, wide-column store in Rust** — BigTable's data model with SQLite's deployment model.

[![CI](https://github.com/CodingAnarchy/pigeonhole/actions/workflows/ci.yml/badge.svg)](https://github.com/CodingAnarchy/pigeonhole/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/pigeonhole.svg)](https://crates.io/crates/pigeonhole)
[![docs.rs](https://img.shields.io/docsrs/pigeonhole)](https://docs.rs/pigeonhole)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **Maturity: 1.0.** The public API of `pigeonhole`, `pigeonhole-arrow` and `phdb` follows semver from 1.0.0 ([D282](docs/design/decisions/phase-3.md#d282)), and every 1.x release reads and writes file format 4 ([`FORMAT.md` §13](FORMAT.md#13-compatibility-promise) is the compatibility promise). All four phases are complete: the core engine (Phase 1), the wide-column model (Phase 2), the latency engine (Phase 3) and hardening (Phase 4), each with its performance gate on the reference machine. <!-- GATE 4: the release gate's verdict in one clause (date, main commit). --> 1.0 ships with documented gaps, listed in [`docs/status.md`](docs/status.md).

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
<!-- GATE 4 (release PR): replace every figure in this section from gate 4: the commit and date, both tables, the durable-commit line, and add the Sync rows marked below. -->
Measured on the reference machine (Hetzner AX102: AMD Ryzen 9 7950X3D, 16 cores, NVMe, Linux), in the release gate of 2026-10-29 on `main` [`cba09184`](https://github.com/CodingAnarchy/pigeonhole/commit/cba09184). 1 million rows (about 1 GB, so reads that miss the 256 MiB block cache hit the OS page cache). [Performance compared with SQLite](docs/guide/performance.md) and [`docs/bench.md`](docs/bench.md) have the full tables and method.

**One client** (`Durability::Buffered`):

| Operation | Throughput | p50 | p99 |
|---|--:|--:|--:|
| Row read (YCSB C) | 392K ops/s | 0.90 µs | 11.8 µs |
| Point get, wide rows (sparse-wide) | — | 3.5 µs | 20.1 µs |
| Put (YCSB A) | — | 5.4 µs | 9.1 µs |
| Scan of up to 100 rows (YCSB E) | 20K ops/s | 48 µs | 112 µs |

**16 clients**, against RocksDB and SQLite (EAV) with the same memory. Throughput, then p99 over every operation:

| Workload | Durability | Pigeonhole | RocksDB | SQLite |
|---|---|--:|--:|--:|
| Reads (YCSB C) | Buffered | 2.26M/s, 27 µs | 1.60M/s, 21 µs | 215K/s, 180 µs |
| | GroupSync (default) | 2.37M/s, 27 µs | 1.42M/s, 23 µs | 212K/s, 174 µs |
| 50% updates (YCSB A) | Buffered | 1.46M/s, 39 µs | 699K/s, 68 µs | 85K/s, 1.1 ms |
| | GroupSync (default) | 488K/s, 98 µs | 191K/s, 211 µs | 39K/s, 82 µs |
| Scans (YCSB E) | Buffered | 220K/s, 190 µs | 132K/s, 270 µs | 44K/s, 1.6 ms |
| | GroupSync (default) | 192K/s, 209 µs | 108K/s, 362 µs | 43K/s, 3.4 ms |
| Sparse wide rows | Buffered | 91K/s, 2.4 ms | 72K/s, 3.3 ms | 41K/s, 3.6 ms |
| | GroupSync (default) | 78K/s, 2.7 ms | 59K/s, 3.5 ms | 34K/s, 8.3 ms |

<!-- Release PR: add a `Sync` row under each workload from gate 4's Durability::Sync arm, e.g.
| | Sync | gate 4 | gate 4 | gate 4 |
and update the commit, date and figures above from gate 4. -->
`Durability::Sync` (a dedicated fsync per commit, never shared) is measured in the 1.0 release gate; its rows are added then.

**Durable commits** (`GroupSync`, 16 shards, 16 commits in flight per shard): 11.1M cells/s, commit p50 72 µs, p99 127 µs.

## Configuration
`Options::default()` is a valid configuration. The options most users set, with their defaults:

- **`durability`**: `GroupSync`, a commit returns once it is fsynced, with concurrent commits sharing the fsync. `Buffered` skips the fsync (survives a process crash, not power loss); `Sync` fsyncs each commit alone ([Durability](docs/guide/durability.md)).
- **`block_cache`**: 256 MiB, decoded SST blocks.
- **`row_cache`**: 64 MiB, hot rows of small families (`0` turns it off).
- **`memtable_budget`**: 64 MiB of write buffer per shard.
- **`shards`**: one per physical core.
- **`adaptive_shard_threads`**: on, sizing the shard threads to the load; `shard_threads(k)` fixes the count instead.
- **`cpu_profile`**: `CpuProfile::Embedded`, leaving CPU for your application; `Server` gives flush and compaction twice the cores.
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

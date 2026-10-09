# Pigeonhole

**An embedded, single-file, wide-column store in Rust** — BigTable's data model with SQLite's deployment model.

[![CI](https://github.com/CodingAnarchy/pigeonhole/actions/workflows/ci.yml/badge.svg)](https://github.com/CodingAnarchy/pigeonhole/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/pigeonhole.svg)](https://crates.io/crates/pigeonhole)
[![docs.rs](https://img.shields.io/docsrs/pigeonhole)](https://docs.rs/pigeonhole)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **Maturity: experimental 0.x.** The core engine (Phase 1) is complete and fault-tested in simulation, and the wide-column model (Phase 2: counter families, blob separation, compaction styles, zstd) is complete in 0.2.0: its performance gate is met as amended by [D193](docs/design/decisions/phase-2.md#d193), with one documented gap, the read tail of wide, heavily overwritten rows ([#387](https://github.com/CodingAnarchy/pigeonhole/issues/387)). The on-disk format and the API may change before 1.0 ([`FORMAT.md`](FORMAT.md)). The latency work (Phase 3) is still to come. Pigeonhole is **not recommended for production use yet**. See [`docs/status.md`](docs/status.md) for the roadmap.

> **Status: usable, disk-backed sync API.** Memtables flush into the file and compact, so data is bounded by the disk, not memory; commits are crash-safe through the write-ahead log, and a clean close leaves one file. The current release is 0.2.0 (see the [changelog](CHANGELOG.md)).

SQLite owns local OLTP and DuckDB owns local OLAP. Pigeonhole targets the missing quadrant: local **sparse, versioned, row-scan-heavy** data — feature stores, time series keyed by entity, crawl and event caches, graph adjacency, per-user state. `cargo add pigeonhole`, open a file, and get rows of arbitrary sparse columns grouped into families, with versions, TTLs, prefix and range scans, and no server.

## Install
```sh
cargo add pigeonhole
```
or `pigeonhole = "0.2"` in `Cargo.toml`. Requires Rust 1.96 (2024 edition). API docs are on [docs.rs](https://docs.rs/pigeonhole); what changed is in the [changelog](CHANGELOG.md) and the [0.2.0 release](https://github.com/CodingAnarchy/pigeonhole/releases/tag/v0.2.0).

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

## Targets
| Goal | Target (NVMe, hot cache) |
|---|---|
| Point get | p50 < 2 µs, p99 < 10 µs; one I/O when cold |
| Batched durable writes | > 1M cells/s across cores |
| Ordered single-family scan | > 1 GB/s decoded per core |
| Open to first read | < 5 ms |

## Documentation
- **Using Pigeonhole** (people and agents integrating it): [`docs/guide/`](docs/guide/README.md)
- **Design**: [`docs/design/spec.md`](docs/design/spec.md)
- **On-disk format**: [`FORMAT.md`](FORMAT.md)
- **Changelog**: [`CHANGELOG.md`](CHANGELOG.md)
- **Contributing** (people and agents building it): [`CONTRIBUTING.md`](CONTRIBUTING.md), [`AGENTS.md`](AGENTS.md)

## Crates
| Crate | Role |
|---|---|
| `pigeonhole` | The public API — depend on this one. |
| `pigeonhole-engine` | Tablets, shards, MVCC, recovery. |
| `pigeonhole-{format,io,pager,wal,memtable,cache,runtime,shm,sst,compaction}` | Engine components. |
| `pigeonhole-sim` | Deterministic simulation and reference model. |
| `pigeonhole-cli` | The `phdb` tool (Phase 4). |
| `pigeonhole-arrow` | Arrow export (Phase 4). |

## License
MIT. See [LICENSE](LICENSE).

# Pigeonhole

**An embedded, single-file, wide-column store in Rust** — BigTable's data model with SQLite's deployment model.

[![CI](https://github.com/CodingAnarchy/pigeonhole/actions/workflows/ci.yml/badge.svg)](https://github.com/CodingAnarchy/pigeonhole/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **Status: under construction (Phase 1).** The API below is the target design and is not usable yet. See [`docs/status.md`](docs/status.md) for progress.

SQLite owns local OLTP and DuckDB owns local OLAP. Pigeonhole targets the missing quadrant: local **sparse, versioned, row-scan-heavy** data — feature stores, time series keyed by entity, crawl and event caches, graph adjacency, per-user state. `cargo add pigeonhole`, open a file, and get rows of arbitrary sparse columns grouped into families, with versions, TTLs, prefix and range scans, and no server.

```rust,ignore
use pigeonhole::{Pigeonhole, Options, Family, Durability};

let db = Pigeonhole::open("crawl.phdb", Options::default())?;
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
}
```

## Data model
A database is a sorted, sparse, versioned map: `(table, row, family, qualifier, timestamp) → value`.

| Concept | Meaning |
|---|---|
| **Table** | A namespace with its own families. Many tables share one file. |
| **Row key** | Arbitrary bytes (≤ 64 KiB), sorted lexicographically. Unit of atomicity and locality. |
| **Family** | Declared up front; its own physical LSM tree with its own policy (compression, bloom bits, versions, TTL, blob threshold, cache priority). |
| **Qualifier** | Arbitrary bytes created on write, sorted within the family. Absent cells cost nothing. |
| **Timestamp** | `u64`, newest first. Hybrid logical clock by default; user-supplied for event time. |
| **Value** | Bytes. Phase 1 caps a value at the smaller of 64 MiB and half a shard's memtable arena; Phase 2 blob separation raises the cap to 4 GiB − 1. Optional typed merge operators. |

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

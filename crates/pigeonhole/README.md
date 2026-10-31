# pigeonhole

**An embedded, single-file, wide-column store**: BigTable's data model (tables, rows,
column families, sparse qualifiers, timestamped versions, TTLs, prefix and range scans) with
SQLite's deployment model (one file, a library, no server).

> **Maturity: 1.0.** The public API of `pigeonhole`, `pigeonhole-arrow` and `phdb` follows
> semver from 1.0.0 (D282), and every 1.x release reads and writes file format 4 (the
> [compatibility promise](https://github.com/CodingAnarchy/pigeonhole/blob/main/FORMAT.md#13-compatibility-promise)).
> All four phases are complete, each with its performance gate on the reference machine:
> the core engine, the wide-column model, the latency engine and hardening.
> The 1.0 release gate passed on 2026-10-30 at `main` [`790ae3a0`](https://github.com/CodingAnarchy/pigeonhole/commit/790ae3a0).
> 1.0 ships with documented gaps, listed in the
> [status page](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/status.md).

> **Status.** The blocking API is implemented, disk-backed and crash-safe through the
> write-ahead log: memtables flush into the file and compact, so data is bounded by the disk,
> not memory (`Options::memtable_budget` only sizes the per-shard write buffer). A clean close
> leaves one file. A table's tablets split and move between shards, so one table's writes
> spread over them. The async API (`get_async`,
> `Scan::stream`, `commit_async`) is on by default through the `async` feature.

## Versioning

From 1.0.0 this crate follows semantic versioning; the types that can grow are
`#[non_exhaustive]`, test hooks (the `__unstable-hooks` feature) and `#[doc(hidden)]` items are
outside the promise, and only a minor release
may raise the minimum Rust version (D283). The internal
`pigeonhole-*` crates it depends on make no stability promise of their own and are pinned
exactly. The file format's promise is separate. Details:
[versions and compatibility](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/concepts.md#versions-and-compatibility).

## Install

```sh
cargo add pigeonhole
```

or `pigeonhole = "1"` in `Cargo.toml`. Requires Rust 1.96 (2024 edition). API docs are on
[docs.rs](https://docs.rs/pigeonhole). See the
[changelog](https://github.com/CodingAnarchy/pigeonhole/blob/main/CHANGELOG.md) and the
[1.0.1 release](https://github.com/CodingAnarchy/pigeonhole/releases/tag/v1.0.1).

## Quickstart

```rust
use pigeonhole::{days, Durability, Family, Options, Pigeonhole};

# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
let users = db
    .table("users")?
    .family("profile", Family::default().max_versions(1))
    .family("events", Family::default().ttl(days(30)))
    .family("stats", Family::counter())
    .create_if_missing()?;

// One row, all families, all or nothing; durable when it returns (GroupSync by default).
users
    .mutate(b"user:42")
    .put("profile", b"name", b"Ada")
    .incr("stats", b"logins", 1)
    .put("events", b"2026-10-06T12:00", b"login")
    .commit()?;

// A point read borrows the value without allocating.
let name = users.get(b"user:42", "profile", b"name")?.unwrap();
assert_eq!(name.value(), b"Ada");

// Ordered scans with filters pushed into the read path; a zero-copy cursor or owned rows.
let mut it = users.scan_prefix(b"user:").family("events").iter()?;
while let Some(row) = it.next_ref()? {
    println!("{:?}: {} events", row.key(), row.len());
}

// Atomic multi-row writes with one durability point.
let mut wb = db.write_batch();
wb.put(&users, b"user:7", "profile", b"name", b"Grace")
    .put(&users, b"user:9", "profile", b"name", b"Linus");
wb.commit_with(Durability::Buffered)?;

db.close()?;
# Ok::<(), pigeonhole::Error>(())
```

(Lines starting with `#` are hidden setup in the rendered docs; this sample runs as a
doctest.)

## Documentation

- [User guide](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/README.md):
  concepts, getting started, durability, transactions, scans and filters, data modeling,
  async, reader processes, recovery, errors, the `phdb` CLI, Arrow export and performance.
- [Agent reference](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/agent-reference.md):
  every type and method, limits and error codes on one page.
- [API docs](https://docs.rs/pigeonhole) and the
  [examples](https://github.com/CodingAnarchy/pigeonhole/tree/main/crates/pigeonhole/examples).

Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). MIT licensed.

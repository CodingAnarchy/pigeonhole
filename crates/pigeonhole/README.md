# pigeonhole

**An embedded, single-file, wide-column store**: BigTable's data model (tables, rows,
column families, sparse qualifiers, timestamped versions, TTLs, prefix and range scans) with
SQLite's deployment model (one file, a library, no server).

> **Status: Phase 1 in progress.** The blocking API is implemented, disk-backed and
> crash-safe through the write-ahead log: memtables flush into the file and compact, so data
> is bounded by the disk, not memory (`Options::memtable_budget` only sizes the per-shard
> write buffer). `flush`, `compact` and `backup` work, and a clean close leaves one file.
> One table still lives on one shard until tablet splits land. The async API arrives in
> Phase 3 behind the `async` feature.

## Quickstart

```rust
use pigeonhole::{days, Durability, Family, Options, Pigeonhole};

# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
let users = db
    .table("users")?
    .family("profile", Family::default().max_versions(1))
    .family("events", Family::default().ttl(days(30)))
    .create_if_missing()?;

// One row, all families, all or nothing; durable when it returns (GroupSync by default).
users
    .mutate(b"user:42")
    .put("profile", b"name", b"Ada")
    .incr("profile", b"logins", 1)
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
  concepts, getting started, durability, scans and filters, data modeling, errors.
- [Agent reference](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/agent-reference.md):
  every type and method, limits and error codes on one page.
- [API docs](https://docs.rs/pigeonhole) and the
  [examples](https://github.com/CodingAnarchy/pigeonhole/tree/main/crates/pigeonhole/examples).

Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). MIT licensed.

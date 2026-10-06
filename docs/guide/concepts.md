# Concepts

> **Status: API frozen; implementation in progress (Phase 1).** Features from later phases are labeled.

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
| `blob_threshold(bytes)` | Values larger than this are stored separately and never rewritten by compaction *(Phase 2)*. |
| compression | LZ4 by default; `uncompressed()`; zstd *(Phase 2)*. |
| cache priority | How long the family's blocks stay cached. |

Put data with different access patterns in different families: small hot metadata in one, large or TTL'd payloads in another.

## Qualifiers
Qualifiers are arbitrary bytes, created on write, sorted within a family. They are the "columns", but they are not declared: a row can have zero or ten million of them, and absent ones cost nothing. Use them for sparse attributes, time buckets (`2026-10-05T12`), or adjacency lists (`edge:<dst>`).

## Timestamps and versions
Every cell version carries a `u64` timestamp in **microseconds since the Unix epoch**, sorted newest first. By default Pigeonhole assigns the commit time, and a default timestamp never goes backwards within a tablet, even across a clock step or a restart. Supply your own with `put_at` for event time; TTL treats user timestamps as microseconds. Reads return the newest version unless asked for more (`versions(n)`, `time_range`).

## Values
Values are bytes, with typed forms for `i64` and `f64`. In Phase 1 a value is limited to the smaller of the WAL segment payload, 64 MiB and half a shard's memtable arena (`ValueTooLarge` beyond that); blob separation (Phase 2) lifts the limit toward 4 GiB. `incr` adds to an `i64` counter without reading it first, using the built-in `pigeonhole.i64_add` merge operator. Custom merge operators (append and so on) are Phase 2.

## Deletes
Deletes write markers: one cell version (`delete_cell`), a whole column (`delete_column`), a family within a row (`delete_family`), or a whole row (`delete_row`, which writes one family marker per family in the same atomic commit). A column or family delete with timestamp *T* hides every version in its scope with timestamp ≤ *T*, regardless of when it was written, so a later `put` with an older timestamp stays hidden; a `put` with a newer timestamp is visible again. A cell delete hides the version at exactly its timestamp, again regardless of when it was written: a later `put` at that same timestamp stays hidden, so write the replacement at another timestamp.

## Multi-process readers (Phase 4)
One writer process and any number of reader processes on the same host can open the same `.phdb` file; readers see each commit as soon as the writer publishes it. A reader's handle (`Pigeonhole::open_reader`) has no write methods, but the reader process still opens the file **read-write** and writes nothing to it: the processes coordinate with byte-range locks on the file, and some of those locks are exclusive, which POSIX grants only on a writable file descriptor. This is the same requirement SQLite has in WAL mode. So every reader process needs write permission on the database file, and a database on read-only media cannot be opened by readers.

## Where next
[Getting started](getting-started.md) · [Data modeling](data-modeling.md) · [Agent reference](agent-reference.md)

# Concepts

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
| `max_versions(n)` | Keep at most *n* versions per cell. |
| `ttl(d)` | Cells older than *d* expire. |
| `bloom_bits(b)` | Bloom/ribbon filter bits per key, to skip files on misses. |
| `blob_threshold(bytes)` | Values larger than this are stored separately and never rewritten by compaction. |
| compression | LZ4 by default, zstd optional. |
| cache priority | How long the family's blocks stay cached. |

Put data with different access patterns in different families: small hot metadata in one, large or TTL'd payloads in another.

## Qualifiers
Qualifiers are arbitrary bytes, created on write, sorted within a family. They are the "columns", but they are not declared: a row can have zero or ten million of them, and absent ones cost nothing. Use them for sparse attributes, time buckets (`2026-10-05T12`), or adjacency lists (`edge:<dst>`).

## Timestamps and versions
Every cell version carries a `u64` timestamp, sorted newest first. By default Pigeonhole assigns a hybrid logical clock; you can supply your own for event time. Reads return the newest version unless asked for more.

## Values
Values are bytes, up to 4 GiB. Typed merge operators (counters, append) let you update a cell without reading it first *(planned, Phase 2)*.

## Deletes
Deletes write a single marker: one cell version, a whole column, a family within a row, or the whole row.

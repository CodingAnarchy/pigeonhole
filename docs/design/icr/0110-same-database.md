# 0110: `Pigeonhole::same_database`

**Status:** Approved (number from the coordinator, 2026-11-03: option (a) for #1268 item 8, keyed on `same_database`, not `db_id`).

## The change
- **pigeonhole** (semver, additive): `Pigeonhole::same_database(&self, other: &Pigeonhole) -> bool`. It returns true when both handles come from the same open: clones of one handle, or of handles from the same `open` or `open_application_owned`. It's an `Arc::ptr_eq` on the handle's inner database.
  - **Two opens of one file are different databases,** even when the first was closed before the second opened. So are two copies of one file, which share `DatabaseInfo::db_id` when the file was copied while closed.
  - **An automatic recovery (`Options::auto_recover`) keeps the open,** so it keeps the answer.
  - **It's cheap,** a pointer comparison, and never fails, even on a closed handle.

## Why
#1268 item 8: `pigeonhole-vector` keeps a per-process registry so that a second `Index::open` of an index shares the first's centroid cache and rebalancer. That crate builds on the public API only, and nothing public identified an open database. `db_id` names a file, not an open: keying on it would hand one database's index to a copy of it open in the same process. Any library that keeps per-database state has the same need.

## Callers changed
None: additive. `pigeonhole-vector`'s registry (`registry.rs`) uses it.

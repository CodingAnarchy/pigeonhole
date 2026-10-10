# 0020: point gets skip the marker seek on memtables without delete markers

**Status:** Approved (coordinator, 2026-10-10; point-get constant factors, plan item 1).

## Change

`pigeonhole-memtable`, additive:

```rust
impl Memtable {
    /// A delete marker is about to be inserted: call before its `insert` (Release).
    pub fn note_marker(&self);
}

impl MemtableReader {
    /// Whether the memtable may hold a delete marker: `false` only in the writer process,
    /// for a memtable whose writer never noted one (Acquire).
    pub fn may_have_markers(&self) -> bool;
}
```

`pigeonhole-compaction`, additive:

```rust
impl<C: Cursor> CellResolver<C> {
    /// `seek_column_encoded` for a cursor none of whose sources holds a delete marker:
    /// one seek straight to the column.
    pub fn seek_column_encoded_unmarked(&mut self, column_prefix: &[u8], row_len: usize)
        -> Result<(), C::Error>;
}
```

`pigeonhole-engine`: internal only. `View::point_sources` returns whether a source may hold a marker; `get_with` picks the seek.

## Why

A point get positions the resolver in two steps:
1. a seek to the row's marker prefix, to record family and row delete markers visible at the snapshot;
2. a forward seek to the column.

On a memtable-resident get that's two skiplist searches, about 2,140 and 1,040 instructions, out of about 7,250 per get (get-mem, Linux callgrind). Most memtables never hold a marker: they're written only by row and family deletes. When none of a get's sources can hold one, the first search finds nothing, so it can go. Measured: get-mem −16.4%.

## Semantics

- **Writer:** the shard calls `note_marker` before inserting any `Kind::FamilyDelete` entry (row deletes are expanded to family deletes). The flag lives in the memtable's process-local `Pin`, shared by the writer and every in-process reader, and is never cleared. A memtable is new after each freeze, so it starts clear.
- **Reader:** a get loads the flag (Acquire) after taking its read point, while building its sources. A marker visible at the read point was linked after its note, and linking happens before the commit becomes visible, so the flag is seen set. A loom model checks this (`loom_a_visible_marker_is_never_missed`), and it fails if the note is moved after the insert.
- **Conservative cases:** any SST source counts as "may have markers", as does a memtable opened without its writer's pin (reader processes, `Pin::foreign`). Those gets keep the two-step seek.
- **Scans, row reads, compaction and flush** are unchanged: only point gets use it.

## Callers

- `pigeonhole-engine`:
  - `shard.rs` (`apply` notes before inserting a marker);
  - `source.rs` (`point_sources` returns whether a source may hold a marker);
  - `read.rs` (`get_with`, `resolve_point`).
- Tests:
  - `pigeonhole-memtable` (unit and loom);
  - `pigeonhole` `tests/delete_markers.rs`: gets race row and family deletes across many fresh memtables, and markers in SSTs still hide cells. Breaking the flag fails both this test and the model check.

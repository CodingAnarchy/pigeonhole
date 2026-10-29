# 0092: `Engine::table_by_id`, a table's current entry by id

**Status:** Approved (coordinator, 2026-10-29; write-path perf-nit audit, hand-off from cli692's read-side audit). Additive: a new public method on `pigeonhole_engine::Engine`. Nothing existing changes signature.

## Change

**`pigeonhole-engine`:** a new method on `Engine`:

```rust
pub fn table_by_id(&self, id: TableId) -> Option<Arc<TableInfo>>
```

It is `Engine::table(name)` keyed by id:
- in a writer process, it looks the id up in the current view's catalog under the view's `arc-swap` guard, as `table` does (#709);
- in a reader process, it uses the reader's catalog;
- in a forked process, it returns `None`, as `table` does.

It returns `None` once the table is dropped. A table created again under the same name has a new id.

## Why

`pigeonhole`'s `TableCore::current_info` runs on every `RowRead::read`, to name the families a latest read returns. It called `engine.table(&name)`, which hashed the name with SipHash (the catalog's `by_name` is a std `HashMap<String, TableId>`), then compared the entry's id with the handle's and fell back to the handle's own entry on a mismatch. The handle already knows its id, so a lookup by id saves the name hash and the `by_name` probe, roughly 50–100 instructions per read on a short name.

The `Arc<TableInfo>` clone stays: the row a read returns keeps the entry to name its families.

## Behaviour

It is identical for every case `current_info` covers:
- a family added through another handle appears, since this is the current catalog's entry;
- a dropped table falls back to the handle's own entry;
- a table dropped and recreated under its name has a new id, so it is not found, and the handle's own entry is used. Before, the name found the new id, and the id check fell back the same way.

## Callers

- `pigeonhole`'s `TableCore::current_info` (crate-private), the only caller in the workspace.
- `cargo semver-checks` sees an added method (minor). The public `pigeonhole` API is unchanged.

# 0068: `Snapshot::table`, the table as a snapshot's catalog knew it

**Status:** Approved (coordinator, 2026-10-23; perf287's review of #927; #892). Additive.

## Change

**`pigeonhole-engine`:** `Snapshot::table(&self, id: TableId) -> Option<&Arc<TableInfo>>`, the table entry in the snapshot's view's catalog (its families as of the snapshot), or `None` if the table did not exist then.

**It borrows:** a lookup takes no reference on the shared `TableInfo`. A transactional `get` resolves its family through it on every call, and an `Arc` clone there would be an atomic increment on one refcount every reading thread shares (perf287's review). A caller that needs to keep the entry clones it.

## Why

#892: reads at a snapshot resolved family names, and named the families they returned, from the catalog as of now, while the engine reads the families as of the snapshot. Families are selected by name, so a family dropped and created again under the same name would resolve to an id the snapshot does not have, and its data at the snapshot would read as empty. Nothing can drop or rename a family today, so the change is defensive: such a feature inherits reads that are right at the snapshot.

## Callers

- **`pigeonhole`, `table.rs`:**
  - `TableCore::info_at` names a snapshot read's families (one clone);
  - `TableCore::family_id_at` resolves a family name at a snapshot (borrowed), falling back to the current catalog for a name only it knows.
- **`pigeonhole`:**
  - `read.rs`: `Selection::start`, behind row reads and scans;
  - `table.rs`: `TableCore::get_at` and `get_async`;
  - `write.rs`: `Transaction::{get, get_async, get_row}`.
- **Tests:**
  - `pigeonhole-engine` `tests/api.rs`: `a_snapshot_names_the_families_of_its_catalog`;
  - `pigeonhole` `tests/api.rs`: `reads_at_a_snapshot_name_the_families_the_snapshot_saw`.

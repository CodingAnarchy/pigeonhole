# 0062: `Transaction::get_row` and the transaction write ops

**Status:** Number assigned by the coordinator (2026-10-21; #694 PR 4). Additive. Full parity with `RowMutation`'s write ops: the plan listed eight, and the coordinator confirmed the `_at` variants (`put_i64_at`, `put_f64_at`, `incr_at`) belong too, making eleven.

## Change

**`pigeonhole::Transaction`:**
- **`get_row(&mut self, table: &Table, row: &[u8], families: &[&str]) -> Result<Option<RowRef<'_>>>`:** reads the row at the transaction's snapshot, the newest version of each column, and records a read of every family it covers: `families`, or every family of the table **at the snapshot** when empty.
  - A family created after the snapshot is neither returned nor recorded. That's consistent: the transaction serializes before that family's creation and any write into it, since it read nothing they wrote.
  - A family the row has no cell in still counts as read, so a commit that adds one (or creates the row) conflicts.
  - A failed read is recorded like `get`'s for `Pigeonhole::transact`'s retry decision (ICR 0060).
  - An unknown family is `FamilyNotFound`.
- **The write ops `RowMutation` has and `Transaction` lacked**, with the same meaning and size checks:
  - `put_at`, `put_i64`, `put_i64_at`, `put_f64`, `put_f64_at`;
  - `incr`, `incr_at` (counter families; a blind write that conflicts with nothing unless the transaction read the cell);
  - `merge`;
  - `delete_cell`, `delete_family`, `delete_row`.

**`pigeonhole_engine::Txn::read_row_into(&mut self, table, row, families: &[FamilyId], spec: &ReadSpec, sink: &mut impl RowSink) -> Result<bool>`:** the engine side. It records a `(row, family)` read for each family (all families at the snapshot when empty), then reads the row at the snapshot.

## Why

#694's plan, PR 4: a transaction could read only single cells and write only `put` and `delete_column`. Reading a whole row, or using typed values and counters, needed workarounds.

Reads are still at (row, family) granularity (D272's C2), so `get_row` costs one read-set entry per family, not per cell.

Scans in a transaction aren't included. They need range read-sets and phantom checks, and are post-1.0 (the guide says so).

## Callers

- `pigeonhole-engine`: `write.rs` (`Txn::read_row_into`).
- `pigeonhole`: `write.rs` (`Transaction::get_row` and the write ops); `tests/transact.rs`.
- Docs: the agent reference, and the changelog.

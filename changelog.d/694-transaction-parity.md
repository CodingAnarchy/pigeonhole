### Added
- `Transaction::get_row` (#694, ICR 0062): read a row at the transaction's snapshot. Each family it covers counts as read, so a commit touching any column of those families, or creating the row, makes the transaction conflict.
- The `RowMutation` write ops that `Transaction` lacked (#694, ICR 0062): `put_at`, `put_i64`, `put_i64_at`, `put_f64`, `put_f64_at`, `incr`, `incr_at`, `merge`, `delete_cell`, `delete_family`, `delete_row`.

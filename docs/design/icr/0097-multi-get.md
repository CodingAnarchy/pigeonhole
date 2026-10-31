# 0097: frontier multi-get, `Table::keys(&KeySet)`

**Status:** Approved (number from the coordinator, 2026-10-31; owner accepted D299 at revision 1 on #489). PR 1 adds the sync read; PR 2 the async one.

## PR 1: the sync multi-get
- **pigeonhole:**
  - `KeySet` (re-exported from the engine): `from_unsorted(impl IntoIterator<Item = impl AsRef<[u8]>>) -> KeySet` (sorted, deduplicated), `from_sorted(..) -> Result<KeySet>` (`InvalidArgument` unless strictly increasing), `len`, `is_empty`, `get(i)`, `iter`.
  - `Table::keys(&self, &KeySet) -> KeysRead<'_>` and `ReadTable::keys`.
  - `KeysRead`: the row-read selection (`family`, `families`, qualifier prefix, range and bounds, `latest`, `time_range`, `value_filter`, `snapshot`), `versions(n)`, `column_limit(n)` (per row and family), and `read(self) -> Result<KeyRows<'t>>`.
  - `KeyRows::next(&mut self) -> Result<Option<(usize, RowRef<'_>)>>`: the next row with a cell the selection keeps and its key's index, lent until the next call. Rows come in key order, under one snapshot; the row cache is not consulted.
- **Internal crates** (no promise):
  - **pigeonhole-engine:** `KeySet`, `KeyCursor` (`next_key`, `row`, `next_cell`, `next_cell_into`, `push_next_cell`, `current_data`, `skip_rest_of_key`, `keys`), `Engine::multi_get`.
  - **pigeonhole-compaction:** `MergingCursor::rebuild_positioned`, `CellResolver::seek_with`.

## PR 2: async
- **pigeonhole** (behind the `async` feature):
  - `KeyRows::poll_next(&mut self, &mut Context) -> Poll<Result<Option<(usize, RowRef<'_>)>>>` and `KeyRows::next_async(&mut self) -> Result<Option<(usize, RowRef<'_>)>>` (an `async fn`): `next`'s rows, lent the same way, never blocking on the device.
  - `KeyRows::into_owned_stream(self) -> nonblocking::KeyStream`: a `Stream<Item = Result<(usize, Row)>>` that holds its table.
- **Internal crates** (no promise):
  - **pigeonhole-engine:** `KeyCursor::poll_next_key` and `KeyCursor::counted`.
  - **pigeonhole-sst:** `SstReader::fetch_for(&self, target, priority) -> Option<Fetch>` and `Fetch::offset`.

## Why
D299 (#489 item 2): a frontier read row by row sets up every read from scratch; a multi-get sets each tablet's read up once and moves its sources forward from key to key.

## Callers changed
None: additive. The pigeonhole model target multi-gets each row read with the rows read last.

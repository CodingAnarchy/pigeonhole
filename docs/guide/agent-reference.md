# Agent reference

> **Status: API frozen; implementation in progress (Phase 1).** Signatures are authoritative (from `crates/pigeonhole/src`); bodies are `todo!()` until Phase 1 lands. Samples are `rust,ignore`. **P2/P3/P4** mark the phase a feature ships in. If this page and the rustdoc disagree, the rustdoc wins.

Import: `use pigeonhole::{...}`. Everything is re-exported at the crate root. Errors: [`errors.md`](errors.md).

## Limits and invariants
| Item | Rule |
|---|---|
| Row key, qualifier | Arbitrary bytes, each ≤ 64 KiB, else `KeyTooLarge`. Sorted byte-wise. |
| Value | P1: ≤ `min(WAL segment payload, 64 MiB, ½ memtable arena)`, else `ValueTooLarge` (D16). P2 blobs lift it; ceiling 2³²−1 bytes. |
| Timestamp | `u64` **microseconds** since the Unix epoch (D11). Default = `max(now, tablet floor + 1)`, never goes backwards. User timestamps are microseconds for TTL. |
| Version order | Newest timestamp first; the same timestamp is ordered by inverted seqno (later commit first). Multiple mutations to the same (row, family, qualifier, timestamp) **within one commit** collapse to the last one written (D34, pending). |
| Atomicity | One `RowMutation` = one row, all families, all-or-nothing. `WriteBatch` = any rows/tables, atomic, one durability point. |
| Builder errors | Surface at `commit`/`read`/`iter`, not at the builder call. |
| Delete rule (D9) | `delete_column`/`delete_family` at ts `T` hides every version in scope with ts ≤ `T`, regardless of commit order. `delete_cell(ts)` hides exactly that version. |
| `delete_row` (D10) | One family marker per family, same commit. |
| Read-your-writes (D19) | `commit` returns after durable at level **and** visible. |
| Durability resolution | per-call → writer default → `GroupSync`. |
| Writer | One writer per file; second open → `WriterLocked`. |
| Handles | `Pigeonhole`, `Table`, `Snapshot`, `Cell`, `Row` are cheap `Clone`. `Table`: `Send + Sync`. |
| Snapshots | Pin data. Drop promptly. |
| Filesystem | Local only (`NetworkFilesystem`). |
| Typed values | `incr` columns are `i64`. Write counters only with `incr` / `put_i64`. |

## Types
| Type | Role |
|---|---|
| `Pigeonhole` | Writer handle. |
| `PigeonholeReader` | Read-only handle in another process (P4). |
| `Shard` | One shard in application-owned mode. |
| `Snapshot` | Point-in-time view. |
| `Options`, `ReaderOptions`, `Family` | Config builders (consume and return `Self`). |
| `Priority` | `Low`, `Normal` (default), `High`. |
| `Compaction` | `Leveled` (default), `Tiered` (P2), `FifoByTime` (P2). |
| `Durability` | `None`, `Buffered`, `GroupSync` (default), `Sync`. |
| `TableBuilder`, `Table`, `ReadTable` | Define/open a table; read-write handle; read-only handle (P4). |
| `RowMutation`, `WriteBatch`, `Transaction` | Writes; `Transaction` is P4. |
| `CommitInfo { seqno: u64, durability: Durability }` | Commit result. |
| `RowRead`, `Scan`, `RowIter` | Read builders; scan iterator. |
| `ValueFilter`, `Condition` | Value predicate; `commit_if` condition (P2). |
| `CellRef<'a>`, `Cell`, `Row`, `RowRef<'a>`, `CellEntry<'a>`, `Value<'a>` | Borrowed and owned results. |
| `Error`, `ErrorCode`, `Result<T>` | Errors. |
| `MergeOperator`, `MergeError` | Custom merge (P2). |
| `days(n: u64) -> Duration` | TTL helper. |

## `Pigeonhole`
| Signature | Semantics |
|---|---|
| `open(path: impl AsRef<Path>, Options) -> Result<Pigeonhole>` | Open or create as writer; replays WAL. |
| `open_reader(path, ReaderOptions) -> Result<PigeonholeReader>` | P4. Read-only, any number of processes. |
| `open_application_owned(path, Options) -> Result<(Pigeonhole, Vec<Shard>)>` | Writer with no threads; you drive each `Shard`. |
| `table(&self, name: &str) -> Result<TableBuilder<'_>>` | Start define/open. |
| `tables(&self) -> Vec<String>` | Table names. |
| `drop_table(&self, name: &str) -> Result<()>` | Drop table and data. |
| `write_batch(&self) -> WriteBatch` | New multi-row batch. |
| `transaction(&self) -> Result<Transaction>` | P4. Optimistic transaction. |
| `snapshot(&self) -> Result<Snapshot>` | Consistent view of everything committed. |
| `default_durability(&self) -> Durability` | Writer default. |
| `set_default_durability(&self, Durability)` | Applies to later commits. |
| `flush(&self) -> Result<()>` | Flush all memtables. |
| `compact(&self) -> Result<()>` | Compact all tables. |
| `backup(&self, dest: impl AsRef<Path>) -> Result<()>` | Consistent copy while writing. |
| `close(self) -> Result<()>` | Last handle out removes sidecars. |

`PigeonholeReader` (P4): `table(&self, &str) -> Result<ReadTable>`, `tables() -> Vec<String>`, `snapshot() -> Result<Snapshot>`. No write methods.
`Snapshot`: `seqno(&self) -> u64`.
`Shard`: `index() -> usize`, `run_once(&mut self, budget: Duration) -> bool` (true if work remains), `set_wakeup(&mut self, Box<dyn Fn() + Send + Sync>)`.

## `Options` (all `self -> Self`; process-local, not stored in file)
| Method | Meaning |
|---|---|
| `durability(Durability)` | Writer default (default `GroupSync`). |
| `shards(usize)` | Shard threads (default CPUs available). `1` is valid. |
| `compaction_cores(usize)` | Extra pinned threads for flush/compaction. |
| `memtable_budget(u64)` | Arena bytes per shard (default 64 MiB). |
| `block_cache(usize)` | Block cache bytes. |
| `row_cache(usize)` | Row cache bytes (default 0 = off). |
| `shm_dir(impl Into<PathBuf>)` | Shared-memory file directory (e.g. tmpfs). |
| `create_if_missing(bool)` | Default true. |
| `merge_operator(Arc<dyn MergeOperator>)` | P2. Register custom operator. |
| `allow_unregistered_merge_operators(bool)` | Open read-only with compaction off if a family names an unregistered operator. |

`ReaderOptions` (P4): `block_cache(usize)`, `shm_dir(..)`, `merge_operator(..)`.

## `Family` (all `self -> Self`; stored in file)
| Method | Meaning |
|---|---|
| `max_versions(u32)` | Keep ≤ n versions per column (0 = all). |
| `ttl(Duration)` | Expire cells older than this by timestamp. |
| `bloom_bits(u8)` | Filter bits per key (0 off; default 10). |
| `blob_threshold(u32)` | P2. Values above go to blobs (default 4096). |
| `lz4()` | Default compression. |
| `zstd(i8)` | P2. zstd at level. |
| `uncompressed()` | No compression. |
| `block_size(u32)` | Data block bytes (default 16 KiB). |
| `merge_operator(&str)` | P2 for custom. Name of registered operator. `incr` needs none (`pigeonhole.i64_add` default). |
| `cache_priority(Priority)` | Block cache priority. |
| `compaction(Compaction)` | Strategy (`Tiered`, `FifoByTime` are P2; the latter needs a TTL). |

## `TableBuilder` / `Table` / `ReadTable`
| Signature | Semantics |
|---|---|
| `TableBuilder::family(self, &str, Family) -> Self` | Declare family; adds if missing, existing keeps stored options. |
| `TableBuilder::create_if_missing(self) -> Result<Table>` | Open or create. |
| `TableBuilder::create(self) -> Result<Table>` | `TableExists` if present. |
| `TableBuilder::open(self) -> Result<Table>` | `TableNotFound` if absent. |
| `Table::name(&self) -> &str` | |
| `Table::families(&self) -> Vec<&str>` | |
| `Table::mutate(&self, row: &[u8]) -> RowMutation<'_>` | Start single-row mutation. |
| `Table::get(&self, row: &[u8], family: &str, qualifier: &[u8]) -> Result<Option<CellRef<'_>>>` | Newest version; zero-copy. |
| `Table::get_at(&self, &Snapshot, row, family, qualifier) -> Result<Option<CellRef<'_>>>` | As of snapshot. |
| `Table::row(&self, row: &[u8]) -> RowRead<'_>` | Start row read. |
| `Table::scan<K: AsRef<[u8]> + ?Sized>(&self, range: impl RangeBounds<&K>) -> Scan<'_>` | Row range. Both ends same type (use slices). |
| `Table::scan_prefix(&self, prefix: &[u8]) -> Scan<'_>` | Rows starting with prefix. |
| `Table::scan_bounds(&self, Bound<&[u8]>, Bound<&[u8]>) -> Scan<'_>` | Explicit bounds. |

`ReadTable` (P4) has `name`, `get`, `get_at`, `row`, `scan`, `scan_prefix`, `scan_bounds` with the same signatures.

## `RowMutation` (builder; each `self -> Self`)
| Method | Semantics |
|---|---|
| `put(family, qualifier: &[u8], value: &[u8])` | Bytes at commit timestamp. |
| `put_at(family, qualifier, ts: u64, value)` | Bytes at explicit timestamp (µs). |
| `put_i64(family, qualifier, i64)` / `put_f64(.., f64)` | Typed values. |
| `incr(family, qualifier, delta: i64)` | Blind atomic `i64` add (merge operand). |
| `merge(family, qualifier, operand: &[u8])` | Operand for the family's operator (P2 for custom). |
| `delete_cell(family, qualifier, ts: u64)` | Delete exactly one version. |
| `delete_column(family, qualifier)` | Delete all versions. |
| `delete_family(family)` | Delete all columns of a family in this row. |
| `delete_row()` | Delete whole row. |
| `durability(Durability)` | Override for this commit. |
| `commit(self) -> Result<CommitInfo>` | Commit. |
| `commit_if(self, &Condition) -> Result<Option<CommitInfo>>` | P2. Compare-and-set on this row; `None` if condition failed. |

`Condition` variants: `Exists { family: String, qualifier: Vec<u8> }`, `Absent { .. }`, `Value { family, qualifier, filter: ValueFilter }`.

## `WriteBatch` (methods take `&mut self -> &mut Self`)
| Method | Semantics |
|---|---|
| `put(&Table, row, family, qualifier, value)` | |
| `put_at(&Table, row, family, qualifier, ts, value)` | |
| `incr(&Table, row, family, qualifier, delta)` | |
| `delete_column(&Table, row, family, qualifier)` | |
| `delete_row(&Table, row)` | |
| `len(&self) -> usize`, `is_empty(&self) -> bool` | |
| `commit(self) -> Result<CommitInfo>` | Writer default durability. |
| `commit_with(self, Durability) -> Result<CommitInfo>` | Override. |

`Transaction` (P4): `get(&mut self, &Table, row, family, qualifier) -> Result<Option<CellRef<'_>>>`, `put(..)`, `delete_column(..)` (as `WriteBatch`, `&mut Self`), `commit(self)`, `commit_with(self, Durability)`; `Conflict` on a conflicting commit.

## `RowRead` / `Scan` (builders, `self -> Self`)
| Method | `RowRead` | `Scan` |
|---|---|---|
| `families(impl IntoIterator<Item = &str>)`, `family(&str)` | ✓ | ✓ |
| `qualifier_prefix(&[u8])` | ✓ | ✓ |
| `qualifier_range(impl RangeBounds<&K>)`, `qualifier_bounds(Bound<&[u8]>, Bound<&[u8]>)` | ✓ | ✓ |
| `latest()` (default), `versions(u32)` (0 = all retained) | ✓ | ✓ |
| `time_range(Range<u64>)` | ✓ | ✓ |
| `column_limit(u32)` | ✓ | |
| `columns_per_row(u32)` | | ✓ |
| `value_filter(ValueFilter)` | ✓ | ✓ |
| `limit(u64)` rows | | ✓ |
| `snapshot(&Snapshot)` | ✓ | ✓ |
| terminal | `read(self) -> Result<Option<RowRef<'t>>>` | `iter(self) -> Result<RowIter<'t>>` |

`ValueFilter`: `Equals(Vec<u8>)`, `Prefix(Vec<u8>)`, `I64(std::cmp::Ordering, i64)`.
`RowIter`: `Iterator<Item = Result<Row>>`; `next_ref(&mut self) -> Result<Option<RowRef<'_>>>` (zero-copy, valid until next call).
Pushdown (D22): qualifier and time filters in the block decoder; versions, column limits, value filters in resolution on snapshot-visible data. Deletes and merge operands always pass the decoder filters. Details: [`scans-and-filters.md`](scans-and-filters.md).

## Results
| Type | Methods |
|---|---|
| `CellRef<'a>` | `value() -> &[u8]`, `typed() -> Value<'_>`, `as_i64() -> Option<i64>`, `timestamp() -> u64`, `to_owned() -> Cell` |
| `Cell` (owned, `'static`, `Send + Sync`) | `value`, `typed`, `as_i64`, `timestamp` |
| `RowRef<'a>` | `key() -> &[u8]`, `len()`, `is_empty()`, `entry(i) -> Option<CellEntry<'_>>`, `iter()`, `get(family, qualifier) -> Option<CellRef<'_>>`, `to_owned() -> Row` |
| `Row` (owned) | `key()`, `len()`, `is_empty()`, `entry(i) -> Option<(&str, &[u8], &Cell)>`, `get(family, qualifier) -> Option<&Cell>`, `view() -> RowRef<'_>` |
| `CellEntry<'a>` | public fields `family: &str`, `qualifier: &[u8]`, `cell: CellRef<'a>` |
| `Value<'a>` | `Bytes(&[u8])`, `I64(i64)`, `F64(f64)`, `Varint(i64)` |
| `CommitInfo` | `seqno`, `durability` |

Cells within a row: ordered by family, qualifier, newest version first.

## Error codes (`ErrorCode`, `#[non_exhaustive]`, `repr(u32)`)
`Io`=1 `Corruption`=2 `WriterLocked`=3 `ShmVersionMismatch`=4 `ShmUnavailable`=5 `UnsupportedFormat`=6 `NetworkFilesystem`=7 `TableNotFound`=8 `TableExists`=9 `FamilyNotFound`=10 `FamilyExists`=11 `UnknownMergeOperator`=12 `MergeFailed`=13 `Conflict`=14 `ReadOnly`=15 `KeyTooLarge`=16 `ValueTooLarge`=17 `NoSpace`=18 `InvalidArgument`=19 `Unsupported`=20 `Closed`=21 `NoReaderSlot`=22 `RecordTooLarge`=23 `Busy`=24. Causes and fixes: [`errors.md`](errors.md). `Error::code()`, `Error::message()`.

## Not yet available
| Feature | Phase |
|---|---|
| zstd, blob separation, `Tiered`/`FifoByTime`, custom merge operators, `commit_if` | P2 |
| `get_async`, `Scan::stream`, `commit_async`, `commit_with_ticket` (module `nonblocking`, feature `async`) | P3 |
| `open_reader`, `PigeonholeReader`, `ReadTable`, `Transaction` | P4 |

## Recipes
### 1. Open, create table, write, read
```rust,ignore
use pigeonhole::{Family, Options, Pigeonhole};

let db = Pigeonhole::open("app.phdb", Options::default())?;
let users = db.table("users")?
    .family("profile", Family::default().max_versions(1))
    .create_if_missing()?;

users.mutate(b"user:42").put("profile", b"name", b"Ada").commit()?;
let name = users.get(b"user:42", "profile", b"name")?.map(|c| c.value().to_vec());
db.close()?;
```

### 2. Counter
```rust,ignore
users.mutate(b"user:42").incr("profile", b"logins", 1).commit()?;
let n: i64 = users.get(b"user:42", "profile", b"logins")?
    .and_then(|c| c.as_i64()).unwrap_or(0);
```

### 3. Prefix scan with pagination
```rust,ignore
use std::ops::Bound;

let mut after: Option<Vec<u8>> = None;
loop {
    let start = match &after { Some(k) => Bound::Excluded(k.as_slice()), None => Bound::Included(&b"user:"[..]) };
    // Upper bound of prefix "user:" is "user;" (':' + 1).
    let mut it = users
        .scan_bounds(start, Bound::Excluded(&b"user;"[..]))
        .family("profile")
        .limit(100)
        .iter()?;
    let mut n = 0;
    while let Some(row) = it.next_ref()? {
        after = Some(row.key().to_vec());
        n += 1;
        // use row
    }
    if n < 100 { break; }
}
```

### 4. Atomic multi-row write with chosen durability
```rust,ignore
use pigeonhole::Durability;

let mut wb = db.write_batch();
wb.put(&users, b"user:1", "profile", b"name", b"A")
  .put(&users, b"user:2", "profile", b"name", b"B")
  .delete_row(&users, b"user:0");
let info = wb.commit_with(Durability::GroupSync)?;
assert_eq!(info.durability, Durability::GroupSync);
```

### 5. Consistent multi-read, versions, event time
```rust,ignore
let snap = db.snapshot()?;
let a = users.get_at(&snap, b"user:1", "profile", b"name")?.map(|c| c.to_owned());
let history = users
    .row(b"user:1").family("profile").qualifier_prefix(b"name")
    .versions(5).time_range(t0_us..t1_us).snapshot(&snap).read()?;
drop(snap); // release promptly

users.mutate(b"user:1").put_at("profile", b"name", event_ts_us, b"Ada").commit()?;
```

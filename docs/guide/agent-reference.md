# Agent reference

> **Status: Phase 1 sync API implemented.** Signatures are authoritative (from `crates/pigeonhole/src`). Samples run as doctests (`#` lines are hidden setup). **P2/P3/P4** mark the phase a feature ships in; "early" marks one that already works. If this page and the rustdoc disagree, the rustdoc wins.

Import: `use pigeonhole::{...}`. Everything is re-exported at the crate root. Errors: [`errors.md`](errors.md).

## Limits and invariants
| Item | Rule |
|---|---|
| Row key, qualifier | Arbitrary bytes, each ≤ 64 KiB, else `KeyTooLarge`. Sorted byte-wise. |
| Value | P1: ≤ `min(WAL segment payload, 64 MiB, ½ memtable arena)`, else `ValueTooLarge` (D16). P2 blobs lift it; ceiling 2³²−1 bytes. |
| Timestamp | `u64` **microseconds** since the Unix epoch (D11). Default = `max(now, tablet floor + 1)`, never goes backwards. User timestamps are microseconds for TTL. |
| Version order | Newest timestamp first; the same timestamp is ordered by inverted seqno (later commit first). Multiple mutations to the same (row, family, qualifier, timestamp) **within one commit** collapse to the last one written (D34). |
| Atomicity | One `RowMutation` = one row, all families, all-or-nothing. `WriteBatch` = any rows/tables, atomic, one durability point. |
| Builder errors | Surface at `commit`/`read`/`iter`, not at the builder call. |
| Purges (D74) | Delete markers and versions beyond `max_versions` are purged by a bottommost compaction with no snapshot that needs them. After that, a write with an **older explicit timestamp** behaves as if they never existed: a `put_at` below a purged delete becomes visible. Default timestamps are never affected. |
| Delete rule (D9, D38) | `delete_column`/`delete_family` at ts `T` hides every version in scope with ts ≤ `T`, regardless of commit order. `delete_cell(ts)` hides every version at exactly `ts`, also regardless of commit order: a later `put_at(.., ts, ..)` at that timestamp stays hidden. To rewrite a deleted version, use another timestamp. |
| `delete_row` (D10) | One family marker per family, same commit. |
| Read-your-writes (D19) | `commit` returns after durable at level **and** visible. |
| Durability resolution | per-call → writer default → `GroupSync`. |
| Writer | One writer per file; second open → `WriterLocked`. |
| Reader processes (D36, P4, early) | `open_reader` opens the `.phdb` file **read-write** (it never writes): the coordination locks are exclusive byte-range locks, which need a writable handle, as in SQLite WAL mode. Readers need write permission on the file; read-only media are not supported. |
| Family order (D39) | A row's cells come by family in **creation order**, or in the order you listed families (`family(..)` calls); then qualifier; then newest version first. |
| Application-owned mode (D40) | Starts no shard or compaction threads: you drive each `Shard`, and `open_application_owned` with `compaction_cores(k)`, `k > 0`, fails with `InvalidArgument`. The default I/O backend (`PreadVfs`) still starts a pool of 2–16 I/O threads (the CPUs available, clamped) that run WAL syncs, root commits and reads; they inherit the opener's CPU affinity. A fully threadless mode is Phase 3. |
| Handles | `Pigeonhole`, `Table`, `Snapshot`, `Cell`, `Row` are cheap `Clone`. `Table`: `Send + Sync`. |
| Snapshots | Pin data. Drop promptly. |
| Filesystem | Local only (`NetworkFilesystem`). |
| macOS/BSD file access | Closing any descriptor of the `.phdb` inside the process drops its writer lock (`fcntl` semantics). Never open the file with `std::fs` while it is open here; use `backup` to copy it. See [Concepts](concepts.md#platform-and-process-notes). |
| Reader liveness | By raw PID: writer and readers must share a PID namespace. |
| Custom `Vfs` clock | `monotonic_nanos` must advance at least every 10 µs; coarser clocks are treated as frozen. |
| Storage | Disk-backed: memtables flush into the file as they fill, so data size is bounded by the disk, not `memtable_budget` (per shard, default 64 MiB; also the shm arena size). A write that finds the arena full stalls while a flush frees room; `Busy` after the 30 s stall timeout is transient (back off, retry), `Busy` for a batch larger than the arena is not (split it). |
| Reopen budget | Reopening with a `memtable_budget` too small to hold the WAL's unflushed data (after a crash) fails with `InvalidArgument`; reopen with a larger one. |
| Files at rest | One file after a clean last `close`. While open, or after a crash: the file plus WAL sidecars and the shm region. Open replays the sidecars. |
| `None` durability (D94) | A `None` commit buffers its WAL record. A later `GroupSync`/`Sync` commit on the same shard, a `flush`, or a clean close makes it durable; a crash before then loses it. |
| Typed values | `incr` columns are `i64`. Write counters only with `incr` / `put_i64`: reading an `incr` on top of a base that is not an 8-byte `i64` fails with `MergeFailed` (D41). `merge` writes untyped operands (custom operators); the built-in `i64` add refuses them at read time. |

## Types
| Type | Role |
|---|---|
| `Pigeonhole` | Writer handle. |
| `PigeonholeReader` | Read-only handle in another process (P4, early). |
| `Shard` | One shard in application-owned mode. |
| `Snapshot` | Point-in-time view. |
| `Options`, `ReaderOptions`, `Family` | Config builders (consume and return `Self`). |
| `Priority` | `Low`, `Normal` (default), `High`. |
| `Compaction` | `Leveled` (default), `Tiered` (P2), `FifoByTime` (P2); P2 ones are refused with `Unsupported` today. |
| `Durability` | `None`, `Buffered`, `GroupSync` (default), `Sync`. |
| `TableBuilder`, `Table`, `ReadTable` | Define/open a table; read-write handle; read-only handle (P4, early). |
| `RowMutation`, `WriteBatch`, `Transaction` | Writes; `Transaction` is P4, early. |
| `CommitInfo { seqno: u64, durability: Durability }` | Commit result. |
| `RowRead`, `Scan`, `RowIter` | Read builders; scan iterator. |
| `ValueFilter`, `Condition` | Value predicate; `commit_if` condition (P2, early). |
| `CellRef<'a>`, `Cell`, `Row`, `RowRef<'a>`, `CellEntry<'a>`, `Value<'a>` | Borrowed and owned results. |
| `Error`, `ErrorCode`, `Result<T>` | Errors. |
| `MergeOperator`, `MergeError` | Custom merge (P2; a family naming a custom operator fails with `UnknownMergeOperator` today). |
| `days(n: u64) -> Duration` | TTL helper. |

## `Pigeonhole`
| Signature | Semantics |
|---|---|
| `open(path: impl AsRef<Path>, Options) -> Result<Pigeonhole>` | Open or create as writer; replays WAL. |
| `open_reader(path, ReaderOptions) -> Result<PigeonholeReader>` | P4, early. Read-only, any number of processes. Needs write permission on the file (D36). |
| `open_application_owned(path, Options) -> Result<(Pigeonhole, Vec<Shard>)>` | Writer with no shard or compaction threads (the default I/O backend still runs 2–16 I/O threads); you drive each `Shard`. `compaction_cores(k > 0)` → `InvalidArgument` (D40). |
| `table(&self, name: &str) -> Result<TableBuilder<'_>>` | Start define/open. |
| `tables(&self) -> Vec<String>` | Table names. |
| `drop_table(&self, name: &str) -> Result<()>` | Drop table and data. |
| `write_batch(&self) -> WriteBatch` | New multi-row batch. |
| `transaction(&self) -> Result<Transaction>` | P4, early. Optimistic transaction. |
| `snapshot(&self) -> Result<Snapshot>` | Consistent view of everything committed. |
| `default_durability(&self) -> Durability` | Writer default. |
| `set_default_durability(&self, Durability)` | Applies to later commits. |
| `flush(&self) -> Result<()>` | Write every memtable into the file; returns when the SSTs are in the manifest. Makes `None` commits durable. |
| `compact(&self) -> Result<()>` | Flush, then merge every level of every table into the last (purges per `max_versions`, TTL and tombstones). |
| `shrink(&self) -> Result<u64>` | Relocate live data from the file's tail into free space and truncate; returns bytes released (`0` if none). Online; costs a rewrite of the tail data. Call after deletes + `compact`. Errors: `Closed`, `ReadOnly`, `NoSpace` (no free extent to move into), `Io`. |
| `backup(&self, dest: impl AsRef<Path>) -> Result<()>` | Consistent single-file copy at a snapshot taken now, while writes continue. `dest` must not exist. Holds its snapshot (memtables included) for the whole run, so a long backup under heavy writes can stall writers into `Busy`. The copy opens with no WAL replay and no sidecars. `Unsupported` if a family stores blob files (P2; not reachable today). |
| `close(self) -> Result<()>` | Flushes memtables, checkpoints the WAL; the last handle out removes the sidecars and shm, leaving one file. |

`PigeonholeReader` (P4, early): `table(&self, &str) -> Result<ReadTable>`, `tables() -> Vec<String>`, `snapshot() -> Result<Snapshot>`. No write methods.
`Snapshot`: `seqno(&self) -> u64`.
`Shard`: `index() -> usize`, `run_once(&mut self, budget: Duration) -> bool` (true if work remains; background work waiting for a time does not count), `next_wakeup(&self) -> Option<Duration>` (time until that background work is due; with `tablet_changes` on, the default, the balancer's next pass keeps it `Some`: at most 100 ms after a write, backing off to 10 s while idle), `set_wakeup(&mut self, Box<dyn Fn() + Send + Sync>)` (fires when work arrives, not when background work falls due). `closed(&self) -> Option<Result<()>>` (the close's outcome once the whole close has finished). Loop: `run_once` until `false`, then sleep until the wakeup fires (work arrived or I/O completed) or `next_wakeup` passes. After `close()`, keep looping until `closed()` is `Some`, then drop the shard; `close()` on a thread that drives no shard (once every shard has been run) waits for this and returns the outcome; on a driving thread, or before every shard has run, it returns `Ok(())` at once and the outcome is only in `closed()`. On a thread that drives a shard (it last called `run_once`), `commit`, `check_and_mutate`, transaction commits, `flush` and `compact` fail with `InvalidArgument` before submitting anything; awaiting an already-submitted commit there blocking fails with `WouldDeadlock` (it will apply: poll it from the event loop). A thread holding a shard it never ran is not detected: run the shard first.

## `Options` (all `self -> Self`; process-local, not stored in file)
| Method | Meaning |
|---|---|
| `durability(Durability)` | Writer default (default `GroupSync`). |
| `shards(usize)` | Shard threads (default CPUs available). `1` is valid. |
| `compaction_cores(usize)` | Extra pinned threads for flush/compaction. Engine-owned mode only (D40). |
| `memtable_budget(u64)` | Arena bytes per shard (default 64 MiB). |
| `block_cache(usize)` | Block cache bytes (default 256 MiB; each reader process has its own). |
| `row_cache(usize)` | Row cache bytes (default 0 = off). |
| `shm_dir(impl Into<PathBuf>)` | Shared-memory file directory (e.g. tmpfs). |
| `create_if_missing(bool)` | Default true. |
| `merge_operator(Arc<dyn MergeOperator>)` | P2. Register custom operator. |
| `allow_unregistered_merge_operators(bool)` | Open read-only with compaction off if a family names an unregistered operator. |
| `tablet_changes(bool)` | Let tablets split, merge and move between shards so one table's writes spread over every shard (default on; off keeps each table as one tablet on one shard). Tablet owners are not stored; a reopen places tablets again. Commits in flight together on one row may apply in either order while its tablet moves. |

`ReaderOptions` (P4, early): `block_cache(usize)` (default 256 MiB **per reader process**, on top of the writer's), `shm_dir(..)`, `merge_operator(..)`.

## `Family` (all `self -> Self`; stored in file)
| Method | Meaning |
|---|---|
| `max_versions(u32)` | Keep ≤ n versions per column (0 = all). |
| `ttl(Duration)` | Expire cells older than this by timestamp. |
| `bloom_bits(u8)` | Filter bits per key (0 off; default 10). |
| `blob_threshold(u32)` | P2. Values above go to blobs (default 4096). Today: stored, values stay inline. |
| `lz4()` | Default compression. |
| `zstd(i8)` | P2. zstd at level. Today: table or family creation fails with `Unsupported`. |
| `uncompressed()` | No compression. |
| `block_size(u32)` | Data block bytes (default 16 KiB). |
| `merge_operator(&str)` | P2 for custom. Name of registered operator. `incr` needs none (`pigeonhole.i64_add` default). |
| `cache_priority(Priority)` | Block cache priority. |
| `compaction(Compaction)` | Strategy (`Tiered`, `FifoByTime` are P2 and refused with `Unsupported` today; the latter needs a TTL). |

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

`ReadTable` (P4, early) has `name`, `get`, `get_at`, `row`, `scan`, `scan_prefix`, `scan_bounds` with the same signatures.

## `RowMutation` (builder; each `self -> Self`)
| Method | Semantics |
|---|---|
| `put(family, qualifier: &[u8], value: &[u8])` | Bytes at commit timestamp. |
| `put_at(family, qualifier, ts: u64, value)` | Bytes at explicit timestamp (µs). |
| `put_i64(family, qualifier, i64)` / `put_f64(.., f64)` | Typed values. |
| `incr(family, qualifier, delta: i64)` | Blind atomic `i64` add (merge operand). |
| `merge(family, qualifier, operand: &[u8])` | Operand for the family's operator (P2 for custom). |
| `delete_cell(family, qualifier, ts: u64)` | Delete the version at `ts`; later puts at that `ts` stay hidden (D38). |
| `delete_column(family, qualifier)` | Delete all versions. |
| `delete_family(family)` | Delete all columns of a family in this row. |
| `delete_row()` | Delete whole row. |
| `durability(Durability)` | Override for this commit. |
| `commit(self) -> Result<CommitInfo>` | Commit. |
| `commit_if(self, &Condition) -> Result<Option<CommitInfo>>` | P2, early. Compare-and-set on this row; `None` if condition failed. |

`Condition` variants: `Exists { family: String, qualifier: Vec<u8> }`, `Absent { .. }`, `Value { family, qualifier, filter: ValueFilter }`.

## `WriteBatch` (methods take `&mut self -> &mut Self`)
| Method | Semantics |
|---|---|
| `put(&Table, row, family, qualifier, value)` | |
| `put_at(&Table, row, family, qualifier, ts, value)` | |
| `put_i64(&Table, row, family, qualifier, i64)` / `put_f64(.., f64)` | Typed values. |
| `incr(&Table, row, family, qualifier, delta)` | |
| `merge(&Table, row, family, qualifier, operand)` | Untyped operand (custom operators, P2). |
| `delete_cell(&Table, row, family, qualifier, ts)` | D38. |
| `delete_column(&Table, row, family, qualifier)` | |
| `delete_family(&Table, row, family)` | |
| `delete_row(&Table, row)` | |
| `len(&self) -> usize`, `is_empty(&self) -> bool` | |
| `commit(self) -> Result<CommitInfo>` | Writer default durability. |
| `commit_with(self, Durability) -> Result<CommitInfo>` | Override. |

`Transaction` (P4, early): `get(&mut self, &Table, row, family, qualifier) -> Result<Option<CellRef<'_>>>`, `put(..)`, `delete_column(..)` (as `WriteBatch`, `&mut Self`), `commit(self)`, `commit_with(self, Durability)`; `Conflict` on a conflicting commit.

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
| `limit(u64)` rows (0: none) | | ✓ |
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

Cells within a row: ordered by family (creation order, or the order the read listed families; D39), then qualifier, then newest version first.

## Error codes (`ErrorCode`, `#[non_exhaustive]`, `repr(u32)`)
`Io`=1 `Corruption`=2 `WriterLocked`=3 `ShmVersionMismatch`=4 `ShmUnavailable`=5 `UnsupportedFormat`=6 `NetworkFilesystem`=7 `TableNotFound`=8 `TableExists`=9 `FamilyNotFound`=10 `FamilyExists`=11 `UnknownMergeOperator`=12 `MergeFailed`=13 `Conflict`=14 `ReadOnly`=15 `KeyTooLarge`=16 `ValueTooLarge`=17 `NoSpace`=18 `InvalidArgument`=19 `Unsupported`=20 `Closed`=21 `NoReaderSlot`=22 `RecordTooLarge`=23 `Busy`=24 `SnapshotExpired`=25 `WouldDeadlock`=26. Causes and fixes: [`errors.md`](errors.md). `Error::code()`, `Error::message()`.

## Not yet available
| Feature | Phase |
|---|---|
| `backup` of databases with blob files ([#58](https://github.com/CodingAnarchy/pigeonhole/issues/58)) | P2 |
| zstd, blob separation, `Tiered`/`FifoByTime`, custom merge operators | P2 |
| `get_async`, `Scan::stream`, `commit_async`, `commit_with_ticket` (module `nonblocking`, feature `async`) | P3 |

## Recipes
### 1. Open, create table, write, read
```rust
use pigeonhole::{Family, Options, Pigeonhole};

# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
let users = db.table("users")?
    .family("profile", Family::default().max_versions(1))
    .create_if_missing()?;

users.mutate(b"user:42").put("profile", b"name", b"Ada").commit()?;
let name = users.get(b"user:42", "profile", b"name")?.map(|c| c.value().to_vec());
db.close()?;
# assert_eq!(name.as_deref(), Some(&b"Ada"[..]));
# Ok::<(), pigeonhole::Error>(())
```

### 2. Counter
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let users = pigeonhole::doc_support::table(&db, "users", &["profile"])?;
users.mutate(b"user:42").incr("profile", b"logins", 1).commit()?;
let n: i64 = users.get(b"user:42", "profile", b"logins")?
    .and_then(|c| c.as_i64()).unwrap_or(0);
# assert_eq!(n, 1);
# Ok::<(), pigeonhole::Error>(())
```

### 3. Prefix scan with pagination
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let users = pigeonhole::doc_support::table(&db, "users", &["profile"])?;
# for i in 0..250 { users.mutate(format!("user:{i:03}").as_bytes()).put("profile", b"name", b"x").commit()?; }
# let mut seen = 0;
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
#       seen += 1;
    }
    if n < 100 { break; }
}
# assert_eq!(seen, 250);
# Ok::<(), pigeonhole::Error>(())
```

### 4. Atomic multi-row write with chosen durability
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let users = pigeonhole::doc_support::table(&db, "users", &["profile"])?;
use pigeonhole::Durability;

let mut wb = db.write_batch();
wb.put(&users, b"user:1", "profile", b"name", b"A")
  .put(&users, b"user:2", "profile", b"name", b"B")
  .delete_row(&users, b"user:0");
let info = wb.commit_with(Durability::GroupSync)?;
assert_eq!(info.durability, Durability::GroupSync);
# Ok::<(), pigeonhole::Error>(())
```

### 5. Consistent multi-read, versions, event time
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let users = pigeonhole::doc_support::table(&db, "users", &["profile"])?;
# let (t0_us, t1_us, event_ts_us) = (0u64, u64::MAX, 1_759_622_400_000_000u64);
let snap = db.snapshot()?;
let a = users.get_at(&snap, b"user:1", "profile", b"name")?.map(|c| c.to_owned());
let history = users
    .row(b"user:1").family("profile").qualifier_prefix(b"name")
    .versions(5).time_range(t0_us..t1_us).snapshot(&snap).read()?;
drop(snap); // release promptly

users.mutate(b"user:1").put_at("profile", b"name", event_ts_us, b"Ada").commit()?;
# Ok::<(), pigeonhole::Error>(())
```

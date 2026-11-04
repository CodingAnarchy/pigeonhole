# 0100: `pigeonhole-vector`, an IVF index on the public API

**Status:** Approved (number from the coordinator, 2026-10-31; the owner accepted D306 at revision 4 on #490, c6548492501). PR 1 (#1267) adds the index with inline rebalancing. PR 2 adds `bulk_load` and the `Rebalancer`. PR 3 adds the box measurements, the defaults and the guide.

## PR 1: the index
`pigeonhole-vector` is on the public 1.x line (D295). Everything here is semver-checked (`crates/vector/public-api.txt`).

- **`Index`** (`Clone`, `Send + Sync`; clones share the centroid cache):
  - `Index::create(&Pigeonhole, name, VectorSpec, IndexOptions) -> Result<Index>`: creates the table `name` with the index's families. It's `TableExists` if the table is there.
  - `Index::open(&Pigeonhole, name) -> Result<Index>`: `InvalidArgument` if the table isn't an index. It queues every list outside its bounds (D306 rev 3 M3).
  - `insert_f32`, `insert_i8`, `insert_bits(&self, id: &[u8], vector) -> Result<()>`: an upsert, with any split it triggers run before it returns.
  - `delete(&self, id) -> Result<bool>`: any merge it triggers runs before it returns.
  - `search_f32`, `search_i8`, `search_bits(&self, query, k) -> Search<'_>`.
  - `verify(&self) -> Result<IndexStats>`: checks the rows against each other at one snapshot. A contradiction is `Corruption`.
  - `verify_at(&self, &Snapshot) -> Result<IndexStats>`: `verify` at a given snapshot of the index's database (#1427, 1.2.0).
  - `ids_at(&self, &Snapshot, impl FnMut(&[u8])) -> Result<()>`: calls back with every vector's id at that snapshot, in ascending byte order, once each, so that a caller can check membership against its own rows (#1427, 1.2.0). It reads the id mappings, which `verify_at` checks against the lists.
  - **Both refuse a snapshot of another database,** as any foreign-snapshot read is refused.
  - `spec`, `options`, `max_list_len` (2 × target), `min_list_len` (target / 4).
- **`IndexOptions`** (`Copy + Eq + Default`):
  - `new()`;
  - `target_list_len(n)`: default 1,000, at least 4, and scaled down so `2 × target` vectors fit in 4 MiB;
  - `nprobe(n)`: default 24 (PR 3: 95.8% recall@10 on SIFT1M at 3.4 ms p50; it was 16 before PR 3), at least 1;
  - `reassign_neighbours(r)`: default 2, and 0 turns phase 2 off.

  They're stored with the index and fixed at creation.
- **`Search<'a>`** (`#[must_use]`):
  - `nprobe(n)`;
  - `snapshot(&Snapshot)`;
  - `parallel(n)`: D296 PR 4's `parallel(n)` on the probed list rows;
  - `run() -> Result<Vec<Hit>>`: nearest first, ties by list row and then id.
- **`Hit`** (`#[non_exhaustive]`): `id: Vec<u8>`, `distance: f64` (`VectorSpec`'s, as `nearest_*` gives it).
- **`IndexStats`** (`#[non_exhaustive]`): `list_lens: Vec<u64>` (by list id), `vectors: u64`.
- **`Error`** (`#[non_exhaustive]`): `Db(pigeonhole::Error)`, `InvalidArgument`, `Corruption`, `Conflict` (the layout changed under every reload of an insert or delete; retrying is safe). It has `code() -> ErrorCode`, `Display`, `std::error::Error` and `From<pigeonhole::Error>`.
- **`Result<T>`**.
- **Hidden and unstable:** `hooks` behind `__unstable-hooks` (ICR 0085's convention), the test seams `arm(Hold::SplitBeforeCommit)` and `skip_rebalance`.

**Not in PR 1:**
- `search_*_async`: it waits for D296's `nearest_*_async`, which follows #1259 in its own PR.
- `bulk_load` and the `Rebalancer` (PR 2).
- A guide page (PR 3).

## PR 2: bulk loading and background rebalancing
- **`IndexOptions::rebalance(Rebalance)`** and `rebalance_mode()`. `Rebalance` (`#[non_exhaustive]`, `Default`) is `Inline` (the default: the write rebalances before it returns) or `Background` (a write marks the lists it took out of bounds, and a `Rebalancer` splits and merges them). It's stored with the index. `G:opts` becomes version 2, and version 1 reads as `Inline`.
- **`Index::open_with_rebalancer(&db, name) -> Result<(Index, Rebalancer)>`** and **`Index::create_with_rebalancer(&db, name, spec, options)`**: the rebalancer is handed to the application, with no crate thread. That's the owner's §6.4 call: the D284 model, with no engine hook. `open` and `create` start the crate's own thread (`pigeonhole-vector`) for a `Background` index. It stops when the last handle drops or the database closes.
- **`Rebalancer`** (`Debug`), mirroring `BackgroundWorker`:
  - `run_once(&mut self, budget: Duration) -> Result<bool>`: splits and merges marked lists until none is left or the budget has passed, always taking at least one. It returns whether to call again: it did some work, or lists are left because the budget ran out. `false` means nothing is left until a write marks a list. That's the contract the zero-budget test pins, after #1271's Windows run showed the old "did any work" answer could stop a caller's loop with lists still marked.
  - `wait(&mut self, timeout)`: until a write marks a list, the index closes, or the timeout passes.
  - `closed(&self) -> bool`: every `Index` handle is gone.
  - `set_wakeup(&mut self, Box<dyn Fn() + Send + Sync>)`: called when a write marks a list, for event loops.
- **`Index::bulk_load_f32` / `_i8` / `_bits(&self, expected: u64, vectors: impl IntoIterator<Item = (K, V)>) -> Result<BulkStats>`** (`K: AsRef<[u8]>`, `V: AsRef<[f32]>`, `AsRef<[i8]>` or `AsRef<[u8]>`). It works on an empty index only (`InvalidArgument` otherwise):
  - k-means (10 Lloyd iterations, evenly spaced seeds) on the first `max(50,000, expected / target)` vectors, into `expected / target` lists;
  - then each vector streams to its nearest list, a batch of 256 per transaction, as an upsert. A repeated id keeps its last vector.
  - Lists left out of bounds are then rebalanced as the index's mode says.
- **`BulkStats`** (`#[non_exhaustive]`, `Copy`): `vectors`, `lists`.
- **Docs:** `Hit` and `Search::run` state the tie order. Hits at an equal distance come by list, then id, so a split can change which tied id makes the cut at `k` (#1268).
- **Hidden and unstable** (`__unstable-hooks`): `Hold::{PlaceBeforeCommit, DeleteBeforeCommit, ReassignBeforeCommit}`, `Armed::wait_for`, and `fail_after_split`.

## `Search::run_async` (#1268 item 6)
- **`Search::run_async(self) -> Result<Vec<Hit>>`**, behind the crate's new default-on `async` feature (`pigeonhole/async`, plus `futures-core`), mirroring `pigeonhole`'s. It's an `async fn` whose future is `Send`.
- **It equals `run`.** It never blocks on the device:
  - the layout version is read with `Table::get_at_async`;
  - a layout that changed is reloaded through `Scan::stream`;
  - the probed lists are read with `Nearest::run_async` (D296 PR 5), which yields within a wide list.
- **It never fans out:** `parallel` is ignored, as for `Nearest::run_async`.

## `Index::open_reader` and `ReadIndex` (#1268 item 5)
- **`Index::open_reader(&PigeonholeReader, name) -> Result<ReadIndex>`:** the index opened from a reader process, with `open`'s checks (`InvalidArgument` for a table that isn't a vector index).
- **`ReadIndex`** (`Clone`, `Debug`): `spec()`, `options()`, and `search_f32`/`search_i8`/`search_bits` returning the same `Search`. So `nprobe`, `snapshot`, `parallel`, `run` and `run_async` all work. It has no write method.
  - **The centroids load with reads only,** at the reader's snapshot. The handle keeps its own cache and reloads it when a search sees `G:layout` change, as `Index` does.
  - **The results equal the writer's `Index` at the same snapshot.**
  - **If the writer restarts,** a search fails with `NoWriter` or `SnapshotExpired` (`Error::Db`) until the reader reattaches; searching again is enough.
  - **The layout cache follows a writer that lost commits:** a search at the current snapshot adopts its fresh layout when the cache's seqno is newer than any now visible (perf287 on #1385). The same rule applies to `Index`.
- **Internal:** the centroid load and the layout check are shared through a crate-private `Rows` trait over `Table` and `ReadTable`, and `Search` reads either kind of index.

## Half-precision indexes (#1268 item 7)
- **`Index::create` takes `F16` and `BF16` specs** (ICR 0104's elements). Centroids stay `f32` of the same dimension and metric, since a mean of halves isn't a half.
- **`Index::insert_f16(id, &[u16])`, `insert_bf16`:** each `u16` is a bit pattern, stored bit for bit. A split's move rewrites the patterns it read, exactly.
- **`Index::search_f16(&[u16], k)`, `search_bf16`, and the same on `ReadIndex`:** the query widens to `f32` exactly and searches as `nearest_f32` searches a half family, so the hits equal `search_f32` with the widened query.
- **`insert_f32` and `search_f32` work on a half index too.** A written `f32` vector is rounded to the element, as `put_vector_f32` rounds into a half family, and it's placed in its list by the rounded value, the one stored, so a later split or merge sees the same point (perf287 on #1387). Other mismatches (`i8` or bits in a half index, `f16` bits in a `bf16` index) are `InvalidArgument`.
- **`Index::bulk_load_f16(expected, vectors)`, `bulk_load_bf16`:** `bulk_load_f32` for half bit patterns, stored bit for bit. The k-means sample and each vector's placement widen into `f32`; the streaming pass refills one buffer per vector. `bulk_load_f32` loads a half index too, rounding as `insert_f32` does.
- **Internal:** a crate-private `half` module widens patterns into a caller's buffer (`Input::widen_into`), checked against `VectorRef::widen_into` over all 65,536 patterns, and rounds `f32` to a half (`half::narrow`), checked against the format's rounding that `put_vector_f32` uses.

## A per-process registry (#1268 item 8, ICR 0110)
- **`Index::open`, `open_with_rebalancer` and the `create` forms share an index already open in the process.** They return a clone of the open handle, sharing its centroid cache, its queue of lists to rebalance and its one rebalancing thread. Before, each open had its own, which was correct but duplicated the work.
- **"Already open" means three things.** An application handle to it is still alive. It's in the same open database, by `Pigeonhole::same_database` (ICR 0110): not another file, a copy of one, or the same file reopened after a close. And its table is still the one opened: the options row still reads through the entry's own table handle, which a dropped (and perhaps recreated) table or a closed database refuses.
- **The crate's thread starts once per index,** at the first `open` in background mode. `open_with_rebalancer` hands out a rebalancer on the shared queue; several take turns list by list (each list is claimed by one at a time), and they share one `set_wakeup`.
- **Internal:** a process-wide `Mutex<Vec<Entry>>` of `(name, Weak<Inner>, Weak<user token>)`. The lock is held across a fresh open (a few reads), so concurrent first opens get one index, and across the stale-entry read. Nothing under it takes it again, and no `Inner` drops under it.

## Writes in a caller's transaction, and prefixed searches (D311 T8, #1342)
For the agent memory store's IVF recall (D311 §8), which writes a memory's vector in the memory's own transaction and searches one namespace of a store-wide index.
- **`Index::insert_f32_in(&mut Transaction, &mut Placed, id, &[f32]) -> Result<()>`,** with `insert_i8_in`, `insert_bits_in`, `insert_f16_in` and `insert_bf16_in`. The vector commits or rolls back with the caller's other writes.
  - **`Placed` is the transaction's record of its index writes** (one per transaction, `Placed::new()`): where it put each id, and whether it created list 0. A transaction's reads don't see its own writes, so a second write of an id in one transaction (an upsert, a delete after an insert, an insert after a delete, a second insert into the index it bootstrapped) acts on the record, not the snapshot (cli692's B1). It belongs to the index its first write used (another is `InvalidArgument`, C1), and is made inside the transaction's body so a retry starts fresh (documented, C2: a `Transaction` has nothing to bind it to).
  - **Placement:** against the shared centroid cache. When the list's version, read in the transaction, differs, the layout is reloaded once and placed against.
  - **If it still differs,** the transaction's snapshot predates a split or merge (the reload's snapshot is newer). The vector is written anyway, and the commit fails with `Conflict`, which `transact` retries, because a version the transaction read was rewritten after its snapshot. The body never loops.
  - **A mapping to a list with no centroid** at the snapshot is `Error::Corruption`, as `verify` reports it (cli692's B2). A merge moves a list's vectors and mappings and deletes its centroid in one transaction that reads the list's whole row (a family read, so any insert into the list conflicts with it), so no consistent snapshot shows one. A stress test of in-transaction writes racing inline splits and merges surfaces no body error.
  - **It doesn't retry, split or merge.**
- **`Index::delete_in(&mut Transaction, &mut Placed, id) -> Result<bool>`:** `false`, writing nothing, when there's no vector for `id` as of the transaction's own earlier writes, else its snapshot.
- **`Index::after_commit(Placed) -> Result<()>`:** after the caller's commit, it splits or merges (or, in background mode, marks) the lists the write left out of bounds, as `insert_f32` and `delete` do after theirs. It reads the lists' current lengths, so calling it after a rollback, twice or late is harmless, and never calling it only delays the rebalance until the next write to those lists.
  - **Why it's separate:** checking the bounds inside the caller's transaction would read the count row, so concurrent inserts into one list would conflict.
- **`Placed`** (`Debug`, `Clone`, `Default`, `PartialEq`, `Eq`, `#[must_use]`): opaque, owned and `Send`.
- **`Search::id_prefix(&[u8])`:** only ids with that prefix (a qualifier prefix on the probed lists' read).
  - **Widening:** a search with a prefix probes `nprobe` lists, then doubles the lists it probes, nearest first, until `k` hits pass, every list is probed, or `max_probe` lists are. Each round reads only its new lists: a round widens only when the earlier ones found fewer than `k`, so they already hold every prefixed vector of the lists read, and the nearest `k` of the union are the nearest over every list probed. Ties come back by probe round, then list, then id (perf287's S1).
  - **Exactness:** a prefixed search that probes every list is exact for the prefix.
- **`Search::max_probe(u32)`:** the widening's cap (default: every list), never below `nprobe`. It has no effect without a prefix.
- **`Search::run_with_stats() -> Result<(Vec<Hit>, SearchStats)>`:** `run` with what it read.
  - `SearchStats` (`#[non_exhaustive]`, `Copy`) holds `lists_probed` and `lists` (the index's lists at the snapshot). A search cut by `max_probe` shows fewer than `k` hits with `lists_probed < lists`.
  - `run_async` widens the same way, and `run_async_with_stats` is `run_with_stats`'s async form.
- **Test seam:** `hooks::Hold::MergeBeforeReload`, between a merge's commit and its cache reload, where the shared cache names a merged-away list.

## The table (internal, not promised)
The table holds:
- the families `vec` (the index's spec), `cen` (an `f32` vector family of the same dimension and metric, or the index's own spec for bits; D306 rev 2 S1), `meta`, `map` and `cnt` (a counter);
- the rows `L{scramble(list)}`, `C{list}`, `N{list}`, `M{id}` and `G`, as D306 describes.

`G:opts` is version 2 (PR 2). Background markers are `Q{list}:meta:q` rows: hints, which `open` re-queues and a rebalancer deletes in the split's or merge's transaction. Writing to the table directly is unsupported.

## Why
D306 (#490 D): an approximate nearest-neighbour index for collections beyond what an exact `nearest_*` scan serves at interactive latency, built from ordinary rows and transactions, so it is as durable and consistent as the database.

## Callers changed
None: a new crate.

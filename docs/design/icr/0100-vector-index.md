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
  - `spec`, `options`, `max_list_len` (2 × target), `min_list_len` (target / 4).
- **`IndexOptions`** (`Copy + Eq + Default`):
  - `new()`;
  - `target_list_len(n)`: default 1,000, at least 4, and scaled down so `2 × target` vectors fit in 4 MiB;
  - `nprobe(n)`: default 16, at least 1;
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
  - `run_once(&mut self, budget: Duration) -> Result<bool>`: splits and merges marked lists until none is left or the budget has passed. Whether it did any.
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

## The table (internal, not promised)
The table holds:
- the families `vec` (the index's spec), `cen` (an `f32` vector family of the same dimension and metric, or the index's own spec for bits; D306 rev 2 S1), `meta`, `map` and `cnt` (a counter);
- the rows `L{scramble(list)}`, `C{list}`, `N{list}`, `M{id}` and `G`, as D306 describes.

`G:opts` is version 2 (PR 2). Background markers are `Q{list}:meta:q` rows: hints, which `open` re-queues and a rebalancer deletes in the split's or merge's transaction. Writing to the table directly is unsupported.

## Why
D306 (#490 D): an approximate nearest-neighbour index for collections beyond what an exact `nearest_*` scan serves at interactive latency, built from ordinary rows and transactions, so it is as durable and consistent as the database.

## Callers changed
None: a new crate.

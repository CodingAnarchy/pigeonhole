# 0102: the Python API (`pigeonhole-db`, `import pigeonhole`)

**Status:** Approved (number from the coordinator, 2026-10-31; owner decision D307 on #696: PyO3 directly over the Rust `pigeonhole` crate, the PyPI name `pigeonhole-db` and CPython 3.11 as the floor, `abi3-py311`).

## The change
A new workspace crate, `crates/python` (`pigeonhole-python`, `publish = false` on crates.io), builds the extension module `pigeonhole._native`. The `pigeonhole` Python package re-exports it.

**The snapshot:** the package's typed stubs, `crates/python/python/pigeonhole/__init__.pyi`. CI's `mypy.stubtest` (the Python workflow) fails if they and the built module disagree, so a PR that changes the Python API shows it in the stubs' diff. That plays the role `public-api.txt` plays for the Rust crates.

### PR 1: the core API
- **Opening:**
  - `open(path, *, create, shards, durability, block_cache, row_cache, memtable_budget, shm_dir, read_only_fallback) -> Database`;
  - `open_reader(path, *, shm_dir, block_cache, row_cache) -> Reader`.
- **`Database`:**
  - tables: `table(name) -> TableBuilder`, `tables()`, `drop_table`;
  - reads and writes: `snapshot(*, durable)`, `write_batch()`, `transaction()`, and `transact(fn, *, max_attempts, deadline, durability)`, which retries on `Conflict` and `Busy` by `RetryPolicy`;
  - maintenance: `flush`, `compact`, `close`; also a context manager.
- **`Reader`:** `table`, `tables`, `snapshot`, `close`; also a context manager.
- **`TableBuilder`:** `family(name, Family | None)`, `create`, `create_if_missing`, `open`.
- **`Table`:**
  - `name`, `families`, `family_info`;
  - reads: `get(..., snapshot)`, `row(..., families, qualifier_prefix, versions, column_limit, snapshot)`, `scan(prefix, *, start, end, families, qualifier_prefix, versions, limit, column_limit, snapshot, chunk) -> ScanIter`;
  - writes: `mutate(row) -> Mutation`, and one-commit `put`, `incr`, `delete`. A reader's table raises `ReadOnly` on any write.
- **Writes:** `Mutation`, `WriteBatch` and `Transaction` take `put`, `put_u64`, `incr`, `aggregate`, `delete`.
  - `Mutation` adds `durability`, `commit`, and `commit_if(Condition)`, which returns `None` when the condition fails.
  - `WriteBatch` adds `commit(*, durability)` and `__len__`. As a context manager it commits on a clean exit and discards on an exception.
  - `Transaction` adds `get`, `get_row`, `commit` and `rollback`. As a context manager it commits on a clean exit.
- **Values:**
  - The Python type picks the cell type: bytes-like and `str` are bytes, `int` is `i64` (`OverflowError` past it), and `float` is `f64`. A `bool` is refused.
  - `aggregate` reads its input by the family's kind: any number for the f64 kinds, a mask for bitwise families, an int or bytes for `hll` and `bloom`.
  - Keys and qualifiers take bytes, bytearray, memoryview or `str` (UTF-8).
- **Results:**
  - `Cell`: `value`, `ts`, `typed`, `as_int`, `as_u64`, `as_float`, and the aggregate readers `hll_estimate`, `sum_f64`, `mean_var`, `histogram_counts`, `quantile` and `bloom_contains`.
  - `Row`: `key`, `get`, `cells`, `len`; iterating gives `(family, qualifier, cell)`.
  - `Snapshot`: `seqno`, `release`; also a context manager.
  - `FamilyInfo` (`Table.family_info`), with typed read-only properties that mirror Rust's `FamilyInfo`: `name`, `counter`, `aggregation`, `max_versions`, `ttl` (a `timedelta`), `bloom_bits`, `blob_threshold`, `block_size`, `compression`, `zstd_level`, `merge_operator`, `cache_priority`, `compaction` and `family` (a `Family`). It is a class rather than a dict, so the stubs pin it (cli692's S1).
- **Options:**
  - `Family(*, max_versions, ttl, bloom_bits, block_size, blob_threshold, compression, zstd_level)`, `Family.counter`, `Family.aggregate(kind, ...)`;
  - `Condition.absent`, `.exists` and `.equals`;
  - `Durability`, an `enum.IntEnum` defined in the package: `NONE`, `BUFFERED`, `GROUP_SYNC`, `SYNC` (0–3). An int with one of those values is accepted too.
- **Errors:** `Error`, plus one subclass per `ErrorCode`, named as the code. Each carries `code` (the name) and `code_num`.

### PR 2: vectors, the 1.1 reads, closing a scan (#1261)
- **Vectors (D303):**
  - `Element` (`F32`, `I8`, `BIT`) and `Metric` (`L2`, `COSINE`, `INNER_PRODUCT`, `HAMMING`), `enum.StrEnum`s in the package;
  - `VectorSpec(dim, element, metric)` with `dim`, `element`, `metric`;
  - `Family.vector(dim, element, metric, *, max_versions, ttl)`, and `FamilyInfo.vector`;
  - `put_vector(family, qualifier, vector, *, ts)` on `Mutation`, and with a table and row first on `WriteBatch` and `Transaction`. The family's element type decides how a buffer (PyO3's `PyBuffer`, so NumPy arrays and `array.array`) or a sequence is read; bits are packed bytes.
  - `Cell.as_vector()`, and `Cell.typed` for vector cells: an `array.array('f'|'b')`, or `bytes` for bits.
- **Reads:**
  - `Table.multi_get(keys, *, families, qualifier_prefix, versions, column_limit, snapshot) -> list[Row]` (D299). It's named `multi_get`, not `keys`, which reads like a dict method in Python.
  - `Table.traverse(seeds, family, *, hops, per_node_limit, hop_limit, max_nodes, parents, qualifier_prefix, snapshot, parallel) -> Traversal` (D304). `Traversal` has `hops`, `seeds` (sorted: hop 1's parents index them), `rows`, `edges`, `hops_cut` and `cut_by_max_nodes`; `Hop` has `nodes` and `parents`.
  - `Table.pages(row, size, *, families, qualifier_prefix, versions, snapshot, after) -> Pages` (D297): an iterator of `Row` pages with `position`, holding no read open between pages. `RowPosition` has `to_bytes`, `from_bytes` and `==`.
- **`ScanIter.close()`** and context-manager support. After `Database.close()`, a live iterator hands out the rows it already read, then raises `Closed`.
- **Not yet:** `nearest` waits for D296 PR 4 (#1259) in `pigeonhole`.

## Why
D307 (#696): most agent frameworks and AI tooling are Python. The owner decided to bind the Rust crate directly rather than go through the C ABI (#690).

## Callers
This is new API, so there are no callers. No Rust interface changes: the crate uses `pigeonhole`'s public API only.

## Later PRs
- **PR 2** (#1261): above, with the per-op overhead benchmark and `docs/guide/python.md`.
- **Next:** `nearest` on scans and `multi_get`, returning a typed `Neighbour`, once #1259 lands.
- **PR 3:** the PyPI publish workflow, after the owner's go.

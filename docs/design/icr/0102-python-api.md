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
- **Options:**
  - `Family(*, max_versions, ttl, bloom_bits, block_size, blob_threshold, compression, zstd_level)`, `Family.counter`, `Family.aggregate(kind, ...)`;
  - `Condition.absent`, `.exists` and `.equals`;
  - `Durability.NONE`, `BUFFERED`, `GROUP_SYNC` and `SYNC`.
- **Errors:** `Error`, plus one subclass per `ErrorCode`, named as the code. Each carries `code` (the name) and `code_num`.

## Why
D307 (#696): most agent frameworks and AI tooling are Python. The owner decided to bind the Rust crate directly rather than go through the C ABI (#690).

## Callers
This is new API, so there are no callers. No Rust interface changes: the crate uses `pigeonhole`'s public API only.

## Later PRs
- **PR 2:** graph reads (`keys`, `traverse`) and wide-row walks; the per-op overhead benchmark; the Python guide page. Each addition lands in the stubs.
- **PR 3:** the PyPI publish workflow, after the owner's go.

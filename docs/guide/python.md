# Python

> **Status:** this guide describes `main`. The bindings (`pigeonhole-db` on PyPI, `import pigeonhole`; D307, ICR 0102, #696) are new in 1.1.0 and aren't on PyPI yet. Every code block on this page runs as a test (`crates/python/tests/test_guide.py`).

The Python bindings are built with PyO3 directly over the Rust `pigeonhole` crate, so Python gets the same engine, file format and guarantees as Rust. Every engine call releases the GIL.

## Install
- **From PyPI, once published:** `pip install pigeonhole-db`, for CPython 3.11 or later. One abi3 wheel per platform covers every Python version: Linux (manylinux and musllinux, x86_64 and aarch64), macOS (universal2) and Windows (x86_64).
- **From a checkout:** `pip install ./crates/python` builds the extension, which needs a Rust toolchain. For development, run `maturin develop` in `crates/python` inside a virtualenv.

The package ships typed stubs (`py.typed`), so editors and type checkers see the whole API.

## A tour
```python
import pigeonhole as ph

with ph.open(path, create=True) as db:
    users = (db.table("users")
               .family("profile")
               .family("stats", ph.Family.counter())
               .create_if_missing())

    users.put(b"u1", "profile", b"name", "Ada")
    users.mutate(b"u1").put("profile", b"age", 36).incr("stats", b"logins", 1).commit()

    cell = users.get(b"u1", "profile", b"name")
    assert (cell.value, cell.typed) == (b"Ada", b"Ada")
    assert users.get(b"u1", "profile", b"age").typed == 36

    for row in users.scan(prefix=b"u"):
        for family, qualifier, cell in row:
            print(row.key, family, qualifier, cell.typed)
```

`path` is any path; these examples get it from the test that runs them.

## Values and types
- **Keys and qualifiers** are `bytes`, `bytearray`, `memoryview` or `str` (as UTF-8). Results always give `bytes`.
- **A value's Python type picks its cell type:**
  - bytes-like or `str` is a byte value;
  - `int` is an `i64` (an `OverflowError` past it; `put_u64` writes a `u64`);
  - `float` is an `f64`;
  - a `bool` is refused, since `True` is an `int` in Python and a stored 1 is rarely what was meant.
- **`Cell.typed`** gives the value back as its type. `Cell.value` is always the raw bytes, and `Cell.ts` is the timestamp in microseconds since the Unix epoch.
- **Aggregate families** (`Family.aggregate("max_f64")`, `"hll"`, `"histogram"`, ...) take inputs through `aggregate`, read by the family's kind: any number for the f64 kinds, a non-negative int for the bitwise ones, an int or bytes for `hll` and `bloom`. Read them with the `Cell` methods (`as_float`, `hll_estimate`, `quantile`, `histogram_counts`, `mean_var`, `bloom_contains`).

```python
import pigeonhole as ph

with ph.open(path, create=True) as db:
    t = (db.table("metrics")
           .family("peak", ph.Family.aggregate("max_f64"))
           .family("seen", ph.Family.aggregate("hll"))
           .create())
    m = t.mutate(b"api")
    for latency in [12, 7.5, 31]:
        m.aggregate("peak", b"ms", latency)
    for user in ["ada", "grace", "ada"]:
        m.aggregate("seen", b"users", user)
    m.commit()
    assert t.get(b"api", "peak", b"ms").as_float() == 31.0
    assert t.get(b"api", "seen", b"users").hll_estimate() == 2
```

## Reads
- **`get(row, family, qualifier)`** returns the newest version of one cell, or `None`.
- **`row(row, families=..., qualifier_prefix=..., versions=..., column_limit=...)`** returns one row's cells.
- **`scan(prefix=...)`, or `scan(start=..., end=...)`,** returns an iterator over rows in key order. It reads `chunk` rows (default 256) per GIL release, all under one snapshot. Close a scan you stop early (`with t.scan() as it:`, or `it.close()`): a live iterator holds its snapshot, and in a reader process its slot.
- **`multi_get(keys)`** reads the rows of many keys in one read. The result is aligned with `keys`: one entry per key, in the order given, with `None` for a key with no row.
- **`pages(row, size)`** walks a wide row a page at a time, each at most `size` cells, with a column's versions never split. Its `position` resumes the walk later, also in another process: `RowPosition.to_bytes()`, then `pages(row, size, after=RowPosition.from_bytes(token))`.

Every read takes `snapshot=`: pass the same one to a chain of reads (or pages) to make them one consistent read.

```python
import pigeonhole as ph

with ph.open(path, create=True) as db:
    t = db.table("feed").family("post").create()
    m = t.mutate(b"timeline")
    for i in range(25):
        m.put("post", f"{i:04}", f"post {i}")
    m.commit()

    walk = t.pages(b"timeline", 10)
    first = next(walk)
    token = walk.position.to_bytes()          # hand this to a client
    later = t.pages(b"timeline", 10, after=ph.RowPosition.from_bytes(token))
    assert len(first) + sum(len(p) for p in later) == 25

    with db.snapshot() as snap:
        t.put(b"timeline", "post", b"9999", "late")
        assert len(t.row(b"timeline", snapshot=snap)) == 25
```

## Writes, batches and transactions
- **`Table.put`, `incr` and `delete`** each commit one change.
- **`mutate(row)`** chains writes to one row and commits them atomically. `commit_if(Condition.absent(...))` commits only if the condition holds, and returns `None` otherwise.
- **`db.write_batch()`** spans rows and tables atomically. As a context manager, it commits on a clean exit and writes nothing if the block raises.
- **`db.transaction()`** is optimistic: its reads see one snapshot, and `commit()` raises `Conflict` if a cell it read changed meanwhile.
- **`db.transact(fn)`** runs `fn(txn)` and retries it on `Conflict` and `Busy`, with backoff. It returns what `fn` returned. `fn` may run more than once, so keep its side effects inside the transaction.

```python
import pigeonhole as ph

with ph.open(path, create=True) as db:
    acct = db.table("accounts").family("bal").create()
    with db.write_batch() as wb:
        wb.put(acct, b"alice", "bal", b"v", 100).put(acct, b"bob", "bal", b"v", 0)

    def transfer(txn, amount=30):
        a = txn.get(acct, b"alice", "bal", b"v").as_int()
        b = txn.get(acct, b"bob", "bal", b"v").as_int()
        txn.put(acct, b"alice", "bal", b"v", a - amount).put(acct, b"bob", "bal", b"v", b + amount)

    db.transact(transfer)
    assert acct.get(b"bob", "bal", b"v").as_int() == 30
```

## Vectors
A vector family stores fixed-size vectors (D303):
- `Family.vector(dim, Element.F32 | I8 | BIT, Metric.L2 | COSINE | INNER_PRODUCT | HAMMING)`.
- `put_vector` takes a buffer or a plain sequence of the family's element type.
  - For `f32`: a NumPy `float32` array or `array.array('f')` is the fast path. NumPy's default `float64` works too, narrowed to `f32`.
  - For `i8`: an `int8` buffer.
  - Bits go in packed, as bytes. An `f32` or `i8` family refuses `bytes`, which in Python would otherwise read as one element per byte.
- `Cell.as_vector()` returns an `array.array` (`'f'` or `'b'`), which `numpy.frombuffer` wraps without a copy, or `bytes` for bits.

`nearest(family, query, k)` finds the `k` cells nearest `query` by the family's metric. It searches a prefix, a `start`/`end` range, the whole table, or the rows of `keys`, and returns `Neighbour`s (`row`, `qualifier`, `distance`, `ts`) nearest first. The search is exact: every vector in the read is scored, and `parallel=n` shares the work across threads.

```python
import array

import pigeonhole as ph

with ph.open(path, create=True) as db:
    docs = (db.table("docs")
              .family("text")
              .family("emb", ph.Family.vector(4, ph.Element.F32, ph.Metric.COSINE))
              .create())
    docs.mutate(b"d1").put("text", b"body", "hello").put_vector("emb", b"v", [0.1, 0.2, 0.3, 0.4]).commit()
    v = docs.get(b"d1", "emb", b"v").as_vector()
    assert v == array.array("f", [0.1, 0.2, 0.3, 0.4])
    docs.mutate(b"d2").put_vector("emb", b"v", [0.4, 0.3, 0.2, 0.1]).commit()
    best = docs.nearest("emb", [0.1, 0.2, 0.3, 0.5], k=1)
    assert best[0].row == b"d1"
```

## Graphs
Store a graph as adjacency rows, as in the [graphs guide](graphs.md): a row per node, and in an edge family one column per edge, its qualifier the destination's key. `traverse(seeds, family, hops=...)` runs a breadth-first search inside the engine. It returns each hop's new nodes, sorted, with `parents=True` for shortest paths, and takes `per_node_limit`, `hop_limit` and `max_nodes`.

```python
import pigeonhole as ph

with ph.open(path, create=True) as db:
    g = db.table("g").family("out").create()
    with db.write_batch() as wb:
        for a, b in [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("d", "e")]:
            wb.put(g, f"n:{a}", "out", f"n:{b}", b"")
    t = g.traverse([b"n:a"], "out", hops=3, parents=True)
    assert [h.nodes for h in t.hops] == [[b"n:b", b"n:c"], [b"n:d"], [b"n:e"]]
```

## Readers, threads and processes
- **Processes:** one process opens a database as the writer (`ph.open`). Any number of others open it as readers (`ph.open_reader(path)`), which see every commit and can't write ([reader processes](readers.md)).
- **Threads:** every engine call releases the GIL, so threads in one process run reads and commits in parallel. Handles (`Database`, `Table`, `Snapshot`) are safe to share between threads.
- **`fork`:** a forked child can't use the handles it inherited. `multiprocessing`'s default start method on Linux before Python 3.14 is `fork`, so open databases inside each worker, or use the `spawn` or `forkserver` start method.

## Errors
Every error is a `pigeonhole.Error`, of a subclass named after its [`ErrorCode`](errors.md) (`TableNotFound`, `Conflict`, `ReadOnly`, `Closed`, ...), with `code` (the name) and `code_num` (the stable number). Catch the subclass you can act on. Bad arguments raise Python's own `TypeError`, `ValueError` and `OverflowError`.

```python
import pigeonhole as ph

with ph.open(path, create=True) as db:
    try:
        db.table("missing").open()
    except ph.TableNotFound as e:
        assert (e.code, e.code_num) == ("TableNotFound", 8)
```

## Performance
Each call costs a little Python on top of the Rust call: converting arguments and results, plus one GIL release. `crates/python/bench/overhead.py --rust` times each operation from Python and from Rust at the same options. On an M-series Mac it measured:

| Operation | Python | Rust |
|---|---|---|
| `get` (memtable hit) | 0.58 µs | 0.62 µs |
| `put` (`Durability.NONE`) | 3.4 µs | 3.8 µs |
| a 3-cell `mutate().commit()` | 3.5 µs | 3.1 µs |
| a scan, per row | 156 ns | 132 ns |
| `multi_get`, per key | 349 ns | 279 ns |

So the binding adds about 0.4 µs to a 3-cell mutation, 24 ns per scanned row and 70 ns per multi-get key. For single gets and puts, the difference is within run-to-run noise. Commits that sync are dominated by the sync either way. Batch writes in a `write_batch` or a mutation, and scan rather than calling `get` in a loop.

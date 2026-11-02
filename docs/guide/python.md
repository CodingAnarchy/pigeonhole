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

### Reads within a byte budget
For filling a context window by size rather than by row count (D308; see [Byte budgets](scans-and-filters.md#byte-budgets)):
- **`scan_within(prefix, bytes=...)`** returns a `ScanPage`: the longest prefix of the scan's cells whose size fits `bytes`. Each cell counts its qualifier, its value and 16 bytes, and each row its key once. A column is kept whole or not at all, and a row may be cut between columns. `limit` applies too: whichever comes first stops the read.
- **`multi_get_within(keys, bytes=...)`** returns a `KeysPage`, the same over a multi-get's rows. Unlike `multi_get`, its rows aren't aligned with `keys`: they come in key order, with no entry for a key that has no cell.
- **A page** has `rows`, `bytes`, `cut_by_budget`, `next_bytes` and `resume`.
  - `next_bytes` is the size of the first column that didn't fit, its row key included. A page resumed with that budget returns it.
  - Pass `after=page.resume` to read the next page.
  - A `ScanPosition` or `KeysPosition` has `to_bytes()` and `from_bytes()`, to hand to a client.
  - A position from another table, or of the other kind, raises `InvalidArgument`.
  - A chain of pages is one consistent read only with one `snapshot=`.
- **`nearest_within(family, query, k, bytes=...)`** returns a `NearestPage`: `nearest`'s neighbours that fit. Each counts its row key, its qualifier and 16 bytes. The budget bounds the result, not the work, and there's no resume position.
- **`traverse(..., byte_budget=n)`** stops the traversal at `n` bytes of nodes, and sets `cut_by_budget`.
  - Each node counts its key and 16 bytes, plus 4 with `parents=True`. The seeds aren't counted.
  - `None`, the default, sets no budget, and 0 is refused.

The budget is keyword-only (`bytes=`) in all three. Budgets and sizes are bytes, not characters.

```python
import pigeonhole as ph

with ph.open(path, create=True) as db:
    t = db.table("docs").family("text").create()
    for i in range(100):
        t.put(f"doc:{i:03}", "text", b"body", bytes(200))
    page = t.scan_within(b"doc:", bytes=2048)
    assert page.bytes <= 2048 and page.cut_by_budget
    token = page.resume.to_bytes()            # hand this to a client
    nxt = t.scan_within(b"doc:", bytes=2048, after=ph.ScanPosition.from_bytes(token))
    assert nxt.rows[0].key == b"doc:009"
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
- `Family.vector(dim, Element.F32 | I8 | BIT | F16 | BF16, Metric.L2 | COSINE | INNER_PRODUCT | HAMMING)`. `F16` and `BF16` (#1263) store each component in 2 bytes, under L2, inner product or cosine.
- `put_vector` takes a buffer or a plain sequence of the family's element type.
  - For `f32`: a NumPy `float32` array or `array.array('f')` is the fast path. NumPy's default `float64` works too, narrowed to `f32`.
  - For `i8`: an `int8` buffer.
  - For `f16` and `bf16`: what `f32` takes. Each component is rounded to nearest, ties to even, and a component that overflows the element is refused. A NumPy `float16` array works too, element by element, and its values convert back exactly.
  - Bits go in packed, as bytes. An `f32` or `i8` family refuses `bytes`, which in Python would otherwise read as one element per byte.
- `Cell.as_vector()` returns an `array.array` (`'f'` or `'b'`), which `numpy.frombuffer` wraps without a copy, or `bytes` for bits. An `f16` or `bf16` vector comes back widened exactly to `array.array('f')`, and `nearest` takes its query as `f32` numbers too.

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
Store a graph as adjacency rows, as in the [graphs guide](graphs.md): a row per node, and in an edge family one column per edge, its qualifier the destination's key. `traverse(seeds, family, hops=...)` runs a breadth-first search inside the engine. It returns each hop's new nodes, sorted, with `parents=True` for shortest paths, and takes `per_node_limit`, `hop_limit`, `max_nodes` and `byte_budget`.

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

## The change feed
Open the database with `change_feed=` (the history to keep, in bytes; `feed_age=` bounds it in time too); later opens that don't mention the feed keep it, and `change_feed_off=True` turns it off. Then `t.feed()` subscribes to a table's committed changes: whole commits, once each, in commit order. A commit arrives once it is durable at its level, so nothing delivered is ever undone (`visible=True` for sooner). Each `Commit` has `seqno`, `timestamp` and `changes`. Each `Change` has `table`, `family`, `kind` (`"put"`, `"merge"`, `"delete_cell"`, `"delete_column"`, `"delete_family"`), `row`, `qualifier`, `timestamp` and `value` (typed as reads type it, `None` for deletes).

`feed.next(timeout=...)` waits for the next commit with the GIL released, and returns `None` if none came in time. `try_next()` never waits, and `for commit in feed:` waits for each in turn. Store `feed.token()` with what the commits fed: `t.feed(resume=token)` continues there, after a reopen too. `tables=[...]` adds tables to the same feed, `prefix=` keeps only rows under it, and `from_snapshot=` starts after what a snapshot saw, so a reader can load a snapshot and then follow the changes without missing or repeating any. When the history a feed needs is gone (retention passed it, or an open with `change_feed_off=True` ended it), the feed raises `FeedExpired` from then on: rebuild from a snapshot and start again. A reader process's table (`ph.open_reader(path).table(...)`) subscribes too, following the running writer's feed. If that writer restarts, `next` raises `SnapshotExpired`: open a new feed with `resume=feed.token()`.

```python
import pigeonhole as ph

with ph.open(path, create=True, change_feed=64 << 20) as db:
    t = db.table("orders").family("f").create()
    feed = t.feed()
    t.put(b"order:1", "f", b"status", b"paid")
    commit = feed.next(timeout=5)
    (change,) = commit.changes
    assert (change.kind, change.row, change.value) == ("put", b"order:1", b"paid")
    token = feed.token()  # store it with what the commit fed
    feed.close()
    t.put(b"order:1", "f", b"status", b"shipped")
    with t.feed(resume=token) as feed:
        assert feed.next(timeout=5).changes[0].value == b"shipped"
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

# Python

> **Status:** this guide describes `main`. The bindings (`pigeonhole-db` on PyPI, `import pigeonhole`; D307, ICR 0102, #696) are new in 1.1.0. Every code block on this page runs as a test (`crates/python/tests/test_guide.py`).

The Python bindings are built with PyO3 directly over the Rust `pigeonhole` crate, so Python gets the same engine, file format and guarantees as Rust. Every engine call releases the GIL.

## Install
- **From PyPI:** `pip install pigeonhole-db`, for CPython 3.11 or later. One abi3 wheel per platform covers every Python version: Linux (manylinux and musllinux, x86_64 and aarch64), macOS (universal2) and Windows (x86_64).
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

### A scan as Arrow batches
**`scan(...).to_arrow()`** hands a scan's cells to pandas, Polars, DuckDB or pyarrow without a Python object per cell (#1480, ICR 0120). It returns an `ArrowStream`: Arrow batches with one Arrow row per cell, in the same schema as the Rust `pigeonhole-arrow` crate and `phdb dump --format arrow` (see [Arrow](arrow.md)).
- **The consumers:** an `ArrowStream` speaks the Arrow PyCapsule protocol, so Polars (`pl.DataFrame(stream)`), DuckDB (a query naming the variable) and pyarrow (`pa.RecordBatchReader.from_stream(stream)`) take it directly. `stream.to_pyarrow()` is the same reader in one call. For pandas, call `.read_pandas()` on that reader.
- **No new required dependency:** pyarrow is optional. `pip install pigeonhole-db[arrow]` adds it (18.0 or later), for `to_pyarrow()`.
- **The selection:** the batches hold the scan's cells: its prefix or range, `families`, `qualifier_prefix`, `versions` (0 for every version), `limit`, `column_limit` and `snapshot`.
- **The options:** `to_arrow`'s keyword arguments are the export's.
  - `batch_cells` (default 8192) and `batch_bytes` (default 8 MiB) close a batch, so memory stays bounded by one batch.
  - `typed_values=True` adds `value_i64` and `value_f64`; `vector_dim=n` adds `value_vector`.
  - `utf8_keys`, `row_dictionary`, and `ts="uint64"` change the key and timestamp types.
- **One consumer per scan:** `to_arrow()` takes the scan, so the iterator ends, and it works only on a scan not yet iterated. A stream is read once: a second consumer raises `ValueError`. Close a stream you never hand over (`with t.scan().to_arrow() as s:`, or `s.close()`), as with a scan, to release its snapshot.
- **Errors while reading:** `to_arrow()` raises pigeonhole's own exceptions for its arguments and for the scan. A read that fails later, mid-stream (the database closed under it, say), comes out of the consumer as the consumer's own exception, carrying pigeonhole's message. For pyarrow that's an `ArrowInvalid` reading `External error: the database is closed`, not a `pigeonhole` error class, so catch the consumer's exception type there.
- **Don't write while a stream is open on the same thread:** an open stream holds its scan's snapshot, like a live scan, so a commit from that thread can wait on room only that snapshot holds, and fail with `Busy` after the write-stall timeout. Read the stream to the end (or close it) first. See [Don't hold a snapshot across your own commit](transactions.md#dont-hold-a-snapshot-across-your-own-commit).
- **The GIL:** the stream reads without the GIL, but a consumer that keeps the GIL while it reads blocks other Python threads for that long.

```python
import pigeonhole as ph
import polars as pl
import pyarrow as pa

with ph.open(path, create=True) as db:
    t = db.table("events").family("e").create()
    with db.write_batch() as wb:
        for i in range(1000):
            wb.put(t, f"u{i % 10}:{i:04}", "e", "n", i)
    df = pl.DataFrame(t.scan(prefix="u3:").to_arrow(typed_values=True))
    assert df.height == 100 and df["value_i64"].sum() == sum(range(3, 1000, 10))

    reader = t.scan().to_arrow().to_pyarrow()    # a pyarrow.RecordBatchReader
    assert reader.read_all().num_rows == 1000
    assert isinstance(reader, pa.RecordBatchReader)
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
- **Increments that return the value (D314):** `mutate(row)....commit_returning()` commits like `commit` and returns an `Incremented`: each counter cell the mutation incremented or set, as a read at the commit sees it. Read one with `value(family, qualifier, ts=None)` or `r[family, qualifier]`; iterating gives the keys `(family, qualifier, ts)`. `Table.incr_returning(...)` is the one-cell form and returns the new value. Concurrent increments each get their own value (`N` callers adding 1 from zero get exactly `1..N`), so they serve sequence numbers and ID allocation. A value is `None` when a read at the commit sees no cell: a counter the mutation deleted (`delete(family, qualifier, ts=0)`), or a bucket already past the family's TTL. A failed commit raises its usual exception, which is safe to retry. If the commit applied but reading the value failed, you get `ph.ReturnedValueError` instead (with the commit's `seqno`, and the read's error as its cause): **the increment landed**, so don't retry it. Values are unique only for as long as the commit survives, so use `Durability.GROUP_SYNC` (the default) or `SYNC` for IDs; after `OutcomeUnknown` there is no value, and a retry counts twice if the first increment landed. The [Counters](data-modeling.md#counters) section's Durability and `OutcomeUnknown` notes cover both.
- **Values a commit replaced (#1445):** `return_previous(family, qualifier)` on a mutation asks `commit_returning()` for that column as it was just before the commit, read in the same step. `previous(family, qualifier)` on the result is its newest visible version then, as a `Cell`, or `None` if it had none.
  - **A lease handoff, for example:** one commit takes the lease and says whom it displaced. That's an **unconditional takeover**: it doesn't check that the lease was free or expired. Taking it only then needs a condition: `commit_if_returning`, next.
  - The displaced holder finds out at its next guarded write. **Guard every holder write** with `commit_if(Condition.equals(family, holder_column, own_id))`, which returns `None` once another holder has taken over.
  - Optionally, an `incr` of an epoch counter in the same commit gives the new holder a fencing token (`r.value(...)`), so systems downstream can refuse an older holder's work.
  - **The rules:** only `commit_returning()` (and `commit_returning_async()`) takes such a mutation: `commit()` and `commit_if()` raise `InvalidArgument`, as does a mutation that writes nothing. `previous` raises `InvalidArgument` for a column the mutation didn't ask for, and `ReturnedValueError` if the read failed after the commit applied, as `value` does. `Returned` is the same class as `Incremented`, under the name the Rust struct has had since 1.2.
- **Compare and set, then read (#1446):** `commit_if_returning(condition)` is a `commit_if` that returns the row whichever way the condition goes (D314 amendment 2). It returns a frozen `Checked`: `applied`, then either `returned` (a `Returned`) when it committed, or `seen` (a `Seen`) when it didn't and nothing was written. `commit_if_returning_async` is the asyncio form.
  - `returned.current(family, qualifier)` is a column as of the commit, so the condition's own column has the value this commit wrote. `previous` and the counters work as for `commit_returning`.
  - `seen.current(family, qualifier)` is the column as the condition saw it: the value it compared, or `None` where it was missing. A compare-and-swap loop retries from it with no extra read.
  - The condition's column is always reported. Ask for more with `return_current(family, qualifier)`. Only `commit_if_returning` takes it: `commit`, `commit_if` and `commit_returning` raise `InvalidArgument` ("use a transaction"), because a `get` after a commit may already see a later writer's. A mutation with no writes is a check-and-read.
  - `current` raises `InvalidArgument` for a column that neither `return_current` nor the condition named. On `returned` it raises `ReturnedValueError` if the read failed after the commit applied. [Compare and set, then read](data-modeling.md#compare-and-set-then-read-commit_if_returning) has the details.
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

```python
import pigeonhole as ph

with ph.open(path, create=True) as db:
    seq = db.table("seq").family("n", ph.Family.counter()).create()
    assert seq.incr_returning(b"orders", "n", b"next", 1) == 1
    r = seq.mutate(b"orders").incr("n", b"next", 1).incr("n", b"shipped", 1).commit_returning()
    assert r.value("n", b"next") == 2 and r["n", b"shipped"] == 1
    assert list(r) == [("n", b"next", 0), ("n", b"shipped", 0)]

    # A lease handoff: an unconditional takeover that says whom it displaced, with a
    # fencing epoch for the new holder.
    leases = db.table("leases").family("s").family("epoch", ph.Family.counter()).create()
    leases.mutate(b"lease:42").put("s", b"holder", b"worker-3").incr("epoch", b"n", 1).commit()
    r = (
        leases.mutate(b"lease:42")
        .put("s", b"holder", b"worker-7")
        .incr("epoch", b"n", 1)
        .return_previous("s", b"holder")
        .commit_returning()
    )
    assert r.previous("s", b"holder").value == b"worker-3"
    assert r.value("epoch", b"n") == 2  # worker-7's fencing token
    # worker-3 finds out at its next write, which (like every holder write) is guarded.
    mine = ph.Condition.equals("s", b"holder", b"worker-3")
    assert leases.mutate(b"lease:42").put("s", b"beat", b"3").commit_if(mine) is None

    # Compare and swap: take the next number, retrying from what a failed attempt saw.
    leases.mutate(b"seq").put("s", b"v", b"41").commit()
    seen = b"0"
    while True:
        n = int(seen)
        r = (
            leases.mutate(b"seq")
            .put("s", b"v", str(n + 1).encode())
            .commit_if_returning(ph.Condition.equals("s", b"v", seen))
        )
        if r.applied:
            break
        seen = r.seen.current("s", b"v").value
    assert n == 41
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

`feed.next(timeout=...)` waits for the next commit with the GIL released, and returns `None` if none came in time. `try_next()` never waits, and `for commit in feed:` waits for each in turn. Store `feed.token()` with what the commits fed: `t.feed(resume=token)` continues there, after a reopen too. `tables=[...]` adds tables to the same feed, `prefix=` keeps only rows under it, and `from_snapshot=` starts after what a snapshot saw, so a reader can load a snapshot and then follow the changes without missing or repeating any. When the history a feed needs is gone (retention passed it, or an open with `change_feed_off=True` ended it), the feed raises `FeedExpired` from then on: rebuild from a snapshot and start again. A reader process's table (`ph.open_reader(path).table(...)`) subscribes too, following the running writer's feed. If that writer restarts, the feed follows it. While no writer runs, `next` raises `NoWriter` (or `SnapshotExpired` while a new one starts): call `next` again on the same feed.

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

## Agent memory
`pigeonhole.memory` binds the [`pigeonhole-memory`](agent-memory.md) store: long-term memory for an agent, with ranked recall, dedup, superseding, forgetting and consolidation, kept in the same file as the application's data. The names and rules are the Rust crate's ([ICR 0107](../design/icr/0107-agent-memory.md)). Every call releases the GIL.

- **Opening.** `mem.Store.create(db, name, embedding=mem.EmbeddingModel(id, dim, "f32"))` pins the store to one embedding model. Without `embedding`, recall is by keyword and recency. `forget=`, `episodic_ttl=` and `near_dedup=` set the store's policies, and `normalization="nfkc"` (or `"nfc"`, the default, or `"none"`) the keyword index's Unicode normalisation, fixed at create. `store.normalization` reads it back ([Agent memory](agent-memory.md)). `mem.Store.open(db, name, model=id)` opens it again: `ModelMismatch` (an `InvalidArgument`) for another model, and `model=None` for a handle that recalls without embeddings. `mem.Store.open_reader(reader, name, model=...)` gives a `ReadStore` in a reader process, with `recall`, `get` and `verify`.
- **Writing.** `store.remember(ns, text, embedding=..., kind=..., importance=..., source=..., session=..., links=[(id, mem.LinkType.DERIVED_FROM, 0.5)], pinned=...)` returns `Remembered(id, outcome)`, where `outcome` is `"new"`, `"merged"` (an exact duplicate) or `"similar"` (a near duplicate the store's `NearDedup(policy="skip")` didn't write). `delete`, `link`, `supersede` and `mark_used` take ids as `MemoryId` or their text form.
- **Transactions.** `remember_in(txn, ...)`, `delete_in` and `forget_in` write in a `pigeonhole.Transaction` and commit with it. They read in the transaction, so they're safe inside `db.transact(fn)`, whose retries re-read. `remember_in` skips near dedup, whose check reads outside the transaction: call `store.near_duplicate(ns, text, embedding=...)` first if you need it. Its reads are at the transaction's snapshot, which doesn't see the transaction's own writes, so two `remember_in` of the same text in one transaction both write. `forget_in` returns `"deleted"`, `"protected"`, `"not_due"` or `"gone"`, and none of these is an error.
- **Recall.** `store.recall(namespaces, text=..., embedding=..., k=10, byte_budget=..., weights=mem.Weights(...), fusion="rrf", include_superseded=..., session=..., snapshot=..., with_signals=...)` returns `RecallHit`s, best first, at one snapshot. `recall_with_stats` adds `RecallStats` (`truncated` says the budget cut the hits). `expand=mem.Expand(hops=1)` also follows the best candidates' links both ways, and gives the memories it reaches a `graph` signal (`Weights(graph=)`, 0.5 by default). `RecallStats.expanded` counts them.
- **Async recall.** `await store.recall_async(...)` and `await store.recall_with_stats_async(...)` take `recall`'s keywords and return what it does, with the recall's reads asynchronous ([Agent memory](agent-memory.md)). `ReadStore` has both too. A recall's polls are CPU work, so it's never polled at the call: it starts on a small pool of poll threads (2 to 8, by the machine's parallelism; started by the first async recall, they live as long as the process), and commits, gets and feed waits keep the asyncio bridge's own thread. Many concurrent recalls then don't hold up the loop's commits. A row longer than a block in the recall's key-set reads is still read synchronously, on a pool thread ([#1652](https://github.com/CodingAnarchy/pigeonhole/issues/1652)).
- **Forgetting.** With a policy (`forget=mem.ForgetPolicy(...)` or `store.set_forget_policy(...)`), a thread of the crate's own forgets in the background. `store.forget(policy=..., after=...)` runs one pass now.
- **Consolidation.** `store.consolidate(ns, fn, policy=mem.ConsolidatePolicy(...))` calls `fn(group)`, a Python callable, for each group of episodic memories. It returns a list of `mem.Memory` to keep in their place (none keeps the group, marked consolidated).
  - **The callable runs outside every transaction, with no lock held,** so it may call the store (`recall` for context) and take its time, for example calling a model.
  - **A group that changes before its commit is skipped,** not retried: `skipped` counts it, and the next pass finds it again.
  - **An exception from `fn`,** `KeyboardInterrupt` included, or a return that isn't a list of `Memory`, stops the pass with `mem.ConsolidateError` (a `MergeFailed`). Its `consolidated` is the pass so far, whose groups stay committed, and its `__cause__` is the exception.
  - **Ctrl-C during a pass** arrives at `fn`'s next call, stopping the pass as above, or when the pass returns: Python handles signals only while it runs Python code.
- **Closing.** Close a store when you're done with it (`store.close()`, or `with`). Its forgetter thread stops once every handle to the store is closed. `db.close()` closes the stores opened on it first. After either, store calls raise `Closed`, and a call already running ends with `Closed`.

```python
import pigeonhole as ph
from pigeonhole import memory as mem

with ph.open(path, create=True) as db, mem.Store.create(
    db, "agents", embedding=mem.EmbeddingModel("toy", 3, "f32")
) as store:
    old = store.remember("user:dana", "Dana uses vim", embedding=[1.0, 0.0, 0.0]).id
    new = store.remember("user:dana", "Dana switched to helix", embedding=[0.9, 0.1, 0.0])
    store.supersede(old, new.id)
    (hit,) = store.recall("user:dana", text="Dana editor helix", k=1)
    assert hit.id == new.id
    store.mark_used([hit.id])

    for note in ["Dana opened the design doc", "Dana commented on the design doc"]:
        store.remember("user:dana", note, embedding=[0.0, 1.0, 0.0], session="mon")

    def summarise(group):
        text = "; ".join(m.text for m in group)
        return [mem.Memory(f"Monday: {text}", embedding=[0.0, 1.0, 0.0], kind="semantic")]

    passed = store.consolidate(
        "user:dana", summarise, policy=mem.ConsolidatePolicy(min_age=0)
    )
    assert passed.committed == 1
```

## asyncio
The calls that wait have native `asyncio` forms (#1479, ICR 0121), so a coroutine awaiting a commit or a feed holds no thread. This replaces `asyncio.to_thread(...)` around the sync calls.
- **The `_async` forms:**
  - `Mutation.commit_async()`, `commit_if_async(condition)` and `commit_returning_async()`;
  - `Table.incr_returning_async(...)`;
  - `WriteBatch.commit_async()` and `Transaction.commit_async()`, both with `durability=`;
  - `Transaction.get_async(...)`;
  - `Feed.next_async()`;
  - `Database.transact_async(f)`.
- **What they return:** the same results and errors as the sync forms (`Conflict`, `ReturnedValueError` and the rest).
  - Each but `transact_async` returns an `asyncio.Future` of the loop it was called on. Call and await it on that loop.
  - It works with `await`, `asyncio.gather`, `wait_for`, `shield` and `asyncio.timeout`. It isn't a coroutine, so wrap it in `asyncio.ensure_future` rather than `create_task`.
  - Called with no running loop, it raises `RuntimeError` and spends nothing.
- **`transact_async(f)` takes an `async def f(txn)`,** retried on `Conflict` and `Busy` exactly as `transact` retries a sync body (the same attempts, backoff and deadline), sleeping with `asyncio.sleep`.
  - Read with `await txn.get_async(...)`, and await reads before committing.
  - A sync body is refused with `TypeError`, as is an `async def` passed to `transact`.
  - `txn.get_row` has no async form yet, so it can block the loop on a cache miss.
- **`Feed.next_async()`** waits for the next commit. For a timeout, use `asyncio.wait_for(feed.next_async(), t)`.
  - A timeout or a cancel loses nothing: the commit it would have returned is the next one any call returns, and `feed.token()` still points before it.
  - One wait at a time: a second `next_async`, or a sync `next`, during one raises `ValueError`, and so does a `next_async` while another thread's sync `next` waits.
- **Cancelling a commit:**
  - Cancelled while it waits for room (an async commit waits when its shard has too many in flight), nothing is written.
  - Once it's submitted, it may still land, and the task can't tell which happened. To keep the outcome, await `asyncio.shield(m.commit_async())`.
- **How it runs:** one `pigeonhole-asyncio` thread per process hands results to the event loops. The engine's threads never take the GIL to do it. Any asyncio loop works, uvloop included.
- **The memory store (#1479's second PR):**
  - `Store.remember_async`, `delete_async`, `link_async`, `supersede_async` and `mark_used_async` take the sync forms' arguments and return their results. Each commit is awaited, and retried as `transact` retries.
  - Their reads inside the transaction can block the loop on a cache miss.
  - `remember_async`'s near-dedup lookup (on a store with near dedup) and an IVF store's settle run on a worker thread.
  - `Store.watch(...)` gives a `Watch` of the store's memory events, with `next`, `try_next` and `next_async`, under the same rules as `Feed.next_async`. `next_async` works in the writer process only: a reader's watch (`ReadStore.watch`) raises `Unsupported` there, so use `next` on a thread of its own.
  - `Store.consolidate_loop(...)` gives a `ConsolidateLoop`. Its `run_once_async` awaits new memories without holding a thread, and a timeout loses no events. It runs the due passes, and so your consolidator, on a worker thread. It returns after the first commit with memory events, where the sync `run_once` reads until its timeout, so call it in a loop.
  - Recall isn't async yet (#1560).
- **An awaited write is a commit:** finish or drop snapshots, scans and Arrow streams before you await a write. A `remember_async`'s settle may commit splits too. Holding a snapshot across them is [Don't hold a snapshot across your own commit](transactions.md#dont-hold-a-snapshot-across-your-own-commit) with the event loop as the thread: the write waits for room only your snapshot holds, and ends in `Busy`.
- **Not yet async:** reads outside a transaction, and memory recall (#1560). Call them through `asyncio.to_thread` meanwhile.

```python
import asyncio

import pigeonhole as ph


async def main(db, t):
    seqno = await t.mutate(b"u1").put("f", b"name", b"Ada").commit_async()
    n = await t.incr_returning_async(b"u1", "n", b"logins", 1)

    async def rename(txn):
        cell = await txn.get_async(t, b"u1", "f", b"name")
        txn.put(t, b"u1", "f", b"name", cell.value + b" L.")

    await db.transact_async(rename)
    many = await asyncio.gather(
        *[t.mutate(f"k{i}").put("f", b"q", b"v").commit_async() for i in range(100)]
    )
    return seqno, n, len(many)


with ph.open(path, create=True) as db:
    t = db.table("users").family("f").family("n", ph.Family.counter()).create()
    seqno, n, many = asyncio.run(main(db, t))
    assert n == 1 and many == 100
    assert t.get(b"u1", "f", b"name").value == b"Ada L."
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

# pigeonhole-db

Python bindings for [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole): an embedded, single-file, wide-column store, with BigTable's data model and SQLite's deployment model. The bindings are built with PyO3 directly over the Rust crate, and every engine call releases the GIL.

```python
import pigeonhole as ph

with ph.open("app.phdb", create=True) as db:
    users = (db.table("users")
               .family("profile")
               .family("stats", ph.Family.counter())
               .create_if_missing())

    users.mutate(b"u1").put("profile", b"name", "Ada").incr("stats", b"logins", 1).commit()
    print(users.get(b"u1", "profile", b"name").value)        # b'Ada'

    for row in users.scan(prefix=b"u"):
        for family, qualifier, cell in row:
            print(row.key, family, qualifier, cell.typed)

    def bump(txn):                                           # retried on Conflict
        n = txn.get(users, b"u1", "stats", b"logins").as_int()
        txn.put(users, b"u1", "profile", b"logins_seen", n)
    db.transact(bump)
```

- **Requirements:** CPython 3.11 or later. One abi3 wheel per platform: Linux (manylinux and musllinux, x86_64 and aarch64), macOS (universal2) and Windows (x86_64).
- **Typed:** the package ships stubs and `py.typed`.
- **Processes:** one writer process per database, and any number of readers through `ph.open_reader(path)`.
- **`fork`:** a forked child can't use handles it inherited. Open databases inside each `multiprocessing` worker, or use the `spawn` or `forkserver` start method.

**Status:** pre-release (#696). It isn't on PyPI yet.

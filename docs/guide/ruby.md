# Ruby

> **Status:** this guide describes `main`. The Ruby bindings are new in 1.1.0 (#697, ICR 0111), and nothing has been published to RubyGems yet. This is PR 1: the core API. Vectors, graphs, byte-budgeted reads and the change feed follow, as do the memory, text and vector-index surfaces.

The `pigeonhole` gem binds the Rust crate directly through magnus and rb-sys (no C layer), as the [Python bindings](python.md) do through PyO3. It needs Ruby 3.3 or later.

## Install
Until the gem is published, build it from a checkout (Rust and Bundler needed):

```sh
cd crates/ruby
bundle install
bundle exec rake compile
ruby -Ilib -e 'require "pigeonhole"; p Pigeonhole::VERSION'
```

## A tour
```ruby
require "pigeonhole"

Pigeonhole.open(path, create: true) do |db|
  users = db.table("users").family("profile").family("hits", Pigeonhole::Family.counter).create_if_missing
  users.put("user:ada", "profile", "name", "Ada Lovelace")
  users.incr("user:ada", "hits", "visits", 1)

  cell = users.get("user:ada", "profile", "name")
  raise unless cell.value == "Ada Lovelace" && cell.value.encoding == Encoding::BINARY
  raise unless users.get("user:ada", "hits", "visits").as_int == 1

  names = users.scan("user:").map { |row| row.get("profile", "name").value }
  raise unless names == ["Ada Lovelace"]
end
```

With a block, `Pigeonhole.open` closes the database however the block exits. Without one, it returns the `Database`, and you call `close`.

## Values and types
- **Keys, qualifiers and byte values are Strings.** A String's bytes are written as they are, whatever its encoding. Reads return binary (`ASCII-8BIT`) Strings, so call `force_encoding("UTF-8")` on text you stored as UTF-8.
- **Cell types follow the Ruby class:** `Integer` is an `i64` cell (`RangeError` past it), `Float` is an `f64` cell, and `String` is bytes. `true`, `false` and `nil` are refused with `TypeError`. `put_u64` writes an unsigned 64-bit cell.
- **Reading a cell:** `Cell#typed` returns the value as its type. `as_int`, `as_u64` and `as_float` read one type (or return `nil`). The aggregate readers are `hll_estimate`, `sum_f64`, `mean_var`, `histogram_counts`, `quantile(q)` and `bloom_contains(x)`.
- **Durability** is a Symbol: `:none`, `:buffered`, `:group_sync` (the default) or `:sync`.
- **Durations** (`ttl:`, `deadline:`) are seconds, as an Integer or a Float.

## Reads
- **One cell:** `table.get(row, family, qualifier, snapshot: nil)` returns a `Cell` or `nil`.
- **One row:** `table.row(row, families:, qualifier_prefix:, versions:, column_limit:, snapshot:)` returns a `Row` or `nil`. `Row` is `Enumerable` over `[family, qualifier, cell]`, with `key`, `get(family, qualifier)`, `cells` and `size`.
- **A scan:** `table.scan(prefix, start:, end:, families:, qualifier_prefix:, versions:, limit:, column_limit:, snapshot:)` returns an `Enumerator` of `Row`s in key order. So `each`, `map`, `first(n)` and `lazy` all work.
  - Rows are read in chunks with the GVL released.
  - Each enumeration reads at one snapshot (the one you pass, or one taken when it starts), so its rows are consistent however slowly you consume them.
  - Enumerating again (or `rewind`) sees newer writes.
- **Snapshots:** `db.snapshot { |s| ... }` releases the snapshot when the block exits. Pass it as `snapshot:` to read several things at one point in time.

```ruby
require "pigeonhole"

Pigeonhole.open(path, create: true) do |db|
  t = db.table("events").family("f").create
  3.times { |i| t.put("event:#{i}", "f", "n", i) }
  db.snapshot do |snap|
    t.put("event:9", "f", "n", 9)  # after the snapshot
    raise unless t.scan("event:", snapshot: snap).count == 3
  end
  raise unless t.scan("event:").count == 4
  raise unless t.scan(start: "event:1", end: "event:9").map(&:key) == ["event:1", "event:2"]
end
```

## Writes, batches and transactions
- **One-commit writes:** `table.put`, `table.incr` and `table.delete` each commit at once and return the commit's sequence number.
- **`table.mutate(row)`** collects one row's writes for a single commit: `put`, `put_u64`, `incr`, `aggregate` and `delete`, then `commit`, or `commit_if(Pigeonhole::Condition.absent(family, qualifier))`, which returns `nil` when the condition fails.
- **`db.write_batch { |batch| ... }`** commits every write in the block at once (the same write methods, with a table and row first). It commits when the block returns normally (`next` included), and discards on an exception, a `break`, a `throw` or an early `return`.
- **`db.transaction { |txn| ... }`** is an optimistic transaction: `txn.get` and `txn.get_row` read at its snapshot, and the commit checks them. Like `write_batch`, it commits on a normal return and rolls back otherwise. A transaction belongs to one thread of control. It's safe to share between threads (calls serialise), but its reads and writes must make sense in whatever order they land.
- **`db.transact(max_attempts:, deadline:, durability:) { |txn| ... }`** runs the block in a transaction and commits it, retrying on `Conflict` and `Busy` with backoff, as the Rust `RetryPolicy` does. It returns the block's value. The block may run more than once, so it should have no other effects.

```ruby
require "pigeonhole"

Pigeonhole.open(path, create: true) do |db|
  acct = db.table("accounts").family("f").create
  acct.put("alice", "f", "balance", 10)
  acct.put("bob", "f", "balance", 0)

  moved = db.transact do |txn|
    a = txn.get(acct, "alice", "f", "balance").as_int
    b = txn.get(acct, "bob", "f", "balance").as_int
    amount = [a, 5].min
    txn.put(acct, "alice", "f", "balance", a - amount)
    txn.put(acct, "bob", "f", "balance", b + amount)
    amount
  end
  raise unless moved == 5 && acct.get("bob", "f", "balance").as_int == 5

  db.write_batch do |batch|
    batch.put(acct, "carol", "f", "balance", 1)
    raise "keep nothing"
  rescue RuntimeError
    nil  # rescued inside the block: the block returns normally, so the batch commits
  end
  raise unless acct.get("carol", "f", "balance")

  ok = acct.mutate("dave").put("f", "balance", 3).commit_if(Pigeonhole::Condition.absent("f", "balance"))
  raise unless ok
end
```

## Tables and families
- **`db.table(name)`** returns a `TableBuilder`. Add families with `family(name, options = nil)`, then call `create`, `create_if_missing` or `open`.
- **`Pigeonhole::Family.new(max_versions:, ttl:, bloom_bits:, block_size:, blob_threshold:, compression:, zstd_level:)`** sets a family's options. `Family.counter` makes a counter family, and `Family.aggregate(kind, ...)` an aggregate one (`:sum`, `:hll`, `:histogram`, ...).
- **`table.family_info(name)`** reads a family's stored options. Its Symbol-valued ones (`aggregation`, `compression`, `cache_priority`, `compaction`) come back as Symbols, `counter?` is a predicate, and `ttl` is seconds or `nil`.

## Readers, threads and processes
- **Processes:** one process opens a database as the writer (`Pigeonhole.open`). Others open it as readers (`Pigeonhole.open_reader(path)`), which see every commit and can't write: a reader's table raises `Pigeonhole::ReadOnly` on writes ([reader processes](readers.md)).
- **Threads:** every engine call releases the GVL, so Ruby threads run reads and commits in parallel. The handles are safe to share between threads.
- **Interrupts:** an engine call can't be interrupted midway. A `Thread#raise`, `Thread#kill` or Ctrl-C that arrives during a call lands when the call returns, and a commit that ran stays committed. Calls are bounded, so the wait is short.
- **Forking servers (Puma, Unicorn, Pitchfork with `preload_app`):**
  - A forked child can't use a handle it inherited: every call raises `Pigeonhole::Unsupported`.
  - Dropping an inherited handle in the child, or the child exiting, leaves the parent's database alone, and never releases the parent's writer lock.
  - Open the database in each worker (`on_worker_boot`, or `before_fork` closing it in the parent if the workers need to write), and remember only one process can be the writer.

## Errors
Every engine error is a `Pigeonhole::Error`, of a subclass named after its [`ErrorCode`](errors.md) (`Pigeonhole::TableNotFound`, `Pigeonhole::Conflict`, `Pigeonhole::ReadOnly`, `Pigeonhole::Closed`, ...), with `code` (the name, a String) and `code_num` (the stable number). Bad arguments raise Ruby's own `ArgumentError`, `TypeError` and `RangeError`. Using a mutation, batch or transaction after its commit or rollback raises a plain `Pigeonhole::Error` with no code.

```ruby
require "pigeonhole"

Pigeonhole.open(path, create: true) do |db|
  begin
    db.table("missing").open
    raise "unreachable"
  rescue Pigeonhole::TableNotFound => e
    raise unless e.code == "TableNotFound" && e.code_num == 8
  end
end
```

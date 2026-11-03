# Ruby

> **Status:** this guide describes `main`. The Ruby bindings are new in 1.1.0 (#697, ICR 0111): the core API, vectors, graphs, byte budgets, the change feed, agent memory, full-text search and the vector index. Nothing has been published to RubyGems yet.

The `pigeonhole` gem binds the Rust crate directly through magnus and rb-sys (no C layer), as the [Python bindings](python.md) do through PyO3. It needs Ruby 3.3 or later.

## Install
Until the gem is published, build it from a checkout (Rust and Bundler needed):

```sh
cd crates/ruby
bundle install
bundle exec rake compile
ruby -Ilib -e 'require "pigeonhole"; p Pigeonhole::VERSION'
```

`bundle exec rake gem:source` builds the source gem, `pkg/source/pigeonhole-<version>.gem`. It carries the engine's crates, so it installs anywhere with Rust 1.96 or later and network access to crates.io: `gem install pkg/source/pigeonhole-*.gem` compiles the extension, with the versions `Cargo.lock` pins. The build is an optimised release build, about a minute on a recent laptop.

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
- **Many rows:** `table.multi_get(keys, families:, qualifier_prefix:, versions:, column_limit:, snapshot:)` reads the rows of many keys in one read. The result is aligned with `keys`: one entry per key, in the order given, with `nil` for a key with no row. `keys` is an Array, or another Enumerable such as a `Set`.
- **A wide row in pages:** `table.pages(row, size, ...)` returns a `Pages`, `Enumerable` over pages of at most `size` cells each (a column's versions are never split). Each page is read when it's asked for, and no read stays open between pages.
  - `position` is where the last page ended. Its `to_bytes` resumes the walk later, also in another process: `table.pages(row, size, after: Pigeonhole::RowPosition.from_bytes(token))`.
  - **A walk is a cursor, not a collection:** every Enumerable method consumes the pages it reads. After `walk.first`, `walk.to_a` holds only the pages after the first, and a second `each` yields only what's left. Call `table.pages` again to read the row again.
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

### Reads within a byte budget
For filling a context window by size rather than by row count (D308; see [Byte budgets](scans-and-filters.md#byte-budgets)):
- **`scan_within(prefix, bytes:, ...)`** returns a `ScanPage`: the longest prefix of the scan's cells whose size fits `bytes`.
  - Each cell counts its qualifier, its value and 16 bytes, and each row its key once.
  - A column is kept whole or not at all, and a row may be cut between columns.
  - `limit:` applies too: whichever comes first stops the read.
- **`multi_get_within(keys, bytes:, ...)`** returns a `KeysPage`, the same over a multi-get's rows. Unlike `multi_get`, its rows aren't aligned with `keys`: they come in key order, with no entry for a key that has no cell.
- **A page** is a frozen `Data` with `rows`, `bytes`, `cut_by_budget`, `next_bytes` and `resume`.
  - `next_bytes` is the size of the first column that didn't fit, its row key included. A page resumed with that budget returns it.
  - Pass `after: page.resume` to read the next page. `resume` is `nil` once the read is exhausted.
  - A `ScanPosition` or `KeysPosition` has `to_bytes` and `.from_bytes`, to hand to a client. A position from another table, or of the other kind, raises `InvalidArgument`.
  - A chain of pages is one consistent read only with one `snapshot:`.
- **`nearest_within(family, query, k, bytes:)`** returns a `NearestPage`: `nearest`'s neighbours that fit. Each counts its row key, its qualifier and 16 bytes. The budget bounds the result, not the work, and there's no resume position.
- **`traverse(..., byte_budget: n)`** stops the traversal at `n` bytes of nodes, and sets `cut_by_budget`.
  - Each node counts its key and 16 bytes, plus 4 with `parents: true`. The seeds aren't counted.
  - `nil`, the default, sets no budget, and 0 is refused.

Budgets and sizes are bytes, not characters.

```ruby
require "pigeonhole"

Pigeonhole.open(path, create: true) do |db|
  t = db.table("docs").family("text").create
  100.times { |i| t.put(format("doc:%03d", i), "text", "body", "\0" * 200) }
  page = t.scan_within("doc:", bytes: 2048)
  raise unless page.bytes <= 2048 && page.cut_by_budget
  token = page.resume.to_bytes # hand this to a client
  nxt = t.scan_within("doc:", bytes: 2048, after: Pigeonhole::ScanPosition.from_bytes(token))
  raise unless nxt.rows.first.key == "doc:009"
end
```

## Vectors
A vector family stores fixed-size vectors (D303):
- **The family:** `Family.vector(dim, element, metric)`, with `element` one of `:f32`, `:i8`, `:bit`, `:f16` and `:bf16`, and `metric` one of `:l2`, `:cosine`, `:inner_product` and `:hamming`. `:f16` and `:bf16` (#1263) store each component in 2 bytes, under L2, inner product or cosine. `FamilyInfo#vector` returns its `VectorSpec` (`dim`, `element`, `metric`).
- **Writes:** `put_vector` (on `Mutation`, and with the table and row first on `WriteBatch` and `Transaction`) takes the family's form:
  - `:f32`, `:f16` and `:bf16`: an Array of Numerics. A half family rounds each component to nearest, ties to even, and refuses one that overflows.
  - `:i8`: an Array of Integers in -128..127.
  - `:bit`: the packed bits, as a String.

  A String is refused for a numeric family, and an Array for a bit family: neither is read as the other. The family checks the dimension at commit.
- **Reads:** `Cell#as_vector` (and `Cell#typed`) returns an Array of Floats (`:f16` and `:bf16` widened exactly), an Array of Integers, or the bits as a binary String.

`nearest(family, query, k)` finds the `k` cells nearest `query` by the family's metric. It searches `prefix:`, a `start:`/`end:` range, the whole table, or the rows of `keys:`. It returns `Neighbour`s (`row`, `qualifier`, `distance`, `ts`; frozen `Data`) nearest first. The search is exact: every vector in the read is scored, and `parallel: n` shares the work across threads.

```ruby
require "pigeonhole"

Pigeonhole.open(path, create: true) do |db|
  docs = db.table("docs")
           .family("text")
           .family("emb", Pigeonhole::Family.vector(4, :f32, :cosine))
           .create
  docs.mutate("d1").put("text", "body", "hello").put_vector("emb", "v", [0.5, 0.25, 0.125, 1.0]).commit
  raise unless docs.get("d1", "emb", "v").as_vector == [0.5, 0.25, 0.125, 1.0]
  docs.mutate("d2").put_vector("emb", "v", [1.0, 0.125, 0.25, 0.5]).commit
  best = docs.nearest("emb", [0.5, 0.25, 0.125, 0.9], 1)
  raise unless best.first.row == "d1"
end
```

## Graphs
Store a graph as adjacency rows, as in the [graphs guide](graphs.md): a row per node, and in an edge family one column per edge, its qualifier the destination's key. `traverse(seeds, family, hops:)` runs a breadth-first search inside the engine, with the GVL released.
- **The result** is a `Traversal` with each hop's new nodes, sorted (`hops`, each a `Hop` with `nodes`), and `parents` with `parents: true`, for shortest paths.
- **Limits:** it takes `per_node_limit:`, `hop_limit:`, `max_nodes:` and `byte_budget:`.

```ruby
require "pigeonhole"

Pigeonhole.open(path, create: true) do |db|
  g = db.table("g").family("out").create
  db.write_batch do |wb|
    [%w[a b], %w[a c], %w[b d], %w[c d], %w[d e]].each { |a, b| wb.put(g, "n:#{a}", "out", "n:#{b}", "") }
  end
  t = g.traverse(["n:a"], "out", hops: 3, parents: true)
  raise unless t.hops.map(&:nodes) == [%w[n:b n:c], ["n:d"], ["n:e"]]
end
```

## The change feed
Open the database with `change_feed:` (the history to keep, in bytes; `feed_age:` bounds it in time too). Later opens that don't mention the feed keep it, and `change_feed_off: true` turns it off. Then `table.feed` subscribes to a table's committed changes: whole commits, once each, in commit order.
- **Delivery:** a commit arrives once it is durable at its level, so nothing delivered is ever undone (`visible: true` for sooner).
- **What arrives:** each `Commit` has `seqno`, `timestamp` and `changes`. Each `Change` has `table`, `family`, `kind` (`:put`, `:merge`, `:delete_cell`, `:delete_column`, `:delete_family`), `row`, `qualifier`, `timestamp` and `value` (typed as reads type it, `nil` for deletes). Both are frozen `Data`.
- **Waiting:** `feed.next(timeout: s)` waits for the next commit with the GVL released, and returns `nil` if none came in time. `try_next` never waits. `each` (and so `first(n)`) waits for each commit in turn; `each(timeout: s)` stops once one doesn't come in time.
- **A feed has no end.** Like `Pages`, a feed is a cursor, and `Enumerable` methods that read to the end (`to_a`, `map`, `count`, `select`) wait for ever on a live feed. `first(n)`, `take(n)` and `find` return. To drain what's there now, use `feed.each(timeout: 1).to_a`.
- **Kinds:** a change's `kind` is `:put`, `:merge`, `:delete_cell`, `:delete_column` or `:delete_family`, or `:unknown` for a kind a later engine adds. Give a `case` on kinds an `else`.
  - A wait runs in 100 ms slices with an interrupt check between them, so `Thread#raise`, `Timeout.timeout` and Ctrl-C reach a consumer blocked in `next`.
- **Resuming:** store `feed.token` with what the commits fed. `table.feed(resume: token)` continues there, after a reopen too.
- **Choosing changes:** `tables: [...]` adds tables to the same feed, `prefix:` keeps only rows under it, and `from_snapshot:` starts after what a snapshot saw. So a consumer can load a snapshot and then follow the changes without missing or repeating any.
- **Expiry:** when the history a feed needs is gone (retention passed it, or an open with `change_feed_off: true` ended it), the feed raises `FeedExpired` from then on. Rebuild from a snapshot and start again.
- **Readers:** a reader process's table (`Pigeonhole.open_reader(path).table(...)`) subscribes too, following the running writer's feed. If that writer restarts, the feed follows it. While no writer runs, `next` raises `NoWriter` (or `SnapshotExpired` while a new one starts): call `next` again on the same feed.
- **Blocks:** with a block, `table.feed` yields the feed and closes it afterwards. `close` ends a subscription early.

```ruby
require "pigeonhole"

Pigeonhole.open(path, create: true, change_feed: 64 << 20) do |db|
  t = db.table("orders").family("f").create
  token = t.feed do |feed|
    t.put("order:1", "f", "status", "paid")
    change = feed.next(timeout: 5).changes.first
    raise unless [change.kind, change.row, change.value] == [:put, "order:1", "paid"]
    feed.token # store it with what the commit fed
  end
  t.put("order:1", "f", "status", "shipped")
  t.feed(resume: token) do |feed|
    raise unless feed.next(timeout: 5).changes.first.value == "shipped"
  end
end
```

## Agent memory
`Pigeonhole::Memory` binds the [`pigeonhole-memory`](agent-memory.md) store: long-term memory for an agent, with ranked recall, dedup, superseding, forgetting and consolidation, kept in the same file as the application's data. The names and rules are the Rust crate's ([ICR 0107](../design/icr/0107-agent-memory.md)), as Python's `pigeonhole.memory` binds them. Every call releases the GVL.

- **Opening.**
  - `Store.create(db, name, embedding: EmbeddingModel.new(id, dim, :f32))` pins the store to one embedding model. Without `embedding:`, recall is by keyword and recency.
  - `forget:`, `episodic_ttl:` (seconds) and `near_dedup:` set the store's policies. `normalization:` (`:nfc` by default, `:nfkc` or `:none`) sets how its text index compares terms, and is fixed at create.
  - `Store.open(db, name, model: id)` opens the store again. It raises `ModelMismatch` (an `InvalidArgument`) for another model, and `model: nil` gives a handle that recalls without embeddings.
  - `Store.open_reader(reader, name, model: ...)` gives a `ReadStore` in a reader process, with `recall`, `get` and `verify`.
  - Each takes a block, which gets the store and closes it afterwards.
- **Writing.**
  - `store.remember(ns, text, embedding:, kind:, importance:, source:, session:, links: [[id, LinkType::DERIVED_FROM, 0.5]], pinned:)` returns `Remembered(id, outcome)`.
  - `outcome` is `:new`, `:merged` (an exact duplicate) or `:similar` (a near duplicate the store's `NearDedup` with policy `:skip` didn't write).
  - `delete`, `link`, `supersede` and `mark_used` take ids as a `MemoryId` or its text form.
- **Transactions.**
  - `remember_in(txn, ...)`, `delete_in` and `forget_in` write in a `Pigeonhole::Transaction` and commit with it.
  - They read in the transaction, so they're safe inside `db.transact { |txn| ... }`, whose retries re-read.
  - `remember_in` skips near dedup: call `store.near_duplicate(ns, text, embedding:)` first if you need it. Its reads are at the transaction's snapshot, so two `remember_in` of the same text in one transaction both write.
  - `forget_in` returns `:deleted`, `:protected`, `:not_due` or `:gone`, and none of these is an error.
- **Recall.**
  - `store.recall(namespaces, text:, embedding:, k:, byte_budget:, weights: Weights.new(...), fusion: :rrf, include_superseded:, session:, snapshot:, with_signals:)` returns `RecallHit`s, best first, at one snapshot.
  - `recall_with_stats` returns `[hits, RecallStats]`. Its `truncated` says the budget cut the hits.
  - `expand: Expand.new(1)` also follows the best candidates' links both ways and gives the memories it reaches a `graph` signal (`Weights.new(graph:)`, 0.5 by default). `RecallStats#expanded` counts them.
- **IVF for large namespaces** (D311 T8). `Store.create(..., ivf: IvfOptions.new(min_namespace_len: 10_000))` keeps a vector index of the store's embeddings, and it needs `embedding:`. A namespace with at least `min_namespace_len` memories with an embedding recalls through it, and a smaller one by the exact scan. `recall(exact: true)` forces the exact scan, and `nprobe:` sets how many lists the index probes. `RecallStats#ivf_namespaces` and `#ivf_fallbacks` say which path ran. `store.ivf` reads the options back.
- **Forgetting.** With a policy (`forget: ForgetPolicy.new(...)`, or `store.forget_policy = ...`), a thread of the crate's own forgets in the background. `store.forget(policy:, after:)` runs one pass now.
- **Consolidation.** `store.consolidate(ns, policy: ConsolidatePolicy.new(...)) { |group| ... }` runs a pass. The block gets each group of episodic memories (an Array of `StoredMemory`) and returns an Array of `Memory` to keep in their place. An empty Array keeps the group, marked consolidated.
  - **The pass runs on a thread of its own, and the block on yours,** outside every transaction and with no lock held. So the block may call the store (`recall` for context) and take its time, for example calling a model.
  - **A group that changes before its commit is skipped,** not retried: `skipped` counts it, and the next pass finds it again.
  - **A `StandardError` from the block,** or a return that isn't an Array of `Memory`, stops the pass with `ConsolidateError` (a `MergeFailed`). Its `consolidated` is the pass so far, and its `cause` is the exception.
  - **Anything else stops the pass too, then takes effect as it is:** `break`, `throw` or `return` out of the block, another exception (`Interrupt`, `SystemExit`), or an interrupt (`Thread#raise`, `Timeout`, Ctrl-C). This differs from Python, where every exception becomes `ConsolidateError`: here it's whatever `rescue => e` would catch.
  - **However the pass stops,** its thread has ended, the groups committed before stay committed, and running the pass again is safe.
- **Watching** (T9). `store.watch(namespaces, from_snapshot:, resume:, visible:)` returns a `Watch` of the store's memory events through the change feed. The feed must be on (`change_feed:` at open) and follow the store's table (`mem_NAME`).
  - `next(timeout:)` returns a `MemoryCommit` of `MemoryEvent`s, each with a `kind` of `:remembered`, `:summarized`, `:superseded` (`by` is the newer memory), `:deleted` or `:updated`.
  - `token` gives a position for a later `resume:`. A reader process's `ReadStore#watch` follows the writer.
  - Like `Feed`, a watch is a cursor: a live watch has no end.
- **A consolidation loop** (T9). `store.consolidate_loop(name, policy:) { |group| ... }` returns a `ConsolidateLoop`, whose `run_once(timeout)` reads new memories and runs a pass with the block for each namespace once its memories are `min_age` old. It returns a `LoopStep`.
  - **Position:** the loop saves where it is in the store under `name`, so a restart resumes it.
  - **Failures:** a `StandardError` from the block fails that namespace's pass. It's reported in `LoopStep#failed` and retried after `min_age`. An interrupt (`Timeout`, Ctrl-C) or another exception during a step stops it. The groups the step still had are refused, and their namespaces are retried after `min_age` too, since the loop counts a refused group as a failed one.
  - **One loop per name:** a newer loop takes the name over, and the older one's next step raises `LoopTakenOver` (a `Conflict`). Stop the older loop then: don't retry it or recreate it.
  - **Clock:** `clock_offset:` shifts the loop's clock for tests.
- **Closing.** Close a store when you're done with it (`store.close`, or a block form). Its forgetter thread stops once every handle to the store is closed. `db.close` closes the stores opened on it first. After either, store calls raise `Closed`, and a call already running ends with `Closed`.

```ruby
mem = Pigeonhole::Memory
Pigeonhole.open(path, create: true) do |db|
  mem::Store.create(db, "agents", embedding: mem::EmbeddingModel.new("toy", 3, :f32)) do |store|
    old = store.remember("user:dana", "Dana uses vim", embedding: [1.0, 0, 0]).id
    new = store.remember("user:dana", "Dana switched to helix", embedding: [0.9, 0.1, 0])
    store.supersede(old, new.id)
    hit = store.recall("user:dana", text: "Dana editor helix", k: 1).first
    raise "recall" unless hit.id == new.id

    store.mark_used([hit.id])
    ["Dana opened the design doc", "Dana commented on the design doc"].each do |note|
      store.remember("user:dana", note, embedding: [0, 1.0, 0], session: "mon")
    end
    passed = store.consolidate("user:dana", policy: mem::ConsolidatePolicy.new(min_age: 0)) do |group|
      [mem::Memory.new("Monday: #{group.map(&:text).join('; ')}", embedding: [0, 1.0, 0], kind: :semantic)]
    end
    raise "consolidate" unless passed.committed == 1
  end
end
```

## Full-text search
`Pigeonhole::Text` binds the [`pigeonhole-text`](text-search.md) index: BM25 ranking over one cell of a table's rows, with phrases, fuzzy terms and highlights ([ICR 0105](../design/icr/0105-text-index.md)). Every call releases the GVL.

- **Creating.** `Text::Index.create(db, name, table:, family:, qualifier:)` indexes the cell `family`/`qualifier` of `table`'s rows, including the rows already there. Its options are fixed at create:
  - `positions:` keeps word positions, which phrases need;
  - `normalization:` is `:nfc` (the default), `:nfkc` or `:none`;
  - `k1:` and `b:` tune BM25;
  - `stop_words:`, `max_terms:` and `max_term_bytes:` bound what's indexed.

  `Text::Index.open(db, name)` opens the index again, and `open_reader(reader, name)` opens a `ReadIndex` in a reader process.
- **Writing.** `index.put(id, text)` writes the source cell and indexes it, in one transaction. `put_in(txn, id, text)` does the same inside your own transaction, beside your other writes. `delete` and `delete_in` remove a document.
- **Searching.** `index.search(query, k)` returns `Hit`s, best first. The query is a String in the query syntax (`"march meeting"~2 postgres~`), parsed when the search runs. Or build it as `Text::Query.new.term("postgres").phrase(%w[march meeting], slop: 2).fuzzy("postgress", :auto)`, and use `Text::Query.parse(str)` to check a string first.
  - `id_prefix:` keeps only document ids that start with it, and `snapshot:` searches at a snapshot.
  - `highlight: true` fills each hit's `text` and `matches`.
  - `search_with_stats` also says how each fuzzy term expanded.
- **Highlights are byte ranges.** A `Match`'s `range` counts bytes of `hit.text`, as the engine gives it. Take it with `hit.text.byteslice(m.range)`: `hit.text[m.range]` counts characters, and goes wrong after the first non-ASCII one.
- **Maintenance.** `verify` checks the index against its source (`consistent?`), and `rebuild` and `repair` fix what it finds.
- **No custom tokenizers.** The engine calls a tokenizer inside its own calls, where a Ruby block can't run, so Ruby indexes use the built-in tokenizer.

```ruby
Pigeonhole.open(path, create: true) do |db|
  notes = db.table("notes").family("body").create
  index = Pigeonhole::Text::Index.create(db, "notes_text", table: notes, family: "body", qualifier: "text")
  db.transact do |txn|
    txn.put(notes, "note:1", "body", "author", "ada")
    index.put_in(txn, "note:1", "We chose Postgres at the café meeting")
  end
  hit = index.search("café postgres", 1, highlight: true).first
  raise "search" unless hit.id == "note:1"
  raise "highlight" unless hit.matches.map { |m| hit.text.byteslice(m.range) }.include?("café")
end
```

## Vector index
`Pigeonhole::Vector` binds the [`pigeonhole-vector`](vectors.md) index: an IVF index of vectors by id, for nearest-neighbour search over many vectors ([ICR 0100](../design/icr/0100-vector-index.md)). Every call releases the GVL.

- **Creating.** `Vector::Index.create(db, name, VectorSpec.new(dim, element, metric))` creates the index. Its options are `nprobe:`, `target_list_len:`, `reassign_neighbours:` and `rebalance:`. `open(db, name)` opens it again, and `open_reader(reader, name)` opens a `ReadIndex` in a reader process.
- **Writing.** `index.insert(id, vector)` takes the forms `put_vector` takes for the index's element: an Array of Numerics (`:f32`, and `:f16`/`:bf16`, which the index rounds), an Array of Integers (`:i8`), or the packed bits as a String (`:bit`). `delete(id)` removes one.
- **Bulk loads.** `index.bulk_load(pairs, expected:)` loads an Enumerable of `[id, vector]` into an empty index, faster than inserting one by one. The pairs go to the index `batch:` at a time (4,096 by default), so a lazy enumerator is never held whole. If enumerating raises, the batches sent before it stay loaded.
- **In your own transaction.** `insert_in(txn, placed, id, vector)` and `delete_in(txn, placed, id)` write the index inside a `Pigeonhole::Transaction`, so the vector commits or rolls back with your other writes.
  - `placed` is a `Vector::Placed`, the transaction's record of its index writes, so a second write of one id acts on the first.
  - **Make it inside the transaction's block,** so a retried attempt starts with a fresh one, and use one per index.
  - Once the transaction commits, `index.after_commit(placed)` splits or merges what the writes left out of bounds.
- **Searching.** `index.search(query, k)` returns `Hit(id, distance)`s, nearest first.
  - `nprobe:` sets how many lists to probe, `parallel:` how many threads read them, and `snapshot:` searches at a snapshot.
  - `id_prefix:` keeps only ids that start with it. The search widens until it finds `k`, probing at most `max_probe:` lists.
  - `search_with_stats` adds `lists_probed`. A prefixed search with fewer than `k` hits and `lists_probed < lists` stopped at `max_probe`.
- **Rebalancing.** With `rebalance: :inline`, the default, a write splits or merges a list before it returns. With `rebalance: :background`, a thread of the crate's own does it. Add `rebalancer: true` to `create` or `open`, and you get `[index, rebalancer]` back, to drive from a Ruby thread instead: `rebalancer.run_once(budget)` works for up to `budget` seconds, and `rebalancer.wait(timeout)` waits for work. Both release the GVL and stay interruptible.

```ruby
Pigeonhole.open(path, create: true) do |db|
  index = Pigeonhole::Vector::Index.create(db, "docs_vec", Pigeonhole::VectorSpec.new(2, :f32, :l2))
  index.bulk_load((0...100).lazy.map { |i| ["doc:#{i}", [i.to_f, 0.0]] }, expected: 100)
  docs = db.table("docs").family("f").create
  placed = db.transact do |txn|
    p = Pigeonhole::Vector::Placed.new
    txn.put(docs, "doc:new", "f", "", "a new document")
    index.insert_in(txn, p, "doc:new", [41.6, 0.0])
    p
  end
  index.after_commit(placed)
  raise "search" unless index.search([41.5, 0.0], 2).map(&:id).sort == ["doc:41", "doc:new"]
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
Every engine error is a `Pigeonhole::Error`, of a subclass named after its [`ErrorCode`](errors.md) (`Pigeonhole::TableNotFound`, `Pigeonhole::Conflict`, `Pigeonhole::ReadOnly`, `Pigeonhole::Closed`, ...), with `code` (the name, a String) and `code_num` (the stable number). Bad arguments raise Ruby's own `ArgumentError`, `TypeError` and `RangeError`. Using a mutation, batch or transaction after its commit or rollback raises `Pigeonhole::SpentError` (an `Error` with no code).

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

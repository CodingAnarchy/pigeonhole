# 0111: the Ruby API (the `pigeonhole-db` gem, `require "pigeonhole"`)

**Status:** Approved (number from the coordinator, 2026-11-03).
- The owner's decisions on #697 (2026-11-03): magnus and rb-sys directly over the Rust `pigeonhole` crate (D250; the 2026-10-31 note), mirroring the Python crate (D307, ICR 0102).
  - **O1:** the RubyGems name is `pigeonhole-db` (amended by the owner on 2026-11-04: RubyGems refused `pigeonhole` as too similar to `pigeon_hole`, and `pigeonhole-db` matches the PyPI name). The require path stays `"pigeonhole"` and the module stays `Pigeonhole`.
  - **O2:** Ruby 3.3 is the floor.
  - **O3:** `gvl.rs` is the crate's single hand-written `unsafe` exception.
- The design is cli692's comment on #697 (c6595164923), its O3 amendment, and simp724's G1 and R1–R4, all adopted.

## The change
A new crate, `crates/ruby` (`pigeonhole-ruby`, `publish = false` on crates.io), builds the native extension `pigeonhole/pigeonhole_native` with magnus 0.9 and rb-sys. The gem's files sit beside it:
- `pigeonhole-db.gemspec`, `Gemfile` and `Rakefile` (rake-compiler);
- `extconf.rb` (rb_sys's `create_rust_makefile`);
- `lib/pigeonhole.rb` (the pure-Ruby parts);
- `sig/pigeonhole.rbs`;
- `test/` (Minitest).

The gem's version is the crate's.

**A Cargo workspace of its own.** The root workspace excludes `crates/ruby` (`exclude`), since building it needs a Ruby with headers (rb-sys) and `cargo --workspace` mustn't require one. It depends on `pigeonhole` by path, keeps its own `Cargo.lock`, and copies the root's package fields, lints and release profile. `ruby.yml` runs its clippy (`-D warnings`), rustdoc, fmt and `cargo deny` (its own lockfile, the root's `deny.toml`), which the root's `--workspace` jobs don't reach.

**The snapshot** is `sig/pigeonhole.rbs`. CI validates it (`rbs validate`), and a test checks that every public method of every class has a declaration and the reverse. So a PR that changes the Ruby API shows it in the signatures' diff, as `__init__.pyi` does for Python.

### PR 1: the core API
ICR 0102's PR 1, one to one, in Ruby's idioms:
- **Opening:**
  - `Pigeonhole.open(path, create:, shards:, durability:, block_cache:, row_cache:, memtable_budget:, shm_dir:, read_only_fallback:)` returns a `Pigeonhole::Database`;
  - `Pigeonhole.open_reader(path, shm_dir:, block_cache:, row_cache:)` returns a `Pigeonhole::Reader`;
  - with a block, each yields the handle and closes it afterwards.
- **`Database`:** `table`, `tables`, `drop_table`, `snapshot`, `write_batch`, `transaction`, `transact` (retries `Conflict` and `Busy` by `RetryPolicy`), `flush`, `compact` and `close`. **`Reader`:** `table`, `tables`, `snapshot` and `close`.
- **Tables:** `TableBuilder` (`family`, `create`, `create_if_missing`, `open`), and `Table` (`get`, `row`, `scan`, `mutate`, `put`, `incr`, `delete`, `families`, `family_info`). A reader's table raises `ReadOnly` on writes.
- **Writes:** `Mutation`, `WriteBatch` and `Transaction`, with `put`, `put_u64`, `incr`, `aggregate` and `delete`.
  - `Mutation` adds `durability`, `commit` and `commit_if` (`nil` when the condition fails).
  - `WriteBatch` adds `commit(durability:)` and `size`.
  - `Transaction` adds `get`, `get_row`, `commit` and `rollback`.
- **Results and options:**
  - `Cell` with the typed readers, `Row` (`Enumerable` over `[family, qualifier, cell]`), `Snapshot`, and `FamilyInfo`;
  - `Family.new`, `Family.counter` and `Family.aggregate`, and `Condition.absent`, `.exists` and `.equals`.
- **Ruby's idioms:**
  - **Blocks:** `open`, `snapshot`, `write_batch` (commit on a normal exit, discard otherwise), `transaction` (commit on a normal exit, roll back otherwise) and `transact`.
  - **Scans:** `scan` returns an `Enumerator` over chunks read without the GVL.
  - **Bytes:** byte values are `String`s. Reads return binary (`ASCII-8BIT`) Strings, and writes take a String's bytes as they are.
  - **Values:** `Integer` is `i64` (`RangeError` past it), `Float` is `f64`, and `true`, `false` and `nil` are refused (`TypeError`).
  - **Durability** is a Symbol (`:none`, `:buffered`, `:group_sync`, `:sync`), and durations are Numeric seconds.
- **Errors:**
  - `Pigeonhole::Error < StandardError`, with one subclass per `ErrorCode`, named as the code. Each carries `code` (the name) and `code_num`.
  - Bad arguments raise Ruby's own `ArgumentError`, `TypeError` and `RangeError`.
  - A `Mutation`, `WriteBatch` or `Transaction` used after its commit, discard or rollback raises `Pigeonhole::SpentError` (an `Error` with a `nil` code; ai1249's P4). Python uses `ValueError` for this.
  - A panic inside an engine call is a `Pigeonhole::Error` with a `nil` code, not magnus's unrescuable `fatal` (simp724's A2).
  - `Pigeonhole::Native::ERROR_CODES` is the binding's name → number table, which a test checks against the `ErrorCode` enum (ai1249's P5).
- **Where Ruby differs from Python's spellings:**
  - `FamilyInfo#counter?` is a predicate.
  - `aggregation`, `compression`, `cache_priority` and `compaction` return Symbols (`:fifo_by_time`).
  - Inputs take a Symbol or a String where Python takes a string literal.
  - `ttl` is Float seconds or `nil`.
  - Table and family names take a String or a Symbol; keys, qualifiers and values must be Strings.
  - `TableBuilder#family` adds to the builder and returns it.
  - Block forms return the block's value.
  - `Database#closed?` and `Reader#closed?` exist.

### PR 2: vectors and the 1.1 reads
ICR 0102's PR 2 and PR 3 (Python's `nearest`) and D308's byte budgets, in Ruby's idioms:
- **Vectors (D303):**
  - `VectorSpec.new(dim, element, metric)`, with `dim`, `element`, `metric`, `==`/`eql?`/`hash`; `Family.vector(dim, element, metric, max_versions:, ttl:)`; `FamilyInfo#vector`.
  - Elements are `:f32`, `:i8`, `:bit`, `:f16` and `:bf16`, and metrics `:l2`, `:cosine`, `:inner_product` and `:hamming` (a Symbol or a String).
  - `put_vector(family, qualifier, vector, ts:)` on `Mutation`, and with the table and row first on `WriteBatch` and `Transaction`. The family's element type decides the form: an Array of Numerics for `:f32`, `:f16` and `:bf16` (narrowed to `f32`; a half family rounds at the write), an Array of Integers in -128..127 for `:i8`, the packed bits as a String for `:bit`. A String for a numeric family, or an Array for a bit family, is a `TypeError`: Ruby has no typed-buffer protocol that would tell bytes and numbers apart, so neither form is read as the other (ICR 0102's S2, in Ruby).
  - `Cell#as_vector`, and `Cell#typed` for a vector cell: an Array of Floats (`:f16` and `:bf16` widened exactly), an Array of Integers, or a binary String of bits.
- **Reads:**
  - `Table#multi_get(keys, families:, qualifier_prefix:, versions:, column_limit:, snapshot:)` returns an Array aligned with `keys`: one entry per key given, in that order, `nil` for a miss, a repeated key repeated. The read itself is one sorted multi-get (D299).
  - `Table#traverse(seeds, family, hops:, per_node_limit:, hop_limit:, max_nodes:, byte_budget:, parents:, qualifier_prefix:, snapshot:, parallel:)` returns a `Traversal` (D304): `seeds` (sorted and deduplicated: hop 1's parents index them), `hops` (each a `Hop` with `nodes`, `parents` and `size`), `rows`, `edges`, `hops_cut`, `cut_by_max_nodes` and `cut_by_budget`.
  - `Table#pages(row, size, families:, qualifier_prefix:, versions:, snapshot:, after:)` returns a `Pages` (D297): `Enumerable` over `Row` pages, each read with the GVL released when it is asked for and no read held open between pages, with `position`. A walk is one cursor: a second `each` continues where the first stopped. `RowPosition` has `to_bytes`, `.from_bytes`, `==`, `eql?` and `hash`.
  - `Table#nearest(family, query, k, prefix:, start:, end:, keys:, qualifier_prefix:, snapshot:, parallel:)` returns `Neighbour`s (`row`, `qualifier`, `distance`, `ts`) nearest first (D296). `query` is read as `put_vector` reads a vector.
- **Byte budgets (D308):**
  - `Table#scan_within(prefix, bytes:, ...)` (the scan's arguments plus `after:`) returns a `ScanPage`; `Table#multi_get_within(keys, bytes:, ...)` (`multi_get`'s plus `after:`) returns a `KeysPage`. A page has `rows`, `bytes`, `cut_by_budget`, `next_bytes`, `resume` and `size`.
  - `ScanPosition` (with `row`) and `KeysPosition` (with `key`) have `to_bytes`, `.from_bytes`, `==`, `eql?` and `hash`. A position of the other kind, or from another table, raises `InvalidArgument`.
  - `Table#nearest_within(family, query, k, bytes:, ...)` returns a `NearestPage` (`neighbours`, `bytes`, `next_bytes`, `cut_by_budget`, `size`); `traverse(byte_budget:)` sets `cut_by_budget`, and 0 is an `ArgumentError`.
- **Where Ruby differs from Python's spellings:**
  - The result objects with no engine object behind them (`Neighbour`, `NearestPage`, `Hop`, `Traversal`, `ScanPage`, `KeysPage`) are frozen `Data` classes, so they compare by value and have `to_h` and `with`. The native side returns Arrays that the Ruby layer wraps, so no wrapped Rust struct holds a Ruby value (and none needs a GC mark function).
  - `keys` and `seeds` take an Array or any other `Enumerable` (a `Set`), where Python takes any sequence.
  - The positions and `Pages` are native classes: they hold engine values.

### PR 2b: the change feed
Split from PR 2 for review size; Python's change feed (D310) in Ruby's idioms:
- **Opening:** `Pigeonhole.open` adds `change_feed:` (the history's size in bytes), `change_feed_off:`, `feed_age:` (seconds) and `feed_tables:`, with Python's rules: an open that names neither inherits the last open's feed, `feed_age:` needs `change_feed:`, and `change_feed_off:` with `change_feed:` is an `ArgumentError`.
- **`Table#feed(tables:, prefix:, visible:, from_snapshot:, from_oldest:, resume:)`** returns a `Feed`; with a block, it yields the feed and closes it afterwards. At most one of the three starts; a reader process's table feeds from the writer's history.
- **`Feed`:** `next(timeout:)`, `try_next`, `each(timeout:)` (`Enumerable`), `token` (a binary String), `oldest_retained`, `close` and `closed?`. A closed feed raises `Closed`.
- **`Commit`** (`seqno`, `timestamp`, `changes`, `size`) and **`Change`** (`table`, `family`, `kind`, `row`, `qualifier`, `timestamp`, `value`) are frozen `Data`. `kind` is a Symbol, and `value` is typed as `Cell#typed` types it.
- **Interrupts:** a wait releases the GVL in 100 ms slices and calls `thread_check_ints` between them, as Python's checks for signals, so `Thread#raise`, `Timeout` and Ctrl-C end a blocked `next` within a slice. One engine call still isn't interruptible (O3); a slice bounds how long one runs.

## The rules
- **The GVL (O3, G1).** Every engine call runs through `gvl::without_gvl`, the crate's only hand-written `unsafe`:
  - The bounds `F: FnOnce() -> R + Send` and `R: Send` keep every Ruby value (all `!Send` in magnus) out of code that runs without the GVL.
  - It calls `rb_thread_call_without_gvl2`, not the non-`2` form, which could `longjmp` over Rust frames when an interrupt is pending. It uses a `ran` flag: a call that ran returns its result, and an interrupt is handled after the method returns, so a commit's outcome is never lost. An interrupt pending before the call starts is raised through magnus's safe `thread_check_ints`, and nothing runs.
  - No unblocking function is passed, so a long engine call isn't interruptible midway. The guide says so.
  - A masked interrupt (`Thread.handle_interrupt(... => :never)`) makes the `2` form decline while `thread_check_ints` raises nothing, so after a second try the function runs once with the GVL held, rather than looping (simp724's A1).
  - Panics are caught in the trampoline, so none unwinds through Ruby's C frames, and come back as an uncoded `Pigeonhole::Error` once the GVL is back (simp724's A2: resuming one would make magnus raise the unrescuable `fatal`).
- **Unsafe.** `#![deny(unsafe_code)]`. Besides `gvl.rs`, the only `#[allow(unsafe_code)]` sites are the items magnus's macros expand (`#[magnus::init]`, `#[magnus::wrap]`).
- **Callbacks into Ruby** (later PRs' consolidators) use a channel, never a GVL re-acquire from Rust (the O3 amendment). The work runs on a Rust thread, and the calling Ruby thread waits through `without_gvl(recv)`, then runs the block with the GVL held.
- **Fork (R1).** A forked child can't use inherited handles (the engine's `Unsupported`). A handle records its creating process, and dropping or closing it in another process neither closes the database nor releases the parent's writer lock.
- **Non-local exits (R2).** The block forms are Ruby `begin`/`ensure` over the native calls, so `break`, `throw` and an early `return` clean up in Ruby and never unwind through Rust: a batch is discarded, a transaction rolled back, a snapshot released, a database closed. `next` is a normal return of the block in Ruby, indistinguishable from reaching its end, so it commits.
- **Scans (R3).** Each enumeration reads at one snapshot: the given one, or one it takes when it starts.
- **Transactions (R4).** A `Transaction` keeps its engine transaction behind a mutex, as Python's does, so calls from several threads serialise. A call after `commit` or `rollback` raises.

## CI and packaging
- **`ruby.yml`:** the extension compiled, the Minitest suite (once plainly and once under `rbs test`'s runtime checking against `sig/pigeonhole.rbs`) and `rbs validate`, on Ruby 3.3 on Linux, macOS and Windows, and Ruby 4.0 on Linux and macOS.
- **`ruby-gems.yml`:** precompiled native gems (oxidize-rb's `cross-gem` over `rb-sys-dock`) for x86_64 and aarch64 Linux (gnu and musl), x86_64 and arm64 macOS, and `x64-mingw-ucrt` Windows, for Ruby 3.3, 3.4 and 4.0, uploaded as artifacts only.
- **The source gem**, for platforms without a native gem: `rake gem:source` (`rakelib/source_gem.rake`) stages the gem in `pkg/source/` with the engine's path crates vendored under `vendor/`, as maturin's sdist does for Python. The design and both reviews are on #697.
  - The vendored set is the closure of the declared normal and build path dependencies, optional ones included.
  - Each crate is its tracked files, without `tests/`, `benches/`, `examples/` and `fuzz/`. Its manifest stands alone: the workspace inheritance is resolved from the root `Cargo.toml` (`edition` and `rust-version` stated explicitly), path dependencies point at the vendored copies, and every dev-dependency table and explicit target is dropped. Of `[lints]`, only `unexpected_cfgs` and its check-cfg list are kept, at "warn".
  - Staging fails on a path dependency outside `vendor/`, and on an `include!`, `include_str!`, `include_bytes!` or `#[path]` target the copy doesn't carry (outside `#[cfg(doctest)]` modules). `crates/ruby/Cargo.lock` is copied unchanged and checked with `cargo metadata --locked`. `extconf.rb` builds with `--locked`, so a user gets the versions CI tested.
  - `ruby-gems.yml`'s `source` job installs the gem from a copy outside the checkout into an empty `GEM_HOME` on Linux and macOS, runs `script/source_gem_smoke.rb`, and uploads the gem as an artifact. A plain `gem build` from the checkout still refuses.
- **Publishing:** nothing goes to RubyGems without the owner's go.

### PR 3a: agent memory
`pigeonhole-memory` as Python's `pigeonhole.memory` binds it (ICR 0102/0107), in Ruby's idioms. The design note and reviews are on #697.
- **Names.** Everything is under `Pigeonhole::Memory`: `Store`, `ReadStore`, `MemoryId`, the option types (`EmbeddingModel`, `Weights`, `Expand`, `ForgetPolicy`, `ConsolidatePolicy`, `NearDedup`, `Memory`) and `LinkType`. The errors are `ModelMismatch < InvalidArgument` and `ConsolidateError < MergeFailed`, still in the base hierarchy (ai1249's Q1).
- **Values.** `MemoryId` and the option types are native values that hold no Ruby object. Every result is a frozen `Data`, built in `memory.rb` from Arrays, as PR 2's are. Its Strings and Arrays are frozen too: `StoredMemory`, `RecallHit`, `Signals`, `Signal`, `RecallStats`, `ForgetPass`, `ConsolidatePass`, `Remembered`, `Marked`, `Link`, `Source` and `StoreStats`. Enums are Symbols, with `:unknown` for a kind a later engine adds. Durations are Numeric seconds, and points in time are µs Integers.
- **`Store.create(..., normalization:)`** takes #1381's setting from the start.
- **Lifecycle.** Each store handle sits in a slot that `close` empties. A call clones the handle and holds no lock while it runs (perf2's C4), so `close` during a call returns at once, and the call ends with its clone. `Database#close` and `Reader#close` close the stores opened on them first: the list and the handle are taken under one lock, so a store opened during the close is either closed by it or refused with `Closed`.
- **The consolidator** follows the owner's O3, the channel pattern.
  - The pass runs on a Rust thread that holds no Ruby value. For each group, that thread sends the group's owned memories over a channel and waits for the answer.
  - The Ruby thread waits with the GVL released, in 100 ms slices with `thread_check_ints` between them. It calls the block with the GVL held, through a Proc argument and not a block, so an exception the method builds isn't handed the block. It sends the block's memories back.
  - **A `StandardError` from the block**, or a return that isn't an Array of `Memory`, becomes `ConsolidateError`, whose `consolidated` is the pass so far and whose `cause` is the exception.
  - **Anything else** takes effect as it is, once the worker has stopped and been joined: `break`, `throw`, `return`, a non-`StandardError` exception, or an interrupt while waiting. That covers perf2's C1, and `Timeout` and `Thread#raise` keep their meaning. It differs from Python, which wraps every exception.
  - **Whichever way the pass stops,** no thread outlives the call, the committed groups stand, and a rerun is safe (C5).
- **`test_signatures`** now walks nested modules, so `Pigeonhole::Memory::*` is held to the RBS like the rest.

### PR 3b: full-text search and the vector index
`pigeonhole-text` and `pigeonhole-vector`, which no other binding has yet. The design note and both reviews are on #697.
- **Names.** `Pigeonhole::Text` holds `Query`, `Index` and `ReadIndex`. `Pigeonhole::Vector` holds `Index`, `ReadIndex`, `Placed`, `Rebalancer` and `BulkLoad`. Their results (`Hit`, `Match`, the stats and options) are frozen `Data` built in `index.rb`, with ids as frozen binary Strings.
- **Text.**
  - A query is a `Query` (immutable: each builder method returns a new one) or a String in the query syntax, parsed when the search runs.
  - `Match#range` is a byte `Range` of `Hit#text`, and the docs say to use `byteslice` (ai1249's P3).
  - Custom tokenizers aren't bound (Q2): the engine calls a tokenizer inside its own calls, where a block can't run.
- **Vector.**
  - `insert` and `search` take `put_vector`'s forms for the index's element. A half index takes `f32` values, which it rounds.
  - `insert_in`/`delete_in` take a `Placed`, made inside the transaction's block (P1), and `after_commit` uses it up.
  - **`bulk_load(pairs, expected:)`** streams (P2). `pairs.each_slice(batch)` feeds a `BulkLoad`, whose `push` converts a batch and sends it over a bounded channel to a loader thread running the crate's one `bulk_load_*` call. `push` waits with the GVL released while the loader is two batches behind, and `finish` joins the loader in interruptible slices.
- **T8 (#1424) in `Pigeonhole::Memory`:**
  - `IvfOptions` and `Store.create(ivf:)`, with `Store#ivf`/`ReadStore#ivf`;
  - `recall(exact:, nprobe:)`;
  - `RecallStats#ivf_namespaces`/`#ivf_fallbacks`;
  - `StoreStats#ivf_mismatched`/`#ivf_counter_drift`, which `consistent?` leaves out, as the crate does (ai1249's N3 on #1425). From #1427, `consistent?` counts `ivf_mismatched`, as the crate now does; `ivf_counter_drift` stays out.
- **Rebalancing (Q3).** `rebalance: :background` uses the crate's own thread. With `rebalancer: true`, `create`/`open` return `[index, rebalancer]` for a Ruby thread to drive. `run_once(budget)` and `wait(timeout)` release the GVL, and `wait` runs in 100 ms slices with interrupt checks. `set_wakeup` isn't bound, for Q2's reason.
### PR 3c: watching memory and the consolidation loop
T9 (#1430, #1432) in `Pigeonhole::Memory`.
- **`Store#watch`/`ReadStore#watch(namespaces, from_snapshot:, resume:, visible:)`** returns a `Watch`. Its `next(timeout:)`, `try_next`, `each(timeout:)`, `token`, `oldest_retained`, `close` and `closed?` are built as `Feed`'s are: slices, and 3b's `Fair` lock for give-way. `MemoryCommit`/`MemoryEvent` are frozen `Data`, and `kind` is a Symbol (`:unknown` for a later kind). `StoredMemory#summary?` reads `meta:summary`.
- **`Store#consolidate_loop(name, policy:, namespaces:, clock_offset:) { |group| ... }`** returns a `ConsolidateLoop`.
  - The crate's loop is owned (#1432's B2). Each `run_once(timeout)` moves it onto a worker thread, and its consolidator is answered with the block by the channel pattern. That uses the `drive` that `consolidate` now shares, so every path joins the worker.
  - The block is kept in a Ruby instance variable and passed to each step, so Rust holds no Ruby value.
  - A `StandardError` from the block fails that group's namespace only, reported in `LoopStep#failed` with the block's exception. A jump or interrupt stops the step and raises once the worker has returned the loop. `break` raises `LocalJumpError`, since the block outlives the call that took it.
  - `LoopTakenOver < Conflict` carries the crate's don't-retry rule.
  - A Ruby clock can't run inside a step, so `clock_offset:` (seconds) shifts the system clock instead.

## Callers changed
None. It's a new crate.

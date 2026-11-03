# 0111: the Ruby API (the `pigeonhole` gem, `require "pigeonhole"`)

**Status:** Approved (number from the coordinator, 2026-11-03).
- The owner's decisions on #697 (2026-11-03): magnus and rb-sys directly over the Rust `pigeonhole` crate (D250; the 2026-10-31 note), mirroring the Python crate (D307, ICR 0102).
  - **O1:** the RubyGems name is `pigeonhole`.
  - **O2:** Ruby 3.3 is the floor.
  - **O3:** `gvl.rs` is the crate's single hand-written `unsafe` exception.
- The design is cli692's comment on #697 (c6595164923), its O3 amendment, and simp724's G1 and R1–R4, all adopted.

## The change
A new workspace crate, `crates/ruby` (`pigeonhole-ruby`, `publish = false` on crates.io), builds the native extension `pigeonhole/pigeonhole_native` with magnus 0.9 and rb-sys. The gem's files sit beside it:
- `pigeonhole.gemspec`, `Gemfile` and `Rakefile` (rake-compiler);
- `extconf.rb` (rb_sys's `create_rust_makefile`);
- `lib/pigeonhole.rb` (the pure-Ruby parts);
- `sig/pigeonhole.rbs`;
- `test/` (Minitest).

The gem's version is the crate's.

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
  - A `Mutation`, `WriteBatch` or `Transaction` used after its commit, discard or rollback raises a plain `Pigeonhole::Error` with a `nil` code. Python uses `ValueError` for this.
- **Where Ruby differs from Python's spellings:**
  - `FamilyInfo#counter?` is a predicate.
  - `aggregation`, `compression`, `cache_priority` and `compaction` return Symbols (`:fifo_by_time`).
  - Inputs take a Symbol or a String where Python takes a string literal.
  - `ttl` is Float seconds or `nil`.
  - Table and family names take a String or a Symbol; keys, qualifiers and values must be Strings.
  - `TableBuilder#family` adds to the builder and returns it.
  - Block forms return the block's value.
  - `Database#closed?` and `Reader#closed?` exist.

## The rules
- **The GVL (O3, G1).** Every engine call runs through `gvl::without_gvl`, the crate's only hand-written `unsafe`:
  - The bounds `F: FnOnce() -> R + Send` and `R: Send` keep every Ruby value (all `!Send` in magnus) out of code that runs without the GVL.
  - It calls `rb_thread_call_without_gvl2`, not the non-`2` form, which could `longjmp` over Rust frames when an interrupt is pending. It uses a `ran` flag: a call that ran returns its result, and an interrupt is handled after the method returns, so a commit's outcome is never lost. An interrupt pending before the call starts is raised through magnus's safe `thread_check_ints`, and nothing runs.
  - No unblocking function is passed, so a long engine call isn't interruptible midway. The guide says so.
  - Panics are caught in the trampoline and resumed after the GVL is back.
- **Unsafe.** `#![deny(unsafe_code)]`. Besides `gvl.rs`, the only `#[allow(unsafe_code)]` sites are the items magnus's macros expand (`#[magnus::init]`, `#[magnus::wrap]`).
- **Callbacks into Ruby** (later PRs' consolidators) use a channel, never a GVL re-acquire from Rust (the O3 amendment). The work runs on a Rust thread, and the calling Ruby thread waits through `without_gvl(recv)`, then runs the block with the GVL held.
- **Fork (R1).** A forked child can't use inherited handles (the engine's `Unsupported`). A handle records its creating process, and dropping or closing it in another process neither closes the database nor releases the parent's writer lock.
- **Non-local exits (R2).** The block forms are Ruby `begin`/`ensure` over the native calls, so `break`, `throw` and an early `return` clean up in Ruby and never unwind through Rust: a batch is discarded, a transaction rolled back, a snapshot released, a database closed. `next` is a normal return of the block in Ruby, indistinguishable from reaching its end, so it commits.
- **Scans (R3).** Each enumeration reads at one snapshot: the given one, or one it takes when it starts.
- **Transactions (R4).** A `Transaction` keeps its engine transaction behind a mutex, as Python's does, so calls from several threads serialise. A call after `commit` or `rollback` raises.

## CI and packaging
- **`ruby.yml`:** the extension compiled, the Minitest suite (once plainly and once under `rbs test`'s runtime checking against `sig/pigeonhole.rbs`) and `rbs validate`, on Ruby 3.3 on Linux, macOS and Windows, and Ruby 4.0 on Linux and macOS.
- **`ruby-gems.yml`:** precompiled native gems (oxidize-rb's `cross-gem` over `rb-sys-dock`) for x86_64 and aarch64 Linux (gnu and musl), x86_64 and arm64 macOS, and `x64-mingw-ucrt` Windows, for Ruby 3.3, 3.4 and 4.0, uploaded as artifacts only.
- **The source-gem fallback** follows in its own PR, since it needs a standalone `Cargo.toml`.
- **Publishing:** nothing goes to RubyGems without the owner's go.

## Later PRs
- **PR 2:** vectors, `multi_get`, `traverse`, `pages`, byte-budgeted reads and the change feed.
- **PR 3:** `pigeonhole-memory`, `pigeonhole-text` and `pigeonhole-vector`.

## Callers changed
None. It's a new crate.

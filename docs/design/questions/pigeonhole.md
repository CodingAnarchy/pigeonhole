# pigeonhole: open questions and proposed decisions (Phase 1 sync API)

Recorded while implementing the public crate over engine Milestone A (PR #41). Each section
states the interim behavior the code implements.

## Q: a `Durability::None` commit before a stronger one is not made durable
The spec's Durability section and the guide said "a `GroupSync` commit also makes any earlier
`Buffered` or `None` records durable", and the sim `Model`'s `crash_window` treats survivors
as a prefix that includes `None` commits. The engine writes no WAL record for a `None` commit
(`shard.rs`, group write loop), so a later `GroupSync` commit cannot make it durable, and
until SST flushes exist (#37) a `None` commit is lost at the next close or crash, even a
clean close. The spec's table ("memtable only, no I/O, survives nothing past the last flush")
supports the engine; its "Mixed levels" bullet does not.

**Interim behavior:** the engine's. The guide (`durability.md` "Mixed levels",
`getting-started.md` "What the current build does not do yet") now says a `None` commit
writes no WAL record and is not made durable by later commits. The public model suite
(`tests/model.rs`) expects every `None` commit to be gone after any reopen
(`Logged::reaches_the_wal`). Needs a decision: keep (and amend the spec's bullet and the
`Model`), or have the engine append `None` records unsynced with the next group.

## Proposed decision: Phase 2 family settings are refused at creation (proposed by pigeonhole)
`Family::zstd`, `Compaction::Tiered` and `Compaction::FifoByTime` are in the frozen API but
their implementations land in Phase 2. Storing them in the file now would make a later flush
or compaction fail (or silently ignore them) on data already written.

**Interim behavior:** `TableBuilder::{create, create_if_missing, open}` refuse a declared
family with any of them with `ErrorCode::Unsupported` before changing the catalog.
`blob_threshold` is accepted and stored (values stay inline until blobs exist; the default is
already 4096). Lifting the refusals: #44. The crate-level example no longer uses `.zstd(3)`.

## Proposed decision: every family has the `i64` add operator unless told otherwise
`Family::merge_operator` says "`incr` needs none: it uses the built-in `pigeonhole.i64_add`,
which is the default operator", but the engine's `FamilyOptions::default()` has no operator
and refuses merge operands on such a family.

**Interim behavior:** `Family::default()` stores `merge_operator = "pigeonhole.i64_add"`, so
`incr` works on any family; `Family::default().merge_operator("")` stores none (operands then
fail at commit with `InvalidArgument`). A family with the operator resolves plain puts as
usual; mixing byte puts and `incr` in one column fails at read with `MergeFailed` (D41).

## Q: registered custom merge operators are not passed to the engine yet
`Options::merge_operator(Arc<dyn MergeOperator>)` is frozen, but on the engine branch
`MergeRegistry` is a stub and the catalog resolves only `pigeonhole.i64_add`.

**Interim behavior:** registered operators are kept and not used; a family naming any other
operator is refused by the engine with `UnknownMergeOperator`. Documented on
`Options::merge_operator` and in the guide. Phase 2 work: #43.

## Proposed decision: `Scan::limit(0)` returns no rows
`ScanSpec::limit` uses 0 for "unlimited"; the public `limit(n)` says "stop after `n` rows".

**Interim behavior:** `limit(0)` yields an empty iterator without starting an engine scan;
no `limit` call means unlimited.

## Proposed decision: `TableBuilder::open` adds declared families that are missing
`TableBuilder::family` says "on an existing table, a family not yet present is added", while
`open` says only "opens an existing table; fails with `TableNotFound`".

**Interim behavior:** all three finishers add missing declared families to an existing table
(a family listed twice is declared once, with its first options); an existing family keeps
its stored options. Concurrent creation of the same table or family is resolved by opening
what the other caller created.

## Proposed decision: table handles resolve families added through other handles
`Table::families()` returns `Vec<&str>` borrowed from the handle, so it can only report the
families the handle was opened with.

**Interim behavior:** `families()` is "as of when this handle was opened" (documented).
Mutations, gets and reads resolve a name the handle does not know against the current
catalog, and a row read or scan names every family it returns through the catalog as of the
read, so a family added through another handle is usable everywhere without reopening.

## Proposed decision: `Error`'s `Display` is the message; unknown engine variants map to `Io`
**Interim behavior:** `Display` prints `message()` only (the code is available separately).
Every current `engine::Error` variant maps to exactly one `ErrorCode` (tested); a variant the
engine adds later (it is `#[non_exhaustive]`) maps to `Io` with the engine's message until it
gets its own code. A merge error's message is built from `MergeError`'s fields.

## Q: the simulation hook cannot size WAL segments, which makes `SimVfs` opens slow
The only hidden hook in the frozen API is `Options::vfs`. With the default 64 MiB WAL
segments, every open on `SimVfs` zero-fills segment files in memory: about 0.5 s per open and
0.9 s for the first table in a debug build, against 2 ms and 6 ms with the 256 KiB segments
the engine's suites use. The public model suite (`tests/model.rs`) reopens many times and
takes about 70 s in parallel in debug.

**Interim behavior:** the suite runs with the defaults. Proposal (needs an ICR, so not done):
a `#[doc(hidden)] Options::wal_segment_size(bytes)` test hook next to `vfs`.

## Note: features available ahead of their phase
The engine already implements conditional commits, optimistic transactions and reader
processes, so `RowMutation::commit_if` (P2), `Transaction` and `Pigeonhole::open_reader` (P4)
work and are tested (`tests/api.rs`); the guide marks them "early". Their hardening stays in
their phases.

## Note: what crosses the future C ABI
Checked against the spec's "Language scope":
- Borrowed results have owned counterparts (`CellRef` → `Cell`, `RowRef` → `Row`), and
  `RowIter::next_ref` is the cursor form of the scan iterator.
- Generic conveniences have non-generic equivalents: `scan(range)` → `scan_bounds`,
  `qualifier_range` → `qualifier_bounds`, `families(iter)` → repeated `family(&str)`;
  `impl AsRef<Path>` parameters accept a `&Path`.
- Errors are `#[repr(u32)]` codes plus a message; merge operators are identified by name in
  the file.
- Two frozen signatures take Rust-only types by nature: `Shard::set_wakeup(Box<dyn Fn>)` (a
  C ABI wraps a function pointer and context in the box) and
  `Options::merge_operator(Arc<dyn MergeOperator>)` (a C ABI would provide a vtable struct).
- Builders (`RowMutation`, `RowRead`, `Scan`) borrow their table, but a C ABI builds and
  finishes one within a single call. `RowIter<'t>` owns its engine cursor and borrows the
  table only as a lifetime, so a C ABI that keeps the `Table` alive next to it can hold one
  across calls; an owned `Table::scan_owned` could be added later if a binding needs it.

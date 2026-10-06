# pigeonhole: questions and proposed decisions (Phase 1 sync API)

Recorded while implementing the public crate over engine Milestone A (PR #41). Every entry
below was resolved in the coordinator's review of PR #47 (2026-10-06); the coordinator
numbers them when folding this file into `decisions.md`.

## Resolved (owner decision): a later stronger commit makes earlier `None` commits durable
The spec's "Mixed levels" says a `GroupSync` commit also makes earlier `Buffered` or `None`
records on its stream durable; the engine wrote no WAL record for a `None` commit, so it was
lost at the next close or crash even after a later stronger commit.

**Decision:** the spec's rule stands. A `None` commit buffers its WAL record (no write, no
sync of its own), so a later stronger commit on the same shard writes it and makes it
durable. Implemented by engine Milestone B, issue #50; this crate changes nothing.

**Until #50:** the guide's durability page states the rule and marks it as arriving with
#50 (today a `None` commit is lost at the next close or crash). The public model suite
(`tests/model.rs`, `Logged::reaches_the_wal`) expects today's behavior and is updated with
#50 and #45.

## Accepted: Phase 2 family settings are refused at creation
`Family::zstd`, `Compaction::Tiered` and `Compaction::FifoByTime` are in the frozen API but
land in Phase 2. `TableBuilder::{create, create_if_missing, open}` refuse a declared family
with any of them with `ErrorCode::Unsupported` before changing the catalog, so they are never
stored only to fail later in flush or compaction. `blob_threshold` is accepted and stored
(values stay inline until blobs exist). Lifting the refusals: #44.

## Accepted: every family has the `i64` add operator unless told otherwise (amends D41)
`Family::default()` stores `merge_operator = "pigeonhole.i64_add"`, so `incr` works on any
family, as `Family::merge_operator`'s documentation promised;
`Family::default().merge_operator("")` stores none (operands then fail at commit with
`InvalidArgument`). Mixing byte puts and `incr` in one column fails at read with
`MergeFailed` (D41); `RowMutation::merge` / `WriteBatch::merge` write untyped operands, which
the built-in operator also refuses at read. The `Family` documentation says so.

## Accepted: `Scan::limit(0)` returns no rows
`ScanSpec::limit` uses 0 for "unlimited"; the public `limit(0)` yields an empty iterator
without starting an engine scan, and no `limit` call means unlimited.

## Accepted: `TableBuilder::open` adds declared families that are missing
All three finishers add missing declared families to an existing table (a family listed
twice is declared once, with its first options); an existing family keeps its stored
options. Concurrent creation of the same table or family opens what the other caller
created. Creating a table needs a non-empty name and at least one declared family, else
`InvalidArgument` (coordinator decision in the same review).

## Resolved: table handles resolve families added through other handles
`Table::families()` returns `Vec<&str>` borrowed from the handle, so it reports the families
the handle was opened with (documented). Mutations, gets and reads resolve a name the handle
does not know against the current catalog, and a row read or scan names every family it
returns through the catalog as of the read.

## Resolved: `Error`'s `Display` is the message; unknown engine variants map to `Io`
`Display` prints `message()` only. Every current `engine::Error` variant maps to exactly one
`ErrorCode` (tested); a variant the engine adds later (it is `#[non_exhaustive]`) maps to
`Io` with the engine's message until it gets its own code. Messages the engine's unit
variants cannot carry are filled in here: `KeyTooLarge` names the part and its size against
the 64 KiB limit, `ValueTooLarge` the value's size against the D16 limit computed from the
open's options, and `Busy` (coordinator decision: a hard failure until engine Milestone B,
#37) says the memtable arena is full and to raise `Options::memtable_budget`.

## Resolved (ICR 0005, approved): a hidden `Options::wal_segment_size` test hook
With the default 64 MiB WAL segments every open on `SimVfs` cost about 0.5 s in a debug
build. The hook (`docs/design/icr/0005-pigeonhole-wal-segment-size-hook.md`) lets the
simulation suites use 256 KiB segments; the public model suite went from about 70 s to about
3 s with three times the operations.

## Tracked (#43): registered custom merge operators are not passed to the engine yet
`Options::merge_operator(Arc<dyn MergeOperator>)` keeps the operators; the engine resolves
only `pigeonhole.i64_add` so far, so a family naming any other operator is refused with
`UnknownMergeOperator`. Documented on `Options::merge_operator` and in the guide.

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
- `WriteBatch` has every mutation `RowMutation` has (`put`, `put_at`, `put_i64`, `put_f64`,
  `incr`, `merge`, `delete_cell`, `delete_column`, `delete_family`, `delete_row`), so a C ABI
  can export one mutation vocabulary.
- Errors are `#[repr(u32)]` codes plus a message; merge operators are identified by name in
  the file.
- Two frozen signatures take Rust-only types by nature: `Shard::set_wakeup(Box<dyn Fn>)` (a
  C ABI wraps a function pointer and context in the box) and
  `Options::merge_operator(Arc<dyn MergeOperator>)` (a C ABI would provide a vtable struct).
- Builders (`RowMutation`, `RowRead`, `Scan`) borrow their table, but a C ABI builds and
  finishes one within a single call. `RowIter<'t>` owns its engine cursor and borrows the
  table only as a lifetime, so a C ABI that keeps the `Table` alive next to it can hold one
  across calls; an owned `Table::scan_owned` could be added later if a binding needs it.

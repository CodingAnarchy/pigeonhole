# 0057: `ErrorCode::NoWriter`

**Status:** Approved (owner, via the coordinator, 2026-10-21; #844, option (b)). Additive.

## Change

- **`pigeonhole`:** `ErrorCode::NoWriter = 29`. A reader process found no running writer to attach to.
- **`pigeonhole-engine`:** `Error::NoWriter`. The enum is `#[non_exhaustive]`.
- **`pigeonhole-shm`:** `Error::NoWriter`.

**When it's returned:**
- by `Pigeonhole::open_reader`;
- by a reader's next snapshot or read when it must re-attach to a new writer generation;
- in both cases, when either:
  - the shared-memory directory names no generation (no writer has built a region, or the last one removed it on close), or
  - opening the named generation's region fails with `NotFound` (its writer closed and no process maps it; on Windows that happens as soon as the writer closes).

Any other failure to open the region stays `Io`, and a database file that doesn't exist is still `Io`.

**The message:** "no writer is running: a reader process attaches to a running writer's shared memory; open the reader again once a writer runs".

In the CLI, `--reader` with no writer running exits 4 ("locked or busy"), like `NoReaderSlot`.

## Why

Without a writer, a reader got an untyped `Io` ("no shared-memory region…: not found"):
- on every platform, from `open_reader` once the writer had closed;
- on Windows also from re-attach, when a new writer opened and closed before the reader's next snapshot (#844).

A program can't act on `Io`. With `NoWriter` it knows to wait for a writer and open the reader again.

`Busy` (option (c) in #844) was rejected: programs couldn't tell "no writer" from a write stall.

## Callers

- `pigeonhole-shm`:
  - `region.rs` `attach` (generation 0, and `NotFound` from opening the region);
  - `lib.rs` (the variant);
  - `tests/sim.rs` `a_reader_with_no_writer_region_gets_no_writer` (before any writer, and on re-attach to a removed generation).
- `pigeonhole-engine`: `error.rs` (the variant, its `Display`, and the shm mapping).
- `pigeonhole`:
  - `error.rs` (the code and mapping);
  - `tests/api.rs` (the stable number and the mapping);
  - `tests/readers.rs` `a_reader_with_no_writer_running_gets_no_writer`.
- `pigeonhole-cli`:
  - `exit.rs` (exit 4);
  - `open.rs` drops its "is a writer process running?" guess from `Io`, since the error now says so;
  - `tests/info.rs` (`--reader` with no writer: exit 4, "no writer is running").
- Docs:
  - errors.md (row 29 and the handling guide);
  - the agent reference;
  - readers.md (#844's caveat, and a no-writer line);
  - cli.md;
  - the changelog.

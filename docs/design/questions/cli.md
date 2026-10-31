# CLI questions

## Q: how `phdb mcp` opens a database, and what `--read-only` means (#1249)
#1249 asks that `--read-only` "opens the database as a reader process". A reader process needs a running writer, so `phdb mcp --read-only` on a database nobody has open fails. The alternative, the shell's `--read-only` (open as the writer and refuse writes), would hold the writer lock for as long as the client keeps the server running, locking the application out while the agent only reads. Without `--read-only`, the server follows the shell's open policy (#692): the writer when none runs, else a reader process with the write tools left out.

**Approved by the coordinator, 2026-10-31,** as stated below. The guide says a writer-mode session holds the writer lock for its lifetime, and recommends `--read-only` plus a separate writer for most setups.

**Interim behavior:** `--read-only` opens only as a reader process (exit 4, `NoWriter`, without a writer) and `tools/list` leaves out `write` and `create_table`. Without it, the shell's policy, plus `--create`. The guide's [AI agents page](../../guide/ai-agents.md#running-the-server) says the writer lock is held for the session and gives the two setups (the agent owns the database, or the application does and the agent reads).

## Q: `phdb mcp`'s result limits and cursors (#1249)
#1249 asks for truncation "with a continuation token" past a configured size, without saying how size is counted or what a token resumes.

**Approved by the coordinator, 2026-10-31** (size counted in JSON bytes).

**Interim behavior:** size is the result's JSON bytes, about (`--max-result-bytes`, default 32 KiB); the first item of a result always fits, so every call makes progress. `read_row` cuts at a column boundary (a `RowPosition`), `scan` between rows (a row too big alone comes back cut and marked `truncated`), `traverse` between nodes (the cursor reruns the traversal and skips). Byte values past `--max-value-bytes` (4 KiB) come back as `{"omitted_bytes": n}`. A cursor carries a hash of the call's other arguments and fails on a mismatch. Cursors hold no snapshot.

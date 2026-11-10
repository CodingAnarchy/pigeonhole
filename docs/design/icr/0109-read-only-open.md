# 0109: `Options::read_only`, a requested read-only open

**Status:** Approved (number from the coordinator; the owner approved D310 amendment (d) on 2026-11-02, brief `brief-d310-inherit-2026-11-02`: phdb's read-only commands open read-only, through a public option).

## The change
- **pigeonhole** (semver, additive): `Options::read_only(bool)`, default off. It forces the open `read_only_fallback` (ICR 0059) falls back to, whatever the file needs.
  - **It writes nothing to the database's files:** no format upgrade, no change-feed session or Root entries (D310), no new WAL segment, no flush, compaction or clean-close commit. The WAL a crashed writer left is replayed into memory and left in place.
  - **Writes are refused:** writes, schema changes, `flush`, `compact` and `shrink` fail with `ReadOnly`, and `DatabaseInfo::read_only` is true.
  - **The replay must fit `memtable_budget`,** or the open fails with `ReadOnly`, as under the fallback.
  - **It never creates a file,** whatever `create_if_missing` says.
  - **It is still a writer-process open otherwise** (simp724 on #1347):
    - It takes the writer lock, which needs write access to the file, so it fails on a read-only file or mount.
    - It excludes a running writer (`WriterLocked`). phdb handles that case by opening a reader process.
    - It creates the writer's shared-memory region.
    - A true read-only open (a shared lock, no shared memory) for read-only media and backups is a separate, Post-1.0 issue (#1348).
- **pigeonhole-engine** (internal, additive; `EngineOptions` is `#[non_exhaustive]`): `EngineOptions::read_only`. `Engine::open` starts with `no_writes` set, opens the pager read-only, skips the unknown-merge-operator refusal (as the fallback does), and never creates a missing file.
- **phdb:** `info`, `dump`, `backup` and `shell --read-only` open with it when no writer runs (`open::for_inspect`). With a writer running they open a reader process, as before. The shell without `--read-only` and `mcp` still open writable.

## Why
D310: an open without the change feed, or with another shard count, ends feed history, so a `phdb info` on a stopped application's database silently expired its feed tokens. A read-only open starts no feed session, so it leaves the history whole. D310 amendment (c) makes writable opens inherit the feed configuration; this covers the commands that only read, and is useful on its own to any tool that inspects a database without changing it.

## Callers changed
None: additive. phdb's read-only commands now use it. `phdb info` reports `read_only: true` for them when it opens as the writer.

## Amendment, 2026-11-05: a true read-only open (#1348; the owner chose option (A))
`Options::read_only(true)` itself becomes the true read-only open. There is no new API, and the semver check sees no change. This lifts the three limits the original listed as #1348's:
- **Read-only files.** The lock handle, like every file, is opened read-only, so the open works on a read-only file or mount.
- **A shared lock.** The writer byte (FORMAT §8.3) is taken *shared*, so read-only opens run together, in one process or many. A writer's exclusive lock and a read-only open's shared one exclude each other: either way round, the second fails with `WriterLocked`.
- **No shared memory.** The handle's arenas and words live in a private in-memory region with the writer's layout. No presence is taken and no region is created, so a reader process beside only read-only opens fails with `NoWriter`.

**What else changes:**
- **The `WriterLocked` message** now says the lock may be a running writer's *or a read-only open's*, in this process or another (simp724's R2).
- **Cleanup waits** while a read-only open holds the byte (simp724's R3). A crashed writer's stale region, or the log files a last reader process removes after a clean close, stay until a process takes the writer lock again.
- **What a caller loses:** a reader process can no longer attach to a `read_only` handle's region, since there is none. phdb only opened a reader process when the read-only open failed with `WriterLocked`, which is now just a running writer.
- **Unchanged:** `read_only_fallback` (ICR 0059), which is decided after a writable attempt already holds the exclusive lock and the region.
- **The handover** (D265, the automatic recovery's reopen) carries the shared lock.
- **Deferred past 1.2.0:** a lockless (`immutable`) mode.

**Tests:**
- `it::read_only_fallback::a_requested_read_only_open_runs_on_read_only_media`: the simulator's read-only media refuses every write open, create, remove and directory sync; two opens run at once. Mutation-checked: a writable lock handle fails it.
- `tests/read_only.rs`:
  - on `pread` and on io_uring (Linux), a writer refused under a read-only open and the reverse, two read-only opens, and a closed one leaving the other's lock (simp724's R1);
  - no region made, and `open_reader` gets `NoWriter`;
  - a crashed writer's database on a `chmod`'d read-only file and directory, replayed, its files byte for byte unchanged.

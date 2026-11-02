# 0109: `Options::read_only`, a requested read-only open

**Status:** Approved (number from the coordinator; the owner approved D310 amendment (d) on 2026-11-02, brief `brief-d310-inherit-2026-11-02`: phdb's read-only commands open read-only, through a public option).

## The change
- **pigeonhole** (semver, additive): `Options::read_only(bool)`, default off. It forces the open `read_only_fallback` (ICR 0059) falls back to, whatever the file needs.
  - **It writes nothing to the database's files:** no format upgrade, no change-feed session or Root entries (D310), no new WAL segment, no flush, compaction or clean-close commit. The WAL a crashed writer left is replayed into memory and left in place.
  - **Writes are refused:** writes, schema changes, `flush`, `compact` and `shrink` fail with `ReadOnly`, and `DatabaseInfo::read_only` is true.
  - **The replay must fit `memtable_budget`,** or the open fails with `ReadOnly`, as under the fallback.
  - **It never creates a file,** whatever `create_if_missing` says.
  - **It still takes the writer lock:** one process at a time, as any writer-process open. To read while a writer runs, open a reader process.
- **pigeonhole-engine** (internal, additive; `EngineOptions` is `#[non_exhaustive]`): `EngineOptions::read_only`. `Engine::open` starts with `no_writes` set, opens the pager read-only, skips the unknown-merge-operator refusal (as the fallback does), and never creates a missing file.
- **phdb:** `info`, `dump`, `backup` and `shell --read-only` open with it when no writer runs (`open::for_inspect`). With a writer running they open a reader process, as before. The shell without `--read-only` and `mcp` still open writable.

## Why
D310: an open without the change feed, or with another shard count, ends feed history, so a `phdb info` on a stopped application's database silently expired its feed tokens. A read-only open starts no feed session, so it leaves the history whole. D310 amendment (c) makes writable opens inherit the feed configuration; this covers the commands that only read, and is useful on its own to any tool that inspects a database without changing it.

## Callers changed
None: additive. phdb's read-only commands now use it. `phdb info` reports `read_only: true` for them when it opens as the writer.

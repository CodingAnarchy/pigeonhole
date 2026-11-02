# Using Pigeonhole from AI agents

> **Status:** this guide describes `main`; the current release on crates.io is 1.0.0, and [`changelog.d/`](../../changelog.d/README.md) lists what `main` adds after it. `phdb mcp` is new in 1.1.0 ([#1249](https://github.com/CodingAnarchy/pigeonhole/issues/1249)).

`phdb mcp` serves a Pigeonhole database to AI agents over the [Model Context Protocol](https://modelcontextprotocol.io) (MCP). Add it to an MCP client (Claude Code, Claude Desktop, an editor, an agent framework) and the agent can list tables, read rows, scan, traverse graphs, find nearest vectors and write in atomic batches, with no glue code. Agent code written in Python can use the [Python bindings](python.md) directly instead.

A database gives an agent memory and state that a folder of markdown files can't: writes are transactional, reads are queryable by key and prefix, cells keep versions, and other processes can read the same file while the agent writes it.

## Running the server
```sh
cargo install pigeonhole-cli
phdb mcp --create memory.phdb        # a new database, writable
phdb mcp app.phdb                    # an existing one
phdb mcp --read-only app.phdb        # beside your running application, reads only
```

The server speaks MCP's stdio transport: one JSON-RPC message per line on standard input and output. The client starts it and talks to it; you don't run it by hand. It logs one line to standard error, and exits when the client closes its input, closing the database.

| Flag | Meaning |
|---|---|
| `--create` | Create the file if it doesn't exist. |
| `--read-only` | Open as a [reader process](readers.md) of a running writer, and leave the write tools out. Without a writer running, the server exits with status 4 (`NoWriter`). |
| `--max-result-bytes N` | A tool result's size, about, in bytes of JSON (default 32 KiB, about 8,000 tokens). A longer result is cut and returns a cursor. |
| `--change-feed BYTES` | Turn the [change feed](#following-changes) on, keeping up to `BYTES` of history. Without it, a database whose application already feeds keeps that feed: `phdb`'s open inherits it, and `feed_poll` serves it. Only as the writer. |
| `--max-value-bytes N` | The longest value a read returns whole by default (4 KiB). A longer one comes back as `{"omitted_bytes": n}`; a read can ask for more with `max_value_bytes`. |
| `--shm-dir DIR`, `--reader`, `--writer` | As for every `phdb` command ([CLI guide](cli.md#how-phdb-opens-a-database)). |

**How it opens the database.** Without `--read-only`, the server opens as `phdb shell` does. With no writer running, it becomes the writer: it holds the writer lock for as long as the client keeps it running, so your application can't open the database as the writer meanwhile (its open fails with `WriterLocked`). With your application running, it opens as a reader process, and the write tools are left out.

**A writer-mode session holds the writer lock for its whole lifetime:** from the moment the client starts the server until it stops it, which for most clients is as long as the client runs. Nothing else can open the database as the writer meanwhile, not your application, not another `phdb` command that writes, not a second agent.

So for most setups, prefer `--read-only` with a separate writer:
- **Recommended: a writer process plus `phdb mcp --read-only`.** Your application, or a small service of your own, is the one writer, and the agent reads through a reader process. The agent sees every commit the writer makes, can't write, and never blocks the writer. If the agent should record things too, have it go through the writer (an endpoint, a queue, or a tool of your own) rather than through the MCP server.
- **Only when the agent alone owns the database** (a scratch store or a private memory nothing else writes): run `phdb mcp` without `--read-only`. Other processes can still open the file as readers, but nothing else can write it until the client stops the server.

## Client configuration
Use an absolute path to the database: clients start servers from a directory of their choosing.

**Claude Code:**
```sh
claude mcp add pigeonhole -- phdb mcp --create /home/me/agent/memory.phdb
```
or, for a project, a `.mcp.json` at its root:
```json
{
  "mcpServers": {
    "pigeonhole": {
      "command": "phdb",
      "args": ["mcp", "--create", "/home/me/agent/memory.phdb"]
    }
  }
}
```

**Claude Desktop** (`claude_desktop_config.json`), **Cursor** (`.cursor/mcp.json`) and most other clients take the same `mcpServers` object.

**VS Code** (`.vscode/mcp.json`):
```json
{
  "servers": {
    "pigeonhole": {
      "type": "stdio",
      "command": "phdb",
      "args": ["mcp", "--read-only", "/srv/app/app.phdb"]
    }
  }
}
```

If `phdb` isn't on the client's `PATH`, give its full path as `command` (`~/.cargo/bin/phdb` after `cargo install`).

## The tools
Eight tools, each with a JSON schema and a description written for the agent. `tools/list` leaves `nearest` out when the database has no [vector family](vectors.md) (the server advertises `listChanged`, and sends `notifications/tools/list_changed` once a call adds the first one), `feed_poll` out unless the database's change feed is on (the application's, which the server inherits, or `--change-feed`; with `--read-only` it's always listed, since a reader can't see the writer's options), and `write` and `create_table` out when the database is read-only.

| Tool | What it does |
|---|---|
| `list_tables` | The tables, and each one's families with their options (aggregation, `max_versions`, `ttl_micros`, ...). Says whether the database is writable. |
| `read_row` | One row's cells. Selects `families`, one `qualifier` or a `qualifier_prefix`, and `versions` (default 1). A wide row comes back in parts. |
| `scan` | Rows in key order: a `prefix`, a `start`/`end` range, or all; `limit` rows (default 100), `columns_per_row` (default 100), and the same selection as `read_row`. |
| `traverse` | Breadth-first hops over an edge family from `seeds` ([Graphs](graphs.md#traversal-hops-inside-the-engine)): `hops`, `per_node_limit`, `hop_limit`, `max_nodes` (default 100,000), and `parents` for shortest paths. |
| `nearest` | The `k` vectors nearest a `query` in a vector family (default 10, at most 1,000), nearest first, each with its `row`, `qualifier`, `ts` and `distance` under the family's metric: an exact search ([Vectors](vectors.md)). The query is a list of the family's `dim` numbers: integers from -128 to 127 for `i8`, 0s and 1s for `bit` (or `{"base64"}` of the packed bytes). Narrows to a `row`, a `prefix` or a `start`/`end` range, and a `qualifier` or `qualifier_prefix`. |
| `feed_poll` | The commits to `tables` since a `token`, whole and in commit order, and the token to pass next ([Following changes](#following-changes)). `max_commits` (default 100), `wait_ms` to wait for a first commit, `prefix`, `from_oldest` and `visible`. |
| `write` | Up to 10,000 operations in one atomic `WriteBatch`: `put`, `incr` (counter families), `aggregate` (other aggregate families) and `delete` (a row, a family, a column, or one version with `ts`). All commit, or none. An `aggregate` value is read by the family's kind: any number for the f64 kinds, a non-negative integer for the bitwise ones, an integer or a string for `hll` and `bloom`. |
| `create_table` | A table and its families, if missing. A family is a name, or an object in `list_tables`' shape (`{"name": "hits", "aggregation": "sum"}`). |

**Encoding.** Values use [`phdb dump`](cli.md#phdb-dump)'s JSON shapes. Row keys, qualifiers and byte values are JSON strings when they are UTF-8, else `{"base64": "..."}`. Integer and float cells are `{"i64": n}` and `{"f64": x}`. Timestamps are microseconds since the Unix epoch. A cell in a result is `{"family", "qualifier", "ts", "value"}`.

**Errors.** A tool that fails returns a result with `isError` and `{"error": {"code", "code_num", "message"}}`, with the [`ErrorCode`](errors.md) name when the database refused the call (`TableNotFound`, `FamilyNotFound`, `ReadOnly`, ...) and `Usage` for a bad argument. The agent sees it and can correct the call. An unknown argument is an error, not ignored, so a misspelt `limt` doesn't silently scan 100 rows.

## Long results and cursors
A result that would pass `--max-result-bytes` stops early and carries a `cursor`. Calling the same tool again with the same arguments plus that `cursor` returns the next part. An agent pages through anything this way, a million-edge hub included, without flooding its context.
- **`read_row`** cuts at a column boundary: every version of a column is in one part.
- **`scan`** cuts between rows. A single row too big for a result comes back marked `"truncated": true` with as many columns as fit; read the rest with `read_row`. A row that `columns_per_row` cut is marked `"columns_capped": true`.
- **`traverse`** cuts between nodes, and each hop in a result says which node it starts at (`first_index`) and how many the hop has (`node_count`).
- **`nearest`** has no cursor, since a ranking can't resume partway. A result that would pass the limit keeps the nearest neighbours that fit and is marked `"truncated": true`; ask for a smaller `k`.

A cursor is bound to its call's arguments: one passed with other arguments fails, rather than resuming some other read at a meaningless place.
- **`read_row` and `scan` cursors** don't hold a snapshot. Each call reads the latest data, so a row written between calls may be missed or seen twice, as with [`resume_after`](graphs.md#hubs-walking-a-wide-row) without a snapshot.
- **A traversal's cursor pages from the result the first call computed.** The server keeps the last traversal that returned a cursor (up to about 64 MiB), so each later page costs only that page, and every page comes from one consistent result: writes between pages change nothing. The server keeps one traversal at a time: the next one that returns a cursor replaces it. A cursor whose traversal was replaced, or was too large to keep, or that comes from before a server restart, runs the traversal again and skips what was returned, as over unchanged data it would. That costs the whole traversal, and writes since the first page can shift its nodes.

## Following changes
When the database's change feed is on, `feed_poll` lets an agent follow what is committed (its own writes, other clients of the same server, and what the application committed before the server started, while the feed's history holds it) without polling whole tables. Each call returns the commits after its `token`, whole (a batch across tables arrives as one) and in commit order, each `{"seqno", "ts", "changes"}`. A change is `{"table", "family", "kind", "row", "qualifier", "ts", "value"}`: `kind` is `put`, `merge` (a counter increment or aggregate input), `delete_cell`, `delete_column` or `delete_family`, and deletes have no `value`. The result's `token` continues exactly where it stopped, so an agent that keeps it misses nothing and sees nothing twice; `more: true` says more commits are ready now.
- **The first call** has no token and starts with the commits after it, or with the oldest history kept (`from_oldest`). To build a view and then keep it current, take a first token, `scan`, then poll from the token: a change committed during the scan may be seen twice, never missed.
- **Nothing is held in the server between calls:** the token is the agent's state.
- **Errors partway through a batch:** the commits gathered before the error come back with their token, and the next call, from that token, reports the error. Keep a token across sessions: it survives the application's restarts and the server's own reconnects.
- **`FeedExpired`** means the history the token needs is gone: retention passed it, or an open turned the feed off since. Re-read with `scan` and start again without a token.
- **The size cap:** a result holds whole commits only, cut between commits; the first commit always comes back whole, however large.
- **Beside a running application** (`--read-only`, a reader process), `feed_poll` follows the application's feed too, from its shared history. If the application restarts during a call, the call fails with `SnapshotExpired`; call again with the same token.

## An example session
An agent keeping notes about users, as the client sends it (requests abbreviated to `tools/call`'s `name` and `arguments`):

```json
{"name": "create_table", "arguments": {"table": "memory", "families": ["fact", {"name": "seen", "aggregation": "sum"}]}}
```
```json
{"table": "memory", "families": [{"name": "fact", "counter": false, "max_versions": 0, ...}, {"name": "seen", "counter": true, "aggregation": "sum", ...}]}
```

```json
{"name": "write", "arguments": {"ops": [
  {"op": "put", "table": "memory", "row": "user:ada", "family": "fact", "qualifier": "language", "value": "Rust"},
  {"op": "put", "table": "memory", "row": "user:ada", "family": "fact", "qualifier": "timezone", "value": "Europe/London"},
  {"op": "incr", "table": "memory", "row": "user:ada", "family": "seen", "qualifier": "sessions", "value": 1}
]}}
```
```json
{"committed": 3, "seqno": 1}
```

```json
{"name": "scan", "arguments": {"table": "memory", "prefix": "user:", "families": ["fact"]}}
```
```json
{"table": "memory", "rows": [{"row": "user:ada", "cells": [
  {"family": "fact", "qualifier": "language", "ts": 1793480000000000, "value": "Rust"},
  {"family": "fact", "qualifier": "timezone", "ts": 1793480000000000, "value": "Europe/London"}
]}]}
```

A later session reads the same facts back, and `"versions": 5` on `read_row` shows how a fact changed over time.

## Modelling tips for agent memory
- **Key rows by entity:** `user:ada`, `doc:1234`, `task:2026-10-31:3`. A prefix scan then lists a kind of entity, and keys that sort by time list it in time order.
- **One family per kind of data,** read together: `fact`, `note`, `link`. A family's options (`max_versions`, `ttl_micros`) apply to all of its cells, so give history and expiring data families of their own.
- **Counters in a `sum` family,** incremented in the same `write` as the data they count.
- **Links as a graph:** an edge family whose qualifiers are other rows' keys, with the reverse edge in the same `write`. Then `traverse` finds what is related, a few hops out. [Data modeling](data-modeling.md) and [Graphs](graphs.md) have more.
- **Embeddings in a vector family** (`create_table` with `"vector": {"dim", "element", "metric"}`), one per note, beside the note's text in the same row. Then `nearest` finds the notes closest in meaning, and `read_row` reads them.

# pigeonhole-cli

`phdb`: the command-line tool for [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole) databases, in the spirit of `sqlite3` and `duckdb`.

```sh
cargo install pigeonhole-cli
phdb info app.phdb
phdb check app.phdb
phdb dump app.phdb > app.jsonl && phdb load --create copy.phdb app.jsonl
phdb --format json info app.phdb
```

**Status: every planned command works** ([#692](https://github.com/CodingAnarchy/pigeonhole/issues/692)). These work:
- `phdb info`: format version, size and free space, and per table its tablets, families (with their stored options) and files.
- `phdb check`: an offline integrity check of every structure in the file, exiting 3 on damage.
- `phdb dump` and `phdb load`: a JSONL export of every cell at one snapshot, or CSV, or an Arrow IPC stream of one table (`--format arrow`), and the load that reads the JSONL back.
- `phdb backup` and `phdb compact`: a consistent single-file copy, and a full compaction with progress.
- `phdb shell`: get, scan, put, incr, delete, create and snapshots from a small command language, interactively or with `-c` and scripts.
- `phdb mcp`: a Model Context Protocol server for AI agents, with tools to list tables, read rows, scan, traverse graphs and write in atomic batches, plus long-term agent memory tools with `--memory` ([guide](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/ai-agents.md)).

- `phdb serve`, with the `serve` feature (off by default): one process serves a database over gRPC and HTTP/JSON on one port, with TLS, scoped tokens, the change feed, backups, read replicas (`phdb promote`), remote MCP at `/mcp`, and clusters of key-range partitions (`phdb cluster`). Install it with `cargo install pigeonhole-cli --features serve` ([guide](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/serve.md)).

The default `embed` feature lets `phdb mcp --embed-url` compute embeddings through an OpenAI-compatible endpoint over HTTPS. `cargo install pigeonhole-cli --no-default-features` leaves the HTTP client out. The `embed-local` feature (off by default) adds `--embed-local`, which runs `BAAI/bge-small-en-v1.5` on the CPU inside `phdb`, with no embedding service: `cargo install pigeonhole-cli --features embed-local`.

Output is `table` for people, or `json`, `jsonl` and `csv` for programs. The JSON is stable within its `"phdb": 1` version, and the exit statuses are documented. While your application has the database open, `phdb` reads it as a reader process. Otherwise it opens as the writer for the length of the command, replaying a crashed WAL as the next open would. The [CLI guide](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/cli.md) has the details.

To use Pigeonhole from your own code, you want the [`pigeonhole`](https://crates.io/crates/pigeonhole) crate.

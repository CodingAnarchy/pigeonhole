# pigeonhole-cli

`phdb`: the command-line tool for [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole) databases, in the spirit of `sqlite3` and `duckdb`.

```sh
cargo install pigeonhole-cli
phdb info app.phdb
phdb check app.phdb
phdb dump app.phdb > app.jsonl && phdb load --create copy.phdb app.jsonl
phdb --format json info app.phdb
```

**Status: in progress** ([#692](https://github.com/CodingAnarchy/pigeonhole/issues/692)). These work:
- `phdb info`: format version, size and free space, and per table its tablets, families (with their stored options) and files.
- `phdb check`: an offline integrity check of every structure in the file, exiting 3 on damage.
- `phdb dump` and `phdb load`: a JSONL export of every cell at one snapshot, or CSV, and the load that reads the JSONL back.
- `phdb backup` and `phdb compact`: a consistent single-file copy, and a full compaction with progress.

An interactive `shell` is coming.

Output is `table` for people, or `json`, `jsonl` and `csv` for programs. The JSON is stable within its `"phdb": 1` version, and the exit statuses are documented. While your application has the database open, `phdb` reads it as a reader process. Otherwise it opens as the writer for the length of the command, replaying a crashed WAL as the next open would. The [CLI guide](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/cli.md) has the details.

To use Pigeonhole from your own code, you want the [`pigeonhole`](https://crates.io/crates/pigeonhole) crate.

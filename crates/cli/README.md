# pigeonhole-cli

`phdb`: the command-line tool for [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole) databases, in the spirit of `sqlite3` and `duckdb`.

```sh
cargo install pigeonhole-cli
phdb info app.phdb
phdb --format json info app.phdb
```

**Status: in progress** ([#692](https://github.com/CodingAnarchy/pigeonhole/issues/692)). `phdb info` works: format version, size and free space, and per table its tablets, families (with their stored options) and files. `check`, `dump`, `load`, `backup`, `compact` and an interactive `shell` are coming.

Output is `table` for people, or `json`, `jsonl` and `csv` for programs. The JSON is stable within its `"phdb": 1` version, and the exit statuses are documented. While your application has the database open, `phdb` reads it as a reader process. Otherwise it opens as the writer for the length of the command, replaying a crashed WAL as the next open would. The [CLI guide](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/cli.md) has the details.

To use Pigeonhole from your own code, you want the [`pigeonhole`](https://crates.io/crates/pigeonhole) crate.

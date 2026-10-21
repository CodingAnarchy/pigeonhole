# The `phdb` command-line tool

`phdb` inspects and maintains Pigeonhole databases from the shell, in the spirit of `sqlite3` and `duckdb`.

> **Status: in progress on `main`, not released yet** ([#692](https://github.com/CodingAnarchy/pigeonhole/issues/692)). `phdb info` works. `check`, `dump`, `load`, `backup`, `compact` and the interactive `shell` are coming, in that order; this page grows with each.

**Install:** `cargo install pigeonhole-cli` installs the `phdb` binary. Until the next release, build it from a checkout with `cargo install --path crates/cli`.

## Commands

| Command | What it does |
|---|---|
| `phdb info <file>` | Format version, file size and free space, and per table its tablets, its families with their stored options, and their files (SSTs per level, blob files). |

**Global flags,** before or after the command:

| Flag | Meaning |
|---|---|
| `--format table\|json\|jsonl\|csv` | Output format (default `table`). |
| `--shm-dir <DIR>` | The shared-memory directory the database's writer uses (`Options::shm_dir`), if not the default. |
| `--reader` | Open only as a reader process; fail unless a writer process has the database open. |
| `--writer` | Open only as the writer; fail if a writer process has it open. |
| `--help`, `--version` | Help (also per command: `phdb info --help`) and version. |

## How `phdb` opens a database

A database has one writer process at a time and any number of reader processes (see [Concepts](concepts.md#multi-process-readers-phase-4-available-now)). A read-only command such as `info` works either way:

- **No writer running:** `phdb` opens the database as the writer for the length of the command, then closes it.
  - If the last writer crashed, opening **replays its WAL**, exactly as your application's next open would, like `sqlite3` rolling back a hot journal. The close then checkpoints it, so the database is one file again.
  - While `phdb` holds the writer lock, your application can't open as the writer: its open fails with `WriterLocked` until `phdb` exits.
- **A writer is running:** `phdb` opens as a reader process. It sees everything the writer has committed and changes nothing.

`phdb` opens with one unpinned shard and small caches, and never creates a file. Families that name a merge operator `phdb` doesn't know open read-only.

`--reader` and `--writer` force one role, and fail rather than fall back.

## Output

Choose with `--format`:

- `table` (the default): aligned text for people. A summary as `name  value` lines, then one line per row with the main columns. A missing value prints as `-`.
- `json`: one pretty-printed JSON document holding everything the command reports.
- `jsonl`: one JSON object per line. First a header (`phdb`, `command` and the summary fields), then one object per row with every column.
- `csv`: a header line of column names, then one line per row with every column (RFC 4180 quoting). A missing value is an empty field.

`table` stays the default when the output is a pipe, so a script gets the same shape it saw in a terminal. Scripts and agents should pass `--format json` or `--format jsonl`.

### Stability for programs

- Every JSON document, and the first line of every `jsonl` stream, carries `"phdb": 1`. While it is `1`, fields are only added, never renamed, removed or retyped, so consumers must ignore fields they don't know. Field order is not significant.
- Error codes are the stable [`ErrorCode`](errors.md) names and numbers.
- Sizes are bytes, durations microseconds, and timestamps microseconds since the Unix epoch, as in the API.
- `table` output is for people and may change in any release.

### `phdb info`

**Summary fields** (the `json` document's top level, and the `jsonl` header):

| Field | Meaning |
|---|---|
| `path` | The file as given. |
| `mode` | `writer` or `reader`: how `phdb` opened it. |
| `format_version` | The file format version. |
| `db_id` | The database's id, 32 hex digits. |
| `shards` | The running writer's shard count in `reader` mode. `null` in `writer` mode: the count is an open option, not stored in the file. |
| `file_bytes` | The main file's length, without the WAL sidecars. |
| `free_bytes` | Space in the file no data holds: what `shrink` could release at most, plus the 64 KiB header. `null` in `reader` mode. |
| `tables` | The table count. In `json`, the array of tables: `name`, `tablets` and `families`. |

**Per family** (each `jsonl` and `csv` row, and each entry of a table's `families` in `json`):

| Field | Meaning |
|---|---|
| `table`, `family` | Names (`jsonl` and `csv` rows; `json` nests families under their table and calls the family's field `name`). |
| `tablets` | Tablets the table is split into now (rows only). |
| `counter` | A counter family (`Family::counter`). |
| `max_versions` | Versions kept per column; 0 keeps all. |
| `ttl_micros` | Time to live; `null` for none. |
| `bloom_bits`, `block_size`, `blob_threshold` | As set with `Family`. |
| `compression`, `zstd_level` | `none`, `lz4` or `zstd`, and zstd's level (`null` otherwise). |
| `merge_operator` | The registered operator the family names, or `null`. |
| `cache_priority` | `low`, `normal` or `high`. |
| `compaction` | `leveled`, `tiered` or `fifo-by-time`. |
| `ssts`, `sst_bytes`, `entries` | SSTs, their bytes and their entries (cell versions and deletion markers) over every level. Data still in memtables isn't counted. |
| `levels` | `json` only: per level holding SSTs, `level`, `ssts`, `bytes` and `entries`. |
| `blob_files`, `blob_bytes`, `blob_live_bytes` | Blob files, their bytes, and the bytes live SSTs still reference. |

A table with no families gets one row with the family columns empty.

Example:

```text
$ phdb info app.phdb
path            app.phdb
mode            writer
format_version  3
db_id           5f1c0e9a2b7d4c3e8a6f0b1d2c3e4f50
shards          -
file_bytes      67239936
free_bytes      66846720
tables          2

table   family   tablets  ssts  sst_bytes  entries  blob_bytes  max_versions  ttl_micros     compression
events  e        1        1     1061       50       0           0             -              zstd
users   profile  1        0     0          0        0           1             -              lz4
users   history  1        0     0          0        0           0             2592000000000  none
```

## Exit status

| Status | Meaning |
|---|---|
| 0 | Success. |
| 1 | An error not listed below (see the error's code). |
| 2 | Usage: bad arguments. |
| 3 | Corruption: the file is damaged (`ErrorCode::Corruption`). |
| 4 | Locked or busy: another process holds the writer lock (`WriterLocked`), the database stayed too busy (`Busy`), or every reader slot is taken (`NoReaderSlot`). |
| 5 | Not found: the file, or a table or family the command names. |
| 6 | Unsupported: the file's format version is newer than this `phdb` (`UnsupportedFormat`), or the writer runs another shared-memory layout (`ShmVersionMismatch`), or the system lacks a feature (`Unsupported`). |

**Errors** go to standard error. With `--format table` or `csv` they print as one line, `phdb: error: <message>`. With `--format json` or `jsonl` they print as one JSON object:

```json
{"phdb":1,"error":{"code":"WriterLocked","code_num":3,"message":"open: another process holds the writer lock"}}
```

`code` is the [`ErrorCode`](errors.md) name and `code_num` its number. For a failure outside the database, the code is a `phdb` name with `code_num` `null`. Today the only such code is `FileNotFound`, for a missing file. The message is for people; its wording may change.

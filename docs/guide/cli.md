# The `phdb` command-line tool

`phdb` inspects and maintains Pigeonhole databases from the shell, in the spirit of `sqlite3` and `duckdb`.

> **Status: on `main`, not released yet** ([#692](https://github.com/CodingAnarchy/pigeonhole/issues/692)). Every command works: `info`, `check`, `dump`, `load`, `backup`, `compact` and the interactive `shell`. CI installs it from a checkout and runs it.

**Install:** `cargo install pigeonhole-cli` installs the `phdb` binary. Until the next release, build it from a checkout with `cargo install --path crates/cli`.

## Commands

| Command | What it does |
|---|---|
| `phdb info <file>` | Format version, file size and free space, and per table its tablets, its families with their stored options, and their files (SSTs per level, blob files). |
| `phdb check <file> [--deep] [--map] [--online]` | Check the file for damage, offline. Exits 3 if anything is damaged. |
| `phdb dump <file> [--table T]... [--prefix P] [--durable]` | Export every cell, with all retained versions, at one snapshot, to standard output: `jsonl` (the default), `csv`, or `arrow` (an Arrow IPC stream of one table). |
| `phdb load <file> [<dump.jsonl> \| -] [--create]` | Load a `jsonl` dump: create its tables and families and write every cell at its timestamp. |
| `phdb backup <file> <dest> [--latest] [--check]` | Copy the database into one new file, consistent at one snapshot, also while your application runs. |
| `phdb compact <file> [--shrink]` | Full compaction, with progress; optionally release free space at the file's end. |
| `phdb shell <file> [-c CMD]... [-f SCRIPT] [--read-only]` | An interactive shell: get, scan, put, incr, delete, create and snapshots. |

**Global flags,** before or after the command:

| Flag | Meaning |
|---|---|
| `--format table\|json\|jsonl\|csv\|arrow` | Output format (default `table`; for `dump`, `jsonl`). `arrow` is for `dump` only. |
| `--shm-dir <DIR>` | The shared-memory directory the database's writer uses (`Options::shm_dir`), if not the default. |
| `--reader` | Open only as a reader process; fail unless a writer process has the database open. |
| `--writer` | Open only as the writer; fail if a writer process has it open. |
| `--help`, `--version` | Help (also per command: `phdb info --help`) and version. |

## How `phdb` opens a database

`check` is the exception: it never opens the database (see [`phdb check`](#phdb-check)). The rest:

A database has one writer process at a time and any number of reader processes (see [Concepts](concepts.md#multi-process-readers-phase-4-available-now)). A read-only command such as `info` works either way:

- **No writer running:** `phdb` opens the database as the writer for the length of the command, then closes it.
  - If the last writer crashed, opening **replays its WAL**, exactly as your application's next open would, like `sqlite3` rolling back a hot journal. The close then checkpoints it, so the database is one file again.
  - While `phdb` holds the writer lock, your application can't open as the writer: its open fails with `WriterLocked` until `phdb` exits.
- **A writer is running:** `phdb` opens as a reader process. It sees everything the writer has committed and changes nothing.

`phdb` opens with one unpinned shard and a 16 MiB block cache (as the writer, with no row cache), and never creates a file. A database `phdb` can't write opens read-only (`read_only_fallback`): families that name a merge operator `phdb` doesn't know, or a feature from a newer version that this one may only read. Such an open writes nothing to the file.

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
| `read_only` | Whether the open can't write: `reader` mode, or a writer that fell back to read-only (see below). |
| `format_version` | The file format version. A read-only open leaves an older file's version as it is. |
| `features` | The file's feature bits, `{"compat", "ro_compat", "incompat"}`: all 0 until a feature added after 1.0 is used. |
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
read_only       false
format_version  4
features        {"compat":0,"ro_compat":0,"incompat":0}
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

### `phdb check`

`phdb check` reads every structure of the file and reports what is damaged:
- both superblock slots;
- the manifest, which is the catalog of tables and files;
- every extent the manifest references (valid, inside the file, not overlapping);
- every block of every SST, with its checksum verified, key order, and agreement with the manifest;
- every blob record;
- the WAL files a crash left, read as the next open would replay them.

It is the library's [`pigeonhole::check`](agent-reference.md#check-offline-integrity-check-icr-0048).

- **Offline, and it changes nothing.** It takes the writer lock, so it can't run while a writer process has the database open (exit 4). It never replays the WAL and never writes: a crashed database is checked as it was left. The lock needs write access to the file, so to check a copy on read-only media, copy it somewhere writable first.
- **A file from a newer build:** a newer format version, or an incompatible feature this `phdb` doesn't know (in a superblock or a WAL segment header), is refused with `UnsupportedFormat` or `UnsupportedFeature` (status 6), not reported as damage. Check it with a `phdb` at least as new.
- **`--online`:** if a writer process has the database open, read every table through a reader process instead, all versions at one snapshot. It's a weaker check: it reaches only the data a scan reads, not index or filter blocks, the other superblock slot or the WAL. With no writer running, `--online` runs the offline check.
- **`--deep`:** also read every separated value (blob) through its pointer, and recount each blob file's live bytes.
- **`--map`:** list the file's regions instead of the findings: superblock pages, the manifest, each SST with its table, family and level, and each blob extent. In `json` they're added as `regions`.
- **Exit status:** 3 if any finding is `corruption`. Warnings and info findings exit 0, except with `--online`: a read that failed for another reason is a `warning` and exits 1.

**A finding** (each row of `table`, `csv` and `jsonl`, and each entry of `findings` in `json`):

| Field | Meaning |
|---|---|
| `structure` | `superblock`, `manifest`, `allocation` (extents), `sst`, `blob` or `wal`. With `--online`: `data`. |
| `severity` | `corruption`: damaged. `warning`: something a crash can also leave; the message says which. `info`: expected after a crash, such as a torn WAL tail. |
| `object` | Which one: the superblock slot, SST id, blob file id or WAL stream. |
| `offset` | Where, in bytes: in the database file, or in the WAL file for `wal`. |
| `message` | What is wrong, for people. The wording may change. |

After a crash, a WAL tail with valid records after a gap is a `warning`, not damage. A power loss that tears the last unsynced write leaves the same picture, and the WAL doesn't record what was synced. Damage inside the WAL is `corruption` when a later segment proves the lost records were synced.

**Summary fields** (the `json` document's top level, and the `jsonl` header):

| Field | Meaning |
|---|---|
| `path`, `mode` | The file, and `offline` or `online`. |
| `result` | `ok` or `corrupt`. |
| `corruption`, `warnings`, `info` | Findings by severity. |
| `deep` | Whether `--deep` was given. |
| `clean_shutdown` | The newest root's clean flag: no root commit since a clean close. A database reopened and then crashed before its first flush still has it set. |
| `file_bytes` | The file's length. |
| `superblocks`, `tables`, `ssts`, `sst_entries`, `blob_files`, `blob_records`, `blob_pointers`, `wal_files`, `wal_records` | How much was read. |

In `json` only:
- `wal`: per WAL file, `stream`, `path`, `len`, `segment_size`, `segments`, `records` (what the next open replays), `checkpoint` and `end` (each `epoch` and `offset`), and `end_slot`.
- With `--map`, `regions`: each with `kind` (`superblock`, `manifest-snapshot`, `manifest-log`, `sst`, `blob`), `offset` and `len`. A superblock adds `slot`. An SST adds `sst`, `table`, `family`, `level` and `used` (its bytes in use). A blob extent adds `blob_file`, `index` and `family`.

`--online` reports `tables`, `rows` and `cells` read, and per finding `table`, `code` (the `ErrorCode`) and `message`.

Example of a damaged file (the summary is cut):

```text
$ phdb check app.phdb
path            app.phdb
mode            offline
result          corrupt
corruption      1
...

structure  severity    object  offset  message
blob       corruption  1       262208  blob file 1: the record at 0: blob record: checksum mismatch
$ echo $?
3
```

### `phdb dump` and `phdb load`

`phdb dump` exports every cell of every table, or of the `--table`s named, with every retained version. It reads them all at one snapshot, so the export is consistent even while a writer runs: it then reads through a reader process. `--durable` reads at a durable snapshot instead, holding only commits durable at their level. `--prefix` keeps the rows starting with a prefix. Deleted cells and expired ones aren't exported.

**`jsonl`** (the default) is what `phdb load` reads. Its lines are, in order:

1. **A header:** `{"phdb": 1, "command": "dump", "format_version", "snapshot": "latest" | "durable", "seqno", "mode"}`.
2. **Per table, before its cells:** `{"table", "families": [...]}`. Each family has the same fields as in `phdb info`: `name`, `counter`, `max_versions`, `ttl_micros`, `bloom_bits`, `blob_threshold`, `compression`, `zstd_level`, `block_size`, `merge_operator`, `cache_priority` and `compaction`.
3. **Per cell:** `{"table", "row", "family", "qualifier", "ts", "value"}`. Within a row, cells are ordered by family, then qualifier, newest version first.

How values are written:
- **Bytes** (`row`, `qualifier`, and a byte `value`) are a JSON string when they are valid UTF-8, and `{"base64": "…"}` otherwise.
- **Typed values** are `{"i64": n}` (also every counter) or `{"f64": x}` (`{"f64_bits": n}` for a NaN or an infinity).
- **`ts`** is microseconds since the Unix epoch.

**`csv`** is for spreadsheets and other tools; `phdb load` doesn't read it.
- The header is `table,row,family,qualifier,ts,type,value`.
- `type` is `bytes`, `base64` (a value that isn't UTF-8), `i64` or `f64`.
- A row key or qualifier that isn't UTF-8 prints as `b64:` followed by base64. A key that really starts with `b64:` is ambiguous here, so use `jsonl` to round-trip.

**`arrow`** is an [Arrow IPC stream](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) of one table, for DuckDB, Polars, pyarrow and other Arrow readers (#691). Name the table with `--table`: a stream has one schema, so `dump --format arrow` without exactly one table fails with status 2.
- **Schema:** the [Arrow export's](arrow.md) long schema, one Arrow row per cell: `row`, `family`, `qualifier`, `ts` (a UTC microsecond timestamp), `value` (the bytes, or a typed value's little-endian encoding), `value_type` (`bytes`, `i64`, `f64`, `varint`), and the typed columns `value_i64` (i64 and varint cells, so every counter) and `value_f64`, null for other cells.
- **Order:** as `jsonl`. Every retained version, at one snapshot.
- **Limit:** a timestamp above `i64::MAX` doesn't fit an Arrow timestamp: the dump fails with status 6 (`Unsupported`), naming the row. `jsonl` dumps it. Any other failure of the Arrow writer is status 1 (`Export`).
- **Check the status.** A dump that fails partway, in any format, leaves what it wrote so far on standard output, and an Arrow stream cut short has no end-of-stream marker: pyarrow and Polars read it as a shorter table without complaint. Treat the output as complete only when `phdb` exits 0.

```text
$ phdb dump --format arrow --table users app.phdb > users.arrows
$ python3 -c 'import polars as pl; print(pl.read_ipc_stream("users.arrows").group_by("family").len())'
$ pip install polars duckdb pyarrow pytz    # DuckDB needs pytz for the UTC timestamp column
$ python3 -c 'import duckdb, pyarrow.ipc as ipc; t = ipc.open_stream("users.arrows").read_all(); print(duckdb.sql("select count(*) from t"))'
```

CI dumps a table this way and reads it with Polars.

**`phdb load`** writes a `jsonl` dump into a database, from a file or standard input:
- **Writer only:** it opens the database as the writer, so it fails with status 4 while another writer runs. `--create` creates the file if it's missing, but only once the input's first line is a dump header: an input that isn't a dump, an empty one included (a `phdb dump` that failed into a pipe writes nothing), fails with status 2 and creates nothing. A dump with only its header creates an empty database: `echo '{"phdb":1,"command":"dump"}' | phdb load --create app.phdb -` (`phdb` has no other command that creates a file).
- **Tables and families:** each table line creates its table and families. An existing family keeps the options stored in the file.
- **Cells:** each is written at its timestamp. Counters are set to their dumped value. A family with the built-in `pigeonhole.i64_add` operator gets its merged values as puts. `phdb` registers no other merge operator, so a family that names one can't be dumped (its first merged cell fails with `UnknownMergeOperator`, status 1) or created by `load`.
- **Durability:** cells are committed in batches at `Durability::None`, and one flush at the end makes the load durable. If the load fails partway, what was committed stays.

A malformed dump fails with status 1 and the code `BadInput`, naming the line.

Round trip:

```text
$ phdb dump app.phdb > app.jsonl
$ phdb load --create copy.phdb app.jsonl
$ phdb dump copy.phdb | tail -n +2 | diff - <(tail -n +2 app.jsonl)    # identical
```

### `phdb backup` and `phdb compact`

`phdb compact` opens the database as the writer, so it fails with status 4 while your application has it open. `phdb backup` follows the read commands' [open policy](#how-phdb-opens-a-database). With no writer running it opens as the writer. While your application runs, it copies from a reader process (`PigeonholeReader::backup_durable`, or `backup` with `--latest`; ICR 0049).

**The cost of a backup from a reader process:** its snapshot pins the whole view in shared memory, including the application's memtables, until the copy is done. On a write-heavy database the application's memtables can then fill, and its commits wait (up to `write_stall_timeout`, then `Busy`). Back up a busy database when writes are light. Or call `Pigeonhole::backup_durable` from the application itself: the writer's own backup releases memtables as soon as it has copied them.

**`phdb backup <file> <dest>`** writes a consistent single-file copy to `dest`, which must not exist. The copy needs no WAL replay to open. It's `Pigeonhole::backup_durable`, a copy at a durable snapshot holding only commits durable at their level. `--latest` uses `Pigeonhole::backup` instead, which also copies commits not yet durable. `--check` then runs `phdb check` on the copy and exits 3 if it's damaged.

| Field | Meaning |
|---|---|
| `path`, `dest` | The database and the copy. |
| `mode` | `writer` or `reader`: how `phdb` opened the database. |
| `snapshot` | `durable` or `latest`. |
| `bytes` | The copy's size: about the size of the live data. The copy keeps none of the source's free space. |
| `elapsed_ms` | How long the copy took. |
| `check` | With `--check`: `ok` or `corrupt`. |

**`phdb compact <file>`** is `Pigeonhole::compact`: it flushes, then merges every level of every table into the last. The merge drops versions past `max_versions`, expired cells and deleted data, as the engine's compaction rules allow. The API reports no progress, so with `--format table` `phdb` prints one line every half second to standard error (`compacting: 2.5 s, 1 flushes, 3 compactions done`). Other formats print nothing there. `--shrink` then runs `Pigeonhole::shrink`, which releases free space at the end of the file. If a reader process attached to `phdb` holds a snapshot, it gives back less; run it again once the snapshot is dropped.

| Field | Meaning |
|---|---|
| `elapsed_ms` | The compaction's wall time. |
| `flushes`, `compactions` | Flushes and compactions finished while it ran. |
| `file_bytes_before`, `file_bytes` | The file's length before and after. |
| `released_bytes` | With `--shrink`: the bytes released (`null` otherwise). |
| `free_bytes` | Free space left in the file afterwards. |

### `phdb shell`

The shell runs one command per line, each a single API call with flags. It isn't SQL: there are no joins, no `WHERE` and no expressions.

**Where the commands come from:**
- the `-c` commands (repeat for several, run in order);
- the `-f` script file;
- whatever is piped to standard input;
- otherwise a terminal, interactively: a `phdb> ` prompt with no line editing or history. Run `rlwrap phdb shell app.phdb` for both (#811 tracks a built-in editor).

In `-c`, script or piped mode, the first failing command stops the run with its exit status, and the message names the line. Interactively, an error prints and the session goes on.

**Writes:** with no writer running, the shell opens as the writer and can write. While your application runs, it opens as a reader process and refuses writes (`ReadOnly`, exit 1). `--read-only` refuses them in either case.

```text
get    <table> <row> [<family>[:<qualifier>]] [--versions N] [--time A..B] [--at SNAP]
scan   <table> [<start> [<end>]] [--prefix P] [--family F]... [--qualifier-prefix Q]
       [--value-eq V | --value-prefix V | --i64 <op><n>] [--versions N] [--time A..B]
       [--limit N] [--columns-per-row N] [--at SNAP]
put    <table> <row> <family>:<qualifier> <value> [--ts US] [--i64 | --f64]
incr   <table> <row> <family>:<qualifier> <delta> [--ts US]
delete <table> <row> [<family>[:<qualifier>]] [--ts US]
create <table> <family>[:counter]...
snapshot [--durable] <name>     release <name>
\tables  \schema [table]  \info  \metrics  \health
\format table|json|jsonl|csv  \timing on|off  \help  \quit
```

**Reading:**
- **`get`** reads one row: all of it, one family, or one column.
- **`scan`** reads a row range (start inclusive, end exclusive) or a `--prefix`.
- **The read flags** map to the API's `RowRead` and `Scan` builders:
  - `--versions 0` returns every retained version;
  - `--time A..B` is a half-open microsecond range;
  - `--i64 >30` (also `<n` and `=n`) keeps `i64` values that compare as given;
  - `--columns-per-row N` is `Scan::column_limit(N)`.
- **`--at`** reads at a named snapshot. `snapshot` takes one (`--durable` for a durable one); `release` drops it.
- **Output:** reads print one row per cell, `row`, `family`, `qualifier`, `ts`, `value`, in the current `\format`.

**Writing:**
- **`put`** writes bytes, or with `--i64` or `--f64` a typed value. `--ts` sets the timestamp.
- **`incr`** adds to a counter.
- **`delete`** removes a row, a family, a column, or with `--ts` one version.
- **`create`** makes a table with default family options. Append `:counter` to a family name for a counter family.

**Values:** words are bytes, so keys and values can be anything.
- A bare word is its UTF-8.
- `"…"` takes escapes: `\n`, `\t`, `\r`, `\0`, `\\`, `\"` and `\xHH`.
- `'…'` is literal.
- `0x…` is hex, and `b64:…` is base64.
- In `table` and `csv` output, bytes that aren't UTF-8 print as `b64:…`.

**Meta commands:**
- **`\tables`** lists the tables; **`\schema`** shows the families' options; **`\info`** is `phdb info`.
- **`\metrics`** shows the writer's counters since the shell opened. It needs the shell to be the writer: a reader process has no writer counters. **`\health`** shows whether the instance failed (always `false` in a reader).
- **`\format`** changes the output format, and **`\timing on`** prints each command's time to standard error.

In `json`, every command prints one document: reads put their cells under `rows`, and writes print `result` and `seqno`.

```text
$ phdb shell app.phdb -c "create users profile" -c "put users u1 profile:name Ada" -c "get users u1"
created  users
result  ok
seqno   3
row  family   qualifier  ts                value
u1   profile  name       1761030000000000  Ada
```

## Exit status

| Status | Meaning |
|---|---|
| 0 | Success. |
| 1 | An error not listed below (see the error's code). |
| 2 | Usage: bad arguments, or a `phdb load` input that isn't a dump (no `phdb dump` header, an empty input included). |
| 3 | Corruption: the file is damaged (`ErrorCode::Corruption`, or `phdb check` found damage). |
| 4 | Locked or busy: another process holds the writer lock (`WriterLocked`), the database stayed too busy (`Busy`), every reader slot is taken (`NoReaderSlot`), or `--reader` found no running writer to attach to (`NoWriter`). |
| 5 | Not found: the file, or a table or family the command names. |
| 6 | Unsupported: the file's format version is newer than this `phdb` (`UnsupportedFormat`), the file uses a feature this `phdb` lacks (`UnsupportedFeature`), or the writer runs another shared-memory layout (`ShmVersionMismatch`), or the system lacks a feature (`Unsupported`). |

**Errors** go to standard error. With `--format table` or `csv` they print as one line, `phdb: error: <message>`. With `--format json` or `jsonl` they print as one JSON object:

```json
{"phdb":1,"error":{"code":"WriterLocked","code_num":3,"message":"open: another process holds the writer lock"}}
```

`code` is the [`ErrorCode`](errors.md) name and `code_num` its number. For a failure outside the database, the code is a `phdb` name with `code_num` `null`. The codes are `FileNotFound` (a missing file), `BadInput` (input `phdb` can't use: a dump `phdb load` can't read, status 2 without a dump header and 1 otherwise; a `backup` destination that exists; an unknown snapshot name in the shell; status 1), `Unsupported` (a cell `dump --format arrow` can't hold; status 6), `Export` (the Arrow writer failed otherwise; status 1) and `Usage` (a combination of flags clap doesn't catch, such as `dump --format table`). The message is for people; its wording may change.

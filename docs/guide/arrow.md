# Arrow export

The `pigeonhole-arrow` crate turns a scan into [Apache Arrow](https://arrow.apache.org/) record batches, so you can hand a table to DuckDB, Polars, DataFusion or anything else that reads Arrow. It reads through the public API only, so the core crates never depend on Arrow.

```toml
[dependencies]
pigeonhole = "1"
pigeonhole-arrow = "1"
```

## A scan as batches
`scan_to_arrow(scan, &options)` takes a `Scan` you've configured as usual, and returns an `ArrowScan`. That's an `Iterator` of `Result<RecordBatch, ArrowError>`, and it's an Arrow `RecordBatchReader`. Batches are built as you pull them, so a large scan is never held in memory all at once.

```rust
use pigeonhole::{Family, Options, Pigeonhole};
use pigeonhole_arrow::{ArrowOptions, scan_to_arrow};
use pigeonhole_arrow::arrow_array::cast::AsArray;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
let t = db.table("users")?.family("profile", Family::default()).create_if_missing()?;
t.mutate(b"u1").put("profile", b"name", b"Ada").put_i64("profile", b"age", 36).commit()?;
t.mutate(b"u2").put("profile", b"name", b"Grace").commit()?;

let mut names = Vec::new();
for batch in scan_to_arrow(t.scan_prefix(b"u").qualifier_prefix(b"name"), &ArrowOptions::default())? {
    let batch = batch?;
    let values = batch.column_by_name("value").unwrap().as_binary::<i32>();
    names.extend(values.iter().map(|v| v.unwrap().to_vec()));
}
assert_eq!(names, [b"Ada".to_vec(), b"Grace".to_vec()]);
# db.close()?;
# Ok(())
# }
```

**Everything the scan selects passes through unchanged:** families, qualifier prefix and range, `versions(n)`, `time_range`, `value_filter`, `snapshot(&s)` and `limit`. The batches hold exactly a plain scan's cells, in the same order. A reader process exports the same way through `ReadTable::scan`, while the writer keeps running.

## The schema: one Arrow row per cell
| column | type | notes |
|---|---|---|
| `row` | `Binary` | the row key, repeated for each of its cells |
| `family` | `Dictionary<Int32, Utf8>` | |
| `qualifier` | `Binary` | |
| `ts` | `Timestamp(Microsecond, "UTC")` | the version's timestamp |
| `value` | `Binary` | the bytes, or a typed value's little-endian encoding (what `CellRef::value` returns) |
| `value_type` | `Dictionary<Int8, Utf8>` | `bytes`, `i64`, `f64` or `varint`: with it the export loses nothing |

`pigeonhole_arrow::schema(&options)` returns the schema without scanning. Options change it:

| `ArrowOptions` | Effect |
|---|---|
| `typed_values(true)` | adds nullable `value_i64: Int64` (i64 and varint cells, so counters and min and max families) and `value_f64: Float64` (f64 cells) |
| `utf8_keys(true)` | `row` and `qualifier` as `Utf8`. A key that isn't valid UTF-8 fails its batch with an error naming the key; nothing is replaced |
| `row_dictionary(true)` | `row` as `Dictionary<Int32, Binary>` (or `Utf8`): each key stored once per batch |
| `ts(TsType::UInt64)` | `ts` as stored. With the default `Timestamp`, a timestamp above `i64::MAX` fails its batch with an error naming the row |
| `batch_cells(n)`, `batch_bytes(n)` | a batch closes at `n` cells (default 8192) or `n` bytes of keys and values (default 8 MiB, at most 1 GiB), whichever comes first. A wide row can span batches |

```rust
use pigeonhole::{Family, Options, Pigeonhole};
use pigeonhole_arrow::{ArrowOptions, scan_to_arrow};
use pigeonhole_arrow::arrow_array::cast::AsArray;
use pigeonhole_arrow::arrow_array::types::Int64Type;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
let t = db.table("stats")?.family("c", Family::counter()).create_if_missing()?;
t.mutate(b"page:1").incr("c", b"views", 3).commit()?;
t.mutate(b"page:1").incr("c", b"views", 4).commit()?;

let options = ArrowOptions::default().typed_values(true).utf8_keys(true);
let batch = scan_to_arrow(t.scan_prefix(b""), &options)?.next().unwrap()?;
let views = batch.column_by_name("value_i64").unwrap().as_primitive::<Int64Type>();
assert_eq!(views.value(0), 7);
assert_eq!(batch.column_by_name("row").unwrap().as_string::<i32>().value(0), "page:1");
# db.close()?;
# Ok(())
# }
```

## Keeping the stream: `into_owned`
An `ArrowScan` borrows its table handle, like a `RowIter`. To pass the stream somewhere that keeps it, such as another thread, a `Box<dyn RecordBatchReader + Send>` or an Arrow consumer, start from `Scan::into_owned()`. The stream is then `Send + 'static`, and it keeps the database open the way a clone of the table handle would.

```rust
use pigeonhole::{Family, Options, Pigeonhole};
use pigeonhole_arrow::arrow_array::RecordBatchReader;
use pigeonhole_arrow::{ArrowOptions, scan_to_arrow};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
t.mutate(b"r").put("f", b"q", b"v").commit()?;

let reader: Box<dyn RecordBatchReader + Send> =
    Box::new(scan_to_arrow(t.scan_prefix(b"").into_owned(), &ArrowOptions::default())?);
let cells = std::thread::spawn(move || reader.map(|b| b.unwrap().num_rows()).sum::<usize>())
    .join()
    .unwrap();
assert_eq!(cells, 1);
# db.close()?;
# Ok(())
# }
```

## Cost
Each row key, qualifier and value is copied once, into the batch's Arrow buffers. Nothing is allocated per cell: allocations are a fixed number per batch, and a test holds the export to that. Zero copy from the database's blocks isn't possible, because blocks hold encoded keys with values interleaved, while Arrow needs each column contiguous.

Measured with `cargo run --release -p pigeonhole-bench --example arrowexport -- DIR 128`: one thread, a warm block cache, 128 MiB per shape, median of five passes, on an Apple M5 (macOS, 2026-10-21). GB/s counts key, qualifier and value bytes:

| shape | plain scan (`next_ref`) | export | export cells/s |
|---|---|---|---|
| narrow: one 24-byte value per row | 0.47 GB/s | 0.39 GB/s | 9.2 M |
| wide: 32 columns of 24 bytes per row | 0.85 GB/s | 0.66 GB/s | 15 M |
| large: one 4000-byte value per row | 12.9 GB/s | 10.1 GB/s | 2.5 M |
| counters: 8 `i64` columns, `typed_values` | 0.48 GB/s | 0.36 GB/s | 13 M |

The export costs about a fifth to a quarter on top of the scan that feeds it. The scan, not the copy, sets the pace for small cells.

## arrow-rs versions
`pigeonhole-arrow` pins one arrow-rs major (60) and re-exports the crates it uses: `pigeonhole_arrow::arrow_array`, `arrow_buffer` and `arrow_schema`. Use those re-exports, or the same major, to name the types. A consumer built on another major (duckdb-rs pins its own, and Polars uses polars-arrow) takes the stream through the Arrow C stream interface instead, below.

## Handing off: the Arrow C stream (feature `ffi`)
The [Arrow C stream interface](https://arrow.apache.org/docs/format/CStreamInterface.html) passes a stream between libraries without sharing an arrow-rs version, or a language. With the `ffi` feature, `ArrowScan<'static>::into_ffi_stream()` gives an `FFI_ArrowArrayStream`: start from `Scan::into_owned()`, then hand the stream over. The consumer owns it and calls its `release` callback when done.

```toml
pigeonhole-arrow = { version = "1", features = ["ffi"] }
```

```rust,ignore
// The `into_ffi_stream` rustdoc example runs this with arrow-rs's own C stream reader.
let stream = scan_to_arrow(table.scan_prefix(b"").into_owned(), &ArrowOptions::default())?
    .into_ffi_stream();
// A C or C++ consumer, or a binding such as pyarrow's `RecordBatchReader._import_from_c`,
// takes the pointer and owns the stream from then on:
let ptr: *mut FFI_ArrowArrayStream = Box::into_raw(Box::new(stream));
```

The feature adds `libc`; `pigeonhole-arrow` itself uses no `unsafe`. Taking the pointer back on the consumer's side is the consumer's `unsafe`.

- **After `close`:** a stream still held gets `Closed` from its next `get_next` (the message is "the database is closed") and ends. `release` is still the consumer's to call, and is safe.
- **Threads:** the stream is `Send`, not `Sync`. The consumer calls it from one thread at a time, which may change, as the C stream interface requires.
- **Panics:** a panic inside `get_next` aborts the process, because it's an `extern "C"` callback. The export doesn't panic on any input it accepts.
- **A stream cut short** by an error leaves the batches already taken; check the error before treating the data as complete.

## DuckDB and Polars without Rust: `phdb dump --format arrow`
The [`phdb`](cli.md#phdb-dump-and-phdb-load) tool writes one table as an Arrow IPC stream in this schema, with the typed columns, every retained version, at one snapshot:

```text
$ phdb dump --format arrow --table users app.phdb > users.arrows
```

```text
>>> import polars as pl
>>> pl.read_ipc_stream("users.arrows").group_by("family").len()

>>> # pip install duckdb pyarrow pytz: DuckDB needs pytz for the UTC timestamp column
>>> import duckdb, pyarrow.ipc as ipc
>>> users = ipc.open_stream("users.arrows").read_all()
>>> duckdb.sql("select decode(row) as row, value_i64 from users where family = 'stats'")
```

CI dumps a table this way and checks it with Polars (`scripts/arrow-smoke.py`).

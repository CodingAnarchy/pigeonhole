# Arrow export

The `pigeonhole-arrow` crate turns a scan into [Apache Arrow](https://arrow.apache.org/) record batches, so you can hand a table to DuckDB, Polars, DataFusion or anything else that reads Arrow. It reads through the public API only, so the core crates never depend on Arrow.

> **Not released yet:** this is on `main` (#691). It ships in the next release.

```toml
[dependencies]
pigeonhole = "0.4"
pigeonhole-arrow = "0.4"
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
| `typed_values(true)` | adds nullable `value_i64: Int64` (i64 and varint cells, so counters) and `value_f64: Float64` (f64 cells) |
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

## arrow-rs versions
`pigeonhole-arrow` pins one arrow-rs major (60) and re-exports the crates it uses: `pigeonhole_arrow::arrow_array`, `arrow_buffer` and `arrow_schema`. Use those re-exports, or the same major, to name the types. A consumer built on another major (duckdb-rs pins its own, and Polars uses polars-arrow) takes the stream through the Arrow C stream interface instead; that's the next step on `main` (#691).

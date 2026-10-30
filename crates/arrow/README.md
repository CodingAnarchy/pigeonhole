# pigeonhole-arrow

Arrow `RecordBatch` export for [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole) scans: `scan_to_arrow` turns a configured `pigeonhole::Scan` into a streaming `RecordBatchReader`, one Arrow row per cell (row, family, qualifier, ts, value, value_type), for handoff to DuckDB, Polars or anything else that reads Arrow. No per-cell allocation; one copy into the Arrow buffers.

```rust
use pigeonhole::{Family, Options, Pigeonhole};
use pigeonhole_arrow::{ArrowOptions, scan_to_arrow};

# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
t.mutate(b"r").put("f", b"q", b"v").commit()?;
for batch in scan_to_arrow(t.scan_prefix(b""), &ArrowOptions::default())? {
    assert_eq!(batch?.num_rows(), 1);
}
db.close()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

(Lines starting with `#` are hidden setup in the rendered docs; this sample runs as a
doctest.)

See the guide's [Arrow page](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/arrow.md) for the schema and options.

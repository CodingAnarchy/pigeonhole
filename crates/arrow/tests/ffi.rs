//! #691 PR 3: an export handed over as an Arrow C stream (feature `ffi`) reads back, through
//! arrow-rs's C stream reader, as the same schema and batches as the export itself, and a
//! failing batch surfaces as the reader's error.
#![cfg(feature = "ffi")]

use std::sync::Arc;

use arrow_array::ffi_stream::ArrowArrayStreamReader;
use arrow_array::{RecordBatch, RecordBatchReader};
use pigeonhole::{Family, Options, Pigeonhole};
use pigeonhole_arrow::{ArrowOptions, scan_to_arrow};
use pigeonhole_io::sim::SimVfs;

#[test]
fn a_c_stream_reads_back_as_the_export() {
    let vfs = SimVfs::new(4);
    let db = Pigeonhole::open(
        "/db/ffi.phdb",
        Options::default().vfs(Arc::clone(&vfs) as _).shards(1),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .family("c", Family::counter())
        .create_if_missing()
        .unwrap();
    for i in 0..50u32 {
        t.mutate(format!("r{i:02}").as_bytes())
            .put("f", b"q", &i.to_le_bytes())
            .put_f64("f", b"x", f64::from(i))
            .incr("c", b"n", i64::from(i))
            .commit()
            .unwrap();
    }
    let o = ArrowOptions::default()
        .typed_values(true)
        .row_dictionary(true)
        .batch_cells(32);
    let direct: Vec<RecordBatch> = scan_to_arrow(t.scan_prefix(b""), &o)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let stream = scan_to_arrow(t.scan_prefix(b"").into_owned(), &o)
        .unwrap()
        .into_ffi_stream();
    let reader = ArrowArrayStreamReader::try_new(stream).unwrap();
    assert_eq!(reader.schema(), pigeonhole_arrow::schema(&o));
    let imported: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
    assert_eq!(imported.len(), 5);
    assert_eq!(imported, direct);

    // A batch that fails (a timestamp above i64::MAX) is the C stream's error.
    t.mutate(b"s")
        .put_at("f", b"q", i64::MAX as u64 + 1, b"v")
        .commit()
        .unwrap();
    let stream = scan_to_arrow(t.scan_prefix(b"s").into_owned(), &ArrowOptions::default())
        .unwrap()
        .into_ffi_stream();
    let mut reader = ArrowArrayStreamReader::try_new(stream).unwrap();
    let e = reader.next().unwrap().unwrap_err().to_string();
    assert!(e.contains("i64::MAX"), "{e}");
    drop(t);
    db.close().unwrap();
}

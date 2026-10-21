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

/// #826 via the C stream: a consumer that holds the stream past `close` gets `Closed` from
/// `get_next`, then the end, and its `release` afterwards is clean, whether it had read a
/// batch or not.
#[test]
fn a_c_stream_held_past_close_reports_closed_and_releases() {
    let vfs = SimVfs::new(5);
    let db = Pigeonhole::open(
        "/db/ffi-close.phdb",
        Options::default().vfs(Arc::clone(&vfs) as _).shards(1),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    for i in 0..100u32 {
        t.mutate(format!("r{i:03}").as_bytes())
            .put("f", b"q", b"v")
            .commit()
            .unwrap();
    }
    let o = ArrowOptions::default().batch_cells(10);
    let stream = |o: &ArrowOptions| {
        ArrowArrayStreamReader::try_new(
            scan_to_arrow(t.scan_prefix(b"").into_owned(), o)
                .unwrap()
                .into_ffi_stream(),
        )
        .unwrap()
    };
    let mut started = stream(&o);
    assert_eq!(started.next().unwrap().unwrap().num_rows(), 10);
    let mut unread = stream(&o);
    let untouched = stream(&o);
    drop(t);
    db.close().unwrap();

    for reader in [&mut started, &mut unread] {
        let e = reader.next().unwrap().unwrap_err().to_string();
        assert!(e.contains("the database is closed"), "{e}");
        assert!(reader.next().is_none(), "the stream ends after the error");
    }
    // Releasing (dropping the reader calls the stream's `release`) is clean after close,
    // also for a stream never read.
    drop(started);
    drop(unread);
    drop(untouched);
}

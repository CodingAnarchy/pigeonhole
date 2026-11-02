//! `scan_to_arrow`'s shape (#691): the schema for each option, batch limits splitting rows,
//! the dictionaries, the errors (an oversized timestamp, a key that is not UTF-8, a scan
//! that cannot start), an owned scan as a `Send + 'static` reader, and a reader process.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, TimestampMicrosecondType};
use arrow_array::{Array, RecordBatchReader};
use arrow_schema::{DataType, TimeUnit};
use pigeonhole::{ErrorCode, Family, Options, Pigeonhole, ReaderOptions, Table};
use pigeonhole_arrow::{ArrowOptions, ArrowScan, TsType, scan_to_arrow};
use pigeonhole_io::sim::SimVfs;

fn open(vfs: &Arc<SimVfs>) -> (Pigeonhole, Table) {
    let db = Pigeonhole::open(
        "/db/x.phdb",
        Options::default().vfs(Arc::clone(vfs) as _).shards(1),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .family("g", Family::default())
        .create_if_missing()
        .unwrap();
    (db, t)
}

fn batch_sizes(export: ArrowScan<'_>) -> Vec<usize> {
    export.map(|b| b.unwrap().num_rows()).collect()
}

#[test]
fn the_schema_follows_the_options() {
    let names = |o: &ArrowOptions| -> Vec<(String, DataType, bool)> {
        pigeonhole_arrow::schema(o)
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
            .collect()
    };
    let dict = |k: DataType, v: DataType| DataType::Dictionary(Box::new(k), Box::new(v));
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    assert_eq!(
        names(&ArrowOptions::default()),
        [
            ("row".into(), DataType::Binary, false),
            (
                "family".into(),
                dict(DataType::Int32, DataType::Utf8),
                false
            ),
            ("qualifier".into(), DataType::Binary, false),
            ("ts".into(), ts, false),
            ("value".into(), DataType::Binary, false),
            (
                "value_type".into(),
                dict(DataType::Int8, DataType::Utf8),
                false
            ),
        ]
    );
    let all = ArrowOptions::default()
        .row_dictionary(true)
        .utf8_keys(true)
        .typed_values(true)
        .ts(TsType::UInt64);
    let n = names(&all);
    assert_eq!(n[0].1, dict(DataType::Int32, DataType::Utf8));
    assert_eq!(n[2].1, DataType::Utf8);
    assert_eq!(n[3].1, DataType::UInt64);
    assert_eq!(
        n[6..],
        [
            ("value_i64".into(), DataType::Int64, true),
            ("value_f64".into(), DataType::Float64, true),
        ]
    );
}

#[test]
fn batch_limits_split_rows_and_dictionaries_hold_per_batch() {
    let vfs = SimVfs::new(1);
    let (db, t) = open(&vfs);
    for r in [&b"r1"[..], b"r2"] {
        let mut m = t.mutate(r);
        for q in 0..5u8 {
            m = m.put(if q < 3 { "f" } else { "g" }, &[b'q', q], &[q; 10]);
        }
        m.commit().unwrap();
    }
    let o = ArrowOptions::default();
    // Ten cells: three per batch splits each row.
    assert_eq!(
        batch_sizes(scan_to_arrow(t.scan_prefix(b""), &o.clone().batch_cells(3)).unwrap()),
        [3, 3, 3, 1]
    );
    // 14 bytes a cell (2 + 2 + 10): a batch closes at or past 30 bytes, so after 3 cells.
    assert_eq!(
        batch_sizes(scan_to_arrow(t.scan_prefix(b""), &o.clone().batch_bytes(30)).unwrap()),
        [3, 3, 3, 1]
    );
    // Even a one-byte limit puts one cell in each batch.
    assert_eq!(
        batch_sizes(scan_to_arrow(t.scan_prefix(b""), &o.clone().batch_bytes(1)).unwrap()).len(),
        10
    );

    // The row dictionary is per batch: a batch of 7 cells over 2 rows stores 2 keys.
    let batches: Vec<_> = scan_to_arrow(
        t.scan_prefix(b""),
        &o.clone().row_dictionary(true).batch_cells(7),
    )
    .unwrap()
    .map(Result::unwrap)
    .collect();
    let rows = batches[0].column(0).as_dictionary::<Int32Type>();
    assert_eq!(rows.values().len(), 2);
    assert_eq!(rows.keys().values().as_ref(), [0, 0, 0, 0, 0, 1, 1]);
    // The family dictionary grows over the scan: each batch's extends the last one's.
    let fam = |i: usize| {
        let d = batches[i].column(1).as_dictionary::<Int32Type>();
        let v = d.values().as_string::<i32>();
        v.iter().map(|s| s.unwrap().to_owned()).collect::<Vec<_>>()
    };
    assert_eq!(fam(0), ["f", "g"]);
    assert_eq!(fam(1), ["f", "g"]);

    // Nothing to scan: no batches.
    assert!(batch_sizes(scan_to_arrow(t.scan_prefix(b"zz"), &o).unwrap()).is_empty());
    assert!(batch_sizes(scan_to_arrow(t.scan_prefix(b"").limit(0), &o).unwrap()).is_empty());
    drop(t);
    db.close().unwrap();
}

#[test]
fn errors_name_what_does_not_fit_and_end_the_stream() {
    let vfs = SimVfs::new(2);
    let (db, t) = open(&vfs);
    let big = i64::MAX as u64 + 1;
    t.mutate(b"a").put_at("f", b"q", 7, b"v").commit().unwrap();
    t.mutate(b"b")
        .put_at("f", b"q", big, b"v")
        .commit()
        .unwrap();
    t.mutate(b"\xff").put("f", b"q", b"v").commit().unwrap();

    // A timestamp above i64::MAX fails the batch it would go in, naming the row.
    let mut export =
        scan_to_arrow(t.scan_prefix(b""), &ArrowOptions::default().batch_cells(1)).unwrap();
    let first = export.next().unwrap().unwrap();
    assert_eq!(
        first
            .column(3)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        7
    );
    let e = export.next().unwrap().unwrap_err().to_string();
    assert!(e.contains(&big.to_string()), "{e}");
    assert!(e.contains("\"b\""), "{e}");
    assert!(export.next().is_none(), "the stream ends after an error");
    // As UInt64 it is exported as stored.
    let cells: usize = scan_to_arrow(
        t.scan_prefix(b""),
        &ArrowOptions::default().ts(TsType::UInt64),
    )
    .unwrap()
    .map(|b| b.unwrap().num_rows())
    .sum();
    assert_eq!(cells, 3);

    // utf8_keys: a row key that is not UTF-8 fails its batch, named in hex.
    let e = scan_to_arrow(
        t.scan_prefix(b"\xff"),
        &ArrowOptions::default().utf8_keys(true),
    )
    .unwrap()
    .next()
    .unwrap()
    .unwrap_err()
    .to_string();
    assert!(e.contains("0xff") && e.contains("row key"), "{e}");

    // A scan that cannot start fails at scan_to_arrow.
    let e = scan_to_arrow(t.scan_prefix(b"").family("nope"), &ArrowOptions::default()).unwrap_err();
    assert_eq!(e.code(), ErrorCode::FamilyNotFound);
    drop(t);
    db.close().unwrap();
}

#[test]
fn an_owned_export_is_a_send_static_reader_on_the_writer_and_a_reader_process() {
    let vfs = SimVfs::new(3);
    let (db, t) = open(&vfs);
    for i in 0..100u32 {
        t.mutate(format!("r{i:03}").as_bytes())
            .put_i64("f", b"n", i64::from(i))
            .put_f64("g", b"x", f64::from(i) / 2.0)
            .commit()
            .unwrap();
    }
    let o = ArrowOptions::default().typed_values(true).batch_cells(64);
    let reader: Box<dyn RecordBatchReader + Send> =
        Box::new(scan_to_arrow(t.scan_prefix(b"").into_owned(), &o).unwrap());
    drop(t);
    let (sum, nulls) = std::thread::spawn(move || {
        let mut sum = 0i64;
        let mut nulls = 0;
        for b in reader {
            let b = b.unwrap();
            let ints = b
                .column_by_name("value_i64")
                .unwrap()
                .as_primitive::<arrow_array::types::Int64Type>();
            sum += ints.iter().flatten().sum::<i64>();
            nulls += ints.null_count();
        }
        (sum, nulls)
    })
    .join()
    .unwrap();
    assert_eq!(sum, (0..100).sum::<i64>());
    assert_eq!(nulls, 100, "the f64 cells have no i64");

    let r = Pigeonhole::open_reader(
        "/db/x.phdb",
        ReaderOptions::default().vfs(Arc::clone(&vfs) as _),
    )
    .unwrap();
    let rt = r.table("t").unwrap();
    let cells: usize = scan_to_arrow(rt.scan_prefix(b"r05"), &o)
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum();
    assert_eq!(cells, 20);
    drop(rt);
    drop(r);
    db.close().unwrap();
}

/// Vector cells (D303, ICR 0108): never refused. `value_type` is `vector` and `value` is
/// the stored encoding. With `vector_dim`, `value_vector` holds each vector's components
/// widened exactly, null for other cells and for bit vectors, and a vector of another
/// dimension fails the batch.
#[test]
fn vector_cells_export_exactly_and_fill_value_vector() {
    use arrow_array::types::{Float32Type, Int8Type};
    use pigeonhole::{Element, Metric, VectorSpec};
    let vfs = SimVfs::new(4);
    let (db, _) = open(&vfs);
    let spec = |d, e, m| Family::vector(VectorSpec::new(d, e, m).unwrap());
    let t = db
        .table("v")
        .unwrap()
        .family("a_f32", spec(4, Element::F32, Metric::L2))
        .family("b_f16", spec(4, Element::F16, Metric::L2))
        .family("c_i8", spec(4, Element::I8, Metric::L2))
        .family("d_bit", spec(32, Element::Bit, Metric::Hamming))
        .family("e_plain", Family::default())
        .create()
        .unwrap();
    t.mutate(b"r")
        .put_vector_f32("a_f32", b"", &[0.5, -1.0, 3.25, 0.0])
        // f16: 1.5, 65,504, the smallest subnormal, -0.
        .put_vector_f16("b_f16", b"", &[0x3e00, 0x7bff, 0x0001, 0x8000])
        .put_vector_i8("c_i8", b"", &[-128, -1, 0, 127])
        .put_vector_bits("d_bit", b"", &[1, 2, 3, 4])
        .put("e_plain", b"", b"bytes")
        .commit()
        .unwrap();

    // Without vector_dim: no extra column; the value is the stored encoding, exactly.
    let batches: Vec<_> = scan_to_arrow(t.scan_prefix(b""), &ArrowOptions::default())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let batch = &batches[0];
    assert!(batch.column_by_name("value_vector").is_none());
    let types = batch.column_by_name("value_type").unwrap();
    let types = types.as_dictionary::<Int8Type>();
    let names = types.values().as_string::<i32>();
    let value = batch.column_by_name("value").unwrap();
    let value = value.as_binary::<i32>();
    let row = t.row(b"r").read().unwrap().unwrap();
    for (i, e) in row.iter().enumerate() {
        let kind = names.value(types.keys().value(i) as usize);
        let want = if e.family == "e_plain" {
            "bytes"
        } else {
            "vector"
        };
        assert_eq!(kind, want, "{}", e.family);
        assert_eq!(value.value(i), e.cell.value(), "{}", e.family);
    }

    // With vector_dim(4): the 4-dim vectors' components, widened exactly.
    let opts = ArrowOptions::default().vector_dim(4);
    let batches: Vec<_> = scan_to_arrow(
        t.scan_prefix(b"")
            .families(["a_f32", "b_f16", "c_i8", "e_plain"]),
        &opts,
    )
    .unwrap()
    .collect::<Result<_, _>>()
    .unwrap();
    let batch = &batches[0];
    assert_eq!(batch.schema(), pigeonhole_arrow::schema(&opts));
    let vectors = batch.column_by_name("value_vector").unwrap();
    let vectors = vectors.as_fixed_size_list();
    assert_eq!(vectors.value_length(), 4);
    let comps = |i: usize| -> Vec<f32> {
        vectors
            .value(i)
            .as_primitive::<Float32Type>()
            .values()
            .to_vec()
    };
    assert_eq!(comps(0), [0.5, -1.0, 3.25, 0.0]);
    let f16 = comps(1);
    assert_eq!(f16[..3], [1.5, 65504.0, 2f32.powi(-24)]);
    assert_eq!(f16[3].to_bits(), (-0.0f32).to_bits());
    assert_eq!(comps(2), [-128.0, -1.0, 0.0, 127.0]);
    assert!(vectors.is_null(3), "a plain cell is null in value_vector");
    assert_eq!(vectors.null_count(), 1);

    // A bit vector is null there; a vector of another dimension fails the batch.
    let bits: Vec<_> = scan_to_arrow(
        t.scan_prefix(b"").family("d_bit"),
        &ArrowOptions::default().vector_dim(32),
    )
    .unwrap()
    .collect::<Result<_, _>>()
    .unwrap();
    assert!(bits[0].column_by_name("value_vector").unwrap().is_null(0));
    let wrong = scan_to_arrow(
        t.scan_prefix(b"").family("a_f32"),
        &ArrowOptions::default().vector_dim(8),
    )
    .unwrap()
    .collect::<Result<Vec<_>, _>>();
    let err = wrong.unwrap_err().to_string();
    assert!(
        err.contains("4-dimension vector") && err.contains("holds 8"),
        "{err}"
    );
    drop(t);
    db.close().unwrap();
}

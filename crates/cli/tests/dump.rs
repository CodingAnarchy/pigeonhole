//! `phdb dump` and `phdb load` (#692): a dump loaded into a new database dumps the same, cell
//! for cell; snapshot tests of both formats; the errors and exit statuses. `dump --format
//! arrow` (#691): an Arrow IPC stream of one table, cell for cell the jsonl dump's.

mod common;

use std::path::{Path, PathBuf};

use common::{TempDir, phdb};
use pigeonhole::{Compaction, Family, Options, Pigeonhole, Priority, days};

const T0: u64 = 1_760_000_000_000_000;

fn options() -> Options {
    Options::default().shards(1).pin_threads(false)
}

/// Two tables with every kind of cell, at fixed timestamps:
/// - `a`: family `p` (3 versions, zstd, high priority) with binary keys and qualifiers,
///   several versions per cell and typed `i64` values; a counter family `c`;
/// - `b`: family `blob` (values over 100 bytes in blob files, tiered); a family `empty`
///   with a TTL and no cells.
fn rich(path: &Path) {
    let db = Pigeonhole::open(path, options()).unwrap();
    let a = db
        .table("a")
        .unwrap()
        .family(
            "p",
            Family::default()
                .max_versions(3)
                .zstd(5)
                .cache_priority(Priority::High),
        )
        .family("c", Family::counter())
        .create()
        .unwrap();
    for i in 0..30u64 {
        let row = format!("row{i:03}");
        for v in 0..3u64 {
            a.mutate(row.as_bytes())
                .put_at("p", b"name", T0 + v, format!("n{i}-{v}").as_bytes())
                .commit()
                .unwrap();
        }
        a.mutate(row.as_bytes())
            .put_i64_at("p", b"score", T0, i as i64 * -7)
            .incr_at("c", b"hits", T0 + 1, i as i64 + 1)
            .commit()
            .unwrap();
    }
    // Keys and qualifiers that are not UTF-8, and a value with control characters.
    a.mutate(&[0xff, 0x00, 0x01])
        .put_at("p", &[0x80, 0x81], T0, &[0xfe, 0xfd])
        .put_at("p", b"tab", T0, b"a\tb\n\"q\"")
        .put_f64_at("p", b"ratio", T0 + 5, 0.125)
        .put_f64_at("p", b"nan", T0 + 6, f64::NAN)
        .commit()
        .unwrap();
    let b = db
        .table("b")
        .unwrap()
        .family(
            "blob",
            Family::default()
                .blob_threshold(100)
                .compaction(Compaction::Tiered),
        )
        .family("empty", Family::default().ttl(days(30)))
        .create()
        .unwrap();
    for i in 0..10u64 {
        b.mutate(format!("k{i}").as_bytes())
            .put_at("blob", b"v", T0, &vec![b'x' + (i as u8 % 3); 500])
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    db.close().unwrap();
}

fn dump(path: &Path, args: &[&str]) -> String {
    let out = phdb(&[&["dump", path.to_str().unwrap()][..], args].concat());
    assert_eq!(out.status, 0, "{out:?}");
    out.stdout
}

/// The lines after the header (which names the snapshot's seqno).
fn body(dump: &str) -> Vec<&str> {
    dump.lines().skip(1).collect()
}

fn loaded(dir: &TempDir, input: &Path) -> PathBuf {
    let target = dir.path().join("loaded.phdb");
    let out = phdb(&[
        "--format",
        "json",
        "load",
        "--create",
        target.to_str().unwrap(),
        input.to_str().unwrap(),
    ]);
    assert_eq!(out.status, 0, "{out:?}");
    let doc: serde_json::Value = serde_json::from_str(&out.stdout).unwrap();
    assert_eq!(doc["tables"], 2);
    target
}

#[test]
fn a_dump_loaded_into_a_new_database_dumps_the_same() {
    let dir = TempDir::new("dump-round-trip");
    let src = dir.path().join("src.phdb");
    rich(&src);
    let first = dump(&src, &[]);
    // 2 table lines; 30 rows × (3 name versions + score + counter); the binary row's 4
    // cells; 10 blobs.
    assert_eq!(body(&first).len(), 2 + 30 * 5 + 4 + 10, "{first}");
    // f64 values with their timestamps; a NaN by its bits (ICR 0051).
    assert!(
        first.contains(r#""qualifier":"ratio","ts":1760000000000005,"value":{"f64":0.125}"#),
        "{first}"
    );
    assert!(
        first.contains(r#""qualifier":"nan","ts":1760000000000006,"value":{"f64_bits":"#),
        "{first}"
    );
    // Bytes that are not UTF-8 are base64; control characters stay in JSON strings.
    assert!(
        first.contains(r#""row":{"base64":"/wAB"},"family":"p","qualifier":{"base64":"gIE="}"#),
        "{first}"
    );
    assert!(first.contains(r#""value":"a\tb\n\"q\"""#), "{first}");
    let file = dir.path().join("dump.jsonl");
    std::fs::write(&file, &first).unwrap();

    let target = loaded(&dir, &file);
    let second = dump(&target, &[]);
    assert_eq!(body(&first), body(&second));

    // The loaded database through the API: options, typed values, versions, counters.
    let db = Pigeonhole::open(&target, options()).unwrap();
    let a = db.table("a").unwrap().open().unwrap();
    let p = a.family_info("p").unwrap();
    assert_eq!((p.max_versions, p.cache_priority), (3, Priority::High));
    assert!(a.family_info("c").unwrap().counter);
    let score = a.get(b"row004", "p", b"score").unwrap().unwrap();
    assert_eq!((score.as_i64(), score.timestamp()), (Some(-28), T0));
    assert_eq!(
        a.get(b"row004", "c", b"hits").unwrap().unwrap().as_i64(),
        Some(5)
    );
    let versions = a
        .row(b"row004")
        .family("p")
        .qualifier_prefix(b"name")
        .versions(0)
        .read()
        .unwrap()
        .unwrap();
    assert_eq!(versions.len(), 3);
    assert_eq!(
        a.get(&[0xff, 0x00, 0x01], "p", &[0x80, 0x81])
            .unwrap()
            .unwrap()
            .value(),
        &[0xfe, 0xfd]
    );
    // f64 cells keep their timestamps and bits.
    let ratio = a.get(&[0xff, 0x00, 0x01], "p", b"ratio").unwrap().unwrap();
    assert_eq!(ratio.timestamp(), T0 + 5);
    assert!(matches!(ratio.typed(), pigeonhole::Value::F64(x) if x == 0.125));
    let nan = a.get(&[0xff, 0x00, 0x01], "p", b"nan").unwrap().unwrap();
    assert_eq!(nan.timestamp(), T0 + 6);
    assert!(matches!(nan.typed(), pigeonhole::Value::F64(x) if x.is_nan()));
    let b = db.table("b").unwrap().open().unwrap();
    assert_eq!(
        b.get(b"k1", "blob", b"v").unwrap().unwrap().value().len(),
        500
    );
    assert_eq!(b.family_info("empty").unwrap().ttl, Some(days(30)));
    db.close().unwrap();
}

#[test]
fn dump_jsonl_and_csv_snapshots() {
    let dir = TempDir::new("dump-snapshots");
    let src = dir.path().join("src.phdb");
    rich(&src);
    let jsonl = dump(&src, &["--table", "a", "--prefix", "row00"]);
    common::snapshot("dump_jsonl", &dir, &jsonl);
    let csv = dump(
        &src,
        &["--format", "csv", "--table", "a", "--prefix", "row00"],
    );
    common::snapshot("dump_csv", &dir, &csv);
    let binary = dump(
        &src,
        &["--format", "csv", "--table", "a", "--prefix", "\u{0}"],
    );
    assert_eq!(
        binary.lines().count(),
        1,
        "the prefix matches no row: {binary}"
    );
}

#[test]
fn a_dump_of_a_live_database_reads_through_a_reader() {
    let dir = TempDir::new("dump-live");
    let src = dir.path().join("src.phdb");
    rich(&src);
    let offline = dump(&src, &[]);
    let db = Pigeonhole::open(&src, options()).unwrap();
    let live = dump(&src, &[]);
    assert!(
        live.lines().next().unwrap().contains("\"mode\":\"reader\""),
        "{live}"
    );
    assert_eq!(body(&offline), body(&live));
    // load needs the writer.
    let file = dir.path().join("dump.jsonl");
    std::fs::write(&file, &offline).unwrap();
    let out = phdb(&["load", src.to_str().unwrap(), file.to_str().unwrap()]);
    assert_eq!(out.status, 4, "{out:?}");
    db.close().unwrap();
}

#[test]
fn errors_and_exit_statuses() {
    let dir = TempDir::new("dump-errors");
    let src = dir.path().join("src.phdb");
    rich(&src);
    let out = phdb(&["dump", "--table", "nope", src.to_str().unwrap()]);
    assert_eq!(out.status, 5, "{out:?}");
    let out = phdb(&["--format", "table", "dump", src.to_str().unwrap()]);
    assert_eq!(out.status, 2, "{out:?}");

    let missing = dir.path().join("missing.phdb");
    let input = dir.path().join("in.jsonl");
    std::fs::write(&input, "{\"phdb\":1,\"command\":\"dump\"}\n").unwrap();
    let out = phdb(&["load", missing.to_str().unwrap(), input.to_str().unwrap()]);
    assert_eq!(out.status, 5, "without --create: {out:?}");

    for (text, needle) in [
        (
            "{\"phdb\":1,\"command\":\"dump\"}\nnot json\n",
            "line 2: not JSON",
        ),
        (
            "{\"phdb\":1,\"command\":\"dump\"}\n{\"table\":\"t\",\"row\":\"r\",\"family\":\"f\",\"qualifier\":\"q\",\"ts\":1,\"value\":\"v\"}\n",
            "line 2: table t has no table line",
        ),
    ] {
        std::fs::write(&input, text).unwrap();
        let out = phdb(&[
            "--format",
            "json",
            "load",
            "--create",
            dir.path().join("bad.phdb").to_str().unwrap(),
            input.to_str().unwrap(),
        ]);
        assert_eq!(out.status, 1, "{out:?}");
        let err: serde_json::Value = serde_json::from_str(&out.stderr).unwrap();
        assert_eq!(err["error"]["code"], "BadInput");
        assert!(
            err["error"]["message"].as_str().unwrap().contains(needle),
            "{err}"
        );
    }
}

/// The cells of an Arrow IPC stream, decoded to the jsonl dump's cell objects (without
/// `table`), so the two dumps compare cell for cell.
fn arrow_cells(stream: &[u8]) -> Vec<serde_json::Value> {
    use pigeonhole_arrow::arrow_array::cast::AsArray;
    use pigeonhole_arrow::arrow_array::types::{
        Float64Type, Int8Type, Int32Type, Int64Type, TimestampMicrosecondType,
    };
    let bytes_json = |b: &[u8]| match std::str::from_utf8(b) {
        Ok(s) => serde_json::json!(s),
        Err(_) => serde_json::json!({ "base64": base64(b) }),
    };
    let reader = arrow_ipc::reader::StreamReader::try_new(stream, None).unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let col = |n: &str| batch.column_by_name(n).unwrap().clone();
        let (row, qualifier, value) = (col("row"), col("qualifier"), col("value"));
        let (row, qualifier, value) = (
            row.as_binary::<i32>(),
            qualifier.as_binary::<i32>(),
            value.as_binary::<i32>(),
        );
        let family = col("family");
        let family = family.as_dictionary::<Int32Type>();
        let names = family.values().as_string::<i32>();
        let ts = col("ts");
        let ts = ts.as_primitive::<TimestampMicrosecondType>();
        let types = col("value_type");
        let types = types.as_dictionary::<Int8Type>();
        let type_names = types.values().as_string::<i32>();
        let ints = col("value_i64");
        let ints = ints.as_primitive::<Int64Type>();
        let floats = col("value_f64");
        let floats = floats.as_primitive::<Float64Type>();
        for i in 0..batch.num_rows() {
            let v = match type_names.value(types.keys().value(i) as usize) {
                "bytes" => bytes_json(value.value(i)),
                "f64" if floats.value(i).is_finite() => {
                    serde_json::json!({ "f64": floats.value(i) })
                }
                "f64" => serde_json::json!({ "f64_bits": floats.value(i).to_bits() }),
                _ => serde_json::json!({ "i64": ints.value(i) }),
            };
            out.push(serde_json::json!({
                "row": bytes_json(row.value(i)),
                "family": names.value(family.keys().value(i) as usize),
                "qualifier": bytes_json(qualifier.value(i)),
                "ts": ts.value(i),
                "value": v,
            }));
        }
    }
    out
}

/// Standard base64 with padding, as the jsonl dump writes it.
fn base64(b: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut s = String::new();
    for c in b.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for k in 0..4 {
            if k <= c.len() {
                s.push(A[(n >> (18 - 6 * k) & 63) as usize] as char);
            } else {
                s.push('=');
            }
        }
    }
    s
}

#[test]
fn an_arrow_dump_is_the_jsonl_dumps_cells() {
    let dir = TempDir::new("dump-arrow");
    let src = dir.path().join("src.phdb");
    rich(&src);
    let jsonl = dump(&src, &["--table", "a"]);
    let want: Vec<serde_json::Value> = body(&jsonl)
        .into_iter()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .filter(|v| v.get("row").is_some())
        .map(|mut v| {
            v.as_object_mut().unwrap().remove("table");
            v
        })
        .collect();
    let (status, stream, err) = common::phdb_bytes(&[
        "--format",
        "arrow",
        "dump",
        "--table",
        "a",
        src.to_str().unwrap(),
    ]);
    assert_eq!(status, 0, "{err}");
    let got = arrow_cells(&stream);
    assert!(got.len() > 100, "{}", got.len());
    assert_eq!(got, want);
    // With a prefix.
    let (status, stream, _) = common::phdb_bytes(&[
        "--format",
        "arrow",
        "dump",
        "--table",
        "a",
        "--prefix",
        "row00",
        src.to_str().unwrap(),
    ]);
    assert_eq!(status, 0);
    let rows: std::collections::BTreeSet<_> = arrow_cells(&stream)
        .into_iter()
        .map(|c| c["row"].to_string())
        .collect();
    assert_eq!(rows.len(), 10);
}

#[test]
fn arrow_dump_errors() {
    let dir = TempDir::new("dump-arrow-errors");
    let src = dir.path().join("src.phdb");
    rich(&src);
    let path = src.to_str().unwrap();
    // One table: the stream has one schema.
    let (status, _, err) = common::phdb_bytes(&["--format", "arrow", "dump", path]);
    assert_eq!(status, 2, "{err}");
    assert!(err.contains("one table"), "{err}");
    // Arrow is dump's alone.
    let (status, _, err) = common::phdb_bytes(&["--format", "arrow", "info", path]);
    assert_eq!(status, 2, "{err}");
    assert!(err.contains("--format arrow is for phdb dump"), "{err}");
    // A timestamp an Arrow Timestamp can't hold: exit 6, and jsonl dumps it.
    let db = Pigeonhole::open(&src, options()).unwrap();
    db.table("a")
        .unwrap()
        .open()
        .unwrap()
        .mutate(b"late")
        .put_at("p", b"q", i64::MAX as u64 + 1, b"v")
        .commit()
        .unwrap();
    db.close().unwrap();
    let (status, _, err) = common::phdb_bytes(&[
        "--format", "arrow", "dump", "--table", "a", "--prefix", "late", path,
    ]);
    assert_eq!(status, 6, "{err}");
    assert!(
        err.contains("Unsupported") || err.contains("i64::MAX"),
        "{err}"
    );
    assert!(dump(&src, &["--table", "a", "--prefix", "late"]).contains("9223372036854775808"));
}

/// #1037: an input with no dump header is refused with exit 2 before the database is opened,
/// so `--create` makes nothing: an empty input (a dump that failed into a pipe writes nothing
/// to standard output), a first line that isn't the header, and one that isn't JSON. A dump
/// after blank lines still loads, and its line numbers count them.
#[test]
fn load_refuses_an_input_without_a_dump_header() {
    let dir = TempDir::new("load-header");
    let input = dir.path().join("in.jsonl");
    let target = dir.path().join("target.phdb");

    // A dump of a missing file fails: the error goes to standard error, standard output is
    // empty, and that is what a pipe hands `load`.
    let missing = dir.path().join("missing.phdb");
    let failed = phdb(&["dump", missing.to_str().unwrap()]);
    assert_eq!(failed.status, 5, "{failed:?}");
    assert!(failed.stdout.is_empty(), "{failed:?}");

    for (what, text, needle) in [
        ("empty", String::new(), "the input is empty"),
        ("a failed dump", failed.stdout.clone(), "the input is empty"),
        ("blank lines only", "\n \n".to_owned(), "the input is empty"),
        (
            "another document",
            "{\"x\":1}\n".to_owned(),
            "line 1: not a phdb dump",
        ),
        (
            "a dump's error document",
            "{\"phdb\":1,\"error\":{\"code\":\"FileNotFound\"}}\n".to_owned(),
            "line 1: not a phdb dump",
        ),
        (
            "not JSON",
            "\nhello\n".to_owned(),
            "line 2: not a phdb dump",
        ),
    ] {
        std::fs::write(&input, &text).unwrap();
        let out = phdb(&[
            "--format",
            "json",
            "load",
            "--create",
            target.to_str().unwrap(),
            input.to_str().unwrap(),
        ]);
        assert_eq!(out.status, 2, "{what}: {out:?}");
        let err: serde_json::Value = serde_json::from_str(&out.stderr).unwrap();
        assert_eq!(err["error"]["code"], "BadInput", "{what}");
        assert!(
            err["error"]["message"].as_str().unwrap().contains(needle),
            "{what}: {err}"
        );
        assert!(!target.exists(), "{what}: --create made a database");
    }

    // Blank lines before the header are fine, and errors after it count them.
    std::fs::write(&input, "\n\n{\"phdb\":1,\"command\":\"dump\"}\nnot json\n").unwrap();
    let out = phdb(&[
        "--format",
        "json",
        "load",
        "--create",
        target.to_str().unwrap(),
        input.to_str().unwrap(),
    ]);
    assert_eq!(out.status, 1, "{out:?}");
    let err: serde_json::Value = serde_json::from_str(&out.stderr).unwrap();
    assert!(
        err["error"]["message"]
            .as_str()
            .unwrap()
            .contains("line 4: not JSON"),
        "{err}"
    );
}

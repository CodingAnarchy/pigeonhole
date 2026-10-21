//! `phdb dump` and `phdb load` (#692): a dump loaded into a new database dumps the same, cell
//! for cell; snapshot tests of both formats; the errors and exit statuses.

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
        ("{\"x\":1}\n", "line 1: not a phdb dump"),
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

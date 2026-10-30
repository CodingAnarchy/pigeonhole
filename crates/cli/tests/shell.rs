//! `phdb shell` (#692) through `-c` and `-f`: the command language, snapshots, read-only
//! mode, errors, and a snapshot of a session's output.

mod common;

use std::path::{Path, PathBuf};

use common::{TempDir, phdb};
use pigeonhole::{Options, Pigeonhole};
use serde_json::Value;

/// A new, empty database (the shell creates its tables).
fn empty(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("s.phdb");
    Pigeonhole::open(&path, Options::default().shards(1).pin_threads(false))
        .unwrap()
        .close()
        .unwrap();
    path
}

/// Runs `commands` with `-c` in `format`, returning the status and stdout.
fn shell(path: &Path, format: &str, commands: &[&str]) -> (u8, String, String) {
    let mut args = vec!["--format", format, "shell", path.to_str().unwrap()];
    for c in commands {
        args.push("-c");
        args.push(c);
    }
    let out = phdb(&args);
    (out.status, out.stdout, out.stderr)
}

/// The `rows` of every JSON document a `--format json` run printed, in order.
fn rows(stdout: &str) -> Vec<Vec<Value>> {
    let mut docs = Vec::new();
    let de = serde_json::Deserializer::from_str(stdout).into_iter::<Value>();
    for doc in de {
        let doc = doc.unwrap();
        assert_eq!(doc["phdb"], 1);
        docs.push(doc["rows"].as_array().cloned().unwrap_or_default());
    }
    docs
}

#[test]
fn a_session_writes_reads_and_scans() {
    let dir = TempDir::new("shell-session");
    let path = empty(&dir);
    let (status, out, err) = shell(
        &path,
        "json",
        &[
            "create users profile stats:counter",
            "put users u1 profile:name Ada --ts 100",
            "put users u1 profile:name Ada2 --ts 200",
            r#"put users "u 2" profile:name "two\nlines""#,
            "put users 0xff00 profile:bin 0x0102",
            "put users u1 profile:age 36 --i64",
            "put users u1 profile:ratio 0.5 --f64",
            "incr users u1 stats:logins 3",
            "incr users u1 stats:logins 4",
            "get users u1 profile:name --versions 0",
            "get users u1 stats",
            "scan users --family profile --qualifier-prefix name",
            "scan users --i64 >30",
            "scan users u1 u3 --limit 1",
            "get users 0xff00",
            "delete users u1 profile:name --ts 200",
            "get users u1 profile:name",
            "delete users u1",
            "get users u1",
        ],
    );
    assert_eq!(status, 0, "{err}");
    let docs = rows(&out);
    // Writes print a summary (no rows); reads print one row per cell.
    let name_versions = &docs[9];
    assert_eq!(name_versions.len(), 2);
    assert_eq!(
        (
            name_versions[0]["ts"].clone(),
            name_versions[0]["value"].clone()
        ),
        (200.into(), "Ada2".into())
    );
    assert_eq!(docs[10][0]["value"], serde_json::json!({"i64": 7}));
    let names = &docs[11];
    assert_eq!(names.len(), 2, "{names:?}");
    assert_eq!(names[0]["row"], "u 2");
    assert_eq!(names[0]["value"], "two\nlines");
    assert_eq!(
        docs[12].len(),
        1,
        "only u1's age is an i64 over 30: {:?}",
        docs[12]
    );
    assert_eq!(docs[12][0]["qualifier"], "age");
    let limited = &docs[13];
    assert!(limited.iter().all(|c| c["row"] == "u1"), "{limited:?}");
    assert_eq!(docs[14][0]["row"], serde_json::json!({"base64": "/wA="}));
    assert_eq!(
        docs[14][0]["value"], "\u{1}\u{2}",
        "valid UTF-8 (control characters): a JSON string"
    );
    assert_eq!(docs[16][0]["value"], "Ada", "the 200 version is deleted");
    assert!(docs[18].is_empty(), "the row is deleted");
}

#[test]
fn snapshots_read_as_of_when_they_were_taken() {
    let dir = TempDir::new("shell-snapshots");
    let path = empty(&dir);
    let (status, out, err) = shell(
        &path,
        "json",
        &[
            "create t f",
            "put t r f:q old",
            "snapshot before",
            "put t r f:q new",
            "get t r f:q --at before",
            "get t r f:q",
            "release before",
            "get t r f:q --at before",
        ],
    );
    assert_eq!(status, 1, "the released snapshot is gone: {err}");
    assert!(
        err.contains("line 8") && err.contains("no snapshot before"),
        "{err}"
    );
    let docs = rows(&out);
    assert_eq!(docs[4][0]["value"], "old");
    assert_eq!(docs[5][0]["value"], "new");
}

#[test]
fn min_and_max_families() {
    // D294: `create` takes `:min` and `:max`, `aggregate` writes their inputs, and `incr`
    // is refused there.
    let dir = TempDir::new("shell-aggregates");
    let path = empty(&dir);
    let (status, out, err) = shell(
        &path,
        "json",
        &[
            "create s lo:min hi:max",
            "aggregate s r lo:t 7",
            "aggregate s r hi:t 7",
            "aggregate s r lo:t -2",
            "aggregate s r hi:t -2",
            "aggregate s r hi:t 3 --ts 3600",
            "get s r lo:t",
            "get s r hi:t --versions 0",
            "incr s r lo:t 1",
        ],
    );
    assert_eq!(status, 1, "the incr is refused: {err}");
    assert!(err.contains("write its inputs with aggregate"), "{err}");
    let docs = rows(&out);
    assert_eq!(docs[6][0]["value"], serde_json::json!({"i64": -2}));
    let hi: Vec<_> = docs[7].iter().map(|c| c["value"].clone()).collect();
    assert_eq!(
        hi,
        [serde_json::json!({"i64": 3}), serde_json::json!({"i64": 7})],
        "the 3600 bucket, then the fixed-timestamp cell"
    );
}

#[test]
fn meta_commands() {
    let dir = TempDir::new("shell-meta");
    let path = empty(&dir);
    let (status, out, err) = shell(
        &path,
        "table",
        &[
            "create b f",
            "create a x y:counter",
            "\\tables",
            "\\schema a",
            "\\health",
            "\\metrics",
            "\\help",
            "\\format csv",
            "\\tables",
            "\\quit",
            "\\tables",
        ],
    );
    assert_eq!(status, 0, "{err}");
    common::snapshot("shell_meta", &dir, &out);
}

#[test]
fn a_script_file_and_errors() {
    let dir = TempDir::new("shell-script");
    let path = empty(&dir);
    let script = dir.path().join("cmds.phdb-shell");
    std::fs::write(
        &script,
        "# set up\ncreate t f\nput t r f:q v\n\nget t r\nget t r f:q --bogus 1\nget t r\n",
    )
    .unwrap();
    let out = phdb(&[
        "--format",
        "jsonl",
        "shell",
        path.to_str().unwrap(),
        "-f",
        script.to_str().unwrap(),
    ]);
    assert_eq!(out.status, 2, "{out:?}");
    assert!(
        out.stderr.contains("line 6") && out.stderr.contains("unknown flag --bogus"),
        "{out:?}"
    );
    // The commands before the failure ran: three reports, the last with the cell.
    assert_eq!(
        out.stdout
            .lines()
            .filter(|l| l.contains("\"command\":\"shell\""))
            .count(),
        3
    );

    for (cmd, status, needle) in [
        ("frobnicate", 2, "unknown command frobnicate"),
        ("get t", 2, "missing row"),
        ("put t r f v", 2, "expected <family>:<qualifier>"),
        ("get nope r", 5, "table nope"),
        (r#"put t r f:q "open"#, 2, "unterminated"),
        ("scan t a --prefix p", 2, "not both"),
        ("incr t r f:q x", 2, "integer delta"),
    ] {
        let (s, _, err) = shell(&path, "table", &[cmd]);
        assert_eq!(s, status, "{cmd}: {err}");
        assert!(err.contains(needle), "{cmd}: {err}");
    }
}

#[test]
fn a_running_writer_makes_the_shell_read_only() {
    let dir = TempDir::new("shell-reader");
    let path = empty(&dir);
    let (status, _, err) = shell(&path, "table", &["create t f", "put t r f:q v"]);
    assert_eq!(status, 0, "{err}");
    let db = Pigeonhole::open(&path, Options::default().shards(1).pin_threads(false)).unwrap();
    let (status, out, err) = shell(&path, "json", &["\\health", "get t r", "put t r f:q w"]);
    assert_eq!(status, 1, "{err}");
    assert!(err.contains("ReadOnly") && err.contains("line 3"), "{err}");
    assert!(out.contains("\"mode\": \"reader\""), "{out}");
    assert_eq!(rows(&out)[1][0]["value"], "v");
    db.close().unwrap();
    // --read-only refuses writes as the writer too.
    let out = phdb(&[
        "shell",
        path.to_str().unwrap(),
        "--read-only",
        "-c",
        "put t r f:q w",
    ]);
    assert_eq!(out.status, 1, "{out:?}");
}

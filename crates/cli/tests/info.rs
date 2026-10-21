//! `phdb info`, the exit statuses and the open policy, through [`pigeonhole_cli::run`], with
//! snapshot tests of every output format (#692).

mod common;

use common::{TempDir, phdb};
use pigeonhole::{Compaction, Family, Options, Pigeonhole, Priority, days};

/// A database with two tables: `events` (flushed, so it has SSTs; fixed timestamps, so its
/// SST bytes are the same every run) and `users` (three families with distinct options,
/// nothing written).
fn sample(dir: &TempDir) -> std::path::PathBuf {
    let path = dir.path().join("sample.phdb");
    let db = Pigeonhole::open(&path, Options::default().shards(1).pin_threads(false)).unwrap();
    let events = db
        .table("events")
        .unwrap()
        .family("e", Family::default().zstd(3))
        .create()
        .unwrap();
    for i in 0..50u32 {
        events
            .mutate(format!("ev{i:03}").as_bytes())
            .put_at("e", b"kind", 1_760_000_000_000_000 + u64::from(i), b"click")
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    db.table("users")
        .unwrap()
        .family(
            "profile",
            Family::default()
                .max_versions(1)
                .cache_priority(Priority::High),
        )
        .family("logins", Family::counter())
        .family(
            "history",
            Family::default()
                .ttl(days(30))
                .uncompressed()
                .compaction(Compaction::Tiered),
        )
        .create()
        .unwrap();
    db.close().unwrap();
    path
}

#[test]
fn info_table() {
    let dir = TempDir::new("info-table");
    let path = sample(&dir);
    let out = phdb(&["info", path.to_str().unwrap()]);
    assert_eq!(out.status, 0, "{out:?}");
    common::snapshot("info_table", &dir, &out.stdout);
}

#[test]
fn info_json() {
    let dir = TempDir::new("info-json");
    let path = sample(&dir);
    let out = phdb(&["--format", "json", "info", path.to_str().unwrap()]);
    assert_eq!(out.status, 0, "{out:?}");
    let doc: serde_json::Value = serde_json::from_str(&out.stdout).unwrap();
    assert_eq!(doc["phdb"], 1);
    assert_eq!(doc["mode"], "writer");
    assert_eq!(doc["tables"][0]["name"], "events");
    assert_eq!(doc["tables"][0]["families"][0]["entries"], 50);
    common::snapshot("info_json", &dir, &out.stdout);
}

#[test]
fn info_jsonl() {
    let dir = TempDir::new("info-jsonl");
    let path = sample(&dir);
    let out = phdb(&["--format", "jsonl", "info", path.to_str().unwrap()]);
    assert_eq!(out.status, 0, "{out:?}");
    // A header line, then one line per family.
    assert_eq!(out.stdout.lines().count(), 1 + 4);
    for line in out.stdout.lines() {
        serde_json::from_str::<serde_json::Value>(line).unwrap();
    }
    common::snapshot("info_jsonl", &dir, &out.stdout);
}

#[test]
fn info_csv() {
    let dir = TempDir::new("info-csv");
    let path = sample(&dir);
    let out = phdb(&["--format", "csv", "info", path.to_str().unwrap()]);
    assert_eq!(out.status, 0, "{out:?}");
    common::snapshot("info_csv", &dir, &out.stdout);
}

#[test]
fn info_on_a_live_database_opens_as_a_reader() {
    let dir = TempDir::new("info-reader");
    let path = sample(&dir);
    let db = Pigeonhole::open(&path, Options::default().shards(2).pin_threads(false)).unwrap();
    let out = phdb(&["--format", "json", "info", path.to_str().unwrap()]);
    assert_eq!(out.status, 0, "{out:?}");
    let doc: serde_json::Value = serde_json::from_str(&out.stdout).unwrap();
    assert_eq!(doc["mode"], "reader");
    assert_eq!(doc["shards"], 2);
    assert_eq!(doc["free_bytes"], serde_json::Value::Null);
    assert_eq!(doc["tables"].as_array().unwrap().len(), 2);

    // `--writer` refuses rather than fall back.
    let out = phdb(&["--writer", "info", path.to_str().unwrap()]);
    assert_eq!(out.status, 4, "{out:?}");
    assert!(out.stderr.contains("phdb: error: open:"), "{out:?}");
    db.close().unwrap();

    // With no writer running, `--reader` cannot attach.
    let out = phdb(&["--reader", "info", path.to_str().unwrap()]);
    assert_eq!(out.status, 1, "{out:?}");
    assert!(
        out.stderr.contains("is a writer process running?"),
        "{out:?}"
    );
}

#[test]
fn errors_and_exit_statuses() {
    let dir = TempDir::new("info-errors");
    let missing = dir.path().join("missing.phdb");
    let out = phdb(&["info", missing.to_str().unwrap()]);
    assert_eq!(out.status, 5);
    assert_eq!(out.stdout, "");
    let out = phdb(&["--format", "json", "info", missing.to_str().unwrap()]);
    assert_eq!(out.status, 5);
    let err: serde_json::Value = serde_json::from_str(&out.stderr).unwrap();
    assert_eq!(err["phdb"], 1);
    assert_eq!(err["error"]["code"], "FileNotFound");
    assert_eq!(err["error"]["code_num"], serde_json::Value::Null);

    // Not a database: corruption, exit 3, with the stable error code.
    let junk = dir.path().join("junk.phdb");
    std::fs::write(&junk, vec![0x5au8; 1 << 16]).unwrap();
    let out = phdb(&["--format", "json", "info", junk.to_str().unwrap()]);
    assert_eq!(out.status, 3, "{out:?}");
    let err: serde_json::Value = serde_json::from_str(&out.stderr).unwrap();
    assert_eq!(err["error"]["code"], "Corruption");
    assert_eq!(err["error"]["code_num"], 2);

    let out = phdb(&["info"]);
    assert_eq!(out.status, 2);
    assert!(out.stderr.contains("Usage"), "{out:?}");
    let out = phdb(&["--format", "xml", "info", "x"]);
    assert_eq!(out.status, 2);
    let out = phdb(&["--help"]);
    assert_eq!(out.status, 0);
    assert!(out.stdout.contains("Exit status"), "{out:?}");
}

#[test]
fn closed_pipe_is_not_an_error() {
    let dir = TempDir::new("info-pipe");
    let path = sample(&dir);
    let mut stdout = common::BrokenPipe;
    let mut stderr = Vec::new();
    let status = pigeonhole_cli::run(
        ["phdb", "info", path.to_str().unwrap()],
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(status.code(), 0);
    assert!(stderr.is_empty());
}

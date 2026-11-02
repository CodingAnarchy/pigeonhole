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
        .family(
            "e",
            Family::default().compression(pigeonhole::Compression::Zstd(3)),
        )
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
                .compression(pigeonhole::Compression::None)
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

    // With no writer running, `--reader` cannot attach: `NoWriter` (ICR 0057), exit 4.
    let out = phdb(&["--reader", "info", path.to_str().unwrap()]);
    assert_eq!(out.status, 4, "{out:?}");
    assert!(out.stderr.contains("no writer is running"), "{out:?}");
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

/// D310 amendment (d), ICR 0109: the commands that only read (`info`, `dump`, `backup`,
/// `shell --read-only`) open read-only and write nothing, so running them between two of an
/// application's fed opens leaves its change-feed token valid. A writable phdb open (the
/// shell without `--read-only`) still ends it until D310 (c) inherits the feed.
#[test]
fn read_only_commands_keep_an_applications_feed_token() {
    use std::time::Duration;
    let dir = TempDir::new("info-feed");
    let path = dir.path().join("app.phdb");
    let app = || {
        Options::default()
            .shards(2)
            .pin_threads(false)
            .change_feed(pigeonhole::FeedRetention::bytes(16 << 20))
    };
    let db = Pigeonhole::open(&path, app()).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let mut feed = t.feed().open().unwrap();
    t.mutate(b"r1").put("f", b"q", b"v").commit().unwrap();
    feed.next(Duration::from_secs(5)).unwrap().expect("r1");
    let token = feed.token();
    t.mutate(b"r2").put("f", b"q", b"v").commit().unwrap();
    drop(feed);
    drop(t);
    db.close().unwrap();

    let p = path.to_str().unwrap();
    let copy = dir.path().join("copy.phdb");
    for args in [
        vec!["info", p],
        vec!["dump", p],
        vec!["backup", p, copy.to_str().unwrap()],
        vec!["shell", p, "--read-only", "-c", "get t r2 f:q"],
    ] {
        let run = phdb(&args);
        assert_eq!(run.status, 0, "{args:?}: {}", run.stderr);
    }
    let info = phdb(&["info", p, "--format", "json"]);
    let doc: serde_json::Value = serde_json::from_str(&info.stdout).unwrap();
    assert_eq!(doc["read_only"], true, "{}", info.stdout);

    let db = Pigeonhole::open(&path, app()).unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    let mut feed = t.feed().resume(&token).open().unwrap();
    let c = feed
        .next(Duration::from_secs(5))
        .unwrap()
        .expect("the commit after the token");
    assert_eq!(c.changes()[0].row(), b"r2");
    drop(feed);
    drop(t);
    db.close().unwrap();
}

/// D310 amendment (c) with phdb's open: a writing command (here the shell's `put`) on an
/// existing database leaves the shard count unset, so the open inherits the application's
/// feed and its shard count, and the app's token resumes through phdb's write, which the
/// feed delivers too.
#[test]
fn a_writable_command_keeps_an_applications_feed_token() {
    use std::time::Duration;
    let dir = TempDir::new("shell-feed");
    let path = dir.path().join("app.phdb");
    let app = || {
        Options::default()
            .shards(3)
            .pin_threads(false)
            .change_feed(pigeonhole::FeedRetention::bytes(16 << 20))
    };
    let db = Pigeonhole::open(&path, app()).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let mut feed = t.feed().open().unwrap();
    t.mutate(b"r1").put("f", b"q", b"v").commit().unwrap();
    feed.next(Duration::from_secs(5)).unwrap().expect("r1");
    let token = feed.token();
    drop(feed);
    drop(t);
    db.close().unwrap();

    let run = phdb(&["shell", path.to_str().unwrap(), "-c", "put t by-phdb f:q v"]);
    assert_eq!(run.status, 0, "{}", run.stderr);

    let db = Pigeonhole::open(&path, app()).unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    let mut feed = t.feed().resume(&token).open().unwrap();
    let c = feed
        .next(Duration::from_secs(5))
        .unwrap()
        .expect("phdb's write, fed under the inherited feed");
    assert_eq!(c.changes()[0].row(), b"by-phdb");
    drop(feed);
    drop(t);
    db.close().unwrap();
}

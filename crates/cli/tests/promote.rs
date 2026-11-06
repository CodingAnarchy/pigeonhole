//! `phdb promote` (#1436): a replica's file becomes a primary that takes writes; a file
//! that isn't a replica's is refused, and so is `phdb serve --replica-of` on one with tables.
#![cfg(feature = "serve")]

mod common;

use std::path::PathBuf;

use common::{TempDir, phdb};
use pigeonhole::{Family, FeedRetention, Options, Pigeonhole};
use serde_json::Value;

fn options() -> Options {
    Options::default().shards(1).pin_threads(false)
}

/// A primary with a few rows, and a replica of it bootstrapped as `phdb serve --replica-of`
/// does: a copy of one durable snapshot, made a replica at that snapshot's feed token.
fn replica(dir: &TempDir) -> (PathBuf, PathBuf) {
    let primary = dir.path().join("primary.phdb");
    let db = Pigeonhole::open(
        &primary,
        options().change_feed(FeedRetention::bytes(16 << 20)),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    for i in 0..5u32 {
        t.mutate(format!("r{i}").as_bytes())
            .put("f", b"q", b"v")
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    let snap = db.durable_snapshot().unwrap();
    let token = db.feed().from_snapshot(&snap).open().unwrap().token();
    let copy = dir.path().join("replica.phdb");
    db.backup_at(&snap, &copy).unwrap();
    drop(snap);
    drop(t);
    db.close().unwrap();
    let r = Pigeonhole::open(&copy, options()).unwrap();
    r.begin_replication(&token).unwrap();
    r.close().unwrap();
    (primary, copy)
}

#[test]
fn promote_makes_a_replica_s_file_take_writes() {
    let dir = TempDir::new("promote");
    let (_, copy) = replica(&dir);
    let file = copy.to_str().unwrap();
    // A replica refuses writes until promoted.
    let r = Pigeonhole::open(&copy, options()).unwrap();
    let t = r.table("t").unwrap().open().unwrap();
    assert!(t.mutate(b"x").put("f", b"q", b"v").commit().is_err());
    drop(t);
    r.close().unwrap();

    let out = phdb(&["--format", "json", "promote", file]);
    assert_eq!(out.status, 0, "{out:?}");
    let doc: Value = serde_json::from_str(&out.stdout).unwrap();
    assert!(
        doc["primary_token"].as_str().is_some_and(|t| !t.is_empty()),
        "{doc}"
    );

    let r = Pigeonhole::open(&copy, options()).unwrap();
    let t = r.table("t").unwrap().open().unwrap();
    t.mutate(b"x").put("f", b"q", b"mine").commit().unwrap();
    assert_eq!(
        t.get(b"r3", "f", b"q").unwrap().map(|c| c.value().to_vec()),
        Some(b"v".to_vec())
    );
    drop(t);
    r.close().unwrap();

    // Promoted once: it's no replica now.
    let out = phdb(&["promote", file]);
    assert_eq!(out.status, 2, "{out:?}");
    assert!(out.stderr.contains("isn't a replica"), "{out:?}");
}

#[test]
fn replica_of_refuses_a_database_of_its_own() {
    let dir = TempDir::new("replica-of-own");
    let (primary, _) = replica(&dir);
    // It would be replaced by the bootstrap: refused before anything is served.
    let out = phdb(&[
        "serve",
        primary.to_str().unwrap(),
        "--listen",
        "127.0.0.1:0",
        "--replica-of",
        "http://127.0.0.1:1",
    ]);
    assert_eq!(out.status, 2, "{out:?}");
    assert!(
        out.stderr.contains("isn't a replica and has tables"),
        "{out:?}"
    );
}

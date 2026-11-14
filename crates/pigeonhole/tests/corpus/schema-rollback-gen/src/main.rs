//! Writes the first step of the schema-rollback corpus (#1853), with 1.2.0's own code: a
//! primary with a change feed, a token of its feed of every table, and a replica of it, as a
//! 1.2 deployment leaves them before a rollback to 1.1.x. The second step, 1.1.3's own code
//! changing the primary's schema, is `rollback-gen schema` (`../rollback-gen`), which also
//! writes `frozen.txt`.
//!
//! `cargo run --release -- <out dir>` writes `<out>/<case>/`, for each case the second step
//! knows (`created`, `created_unwritten`, `dropped`, `family`), all four the same here:
//! - `data.phdb` (and its WAL and feed sidecar): the primary. Tables `a` (family `f`), `b`
//!   (family `f`) and `feed_tokens` (family `t`). `feed_tokens` row `all`, qualifier `token`:
//!   a token of the feed of every table, taken before the first commit. Then one commit to
//!   `a` (`a0`), one to `b` (`b0`), and, after the replica's bootstrap, one more to `a`
//!   (`a1`).
//! - `replica.phdb` (and its WAL): a replica of the primary, bootstrapped from a backup and a
//!   feed of one snapshot (after `b0`) and caught up with every commit after it, as
//!   `phdb serve --replica-of` keeps one. Its stored token resumes the primary's feed.

use std::path::Path;
use std::time::Duration;

use pigeonhole::replication::ReplicatedEvent;
use pigeonhole::{Family, FeedRetention, Options, Pigeonhole};

const CASES: [&str; 4] = ["created", "created_unwritten", "dropped", "family"];

fn options() -> Options {
    Options::default()
        .shards(1)
        .memtable_budget(4 << 20)
        .wal_segment_size(256 << 10)
}

fn put(db: &Pigeonhole, table: &str, family: &str, value: &[u8]) {
    db.table(table)
        .unwrap()
        .open()
        .unwrap()
        .mutate(b"r")
        .put(family, b"q", value)
        .commit()
        .unwrap();
}

fn write(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let primary = Pigeonhole::open(
        dir.join("data.phdb"),
        options()
            .create_if_missing(true)
            .change_feed(FeedRetention::bytes(4 << 20)),
    )
    .unwrap();
    for (table, family) in [("a", "f"), ("b", "f"), ("feed_tokens", "t")] {
        primary
            .table(table)
            .unwrap()
            .family(family, Family::default())
            .create()
            .unwrap();
    }
    let token = primary.feed().open().unwrap().token();
    primary
        .table("feed_tokens")
        .unwrap()
        .open()
        .unwrap()
        .mutate(b"all")
        .put("t", b"token", token.as_bytes())
        .commit()
        .unwrap();
    put(&primary, "a", "f", b"a0");
    put(&primary, "b", "f", b"b0");

    // The replica's bootstrap: a backup and a feed of every table, of one snapshot.
    let snap = primary.durable_snapshot().unwrap();
    primary.backup_at(&snap, dir.join("replica.phdb")).unwrap();
    let mut feed = primary.feed().from_snapshot(&snap).open().unwrap();
    let start = feed.token();
    drop(snap);
    let replica = Pigeonhole::open(dir.join("replica.phdb"), options()).unwrap();
    replica.begin_replication(&start).unwrap();

    // One more commit, which the replica applies as its follow does.
    put(&primary, "a", "f", b"a1");
    let mut events: Vec<ReplicatedEvent> = Vec::new();
    while feed
        .next_event(Duration::from_millis(200))
        .unwrap()
        .is_some()
    {
        events.push(feed.replicated().unwrap());
    }
    assert!(!events.is_empty() && !events.iter().any(ReplicatedEvent::is_catalog));
    replica
        .apply_replicated(&events, events.last().unwrap().token())
        .unwrap();
    assert_eq!(
        replica.replicated_token().unwrap().as_ref(),
        Some(events.last().unwrap().token())
    );
    drop(feed);
    replica.close().unwrap();
    primary.close().unwrap();
}

fn main() {
    let out = std::env::args()
        .nth(1)
        .expect("usage: schema-rollback-gen <out dir>");
    let out = Path::new(&out);
    for case in CASES {
        let dir = out.join(case);
        assert!(
            !dir.exists(),
            "{} exists: write into a new directory",
            dir.display()
        );
        write(&dir);
        println!("wrote {}", dir.display());
    }
}

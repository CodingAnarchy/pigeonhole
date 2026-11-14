//! The change feed's and replication's measured shapes (#1828), one per run, for
//! `scripts/instructions-per-cell.sh`:
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example feedshapes -- SHAPE ITERATIONS DIR
//! ```
//!
//! | Shape | One iteration | Units (`units N` on stderr) |
//! |---|---|---|
//! | `feed-drain` | 2,560 commits of one 100-byte cell to table `t`, written in setup, then drained by a feed of `t` (`Table::feed`) with `try_next` | commits |
//! | `feed-drain-all` | the same, drained by a feed of every table (`Pigeonhole::feed`, D315) | commits |
//! | `replicate-apply` | the same commits, as a primary's feed of every table delivered them (`Feed::replicated`) and encoded in setup: each decoded (`ReplicatedEvent::decode`) and applied to a replica with `apply_replicated` in runs of 64 (D315) | commits |
//!
//! Each feed is opened before the setup's commits and drained only once the commits are in
//! the WAL files (a second feed, drained in setup, waits for that): the background sync a
//! durable feed waits on for `Buffered` commits stays out of the measured work. The reader
//! reads those files on the measured thread (`PIGEONHOLE_IO=pread` in the workflow).
//!
//! `replicate-apply` counts the replica's side: decoding the events, building the run's
//! batch, and its commit, which `apply_replicated` makes at `GroupSync` whatever the
//! database's durability. The shards are application-owned (`support/shards.rs`), so the
//! shard's side of each commit is counted along with the caller's; the sync itself is a
//! system call, which callgrind doesn't count.
//!
//! Only the measured work runs inside functions named `shape_*`, each counting its thread's
//! work with a `Measured` guard (`support/measure.rs`).
#[path = "support/measure.rs"]
mod measure;
#[path = "support/shards.rs"]
mod shards;
use measure::Measured;
use shards::Shards;

use std::time::Duration;

use pigeonhole::replication::ReplicatedEvent;
use pigeonhole::{Durability, Family, Feed, FeedEvent, FeedRetention, Options, Pigeonhole, Table};

const VALUE: [u8; 100] = [7; 100];
/// Commits per iteration: 40 runs of [`RUN`].
const PER_ITERATION: usize = 2_560;
/// Events per `apply_replicated` run.
const RUN: usize = 64;
/// Every setup cell's timestamp is fixed (the commit's own still comes from the clock).
const TS: u64 = 1_600_000_000_000_000;

fn main() {
    measure::fix_hash_seeds();
    let mut args = std::env::args().skip(1);
    let usage = "usage: feedshapes SHAPE ITERATIONS DIR";
    let shape = args.next().expect(usage);
    let iters: usize = args.next().expect(usage).parse().expect("ITERATIONS");
    let base = std::path::PathBuf::from(args.next().expect(usage));
    let dir = base.join(format!("phdb-feedshapes-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    // `SHAPE_SETUP_ONLY=1`: the setup alone, without the measured work (the script checks
    // that callgrind then counts nothing inside the `shape_` functions).
    let setup_only = std::env::var_os("SHAPE_SETUP_ONLY").is_some();
    let n = PER_ITERATION * iters;
    let rows: Vec<Vec<u8>> = (0..n).map(|i| format!("f:{i:010}").into_bytes()).collect();
    let feed_on = |o: Options| o.change_feed(FeedRetention::bytes(1 << 30));
    let units = match shape.as_str() {
        "feed-drain" | "feed-drain-all" => {
            let (db, shards) = Shards::open_with(&dir.join("f.phdb"), None, feed_on);
            let t = table(&db);
            let open = || {
                if shape == "feed-drain-all" {
                    db.feed().open().unwrap()
                } else {
                    t.feed().open().unwrap()
                }
            };
            let mut feed = open();
            let mut setup_feed = open();
            write(&t, &rows);
            // Every commit is in the files once a durable feed has delivered the last one.
            for _ in 0..n {
                setup_feed
                    .next(Duration::from_secs(30))
                    .unwrap()
                    .expect("the setup's commits reach a feed");
            }
            drop(setup_feed);
            let mut drained = 0;
            if !setup_only {
                shards.measure(&mut || drained = shape_feed_drain(&mut feed));
                assert_eq!(drained, n, "the feed delivers every setup commit");
            }
            drop(feed);
            drop(t);
            db.close().unwrap();
            shards.join();
            drained
        }
        "replicate-apply" => {
            let (catalog, events) = primary_events(&dir.join("primary.phdb"), &rows);
            let (db, shards) = Shards::open(&dir.join("replica.phdb"), None);
            for event in &catalog {
                db.apply_catalog(event, event.token()).unwrap();
            }
            let mut applied = 0;
            if !setup_only {
                shards.measure(&mut || applied = shape_replicate_apply(&db, &events));
                assert_eq!(applied, n);
                assert_eq!(
                    db.replicated_token().unwrap().as_ref(),
                    Some(
                        ReplicatedEvent::decode(events.last().unwrap())
                            .unwrap()
                            .token()
                    ),
                    "the replica applied every event"
                );
            }
            db.close().unwrap();
            shards.join();
            applied
        }
        other => panic!("unknown shape {other}: feed-drain, feed-drain-all or replicate-apply"),
    };
    eprintln!("units {}", if setup_only { 0 } else { units });
    std::fs::remove_dir_all(&dir).ok();
}

/// Table `t`, one family.
fn table(db: &Pigeonhole) -> Table {
    db.table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap()
}

/// One commit of one cell per row.
fn write(t: &Table, rows: &[Vec<u8>]) {
    for (i, row) in rows.iter().enumerate() {
        t.mutate(row)
            .put_at("f", b"q", TS + i as u64, &VALUE)
            .commit()
            .unwrap();
    }
}

/// A primary at `path` with `rows` written, and its feed of every table from the oldest
/// commit, as a replica takes it: the catalog changes (table `t`'s creation), and every
/// commit encoded. The primary is closed before the measured work.
fn primary_events(
    path: &std::path::Path,
    rows: &[Vec<u8>],
) -> (Vec<ReplicatedEvent>, Vec<Vec<u8>>) {
    let db = Pigeonhole::open(
        path,
        Options::default()
            .shards(1)
            .durability(Durability::Buffered)
            .change_feed(FeedRetention::bytes(1 << 30)),
    )
    .unwrap();
    let mut feed: Feed = db.feed().open().unwrap();
    let t = table(&db);
    write(&t, rows);
    let (mut catalog, mut commits) = (Vec::new(), Vec::new());
    while commits.len() < rows.len() {
        let event = feed
            .next_event(Duration::from_secs(30))
            .unwrap()
            .expect("the primary's commits reach its feed");
        let is_catalog = matches!(event, FeedEvent::Catalog(_));
        let replicated = feed.replicated().expect("an event was delivered");
        if is_catalog {
            assert!(
                commits.is_empty(),
                "the table is created before its commits"
            );
            catalog.push(replicated);
        } else {
            commits.push(replicated.encode());
        }
    }
    drop(feed);
    drop(t);
    db.close().unwrap();
    (catalog, commits)
}

#[inline(never)]
fn shape_feed_drain(feed: &mut Feed) -> usize {
    let _measured = Measured::start();
    let mut n = 0;
    while let Some(commit) = feed.try_next().unwrap() {
        n += commit.changes().len();
        std::hint::black_box(commit);
    }
    n
}

#[inline(never)]
fn shape_replicate_apply(db: &Pigeonhole, events: &[Vec<u8>]) -> usize {
    let _measured = Measured::start();
    let mut n = 0;
    for run in events.chunks(RUN) {
        let run: Vec<ReplicatedEvent> = run
            .iter()
            .map(|e| ReplicatedEvent::decode(e).unwrap())
            .collect();
        let token = run.last().expect("a run is never empty").token().clone();
        db.apply_replicated(&run, &token).unwrap();
        n += run.len();
    }
    n
}

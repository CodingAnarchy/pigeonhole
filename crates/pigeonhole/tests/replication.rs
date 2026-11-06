//! D315 PR 3: a replica applies a primary's feed of every table with the primary's ids,
//! storing the primary's token in the same commit as what it applied, and resumes from it
//! after a crash with nothing applied twice or skipped.
#![cfg(feature = "__unstable-replication")]

use std::sync::Arc;
use std::time::Duration;

use pigeonhole::replication::ReplicatedEvent;
use pigeonhole::{ErrorCode, Family, Feed, FeedEvent, FeedRetention, Options, Pigeonhole, Value};
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};

fn options(vfs: &Arc<SimVfs>) -> Options {
    Options::default()
        .vfs(Arc::clone(vfs) as _)
        .shards(2)
        .memtable_budget(4 << 20)
        .wal_segment_size(256 << 10)
        .change_feed(FeedRetention::bytes(16 << 20))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Every cell of every table, every version, with its timestamp.
fn contents(db: &Pigeonhole) -> Vec<String> {
    let mut tables = db.tables();
    tables.sort();
    let mut lines = Vec::new();
    for name in tables {
        let t = db.table(&name).unwrap().open().unwrap();
        let scan = t.scan_prefix(b"").versions(0);
        let mut it = scan.iter().unwrap();
        while let Some(row) = it.next_ref().unwrap() {
            for e in row.iter() {
                let value = match e.cell.typed() {
                    Value::Bytes(b) => hex(b),
                    other => format!("{other:?}"),
                };
                lines.push(format!(
                    "{name}\t{}\t{}\t{}\t{}\t{value}",
                    hex(row.key()),
                    e.family,
                    hex(e.qualifier),
                    e.cell.timestamp()
                ));
            }
        }
    }
    lines
}

/// The events the feed has ready now, as a replica takes them.
fn ready(feed: &mut Feed) -> Vec<ReplicatedEvent> {
    let mut out = Vec::new();
    while feed
        .next_event(Duration::from_millis(200))
        .unwrap()
        .is_some()
    {
        // Through the wire encoding a replica in another process reads (#1436).
        let event = feed.replicated().expect("an event was delivered");
        let decoded = ReplicatedEvent::decode(&event.encode()).unwrap();
        assert_eq!(decoded, event);
        out.push(decoded);
    }
    out
}

/// Applies `events` in runs of at most `run` commits, each catalog change on its own.
fn apply(replica: &Pigeonhole, events: &[ReplicatedEvent], run: usize) {
    let mut i = 0;
    while i < events.len() {
        if events[i].is_catalog() {
            replica
                .apply_catalog(&events[i], events[i].token())
                .unwrap();
            i += 1;
            continue;
        }
        let end = (i..events.len())
            .take(run)
            .take_while(|&j| !events[j].is_catalog())
            .last()
            .unwrap()
            + 1;
        let part = &events[i..end];
        replica
            .apply_replicated(part, part.last().unwrap().token())
            .unwrap();
        i = end;
    }
}

/// [`apply`], stopping at the first error (a crash inside it).
fn try_apply(
    replica: &Pigeonhole,
    events: &[ReplicatedEvent],
    run: usize,
) -> pigeonhole::Result<()> {
    for chunk in events.chunk_by(|a, b| !a.is_catalog() && !b.is_catalog()) {
        if chunk[0].is_catalog() {
            replica.apply_catalog(&chunk[0], chunk[0].token())?;
            continue;
        }
        for part in chunk.chunks(run) {
            replica.apply_replicated(part, part.last().unwrap().token())?;
        }
    }
    Ok(())
}

/// A primary's workload: tables created and dropped, puts with explicit and default
/// timestamps, every kind of delete, and counter increments.
fn workload(primary: &Pigeonhole, step: u32) {
    let t = primary
        .table("t")
        .unwrap()
        .family("a", Family::default())
        .family("b", Family::default().max_versions(2))
        .create_if_missing()
        .unwrap();
    let c = primary
        .table("c")
        .unwrap()
        .family("n", Family::counter())
        .create_if_missing()
        .unwrap();
    let row = format!("r{}", step % 5);
    t.mutate(row.as_bytes())
        .put("a", b"q", format!("v{step}").as_bytes())
        .put_at("b", b"q", 100 + u64::from(step % 3), b"x")
        .commit()
        .unwrap();
    c.mutate(b"hits").incr("n", b"q", 1).commit().unwrap();
    match step % 4 {
        0 => {
            t.mutate(row.as_bytes())
                .delete_column("a", b"q")
                .commit()
                .unwrap();
        }
        1 => {
            t.mutate(row.as_bytes())
                .delete_cell("b", b"q", 100)
                .commit()
                .unwrap();
        }
        2 => {
            t.mutate(b"gone").put("a", b"q", b"1").commit().unwrap();
            t.mutate(b"gone").delete_row().commit().unwrap();
        }
        _ => {
            let tmp = primary
                .table(&format!("tmp{step}"))
                .unwrap()
                .family("f", Family::default())
                .create()
                .unwrap();
            tmp.mutate(b"x").put("f", b"q", b"y").commit().unwrap();
            drop(tmp);
            if step % 8 == 3 {
                primary.drop_table(&format!("tmp{step}")).unwrap();
            }
        }
    }
}

#[test]
fn a_replica_applies_a_primary_s_feed_and_matches_it() {
    let (pv, rv) = (SimVfs::new(1), SimVfs::new(2));
    let primary = Pigeonhole::open("/p/data.phdb", options(&pv)).unwrap();
    let mut feed = primary.feed().open().unwrap();
    let replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
    assert_eq!(replica.replicated_token().unwrap(), None);
    for step in 0..24 {
        workload(&primary, step);
        let events = ready(&mut feed);
        apply(&replica, &events, 1 + step as usize % 4);
    }
    assert_eq!(contents(&replica), contents(&primary));
    assert_eq!(replica.replicated_token().unwrap(), Some(feed.token()));

    // The state table is the replica's own: not listed, not found, not droppable, and no
    // name in its namespace can be made.
    assert!(
        !replica
            .tables()
            .iter()
            .any(|t| t.starts_with("__pigeonhole."))
    );
    assert!(
        replica
            .table("__pigeonhole.replication")
            .unwrap()
            .open()
            .is_err()
    );
    // No name in its namespace can be made, anywhere (here on the primary).
    let e = primary
        .table("__pigeonhole.mine")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
    // ro_compat bit 0 is set: a build without it opens the file read-only.
    assert_eq!(replica.info().unwrap().features.ro_compat & 1, 1);

    // A feed of every table on the replica sees the replicated commits, never its state.
    let mut own = replica.feed().open().unwrap();
    workload(&primary, 24);
    let events = ready(&mut feed);
    apply(&replica, &events, 8);
    while let Some(ev) = own.next_event(Duration::from_millis(200)).unwrap() {
        if let FeedEvent::Commit(c) = ev {
            assert!(
                c.changes()
                    .iter()
                    .all(|ch| !ch.table().starts_with("__pigeonhole."))
            );
        }
    }
    drop((feed, own));
    replica.close().unwrap();
    primary.close().unwrap();
    // The state outlives a reopen.
    let replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
    assert!(replica.replicated_token().unwrap().is_some());
    replica.close().unwrap();
}

#[test]
fn a_run_is_refused_unless_it_chains_on_what_was_applied() {
    let (pv, rv) = (SimVfs::new(3), SimVfs::new(4));
    let primary = Pigeonhole::open("/p/data.phdb", options(&pv)).unwrap();
    let mut feed = primary.feed().open().unwrap();
    let replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
    workload(&primary, 0);
    workload(&primary, 1);
    let events = ready(&mut feed);
    let first_commit = events.iter().position(|e| !e.is_catalog()).unwrap();
    let invalid = |r: pigeonhole::Result<()>| {
        let e = r.unwrap_err();
        assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
    };
    let t = events[0].token();
    invalid(replica.apply_replicated(&[], t));
    // A catalog change goes through apply_catalog, and a commit doesn't.
    invalid(replica.apply_replicated(&events[..1], t));
    invalid(replica.apply_catalog(&events[first_commit], events[first_commit].token()));
    // A gap: the first event left out.
    let tail = &events[1..2];
    invalid(replica.apply_replicated(tail, tail[0].token()));
    // Applied in order, then the same again: an overlap.
    apply(&replica, &events[..first_commit + 1], 4);
    let again = &events[first_commit..first_commit + 1];
    invalid(replica.apply_replicated(again, again[0].token()));
    // A token that isn't the run's last event's.
    let next = &events[first_commit + 1..first_commit + 3];
    invalid(replica.apply_replicated(next, next[0].token()));
    apply(&replica, &events[first_commit + 1..], 4);
    assert_eq!(contents(&replica), contents(&primary));
    drop(feed);
    replica.close().unwrap();
    primary.close().unwrap();
}

#[test]
fn a_run_writing_one_cell_twice_at_one_timestamp_keeps_the_later_write() {
    let (pv, rv) = (SimVfs::new(5), SimVfs::new(6));
    let primary = Pigeonhole::open("/p/data.phdb", options(&pv)).unwrap();
    let mut feed = primary.feed().open().unwrap();
    let replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
    let t = primary
        .table("t")
        .unwrap()
        .family("a", Family::default())
        .create()
        .unwrap();
    let c = primary
        .table("c")
        .unwrap()
        .family("n", Family::counter())
        .create()
        .unwrap();
    for v in [b"first", b"later"] {
        t.mutate(b"r").put_at("a", b"q", 7, v).commit().unwrap();
        c.mutate(b"hits").incr("n", b"q", 1).commit().unwrap();
    }
    t.mutate(b"r").delete_cell("a", b"q", 7).commit().unwrap();
    t.mutate(b"r")
        .put_at("a", b"q", 7, b"last")
        .commit()
        .unwrap();
    let events = ready(&mut feed);
    // Every commit in one run: it splits where a cell and timestamp repeat.
    apply(&replica, &events, usize::MAX);
    assert_eq!(contents(&replica), contents(&primary));
    let r = replica.table("c").unwrap().open().unwrap();
    assert_eq!(
        r.get(b"hits", "n", b"q").unwrap().unwrap().typed(),
        Value::I64(2)
    );
    drop(feed);
    replica.close().unwrap();
    primary.close().unwrap();
}

#[test]
fn a_replica_resumes_from_its_token_after_a_crash_with_nothing_twice_or_skipped() {
    for seed in 0..6u64 {
        let (pv, rv) = (SimVfs::new(10 + seed), SimVfs::new(20 + seed));
        let primary = Pigeonhole::open("/p/data.phdb", options(&pv)).unwrap();
        // The primary's feed from its start, as a replica's first subscription is.
        let start = primary.feed().open().unwrap();
        let mut token = start.token();
        drop(start);
        let mut replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
        for round in 0..4u32 {
            for step in 0..6 {
                workload(&primary, round * 6 + step);
            }
            // The replica follows from its stored token (or the start), applies some of
            // what's ready, then crashes.
            let mut feed = primary.feed().resume(&token).open().unwrap();
            let events = ready(&mut feed);
            let upto = (events.len() * (1 + (seed as usize + round as usize) % 3)) / 3;
            apply(&replica, &events[..upto], 1 + seed as usize % 3);
            drop(feed);
            if round % 2 == 1 {
                // A power loss inside an apply: what it applied and the token stored with
                // it survive together or not at all.
                let k = 1 + (seed * 7 + u64::from(round) * 13) % 40;
                let mut plan = FaultPlan::none();
                plan.crash_after_ops = Some(rv.mutating_ops() + k);
                rv.set_faults(plan);
                let _ = try_apply(&replica, &events[upto..], 2);
                rv.set_faults(FaultPlan::none());
            }
            let kind = if (seed + u64::from(round)) % 2 == 0 {
                CrashKind::Power
            } else {
                CrashKind::Process
            };
            rv.crash(kind);
            drop(replica);
            replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
            if let Some(t) = replica.replicated_token().unwrap() {
                token = t;
            }
        }
        // Caught up from the last stored token: the same as the primary, every counter
        // increment once.
        let mut feed = primary.feed().resume(&token).open().unwrap();
        let events = ready(&mut feed);
        apply(&replica, &events, 3);
        assert_eq!(contents(&replica), contents(&primary), "seed {seed}");
        drop(feed);
        replica.close().unwrap();
        primary.close().unwrap();
    }
}

/// A replica takes only its primary's changes (simp724's R6 on #1562): once it has applied
/// one, a local schema change would take the ids the primary assigns next, and a local
/// write would silently diverge the mirror, so both are refused with `ReadOnly`; replication
/// goes on.
#[test]
fn a_replica_refuses_local_writes_and_schema_changes() {
    let (pv, rv) = (SimVfs::new(30), SimVfs::new(31));
    let primary = Pigeonhole::open("/p/data.phdb", options(&pv)).unwrap();
    let mut feed = primary.feed().open().unwrap();
    let replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
    workload(&primary, 0);
    apply(&replica, &ready(&mut feed), 4);
    let read_only = |r: pigeonhole::Result<()>| {
        let e = r.unwrap_err();
        assert_eq!(e.code(), ErrorCode::ReadOnly, "{e}");
    };
    read_only(
        replica
            .table("local")
            .unwrap()
            .family("f", Family::default())
            .create()
            .map(drop),
    );
    let t = replica.table("t").unwrap().open().unwrap();
    read_only(t.mutate(b"local").put("a", b"q", b"v").commit().map(drop));
    read_only(replica.drop_table("t"));
    drop(t);
    // Replication goes on, across a reopen too.
    replica.close().unwrap();
    let replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
    let t = replica.table("t").unwrap().open().unwrap();
    read_only(t.mutate(b"local").put("a", b"q", b"v").commit().map(drop));
    drop(t);
    for step in 1..6 {
        workload(&primary, step);
    }
    apply(&replica, &ready(&mut feed), 4);
    assert_eq!(contents(&replica), contents(&primary));
    drop(feed);
    replica.close().unwrap();
    primary.close().unwrap();
}

/// A run larger than a commit should carry splits by bytes (simp724's S1 on #1562), between
/// primary commits, and still lands whole.
#[test]
fn a_large_run_splits_by_bytes() {
    let (pv, rv) = (SimVfs::new(32), SimVfs::new(33));
    let primary = Pigeonhole::open("/p/data.phdb", options(&pv)).unwrap();
    let mut feed = primary.feed().open().unwrap();
    let replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
    let t = primary
        .table("big")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    // 4 MiB of values over 2 shards: well past a quarter of one shard's memtable share.
    let value = vec![7u8; 256 << 10];
    for i in 0..16u32 {
        t.mutate(&i.to_be_bytes())
            .put("f", b"q", &value)
            .commit()
            .unwrap();
    }
    let events = ready(&mut feed);
    // The table's creation first, then the 16 commits as one run.
    apply(&replica, &events[..1], 1);
    let before = replica.visible_seqno();
    apply(&replica, &events[1..], usize::MAX);
    // A commit per part: 4 MiB against a 512 KiB cap is at least 8.
    let commits = replica.visible_seqno() - before;
    assert!(commits >= 8, "the run committed as {commits} commits");
    assert_eq!(contents(&replica), contents(&primary));
    assert_eq!(replica.replicated_token().unwrap(), Some(feed.token()));
    drop((feed, t));
    replica.close().unwrap();
    primary.close().unwrap();
}

/// The bootstrap handshake (#1436, ICR 0125): a backup of one durable snapshot and a feed of
/// every table opened from that snapshot meet exactly. Catalog changes before the snapshot
/// are in the copy, never replayed; those after arrive as events (simp724's P2 note).
#[test]
fn a_backup_and_a_feed_from_one_snapshot_bootstrap_a_replica() {
    let vfs = SimVfs::new(40);
    let primary = Pigeonhole::open("/p/data.phdb", options(&vfs)).unwrap();
    for step in 0..6 {
        workload(&primary, step);
    }
    let snap = primary.durable_snapshot().unwrap();
    primary.backup_at(&snap, "/p/copy.phdb").unwrap();
    let mut feed = primary.feed().from_snapshot(&snap).open().unwrap();
    // The bootstrap's token: the feed's, before it delivers anything.
    let start = feed.token();
    drop(snap);
    // After the snapshot: more commits, a table created and one dropped.
    for step in 6..14 {
        workload(&primary, step);
    }
    let replica = Pigeonhole::open("/p/copy.phdb", options(&vfs)).unwrap();
    // A copy holds the primary's tables: the from-nothing first apply refuses it.
    let events = ready(&mut feed);
    assert!(
        events.iter().any(ReplicatedEvent::is_catalog),
        "catalog changes after S"
    );
    replica.begin_replication(&start).unwrap();
    // Already a replica: a second bootstrap is refused.
    let e = replica.begin_replication(&start).unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
    apply(&replica, &events, 4);
    assert_eq!(contents(&replica), contents(&primary));
    drop(feed);
    replica.close().unwrap();
    primary.close().unwrap();
}

/// `promote` (#1436, ICR 0125): the replica becomes a primary at what it applied last. It
/// returns the token it stopped at, takes writes, keeps its own feed going without a catalog
/// event for the state table, clears `ro_compat` bit 0, and refuses any apply after it.
#[test]
fn a_promoted_replica_takes_writes_and_refuses_applies() {
    let (pv, rv) = (SimVfs::new(41), SimVfs::new(42));
    let primary = Pigeonhole::open("/p/data.phdb", options(&pv)).unwrap();
    let mut feed = primary.feed().open().unwrap();
    let replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
    for step in 0..4 {
        workload(&primary, step);
    }
    apply(&replica, &ready(&mut feed), 4);
    let mut own = replica.feed().open().unwrap();
    let stopped = replica.promote().unwrap();
    assert_eq!(stopped, Some(feed.token()));
    assert_eq!(
        replica.info().unwrap().features.ro_compat & 1,
        0,
        "the bit is cleared"
    );
    let e = replica.promote().unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
    // Writes are taken.
    let t = replica.table("t").unwrap().open().unwrap();
    t.mutate(b"local").put("a", b"q", b"v").commit().unwrap();
    // No catalog event for the state table's drop on the replica's own feed.
    while let Some(ev) = own.next_event(Duration::from_millis(200)).unwrap() {
        assert!(!matches!(ev, FeedEvent::Catalog(_)), "{ev:?}");
    }
    // An apply after the promote is refused, and nothing comes back.
    workload(&primary, 4);
    let events = ready(&mut feed);
    let first = events.iter().position(|e| !e.is_catalog()).unwrap();
    let e = replica
        .apply_replicated(&events[first..=first], events[first].token())
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
    assert_eq!(replica.info().unwrap().features.ro_compat & 1, 0);
    assert_eq!(replica.replicated_token().unwrap(), None);
    drop((t, own, feed));
    replica.close().unwrap();
    // After a reopen: an ordinary database.
    let replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
    replica
        .table("t")
        .unwrap()
        .open()
        .unwrap()
        .mutate(b"later")
        .put("a", b"q", b"v")
        .commit()
        .unwrap();
    replica.close().unwrap();
    primary.close().unwrap();
}

/// `replace_with` (#1436: a replica's re-bootstrap): a handle's file is swapped for a fresh
/// copy under its locks. Every clone then reads the copy, the old copy's WAL is gone, and a
/// second process's writer is refused before and after.
#[test]
fn replace_with_swaps_a_handle_s_file_for_a_copy_under_its_locks() {
    let vfs = SimVfs::new(43);
    let primary = Pigeonhole::open("/p/data.phdb", options(&vfs)).unwrap();
    for step in 0..6 {
        workload(&primary, step);
    }
    let replica = Pigeonhole::open("/r/data.phdb", options(&vfs)).unwrap();
    replica
        .table("old")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let clone = replica.clone();
    let other = || {
        let vfs = Arc::clone(&vfs);
        std::thread::spawn(move || {
            vfs.enter_process(pigeonhole_io::ProcessId {
                pid: 99,
                start_time: 1,
            });
            Pigeonhole::open("/r/data.phdb", options(&vfs))
                .err()
                .map(|e| e.code())
        })
        .join()
        .unwrap()
    };
    assert_eq!(other(), Some(ErrorCode::WriterLocked));
    // A side copy whose last writer (another process) didn't close it is refused: its WAL,
    // named after the side path, wouldn't be replayed after the rename (simp724's A1).
    let snap = primary.durable_snapshot().unwrap();
    primary.backup_at(&snap, "/r/crashed.phdb").unwrap();
    let crashed = pigeonhole_io::ProcessId {
        pid: 77,
        start_time: 1,
    };
    {
        let vfs = Arc::clone(&vfs);
        std::thread::spawn(move || {
            vfs.enter_process(crashed);
            let copy = Pigeonhole::open("/r/crashed.phdb", options(&vfs)).unwrap();
            copy.table("only_in_wal")
                .unwrap()
                .family("f", Family::default())
                .create()
                .unwrap();
            std::mem::forget(copy);
        })
        .join()
        .unwrap();
    }
    vfs.crash_process(crashed);
    let refused = replica.replace_with("/r/crashed.phdb").unwrap_err();
    assert_eq!(refused.code(), ErrorCode::InvalidArgument, "{refused}");
    assert!(
        replica.table("old").unwrap().open().is_ok(),
        "a refused replace leaves the database as it was"
    );

    // The bootstrap's way: the copy opened under its own name (its WAL named after it),
    // changed, closed cleanly, then swapped in. Its WAL goes with the replace.
    primary.backup_at(&snap, "/r/side.phdb").unwrap();
    drop(snap);
    let side = Pigeonhole::open("/r/side.phdb", options(&vfs)).unwrap();
    side.table("in_side")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    side.close().unwrap();
    let side_wal = |vfs: &SimVfs| {
        pigeonhole_io::Vfs::list_dir(vfs, std::path::Path::new("/r"))
            .unwrap()
            .into_iter()
            .filter(|p| {
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("side.phdb-"))
            })
            .count()
    };
    assert!(side_wal(&vfs) > 0, "the side copy's open left its own WAL");
    let before = vfs.shared_regions();
    replica.replace_with("/r/side.phdb").unwrap();
    assert_eq!(side_wal(&vfs), 0, "the side copy's WAL and sidecar go");
    // The new file's region in place of the old one's (simp724's A3).
    assert_eq!(
        vfs.shared_regions(),
        before,
        "the old file's shm region goes"
    );
    assert!(replica.table("in_side").unwrap().open().is_ok());
    assert_eq!(
        contents(&clone),
        contents(&primary),
        "every clone reads the copy"
    );
    assert!(
        replica.table("old").unwrap().open().is_err(),
        "the old file is gone"
    );
    assert_eq!(other(), Some(ErrorCode::WriterLocked));
    replica.close().unwrap();
    primary.close().unwrap();
}

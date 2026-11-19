//! D315 PR 3: a replica applies a primary's feed of every table with the primary's ids,
//! storing the primary's token in the same commit as what it applied, and resumes from it
//! after a crash with nothing applied twice or skipped.
#![cfg(feature = "__unstable-replication")]

use std::sync::Arc;
use std::time::Duration;

use pigeonhole::replication::ReplicatedEvent;
use pigeonhole::{
    ErrorCode, Family, Feed, FeedEvent, FeedRetention, FeedToken, Options, Pigeonhole, Value,
};
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
        if events[i].is_range_drop() {
            replica
                .apply_range_drop(&events[i], events[i].token())
                .unwrap();
            i += 1;
            continue;
        }
        let end = (i..events.len())
            .take(run)
            .take_while(|&j| !events[j].is_catalog() && !events[j].is_range_drop())
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
    let alone = |e: &ReplicatedEvent| e.is_catalog() || e.is_range_drop();
    for chunk in events.chunk_by(|a, b| !alone(a) && !alone(b)) {
        if chunk[0].is_catalog() {
            replica.apply_catalog(&chunk[0], chunk[0].token())?;
            continue;
        }
        if chunk[0].is_range_drop() {
            replica.apply_range_drop(&chunk[0], chunk[0].token())?;
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

/// A reader process attached to the old file when `replace_with` swaps a new copy in, and
/// closing after it, must not remove the new engine's WAL (simp724's B1 on #1583). Its
/// last-one-out cleanup takes the writer byte on its own (old, now unlocked) inode, and the
/// WAL files are found by path, which names the new file; the new file still reads clean
/// until its first root commit. Commits after the swap, before any flush, then live only in
/// that WAL: a crash and reopen keep them.
#[test]
fn a_reader_of_the_replaced_file_never_removes_the_new_file_s_wal() {
    let vfs = SimVfs::new(44);
    let primary = Pigeonhole::open("/p/data.phdb", options(&vfs)).unwrap();
    primary
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let snap = primary.durable_snapshot().unwrap();
    primary.backup_at(&snap, "/r/side.phdb").unwrap();
    drop(snap);
    primary.close().unwrap();
    let db = Pigeonhole::open("/r/data.phdb", options(&vfs)).unwrap();

    // A reader process on the old file, until told to close.
    let (attached_tx, attached) = std::sync::mpsc::channel();
    let (close_tx, close) = std::sync::mpsc::channel::<()>();
    let reader = {
        let vfs = Arc::clone(&vfs);
        std::thread::spawn(move || {
            vfs.enter_process(pigeonhole_io::ProcessId {
                pid: 88,
                start_time: 1,
            });
            let r = Pigeonhole::open_reader(
                "/r/data.phdb",
                pigeonhole::ReaderOptions::default().vfs(Arc::clone(&vfs) as _),
            )
            .unwrap();
            attached_tx.send(()).unwrap();
            close.recv().unwrap();
            drop(r);
        })
    };
    attached.recv().unwrap();

    db.replace_with("/r/side.phdb").unwrap();
    // Writes to a table the copy has: no catalog change, so no root commit, so the new
    // file still reads clean; the rows are only in its WAL.
    let t = db.table("t").unwrap().open().unwrap();
    for i in 0..20u32 {
        t.mutate(format!("r{i:02}").as_bytes())
            .put("f", b"q", b"after the swap")
            .commit()
            .unwrap();
    }
    close_tx.send(()).unwrap();
    reader.join().unwrap();

    vfs.crash(CrashKind::Process);
    drop(t);
    drop(db);
    let db = Pigeonhole::open("/r/data.phdb", options(&vfs)).unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    for i in 0..20u32 {
        assert!(
            t.get(format!("r{i:02}").as_bytes(), "f", b"q")
                .unwrap()
                .is_some(),
            "r{i:02}, committed after the swap, survives the crash"
        );
    }
    drop(t);
    db.close().unwrap();
}

/// D331 (ICR 0141): a primary's feed-silent commits reach a replica that includes them, as
/// version 2 events (the rest stay version 1), and the replica applies them silent: it
/// matches the primary, and neither's own subscription ever sees a silent write. A feed
/// that doesn't include them (any client's) passes them over. A newer event version is
/// refused.
#[test]
fn a_replica_applies_silent_commits_silent() {
    let (pv, rv) = (SimVfs::new(31), SimVfs::new(32));
    let primary = Pigeonhole::open("/p/data.phdb", options(&pv)).unwrap();
    let mut feed = primary.feed().open().unwrap();
    feed.include_silent();
    let mut client = primary.feed().open().unwrap();
    let replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
    let mut own = replica.feed().open().unwrap();
    let t = primary
        .table("t")
        .unwrap()
        .family("a", Family::default())
        .create_if_missing()
        .unwrap();
    for step in 0..12u32 {
        let silent = step % 3 != 0;
        let mut wb = primary.write_batch();
        if silent {
            wb.mark_feed_silent();
        }
        let row = format!("{}{step}", if silent { "quiet" } else { "loud" });
        wb.put(&t, row.as_bytes(), "a", b"q", b"v");
        wb.commit().unwrap();
        let events = ready(&mut feed);
        for e in &events {
            let version = e.encode()[0];
            assert_eq!(version, if e.is_silent() { 2 } else { 1 });
        }
        assert!(events.iter().any(|e| e.is_silent()) == silent);
        apply(&replica, &events, 3);
    }
    assert_eq!(contents(&replica), contents(&primary));

    for (who, f) in [
        ("the primary's client", &mut client),
        ("the replica's own", &mut own),
    ] {
        while let Some(ev) = f.next_event(Duration::from_millis(200)).unwrap() {
            if let FeedEvent::Commit(c) = ev {
                assert!(
                    c.changes().iter().all(|ch| !ch.row().starts_with(b"quiet")),
                    "{who} feed delivered a silent write"
                );
            }
        }
    }

    let mut wb = primary.write_batch();
    wb.put(&t, b"last", "a", b"q", b"v");
    wb.commit().unwrap();
    let mut bytes = ready(&mut feed)[0].encode();
    bytes[0] = 3;
    assert!(
        ReplicatedEvent::decode(&bytes).is_err(),
        "a newer version is refused"
    );
    drop((feed, client, own));
    replica.close().unwrap();
    primary.close().unwrap();
}

/// D331 PR 4 (ICR 0141): a range move's target writes the source's changes as the source
/// logged them, by name into its own ids, feed-silently: puts at explicit timestamps, a
/// column delete that hides an older `put_at` written after it, counter increments and a
/// counter delete, in commit order. The target then reads as the source does, its own feed
/// carries none of it, and a family the target lacks fails the whole batch.
#[test]
fn a_logged_batch_writes_another_databases_changes_by_name() {
    let (sv, tv) = (SimVfs::new(41), SimVfs::new(42));
    let source = Pigeonhole::open("/s/data.phdb", options(&sv)).unwrap();
    let target = Pigeonhole::open("/t/data.phdb", options(&tv)).unwrap();
    let mut feed = source.feed().open().unwrap();
    // The target's ids differ from the source's: another table and family come first.
    target
        .table("other")
        .unwrap()
        .family("x", Family::default())
        .create_if_missing()
        .unwrap();
    let make = |db: &Pigeonhole| {
        db.table("t")
            .unwrap()
            .family("z", Family::default())
            .family("a", Family::default())
            .family("n", Family::counter())
            .create_if_missing()
            .unwrap()
    };
    let (s, t) = (make(&source), make(&target));
    let mut own = target.feed().open().unwrap();

    s.mutate(b"r1")
        .put_at("a", b"q", 10, b"v10")
        .commit()
        .unwrap();
    s.mutate(b"r1")
        .put_at("a", b"q", 20, b"v20")
        .commit()
        .unwrap();
    s.mutate(b"r2").put("a", b"q", b"now").commit().unwrap();
    s.mutate(b"r2").delete_column("a", b"q").commit().unwrap();
    // Older than the delete: hidden on the source, and must be on the target too.
    s.mutate(b"r2")
        .put_at("a", b"q", 5, b"old")
        .commit()
        .unwrap();
    s.mutate(b"c").incr("n", b"q", 3).commit().unwrap();
    s.mutate(b"c").delete_column("n", b"q").commit().unwrap();
    s.mutate(b"c").incr("n", b"q", 4).commit().unwrap();

    while let Some(ev) = feed.next_event(Duration::from_millis(200)).unwrap() {
        let FeedEvent::Commit(c) = ev else { continue };
        let mut wb = target.logged_batch();
        wb.mark_feed_silent();
        for ch in c.changes() {
            wb.push(
                &t,
                ch.row(),
                ch.family(),
                ch.kind(),
                ch.qualifier(),
                ch.timestamp(),
                ch.stored(),
            );
        }
        wb.commit().unwrap();
    }
    let of_t = |db: &Pigeonhole| -> Vec<String> {
        contents(db)
            .into_iter()
            .filter(|l| l.starts_with("t\t"))
            .collect()
    };
    assert_eq!(of_t(&target), of_t(&source));
    assert_eq!(
        t.get(b"c", "n", b"q").unwrap().map(|c| c.as_i64()),
        Some(Some(4))
    );
    assert!(t.get(b"r2", "a", b"q").unwrap().is_none());
    assert!(
        own.next_event(Duration::from_millis(200))
            .unwrap()
            .is_none(),
        "the target's own feed carries none of the source's changes"
    );

    // A family the target lacks fails the batch whole: nothing of it commits.
    let mut wb = target.logged_batch();
    wb.push(
        &t,
        b"r3",
        "a",
        pigeonhole::ChangeKind::Put,
        b"q",
        7,
        b"\x00x",
    );
    wb.push(
        &t,
        b"r3",
        "missing",
        pigeonhole::ChangeKind::Put,
        b"q",
        7,
        b"\x00x",
    );
    assert_eq!(wb.commit().unwrap_err().code(), ErrorCode::FamilyNotFound);
    assert!(t.get(b"r3", "a", b"q").unwrap().is_none());
    drop((feed, own));
    target.close().unwrap();
    source.close().unwrap();
}

/// Every row of `t` with a cell in family "a" qualifier "q", as `row=value`, at `snap` if
/// given.
fn rows_of(t: &pigeonhole::Table, snap: Option<&pigeonhole::Snapshot>) -> Vec<String> {
    let mut out = Vec::new();
    for i in 0..10u8 {
        let row = [b'r', b'0' + i];
        let text = |v: Value<'_>| match v {
            Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
            other => format!("{other:?}"),
        };
        let cell = match snap {
            Some(s) => t
                .get_at(s, &row, "a", b"q")
                .unwrap()
                .map(|c| text(c.typed())),
            None => t.get(&row, "a", b"q").unwrap().map(|c| text(c.typed())),
        };
        if let Some(v) = cell {
            out.push(format!("{}={v}", String::from_utf8_lossy(&row)));
        }
    }
    out
}

/// D331 PR 4a (B1, the coordinator's test): a dropped range takes writes at older
/// timestamps again, which delete markers never would. Rows `r3..r7` are dropped from the
/// memtable and the SSTs alike, a column delete inside the range goes with them, a snapshot
/// taken before still reads the range whole, the rows outside are untouched, and writes of
/// the range at timestamps below the dropped versions and below the dropped marker are then
/// visible: before a crash, after it, and after the drop is run again.
#[test]
fn a_dropped_range_takes_older_writes_again() {
    let vfs = SimVfs::new(43);
    let db = Pigeonhole::open("/d/data.phdb", options(&vfs)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("a", Family::default())
        .create_if_missing()
        .unwrap();
    for i in 0..10u8 {
        t.mutate(&[b'r', b'0' + i])
            .put_at("a", b"q", 100, format!("v{i}").as_bytes())
            .commit()
            .unwrap();
        // Half the rows in the SSTs, half in the memtable.
        if i == 4 {
            db.flush().unwrap();
        }
    }
    t.mutate(b"r5").delete_column("a", b"q").commit().unwrap();
    let before = db.snapshot().unwrap();
    let whole = rows_of(&t, Some(&before));
    assert_eq!(whole.len(), 9, "{whole:?}");

    db.drop_range(&t, b"r3", Some(b"r7")).unwrap();
    let outside = ["r0=v0", "r1=v1", "r2=v2", "r7=v7", "r8=v8", "r9=v9"];
    assert_eq!(rows_of(&t, None), outside);
    assert_eq!(
        rows_of(&t, Some(&before)),
        whole,
        "a snapshot keeps its view"
    );
    drop(before);

    // Below the dropped versions (ts 100) and below the dropped marker: visible now.
    for row in [&b"r3"[..], b"r5"] {
        t.mutate(row)
            .put_at("a", b"q", 50, b"old")
            .commit()
            .unwrap();
    }
    let expect = [
        "r0=v0", "r1=v1", "r2=v2", "r3=old", "r5=old", "r7=v7", "r8=v8", "r9=v9",
    ];
    assert_eq!(rows_of(&t, None), expect);

    vfs.crash(CrashKind::Process);
    drop(t);
    drop(db);
    let db = Pigeonhole::open("/d/data.phdb", options(&vfs)).unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    assert_eq!(rows_of(&t, None), expect, "after a crash");
    // Run again (the caller's rule after a crash): the range's own writes since go too.
    db.drop_range(&t, b"r3", Some(b"r7")).unwrap();
    assert_eq!(rows_of(&t, None), outside);
    db.flush().unwrap();
    db.compact().unwrap();
    assert_eq!(rows_of(&t, None), outside, "after compaction");
    drop(t);
    db.close().unwrap();
}

/// D331 PR 4a: a range drop refuses a range that ends at or before it starts.
#[test]
fn a_range_drop_refuses_a_bad_range() {
    let vfs = SimVfs::new(44);
    let db = Pigeonhole::open("/d/data.phdb", options(&vfs)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("a", Family::default())
        .create_if_missing()
        .unwrap();
    for (start, end) in [(&b"m"[..], &b"m"[..]), (b"m", b"a")] {
        assert_eq!(
            db.drop_range(&t, start, Some(end)).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );
    }
    drop(t);
    db.close().unwrap();
}

/// D331 PR 4a (perf2's condition 5): a range drop is feed-silent, and a subscription open
/// across it goes on: its seqno passes with no event, the commits on both sides of it
/// arrive once each, and tokens taken just before and just after it resume, also after a
/// crash.
#[test]
fn a_feed_passes_a_range_drop_and_its_tokens_resume() {
    let vfs = SimVfs::new(45);
    let db = Pigeonhole::open("/d/data.phdb", options(&vfs)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("a", Family::default())
        .create_if_missing()
        .unwrap();
    let commit = |t: &pigeonhole::Table, row: &[u8]| {
        t.mutate(row).put("a", b"q", b"v").commit().unwrap().seqno
    };
    let next_seqno = |feed: &mut Feed| match feed.next_event(Duration::from_secs(5)).unwrap() {
        Some(FeedEvent::Commit(c)) => c.seqno(),
        other => panic!("expected a commit, got {other:?}"),
    };
    let mut feed = t.feed().open().unwrap();
    let one = commit(&t, b"r1");
    commit(&t, b"r6");
    assert_eq!(next_seqno(&mut feed), one);
    next_seqno(&mut feed);
    let before = FeedToken::from_bytes(feed.token().as_bytes().to_vec());
    db.drop_range(&t, b"r5", Some(b"r9")).unwrap();
    assert!(
        feed.next_event(Duration::from_millis(200))
            .unwrap()
            .is_none(),
        "a range drop delivers nothing"
    );
    let after = FeedToken::from_bytes(feed.token().as_bytes().to_vec());
    let two = commit(&t, b"r2");
    assert_eq!(next_seqno(&mut feed), two);
    drop(feed);

    vfs.crash(CrashKind::Process);
    drop(t);
    drop(db);
    let db = Pigeonhole::open("/d/data.phdb", options(&vfs)).unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    for token in [&before, &after] {
        let mut feed = t.feed().resume(token).open().unwrap();
        assert_eq!(next_seqno(&mut feed), two);
        assert!(
            feed.next_event(Duration::from_millis(200))
                .unwrap()
                .is_none()
        );
    }
    drop(t);
    db.close().unwrap();
}

/// D331 PR 4a (perf2's condition 1): a primary's range drop reaches a replica in feed
/// order, between the commits before it and after it. The replica lags: it applies the
/// writes of the range, the drop, and a write of the range at an older timestamp only
/// after all of them happened, and then reads as the primary: the dropped rows gone, the
/// older write visible. A client's feed of the primary never sees the drop.
#[test]
fn a_replica_applies_a_range_drop_in_feed_order() {
    let (pv, rv) = (SimVfs::new(46), SimVfs::new(47));
    let primary = Pigeonhole::open("/p/data.phdb", options(&pv)).unwrap();
    let mut feed = primary.feed().open().unwrap();
    feed.include_silent();
    let replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
    let t = primary
        .table("t")
        .unwrap()
        .family("a", Family::default())
        .create_if_missing()
        .unwrap();
    let mut client = t.feed().open().unwrap();
    for i in 0..10u8 {
        t.mutate(&[b'r', b'0' + i])
            .put_at("a", b"q", 100, format!("v{i}").as_bytes())
            .commit()
            .unwrap();
    }
    primary.drop_range(&t, b"r3", Some(b"r7")).unwrap();
    t.mutate(b"r4")
        .put_at("a", b"q", 50, b"old")
        .commit()
        .unwrap();

    let events = ready(&mut feed);
    let drops: Vec<usize> = (0..events.len())
        .filter(|&i| events[i].is_range_drop())
        .collect();
    assert_eq!(
        drops.len(),
        1,
        "one drop event, though every stream logged it"
    );
    assert!(
        drops[0] + 1 < events.len() && !events[drops[0] + 1].is_catalog(),
        "the drop comes before the write after it"
    );
    apply(&replica, &events, 3);
    assert_eq!(contents(&replica), contents(&primary));
    let rt = replica.table("t").unwrap().open().unwrap();
    assert_eq!(
        rows_of(&rt, None),
        [
            "r0=v0", "r1=v1", "r2=v2", "r4=old", "r7=v7", "r8=v8", "r9=v9"
        ]
    );

    let mut seen = 0;
    while let Some(ev) = client.next_event(Duration::from_millis(200)).unwrap() {
        assert!(matches!(ev, FeedEvent::Commit(_)), "{ev:?}");
        seen += 1;
    }
    assert_eq!(
        seen, 11,
        "the client sees every commit and nothing for the drop"
    );
    drop((feed, client, rt, t));
    replica.close().unwrap();
    primary.close().unwrap();
}

/// D331 PR 4a: a range drop's event round-trips, bounded and to the table's end, and its
/// bytes re-encode identically (feed1248's S2); a truncated one is refused, and so is one in
/// any version but the plain one (the coordinator's condition; feed1248's M1: version 2
/// here, and version 3, D329 PR 2b's flags form, whichever lands first).
#[test]
fn a_range_drop_event_round_trips_and_a_flagged_one_is_refused() {
    let vfs = SimVfs::new(48);
    let db = Pigeonhole::open("/d/data.phdb", options(&vfs)).unwrap();
    let mut feed = db.feed().open().unwrap();
    feed.include_silent();
    let t = db
        .table("t")
        .unwrap()
        .family("a", Family::default())
        .create_if_missing()
        .unwrap();
    db.drop_range(&t, b"b", Some(b"m\x00")).unwrap();
    db.drop_range(&t, b"x", None).unwrap();
    let drops: Vec<ReplicatedEvent> = ready(&mut feed)
        .into_iter()
        .filter(ReplicatedEvent::is_range_drop)
        .collect();
    assert_eq!(drops.len(), 2, "both drops are delivered");
    for event in &drops {
        let bytes = event.encode();
        assert_eq!(bytes[0], 1, "the plain version");
        let back = ReplicatedEvent::decode(&bytes).unwrap();
        assert_eq!(&back, event);
        assert_eq!(back.encode(), bytes, "re-encoded byte for byte");
        for cut in 1..bytes.len() {
            assert!(
                ReplicatedEvent::decode(&bytes[..cut]).is_err(),
                "truncated at {cut}"
            );
        }
        for version in [2u8, 3] {
            let mut flagged = bytes.clone();
            flagged[0] = version;
            assert_eq!(
                ReplicatedEvent::decode(&flagged).unwrap_err().code(),
                ErrorCode::InvalidArgument,
                "version {version}"
            );
        }
    }
    drop((feed, t));
    db.close().unwrap();
}

/// D331 PR 4a (perf2's conditions 2 and 3): a crash at any step of a range drop leaves the
/// range whole or gone, never partly, with the rows outside it untouched, and the drop run
/// again (the caller's rule) finishes it. The crash comes after the drop's `k`th mutating
/// operation, for every `k` until the drop completes first: after its splits, after the
/// record reached some streams or all, before and after its manifest commit, with power and
/// process crashes in turn. Each reopen then takes a write at a lower seqno than nothing
/// (a commit), the drop again, and a write of the range at an older timestamp, visible.
#[test]
fn a_range_drop_cut_by_a_crash_finishes_when_run_again() {
    let outside = ["r0=v0", "r1=v1", "r2=v2", "r7=v7", "r8=v8", "r9=v9"];
    let whole = [
        "r0=v0", "r1=v1", "r2=v2", "r3=v3", "r4=v4", "r5=v5", "r6=v6", "r7=v7", "r8=v8", "r9=v9",
    ];
    let make = |db: &Pigeonhole| {
        db.table("t")
            .unwrap()
            .family("a", Family::default())
            .create_if_missing()
            .unwrap()
    };
    let mut finished = false;
    let mut k = 1u64;
    while !finished {
        assert!(k < 500, "the drop never finished before its crash point");
        let vfs = SimVfs::new(1000 + k);
        let db = Pigeonhole::open("/d/data.phdb", options(&vfs)).unwrap();
        let t = make(&db);
        for i in 0..10u8 {
            t.mutate(&[b'r', b'0' + i])
                .put_at("a", b"q", 100, format!("v{i}").as_bytes())
                .commit()
                .unwrap();
            if i == 4 {
                db.flush().unwrap();
            }
        }
        let mut plan = FaultPlan::none();
        plan.crash_after_ops = Some(vfs.mutating_ops() + k);
        vfs.set_faults(plan);
        finished = db.drop_range(&t, b"r3", Some(b"r7")).is_ok();
        vfs.set_faults(FaultPlan::none());
        vfs.crash(if k.is_multiple_of(2) {
            CrashKind::Power
        } else {
            CrashKind::Process
        });
        drop(t);
        drop(db);

        let db = Pigeonhole::open("/d/data.phdb", options(&vfs)).unwrap();
        let t = db.table("t").unwrap().open().unwrap();
        let rows = rows_of(&t, None);
        assert!(
            rows == whole || rows == outside,
            "k {k}: a crashed drop left the range partly: {rows:?}"
        );
        assert!(
            !finished || rows == outside,
            "k {k}: a drop that returned came back after a crash: {rows:?}"
        );
        t.mutate(b"r0")
            .put_at("a", b"q", 100, b"v0")
            .commit()
            .unwrap();
        db.drop_range(&t, b"r3", Some(b"r7")).unwrap();
        assert_eq!(rows_of(&t, None), outside, "k {k}: run again");
        t.mutate(b"r4")
            .put_at("a", b"q", 50, b"old")
            .commit()
            .unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        let mut expect = outside.to_vec();
        expect.insert(3, "r4=old");
        assert_eq!(rows_of(&t, None), expect, "k {k}: after the re-run");
        drop(t);
        db.close().unwrap();
        k += 1;
    }
    // The sweep crossed the drop's steps: its splits, a record per stream (2 here) and
    // its manifest commit each took at least one mutating operation.
    assert!(k > 4, "the drop finished at its {}th operation", k - 1);
    eprintln!("the drop finished at operation {}", k - 1);
}

/// D331 PR 4a (feed1248 on #2089): a range drop followed at once by a catalog change reaches
/// a replica's follower as one drop, then the change, and the replica matches its primary:
/// the change isn't taken for a second drop.
#[test]
fn a_catalog_change_right_after_a_range_drop_is_delivered_as_itself() {
    let (pv, rv) = (SimVfs::new(50), SimVfs::new(51));
    let primary = Pigeonhole::open("/p/data.phdb", options(&pv)).unwrap();
    let mut feed = primary.feed().open().unwrap();
    feed.include_silent();
    let replica = Pigeonhole::open("/r/data.phdb", options(&rv)).unwrap();
    let t = primary
        .table("t")
        .unwrap()
        .family("a", Family::default())
        .create_if_missing()
        .unwrap();
    t.mutate(b"r1").put("a", b"q", b"v").commit().unwrap();
    primary.drop_range(&t, b"r0", Some(b"r5")).unwrap();
    primary
        .table("u")
        .unwrap()
        .family("b", Family::default())
        .create_if_missing()
        .unwrap();
    let events = ready(&mut feed);
    let kinds: Vec<&str> = events
        .iter()
        .map(|e| {
            if e.is_range_drop() {
                "drop"
            } else if e.is_catalog() {
                "catalog"
            } else {
                "commit"
            }
        })
        .collect();
    assert_eq!(kinds, ["catalog", "commit", "drop", "catalog"], "{kinds:?}");
    apply(&replica, &events, 3);
    assert_eq!(contents(&replica), contents(&primary));
    assert!(replica.table("u").unwrap().open().is_ok());
    drop((feed, t));
    replica.close().unwrap();
    primary.close().unwrap();
}

/// D331 PR 4a (B2): a range copied chunk by chunk from one snapshot, and written back by
/// name into another database, reads there as the source did at the snapshot, in every
/// family kind: versions (a capped family too), a column delete hiding an older `put_at`
/// written after it, a TTL family, a counter with a delete and increments after it, and
/// sum, max and HLL aggregates, some of each in SSTs and some in the memtable. A write after
/// the snapshot isn't copied, and every chunk holds whole rows.
#[test]
fn a_range_copied_at_a_snapshot_reads_as_its_source() {
    use pigeonhole::Aggregation;
    let (sv, tv) = (SimVfs::new(52), SimVfs::new(53));
    let source = Pigeonhole::open("/s/data.phdb", options(&sv)).unwrap();
    let target = Pigeonhole::open("/t/data.phdb", options(&tv)).unwrap();
    let make = |db: &Pigeonhole| {
        db.table("t")
            .unwrap()
            .family("a", Family::default())
            .family("v", Family::default().max_versions(2))
            .family("l", Family::default().ttl(Duration::from_secs(3600)))
            .family("n", Family::counter())
            .family("s", Family::aggregate(Aggregation::Sum))
            .family("m", Family::aggregate(Aggregation::Max))
            .family("h", Family::aggregate(Aggregation::Hll))
            .create_if_missing()
            .unwrap()
    };
    let (s, t) = (make(&source), make(&target));
    for i in 0..40u32 {
        let row = format!("r{i:02}");
        let r = row.as_bytes();
        s.mutate(r)
            .put_at("a", b"q", 100 + u64::from(i), b"first")
            .put_at("a", b"q", 200 + u64::from(i), b"second")
            .put_at("v", b"q", 10, b"v10")
            .put_at("v", b"q", 20, b"v20")
            .put_at("v", b"q", 30, b"v30")
            .put("l", b"q", b"lives")
            .incr("n", b"c", 3)
            .aggregate("s", b"q", i64::from(i))
            .aggregate("m", b"q", i64::from(i % 7))
            .aggregate_bytes("h", b"q", r)
            .commit()
            .unwrap();
        if i % 3 == 0 {
            s.mutate(r).delete_column("a", b"q").commit().unwrap();
            s.mutate(r)
                .put_at("a", b"q", 50, b"hidden")
                .commit()
                .unwrap();
            s.mutate(r).delete_column("n", b"c").commit().unwrap();
        }
        s.mutate(r)
            .incr("n", b"c", 4)
            .aggregate("s", b"q", 1)
            .aggregate("m", b"q", 9)
            .aggregate_bytes("h", b"q", b"shared")
            .commit()
            .unwrap();
        if i == 20 {
            source.flush().unwrap();
        }
    }
    let snap = source.snapshot().unwrap();
    s.mutate(b"zz").put("a", b"q", b"after").commit().unwrap();

    let mut after = Vec::new();
    let mut chunks: Vec<Vec<Vec<u8>>> = Vec::new();
    loop {
        let chunk = source.copy_range(&snap, &s, &after, None, 300).unwrap();
        let mut rows: Vec<Vec<u8>> = chunk.cells.iter().map(|c| c.row.clone()).collect();
        rows.dedup();
        let mut wb = target.logged_batch();
        wb.mark_feed_silent();
        for c in &chunk.cells {
            wb.push(&t, &c.row, &c.family, c.kind, &c.qualifier, c.ts, &c.stored);
        }
        if !wb.is_empty() {
            wb.commit().unwrap();
        }
        chunks.push(rows);
        match chunk.next {
            Some(n) => after = n,
            None => break,
        }
    }
    assert!(chunks.len() > 3, "{} chunks", chunks.len());
    let mut seen = std::collections::HashSet::new();
    for rows in &chunks {
        for r in rows {
            assert!(seen.insert(r.clone()), "row {r:?} in two chunks");
        }
    }
    let of_t = |db: &Pigeonhole| -> Vec<String> {
        contents(db)
            .into_iter()
            .filter(|l| l.starts_with("t\t") && !l.contains(&format!("\t{}\t", hex(b"zz"))))
            .collect()
    };
    assert_eq!(of_t(&target), of_t(&source));
    assert!(
        t.get(b"zz", "a", b"q").unwrap().is_none(),
        "a write after the snapshot"
    );
    drop((snap, s, t));
    target.close().unwrap();
    source.close().unwrap();
}

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
        out.push(feed.replicated().expect("an event was delivered"));
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
    let e = replica
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

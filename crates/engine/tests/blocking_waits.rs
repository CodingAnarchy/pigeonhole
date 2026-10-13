//! Issue #135: blocking waits and the application-owned close, with I/O held in flight
//! across scheduling points (a WAL group unresolved, a manifest root commit blocked).

mod gate;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, Error, FamilyOptions, Predicate, TableInfo, ValueRef,
    WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;

const DB: &str = "/db/w.phdb";

fn options(vfs: VfsRef, shards: usize) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = shards;
    o.pin_threads = false;
    o.compaction_threads = 0;
    o.memtable_budget = 4 << 20;
    o.wal.segment_size = 256 << 10;
    o.wal.spare_segments = 1;
    o
}

fn wal(stream: u32) -> PathBuf {
    pigeonhole_wal::stream_path(Path::new(DB), pigeonhole_format::StreamId(stream))
}

fn put(t: &TableInfo, row: &str) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row.as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    wb
}

/// A cross-shard batch: a row of `first` (whose shard coordinates it), then one of `second`.
fn cross(first: &TableInfo, second: &TableInfo, row: &str) -> WriteBatch {
    let mut wb = put(first, row);
    wb.put(
        second.id,
        second.families[0].id,
        row.as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    wb
}

fn table(db: &Engine, name: &str) -> Arc<TableInfo> {
    db.create_table(name, &[("f".into(), FamilyOptions::default())])
        .unwrap()
}

/// The shard owning `t` (one tablet per new table): commit to it and look at the stream
/// the record went to. The shards must be running.
fn shard_of(db: &Engine, t: &TableInfo) -> u32 {
    db.record_history(true);
    db.take_appended();
    db.commit(put(t, "probe"), Some(Durability::Buffered))
        .unwrap();
    u32::from(db.take_appended().last().expect("one record").stream)
}

/// Runs `shard` until it is idle.
fn idle(shard: &mut EngineShard) {
    while shard.run_once(u64::MAX) {}
}

/// Work for an application-owned shard's thread to run between `run_once` calls.
type Cmd = Box<dyn FnOnce(&mut EngineShard) + Send>;

/// An application thread driving one shard (the documented loop: run until idle, sleep
/// until the wakeup), until the database's close has finished. Runs commands sent to it
/// between iterations, so a test can act on the driving thread.
fn driver(mut shard: EngineShard) -> Driver {
    let (tx, rx) = mpsc::channel::<Cmd>();
    let h = thread::spawn(move || {
        let me = thread::current();
        shard.set_wakeup(Box::new(move || me.unpark()));
        loop {
            idle(&mut shard);
            while let Ok(cmd) = rx.try_recv() {
                cmd(&mut shard);
                idle(&mut shard);
            }
            if let Some(outcome) = shard.closed() {
                return outcome;
            }
            // No timeout: the wakeup (or `on_driver`) must unpark us.
            thread::park();
        }
    });
    (tx, h)
}

/// A driver thread: its command queue and handle.
type Driver = (
    mpsc::Sender<Cmd>,
    thread::JoinHandle<pigeonhole_engine::Result<()>>,
);

/// Runs `f` on driver `d` and returns its result (`None` after 10 s: a hang).
fn on_driver<T: Send + 'static>(
    d: &Driver,
    f: impl FnOnce(&mut EngineShard) -> T + Send + 'static,
) -> Option<T> {
    let (rtx, rrx) = mpsc::channel();
    d.0.send(Box::new(move |s: &mut EngineShard| {
        let _ = rtx.send(f(s));
    }))
    .unwrap();
    d.1.thread().unpark();
    rrx.recv_timeout(Duration::from_secs(10)).ok()
}

// ---- 1-2 F2: thread-side manifest commits ----

/// A flush's manifest pump holds the writer's exclusion with its root commit in flight.
/// The root commit completes, but the pump's shard is not run (its driver is the thread
/// now making a catalog change). The catalog change used to poll for ever; it finishes the
/// pump's commit itself and proceeds.
#[test]
fn a_catalog_change_finishes_a_pump_commit_whose_shard_is_not_run() {
    let (vfs, gate) = gate::vfs(1351);
    let (db, mut shards) =
        Engine::open_application_owned(Path::new(DB), options(Arc::clone(&vfs), 1)).unwrap();
    let mut shard = shards.pop().unwrap();
    let t = table(&db, "t");
    for i in 0..50 {
        drop(
            db.submit(put(&t, &format!("r{i:03}")), Some(Durability::None))
                .unwrap(),
        );
    }
    idle(&mut shard);
    // The flush's root commit stays in flight while the shard runs.
    gate.hold(Path::new(DB));
    let flushed = db.flush_pending().unwrap();
    for _ in 0..10_000 {
        if !gate.held().is_empty() {
            break;
        }
        idle(&mut shard);
        thread::sleep(Duration::from_micros(100));
    }
    assert_eq!(
        gate.held(),
        [PathBuf::from(DB)],
        "the flush's root commit never started"
    );
    // The root commit completes; nobody runs the shard.
    gate.release();
    let db2 = Arc::clone(&db);
    let created = gate::within(10, move || {
        db2.create_table("u", &[("f".into(), FamilyOptions::default())])
            .map(|_| ())
    });
    assert!(
        matches!(created, Some(Ok(()))),
        "create_table with a completed pump commit pending: {created:?}"
    );
    assert!(db.table("u").is_some());
    // The flush's own reply arrived with the commit the thread finished.
    let mut flushed = flushed;
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    for _ in 0..1000 {
        if std::pin::Pin::new(&mut flushed).poll(&mut cx).is_ready() {
            break;
        }
        idle(&mut shard);
    }
    db.close().unwrap();
    while shard.closed().is_none() {
        shard.run_once(u64::MAX);
    }
    shard.closed().unwrap().unwrap();
    drop(shard);
    drop(db);
    assert!(Engine::inspect_manifest(&vfs, Path::new(DB)).unwrap().clean);
}

// ---- 1-2 F1 / 3-4 3.3: a blocking wait on a thread that drives a shard ----

/// Thread X drives shard B, which coordinates a cross-shard `Sync` commit whose sync is in
/// flight (its seqno holds the global watermark; a single-shard group no longer does,
/// D227). X commits to a table on shard A (driven by thread Y) and waits: only X can
/// let B publish, so the wait used to spin for ever. Now (amended D88) calls that would
/// submit and block are refused before submitting anything (`InvalidArgument`), and
/// waiting on an already-submitted commit fails with `WouldDeadlock` while the commit
/// still applies.
#[test]
fn a_blocking_wait_on_a_thread_driving_another_shard_fails_instead_of_deadlocking() {
    let (vfs, gate) = gate::vfs(1352);
    let (db, shards) = Engine::open_application_owned(Path::new(DB), options(vfs, 2)).unwrap();
    let drivers: Vec<_> = shards.into_iter().map(driver).collect();
    let (on_a, on_b) = tables_on_0_and_1(&db);
    let x = &drivers[1];
    gate.hold(&wal(1));
    let pending = db
        .submit(cross(&on_b, &on_a, "held"), Some(Durability::Sync))
        .unwrap();
    gate.wait_held(&wal(1));

    let (db2, on_a2) = (Arc::clone(&db), Arc::clone(&on_a));
    let refused = on_driver(x, move |_| {
        db2.commit(put(&on_a2, "refused"), Some(Durability::None))
    })
    .expect("commit on the driving thread hung");
    assert!(
        matches!(refused, Err(Error::InvalidArgument(ref m)) if m.contains("nothing was submitted")),
        "{refused:?}"
    );
    let (db2, on_a2) = (Arc::clone(&db), Arc::clone(&on_a));
    let waited = on_driver(x, move |_| {
        db2.submit(put(&on_a2, "from-x"), Some(Durability::None))
            .and_then(|p| p.wait())
    })
    .expect("the wait on the driving thread hung");
    assert!(matches!(waited, Err(Error::WouldDeadlock)), "{waited:?}");
    let (db2, on_a2) = (Arc::clone(&db), Arc::clone(&on_a));
    let checked = on_driver(x, move |_| {
        db2.check_and_mutate(
            on_a2.id,
            b"cas",
            &Predicate::Absent {
                family: on_a2.families[0].id,
                qualifier: b"q".to_vec(),
            },
            put(&on_a2, "cas"),
            Some(Durability::None),
        )
    })
    .expect("check_and_mutate on the driving thread hung");
    assert!(
        matches!(checked, Err(Error::InvalidArgument(_))),
        "{checked:?}"
    );
    let db2 = Arc::clone(&db);
    let flushed = on_driver(x, move |_| db2.flush()).expect("flush on the driving thread hung");
    assert!(
        matches!(flushed, Err(Error::InvalidArgument(_))),
        "{flushed:?}"
    );

    // Off the driving threads, waits work. The submitted commit applied; the refused ones
    // did not.
    gate.release();
    pending.wait().unwrap();
    // Everything above was submitted before this one: once it is visible, so is that.
    db.commit(put(&on_a, "barrier"), Some(Durability::None))
        .unwrap();
    for (row, landed) in [(&b"from-x"[..], true), (b"refused", false), (b"cas", false)] {
        assert_eq!(
            db.get_latest(on_a.id, on_a.families[0].id, row, b"q")
                .unwrap()
                .is_some(),
            landed,
            "row {:?}",
            String::from_utf8_lossy(row)
        );
    }
    db.close().unwrap();
    for (_, h) in drivers {
        h.join().unwrap().unwrap();
    }
}

/// Creates two tables and returns them ordered by owning shard (0, then 1). The shards
/// must be running.
fn tables_on_0_and_1(db: &Engine) -> (Arc<TableInfo>, Arc<TableInfo>) {
    let a = table(db, "a");
    let b = table(db, "b");
    match (shard_of(db, &a), shard_of(db, &b)) {
        (0, 1) => (a, b),
        (1, 0) => (b, a),
        other => panic!("tables on shards {other:?}"),
    }
}

/// A commit waiting for visibility behind a cross-shard commit shard 1 coordinates (its
/// seqno held until every participant applied, which waits on shard 1's held sync) ends with
/// `OutcomeUnknown` when shard 1 dies (its `EngineShard` is dropped mid-commit), instead of
/// parking for ever, and the close then ends (unclean) instead of waiting on the dead shard. A
/// single-shard group no longer holds visibility while its sync runs (D227).
#[test]
fn a_visibility_wait_ends_when_the_shard_holding_it_dies() {
    let (vfs, gate) = gate::vfs(1358);
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options(vfs, 2)).unwrap();
    let mut b = shards.pop().unwrap();
    let a = driver(shards.pop().unwrap());
    // This thread drives shard 1; helper threads commit.
    let (db2, mut b) = (Arc::clone(&db), {
        let me = thread::current();
        b.set_wakeup(Box::new(move || me.unpark()));
        b
    });
    let tables = thread::spawn(move || tables_on_0_and_1(&db2));
    while !tables.is_finished() {
        idle(&mut b);
        thread::park_timeout(Duration::from_millis(1));
    }
    let (on_a, on_b) = tables.join().unwrap();
    gate.hold(&wal(1));
    drop(
        db.submit(cross(&on_b, &on_a, "held"), Some(Durability::Sync))
            .unwrap(),
    );
    while gate.held().is_empty() {
        idle(&mut b);
        thread::park_timeout(Duration::from_millis(1));
    }
    let (tx, rx) = mpsc::channel();
    let (db2, on_a2) = (Arc::clone(&db), Arc::clone(&on_a));
    thread::spawn(move || {
        let _ = tx.send(db2.commit(put(&on_a2, "waits"), Some(Durability::None)));
    });
    assert!(
        rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "visible while shard 1 held the watermark"
    );
    // Dropping the shard syncs its stream, and that sync waits for every older sync of the
    // stream to finish (#179), the held sync included: release it from another thread once
    // the drop is under way, as the I/O backend would complete it. Shard 1 never runs
    // again, so the cross-shard commit stays held and keeps the watermark down.
    let releaser = {
        let gate = Arc::clone(&gate);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            gate.release();
        })
    };
    drop(b);
    releaser.join().unwrap();
    let ended = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the visibility wait outlived the shard holding it");
    // Acknowledged by its shard but never visible here: the next open decides (ICR 0038).
    assert!(matches!(ended, Err(Error::OutcomeUnknown)), "{ended:?}");
    // The close ends (it no longer waits on the dead shard) and is unclean.
    assert!(db.close().is_err(), "a close with a dead shard reported Ok");
    assert!(
        a.1.join().unwrap().is_err(),
        "a close with a dropped shard is unclean"
    );
}

/// The symmetric case: a *participant* of a cross-shard commit dies after its PREPARE and
/// before the coordinator's decision reaches it. Its `Applied` never comes, so the
/// coordinator's share of the commit never settles; the close must still end (unclean)
/// rather than wait for that reply for ever, and the commit's waiter hears `OutcomeUnknown`
/// (decided to commit, so its COMMIT may be in the WAL).
#[test]
fn a_close_ends_when_a_participant_dies_before_it_applied() {
    let (vfs, gate) = gate::vfs(1359);
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options(vfs, 2)).unwrap();
    let mut b = shards.pop().unwrap();
    let a = driver(shards.pop().unwrap());
    let (db2, mut b) = (Arc::clone(&db), {
        let me = thread::current();
        b.set_wakeup(Box::new(move || me.unpark()));
        b
    });
    let tables = thread::spawn(move || tables_on_0_and_1(&db2));
    while !tables.is_finished() {
        idle(&mut b);
        thread::park_timeout(Duration::from_millis(1));
    }
    let (on_a, on_b) = tables.join().unwrap();
    // Shard 0 coordinates. Its sync is held, so its decision waits while shard 1 (driven
    // here) prepares; shard 1 then stops running and is dropped before the decision.
    gate.hold(&wal(0));
    let pending = db
        .submit(cross(&on_a, &on_b, "dies"), Some(Durability::Sync))
        .unwrap();
    while gate.held().is_empty() {
        idle(&mut b);
        thread::park_timeout(Duration::from_millis(1));
    }
    for _ in 0..20 {
        idle(&mut b);
        thread::park_timeout(Duration::from_millis(1));
    }
    drop(b);
    gate.release();
    let (tx, rx) = mpsc::channel();
    let db2 = Arc::clone(&db);
    thread::spawn(move || {
        let _ = tx.send(db2.close());
    });
    let closed = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the close waited for a dead participant's reply");
    assert!(closed.is_err(), "a close with a dead shard reported Ok");
    assert!(
        a.1.join().unwrap().is_err(),
        "a close with a dropped shard is unclean"
    );
    // Decided to commit (shard 1 prepared before it died), but shard 1 never applied it:
    // the COMMIT may be in the WAL, so the outcome is the next open's (ICR 0038).
    let waited = pending.wait();
    assert!(matches!(waited, Err(Error::OutcomeUnknown)), "{waited:?}");
}

/// ctr274's late decision: a participant's close reports while a live coordinator's
/// decision is still on its way (a third shard died, so the close stops waiting for
/// undecided shares). The decision then arrives at a shard that reported: it must apply
/// nothing (no memtable change and no flush after the report), answer so the coordinator's
/// close ends, and leave the share to the next open, which decides it by its COMMIT.
/// All three shards are driven by hand on this thread. Removing `on_decide`'s guard
/// trips the debug assertion in the shard's apply.
#[test]
fn a_decision_after_a_participants_close_reported_is_not_applied() {
    let (vfs, gate) = gate::vfs(1360);
    let (db, mut shards) =
        Engine::open_application_owned(Path::new(DB), options(Arc::clone(&vfs), 3)).unwrap();
    let mut d = shards.pop().unwrap();
    let mut p = shards.pop().unwrap();
    let mut c = shards.pop().unwrap();
    let db2 = Arc::clone(&db);
    let tables = thread::spawn(move || {
        let mut on = [None, None];
        for i in 0.. {
            let t = table(&db2, &format!("t{i}"));
            match shard_of(&db2, &t) {
                s @ 0..=1 if on[s as usize].is_none() => on[s as usize] = Some(t),
                _ => {}
            }
            if on.iter().all(Option::is_some) {
                break;
            }
        }
        on.map(Option::unwrap)
    });
    while !tables.is_finished() {
        idle(&mut c);
        idle(&mut p);
        idle(&mut d);
    }
    let [on_c, on_p] = tables.join().unwrap();
    // Shard 0 coordinates and sends shard 1 its share; shard 1 prepares and answers.
    // Shard 0 does not run again until shard 1's close has reported.
    let pending = db
        .submit(cross(&on_c, &on_p, "late"), Some(Durability::GroupSync))
        .unwrap();
    idle(&mut c);
    idle(&mut p);
    drop(d);
    let (tx, rx) = mpsc::channel();
    let db2 = Arc::clone(&db);
    thread::spawn(move || {
        let _ = tx.send(db2.close());
    });
    // Shard 1's close runs to its report (a shard died, so the undecided share does not
    // hold it).
    for _ in 0..200 {
        idle(&mut p);
        thread::sleep(Duration::from_millis(1));
    }
    // Now the coordinator decides (commit: shard 1 prepared before the close gave up) and
    // shard 1 hears it after its report. Shard 1 must answer without applying (debug builds
    // assert that no shard applies after its close reported), and the coordinator, kept in
    // its close's flush by the held main-file syncs, must then keep the seqno held: a read
    // never sees shard 0's share without shard 1's.
    gate.hold(Path::new(DB));
    for _ in 0..3 {
        idle(&mut c);
        idle(&mut p);
    }
    let snap = db.snapshot().unwrap();
    let seen = [&on_c, &on_p].map(|t| {
        db.get(&snap, t.id, t.families[0].id, b"late", b"q")
            .unwrap()
            .is_some()
    });
    assert!(
        seen[0] == seen[1],
        "half a cross-shard commit is visible (shard 0, shard 1): {seen:?}"
    );
    drop(snap);
    gate.release();
    // Either way the coordinator's close then ends.
    let started = std::time::Instant::now();
    let closed = loop {
        idle(&mut c);
        idle(&mut p);
        if let Ok(r) = rx.try_recv() {
            break r;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the coordinator's close waited for the reported participant's answer"
        );
        thread::sleep(Duration::from_millis(1));
    };
    assert!(closed.is_err(), "a close with a dead shard reported Ok");
    drop((c, p));
    // Committed in the WAL, never visible here: the next open decides (ICR 0038).
    let waited = pending.wait();
    assert!(matches!(waited, Err(Error::OutcomeUnknown)), "{waited:?}");
    drop(db);
    let (db, shards) =
        Engine::open_application_owned(Path::new(DB), options(Arc::clone(&vfs), 3)).unwrap();
    let a = shards.into_iter().map(driver).collect::<Vec<_>>();
    let (on_c, on_p) = (db.table(&on_c.name).unwrap(), db.table(&on_p.name).unwrap());
    let snap = db.snapshot().unwrap();
    for t in [&on_c, &on_p] {
        assert!(
            db.get(&snap, t.id, t.families[0].id, b"late", b"q")
                .unwrap()
                .is_some(),
            "the decided commit is lost after reopen"
        );
    }
    drop(snap);
    db.close().unwrap();
    for (_, h) in a {
        h.join().unwrap().unwrap();
    }
}

/// The coordinator's side: a participant's PREPARE answer reaches a coordinator whose close
/// already gave up (a third shard died). It decides abort, so a cross-shard commit whose
/// caller hears `Closed` from such a close is never committed, whatever the timing: no
/// COMMIT record is logged after the close stopped waiting for it, and the next open
/// finds neither share.
#[test]
fn a_prepare_answer_after_the_coordinators_close_gave_up_aborts() {
    let (vfs, gate) = gate::vfs(1361);
    let (db, mut shards) =
        Engine::open_application_owned(Path::new(DB), options(Arc::clone(&vfs), 3)).unwrap();
    let mut d = shards.pop().unwrap();
    let mut p = shards.pop().unwrap();
    let mut c = shards.pop().unwrap();
    let db2 = Arc::clone(&db);
    let tables = thread::spawn(move || {
        let mut on = [None, None];
        for i in 0.. {
            let t = table(&db2, &format!("t{i}"));
            match shard_of(&db2, &t) {
                s @ 0..=1 if on[s as usize].is_none() => on[s as usize] = Some(t),
                _ => {}
            }
            if on.iter().all(Option::is_some) {
                break;
            }
        }
        on.map(Option::unwrap)
    });
    while !tables.is_finished() {
        idle(&mut c);
        idle(&mut p);
        idle(&mut d);
    }
    let [on_c, on_p] = tables.join().unwrap();
    // Shard 0 coordinates and sends shard 1 its share, which shard 1 does not see yet.
    let pending = db
        .submit(cross(&on_c, &on_p, "late"), Some(Durability::GroupSync))
        .unwrap();
    idle(&mut c);
    // Shard 2 dies and the close starts. Shard 0's close stays in its flush (the main
    // file's syncs are held) while shard 1 prepares and answers late.
    gate.hold(Path::new(DB));
    drop(d);
    let (tx, rx) = mpsc::channel();
    let db2 = Arc::clone(&db);
    thread::spawn(move || {
        let _ = tx.send(db2.close());
    });
    for _ in 0..200 {
        idle(&mut c);
        thread::sleep(Duration::from_millis(1));
    }
    let started = std::time::Instant::now();
    let closed = loop {
        idle(&mut p);
        idle(&mut c);
        if started.elapsed() > Duration::from_millis(300) {
            gate.release();
        }
        if let Ok(r) = rx.try_recv() {
            break r;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the close never ended"
        );
        thread::sleep(Duration::from_millis(1));
    };
    assert!(closed.is_err(), "a close with a dead shard reported Ok");
    drop((c, p));
    let waited = pending.wait();
    assert!(matches!(waited, Err(Error::Closed)), "{waited:?}");
    drop(db);
    let (db, shards) =
        Engine::open_application_owned(Path::new(DB), options(Arc::clone(&vfs), 3)).unwrap();
    let a = shards.into_iter().map(driver).collect::<Vec<_>>();
    let (on_c, on_p) = (db.table(&on_c.name).unwrap(), db.table(&on_p.name).unwrap());
    let snap = db.snapshot().unwrap();
    for t in [&on_c, &on_p] {
        assert!(
            db.get(&snap, t.id, t.families[0].id, b"late", b"q")
                .unwrap()
                .is_none(),
            "a commit whose caller heard Closed was committed"
        );
    }
    drop(snap);
    db.close().unwrap();
    for (_, h) in a {
        h.join().unwrap().unwrap();
    }
}

/// A participant's apply of a cross-shard share fails (its shard poisons itself, as an arena
/// miscount would). The commit was decided to commit, so its seqno stays held: nothing of
/// it is ever visible half applied, and its caller gets `OutcomeUnknown`. Held for good it
/// would stall every later commit's visibility wait, so visibility ends as on a shard's
/// death: a later commit on a healthy shard returns (`OutcomeUnknown`) instead of hanging,
/// and the close is unclean (the next open decides the commit from the WAL).
#[test]
fn a_failed_share_apply_ends_visibility_instead_of_stalling_it() {
    let (vfs, _gate) = gate::vfs(1362);
    let (db, shards) = Engine::open_application_owned(Path::new(DB), options(vfs, 2)).unwrap();
    let drivers: Vec<_> = shards.into_iter().map(driver).collect();
    let (on_a, on_b) = tables_on_0_and_1(&db);
    db.fail_next_apply();
    let (db2, a2, b2) = (Arc::clone(&db), Arc::clone(&on_a), Arc::clone(&on_b));
    let crossed = gate::within(10, move || {
        db2.commit(cross(&b2, &a2, "fails"), Some(Durability::Sync))
    })
    .expect("the cross-shard commit hung");
    assert!(matches!(crossed, Err(Error::OutcomeUnknown)), "{crossed:?}");
    // Later commits end at once on both shards: the healthy one's with `OutcomeUnknown`
    // (acknowledged, never visible here), the poisoned one's with its failure.
    let mut unknown = 0;
    for t in [&on_a, &on_b] {
        let (db2, t2) = (Arc::clone(&db), Arc::clone(t));
        let later = gate::within(10, move || {
            db2.commit(put(&t2, "later"), Some(Durability::Sync))
        })
        .expect("a later commit hung behind the held seqno");
        assert!(later.is_err(), "{later:?}");
        unknown += usize::from(matches!(later, Err(Error::OutcomeUnknown)));
    }
    assert_eq!(
        unknown, 1,
        "the healthy shard's commit is acknowledged, never visible"
    );
    // Nothing of the cross-shard commit is visible.
    for t in [&on_a, &on_b] {
        assert!(
            db.get_latest(t.id, t.families[0].id, b"fails", b"q")
                .unwrap()
                .is_none(),
            "part of the cross-shard commit is visible"
        );
    }
    let closed = gate::within(10, {
        let db = Arc::clone(&db);
        move || db.close()
    })
    .expect("the close hung on the held seqno");
    assert!(closed.is_err(), "the close after a failed apply is unclean");
    for (_, h) in drivers {
        let _ = h.join().unwrap();
    }
}

// ---- 3-4 3.1: the application-owned close ----

/// The documented loop, with the close's root commits in flight across `run_once` calls:
/// `close()` (on a thread that drives no shard) waits for the shards and returns the
/// result, and the close is clean. It used to return at once, the threads stopped at the
/// first idle `run_once` and dropped their shards, and the close was unclean every time.
#[test]
fn app_owned_close_waits_for_in_flight_close_io_and_is_clean() {
    let (vfs, gate) = gate::vfs(1353);
    let (db, shards) =
        Engine::open_application_owned(Path::new(DB), options(Arc::clone(&vfs), 2)).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    // The rustdoc example's loop before #135: stop at the first idle `run_once` once told.
    let threads: Vec<_> = shards
        .into_iter()
        .map(|mut shard| {
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let me = thread::current();
                shard.set_wakeup(Box::new(move || me.unpark()));
                loop {
                    idle(&mut shard);
                    if stop.load(Ordering::Acquire) {
                        return;
                    }
                    thread::park();
                }
            })
        })
        .collect();
    let t = table(&db, "t");
    for i in 0..500 {
        db.commit(put(&t, &format!("r{i:04}")), Some(Durability::Buffered))
            .unwrap();
    }
    // The close's flush commits stay in flight for a while.
    gate.hold(Path::new(DB));
    let g = Arc::clone(&gate);
    let releaser = thread::spawn(move || {
        g.wait_held(Path::new(DB));
        thread::sleep(Duration::from_millis(100));
        g.release();
    });
    let closed = gate::within(20, {
        let db = Arc::clone(&db);
        move || db.close()
    });
    assert!(matches!(closed, Some(Ok(()))), "{closed:?}");
    stop.store(true, Ordering::Release);
    for t in threads {
        t.thread().unpark();
        t.join().unwrap();
    }
    releaser.join().unwrap();
    drop(db);
    let info = Engine::inspect_manifest(&vfs, Path::new(DB)).unwrap();
    assert!(info.clean, "the close was not clean");
    for s in 0..2 {
        assert!(!vfs.exists(&wal(s)).unwrap(), "{:?} left behind", wal(s));
    }
}

/// A WAL sync failing during the close: `close()` reports it (it used to return `Ok`), and
/// so does every shard's `closed()`.
#[test]
fn app_owned_close_reports_a_failed_final_sync() {
    let (vfs, gate) = gate::vfs(1354);
    let (db, shards) =
        Engine::open_application_owned(Path::new(DB), options(Arc::clone(&vfs), 2)).unwrap();
    let drivers: Vec<_> = shards.into_iter().map(driver).collect();
    let t = table(&db, "t");
    for i in 0..100 {
        db.commit(put(&t, &format!("r{i:04}")), Some(Durability::Buffered))
            .unwrap();
    }
    gate.fail(&wal(0));
    gate.fail(&wal(1));
    let closed = gate::within(20, {
        let db = Arc::clone(&db);
        move || db.close()
    })
    .expect("close hung");
    assert!(closed.is_err(), "a failed close reported Ok");
    for (_, h) in drivers {
        assert!(h.join().unwrap().is_err(), "a shard's closed() reported Ok");
    }
    drop(db);
    assert!(!Engine::inspect_manifest(&vfs, Path::new(DB)).unwrap().clean);
}

/// One thread drives every shard: `close()` cannot wait there; the thread keeps driving
/// until `closed()` reports the outcome, with the close's I/O in flight across its calls.
#[test]
fn app_owned_close_from_the_driving_thread_completes_through_closed() {
    let (vfs, gate) = gate::vfs(1355);
    let (db, mut shards) =
        Engine::open_application_owned(Path::new(DB), options(Arc::clone(&vfs), 2)).unwrap();
    let t = table(&db, "t");
    for i in 0..200 {
        drop(
            db.submit(put(&t, &format!("r{i:04}")), Some(Durability::Buffered))
                .unwrap(),
        );
    }
    for s in &mut shards {
        idle(s);
    }
    gate.hold(Path::new(DB));
    gate.hold(&wal(0));
    gate.hold(&wal(1));
    db.close().unwrap();
    let mut released = false;
    while shards[0].closed().is_none() {
        let mut more = false;
        for s in &mut shards {
            more |= s.run_once(u64::MAX);
        }
        if !more && !released {
            // Idle with the close's syncs held: `run_once` says nothing to do, yet the
            // close has not finished. Complete them (as the I/O backend would).
            assert!(!gate.held().is_empty());
            assert!(shards.iter().all(|s| s.closed().is_none()));
            gate.release();
            released = true;
        }
    }
    for s in &shards {
        s.closed().unwrap().unwrap();
    }
    drop(shards);
    drop(db);
    assert!(Engine::inspect_manifest(&vfs, Path::new(DB)).unwrap().clean);
}

// ---- 3-4 3.4: a panicked shard thread ----

/// An engine-owned shard thread panics: `close` (and the drop of the last handle) report an
/// error instead of hanging for ever.
#[test]
fn a_panicked_shard_thread_does_not_hang_close() {
    let (vfs, gate) = gate::vfs(1356);
    let db = Engine::open(Path::new(DB), options(vfs, 2)).unwrap();
    let t = table(&db, "t");
    let s = shard_of(&db, &t);
    gate.panic(&wal(s));
    let db2 = Arc::clone(&db);
    let t2 = Arc::clone(&t);
    let committed = gate::within(10, move || {
        db2.commit(put(&t2, "boom"), Some(Durability::Sync))
    })
    .expect("a commit to the panicked shard hung");
    assert!(committed.is_err());
    let closed = gate::within(10, move || db.close()).expect("close hung after a shard panic");
    assert!(
        matches!(closed, Err(Error::Io(ref e)) if e.to_string().contains("panicked")),
        "{closed:?}"
    );
}

/// Shard 1 is poisoned while its `Sync` group is unresolved (the group's sync fails). A
/// commit on shard 0 does not wait behind that group at all: the group is applied, so it
/// holds no visibility while its sync runs (D227). The failed sync then resolves the group
/// (its members get the poison error). A cross-shard commit that then reaches the
/// poisoned participant fails without pinning the watermark either.
#[test]
fn a_visibility_wait_behind_a_poisoned_shards_group_ends() {
    let (vfs, gate) = gate::vfs(1359);
    let db = Engine::open(Path::new(DB), options(vfs, 2)).unwrap();
    let (on_a, on_b) = tables_on_0_and_1(&db);
    gate.fail(&wal(1));
    gate.hold(&wal(1));
    let doomed = db
        .submit(put(&on_b, "doomed"), Some(Durability::Sync))
        .unwrap();
    gate.wait_held(&wal(1));
    let (tx, rx) = mpsc::channel();
    let (db2, on_a2) = (Arc::clone(&db), Arc::clone(&on_a));
    thread::spawn(move || {
        let _ = tx.send(db2.commit(put(&on_a2, "waits"), Some(Durability::None)));
    });
    // D227: shard 1's group, applied with its sync held, holds back no other shard's
    // commits: the commit on shard 0 is visible and returns meanwhile.
    rx.recv_timeout(Duration::from_secs(10))
        .expect("a commit on shard 0 waited behind shard 1's unsynced group")
        .unwrap();
    // The held sync completes with its (injected) failure: shard 1 is poisoned.
    gate.release();
    let doomed = gate::within(10, move || doomed.wait()).expect("the poisoned commit hung");
    assert!(matches!(doomed, Err(Error::Io(_))), "{doomed:?}");

    // A cross-shard commit with the poisoned participant fails, and leaves nothing held.
    let mut both = put(&on_a, "both");
    both.put(
        on_b.id,
        on_b.families[0].id,
        b"both",
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    let db2 = Arc::clone(&db);
    let crossed = gate::within(10, move || db2.commit(both, Some(Durability::Sync)))
        .expect("a cross-shard commit with a poisoned participant hung");
    assert!(
        crossed.is_err(),
        "a commit through a poisoned shard succeeded"
    );
    let db2 = Arc::clone(&db);
    let (on_a2, on_a3) = (Arc::clone(&on_a), Arc::clone(&on_a));
    let after = gate::within(10, move || {
        db2.commit(put(&on_a2, "after"), Some(Durability::None))
    })
    .expect("a later commit's visibility wait hung behind the poisoned shard");
    after.unwrap();
    assert!(
        db.get_latest(on_a3.id, on_a3.families[0].id, b"after", b"q")
            .unwrap()
            .is_some()
    );
    let db2 = Arc::clone(&db);
    let closed = gate::within(10, move || db2.close()).expect("close hung");
    assert!(closed.is_err(), "a close with a poisoned shard reported Ok");
}

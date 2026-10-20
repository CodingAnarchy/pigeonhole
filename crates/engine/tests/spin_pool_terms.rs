//! D256 and D257: the balancer's pool term and its commits in flight come from the real
//! counters, not only from the test overrides (ctr274's review of #737): on an engine-owned
//! store with one compaction thread, a flush runs on the pool and the busy fraction the
//! balancer publishes exceeds its turn-only fraction (with the pool counter read as zero
//! the two would be equal), the handled commits leave the shard thread spinning (counted
//! and reported, no term since D257), one blocking client's single-shard commits read at
//! most about one commit in flight, its cross-shard commits count at the coordinator and at
//! the participant (D262: at most about two over the store, the participant's share below
//! the coordinator's), and a commit that fails at its shard counts too (ctr274's review of
//! #741: the counting sites each have a test that fails without them).

#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::time::{Duration, Instant};

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyOptions, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;

#[test]
fn the_spin_and_pool_terms_lift_the_published_busy_above_the_turns() {
    let dir = std::env::temp_dir().join(format!("phdb-spin-pool-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    o.create_if_missing = true;
    o.shards = 1;
    o.compaction_threads = 1;
    o.pin_threads = false;
    o.durability = Durability::Buffered;
    o.balance_interval_nanos = 10_000_000;
    let db = Engine::open(&dir.join("terms.phdb"), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    // Commits keep the thread spinning after each; a flush puts a pool slice on its account.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut i = 0u32;
    let (busy, turn) = loop {
        for _ in 0..200 {
            let mut wb = WriteBatch::new();
            wb.put(
                t.id,
                f,
                format!("r{i:06}").as_bytes(),
                b"q",
                None,
                ValueRef::Bytes(&[7; 256]),
            )
            .unwrap();
            db.commit(wb, None).unwrap();
            i += 1;
        }
        db.flush().unwrap();
        std::thread::sleep(Duration::from_millis(30));
        let stats = &db.shard_stats()[0];
        if let Some((busy, turn)) = db.shard_busy_terms(0)
            && stats.pool_busy_nanos > 0
            && stats.spin_time_nanos > 0
            && busy > turn
        {
            break (busy, turn);
        }
        assert!(
            Instant::now() < deadline,
            "after 10 s: terms {:?}, pool {} spin {} turns {}",
            db.shard_busy_terms(0),
            stats.pool_busy_nanos,
            stats.spin_time_nanos,
            stats.busy_nanos
        );
    };
    assert!(busy > turn, "busy {busy} turn {turn}");
    // D257: one blocking client is at most one commit in flight, and the counter moved.
    let stats = &db.shard_stats()[0];
    assert!(
        stats.commit_inflight_nanos > 0,
        "no commit in-flight time counted"
    );
    let inflight = db.shard_inflight(0).expect("published");
    assert!(
        inflight > 0.0 && inflight <= 1.05,
        "one blocking client read {inflight} commits in flight"
    );
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

fn open(name: &str, shards: usize) -> (std::path::PathBuf, std::sync::Arc<Engine>) {
    let dir = std::env::temp_dir().join(format!("phdb-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    o.create_if_missing = true;
    o.shards = shards;
    o.compaction_threads = 1;
    o.pin_threads = false;
    o.durability = Durability::Buffered;
    o.balance_interval_nanos = 10_000_000;
    // The two shards stay two: packing one client's load onto one shard would turn the
    // cross-shard commits below into single ones.
    o.balance_pack = false;
    let db = Engine::open(&dir.join(format!("{name}.phdb")), o).unwrap();
    (dir, db)
}

fn put(t: &TableInfo, row: &[u8]) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row,
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    wb
}

/// D257: a cross-shard commit counts once, at its coordinator, so one blocking client whose
/// every commit spans both shards still reads at most about one commit in flight over the
/// store, and more than none (without the coordinator's count it would read none: the
/// participants' PREPAREs and COMMIT records count nothing).
#[test]
fn one_clients_cross_shard_commits_count_at_the_coordinator_and_the_participant() {
    let (dir, db) = open("inflight-cross", 2);
    let tables: Vec<std::sync::Arc<TableInfo>> = (0..8)
        .map(|i| {
            db.create_table(&format!("t{i}"), &[("f".into(), FamilyOptions::default())])
                .unwrap()
        })
        .collect();
    // A row on each shard (tables spread over the shards; the map may still change).
    let pair = |n: u32| {
        let mut on = [None, None];
        for t in &tables {
            for i in 0..16u32 {
                let row = format!("r{n:04}-{i:02}").into_bytes();
                let s = db.shard_of(t.id, &row).unwrap();
                if on[s].is_none() {
                    on[s] = Some((t.id, t.families[0].id, row));
                }
            }
        }
        on
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut n = 0u32;
    let store_inflight = loop {
        let [Some(a), Some(b)] = pair(n) else {
            panic!("no row pair on two shards");
        };
        for _ in 0..100 {
            let mut wb = WriteBatch::new();
            wb.put(a.0, a.1, &a.2, b"q", None, ValueRef::Bytes(b"1"))
                .unwrap();
            wb.put(b.0, b.1, &b.2, b"q", None, ValueRef::Bytes(b"2"))
                .unwrap();
            db.commit(wb, None).unwrap();
        }
        n += 1;
        std::thread::sleep(Duration::from_millis(20));
        let published: Vec<f64> = (0..2).filter_map(|s| db.shard_inflight(s)).collect();
        if published.len() == 2 && published.iter().sum::<f64>() > 0.0 {
            break published.iter().sum::<f64>();
        }
        assert!(
            Instant::now() < deadline,
            "after 10 s: published {published:?}, counters {:?}",
            db.shard_stats()
                .iter()
                .map(|s| s.commit_inflight_nanos)
                .collect::<Vec<_>>()
        );
    };
    // Shard 0 owns each pair's first row, so it coordinates every commit and shard 1
    // prepares every share (D262): the participant counts the round from the client's
    // submit to the decision, which the coordinator's reply follows, so its count is above
    // zero and below the coordinator's.
    let counted: Vec<u64> = db
        .shard_stats()
        .iter()
        .map(|s| s.commit_inflight_nanos)
        .collect();
    assert!(
        counted[0] > 0,
        "the coordinator counted no cross-shard commit's in-flight time: {counted:?}"
    );
    assert!(
        counted[1] > 0,
        "the participant counted no cross-shard round's in-flight time: {counted:?}"
    );
    assert!(
        counted[1] < counted[0],
        "the participant's rounds end at the decision, before the coordinator's reply: {counted:?}"
    );
    // The coordinator prepares its own share too and must not count it at the decision on
    // top of its reply: it adds one span per round (its reply), as the participant does (its
    // decision); counting its own share too would double its spans. Counts, not a ratio of
    // the nanos: the reply hop's share of a round varies by platform (1.1× the decision span
    // on the Linux runners, 1.7× on the macOS one).
    let counts: Vec<u64> = (0..2).map(|s| db.inflight_counts(s)).collect();
    assert_eq!(
        counts[0], counts[1],
        "the coordinator added {} spans and the participant {}: one per round each, or the \
         coordinator counted its own share at the decision on top of its reply",
        counts[0], counts[1]
    );
    assert!(
        store_inflight <= 2.1,
        "one blocking client read {store_inflight} commit rounds in flight over two shards"
    );
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// D257: a commit that fails at its shard (here a transaction whose read was overwritten:
/// `Conflict` from validation) counts its time in flight like a committed one, so a client
/// whose commits keep failing still reads as present. The counter is read after the one
/// committed write that the transactions conflict with, so the rise is the failures' alone.
#[test]
fn failed_commits_count_their_time_in_flight() {
    let (dir, db) = open("inflight-failed", 1);
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    let mut txns = Vec::new();
    for _ in 0..100 {
        let mut txn = db.begin().unwrap();
        let _ = txn.get(t.id, f, b"x", b"q").unwrap();
        txn.batch()
            .put(t.id, f, b"x", b"q", None, ValueRef::Bytes(b"txn"))
            .unwrap();
        txns.push(txn);
    }
    db.commit(put(&t, b"x"), None).unwrap();
    let before = db.shard_stats()[0].commit_inflight_nanos;
    for txn in txns {
        let r = txn.commit(None);
        assert!(matches!(r, Err(Error::Conflict)), "{r:?}");
    }
    let rise = db.shard_stats()[0].commit_inflight_nanos - before;
    assert!(rise > 0, "100 failed commits counted no time in flight");
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

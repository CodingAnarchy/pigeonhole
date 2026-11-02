//! D311 T2 (#1336): forgetting. Retention and TTL decisions, protection, bounded resumable
//! passes, the races each delete's re-evaluation (M2) guards against, the persisted policy,
//! and the Forgetter on the crate's thread and the application's.

use std::time::{Duration, Instant};

use pigeonhole::{ErrorCode, Options, Pigeonhole};
use pigeonhole_memory::{
    Error, ForgetPolicy, Forgot, Kind, LinkType, Memory, MemoryId, OpenOptions, Remembered, Store,
    StoreOptions,
};

const DAY_US: u64 = 24 * 3600 * 1_000_000;

fn open(dir: &pigeonhole::doc_support::TempDir) -> Pigeonhole {
    Pigeonhole::open(dir.join("mem.phdb"), Options::default().shards(2)).expect("open")
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

fn check(store: &Store, ctx: &str) {
    let st = store.verify().unwrap();
    assert!(st.is_consistent(), "{ctx}: {st:?}");
}

fn ids(store: &Store) -> Vec<MemoryId> {
    let st = store.verify().unwrap();
    let _ = st;
    store
        .recall(&["a"])
        .text("note")
        .k(1000)
        .candidates(1000)
        .include_superseded(true)
        .run()
        .unwrap()
        .into_iter()
        .map(|h| h.id)
        .collect()
}

#[test]
fn retention_and_ttl_decide_and_protection_holds() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let policy = ForgetPolicy::new()
        .min_retention(0.2)
        .max_age(Kind::Procedural, Duration::from_secs(3600));
    let store = Store::create(&db, "m", StoreOptions::new().forget(policy.clone())).unwrap();
    let ns = "a";
    let low = store
        .remember(ns, Memory::new("note low").importance(0.1))
        .unwrap();
    let high = store
        .remember(ns, Memory::new("note high").importance(0.9))
        .unwrap();
    let pinned = store
        .remember(ns, Memory::new("note pinned").importance(0.0).pinned(true))
        .unwrap();
    let source = store
        .remember(ns, Memory::new("note source").importance(0.0))
        .unwrap();
    let summary = store
        .remember(
            ns,
            Memory::new("note summary")
                .kind(Kind::Semantic)
                .importance(0.9)
                .link(source.id().clone(), LinkType::DERIVED_FROM, 1.0),
        )
        .unwrap();
    let proc_ = store
        .remember(
            ns,
            Memory::new("note howto")
                .kind(Kind::Procedural)
                .importance(1.0),
        )
        .unwrap();
    // Now: retention is importance (plus the use term), so `low` (0.1) is due and `high`
    // (0.9) isn't; `pinned` and `source` are due but protected; `proc_` is young.
    let pass = store.forget().now_us(now()).run().unwrap();
    assert_eq!((pass.deleted, pass.protected), (1, 2), "{pass:?}");
    let left = ids(&store);
    assert!(!left.contains(low.id()));
    for id in [
        high.id(),
        pinned.id(),
        source.id(),
        summary.id(),
        proc_.id(),
    ] {
        assert!(left.contains(id), "{id:?}");
    }
    // Two days on, at half-life 7 days, `high` decays to 0.9 · 2^(−2/7) ≈ 0.74: kept. The
    // procedural memory is past its hour: forgotten whatever its retention.
    let pass = store.forget().now_us(now() + 2 * DAY_US).run().unwrap();
    assert_eq!(pass.deleted, 1, "{pass:?}");
    assert!(!ids(&store).contains(proc_.id()));
    // A month on, `high` (and `summary`) decay below 0.2; `source` loses its protection once
    // `summary`, the semantic memory derived from it, is gone.
    let pass = store.forget().now_us(now() + 30 * DAY_US).run().unwrap();
    assert!(pass.deleted >= 2, "{pass:?}");
    let pass = store.forget().now_us(now() + 30 * DAY_US).run().unwrap();
    let left = ids(&store);
    assert!(!left.contains(source.id()), "{pass:?}");
    assert_eq!(left, vec![pinned.id().clone()]);
    check(&store, "after forgetting");
    db.close().unwrap();
}

#[test]
fn uses_raise_retention_and_passes_are_bounded_and_resumable() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "m", StoreOptions::new()).unwrap();
    let ns = "a";
    let mut all = Vec::new();
    for i in 0..10 {
        all.push(
            store
                .remember(ns, Memory::new(format!("note {i}")).importance(0.0))
                .unwrap(),
        );
    }
    // A used memory: ln(1 + 3) · 0.1 ≈ 0.14 > 0.1, so it stays.
    for _ in 0..3 {
        store.mark_used(&[all[0].id().clone()]).unwrap();
    }
    let policy = ForgetPolicy::new().min_retention(0.1).max_per_pass(4);
    let t = now();
    let first = store.forget().policy(&policy).now_us(t).run().unwrap();
    assert_eq!(first.deleted, 4);
    let resume = first.resume.clone().expect("more to look at");
    let second = store
        .forget()
        .policy(&policy)
        .after(&resume)
        .now_us(t)
        .run()
        .unwrap();
    assert_eq!(second.deleted, 4);
    let third = store
        .forget()
        .policy(&policy)
        .after(second.resume.as_ref().unwrap())
        .now_us(t)
        .run()
        .unwrap();
    assert_eq!((third.deleted, third.resume.clone()), (1, None));
    assert_eq!(ids(&store), vec![all[0].id().clone()]);
    check(&store, "bounded passes");
    db.close().unwrap();
}

/// A memory due under `policy`, a transaction that re-evaluated it (`forget_in`), then
/// `race` committed in between: the forget's commit conflicts, and a retry keeps the memory.
fn race(name: &str, race: impl FnOnce(&Store, &MemoryId)) {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let policy = ForgetPolicy::new().min_retention(0.15);
    let store = Store::create(&db, "m", StoreOptions::new().forget(policy)).unwrap();
    let ns = "a";
    let m = store
        .remember(ns, Memory::new("note raced").importance(0.1))
        .unwrap();
    let t = now();
    let mut txn = db.transaction().unwrap();
    assert_eq!(
        store.forget_in(&mut txn, m.id(), t).unwrap(),
        Forgot::Deleted
    );
    race(&store, m.id());
    assert_eq!(
        txn.commit().unwrap_err().code(),
        ErrorCode::Conflict,
        "{name}"
    );
    // Re-evaluated now, the memory isn't due (or is protected).
    let pass = store.forget().now_us(t).run().unwrap();
    assert_eq!(pass.deleted, 0, "{name}: {pass:?}");
    assert!(ids(&store).contains(m.id()), "{name}");
    check(&store, name);
    db.close().unwrap();
}

#[test]
fn forget_racing_mark_used_conflicts() {
    // Two uses: 0.1 + 0.1 · ln 3 ≈ 0.21, above 0.15.
    race("mark_used", |s, id| {
        s.mark_used(std::slice::from_ref(id)).unwrap();
        s.mark_used(std::slice::from_ref(id)).unwrap();
    });
}

#[test]
fn forget_racing_a_merge_that_raises_importance_conflicts() {
    race("merge", |s, id| {
        let r = s
            .remember("a", Memory::new("note raced").importance(0.9))
            .unwrap();
        assert_eq!(r, Remembered::Merged(id.clone()));
    });
}

#[test]
fn forget_racing_a_new_derived_from_link_conflicts() {
    race("derived_from", |s, id| {
        s.remember(
            "a",
            Memory::new("note summary")
                .kind(Kind::Semantic)
                .importance(1.0)
                .link(id.clone(), LinkType::DERIVED_FROM, 1.0),
        )
        .unwrap();
    });
}

#[test]
fn the_policy_persists_changes_and_is_required() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "m", StoreOptions::new()).unwrap();
    assert_eq!(store.forget_policy().unwrap(), None);
    // No policy and no override: refused.
    assert!(matches!(
        store.forget().run(),
        Err(Error::InvalidArgument(_))
    ));
    let mut txn = db.transaction().unwrap();
    let m = store.remember("a", Memory::new("note")).unwrap();
    assert!(matches!(
        store.forget_in(&mut txn, m.id(), now()),
        Err(Error::InvalidArgument(_))
    ));
    drop(txn);
    let p = ForgetPolicy::new()
        .min_retention(0.3)
        .max_age(Kind::Episodic, Duration::from_secs(60));
    store.set_forget_policy(Some(p.clone())).unwrap();
    assert_eq!(store.forget_policy().unwrap(), Some(p.clone()));
    drop(store);
    let again = Store::open_with(
        &db,
        "m",
        OpenOptions::new().without_embeddings().own_forgetter(),
    )
    .unwrap();
    assert_eq!(again.forget_policy().unwrap(), Some(p));
    again.set_forget_policy(None).unwrap();
    assert_eq!(again.forget_policy().unwrap(), None);
    // A bad policy is refused.
    assert!(
        again
            .set_forget_policy(Some(ForgetPolicy::new().max_per_pass(0)))
            .is_err()
    );
    // The episodic TTL shorthand.
    let ttl = Store::create(
        &db,
        "ttl",
        StoreOptions::new().episodic_ttl(Duration::from_secs(10)),
    )
    .unwrap();
    assert_eq!(
        ttl.forget_policy().unwrap(),
        Some(ForgetPolicy::new().max_age(Kind::Episodic, Duration::from_secs(10)))
    );
    // OpenOptions must name a model or no model.
    assert!(matches!(
        Store::open_with(&db, "m", OpenOptions::new()),
        Err(Error::InvalidArgument(_))
    ));
    db.close().unwrap();
}

#[test]
fn an_application_runs_its_own_forgetter() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let policy = ForgetPolicy::new().min_retention(0.5).max_per_pass(2);
    let store = Store::create_with(
        &db,
        "m",
        StoreOptions::new().forget(policy),
        OpenOptions::new().without_embeddings().own_forgetter(),
    )
    .unwrap();
    for i in 0..5 {
        store
            .remember("a", Memory::new(format!("note {i}")).importance(0.1))
            .unwrap();
    }
    let mut f = store.take_forgetter().expect("own_forgetter hands it out");
    assert!(store.take_forgetter().is_none());
    // Two per pass: 5 memories take three passes; run_once keeps going within its budget.
    let mut calls = 0;
    while f.run_once(Duration::from_secs(5)).unwrap() {
        calls += 1;
    }
    assert!(calls <= 3);
    assert!(ids(&store).is_empty());
    assert!(!f.closed());
    let clone = store.clone();
    drop(store);
    assert!(!f.closed(), "a clone still holds the store");
    drop(clone);
    assert!(f.closed());
    db.close().unwrap();
}

#[test]
fn the_crates_forgetter_thread_runs_the_policy() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "m", StoreOptions::new()).unwrap();
    for i in 0..3 {
        store
            .remember("a", Memory::new(format!("note {i}")).importance(0.0))
            .unwrap();
    }
    // Setting a policy starts the thread and wakes it.
    store
        .set_forget_policy(Some(
            ForgetPolicy::new()
                .min_retention(0.5)
                .interval(Duration::from_millis(50)),
        ))
        .unwrap();
    let start = Instant::now();
    while !ids(&store).is_empty() {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "the forgetter didn't run"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    check(&store, "the thread forgot");
    drop(store);
    db.close().unwrap();
}

/// simp724's B1 on #1380: a failed pass backs off (5 s, doubling up to the interval) instead
/// of retrying at once, for the crate's thread and an application's loop alike.
#[test]
fn a_failed_pass_backs_off() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create_with(
        &db,
        "m",
        StoreOptions::new().forget(ForgetPolicy::new().min_retention(0.5)),
        OpenOptions::new().without_embeddings().own_forgetter(),
    )
    .unwrap();
    store
        .remember("a", Memory::new("note").importance(0.1))
        .unwrap();
    let mut f = store.take_forgetter().unwrap();
    db.close().unwrap();
    assert!(f.run_once(Duration::from_secs(1)).is_err());
    // Within the backoff no pass runs (one would fail again on the closed database), and
    // wait sleeps out its timeout instead of returning after a millisecond.
    let start = Instant::now();
    let mut passes = 0;
    while start.elapsed() < Duration::from_millis(100) {
        assert!(
            !f.run_once(Duration::from_secs(1)).unwrap(),
            "no pass is due"
        );
        passes += 1;
        f.wait(Duration::from_millis(25));
    }
    assert!(passes <= 5, "{passes} calls in 100 ms");
    assert!(!f.closed());
    drop(store);
    assert!(f.closed());
}

/// simp724's Q1 on #1380: closing the database doesn't wait for a forget pass. The deletion
/// it had open fails with `Closed` and is dropped whole, so the memory and its index entries
/// are all still there after a reopen.
#[test]
fn a_close_during_a_pass_drops_its_deletion_whole() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(
        &db,
        "m",
        StoreOptions::new().forget(ForgetPolicy::new().min_retention(0.5)),
    )
    .unwrap();
    let m = store
        .remember("a", Memory::new("note doomed").importance(0.1))
        .unwrap();
    let mut txn = db.transaction().unwrap();
    assert_eq!(
        store.forget_in(&mut txn, m.id(), now()).unwrap(),
        Forgot::Deleted
    );
    drop(store);
    db.close().unwrap();
    assert_eq!(txn.commit().unwrap_err().code(), ErrorCode::Closed);
    let db = open(&dir);
    let store = Store::open_with(
        &db,
        "m",
        OpenOptions::new().without_embeddings().own_forgetter(),
    )
    .unwrap();
    assert_eq!(ids(&store), vec![m.id().clone()]);
    check(&store, "after a close mid-pass");
    drop(store);
    db.close().unwrap();
}

/// cli692's S1 on #1380: a policy read back can be inspected.
#[test]
fn a_policy_reads_back_through_its_getters() {
    let p = ForgetPolicy::new()
        .min_retention(0.3)
        .max_age(Kind::Episodic, Duration::from_secs(60))
        .half_life(Duration::from_secs(10))
        .use_weight(0.2)
        .max_per_pass(7)
        .interval(Duration::from_secs(5));
    assert_eq!(p.get_min_retention(), Some(0.3));
    assert_eq!(p.get_max_age(Kind::Episodic), Some(Duration::from_secs(60)));
    assert_eq!(p.get_max_age(Kind::Semantic), None);
    assert_eq!(p.get_half_life(), Duration::from_secs(10));
    assert_eq!(p.get_use_weight(), 0.2);
    assert_eq!(p.get_max_per_pass(), 7);
    assert_eq!(p.get_interval(), Duration::from_secs(5));
    // episodic_ttl after forget adds to the policy; forget after episodic_ttl replaces it.
    let after = StoreOptions::new()
        .forget(ForgetPolicy::new().min_retention(0.3))
        .episodic_ttl(Duration::from_secs(9));
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let s = Store::create(&db, "after", after).unwrap();
    let got = s.forget_policy().unwrap().unwrap();
    assert_eq!(got.get_min_retention(), Some(0.3));
    assert_eq!(
        got.get_max_age(Kind::Episodic),
        Some(Duration::from_secs(9))
    );
    let before = StoreOptions::new()
        .episodic_ttl(Duration::from_secs(9))
        .forget(ForgetPolicy::new().min_retention(0.3));
    let s2 = Store::create(&db, "before", before).unwrap();
    assert_eq!(
        s2.forget_policy()
            .unwrap()
            .unwrap()
            .get_max_age(Kind::Episodic),
        None
    );
    drop((s, s2));
    db.close().unwrap();
}

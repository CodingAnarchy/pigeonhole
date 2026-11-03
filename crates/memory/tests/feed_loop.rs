//! D311 T9 part 2 (#1343): the feed-driven consolidation loop (`Store::consolidate_loop`).
//! - A namespace's pass runs once its oldest new memory is `min_age` old, on the loop's
//!   clock, and not before.
//! - Its own summaries and `mark_used` don't re-arm it (cli692's A1, simp724's R3).
//! - A loop dropped while a namespace waits resumes from its saved position and runs the
//!   pass.
//! - A second loop of the same name takes over, and the first fails at its next save (a
//!   restarted process must take over from one that died).
//! - A consolidator failing partway through leaves the committed groups committed, and the
//!   retry folds the rest, with no group folded twice (simp724's R2).
//! - The first start catches up on memories made before it, only where some are
//!   unconsolidated (R4).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use pigeonhole::{FeedRetention, Options, Pigeonhole};
use pigeonhole_memory::{
    ConsolidatePolicy, Consolidator, Error, Memory, MemoryId, Store, StoreOptions, StoredMemory,
};

const SEC: u64 = 1_000_000;

fn open(dir: &pigeonhole::doc_support::TempDir) -> Pigeonhole {
    Pigeonhole::open(
        dir.join("mem.phdb"),
        Options::default()
            .shards(2)
            .change_feed(FeedRetention::bytes(64 << 20)),
    )
    .unwrap()
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

/// Folds a group into one summary; counts its calls, and fails the `fail_at`-th one.
struct Concat {
    calls: AtomicUsize,
    fail_at: usize,
}

impl Concat {
    fn new() -> Self {
        Concat {
            calls: AtomicUsize::new(0),
            fail_at: usize::MAX,
        }
    }
}

impl Consolidator for Concat {
    fn consolidate(
        &self,
        group: &[StoredMemory],
    ) -> Result<Vec<Memory>, Box<dyn std::error::Error + Send + Sync>> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.fail_at {
            return Err("the model timed out".into());
        }
        let text: Vec<&str> = group.iter().map(|m| m.text.as_str()).collect();
        Ok(vec![Memory::new(format!("summary: {}", text.join(" / ")))])
    }
}

/// Every pair in one session groups (similarity 0), with `min_age`.
fn policy(min_age: Duration) -> ConsolidatePolicy {
    ConsolidatePolicy::new()
        .min_age(min_age)
        .group_similarity(0.0)
}

/// Summaries in `ns`: memories whose text starts with "summary:".
fn summaries(store: &Store, ns: &str) -> usize {
    store
        .recall(&[ns])
        .text("summary")
        .k(1000)
        .run()
        .unwrap()
        .iter()
        .filter(|h| h.text.starts_with("summary:"))
        .count()
}

fn pair(store: &Store, ns: &str, session: &str) -> (MemoryId, MemoryId) {
    let a = store
        .remember(
            ns,
            Memory::new(format!("{session} first note")).session(session),
        )
        .unwrap();
    let b = store
        .remember(
            ns,
            Memory::new(format!("{session} second note")).session(session),
        )
        .unwrap();
    (a.id().clone(), b.id().clone())
}

#[test]
fn a_namespace_is_consolidated_once_its_memories_are_old_enough() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let c = Concat::new();
    let clock = Arc::new(AtomicU64::new(now()));
    let t = Arc::clone(&clock);
    let p = policy(Duration::from_secs(10));
    let mut l = store
        .consolidate_loop("main", &c)
        .policy(&p)
        .clock(move || t.load(Ordering::SeqCst));
    // The first step catches up on an empty store and starts watching.
    let step = l.run_once(Duration::ZERO).unwrap();
    assert!(step.passes.is_empty() && step.next_due.is_none());
    let (a, _) = pair(&store, "n", "s1");
    let step = l.run_once(Duration::from_secs(2)).unwrap();
    assert_eq!(step.events, 2);
    assert!(step.passes.is_empty(), "too young");
    assert_eq!(step.next_due, Some(a.created_us() + 10 * SEC));
    // Five seconds on: still too young.
    clock.fetch_add(5 * SEC, Ordering::SeqCst);
    assert!(l.run_once(Duration::ZERO).unwrap().passes.is_empty());
    // Eleven: the pass runs.
    clock.fetch_add(6 * SEC, Ordering::SeqCst);
    let step = l.run_once(Duration::ZERO).unwrap();
    assert_eq!(step.passes.len(), 1, "{step:?}");
    assert_eq!(step.passes[0].1.committed, 1);
    assert_eq!(step.next_due, None);
    assert_eq!(summaries(&store, "n"), 1);
    // The pass's own summary, and a recall's mark_used, don't re-arm the namespace.
    let hit = store.recall(&["n"]).text("note").run().unwrap()[0]
        .id
        .clone();
    store.mark_used(&[hit]).unwrap();
    clock.fetch_add(60 * SEC, Ordering::SeqCst);
    let step = l.run_once(Duration::from_millis(500)).unwrap();
    assert!(
        step.events >= 2,
        "the summary's commit and the mark: {step:?}"
    );
    assert!(step.passes.is_empty(), "{step:?}");
    assert_eq!(c.calls.load(Ordering::SeqCst), 1);
    db.close().unwrap();
}

#[test]
fn a_loop_resumes_its_pending_namespaces_after_a_restart() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let c = Concat::new();
    let p = policy(Duration::from_secs(10));
    let start = now();
    {
        let mut l = store
            .consolidate_loop("main", &c)
            .policy(&p)
            .clock(move || start);
        l.run_once(Duration::ZERO).unwrap();
        pair(&store, "n", "s1");
        let step = l.run_once(Duration::from_secs(2)).unwrap();
        assert!(step.passes.is_empty() && step.next_due.is_some());
    }
    // Restarted later: the saved position still has `n` pending, so its pass runs, though
    // the memories' events were read before the restart.
    let mut l = store
        .consolidate_loop("main", &c)
        .policy(&p)
        .clock(move || start + 20 * SEC);
    let step = l.run_once(Duration::ZERO).unwrap();
    assert_eq!(step.passes.len(), 1, "{step:?}");
    assert_eq!(summaries(&store, "n"), 1);
    db.close().unwrap();
}

#[test]
fn a_second_loop_of_the_same_name_takes_over_and_the_first_fails() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let c = Concat::new();
    let mut one = store.consolidate_loop("main", &c);
    let mut two = store.consolidate_loop("main", &c);
    one.run_once(Duration::ZERO).unwrap();
    // `two` starts from `one`'s saved position and takes the name over...
    two.run_once(Duration::ZERO).unwrap();
    // ...so `one`'s next save finds a newer one, and fails.
    let r = one.run_once(Duration::ZERO);
    assert!(
        matches!(&r, Err(Error::InvalidArgument(m)) if m.contains("another consolidation loop")),
        "{r:?}"
    );
    two.run_once(Duration::ZERO).unwrap();
    // A differently named loop is independent.
    store
        .consolidate_loop("other", &c)
        .run_once(Duration::ZERO)
        .unwrap();
    for bad in ["", "a:b"] {
        let r = store.consolidate_loop(bad, &c).run_once(Duration::ZERO);
        assert!(
            matches!(r, Err(Error::InvalidArgument(_))),
            "{bad:?}: {r:?}"
        );
    }
    db.close().unwrap();
}

#[test]
fn a_failure_partway_leaves_committed_groups_and_the_retry_folds_the_rest() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let mut c = Concat::new();
    c.fail_at = 2;
    let clock = Arc::new(AtomicU64::new(now()));
    let t = Arc::clone(&clock);
    let p = policy(Duration::from_secs(1));
    let mut l = store
        .consolidate_loop("main", &c)
        .policy(&p)
        .clock(move || t.load(Ordering::SeqCst));
    l.run_once(Duration::ZERO).unwrap();
    for s in ["s1", "s2", "s3"] {
        pair(&store, "n", s);
    }
    l.run_once(Duration::from_secs(2)).unwrap();
    clock.fetch_add(2 * SEC, Ordering::SeqCst);
    // Three groups; the second call fails: the first group's summary, marks and source
    // links committed together, the rest didn't.
    let step = l.run_once(Duration::ZERO).unwrap();
    assert_eq!(step.failed.len(), 1, "{step:?}");
    assert!(matches!(&step.failed[0].1, Error::Consolidate { pass, .. } if pass.committed == 1));
    assert_eq!(summaries(&store, "n"), 1);
    assert!(store.verify().unwrap().is_consistent());
    // Retried after min_age, not at once.
    assert!(l.run_once(Duration::ZERO).unwrap().passes.is_empty());
    clock.fetch_add(2 * SEC, Ordering::SeqCst);
    let step = l.run_once(Duration::ZERO).unwrap();
    assert_eq!(step.passes.len(), 1, "{step:?}");
    assert_eq!(
        step.passes[0].1.committed, 2,
        "the two left, not the first again"
    );
    assert_eq!(summaries(&store, "n"), 3);
    assert!(store.verify().unwrap().is_consistent());
    db.close().unwrap();
}

#[test]
fn the_first_start_catches_up_where_memories_are_unconsolidated() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let c = Concat::new();
    // Before any loop: `done` is consolidated by hand, `todo` isn't.
    pair(&store, "done", "s1");
    pair(&store, "todo", "s1");
    let p = policy(Duration::ZERO);
    let pass = store.consolidate("done", &c).policy(&p).run().unwrap();
    assert_eq!(pass.committed, 1);
    let mut l = store.consolidate_loop("main", &c).policy(&p);
    let step = l.run_once(Duration::ZERO).unwrap();
    let ns: Vec<&str> = step.passes.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(ns, vec!["todo"], "{step:?}");
    assert_eq!(summaries(&store, "todo"), 1);
    assert_eq!(summaries(&store, "done"), 1);
    db.close().unwrap();
}

//! D311 T9 (#1343): memory events from the change feed (`Store::watch`, `ReadStore::watch`).
//! - Each write gives its events: remember, an exact-dedup merge, `mark_used`, `link`,
//!   `supersede`, `delete` and a consolidation pass (a summary is `Summarized`). The store's
//!   bookkeeping rows give none.
//! - A namespace filter works, for one namespace and several.
//! - A token resumes where a watch stopped, and a reader process watches the writer.
//! - Bad namespaces and a database without a change feed are refused at `open`.

use std::time::Duration;

use pigeonhole::{FeedRetention, Options, Pigeonhole, ReaderOptions};
use pigeonhole_memory::{
    ConsolidatePolicy, Consolidator, Error, LinkType, Memory, MemoryCommit, MemoryEvent, MemoryId,
    Store, StoreOptions, StoredMemory, Watch, WatchToken,
};

fn open(dir: &pigeonhole::doc_support::TempDir) -> Pigeonhole {
    Pigeonhole::open(
        dir.join("mem.phdb"),
        Options::default()
            .shards(2)
            .change_feed(FeedRetention::bytes(64 << 20)),
    )
    .unwrap()
}

/// The next commit's events, waiting up to 5 s for it.
fn next(w: &mut Watch) -> Vec<MemoryEvent> {
    let c: MemoryCommit = w
        .next(Duration::from_secs(5))
        .unwrap()
        .expect("a commit within 5 s");
    assert!(!c.events.is_empty());
    c.events
}

/// No commit with events is waiting.
fn quiet(w: &mut Watch) {
    assert_eq!(w.next(Duration::from_millis(200)).unwrap(), None);
}

fn sorted(mut e: Vec<MemoryEvent>) -> Vec<MemoryEvent> {
    e.sort_by(|a, b| a.id().cmp(b.id()));
    e
}

struct Concat;

impl Consolidator for Concat {
    fn consolidate(
        &self,
        group: &[StoredMemory],
    ) -> Result<Vec<Memory>, Box<dyn std::error::Error + Send + Sync>> {
        let text: Vec<&str> = group.iter().map(|m| m.text.as_str()).collect();
        Ok(vec![Memory::new(format!("summary: {}", text.join(" / ")))])
    }
}

#[test]
fn each_write_gives_its_events() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let mut w = store.watch(&[]).open().unwrap();

    let a = store
        .remember("n", Memory::new("deploys run at noon"))
        .unwrap();
    let a = a.id().clone();
    assert_eq!(next(&mut w), vec![MemoryEvent::Remembered(a.clone())]);
    // An exact duplicate merges into `a`: an update, not a new memory.
    store
        .remember("n", Memory::new("deploys  run at noon "))
        .unwrap();
    assert_eq!(next(&mut w), vec![MemoryEvent::Updated(a.clone())]);
    store.mark_used(std::slice::from_ref(&a)).unwrap();
    assert_eq!(next(&mut w), vec![MemoryEvent::Updated(a.clone())]);

    let b = store
        .remember("n", Memory::new("deploys run at one"))
        .unwrap()
        .id()
        .clone();
    assert_eq!(next(&mut w), vec![MemoryEvent::Remembered(b.clone())]);
    // A link touches both ends.
    store.link(&a, &b, LinkType::SIMILAR_TO, 0.5).unwrap();
    assert_eq!(
        sorted(next(&mut w)),
        sorted(vec![
            MemoryEvent::Updated(a.clone()),
            MemoryEvent::Updated(b.clone())
        ])
    );
    // A supersede: the old memory is superseded, the new one gets the link.
    store.supersede(&a, &b).unwrap();
    assert_eq!(
        sorted(next(&mut w)),
        sorted(vec![
            MemoryEvent::Superseded {
                id: a.clone(),
                by: b.clone()
            },
            MemoryEvent::Updated(b.clone()),
        ])
    );
    // A delete: the memory goes, and its link's other end is updated.
    store.delete(&b).unwrap();
    let ev = next(&mut w);
    assert!(ev.contains(&MemoryEvent::Deleted(b.clone())), "{ev:?}");
    assert!(ev.contains(&MemoryEvent::Updated(a.clone())), "{ev:?}");
    assert_eq!(ev.len(), 2, "{ev:?}");
    quiet(&mut w);

    // A consolidation pass: its summary is `Summarized`, and its sources are updated.
    let c1 = store
        .remember(
            "c",
            Memory::new("the cache is cold after a restart").session("s"),
        )
        .unwrap()
        .id()
        .clone();
    let c2 = store
        .remember("c", Memory::new("the cache warms in a minute").session("s"))
        .unwrap()
        .id()
        .clone();
    assert_eq!(next(&mut w), vec![MemoryEvent::Remembered(c1.clone())]);
    assert_eq!(next(&mut w), vec![MemoryEvent::Remembered(c2.clone())]);
    let policy = ConsolidatePolicy::new()
        .min_age(Duration::ZERO)
        .group_similarity(0.0);
    let pass = store
        .consolidate("c", &Concat)
        .policy(&policy)
        .run()
        .unwrap();
    assert_eq!(pass.committed, 1, "{pass:?}");
    let ev = next(&mut w);
    let summaries: Vec<&MemoryId> = ev
        .iter()
        .filter_map(|e| match e {
            MemoryEvent::Summarized(id) => Some(id),
            _ => None,
        })
        .collect();
    assert_eq!(summaries.len(), 1, "{ev:?}");
    let stored = store.get(summaries[0]).unwrap().unwrap();
    assert!(stored.summary, "StoredMemory::summary");
    assert!(!store.get(&a).unwrap().unwrap().summary);
    assert!(ev.contains(&MemoryEvent::Updated(c1)), "{ev:?}");
    assert!(ev.contains(&MemoryEvent::Updated(c2)), "{ev:?}");
    assert!(
        !ev.iter().any(|e| matches!(e, MemoryEvent::Remembered(_))),
        "{ev:?}"
    );
    quiet(&mut w);
    db.close().unwrap();
}

#[test]
fn a_namespace_filter_keeps_only_its_namespaces() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let mut one = store.watch(&["a"]).open().unwrap();
    let mut two = store.watch(&["a", "b"]).open().unwrap();
    for ns in ["c", "b", "a", "ab"] {
        store.remember(ns, Memory::new(format!("in {ns}"))).unwrap();
    }
    let ns_of = |ev: Vec<MemoryEvent>| -> Vec<String> {
        ev.iter().map(|e| e.id().namespace().to_owned()).collect()
    };
    assert_eq!(ns_of(next(&mut one)), vec!["a"]);
    quiet(&mut one);
    assert_eq!(ns_of(next(&mut two)), vec!["b"]);
    assert_eq!(ns_of(next(&mut two)), vec!["a"]);
    quiet(&mut two);
    db.close().unwrap();
}

#[test]
fn a_token_resumes_where_the_watch_stopped() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let mut w = store.watch(&[]).open().unwrap();
    let first = store
        .remember("n", Memory::new("one"))
        .unwrap()
        .id()
        .clone();
    assert_eq!(next(&mut w), vec![MemoryEvent::Remembered(first)]);
    let token = WatchToken::from_bytes(w.token().as_bytes().to_vec());
    drop(w);
    let second = store
        .remember("n", Memory::new("two"))
        .unwrap()
        .id()
        .clone();
    let mut again = store.watch(&[]).resume(&token).open().unwrap();
    assert_eq!(next(&mut again), vec![MemoryEvent::Remembered(second)]);
    quiet(&mut again);
    db.close().unwrap();
}

#[test]
fn a_reader_process_watches_the_writers_memories() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let reader = Pigeonhole::open_reader(dir.join("mem.phdb"), ReaderOptions::default()).unwrap();
    let rs = Store::open_reader_without_embeddings(&reader, "notes").unwrap();
    let mut w = rs.watch(&["n"]).open().unwrap();
    // The reader's watch blocks on its own thread, as a reader process would run it.
    let watcher = std::thread::spawn(move || {
        let mut got = Vec::new();
        while got.len() < 2 {
            if let Some(c) = w.next(Duration::from_secs(10)).unwrap() {
                got.extend(c.events);
            } else {
                break;
            }
        }
        got
    });
    let m = store
        .remember("n", Memory::new("from the writer"))
        .unwrap()
        .id()
        .clone();
    store.delete(&m).unwrap();
    let got = watcher.join().unwrap();
    assert_eq!(
        got,
        vec![MemoryEvent::Remembered(m.clone()), MemoryEvent::Deleted(m)]
    );
    drop(rs);
    drop(reader);
    db.close().unwrap();
}

#[test]
fn bad_namespaces_and_a_database_without_a_feed_are_refused() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    for bad in ["", "a\0b"] {
        let r = store.watch(&[bad]).open();
        assert!(
            matches!(r, Err(Error::InvalidArgument(_))),
            "{bad:?}: {r:?}"
        );
    }
    drop(store);
    db.close().unwrap();
    let plain = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(plain.join("mem.phdb"), Options::default()).unwrap();
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let r = store.watch(&[]).open();
    assert!(
        matches!(&r, Err(e) if e.code() == pigeonhole::ErrorCode::InvalidArgument),
        "{r:?}"
    );
    drop(store);
    db.close().unwrap();
}

/// #1343's async form (D310 PR 5a): `next_async` in the writer gets the same events, waits
/// without a thread (a write from another thread wakes it), and passes over commits without
/// memory events. In a reader process it's `Unsupported`.
#[test]
fn next_async_in_the_writer_and_unsupported_in_a_reader() {
    use pigeonhole::doc_support::block_on;
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let mut w = store.watch(&[]).open().unwrap();
    // Already there: ready at once.
    let a = store
        .remember("n", Memory::new("first"))
        .unwrap()
        .id()
        .clone();
    let c = block_on(w.next_async()).unwrap();
    assert_eq!(c.events, vec![MemoryEvent::Remembered(a)]);
    // Not yet there: the task waits, and a write from another thread wakes it. A commit to
    // another table comes first and is passed over.
    let other = db
        .table("other")
        .unwrap()
        .family("f", pigeonhole::Family::default())
        .create_if_missing()
        .unwrap();
    let writer = {
        let store = store.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            other.mutate(b"x").put("f", b"q", b"v").commit().unwrap();
            store
                .remember("n", Memory::new("second"))
                .unwrap()
                .id()
                .clone()
        })
    };
    let c = block_on(w.next_async()).unwrap();
    let b = writer.join().unwrap();
    assert_eq!(c.events, vec![MemoryEvent::Remembered(b)]);

    let reader = Pigeonhole::open_reader(dir.join("mem.phdb"), ReaderOptions::default()).unwrap();
    let rs = Store::open_reader_without_embeddings(&reader, "notes").unwrap();
    let mut rw = rs.watch(&[]).open().unwrap();
    let r = block_on(rw.next_async());
    assert!(
        matches!(&r, Err(e) if e.code() == pigeonhole::ErrorCode::Unsupported),
        "{r:?}"
    );
    drop(rw);
    drop(rs);
    drop(reader);
    db.close().unwrap();
}

/// cli692 on #1438: a burst of commits the watch passes over (another namespace's, for a
/// watch of several, which reads every commit of the store) makes `poll_next` yield after
/// `PASS_OVER_PER_POLL` of them (waking its task), rather than run through them all in one
/// call; the memory event after the burst still arrives.
#[test]
fn poll_next_yields_during_a_burst_of_other_commits() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let mut w = store.watch(&["a", "b"]).open().unwrap();
    let burst = 3 * pigeonhole_memory::PASS_OVER_PER_POLL + 5;
    for i in 0..burst {
        store
            .remember("other", Memory::new(format!("not watched {i}")))
            .unwrap();
    }
    let m = store
        .remember("a", Memory::new("after the burst"))
        .unwrap()
        .id()
        .clone();
    let count = Arc::new(Count(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut pendings = 0;
    let got = loop {
        match w.poll_next(&mut cx) {
            Poll::Ready(r) => break r.unwrap(),
            Poll::Pending => {
                pendings += 1;
                assert!(pendings < 1000, "never became ready");
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    };
    assert_eq!(got.events, vec![MemoryEvent::Remembered(m)]);
    // At least the burst's yields, each with a wake of its own.
    assert!(pendings >= 3, "{pendings} pendings");
    assert!(count.0.load(Ordering::SeqCst) >= 3);
    db.close().unwrap();
}

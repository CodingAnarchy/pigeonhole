//! D311 T3 (#1337): consolidation. Grouping, the commit and its re-reads (S1, and simp724's
//! C1–C4 on #1337), the source policies, a consolidator's failure, and two consolidations of
//! one group, each ending in `verify`.

use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use pigeonhole::{Element, Options, Pigeonhole};
use pigeonhole_memory::{
    ConsolidatePolicy, Consolidator, EmbeddingModel, Error, Kind, LinkType, Memory, MemoryId,
    Remembered, SourcePolicy, Store, StoreOptions, StoredMemory, Weights,
};

type Out = Result<Vec<Memory>, Box<dyn std::error::Error + Send + Sync>>;

/// A consolidator from a closure.
struct F<G>(G);

impl<G: Fn(&[StoredMemory]) -> Out + Send + Sync> Consolidator for F<G> {
    fn consolidate(&self, group: &[StoredMemory]) -> Out {
        (self.0)(group)
    }
}

fn open(dir: &pigeonhole::doc_support::TempDir) -> Pigeonhole {
    Pigeonhole::open(dir.join("mem.phdb"), Options::default().shards(2)).expect("open")
}

fn plain(db: &Pigeonhole) -> Store {
    Store::create(db, "m", StoreOptions::new()).unwrap()
}

fn check(store: &Store, ctx: &str) {
    let st = store.verify().unwrap();
    assert!(st.is_consistent(), "{ctx}: {st:?}");
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

fn any_age() -> ConsolidatePolicy {
    ConsolidatePolicy::new().min_age(Duration::ZERO)
}

/// One summary joining the group's texts.
fn summary(text: &str) -> impl Fn(&[StoredMemory]) -> Out + Send + Sync + '_ {
    move |_| Ok(vec![Memory::new(text).importance(0.9)])
}

fn put(store: &Store, text: &str, session: &str, source: &str) -> MemoryId {
    store
        .remember(
            "a",
            Memory::new(text)
                .importance(0.4)
                .session(session)
                .source(source),
        )
        .unwrap()
        .id()
        .clone()
}

fn derived(store: &Store, id: &MemoryId) -> Vec<MemoryId> {
    let mut v: Vec<MemoryId> = store
        .get(id)
        .unwrap()
        .unwrap()
        .links
        .into_iter()
        .filter(|l| l.ty == LinkType::DERIVED_FROM)
        .map(|l| l.id)
        .collect();
    v.sort();
    v
}

/// The summary written by a pass: the namespace's semantic memories.
fn semantic(store: &Store) -> Vec<StoredMemory> {
    store
        .recall(&["a"])
        .text("note")
        .k(100)
        .candidates(100)
        .include_superseded(true)
        .run()
        .unwrap()
        .into_iter()
        .filter(|h| h.kind == Kind::Semantic)
        .map(|h| store.get(&h.id).unwrap().unwrap())
        .collect()
}

#[test]
fn keep_links_the_summary_marks_the_sources_and_moves_their_provenance() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = plain(&db);
    let a = put(&store, "note one", "s1", "call-1");
    let b = put(&store, "note two", "s1", "call-2");
    let other = put(&store, "note elsewhere", "s2", "call-3");
    let c = F(summary("note summary"));
    let pass = store.consolidate("a", &c).policy(&any_age()).run().unwrap();
    // Session s2 has one memory: a group of one, left alone at min_group 2.
    assert_eq!(
        (
            pass.groups,
            pass.committed,
            pass.skipped,
            pass.created,
            pass.sources
        ),
        (1, 1, 0, 1, 2),
        "{pass:?}"
    );
    let s = &semantic(&store)[0];
    assert_eq!(s.text, "note summary");
    assert_eq!(s.session.as_deref(), Some("s1"));
    let mut want = vec![a.clone(), b.clone()];
    want.sort();
    assert_eq!(derived(&store, &s.id), want);
    let mut names: Vec<&str> = s.sources.iter().map(|x| x.name.as_str()).collect();
    names.sort();
    assert_eq!(names, ["call-1", "call-2"]);
    for id in [&a, &b] {
        let m = store.get(id).unwrap().unwrap();
        assert!(m.consolidated && m.superseded_by.is_none(), "{m:?}");
    }
    assert!(!store.get(&other).unwrap().unwrap().consolidated);
    // Consolidated memories aren't grouped again.
    let again = store.consolidate("a", &c).policy(&any_age()).run().unwrap();
    assert_eq!(again.groups, 0, "{again:?}");
    check(&store, "keep");
    // Recall weights a consolidated memory's importance signal (0.5 by default): by
    // importance alone (linear fusion), `a` (0.4) ties `other` (0.4) unweighted, and falls
    // behind it weighted (0.2).
    let scores = |w: Weights| {
        let hits = store
            .recall(&["a"])
            .text("note")
            .fusion(pigeonhole_memory::Fusion::Linear)
            .weights(w)
            .k(10)
            .run()
            .unwrap();
        let score = |id: &MemoryId| hits.iter().find(|h| &h.id == id).unwrap().score;
        (score(&a), score(&other))
    };
    let mut w = Weights::default();
    w.vector = 0.0;
    w.keyword = 0.0;
    w.recency = 0.0;
    w.uses = 0.0;
    let (sa, so) = scores(w);
    assert!(sa < so, "{sa} {so}");
    w.consolidated = 1.0;
    let (sa, so) = scores(w);
    assert_eq!(sa, so);
    db.close().unwrap();
}

#[test]
fn delete_and_supersede_handle_the_sources() {
    for policy in [SourcePolicy::Delete, SourcePolicy::Supersede] {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let store = plain(&db);
        let a = put(&store, "note one", "s1", "call-1");
        let b = put(&store, "note two", "s1", "call-2");
        // `b` superseded an older fact, which mustn't come back when `b` goes (C3).
        let old = store
            .remember("a", Memory::new("note old fact"))
            .unwrap()
            .id()
            .clone();
        store.supersede(&old, &b).unwrap();
        // The application's own link to a source: under Delete it goes with the source.
        let about = LinkType::custom(20).unwrap();
        let c = F(move |g: &[StoredMemory]| {
            Ok(vec![Memory::new("note summary").importance(0.9).link(
                g[0].id.clone(),
                about,
                0.5,
            )])
        });
        let p = any_age().sources(policy);
        let pass = store.consolidate("a", &c).policy(&p).run().unwrap();
        assert_eq!(
            (pass.committed, pass.sources),
            (1, 2),
            "{policy:?}: {pass:?}"
        );
        let s = semantic(&store).remove(0);
        let mut names: Vec<&str> = s.sources.iter().map(|x| x.name.as_str()).collect();
        names.sort();
        assert_eq!(names, ["call-1", "call-2"], "{policy:?}");
        match policy {
            SourcePolicy::Delete => {
                assert!(store.get(&a).unwrap().is_none());
                assert!(store.get(&b).unwrap().is_none());
                assert!(derived(&store, &s.id).is_empty());
                assert!(s.links.iter().all(|l| l.ty != about), "{:?}", s.links);
                assert_eq!(
                    store.get(&old).unwrap().unwrap().superseded_by,
                    Some(s.id.clone()),
                    "the old fact moves to the summary"
                );
            }
            _ => {
                for id in [&a, &b] {
                    assert_eq!(
                        store.get(id).unwrap().unwrap().superseded_by,
                        Some(s.id.clone())
                    );
                }
                assert_eq!(
                    store.get(&old).unwrap().unwrap().superseded_by,
                    Some(b.clone())
                );
            }
        }
        check(&store, &format!("{policy:?}"));
        db.close().unwrap();
    }
}

#[test]
fn nothing_returned_marks_or_deletes_the_sources() {
    for policy in [
        SourcePolicy::Keep,
        SourcePolicy::Delete,
        SourcePolicy::Supersede,
    ] {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let store = plain(&db);
        let a = put(&store, "note one", "s1", "c1");
        let b = put(&store, "note two", "s1", "c2");
        let old = store
            .remember("a", Memory::new("note old fact"))
            .unwrap()
            .id()
            .clone();
        store.supersede(&old, &b).unwrap();
        let c = F(|_: &[StoredMemory]| Ok(vec![]));
        let p = any_age().sources(policy);
        let pass = store.consolidate("a", &c).policy(&p).run().unwrap();
        assert_eq!((pass.committed, pass.created), (1, 0), "{policy:?}");
        let ma = store.get(&a).unwrap();
        let mb = store.get(&b).unwrap().unwrap();
        // `b` superseded something, so it stays even under Delete (C3), marked.
        assert!(mb.consolidated, "{policy:?}");
        if policy == SourcePolicy::Delete {
            assert!(ma.is_none());
        } else {
            assert!(ma.unwrap().consolidated, "{policy:?}");
        }
        assert_eq!(store.get(&old).unwrap().unwrap().superseded_by, Some(b));
        check(&store, &format!("nothing, {policy:?}"));
        db.close().unwrap();
    }
}

/// simp724's C2: a summary that's one of the group's own texts merges into that memory,
/// which becomes the result: never deleted, superseded by itself, or linked to itself.
#[test]
fn a_summary_equal_to_a_source_makes_that_source_the_result() {
    for policy in [
        SourcePolicy::Keep,
        SourcePolicy::Delete,
        SourcePolicy::Supersede,
    ] {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let store = plain(&db);
        let a = put(&store, "note the fact", "s1", "c1");
        let b = put(&store, "note the  fact, again", "s1", "c2");
        let c = F(|_: &[StoredMemory]| Ok(vec![Memory::new("note  the fact")]));
        let p = any_age().sources(policy);
        let pass = store.consolidate("a", &c).policy(&p).run().unwrap();
        assert_eq!(
            (pass.committed, pass.created, pass.merged),
            (1, 0, 1),
            "{pass:?}"
        );
        let ma = store.get(&a).unwrap().unwrap();
        assert!(
            ma.consolidated && ma.superseded_by.is_none(),
            "{policy:?}: {ma:?}"
        );
        assert!(!derived(&store, &a).contains(&a));
        let mut names: Vec<&str> = ma.sources.iter().map(|x| x.name.as_str()).collect();
        names.sort();
        assert_eq!(names, ["c1", "c2"], "{policy:?}");
        let mb = store.get(&b).unwrap();
        match policy {
            SourcePolicy::Delete => assert!(mb.is_none()),
            SourcePolicy::Supersede => {
                assert_eq!(mb.unwrap().superseded_by, Some(a.clone()));
            }
            _ => {
                assert!(mb.unwrap().consolidated);
                assert_eq!(derived(&store, &a), vec![b.clone()]);
            }
        }
        check(&store, &format!("merged into a source, {policy:?}"));
        db.close().unwrap();
    }
}

/// The acceptance test: two consolidations of one group. The second runs to completion
/// inside the first's consolidator call; the first then finds its group taken and skips.
#[test]
fn two_consolidations_of_one_group_one_commits_and_the_other_skips() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = plain(&db);
    put(&store, "note one", "s1", "c1");
    put(&store, "note two", "s1", "c2");
    let inner = F(summary("note inner summary"));
    let outer = F(|_: &[StoredMemory]| {
        let pass = store
            .consolidate("a", &inner)
            .policy(&any_age())
            .run()
            .unwrap();
        assert_eq!(pass.committed, 1, "{pass:?}");
        Ok(vec![Memory::new("note outer summary")])
    });
    let pass = store
        .consolidate("a", &outer)
        .policy(&any_age())
        .run()
        .unwrap();
    assert_eq!(
        (pass.committed, pass.skipped, pass.created),
        (0, 1, 0),
        "{pass:?}"
    );
    let texts: Vec<String> = semantic(&store).into_iter().map(|m| m.text).collect();
    assert_eq!(texts, ["note inner summary"]);
    check(&store, "nested");
    db.close().unwrap();
}

/// The same with two threads that both group, then commit together: whichever loses (on
/// its re-reads, or on the commit's conflict) skips, and only one summary lands.
#[test]
fn two_concurrent_consolidations_commit_once() {
    for round in 0..8 {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let store = plain(&db);
        put(&store, "note one", "s1", "c1");
        put(&store, "note two", "s1", "c2");
        let gate = Arc::new(Barrier::new(2));
        let passes = Arc::new(Mutex::new(Vec::new()));
        std::thread::scope(|sc| {
            for i in 0..2 {
                let (store, gate, passes) = (store.clone(), gate.clone(), passes.clone());
                sc.spawn(move || {
                    let text = format!("note summary {i}");
                    let c = F(move |_: &[StoredMemory]| {
                        gate.wait();
                        Ok(vec![Memory::new(text.clone())])
                    });
                    let pass = store.consolidate("a", &c).policy(&any_age()).run().unwrap();
                    passes.lock().unwrap().push(pass);
                });
            }
        });
        let passes = passes.lock().unwrap();
        let committed: u64 = passes.iter().map(|p| p.committed).sum();
        let skipped: u64 = passes.iter().map(|p| p.skipped).sum();
        assert_eq!((committed, skipped), (1, 1), "round {round}: {passes:?}");
        assert_eq!(semantic(&store).len(), 1, "round {round}");
        check(&store, &format!("concurrent, round {round}"));
        db.close().unwrap();
    }
}

#[test]
fn groups_follow_sessions_similarity_age_and_the_limits() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let model = EmbeddingModel::new("e", 2, Element::F32).unwrap();
    let store = Store::create(&db, "m", StoreOptions::new().embedding(model)).unwrap();
    let mem = |text: &str, e: [f32; 2], session: &str| {
        store
            .remember(
                "a",
                Memory::new(text).embedding(e.to_vec()).session(session),
            )
            .unwrap()
            .id()
            .clone()
    };
    let x1 = mem("note x1", [1.0, 0.0], "s1");
    let y1 = mem("note y1", [0.0, 1.0], "s1");
    let x2 = mem("note x2", [0.99, 0.1], "s1");
    let y2 = mem("note y2", [0.1, 0.99], "s1");
    let x3 = mem("note x3 other session", [1.0, 0.0], "s2");
    let pinned = store
        .remember(
            "a",
            Memory::new("note pinned")
                .embedding(vec![1.0, 0.0])
                .session("s1")
                .pinned(true),
        )
        .unwrap()
        .id()
        .clone();
    let seen = Mutex::new(Vec::new());
    let c = F(|g: &[StoredMemory]| {
        seen.lock()
            .unwrap()
            .push(g.iter().map(|m| m.id.clone()).collect::<Vec<_>>());
        Ok(vec![])
    });
    // Too young: nothing.
    let pass = store
        .consolidate("a", &c)
        .policy(&ConsolidatePolicy::new().sources(SourcePolicy::Keep))
        .run()
        .unwrap();
    assert_eq!(pass.groups, 0);
    // By session, then by similarity to the seed: {x1, x2} and {y1, y2}; x3 is alone in s2,
    // the pinned memory is never a candidate. At most one group a pass.
    let p = ConsolidatePolicy::new().max_groups(1);
    let pass = store
        .consolidate("a", &c)
        .policy(&p)
        .now_us(now() + 2 * 3600 * 1_000_000)
        .run()
        .unwrap();
    assert_eq!(pass.groups, 1);
    let pass = store
        .consolidate("a", &c)
        .policy(&p)
        .now_us(now() + 2 * 3600 * 1_000_000)
        .run()
        .unwrap();
    assert_eq!(pass.groups, 1);
    // As sets: ids made in the same microsecond don't sort in creation order.
    let sorted = |mut g: Vec<Vec<MemoryId>>| {
        g.iter_mut().for_each(|g| g.sort());
        g.sort();
        g
    };
    let groups = sorted(seen.lock().unwrap().clone());
    assert_eq!(groups, sorted(vec![vec![x1, x2], vec![y1, y2]]));
    // Groups of one when asked: x3 (pinned still excluded).
    seen.lock().unwrap().clear();
    let pass = store
        .consolidate("a", &c)
        .policy(&any_age().min_group(1))
        .run()
        .unwrap();
    assert_eq!(pass.groups, 1);
    assert_eq!(seen.lock().unwrap().clone(), vec![vec![x3]]);
    assert!(!store.get(&pinned).unwrap().unwrap().consolidated);
    check(&store, "grouping");
    db.close().unwrap();
}

#[test]
fn a_failed_consolidator_stops_the_pass_and_keeps_what_committed() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = plain(&db);
    put(&store, "note a1", "s1", "c1");
    put(&store, "note a2", "s1", "c2");
    put(&store, "note b1", "s2", "c3");
    put(&store, "note b2", "s2", "c4");
    let calls = Mutex::new(0);
    let c = F(|_: &[StoredMemory]| {
        let mut n = calls.lock().unwrap();
        *n += 1;
        if *n == 2 {
            return Err("the model is down".into());
        }
        Ok(vec![Memory::new("note summary")])
    });
    let err = store
        .consolidate("a", &c)
        .policy(&any_age())
        .run()
        .unwrap_err();
    let Error::Consolidate { error, pass } = &err else {
        panic!("{err:?}")
    };
    assert_eq!(error.to_string(), "the model is down");
    assert_eq!((pass.groups, pass.committed), (2, 1), "{pass:?}");
    assert_eq!(err.code(), pigeonhole::ErrorCode::MergeFailed);
    assert_eq!(semantic(&store).len(), 1);
    check(&store, "failed");
    db.close().unwrap();
}

#[test]
fn a_bad_policy_or_returned_memory_is_refused() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = plain(&db);
    put(&store, "note one", "s1", "c1");
    put(&store, "note two", "s1", "c2");
    let c = F(summary("note s"));
    for p in [
        any_age().min_group(0),
        any_age().min_group(3).max_group(2),
        any_age().max_groups(0),
        any_age().group_similarity(1.5),
        any_age().group_similarity(f32::NAN),
    ] {
        assert!(
            matches!(
                store.consolidate("a", &c).policy(&p).run(),
                Err(Error::InvalidArgument(_))
            ),
            "{p:?}"
        );
    }
    // An embedding in a store without a model: refused before anything is written.
    let bad = F(|_: &[StoredMemory]| Ok(vec![Memory::new("note s").embedding(vec![1.0])]));
    assert!(matches!(
        store.consolidate("a", &bad).policy(&any_age()).run(),
        Err(Error::InvalidArgument(_))
    ));
    assert!(semantic(&store).is_empty());
    check(&store, "refused");
    db.close().unwrap();
}

/// Two returned memories of one text are one memory: the transaction reads at its snapshot,
/// so exact dedup wouldn't see the first, and the pass folds them as dedup would.
#[test]
fn two_returned_memories_of_one_text_are_one() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = plain(&db);
    put(&store, "note one", "s1", "c1");
    put(&store, "note two", "s1", "c2");
    let c = F(|_: &[StoredMemory]| {
        Ok(vec![
            Memory::new("note summary"),
            Memory::new("note  summary "),
        ])
    });
    let pass = store.consolidate("a", &c).policy(&any_age()).run().unwrap();
    assert_eq!((pass.created, pass.merged), (1, 1), "{pass:?}");
    assert_eq!(semantic(&store).len(), 1);
    // And it dedups as a remember would: the same text merges into it.
    let r = store.remember("a", Memory::new("note summary")).unwrap();
    assert!(matches!(r, Remembered::Merged(_)), "{r:?}");
    check(&store, "folded");
    db.close().unwrap();
}

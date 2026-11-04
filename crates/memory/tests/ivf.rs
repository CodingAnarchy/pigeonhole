//! D311 T8 (#1342): IVF recall for large namespaces (`StoreOptions::ivf`).
//! - Probing every list, an IVF recall equals the exact one, hit for hit and score for
//!   score, in the large namespace, at a snapshot taken before splits, and through the
//!   per-list fallback in a sparse namespace.
//! - At the default `nprobe` it recalls most of what exact search does.
//! - The index and the namespace counter follow every write: remember, an exact-dedup merge,
//!   delete, a rolled-back and a committed `remember_in`, and a forget pass.
//! - Two remembers into one namespace don't conflict through the counter.
//! - `verify` reports a drifted counter apart from mismatches.
//! - `verify` checks the index's membership, not its count, at its own snapshot (#1427).

use std::collections::BTreeSet;

use pigeonhole::{Element, Options, Pigeonhole, RetryPolicy};
use pigeonhole_memory::{
    EmbeddingModel, Error, ForgetPolicy, IvfOptions, Kind, Memory, MemoryId, RecallHit, Store,
    StoreOptions, Weights,
};

const DIM: usize = 8;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn unit(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 24) as f32 * 2.0 - 1.0
    }
}

/// An embedding near one of 6 cluster centres, so the index's lists are meaningful.
fn emb(rng: &mut Rng) -> Vec<f32> {
    let c = (rng.next() % 6) as f32;
    (0..DIM)
        .map(|d| if d == (c as usize) { 3.0 } else { 0.0 } + rng.unit() * 0.7)
        .collect()
}

fn open() -> (pigeonhole::doc_support::TempDir, Pigeonhole) {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("mem.phdb"), Options::default().shards(2)).unwrap();
    (dir, db)
}

fn create(db: &Pigeonhole, ivf: IvfOptions) -> Store {
    let model = EmbeddingModel::new("test-model", DIM as u32, Element::F32).unwrap();
    Store::create(db, "notes", StoreOptions::new().embedding(model).ivf(ivf)).unwrap()
}

/// Small lists, so a few hundred memories make many of them; IVF from 100 memories.
fn small() -> IvfOptions {
    IvfOptions::new()
        .min_namespace_len(100)
        .target_list_len(8)
        .nprobe(2)
}

/// Vector-only weights: the recall's order is the vector signal's.
fn vector_only() -> Weights {
    let mut w = Weights::default();
    w.keyword = 0.0;
    w.recency = 0.0;
    w.importance = 0.0;
    w.uses = 0.0;
    w
}

fn ids(hits: &[RecallHit]) -> Vec<MemoryId> {
    hits.iter().map(|h| h.id.clone()).collect()
}

/// A large namespace `big` (400 memories) and a small one `small` (20).
fn fill(store: &Store, rng: &mut Rng) {
    for (ns, n) in [("big", 400), ("small", 20)] {
        for i in 0..n {
            store
                .remember(
                    ns,
                    Memory::new(format!("{ns} memory {i}")).embedding(emb(rng)),
                )
                .unwrap();
        }
    }
}

#[test]
fn probing_every_list_an_ivf_recall_equals_the_exact_one() {
    let (_dir, db) = open();
    let store = create(&db, small());
    let mut rng = Rng(0x5eed);
    fill(&store, &mut rng);
    assert!(store.verify().unwrap().is_consistent());
    for i in 0..20 {
        let q = emb(&mut rng);
        let exact = store
            .recall(&["big"])
            .embedding(q.clone())
            .weights(vector_only())
            .exact(true)
            .run_with_stats()
            .unwrap();
        assert_eq!(exact.1.ivf_namespaces, 0);
        let all = store
            .recall(&["big"])
            .embedding(q.clone())
            .weights(vector_only())
            .nprobe(10_000)
            .run_with_stats()
            .unwrap();
        assert_eq!(all.1.ivf_namespaces, 1, "query {i}");
        assert_eq!(ids(&all.0), ids(&exact.0), "query {i}");
        let scores = |h: &[RecallHit]| h.iter().map(|h| h.score).collect::<Vec<_>>();
        assert_eq!(scores(&all.0), scores(&exact.0), "query {i}");
    }
}

#[test]
fn at_the_default_nprobe_ivf_recalls_most_of_what_exact_does() {
    let (_dir, db) = open();
    let store = create(
        &db,
        IvfOptions::new().min_namespace_len(100).target_list_len(8),
    );
    let mut rng = Rng(0xfeed);
    fill(&store, &mut rng);
    let (mut found, mut total) = (0, 0);
    for _ in 0..30 {
        let q = emb(&mut rng);
        let run = |exact: bool| {
            store
                .recall(&["big"])
                .embedding(q.clone())
                .weights(vector_only())
                .exact(exact)
                .run()
                .unwrap()
        };
        let want: BTreeSet<MemoryId> = ids(&run(true)).into_iter().collect();
        let got: BTreeSet<MemoryId> = ids(&run(false)).into_iter().collect();
        found += want.intersection(&got).count();
        total += want.len();
    }
    let recall = found as f64 / total as f64;
    assert!(recall >= 0.8, "recall@10 against exact: {recall:.3}");
}

#[test]
fn a_small_namespace_is_exact_and_a_sparse_one_falls_back() {
    let (_dir, db) = open();
    let store = create(&db, small());
    let mut rng = Rng(0xabc);
    fill(&store, &mut rng);
    let q = emb(&mut rng);
    // Below min_namespace_len: exact, not counted as IVF.
    let (_, st) = store
        .recall(&["small"])
        .embedding(q.clone())
        .run_with_stats()
        .unwrap();
    assert_eq!((st.ivf_namespaces, st.ivf_fallbacks), (0, 0));

    // A store whose threshold every namespace passes: `small`'s 20 memories are sparse among
    // the lists nearest the query, so the widening hits its cap and recall searches it
    // exactly, with the exact answer.
    let (_dir2, db2) = open();
    let low = create(&db2, small().min_namespace_len(1));
    let mut rng = Rng(0xabc);
    fill(&low, &mut rng);
    let q = emb(&mut rng);
    let (hits, st) = low
        .recall(&["small"])
        .embedding(q.clone())
        .weights(vector_only())
        .run_with_stats()
        .unwrap();
    assert_eq!((st.ivf_namespaces, st.ivf_fallbacks), (0, 1));
    let exact = low
        .recall(&["small"])
        .embedding(q)
        .weights(vector_only())
        .exact(true)
        .run()
        .unwrap();
    assert_eq!(ids(&hits), ids(&exact));
}

#[test]
fn a_recall_at_a_snapshot_from_before_splits_equals_the_exact_one_there() {
    let (_dir, db) = open();
    let store = create(&db, small());
    let mut rng = Rng(0x77);
    for i in 0..150 {
        store
            .remember("big", Memory::new(format!("m{i}")).embedding(emb(&mut rng)))
            .unwrap();
    }
    let s = db.snapshot().unwrap();
    // Enough more to split lists after the snapshot.
    for i in 150..450 {
        store
            .remember("big", Memory::new(format!("m{i}")).embedding(emb(&mut rng)))
            .unwrap();
    }
    for _ in 0..10 {
        let q = emb(&mut rng);
        let at = |exact: bool| {
            store
                .recall(&["big"])
                .embedding(q.clone())
                .weights(vector_only())
                .snapshot(&s)
                .nprobe(10_000)
                .exact(exact)
                .run()
                .unwrap()
        };
        assert_eq!(ids(&at(false)), ids(&at(true)));
    }
}

#[test]
fn the_index_and_the_counter_follow_every_write() {
    let (_dir, db) = open();
    let store = create(&db, small());
    let mut rng = Rng(0x99);
    let mut kept = Vec::new();
    for i in 0..120 {
        let r = store
            .remember(
                "big",
                Memory::new(format!("note {i}")).embedding(emb(&mut rng)),
            )
            .unwrap();
        kept.push(r.id().clone());
    }
    // `verify` reads the index at its own snapshot (#1427), so this holds on a busy store
    // too: see `verify_checks_the_index_s_membership_at_its_own_snapshot`.
    let ok = |what: &str| {
        let st = store.verify().unwrap();
        assert!(st.is_consistent(), "{what}: {st:?}");
        assert_eq!(st.ivf_mismatched, 0, "{what}: {st:?}");
        assert_eq!(st.ivf_counter_drift, 0, "{what}: {st:?}");
    };
    ok("after the remembers");
    // An exact duplicate merges: no second vector.
    store
        .remember("big", Memory::new("note 7").embedding(emb(&mut rng)))
        .unwrap();
    ok("after a merge");
    for id in kept.drain(..30) {
        assert!(store.delete(&id).unwrap());
    }
    ok("after deletes");
    // A rolled-back remember_in leaves the index and the counter as they were.
    let r = db.transact(&RetryPolicy::default(), |txn| {
        store.remember_in(txn, "big", Memory::new("never").embedding(emb(&mut rng)))?;
        Err::<(), Error>(Error::InvalidArgument("rolled back".into()))
    });
    assert!(r.is_err());
    ok("after a rolled-back remember_in");
    // A committed remember_in: its split work waits for the next write, which is fine.
    db.transact(&RetryPolicy::default(), |txn| {
        store.remember_in(txn, "big", Memory::new("kept").embedding(emb(&mut rng)))
    })
    .unwrap();
    ok("after a remember_in");
    // A forget pass deletes through `delete_in`, so the index and the counter follow.
    let policy = ForgetPolicy::default().max_age(Kind::Episodic, std::time::Duration::from_secs(1));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    let far = now + 3_600_000_000;
    let pass = store.forget().policy(&policy).now_us(far).run().unwrap();
    assert!(pass.deleted > 0, "{pass:?}");
    ok("after a forget pass");
}

#[test]
fn two_remembers_into_one_namespace_dont_conflict_through_the_counter() {
    let (_dir, db) = open();
    let store = create(&db, small());
    let mut rng = Rng(0x42);
    for i in 0..10 {
        store
            .remember(
                "big",
                Memory::new(format!("seed {i}")).embedding(emb(&mut rng)),
            )
            .unwrap();
    }
    // Both transactions read and write before either commits: only the counter (a blind
    // increment) and distinct rows are shared, so both commit.
    let mut a = db.transaction().unwrap();
    let mut b = db.transaction().unwrap();
    store
        .remember_in(
            &mut a,
            "big",
            Memory::new("from a").embedding(emb(&mut rng)),
        )
        .unwrap();
    store
        .remember_in(
            &mut b,
            "big",
            Memory::new("from b").embedding(emb(&mut rng)),
        )
        .unwrap();
    a.commit().unwrap();
    // The one shared read they could collide on is the list's version, which only a split
    // changes; with 10 memories there's one list and no split.
    if let Err(e) = b.commit() {
        panic!("the second remember conflicted: {:?}", e.code());
    }
    let st = store.verify().unwrap();
    assert!(
        st.is_consistent() && st.ivf_mismatched == 0 && st.ivf_counter_drift == 0,
        "{st:?}"
    );
}

/// simp724's R1 on #1424: a caller writing only through `remember_in` never settles, and
/// its records stop at the cap instead of growing for ever. The next `remember` settles them.
#[cfg(feature = "__unstable-hooks")]
#[test]
fn records_from_remember_in_alone_stay_bounded() {
    use pigeonhole_memory::hooks::ivf_pending;
    let (_dir, db) = open();
    let store = create(&db, small());
    let mut rng = Rng(0x1024);
    for i in 0..1_100 {
        db.transact(&RetryPolicy::default(), |txn| {
            store.remember_in(
                txn,
                "big",
                Memory::new(format!("in {i}")).embedding(emb(&mut rng)),
            )
        })
        .unwrap();
    }
    assert_eq!(ivf_pending(&store), 1_024, "capped");
    store
        .remember("big", Memory::new("settles").embedding(emb(&mut rng)))
        .unwrap();
    assert_eq!(ivf_pending(&store), 0, "settled");
    let st = store.verify().unwrap();
    assert!(st.is_consistent() && st.ivf_mismatched == 0, "{st:?}");
    // The settle split the lists the remember_in writes left out of bounds.
    let exact = |exact: bool| {
        store
            .recall(&["big"])
            .embedding(emb(&mut Rng(7)))
            .weights(vector_only())
            .nprobe(10_000)
            .exact(exact)
            .run()
            .unwrap()
    };
    assert_eq!(ids(&exact(false)), ids(&exact(true)));
}

#[test]
fn a_store_without_a_model_refuses_ivf_and_ivf_reads_back() {
    let (_dir, db) = open();
    let r = Store::create(&db, "plain", StoreOptions::new().ivf(IvfOptions::new()));
    assert!(matches!(r, Err(Error::InvalidArgument(_))), "{r:?}");
    let model = EmbeddingModel::new("test-model", DIM as u32, Element::F32).unwrap();
    let r = Store::create(
        &db,
        "zero",
        StoreOptions::new()
            .embedding(model.clone())
            .ivf(IvfOptions::new().min_namespace_len(0)),
    );
    assert!(matches!(r, Err(Error::InvalidArgument(_))), "{r:?}");
    let store = create(&db, small());
    assert_eq!(store.ivf(), Some(small()));
    drop(store);
    let again = Store::open(&db, "notes", "test-model").unwrap();
    assert_eq!(again.ivf(), Some(small()));
    let plain = Store::create(&db, "noivf", StoreOptions::new().embedding(model)).unwrap();
    assert_eq!(plain.ivf(), None);
}

/// cli692's R1 on #1424: a `remember_in` and a `delete_in` of the same memory in one
/// transaction. The delete reads at the transaction's snapshot, which doesn't have the
/// memory yet, so it deletes nothing, neither the memory nor its vector: the store and the
/// index agree.
#[test]
fn a_delete_in_after_a_remember_in_in_one_transaction_deletes_nothing() {
    let (_dir, db) = open();
    let store = create(&db, small());
    let mut rng = Rng(0x31);
    let (deleted, _) = db
        .transact(&RetryPolicy::default(), |txn| {
            let r = store.remember_in(txn, "big", Memory::new("brief").embedding(emb(&mut rng)))?;
            store.delete_in(txn, r.id())
        })
        .unwrap();
    assert!(
        !deleted,
        "the transaction's snapshot has no such memory to delete"
    );
    let st = store.verify().unwrap();
    assert_eq!(
        (st.memories, st.ivf_mismatched, st.ivf_counter_drift),
        (1, 0, 0),
        "{st:?}"
    );
}

/// #1427 (simp724's R3 on #1424): `verify` compares the index's ids with the memories that
/// have an embedding, so a missing vector and a stale one no longer cancel out, and it reads
/// both at one snapshot, so it holds while writers run.
#[test]
fn verify_checks_the_index_s_membership_at_its_own_snapshot() {
    let (_dir, db) = open();
    let store = create(&db, small());
    let mut rng = Rng(0x1427);
    let mut kept = Vec::new();
    for i in 0..150 {
        let r = store
            .remember(
                "big",
                Memory::new(format!("m {i}")).embedding(emb(&mut rng)),
            )
            .unwrap();
        kept.push(r.id().clone());
    }

    // While another thread remembers and deletes, every `verify` agrees with itself.
    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|sc| {
        let writer = store.clone();
        let mut wrng = Rng(0x7);
        let done = &done;
        sc.spawn(move || {
            let mut mine = Vec::new();
            for i in 0..200 {
                let r = writer
                    .remember(
                        "big",
                        Memory::new(format!("w {i}")).embedding(emb(&mut wrng)),
                    )
                    .unwrap();
                mine.push(r.id().clone());
                if i % 3 == 0 {
                    assert!(writer.delete(&mine.remove(0)).unwrap());
                }
            }
            done.store(true, std::sync::atomic::Ordering::Release);
        });
        let mut checks = 0;
        while !done.load(std::sync::atomic::Ordering::Acquire) || checks < 3 {
            let st = store.verify().unwrap();
            assert!(st.is_consistent(), "check {checks}: {st:?}");
            assert_eq!(
                (st.ivf_missing, st.ivf_stale),
                (0, 0),
                "check {checks}: {st:?}"
            );
            checks += 1;
        }
    });

    // Damage the index behind the store's back: one memory's vector removed and one vector
    // with no memory added. The count is unchanged, which the old count-only check passed.
    let index = pigeonhole_vector::Index::open(&db, "mem_notes_ivf").unwrap();
    let before = store.verify().unwrap();
    let mut held = Vec::new();
    index
        .ids_at(&db.snapshot().unwrap(), |id| held.push(id.to_vec()))
        .unwrap();
    assert!(index.delete(&held[0]).unwrap());
    index.insert_f32(b"\xffstale", &emb(&mut rng)).unwrap();
    let st = store.verify().unwrap();
    assert_eq!(st.memories, before.memories);
    assert_eq!(
        (st.ivf_missing, st.ivf_stale, st.ivf_mismatched),
        (1, 1, 2),
        "{st:?}"
    );
    assert!(!st.is_consistent(), "{st:?}");
}

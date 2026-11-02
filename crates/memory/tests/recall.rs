//! D311 T1's second PR (#1335, ICR 0107): recall. Each signal's value against an
//! independent computation, the ranks against the values, the fused score against its
//! formula, the candidate selection against an independent one, the byte budget, the
//! filters, the linear blend, several namespaces, and a reader process.

use std::collections::{BTreeMap, BTreeSet};

use pigeonhole::{Element, Options, Pigeonhole, ReaderOptions};
use pigeonhole_memory::{
    EmbeddingModel, Error, Fusion, Memory, MemoryId, RecallHit, Signal, Store, StoreOptions,
    Weights,
};
use pigeonhole_text::{Index, Query};

const MODEL: &str = "test-embed-v1";
const DIM: usize = 4;
const HALF_LIFE_US: f64 = 7.0 * 24.0 * 3600.0 * 1e6;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn unit(&mut self) -> f32 {
        (self.below(2001) as f32 - 1000.0) / 1000.0
    }
}

const VOCAB: &[&str] = &[
    "postgres", "meeting", "deploy", "tuesday", "alice", "bob", "index", "memory", "agent",
    "vector", "search", "budget", "release", "window", "schema",
];

fn text(rng: &mut Rng) -> String {
    let n = 2 + rng.below(6);
    (0..n)
        .map(|_| VOCAB[rng.below(VOCAB.len() as u64) as usize])
        .collect::<Vec<_>>()
        .join(" ")
}

fn emb(rng: &mut Rng) -> Vec<f32> {
    let mut v: Vec<f32> = (0..DIM).map(|_| rng.unit()).collect();
    if v.iter().all(|x| *x == 0.0) {
        v[0] = 1.0;
    }
    v
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    let dot: f64 = a
        .iter()
        .zip(b)
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum();
    let na: f64 = a.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
    let nb: f64 = b.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
    dot / (na * nb)
}

/// A memory as the fixture wrote it: its text, embedding, importance and uses.
type Written = (String, Vec<f32>, f64, u64);

struct Fixture {
    _dir: pigeonhole::doc_support::TempDir,
    db: Pigeonhole,
    store: Store,
    /// Per memory: its text, embedding, importance and uses.
    mems: BTreeMap<MemoryId, Written>,
}

fn fixture(seed: u64) -> Fixture {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("mem.phdb"), Options::default().shards(2)).unwrap();
    let model = EmbeddingModel::new(MODEL, DIM as u32, Element::F32).unwrap();
    let store = Store::create(&db, "notes", StoreOptions::new().embedding(model)).unwrap();
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut mems = BTreeMap::new();
    for (ns, n) in [("a", 40), ("b", 10)] {
        for i in 0..n {
            let t = format!("{} m{i}", text(&mut rng));
            let e = emb(&mut rng);
            let imp = rng.below(1001) as f64 / 1000.0;
            let r = store
                .remember(
                    ns,
                    Memory::new(t.clone()).embedding(e.clone()).importance(imp),
                )
                .unwrap();
            mems.insert(r.id().clone(), (t, e, imp, 0));
        }
    }
    // Some uses.
    let ids: Vec<MemoryId> = mems.keys().cloned().collect();
    for id in ids.iter().step_by(3) {
        store.mark_used(std::slice::from_ref(id)).unwrap();
        mems.get_mut(id).unwrap().3 += 1;
    }
    Fixture {
        _dir: dir,
        db,
        store,
        mems,
    }
}

/// A recall's hits are internally consistent: each signal's value against an independent
/// computation, its rank against the values, the fused score against RRF's formula, and
/// the order (score descending, then id).
fn check_rrf(
    f: &Fixture,
    hits: &[RecallHit],
    q_text: &str,
    q_emb: &[f32],
    now: u64,
    bm25: &BTreeMap<Vec<u8>, f64>,
    ctx: &str,
) {
    let close = |a: f64, b: f64, tol: f64| (a - b).abs() <= tol * b.abs().max(1.0);
    for h in hits {
        let (t, e, imp, uses) = &f.mems[&h.id];
        assert_eq!(&h.text, t, "{ctx}");
        let s = h.signals.expect("with_signals");
        let v = s.vector.expect("every memory has an embedding").value;
        assert!(
            close(v, cosine_similarity(q_emb, e), 1e-5),
            "{ctx}: similarity {v}"
        );
        let age = now.saturating_sub(h.id.created_us()) as f64;
        assert!(
            close(s.recency.unwrap().value, (-age / HALF_LIFE_US).exp(), 1e-12),
            "{ctx}"
        );
        assert!(close(s.importance.unwrap().value, *imp, 1e-6), "{ctx}");
        assert_eq!(s.use_.is_some(), *uses > 0, "{ctx}");
        let key = row_key(&h.id);
        match (s.keyword, bm25.get(&key)) {
            (Some(k), Some(b)) => assert_eq!(k.value, *b, "{ctx}"),
            (None, None) => {}
            other => panic!("{ctx}: keyword {other:?} for {q_text:?}"),
        }
    }
    // Ranks: competition ranking (1 + how many values are strictly greater), among the hits
    // that have the signal (the hits are every candidate here).
    let sigs: [fn(&pigeonhole_memory::Signals) -> Option<Signal>; 5] = [
        |s| s.vector,
        |s| s.keyword,
        |s| s.recency,
        |s| s.importance,
        |s| s.use_,
    ];
    for get in sigs {
        let ranked: Vec<(&MemoryId, Signal)> = hits
            .iter()
            .filter_map(|h| get(h.signals.as_ref().unwrap()).map(|s| (&h.id, s)))
            .collect();
        for (_, s) in &ranked {
            let above = ranked.iter().filter(|(_, o)| o.value > s.value).count();
            assert_eq!(s.rank as usize, above + 1, "{ctx}");
        }
    }
    for h in hits {
        let s = h.signals.unwrap();
        let want: f64 = [s.vector, s.keyword, s.recency, s.importance, s.use_]
            .iter()
            .flatten()
            .map(|x| 1.0 / (60.0 + f64::from(x.rank)))
            .sum();
        assert!(
            close(h.score, want, 1e-12),
            "{ctx}: score {} vs {want}",
            h.score
        );
    }
    assert!(
        hits.windows(2)
            .all(|w| w[0].score > w[1].score || (w[0].score == w[1].score && w[0].id < w[1].id)),
        "{ctx}: order"
    );
}

fn row_key(id: &MemoryId) -> Vec<u8> {
    let b = id.to_bytes();
    let ns_len = u16::from_le_bytes([b[0], b[1]]) as usize;
    [&b[2..2 + ns_len], b"\0m", &b[2 + ns_len..]].concat()
}

#[test]
fn every_signal_rank_and_score_matches_its_definition() {
    for seed in [1u64, 2, 3] {
        let f = fixture(seed);
        let mut rng = Rng(seed + 100);
        let index = Index::open(&f.db, "mem_notes_text").unwrap();
        for q in 0..10 {
            let qt = text(&mut rng);
            let qe = emb(&mut rng);
            let now = pigeonhole_memory_now() + 3_600_000_000;
            let s = f.db.snapshot().unwrap();
            let bm25: BTreeMap<Vec<u8>, f64> = index
                .search(Query::new().text(qt.clone()), 1000)
                .id_prefix(b"a\0m".to_vec())
                .snapshot(&s)
                .run()
                .unwrap()
                .into_iter()
                .map(|h| (h.id, h.score))
                .collect();
            let (hits, stats) = f
                .store
                .recall(&["a"])
                .text(qt.clone())
                .embedding(qe.clone())
                .candidates(1000)
                .k(1000)
                .at(now)
                .snapshot(&s)
                .with_signals(true)
                .run_with_stats()
                .unwrap();
            let ctx = format!("seed {seed}, query {q}: {qt:?}");
            // Every memory of `a` is a candidate (recency alone takes all 40); none of `b`.
            assert_eq!(stats.candidates, 40, "{ctx}");
            assert_eq!(hits.len(), 40, "{ctx}");
            assert!(hits.iter().all(|h| h.id.namespace() == "a"), "{ctx}");
            check_rrf(&f, &hits, &qt, &qe, now, &bm25, &ctx);
            // The top k is the full ranking's prefix.
            let top = f
                .store
                .recall(&["a"])
                .text(qt.clone())
                .embedding(qe.clone())
                .candidates(1000)
                .k(5)
                .at(now)
                .snapshot(&s)
                .run()
                .unwrap();
            let ids: Vec<_> = top.iter().map(|h| &h.id).collect();
            let full: Vec<_> = hits.iter().take(5).map(|h| &h.id).collect();
            assert_eq!(ids, full, "{ctx}");
        }
        f.db.close().unwrap();
    }
}

fn pigeonhole_memory_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

#[test]
fn candidates_are_the_union_of_each_signals_top_n() {
    let f = fixture(7);
    let mut rng = Rng(77);
    let index = Index::open(&f.db, "mem_notes_text").unwrap();
    let n = 5;
    for _ in 0..10 {
        let qt = text(&mut rng);
        let qe = emb(&mut rng);
        let s = f.db.snapshot().unwrap();
        let in_a: Vec<(&MemoryId, &Written)> = f
            .mems
            .iter()
            .filter(|(id, _)| id.namespace() == "a")
            .collect();
        // Vector: the n most similar (distinct random embeddings, so no ties in practice).
        let mut by_sim: Vec<_> = in_a
            .iter()
            .map(|(id, m)| (cosine_similarity(&qe, &m.1), (*id).clone()))
            .collect();
        by_sim.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let mut want: BTreeSet<MemoryId> = by_sim.iter().take(n).map(|x| x.1.clone()).collect();
        // Keyword: the index's own top n.
        for h in index
            .search(Query::new().text(qt.clone()), n)
            .id_prefix(b"a\0m".to_vec())
            .snapshot(&s)
            .run()
            .unwrap()
        {
            want.insert(
                in_a.iter()
                    .find(|(id, _)| row_key(id) == h.id)
                    .unwrap()
                    .0
                    .clone(),
            );
        }
        // Recency: the n newest, which sort first by id.
        let mut newest: Vec<&MemoryId> = in_a.iter().map(|(id, _)| *id).collect();
        newest.sort_by_key(|id| std::cmp::Reverse(id.created_us()));
        let mut by_key: Vec<Vec<u8>> = in_a.iter().map(|(id, _)| row_key(id)).collect();
        by_key.sort();
        for k in by_key.iter().take(n) {
            want.insert(
                in_a.iter()
                    .find(|(id, _)| row_key(id) == *k)
                    .unwrap()
                    .0
                    .clone(),
            );
        }
        let (hits, stats) = f
            .store
            .recall(&["a"])
            .text(qt.clone())
            .embedding(qe.clone())
            .candidates(n)
            .k(1000)
            .snapshot(&s)
            .run_with_stats()
            .unwrap();
        let got: BTreeSet<MemoryId> = hits.into_iter().map(|h| h.id).collect();
        assert_eq!(got, want, "{qt:?}");
        assert_eq!(stats.candidates, want.len());
    }
    f.db.close().unwrap();
}

#[test]
fn the_byte_budget_keeps_whole_hits_and_at_least_one() {
    let f = fixture(11);
    let s = f.db.snapshot().unwrap();
    let all = f
        .store
        .recall(&["a"])
        .text("postgres meeting")
        .k(1000)
        .candidates(1000)
        .snapshot(&s)
        .run()
        .unwrap();
    let size = |h: &RecallHit| (h.text.len() + h.id.to_bytes().len() + 16) as u64;
    let three: u64 = all.iter().take(3).map(size).sum();
    let (cut, st) = f
        .store
        .recall(&["a"])
        .text("postgres meeting")
        .k(1000)
        .candidates(1000)
        .snapshot(&s)
        .byte_budget(three)
        .run_with_stats()
        .unwrap();
    assert_eq!(cut, all[..3].to_vec());
    assert!(st.truncated);
    // A budget below the first hit still returns it.
    let (one, st) = f
        .store
        .recall(&["a"])
        .text("postgres meeting")
        .snapshot(&s)
        .candidates(1000)
        .byte_budget(1)
        .run_with_stats()
        .unwrap();
    assert_eq!(one.len(), 1);
    assert!(st.truncated);
    // k stops it first: not truncated.
    let (two, st) = f
        .store
        .recall(&["a"])
        .text("postgres meeting")
        .snapshot(&s)
        .candidates(1000)
        .k(2)
        .byte_budget(1 << 20)
        .run_with_stats()
        .unwrap();
    assert_eq!(two.len(), 2);
    assert!(!st.truncated);
    f.db.close().unwrap();
}

#[test]
fn superseded_memories_and_other_sessions_are_left_out() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("mem.phdb"), Options::default()).unwrap();
    let store = Store::create(&db, "s", StoreOptions::new()).unwrap();
    let old = store
        .remember("a", Memory::new("deploy window tuesday").session("s1"))
        .unwrap();
    let new = store
        .remember("a", Memory::new("deploy window thursday").session("s2"))
        .unwrap();
    store.supersede(old.id(), new.id()).unwrap();
    let ids = |r: Vec<RecallHit>| r.into_iter().map(|h| h.id).collect::<Vec<_>>();
    let hits = ids(store.recall(&["a"]).text("deploy window").run().unwrap());
    assert_eq!(hits, vec![new.id().clone()]);
    let both = ids(store
        .recall(&["a"])
        .text("deploy window")
        .include_superseded(true)
        .run()
        .unwrap());
    assert_eq!(both.len(), 2);
    let s1 = ids(store
        .recall(&["a"])
        .text("deploy")
        .include_superseded(true)
        .session("s1")
        .run()
        .unwrap());
    assert_eq!(s1, vec![old.id().clone()]);
    db.close().unwrap();
}

#[test]
fn the_linear_blend_sums_min_max_normalised_signals() {
    let f = fixture(13);
    let s = f.db.snapshot().unwrap();
    let mut w = Weights::default();
    w.recency = 0.0;
    w.use_ = 0.0;
    let hits = f
        .store
        .recall(&["a"])
        .text("postgres alice")
        .embedding(vec![1.0, 0.0, 0.5, 0.0])
        .candidates(1000)
        .k(1000)
        .snapshot(&s)
        .fusion(Fusion::Linear)
        .weights(w)
        .with_signals(true)
        .run()
        .unwrap();
    let norm = |get: fn(&pigeonhole_memory::Signals) -> Option<Signal>| {
        let vals: Vec<f64> = hits
            .iter()
            .filter_map(|h| get(h.signals.as_ref().unwrap()).map(|s| s.value))
            .collect();
        let (lo, hi) = vals
            .iter()
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(l, h), v| {
                (l.min(*v), h.max(*v))
            });
        move |v: f64| if hi > lo { (v - lo) / (hi - lo) } else { 1.0 }
    };
    let (nv, nk, ni) = (
        norm(|s| s.vector),
        norm(|s| s.keyword),
        norm(|s| s.importance),
    );
    for h in &hits {
        let s = h.signals.unwrap();
        let want = s.vector.map_or(0.0, |x| nv(x.value))
            + s.keyword.map_or(0.0, |x| nk(x.value))
            + s.importance.map_or(0.0, |x| ni(x.value));
        assert!((h.score - want).abs() < 1e-9, "{} vs {want}", h.score);
    }
    f.db.close().unwrap();
}

#[test]
fn several_namespaces_fuse_together_and_bad_calls_are_refused() {
    let f = fixture(17);
    let hits = f
        .store
        .recall(&["a", "b"])
        .embedding(vec![0.5, 0.5, 0.5, 0.5])
        .candidates(1000)
        .k(1000)
        .run()
        .unwrap();
    assert_eq!(hits.len(), 50);
    let nss: BTreeSet<&str> = hits.iter().map(|h| h.id.namespace()).collect();
    assert_eq!(nss, BTreeSet::from(["a", "b"]));
    // Neither text nor embedding; a wrong dimension; no namespace; a bad one.
    for r in [
        f.store.recall(&["a"]).run(),
        f.store.recall(&["a"]).embedding(vec![1.0]).run(),
        f.store.recall(&[]).text("x").run(),
        f.store.recall(&["a\0"]).text("x").run(),
    ] {
        assert!(matches!(r, Err(Error::InvalidArgument(_))), "{r:?}");
    }
    // A handle without the model recalls by keyword, and refuses an embedding.
    let bare = Store::open_without_embeddings(&f.db, "notes").unwrap();
    assert!(
        !bare
            .recall(&["a"])
            .text("postgres")
            .run()
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        bare.recall(&["a"])
            .embedding(vec![1.0, 0.0, 0.0, 0.0])
            .run(),
        Err(Error::InvalidArgument(_))
    ));
    f.db.close().unwrap();
}

#[test]
fn a_reader_process_recalls_what_the_writer_committed() {
    let f = fixture(19);
    let reader =
        Pigeonhole::open_reader(f._dir.join("mem.phdb"), ReaderOptions::default()).unwrap();
    assert!(matches!(
        Store::open_reader(&reader, "notes", "another-model"),
        Err(Error::Mismatch(_))
    ));
    let rs = Store::open_reader(&reader, "notes", MODEL).unwrap();
    let now = pigeonhole_memory_now();
    let q = |r: pigeonhole_memory::Recall<'_>| {
        r.text("postgres deploy")
            .embedding(vec![0.2, -0.4, 0.9, 0.1])
            .candidates(1000)
            .k(20)
            .at(now)
            .run()
            .unwrap()
    };
    let from_reader = q(rs.recall(&["a", "b"]));
    let from_writer = q(f.store.recall(&["a", "b"]));
    assert_eq!(from_reader, from_writer);
    let id = &from_reader[0].id;
    assert_eq!(rs.get(id).unwrap().unwrap().text, from_reader[0].text);
    assert!(rs.verify().unwrap().is_consistent());
    drop(rs);
    drop(reader);
    f.db.close().unwrap();
}

#[test]
fn equal_values_share_a_rank_so_ties_favour_no_one() {
    // Two memories at the default importance: neither gets a better importance rank. The
    // query matches the older one by embedding and keyword; the newer one wins only on
    // recency, so the older comes first. (With the old tie-break by id, the newer one also
    // took importance rank 1 and the two tied.)
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("mem.phdb"), Options::default()).unwrap();
    let model = EmbeddingModel::new(MODEL, DIM as u32, Element::F32).unwrap();
    let store = Store::create(&db, "t", StoreOptions::new().embedding(model)).unwrap();
    let a = store
        .remember(
            "a",
            Memory::new("we chose postgres").embedding(vec![1.0, 0.0, 0.0, 0.0]),
        )
        .unwrap();
    store
        .remember(
            "a",
            Memory::new("lunch was good").embedding(vec![0.0, 1.0, 0.0, 0.0]),
        )
        .unwrap();
    let hits = store
        .recall(&["a"])
        .text("postgres")
        .embedding(vec![1.0, 0.1, 0.0, 0.0])
        .with_signals(true)
        .run()
        .unwrap();
    assert_eq!(&hits[0].id, a.id());
    let ranks: Vec<u32> = hits
        .iter()
        .map(|h| h.signals.unwrap().importance.unwrap().rank)
        .collect();
    assert_eq!(ranks, [1, 1]);
    db.close().unwrap();
}

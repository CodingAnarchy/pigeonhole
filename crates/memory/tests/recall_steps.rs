//! #1915 (ICR 0139): a recall in steps over namespaces split between stores equals one
//! recall over them all. Each "store" here is a recall over some of one store's namespaces,
//! which is what a cluster partition holds (whole namespaces, nothing else). So the whole
//! protocol is checked exactly: the pools gathered apart and merged, the ranking, the graph
//! walk hop by hop with every hop split by store, and the bodies. Every step goes through its
//! byte form, as it would between nodes. Both fusions, with and without keywords, the
//! filters, an expansion whose `max` and byte budget bind across the stores, and the byte
//! budget's cut of the hits. Also the steps' refusals and the byte forms' versions.

use pigeonhole::{Element, Options, Pigeonhole, Snapshot};
use pigeonhole_memory::{
    EmbeddingModel, Error, Expand, Fusion, LinkType, Memory, MemoryId, Recall, RecallBodies,
    RecallHit, RecallHop, RecallHopPart, RecallPool, RecallStats, Store, StoreOptions, Weights,
};

const MODEL: &str = "test-embed-v1";
const DIM: usize = 4;

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

    fn emb(&mut self) -> Vec<f32> {
        let mut v: Vec<f32> = (0..DIM)
            .map(|_| (self.below(2001) as f32 - 1000.0) / 1000.0)
            .collect();
        if v.iter().all(|x| *x == 0.0) {
            v[0] = 1.0;
        }
        v
    }
}

const VOCAB: &[&str] = &[
    "postgres", "meeting", "deploy", "tuesday", "alice", "bob", "index", "memory", "agent",
    "vector", "search", "budget",
];

/// The namespaces and how many memories each holds.
const NAMESPACES: [(&str, u64); 3] = [("a", 40), ("b", 30), ("c", 10)];
/// The stores the namespaces are split between.
const STORES: [&[&str]; 2] = [&["a"], &["b", "c"]];

/// A store over [`NAMESPACES`], with random links (a hub in each), sessions, importances
/// and some superseded memories.
fn random_store(seed: u64) -> (pigeonhole::doc_support::TempDir, Pigeonhole, Store) {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("mem.phdb"), Options::default().shards(2)).unwrap();
    let model = EmbeddingModel::new(MODEL, DIM as u32, Element::F32).unwrap();
    let store = Store::create(&db, "notes", StoreOptions::new().embedding(model)).unwrap();
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let ty = LinkType::custom(16).unwrap();
    for (ns, n) in NAMESPACES {
        let mut ids = Vec::new();
        for i in 0..n {
            let words = 2 + rng.below(4);
            let t: Vec<&str> = (0..words)
                .map(|_| VOCAB[rng.below(VOCAB.len() as u64) as usize])
                .collect();
            let m = Memory::new(format!("{} {ns}{i}", t.join(" ")))
                .embedding(rng.emb())
                .importance(rng.below(5) as f64 / 4.0)
                .session(if rng.below(3) == 0 { "s2" } else { "s1" });
            ids.push(store.remember(ns, m).unwrap().id().clone());
        }
        for to in ids.iter().skip(1).step_by(2) {
            let w = (1 + rng.below(1000)) as f32 / 1000.0;
            store.link(&ids[0], to, ty, w).unwrap();
        }
        for from in &ids {
            for _ in 0..rng.below(4) {
                let to = &ids[rng.below(n) as usize];
                let w = (1 + rng.below(1000)) as f32 / 1000.0;
                store.link(from, to, ty, w).unwrap();
            }
        }
        for _ in 0..n / 8 {
            let old = &ids[rng.below(n) as usize];
            let new = &ids[rng.below(n) as usize];
            if old != new && store.get(old).unwrap().unwrap().superseded_by.is_none() {
                let _ = store.supersede(old, new);
            }
        }
    }
    (dir, db, store)
}

/// One recall's arguments.
#[derive(Clone, Debug)]
struct Case {
    text: Option<&'static str>,
    embedding: Vec<f32>,
    k: usize,
    fusion: Fusion,
    budget: Option<u64>,
    expand: Option<Expand>,
    session: Option<&'static str>,
    include_superseded: bool,
    with_signals: bool,
    with_embeddings: bool,
}

impl Case {
    fn apply<'a>(&self, r: Recall<'a>, s: &'a Snapshot, at: u64) -> Recall<'a> {
        let mut r = r
            .embedding(self.embedding.clone())
            .k(self.k)
            .candidates(8)
            .snapshot(s)
            .now_us(at)
            .fusion(self.fusion)
            .weights(Weights::default())
            .include_superseded(self.include_superseded)
            .with_signals(self.with_signals)
            .with_embeddings(self.with_embeddings);
        if let Some(t) = self.text {
            r = r.text(t);
        }
        if let Some(b) = self.budget {
            r = r.byte_budget(b);
        }
        if let Some(x) = self.expand {
            r = r.expand(x);
        }
        if let Some(sess) = self.session {
            r = r.session(sess);
        }
        r
    }
}

/// The store holding namespace `ns`.
fn store_of(ns: &str) -> usize {
    STORES.iter().position(|s| s.contains(&ns)).unwrap()
}

/// The recall in steps over [`STORES`], each step through its byte form.
fn in_steps(store: &Store, s: &Snapshot, at: u64, c: &Case) -> (Vec<RecallHit>, RecallStats) {
    let part = |i: usize| c.apply(store.recall(STORES[i]), s, at);
    let pools = (0..STORES.len()).map(|i| {
        let p = part(i).gather().unwrap();
        RecallPool::from_bytes(&p.to_bytes()).unwrap()
    });
    let mut ranking = RecallPool::merge(pools)
        .unwrap()
        .rank(c.fusion, Weights::default());
    if let Some(x) = c.expand {
        let mut walk = ranking.walk(x, c.budget);
        while let Some(hop) = walk.next_hop() {
            let mut parts = Vec::new();
            for (i, share) in hop.split(store_of) {
                let share = RecallHop::from_bytes(&share.to_bytes()).unwrap();
                let p = part(i).hop(&share).unwrap();
                parts.push(RecallHopPart::from_bytes(&p.to_bytes()).unwrap());
            }
            walk.settle(parts).unwrap();
        }
        ranking = walk.finish();
    }
    // k + 4 bodies, as the entry node asks for (simp724's nit c).
    let top = ranking.top(c.k + 4);
    let bodies = (0..STORES.len()).map(|i| {
        let ids: Vec<MemoryId> = top
            .iter()
            .filter(|id| store_of(id.namespace()) == i)
            .cloned()
            .collect();
        let b = part(i).bodies(&ids).unwrap();
        RecallBodies::from_bytes(&b.to_bytes()).unwrap()
    });
    ranking.finish(RecallBodies::merge(bodies), c.k, c.budget, c.with_signals)
}

/// Random cases for a store: every combination the steps must keep.
fn cases(rng: &mut Rng) -> Vec<Case> {
    let mut out = Vec::new();
    for fusion in [Fusion::Rrf, Fusion::Linear] {
        for text in [None, Some("postgres deploy")] {
            for expand in [
                None,
                Some(Expand::hops(1).max(1)),
                Some(Expand::hops(1).max(6)),
                Some(Expand::hops(2).max(15)),
                Some(Expand::hops(3)),
                Some(Expand::hops(4).max(40)),
            ] {
                for budget in [None, Some(300), Some(900), Some(2500), Some(6000)] {
                    out.push(Case {
                        text,
                        embedding: rng.emb(),
                        k: 1 + rng.below(25) as usize,
                        fusion,
                        budget,
                        expand,
                        session: (rng.below(4) == 0).then_some("s1"),
                        include_superseded: rng.below(3) == 0,
                        with_signals: rng.below(2) == 0,
                        with_embeddings: rng.below(4) == 0,
                    });
                }
            }
        }
    }
    out
}

#[test]
fn a_recall_in_steps_over_split_namespaces_is_the_one_recall() {
    // How often the expansion's bounds bound with seeds in both stores: the cases that
    // a walk per store would get wrong.
    let (mut max_across, mut budget_across) = (0, 0);
    for seed in 1..=4u64 {
        let (_dir, db, store) = random_store(seed);
        let s = db.snapshot().unwrap();
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let mut rng = Rng(seed);
        for c in cases(&mut rng) {
            let whole = c
                .apply(store.recall(&["a", "b", "c"]), &s, at)
                .run_with_stats()
                .unwrap();
            let steps = in_steps(&store, &s, at, &c);
            assert_eq!(steps, whole, "seed {seed}: {c:?}");
            let (hits, stats) = &whole;
            let graphed: std::collections::BTreeSet<usize> = hits
                .iter()
                .filter(|h| h.signals.is_some_and(|g| g.graph.is_some()))
                .map(|h| store_of(h.id.namespace()))
                .collect();
            if stats.expansion_truncated && graphed.len() == 2 {
                match c.expand {
                    Some(x) if stats.expanded == x.max => max_across += 1,
                    _ => budget_across += 1,
                }
            }
        }
    }
    assert!(max_across > 0, "no case had max bind across the stores");
    assert!(
        budget_across > 0,
        "no case had the budget bind across the stores"
    );
}

#[test]
fn the_steps_refuse_what_isnt_theirs() {
    let (_dir, db, store) = random_store(7);
    let s = db.snapshot().unwrap();
    let q = Rng(7).emb();
    let r = || store.recall(&["a"]).embedding(q.clone()).snapshot(&s);
    // Bodies of another namespace's memory.
    let b = store
        .recall(&["b"])
        .embedding(q.clone())
        .snapshot(&s)
        .gather()
        .unwrap()
        .rank(Fusion::Rrf, Weights::default())
        .top(1);
    assert!(matches!(r().bodies(&b), Err(Error::InvalidArgument(_))));
    // A hop from another namespace's memory.
    let mut walk = store
        .recall(&["b"])
        .embedding(q.clone())
        .snapshot(&s)
        .gather()
        .unwrap()
        .rank(Fusion::Rrf, Weights::default())
        .walk(Expand::hops(1), None);
    let hop = walk.next_hop().unwrap();
    assert!(matches!(r().hop(&hop), Err(Error::InvalidArgument(_))));
    walk.settle([]).unwrap();
    // A namespace gathered twice.
    let twice = RecallPool::merge([r().gather().unwrap(), r().gather().unwrap()]);
    assert!(matches!(twice, Err(Error::InvalidArgument(_))));
}

#[test]
fn the_byte_forms_refuse_another_version_and_malformed_bytes() {
    let (_dir, db, store) = random_store(8);
    let s = db.snapshot().unwrap();
    let q = Rng(8).emb();
    let r = || store.recall(&["a", "b"]).embedding(q.clone()).snapshot(&s);
    let ranking = r().gather().unwrap().rank(Fusion::Rrf, Weights::default());
    let pool = r().gather().unwrap().to_bytes();
    let bodies = r().bodies(&ranking.top(5)).unwrap().to_bytes();
    let walk = ranking.walk(Expand::hops(1), Some(1000));
    let hop = walk.next_hop().unwrap();
    let part = r().hop(&hop).unwrap().to_bytes();
    let hop = hop.to_bytes();
    type Check = fn(&[u8]) -> bool;
    let forms: [(&str, &[u8], Check); 4] = [
        ("pool", &pool, |b| RecallPool::from_bytes(b).is_ok()),
        ("bodies", &bodies, |b| RecallBodies::from_bytes(b).is_ok()),
        ("hop", &hop, |b| RecallHop::from_bytes(b).is_ok()),
        ("part", &part, |b| RecallHopPart::from_bytes(b).is_ok()),
    ];
    for (what, bytes, ok) in forms {
        assert!(ok(bytes), "{what}");
        // Another version.
        let mut v = bytes.to_vec();
        v[0] = 2;
        assert!(!ok(&v), "{what} of version 2");
        // A byte more, and every truncation.
        let mut more = bytes.to_vec();
        more.push(0);
        assert!(!ok(&more), "{what} with a byte more");
        for n in 0..bytes.len() {
            assert!(!ok(&bytes[..n]), "{what} cut at {n}");
        }
    }
    let msg = RecallPool::from_bytes(&[9]).unwrap_err().to_string();
    assert!(msg.contains("version 9"), "{msg}");
}

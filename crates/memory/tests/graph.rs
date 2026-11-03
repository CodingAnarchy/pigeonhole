//! D311 T4 (#1338): graph expansion in recall. Every graph value against a brute-force
//! reference that follows both link directions over the stored links at a snapshot (no cut,
//! a `max` cut, budget cuts, the filters), the budget at its boundary, `max` keeping the
//! best-valued, each filter on a bridge, the signal's absence without `expand` and for
//! seeds, the argument errors, and a reader process.

use std::collections::{BTreeMap, BTreeSet};

use pigeonhole::{Element, Options, Pigeonhole, ReaderOptions};
use pigeonhole_memory::{
    EmbeddingModel, Error, Expand, LinkType, Memory, MemoryId, RecallHit, RecallStats, Store,
    StoreOptions, StoredMemory, Weights,
};

const MODEL: &str = "test-embed-v1";
const DIM: usize = 4;
/// D308's per-node overhead, which the expansion's budget counts.
const NODE_OVERHEAD: u64 = 16;

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
    "vector", "search", "budget",
];

fn emb(rng: &mut Rng) -> Vec<f32> {
    let mut v: Vec<f32> = (0..DIM).map(|_| rng.unit()).collect();
    if v.iter().all(|x| *x == 0.0) {
        v[0] = 1.0;
    }
    v
}

fn row_key(id: &MemoryId) -> Vec<u8> {
    let b = id.to_bytes();
    let ns_len = u16::from_le_bytes([b[0], b[1]]) as usize;
    [&b[2..2 + ns_len], b"\0m", &b[2 + ns_len..]].concat()
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

fn open(dir: &pigeonhole::doc_support::TempDir) -> (Pigeonhole, Store) {
    let db = Pigeonhole::open(dir.join("mem.phdb"), Options::default().shards(2)).unwrap();
    let model = EmbeddingModel::new(MODEL, DIM as u32, Element::F32).unwrap();
    let store = Store::create(&db, "notes", StoreOptions::new().embedding(model)).unwrap();
    (db, store)
}

fn ty() -> LinkType {
    LinkType::custom(16).unwrap()
}

/// A store of two namespaces with random links (a hub among them), some superseded
/// memories and two sessions.
fn random_store(seed: u64) -> (pigeonhole::doc_support::TempDir, Pigeonhole, Store) {
    let dir = pigeonhole::doc_support::temp_dir();
    let (db, store) = open(&dir);
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    for (ns, n) in [("a", 60u64), ("b", 12)] {
        let mut ids = Vec::new();
        for i in 0..n {
            let words = 2 + rng.below(4);
            let t: Vec<&str> = (0..words)
                .map(|_| VOCAB[rng.below(VOCAB.len() as u64) as usize])
                .collect();
            let m = Memory::new(format!("{} {ns}{i}", t.join(" ")))
                .embedding(emb(&mut rng))
                .session(if rng.below(3) == 0 { "s2" } else { "s1" });
            ids.push(store.remember(ns, m).unwrap().id().clone());
        }
        // The hub: the first memory links to half the namespace.
        for to in ids.iter().skip(1).step_by(2) {
            let w = (1 + rng.below(1000)) as f32 / 1000.0;
            store.link(&ids[0], to, ty(), w).unwrap();
        }
        for from in &ids {
            for _ in 0..rng.below(4) {
                let to = &ids[rng.below(n) as usize];
                let w = (1 + rng.below(1000)) as f32 / 1000.0;
                store.link(from, to, ty(), w).unwrap();
            }
        }
        for _ in 0..n / 8 {
            let old = &ids[rng.below(n) as usize];
            let new = &ids[rng.below(n) as usize];
            if old != new && store.get(old).unwrap().unwrap().superseded_by.is_none() {
                // Only when it doesn't refuse (a chain's other end, say).
                let _ = store.supersede(old, new);
            }
        }
    }
    (dir, db, store)
}

/// What the recall's filters let through.
#[derive(Clone, Copy)]
struct Filters<'a> {
    include_superseded: bool,
    session: Option<&'a str>,
}

impl Filters<'_> {
    fn pass(&self, m: &StoredMemory) -> bool {
        (self.include_superseded || m.superseded_by.is_none())
            && self.session.is_none_or(|s| m.session.as_deref() == Some(s))
    }
}

/// The graph values an expansion gives, and whether it was cut: breadth-first from `seeds`
/// over both directions of the stored links, valued, filtered, bounded and cut as
/// [`Expand`] documents.
fn reference(
    store: &Store,
    seeds: &[MemoryId],
    x: (u32, usize),
    budget: Option<u64>,
    f: Filters<'_>,
) -> (BTreeMap<MemoryId, f64>, bool) {
    let (hops, max) = x;
    let mut out = BTreeMap::new();
    let mut visited: BTreeSet<MemoryId> = seeds.iter().cloned().collect();
    let mut level: Vec<(MemoryId, f64)> = seeds
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), 1.0 / (i + 1) as f64))
        .collect();
    let mut left = budget;
    for _ in 0..hops {
        if out.len() == max {
            break;
        }
        let mut reached: BTreeMap<Vec<u8>, (MemoryId, f64)> = BTreeMap::new();
        let mut bytes = 0u64;
        for (q, vq) in &level {
            let m = store.get(q).unwrap().expect("a walked memory exists");
            for l in m.links.iter().chain(&m.backlinks) {
                if l.id == *q {
                    continue; // a traversal from q never returns q
                }
                bytes += row_key(&l.id).len() as u64 + NODE_OVERHEAD;
                if visited.contains(&l.id) {
                    continue;
                }
                let Some(to) = store.get(&l.id).unwrap() else {
                    continue;
                };
                if !f.pass(&to) {
                    continue;
                }
                let v = vq * f64::from(l.weight);
                let e = reached.entry(row_key(&l.id)).or_insert((l.id.clone(), v));
                if v > e.1 {
                    e.1 = v;
                }
            }
        }
        if let Some(b) = left.as_mut() {
            if bytes > *b {
                return (out, true);
            }
            *b -= bytes;
        }
        let mut l: Vec<(Vec<u8>, MemoryId, f64)> =
            reached.into_iter().map(|(k, (id, v))| (k, id, v)).collect();
        l.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
        let room = max - out.len();
        let cut = l.len() > room;
        l.truncate(room);
        for (_, id, v) in &l {
            out.insert(id.clone(), *v);
            visited.insert(id.clone());
        }
        if cut {
            return (out, true);
        }
        if l.is_empty() {
            break;
        }
        level = l.into_iter().map(|(_, id, v)| (id, v)).collect();
    }
    (out, false)
}

/// Every hit's graph signal against the reference, its rank against the values, and (with
/// every hit returned) the set of memories with one and the fused score with the graph's
/// weight.
fn check(
    hits: &[RecallHit],
    stats: &RecallStats,
    want: &(BTreeMap<MemoryId, f64>, bool),
    all: bool,
    ctx: &str,
) {
    assert_eq!(stats.expanded, want.0.len(), "{ctx}: expanded");
    assert_eq!(stats.expansion_truncated, want.1, "{ctx}: truncated");
    for h in hits {
        let g = h.signals.unwrap().graph.map(|s| s.value);
        assert_eq!(g, want.0.get(&h.id).copied(), "{ctx}: {}", h.id);
    }
    if !all {
        return;
    }
    let got: BTreeSet<&MemoryId> = hits
        .iter()
        .filter(|h| h.signals.unwrap().graph.is_some())
        .map(|h| &h.id)
        .collect();
    assert_eq!(
        got,
        want.0.keys().collect(),
        "{ctx}: who has a graph signal"
    );
    let graphed: Vec<f64> = hits
        .iter()
        .filter_map(|h| h.signals.unwrap().graph.map(|s| s.value))
        .collect();
    for h in hits {
        let s = h.signals.unwrap();
        if let Some(g) = s.graph {
            let above = graphed.iter().filter(|v| **v > g.value).count();
            assert_eq!(g.rank as usize, above + 1, "{ctx}: graph rank");
        }
        let w = Weights::default();
        let want: f64 = [
            (s.vector, w.vector),
            (s.keyword, w.keyword),
            (s.recency, w.recency),
            (s.importance, w.importance),
            (s.uses, w.uses),
            (s.graph, w.graph),
        ]
        .iter()
        .filter_map(|(x, w)| x.map(|x| w / (60.0 + f64::from(x.rank))))
        .sum();
        assert!(
            (h.score - want).abs() <= 1e-12,
            "{ctx}: score {} vs {want}",
            h.score
        );
    }
}

fn configure<'a>(
    r: pigeonhole_memory::Recall<'a>,
    q: &[f32],
    s: &'a pigeonhole::Snapshot,
    at: u64,
    f: Filters<'_>,
) -> pigeonhole_memory::Recall<'a> {
    let mut r = r
        .text("deploy memory")
        .embedding(q.to_vec())
        .candidates(5)
        .k(1000)
        .snapshot(s)
        .now_us(at)
        .include_superseded(f.include_superseded)
        .with_signals(true);
    if let Some(sess) = f.session {
        r = r.session(sess);
    }
    r
}

#[test]
fn every_graph_value_matches_a_brute_force_walk_of_both_link_directions() {
    for seed in [1u64, 2, 3, 4] {
        let (_dir, db, store) = random_store(seed);
        let s = db.snapshot().unwrap();
        let at = now();
        let mut rng = Rng(seed | 1);
        let q = emb(&mut rng);
        let (mut beyond, mut truncated) = (false, false);
        let variants: [(u32, usize, Option<u64>, Filters<'_>); 8] = [
            (
                1,
                1000,
                None,
                Filters {
                    include_superseded: false,
                    session: None,
                },
            ),
            (
                2,
                1000,
                None,
                Filters {
                    include_superseded: false,
                    session: None,
                },
            ),
            (
                4,
                1000,
                None,
                Filters {
                    include_superseded: true,
                    session: None,
                },
            ),
            (
                3,
                7,
                None,
                Filters {
                    include_superseded: false,
                    session: None,
                },
            ),
            (
                2,
                1000,
                None,
                Filters {
                    include_superseded: false,
                    session: Some("s1"),
                },
            ),
            (
                2,
                1000,
                Some(400),
                Filters {
                    include_superseded: false,
                    session: None,
                },
            ),
            (
                3,
                1000,
                Some(2_000),
                Filters {
                    include_superseded: true,
                    session: None,
                },
            ),
            (
                2,
                1000,
                Some(1 << 20),
                Filters {
                    include_superseded: false,
                    session: None,
                },
            ),
        ];
        for (hops, max, budget, f) in variants {
            let ctx = format!("seed {seed}, {hops} hops, max {max}, budget {budget:?}");
            let plain = configure(store.recall(&["a", "b"]), &q, &s, at, f)
                .run()
                .unwrap();
            let seeds: Vec<MemoryId> = plain.iter().take(10).map(|h| h.id.clone()).collect();
            let want = reference(&store, &seeds, (hops, max), budget, f);
            let mut r = configure(store.recall(&["a", "b"]), &q, &s, at, f)
                .expand(Expand::hops(hops).max(max));
            if let Some(b) = budget {
                r = r.byte_budget(b);
            }
            let (hits, stats) = r.run_with_stats().unwrap();
            check(&hits, &stats, &want, budget.is_none(), &ctx);
            beyond |= want.0.keys().any(|id| plain.iter().all(|h| h.id != *id));
            truncated |= want.1;
            if budget.is_none() {
                // The candidates: the plain recall's, and the memories the walk reached.
                let mut ids: BTreeSet<&MemoryId> = plain.iter().map(|h| &h.id).collect();
                ids.extend(want.0.keys());
                assert_eq!(
                    hits.iter().map(|h| &h.id).collect::<BTreeSet<_>>(),
                    ids,
                    "{ctx}: candidates"
                );
                for h in hits.iter().take(10) {
                    if seeds.contains(&h.id) {
                        assert!(h.signals.unwrap().graph.is_none(), "{ctx}: a seed");
                    }
                }
                assert!(seeds.iter().all(|id| !want.0.contains_key(id)), "{ctx}");
            }
        }
        assert!(
            beyond,
            "seed {seed}: no walk reached past the plain candidates"
        );
        assert!(truncated, "seed {seed}: no variant was cut");
        drop(s);
        db.close().unwrap();
    }
}

/// `A` (the only seed) links to `B`, which links to `C`, among fillers nothing links to.
struct Chain {
    _dir: pigeonhole::doc_support::TempDir,
    db: Pigeonhole,
    store: Store,
    a: MemoryId,
    b: MemoryId,
    c: MemoryId,
    q: Vec<f32>,
}

fn chain(b_session: &str) -> Chain {
    let dir = pigeonhole::doc_support::temp_dir();
    let (db, store) = open(&dir);
    let r = |t: &str, e: [f32; 4], sess: &str| {
        store
            .remember("a", Memory::new(t).embedding(e.to_vec()).session(sess))
            .unwrap()
            .id()
            .clone()
    };
    let c = r("the cache is warm", [0.0, 0.0, 1.0, 0.0], "s");
    let b = r("the bridge memory", [0.0, 0.0, 0.0, 1.0], b_session);
    for i in 0..15 {
        r(&format!("filler {i}"), [0.0, -1.0, 0.0, 0.0], "s");
    }
    let a = r("the seed memory", [1.0, 0.0, 0.0, 0.0], "s");
    store.link(&a, &b, ty(), 0.8).unwrap();
    store.link(&b, &c, ty(), 0.5).unwrap();
    Chain {
        _dir: dir,
        db,
        store,
        a,
        b,
        c,
        q: vec![1.0, 0.0, 0.0, 0.0],
    }
}

fn graph_of(hits: &[RecallHit], id: &MemoryId) -> Option<f64> {
    hits.iter()
        .find(|h| h.id == *id)
        .and_then(|h| h.signals.unwrap().graph.map(|s| s.value))
}

#[test]
fn the_walk_passes_only_through_memories_the_recall_could_return() {
    let run = |ch: &Chain, include: bool, session: Option<&str>| {
        let mut r = ch
            .store
            .recall(&["a"])
            .embedding(ch.q.clone())
            .candidates(1)
            .k(100)
            .include_superseded(include)
            .with_signals(true)
            .expand(Expand::hops(2));
        if let Some(s) = session {
            r = r.session(s);
        }
        r.run().unwrap()
    };
    // Both ends of the bridge, both directions: A → B by A's links, B → C by B's.
    let ch = chain("s");
    let hits = run(&ch, false, None);
    let seed = hits
        .iter()
        .find(|h| h.id == ch.a)
        .expect("the seed is a hit");
    assert!(seed.signals.unwrap().graph.is_none());
    assert_eq!(graph_of(&hits, &ch.b), Some(f64::from(0.8f32)));
    assert_eq!(
        graph_of(&hits, &ch.c),
        Some(f64::from(0.8f32) * f64::from(0.5f32))
    );
    ch.db.close().unwrap();

    // The bridge in another session.
    let ch = chain("t");
    let hits = run(&ch, false, Some("s"));
    assert!(
        hits.iter().all(|h| h.id != ch.b && h.id != ch.c),
        "{hits:?}"
    );
    let hits = run(&ch, false, None);
    assert!(graph_of(&hits, &ch.c).is_some());
    ch.db.close().unwrap();

    // The bridge superseded (by a new memory, which then is the newest and a seed too: its
    // link to B is the supersede's own).
    let ch = chain("s");
    let x = ch
        .store
        .remember(
            "a",
            Memory::new("the bridge, revised").embedding(vec![0.0, 0.0, 0.0, 1.0]),
        )
        .unwrap()
        .id()
        .clone();
    ch.store.supersede(&ch.b, &x).unwrap();
    let hits = run(&ch, false, None);
    assert!(
        hits.iter().all(|h| h.id != ch.b && h.id != ch.c),
        "{hits:?}"
    );
    let hits = run(&ch, true, None);
    assert!(graph_of(&hits, &ch.b).is_some());
    assert!(graph_of(&hits, &ch.c).is_some());
    ch.db.close().unwrap();

    // Following a backlink: from C as the seed, B and then A.
    let ch = chain("s");
    let hits = ch
        .store
        .recall(&["a"])
        .embedding(vec![0.0, 0.0, 1.0, 0.0])
        .candidates(1)
        .k(100)
        .with_signals(true)
        .expand(Expand::hops(2))
        .run()
        .unwrap();
    assert!(graph_of(&hits, &ch.b).is_some());
    assert!(graph_of(&hits, &ch.a).is_none(), "A is a seed (the newest)");
    ch.db.close().unwrap();
}

#[test]
fn max_keeps_the_best_valued_neighbours_not_the_newest() {
    let dir = pigeonhole::doc_support::temp_dir();
    let (db, store) = open(&dir);
    let mut ns = Vec::new();
    // Created oldest first, so the newest (first in key order) has the lowest weight.
    for i in 0..5 {
        ns.push(
            store
                .remember(
                    "a",
                    Memory::new(format!("neighbour {i}")).embedding(vec![0.0, 1.0, 0.0, 0.0]),
                )
                .unwrap()
                .id()
                .clone(),
        );
    }
    let a = store
        .remember(
            "a",
            Memory::new("the seed").embedding(vec![1.0, 0.0, 0.0, 0.0]),
        )
        .unwrap()
        .id()
        .clone();
    for (i, n) in ns.iter().enumerate() {
        store.link(&a, n, ty(), 0.5 - 0.1 * i as f32).unwrap();
    }
    let (hits, stats) = store
        .recall(&["a"])
        .embedding(vec![1.0, 0.0, 0.0, 0.0])
        .candidates(1)
        .k(100)
        .with_signals(true)
        .expand(Expand::hops(1).max(2))
        .run_with_stats()
        .unwrap();
    assert_eq!(stats.expanded, 2);
    assert!(stats.expansion_truncated);
    let got: BTreeSet<&MemoryId> = hits
        .iter()
        .filter(|h| h.signals.unwrap().graph.is_some())
        .map(|h| &h.id)
        .collect();
    assert_eq!(got, [&ns[0], &ns[1]].into_iter().collect());
    db.close().unwrap();
}

#[test]
fn a_hop_the_budget_cant_hold_whole_is_dropped() {
    let ch = chain("s");
    let d = ch
        .store
        .remember(
            "a",
            Memory::new("two hops on").embedding(vec![0.0, -1.0, 0.0, 0.0]),
        )
        .unwrap()
        .id()
        .clone();
    // `d` is now the newest, so with one candidate per signal the seeds are A (vector) and
    // D (recency), which has no links.
    let node = |id: &MemoryId| row_key(id).len() as u64 + NODE_OVERHEAD;
    // Hop 1 reads A's link to B (D has none, and no backlinks reach A or D); hop 2 reads B's
    // link to C and its backlink to A.
    let hop1 = node(&ch.b);
    let hop2 = node(&ch.c) + node(&ch.a);
    let run = |budget: u64| {
        ch.store
            .recall(&["a"])
            .embedding(ch.q.clone())
            .candidates(1)
            .k(100)
            .byte_budget(budget)
            .expand(Expand::hops(2))
            .run_with_stats()
            .unwrap()
            .1
    };
    let st = run(hop1 + hop2);
    assert_eq!((st.expanded, st.expansion_truncated), (2, false));
    let st = run(hop1 + hop2 - 1);
    assert_eq!((st.expanded, st.expansion_truncated), (1, true));
    let st = run(hop1);
    assert_eq!((st.expanded, st.expansion_truncated), (1, true));
    let st = run(hop1 - 1);
    assert_eq!((st.expanded, st.expansion_truncated), (0, true));
    assert!(!row_key(&d).is_empty());
    ch.db.close().unwrap();
}

#[test]
fn without_expand_there_is_no_graph_signal_and_its_weight_does_nothing() {
    let (_dir, db, store) = random_store(9);
    let s = db.snapshot().unwrap();
    let at = now();
    let run = |w: f64, x: Option<Expand>| {
        let mut weights = Weights::default();
        weights.graph = w;
        let mut r = store
            .recall(&["a"])
            .text("deploy")
            .embedding(vec![0.3, 0.1, -0.2, 0.9])
            .candidates(5)
            .k(100)
            .snapshot(&s)
            .now_us(at)
            .weights(weights)
            .with_signals(true);
        if let Some(x) = x {
            r = r.expand(x);
        }
        r.run_with_stats().unwrap()
    };
    let (a, sa) = run(0.5, None);
    let (b, _) = run(7.0, None);
    assert_eq!(a, b);
    assert!(a.iter().all(|h| h.signals.unwrap().graph.is_none()));
    assert_eq!((sa.expanded, sa.expansion_truncated), (0, false));
    let (c, sc) = run(0.5, Some(Expand::hops(1)));
    assert!(sc.expanded > 0, "the hub fixture reaches something");
    assert!(sc.candidates >= sa.candidates);
    for h in a.iter().take(10) {
        let e = c.iter().find(|x| x.id == h.id).unwrap();
        assert!(
            e.signals.unwrap().graph.is_none(),
            "a seed has no graph signal"
        );
    }
    for hops in [0, 5] {
        assert!(matches!(
            store
                .recall(&["a"])
                .text("deploy")
                .expand(Expand::hops(hops))
                .run(),
            Err(Error::InvalidArgument(_))
        ));
    }
    drop(s);
    db.close().unwrap();
}

#[test]
fn a_reader_process_expands_as_the_writer_does() {
    let (dir, db, store) = random_store(11);
    let reader = Pigeonhole::open_reader(dir.join("mem.phdb"), ReaderOptions::default()).unwrap();
    let rs = Store::open_reader(&reader, "notes", MODEL).unwrap();
    let at = now();
    let q = |r: pigeonhole_memory::Recall<'_>| {
        r.text("postgres deploy")
            .embedding(vec![0.2, -0.4, 0.9, 0.1])
            .candidates(5)
            .k(100)
            .now_us(at)
            .with_signals(true)
            .expand(Expand::hops(2))
            .run_with_stats()
            .unwrap()
    };
    let (from_reader, sr) = q(rs.recall(&["a", "b"]));
    let (from_writer, sw) = q(store.recall(&["a", "b"]));
    assert!(sw.expanded > 0);
    assert_eq!(from_reader, from_writer);
    assert_eq!(sr, sw);
    drop(rs);
    drop(reader);
    db.close().unwrap();
}

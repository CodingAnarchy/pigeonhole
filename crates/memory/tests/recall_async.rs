//! #1560 PR 3 (ICR 0124): `Recall::run_async` and `run_with_stats_async` return exactly what
//! `run` and `run_with_stats` do (hits, scores, signals and stats), over seeded stores and
//! recalls: text only, embedding only and both; exact and IVF, with the IVF path's fall-back
//! to exact; graph expansions of 1 to 4 hops, with `max` and byte-budget cuts, a budget cut
//! inside a hop among them (ai1249's M1); the filters; the linear blend; `k` and the
//! candidates. From a cold block cache, without a synchronous read; in a reader process too;
//! and the futures are `Send`.
//!
//! The store is on the simulated device with its I/O deferred and completed after each
//! `Pending`, so every block a recall misses is fetched and no read completes at submit,
//! which would count as synchronous on real files. A recall reads nothing synchronously:
//! since #1652, the engine's key-set reads (the multi-get's memory rows, a graph expansion's
//! traversal, the IVF search's list rows) fetch a row's later blocks too.
#![cfg(feature = "async")]

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole::{Element, Options, Pigeonhole, ReaderOptions};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_memory::{
    EmbeddingModel, Expand, Fusion, IvfOptions, LinkType, Memory, MemoryId, Recall, Store,
    StoreOptions, Weights,
};

const PATH: &str = "/db/mem.phdb";
const MODEL: &str = "test-embed-v1";
const DIM: usize = 4;

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

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

fn ty() -> LinkType {
    LinkType::custom(16).unwrap()
}

/// IVF from 20 memories in a namespace, in small lists: `a` (300) searches the index, and
/// `b` (30) is sparse among the lists nearest a query, so it falls back to exact.
fn ivf() -> IvfOptions {
    IvfOptions::new()
        .min_namespace_len(20)
        .target_list_len(8)
        .nprobe(2)
}

/// Two namespaces of memories with random links (a hub in each), sessions, uses and some
/// superseded, flushed and compacted into SST blocks, the database reopened so the block
/// cache is cold.
fn build(vfs: &Arc<SimVfs>, seed: u64) -> Pigeonhole {
    let opts = || Options::default().vfs(Arc::clone(vfs) as _).shards(2);
    let db = Pigeonhole::open(PATH, opts()).expect("open");
    let model = EmbeddingModel::new(MODEL, DIM as u32, Element::F32).unwrap();
    let store = Store::create(
        &db,
        "notes",
        StoreOptions::new().embedding(model).ivf(ivf()),
    )
    .expect("create");
    fill(&store, seed);
    drop(store);
    db.flush().unwrap();
    db.compact().unwrap();
    db.close().unwrap();
    Pigeonhole::open(PATH, opts()).expect("reopen")
}

fn fill(store: &Store, seed: u64) {
    let mut rng = Rng::new(seed);
    for (ns, n) in [("a", 300u64), ("b", 30)] {
        let mut ids: Vec<MemoryId> = Vec::new();
        for i in 0..n {
            let m = Memory::new(format!("{} {ns}{i}", text(&mut rng)))
                .embedding(emb(&mut rng))
                .importance(rng.below(1001) as f64 / 1000.0)
                .session(if rng.below(3) == 0 { "s2" } else { "s1" });
            ids.push(store.remember(ns, m).unwrap().id().clone());
        }
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
        for id in ids.iter().step_by(5) {
            store.mark_used(std::slice::from_ref(id)).unwrap();
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
}

/// Polls `fut` to completion with the device's I/O deferred, completing it after each
/// `Pending`.
fn drive<F: Future>(vfs: &SimVfs, fut: F) -> (F::Output, u32) {
    let mut fut = std::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    vfs.set_deferred_io(true);
    let mut pending = 0;
    let out = loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => break v,
            Poll::Pending => {
                pending += 1;
                assert!(pending < 1_000_000, "never resolved");
                vfs.complete_all_io();
            }
        }
    };
    vfs.set_deferred_io(false);
    (out, pending)
}

/// One random recall's settings.
#[derive(Debug, Clone)]
struct Case {
    namespaces: Vec<&'static str>,
    text: Option<String>,
    embedding: Option<Vec<f32>>,
    exact: bool,
    k: usize,
    candidates: usize,
    budget: Option<u64>,
    expand: Option<(u32, usize)>,
    session: Option<&'static str>,
    include_superseded: bool,
    linear: bool,
    with_signals: bool,
}

impl Case {
    fn random(rng: &mut Rng) -> Self {
        let namespaces = match rng.below(3) {
            0 => vec!["a"],
            1 => vec!["b"],
            _ => vec!["a", "b"],
        };
        // Text only, an embedding only, or both.
        let which = rng.below(3);
        let text = (which != 1).then(|| text(rng));
        let embedding = (which != 0).then(|| emb(rng));
        Case {
            namespaces,
            text,
            embedding,
            exact: rng.below(4) == 0,
            k: [1, 5, 10, 50][rng.below(4) as usize],
            candidates: [1, 5, 20, 50][rng.below(4) as usize],
            budget: [None, None, Some(200), Some(800), Some(3000)][rng.below(5) as usize],
            expand: (rng.below(2) == 0).then(|| {
                (
                    1 + rng.below(4) as u32,
                    [3, 20, 1000][rng.below(3) as usize],
                )
            }),
            session: [None, None, Some("s1"), Some("s2")][rng.below(4) as usize],
            include_superseded: rng.below(3) == 0,
            linear: rng.below(4) == 0,
            with_signals: rng.below(2) == 0,
        }
    }

    fn apply<'a>(&self, r: Recall<'a>, s: &'a pigeonhole::Snapshot, now: u64) -> Recall<'a> {
        let mut r = r
            .k(self.k)
            .candidates(self.candidates)
            .snapshot(s)
            .now_us(now)
            .exact(self.exact)
            .include_superseded(self.include_superseded)
            .with_signals(self.with_signals);
        if let Some(t) = &self.text {
            r = r.text(t.clone());
        }
        if let Some(e) = &self.embedding {
            r = r.embedding(e.clone());
        }
        if let Some(b) = self.budget {
            r = r.byte_budget(b);
        }
        if let Some((hops, max)) = self.expand {
            r = r.expand(Expand::hops(hops).max(max));
        }
        if let Some(sess) = self.session {
            r = r.session(sess);
        }
        if self.linear {
            r = r.fusion(Fusion::Linear);
        }
        // The graph signal counts.
        let mut w = Weights::default();
        w.graph = 1.0;
        r.weights(w)
    }
}

/// What the property test met, so it can say it covered the paths it claims to.
#[derive(Debug, Default)]
struct Seen {
    ivf: usize,
    fallbacks: usize,
    expanded: usize,
    /// An expansion the byte budget cut after a hop gave some memories their graph value:
    /// the cut came inside a later hop.
    budget_cut_mid_walk: usize,
    truncated: usize,
    pendings: u32,
}

#[test]
fn async_equals_sync_over_random_stores_and_recalls() {
    let seed: u64 = std::env::var("MEMORY_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1560);
    let mut seen = Seen::default();
    for seed in [seed, seed + 1] {
        let vfs = SimVfs::new(seed);
        let db = build(&vfs, seed);
        let store = Store::open(&db, "notes", MODEL).expect("open store");
        let s = db.snapshot().unwrap();
        let now = 1_900_000_000_000_000;
        let mut rng = Rng::new(seed ^ 0xa5a5);
        for i in 0..60 {
            let case = Case::random(&mut rng);
            let ctx = format!("seed {seed} (MEMORY_SEED to replay), case {i}: {case:?}");
            let before = db.engine_metrics().async_sync_reads;
            let (got, pending) = drive(
                &vfs,
                case.apply(store.recall(&case.namespaces), &s, now)
                    .run_with_stats_async(),
            );
            let reads = db.engine_metrics().async_sync_reads - before;
            seen.pendings += pending;
            let want = case
                .apply(store.recall(&case.namespaces), &s, now)
                .run_with_stats();
            let (got, want) = match (got, want) {
                (Ok(g), Ok(w)) => (g, w),
                (g, w) => {
                    assert_eq!(format!("{g:?}"), format!("{w:?}"), "{ctx}");
                    continue;
                }
            };
            assert_eq!(got, want, "{ctx}");
            assert_eq!(reads, 0, "{ctx}: synchronous reads");
            let (hits, _) = drive(
                &vfs,
                case.apply(store.recall(&case.namespaces), &s, now)
                    .run_async(),
            );
            assert_eq!(hits.expect("run_async"), want.0, "{ctx}");
            let st = &want.1;
            seen.ivf += st.ivf_namespaces;
            seen.fallbacks += st.ivf_fallbacks;
            seen.expanded += st.expanded;
            seen.truncated += usize::from(st.truncated);
            if case.budget.is_some()
                && st.expansion_truncated
                && st.expanded > 0
                && case.expand.is_some_and(|(_, max)| st.expanded < max)
            {
                seen.budget_cut_mid_walk += 1;
            }
        }
        drop(store);
        vfs.complete_all_io();
        db.close().unwrap();
    }
    // The paths the test claims to cover.
    assert!(seen.pendings > 0, "nothing was fetched: {seen:?}");
    assert!(seen.ivf > 0, "no IVF recall: {seen:?}");
    assert!(seen.fallbacks > 0, "no IVF fall-back: {seen:?}");
    assert!(seen.expanded > 0, "no expansion: {seen:?}");
    assert!(
        seen.budget_cut_mid_walk > 0,
        "no budget cut inside a walk: {seen:?}"
    );
    assert!(
        seen.truncated > 0,
        "no byte-budget cut of the hits: {seen:?}"
    );
}

/// ai1249's M1: a budget that runs out inside a hop, after some of its links were read,
/// drops the hop whole in both forms, and each budget from none to past the whole walk
/// gives the same expansion.
#[test]
fn an_expansion_cut_inside_a_hop_by_the_budget_is_the_same_async() {
    let vfs = SimVfs::new(9);
    let db = build(&vfs, 9);
    let store = Store::open(&db, "notes", MODEL).expect("open store");
    let s = db.snapshot().unwrap();
    let now = 1_900_000_000_000_000;
    let mut q = Rng::new(3);
    let e = emb(&mut q);
    let mut cuts = 0;
    for budget in (100..4000).step_by(100) {
        let recall = || {
            store
                .recall(&["a"])
                .text("deploy memory")
                .embedding(e.clone())
                .candidates(5)
                .k(1000)
                .snapshot(&s)
                .now_us(now)
                .byte_budget(budget)
                .with_signals(true)
                .expand(Expand::hops(3).max(1000))
        };
        let (got, _) = drive(&vfs, recall().run_with_stats_async());
        let want = recall().run_with_stats().unwrap();
        assert_eq!(got.unwrap(), want, "budget {budget}");
        if want.1.expansion_truncated {
            cuts += 1;
        }
    }
    assert!(cuts > 0, "no budget cut the expansion");
    drop(store);
    vfs.complete_all_io();
    db.close().unwrap();
}

/// ai1249's reader-process point: every read a recall makes has an async form in a reader
/// process, so a `ReadStore`'s async recall refuses nothing a writer's doesn't, and returns
/// what its sync recall does.
#[test]
fn a_reader_process_recalls_async_what_it_recalls_sync() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("mem.phdb"), Options::default().shards(2)).unwrap();
    let model = EmbeddingModel::new(MODEL, DIM as u32, Element::F32).unwrap();
    let store = Store::create(
        &db,
        "notes",
        StoreOptions::new().embedding(model).ivf(ivf()),
    )
    .unwrap();
    fill(&store, 77);
    db.flush().unwrap();
    let reader = Pigeonhole::open_reader(dir.join("mem.phdb"), ReaderOptions::default()).unwrap();
    let rs = Store::open_reader(&reader, "notes", MODEL).unwrap();
    let s = reader.snapshot().unwrap();
    let mut rng = Rng::new(78);
    for i in 0..20 {
        let case = Case::random(&mut rng);
        let got = pigeonhole::doc_support::block_on(
            case.apply(rs.recall(&case.namespaces), &s, 1)
                .run_with_stats_async(),
        );
        let want = case
            .apply(rs.recall(&case.namespaces), &s, 1)
            .run_with_stats();
        assert_eq!(
            format!("{got:?}"),
            format!("{want:?}"),
            "case {i}: {case:?}"
        );
        assert!(got.is_ok(), "case {i}: {got:?}");
    }
    drop(rs);
    drop(reader);
    drop(store);
    db.close().unwrap();
}

#[test]
fn async_recall_futures_are_send() {
    fn send<T: Send>(_: &T) {}
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("mem.phdb"), Options::default()).unwrap();
    let model = EmbeddingModel::new(MODEL, DIM as u32, Element::F32).unwrap();
    let store = Store::create(&db, "notes", StoreOptions::new().embedding(model)).unwrap();
    let r = store.recall(&["a"]).text("memory").expand(Expand::hops(2));
    let f = r.run_with_stats_async();
    send(&f);
    drop(f);
    let f = store.recall(&["a"]).text("memory").run_async();
    send(&f);
    drop(f);
    drop(store);
    db.close().unwrap();
}

/// ai1249's R1: a warm recall whose filter doubles its lists to a large namespace's size
/// yields between its rows (256 per poll, shared by its recency lists and multi-gets)
/// rather than reading them all in one poll, and still returns what `run` does.
#[test]
fn a_warm_filtered_recall_yields_between_its_rows() {
    use std::sync::atomic::{AtomicU32, Ordering};
    struct Count(AtomicU32);
    impl std::task::Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let vfs = SimVfs::new(31);
    let db = build(&vfs, 31);
    let store = Store::open(&db, "notes", MODEL).expect("open store");
    let s = db.snapshot().unwrap();
    let recall = || {
        store
            .recall(&["a"])
            .text("memory deploy")
            .candidates(150)
            .k(20)
            .session("s2")
            .snapshot(&s)
            .now_us(1_900_000_000_000_000)
    };
    // Warm: the sync recall reads every block the async one will.
    let want = recall().run_with_stats().unwrap();
    let count = Arc::new(Count(AtomicU32::new(0)));
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let (got, pendings) = {
        let mut fut = std::pin::pin!(recall().run_with_stats_async());
        let mut pendings = 0;
        let got = loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(r) => break r.unwrap(),
                Poll::Pending => {
                    pendings += 1;
                    // Warm: every `Pending` is a yield, its task already woken.
                    assert_eq!(
                        count.0.load(Ordering::Relaxed),
                        pendings,
                        "a Pending without a wake"
                    );
                }
            }
        };
        (got, pendings)
    };
    assert_eq!(got, want);
    // The session filter doubles `n` past the namespace's 300 memories: its newest list
    // and the multi-gets read hundreds of rows.
    assert!(pendings >= 2, "{pendings} yields");
    drop(store);
    db.close().unwrap();
}

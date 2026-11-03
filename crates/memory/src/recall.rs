//! Recall (D311 §3): vector, keyword and recency candidates per namespace, every signal
//! computed for every candidate, fused by reciprocal rank fusion (or a linear blend), and
//! returned in fused order within a byte budget. Read-only, at one snapshot.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use pigeonhole::{KeySet, Pigeonhole, PigeonholeReader, ReadTable, Snapshot, Table, VectorSpec};
use pigeonhole_text::{Index, Query, ReadIndex};

use crate::error::{Error, Result, corrupt};
use crate::layout::*;
use crate::store::{EmbeddingModel, Kind};

/// RRF's constant (Cormack et al., 2009; D311 call 4).
const RRF_K: f64 = 60.0;
/// D308's per-item overhead, which a recall hit's size counts too.
const ITEM_OVERHEAD: u64 = 16;
/// The bytes `with_signals` adds to a hit: six signals, a rank (`u32`) and a value (`f64`)
/// each.
const SIGNALS_BYTES: u64 = 6 * (4 + 8);
/// How many of the fused candidates seed a graph expansion (D311 §3, `M`).
const GRAPH_SEEDS: usize = 10;

/// How the signals are combined (D311 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Fusion {
    /// Reciprocal rank fusion: `Σ w_s / (60 + rank_s)` over the signals a memory ranks in.
    /// It needs no normalisation across the signals' unlike scales.
    #[default]
    Rrf,
    /// A weighted sum of each signal min-max normalised over the candidates.
    Linear,
}

/// Each signal's weight in the fusion (D311 §10a). Plain data, so bindings can build it:
/// start from `Weights::default()` (`recency` at 0.25, `graph` and `consolidated` at 0.5, the
/// rest at 1) and set the fields. Under
/// [`Fusion::Rrf`] a weight multiplies the signal's `1 / (60 + rank)`; under
/// [`Fusion::Linear`], its min-max-normalised value. A weight of 0 leaves the signal out.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub struct Weights {
    /// Embedding similarity.
    pub vector: f64,
    /// BM25, for keyword hits.
    pub keyword: f64,
    /// `exp(−age / half_life)`: 0.25 by default. At 1, as before 1.1.0, recency outweighed
    /// what a query asked for on both of T5's benchmarks (the agent memory guide's "How well
    /// it recalls").
    pub recency: f64,
    /// The memory's importance.
    pub importance: f64,
    /// `ln(1 + uses)`, decayed by the time since the last use.
    pub uses: f64,
    /// The graph signal of a memory a graph expansion reached ([`Recall::expand`]): 0.5 by
    /// default. It has no effect on a recall without `expand`.
    pub graph: f64,
    /// Not a signal's weight: a factor on the importance signal of a memory a consolidation
    /// took in ([`Store::consolidate`](crate::Store::consolidate)), so its summary ranks
    /// ahead of it. 0.5 by default; 1 leaves consolidated memories as they were.
    pub consolidated: f64,
}

impl Default for Weights {
    fn default() -> Self {
        Weights {
            vector: 1.0,
            keyword: 1.0,
            recency: 0.25,
            importance: 1.0,
            uses: 1.0,
            graph: 0.5,
            consolidated: 0.5,
        }
    }
}

/// One signal's rank among the candidates (1 is best) and its value.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub struct Signal {
    /// Its rank for this signal among the candidates that have it (not the final order),
    /// 1-based: 1 is the best. Equal values share a rank, one more than how many candidates
    /// have a greater value.
    pub rank: u32,
    /// Its value.
    pub value: f64,
}

/// A hit's signals ([`Recall::with_signals`]): `None` where the memory doesn't rank.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[non_exhaustive]
pub struct Signals {
    /// Cosine similarity to the query embedding.
    pub vector: Option<Signal>,
    /// Its BM25 score, if it was a keyword hit.
    pub keyword: Option<Signal>,
    /// Its recency.
    pub recency: Option<Signal>,
    /// Its importance.
    pub importance: Option<Signal>,
    /// Its decayed use, if it was ever used.
    pub uses: Option<Signal>,
    /// Its graph value, if a graph expansion reached it ([`Recall::expand`]): never for a
    /// seed, nor without `expand`.
    pub graph: Option<Signal>,
}

/// A graph expansion of a recall ([`Recall::expand`], D311 §3): the top 10 fused candidates
/// seed a walk of the memories' links, in both directions, and every memory it reaches is
/// scored like the other candidates, with a **graph** signal besides.
///
/// - **The graph signal:** seed `i` (its 1-based place in the fused order before the
///   expansion) has the value `1 / i`. A memory first reached at hop `h` has the largest
///   `value(q) × weight` over the links between it and the memories `q` kept at hop
///   `h − 1`, either way round: the best of its shortest paths, not of every path. Seeds
///   have no graph signal.
/// - **The walk:** each hop reads every link and backlink of the previous hop's memories
///   through the engine's [`Traverse`](pigeonhole::Traverse), at the recall's snapshot. It
///   passes only through memories the recall could return, by the same test as its other
///   candidates: not a row without content, a superseded memory (without
///   [`include_superseded`](Recall::include_superseded)), or another
///   [`session`](Recall::session)'s.
/// - **Its bounds:** at most [`max`](Expand::max) memories get a graph signal, over every
///   hop: the best-valued of each hop (ties by id) while there's room. With a
///   [`byte_budget`](Recall::byte_budget) of `B`, the walk reads at most `B` bytes of nodes
///   besides (D308's count: each node it reads, its key and 16 bytes), so a recall holds up
///   to `2B`. A hop that doesn't fit whole is dropped, with the hops before it kept exact.
///   [`RecallStats::expansion_truncated`] says when either bound stopped it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Expand {
    /// Hops to take, 1 to [`MAX_HOPS`](Self::MAX_HOPS).
    pub hops: u32,
    /// At most this many memories get a graph signal: 20 by default. 0 gives none (the walk
    /// doesn't run), as [`Recall::k`] of 0 returns no hits.
    pub max: usize,
}

impl Expand {
    /// The most hops an expansion takes.
    pub const MAX_HOPS: u32 = 4;

    /// An expansion `n` hops deep (1 to [`MAX_HOPS`](Self::MAX_HOPS); the recall checks it), scoring
    /// at most 20 memories.
    pub fn hops(n: u32) -> Self {
        Expand { hops: n, max: 20 }
    }

    /// At most `m` memories get a graph signal (0: none).
    pub fn max(mut self, m: usize) -> Self {
        self.max = m;
        self
    }
}

/// One recalled memory.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RecallHit {
    /// Its id.
    pub id: MemoryId,
    /// Its text.
    pub text: String,
    /// Its kind.
    pub kind: Kind,
    /// Its importance.
    pub importance: f64,
    /// When it was created, in µs.
    pub created_us: u64,
    /// The memory that superseded it: only with
    /// [`include_superseded`](Recall::include_superseded), which keeps such memories.
    pub superseded_by: Option<MemoryId>,
    /// Its fused score.
    pub score: f64,
    /// Its signals, with [`Recall::with_signals`].
    pub signals: Option<Signals>,
    /// Its embedding, with [`Recall::with_embeddings`].
    pub embedding: Option<Vec<f32>>,
}

/// What a recall met besides its hits.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RecallStats {
    /// The candidates scored, over every namespace.
    pub candidates: usize,
    /// Whether the byte budget cut the list (not `k`), including when the first hit alone
    /// was larger than the budget.
    pub truncated: bool,
    /// The memories a graph expansion gave a graph signal ([`Recall::expand`]).
    pub expanded: usize,
    /// Whether [`Expand::max`] or the byte budget stopped the graph expansion.
    pub expansion_truncated: bool,
    /// Namespaces whose vector signal came from the store's IVF index (D311 T8,
    /// [`StoreOptions::ivf`](crate::StoreOptions::ivf)), counted once per namespace.
    pub ivf_namespaces: usize,
    /// Namespaces large enough for the IVF index that were searched exactly anyway: the
    /// namespace was sparse among the lists nearest the query, so widening hit its cap.
    pub ivf_fallbacks: usize,
}

/// The table behind a recall: the writer's or a reader process's.
#[derive(Clone, Copy)]
pub(crate) enum Rows<'a> {
    W(&'a Table),
    R(&'a ReadTable),
}

/// The database a recall takes its snapshot from when none is given.
#[derive(Clone, Copy)]
pub(crate) enum Snap<'a> {
    W(&'a Pigeonhole),
    R(&'a PigeonholeReader),
}

/// The text index behind a recall.
#[derive(Clone, Copy)]
pub(crate) enum Idx<'a> {
    W(&'a Index),
    R(&'a ReadIndex),
}

/// A recall to run ([`Store::recall`](crate::Store::recall)).
#[must_use = "a recall does nothing until it runs"]
pub struct Recall<'a> {
    rows: Rows<'a>,
    index: Idx<'a>,
    ivf: Option<&'a crate::ivf::Ivf>,
    exact: bool,
    nprobe: Option<u32>,
    model: Option<&'a EmbeddingModel>,
    namespaces: Vec<String>,
    text: Option<String>,
    embedding: Option<Vec<f32>>,
    k: usize,
    per_signal: usize,
    byte_budget: Option<u64>,
    snapshot: Option<&'a Snapshot>,
    db: Snap<'a>,
    at_us: Option<u64>,
    half_life_us: u64,
    fusion: Fusion,
    weights: Weights,
    include_superseded: bool,
    session: Option<String>,
    with_signals: bool,
    with_embeddings: bool,
    expand: Option<Expand>,
}

impl std::fmt::Debug for Recall<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recall")
            .field("namespaces", &self.namespaces)
            .field("text", &self.text)
            .field("k", &self.k)
            .finish_non_exhaustive()
    }
}

impl<'a> Recall<'a> {
    pub(crate) fn new(
        rows: Rows<'a>,
        index: Idx<'a>,
        ivf: Option<&'a crate::ivf::Ivf>,
        model: Option<&'a EmbeddingModel>,
        db: Snap<'a>,
        namespaces: &[&str],
    ) -> Self {
        Recall {
            rows,
            index,
            ivf,
            exact: false,
            nprobe: None,
            model,
            namespaces: namespaces.iter().map(|s| (*s).to_owned()).collect(),
            text: None,
            embedding: None,
            k: 10,
            per_signal: 50,
            byte_budget: None,
            snapshot: None,
            db,
            at_us: None,
            half_life_us: 7 * 24 * 3600 * 1_000_000,
            fusion: Fusion::Rrf,
            weights: Weights::default(),
            include_superseded: false,
            session: None,
            with_signals: false,
            with_embeddings: false,
            expand: None,
        }
    }

    /// The query text, for keyword recall: tokenised as the memories were.
    pub fn text(mut self, t: impl Into<String>) -> Self {
        self.text = Some(t.into());
        self
    }

    /// The query embedding, for vector recall: from the store's model, as `f32` whatever
    /// the store's element type, at its dimension. `InvalidArgument` on a store without a
    /// model, or a handle opened without it; recall by text alone there.
    pub fn embedding(mut self, e: impl Into<Vec<f32>>) -> Self {
        self.embedding = Some(e.into());
        self
    }

    /// At most `k` hits: 10 by default. `k`, [`candidates`](Self::candidates) and
    /// [`byte_budget`](Self::byte_budget) apply in that order of scope: candidates bound
    /// what is scored, then the fused list is returned until `k` hits or the budget, which
    /// comes first.
    pub fn k(mut self, k: usize) -> Self {
        self.k = k;
        self
    }

    /// How many candidates each signal contributes per namespace: 50 by default, counted
    /// after the filters (superseded memories, [`session`](Self::session)). A signal reads
    /// further down its list until that many pass or its namespace is exhausted.
    pub fn candidates(mut self, n: usize) -> Self {
        self.per_signal = n;
        self
    }

    /// At most `bytes` of hits, by D308's rules: each hit's text, its id and 16 bytes, plus
    /// its signals and embedding when asked for. A hit is whole or left out. As with the
    /// engine's byte budgets, the first hit is returned even if it alone is larger, and
    /// [`RecallStats::truncated`] says the budget cut the list. A graph expansion
    /// ([`expand`](Self::expand)) reads up to the same number of bytes of nodes besides, so
    /// a recall holds up to twice this.
    pub fn byte_budget(mut self, bytes: u64) -> Self {
        self.byte_budget = Some(bytes);
        self
    }

    /// Recalls at `snapshot` rather than the latest state.
    pub fn snapshot(mut self, snapshot: &'a Snapshot) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    /// Ages (recency, use) are measured from `us`, in µs since the Unix epoch, rather than
    /// the wall clock. With an explicit [`snapshot`](Self::snapshot) too, the same call
    /// always returns the same result.
    pub fn now_us(mut self, us: u64) -> Self {
        self.at_us = Some(us);
        self
    }

    /// Recency's and use's half-life: 7 days by default.
    pub fn half_life(mut self, d: std::time::Duration) -> Self {
        self.half_life_us = (d.as_micros() as u64).max(1);
        self
    }

    /// The fusion: RRF by default.
    pub fn fusion(mut self, f: Fusion) -> Self {
        self.fusion = f;
        self
    }

    /// The signals' weights.
    pub fn weights(mut self, w: Weights) -> Self {
        self.weights = w;
        self
    }

    /// Superseded memories too: left out by default.
    pub fn include_superseded(mut self, on: bool) -> Self {
        self.include_superseded = on;
        self
    }

    /// Only memories of this session.
    pub fn session(mut self, s: impl Into<String>) -> Self {
        self.session = Some(s.into());
        self
    }

    /// Fills each hit's [`signals`](RecallHit::signals).
    pub fn with_signals(mut self, on: bool) -> Self {
        self.with_signals = on;
        self
    }

    /// Fills each hit's [`embedding`](RecallHit::embedding).
    pub fn with_embeddings(mut self, on: bool) -> Self {
        self.with_embeddings = on;
        self
    }

    /// Searches embeddings exactly in every namespace, even where the store's IVF index
    /// would serve ([`StoreOptions::ivf`](crate::StoreOptions::ivf)): the ground truth an
    /// IVF recall can be checked against. No effect on a store without the index.
    pub fn exact(mut self, on: bool) -> Self {
        self.exact = on;
        self
    }

    /// The lists an IVF search probes first, for this recall (the store's
    /// [`IvfOptions::nprobe`](crate::IvfOptions::nprobe) by default). No effect on a store
    /// without the index, or on a namespace recalled exactly.
    pub fn nprobe(mut self, n: u32) -> Self {
        self.nprobe = Some(n);
        self
    }

    /// Expands the candidates through the memories' links ([`Expand`]): none by default.
    /// `InvalidArgument` from the run for hops outside 1 to [`Expand::MAX_HOPS`].
    pub fn expand(mut self, x: Expand) -> Self {
        self.expand = Some(x);
        self
    }

    /// The hits, best first. [`run_with_stats`](Self::run_with_stats) also says whether
    /// the byte budget cut them.
    pub fn run(self) -> Result<Vec<RecallHit>> {
        self.run_with_stats().map(|(h, _)| h)
    }

    /// [`run`](Self::run), with what the recall met.
    pub fn run_with_stats(self) -> Result<(Vec<RecallHit>, RecallStats)> {
        if self.namespaces.is_empty() {
            return Err(Error::InvalidArgument(
                "recall takes at least one namespace".into(),
            ));
        }
        for ns in &self.namespaces {
            check_namespace(ns)?;
        }
        let text = self.text.as_deref().filter(|t| !t.trim().is_empty());
        if text.is_none() && self.embedding.is_none() {
            return Err(Error::InvalidArgument(
                "recall needs text or an embedding".into(),
            ));
        }
        let spec = match (&self.embedding, self.model) {
            (Some(_), None) => {
                return Err(Error::InvalidArgument(
                    "the store has no embedding model, so recall takes no embedding".into(),
                ));
            }
            (Some(e), Some(m)) if e.len() != m.dim() as usize => {
                return Err(Error::InvalidArgument(format!(
                    "a query embedding of {} dimensions, and the store's model has {}",
                    e.len(),
                    m.dim()
                )));
            }
            (Some(_), Some(m)) => Some(m.spec()),
            (None, _) => None,
        };
        if let Some(x) = self.expand
            && !(1..=Expand::MAX_HOPS).contains(&x.hops)
        {
            return Err(Error::InvalidArgument(format!(
                "an expansion takes 1 to {} hops, not {}",
                Expand::MAX_HOPS,
                x.hops
            )));
        }
        let owned;
        let s = match self.snapshot {
            Some(s) => s,
            None => {
                owned = match self.db {
                    Snap::W(db) => db.snapshot()?,
                    Snap::R(r) => r.snapshot()?,
                };
                &owned
            }
        };
        let now = self.at_us.unwrap_or_else(now_us);
        let mut stats = RecallStats::default();
        if self.k == 0 {
            return Ok((Vec::new(), stats));
        }
        // 1. Candidates: per namespace and signal, its best `n`, read in one multi-get; the
        // filters (superseded, session) apply to each. Until every list holds `per_signal`
        // that pass, or came back short (its namespace is exhausted), `n` doubles, so a
        // filtered recall still gets each signal's best `per_signal` eligible memories
        // (simp724's N1 on #1374).
        let mut n = self.per_signal.max(1);
        // Each namespace's path for the vector signal: the IVF index once its counter (read
        // at the recall's snapshot, as the search and the memories are) reaches the store's
        // `min_namespace_len`. `None`: exact.
        let mut ivf_counts: BTreeMap<&str, u64> = BTreeMap::new();
        if let (Some(ivf), Some(_), false) = (self.ivf, &self.embedding, self.exact) {
            let min = ivf.options().get_min_namespace_len();
            for ns in &self.namespaces {
                let c = crate::ivf::count(&self.rows, s, ns)?;
                if c >= min {
                    ivf_counts.insert(ns.as_str(), c);
                }
            }
        }
        let mut fell_back: BTreeSet<&str> = BTreeSet::new();
        let (lists, eligible) = loop {
            let mut lists: Vec<(usize, Vec<Vec<u8>>, bool)> = Vec::new();
            let mut scores: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
            for ns in &self.namespaces {
                let prefix = [ns.as_bytes(), MEMORY_TAG].concat();
                if let Some(e) = &self.embedding {
                    let via_ivf = match (self.ivf, ivf_counts.get(ns.as_str())) {
                        (Some(ivf), Some(&c)) if !fell_back.contains(ns.as_str()) => {
                            let found = ivf.nearest(s, &prefix, e, n, c, self.nprobe)?;
                            if found.is_none() {
                                fell_back.insert(ns.as_str());
                            }
                            found
                        }
                        _ => None,
                    };
                    let l = match via_ivf {
                        Some(l) => l,
                        None => self.rows.nearest(s, &prefix, e, n)?,
                    };
                    let full = l.len() == n;
                    lists.push((0, l, full));
                }
                if let Some(t) = text {
                    let hits = self.index.search(s, t, &prefix, n)?;
                    let full = hits.len() == n;
                    let mut l = Vec::with_capacity(hits.len());
                    for (id, score) in hits {
                        scores.insert(id.clone(), score);
                        l.push(id);
                    }
                    lists.push((1, l, full));
                }
                let l = self.rows.newest(s, &prefix, n)?;
                let full = l.len() == n;
                lists.push((2, l, full));
            }
            let keys: BTreeSet<&Vec<u8>> = lists.iter().flat_map(|(_, l, _)| l.iter()).collect();
            let set = KeySet::from_unsorted(keys.into_iter().collect::<Vec<_>>());
            let mut eligible: BTreeMap<Vec<u8>, Cand> = BTreeMap::new();
            self.rows.read_each(s, &set, |key, row| {
                if let Some(c) = self.candidate(key, row, spec.as_ref(), now)? {
                    eligible.insert(key.to_vec(), c);
                }
                Ok(())
            })?;
            let done = lists.iter().all(|(_, l, full)| {
                !full || l.iter().filter(|k| eligible.contains_key(*k)).count() >= self.per_signal
            });
            if done {
                // BM25 counts for the keyword lists' kept keys only (below).
                for c in eligible.values_mut() {
                    c.vals[1] = None;
                }
                for (sig, l, _) in &lists {
                    if *sig == 1 {
                        let top: Vec<&Vec<u8>> = l
                            .iter()
                            .filter(|k| eligible.contains_key(*k))
                            .take(self.per_signal)
                            .collect();
                        for k in top {
                            if let Some(c) = eligible.get_mut(k) {
                                c.vals[1] = scores.get(k).copied();
                            }
                        }
                    }
                }
                break (lists, eligible);
            }
            n = n.saturating_mul(2);
        };
        // Each list's best `per_signal` eligible memories, together.
        let mut kept: BTreeSet<&Vec<u8>> = BTreeSet::new();
        for (_, l, _) in &lists {
            for k in l
                .iter()
                .filter(|k| eligible.contains_key(*k))
                .take(self.per_signal)
            {
                kept.insert(k);
            }
        }
        let kept: BTreeSet<Vec<u8>> = kept.into_iter().cloned().collect();
        let mut cands: Vec<Cand> = eligible
            .into_iter()
            .filter(|(k, _)| kept.contains(k))
            .map(|(_, c)| c)
            .collect();
        // 2. The graph expansion: seeded by the fused order without it.
        if let Some(x) = self.expand {
            let mut w = self.signal_weights();
            w[5] = 0.0;
            fuse(&mut cands, &w, self.fusion);
            let seeds = cands
                .iter()
                .take(GRAPH_SEEDS)
                .enumerate()
                .map(|(i, c)| (c.key.clone(), 1.0 / (i + 1) as f64))
                .collect();
            self.expand_graph(s, &mut cands, seeds, x, spec.as_ref(), now, &mut stats)?;
            for c in &mut cands {
                c.score = 0.0;
                c.ranks = [None; 6];
            }
        }
        stats.candidates = cands.len();
        stats.ivf_fallbacks = fell_back.len();
        stats.ivf_namespaces = ivf_counts.len() - fell_back.len();
        // 3. Fusion.
        fuse(&mut cands, &self.signal_weights(), self.fusion);
        // 4. In fused order, within k and the byte budget.
        let mut out = Vec::new();
        let mut used = 0u64;
        for c in cands {
            if out.len() == self.k {
                break;
            }
            let mut hit = c.hit;
            hit.score = c.score;
            if self.with_signals {
                let sig = |i: usize| {
                    c.vals[i].map(|value| Signal {
                        rank: c.ranks[i].expect("a value has a rank"),
                        value,
                    })
                };
                hit.signals = Some(Signals {
                    vector: sig(0),
                    keyword: sig(1),
                    recency: sig(2),
                    importance: sig(3),
                    uses: sig(4),
                    graph: sig(5),
                });
            }
            let size = hit_size(&hit, self.with_signals);
            if let Some(budget) = self.byte_budget
                && used + size > budget
            {
                stats.truncated = true;
                // D308: the first item always, even past the budget.
                if !out.is_empty() {
                    break;
                }
            }
            used += size;
            out.push(hit);
        }
        Ok((out, stats))
    }

    /// The signals' weights, in [`Cand::vals`]' order.
    fn signal_weights(&self) -> [f64; 6] {
        let w = &self.weights;
        [
            w.vector,
            w.keyword,
            w.recency,
            w.importance,
            w.uses,
            w.graph,
        ]
    }

    /// Walks the links from `seeds` (each with its value, best first) as [`Expand`] says,
    /// adding the memories it reaches to `cands` and setting their graph values.
    #[allow(clippy::too_many_arguments)]
    fn expand_graph(
        &self,
        s: &Snapshot,
        cands: &mut Vec<Cand>,
        seeds: Vec<(Vec<u8>, f64)>,
        x: Expand,
        spec: Option<&VectorSpec>,
        now: u64,
        stats: &mut RecallStats,
    ) -> Result<()> {
        let mut at: BTreeMap<Vec<u8>, usize> = cands
            .iter()
            .enumerate()
            .map(|(i, c)| (c.key.clone(), i))
            .collect();
        // Seeds and earlier hops' memories: not reached again.
        let mut visited: BTreeSet<Vec<u8>> = seeds.iter().map(|(k, _)| k.clone()).collect();
        // Rows the recall couldn't return: neither scored nor walked through.
        let mut ineligible: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut budget = self.byte_budget;
        let mut graphed = 0usize;
        let mut level = seeds;
        for _ in 0..x.hops {
            if graphed == x.max {
                break;
            }
            // Every link of this hop, each memory valued by its best one. A hop the budget
            // can't hold whole is dropped, so the hops kept are exact.
            let mut reached: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
            let mut read = 0u64;
            for (q, vq) in &level {
                for fam in [LINKS, BACKLINKS] {
                    // With nothing left, a budget of 1 still cuts at the first node (and
                    // only if there is one); 0 would mean no budget.
                    let left = budget.map_or(0, |b| b.saturating_sub(read).max(1));
                    let (edges, cut) = self.rows.edges(s, q, fam, left)?;
                    if cut {
                        stats.expansion_truncated = true;
                        stats.expanded = graphed;
                        return Ok(());
                    }
                    read += edges
                        .iter()
                        .map(|(k, _)| k.len() as u64 + ITEM_OVERHEAD)
                        .sum::<u64>();
                    for (n, w) in edges {
                        if visited.contains(&n) || ineligible.contains(&n) {
                            continue;
                        }
                        let v = vq * f64::from(w);
                        let e = reached.entry(n).or_insert(v);
                        if v > *e {
                            *e = v;
                        }
                    }
                }
            }
            if let Some(b) = budget.as_mut() {
                *b = b.saturating_sub(read);
            }
            // The recall's own filters: what it couldn't return is neither scored nor walked
            // through.
            let unknown: Vec<&Vec<u8>> = reached.keys().filter(|k| !at.contains_key(*k)).collect();
            let mut fresh: BTreeMap<Vec<u8>, Cand> = BTreeMap::new();
            if !unknown.is_empty() {
                let set = KeySet::from_unsorted(unknown.iter().copied());
                self.rows.read_each(s, &set, |key, row| {
                    if let Some(c) = self.candidate(key, row, spec, now)? {
                        fresh.insert(key.to_vec(), c);
                    }
                    Ok(())
                })?;
                for k in unknown {
                    if !fresh.contains_key(k) {
                        ineligible.insert(k.clone());
                    }
                }
            }
            let mut l: Vec<(Vec<u8>, f64)> = reached
                .into_iter()
                .filter(|(k, _)| !ineligible.contains(k))
                .collect();
            l.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            // The best-valued ones, up to `max` in all.
            if l.len() > x.max - graphed {
                l.truncate(x.max - graphed);
                stats.expansion_truncated = true;
            }
            for (n, v) in &l {
                let i = match at.get(n) {
                    Some(&i) => i,
                    None => {
                        let c = fresh.remove(n).expect("an eligible row was read");
                        at.insert(n.clone(), cands.len());
                        cands.push(c);
                        cands.len() - 1
                    }
                };
                cands[i].vals[5] = Some(*v);
                visited.insert(n.clone());
            }
            graphed += l.len();
            if l.is_empty() {
                break;
            }
            level = l;
        }
        stats.expanded = graphed;
        Ok(())
    }

    /// One candidate from its row, or `None` if it's filtered out or gone. Its keyword
    /// signal is filled in by the caller.
    fn candidate(
        &self,
        key: &[u8],
        row: &pigeonhole::RowRef<'_>,
        spec: Option<&VectorSpec>,
        now: u64,
    ) -> Result<Option<Cand>> {
        let Some(id) = MemoryId::from_row(key) else {
            return Ok(None);
        };
        let Some(text) = row.get(CONTENT, TEXT) else {
            return Ok(None);
        };
        let superseded_by = row
            .get(META, SUPERSEDED_BY)
            .map(|c| MemoryId::from_row(c.value()).ok_or_else(|| corrupt("superseded_by")))
            .transpose()?;
        if !self.include_superseded && superseded_by.is_some() {
            return Ok(None);
        }
        if let Some(want) = &self.session
            && !row
                .get(META, SESSION)
                .is_some_and(|c| c.value() == want.as_bytes())
        {
            return Ok(None);
        }
        let text = std::str::from_utf8(text.value())
            .map_err(|_| corrupt("a memory's text"))?
            .to_owned();
        let kind = match row.get(META, KIND).and_then(|c| c.value().first().copied()) {
            Some(1) => Kind::Semantic,
            Some(2) => Kind::Procedural,
            _ => Kind::Episodic,
        };
        let importance = row
            .get(PEAK, IMPORTANCE)
            .and_then(|c| c.as_i64())
            .map_or(0.0, |p| p as f64 / PPM);
        let importance_signal = if row.get(META, CONSOLIDATED).is_some() {
            importance * self.weights.consolidated
        } else {
            importance
        };
        let uses = row
            .get(STATS, USES)
            .and_then(|c| c.as_i64())
            .unwrap_or(0)
            .max(0);
        let last_used = row.get(PEAK, LAST_USED).and_then(|c| c.as_i64());
        let stored = row.get(EMB, V);
        let vector = match (&self.embedding, spec, &stored) {
            (Some(q), Some(spec), Some(c)) => match c.as_vector() {
                Some(v) => Some(1.0 - similarity_distance(spec, q, v)?),
                None => None,
            },
            _ => None,
        };
        let embedding = if self.with_embeddings {
            stored.and_then(|c| c.as_vector().and_then(|v| v.to_f32_vec()))
        } else {
            None
        };
        let created = id.created_us();
        let age = now.saturating_sub(created) as f64;
        let recency = (-age / self.half_life_us as f64).exp();
        let used = (uses > 0).then(|| {
            let since = last_used.map_or(age, |l| now.saturating_sub(l.max(0) as u64) as f64);
            (1.0 + uses as f64).ln() * (-since / self.half_life_us as f64).exp()
        });
        Ok(Some(Cand {
            key: key.to_vec(),
            vals: [
                vector,
                None,
                Some(recency),
                Some(importance_signal),
                used,
                None,
            ],
            ranks: [None; 6],
            score: 0.0,
            hit: RecallHit {
                id,
                text,
                kind,
                importance,
                created_us: created,
                superseded_by,
                score: 0.0,
                signals: None,
                embedding,
            },
        }))
    }
}

/// A linked memory's row and the link's weight.
type Edge = (Vec<u8>, f32);

struct Cand {
    /// Its row.
    key: Vec<u8>,
    /// vector, keyword, recency, importance, use, graph.
    vals: [Option<f64>; 6],
    ranks: [Option<u32>; 6],
    score: f64,
    hit: RecallHit,
}

/// Ranks every signal among `cands` and adds each one's weighted term to their scores, then
/// sorts them best first (ties by id).
fn fuse(cands: &mut [Cand], w: &[f64; 6], fusion: Fusion) {
    for (sig, &w) in w.iter().enumerate() {
        let mut order: Vec<usize> = (0..cands.len())
            .filter(|&i| cands[i].vals[sig].is_some())
            .collect();
        order.sort_by(|&a, &b| {
            cands[b].vals[sig]
                .unwrap()
                .total_cmp(&cands[a].vals[sig].unwrap())
                .then_with(|| cands[a].hit.id.cmp(&cands[b].hit.id))
        });
        // Competition ranking: equal values share a rank (1 + how many are strictly
        // greater), so a tie (every importance at its default, say) gives no candidate an
        // edge.
        for (r, &i) in order.iter().enumerate() {
            let rank = if r > 0 && cands[order[r - 1]].vals[sig] == cands[i].vals[sig] {
                cands[order[r - 1]].ranks[sig].expect("ranked above")
            } else {
                r as u32 + 1
            };
            cands[i].ranks[sig] = Some(rank);
        }
        match fusion {
            Fusion::Rrf => {
                for &i in &order {
                    let r = f64::from(cands[i].ranks[sig].expect("ranked"));
                    cands[i].score += w / (RRF_K + r);
                }
            }
            Fusion::Linear => {
                let (lo, hi) =
                    order
                        .iter()
                        .fold((f64::INFINITY, f64::NEG_INFINITY), |(l, h), &i| {
                            let v = cands[i].vals[sig].expect("ranked");
                            (l.min(v), h.max(v))
                        });
                for &i in &order {
                    let v = cands[i].vals[sig].expect("ranked");
                    let norm = if hi > lo { (v - lo) / (hi - lo) } else { 1.0 };
                    cands[i].score += w * norm;
                }
            }
        }
    }
    cands.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.hit.id.cmp(&b.hit.id))
    });
}

/// A hit's size for the byte budget: its text, its id's bytes and 16, plus its signals and
/// embedding when they're returned.
pub(crate) fn hit_size(hit: &RecallHit, with_signals: bool) -> u64 {
    hit.text.len() as u64
        + hit.id.to_bytes().len() as u64
        + ITEM_OVERHEAD
        + if with_signals { SIGNALS_BYTES } else { 0 }
        + hit.embedding.as_ref().map_or(0, |e| 4 * e.len() as u64)
}

/// The cosine distance the store's family ranks by.
fn similarity_distance(spec: &VectorSpec, q: &[f32], v: pigeonhole::VectorRef<'_>) -> Result<f64> {
    Ok(spec.distance_f32(q, v)?)
}

impl Rows<'_> {
    /// A counter cell's value at `s`.
    pub(crate) fn counter_at(
        &self,
        s: &Snapshot,
        row: &[u8],
        family: &str,
        q: &[u8],
    ) -> Result<Option<i64>> {
        Ok(match self {
            Rows::W(t) => t.get_at(s, row, family, q)?.and_then(|c| c.as_i64()),
            Rows::R(t) => t.get_at(s, row, family, q)?.and_then(|c| c.as_i64()),
        })
    }

    pub(crate) fn name(&self) -> &str {
        match self {
            Rows::W(t) => t.name(),
            Rows::R(t) => t.name(),
        }
    }

    pub(crate) fn has_family(&self, f: &str) -> bool {
        match self {
            Rows::W(t) => t.family_info(f).is_some(),
            Rows::R(t) => t.family_info(f).is_some(),
        }
    }

    /// A `G` row column's value.
    pub(crate) fn global(&self, q: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(match self {
            Rows::W(t) => t.get(GLOBAL, META, q)?.map(|c| c.value().to_vec()),
            Rows::R(t) => t.get(GLOBAL, META, q)?.map(|c| c.value().to_vec()),
        })
    }

    /// A `G` row column's `u64`.
    pub(crate) fn global_u64(&self, q: &[u8]) -> Result<Option<u64>> {
        Ok(match self {
            Rows::W(t) => t.get(GLOBAL, META, q)?.and_then(|c| c.as_u64()),
            Rows::R(t) => t.get(GLOBAL, META, q)?.and_then(|c| c.as_u64()),
        })
    }

    /// Calls `f` with every row, at `s`.
    pub(crate) fn scan_all(
        &self,
        s: &Snapshot,
        mut f: impl FnMut(&[u8], &pigeonhole::RowRef<'_>) -> Result<()>,
    ) -> Result<()> {
        macro_rules! go {
            ($t:expr) => {{
                let mut it = $t.scan_prefix(b"").snapshot(s).iter()?;
                while let Some(r) = it.next_ref()? {
                    let key = r.key().to_vec();
                    f(&key, &r)?;
                }
            }};
        }
        match self {
            Rows::W(t) => go!(t),
            Rows::R(t) => go!(t),
        }
        Ok(())
    }

    /// The `n` nearest memory rows under `prefix`.
    fn nearest(&self, s: &Snapshot, prefix: &[u8], q: &[f32], n: usize) -> Result<Vec<Vec<u8>>> {
        let found = match self {
            Rows::W(t) => t
                .scan_prefix(prefix)
                .family(EMB)
                .snapshot(s)
                .nearest_f32(q, n)
                .run()?,
            Rows::R(t) => t
                .scan_prefix(prefix)
                .family(EMB)
                .snapshot(s)
                .nearest_f32(q, n)
                .run()?,
        };
        Ok(found.into_iter().map(|n| n.row).collect())
    }

    /// The newest `n` memory rows under `prefix` (rids sort newest first).
    fn newest(&self, s: &Snapshot, prefix: &[u8], n: usize) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        macro_rules! go {
            ($t:expr) => {{
                let mut it = $t
                    .scan_prefix(prefix)
                    .family(CONTENT)
                    .snapshot(s)
                    .limit(n as u64)
                    .iter()?;
                while let Some(r) = it.next_ref()? {
                    out.push(r.key().to_vec());
                }
            }};
        }
        match self {
            Rows::W(t) => go!(t),
            Rows::R(t) => go!(t),
        }
        Ok(out)
    }

    /// The memories `from` links to (`LINKS`) or that link to it (`BACKLINKS`), at `s`,
    /// through a one-hop [`Traverse`](pigeonhole::Traverse) with `budget` (0: none), each
    /// with its link's weight, in key order; and whether the budget cut them. The weights
    /// come from `from`'s cells over the range the traversal returned.
    fn edges(
        &self,
        s: &Snapshot,
        from: &[u8],
        fam: &str,
        budget: u64,
    ) -> Result<(Vec<Edge>, bool)> {
        macro_rules! go {
            ($t:expr) => {{
                let t = $t
                    .traverse(&KeySet::from_unsorted([from]))
                    .family(fam)
                    .snapshot(s)
                    .byte_budget(budget)
                    .run()?;
                let cut = t.stats().cut_by_budget;
                let nodes: Vec<Vec<u8>> = t
                    .hops()
                    .first()
                    .map(|h| h.nodes().iter().map(<[u8]>::to_vec).collect())
                    .unwrap_or_default();
                let (Some(first), Some(last)) = (nodes.first(), nodes.last()) else {
                    return Ok((Vec::new(), cut));
                };
                let mut weights: BTreeMap<Vec<u8>, f32> = BTreeMap::new();
                if let Some(row) = $t
                    .row(from)
                    .family(fam)
                    .qualifier_bounds(Bound::Included(first), Bound::Included(last))
                    .snapshot(s)
                    .read()?
                {
                    for e in row.iter() {
                        let v = e.cell.value();
                        if v.len() != 5 {
                            return Err(corrupt("a link's value"));
                        }
                        let w = f32::from_le_bytes(v[1..].try_into().expect("4"));
                        weights.insert(e.qualifier.to_vec(), w);
                    }
                }
                let mut out = Vec::with_capacity(nodes.len());
                for n in nodes {
                    let w = *weights
                        .get(&n)
                        .ok_or_else(|| corrupt("a link the traversal read"))?;
                    out.push((n, w));
                }
                Ok((out, cut))
            }};
        }
        match self {
            Rows::W(t) => go!(t),
            Rows::R(t) => go!(t),
        }
    }

    /// Calls `f` with each row of `keys` that has a cell, at `s`.
    fn read_each(
        &self,
        s: &Snapshot,
        keys: &KeySet,
        mut f: impl FnMut(&[u8], &pigeonhole::RowRef<'_>) -> Result<()>,
    ) -> Result<()> {
        macro_rules! go {
            ($t:expr) => {{
                let mut rows = $t
                    .keys(keys)
                    .families(
                        [CONTENT, META, STATS, PEAK, EMB]
                            .into_iter()
                            .filter(|f| $t.family_info(f).is_some()),
                    )
                    .snapshot(s)
                    .read()?;
                while let Some((_, row)) = rows.next()? {
                    let key = row.key().to_vec();
                    f(&key, &row)?;
                }
            }};
        }
        match self {
            Rows::W(t) => go!(t),
            Rows::R(t) => go!(t),
        }
        Ok(())
    }
}

impl Idx<'_> {
    /// The top `n` keyword hits under `prefix`, with their BM25 scores.
    fn search(
        &self,
        s: &Snapshot,
        text: &str,
        prefix: &[u8],
        n: usize,
    ) -> Result<Vec<(Vec<u8>, f64)>> {
        let q = Query::new().text(text);
        let hits = match self {
            Idx::W(i) => i.search(q, n).snapshot(s).id_prefix(prefix).run()?,
            Idx::R(i) => i.search(q, n).snapshot(s).id_prefix(prefix).run()?,
        };
        Ok(hits.into_iter().map(|h| (h.id, h.score)).collect())
    }
}

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

mod pool;
pub use pool::{RecallBodies, RecallHop, RecallHopPart, RecallPool, RecallRanking, RecallWalk};

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
    /// The store's namespace counts by snapshot (#2040); `None` in a reader process.
    counts: Option<&'a crate::ivf::NsCounts>,
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

/// What a recall's steps share, once its arguments are checked.
struct Plan<'p> {
    s: &'p Snapshot,
    /// The text to search for, if any.
    text: Option<&'p str>,
    /// The query embedding's spec, if there's one.
    spec: Option<VectorSpec>,
    now: u64,
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
        counts: Option<&'a crate::ivf::NsCounts>,
        model: Option<&'a EmbeddingModel>,
        db: Snap<'a>,
        namespaces: &[&str],
    ) -> Self {
        Recall {
            rows,
            index,
            ivf,
            counts,
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
    ///
    /// [`run_with_stats_async`](Self::run_with_stats_async) is its async twin: the same
    /// steps in the same order, sharing every decision (`Pass`, `Hop`, `Walk`), with each
    /// read's async form. A change to one goes into the other.
    pub fn run_with_stats(self) -> Result<(Vec<RecallHit>, RecallStats)> {
        let (text, spec) = self.check()?;
        let owned = self.own_snapshot()?;
        let s = self.snapshot.or(owned.as_ref()).expect("a snapshot");
        let plan = self.plan(s, text, spec);
        if self.k == 0 {
            return Ok((Vec::new(), RecallStats::default()));
        }
        // 1. Candidates, and the fusion.
        let mut ranking = self.pool(&plan)?.rank(self.fusion, self.weights);
        // 2. The graph expansion, seeded by the fused order without it.
        if let Some(x) = self.expand {
            let mut walk = ranking.walk(x, self.byte_budget);
            while let Some(hop) = walk.next_hop() {
                let part = self.hop_part(&plan, &hop)?;
                walk.settle([part])?;
            }
            ranking = walk.finish();
        }
        // 3. The hits, with the bodies read with their candidates.
        Ok(ranking.finish(
            RecallBodies::default(),
            self.k,
            self.byte_budget,
            self.with_signals,
        ))
    }

    /// Step 1 alone ([`RecallPool`]): this store's candidates for the recall, with every
    /// signal's raw value, unfused. A recall over namespaces in several stores (a cluster's
    /// partitions) merges each store's pool ([`RecallPool::merge`]) and ranks the whole
    /// ([`RecallPool::rank`]), which fuses as one store holding every namespace would.
    ///
    /// The same refusals as [`run`](Self::run). With `k` of 0 the pool is empty.
    pub fn gather(self) -> Result<RecallPool> {
        let (text, spec) = self.check()?;
        let owned = self.own_snapshot()?;
        let s = self.snapshot.or(owned.as_ref()).expect("a snapshot");
        let plan = self.plan(s, text, spec);
        if self.k == 0 {
            return Ok(RecallPool::default());
        }
        self.pool(&plan)
    }

    /// Step 3 alone ([`RecallBodies`]): the bodies of `ids` (a merged ranking's
    /// [`top`](RecallRanking::top)), read at the recall's snapshot through its filters. An
    /// id that's gone, or that the filters now leave out (superseded since, say), has no
    /// body, and [`RecallRanking::finish`] skips it.
    ///
    /// The same refusals as [`run`](Self::run), and an id outside the recall's namespaces
    /// is `InvalidArgument`.
    pub fn bodies(self, ids: &[MemoryId]) -> Result<RecallBodies> {
        let (text, spec) = self.check()?;
        for id in ids {
            if !self.namespaces.iter().any(|ns| ns == id.namespace()) {
                return Err(Error::InvalidArgument(format!(
                    "memory {id} isn't in the recall's namespaces"
                )));
            }
        }
        let owned = self.own_snapshot()?;
        let s = self.snapshot.or(owned.as_ref()).expect("a snapshot");
        let plan = self.plan(s, text, spec);
        let set = KeySet::from_unsorted(ids.iter().map(MemoryId::row));
        let mut read: BTreeMap<Vec<u8>, Cand> = BTreeMap::new();
        self.rows
            .read_each(s, &set, |key, row| self.admit(&mut read, key, row, &plan))?;
        Ok(RecallBodies::from_cands(read.into_values()))
    }

    /// One hop of a graph expansion alone ([`RecallWalk`]): the links of the hop's
    /// memories in this store, at the recall's snapshot, as [`Expand`] says, with the
    /// memories they reach that the recall could return scored like its other candidates.
    /// The walk ([`RecallRanking::walk`]) settles every store's part of the hop together,
    /// so its bounds ([`Expand::max`], the byte budget) are the one store's.
    ///
    /// The same refusals as [`run`](Self::run), and a hop from a memory outside the
    /// recall's namespaces is `InvalidArgument`.
    pub fn hop(self, hop: &RecallHop) -> Result<RecallHopPart> {
        let (text, spec) = self.check()?;
        self.check_hop(hop)?;
        let owned = self.own_snapshot()?;
        let s = self.snapshot.or(owned.as_ref()).expect("a snapshot");
        let plan = self.plan(s, text, spec);
        self.hop_part(&plan, hop)
    }

    /// A hop's memories are in the recall's namespaces.
    fn check_hop(&self, hop: &RecallHop) -> Result<()> {
        for ns in hop.namespaces() {
            if !self.namespaces.contains(&ns) {
                return Err(Error::InvalidArgument(format!(
                    "a recall hop from namespace {ns:?}, outside the recall's"
                )));
            }
        }
        Ok(())
    }

    /// Step 1: the pool, with its IVF counts.
    fn pool(&self, plan: &Plan<'_>) -> Result<RecallPool> {
        let ivf_counts = self.ivf_counts(plan.s)?;
        let (cands, fell_back) = self.gather_cands(plan, &ivf_counts)?;
        Ok(RecallPool::new(cands, &ivf_counts, &fell_back))
    }

    /// The snapshot the recall takes when none is given; `None` with one.
    fn own_snapshot(&self) -> Result<Option<Snapshot>> {
        if self.snapshot.is_some() {
            return Ok(None);
        }
        Ok(Some(match self.db {
            Snap::W(db) => db.snapshot()?,
            Snap::R(r) => r.snapshot()?,
        }))
    }

    fn plan<'p>(
        &self,
        s: &'p Snapshot,
        text: Option<&'p str>,
        spec: Option<VectorSpec>,
    ) -> Plan<'p> {
        Plan {
            s,
            text,
            spec,
            now: self.at_us.unwrap_or_else(now_us),
        }
    }

    /// The arguments' checks: the text to search for, if any, and the query embedding's
    /// spec, if there's one.
    fn check(&self) -> Result<(Option<&str>, Option<VectorSpec>)> {
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
        Ok((text, spec))
    }

    /// Each namespace's path for the vector signal: the IVF index once its counter (read
    /// at the recall's snapshot, as the search and the memories are) reaches the store's
    /// `min_namespace_len`. Not in the map: exact.
    fn ivf_counts(&self, s: &Snapshot) -> Result<BTreeMap<&str, u64>> {
        let Some(min) = self.ivf_min() else {
            return Ok(BTreeMap::new());
        };
        let at = self.counts_at(s);
        let mut counts = Vec::with_capacity(self.namespaces.len());
        for ns in &self.namespaces {
            counts.push(match self.cached_count(ns, at) {
                Some(c) => c,
                None => self.read_count(ns, at, crate::ivf::count(&self.rows, s, ns)?),
            });
        }
        Ok(self.over(min, counts))
    }

    /// The key a recall at `s` keeps its namespace counts under, or `None` when it keeps
    /// none (a reader process: a writer's restart can lose commits a reader saw and use their
    /// seqnos again, and a reader has no generation to tell them apart). Read before the
    /// counts: a recovery after it fails the reads at `s` instead of keeping them.
    fn counts_at(&self, s: &Snapshot) -> Option<crate::ivf::At> {
        match (self.counts, &self.db) {
            (Some(_), Snap::W(db)) => Some((db.recovery_stats().generation, s.seqno())),
            _ => None,
        }
    }

    /// `ns`'s count from an earlier recall at the same snapshot, if there is one (#2040).
    fn cached_count(&self, ns: &str, at: Option<crate::ivf::At>) -> Option<u64> {
        self.counts?.get(ns, at?)
    }

    /// `c`, `ns`'s count just read, kept for later recalls at the same snapshot.
    fn read_count(&self, ns: &str, at: Option<crate::ivf::At>, c: u64) -> u64 {
        if let (Some(counts), Some(at)) = (self.counts, at) {
            counts.put(ns, at, c);
        }
        c
    }

    /// The namespaces of `counts` (in the recall's order) that reach `min`, with their
    /// counts: the IVF index serves those.
    fn over(&self, min: u64, counts: Vec<u64>) -> BTreeMap<&str, u64> {
        self.namespaces
            .iter()
            .zip(counts)
            .filter(|&(_, c)| c >= min)
            .map(|(ns, c)| (ns.as_str(), c))
            .collect()
    }

    /// The namespace length from which the IVF index serves this recall, if it can.
    fn ivf_min(&self) -> Option<u64> {
        match (self.ivf, &self.embedding, self.exact) {
            (Some(ivf), Some(_), false) => Some(ivf.options().get_min_namespace_len()),
            _ => None,
        }
    }

    /// The IVF index and `ns`'s count, if its vector list comes from the index this pass:
    /// it's large enough, and hasn't fallen back to exact.
    fn ivf_for(
        &self,
        ns: &str,
        ivf_counts: &BTreeMap<&str, u64>,
        fell_back: &BTreeSet<&str>,
    ) -> Option<(&'a crate::ivf::Ivf, u64)> {
        match (self.ivf, ivf_counts.get(ns)) {
            (Some(ivf), Some(&c)) if !fell_back.contains(ns) => Some((ivf, c)),
            _ => None,
        }
    }

    /// Adds the candidate in `row`, if it passes the filters, to `into`.
    fn admit(
        &self,
        into: &mut BTreeMap<Vec<u8>, Cand>,
        key: &[u8],
        row: &pigeonhole::RowRef<'_>,
        plan: &Plan<'_>,
    ) -> Result<()> {
        if let Some(c) = self.candidate(key, row, plan.spec.as_ref(), plan.now)? {
            into.insert(key.to_vec(), c);
        }
        Ok(())
    }

    /// Step 1, the candidates: per namespace and signal, its best `n`, every namespace's
    /// rows read in one multi-get; the filters (superseded, session) apply to each. Until
    /// each of a namespace's lists holds `per_signal` that pass, or came back short (the
    /// namespace is exhausted), its `n` doubles, so a filtered recall still gets each
    /// signal's best `per_signal` eligible memories (simp724's N1 on #1374). Each namespace
    /// doubles its own `n`, so its candidates depend on it alone, and a recall's pool over
    /// several stores is the one store's (#1915). Also the namespaces whose IVF search fell
    /// back to exact.
    fn gather_cands(
        &self,
        plan: &Plan<'_>,
        ivf_counts: &BTreeMap<&str, u64>,
    ) -> Result<(Vec<Cand>, BTreeSet<&str>)> {
        let s = plan.s;
        let mut fell_back: BTreeSet<&str> = BTreeSet::new();
        let mut open = self.open_namespaces();
        let mut out = Vec::new();
        while !open.is_empty() {
            let mut passes = Vec::with_capacity(open.len());
            for &(ns, n) in &open {
                let mut pass = Pass::new(n);
                let prefix = [ns.as_bytes(), MEMORY_TAG].concat();
                if let Some(e) = &self.embedding {
                    let via_ivf = match self.ivf_for(ns, ivf_counts, &fell_back) {
                        Some((ivf, c)) => {
                            let found = ivf.nearest(s, &prefix, e, n, c, self.nprobe)?;
                            if found.is_none() {
                                fell_back.insert(ns);
                            }
                            found
                        }
                        None => None,
                    };
                    let l = match via_ivf {
                        Some(l) => l,
                        None => self.rows.nearest(s, &prefix, e, n)?,
                    };
                    pass.list(0, l);
                }
                if let Some(t) = plan.text {
                    pass.keyword(self.index.search(s, t, &prefix, n)?);
                }
                pass.list(2, self.rows.newest(s, &prefix, n)?);
                passes.push(pass);
            }
            let mut eligible: BTreeMap<Vec<u8>, Cand> = BTreeMap::new();
            self.rows
                .read_each(s, &Pass::all_keys(&passes), |key, row| {
                    self.admit(&mut eligible, key, row, plan)
                })?;
            open = self.settle_passes(open, passes, eligible, &mut out);
        }
        Ok((out, fell_back))
    }

    /// The recall's namespaces, each once, in order, at the first pass's `n`.
    fn open_namespaces(&self) -> Vec<(&str, usize)> {
        let mut seen = BTreeSet::new();
        self.namespaces
            .iter()
            .filter(|ns| seen.insert(ns.as_str()))
            .map(|ns| (ns.as_str(), self.per_signal.max(1)))
            .collect()
    }

    /// Settles each open namespace's pass, adding the settled ones' candidates to `out`:
    /// the namespaces still open, each at twice its `n`.
    fn settle_passes<'n>(
        &self,
        open: Vec<(&'n str, usize)>,
        passes: Vec<Pass>,
        mut eligible: BTreeMap<Vec<u8>, Cand>,
        out: &mut Vec<Cand>,
    ) -> Vec<(&'n str, usize)> {
        let mut next = Vec::new();
        for ((ns, n), pass) in open.into_iter().zip(passes) {
            let own = pass.own(&mut eligible);
            match pass.settle(own, self.per_signal) {
                Some(cands) => out.extend(cands),
                None => next.push((ns, n.saturating_mul(2))),
            }
        }
        next
    }

    /// One hop's part in this store ([`hop`](Self::hop)): each of the hop's memories'
    /// links and backlinks, read within the budget left (a part past it cuts, and so does
    /// the walk, as one store's hop would), then the rows they reach that aren't known yet,
    /// read through the recall's filters.
    fn hop_part(&self, plan: &Plan<'_>, req: &RecallHop) -> Result<RecallHopPart> {
        let s = plan.s;
        let mut hop = Hop::default();
        for (q, vq) in &req.level {
            for fam in [LINKS, BACKLINKS] {
                let (edges, cut) = self.rows.edges(s, q, fam, hop.left(req.left))?;
                if cut {
                    return Ok(RecallHopPart::cut());
                }
                hop.add(*vq, edges, &req.visited, &req.ineligible);
            }
        }
        let unknown = req.unknown(&hop);
        let mut fresh: BTreeMap<Vec<u8>, Cand> = BTreeMap::new();
        if !unknown.is_empty() {
            let set = KeySet::from_unsorted(unknown.iter());
            self.rows
                .read_each(s, &set, |key, row| self.admit(&mut fresh, key, row, plan))?;
        }
        Ok(RecallHopPart::new(hop, &unknown, fresh))
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
            id,
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
            body: Some(Body {
                text,
                kind,
                importance,
                superseded_by,
                embedding,
            }),
        }))
    }
}

/// A linked memory's row and the link's weight.
type Edge = (Vec<u8>, f32);

pub(crate) struct Cand {
    /// Its row.
    pub(crate) key: Vec<u8>,
    /// Its id, from `key`.
    pub(crate) id: MemoryId,
    /// vector, keyword, recency, importance, use, graph.
    pub(crate) vals: [Option<f64>; 6],
    pub(crate) ranks: [Option<u32>; 6],
    pub(crate) score: f64,
    /// What a hit returns besides its scores: read with the candidate in one store, and
    /// `None` in a pool from bytes ([`RecallPool::from_bytes`]) until its bodies come.
    pub(crate) body: Option<Body>,
}

/// A candidate's body: what its hit returns besides its id and scores.
#[derive(Clone, PartialEq)]
pub(crate) struct Body {
    pub(crate) text: String,
    pub(crate) kind: Kind,
    pub(crate) importance: f64,
    pub(crate) superseded_by: Option<MemoryId>,
    pub(crate) embedding: Option<Vec<f32>>,
}

impl Body {
    /// The hit, scored, for memory `id`.
    fn hit(self, id: MemoryId, score: f64) -> RecallHit {
        RecallHit {
            created_us: id.created_us(),
            id,
            text: self.text,
            kind: self.kind,
            importance: self.importance,
            superseded_by: self.superseded_by,
            score,
            signals: None,
            embedding: self.embedding,
        }
    }
}

/// One namespace's pass of the gather at `n` per list: its lists in order (vector,
/// keyword, recency), each with whether it came back full, and the keyword scores. Both
/// forms of the recall fill it in the same order and settle it here.
struct Pass {
    n: usize,
    lists: Vec<(usize, Vec<Vec<u8>>, bool)>,
    scores: BTreeMap<Vec<u8>, f64>,
}

impl Pass {
    fn new(n: usize) -> Self {
        Pass {
            n,
            lists: Vec::new(),
            scores: BTreeMap::new(),
        }
    }

    /// Signal `sig`'s list.
    fn list(&mut self, sig: usize, l: Vec<Vec<u8>>) {
        let full = l.len() == self.n;
        self.lists.push((sig, l, full));
    }

    /// The keyword list, with its BM25 scores.
    fn keyword(&mut self, hits: Vec<(Vec<u8>, f64)>) {
        let mut l = Vec::with_capacity(hits.len());
        for (id, score) in hits {
            self.scores.insert(id.clone(), score);
            l.push(id);
        }
        self.list(1, l);
    }

    /// Every pass's rows, for one multi-get.
    fn all_keys(passes: &[Pass]) -> KeySet {
        let keys: BTreeSet<&Vec<u8>> = passes
            .iter()
            .flat_map(|p| p.lists.iter().flat_map(|(_, l, _)| l.iter()))
            .collect();
        KeySet::from_unsorted(keys.into_iter().collect::<Vec<_>>())
    }

    /// This pass's rows of `eligible`, taken out of it.
    fn own(&self, eligible: &mut BTreeMap<Vec<u8>, Cand>) -> BTreeMap<Vec<u8>, Cand> {
        let mut own = BTreeMap::new();
        for (_, l, _) in &self.lists {
            for k in l {
                if let Some(c) = eligible.remove(k) {
                    own.insert(k.clone(), c);
                }
            }
        }
        own
    }

    /// The candidates, once every list holds `per_signal` that pass the filters or came back
    /// short (its namespace is exhausted): each list's best `per_signal` eligible memories,
    /// together. `None`: a list is full with fewer, so `n` doubles.
    fn settle(self, mut eligible: BTreeMap<Vec<u8>, Cand>, per_signal: usize) -> Option<Vec<Cand>> {
        let done = self.lists.iter().all(|(_, l, full)| {
            !full || l.iter().filter(|k| eligible.contains_key(*k)).count() >= per_signal
        });
        if !done {
            return None;
        }
        // BM25 counts for the keyword lists' kept keys only (below).
        for c in eligible.values_mut() {
            c.vals[1] = None;
        }
        for (sig, l, _) in &self.lists {
            if *sig == 1 {
                let top: Vec<&Vec<u8>> = l
                    .iter()
                    .filter(|k| eligible.contains_key(*k))
                    .take(per_signal)
                    .collect();
                for k in top {
                    if let Some(c) = eligible.get_mut(k) {
                        c.vals[1] = self.scores.get(k).copied();
                    }
                }
            }
        }
        let mut kept: BTreeSet<&Vec<u8>> = BTreeSet::new();
        for (_, l, _) in &self.lists {
            for k in l
                .iter()
                .filter(|k| eligible.contains_key(*k))
                .take(per_signal)
            {
                kept.insert(k);
            }
        }
        let kept: BTreeSet<Vec<u8>> = kept.into_iter().cloned().collect();
        Some(
            eligible
                .into_iter()
                .filter(|(k, _)| kept.contains(k))
                .map(|(_, c)| c)
                .collect(),
        )
    }
}

/// A graph expansion's state between hops ([`RecallWalk`]), on the store or the entry node.
struct Walk {
    /// Each candidate's index in `cands`.
    at: BTreeMap<Vec<u8>, usize>,
    /// Seeds and earlier hops' memories: not reached again.
    visited: BTreeSet<Vec<u8>>,
    /// Rows the recall couldn't return: neither scored nor walked through.
    ineligible: BTreeSet<Vec<u8>>,
    budget: Option<u64>,
    graphed: usize,
    max: usize,
    /// The memories the next hop walks from, each with its value, best first.
    level: Vec<(Vec<u8>, f64)>,
}

/// One hop's links: every memory it reaches, valued by its best link, and the bytes read.
#[derive(Default)]
pub(crate) struct Hop {
    reached: BTreeMap<Vec<u8>, f64>,
    read: u64,
}

impl Hop {
    /// The budget for the hop's next `edges`: what's left of `budget` after this hop's
    /// reads. With nothing left, a budget of 1 still cuts at the first node (and only if
    /// there is one); 0 would mean no budget.
    fn left(&self, budget: Option<u64>) -> u64 {
        budget.map_or(0, |b| b.saturating_sub(self.read).max(1))
    }

    /// The links from a memory valued `vq`.
    fn add(
        &mut self,
        vq: f64,
        edges: Vec<Edge>,
        visited: &BTreeSet<Vec<u8>>,
        ineligible: &BTreeSet<Vec<u8>>,
    ) {
        self.read += edges
            .iter()
            .map(|(k, _)| k.len() as u64 + ITEM_OVERHEAD)
            .sum::<u64>();
        for (n, w) in edges {
            if visited.contains(&n) || ineligible.contains(&n) {
                continue;
            }
            let v = vq * f64::from(w);
            let e = self.reached.entry(n).or_insert(v);
            if v > *e {
                *e = v;
            }
        }
    }
}

impl Walk {
    fn new(cands: &[Cand], seeds: Vec<(Vec<u8>, f64)>, x: Expand, budget: Option<u64>) -> Self {
        Walk {
            at: cands
                .iter()
                .enumerate()
                .map(|(i, c)| (c.key.clone(), i))
                .collect(),
            visited: seeds.iter().map(|(k, _)| k.clone()).collect(),
            ineligible: BTreeSet::new(),
            budget,
            graphed: 0,
            max: x.max,
            level: seeds,
        }
    }

    /// Whether `max` memories have their graph signal.
    fn full(&self) -> bool {
        self.graphed == self.max
    }

    /// A hop the budget can't hold whole is dropped, so the hops kept are exact.
    fn cut(&self, stats: &mut RecallStats) {
        stats.expansion_truncated = true;
        stats.expanded = self.graphed;
    }

    /// Ends a hop: its best-valued memories, up to `max` in all, get their graph values
    /// (`fresh` holds the new candidates' rows) and are the next hop's level. `false`: it
    /// reached none, so the walk ends.
    fn settle(
        &mut self,
        hop: Hop,
        mut fresh: BTreeMap<Vec<u8>, Cand>,
        cands: &mut Vec<Cand>,
        stats: &mut RecallStats,
    ) -> Result<bool> {
        if let Some(b) = self.budget.as_mut() {
            *b = b.saturating_sub(hop.read);
        }
        let mut l: Vec<(Vec<u8>, f64)> = hop
            .reached
            .into_iter()
            .filter(|(k, _)| !self.ineligible.contains(k))
            .collect();
        l.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        if l.len() > self.max - self.graphed {
            l.truncate(self.max - self.graphed);
            stats.expansion_truncated = true;
        }
        for (n, v) in &l {
            let i = match self.at.get(n) {
                Some(&i) => i,
                None => {
                    let c = fresh.remove(n).ok_or_else(|| {
                        Error::InvalidArgument(
                            "a recall hop reached a memory it didn't read".into(),
                        )
                    })?;
                    self.at.insert(n.clone(), cands.len());
                    cands.push(c);
                    cands.len() - 1
                }
            };
            cands[i].vals[5] = Some(*v);
            self.visited.insert(n.clone());
        }
        self.graphed += l.len();
        if l.is_empty() {
            return Ok(false);
        }
        self.level = l;
        Ok(true)
    }
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
                .then_with(|| cands[a].id.cmp(&cands[b].id))
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
    cands.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
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

/// A one-hop traversal's nodes, and whether its budget cut them.
fn linked(t: &pigeonhole::Traversal) -> (Vec<Vec<u8>>, bool) {
    let nodes = t
        .hops()
        .first()
        .map(|h| h.nodes().iter().map(<[u8]>::to_vec).collect())
        .unwrap_or_default();
    (nodes, t.stats().cut_by_budget)
}

/// Each of `nodes` with its link's weight, from the linking memory's `row` over the range
/// the traversal returned.
fn weighted(nodes: Vec<Vec<u8>>, row: Option<&pigeonhole::RowRef<'_>>) -> Result<Vec<Edge>> {
    let mut weights: BTreeMap<Vec<u8>, f32> = BTreeMap::new();
    if let Some(row) = row {
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
    Ok(out)
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
                let (nodes, cut) = linked(&t);
                let (Some(first), Some(last)) = (nodes.first(), nodes.last()) else {
                    return Ok((Vec::new(), cut));
                };
                let row = $t
                    .row(from)
                    .family(fam)
                    .qualifier_bounds(Bound::Included(first), Bound::Included(last))
                    .snapshot(s)
                    .read()?;
                Ok((weighted(nodes, row.as_ref())?, cut))
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

/// The async recall (#1560, ICR 0124): [`Recall::run_with_stats`]'s steps in the same order,
/// with each read's async form and the same shared decisions (`Pass`, `Hop`, `Walk`).
#[cfg(feature = "async")]
impl<'a> Recall<'a> {
    /// [`run`](Self::run) as a future: the same hits, with the recall's reads asynchronous,
    /// so it doesn't block an executor thread on the cache's misses. Each namespace's
    /// vector, keyword and recency lists are read together. A poll yields after 256 rows
    /// of its own (its recency lists' and candidates' rows), so a warm recall over a large
    /// namespace doesn't hold the thread either. The future is `Send`.
    ///
    /// The same refusals as `run`. It works in a reader process
    /// ([`ReadStore::recall`](crate::ReadStore::recall)) too: every read a recall makes has
    /// an async form there, so none is refused.
    pub async fn run_async(self) -> Result<Vec<RecallHit>> {
        Ok(self.run_with_stats_async().await?.0)
    }

    /// [`run_with_stats`](Self::run_with_stats) as a future: [`run_async`](Self::run_async),
    /// with what the recall met. The same stats as the sync form.
    ///
    /// The async twin of `run_with_stats`: a change to one goes into the other.
    pub async fn run_with_stats_async(self) -> Result<(Vec<RecallHit>, RecallStats)> {
        let (text, spec) = self.check()?;
        let owned = self.own_snapshot()?;
        let s = self.snapshot.or(owned.as_ref()).expect("a snapshot");
        let plan = self.plan(s, text, spec);
        if self.k == 0 {
            return Ok((Vec::new(), RecallStats::default()));
        }
        // The row loops' yield budget, shared by every list and multi-get (ai1249's R1).
        let rows = Yields::new();
        // 1. Candidates, and the fusion.
        let ivf_counts = self.ivf_counts_async(s).await?;
        let (cands, fell_back) = self.gather_cands_async(&plan, &ivf_counts, &rows).await?;
        let mut ranking =
            RecallPool::new(cands, &ivf_counts, &fell_back).rank(self.fusion, self.weights);
        // 2. The graph expansion, seeded by the fused order without it.
        if let Some(x) = self.expand {
            let mut walk = ranking.walk(x, self.byte_budget);
            while let Some(hop) = walk.next_hop() {
                let part = self.hop_part_async(&plan, &hop, &rows).await?;
                walk.settle([part])?;
            }
            ranking = walk.finish();
        }
        // 3. The hits, with the bodies read with their candidates.
        Ok(ranking.finish(
            RecallBodies::default(),
            self.k,
            self.byte_budget,
            self.with_signals,
        ))
    }

    /// [`ivf_counts`](Self::ivf_counts)' async twin.
    async fn ivf_counts_async(&self, s: &Snapshot) -> Result<BTreeMap<&str, u64>> {
        let Some(min) = self.ivf_min() else {
            return Ok(BTreeMap::new());
        };
        let at = self.counts_at(s);
        let mut counts = Vec::with_capacity(self.namespaces.len());
        for ns in &self.namespaces {
            counts.push(match self.cached_count(ns, at) {
                Some(c) => c,
                None => {
                    let c = crate::ivf::count_async(&self.rows, s, ns).await?;
                    self.read_count(ns, at, c)
                }
            });
        }
        Ok(self.over(min, counts))
    }

    /// [`gather_cands`](Self::gather_cands)' async twin: per namespace, its vector, keyword
    /// and recency lists are read together, and added to its pass in the sync form's order.
    async fn gather_cands_async(
        &self,
        plan: &Plan<'_>,
        ivf_counts: &BTreeMap<&str, u64>,
        rows: &Yields,
    ) -> Result<(Vec<Cand>, BTreeSet<&str>)> {
        let s = plan.s;
        let mut fell_back: BTreeSet<&str> = BTreeSet::new();
        let mut open = self.open_namespaces();
        let mut out = Vec::new();
        while !open.is_empty() {
            let mut passes = Vec::with_capacity(open.len());
            for &(ns, n) in &open {
                let mut pass = Pass::new(n);
                let prefix = [ns.as_bytes(), MEMORY_TAG].concat();
                let ivf = self.ivf_for(ns, ivf_counts, &fell_back);
                let vector = async {
                    let Some(e) = &self.embedding else {
                        return Ok(None);
                    };
                    let via_ivf = match ivf {
                        Some((ivf, c)) => {
                            ivf.nearest_async(s, &prefix, e, n, c, self.nprobe).await?
                        }
                        None => None,
                    };
                    let fell = ivf.is_some() && via_ivf.is_none();
                    let l = match via_ivf {
                        Some(l) => l,
                        None => self.rows.nearest_async(s, &prefix, e, n).await?,
                    };
                    Ok::<_, Error>(Some((l, fell)))
                };
                let keyword = async {
                    match plan.text {
                        Some(t) => self.index.search_async(s, t, &prefix, n).await.map(Some),
                        None => Ok(None),
                    }
                };
                let newest = self.rows.newest_async(s, &prefix, n, rows);
                let (vector, keyword, newest) = join3(vector, keyword, newest).await;
                if let Some((l, fell)) = vector? {
                    if fell {
                        fell_back.insert(ns);
                    }
                    pass.list(0, l);
                }
                if let Some(hits) = keyword? {
                    pass.keyword(hits);
                }
                pass.list(2, newest?);
                passes.push(pass);
            }
            let mut eligible: BTreeMap<Vec<u8>, Cand> = BTreeMap::new();
            self.rows
                .read_each_async(s, &Pass::all_keys(&passes), rows, |key, row| {
                    self.admit(&mut eligible, key, row, plan)
                })
                .await?;
            open = self.settle_passes(open, passes, eligible, &mut out);
        }
        Ok((out, fell_back))
    }

    /// [`hop_part`](Self::hop_part)'s async twin: the same budget arithmetic ([`Hop`]),
    /// link by link.
    async fn hop_part_async(
        &self,
        plan: &Plan<'_>,
        req: &RecallHop,
        rows: &Yields,
    ) -> Result<RecallHopPart> {
        let s = plan.s;
        let mut hop = Hop::default();
        for (q, vq) in &req.level {
            for fam in [LINKS, BACKLINKS] {
                let left = hop.left(req.left);
                let (edges, cut) = self.rows.edges_async(s, q, fam, left).await?;
                if cut {
                    return Ok(RecallHopPart::cut());
                }
                hop.add(*vq, edges, &req.visited, &req.ineligible);
            }
        }
        let unknown = req.unknown(&hop);
        let mut fresh: BTreeMap<Vec<u8>, Cand> = BTreeMap::new();
        if !unknown.is_empty() {
            let set = KeySet::from_unsorted(unknown.iter());
            self.rows
                .read_each_async(s, &set, rows, |key, row| {
                    self.admit(&mut fresh, key, row, plan)
                })
                .await?;
        }
        Ok(RecallHopPart::new(hop, &unknown, fresh))
    }
}

/// Rows an async recall reads and handles in one poll before it yields: its recency
/// lists' and multi-gets' rows together (ai1249's R1 on #1654). A filtered recall's
/// doubling can grow a list to its namespace's size, so without it a warm recall over a
/// large namespace would read three lists' worth of rows in one poll. The reads it calls
/// (the vector, text and traversal searches) yield on their own budgets.
#[cfg(feature = "async")]
const RECALL_POLL_ROWS: u32 = 256;

/// An async recall's shared row budget ([`RECALL_POLL_ROWS`]): an atomic, so the joined
/// lists share it and the future stays `Send`.
#[cfg(feature = "async")]
struct Yields(std::sync::atomic::AtomicU32);

#[cfg(feature = "async")]
impl Yields {
    fn new() -> Self {
        Yields(std::sync::atomic::AtomicU32::new(RECALL_POLL_ROWS))
    }

    /// Counts a row; after [`RECALL_POLL_ROWS`] of them, wakes the task and returns
    /// `Pending` once, with the budget renewed.
    async fn row(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        if self.0.fetch_sub(1, Relaxed) > 1 {
            return;
        }
        self.0.store(RECALL_POLL_ROWS, Relaxed);
        let mut yielded = false;
        std::future::poll_fn(|cx| {
            if yielded {
                return std::task::Poll::Ready(());
            }
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        })
        .await;
    }
}

/// Polls three futures together until each is done, each in turn on every wake, and
/// returns their outputs in order: the recall's "awaited together" without a dependency
/// for it (ai1249 on #1560).
#[cfg(feature = "async")]
async fn join3<A: Future, B: Future, C: Future>(
    a: A,
    b: B,
    c: C,
) -> (A::Output, B::Output, C::Output) {
    let (mut a, mut b, mut c) = (std::pin::pin!(a), std::pin::pin!(b), std::pin::pin!(c));
    let (mut ra, mut rb, mut rc) = (None, None, None);
    std::future::poll_fn(move |cx| {
        if ra.is_none()
            && let std::task::Poll::Ready(v) = a.as_mut().poll(cx)
        {
            ra = Some(v);
        }
        if rb.is_none()
            && let std::task::Poll::Ready(v) = b.as_mut().poll(cx)
        {
            rb = Some(v);
        }
        if rc.is_none()
            && let std::task::Poll::Ready(v) = c.as_mut().poll(cx)
        {
            rc = Some(v);
        }
        if ra.is_some() && rb.is_some() && rc.is_some() {
            std::task::Poll::Ready((ra.take().unwrap(), rb.take().unwrap(), rc.take().unwrap()))
        } else {
            std::task::Poll::Pending
        }
    })
    .await
}

#[cfg(feature = "async")]
impl Rows<'_> {
    /// [`counter_at`](Self::counter_at), async.
    pub(crate) async fn counter_at_async(
        &self,
        s: &Snapshot,
        row: &[u8],
        family: &str,
        q: &[u8],
    ) -> Result<Option<i64>> {
        let get = match self {
            Rows::W(t) => t.get_at_async(s, row, family, q),
            Rows::R(t) => t.get_at_async(s, row, family, q),
        };
        Ok(get.await?.and_then(|c| c.as_i64()))
    }

    /// [`nearest`](Self::nearest), async.
    async fn nearest_async(
        &self,
        s: &Snapshot,
        prefix: &[u8],
        q: &[f32],
        n: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let scan = match self {
            Rows::W(t) => t.scan_prefix(prefix),
            Rows::R(t) => t.scan_prefix(prefix),
        };
        let found = scan
            .family(EMB)
            .snapshot(s)
            .nearest_f32(q, n)
            .run_async()
            .await?;
        Ok(found.into_iter().map(|n| n.row).collect())
    }

    /// [`newest`](Self::newest), async: the scan as a stream.
    async fn newest_async(
        &self,
        s: &Snapshot,
        prefix: &[u8],
        n: usize,
        yields: &Yields,
    ) -> Result<Vec<Vec<u8>>> {
        use futures_core::Stream;
        let scan = match self {
            Rows::W(t) => t.scan_prefix(prefix),
            Rows::R(t) => t.scan_prefix(prefix),
        };
        let mut rows = std::pin::pin!(scan.family(CONTENT).snapshot(s).limit(n as u64).stream());
        let mut out = Vec::new();
        while let Some(r) = std::future::poll_fn(|cx| rows.as_mut().poll_next(cx)).await {
            out.push(r?.key().to_vec());
            yields.row().await;
        }
        Ok(out)
    }

    /// [`edges`](Self::edges), async: the traversal and the linking row's read.
    async fn edges_async(
        &self,
        s: &Snapshot,
        from: &[u8],
        fam: &str,
        budget: u64,
    ) -> Result<(Vec<Edge>, bool)> {
        let seeds = KeySet::from_unsorted([from]);
        let traverse = match self {
            Rows::W(t) => t.traverse(&seeds),
            Rows::R(t) => t.traverse(&seeds),
        };
        let t = traverse
            .family(fam)
            .snapshot(s)
            .byte_budget(budget)
            .run_async()
            .await?;
        let (nodes, cut) = linked(&t);
        let (Some(first), Some(last)) = (nodes.first(), nodes.last()) else {
            return Ok((Vec::new(), cut));
        };
        let read = match self {
            Rows::W(t) => t.row(from),
            Rows::R(t) => t.row(from),
        };
        let row = read
            .family(fam)
            .qualifier_bounds(Bound::Included(first), Bound::Included(last))
            .snapshot(s)
            .read_async()
            .await?;
        let view = row.as_ref().map(pigeonhole::Row::view);
        Ok((weighted(nodes, view.as_ref())?, cut))
    }

    /// [`read_each`](Self::read_each), async: the multi-get's rows as they're read.
    async fn read_each_async(
        &self,
        s: &Snapshot,
        keys: &KeySet,
        yields: &Yields,
        mut f: impl FnMut(&[u8], &pigeonhole::RowRef<'_>) -> Result<()>,
    ) -> Result<()> {
        let read = match self {
            Rows::W(t) => t.keys(keys),
            Rows::R(t) => t.keys(keys),
        };
        let mut rows = read
            .families(
                [CONTENT, META, STATS, PEAK, EMB]
                    .into_iter()
                    .filter(|f| self.has_family(f)),
            )
            .snapshot(s)
            .read()?;
        while let Some((_, row)) = rows.next_async().await? {
            let key = row.key().to_vec();
            f(&key, &row)?;
            yields.row().await;
        }
        Ok(())
    }
}

#[cfg(feature = "async")]
impl Idx<'_> {
    /// [`search`](Self::search), async.
    async fn search_async(
        &self,
        s: &Snapshot,
        text: &str,
        prefix: &[u8],
        n: usize,
    ) -> Result<Vec<(Vec<u8>, f64)>> {
        let q = Query::new().text(text);
        let search = match self {
            Idx::W(i) => i.search(q, n),
            Idx::R(i) => i.search(q, n),
        };
        let hits = search.snapshot(s).id_prefix(prefix).run_async().await?;
        Ok(hits.into_iter().map(|h| (h.id, h.score)).collect())
    }
}

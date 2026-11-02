//! Recall (D311 §3): vector, keyword and recency candidates per namespace, every signal
//! computed for every candidate, fused by reciprocal rank fusion (or a linear blend), and
//! returned in fused order within a byte budget. Read-only, at one snapshot.

use std::collections::{BTreeMap, BTreeSet};

use pigeonhole::{KeySet, Pigeonhole, PigeonholeReader, ReadTable, Snapshot, Table, VectorSpec};
use pigeonhole_text::{Index, Query, ReadIndex};

use crate::error::{Error, Result, corrupt};
use crate::layout::*;
use crate::store::{EmbeddingModel, Kind};

/// RRF's constant (Cormack et al., 2009; D311 call 4).
const RRF_K: f64 = 60.0;
/// D308's per-item overhead, which a recall hit's size counts too.
const ITEM_OVERHEAD: u64 = 16;
/// The bytes `with_signals` adds to a hit: five signals, a rank (`u32`) and a value (`f64`)
/// each.
const SIGNALS_BYTES: u64 = 5 * (4 + 8);

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
/// start from `Weights::default()` (every weight 1) and set the fields.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub struct Weights {
    /// Embedding similarity.
    pub vector: f64,
    /// BM25, for keyword hits.
    pub keyword: f64,
    /// `exp(−age / half_life)`.
    pub recency: f64,
    /// The memory's importance.
    pub importance: f64,
    /// `ln(1 + uses)`, decayed by the time since the last use.
    pub use_: f64,
}

impl Default for Weights {
    fn default() -> Self {
        Weights {
            vector: 1.0,
            keyword: 1.0,
            recency: 1.0,
            importance: 1.0,
            use_: 1.0,
        }
    }
}

/// One signal's rank among the candidates (1 is best) and its value.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub struct Signal {
    /// Its rank among the candidates that have it, from 1. Equal values share a rank: one
    /// more than how many candidates have a greater value.
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
    pub use_: Option<Signal>,
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
    /// Whether the byte budget cut the list (not `k`).
    pub truncated: bool,
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
        model: Option<&'a EmbeddingModel>,
        db: Snap<'a>,
        namespaces: &[&str],
    ) -> Self {
        Recall {
            rows,
            index,
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
        }
    }

    /// The query text, for keyword recall: tokenised as the memories were.
    pub fn text(mut self, t: impl Into<String>) -> Self {
        self.text = Some(t.into());
        self
    }

    /// The query embedding, for vector recall: from the store's model, as `f32` whatever
    /// the store's element type.
    pub fn embedding(mut self, e: impl Into<Vec<f32>>) -> Self {
        self.embedding = Some(e.into());
        self
    }

    /// At most `k` hits: 10 by default.
    pub fn k(mut self, k: usize) -> Self {
        self.k = k;
        self
    }

    /// How many candidates each signal contributes per namespace: 50 by default.
    pub fn candidates(mut self, n: usize) -> Self {
        self.per_signal = n;
        self
    }

    /// At most `bytes` of hits, by D308's rules: each hit's text, its id and 16 bytes, plus
    /// its signals and embedding when asked for. A hit is whole or left out, and at least
    /// one is returned.
    pub fn byte_budget(mut self, bytes: u64) -> Self {
        self.byte_budget = Some(bytes);
        self
    }

    /// Recalls at `snapshot` rather than the latest state.
    pub fn snapshot(mut self, snapshot: &'a Snapshot) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    /// Ages are measured from `us` (µs since the Unix epoch) rather than now. With an
    /// explicit snapshot too, the same call always returns the same result.
    pub fn at(mut self, us: u64) -> Self {
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

    /// The hits, best first.
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
        // 1. Candidates, per namespace.
        let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut bm25: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
        for ns in &self.namespaces {
            let prefix = [ns.as_bytes(), MEMORY_TAG].concat();
            if let Some(e) = &self.embedding {
                for n in self.rows.nearest(s, &prefix, e, self.per_signal)? {
                    keys.insert(n);
                }
            }
            if let Some(t) = text {
                for (id, score) in self.index.search(s, t, &prefix, self.per_signal)? {
                    keys.insert(id.clone());
                    bm25.insert(id, score);
                }
            }
            for k in self.rows.newest(s, &prefix, self.per_signal)? {
                keys.insert(k);
            }
        }
        // 2. Read them, in one multi-get, and compute every signal.
        let mut cands = Vec::new();
        let set = KeySet::from_unsorted(keys.iter().collect::<Vec<_>>());
        self.rows.read_each(s, &set, |key, row| {
            let Some(id) = MemoryId::from_row(key) else {
                return Ok(());
            };
            let Some(text) = row.get(CONTENT, TEXT) else {
                return Ok(());
            };
            if !self.include_superseded && row.get(META, SUPERSEDED_BY).is_some() {
                return Ok(());
            }
            if let Some(want) = &self.session
                && !row
                    .get(META, SESSION)
                    .is_some_and(|c| c.value() == want.as_bytes())
            {
                return Ok(());
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
            let uses = row
                .get(STATS, USES)
                .and_then(|c| c.as_i64())
                .unwrap_or(0)
                .max(0);
            let last_used = row.get(PEAK, LAST_USED).and_then(|c| c.as_i64());
            let stored = row.get(EMB, V);
            let vector = match (&self.embedding, &spec, &stored) {
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
            let use_ = (uses > 0).then(|| {
                let since = last_used.map_or(age, |l| now.saturating_sub(l.max(0) as u64) as f64);
                (1.0 + uses as f64).ln() * (-since / self.half_life_us as f64).exp()
            });
            cands.push(Cand {
                vals: [
                    vector,
                    bm25.get(key).copied(),
                    Some(recency),
                    Some(importance),
                    use_,
                ],
                ranks: [None; 5],
                score: 0.0,
                hit: RecallHit {
                    id,
                    text,
                    kind,
                    importance,
                    created_us: created,
                    score: 0.0,
                    signals: None,
                    embedding,
                },
            });
            Ok(())
        })?;
        stats.candidates = cands.len();
        // 3. Fusion.
        let w = [
            self.weights.vector,
            self.weights.keyword,
            self.weights.recency,
            self.weights.importance,
            self.weights.use_,
        ];
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
            // greater), so a tie (every importance at its default, say) gives no candidate
            // an edge.
            for (r, &i) in order.iter().enumerate() {
                let rank = if r > 0 && cands[order[r - 1]].vals[sig] == cands[i].vals[sig] {
                    cands[order[r - 1]].ranks[sig].expect("ranked above")
                } else {
                    r as u32 + 1
                };
                cands[i].ranks[sig] = Some(rank);
            }
            match self.fusion {
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
                    use_: sig(4),
                });
            }
            let size = hit_size(&hit, self.with_signals);
            if let Some(budget) = self.byte_budget
                && !out.is_empty()
                && used + size > budget
            {
                stats.truncated = true;
                break;
            }
            used += size;
            out.push(hit);
        }
        Ok((out, stats))
    }
}

struct Cand {
    /// vector, keyword, recency, importance, use.
    vals: [Option<f64>; 5],
    ranks: [Option<u32>; 5],
    score: f64,
    hit: RecallHit,
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

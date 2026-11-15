//! A recall in steps (ICR 0139, #1915): the pool of candidates with their raw signals, the
//! ranking that fuses it, and the bodies of the hits. One store runs them in process
//! ([`Recall::run`](super::Recall::run)). A recall over namespaces in several stores (a
//! cluster's partitions) gathers a pool from each, merges them and ranks the whole, then asks
//! each store for its hits' bodies. A namespace is wholly in one store, and its candidates
//! depend only on itself, so the merged pool is the one store's, and the fusion matches it.
//!
//! The byte forms are versioned: a version this build doesn't know is refused, never guessed
//! at.

use std::collections::BTreeMap;

use super::{Body, Cand, Fusion, GRAPH_SEEDS, RecallHit, RecallStats, Signal, Signals, Weights};
use super::{fuse, hit_size};
use crate::error::{Error, Result};
use crate::layout::MemoryId;
use crate::store::Kind;

/// The byte forms' version.
const VERSION: u8 = 1;

/// A recall's candidates with every signal's raw value, unfused ([`Recall::gather`]).
///
/// [`Recall::gather`]: super::Recall::gather
#[derive(Default)]
pub struct RecallPool {
    cands: Vec<Cand>,
    ivf_namespaces: usize,
    ivf_fallbacks: usize,
}

impl std::fmt::Debug for RecallPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecallPool")
            .field("candidates", &self.cands.len())
            .finish_non_exhaustive()
    }
}

impl RecallPool {
    pub(crate) fn new(
        cands: Vec<Cand>,
        ivf_counts: &BTreeMap<&str, u64>,
        fell_back: &std::collections::BTreeSet<&str>,
    ) -> Self {
        RecallPool {
            cands,
            ivf_namespaces: ivf_counts.len() - fell_back.len(),
            ivf_fallbacks: fell_back.len(),
        }
    }

    /// How many candidates it holds.
    pub fn len(&self) -> usize {
        self.cands.len()
    }

    /// Whether it holds none.
    pub fn is_empty(&self) -> bool {
        self.cands.is_empty()
    }

    /// The pools of stores holding disjoint namespaces, as one. A memory in two of them is
    /// `InvalidArgument`: its namespace was gathered twice.
    pub fn merge(parts: impl IntoIterator<Item = RecallPool>) -> Result<RecallPool> {
        let mut out = RecallPool::default();
        let mut seen = std::collections::BTreeSet::new();
        for p in parts {
            for c in &p.cands {
                if !seen.insert(c.key.clone()) {
                    return Err(Error::InvalidArgument(format!(
                        "memory {} is in two recall pools",
                        c.id
                    )));
                }
            }
            out.cands.extend(p.cands);
            out.ivf_namespaces += p.ivf_namespaces;
            out.ivf_fallbacks += p.ivf_fallbacks;
        }
        Ok(out)
    }

    /// The byte form: the candidates' ids and signal values, without their bodies.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.u32(self.ivf_namespaces as u32);
        w.u32(self.ivf_fallbacks as u32);
        w.u32(self.cands.len() as u32);
        for c in &self.cands {
            w.bytes(&c.key);
            w.vals(&c.vals);
        }
        w.out
    }

    /// The pool a byte form names. A version this build doesn't know, or anything
    /// malformed, is `InvalidArgument`.
    pub fn from_bytes(bytes: &[u8]) -> Result<RecallPool> {
        let mut r = Reader::new(bytes, "recall pool")?;
        let ivf_namespaces = r.u32()? as usize;
        let ivf_fallbacks = r.u32()? as usize;
        let n = r.u32()?;
        let mut cands = Vec::new();
        for _ in 0..n {
            let key = r.bytes()?.to_vec();
            let id = MemoryId::from_row(&key).ok_or_else(|| r.bad())?;
            let vals = r.vals()?;
            cands.push(Cand {
                key,
                id,
                vals,
                ranks: [None; 6],
                score: 0.0,
                body: None,
            });
        }
        r.end()?;
        Ok(RecallPool {
            cands,
            ivf_namespaces,
            ivf_fallbacks,
        })
    }

    /// The candidates fused, best first, as one store holding them all would: by
    /// `fusion`, with `weights`.
    pub fn rank(self, fusion: Fusion, weights: Weights) -> RecallRanking {
        let mut r = RecallRanking {
            cands: self.cands,
            fusion,
            weights: [
                weights.vector,
                weights.keyword,
                weights.recency,
                weights.importance,
                weights.uses,
                weights.graph,
            ],
            stats: RecallStats {
                ivf_namespaces: self.ivf_namespaces,
                ivf_fallbacks: self.ivf_fallbacks,
                ..RecallStats::default()
            },
        };
        fuse(&mut r.cands, &r.weights, fusion);
        r
    }
}

/// A recall's candidates in fused order ([`RecallPool::rank`]).
pub struct RecallRanking {
    pub(crate) cands: Vec<Cand>,
    fusion: Fusion,
    /// The signals' weights, in [`Cand::vals`]' order.
    weights: [f64; 6],
    pub(crate) stats: RecallStats,
}

impl std::fmt::Debug for RecallRanking {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecallRanking")
            .field("candidates", &self.cands.len())
            .finish_non_exhaustive()
    }
}

impl RecallRanking {
    /// The best `k` ids, best first: the hits [`Recall::bodies`](super::Recall::bodies)
    /// reads.
    pub fn top(&self, k: usize) -> Vec<MemoryId> {
        self.cands.iter().take(k).map(|c| c.id.clone()).collect()
    }

    /// A graph expansion's seeds: the best [`GRAPH_SEEDS`], each valued by its rank. Taken
    /// before any candidate has a graph value, so the order is the one without the graph
    /// signal.
    pub(crate) fn seeds(&self) -> Vec<(Vec<u8>, f64)> {
        self.cands
            .iter()
            .take(GRAPH_SEEDS)
            .enumerate()
            .map(|(i, c)| (c.key.clone(), 1.0 / (i + 1) as f64))
            .collect()
    }

    /// Fuses again from scratch, after an expansion added candidates and graph values.
    pub(crate) fn refuse(&mut self) {
        for c in &mut self.cands {
            c.score = 0.0;
            c.ranks = [None; 6];
        }
        fuse(&mut self.cands, &self.weights, self.fusion);
    }

    /// The hits: the candidates in fused order, each with its body (read with it in one
    /// store, or from `bodies`), at most `k`, within `byte_budget` as
    /// [`Recall::byte_budget`](super::Recall::byte_budget) says, with their signals if
    /// `with_signals`. A candidate with no body (gone since its pool was gathered) is
    /// skipped, and the next one takes its place.
    pub fn finish(
        mut self,
        mut bodies: RecallBodies,
        k: usize,
        byte_budget: Option<u64>,
        with_signals: bool,
    ) -> (Vec<RecallHit>, RecallStats) {
        let mut stats = std::mem::take(&mut self.stats);
        stats.candidates = self.cands.len();
        let mut out = Vec::new();
        let mut used = 0u64;
        for c in self.cands {
            if out.len() == k {
                break;
            }
            let Some(body) = c.body.or_else(|| bodies.map.remove(&c.key)) else {
                continue;
            };
            let mut hit = body.hit(c.id, c.score);
            if with_signals {
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
            let size = hit_size(&hit, with_signals);
            if let Some(budget) = byte_budget
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
        (out, stats)
    }
}

/// The bodies of a recall's hits ([`Recall::bodies`](super::Recall::bodies)), for
/// [`RecallRanking::finish`].
#[derive(Default)]
pub struct RecallBodies {
    map: BTreeMap<Vec<u8>, Body>,
}

impl std::fmt::Debug for RecallBodies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecallBodies")
            .field("bodies", &self.map.len())
            .finish_non_exhaustive()
    }
}

impl RecallBodies {
    pub(crate) fn from_cands(cands: impl IntoIterator<Item = Cand>) -> Self {
        RecallBodies {
            map: cands
                .into_iter()
                .filter_map(|c| c.body.map(|b| (c.key, b)))
                .collect(),
        }
    }

    /// How many bodies it holds.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether it holds none.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Several stores' bodies, as one.
    pub fn merge(parts: impl IntoIterator<Item = RecallBodies>) -> RecallBodies {
        let mut out = RecallBodies::default();
        for p in parts {
            out.map.extend(p.map);
        }
        out
    }

    /// The byte form.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.u32(self.map.len() as u32);
        for (key, b) in &self.map {
            w.bytes(key);
            w.bytes(b.text.as_bytes());
            w.out.push(match b.kind {
                Kind::Episodic => 0,
                Kind::Semantic => 1,
                Kind::Procedural => 2,
            });
            w.f64(b.importance);
            match &b.superseded_by {
                Some(id) => {
                    w.out.push(1);
                    w.bytes(&id.row());
                }
                None => w.out.push(0),
            }
            match &b.embedding {
                Some(e) => {
                    w.out.push(1);
                    w.u32(e.len() as u32);
                    for x in e {
                        w.out.extend_from_slice(&x.to_le_bytes());
                    }
                }
                None => w.out.push(0),
            }
        }
        w.out
    }

    /// The bodies a byte form names. A version this build doesn't know, or anything
    /// malformed, is `InvalidArgument`.
    pub fn from_bytes(bytes: &[u8]) -> Result<RecallBodies> {
        let mut r = Reader::new(bytes, "recall bodies")?;
        let n = r.u32()?;
        let mut map = BTreeMap::new();
        for _ in 0..n {
            let key = r.bytes()?.to_vec();
            MemoryId::from_row(&key).ok_or_else(|| r.bad())?;
            let text = std::str::from_utf8(r.bytes()?)
                .map_err(|_| r.bad())?
                .to_owned();
            let kind = match r.u8()? {
                0 => Kind::Episodic,
                1 => Kind::Semantic,
                2 => Kind::Procedural,
                _ => return Err(r.bad()),
            };
            let importance = r.f64()?;
            let superseded_by = match r.u8()? {
                0 => None,
                1 => Some(MemoryId::from_row(r.bytes()?).ok_or_else(|| r.bad())?),
                _ => return Err(r.bad()),
            };
            let embedding = match r.u8()? {
                0 => None,
                1 => {
                    let d = r.u32()? as usize;
                    let raw = r.take(d.checked_mul(4).ok_or_else(|| r.bad())?)?;
                    Some(
                        raw.chunks_exact(4)
                            .map(|c| f32::from_le_bytes(c.try_into().expect("4")))
                            .collect(),
                    )
                }
                _ => return Err(r.bad()),
            };
            map.insert(
                key,
                Body {
                    text,
                    kind,
                    importance,
                    superseded_by,
                    embedding,
                },
            );
        }
        r.end()?;
        Ok(RecallBodies { map })
    }
}

/// A byte form under way: the version, then little-endian fields.
struct Writer {
    out: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Writer { out: vec![VERSION] }
    }

    fn u32(&mut self, v: u32) {
        self.out.extend_from_slice(&v.to_le_bytes());
    }

    fn f64(&mut self, v: f64) {
        self.out.extend_from_slice(&v.to_le_bytes());
    }

    fn bytes(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.out.extend_from_slice(b);
    }

    /// Six optional values: a presence mask, then each present one.
    fn vals(&mut self, vals: &[Option<f64>; 6]) {
        let mask = vals
            .iter()
            .enumerate()
            .fold(0u8, |m, (i, v)| if v.is_some() { m | 1 << i } else { m });
        self.out.push(mask);
        for v in vals.iter().flatten() {
            self.f64(*v);
        }
    }
}

/// A byte form being read.
struct Reader<'b> {
    rest: &'b [u8],
    what: &'static str,
}

impl<'b> Reader<'b> {
    /// Checks the version.
    fn new(bytes: &'b [u8], what: &'static str) -> Result<Self> {
        let mut r = Reader { rest: bytes, what };
        match r.u8()? {
            VERSION => Ok(r),
            v => Err(Error::InvalidArgument(format!(
                "a {what} of version {v}, and this build reads version {VERSION}"
            ))),
        }
    }

    fn bad(&self) -> Error {
        Error::InvalidArgument(format!("a malformed {}", self.what))
    }

    fn take(&mut self, n: usize) -> Result<&'b [u8]> {
        if self.rest.len() < n {
            return Err(self.bad());
        }
        let (a, b) = self.rest.split_at(n);
        self.rest = b;
        Ok(a)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4")))
    }

    fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().expect("8")))
    }

    fn bytes(&mut self) -> Result<&'b [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }

    fn vals(&mut self) -> Result<[Option<f64>; 6]> {
        let mask = self.u8()?;
        if mask >> 6 != 0 {
            return Err(self.bad());
        }
        let mut vals = [None; 6];
        for (i, v) in vals.iter_mut().enumerate() {
            if mask & 1 << i != 0 {
                *v = Some(self.f64()?);
            }
        }
        Ok(vals)
    }

    /// Nothing may follow.
    fn end(&self) -> Result<()> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(self.bad())
        }
    }
}

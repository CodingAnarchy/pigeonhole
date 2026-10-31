//! Shared helpers for the index's tests: a seeded generator, vectors of each kind, a brute
//! force, and the invariant check (D306 §8).

#![allow(dead_code)]

use std::collections::BTreeMap;

use pigeonhole::{Element, Metric, Options, Pigeonhole, Vector, VectorSpec};
use pigeonhole_vector::{Hit, Index};

/// The seeds a test runs: `VECTOR_SEED` alone when set, else `defaults`.
pub fn seeds(defaults: &[u64]) -> Vec<u64> {
    match std::env::var("VECTOR_SEED") {
        Ok(s) => vec![s.parse().expect("VECTOR_SEED is a u64")],
        Err(_) => defaults.to_vec(),
    }
}

/// xorshift64*: deterministic per seed.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// Uniform in [-1, 1).
    pub fn unit(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 23) as f32 - 1.0
    }
}

/// A vector of the index's kind, owned.
#[derive(Debug, Clone, PartialEq)]
pub enum V {
    F32(Vec<f32>),
    I8(Vec<i8>),
    Bits(Vec<u8>),
}

impl V {
    /// A random vector of `spec`'s kind near `centre` (one of a few clusters, so splits
    /// have structure to find).
    pub fn random(rng: &mut Rng, spec: &VectorSpec, centre: u64) -> Self {
        let dim = spec.dim() as usize;
        let mut c = Rng::new(centre + 1);
        match spec.element() {
            Element::F32 => V::F32((0..dim).map(|_| 4.0 * c.unit() + rng.unit()).collect()),
            Element::I8 => V::I8(
                (0..dim)
                    .map(|_| (60.0 * c.unit() + 30.0 * rng.unit()) as i8)
                    .collect(),
            ),
            _ => V::Bits(
                (0..dim / 8)
                    .map(|_| {
                        (c.next() as u8) ^ (rng.next() as u8 & rng.next() as u8 & rng.next() as u8)
                    })
                    .collect(),
            ),
        }
    }

    pub fn insert(&self, index: &Index, id: &[u8]) -> pigeonhole_vector::Result<()> {
        match self {
            V::F32(x) => index.insert_f32(id, x),
            V::I8(x) => index.insert_i8(id, x),
            V::Bits(x) => index.insert_bits(id, x),
        }
    }

    pub fn search(&self, index: &Index, k: usize, nprobe: u32) -> Vec<Hit> {
        match self {
            V::F32(x) => index.search_f32(x, k).nprobe(nprobe).run(),
            V::I8(x) => index.search_i8(x, k).nprobe(nprobe).run(),
            V::Bits(x) => index.search_bits(x, k).nprobe(nprobe).run(),
        }
        .expect("search")
    }

    /// The distance `nearest_*` ranks by: `spec`'s, from this query to `stored`.
    pub fn distance(&self, spec: &VectorSpec, stored: &V) -> f64 {
        let s = match stored {
            V::F32(x) => Vector::from_f32(x),
            V::I8(x) => Vector::from_i8(x),
            V::Bits(x) => Vector::from_bits(spec.dim(), x),
        }
        .expect("stored vector");
        match self {
            V::F32(q) => spec.distance_f32(q, s.as_ref()),
            V::I8(q) => spec.distance_i8(q, s.as_ref()),
            V::Bits(q) => spec.distance_bits(q, s.as_ref()),
        }
        .expect("distance")
    }
}

/// The specs the model runs over: one of each element, every metric.
pub fn specs() -> Vec<VectorSpec> {
    vec![
        VectorSpec::new(8, Element::F32, Metric::L2).unwrap(),
        VectorSpec::new(8, Element::F32, Metric::Cosine).unwrap(),
        VectorSpec::new(6, Element::I8, Metric::InnerProduct).unwrap(),
        VectorSpec::new(6, Element::I8, Metric::Cosine).unwrap(),
        VectorSpec::new(32, Element::Bit, Metric::Hamming).unwrap(),
    ]
}

pub fn open(dir: &pigeonhole::doc_support::TempDir) -> Pigeonhole {
    Pigeonhole::open(dir.join("idx.phdb"), Options::default().shards(2)).expect("open")
}

/// The invariants after an operation (D306 §8): the rows agree with each other
/// (`verify`), the index holds exactly the model's vectors, and every list is within its
/// bounds.
pub fn check(index: &Index, model: &BTreeMap<Vec<u8>, V>, ctx: &str) {
    let stats = index
        .verify()
        .unwrap_or_else(|e| panic!("{ctx}: verify: {e}"));
    assert_eq!(stats.vectors, model.len() as u64, "{ctx}: vectors");
    let (min, max) = (index.min_list_len(), index.max_list_len());
    for len in &stats.list_lens {
        assert!(
            *len <= max,
            "{ctx}: a list of {len} > {max}: {:?}",
            stats.list_lens
        );
        if stats.list_lens.len() > 1 {
            assert!(
                *len >= min,
                "{ctx}: a list of {len} < {min}: {:?}",
                stats.list_lens
            );
        }
    }
}

/// Checks a search with every list probed against a brute force over `model`: the same
/// distances in the same order, and the same ids, up to ties at the k-th distance (the
/// engine breaks those by list row, which the model doesn't know).
pub fn exact(index: &Index, model: &BTreeMap<Vec<u8>, V>, q: &V, k: usize, ctx: &str) {
    let spec = index.spec();
    let lists = index.verify().expect("verify").list_lens.len() as u32;
    let got = q.search(index, k, lists.max(1));
    let mut all: Vec<(f64, &Vec<u8>)> = model
        .iter()
        .map(|(id, v)| (q.distance(&spec, v), id))
        .collect();
    all.sort_by(|a, b| a.0.total_cmp(&b.0));
    let want: Vec<f64> = all.iter().take(k).map(|(d, _)| *d).collect();
    let dists: Vec<f64> = got.iter().map(|h| h.distance).collect();
    assert_eq!(dists, want, "{ctx}: distances");
    let Some(&last) = want.last() else {
        return;
    };
    for h in &got {
        let d = q.distance(&spec, &model[&h.id]);
        assert_eq!(d, h.distance, "{ctx}: id {:?}'s distance", h.id);
    }
    let below: Vec<&Vec<u8>> = all
        .iter()
        .filter(|(d, _)| *d < last)
        .map(|(_, id)| *id)
        .collect();
    for id in below {
        assert!(got.iter().any(|h| &h.id == id), "{ctx}: {id:?} missing");
    }
}

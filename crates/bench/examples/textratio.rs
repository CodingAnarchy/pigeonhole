//! The async text search against the sync one, warm (#1560 PR 2, perf287's ask): for each of
//! the text shapes' query kinds, the best of `PASSES` passes of every query through
//! `Search::run` and through `Search::run_async`, and their ratio. A warm search never waits
//! on the device, so the ratio is the async machinery's cost (polls, the resumable loop,
//! seeks for skips). perf287's bar: within about 1.3×.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example textratio -- [DOCS] [PASSES] [DIR]
//! ```
//!
//! Defaults: 30,000 documents (the text shapes' Zipf corpus, smaller), 5 passes. It builds
//! the index first (about a minute in release). Prints one line per shape.

use std::future::Future;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use pigeonhole::{Family, Options, Pigeonhole};
use pigeonhole_text::{Fuzziness, Index, IndexOptions, Query, Source};

const VOCAB: usize = 50_000;
const QUERIES: usize = 50;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

struct Zipf(Vec<f64>);

impl Zipf {
    fn new() -> Self {
        let mut cdf = Vec::with_capacity(VOCAB);
        let mut acc = 0.0;
        for i in 1..=VOCAB {
            acc += 1.0 / i as f64;
            cdf.push(acc);
        }
        cdf.iter_mut().for_each(|c| *c /= acc);
        Zipf(cdf)
    }

    fn rank(&self, rng: &mut Rng) -> usize {
        let u = rng.unit();
        self.0.partition_point(|&c| c < u).min(VOCAB - 1)
    }
}

fn word(rank: usize) -> String {
    format!("w{rank}")
}

/// Polls `fut` to completion on this thread: a warm search only yields at its step budget,
/// waking itself.
fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

fn shapes(z: &Zipf, corpus: &[Vec<usize>]) -> Vec<(&'static str, Vec<Query>, usize)> {
    let mut rng = Rng(0x5EED_1560);
    let terms = |n: usize, rng: &mut Rng| -> Vec<Query> {
        (0..QUERIES)
            .map(|_| (0..n).fold(Query::new(), |q, _| q.term(word(z.rank(rng)))))
            .collect()
    };
    let rare = (0..QUERIES)
        .map(|_| Query::new().term(word(1_000 + rng.below(VOCAB - 1_000))))
        .collect();
    let hub = (0..QUERIES)
        .map(|_| Query::new().term(word(rng.below(20))))
        .collect();
    let two = terms(2, &mut rng);
    let five = terms(5, &mut rng);
    let k100 = terms(2, &mut rng);
    let fuzzy = (0..QUERIES)
        .map(|_| {
            let mut w = word(z.rank(&mut rng)).into_bytes();
            let at = 1 + rng.below(w.len() - 1);
            w[at] = b'0' + ((w[at] - b'0' + 1 + rng.below(9) as u8) % 10);
            Query::new().fuzzy(String::from_utf8(w).expect("ASCII"), Fuzziness::One)
        })
        .collect();
    // Adjacent pairs of one hub word and one other, as the text shapes draw them.
    let mut pairs = Vec::new();
    while pairs.len() < 25 {
        let d = &corpus[rng.below(corpus.len())];
        let i = rng.below(d.len() - 1);
        if (d[i] < 20) != (d[i + 1] < 20) {
            pairs.push((word(d[i]), word(d[i + 1])));
        }
    }
    let phrase = |slop| -> Vec<Query> {
        pairs
            .iter()
            .map(|(a, b)| Query::new().phrase_with_slop([a.clone(), b.clone()], slop))
            .collect()
    };
    vec![
        ("text-term-rare", rare, 10),
        ("text-term-hub", hub, 10),
        ("text-two-terms", two, 10),
        ("text-five-terms", five, 10),
        ("text-two-terms-k100", k100, 100),
        ("text-fuzzy", fuzzy, 10),
        ("text-phrase", phrase(0), 10),
        ("text-phrase-slop2", phrase(2), 10),
    ]
}

fn main() -> pigeonhole_text::Result<()> {
    let mut args = std::env::args().skip(1);
    let docs: usize = args.next().map_or(30_000, |s| s.parse().expect("DOCS"));
    let passes: u32 = args.next().map_or(5, |s| s.parse().expect("PASSES"));
    let given = args.next().map(std::path::PathBuf::from);
    let dir = given
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join(format!("textratio-{}", std::process::id())));
    std::fs::create_dir_all(&dir).expect("dir");
    let db = Pigeonhole::open(dir.join("t.phdb"), Options::default().shards(1))?;
    let table = db
        .table("docs")?
        .family("body", Family::default())
        .create_if_missing()?;
    let index = Index::create(
        &db,
        "docs_text",
        Source::new(&table, "body", b"text"),
        IndexOptions::new().positions(true),
    )?;
    let z = Zipf::new();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let corpus: Vec<Vec<usize>> = (0..docs)
        .map(|_| (0..20 + rng.below(41)).map(|_| z.rank(&mut rng)).collect())
        .collect();
    for (i, d) in corpus.iter().enumerate() {
        let text = d.iter().map(|&r| word(r)).collect::<Vec<_>>().join(" ");
        index.put(format!("d{i:08}").as_bytes(), &text)?;
    }
    db.flush()?;
    db.compact()?;
    for (name, queries, k) in shapes(&z, &corpus) {
        let (mut sync_best, mut async_best) = (f64::MAX, f64::MAX);
        let mut check = (0usize, 0usize);
        for pass in 0..=passes {
            let start = Instant::now();
            for q in &queries {
                check.0 += index.search(q.clone(), k).run()?.len();
            }
            let s = start.elapsed().as_secs_f64();
            let start = Instant::now();
            for q in &queries {
                check.1 += block_on(index.search(q.clone(), k).run_async())?.len();
            }
            let a = start.elapsed().as_secs_f64();
            // Pass 0 warms the cache.
            if pass > 0 {
                sync_best = sync_best.min(s);
                async_best = async_best.min(a);
            }
        }
        assert_eq!(
            check.0, check.1,
            "{name}: the async search found other hits"
        );
        let per = |t: f64| t * 1e6 / queries.len() as f64;
        println!(
            "{name:<20} sync {:>9.1} µs/query  async {:>9.1} µs/query  ratio {:.3}",
            per(sync_best),
            per(async_best),
            async_best / sync_best
        );
    }
    drop(index);
    db.close()?;
    // Only the directory this run made for itself goes; a given one is the caller's.
    if given.is_none() {
        std::fs::remove_dir_all(&dir).ok();
    }
    Ok(())
}

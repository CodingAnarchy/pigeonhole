//! D309's measurements (§7, rev 6 §B.8): index a Zipf corpus, then time searches.
//!
//!     cargo run --release -p pigeonhole-text --example bench -- [DOCS] [QUERIES] [DIR]
//!
//! The corpus: `DOCS` documents (default 1,000,000) of about 40 words, the length of an MS
//! MARCO passage, from a 50,000-word vocabulary with Zipf frequencies. It's seeded, so every
//! run indexes the same text.
//!
//! **Indexing, twice:** with positions off, then on (the owner's write-cost row). Each pass
//! writes `Buffered`, one document per transaction, and prints the time per document and
//! the index table's stored bytes after a flush and a full compaction (with its time).
//!
//! **A full compaction** follows the flush, so stored bytes and search latency are of a
//! compacted index. #1332 fixed the compaction write amplification (#1313) that kept the
//! first numbers flush only.
//!
//! **Searches,** on the positions-on index: `QUERIES` (default 1,000) of each kind, at top
//! 10 unless noted, printing p50, p99 and the mean in µs:
//! - `terms-2`: two terms drawn by Zipf frequency (a mix of common and rare);
//! - `terms-2-hub`: two of the 20 most common terms (MaxScore's hard case, §7's risk);
//! - `phrase-rare`, `phrase-mixed`, `phrase-hub`: an exact two-word phrase drawn from a
//!   document, its words both rare (rank ≥ 1,000), one each, or both common (rank < 20);
//! - `slop-2`, `slop-10`: the mixed phrases with slop 2 and 10;
//! - `terms-2-k1000`: `terms-2` at top 1,000;
//! - `fuzzy-1`, `fuzzy-2`: a fuzzy term (D309 PR 4) at 1 and 2 edits, a word drawn by Zipf
//!   frequency with one digit replaced. The vocabulary is `w0` to `w49999`, so a term has
//!   dozens of terms within 2 edits: a dense dictionary, the hard case for the walk.
//! - `fuzzy-2-prefix2`: `fuzzy-2` with `prefix_length(2)`;
//! - `fuzzy-1-term`: `fuzzy-1` plus a term drawn by Zipf frequency, so the fuzzy clause can
//!   be non-essential (#1333).
//!
//! The fuzzy kinds also print the mean expansions per query.
//!
//! Run it on the reference machine for the published numbers. On a laptop, use a few
//! thousand documents to check that it runs.

use std::time::Instant;

use pigeonhole::{Durability, Family, Options, Pigeonhole};
use pigeonhole_text::{Fuzziness, Index, IndexOptions, Query, Source};

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

const VOCAB: usize = 50_000;

struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new() -> Self {
        let mut cdf = Vec::with_capacity(VOCAB);
        let mut acc = 0.0;
        for i in 1..=VOCAB {
            acc += 1.0 / i as f64;
            cdf.push(acc);
        }
        for c in &mut cdf {
            *c /= acc;
        }
        Zipf { cdf }
    }

    fn rank(&self, rng: &mut Rng) -> usize {
        let u = rng.unit();
        self.cdf.partition_point(|&c| c < u).min(VOCAB - 1)
    }
}

fn word(rank: usize) -> String {
    format!("w{rank}")
}

/// The corpus, as each document's word ranks.
fn corpus(docs: usize) -> Vec<Vec<usize>> {
    let z = Zipf::new();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    (0..docs)
        .map(|_| {
            let n = 20 + rng.below(41);
            (0..n).map(|_| z.rank(&mut rng)).collect()
        })
        .collect()
}

fn text(ranks: &[usize]) -> String {
    ranks.iter().map(|&r| word(r)).collect::<Vec<_>>().join(" ")
}

fn doc_id(i: usize) -> Vec<u8> {
    format!("d{i:08}").into_bytes()
}

/// Indexes the corpus into a fresh database; prints the write cost and the stored bytes.
fn build(
    dir: &std::path::Path,
    corpus: &[Vec<usize>],
    positions: bool,
) -> Result<(Pigeonhole, Index), Box<dyn std::error::Error>> {
    let path = dir.join(format!("bench-{positions}.phdb"));
    let db = Pigeonhole::open(&path, Options::default().durability(Durability::Buffered))?;
    let docs = db
        .table("docs")?
        .family("body", Family::default())
        .create()?;
    let index = Index::create(
        &db,
        "docs_text",
        Source::new(&docs, "body", b"text"),
        IndexOptions::new().positions(positions),
    )?;
    let start = Instant::now();
    let tenth = (corpus.len() / 10).max(1);
    let mut lap = Instant::now();
    for (i, d) in corpus.iter().enumerate() {
        index.put(&doc_id(i), &text(d))?;
        if (i + 1) % tenth == 0 {
            // The rate over each tenth, to see whether the cost per document grows.
            println!(
                "  positions={positions}: {} documents, {:.1} µs per document over the last {tenth}",
                i + 1,
                lap.elapsed().as_secs_f64() * 1e6 / tenth as f64
            );
            lap = Instant::now();
        }
    }
    let per_doc = start.elapsed().as_secs_f64() * 1e6 / corpus.len() as f64;
    db.flush()?;
    let c = Instant::now();
    db.compact()?;
    let compact_s = c.elapsed().as_secs_f64();
    let info = db.info()?;
    let table = info
        .tables
        .iter()
        .find(|t| t.name == "docs_text")
        .ok_or("no index table")?;
    let bytes: u64 = table
        .storage
        .iter()
        .map(|f| f.levels.iter().map(|l| l.bytes).sum::<u64>() + f.blob_bytes)
        .sum();
    println!(
        "write positions={positions}: {per_doc:.1} µs per document, {bytes} stored bytes after a full compaction ({:.1} bytes per document; the compaction took {compact_s:.1} s)",
        bytes as f64 / corpus.len() as f64
    );
    Ok((db, index))
}

fn time(
    name: &str,
    index: &Index,
    queries: &[Query],
    k: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    time_with(name, index, queries, k, 0)
}

/// [`time`], with each fuzzy term's first `prefix_length` characters fixed.
fn time_with(
    name: &str,
    index: &Index,
    queries: &[Query],
    k: usize,
    prefix_length: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let search = |q: &Query| index.search(q.clone(), k).prefix_length(prefix_length);
    // One warm-up pass, so the block cache holds what the queries read.
    for q in queries.iter().take(50) {
        search(q).run()?;
    }
    let mut us: Vec<f64> = Vec::with_capacity(queries.len());
    let mut hits = 0usize;
    let mut expansions = 0usize;
    for q in queries {
        let t = Instant::now();
        let (h, stats) = search(q).run_with_stats()?;
        us.push(t.elapsed().as_secs_f64() * 1e6);
        hits += h.len();
        expansions += stats.fuzzy.iter().map(|f| f.expansions).sum::<usize>();
    }
    us.sort_by(f64::total_cmp);
    let at = |p: f64| us[((us.len() as f64 * p) as usize).min(us.len() - 1)];
    let mean = us.iter().sum::<f64>() / us.len() as f64;
    println!(
        "search {name}: p50 {:.0} µs, p99 {:.0} µs, mean {mean:.0} µs ({} queries, {:.1} hits each)",
        at(0.5),
        at(0.99),
        us.len(),
        hits as f64 / us.len() as f64
    );
    if expansions > 0 {
        println!(
            "search {name}: {:.1} expansions each",
            expansions as f64 / us.len() as f64
        );
    }
    Ok(())
}

/// Two-word phrases taken from documents, whose words' ranks satisfy `pick`.
fn phrases(
    corpus: &[Vec<usize>],
    n: usize,
    rng: &mut Rng,
    pick: impl Fn(usize, usize) -> bool,
) -> Vec<(String, String)> {
    let mut out = Vec::with_capacity(n);
    let mut tries = 0;
    while out.len() < n && tries < n * 10_000 {
        tries += 1;
        let d = &corpus[rng.below(corpus.len())];
        let i = rng.below(d.len() - 1);
        if pick(d[i], d[i + 1]) {
            out.push((word(d[i]), word(d[i + 1])));
        }
    }
    out
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let docs: usize = args.next().map_or(Ok(1_000_000), |a| a.parse())?;
    let nq: usize = args.next().map_or(Ok(1_000), |a| a.parse())?;
    let dir = args
        .next()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("pigeonhole-text-bench-{}", std::process::id()))
        });
    std::fs::create_dir_all(&dir)?;
    println!(
        "{docs} documents, {nq} queries per kind, in {}",
        dir.display()
    );
    let corpus = corpus(docs);
    {
        let (db, _) = build(&dir, &corpus, false)?;
        db.close()?;
    }
    let (db, index) = build(&dir, &corpus, true)?;
    let z = Zipf::new();
    let mut rng = Rng(0x5EED_D309);
    let terms2: Vec<Query> = (0..nq)
        .map(|_| {
            Query::new()
                .term(word(z.rank(&mut rng)))
                .term(word(z.rank(&mut rng)))
        })
        .collect();
    let hub: Vec<Query> = (0..nq)
        .map(|_| {
            Query::new()
                .term(word(rng.below(20)))
                .term(word(rng.below(20)))
        })
        .collect();
    let as_phrase = |ps: &[(String, String)], slop: u16| -> Vec<Query> {
        ps.iter()
            .map(|(a, b)| Query::new().phrase_with_slop([a.clone(), b.clone()], slop))
            .collect()
    };
    let rare = phrases(&corpus, nq, &mut rng, |a, b| a >= 1_000 && b >= 1_000);
    let mixed = phrases(&corpus, nq, &mut rng, |a, b| (a < 20) != (b < 20));
    let common = phrases(&corpus, nq, &mut rng, |a, b| a < 20 && b < 20);
    time("terms-2", &index, &terms2, 10)?;
    time("terms-2-hub", &index, &hub, 10)?;
    time("phrase-rare", &index, &as_phrase(&rare, 0), 10)?;
    time("phrase-mixed", &index, &as_phrase(&mixed, 0), 10)?;
    time("phrase-hub", &index, &as_phrase(&common, 0), 10)?;
    time("slop-2", &index, &as_phrase(&mixed, 2), 10)?;
    time("slop-10", &index, &as_phrase(&mixed, 10), 10)?;
    time("terms-2-k1000", &index, &terms2, 1_000)?;
    // A Zipf-drawn word with one digit replaced (`w` and at least one digit stay).
    let typos: Vec<String> = (0..nq)
        .map(|_| {
            let mut w = word(z.rank(&mut rng)).into_bytes();
            let at = 1 + rng.below(w.len() - 1);
            w[at] = b'0' + ((w[at] - b'0' + 1 + rng.below(9) as u8) % 10);
            String::from_utf8(w).expect("ASCII")
        })
        .collect();
    let fuzzy = |f: Fuzziness| -> Vec<Query> {
        typos
            .iter()
            .map(|t| Query::new().fuzzy(t.clone(), f))
            .collect()
    };
    time("fuzzy-1", &index, &fuzzy(Fuzziness::One), 10)?;
    time("fuzzy-2", &index, &fuzzy(Fuzziness::Two), 10)?;
    time_with("fuzzy-2-prefix2", &index, &fuzzy(Fuzziness::Two), 10, 2)?;
    let fuzzy_term: Vec<Query> = typos
        .iter()
        .map(|t| {
            Query::new()
                .fuzzy(t.clone(), Fuzziness::One)
                .term(word(z.rank(&mut rng)))
        })
        .collect();
    time("fuzzy-1-term", &index, &fuzzy_term, 10)?;
    drop(index);
    db.close()?;
    Ok(())
}

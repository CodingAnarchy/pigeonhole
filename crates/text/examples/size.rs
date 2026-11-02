//! The cost of positions (D309 §2, §7): indexes one seeded corpus twice, with positions on
//! and off, and prints the index table's stored bytes by family (after a flush and a full
//! compaction) and the write time per document (`Buffered`, so a commit's fsync doesn't
//! dominate).
//!
//!     cargo run --release -p pigeonhole-text --example size -- [DOCS] [WORDS_PER_DOC]
//!
//! The corpus: `DOCS` documents (default 20,000) of about `WORDS_PER_DOC` words (default 40,
//! the length of an MS MARCO passage), drawn from a 50,000-word vocabulary with Zipf
//! frequencies. The file's own size moves in preallocated steps, so it isn't the measure.

use std::time::Instant;

use pigeonhole::{Durability, Family, Options, Pigeonhole};
use pigeonhole_text::{Index, IndexOptions, Source};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

const VOCAB: usize = 50_000;

/// A word index with Zipf (s = 1) frequencies, by inverting the harmonic CDF.
fn zipf(rng: &mut Rng, cdf: &[f64]) -> usize {
    let u = rng.unit();
    cdf.partition_point(|&c| c < u).min(VOCAB - 1)
}

fn corpus(docs: usize, words: usize) -> Vec<String> {
    let mut cdf = Vec::with_capacity(VOCAB);
    let mut acc = 0.0;
    for i in 1..=VOCAB {
        acc += 1.0 / i as f64;
        cdf.push(acc);
    }
    for c in &mut cdf {
        *c /= acc;
    }
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    (0..docs)
        .map(|_| {
            let n = words / 2 + (rng.next() as usize % words.max(1));
            (0..n)
                .map(|_| format!("w{}", zipf(&mut rng, &cdf)))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

/// The index table's stored bytes by family, its total, and the write time per document.
type Measured = (Vec<(String, u64)>, u64, f64);

fn run(texts: &[String], positions: bool) -> Result<Measured, Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!(
        "pigeonhole-text-size-{}-{positions}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("size.phdb");
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
    for (i, t) in texts.iter().enumerate() {
        index.put(format!("d{i:08}").as_bytes(), t)?;
    }
    let per_doc = start.elapsed().as_secs_f64() * 1e6 / texts.len() as f64;
    drop(index);
    db.flush()?;
    db.compact()?;
    let info = db.info()?;
    let table = info
        .tables
        .iter()
        .find(|t| t.name == "docs_text")
        .ok_or("no index table")?;
    let fams: Vec<(String, u64)> = table
        .storage
        .iter()
        .map(|f| {
            let sst: u64 = f.levels.iter().map(|l| l.bytes).sum();
            (f.family.clone(), sst + f.blob_bytes)
        })
        .collect();
    let total = fams.iter().map(|f| f.1).sum();
    db.close()?;
    std::fs::remove_dir_all(&dir)?;
    Ok((fams, total, per_doc))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let docs: usize = args.next().map_or(Ok(20_000), |a| a.parse())?;
    let words: usize = args.next().map_or(Ok(40), |a| a.parse())?;
    let texts = corpus(docs, words);
    let source: usize = texts.iter().map(String::len).sum();
    println!("{docs} documents, {source} bytes of text");
    let (off_f, off, off_us) = run(&texts, false)?;
    let (on_f, on, on_us) = run(&texts, true)?;
    println!("positions off: {off} bytes, {off_us:.1} µs per document; {off_f:?}");
    println!("positions on:  {on} bytes, {on_us:.1} µs per document; {on_f:?}");
    println!(
        "positions add {:.1}% to the file and {:.1}% to the write time",
        100.0 * (on as f64 - off as f64) / off as f64,
        100.0 * (on_us - off_us) / off_us
    );
    Ok(())
}

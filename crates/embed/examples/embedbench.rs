//! The local model's CPU cost (D318 §4): latency per text at 16, 64 and 256 tokens, and
//! throughput in batches of 32, on 1 thread and on the given thread counts.
//!
//! ```sh
//! cargo run --release -p pigeonhole-embed --example embedbench -- MODEL_DIR [THREADS,...] [ROUNDS]
//! ```
//!
//! `MODEL_DIR` holds the model's three files. The default thread counts are 1, 4 and every
//! core; rounds default to 200 (single texts) and 10 (batches).

use std::time::Instant;

use pigeonhole_embed::{LoadOptions, LocalEmbedder, Model};

/// A text of about `tokens` tokens (one per short common word, plus [CLS] and [SEP]).
fn text(tokens: usize) -> String {
    const WORDS: [&str; 8] = [
        "the", "meeting", "moved", "to", "thursday", "after", "lunch", "again",
    ];
    (0..tokens.saturating_sub(2))
        .map(|i| WORDS[i % WORDS.len()])
        .collect::<Vec<_>>()
        .join(" ")
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let dir = args
        .next()
        .ok_or("usage: embedbench MODEL_DIR [THREADS,...] [ROUNDS]")?;
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    let threads: Vec<usize> = match args.next() {
        Some(t) => t.split(',').map(str::parse).collect::<Result<_, _>>()?,
        None => {
            let mut t = vec![1, 4, cores];
            t.dedup();
            t
        }
    };
    let rounds: usize = args.next().map_or(Ok(200), |r| r.parse())?;
    println!("model {} | cores {cores}", Model::BgeSmallEnV15.id());
    println!("threads | tokens | p50 ms | p99 ms | batch-32 texts/s");
    for &n in &threads {
        let e = LocalEmbedder::open(
            Model::BgeSmallEnV15,
            &LoadOptions::new().dir(&dir).threads(n),
        )?;
        for tokens in [16, 64, 256] {
            let t = text(tokens);
            for _ in 0..5 {
                e.embed_passages(&[&t])?;
            }
            let mut ms: Vec<f64> = (0..rounds)
                .map(|_| {
                    let s = Instant::now();
                    e.embed_passages(&[&t])
                        .map(|_| s.elapsed().as_secs_f64() * 1e3)
                })
                .collect::<Result<_, _>>()?;
            ms.sort_by(f64::total_cmp);
            let batch: Vec<&str> = std::iter::repeat_n(t.as_str(), 32).collect();
            let brounds = (rounds / 20).max(3);
            let s = Instant::now();
            for _ in 0..brounds {
                e.embed_passages(&batch)?;
            }
            let rate = (32 * brounds) as f64 / s.elapsed().as_secs_f64();
            println!(
                "{n:7} | {tokens:6} | {:6.2} | {:6.2} | {rate:8.1}",
                pct(&ms, 0.5),
                pct(&ms, 0.99)
            );
        }
    }
    Ok(())
}

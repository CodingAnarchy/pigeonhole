//! Agent memory's measured shapes (#1828), one per run, for `scripts/instructions-per-cell.sh`:
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example memshapes -- SHAPE ITERATIONS DIR
//! ```
//!
//! | Shape | One iteration | Units (`units N` on stderr) |
//! |---|---|---|
//! | `memory-recall` | 64 fused recalls (`Store::recall`, the default weights, text and an embedding, `k` 10) over one namespace of 2,000 memories written in setup | recalls |
//!
//! The memories are synthetic and the same on every run: 8 to 23 words each, drawn from a
//! fixed 512-word vocabulary by a seeded generator, and a 64-dimension `f32` embedding from
//! the same generator. The queries are 3 words with an embedding of their own, so every recall
//! runs all three of its default signals: vector similarity over the whole namespace (no IVF),
//! BM25 over the text index, and recency.
//!
//! A memory's id is its creation time plus 8 random bytes, so ties in the fusion sort may fall
//! differently between runs. Recall's clock is fixed (`now_us`), so recency doesn't move with
//! the wall clock. The shards are application-owned (`support/shards.rs`), and their work is
//! counted with the caller's.
//!
//! Only the measured work runs inside functions named `shape_*`, each counting its thread's
//! work with a `Measured` guard (`support/measure.rs`).
#[path = "support/measure.rs"]
mod measure;
#[path = "support/shards.rs"]
mod shards;
use measure::Measured;
use shards::Shards;

use pigeonhole::Element;
use pigeonhole_memory::{EmbeddingModel, Memory, Store, StoreOptions};

const STORE: &str = "bench";
const NS: &str = "agent";
/// Memories in the namespace.
const MEMORIES: usize = 2_000;
/// Recalls per iteration.
const PER_ITERATION: usize = 64;
const DIM: usize = 64;
const VOCABULARY: usize = 512;
/// The recall clock, fixed and later than any setup memory: recency ranks by creation order,
/// which is the setup's write order, whatever the wall clock says.
const NOW_US: u64 = 1_900_000_000_000_000;

fn main() {
    measure::fix_hash_seeds();
    let mut args = std::env::args().skip(1);
    let usage = "usage: memshapes SHAPE ITERATIONS DIR";
    let shape = args.next().expect(usage);
    let iters: usize = args.next().expect(usage).parse().expect("ITERATIONS");
    let base = std::path::PathBuf::from(args.next().expect(usage));
    let dir = base.join(format!("phdb-memshapes-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    // `SHAPE_SETUP_ONLY=1`: the setup alone, without the measured work.
    let setup_only = std::env::var_os("SHAPE_SETUP_ONLY").is_some();
    let units = match shape.as_str() {
        "memory-recall" => {
            let (db, shards) = Shards::open(&dir.join("m.phdb"), None);
            let model = EmbeddingModel::new("memshapes", DIM as u32, Element::F32).unwrap();
            let store = Store::create(&db, STORE, StoreOptions::new().embedding(model)).unwrap();
            let mut rng = Rng(0x5eed_1828);
            let words = vocabulary();
            for _ in 0..MEMORIES {
                let n = 8 + rng.below(16);
                let text = sentence(&mut rng, &words, n);
                let memory = Memory::new(text).embedding(embedding(&mut rng));
                store.remember(NS, memory).unwrap();
            }
            let queries: Vec<(String, Vec<f32>)> = (0..PER_ITERATION * iters)
                .map(|_| (sentence(&mut rng, &words, 3), embedding(&mut rng)))
                .collect();
            // The measured queries once, outside the count. A recall reads counters: the
            // namespace's (`stats:embedded`, D311 T8, 2,000 setup increments) and the text
            // index's per-term ones. Its first fold of a long chain sends the shard a D291
            // checkpoint, applied on the shard's thread; until then, every recall re-folds the
            // chain. Measured, how many did depended on the host's thread scheduling: +3% to
            // +6% on some runners (#2007, #2029, #2033; #2040 for what this costs an app).
            for (text, e) in &queries {
                std::hint::black_box(
                    store
                        .recall(&[NS])
                        .text(text.as_str())
                        .embedding(e.clone())
                        .k(10)
                        .now_us(NOW_US)
                        .run()
                        .unwrap(),
                );
            }
            // Then wait, still outside the count, until the shard has applied them: the
            // checkpoint count holds still for 100 ms.
            let (mut seen, mut still) = (checkpoints(&db), std::time::Instant::now());
            let t0 = std::time::Instant::now();
            while still.elapsed() < std::time::Duration::from_millis(100) {
                assert!(
                    t0.elapsed() < std::time::Duration::from_secs(60),
                    "counter checkpoints still landing after 60 s"
                );
                std::thread::sleep(std::time::Duration::from_millis(5));
                let now = checkpoints(&db);
                if now != seen {
                    (seen, still) = (now, std::time::Instant::now());
                }
            }
            let mut recalled = 0;
            if !setup_only {
                let before = checkpoints(&db);
                shards.measure(&mut || recalled = shape_memory_recall(&store, &queries));
                // simp724: a measured recall that folds a long counter chain again (its
                // checkpoint not in place before the count) fails the shape, not drifts it.
                assert_eq!(
                    checkpoints(&db),
                    before,
                    "a counter checkpoint landed inside the measured recalls"
                );
                assert_eq!(recalled, queries.len());
            }
            drop(store);
            db.close().unwrap();
            shards.join();
            recalled
        }
        other => panic!("unknown shape {other}: memory-recall"),
    };
    eprintln!("units {}", if setup_only { 0 } else { units });
    std::fs::remove_dir_all(&dir).ok();
}

/// splitmix64: the same memories and queries on every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// `VOCABULARY` distinct lowercase words of 4 to 9 letters.
fn vocabulary() -> Vec<String> {
    let mut rng = Rng(0x0057_0cab);
    let mut words = std::collections::BTreeSet::new();
    while words.len() < VOCABULARY {
        let len = 4 + rng.below(6);
        let w: String = (0..len)
            .map(|_| (b'a' + rng.below(26) as u8) as char)
            .collect();
        words.insert(w);
    }
    words.into_iter().collect()
}

/// `n` words, the low-numbered ones more often (a skewed draw, as in natural text).
fn sentence(rng: &mut Rng, words: &[String], n: usize) -> String {
    (0..n)
        .map(|_| {
            let a = rng.below(words.len());
            let b = rng.below(words.len());
            words[a.min(b)].as_str()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn embedding(rng: &mut Rng) -> Vec<f32> {
    (0..DIM)
        .map(|_| (rng.next() >> 40) as f32 / (1u64 << 24) as f32 - 0.5)
        .collect()
}

#[inline(never)]
/// D291 counter checkpoints the shard has kept since open (a test hook).
fn checkpoints(db: &pigeonhole::Pigeonhole) -> u64 {
    db.internal_metrics()
        .into_iter()
        .find(|(n, _)| *n == "counter_checkpoints_kept")
        .map_or(0, |(_, v)| v)
}

fn shape_memory_recall(store: &Store, queries: &[(String, Vec<f32>)]) -> usize {
    let _measured = Measured::start();
    let mut n = 0;
    for (text, e) in queries {
        let hits = store
            .recall(&[NS])
            .text(text.as_str())
            .embedding(e.clone())
            .k(10)
            .now_us(NOW_US)
            .run()
            .unwrap();
        std::hint::black_box(&hits);
        n += 1;
    }
    n
}

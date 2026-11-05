//! Text-search microbenchmarks (#1560 PR 0, perf287's T3): the instructions floor of
//! `pigeonhole-text`'s sync search, measured before its read path is made async. One shape
//! per run:
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example textshapes -- SHAPE [ITERATIONS] [DIR]
//! cargo run --release -p pigeonhole-bench --example textshapes -- build DIR
//! ```
//!
//! **The index:** 100,000 documents of 20 to 60 words, from a 50,000-word vocabulary with
//! Zipf frequencies: `pigeonhole-text`'s bench corpus (`crates/text/examples/bench.rs`), same
//! seed, at a tenth of its default size. It's indexed with positions, then flushed and fully
//! compacted. Its most common term, `w0`, is in [`HUB_POSTINGS`] (95,649) of them, which the
//! build checks. Its third, `w2`, is in 67,972: the hub term of `maxscore.rs`' measurement
//! (#1368). These are MaxScore's hard case, posting rows of many blocks that the walk mostly
//! skips.
//!
//! **One build per side.** With `TEXTSHAPES_INDEX=DIR` set, `build DIR` writes the index
//! there natively. The instructions workflow runs it once per side before the shapes, which
//! then only open it. Without the variable, a shape builds the index into its own scratch
//! directory first. The build reads a stepped wall clock from a fixed start (`FixedClock`),
//! so the index is byte-identical however and wherever it's built: wall-clock timestamps
//! would change the SSTs' encoding and block boundaries, and with them the counts (#1305).
//!
//! **Shapes.** Each runs 50 queries of one kind ([`QUERIES`]) per iteration, at top 10 unless
//! noted, over the warm index (a pass of every query runs before measuring). Counted per
//! search. Two independent builds of the index count within 4 instructions per search of each
//! other on `text-term-hub` (about 66 million each).
//! - `text-term-rare`: one term of rank 1,000 or more;
//! - `text-term-hub`: one of the 20 most common terms;
//! - `text-two-terms`, `text-five-terms`: two or five terms drawn by Zipf frequency;
//! - `text-two-terms-k100`: `text-two-terms` at top 100;
//! - `text-fuzzy`: a fuzzy term at 1 edit, a Zipf-drawn word with one digit replaced (a
//!   dense dictionary: the vocabulary is `w0` to `w49999`);
//! - `text-phrase`, `text-phrase-slop2`: a two-word phrase taken from a document, one word
//!   among the 20 most common and one not, at slop 0 and 2.
//!
//! It follows the shape-binary contract of `scripts/instructions-per-cell.sh`. The measured
//! work is one `#[inline(never)]` `shape_search` under a `Measured` guard, on a fresh thread,
//! printing `units N`. With `SHAPE_SETUP_ONLY=1` it opens and warms the index but searches
//! nothing.

#[path = "support/driver.rs"]
mod driver;
#[path = "support/inline_io.rs"]
mod inline_io;
#[path = "support/measure.rs"]
mod measure;

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use measure::Measured;
use pigeonhole::{Durability, Family, Options};
use pigeonhole_io::{
    Completion, FileRef, OpenOptions, ProcessId, Result as IoResult, SharedOpen, SharedRegion, Vfs,
    VfsRef,
};
use pigeonhole_text::{Fuzziness, Index, IndexOptions, Query, Source};

/// Documents in the index.
const DOCS: usize = 100_000;
/// The vocabulary: `w0` to `w49999`.
const VOCAB: usize = 50_000;
/// Queries per shape: 50, each searched once per iteration (the job's 4: 200 searches). A
/// hub search is about 66 million instructions, so 200 per shape keeps the job's added
/// valgrind time to a few minutes per side.
const QUERIES: usize = 50;
/// Documents holding the most common term, `w0`, checked by `build`: it pins the corpus.
const HUB_POSTINGS: usize = 95_649;
/// The index directory's name inside `DIR` when the shape builds it itself.
const INDEX_DIR: &str = "phdb-textshapes-index";
/// The wall clock's start (µs since the epoch), and how far it moves per reading.
const CLOCK_START_US: u64 = 1_600_000_000_000_000;
/// A reopened index's clock starts this far after the build's, so no timestamp it reads is
/// earlier than one the build wrote.
const REOPEN_OFFSET_US: u64 = 1 << 40;

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

/// Zipf ranks over the vocabulary, as `pigeonhole-text`'s bench draws them.
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

/// The corpus, as each document's word ranks (the text bench's seed and lengths).
fn corpus() -> Vec<Vec<usize>> {
    let z = Zipf::new();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    (0..DOCS)
        .map(|_| {
            let n = 20 + rng.below(41);
            (0..n).map(|_| z.rank(&mut rng)).collect()
        })
        .collect()
}

fn doc_id(i: usize) -> Vec<u8> {
    format!("d{i:08}").into_bytes()
}

/// `InlineVfs` (inline I/O, a stepped monotonic clock) with a stepped wall clock too, from
/// `start`: the build's commit timestamps, and so its SSTs, are the same on every run.
/// Files come straight from the inner VFS, so reads aren't wrapped.
struct FixedClock {
    inner: VfsRef,
    wall: AtomicU64,
    /// `random_u64`'s sequence: a file's random ids are the same on every build too.
    random: AtomicU64,
}

impl FixedClock {
    fn vfs(start: u64) -> VfsRef {
        Arc::new(FixedClock {
            inner: inline_io::InlineVfs::vfs(),
            wall: AtomicU64::new(start),
            random: AtomicU64::new(0x1560_0000),
        })
    }
}

impl fmt::Debug for FixedClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedClock").finish_non_exhaustive()
    }
}

impl Vfs for FixedClock {
    fn open(&self, path: &Path, opts: OpenOptions) -> IoResult<FileRef> {
        self.inner.open(path, opts)
    }

    fn remove(&self, path: &Path) -> IoResult<()> {
        self.inner.remove(path)
    }

    fn exists(&self, path: &Path) -> IoResult<bool> {
        self.inner.exists(path)
    }

    fn list_dir(&self, dir: &Path) -> IoResult<Vec<PathBuf>> {
        self.inner.list_dir(dir)
    }

    fn sync_dir(&self, dir: &Path) -> IoResult<()> {
        self.inner.sync_dir(dir)
    }

    fn submit_sync_dir(&self, dir: &Path) -> Completion<()> {
        self.inner.submit_sync_dir(dir)
    }

    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: SharedOpen,
    ) -> IoResult<SharedRegion> {
        self.inner.open_shared(name, dir, len, mode)
    }

    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> IoResult<()> {
        self.inner.remove_shared(name, dir)
    }

    fn now_micros(&self) -> u64 {
        self.wall.fetch_add(1, Ordering::Relaxed)
    }

    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos()
    }

    fn current_process(&self) -> ProcessId {
        self.inner.current_process()
    }

    fn random_u64(&self) -> u64 {
        // splitmix64 over a counter: deterministic, and well spread.
        let mut z = self
            .random
            .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
            .wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

fn options(clock_start: u64) -> Options {
    Options::default()
        .vfs(FixedClock::vfs(clock_start))
        .durability(Durability::Buffered)
        .block_cache(256 << 20)
}

/// Indexes the corpus into `dir` (which must not exist), flushes and fully compacts it, and
/// checks the hub term's postings. Written under a temporary name and renamed, so a
/// directory that exists is a complete index.
fn build(dir: &Path) {
    let tmp = dir.with_extension(format!("building-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).expect("create the build directory");
    let (db, driver) = driver::Driver::open(&tmp.join("text.phdb"), options(CLOCK_START_US));
    let docs = db
        .table("docs")
        .unwrap()
        .family("body", Family::default())
        .create()
        .unwrap();
    let index = Index::create(
        &db,
        "docs_text",
        Source::new(&docs, "body", b"text"),
        IndexOptions::new().positions(true),
    )
    .unwrap();
    for (i, d) in corpus().iter().enumerate() {
        let text = d.iter().map(|&r| word(r)).collect::<Vec<_>>().join(" ");
        index.put(&doc_id(i), &text).unwrap();
        // A flush every 20,000 documents bounds the build's memory (the driver's memtable
        // budget never flushes on its own). The full compaction below merges them.
        if (i + 1) % 20_000 == 0 {
            db.flush().unwrap();
        }
    }
    db.flush().unwrap();
    db.compact().unwrap();
    // The space the flushes and the compaction freed goes back, so the file is the index.
    db.shrink().unwrap();
    driver.wait_idle();
    let hub = index
        .search(Query::new().term(word(0)), DOCS)
        .run()
        .unwrap()
        .len();
    let third = index
        .search(Query::new().term(word(2)), DOCS)
        .run()
        .unwrap()
        .len();
    eprintln!("hub postings: w0 {hub}, w2 {third}");
    assert_eq!(
        hub, HUB_POSTINGS,
        "the hub term's postings: the corpus changed, so the shapes' documentation is wrong"
    );
    drop((index, docs));
    db.close().unwrap();
    driver.join();
    std::fs::rename(&tmp, dir).expect("move the built index into place");
    eprintln!("built {} documents; hub postings {hub}", DOCS);
}

/// Two-word phrases from documents, one word among the 20 most common and one not.
fn phrases(corpus: &[Vec<usize>], rng: &mut Rng) -> Vec<(String, String)> {
    let mut out = Vec::with_capacity(QUERIES);
    while out.len() < QUERIES {
        let d = &corpus[rng.below(corpus.len())];
        let i = rng.below(d.len() - 1);
        if (d[i] < 20) != (d[i + 1] < 20) {
            out.push((word(d[i]), word(d[i + 1])));
        }
    }
    out
}

/// The shape's queries and `k`, seeded: the same on every run.
fn queries(shape: &str) -> (Vec<Query>, usize) {
    let z = Zipf::new();
    let mut rng = Rng(0x5EED_1560);
    let terms = |n: usize, rng: &mut Rng| -> Vec<Query> {
        (0..QUERIES)
            .map(|_| (0..n).fold(Query::new(), |q, _| q.term(word(z.rank(rng)))))
            .collect()
    };
    match shape {
        "text-term-rare" => (
            (0..QUERIES)
                .map(|_| Query::new().term(word(1_000 + rng.below(VOCAB - 1_000))))
                .collect(),
            10,
        ),
        "text-term-hub" => (
            (0..QUERIES)
                .map(|_| Query::new().term(word(rng.below(20))))
                .collect(),
            10,
        ),
        "text-two-terms" => (terms(2, &mut rng), 10),
        "text-five-terms" => (terms(5, &mut rng), 10),
        "text-two-terms-k100" => (terms(2, &mut rng), 100),
        "text-fuzzy" => (
            (0..QUERIES)
                .map(|_| {
                    let mut w = word(z.rank(&mut rng)).into_bytes();
                    let at = 1 + rng.below(w.len() - 1);
                    w[at] = b'0' + ((w[at] - b'0' + 1 + rng.below(9) as u8) % 10);
                    Query::new().fuzzy(String::from_utf8(w).expect("ASCII"), Fuzziness::One)
                })
                .collect(),
            10,
        ),
        "text-phrase" | "text-phrase-slop2" => {
            let slop = if shape == "text-phrase" { 0 } else { 2 };
            let ps = phrases(&corpus(), &mut rng);
            (
                ps.into_iter()
                    .map(|(a, b)| Query::new().phrase_with_slop([a, b], slop))
                    .collect(),
                10,
            )
        }
        other => panic!(
            "unknown shape {other}: text-term-rare, text-term-hub, text-two-terms, \
             text-five-terms, text-two-terms-k100, text-fuzzy, text-phrase or text-phrase-slop2"
        ),
    }
}

/// The measured work: every query once. Returns the searches run.
#[inline(never)]
fn shape_search(index: &Index, queries: &[Query], k: usize) -> usize {
    let _measured = Measured::start();
    let mut hits = 0usize;
    for q in queries {
        hits += index.search(q.clone(), k).run().unwrap().len();
    }
    std::hint::black_box(hits);
    queries.len()
}

fn main() {
    measure::fix_hash_seeds();
    let mut args = std::env::args().skip(1);
    let shape = args.next().expect("SHAPE");
    if shape == "build" {
        let dir = PathBuf::from(args.next().expect("build DIR"));
        if !dir.exists() {
            build(&dir);
        }
        return;
    }
    let iters: usize = args.next().map_or(4, |s| s.parse().expect("ITERATIONS"));
    let base = args.next().map_or_else(std::env::temp_dir, PathBuf::from);
    let dir =
        std::env::var_os("TEXTSHAPES_INDEX").map_or_else(|| base.join(INDEX_DIR), PathBuf::from);
    if !dir.exists() {
        build(&dir);
    }
    // The queries are built before the index is opened, as the measured iterations' inputs.
    let (queries, k) = queries(&shape);
    // Read-only (#1348): the open writes nothing, so every shape reads the same bytes in
    // place, and nothing runs in the background.
    let (db, driver) = driver::Driver::open(
        &dir.join("text.phdb"),
        options(CLOCK_START_US + REOPEN_OFFSET_US).read_only(true),
    );
    let index = Index::open(&db, "docs_text").unwrap();
    // Warm: every query once, so the block cache holds what the measured passes read.
    for q in &queries {
        index.search(q.clone(), k).run().unwrap();
    }
    driver.wait_idle();
    // `SHAPE_SETUP_ONLY=1`: the setup alone (the script checks it counts nothing).
    let iters = if std::env::var_os("SHAPE_SETUP_ONLY").is_some() {
        0
    } else {
        iters
    };
    let mut units = 0;
    for _ in 0..iters {
        units += driver::on_fresh_thread(|| shape_search(&index, &queries, k));
    }
    eprintln!("units {units}");
    drop(index);
    db.close().unwrap();
    driver.join();
}

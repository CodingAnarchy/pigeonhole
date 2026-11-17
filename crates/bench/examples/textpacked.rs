//! Text-search microbenchmarks over a **packed** index (#1585, ICR 0143): `textshapes.rs`'
//! index, copied and packed (`Index::pack`, layout version 2), searched with the same
//! queries. One shape per run:
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example textpacked -- build SRC DIR
//! TEXTPACKED_INDEX=DIR cargo run --release -p pigeonhole-bench --example textpacked -- SHAPE
//! ```
//!
//! **The index.** `build SRC DIR` copies the index `textshapes build SRC` wrote (100,000
//! documents, with positions), packs the copy, then flushes, compacts and shrinks it, with
//! the same stepped clock, so the packed file is laid out the same wherever it's built. The
//! instructions workflow runs it once per side, after the textshapes build. Every posting
//! of a term with 32 or more is then in a block; the build checks that the common terms are.
//!
//! **Shapes**, each the queries of the `textshapes` shape it is named after, at top 10,
//! counted per search, one pass:
//! - `text-term-hub-packed`: one of the 20 most common terms. #1585's measure: against
//!   `text-term-hub`, the same searches over cells;
//! - `text-two-terms-packed`: two terms drawn by Zipf frequency (the walk beside point
//!   lookups into blocks);
//! - `text-phrase-packed`: a two-word phrase from a document, one word common (the phrase
//!   join over blocks).
//!
//! **Why its own file, with `textshapes.rs`' corpus helpers copied in.** The workflow builds
//! the base with the head's example sources. A file that uses an API its PR adds
//! (`Index::pack`) can't build there, and base then runs its own examples, with every shape
//! of a changed file (or of all files, if `support/` changed) marked not comparable. As a
//! new file, this one leaves `textshapes.rs` and `support/` untouched, so the unpacked text
//! shapes stay compared like for like in the PR that adds the packed tier.
//!
//! It follows the shape-binary contract of `scripts/instructions-per-cell.sh`, as
//! `textshapes.rs` does.

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
use pigeonhole::{Durability, Options};
use pigeonhole_io::{
    Completion, FileRef, OpenOptions, ProcessId, Result as IoResult, SharedOpen, SharedRegion, Vfs,
    VfsRef,
};
use pigeonhole_text::{Index, Query};

/// `textshapes.rs`' constants: its vocabulary, its queries per shape and their seed, its
/// phrase list's name, and its clock.
const VOCAB: usize = 50_000;
const QUERIES: usize = 50;
const PHRASES_FILE: &str = "phrases.txt";
const QUERY_SEED: u64 = 0x5EED_1560;
const CLOCK_START_US: u64 = 1_600_000_000_000_000;
/// The pack's clock starts this far after the textshapes build's, and a shape's read-only
/// open twice as far: no timestamp read is earlier than one written.
const PACK_OFFSET_US: u64 = 1 << 40;

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

/// Copies the textshapes index at `src` to `dir` (which must not exist) and packs the copy.
/// Written under a temporary name and renamed, so a directory that exists is complete.
fn build(src: &Path, dir: &Path) {
    let tmp = dir.with_extension(format!("building-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).expect("create the build directory");
    for entry in std::fs::read_dir(src).expect("the textshapes index: build it first") {
        let entry = entry.expect("list the textshapes index");
        if entry.file_type().expect("a file type").is_file() {
            std::fs::copy(entry.path(), tmp.join(entry.file_name())).expect("copy the index");
        }
    }
    let (db, driver) = driver::Driver::open(
        &tmp.join("text.phdb"),
        options(CLOCK_START_US + PACK_OFFSET_US),
    );
    let index = Index::open(&db, "docs_text").unwrap();
    let before = index.search(Query::new().term(word(0)), 10).run().unwrap();
    let packed = index.pack().unwrap();
    db.flush().unwrap();
    db.compact().unwrap();
    db.shrink().unwrap();
    driver.wait_idle();
    let stats = index.verify().unwrap();
    eprintln!("packed: {packed:?}; {stats:?}");
    assert!(stats.is_consistent(), "{stats:?}");
    // Zipf's tail: most terms are rare and stay cells, but they hold few of the postings.
    assert!(
        stats.packed * 10 >= stats.postings * 8,
        "most postings are in blocks: {stats:?}"
    );
    assert_eq!(stats.tombstones, 0);
    let after = index.search(Query::new().term(word(0)), 10).run().unwrap();
    assert_eq!(after, before, "the pack changed a search's result");
    drop(index);
    db.close().unwrap();
    driver.join();
    std::fs::rename(&tmp, dir).expect("move the packed index into place");
}

/// The shape's queries, as `textshapes.rs` draws the shape it is named after.
fn queries(shape: &str, dir: &Path) -> Vec<Query> {
    let z = Zipf::new();
    let mut rng = Rng(QUERY_SEED);
    match shape {
        "text-term-hub-packed" => (0..QUERIES)
            .map(|_| Query::new().term(word(rng.below(20))))
            .collect(),
        "text-two-terms-packed" => (0..QUERIES)
            .map(|_| (0..2).fold(Query::new(), |q, _| q.term(word(z.rank(&mut rng)))))
            .collect(),
        "text-phrase-packed" => std::fs::read_to_string(dir.join(PHRASES_FILE))
            .expect("the phrase list the textshapes build wrote")
            .lines()
            .map(|l| {
                let (a, b) = l.split_once(' ').expect("a phrase line is `a b`");
                Query::new().phrase([a, b])
            })
            .collect(),
        other => panic!(
            "unknown shape {other}: text-term-hub-packed, text-two-terms-packed or \
             text-phrase-packed"
        ),
    }
}

/// The measured work: every query once. Returns the searches run.
#[inline(never)]
fn shape_search(index: &Index, queries: &[Query]) -> usize {
    let _measured = Measured::start();
    let mut hits = 0usize;
    for q in queries {
        hits += index.search(q.clone(), 10).run().unwrap().len();
    }
    std::hint::black_box(hits);
    queries.len()
}

fn main() {
    measure::fix_hash_seeds();
    let mut args = std::env::args().skip(1);
    let shape = args.next().expect("SHAPE");
    if shape == "build" {
        let src = PathBuf::from(args.next().expect("build SRC DIR"));
        let dir = PathBuf::from(args.next().expect("build SRC DIR"));
        if !dir.exists() {
            build(&src, &dir);
        }
        return;
    }
    // One pass, whatever `ITERATIONS` says, as `textshapes.rs`.
    let iters: usize = args
        .next()
        .map_or(1, |s| s.parse::<usize>().expect("ITERATIONS").min(1));
    let dir = PathBuf::from(
        std::env::var_os("TEXTPACKED_INDEX")
            .expect("TEXTPACKED_INDEX: the directory `textpacked build SRC DIR` wrote"),
    );
    let queries = queries(&shape, &dir);
    // Read-only (#1348): the open writes nothing, and nothing runs in the background.
    let (db, driver) = driver::Driver::open(
        &dir.join("text.phdb"),
        options(CLOCK_START_US + 2 * PACK_OFFSET_US).read_only(true),
    );
    let index = Index::open(&db, "docs_text").unwrap();
    // Warm: every query once, so the block cache holds what the measured pass reads.
    for q in &queries {
        index.search(q.clone(), 10).run().unwrap();
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
        units += driver::on_fresh_thread(|| shape_search(&index, &queries));
    }
    eprintln!("units {units}");
    drop(index);
    db.close().unwrap();
    driver.join();
}

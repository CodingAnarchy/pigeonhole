//! The format corpus (#496 PR 6, D269): small database files that every later build must
//! open, read, check and write. Each entry is a directory: the main file, its WAL files, and
//! `expected.txt`, every cell the generating build read back.
//!
//! - `tests/corpus/format4/<entry>/` is written by the current build. It is regenerated
//!   whenever an entry is added, and `corpus_entries_open_read_check_and_write` checks it.
//! - `tests/corpus/v<release>/<entry>/` is written once by a release build
//!   (`scripts/release-corpus.sh`, FORMAT §13) and frozen: `frozen.txt` holds each file's
//!   length and xxh3-64. `release_corpus_entries_open_read_check_and_write` checks every one.
//!   A build that can't read a release's files breaks the promise: fix the build, never the
//!   corpus.
//! - `tests/corpus/v0.*/` is the upgrade corpus, written by released 0.x builds
//!   (`upgrade_corpus_entries_open_upgrade_and_read_exactly`).
//!
//! `generate_corpus` is ignored; `cargo test -p pigeonhole --test corpus -- --ignored
//! generate_corpus` rewrites `format4/`. `PIGEONHOLE_CORPUS_OUT=<dir>` writes the entries to
//! another directory instead (CI's 1.0.0 job opens them with the published 1.0.0), and
//! `PIGEONHOLE_CORPUS_RELEASE=<version>` writes and freezes `v<version>/` (only from that
//! version's build). Entries are written through the simulator, so a crash state is exact,
//! and their files exported as they were left.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use std::time::Duration;

use pigeonhole::{
    Aggregation, Durability, Element, Family, FeedRetention, Metric, Options, Pigeonhole, Value,
    VectorSpec,
};
use pigeonhole_io::sim::{CrashKind, SimVfs};
use pigeonhole_io::{OpenOptions, Vfs};

/// Where the corpus lives.
fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/format4")
}

/// The release corpora: written by each release's build, then frozen. A directory holding
/// only its README is a placeholder until that release writes it.
const RELEASE_CORPORA: [&str; 3] = ["1.0.0", "1.1.0", "1.2.0"];

/// Entries only a build with a feature has, each with whether this build has it: `replica`
/// needs `__unstable-replication`, which `scripts/release-corpus.sh` writes the release corpus
/// with. A build without the feature still checks such an entry's files against `frozen.txt`,
/// but skips opening it; a build with it must open it (ai1249's N1 on #1790).
const FEATURE_ENTRIES: [(&str, bool); 1] = [("replica", cfg!(feature = "__unstable-replication"))];

/// The frozen hash list's name inside a release corpus.
const FROZEN: &str = "frozen.txt";

/// The database file's name inside an entry.
const DB: &str = "data.phdb";

/// A merge operator every corpus open registers: concatenation of stored payloads.
#[derive(Debug)]
struct Concat;

impl pigeonhole::MergeOperator for Concat {
    fn name(&self) -> &str {
        "corpus.concat"
    }
    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), pigeonhole::MergeError> {
        // Stored values: a tag byte, then the payload.
        let newer = acc.split_off(1);
        acc.extend_from_slice(&older[1..]);
        acc.extend_from_slice(&newer);
        Ok(())
    }
    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), pigeonhole::MergeError> {
        if let Some(base) = base {
            let newer = acc.split_off(1);
            acc.extend_from_slice(&base[1..]);
            acc.extend_from_slice(&newer);
        }
        Ok(())
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// A value as `expected.txt` writes it. A vector is its element, dimension and stored bytes,
/// which every element reads back exactly.
fn value_line(v: Value<'_>) -> String {
    match v {
        Value::Bytes(b) => format!("bytes:{}", hex(b)),
        Value::I64(n) => format!("i64:{n}"),
        Value::F64(x) => format!("f64:{:016x}", x.to_bits()),
        Value::Varint(n) => format!("varint:{n}"),
        Value::Vector(v) => format!("vector:{:?}:{}:{}", v.element(), v.dim(), hex(v.as_bytes())),
        other => panic!("a value type this test doesn't know: {other:?}"),
    }
}

/// Every cell of every table, all retained versions, one line each, in scan order.
fn contents(db: &Pigeonhole) -> Vec<String> {
    let mut tables = db.tables();
    tables.sort();
    let mut lines = Vec::new();
    for name in tables {
        let t = db.table(&name).unwrap().open().unwrap();
        let scan = t.scan_prefix(b"").versions(0);
        let mut it = scan.iter().unwrap();
        while let Some(row) = it.next_ref().unwrap() {
            for e in row.iter() {
                let value = value_line(e.cell.typed());
                lines.push(format!(
                    "{name}\t{}\t{}\t{}\t{}\t{value}",
                    hex(row.key()),
                    e.family,
                    hex(e.qualifier),
                    e.cell.timestamp()
                ));
            }
        }
    }
    lines
}

fn sim_options(vfs: &Arc<SimVfs>, shards: usize) -> Options {
    Options::default()
        .vfs(Arc::clone(vfs) as _)
        .shards(shards)
        .memtable_budget(4 << 20)
        .wal_segment_size(256 << 10)
        .merge_operator(Arc::new(Concat))
}

/// Timestamps fixed, so `expected.txt` does not depend on a clock.
const T0: u64 = 1_767_225_600_000_000;

/// Writes `e` on a new simulator seeded with `seed`, at `path`, and leaves it as the entry
/// says: crashed, or closed cleanly with its free tail given back. `observer_delay` widens the
/// window between each root commit's durable superblock and its feed sidecar entry (zero: as
/// the engine runs; the regression test of #1504 sets it).
///
/// A crash entry's crash waits for any root commit in progress, and keeps new ones from
/// starting, until the crash has happened ([`Pigeonhole::hold_root_commits`]). Otherwise a
/// background compaction's root commit can be durable without its sidecar Root entry, and
/// the next open expires the feed (D310 §2.2's fail-closed rule, #1504): a real behaviour, but
/// not what a crash entry is for, and not reproducible.
///
/// The crash is a process crash, which keeps the sidecar's written but unsynced entries, so
/// the hold alone is enough. A power-loss crash entry would still expire its feed on an
/// unsynced sidecar entry, fail closed by design (simp724 on #1508).
fn write_entry(e: &Entry, seed: u64, path: &str, observer_delay: Duration) -> Arc<SimVfs> {
    let vfs = SimVfs::new(seed);
    let mut db = Pigeonhole::open(path, (e.options)(sim_options(&vfs, e.shards))).unwrap();
    db.delay_root_observer(observer_delay);
    (e.write)(&db);
    if let Some(then) = e.then {
        db.close().unwrap();
        db = Pigeonhole::open(path, (e.then_options)(sim_options(&vfs, e.shards))).unwrap();
        db.delay_root_observer(observer_delay);
        then(&db);
    }
    if e.crash {
        let hold = db
            .hold_root_commits(Duration::from_secs(60))
            .expect("a root commit still in progress after 60 s");
        vfs.crash(CrashKind::Process);
        // Releasing commits what queued meanwhile, now onto the crashed simulator: each such
        // commit fails, as it would have in the crashed process, and leaves the files as the
        // crash left them (feed1248 on #1508).
        drop(hold);
        drop(db);
    } else {
        // Keep the checked-in files small: give back the file's free tail.
        db.shrink().unwrap();
        db.close().unwrap();
    }
    vfs
}

/// #1504: with every root commit's sidecar entry delayed by 100 ms, the crash entries still
/// reopen with their feed intact. Without the generator's hold on root commits, `feed_crash`
/// fails here every time (5 of 5 runs; 2 of 3 with a 20 ms delay): a compaction's root is
/// durable at the crash and its Root entry is not, so the reopen expires every token
/// (`FeedExpired: history has a gap`).
#[test]
fn crash_entries_survive_a_slow_root_observer() {
    for (seed, e) in entries().into_iter().enumerate().filter(|(_, e)| e.crash) {
        let path = format!("/db/{DB}");
        let vfs = write_entry(&e, 9_000 + seed as u64, &path, Duration::from_millis(100));
        let db = Pigeonhole::open(&path, sim_options(&vfs, e.shards)).unwrap();
        if let Some(extra) = e.extra {
            // The feed entries' listings resume from a stored token: an expired feed panics.
            assert!(!extra(&db).is_empty(), "{}: an empty listing", e.name);
        }
        db.close().unwrap();
    }
}

/// The marker file of an entry that 1.0.0 must refuse: it holds the `incompat` feature
/// bits the entry uses (decimal), which 1.0.0 lacks. The name stays, since the frozen
/// release corpora have it; what a release must do with the entry depends on which of those
/// bits it reads. CI's 1.0.0 arm fails if such an entry opens, or if
/// the refusal names other bits (`tests/corpus/compat-reader`), and its 1.1.0 arm, whose
/// release reads bit 0, fails if one using only bit 0 doesn't open (`compat-reader-1.1`).
const REFUSED_BY: &str = "refused-by-1.0.0.txt";

/// The file of an entry's extra listing: what [`Entry::extra`] read from the generating
/// build, compared with what the checking build reads.
const EXTRA: &str = "expected-extra.txt";

/// One corpus entry: how it is written, whether it is left crashed (WAL replay needed), and
/// the `incompat` feature bits it uses that 1.0.0 lacks (0: 1.0.0 opens it).
struct Entry {
    name: &'static str,
    shards: usize,
    /// Settings the writing open adds to the simulator's (the change feed's). A later open
    /// mentions none of them, as the check's doesn't.
    options: fn(Options) -> Options,
    write: fn(&Pigeonhole),
    crash: bool,
    refused_by_1_0_0: u64,
    /// What a structure built on the public API reads back (a text index's, a vector
    /// index's or a memory store's `verify`, the change feed's retained commits), one line
    /// each: only what every later build still reports, never a whole `Debug`.
    extra: Option<fn(&Pigeonhole) -> String>,
    /// A later session's writes: the generator closes the database cleanly after `write`,
    /// reopens it with the simulator's options alone (so it inherits what the first open
    /// set), and runs this before the entry's crash or close.
    then: Option<fn(&Pigeonhole)>,
    /// What the later session's open adds to the simulator's options ([`same`]: nothing, so
    /// it inherits what the first open set).
    then_options: fn(Options) -> Options,
    /// Whether 1.1.0, the first release with the feed, must resume the entry's stored table
    /// feed token and deliver what this build listed: `compat-reader-1.1` does it for an entry
    /// with the marker file [`FEED_RESUMES`] (the D315 amendment: a 1.2 sidecar after clean
    /// closes holds only entries 1.1.0 reads, when no session changed the catalog).
    feed_resumes_by_1_1: bool,
    /// The `ro_compat` feature bits the entry uses that the published releases lack: each
    /// must open it read-only and only read-only (marker file [`READ_ONLY_BY`]).
    read_only_by: u64,
}

/// The marker file of an entry the published releases must open read-only
/// ([`Entry::read_only_by`]): it holds those `ro_compat` bits (decimal). The compat
/// readers fail the entry if it opens read-write.
const READ_ONLY_BY: &str = "read-only-by-releases.txt";

/// The marker file of an entry whose stored feed token 1.1.0 must resume
/// ([`Entry::feed_resumes_by_1_1`]).
const FEED_RESUMES: &str = "feed-resumes-by-1.1.0.txt";

/// An entry's open adds nothing.
fn same(o: Options) -> Options {
    o
}

fn entries() -> Vec<Entry> {
    #[allow(unused_mut)]
    let mut entries = vec![
        Entry {
            name: "empty",
            shards: 1,
            options: same,
            write: |_| {},
            crash: false,
            refused_by_1_0_0: 0,
            extra: None,
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "values",
            shards: 1,
            options: same,
            write: write_values,
            crash: false,
            refused_by_1_0_0: 0,
            extra: None,
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "counters",
            shards: 2,
            options: same,
            write: write_counters,
            crash: false,
            refused_by_1_0_0: 0,
            extra: None,
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "blobs",
            shards: 1,
            options: same,
            write: write_blobs,
            crash: false,
            refused_by_1_0_0: 0,
            extra: None,
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "merge",
            shards: 1,
            options: same,
            write: write_merge,
            crash: false,
            refused_by_1_0_0: 0,
            extra: None,
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "wal_crash",
            shards: 2,
            options: same,
            write: write_wal_crash,
            crash: true,
            refused_by_1_0_0: 0,
            extra: None,
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "families",
            shards: 1,
            options: same,
            write: write_families,
            crash: false,
            refused_by_1_0_0: 0,
            extra: None,
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "wal_ops",
            shards: 2,
            options: same,
            write: write_wal_ops,
            crash: true,
            refused_by_1_0_0: 0,
            extra: None,
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "checkpoints",
            shards: 1,
            options: same,
            write: write_checkpoints,
            crash: false,
            refused_by_1_0_0: 0,
            extra: None,
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "aggregates",
            shards: 2,
            options: same,
            write: write_aggregates,
            crash: true,
            refused_by_1_0_0: pigeonhole_format::manifest::INCOMPAT_AGGREGATES,
            extra: None,
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "vectors",
            shards: 2,
            options: same,
            write: write_vectors,
            crash: false,
            refused_by_1_0_0: pigeonhole_format::manifest::INCOMPAT_AGGREGATES,
            extra: Some(vectors_extra),
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "text",
            shards: 1,
            options: same,
            write: write_text,
            crash: false,
            // The index keeps its statistics in an aggregate family.
            refused_by_1_0_0: pigeonhole_format::manifest::INCOMPAT_AGGREGATES,
            extra: Some(text_extra),
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "memory",
            shards: 1,
            options: same,
            write: write_memory,
            crash: false,
            refused_by_1_0_0: pigeonhole_format::manifest::INCOMPAT_AGGREGATES,
            extra: Some(memory_extra),
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "feed",
            shards: 2,
            options: feed_options,
            write: write_feed,
            crash: false,
            refused_by_1_0_0: 0,
            extra: Some(feed_extra),
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "feed_crash",
            shards: 2,
            options: feed_options,
            write: write_feed,
            crash: true,
            refused_by_1_0_0: 0,
            extra: Some(feed_extra),
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "feed_sessions",
            shards: 2,
            options: feed_options,
            write: write_feed_session,
            crash: false,
            refused_by_1_0_0: 0,
            extra: Some(feed_oldest_extra),
            then: Some(write_feed_session),
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "returning",
            shards: 1,
            options: same,
            write: write_returning,
            crash: false,
            refused_by_1_0_0: 0,
            extra: None,
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "feed_catalog",
            shards: 2,
            options: feed_options,
            write: write_feed_catalog,
            crash: false,
            refused_by_1_0_0: 0,
            extra: Some(feed_catalog_extra),
            then: None,
            then_options: same,
            feed_resumes_by_1_1: false,
            read_only_by: 0,
        },
        Entry {
            name: "feed_root_intent",
            shards: 2,
            options: same,
            write: create_feed_tables,
            crash: false,
            refused_by_1_0_0: 0,
            extra: Some(feed_extra),
            then: Some(write_feed_commits),
            then_options: feed_options,
            feed_resumes_by_1_1: true,
            read_only_by: 0,
        },
    ];
    // A replica (D315 PR 3): its state table, and `ro_compat` bit 0, which every published
    // release lacks, so each opens it read-only.
    #[cfg(feature = "__unstable-replication")]
    entries.push(Entry {
        name: "replica",
        shards: 2,
        options: same,
        write: write_replica,
        crash: false,
        refused_by_1_0_0: 0,
        extra: None,
        then: None,
        then_options: same,
        feed_resumes_by_1_1: false,
        read_only_by: pigeonhole_format::manifest::RO_COMPAT_REPLICA,
    });
    entries
}

/// A replica of a small primary (D315 PR 3): the primary's feed of every table applied with
/// the primary's ids, a catalog change and a run at a time, its token stored with each.
#[cfg(feature = "__unstable-replication")]
fn write_replica(db: &Pigeonhole) {
    let vfs = SimVfs::new(77);
    let primary = Pigeonhole::open(
        "/primary/data.phdb",
        sim_options(&vfs, 2).change_feed(pigeonhole::FeedRetention::bytes(4 << 20)),
    )
    .unwrap();
    let mut feed = primary.feed().open().unwrap();
    let t = primary
        .table("mirrored")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    for i in 0..20u64 {
        t.mutate(format!("r{i:02}").as_bytes())
            .put_at("f", b"q", T0 + i, format!("v{i}").as_bytes())
            .commit()
            .unwrap();
    }
    t.mutate(b"r03")
        .delete_cell("f", b"q", T0 + 3)
        .commit()
        .unwrap();
    let mut run = Vec::new();
    while feed
        .next_event(Duration::from_millis(200))
        .unwrap()
        .is_some()
    {
        let ev = feed.replicated().unwrap();
        if ev.is_catalog() {
            db.apply_catalog(&ev, ev.token()).unwrap();
        } else {
            run.push(ev);
            if run.len() == 4 {
                db.apply_replicated(&run, run[3].token()).unwrap();
                run.clear();
            }
        }
    }
    if let Some(last) = run.last() {
        db.apply_replicated(&run, last.token()).unwrap();
    }
    drop(feed);
    primary.close().unwrap();
}

/// Min, max and HLL families (D294, the first `incompat` feature bit), and the bitwise, `f64`,
/// exact-sum, mean-variance, histogram, quantile and bloom families (D298, the same bit; the
/// last three with `kind_params`), with bytes elements into HLL and bloom families: inputs at the fixed timestamp and in buckets, puts,
/// deletes and checkpointed reads, sparse and dense sketches, signed zeros and infinities;
/// flushed, then flushed and compacted, so compaction-folded bases are on disk, then more in
/// the WAL that the crash leaves for replay.
fn write_aggregates(db: &Pigeonhole) {
    let t = db
        .table("aggregates")
        .unwrap()
        .family("lo", Family::aggregate(Aggregation::Min))
        .family("hi", Family::aggregate(Aggregation::Max).max_versions(3))
        .family("users", Family::aggregate(Aggregation::Hll))
        .family("seen", Family::aggregate(Aggregation::BitOr))
        .family("common", Family::aggregate(Aggregation::BitAnd))
        .family("parity", Family::aggregate(Aggregation::BitXor))
        .family("coldest", Family::aggregate(Aggregation::MinF64))
        .family("warmest", Family::aggregate(Aggregation::MaxF64))
        .family("total", Family::aggregate(Aggregation::SumF64))
        .family("spread", Family::aggregate(Aggregation::MeanVar))
        .family(
            "latency_h",
            Family::aggregate(Aggregation::Histogram(
                pigeonhole::HistogramParams::new([-1.0, 0.0, 10.0, 100.0]).unwrap(),
            )),
        )
        .family(
            "seen_b",
            Family::aggregate(Aggregation::Bloom(
                pigeonhole::BloomParams::new(512, 3).unwrap(),
            )),
        )
        .family("members", Family::aggregate(Aggregation::Set))
        .family(
            "latency_q",
            Family::aggregate(Aggregation::Quantiles(
                pigeonhole::QuantileParams::default()
                    .mantissa_bits(4)
                    .max_buckets(16),
            )),
        )
        .create()
        .unwrap();
    for i in 0..40u64 {
        let row = format!("a{:02}", i % 6);
        let v = (i as i64 * 37) % 101 - 50;
        let mut m = t
            .mutate(row.as_bytes())
            .aggregate("lo", b"all", v)
            .aggregate("hi", b"all", v)
            .aggregate_at("lo", b"hourly", T0 + (i % 4) * 3_600_000_000, v)
            .aggregate_at("hi", b"hourly", T0 + (i % 4) * 3_600_000_000, v)
            .aggregate_at("users", b"hourly", T0 + (i % 4) * 3_600_000_000, v);
        // Bits from a mixing constant; temperatures with signed zeros and infinities.
        let bits = 0x9e37_79b9_7f4a_7c15u64.rotate_left(i as u32 * 5) | 1 << (i % 64);
        let x = match i % 9 {
            0 => -0.0,
            1 => 0.0,
            2 => f64::INFINITY,
            3 => f64::NEG_INFINITY,
            _ => v as f64 / 4.0,
        };
        m = m
            .aggregate_u64("seen", b"all", bits)
            .aggregate_u64("common", b"all", bits)
            .aggregate_u64("parity", b"all", bits)
            .aggregate_u64_at("parity", b"hourly", T0 + (i % 4) * 3_600_000_000, bits)
            .aggregate_f64("coldest", b"all", x)
            .aggregate_f64("warmest", b"all", x)
            .aggregate_f64_at("warmest", b"hourly", T0 + (i % 4) * 3_600_000_000, x);
        // Exact sums and mean/variance of finite values, with magnitudes that cancel.
        let y = match i % 5 {
            0 => 1e300,
            1 => -1e300,
            2 => 5e-324,
            _ => v as f64 / 8.0,
        };
        m = m
            .aggregate_f64("total", b"all", y)
            .aggregate_f64_at("total", b"hourly", T0 + (i % 4) * 3_600_000_000, y)
            .aggregate_f64("spread", b"all", y)
            .aggregate_f64("latency_h", b"all", x)
            .aggregate_f64_at("latency_h", b"hourly", T0 + (i % 4) * 3_600_000_000, y)
            .aggregate_f64("latency_q", b"all", y)
            .aggregate("seen_b", b"all", v)
            .aggregate_bytes("seen_b", b"names", format!("user-{}", i % 7).as_bytes())
            .aggregate_bytes("users", b"names", format!("user-{}", i % 7).as_bytes())
            // A set across two containers: adds, and removes of earlier adds (D298).
            .set_add("members", b"all", (i % 13) as u32 * 6000);
        if i % 4 == 0 {
            m = m.set_remove("members", b"all", ((i + 5) % 13) as u32 * 6000);
        }
        // Row a00's distinct users pass the sparse/dense boundary; the others stay sparse.
        let users = if i % 6 == 0 { 400 } else { 3 };
        for u in 0..users {
            m = m.aggregate("users", b"all", (i * 1_000 + u) as i64);
        }
        m.commit().unwrap();
        match i {
            10 => {
                t.mutate(b"a01")
                    .put_i64("lo", b"all", -1_000)
                    .put_i64("hi", b"all", 1_000)
                    .put_i64("common", b"all", -1)
                    .put_f64("coldest", b"all", 1.0e6)
                    .put_f64("total", b"all", 0.5)
                    .commit()
                    .unwrap();
            }
            20 => {
                t.mutate(b"a02")
                    .delete_column("lo", b"all")
                    .delete_column("hi", b"all")
                    .delete_column("parity", b"all")
                    .delete_column("warmest", b"all")
                    .delete_column("spread", b"all")
                    .commit()
                    .unwrap();
                // A read that folds what the flush below writes as a base.
                let _ = t.get(b"a03", "lo", b"all").unwrap();
                db.flush().unwrap();
            }
            // A compaction folds every kind's inputs into its stored bases (D298): a settled
            // set, merged HLL sketches, and the mean/variance, histogram and quantile states.
            // The commits after it stay on top, in the WAL the crash leaves (perf2's S1 on
            // #1466).
            30 => {
                db.flush().unwrap();
                db.compact().unwrap();
            }
            _ => {}
        }
    }
}

/// Every value type, every delete kind, several versions, three codecs; flushed and
/// compacted, then a row on top that the close flushes.
fn write_values(db: &Pigeonhole) {
    let t = db
        .table("values")
        .unwrap()
        .family("lz4", Family::default().max_versions(0))
        .family(
            "zstd",
            Family::default()
                .compression(pigeonhole::Compression::Zstd(3))
                .max_versions(0),
        )
        .family(
            "raw",
            Family::default()
                .compression(pigeonhole::Compression::None)
                .max_versions(3),
        )
        .create()
        .unwrap();
    for i in 0..40u64 {
        let row = format!("row{i:03}");
        t.mutate(row.as_bytes())
            .put_at("lz4", b"b", T0 + i, format!("bytes {i}").as_bytes())
            .put_i64_at("zstd", b"i", T0 + i, -(i as i64) * 1_000_003)
            .put_f64_at("raw", b"f", T0 + i, i as f64 / 7.0)
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    for i in 0..40u64 {
        let row = format!("row{i:03}");
        let mut m = t
            .mutate(row.as_bytes())
            .put_at("lz4", b"b", T0 + 100 + i, b"second version");
        m = match i % 4 {
            0 => m.delete_cell("zstd", b"i", T0 + i),
            1 => m.delete_column("raw", b"f"),
            2 => m.delete_family("zstd"),
            _ => m,
        };
        m.commit().unwrap();
    }
    t.mutate(b"row039").delete_row().commit().unwrap();
    db.compact().unwrap();
    t.mutate(b"row100")
        .put_at("lz4", b"after", T0 + 500, b"after compaction")
        .put_f64_at("raw", b"nan", T0 + 500, f64::NAN)
        .commit()
        .unwrap();
}

fn write_counters(db: &Pigeonhole) {
    let t = db
        .table("counters")
        .unwrap()
        .family("c", Family::counter())
        .create()
        .unwrap();
    for i in 0..30u64 {
        let row = format!("k{:02}", i % 7);
        t.mutate(row.as_bytes())
            .incr_at("c", b"hits", T0 + i, i as i64 + 1)
            .commit()
            .unwrap();
        if i == 15 {
            db.flush().unwrap();
        }
    }
}

fn write_blobs(db: &Pigeonhole) {
    let t = db
        .table("blobs")
        .unwrap()
        .family("v", Family::default().blob_threshold(256))
        .create()
        .unwrap();
    for i in 0..24u64 {
        let value: Vec<u8> = (0..(300 + i * 97)).map(|b| (b * 31 + i) as u8).collect();
        t.mutate(format!("blob{i:02}").as_bytes())
            .put_at("v", b"x", T0 + i, &value)
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    // Overwrite half, so some blob bytes are dead, and compact.
    for i in (0..24u64).step_by(2) {
        t.mutate(format!("blob{i:02}").as_bytes())
            .put_at("v", b"x", T0 + 100 + i, b"small now")
            .commit()
            .unwrap();
    }
    db.compact().unwrap();
}

fn write_merge(db: &Pigeonhole) {
    let t = db
        .table("log")
        .unwrap()
        .family("l", Family::default().merge_operator("corpus.concat"))
        .create()
        .unwrap();
    for i in 0..12u64 {
        let row = format!("log{}", i % 3);
        t.mutate(row.as_bytes())
            .merge("l", b"q", format!("[{i}]").as_bytes())
            .commit()
            .unwrap();
        if i == 6 {
            db.flush().unwrap();
        }
    }
}

/// Two shards; flushed data, then synced commits (cross-shard batches among them) that a
/// process crash leaves only in the WAL.
fn write_wal_crash(db: &Pigeonhole) {
    let t = db
        .table("events")
        .unwrap()
        .family("e", Family::default())
        .create()
        .unwrap();
    for i in 0..20u64 {
        t.mutate(format!("ev{i:03}").as_bytes())
            .put_at("e", b"q", T0 + i, b"flushed")
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    for i in 20..60u64 {
        let mut batch = db.write_batch();
        for j in 0..3u64 {
            let row = format!("ev{:03}", (i * 7 + j * 13) % 100);
            batch.put_at(&t, row.as_bytes(), "e", b"q", T0 + i, b"logged");
        }
        batch.commit_with(Durability::Sync).unwrap();
    }
}

/// Every family setting the manifest records (TTL, version and column limits, bloom bits,
/// block size, cache priority, compaction styles), flushed and compacted, beside a table
/// created, written, flushed and dropped.
fn write_families(db: &Pigeonhole) {
    // A TTL long enough that no cell written at T0 expires while the corpus is in use.
    let century = std::time::Duration::from_secs(100 * 365 * 24 * 3600);
    let t = db
        .table("settings")
        .unwrap()
        .family("ttl", Family::default().ttl(century))
        .family("versions", Family::default().max_versions(2))
        .family("bloom", Family::default().bloom_bits(16))
        .family("blocks", Family::default().block_size(1024))
        .family(
            "hot",
            Family::default().cache_priority(pigeonhole::Priority::High),
        )
        .family(
            "tiered",
            Family::default().compaction(pigeonhole::Compaction::Tiered),
        )
        .family(
            "fifo",
            Family::default().compaction(pigeonhole::Compaction::FifoByTime),
        )
        .create()
        .unwrap();
    let gone = db
        .table("dropped")
        .unwrap()
        .family("d", Family::default())
        .create()
        .unwrap();
    for i in 0..30u64 {
        gone.mutate(format!("gone{i:02}").as_bytes())
            .put_at("d", b"q", T0 + i, b"dropped with its table")
            .commit()
            .unwrap();
    }
    for round in 0..3u64 {
        for i in 0..30u64 {
            let ts = T0 + round * 100 + i;
            let v = format!("r{round} v{i}");
            t.mutate(format!("set{i:02}").as_bytes())
                .put_at("ttl", b"q", ts, v.as_bytes())
                .put_at("versions", b"q", ts, v.as_bytes())
                .put_at("bloom", b"q", ts, v.as_bytes())
                .put_at("blocks", b"q", ts, &[round as u8; 200])
                .put_at("hot", b"q", ts, v.as_bytes())
                .put_at("tiered", b"q", ts, v.as_bytes())
                .put_at("fifo", b"q", ts, v.as_bytes())
                .commit()
                .unwrap();
        }
        db.flush().unwrap();
    }
    drop(gone);
    db.drop_table("dropped").unwrap();
    db.compact().unwrap();
}

/// Two shards, a flushed base, then one synced batch per operation kind (every value type,
/// every delete kind, counter increments, merge operands, a separated value, rows on both
/// shards), which a process crash leaves only in the WAL.
fn write_wal_ops(db: &Pigeonhole) {
    let t = db
        .table("ops")
        .unwrap()
        .family("v", Family::default().max_versions(0))
        .family("c", Family::counter())
        .family("l", Family::default().merge_operator("corpus.concat"))
        .family("b", Family::default().blob_threshold(256))
        .create()
        .unwrap();
    for i in 0..16u64 {
        let row = format!("op{i:02}");
        t.mutate(row.as_bytes())
            .put_at("v", b"base", T0 + i, b"flushed")
            .put_at("v", b"gone", T0 + i, b"deleted in the WAL")
            .incr_at("c", b"n", T0, 1)
            .merge("l", b"m", b"<flushed>")
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    let sync = Durability::Sync;
    for i in 0..16u64 {
        let row = format!("op{i:02}");
        let row = row.as_bytes();
        let ts = T0 + 100 + i;
        let mut b = db.write_batch();
        match i % 8 {
            0 => b.put_at(&t, row, "v", b"bytes", ts, b"logged bytes"),
            1 => b.put_i64_at(&t, row, "v", b"i64", ts, -(i as i64) * 7_000_001),
            2 => b.put_f64_at(&t, row, "v", b"f64", ts, i as f64 / 3.0),
            3 => b.delete_cell(&t, row, "v", b"gone", T0 + i),
            4 => b.delete_column(&t, row, "v", b"gone"),
            5 => b.delete_family(&t, row, "v"),
            6 => b.incr_at(&t, row, "c", b"n", T0, 41),
            _ => b.merge(&t, row, "l", b"m", b"<logged>"),
        };
        b.commit_with(sync).unwrap();
    }
    let big: Vec<u8> = (0..4000u32).map(|b| (b * 13) as u8).collect();
    let mut b = db.write_batch();
    b.put_at(&t, b"op00", "b", b"blob", T0 + 200, &big);
    b.commit_with(sync).unwrap();
    // Rows on both shards in one batch, then a row deleted whole.
    let mut b = db.write_batch();
    for i in 0..8u64 {
        b.put_at(
            &t,
            format!("x{i}").as_bytes(),
            "v",
            b"x",
            T0 + 300,
            b"cross",
        );
    }
    b.commit_with(sync).unwrap();
    let mut b = db.write_batch();
    b.delete_row(&t, b"op15");
    b.commit_with(sync).unwrap();
}

/// A counter read that folds many increments keeps its result as the counter's base (D291,
/// at the default step count): two rounds of increments, each read and flushed, so level 0
/// holds the bases (a `Put` in a counter family) beside increments flushed after them.
fn write_checkpoints(db: &Pigeonhole) {
    let t = db
        .table("tallies")
        .unwrap()
        .family("c", Family::counter())
        .create()
        .unwrap();
    for round in 0..2u64 {
        for i in 0..300u64 {
            t.mutate(format!("hot{}", i % 2).as_bytes())
                .incr_at("c", b"n", T0 + round, 1)
                .commit()
                .unwrap();
        }
        for row in [&b"hot0"[..], b"hot1"] {
            assert!(t.get(row, "c", b"n").unwrap().is_some());
        }
        db.flush().unwrap();
    }
    for i in 0..10u64 {
        t.mutate(b"hot0")
            .incr_at("c", b"n", T0 + 1, i as i64)
            .commit()
            .unwrap();
    }
}

/// Vector families of every element (D303, value tag 0x04 under `incompat` bit 0), flushed
/// and compacted with a version and a delete on top, and a `pigeonhole-vector` IVF index
/// (D306) over an `f32` family: enough vectors to split its lists, then deletes.
fn write_vectors(db: &Pigeonhole) {
    let spec =
        |dim, element, metric| Family::vector(VectorSpec::new(dim, element, metric).unwrap());
    let t = db
        .table("vectors")
        .unwrap()
        .family("f32", spec(4, Element::F32, Metric::L2).max_versions(2))
        .family("f16", spec(4, Element::F16, Metric::Cosine))
        .family("bf16", spec(4, Element::BF16, Metric::InnerProduct))
        .family("i8", spec(4, Element::I8, Metric::L2))
        .family("bits", spec(16, Element::Bit, Metric::Hamming))
        .create()
        .unwrap();
    for i in 0..12u16 {
        let x = f32::from(i) - 5.5;
        t.mutate(format!("v{i:02}").as_bytes())
            .put_vector_f32("f32", b"e", &[x, -x, x / 4.0, 1.0])
            .put_vector_f16("f16", b"e", &[0x3c00 + i, 0xbc00, 0x3800, i | 0x0400])
            .put_vector_bf16("bf16", b"e", &[0x3f80 + i, 0xbf80, 0x4000, 0x3f00])
            .put_vector_i8("i8", b"e", &[i as i8, -(i as i8), 127, -128])
            .put_vector_bits("bits", b"e", &[i as u8, !(i as u8)])
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    t.mutate(b"v03")
        .put_vector_f32("f32", b"e", &[0.5, 0.25, -0.125, 2.0])
        .delete_column("bits", b"e")
        .commit()
        .unwrap();
    let ivf = pigeonhole_vector::Index::create(
        db,
        "ivf",
        VectorSpec::new(4, Element::F32, Metric::L2).unwrap(),
        pigeonhole_vector::IndexOptions::new().target_list_len(8),
    )
    .unwrap();
    for i in 0..60u32 {
        let x = i as f32;
        let v = [
            (x * 0.37).sin(),
            (x * 0.71).cos(),
            x / 60.0,
            (i % 7) as f32 - 3.0,
        ];
        ivf.insert_f32(format!("item{i:03}").as_bytes(), &v)
            .unwrap();
    }
    for i in [3u32, 17, 41] {
        assert!(ivf.delete(format!("item{i:03}").as_bytes()).unwrap());
    }
}

/// The IVF index's `verify`: its vector count and list lengths.
fn vectors_extra(db: &Pigeonhole) -> String {
    let stats = pigeonhole_vector::Index::open(db, "ivf")
        .unwrap()
        .verify()
        .unwrap();
    format!(
        "ivf vectors {}\nivf list_lens {:?}\n",
        stats.vectors, stats.list_lens
    )
}

/// A `pigeonhole-text` index with positions (D309) over a source column, written in the
/// documents' transactions, with a rewrite and a delete, flushed. Its statistics family is an
/// aggregate kind, so 1.0.0 refuses the file (`incompat` bit 0).
fn write_text(db: &Pigeonhole) {
    let docs = db
        .table("docs")
        .unwrap()
        .family("d", Family::default())
        .create()
        .unwrap();
    let index = pigeonhole_text::Index::create(
        db,
        "docs_text",
        pigeonhole_text::Source::new(&docs, "d", b"body"),
        pigeonhole_text::IndexOptions::new().positions(true),
    )
    .unwrap();
    let texts = [
        "the quick brown fox jumps over the lazy dog",
        "a lazy afternoon with a brown dog",
        "foxes and dogs are not the same animal",
        "Straße, café and naïve: normalised text",
        "the dog sleeps; the fox watches the dog",
        "nothing in common here",
    ];
    for (i, text) in texts.iter().enumerate() {
        let doc = format!("doc{i}");
        let mut txn = db.transaction().unwrap();
        txn.put(&docs, doc.as_bytes(), "d", b"body", text.as_bytes());
        index.put_in(&mut txn, doc.as_bytes(), text).unwrap();
        txn.commit().unwrap();
    }
    db.flush().unwrap();
    let mut txn = db.transaction().unwrap();
    txn.put(
        &docs,
        b"doc1",
        "d",
        b"body",
        b"a busy morning with a brown fox",
    );
    index
        .put_in(&mut txn, b"doc1", "a busy morning with a brown fox")
        .unwrap();
    assert!(index.delete_in(&mut txn, b"doc5").unwrap());
    txn.delete_column(&docs, b"doc5", "d", b"body");
    txn.commit().unwrap();
}

/// The text index's `verify`: its counts and whether every check agreed.
fn text_extra(db: &Pigeonhole) -> String {
    let stats = pigeonhole_text::Index::open(db, "docs_text")
        .unwrap()
        .verify()
        .unwrap();
    format!(
        "text documents {}\ntext terms {}\ntext postings {}\ntext consistent {}\n",
        stats.documents,
        stats.terms,
        stats.postings,
        stats.is_consistent()
    )
}

/// A `pigeonhole-memory` store (D311): memories in two namespaces with embeddings (a vector
/// family, `incompat` bit 0), its text index, and its IVF index (T8) once a namespace passes
/// `min_namespace_len`; a link, a supersede, uses and a delete; flushed.
fn write_memory(db: &Pigeonhole) {
    use pigeonhole_memory::{EmbeddingModel, IvfOptions, LinkType, Memory, Store, StoreOptions};
    let store = Store::create(
        db,
        "agents",
        StoreOptions::new()
            .embedding(EmbeddingModel::new("corpus-8", 8, Element::F32).unwrap())
            .ivf(IvfOptions::new().min_namespace_len(4)),
    )
    .unwrap();
    let mut ids = Vec::new();
    for i in 0..24u32 {
        let ns = if i % 3 == 0 { "agent:b" } else { "agent:a" };
        let embedding: Vec<f32> = (0..8u32)
            .map(|d| ((i * 7 + d * 3) % 11) as f32 - 4.5)
            .collect();
        let r = store
            .remember(
                ns,
                Memory::new(format!("note {i} about topic {}", i % 5))
                    .importance(f64::from(i % 10) / 10.0)
                    .embedding(embedding),
            )
            .unwrap();
        ids.push(r.id().clone());
    }
    store
        .link(&ids[1], &ids[2], LinkType::SIMILAR_TO, 0.5)
        .unwrap();
    store.supersede(&ids[4], &ids[7]).unwrap();
    store.mark_used(&ids[5..8]).unwrap();
    assert!(store.delete(&ids[10]).unwrap());
    db.flush().unwrap();
}

/// The memory store's `verify`: its count, and whether it, its text index and its IVF index
/// agree.
fn memory_extra(db: &Pigeonhole) -> String {
    let stats = pigeonhole_memory::Store::open(db, "agents", "corpus-8")
        .unwrap()
        .verify()
        .unwrap();
    format!(
        "memory memories {}\nmemory consistent {}\n",
        stats.memories,
        stats.is_consistent()
    )
}

/// The change feed on for every table (D310): its retention, recorded in the `-feed`
/// sidecar, so a later open that doesn't mention the feed inherits it. (`feed_tables` names
/// tables that exist at the open, and an entry's tables are made by its writer.)
fn feed_options(o: Options) -> Options {
    o.change_feed(FeedRetention::bytes(4 << 20))
}

/// Where the feed entries keep the token their check resumes from: a subscriber's token from
/// before the first commit, stored as an application stores one with its work, beside the
/// seqno of the last commit the feed delivers (`last`), where the check stops.
const FEED_TOKEN: (&str, &[u8]) = ("feed_tokens", b"events");

/// How long the check waits for each commit of the retained history: long, so a slow runner
/// never ends the listing early (feed1248's P1 on #1466). The history is all on disk, so a
/// healthy build delivers it at once.
const FEED_WAIT: Duration = Duration::from_secs(30);

/// Commits to two fed tables (puts, counter increments, deletes), with flushes between, so
/// the retained history runs past the checkpoint and over several WAL segments: the WAL keeps
/// it, ring-recycled, and the sidecar records the session. A subscriber to one table takes
/// its token before the first commit and stores it; the check resumes from that token, and
/// the other table's commits are passed over.
fn write_feed(db: &Pigeonhole) {
    let events = db
        .table("events")
        .unwrap()
        .family("e", Family::default())
        .family("n", Family::counter())
        .create()
        .unwrap();
    let other = db
        .table("other")
        .unwrap()
        .family("e", Family::default())
        .create()
        .unwrap();
    let tokens = db
        .table(FEED_TOKEN.0)
        .unwrap()
        .family("t", Family::default())
        .create()
        .unwrap();
    let token = events.feed().open().unwrap().token();
    tokens
        .mutate(FEED_TOKEN.1)
        .put("t", b"token", token.as_bytes())
        .commit()
        .unwrap();
    let mut last = 0;
    for i in 0..300u64 {
        let row = format!("r{:02}", i % 7);
        let mut m = events
            .mutate(row.as_bytes())
            .put_at(
                "e",
                b"what",
                T0 + i,
                format!("event {i:03} ").repeat(150).as_bytes(),
            )
            .incr_at("n", b"count", T0, 1);
        if i % 9 == 4 {
            m = m.delete_column("e", b"old");
        }
        last = m.commit().unwrap().seqno;
        if i % 5 == 0 {
            other
                .mutate(row.as_bytes())
                .put("e", b"x", b"not followed")
                .commit()
                .unwrap();
        }
        if i % 100 == 99 {
            db.flush().unwrap();
        }
    }
    tokens
        .mutate(FEED_TOKEN.1)
        .put_i64("t", b"last", last as i64)
        .commit()
        .unwrap();
}

/// `feed_root_intent`'s first session, with the feed off: the tables, and no catalog
/// change in any feeding session's sidecar, which 1.1.0 couldn't read past.
fn create_feed_tables(db: &Pigeonhole) {
    for (name, families) in [
        ("events", &["e"][..]),
        ("other", &["e"][..]),
        (FEED_TOKEN.0, &["t"][..]),
    ] {
        let mut b = db.table(name).unwrap();
        for f in families {
            b = b.family(f, Family::default());
        }
        b.create().unwrap();
    }
}

/// `feed_root_intent`'s second session, with the feed on (D315 amendment, #1504): a table
/// feed's token, stored, then commits to two tables with flushes between, so the session's
/// root commits record their intents and overwrite them with their Root entries, and a clean
/// close. Its sidecar then holds Session and Root entries only, as 1.1.0's do.
fn write_feed_commits(db: &Pigeonhole) {
    let events = db.table("events").unwrap().open().unwrap();
    let other = db.table("other").unwrap().open().unwrap();
    let tokens = db.table(FEED_TOKEN.0).unwrap().open().unwrap();
    let token = events.feed().open().unwrap().token();
    tokens
        .mutate(FEED_TOKEN.1)
        .put("t", b"token", token.as_bytes())
        .commit()
        .unwrap();
    let mut last = 0;
    for i in 0..60u64 {
        let row = format!("r{:02}", i % 7);
        last = events
            .mutate(row.as_bytes())
            .put_at(
                "e",
                b"what",
                T0 + i,
                format!("event {i:03} ").repeat(40).as_bytes(),
            )
            .commit()
            .unwrap()
            .seqno;
        if i % 4 == 0 {
            other
                .mutate(row.as_bytes())
                .put("e", b"x", b"not followed")
                .commit()
                .unwrap();
        }
        if i % 20 == 19 {
            db.flush().unwrap();
        }
    }
    tokens
        .mutate(FEED_TOKEN.1)
        .put_i64("t", b"last", last as i64)
        .commit()
        .unwrap();
}

/// Every commit after the stored token, as a resumed feed delivers them, up to the stored
/// last seqno: one line per change, with a value's bytes as their length and xxh3-64 (the
/// puts are long).
fn feed_extra(db: &Pigeonhole) -> String {
    // The open inherited the feed from the sidecar (feed1248's S1 on #1466).
    assert!(
        db.info().unwrap().feed.is_some(),
        "the open didn't inherit the change feed"
    );
    let tokens = db.table(FEED_TOKEN.0).unwrap().open().unwrap();
    let stored = tokens
        .get(FEED_TOKEN.1, "t", b"token")
        .unwrap()
        .expect("the stored token");
    let token = pigeonhole::FeedToken::from_bytes(stored.value());
    let last = tokens
        .get(FEED_TOKEN.1, "t", b"last")
        .unwrap()
        .expect("the stored last seqno")
        .as_i64()
        .unwrap() as u64;
    let events = db.table("events").unwrap().open().unwrap();
    let mut feed = events.feed().resume(&token).open().unwrap();
    let mut out = String::new();
    loop {
        let c = feed
            .next(FEED_WAIT)
            .unwrap()
            .unwrap_or_else(|| panic!("the feed stopped before seqno {last}"));
        for ch in c.changes() {
            out.push_str(&format!(
                "{}\t{}\t{}\t{:?}\t{}\t{}\t{}\t{}\n",
                c.seqno(),
                ch.table(),
                ch.family(),
                ch.kind(),
                hex(ch.row()),
                hex(ch.qualifier()),
                ch.timestamp(),
                ch.value().map_or_else(
                    || "-".to_owned(),
                    |v| match v {
                        Value::Bytes(b) => format!(
                            "bytes:{}:{:016x}",
                            b.len(),
                            pigeonhole_format::checksum::xxh3_64(b)
                        ),
                        other => value_line(other),
                    }
                )
            ));
        }
        if c.seqno() >= last {
            break;
        }
    }
    out
}

/// Where `feed_catalog` keeps a feed of every table's token, stored as `FEED_TOKEN`'s is.
const FEED_ALL_TOKEN: (&str, &[u8]) = ("feed_tokens", b"all");

/// D315: a feed of every table's token from before the first commit, then commits among
/// catalog changes: a table created with two families, a family added to it, and two tables
/// dropped, one from before the token, beside a table from before the token that survives. The check resumes a feed of every table from the
/// token and lists every event: the commits, and each catalog change with its names.
fn write_feed_catalog(db: &Pigeonhole) {
    let tokens = db
        .table(FEED_ALL_TOKEN.0)
        .unwrap()
        .family("t", Family::default())
        .create()
        .unwrap();
    let events = db
        .table("events")
        .unwrap()
        .family("e", Family::default())
        .create()
        .unwrap();
    let kept = db
        .table("kept")
        .unwrap()
        .family("k", Family::default())
        .create()
        .unwrap();
    let token = db.feed().open().unwrap().token();
    tokens
        .mutate(FEED_ALL_TOKEN.1)
        .put("t", b"token", token.as_bytes())
        .commit()
        .unwrap();
    let mut late = None;
    for i in 0..120u64 {
        match i {
            30 => {
                late = Some(
                    db.table("late")
                        .unwrap()
                        .family("a", Family::default())
                        .family("n", Family::counter())
                        .create()
                        .unwrap(),
                )
            }
            60 => {
                late = Some(
                    db.table("late")
                        .unwrap()
                        .family("b", Family::default().max_versions(2))
                        .open()
                        .unwrap(),
                )
            }
            90 => db.drop_table("events").unwrap(),
            110 => {
                late = None;
                db.drop_table("late").unwrap();
            }
            _ => {}
        }
        let row = format!("r{:02}", i % 7);
        let value = format!("event {i:03} ").repeat(40);
        if i < 90 {
            events
                .mutate(row.as_bytes())
                .put_at("e", b"what", T0 + i, value.as_bytes())
                .commit()
                .unwrap();
        }
        if i % 4 == 1 {
            kept.mutate(row.as_bytes())
                .put_at("k", b"kept", T0 + i, value.as_bytes())
                .commit()
                .unwrap();
        }
        if let Some(late) = &late
            && i % 3 == 0
        {
            let family = if i >= 60 { "b" } else { "a" };
            late.mutate(row.as_bytes())
                .put_at(family, b"late", T0 + i, value.as_bytes())
                .commit()
                .unwrap();
        }
        if i % 50 == 49 {
            db.flush().unwrap();
        }
    }
    drop(events);
    // A last commit after the final drop, so the check lists it: the token table's, which
    // the listing leaves out.
    let last = tokens
        .mutate(FEED_ALL_TOKEN.1)
        .put("t", b"end", b"")
        .commit()
        .unwrap()
        .seqno;
    tokens
        .mutate(FEED_ALL_TOKEN.1)
        .put_i64("t", b"last", last as i64)
        .commit()
        .unwrap();
}

/// Every event after the stored all-tables token, up to the stored last seqno: each commit's
/// changes, as `feed_extra` lists them, and each catalog change's edits with their names and
/// a family's kind and version limit. The token table's own commit is left out (the token's
/// bytes are this database's). A change to a table dropped by the time of the check has its
/// table and family listed as `(dropped)`: whether a feed names those commits isn't frozen
/// (ai1249 on #1515), so a later build that names them still passes. The catalog lines name
/// every table.
fn feed_catalog_extra(db: &Pigeonhole) -> String {
    use pigeonhole::{CatalogEdit, FeedEvent};
    let tokens = db.table(FEED_ALL_TOKEN.0).unwrap().open().unwrap();
    let stored = tokens
        .get(FEED_ALL_TOKEN.1, "t", b"token")
        .unwrap()
        .expect("the stored token");
    let token = pigeonhole::FeedToken::from_bytes(stored.value());
    let last = tokens
        .get(FEED_ALL_TOKEN.1, "t", b"last")
        .unwrap()
        .expect("the stored last seqno")
        .as_i64()
        .unwrap() as u64;
    let family = |f: &pigeonhole::FamilyInfo| {
        format!(
            "{}(counter={},max_versions={})",
            f.name, f.counter, f.max_versions
        )
    };
    let live = db.tables();
    let mut feed = db.feed().resume(&token).open().unwrap();
    let mut out = String::new();
    loop {
        let event = feed
            .next_event(FEED_WAIT)
            .unwrap()
            .unwrap_or_else(|| panic!("the feed stopped before seqno {last}"));
        let c = match event {
            FeedEvent::Catalog(change) => {
                for e in change.edits() {
                    let edit = match e {
                        CatalogEdit::TableCreated {
                            table, families, ..
                        } => {
                            let f: Vec<String> = families.iter().map(family).collect();
                            format!("created\t{table}\t{}", f.join(","))
                        }
                        CatalogEdit::FamilyAdded {
                            table, family: f, ..
                        } => {
                            format!("family\t{table}\t{}", family(f))
                        }
                        CatalogEdit::TableDropped { table, .. } => format!("dropped\t{table}"),
                        other => panic!("an edit this check doesn't know: {other:?}"),
                    };
                    out.push_str(&format!("{}\tcatalog\t{edit}\n", change.seqno()));
                }
                continue;
            }
            FeedEvent::Commit(c) => c,
            other => panic!("an event this check doesn't know: {other:?}"),
        };
        for ch in c.changes() {
            if ch.table() == FEED_ALL_TOKEN.0 {
                continue;
            }
            let (table, family) = if live.iter().any(|t| t == ch.table()) {
                (ch.table(), ch.family())
            } else {
                ("(dropped)", "(dropped)")
            };
            let value = match ch.value() {
                Some(Value::Bytes(b)) => format!(
                    "bytes:{}:{:016x}",
                    b.len(),
                    pigeonhole_format::checksum::xxh3_64(b)
                ),
                Some(other) => value_line(other),
                None => "-".to_owned(),
            };
            out.push_str(&format!(
                "{}\t{}\t{}\t{:?}\t{}\t{}\t{}\t{value}\n",
                c.seqno(),
                table,
                family,
                ch.kind(),
                hex(ch.row()),
                hex(ch.qualifier()),
                ch.timestamp(),
            ));
        }
        if c.seqno() >= last {
            break;
        }
    }
    out
}

/// Where `feed_sessions` keeps what its check needs: the seqno of the first commit to
/// `events` (where a feed from the oldest history must start) and of the last.
const FEED_SPAN: (&str, &[u8]) = ("feed_tokens", b"span");

/// One session of `feed_sessions`: commits to `events` and `other` over WAL segments, then a
/// clean close. The first session makes the tables and records the first commit's seqno; each
/// session records the last. Written twice, so the files keep two closed sessions of history.
///
/// Both sessions must fit the feed's retention, or the oldest history is rightly gone: 4 MiB
/// over 2 streams is 8 segments of 256 KiB per stream, and the two sessions use about 4–5
/// (each open starts a fresh segment). A later change to the WAL's framing or to retention
/// that tips this over shows up as this entry failing: check the sizing before suspecting
/// the feed (feed1248 on #1476).
fn write_feed_session(db: &Pigeonhole) {
    let fresh = !db.tables().iter().any(|t| t == "events");
    if fresh {
        db.table("events")
            .unwrap()
            .family("e", Family::default())
            .family("n", Family::counter())
            .create()
            .unwrap();
        db.table("other")
            .unwrap()
            .family("e", Family::default())
            .create()
            .unwrap();
        db.table(FEED_SPAN.0)
            .unwrap()
            .family("t", Family::default())
            .create()
            .unwrap();
    }
    let events = db.table("events").unwrap().open().unwrap();
    let other = db.table("other").unwrap().open().unwrap();
    let span = db.table(FEED_SPAN.0).unwrap().open().unwrap();
    let base = if fresh { 0 } else { 150 };
    for i in base..base + 150u64 {
        let row = format!("r{:02}", i % 7);
        let seqno = events
            .mutate(row.as_bytes())
            .put_at(
                "e",
                b"what",
                T0 + i,
                format!("session event {i:03} ").repeat(120).as_bytes(),
            )
            .incr_at("n", b"count", T0, 1)
            .commit()
            .unwrap()
            .seqno;
        if i == 0 {
            span.mutate(FEED_SPAN.1)
                .put_i64("t", b"first", seqno as i64)
                .commit()
                .unwrap();
        }
        if i == base + 149 {
            span.mutate(FEED_SPAN.1)
                .put_i64("t", b"last", seqno as i64)
                .commit()
                .unwrap();
        }
        if i % 4 == 0 {
            other
                .mutate(row.as_bytes())
                .put("e", b"x", b"not followed")
                .commit()
                .unwrap();
        }
        if i % 50 == 49 {
            db.flush().unwrap();
        }
    }
}

/// #1465 (D310): a feed from the oldest history, in an open after two closed sessions, starts
/// at the first session's first commit and delivers both sessions' commits up to the last.
/// The listing asserts the start itself, so a build that loses the earlier sessions can't
/// write (or pass) a short listing.
fn feed_oldest_extra(db: &Pigeonhole) -> String {
    assert!(
        db.info().unwrap().feed.is_some(),
        "the open didn't inherit the change feed"
    );
    let span = db.table(FEED_SPAN.0).unwrap().open().unwrap();
    let seqno = |q: &[u8]| {
        span.get(FEED_SPAN.1, "t", q)
            .unwrap()
            .expect("the stored span")
            .as_i64()
            .unwrap() as u64
    };
    let (first, last) = (seqno(b"first"), seqno(b"last"));
    let events = db.table("events").unwrap().open().unwrap();
    let mut feed = events.feed().from_oldest().open().unwrap();
    let mut out = String::new();
    loop {
        let c = feed
            .next(FEED_WAIT)
            .unwrap()
            .unwrap_or_else(|| panic!("the feed from the oldest history stopped before {last}"));
        if out.is_empty() {
            assert_eq!(
                c.seqno(),
                first,
                "a feed from the oldest history must start at the first session's first commit"
            );
        }
        for ch in c.changes() {
            out.push_str(&format!(
                "{}\t{}\t{:?}\t{}\t{}\t{}\t{}\n",
                c.seqno(),
                ch.family(),
                ch.kind(),
                hex(ch.row()),
                hex(ch.qualifier()),
                ch.timestamp(),
                ch.value().map_or_else(
                    || "-".to_owned(),
                    |v| match v {
                        Value::Bytes(b) => format!(
                            "bytes:{}:{:016x}",
                            b.len(),
                            pigeonhole_format::checksum::xxh3_64(b)
                        ),
                        other => value_line(other),
                    }
                )
            ));
        }
        if c.seqno() >= last {
            break;
        }
    }
    out
}

/// D314: `commit_returning` increments on a hot counter whose base is an older SST's `Put`.
/// The first returning commit reads through to that base and leaves a D291 checkpoint at its
/// own seqno; later ones checkpoint every eight folded operands. The flush's combining may
/// drop the older checkpoints, so what's sure to be on disk is the newest write-time
/// checkpoint as the counter's base, at its commit's seqno, with the later operands above it
/// (perf2 on #1466 and #1472). Each returned value is the count so far.
fn write_returning(db: &Pigeonhole) {
    let t = db
        .table("returning")
        .unwrap()
        .family("c", Family::counter())
        .create()
        .unwrap();
    t.mutate(b"hot").put_i64("c", b"n", 100).commit().unwrap();
    db.flush().unwrap();
    for want in 101..=120i64 {
        let r = t
            .mutate(b"hot")
            .incr("c", b"n", 1)
            .commit_returning()
            .unwrap();
        assert_eq!(r.value("c", b"n").unwrap(), Some(want));
    }
    db.flush().unwrap();
    assert_eq!(
        t.get(b"hot", "c", b"n").unwrap().and_then(|c| c.as_i64()),
        Some(120)
    );
}

/// The database's files in the simulator's `/db`, by name: the main file, its WAL files and
/// the change feed's sidecar (no shared memory).
fn sim_files(vfs: &SimVfs) -> BTreeMap<String, Vec<u8>> {
    vfs.list_dir(Path::new("/db"))
        .unwrap()
        .into_iter()
        .filter_map(|p| {
            let name = p.file_name()?.to_str()?.to_owned();
            (name == DB || name == "data.phdb-feed" || name.starts_with("data.phdb-wal-"))
                .then_some((p, name))
        })
        .map(|(p, name)| {
            let f = vfs.open(&p, OpenOptions::read()).unwrap();
            let mut bytes = vec![0u8; f.len().unwrap() as usize];
            f.read_at(&mut bytes, 0).unwrap();
            (name, bytes)
        })
        .collect()
}

/// One line of a release corpus's `frozen.txt`: path, length and xxh3-64.
fn frozen_line(path: &str, bytes: &[u8]) -> String {
    format!(
        "{path} {} {:016x}",
        bytes.len(),
        pigeonhole_format::checksum::xxh3_64(bytes)
    )
}

#[test]
#[ignore = "rewrites tests/corpus/format4, or writes the directory the environment names"]
fn generate_corpus() {
    let release = std::env::var("PIGEONHOLE_CORPUS_RELEASE").ok();
    let root = match (&release, std::env::var_os("PIGEONHOLE_CORPUS_OUT")) {
        (Some(version), _) => {
            assert_eq!(
                version,
                env!("CARGO_PKG_VERSION"),
                "a release corpus is written by that release's own build"
            );
            let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/corpus")
                .join(format!("v{version}"));
            assert!(
                !dir.join(FROZEN).exists(),
                "v{version} is frozen: never rewrite a release corpus"
            );
            dir
        }
        (None, Some(out)) => PathBuf::from(out),
        (None, None) => corpus_dir(),
    };
    let mut frozen = Vec::new();
    for (seed, e) in entries().into_iter().enumerate() {
        let path = format!("/db/{DB}");
        let vfs = write_entry(&e, 9_000 + seed as u64, &path, Duration::ZERO);
        let files = sim_files(&vfs);
        // What the files hold, as this build reads them (a reopen replays a crashed entry).
        let db = Pigeonhole::open(&path, sim_options(&vfs, e.shards)).unwrap();
        let mut expected = contents(&db).join("\n");
        expected.push('\n');
        let extra = e.extra.map(|f| f(&db));
        db.close().unwrap();

        let dir = root.join(e.name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for (name, bytes) in &files {
            std::fs::write(dir.join(name), bytes).unwrap();
        }
        std::fs::write(dir.join("expected.txt"), &expected).unwrap();
        if e.feed_resumes_by_1_1 {
            let marker =
                "compat-reader-1.1 resumes this entry's stored table feed token (D315 amendment)\n";
            std::fs::write(dir.join(FEED_RESUMES), marker).unwrap();
            // Frozen like the other markers: a release corpus lists every file it holds.
            frozen.push(frozen_line(
                &format!("{}/{FEED_RESUMES}", e.name),
                marker.as_bytes(),
            ));
        }
        if let Some(extra) = &extra {
            std::fs::write(dir.join(EXTRA), extra).unwrap();
            frozen.push(frozen_line(
                &format!("{}/{EXTRA}", e.name),
                extra.as_bytes(),
            ));
        }
        for (name, bytes) in &files {
            frozen.push(frozen_line(&format!("{}/{name}", e.name), bytes));
        }
        if e.read_only_by != 0 {
            let marker = format!("{}\n", e.read_only_by);
            std::fs::write(dir.join(READ_ONLY_BY), &marker).unwrap();
            frozen.push(frozen_line(
                &format!("{}/{READ_ONLY_BY}", e.name),
                marker.as_bytes(),
            ));
        }
        if e.refused_by_1_0_0 != 0 {
            let marker = format!("{}\n", e.refused_by_1_0_0);
            std::fs::write(dir.join(REFUSED_BY), &marker).unwrap();
            frozen.push(frozen_line(
                &format!("{}/{REFUSED_BY}", e.name),
                marker.as_bytes(),
            ));
        }
        frozen.push(frozen_line(
            &format!("{}/expected.txt", e.name),
            expected.as_bytes(),
        ));
        eprintln!(
            "{}: {}",
            e.name,
            files
                .iter()
                .map(|(n, b)| format!("{n} {} KiB", b.len() >> 10))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if let Some(version) = release {
        let mut text = format!(
            "# The v{version} corpus, written by the {version} build: path, length, xxh3-64.\n\
             # Frozen: no file or line ever changes (FORMAT.md §13).\n"
        );
        frozen.sort();
        for line in frozen {
            text.push_str(&line);
            text.push('\n');
        }
        std::fs::write(root.join(FROZEN), text).unwrap();
    }
}

/// A temporary directory removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("pigeonhole-corpus-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn real_options() -> Options {
    Options::default()
        .shards(2)
        .pin_threads(false)
        .memtable_budget(8 << 20)
        .block_cache(8 << 20)
        .create_if_missing(false)
        .merge_operator(Arc::new(Concat))
}

/// Opens a copy of the entry in `src`, reads every cell exactly as `expected.txt` says (and
/// its extra listing as `expected-extra.txt` does), writes through a flush and a compaction,
/// passes the offline check, and reads everything again after a reopen.
fn check_entry(label: &str, src: &Path, extra: Option<fn(&Pigeonhole) -> String>) {
    assert!(
        src.join("expected.txt").exists(),
        "corpus entry {label} is missing: run generate_corpus"
    );
    // A copy: opening writes (WAL replay, the format upgrade of an older file).
    let tmp = TempDir::new(&label.replace('/', "-"));
    for f in std::fs::read_dir(src).unwrap() {
        let f = f.unwrap();
        std::fs::copy(f.path(), tmp.0.join(f.file_name())).unwrap();
    }
    let path = tmp.0.join(DB);
    let expected: Vec<String> = std::fs::read_to_string(src.join("expected.txt"))
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect();

    // Read: every cell, as the generating build read it.
    let db = Pigeonhole::open(&path, real_options())
        .unwrap_or_else(|err| panic!("{label}: open: {err}"));
    assert_eq!(contents(&db), expected, "{label}: contents");
    // An older release's entry may predate its listing; `format4/` always has it (checked
    // by `corpus_entries_open_read_check_and_write`).
    match (extra, std::fs::read_to_string(src.join(EXTRA))) {
        (Some(f), Ok(want)) => assert_eq!(f(&db), want, "{label}: {EXTRA}"),
        (None, Ok(_)) => panic!("{label}: {EXTRA} without a check"),
        (_, Err(_)) => {}
    }
    // Write on top, through a flush and a compaction. A replica (D315 PR 3; the entry with
    // [`READ_ONLY_BY`]) takes only its primary's changes: a local table is refused, and the
    // flush, compaction, check and reread still run.
    let replica = src.join(READ_ONLY_BY).exists();
    match db
        .table("corpus_writes")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
    {
        Ok(t) => {
            assert!(!replica, "{label}: a replica took a local table");
            t.mutate(b"written")
                .put("f", b"q", b"by this build")
                .commit()
                .unwrap();
        }
        Err(e) if replica && e.code() == pigeonhole::ErrorCode::ReadOnly => {}
        Err(e) => panic!("{label}: create: {e}"),
    }
    db.flush().unwrap();
    db.compact().unwrap();
    db.close().unwrap();

    // The offline check finds nothing, and a reopen reads both.
    let report = pigeonhole::check(&path, pigeonhole::CheckOptions::default())
        .unwrap_or_else(|err| panic!("{label}: check: {err}"));
    assert!(report.is_ok(), "{label}: {:#?}", report.findings);
    let db = Pigeonhole::open(&path, real_options()).unwrap();
    let after = contents(&db);
    let (old, new): (Vec<_>, Vec<_>) = after
        .into_iter()
        .partition(|l| !l.starts_with("corpus_writes\t"));
    assert_eq!(old, expected, "{label}: contents after the write");
    assert_eq!(new.len(), usize::from(!replica), "{label}: {new:?}");
    db.close().unwrap();
}

#[test]
fn corpus_entries_open_read_check_and_write() {
    let root = corpus_dir();
    for e in entries() {
        assert_eq!(
            root.join(e.name).join(EXTRA).exists(),
            e.extra.is_some(),
            "{}: {EXTRA} doesn't match the entry: run generate_corpus",
            e.name
        );
        check_entry(e.name, &root.join(e.name), e.extra);
    }
}

/// Every release corpus (FORMAT §13): its files match `frozen.txt` byte for byte, and every
/// entry opens, reads exactly, writes and checks. A corpus not written yet is a directory
/// holding only its README.
#[test]
fn release_corpus_entries_open_read_check_and_write() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus");
    for version in RELEASE_CORPORA {
        let dir = root.join(format!("v{version}"));
        let present: Vec<String> = std::fs::read_dir(&dir)
            .unwrap_or_else(|err| panic!("v{version}: {err}"))
            .map(|f| f.unwrap().file_name().into_string().unwrap())
            .collect();
        if !dir.join(FROZEN).exists() {
            assert_eq!(
                present,
                ["README.md"],
                "v{version} holds files but no {FROZEN}: write it with scripts/release-corpus.sh"
            );
            eprintln!("v{version}: not written yet (placeholder)");
            continue;
        }
        let listed: BTreeMap<String, (usize, u64)> = std::fs::read_to_string(dir.join(FROZEN))
            .unwrap()
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(|l| {
                let mut f = l.split(' ');
                let (path, len, hash) = (f.next().unwrap(), f.next().unwrap(), f.next().unwrap());
                (
                    path.to_owned(),
                    (len.parse().unwrap(), u64::from_str_radix(hash, 16).unwrap()),
                )
            })
            .collect();
        let mut entries: Vec<&str> = present
            .iter()
            .filter(|n| dir.join(n).is_dir())
            .map(String::as_str)
            .collect();
        entries.sort_unstable();
        assert!(!entries.is_empty(), "v{version}: no entries");
        let mut on_disk = BTreeMap::new();
        for entry in &entries {
            for f in std::fs::read_dir(dir.join(entry)).unwrap() {
                let f = f.unwrap();
                let bytes = std::fs::read(f.path()).unwrap();
                let path = format!("{entry}/{}", f.file_name().into_string().unwrap());
                on_disk.insert(
                    path,
                    (bytes.len(), pigeonhole_format::checksum::xxh3_64(&bytes)),
                );
            }
        }
        assert_eq!(
            on_disk, listed,
            "v{version} no longer matches {FROZEN}: a release corpus never changes"
        );
        for entry in entries {
            // A release corpus's entries are this build's too: entries are never removed. One
            // behind a feature this build lacks is skipped (its files were checked above).
            let Some(e) = crate::entries().into_iter().find(|e| e.name == entry) else {
                assert!(
                    FEATURE_ENTRIES.iter().any(|&(n, on)| n == entry && !on),
                    "v{version}/{entry}: not an entry of this build"
                );
                eprintln!("v{version}/{entry}: behind a feature this build lacks; skipped");
                continue;
            };
            check_entry(&format!("v{version}/{entry}"), &dir.join(entry), e.extra);
        }
    }
}

/// The upgrade corpus (FORMAT §13): databases written by released 0.2.x and 0.3.x builds
/// from crates.io, in `tests/corpus/v<version>/<entry>/`, written once by
/// `tests/corpus/legacy-gen` and frozen. A 1.x build opens every one: read-only it reads it
/// without writing a byte, writable it upgrades it to format 4 and reads it exactly.
const UPGRADE_VERSIONS: [&str; 2] = ["0.2.5", "0.3.3"];
const UPGRADE_ENTRIES: [&str; 6] = [
    "wal_only",
    "flushed",
    "compacted",
    "counters",
    "blobs",
    "merge_crash",
];

/// Both superblock slots' format versions (a slot never written reads `None`).
fn slot_versions(path: &Path) -> [Option<u32>; 2] {
    let bytes = std::fs::read(path).unwrap();
    [0usize, 1].map(|slot| {
        let page = &bytes[slot * 4096..(slot + 1) * 4096];
        pigeonhole_format::superblock::Superblock::decode(page)
            .ok()
            .map(|s| s.version.0)
    })
}

/// Every file of the entry copied to `dir`, by name, with its bytes.
fn files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|f| {
            let f = f.unwrap();
            (
                f.file_name().into_string().unwrap(),
                std::fs::read(f.path()).unwrap(),
            )
        })
        .collect()
}

#[test]
fn upgrade_corpus_entries_open_upgrade_and_read_exactly() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus");
    for version in UPGRADE_VERSIONS {
        for entry in UPGRADE_ENTRIES {
            let name = format!("v{version}/{entry}");
            let src = root.join(format!("v{version}")).join(entry);
            assert!(src.join("expected.txt").exists(), "{name} is missing");
            let tmp = TempDir::new(&format!("upgrade-{version}-{entry}"));
            for f in std::fs::read_dir(&src).unwrap() {
                let f = f.unwrap();
                std::fs::copy(f.path(), tmp.0.join(f.file_name())).unwrap();
            }
            let path = tmp.0.join(DB);
            let expected: Vec<String> = std::fs::read_to_string(src.join("expected.txt"))
                .unwrap()
                .lines()
                .filter(|l| !l.is_empty())
                .map(str::to_owned)
                .collect();
            let before = slot_versions(&path);
            assert!(
                before.iter().flatten().all(|&v| v < 4),
                "{name}: written by a 0.x build, so an older format: {before:?}"
            );

            // Read-only (`read_only_fallback`, taken because this process doesn't register the
            // entry's merge operator): reads what it can, replays the WAL into memory, and
            // writes nothing, so the old build could still open the file.
            if entry == "merge_crash" {
                let untouched = files(&tmp.0);
                let options = real_options_without_merge().read_only_fallback(true);
                let db = Pigeonhole::open(&path, options)
                    .unwrap_or_else(|err| panic!("{name}: read-only open: {err}"));
                assert!(
                    db.info().unwrap().read_only,
                    "{name}: the fallback was taken"
                );
                let t = db.table("log").unwrap().open().unwrap();
                let mut plain = Vec::new();
                let scan = t.scan_prefix(b"").family("p").versions(0);
                let mut it = scan.iter().unwrap();
                while let Some(row) = it.next_ref().unwrap() {
                    for e in row.iter() {
                        let Value::Bytes(b) = e.cell.typed() else {
                            panic!("{name}: a plain cell")
                        };
                        plain.push(format!(
                            "log\t{}\t{}\t{}\t{}\tbytes:{}",
                            hex(row.key()),
                            e.family,
                            hex(e.qualifier),
                            e.cell.timestamp(),
                            hex(b)
                        ));
                    }
                }
                drop(it);
                let want: Vec<_> = expected
                    .iter()
                    .filter(|l| l.split('\t').nth(2) == Some("p"))
                    .cloned()
                    .collect();
                assert_eq!(plain, want, "{name}: the plain family, read-only");
                assert!(
                    t.mutate(b"x").put("p", b"n", b"no").commit().is_err(),
                    "{name}: a read-only handle refuses writes"
                );
                db.close().unwrap();
                assert_eq!(slot_versions(&path), before, "{name}: not upgraded");
                assert!(
                    files(&tmp.0) == untouched,
                    "{name}: the read-only open changed a file"
                );
            }

            // Writable: upgrades both superblock slots to format 4 and reads every cell as
            // the old build read it.
            let db = Pigeonhole::open(&path, real_options())
                .unwrap_or_else(|err| panic!("{name}: open: {err}"));
            assert_eq!(contents(&db), expected, "{name}: contents");
            let t = db
                .table("corpus_writes")
                .unwrap()
                .family("f", Family::default())
                .create_if_missing()
                .unwrap();
            t.mutate(b"written")
                .put("f", b"q", b"by this build")
                .commit()
                .unwrap();
            db.flush().unwrap();
            db.compact().unwrap();
            db.close().unwrap();
            // Read once closed: on Windows the open database's lock range refuses reads.
            assert_eq!(slot_versions(&path), [Some(4), Some(4)], "{name}: upgraded");
            let report = pigeonhole::check(&path, pigeonhole::CheckOptions::default())
                .unwrap_or_else(|err| panic!("{name}: check: {err}"));
            assert!(report.is_ok(), "{name}: {:#?}", report.findings);
            let db = Pigeonhole::open(&path, real_options()).unwrap();
            let (old, new): (Vec<_>, Vec<_>) = contents(&db)
                .into_iter()
                .partition(|l| !l.starts_with("corpus_writes\t"));
            assert_eq!(old, expected, "{name}: contents after the write");
            assert_eq!(new.len(), 1, "{name}: {new:?}");
            db.close().unwrap();
        }
    }
}

fn real_options_without_merge() -> Options {
    Options::default()
        .shards(2)
        .pin_threads(false)
        .memtable_budget(8 << 20)
        .block_cache(8 << 20)
        .create_if_missing(false)
}

/// The rollback corpus (#1837): databases a 1.2 build left after a crash inside a root
/// commit, then opened, written and closed by 1.1.3, in `tests/corpus/rollback-1.1.3/<entry>/`,
/// written once by `tests/corpus/rollback-gen` from 1.1.3's own code and frozen. Each case is
/// a root that landed or didn't, with or without the root's Catalog entry.
const ROLLBACK_ENTRIES: [&str; 4] = [
    "landed",
    "landed_catalog",
    "not_landed",
    "not_landed_catalog",
];

/// 1.1.3's CHANGELOG and 1.2.0's upgrade note promise that a change feed survives a rollback
/// to 1.1.3 and the move back: 1.1.3 resolves the intent 1.2's crash left, as 1.2 does, and
/// keeps the feed, so this build reopens each entry without feed options (as `phdb` or a tool
/// does), keeps the feed, and resumes the token taken before the rollback through both
/// commits, the one before it and 1.1.3's, with nothing skipped. That includes the root whose
/// Catalog entry 1.1.3 can't read and expires on: back on 1.2 the token resumes again.
#[test]
fn a_rollback_to_1_1_3_and_back_keeps_the_feed() {
    use pigeonhole_format::feed::{FEED_SUFFIX, FeedEntry, FeedHeader, decode_entries};
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/rollback-1.1.3");
    // Frozen, as a release corpus is: 1.1.3 wrote these bytes, and only a build that reads
    // them proves the promise.
    let listed: BTreeMap<String, (usize, u64)> = std::fs::read_to_string(root.join(FROZEN))
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| {
            let mut f = l.split(' ');
            let (path, len, hash) = (f.next().unwrap(), f.next().unwrap(), f.next().unwrap());
            (
                path.to_owned(),
                (len.parse().unwrap(), u64::from_str_radix(hash, 16).unwrap()),
            )
        })
        .collect();
    let mut on_disk = BTreeMap::new();
    for entry in ROLLBACK_ENTRIES {
        for f in std::fs::read_dir(root.join(entry)).unwrap() {
            let f = f.unwrap();
            let bytes = std::fs::read(f.path()).unwrap();
            on_disk.insert(
                format!("{entry}/{}", f.file_name().into_string().unwrap()),
                (bytes.len(), pigeonhole_format::checksum::xxh3_64(&bytes)),
            );
        }
    }
    assert_eq!(on_disk, listed, "rollback-1.1.3 no longer matches {FROZEN}");
    for entry in ROLLBACK_ENTRIES {
        let src = root.join(entry);
        let tmp = TempDir::new(&format!("rollback-{entry}"));
        for f in std::fs::read_dir(&src).unwrap() {
            let f = f.unwrap();
            std::fs::copy(f.path(), tmp.0.join(f.file_name())).unwrap();
        }
        // 1.1.3 left no intent anywhere: one in front of its own Session entry would break
        // the history here.
        let sidecar = std::fs::read(tmp.0.join(format!("{DB}{FEED_SUFFIX}"))).unwrap();
        let (entries, end) = decode_entries(&sidecar[FeedHeader::LEN..]);
        assert_eq!(
            FeedHeader::LEN + end,
            sidecar.len(),
            "{entry}: nothing torn"
        );
        assert!(
            !entries
                .iter()
                .any(|e| matches!(e, FeedEntry::RootIntent { .. })),
            "{entry}: no intent left: {entries:?}"
        );

        // Unset shards: the open takes the feed's stream count from the sidecar, as it does
        // its retention.
        let options = Options::default()
            .pin_threads(false)
            .memtable_budget(8 << 20)
            .create_if_missing(false);
        let db = Pigeonhole::open(tmp.0.join(DB), options)
            .unwrap_or_else(|err| panic!("{entry}: open: {err}"));
        assert!(
            db.info().unwrap().feed.is_some(),
            "{entry}: the open kept the change feed"
        );
        let tokens = db.table("feed_tokens").unwrap().open().unwrap();
        let get = |q: &[u8]| {
            tokens
                .get(b"events", "t", q)
                .unwrap()
                .unwrap_or_else(|| panic!("{entry}: feed_tokens has {q:?}"))
        };
        let token = pigeonhole::FeedToken::from_bytes(get(b"token").value());
        let want = [b"one".as_slice(), b"two".as_slice()].map(|q| get(q).as_i64().unwrap() as u64);
        let events = db.table("events").unwrap().open().unwrap();
        let mut feed = events
            .feed()
            .resume(&token)
            .open()
            .unwrap_or_else(|err| panic!("{entry}: resume: {err}"));
        for (seqno, value) in want.iter().zip([b"one".as_slice(), b"two".as_slice()]) {
            let c = feed
                .next(FEED_WAIT)
                .unwrap_or_else(|err| panic!("{entry}: the feed ended: {err}"))
                .unwrap_or_else(|| panic!("{entry}: nothing within {FEED_WAIT:?}"));
            assert_eq!(c.seqno(), *seqno, "{entry}: the commits in order");
            let [ch] = c.changes() else {
                panic!("{entry}: one change in seqno {seqno}")
            };
            assert!(
                matches!(ch.value(), Some(Value::Bytes(b)) if b == value),
                "{entry}: seqno {seqno}: {:?}",
                ch.value()
            );
        }
        drop(feed);
        db.close().unwrap();
    }
}

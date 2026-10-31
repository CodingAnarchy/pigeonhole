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

use pigeonhole::{Aggregation, Durability, Family, Options, Pigeonhole, Value};
use pigeonhole_io::sim::{CrashKind, SimVfs};
use pigeonhole_io::{OpenOptions, Vfs};

/// Where the corpus lives.
fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/format4")
}

/// The release corpora: written by each release's build, then frozen. A directory holding
/// only its README is a placeholder until that release writes it.
const RELEASE_CORPORA: [&str; 1] = ["1.0.0"];

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
                let value = match e.cell.typed() {
                    Value::Bytes(b) => format!("bytes:{}", hex(b)),
                    Value::I64(n) => format!("i64:{n}"),
                    Value::F64(x) => format!("f64:{:016x}", x.to_bits()),
                    Value::Varint(n) => format!("varint:{n}"),
                    other => panic!("a value type this test doesn't know: {other:?}"),
                };
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

/// The marker file of an entry that 1.0.0 must refuse: it holds the `incompat` feature
/// bits the refusal names (decimal). CI's 1.0.0 job fails if such an entry opens, or if
/// the refusal names other bits (`tests/corpus/compat-reader`).
const REFUSED_BY: &str = "refused-by-1.0.0.txt";

/// One corpus entry: how it is written, whether it is left crashed (WAL replay needed), and
/// the `incompat` feature bits it uses that 1.0.0 lacks (0: 1.0.0 opens it).
struct Entry {
    name: &'static str,
    shards: usize,
    write: fn(&Pigeonhole),
    crash: bool,
    refused_by_1_0_0: u64,
}

fn entries() -> Vec<Entry> {
    vec![
        Entry {
            name: "empty",
            shards: 1,
            write: |_| {},
            crash: false,
            refused_by_1_0_0: 0,
        },
        Entry {
            name: "values",
            shards: 1,
            write: write_values,
            crash: false,
            refused_by_1_0_0: 0,
        },
        Entry {
            name: "counters",
            shards: 2,
            write: write_counters,
            crash: false,
            refused_by_1_0_0: 0,
        },
        Entry {
            name: "blobs",
            shards: 1,
            write: write_blobs,
            crash: false,
            refused_by_1_0_0: 0,
        },
        Entry {
            name: "merge",
            shards: 1,
            write: write_merge,
            crash: false,
            refused_by_1_0_0: 0,
        },
        Entry {
            name: "wal_crash",
            shards: 2,
            write: write_wal_crash,
            crash: true,
            refused_by_1_0_0: 0,
        },
        Entry {
            name: "families",
            shards: 1,
            write: write_families,
            crash: false,
            refused_by_1_0_0: 0,
        },
        Entry {
            name: "wal_ops",
            shards: 2,
            write: write_wal_ops,
            crash: true,
            refused_by_1_0_0: 0,
        },
        Entry {
            name: "checkpoints",
            shards: 1,
            write: write_checkpoints,
            crash: false,
            refused_by_1_0_0: 0,
        },
        Entry {
            name: "aggregates",
            shards: 2,
            write: write_aggregates,
            crash: true,
            refused_by_1_0_0: pigeonhole_format::manifest::INCOMPAT_AGGREGATES,
        },
    ]
}

/// Min, max and HLL families (D294, the first `incompat` feature bit), and the bitwise and
/// `f64` families (D298, the same bit): inputs at the fixed timestamp and in buckets, puts,
/// deletes and checkpointed reads, sparse and dense sketches, signed zeros and infinities;
/// flushed, then more in the WAL that the crash leaves for replay.
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
                    .commit()
                    .unwrap();
            }
            20 => {
                t.mutate(b"a02")
                    .delete_column("lo", b"all")
                    .delete_column("hi", b"all")
                    .delete_column("parity", b"all")
                    .delete_column("warmest", b"all")
                    .commit()
                    .unwrap();
                // A read that folds what the flush below writes as a base.
                let _ = t.get(b"a03", "lo", b"all").unwrap();
                db.flush().unwrap();
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

/// The database's files in the simulator's `/db`, by name (no shared memory).
fn sim_files(vfs: &SimVfs) -> BTreeMap<String, Vec<u8>> {
    vfs.list_dir(Path::new("/db"))
        .unwrap()
        .into_iter()
        .filter_map(|p| {
            let name = p.file_name()?.to_str()?.to_owned();
            (name == DB || name.starts_with("data.phdb-wal-")).then_some((p, name))
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
        let vfs = SimVfs::new(9_000 + seed as u64);
        let path = format!("/db/{DB}");
        let db = Pigeonhole::open(&path, sim_options(&vfs, e.shards)).unwrap();
        (e.write)(&db);
        if e.crash {
            vfs.crash(CrashKind::Process);
            drop(db);
        } else {
            // Keep the checked-in files small: give back the file's free tail.
            db.shrink().unwrap();
            db.close().unwrap();
        }
        let files = sim_files(&vfs);
        // What the files hold, as this build reads them (a reopen replays a crashed entry).
        let db = Pigeonhole::open(&path, sim_options(&vfs, e.shards)).unwrap();
        let mut expected = contents(&db).join("\n");
        expected.push('\n');
        db.close().unwrap();

        let dir = root.join(e.name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for (name, bytes) in &files {
            std::fs::write(dir.join(name), bytes).unwrap();
        }
        std::fs::write(dir.join("expected.txt"), &expected).unwrap();
        for (name, bytes) in &files {
            frozen.push(frozen_line(&format!("{}/{name}", e.name), bytes));
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

/// Opens a copy of the entry in `src`, reads every cell exactly as `expected.txt` says,
/// writes through a flush and a compaction, passes the offline check, and reads everything
/// again after a reopen.
fn check_entry(label: &str, src: &Path) {
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
    // Write on top, through a flush and a compaction.
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
    assert_eq!(new.len(), 1, "{label}: {new:?}");
    db.close().unwrap();
}

#[test]
fn corpus_entries_open_read_check_and_write() {
    let root = corpus_dir();
    for e in entries() {
        check_entry(e.name, &root.join(e.name));
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
            check_entry(&format!("v{version}/{entry}"), &dir.join(entry));
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

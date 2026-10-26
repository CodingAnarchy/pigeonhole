//! The format corpus (#496 PR 6, D269): small database files that every later build must
//! open, read, check and write. They live in `tests/corpus/format4/<entry>/`: the main file,
//! its WAL files, and `expected.txt`, every cell the generating build read back. The corpus
//! is regenerated once, by the 1.0 release candidate, and never again: a build that cannot
//! read a 1.0 file breaks FORMAT §13's promise.
//!
//! `corpus_entries_open_read_check_and_write` runs in CI. `generate_corpus` is ignored;
//! regenerate with
//! `cargo test -p pigeonhole --test corpus -- --ignored generate_corpus` (it rewrites the
//! directory). Entries are written through the simulator, so a crash state is exact, and
//! their files exported as they were left.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pigeonhole::{Durability, Family, Options, Pigeonhole, Value};
use pigeonhole_io::sim::{CrashKind, SimVfs};
use pigeonhole_io::{OpenOptions, Vfs};

/// Where the corpus lives.
fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/format4")
}

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

/// One corpus entry: how it is written, and whether it is left crashed (WAL replay needed).
struct Entry {
    name: &'static str,
    shards: usize,
    write: fn(&Pigeonhole),
    crash: bool,
}

fn entries() -> Vec<Entry> {
    vec![
        Entry {
            name: "empty",
            shards: 1,
            write: |_| {},
            crash: false,
        },
        Entry {
            name: "values",
            shards: 1,
            write: write_values,
            crash: false,
        },
        Entry {
            name: "counters",
            shards: 2,
            write: write_counters,
            crash: false,
        },
        Entry {
            name: "blobs",
            shards: 1,
            write: write_blobs,
            crash: false,
        },
        Entry {
            name: "merge",
            shards: 1,
            write: write_merge,
            crash: false,
        },
        Entry {
            name: "wal_crash",
            shards: 2,
            write: write_wal_crash,
            crash: true,
        },
    ]
}

/// Every value type, every delete kind, several versions, three codecs; flushed and
/// compacted, then a row on top that the close flushes.
fn write_values(db: &Pigeonhole) {
    let t = db
        .table("values")
        .unwrap()
        .family("lz4", Family::default().max_versions(0))
        .family("zstd", Family::default().zstd(3).max_versions(0))
        .family("raw", Family::default().uncompressed().max_versions(3))
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

#[test]
#[ignore = "rewrites tests/corpus; run once at the 1.0 release candidate"]
fn generate_corpus() {
    let root = corpus_dir();
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
        std::fs::write(dir.join("expected.txt"), expected).unwrap();
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

#[test]
fn corpus_entries_open_read_check_and_write() {
    let root = corpus_dir();
    let mut seen = 0;
    for e in entries() {
        let src = root.join(e.name);
        assert!(
            src.join("expected.txt").exists(),
            "corpus entry {} is missing: run generate_corpus",
            e.name
        );
        seen += 1;
        // A copy: opening writes (WAL replay, the format upgrade of an older file).
        let tmp = TempDir::new(e.name);
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

        // Read: every cell, as the generating build read it.
        let db = Pigeonhole::open(&path, real_options())
            .unwrap_or_else(|err| panic!("{}: open: {err}", e.name));
        assert_eq!(contents(&db), expected, "{}: contents", e.name);
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
            .unwrap_or_else(|err| panic!("{}: check: {err}", e.name));
        assert!(report.is_ok(), "{}: {:#?}", e.name, report.findings);
        let db = Pigeonhole::open(&path, real_options()).unwrap();
        let after = contents(&db);
        let (old, new): (Vec<_>, Vec<_>) = after
            .into_iter()
            .partition(|l| !l.starts_with("corpus_writes\t"));
        assert_eq!(old, expected, "{}: contents after the write", e.name);
        assert_eq!(new.len(), 1, "{}: {new:?}", e.name);
        db.close().unwrap();
    }
    assert_eq!(seen, entries().len());
}

//! Writes the upgrade corpus (FORMAT §13): databases written by released 0.2.x and 0.3.x
//! builds from crates.io, which every 1.x build must open, upgrade and read exactly.
//!
//! `cargo run --release -- <out dir>` writes `<out>/v0.2.5/<entry>/` and
//! `<out>/v0.3.3/<entry>/`: the database's files as the old build left them, and
//! `expected.txt`, every cell that build reads back from a copy (one per line: table, row
//! (hex), family, qualifier (hex), timestamp, value). A crashed entry is written by a child
//! process that exits without closing, so its WAL holds synced commits replay must recover.

use std::path::{Path, PathBuf};

/// Timestamps fixed, so `expected.txt` does not depend on a clock.
const T0: u64 = 1_767_225_600_000_000;

/// The database file's name inside an entry.
const DB: &str = "data.phdb";

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The files of the database at `dir/DB` (the main file and its WAL files), by name.
fn db_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_str().unwrap().starts_with(DB))
        .collect();
    v.sort();
    v
}

fn copy_db(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for f in db_files(from) {
        std::fs::copy(&f, to.join(f.file_name().unwrap())).unwrap();
    }
}

macro_rules! generator {
    ($module:ident, $crate_name:ident, $version:literal) => {
        mod $module {
            use super::*;
            use std::sync::Arc;

            use $crate_name::{
                Durability, Family, MergeError, MergeOperator, Options, Pigeonhole, Value,
            };

            pub const VERSION: &str = $version;

            /// `corpus.concat`, as the format 4 corpus registers it: concatenated payloads.
            #[derive(Debug)]
            struct Concat;

            impl MergeOperator for Concat {
                fn name(&self) -> &str {
                    "corpus.concat"
                }
                fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError> {
                    // Stored values: a tag byte, then the payload.
                    let newer = acc.split_off(1);
                    acc.extend_from_slice(&older[1..]);
                    acc.extend_from_slice(&newer);
                    Ok(())
                }
                fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError> {
                    if let Some(base) = base {
                        let newer = acc.split_off(1);
                        acc.extend_from_slice(&base[1..]);
                        acc.extend_from_slice(&newer);
                    }
                    Ok(())
                }
            }

            fn options(shards: usize) -> Options {
                Options::default()
                    .shards(shards)
                    .pin_threads(false)
                    .wal_segment_size(256 << 10)
                    .merge_operator(Arc::new(Concat))
            }

            /// Every cell of every table, all retained versions, one line each.
            pub fn contents(path: &Path) -> Vec<String> {
                let db = Pigeonhole::open(path, options(2)).unwrap();
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
                db.close().unwrap();
                lines
            }

            /// Three families (LZ4, zstd, uncompressed), bytes, i64 and f64 values, a second
            /// version of each row and every delete kind.
            fn values(db: &Pigeonhole, flush_between: bool) {
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
                        // No `put_f64_at` before 0.4: the clock's timestamp, recorded in expected.txt.
                        .put_f64("raw", b"f", i as f64 / 7.0)
                        .commit()
                        .unwrap();
                }
                if flush_between {
                    db.flush().unwrap();
                }
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
            }

            /// Synced commits and nothing flushed: everything is in the WAL.
            fn wal_only(db: &Pigeonhole) {
                let t = db
                    .table("events")
                    .unwrap()
                    .family("e", Family::default())
                    .create()
                    .unwrap();
                for i in 0..60u64 {
                    t.mutate(format!("ev{:03}", (i * 7) % 100).as_bytes())
                        .put_at("e", b"q", T0 + i, format!("logged {i}").as_bytes())
                        .durability(Durability::Sync)
                        .commit()
                        .unwrap();
                }
            }

            fn flushed(db: &Pigeonhole) {
                values(db, false);
                db.flush().unwrap();
            }

            fn compacted(db: &Pigeonhole) {
                values(db, true);
                db.flush().unwrap();
                db.compact().unwrap();
            }

            /// A counter family over two shards, partly flushed.
            fn counters(db: &Pigeonhole) {
                let t = db
                    .table("counters")
                    .unwrap()
                    .family("c", Family::counter())
                    .create()
                    .unwrap();
                for i in 0..30u64 {
                    t.mutate(format!("k{:02}", i % 7).as_bytes())
                        .incr_at("c", b"hits", T0 + i, i as i64 + 1)
                        .commit()
                        .unwrap();
                    if i == 15 {
                        db.flush().unwrap();
                    }
                }
            }

            /// Separated values, half overwritten, then compacted.
            fn blobs(db: &Pigeonhole) {
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
                for i in (0..24u64).step_by(2) {
                    t.mutate(format!("blob{i:02}").as_bytes())
                        .put_at("v", b"x", T0 + 100 + i, b"small now")
                        .commit()
                        .unwrap();
                }
                db.compact().unwrap();
            }

            /// A merge family (`corpus.concat`) beside a plain one; operands in SSTs and, after
            /// the crash, synced in the WAL only.
            fn merge_crash(db: &Pigeonhole) {
                let t = db
                    .table("log")
                    .unwrap()
                    .family("l", Family::default().merge_operator("corpus.concat"))
                    .family("p", Family::default())
                    .create()
                    .unwrap();
                for i in 0..24u64 {
                    let row = format!("log{}", i % 3);
                    t.mutate(row.as_bytes())
                        .merge("l", b"q", format!("[{i}]").as_bytes())
                        .put_at("p", b"n", T0 + i, format!("plain {i}").as_bytes())
                        .durability(Durability::Sync)
                        .commit()
                        .unwrap();
                    if i == 11 {
                        db.flush().unwrap();
                    }
                }
            }

            /// Entries: name, shards, writer, and whether it is left crashed.
            pub fn entries() -> Vec<(&'static str, usize, fn(&Pigeonhole), bool)> {
                vec![
                    ("wal_only", 2, wal_only, true),
                    ("flushed", 1, flushed, false),
                    ("compacted", 1, compacted, false),
                    ("counters", 2, counters, false),
                    ("blobs", 1, blobs, false),
                    ("merge_crash", 1, merge_crash, true),
                ]
            }

            /// Writes `entry` at `dir/DB`; a crashed entry exits the process without closing.
            pub fn write(entry: &str, dir: &Path) {
                let (_, shards, f, crash) = entries()
                    .into_iter()
                    .find(|e| e.0 == entry)
                    .expect("entry");
                std::fs::create_dir_all(dir).unwrap();
                let db = Pigeonhole::open(dir.join(DB), options(shards)).unwrap();
                f(&db);
                if crash {
                    std::process::exit(0);
                }
                db.close().unwrap();
            }
        }
    };
}

generator!(v02, p02, "0.2.5");
generator!(v03, p03, "0.3.3");

fn main() {
    let args: Vec<String> = std::env::args().collect();
    // Child: `--write <version> <entry> <dir>`.
    if args.get(1).map(String::as_str) == Some("--write") {
        let dir = Path::new(&args[4]);
        match args[2].as_str() {
            v02::VERSION => v02::write(&args[3], dir),
            v03::VERSION => v03::write(&args[3], dir),
            v => panic!("unknown version {v}"),
        }
        return;
    }
    let out = PathBuf::from(args.get(1).expect("usage: legacy-gen <out dir>"));
    let scratch = out.join(".scratch");
    let versions: [(&str, Vec<&str>, fn(&Path) -> Vec<String>); 2] = [
        (
            v02::VERSION,
            v02::entries().iter().map(|e| e.0).collect(),
            v02::contents,
        ),
        (
            v03::VERSION,
            v03::entries().iter().map(|e| e.0).collect(),
            v03::contents,
        ),
    ];
    for (version, names, contents) in versions {
        for name in names {
            let dir = out.join(format!("v{version}")).join(name);
            assert!(!dir.exists(), "{} exists; remove it first", dir.display());
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--write", version, name, dir.to_str().unwrap()])
                .status()
                .unwrap();
            assert!(status.success(), "{version} {name}");
            // Read back through a copy, so the entry keeps the files as they were left.
            let copy = scratch.join(format!("{version}-{name}"));
            copy_db(&dir, &copy);
            let lines = contents(&copy.join(DB));
            let mut text = lines.join("\n");
            text.push('\n');
            std::fs::write(dir.join("expected.txt"), text).unwrap();
            let files: Vec<_> = db_files(&dir)
                .iter()
                .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
                .collect();
            println!("v{version}/{name}: {} cells, files {files:?}", lines.len());
        }
    }
    std::fs::remove_dir_all(&scratch).unwrap();
}

//! Opens files written by a newer build with the published 1.0.0 (FORMAT §13, "newer files").
//!
//! `cargo run --release -- <corpus dir>` checks every entry under the directory: a database
//! written by `tests/corpus.rs`'s `generate_corpus` (`PIGEONHOLE_CORPUS_OUT=<dir>`), with its
//! `expected.txt`. On a copy of each, 1.0.0 opens it read-write, reads every cell exactly,
//! writes through a flush and a compaction, passes the offline check, and reads everything
//! again after a reopen. An entry that uses a feature 1.0.0 lacks may be refused with
//! `UnsupportedFeature`, as the promise allows: an `ro_compat` one is then read read-only
//! with `read_only_fallback`, an `incompat` one is reported and skipped. An entry with a
//! `refused-by-1.0.0.txt` (D294: it names the `incompat` bits it uses) must be refused, for
//! exactly those bits: opening it, read-write or read-only, fails the entry. Exits 1 if any
//! entry fails.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use pigeonhole::{ErrorCode, Family, MergeError, MergeOperator, Options, Pigeonhole, Value};

/// The database file's name inside an entry.
const DB: &str = "data.phdb";

/// `corpus.concat`, as the corpus registers it: concatenated payloads.
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

fn options() -> Options {
    Options::default()
        .shards(2)
        .pin_threads(false)
        .memtable_budget(8 << 20)
        .block_cache(8 << 20)
        .create_if_missing(false)
        .merge_operator(Arc::new(Concat))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Every cell of every table, all retained versions, one line each, in scan order: the
/// format of `expected.txt`.
fn contents(db: &Pigeonhole) -> Result<Vec<String>, String> {
    let mut tables = db.tables();
    tables.sort();
    let mut lines = Vec::new();
    for name in tables {
        let t = db
            .table(&name)
            .map_err(|e| e.to_string())?
            .open()
            .map_err(|e| e.to_string())?;
        let scan = t.scan_prefix(b"").versions(0);
        let mut it = scan.iter().map_err(|e| e.to_string())?;
        while let Some(row) = it.next_ref().map_err(|e| e.to_string())? {
            for e in row.iter() {
                let value = match e.cell.typed() {
                    Value::Bytes(b) => format!("bytes:{}", hex(b)),
                    Value::I64(n) => format!("i64:{n}"),
                    Value::F64(x) => format!("f64:{:016x}", x.to_bits()),
                    Value::Varint(n) => format!("varint:{n}"),
                    other => return Err(format!("a value type 1.0.0 doesn't print: {other:?}")),
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
    Ok(lines)
}

/// What happened to one entry.
enum Outcome {
    Passed,
    ReadOnly,
    Refused(String),
}

fn check(src: &Path, tmp: &Path) -> Result<Outcome, String> {
    std::fs::create_dir_all(tmp).map_err(|e| e.to_string())?;
    for f in std::fs::read_dir(src).map_err(|e| e.to_string())? {
        let f = f.map_err(|e| e.to_string())?;
        std::fs::copy(f.path(), tmp.join(f.file_name())).map_err(|e| e.to_string())?;
    }
    let path = tmp.join(DB);
    let expected: Vec<String> = std::fs::read_to_string(src.join("expected.txt"))
        .map_err(|e| e.to_string())?
        .lines()
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect();
    let same = |got: Vec<String>, when: &str| {
        if got == expected {
            return Ok(());
        }
        let first = got.iter().zip(&expected).position(|(a, b)| a != b);
        Err(format!(
            "{when}: {} cells, expected {}; first difference at line {first:?}",
            got.len(),
            expected.len()
        ))
    };

    let db = match Pigeonhole::open(&path, options()) {
        Ok(db) => db,
        Err(err) if err.code() == ErrorCode::UnsupportedFeature => {
            // A feature 1.0.0 lacks. An ro_compat one still opens read-only.
            match Pigeonhole::open(&path, options().read_only_fallback(true)) {
                Ok(db) => {
                    same(contents(&db)?, "read-only")?;
                    db.close().map_err(|e| e.to_string())?;
                    return Ok(Outcome::ReadOnly);
                }
                Err(err) if err.code() == ErrorCode::UnsupportedFeature => {
                    return Ok(Outcome::Refused(err.to_string()));
                }
                Err(err) => return Err(format!("read-only open: {err}")),
            }
        }
        Err(err) => return Err(format!("open: {err}")),
    };
    same(contents(&db)?, "read")?;
    let t = db
        .table("compat_writes")
        .and_then(|b| b.family("f", Family::default()).create_if_missing())
        .map_err(|e| format!("create: {e}"))?;
    t.mutate(b"written")
        .put("f", b"q", b"by 1.0.0")
        .commit()
        .map_err(|e| format!("write: {e}"))?;
    db.flush().map_err(|e| format!("flush: {e}"))?;
    db.compact().map_err(|e| format!("compact: {e}"))?;
    db.close().map_err(|e| format!("close: {e}"))?;

    let report = pigeonhole::check(&path, pigeonhole::CheckOptions::default())
        .map_err(|e| format!("check: {e}"))?;
    if !report.is_ok() {
        return Err(format!("check: {:#?}", report.findings));
    }
    let db = Pigeonhole::open(&path, options()).map_err(|e| format!("reopen: {e}"))?;
    let (old, new): (Vec<_>, Vec<_>) = contents(&db)?
        .into_iter()
        .partition(|l| !l.starts_with("compat_writes\t"));
    same(old, "after the write")?;
    if new.len() != 1 {
        return Err(format!("the written row reads back as {new:?}"));
    }
    db.close().map_err(|e| format!("close: {e}"))?;
    Ok(Outcome::Passed)
}

fn main() {
    let dir = PathBuf::from(
        std::env::args()
            .nth(1)
            .expect("usage: compat-reader <corpus dir>"),
    );
    let scratch = std::env::temp_dir().join(format!("compat-reader-{}", std::process::id()));
    let mut entries: Vec<_> = std::fs::read_dir(&dir)
        .expect("the corpus directory")
        .map(|e| e.unwrap().path())
        .filter(|p| p.join("expected.txt").exists())
        .collect();
    entries.sort();
    assert!(
        !entries.is_empty(),
        "no corpus entries in {}",
        dir.display()
    );
    let mut failed = 0;
    for src in &entries {
        let name = src.file_name().unwrap().to_string_lossy().into_owned();
        // The bits a marked entry must be refused for.
        let must_refuse = std::fs::read_to_string(src.join("refused-by-1.0.0.txt"))
            .ok()
            .map(|bits| format!("incompatible feature bits {})", bits.trim()));
        match (check(src, &scratch.join(&name)), &must_refuse) {
            (Ok(Outcome::Refused(why)), Some(bits)) if why.contains(bits.as_str()) => {
                println!("{name}: refused, as it must be: {why}");
            }
            (Ok(Outcome::Refused(why)), Some(bits)) => {
                println!("{name}: FAILED: refused, but not for {bits}: {why}");
                failed += 1;
            }
            (Ok(_), Some(bits)) => {
                println!("{name}: FAILED: opened, but must be refused for {bits}");
                failed += 1;
            }
            (Ok(Outcome::Passed), None) => println!("{name}: ok"),
            (Ok(Outcome::ReadOnly), None) => {
                println!("{name}: ok, read-only (an ro_compat feature)");
            }
            (Ok(Outcome::Refused(why)), None) => println!("{name}: refused, as allowed: {why}"),
            (Err(why), _) => {
                println!("{name}: FAILED: {why}");
                failed += 1;
            }
        }
    }
    let _ = std::fs::remove_dir_all(&scratch);
    println!("{} entries, {failed} failed", entries.len());
    if failed > 0 {
        std::process::exit(1);
    }
}

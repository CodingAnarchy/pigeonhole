//! Opens files written by a newer build with a published release (FORMAT §13, "newer files").
//!
//! Two manifests build this file: `compat-reader` against `pigeonhole =1.0.0`, and
//! `compat-reader-1.1` against `=1.1.0` (with its `v1_1` feature, for the value types 1.0.0
//! lacks; #1489). Each package's version is the release it pins, and below "1.0.0" means that
//! release.
//!
//! `cargo run --release -- <corpus dir>` checks every entry under the directory: a database
//! written by `tests/corpus.rs`'s `generate_corpus` (`PIGEONHOLE_CORPUS_OUT=<dir>`), with its
//! `expected.txt`. On a copy of each, 1.0.0 opens it read-write, reads every cell exactly,
//! writes through a flush and a compaction, passes the offline check, and reads everything
//! again after a reopen. An entry that uses a feature 1.0.0 lacks may be refused with
//! `UnsupportedFeature`, as the promise allows: an `ro_compat` one is then read read-only
//! with `read_only_fallback`, an `incompat` one is reported and skipped. An entry with a
//! `refused-by-1.0.0.txt` (D294: it names the `incompat` bits it uses, which 1.0.0 lacks)
//! must be refused by a release that lacks any of them, for exactly the ones it lacks: opening
//! it, read-write or read-only, fails the entry. A release that knows all of them (1.1.0
//! knows bit 0) must open it read-write, and a refusal fails the entry. Exits 1 if any entry
//! fails.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use pigeonhole::{ErrorCode, Family, MergeError, MergeOperator, Options, Pigeonhole, Value};

/// The database file's name inside an entry.
const DB: &str = "data.phdb";

/// The release this reader is built against: its package's version, which is the version
/// its manifest pins.
const RELEASE: &str = env!("CARGO_PKG_VERSION");

/// The `incompat` feature bits that release reads (FORMAT §8.2): 1.1.0 added bit 0 (D294,
/// D298 and vector families).
const KNOWN_INCOMPAT: u64 = if cfg!(feature = "v1_1") { 1 } else { 0 };

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
                    // As tests/corpus.rs's `value_line` prints it.
                    #[cfg(feature = "v1_1")]
                    Value::Vector(v) => {
                        format!("vector:{:?}:{}:{}", v.element(), v.dim(), hex(v.as_bytes()))
                    }
                    other => {
                        return Err(format!(
                            "a value type {RELEASE}'s reader doesn't print: {other:?}"
                        ));
                    }
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

    // 1.1.0 resumes the entry's feed first, where the entry asks for it, in an open with the
    // feed on (it doesn't inherit the feed from the sidecar, as later builds do). Not every
    // feed entry can: a 1.2 session that changed the catalog left Catalog entries (type 4),
    // which 1.1.0 doesn't know, and its feed expires there, fail closed.
    #[cfg(feature = "v1_1")]
    if src.join("feed-resumes-by-1.1.0.txt").exists() {
        let fed = options().change_feed(pigeonhole::FeedRetention::bytes(4 << 20));
        let db = Pigeonhole::open(&path, fed).map_err(|e| format!("feed open: {e}"))?;
        feed_resumes(&db, src)?;
        db.close().map_err(|e| format!("feed close: {e}"))?;
    }

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
        .put("f", b"q", format!("by {RELEASE}").as_bytes())
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

/// 1.1.0 (the first release with the change feed, D310) resumes a newer build's feed: for an
/// entry that stored a table feed's token (`feed_tokens`/`events`, `tests/corpus.rs`), the
/// commits it delivers after that token, up to the stored last seqno, are the ones the
/// generating build listed in `expected-extra.txt` (every field but the value's hash, which
/// would need a hash crate here). A newer build's sidecar entry this release doesn't know
/// would end the history and expire the feed, so this is the check that a newer build's
/// sidecar, after clean closes, holds only entries 1.1.0 reads (the D315 amendment's
/// `RootIntent`, overwritten in place, #1504).
#[cfg(feature = "v1_1")]
fn feed_resumes(db: &Pigeonhole, src: &Path) -> Result<(), String> {
    let Ok(expected) = std::fs::read_to_string(src.join("expected-extra.txt")) else {
        return Ok(());
    };
    let Ok(tokens) = db.table("feed_tokens").and_then(|b| b.open()) else {
        return Ok(());
    };
    let Some(stored) = tokens
        .get(b"events", "t", b"token")
        .map_err(|e| format!("feed token: {e}"))?
    else {
        return Ok(());
    };
    let token = pigeonhole::FeedToken::from_bytes(stored.value());
    let last = tokens
        .get(b"events", "t", b"last")
        .map_err(|e| format!("feed last: {e}"))?
        .and_then(|c| c.as_i64())
        .ok_or("feed: no stored last seqno")? as u64;
    let events = db
        .table("events")
        .and_then(|b| b.open())
        .map_err(|e| format!("feed table: {e}"))?;
    let mut feed = events
        .feed()
        .resume(&token)
        .open()
        .map_err(|e| format!("feed open: {e}"))?;
    let mut got = Vec::new();
    loop {
        let c = feed
            .next(std::time::Duration::from_secs(30))
            .map_err(|e| format!("feed: {e}"))?
            .ok_or_else(|| format!("feed: stopped before seqno {last}"))?;
        for ch in c.changes() {
            got.push(format!(
                "{}\t{}\t{}\t{:?}\t{}\t{}\t{}",
                c.seqno(),
                ch.table(),
                ch.family(),
                ch.kind(),
                hex(ch.row()),
                hex(ch.qualifier()),
                ch.timestamp()
            ));
        }
        if c.seqno() >= last {
            break;
        }
    }
    // The generating build's lines, less the value field.
    let want: Vec<String> = expected
        .lines()
        .map(|l| l.splitn(8, '\t').take(7).collect::<Vec<_>>().join("\t"))
        .collect();
    if got != want {
        let at = got.iter().zip(&want).position(|(g, w)| g != w);
        return Err(format!(
            "feed: {} lines resumed, {} expected; first difference at {at:?}",
            got.len(),
            want.len()
        ));
    }
    Ok(())
}

fn main() {
    let dir = PathBuf::from(
        std::env::args()
            .nth(1)
            .expect("usage: compat-reader <corpus dir>"),
    );
    println!("checking with pigeonhole {RELEASE}");
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
        // A marked entry's bits, less the ones this release reads: what it must be refused
        // for, if any are left. A marked entry this release reads must open.
        let marked = std::fs::read_to_string(src.join("refused-by-1.0.0.txt"))
            .ok()
            .map(|bits| {
                bits.trim()
                    .parse::<u64>()
                    .expect("refused-by-1.0.0.txt: a decimal bit mask")
            });
        let unknown = marked
            .map(|bits| bits & !KNOWN_INCOMPAT)
            .filter(|&b| b != 0);
        let must_refuse = unknown.map(|bits| format!("incompatible feature bits {bits})"));
        let outcome = check(src, &scratch.join(&name));
        if marked.is_some() && must_refuse.is_none() {
            match outcome {
                Ok(Outcome::Passed) => println!("{name}: ok (incompat bits {RELEASE} reads)"),
                Ok(Outcome::ReadOnly) => {
                    println!("{name}: FAILED: read-only, but {RELEASE} reads every bit it uses");
                    failed += 1;
                }
                Ok(Outcome::Refused(why)) => {
                    println!(
                        "{name}: FAILED: refused, but {RELEASE} reads every bit it uses: {why}"
                    );
                    failed += 1;
                }
                Err(why) => {
                    println!("{name}: FAILED: {why}");
                    failed += 1;
                }
            }
            continue;
        }
        match (outcome, &must_refuse) {
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

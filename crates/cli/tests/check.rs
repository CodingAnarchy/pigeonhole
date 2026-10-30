//! `phdb check` (#692): clean on a healthy database, and for every structure the check
//! covers, exit 3 naming that structure once bytes in it are damaged. The bytes to damage
//! come from `phdb check --map --format json` itself, so the tests use nothing but the
//! public interface. WAL cases need a crashed database: the test binary reruns itself as a
//! child that commits and exits without closing.

mod common;

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use common::{TempDir, fixture, phdb};
use pigeonhole::{Durability, Family, Options, Pigeonhole};
use serde_json::Value;

const CHILD: &str = "PHDB_TEST_CRASH_CHILD";

/// [`build_sample`]'s database (built once per test binary, each caller getting a copy).
fn sample(dir: &TempDir) -> PathBuf {
    static TEMPLATE: OnceLock<PathBuf> = OnceLock::new();
    fixture(&TEMPLATE, "c.phdb", build_sample, dir)
}

/// A database with SSTs and a blob file, closed cleanly. Fixed timestamps keep its bytes the
/// same every run.
fn build_sample(path: &Path) {
    let db = Pigeonhole::open(path, Options::default().shards(1).pin_threads(false)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default().blob_threshold(100))
        .create()
        .unwrap();
    for i in 0..200u64 {
        let v = if i % 10 == 0 {
            vec![b'b'; 300]
        } else {
            vec![b's'; 20]
        };
        t.mutate(format!("r{i:04}").as_bytes())
            .put_at("f", b"q", 1_760_000_000_000_000 + i, &v)
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    db.close().unwrap();
}

/// Run by the child process: commits about 700 KiB of log in 256 KiB WAL segments, with no
/// flush, then exits without closing, leaving the sidecar as a crash would.
#[test]
fn crash_child() {
    let Ok(path) = std::env::var(CHILD) else {
        return;
    };
    let db = Pigeonhole::open(
        &path,
        Options::default()
            .shards(1)
            .pin_threads(false)
            .wal_segment_size(256 << 10)
            // A process crash keeps what was handed to the kernel; an fsync per commit
            // (the default) adds nothing a process crash shows and costs minutes on
            // Windows CI.
            .durability(Durability::Buffered),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    let v = vec![b'v'; 200];
    for i in 0..3000u32 {
        t.mutate(format!("w{i:05}").as_bytes())
            .put("f", b"q", &v)
            .commit()
            .unwrap();
    }
    std::process::exit(0);
}

/// A crashed database: `crash_child` run in a child process (once per test binary, each
/// caller getting a copy).
fn crashed(dir: &TempDir) -> PathBuf {
    static TEMPLATE: OnceLock<PathBuf> = OnceLock::new();
    fixture(
        &TEMPLATE,
        "crashed.phdb",
        |path| {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "crash_child", "--nocapture", "--test-threads=1"])
                .env(CHILD, path)
                .status()
                .unwrap();
            assert!(status.success());
        },
        dir,
    )
}

fn check_json(args: &[&str]) -> (u8, Value) {
    let out = phdb(&[&["--format", "json", "check"][..], args].concat());
    assert!(out.stdout.starts_with('{'), "{out:?}");
    (out.status, serde_json::from_str(&out.stdout).unwrap())
}

fn map(path: &Path) -> Value {
    let (status, doc) = check_json(&["--map", path.to_str().unwrap()]);
    assert_eq!(status, 0, "{doc:#}");
    doc
}

fn region(doc: &Value, kind: &str) -> Value {
    doc["regions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["kind"] == kind)
        .unwrap_or_else(|| panic!("no {kind} region: {doc:#}"))
        .clone()
}

fn flip(path: &Path, offset: u64) {
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut b = [0u8];
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.read_exact(&mut b).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(&[b[0] ^ 0xff]).unwrap();
    f.sync_all().unwrap();
}

/// Asserts `phdb check` exits 3 with a corruption finding about `structure`.
fn corrupt_in(path: &Path, structure: &str) {
    let (status, doc) = check_json(&[path.to_str().unwrap()]);
    assert_eq!(status, 3, "{doc:#}");
    assert_eq!(doc["result"], "corrupt");
    assert!(
        doc["findings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["structure"] == structure && f["severity"] == "corruption"),
        "no {structure} corruption: {doc:#}"
    );
    // The human output exits the same way.
    assert_eq!(phdb(&["check", path.to_str().unwrap()]).status, 3);
}

fn u(v: &Value) -> u64 {
    v.as_u64().unwrap()
}

#[test]
fn healthy_table() {
    let dir = TempDir::new("check-table");
    let path = sample(&dir);
    let out = phdb(&["check", path.to_str().unwrap()]);
    assert_eq!(out.status, 0, "{out:?}");
    common::snapshot("check_table", &dir, &out.stdout);
}

#[test]
fn healthy_json() {
    let dir = TempDir::new("check-json");
    let path = sample(&dir);
    let (status, doc) = check_json(&["--deep", path.to_str().unwrap()]);
    assert_eq!(status, 0, "{doc:#}");
    assert_eq!(doc["phdb"], 1);
    assert_eq!(doc["result"], "ok");
    assert_eq!(doc["sst_entries"], 200);
    assert_eq!(doc["blob_pointers"], 20);
    assert!(doc.get("regions").is_none(), "regions only with --map");
    let out = phdb(&["--format", "json", "check", path.to_str().unwrap()]);
    common::snapshot("check_json", &dir, &out.stdout);
}

#[test]
fn the_map_lists_every_region() {
    let dir = TempDir::new("check-map");
    let path = sample(&dir);
    let doc = map(&path);
    let kinds: Vec<_> = doc["regions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["kind"].as_str().unwrap().to_owned())
        .collect();
    for k in ["superblock", "manifest-snapshot", "sst", "blob"] {
        assert!(kinds.iter().any(|x| x == k), "{k}: {kinds:?}");
    }
    let sst = region(&doc, "sst");
    assert_eq!(
        (sst["table"].clone(), sst["family"].clone()),
        ("t".into(), "f".into())
    );
    // CSV and table output list the regions as rows.
    let out = phdb(&["--format", "csv", "check", "--map", path.to_str().unwrap()]);
    assert!(out.stdout.starts_with("kind,offset,len,"), "{out:?}");
    assert!(out.stdout.lines().any(|l| l.starts_with("sst,")), "{out:?}");
}

#[test]
fn a_damaged_superblock_slot_exits_3() {
    let dir = TempDir::new("check-superblock");
    let path = sample(&dir);
    flip(&path, 4096 + 16);
    corrupt_in(&path, "superblock");
}

#[test]
fn a_damaged_manifest_exits_3() {
    let dir = TempDir::new("check-manifest");
    let path = sample(&dir);
    let m = region(&map(&path), "manifest-snapshot");
    flip(&path, u(&m["offset"]) + 80);
    corrupt_in(&path, "manifest");
}

#[test]
fn a_damaged_sst_block_exits_3() {
    let dir = TempDir::new("check-sst");
    let path = sample(&dir);
    let s = region(&map(&path), "sst");
    flip(&path, u(&s["offset"]) + 20);
    corrupt_in(&path, "sst");
}

#[test]
fn a_damaged_sst_footer_exits_3() {
    let dir = TempDir::new("check-footer");
    let path = sample(&dir);
    let s = region(&map(&path), "sst");
    flip(&path, u(&s["offset"]) + u(&s["used"]) - 30);
    corrupt_in(&path, "sst");
}

#[test]
fn a_damaged_blob_record_exits_3() {
    let dir = TempDir::new("check-blob");
    let path = sample(&dir);
    let b = region(&map(&path), "blob");
    flip(&path, u(&b["offset"]) + 64 + 16 + 5);
    corrupt_in(&path, "blob");
}

#[test]
fn a_damaged_blob_extent_header_exits_3() {
    let dir = TempDir::new("check-blob-header");
    let path = sample(&dir);
    let b = region(&map(&path), "blob");
    flip(&path, u(&b["offset"]) + 3);
    corrupt_in(&path, "blob");
}

#[test]
fn a_truncated_file_exits_3() {
    let dir = TempDir::new("check-truncated");
    let path = sample(&dir);
    let doc = map(&path);
    let end = doc["regions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| u(&r["offset"]) + u(&r["len"]))
        .max()
        .unwrap();
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(end - 64 * 1024).unwrap();
    f.sync_all().unwrap();
    drop(f);
    corrupt_in(&path, "allocation");
}

#[test]
fn a_crashed_database_checks_clean_and_damage_inside_its_wal_exits_3() {
    let dir = TempDir::new("check-wal");
    let path = crashed(&dir);
    let (status, doc) = check_json(&[path.to_str().unwrap()]);
    assert_eq!(status, 0, "{doc:#}");
    assert_eq!(doc["clean_shutdown"], false);
    let w = doc["wal"][0].clone();
    assert!(u(&w["segments"]) >= 3, "{w:#}");
    assert!(u(&doc["wal_records"]) >= 3000, "{doc:#}");
    // Damage the first record replay reads: in the checkpoint's segment, which has a
    // successor, so the loss is provable (its successor continues past the damage).
    let seg = u(&w["segment_size"]);
    let wal = PathBuf::from(w["path"].as_str().unwrap());
    let checkpoint = u(&w["checkpoint"]["offset"]);
    let mut slot = None;
    for s in 0..u(&w["len"]) / seg {
        if Some(s) == w["end_slot"].as_u64() {
            continue;
        }
        let mut h = [0u8; 12];
        let mut f = std::fs::File::open(&wal).unwrap();
        f.seek(SeekFrom::Start(s * seg + checkpoint.max(32 * 1024)))
            .unwrap();
        f.read_exact(&mut h).unwrap();
        if h[10] == 1 || h[10] == 2 {
            slot = Some(s);
            break;
        }
    }
    let slot = slot.expect("a segment before the last with records");
    flip(&wal, slot * seg + checkpoint.max(32 * 1024) + 12 + 4);
    corrupt_in(&path, "wal");
}

#[test]
fn a_running_writer_needs_online() {
    let dir = TempDir::new("check-online");
    let path = sample(&dir);
    let db = Pigeonhole::open(&path, Options::default().shards(1).pin_threads(false)).unwrap();
    let out = phdb(&["check", path.to_str().unwrap()]);
    assert_eq!(out.status, 4, "{out:?}");
    assert!(out.stderr.contains("--online"), "{out:?}");
    let (status, doc) = check_json(&["--online", path.to_str().unwrap()]);
    assert_eq!(status, 0, "{doc:#}");
    assert_eq!(doc["mode"], "online");
    assert_eq!(doc["rows"], 200);
    db.close().unwrap();
    // With no writer, --online runs the offline check.
    let (status, doc) = check_json(&["--online", path.to_str().unwrap()]);
    assert_eq!((status, doc["mode"].clone()), (0, "offline".into()));
}

#[test]
fn a_missing_file_exits_5() {
    let dir = TempDir::new("check-missing");
    let out = phdb(&["check", dir.path().join("nope.phdb").to_str().unwrap()]);
    assert_eq!(out.status, 5);
}

#[test]
fn a_damaged_report_lists_its_findings() {
    let dir = TempDir::new("check-findings");
    let path = sample(&dir);
    let b = region(&map(&path), "blob");
    flip(&path, u(&b["offset"]) + 64 + 16 + 5);
    let out = phdb(&["check", path.to_str().unwrap()]);
    assert_eq!(out.status, 3, "{out:?}");
    common::snapshot("check_damaged_table", &dir, &out.stdout);
    let out = phdb(&["--format", "jsonl", "check", path.to_str().unwrap()]);
    assert_eq!(out.status, 3, "{out:?}");
    common::snapshot("check_damaged_jsonl", &dir, &out.stdout);
}

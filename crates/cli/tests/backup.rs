//! `phdb backup` and `phdb compact` (#692): a backup holds what the database holds and
//! checks clean; a compaction keeps every cell and merges the SSTs; the refusals.

mod common;

use std::path::{Path, PathBuf};

use common::{TempDir, phdb};
use pigeonhole::{Family, Options, Pigeonhole};
use serde_json::Value;

const T0: u64 = 1_760_000_000_000_000;

fn options() -> Options {
    Options::default().shards(1).pin_threads(false)
}

/// A database with overwritten and deleted cells over several flushes (so several SSTs),
/// closed cleanly.
fn sample(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("src.phdb");
    let db = Pigeonhole::open(&path, options()).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default().max_versions(1))
        .family("big", Family::default().blob_threshold(100))
        .create()
        .unwrap();
    for round in 0..4u64 {
        for i in 0..100u64 {
            t.mutate(format!("r{i:03}").as_bytes())
                .put_at("f", b"q", T0 + round, format!("v{round}").as_bytes())
                .commit()
                .unwrap();
        }
        if round == 2 {
            for i in 0..20u64 {
                t.mutate(format!("r{i:03}").as_bytes())
                    .delete_row()
                    .commit()
                    .unwrap();
            }
        }
        t.mutate(format!("blob{round}").as_bytes())
            .put_at("big", b"v", T0, &[b'x'; 400])
            .commit()
            .unwrap();
        db.flush().unwrap();
    }
    db.close().unwrap();
    path
}

fn json(args: &[&str]) -> (u8, Value, String) {
    let out = phdb(&[&["--format", "json"][..], args].concat());
    let doc = if out.stdout.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&out.stdout).unwrap()
    };
    (out.status, doc, out.stderr)
}

/// The dump of `path` without its header line.
fn cells(path: &Path) -> Vec<String> {
    let out = phdb(&["dump", path.to_str().unwrap()]);
    assert_eq!(out.status, 0, "{out:?}");
    out.stdout.lines().skip(1).map(str::to_owned).collect()
}

/// Per family, the levels holding SSTs and the entries in them (from `phdb info`).
fn layout(path: &Path) -> Vec<(usize, u64)> {
    let (status, doc, _) = json(&["info", path.to_str().unwrap()]);
    assert_eq!(status, 0);
    doc["tables"][0]["families"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            let levels = f["levels"].as_array().unwrap().len();
            (levels, f["entries"].as_u64().unwrap())
        })
        .collect()
}

#[test]
fn a_backup_holds_the_same_cells_and_checks_clean() {
    let dir = TempDir::new("backup");
    let src = sample(&dir);
    for (name, latest) in [("durable.phdb", false), ("latest.phdb", true)] {
        let dest = dir.path().join(name);
        let mut args = vec![
            "backup",
            "--check",
            src.to_str().unwrap(),
            dest.to_str().unwrap(),
        ];
        if latest {
            args.push("--latest");
        }
        let (status, doc, _) = json(&args);
        assert_eq!(status, 0, "{doc:#}");
        assert_eq!(doc["snapshot"], if latest { "latest" } else { "durable" });
        assert_eq!(doc["check"], "ok");
        assert!(doc["bytes"].as_u64().unwrap() > 0);
        assert_eq!(cells(&src), cells(&dest), "{name}");
    }
}

#[test]
fn backup_refusals_and_a_live_writer() {
    let dir = TempDir::new("backup-refusals");
    let src = sample(&dir);
    let taken = dir.path().join("taken.phdb");
    std::fs::write(&taken, b"x").unwrap();
    let (status, _, err) = json(&["backup", src.to_str().unwrap(), taken.to_str().unwrap()]);
    assert_eq!(status, 1, "{err}");
    assert!(err.contains("BadInput") && err.contains("exists"), "{err}");

    let missing = dir.path().join("missing.phdb");
    let dest = dir.path().join("dest.phdb");
    let (status, _, _) = json(&["backup", missing.to_str().unwrap(), dest.to_str().unwrap()]);
    assert_eq!(status, 5);

    // With the writer running, the backup comes from a reader process (ICR 0049).
    let db = Pigeonhole::open(&src, options()).unwrap();
    let (status, doc, err) = json(&["backup", src.to_str().unwrap(), dest.to_str().unwrap()]);
    assert_eq!(status, 0, "{err}");
    assert_eq!(doc["mode"], "reader");
    db.close().unwrap();
    assert_eq!(cells(&src), cells(&dest));
}

#[test]
fn a_compaction_keeps_every_cell_and_merges_the_ssts() {
    let dir = TempDir::new("compact");
    let src = sample(&dir);
    let before_cells = cells(&src);
    let before = layout(&src);
    let (status, doc, _) = json(&["compact", "--shrink", src.to_str().unwrap()]);
    assert_eq!(status, 0, "{doc:#}");
    assert!(doc["compactions"].as_u64().unwrap() >= 1, "{doc:#}");
    assert!(doc["released_bytes"].is_u64(), "{doc:#}");
    assert_eq!(cells(&src), before_cells);
    // Every family's SSTs now sit in one level, with no more entries than before (what
    // the merge purges is the engine's policy, tested there).
    let after = layout(&src);
    assert!(after.iter().all(|f| f.0 <= 1), "{before:?} -> {after:?}");
    let entries = |l: &[(usize, u64)]| l.iter().map(|f| f.1).sum::<u64>();
    assert!(
        entries(&after) <= entries(&before),
        "{before:?} -> {after:?}"
    );
    assert!(phdb(&["check", src.to_str().unwrap()]).status == 0);

    // Without --shrink nothing is released; table output is the summary.
    let out = phdb(&["compact", src.to_str().unwrap()]);
    assert_eq!(out.status, 0, "{out:?}");
    assert!(out.stdout.contains("compactions"), "{out:?}");
    assert!(
        out.stdout
            .lines()
            .any(|l| l.starts_with("released_bytes") && l.ends_with('-')),
        "{out:?}"
    );
}

#[test]
fn compact_refusals() {
    let dir = TempDir::new("compact-refusals");
    let src = sample(&dir);
    let (status, _, _) = json(&["compact", dir.path().join("nope.phdb").to_str().unwrap()]);
    assert_eq!(status, 5);
    let db = Pigeonhole::open(&src, options()).unwrap();
    let (status, _, err) = json(&["compact", src.to_str().unwrap()]);
    assert_eq!(status, 4, "{err}");
    db.close().unwrap();
}

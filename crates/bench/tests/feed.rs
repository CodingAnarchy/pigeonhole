//! D310 PR 5b: `--feed` end to end through the real binary. A ycsb-a run opens with the
//! change feed on and follows the bench table's feed from a durable and a visible thread and
//! a reader process beside its measured phase; the record reports their deliveries and the
//! feed's syncs.

use std::process::Command;

fn run(name: &str, args: &[&str]) -> serde_json::Value {
    let dir = std::env::temp_dir().join(format!("phdb-bench-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let json = dir.join("out.json");
    let out = Command::new(env!("CARGO_BIN_EXE_phdb-bench"))
        .args([
            "ycsb-a",
            "--scale",
            "smoke",
            "--threads",
            "2",
            "--min-secs",
            "2",
        ])
        .args(args)
        .arg("--dir")
        .arg(&dir)
        .arg("--json")
        .arg(&json)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let suite: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&json).unwrap()).unwrap();
    std::fs::remove_dir_all(&dir).ok();
    suite["results"][0].clone()
}

#[test]
fn a_run_reports_its_feed_subscribers_and_the_feed_s_syncs() {
    let rec = run(
        "feed",
        &[
            "--feed",
            "64",
            "--feed-subscribers",
            "1",
            "--feed-visible-subscribers",
            "1",
            "--reader-feed-processes",
            "1",
        ],
    );
    assert!(
        rec["store_config"]
            .as_str()
            .unwrap()
            .contains(" feed=64MiB subs=1/1/1"),
        "{}",
        rec["store_config"]
    );
    let f = &rec["detail"]["feed"];
    assert_eq!(
        (
            &f["durable_subscribers"],
            &f["visible_subscribers"],
            &f["reader_subscribers"]
        ),
        (&1.into(), &1.into(), &1.into()),
        "{f}"
    );
    // Buffered commits: every subscriber is delivered the phase's commits.
    for kind in ["durable_delivery", "visible_delivery", "reader_delivery"] {
        assert!(f[kind]["count"].as_u64().unwrap() > 100, "{kind}: {f}");
    }
    // The visible frontier never trails the durable one.
    assert!(
        f["visible_delivery"]["p50_ns"].as_u64() <= f["durable_delivery"]["p50_ns"].as_u64(),
        "{f}"
    );
    assert_eq!(
        (f["expired"].as_u64(), f["errors"].as_u64()),
        (Some(0), Some(0)),
        "{f}"
    );
    // Buffered commits on a durable feed: the feed's own syncs ran, and no group sync.
    assert!(f["background_syncs"].as_u64().unwrap() >= 1, "{f}");
    assert_eq!(f["group_syncs"].as_u64(), Some(0), "{f}");
    assert_eq!(f["retained_taken"].as_u64(), Some(0), "{f}");
    // Sampled; 0 until a segment the checkpoint passed is kept, which a smoke run may not reach.
    assert!(f["retained_wal_peak"].is_u64(), "{f}");
}

#[test]
fn without_feed_the_record_has_no_feed_section_and_none_commits_run() {
    let rec = run("nofeed", &["--durability", "none"]);
    assert!(
        rec["store_config"].as_str().unwrap().contains(" none"),
        "{}",
        rec["store_config"]
    );
    assert!(rec["detail"]["feed"].is_null(), "{}", rec["detail"]);
}

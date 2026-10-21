//! #683 (D247 item K): `--reader-processes` end to end through the real binary. A ycsb-a run
//! spawns two `phdb-bench reader` processes beside its measured phase; they attach through
//! `Pigeonhole::open_reader`, read ycsb rows, and the record reports their reads.

use std::process::Command;

#[test]
fn a_run_reports_its_reader_processes() {
    let dir = std::env::temp_dir().join(format!("phdb-bench-readers-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let json = dir.join("out.json");
    let out = Command::new(env!("CARGO_BIN_EXE_phdb-bench"))
        .args(["ycsb-a", "--scale", "smoke", "--threads", "2"])
        .args(["--reader-processes", "2", "--min-secs", "2"])
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
    let rec = &suite["results"][0];
    assert!(
        rec["store_config"].as_str().unwrap().contains(" readers=2"),
        "{}",
        rec["store_config"]
    );
    let r = &rec["detail"]["reader_processes"];
    assert_eq!(r["processes"], 2, "{r}");
    assert!(r["reads"]["count"].as_u64().unwrap() > 1_000, "{r}");
    assert!(r["reads_per_sec"].as_f64().unwrap() > 0.0, "{r}");
    let (p50, p99) = (r["reads"]["p50_ns"].as_u64(), r["reads"]["p99_ns"].as_u64());
    assert!(p50 <= p99, "{r}");
    // The writer saw both processes attached (#842's metrics, sampled every 100 ms).
    assert_eq!(r["max_slots_active"], 2, "{r}");
    assert!(
        r["max_view_lag"].is_u64() && r["max_seqno_lag"].is_u64(),
        "{r}"
    );
    // Every row is loaded and the writer stays up: no misses, no errors.
    assert_eq!(
        (r["misses"].as_u64(), r["errors"].as_u64()),
        (Some(0), Some(0)),
        "{r}"
    );
    // The writer's own run is unaffected in shape: it measured its operations.
    assert!(rec["operations"].as_u64().unwrap() > 0);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn reader_needs_its_arguments() {
    let out = Command::new(env!("CARGO_BIN_EXE_phdb-bench"))
        .args(["reader", "--records", "10"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--path"));
}

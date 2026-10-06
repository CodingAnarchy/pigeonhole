//! Runs every example as a test, so `cargo test` keeps them working.

#[path = "../examples/adjacency_scan.rs"]
mod adjacency_scan;
#[path = "../examples/counters.rs"]
mod counters;
#[path = "../examples/quickstart.rs"]
mod quickstart;
#[path = "../examples/time_series_ttl.rs"]
mod time_series_ttl;

#[test]
fn quickstart_runs() {
    quickstart::main().expect("quickstart");
}

#[test]
fn time_series_ttl_runs() {
    time_series_ttl::main().expect("time_series_ttl");
}

#[test]
fn adjacency_scan_runs() {
    adjacency_scan::main().expect("adjacency_scan");
}

#[test]
fn counters_runs() {
    counters::main().expect("counters");
}

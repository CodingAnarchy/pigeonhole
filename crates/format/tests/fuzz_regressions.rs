//! Inputs the fuzz targets in `crates/format/fuzz` once crashed on, replayed through every
//! harness. Each file in `tests/fuzz-regressions` is named after its issue.

mod common;

use common::harness;

#[test]
fn fuzz_reproducers_no_longer_panic() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fuzz-regressions");
    let mut n = 0;
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        let data = std::fs::read(&path).unwrap();
        eprintln!("{}", path.display());
        harness::all(&data);
        n += 1;
    }
    assert!(n > 0, "no reproducers in {}", dir.display());
}

//! `Suite::to_markdown` on committed gate results, pinned byte for byte against
//! `tests/golden/suite-markdown.md`. The inputs between them reach every section: the
//! per-result table, reads, operation types, cross-shard rounds, shard spread, stalls, and
//! the scaling (pinned, unpinned, sync), placement and group-commit (pinned, unpinned)
//! verdicts. After an intended change to the report, rewrite the golden file with
//! `BLESS=1 cargo test -p pigeonhole-bench --test markdown` and review its diff.
use std::path::Path;

use pigeonhole_bench::Suite;

const INPUTS: &[&str] = &[
    "2026-10-21/all-uring-1.json",
    "2026-10-21/contended-buffered-cross-shard-contention.json",
    "2026-10-13/scaling-4.json",
    "2026-10-11/scaling-4.json",
    "2026-10-13/placement-8.json",
    "2026-10-13/all-uring-1-run1.json",
];

#[test]
fn suite_markdown_matches_the_golden_file() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let results = root.join("baselines/phase3-gate/results");
    let mut got = String::new();
    for input in INPUTS {
        let json = std::fs::read_to_string(results.join(input)).unwrap();
        let suite = Suite::from_json(&json).unwrap();
        got.push_str(&format!("<!-- {input} -->\n{}\n", suite.to_markdown()));
    }
    let golden = root.join("tests/golden/suite-markdown.md");
    if std::env::var_os("BLESS").is_some() {
        std::fs::write(&golden, &got).unwrap();
        return;
    }
    let want = std::fs::read_to_string(&golden).unwrap();
    if got != want {
        let line = got.lines().zip(want.lines()).position(|(g, w)| g != w);
        panic!(
            "Suite::to_markdown differs from {} (first differing line: {line:?})",
            golden.display()
        );
    }
}

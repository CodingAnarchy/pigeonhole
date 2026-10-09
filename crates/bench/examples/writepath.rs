//! A write-path microbenchmark (#287): 1-cell and 16-cell commits of 100-byte values,
//! then a flush and a full compaction of what they wrote. Each phase is a function of its
//! own (`writepath_commit_one`, `writepath_commit_sixteen`, `writepath_flush`,
//! `writepath_compact`) so callgrind can count it alone (`--toggle-collect`), and the
//! number of ops or entries it handled goes to stderr as `ops <phase> <n>`. On macOS the
//! process's retired instructions per op are printed as well (they include the kernel and
//! the engine's other threads, so they are only a local guide).
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example writepath -- [OPS] [DIR]
//! ```
//!
//! `OPS` 1-cell commits and `OPS / 4` 16-cell commits (default 20,000: the memtable holds
//! them, so no flush runs during the commits). `WRITE_PHASE=one|sixteen` runs only that
//! commit phase; `WRITE_PHASE=loop` repeats commits, a flush and a compaction until
//! killed, to profile with `sample` or `perf`.
use pigeonhole::{Durability, Family, Options, Pigeonhole, Table};

fn main() {
    let ops: usize = std::env::args()
        .nth(1)
        .map_or(20_000, |s| s.parse().expect("OPS"));
    let base = std::env::args()
        .nth(2)
        .map_or_else(std::env::temp_dir, std::path::PathBuf::from);
    let phase = std::env::var("WRITE_PHASE").ok();
    let dir = base.join(format!("phdb-writepath-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    let db = Pigeonhole::open(
        dir.join("w.phdb"),
        Options::default()
            .durability(Durability::Buffered)
            .memtable_budget(1 << 30)
            .block_cache(256 << 20),
    )
    .expect("open");
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    // Keys are formatted up front, outside the measured functions.
    let quals: Vec<Vec<u8>> = (0..16).map(|q| format!("q{q:02}").into_bytes()).collect();
    let rows = |prefix: &str, n: usize| -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| format!("{prefix}:{i:010}").into_bytes())
            .collect()
    };
    if phase.as_deref() == Some("loop") {
        let mut round = 0u64;
        loop {
            writepath_commit_sixteen(&t, &rows(&format!("r{round}"), ops), &quals);
            writepath_flush(&db);
            writepath_compact(&db);
            round += 1;
        }
    }
    let mut entries = 0;
    if phase.as_deref().is_none_or(|p| p == "one") {
        let rows = rows("one", ops);
        measure("commit_one", ops, || writepath_commit_one(&t, &rows));
        entries += ops;
    }
    if phase.as_deref().is_none_or(|p| p == "sixteen") {
        let rows = rows("six", ops / 4);
        measure("commit_sixteen", ops / 4, || {
            writepath_commit_sixteen(&t, &rows, &quals)
        });
        entries += ops / 4 * 16;
    }
    if phase.is_none() {
        measure("flush", entries, || writepath_flush(&db));
        measure("compact", entries, || writepath_compact(&db));
    }
    drop(t);
    db.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

const VALUE: [u8; 100] = [7; 100];

#[inline(never)]
fn writepath_commit_one(t: &Table, rows: &[Vec<u8>]) {
    for row in rows {
        t.mutate(row).put("f", b"q", &VALUE).commit().unwrap();
    }
}

#[inline(never)]
fn writepath_commit_sixteen(t: &Table, rows: &[Vec<u8>], quals: &[Vec<u8>]) {
    for row in rows {
        let mut m = t.mutate(row);
        for q in quals {
            m = m.put("f", q, &VALUE);
        }
        m.commit().unwrap();
    }
}

#[inline(never)]
fn writepath_flush(db: &Pigeonhole) {
    db.flush().unwrap();
}

#[inline(never)]
fn writepath_compact(db: &Pigeonhole) {
    db.compact().unwrap();
}

/// Runs `f`, reports its op count on stderr and, on macOS, the instructions per op.
fn measure(phase: &str, n: usize, f: impl FnOnce()) {
    let before = instructions();
    f();
    let after = instructions();
    eprintln!("ops {phase} {n}");
    if let (Some(a), Some(b)) = (before, after) {
        println!(
            "{phase}: {:.0} instructions per op",
            (b - a) as f64 / n as f64
        );
    }
}

/// Instructions retired by the whole process (`proc_pid_rusage`, `ri_instructions`).
#[cfg(target_os = "macos")]
fn instructions() -> Option<u64> {
    unsafe extern "C" {
        fn proc_pid_rusage(pid: i32, flavor: i32, buffer: *mut u64) -> i32;
    }
    // RUSAGE_INFO_V4: a 16-byte uuid, then u64 fields; `ri_instructions` is the 30th.
    const RUSAGE_INFO_V4: i32 = 4;
    let mut b = [0u64; 64];
    // SAFETY: the buffer is larger than `rusage_info_v4` (and suitably aligned).
    let r = unsafe { proc_pid_rusage(std::process::id() as i32, RUSAGE_INFO_V4, b.as_mut_ptr()) };
    (r == 0).then_some(b[2 + 29])
}

#[cfg(not(target_os = "macos"))]
fn instructions() -> Option<u64> {
    None
}

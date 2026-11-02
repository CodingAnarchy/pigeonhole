//! D198: the spin before parking stays bounded. Engine-owned shards spin only after handling
//! a message, and polls that find nothing back off, so an idle database takes no CPU and a
//! sparse client pays little for the spin windows.
//!
//! Its own test binary, so no other test's threads add to the process's CPU time.

#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::time::{Duration, Instant};

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

/// This process's user plus system CPU time, in nanoseconds (`CLOCK_PROCESS_CPUTIME_ID`). The
/// 10 ms ticks of `/proc/self/stat` made one tick the whole idle budget: 2 ticks of runner
/// noise in the idle second failed the test (#1331).
fn process_cpu() -> Duration {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    Duration::new(t.tv_sec as u64, t.tv_nsec as u32)
}

fn cpu_share(f: impl FnOnce()) -> f64 {
    let (cpu0, wall0) = (process_cpu(), Instant::now());
    f();
    (process_cpu() - cpu0).as_secs_f64() / wall0.elapsed().as_secs_f64()
}

#[test]
fn spinning_stops_when_the_database_goes_idle() {
    let dir = std::env::temp_dir().join(format!("phdb-idle-spin-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    o.create_if_missing = true;
    o.shards = 2;
    o.durability = Durability::Buffered;
    // D198's windows are the defaults (the commit window from D253, the shard window from
    // D198's 2026-10-19 amendment).
    assert_eq!((o.commit_spin_nanos, o.shard_spin_nanos), (50_000, 200_000));
    let db = Engine::open(&dir.join("idle.phdb"), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let commit = |i: u32| {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            t.families[0].id,
            format!("r{i:06}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(b"v"),
        )
        .unwrap();
        db.commit(wb, None).unwrap();
    };
    // Busy: the spin is what it is for.
    for i in 0..2_000 {
        commit(i);
    }
    // Idle: no shard and no client keeps polling.
    std::thread::sleep(Duration::from_millis(200));
    // The better of two windows: a spin shows in both, while one background job (a flush,
    // a spare segment's preparation) landing in one of them doesn't decide it (#1331).
    let idle = (0..2)
        .map(|_| cpu_share(|| std::thread::sleep(Duration::from_millis(1_000))))
        .fold(f64::INFINITY, f64::min);
    assert!(idle < 0.01, "{:.2}% of a core while idle", idle * 100.0);
    // Sparse: a commit every 2 ms; polls that find nothing back off.
    let sparse = cpu_share(|| {
        for i in 0..500 {
            commit(10_000 + i);
            std::thread::sleep(Duration::from_millis(2));
        }
    });
    assert!(
        sparse < 0.10,
        "{:.1}% of a core for a commit every 2 ms",
        sparse * 100.0
    );
    db.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

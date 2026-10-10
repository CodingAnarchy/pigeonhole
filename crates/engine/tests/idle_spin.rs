//! D198: the spin before parking stays bounded. Engine-owned shards spin only after handling
//! a message, and polls that find nothing back off, so an idle database takes no CPU and a
//! sparse client pays little for the spin windows.
//!
//! Its own test binary, so no other test's threads add to the process's CPU time.

#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::time::{Duration, Instant};

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

/// This process's user plus system CPU time.
#[cfg(target_os = "linux")]
fn process_cpu() -> Duration {
    // Fields 14 and 15 of /proc/self/stat (after the parenthesized command name), in clock
    // ticks of 1/100 s.
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let rest = &stat[stat.rfind(')').unwrap() + 2..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let ticks: u64 = fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
    Duration::from_millis(ticks * 10)
}

/// This process's user plus system CPU time.
#[cfg(target_os = "macos")]
fn process_cpu() -> Duration {
    // `ps` prints `[[hh:]mm:]ss.cc`.
    let out = std::process::Command::new("ps")
        .args(["-o", "time=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    let text = String::from_utf8(out.stdout).unwrap();
    let mut secs = 0.0f64;
    for part in text.trim().split(':') {
        secs = secs * 60.0 + part.parse::<f64>().unwrap();
    }
    Duration::from_secs_f64(secs)
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
    // D198's windows are the defaults.
    assert_eq!((o.commit_spin_nanos, o.shard_spin_nanos), (15_000, 50_000));
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
    let idle = cpu_share(|| std::thread::sleep(Duration::from_millis(1_000)));
    assert!(idle < 0.01, "{:.1}% of a core while idle", idle * 100.0);
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

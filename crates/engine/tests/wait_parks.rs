//! Issue #135 (1-2 F1): a blocking commit wait whose seqno another shard's in-flight group
//! holds below the global watermark parks instead of spinning, and wakes when it publishes.
//!
//! Its own test binary, so no other test's threads add to the process's CPU time.

#![cfg(any(target_os = "linux", target_os = "macos"))]

mod gate;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, TableInfo, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

const DB: &str = "/db/park.phdb";

fn wal(stream: u32) -> PathBuf {
    pigeonhole_wal::stream_path(Path::new(DB), pigeonhole_format::StreamId(stream))
}

fn put(t: &TableInfo, row: &str) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row.as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    wb
}

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

#[test]
fn a_commit_wait_held_by_another_shards_group_parks() {
    let (vfs, gate) = gate::vfs(1357);
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.memtable_budget = 4 << 20;
    o.wal.segment_size = 256 << 10;
    o.wal.spare_segments = 1;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let mut tables = Vec::new();
    for name in ["a", "b"] {
        let t = db
            .create_table(name, &[("f".into(), FamilyOptions::default())])
            .unwrap();
        db.record_history(true);
        db.take_appended();
        db.commit(put(&t, "probe"), Some(Durability::Buffered))
            .unwrap();
        let shard = u32::from(db.take_appended().last().expect("one record").stream);
        tables.push((t, shard));
    }
    let [(a, sa), (b, sb)] = <[_; 2]>::try_from(tables).unwrap();
    assert_ne!(sa, sb, "one table per shard");
    // Shard B's `Sync` group stays in flight: the global watermark stays below it.
    gate.hold(&wal(sb));
    let held = db.submit(put(&b, "held"), Some(Durability::Sync)).unwrap();
    gate.wait_held(&wal(sb));
    // A `None` commit on shard A resolves at once, then waits for visibility.
    let (db2, a2) = (Arc::clone(&db), Arc::clone(&a));
    let waiter = std::thread::spawn(move || {
        let r = db2.commit(put(&a2, "waits"), Some(Durability::None));
        (r, Instant::now())
    });
    std::thread::sleep(Duration::from_millis(200));
    assert!(!waiter.is_finished(), "visible before shard B published");
    let (cpu0, wall0) = (process_cpu(), Instant::now());
    std::thread::sleep(Duration::from_millis(1_500));
    let (cpu, wall) = (process_cpu() - cpu0, wall0.elapsed());
    assert!(!waiter.is_finished(), "visible before shard B published");
    assert!(
        cpu.as_secs_f64() < wall.as_secs_f64() * 0.1,
        "{cpu:?} of CPU in {wall:?} of waiting for visibility (issue #135)"
    );
    // Shard B's sync completes: the waiter wakes promptly.
    let released = Instant::now();
    gate.release();
    let (r, woke) = waiter.join().unwrap();
    r.unwrap();
    assert!(
        woke.duration_since(released) < Duration::from_secs(1),
        "woke {:?} after the publish",
        woke.duration_since(released)
    );
    held.wait().unwrap();
    db.close().unwrap();
}

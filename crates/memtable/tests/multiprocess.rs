//! A reader in a second process traverses the memtable through its own mapping of the
//! shared region. The test binary re-executes itself as the child (the pattern of
//! `pigeonhole-io`'s `tests/pread.rs`), which attaches to the region by name, waits for the
//! writer's entries to appear, and verifies them. Nothing in the arena can be an address:
//! the child maps the region wherever its address space allows.
#![cfg(not(miri))]

mod common;

use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pigeonhole_io::pread::PreadVfs;
use pigeonhole_io::{SharedOpen, Vfs};
use pigeonhole_memtable::{ArenaRegion, Memtable, MemtableReader, ShardArena};

use common::Rng;

const CHILD_ENV: &str = "PIGEONHOLE_MEMTABLE_CHILD";
const LEN: usize = 4 << 20;
const CHUNK: usize = 64 * 1024;
const ENTRIES: usize = 5000;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A region name unique to this process and call (macOS limits names to 31 bytes).
fn unique() -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("phmt{}x{n}", std::process::id())
}

fn sorted_entries(seed: u64) -> Vec<common::Entry> {
    let mut entries = common::entries(seed, ENTRIES);
    entries.sort();
    entries
}

/// Runs in the child process; an empty test without the environment variable.
#[test]
fn reader_process_child() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let mut parts = spec.split('|');
    let name = parts.next().unwrap();
    let root: u32 = parts.next().unwrap().parse().unwrap();
    let seed: u64 = parts.next().unwrap().parse().unwrap();

    let vfs = PreadVfs::new(1);
    let region = vfs
        .open_shared(name, None, LEN as u64, SharedOpen::Attach)
        .expect("attach to the writer's region");
    let arena = ArenaRegion::new(region, 0, LEN).unwrap();
    let reader = MemtableReader::open(arena, root).expect("open the published root");

    // The writer may still be inserting: watch the count grow, scanning sorted prefixes
    // meanwhile, until every entry is published.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = 0;
    loop {
        let n = reader.len();
        if n != last {
            let seen = common::scan(&reader);
            assert!(seen.len() >= n, "count {n} but scanned {}", seen.len());
            assert!(seen.windows(2).all(|w| w[0].0 < w[1].0));
            last = n;
        }
        if n >= ENTRIES {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the writer"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let expected = sorted_entries(seed);
    common::verify(&reader, &expected, &mut Rng::new(seed ^ 0xC0FFEE));
    println!("child verified {ENTRIES} entries");
}

#[test]
fn reader_process_traverses_the_memtable() {
    let seed = common::seed();
    let name = unique();
    let vfs = PreadVfs::new(1);
    let region = vfs
        .open_shared(&name, None, LEN as u64, SharedOpen::CreateNew)
        .unwrap();
    let mut arena = ShardArena::new(ArenaRegion::new(region.clone(), 0, LEN).unwrap(), CHUNK);
    let mut mt = Memtable::create(&mut arena).unwrap();

    // Start the reader first, so it observes the memtable growing.
    let child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "reader_process_child",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(CHILD_ENV, format!("{name}|{}|{seed}", mt.root()))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    for (k, v) in common::entries(seed, ENTRIES) {
        mt.insert(&mut arena, &k, &v).unwrap();
    }
    mt.freeze();

    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && stdout.contains("child verified"),
        "reader process failed ({}):\n{stdout}\n{stderr}",
        out.status
    );

    // A second mapping in this process lands at another address: offsets still resolve.
    let again = vfs
        .open_shared(&name, None, LEN as u64, SharedOpen::Attach)
        .unwrap();
    let reader = MemtableReader::open(ArenaRegion::new(again, 0, LEN).unwrap(), mt.root()).unwrap();
    common::verify(&reader, &sorted_entries(seed), &mut Rng::new(seed ^ 0xBEEF));

    vfs.remove_shared(&name, None).unwrap();
}

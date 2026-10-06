//! `SimVfs`-only behavior: crash models, fault injection and determinism from a seed.
//! Every seeded test names its seed in its failure message.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};
use pigeonhole_io::{ErrorKind, FileRef, IoBuf, LockMode, OpenOptions, ProcessId, SharedOpen, Vfs};

const DIR: &str = "/db";

fn path(name: &str) -> PathBuf {
    Path::new(DIR).join(name)
}

/// Creates `name`, makes its directory entry durable, and returns a handle.
fn create_durable(vfs: &SimVfs, name: &str) -> FileRef {
    let f = vfs
        .open(&path(name), OpenOptions::read_write_create())
        .unwrap();
    vfs.sync_dir(Path::new(DIR)).unwrap();
    f
}

fn contents(vfs: &SimVfs, name: &str) -> Vec<u8> {
    let f = vfs.open(&path(name), OpenOptions::read()).unwrap();
    let mut buf = vec![0; f.len().unwrap() as usize];
    f.read_at(&mut buf, 0).unwrap();
    buf
}

fn plan(f: impl FnOnce(&mut FaultPlan)) -> FaultPlan {
    let mut p = FaultPlan::none();
    f(&mut p);
    p
}

#[test]
fn power_loss_keeps_only_synced_data() {
    let vfs = SimVfs::new(1);
    let f = create_durable(&vfs, "f");
    f.write_at(b"synced", 0).unwrap();
    f.sync_all().unwrap();
    f.write_at(b"!unsynced", 6).unwrap();
    f.set_len(100).unwrap();
    vfs.crash(CrashKind::Power);
    assert_eq!(contents(&vfs, "f"), b"synced");
}

#[test]
fn sync_data_does_not_persist_a_length_change() {
    let vfs = SimVfs::new(11);
    let f = create_durable(&vfs, "f");
    f.write_at(b"head", 0).unwrap();
    f.sync_all().unwrap();
    // Growth by a write past the end, then by `allocate`: data synced, length not.
    f.write_at(b"tail", 4).unwrap();
    f.allocate(0, 4096).unwrap();
    f.write_at(b"in-extent", 1000).unwrap();
    f.sync_data().unwrap();
    assert_eq!(f.len().unwrap(), 4096, "reads see the new length");
    vfs.crash(CrashKind::Power);
    assert_eq!(contents(&vfs, "f"), b"head", "seed 11");

    // `sync_all` makes the same sequence durable.
    let f = vfs
        .open(&path("f"), OpenOptions::read_write_create())
        .unwrap();
    f.allocate(0, 4096).unwrap();
    f.write_at(b"in-extent", 1000).unwrap();
    f.sync_all().unwrap();
    vfs.crash(CrashKind::Power);
    let after = contents(&vfs, "f");
    assert_eq!(after.len(), 4096, "seed 11");
    assert_eq!(&after[1000..1009], b"in-extent", "seed 11");
}

#[test]
fn sync_data_does_not_persist_a_shrink() {
    let vfs = SimVfs::new(12);
    let f = create_durable(&vfs, "f");
    f.write_at(b"0123456789", 0).unwrap();
    f.sync_all().unwrap();
    f.set_len(4).unwrap();
    f.sync_data().unwrap();
    vfs.crash(CrashKind::Power);
    // The old length comes back; the cut bytes read as zeros.
    assert_eq!(contents(&vfs, "f"), b"0123\0\0\0\0\0\0", "seed 12");
}

#[test]
fn sync_data_length_may_survive_under_faults() {
    // With a fault plan active, a length synced only by `sync_data` survives on some seeds
    // and not on others; the data synced inside it is intact whenever it does.
    let (mut kept, mut lost) = (false, false);
    for seed in 0..64 {
        let vfs = SimVfs::with_faults(seed, plan(|p| p.torn_writes = true));
        let f = create_durable(&vfs, "f");
        f.allocate(0, 512).unwrap();
        f.write_at(b"data", 0).unwrap();
        f.sync_data().unwrap();
        vfs.crash(CrashKind::Power);
        match contents(&vfs, "f").as_slice() {
            [] => lost = true,
            bytes => {
                assert_eq!(bytes.len(), 512, "seed {seed}");
                assert_eq!(&bytes[..4], b"data", "seed {seed}");
                kept = true;
            }
        }
    }
    assert!(kept && lost);
}

#[test]
fn process_crash_keeps_written_data_until_power_loss() {
    let vfs = SimVfs::new(2);
    let f = create_durable(&vfs, "f");
    f.write_at(b"durable", 0).unwrap();
    f.sync_all().unwrap();
    f.write_at(b"+kernel", 7).unwrap();
    vfs.crash(CrashKind::Process);
    assert_eq!(contents(&vfs, "f"), b"durable+kernel");
    // Still unsynced: a later power loss drops it.
    vfs.crash(CrashKind::Power);
    assert_eq!(contents(&vfs, "f"), b"durable");
}

#[test]
fn directory_entries_need_sync_dir() {
    let vfs = SimVfs::new(3);
    drop(create_durable(&vfs, "kept"));
    let lost = vfs
        .open(&path("lost"), OpenOptions::read_write_create())
        .unwrap();
    lost.write_at(b"x", 0).unwrap();
    lost.sync_all().unwrap(); // file data synced, but its name never was
    vfs.remove(&path("kept")).unwrap(); // unsynced removal
    assert!(!vfs.exists(&path("kept")).unwrap());

    vfs.crash(CrashKind::Power);
    assert!(
        vfs.exists(&path("kept")).unwrap(),
        "unsynced removal reverts"
    );
    assert!(
        !vfs.exists(&path("lost")).unwrap(),
        "unsynced creation reverts"
    );

    vfs.remove(&path("kept")).unwrap();
    vfs.sync_dir(Path::new(DIR)).unwrap();
    vfs.crash(CrashKind::Power);
    assert!(!vfs.exists(&path("kept")).unwrap());
    assert_eq!(vfs.list_dir(Path::new(DIR)).unwrap(), Vec::<PathBuf>::new());
}

#[test]
fn crashed_handles_fail_and_reopen_recovers() {
    let vfs = SimVfs::new(4);
    let f = create_durable(&vfs, "f");
    f.lock(8192, LockMode::Exclusive).unwrap();
    vfs.crash(CrashKind::Process);
    for err in [
        f.write_at(b"x", 0).unwrap_err(),
        f.read_at(&mut [0], 0).unwrap_err(),
        f.sync_data().unwrap_err(),
        f.len().unwrap_err(),
        f.lock(1, LockMode::Shared).unwrap_err(),
        f.submit_read(IoBuf::zeroed(1), 0).wait().unwrap_err(),
    ] {
        assert_eq!(err.kind, ErrorKind::Crashed);
    }
    // The crash released the dead handle's locks.
    let g = vfs
        .open(&path("f"), OpenOptions::read_write_create())
        .unwrap();
    g.lock(8192, LockMode::Exclusive).unwrap();
    drop(f); // dropping a dead handle is harmless
    g.write_at(b"ok", 0).unwrap();
}

#[test]
fn shared_memory_survives_process_crash_not_power_loss() {
    let vfs = SimVfs::new(5);
    let r = vfs
        .open_shared("phdb-test", None, 4096, SharedOpen::CreateNew)
        .unwrap();
    r.write(0, b"x");
    vfs.crash(CrashKind::Process);
    vfs.open_shared("phdb-test", None, 4096, SharedOpen::Attach)
        .unwrap();
    vfs.crash(CrashKind::Power);
    assert_eq!(
        vfs.open_shared("phdb-test", None, 4096, SharedOpen::Attach)
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
}

/// One unsynced 8-sector write over durable zeros, crashed with `plan`.
fn torn_outcome(seed: u64, plan: FaultPlan) -> Vec<u8> {
    let vfs = SimVfs::with_faults(seed, plan);
    let f = create_durable(&vfs, "f");
    f.set_len(4096).unwrap();
    f.sync_all().unwrap();
    f.write_at(&[0xAB; 4096], 0).unwrap();
    vfs.crash(CrashKind::Power);
    contents(&vfs, "f")
}

#[test]
fn torn_writes_tear_at_sector_boundaries() {
    let mut partial = 0;
    for seed in 0..64 {
        let out = torn_outcome(seed, plan(|p| p.torn_writes = true));
        assert_eq!(out.len(), 4096, "seed {seed}");
        let mut sectors = out.chunks(512).map(|s| {
            assert!(
                s.iter().all(|&b| b == 0) || s.iter().all(|&b| b == 0xAB),
                "seed {seed}: a sector was split"
            );
            s[0] == 0xAB
        });
        let first = sectors.next().unwrap();
        if sectors.any(|s| s != first) {
            partial += 1;
        }
    }
    assert!(partial > 0, "no seed in 0..64 produced a torn write");
    // Without the fault, the unsynced write is simply gone.
    assert_eq!(torn_outcome(0, FaultPlan::none()), vec![0; 4096]);
}

/// Three unsynced writes to separate sectors; which survived a power loss.
fn survivors(seed: u64, plan: FaultPlan) -> [bool; 3] {
    let vfs = SimVfs::with_faults(seed, plan);
    let f = create_durable(&vfs, "f");
    f.set_len(3 * 512).unwrap();
    f.sync_all().unwrap();
    for i in 0..3u64 {
        f.write_at(&[1 + i as u8; 512], i * 512).unwrap();
    }
    vfs.crash(CrashKind::Power);
    let out = contents(&vfs, "f");
    std::array::from_fn(|i| out[i * 512] != 0)
}

#[test]
fn unsynced_writes_survive_in_order_unless_reordering() {
    let mut saw_reorder = false;
    for seed in 0..64 {
        let s = survivors(seed, plan(|p| p.torn_writes = true));
        // In-order: a later write survives only if every earlier one did.
        for later in 1..3 {
            if s[later] {
                assert!(s[..later].iter().all(|&x| x), "seed {seed}: {s:?}");
            }
        }
        let r = survivors(seed, plan(|p| p.reorder_unsynced = true));
        saw_reorder |= r[2] && !r[0];
    }
    assert!(
        saw_reorder,
        "reorder_unsynced never let a later write outlive an earlier one"
    );
}

#[test]
fn enospc_after_byte_budget() {
    let vfs = SimVfs::with_faults(6, plan(|p| p.enospc_after_bytes = Some(1000)));
    let f = create_durable(&vfs, "f");
    f.write_at(&[1; 600], 0).unwrap();
    assert_eq!(
        f.write_at(&[2; 600], 600).unwrap_err().kind,
        ErrorKind::NoSpace
    );
    assert_eq!(f.len().unwrap(), 600, "a failed write changes nothing");
    f.write_at(&[3; 400], 600).unwrap();
    assert_eq!(f.write_at(&[4; 1], 0).unwrap_err().kind, ErrorKind::NoSpace);
    vfs.set_faults(FaultPlan::none());
    f.write_at(&[4; 1], 0).unwrap();
}

#[test]
fn io_error_injection_rate() {
    let vfs = SimVfs::with_faults(7, plan(|p| p.io_error_ppm = 1_000_000));
    let f = create_durable(&vfs, "f");
    assert_eq!(f.write_at(b"x", 0).unwrap_err().kind, ErrorKind::Other);
    assert_eq!(f.read_at(&mut [0], 0).unwrap_err().kind, ErrorKind::Other);

    let vfs = SimVfs::with_faults(8, plan(|p| p.io_error_ppm = 100_000));
    let f = create_durable(&vfs, "f");
    let failures = (0..1000u64)
        .filter(|&i| f.write_at(b"x", i).is_err())
        .count();
    assert!(
        (50..200).contains(&failures),
        "seed 8: {failures} of 1000 failed at 10%"
    );
}

/// A tiny log: append fixed records, syncing every third one (`sync_all`, since appends grow
/// the file). Returns how many records were acknowledged as synced before the first error.
fn log_workload(vfs: &SimVfs) -> usize {
    let Ok(f) = vfs.open(&path("log"), OpenOptions::read_write_create()) else {
        return 0;
    };
    if vfs.sync_dir(Path::new(DIR)).is_err() {
        return 0;
    }
    let mut synced = 0;
    for i in 0..12u64 {
        if f.write_at(&[i as u8 + 1; 100], i * 100).is_err() {
            break;
        }
        if i % 3 == 2 {
            if f.sync_all().is_err() {
                break;
            }
            synced = i as usize + 1;
        }
    }
    synced
}

#[test]
fn crash_at_every_mutating_operation() {
    let total = {
        let vfs = SimVfs::new(9);
        log_workload(&vfs);
        vfs.mutating_ops()
    };
    assert!(total > 12);
    for seed in [9, 10] {
        for n in 1..=total {
            let mut p = FaultPlan::all();
            p.io_error_ppm = 0;
            p.crash_after_ops = Some(n);
            let vfs = SimVfs::with_faults(seed, p);
            let acked = log_workload(&vfs);
            assert_eq!(vfs.mutating_ops(), n, "seed {seed} crash point {n}");
            // Every record acknowledged as synced is intact after the crash.
            if vfs.exists(&path("log")).unwrap() {
                let data = contents(&vfs, "log");
                assert!(data.len() >= acked * 100, "seed {seed} crash point {n}");
                for i in 0..acked {
                    assert!(
                        data[i * 100..(i + 1) * 100]
                            .iter()
                            .all(|&b| b == i as u8 + 1),
                        "seed {seed} crash point {n}: record {i} damaged"
                    );
                }
            } else {
                assert_eq!(acked, 0, "seed {seed} crash point {n}");
            }
        }
    }
}

/// A seeded random workload under every fault, crashed partway; returns everything
/// observable: each operation's outcome and the files after the crash.
fn chaos(seed: u64) -> (Vec<String>, BTreeMap<PathBuf, Vec<u8>>) {
    let mut p = FaultPlan::all();
    p.io_error_ppm = 50_000;
    p.enospc_after_bytes = Some(20_000);
    let vfs: Arc<SimVfs> = SimVfs::with_faults(seed, p);
    // The workload's own choices come from a generator seeded alike.
    let mut x = seed ^ 0xD1B5_4A32_D192_ED03;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let files: Vec<FileRef> = (0..3)
        .map(|i| {
            vfs.open(&path(&format!("f{i}")), OpenOptions::read_write_create())
                .unwrap()
        })
        .collect();
    let mut log = Vec::new();
    for step in 0..200 {
        let f = &files[(next() % 3) as usize];
        let off = next() % 4096;
        let outcome = match next() % 6 {
            0..=2 => f.write_at(&vec![step as u8; (next() % 1500) as usize + 1], off),
            3 => f.sync_data(),
            4 => vfs.sync_dir(Path::new(DIR)),
            _ => f.read_at(&mut [0; 16], off),
        };
        log.push(format!("{step}: {:?}", outcome.map_err(|e| e.kind)));
        if step == 150 {
            vfs.crash(CrashKind::Power);
        }
    }
    let state = vfs
        .list_dir(Path::new(DIR))
        .unwrap()
        .into_iter()
        .map(|p| {
            let f = vfs.open(&p, OpenOptions::read()).unwrap();
            let mut buf = vec![0; f.len().unwrap() as usize];
            f.read_at(&mut buf, 0).ok();
            (p, buf)
        })
        .collect();
    (log, state)
}

#[test]
fn same_seed_same_outcome() {
    let mut distinct = std::collections::HashSet::new();
    for seed in 0..8 {
        let a = chaos(seed);
        let b = chaos(seed);
        assert!(a == b, "seed {seed}: two runs diverged");
        distinct.insert(format!("{a:?}"));
    }
    assert!(
        distinct.len() > 1,
        "different seeds should explore different outcomes"
    );
}

#[test]
fn clocks_move_only_when_advanced() {
    let vfs = SimVfs::new(11);
    let (wall, mono) = (vfs.now_micros(), vfs.monotonic_nanos());
    assert_eq!((vfs.now_micros(), vfs.monotonic_nanos()), (wall, mono));
    vfs.advance(2_500_000);
    assert_eq!(vfs.monotonic_nanos(), mono + 2_500_000);
    assert_eq!(vfs.now_micros(), wall + 2_500);
}

#[test]
fn simulated_processes() {
    let vfs = SimVfs::new(12);
    let main = vfs.current_process();
    let reader = ProcessId {
        pid: 42,
        start_time: 7,
    };
    let seen = std::thread::scope(|s| {
        s.spawn(|| {
            vfs.enter_process(reader);
            vfs.current_process()
        })
        .join()
        .unwrap()
    });
    assert_eq!(seen, reader);
    assert_eq!(vfs.current_process(), main, "other threads are unaffected");
    assert!(vfs.process_alive(reader));
    vfs.kill_process(reader);
    assert!(!vfs.process_alive(reader));
    assert!(vfs.process_alive(main));
    assert_eq!(vfs.seed(), 12);
}

#[test]
fn crashing_one_process_spares_the_others() {
    let vfs = SimVfs::new(13);
    drop(create_durable(&vfs, "f"));
    let writer = ProcessId {
        pid: 10,
        start_time: 1,
    };
    let reader = ProcessId {
        pid: 20,
        start_time: 1,
    };
    let region = vfs
        .open_shared("phdb-x", None, 4096, SharedOpen::CreateNew)
        .unwrap();
    let open_as = |p: ProcessId| {
        std::thread::scope(|s| {
            s.spawn(|| {
                vfs.enter_process(p);
                vfs.open(&path("f"), OpenOptions::read_write_create())
                    .unwrap()
            })
            .join()
            .unwrap()
        })
    };
    let w = open_as(writer);
    let r = open_as(reader);
    w.lock(8192, LockMode::Exclusive).unwrap();
    w.write_at(b"unsynced", 0).unwrap();
    r.lock(8193, LockMode::Shared).unwrap();

    vfs.crash_process(writer);
    assert_eq!(w.write_at(b"x", 0).unwrap_err().kind, ErrorKind::Crashed);
    assert!(!vfs.process_alive(writer));
    assert!(vfs.process_alive(reader));
    // The reader keeps working and sees the dead writer's data (still in the kernel).
    let mut buf = [0u8; 8];
    r.read_at(&mut buf, 0).unwrap();
    assert_eq!(&buf, b"unsynced");
    // The writer's lock is gone; the reader's remains.
    let next = open_as(ProcessId {
        pid: 30,
        start_time: 1,
    });
    next.lock(8192, LockMode::Exclusive).unwrap();
    assert_eq!(
        next.lock(8193, LockMode::Exclusive).unwrap_err().kind,
        ErrorKind::Locked
    );
    // Shared memory is untouched.
    vfs.open_shared("phdb-x", None, 4096, SharedOpen::Attach)
        .unwrap();
    drop(region);
}

#[test]
fn files_are_limited_to_four_gib() {
    let vfs = SimVfs::new(14);
    let f = create_durable(&vfs, "f");
    assert_eq!(
        f.write_at(b"x", u64::MAX).unwrap_err().kind,
        ErrorKind::Other
    );
    assert_eq!(
        f.write_at(b"x", 1 << 32).unwrap_err().kind,
        ErrorKind::Other
    );
    assert_eq!(f.set_len(u64::MAX).unwrap_err().kind, ErrorKind::Other);
    assert_eq!(f.allocate(u64::MAX, 2).unwrap_err().kind, ErrorKind::Other);
    assert_eq!(
        f.read_at(&mut [0], u64::MAX).unwrap_err().kind,
        ErrorKind::UnexpectedEof
    );
    assert_eq!(f.len().unwrap(), 0, "failed calls change nothing");
}

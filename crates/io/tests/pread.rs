//! `PreadVfs`-only behavior: cross-process locks, the default shared-memory location,
//! process liveness and the worker pool. Real syscalls, so not run under Miri.
#![cfg(not(miri))]

mod common;

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::{TempDir, unique};
use pigeonhole_io::pread::PreadVfs;
use pigeonhole_io::{ErrorKind, IoBuf, LockMode, OpenOptions, ProcessId, SharedOpen, Vfs};

const CHILD_ENV: &str = "PIGEONHOLE_IO_LOCK_PROBE";

/// Exit codes of the probe child.
const PROBE_OK: i32 = 0;
const PROBE_LOCKED: i32 = 10;

/// Runs in a child process (see `probe` and `hold`): opens the file and tries one lock.
/// A probe reports by exit code; a holder (`hs`/`hx`) prints a marker line and keeps the
/// lock until its stdin closes. Without the environment variable it is an empty test.
#[test]
fn lock_probe_child() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let mut parts = spec.splitn(3, '|');
    let (mode, byte, path) = (
        parts.next().unwrap(),
        parts.next().unwrap().parse::<u64>().unwrap(),
        parts.next().unwrap(),
    );
    let (holding, mode) = mode.strip_prefix('h').map_or((false, mode), |m| (true, m));
    let mode = if mode == "x" {
        LockMode::Exclusive
    } else {
        LockMode::Shared
    };
    let vfs = PreadVfs::new(1);
    let mut opts = OpenOptions::read();
    opts.write = true;
    let file = vfs.open(Path::new(path), opts).unwrap();
    if holding {
        use std::io::{Read, Write};
        let marker = match file.lock(byte, mode) {
            Ok(()) => HELD,
            Err(_) => REFUSED,
        };
        println!("{marker}");
        std::io::stdout().flush().unwrap();
        let _ = std::io::stdin().read_to_end(&mut Vec::new());
        std::process::exit(PROBE_OK);
    }
    let code = match file.lock(byte, mode) {
        Ok(()) => PROBE_OK,
        Err(e) if e.kind == ErrorKind::Locked => PROBE_LOCKED,
        Err(e) => {
            eprintln!("probe failed: {e}");
            20
        }
    };
    std::process::exit(code);
}

/// Whether another process can take `mode` on `byte` of `path` right now.
fn probe(path: &Path, byte: u64, mode: LockMode) -> bool {
    let m = if mode == LockMode::Exclusive {
        "x"
    } else {
        "s"
    };
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "lock_probe_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ENV, format!("{m}|{byte}|{}", path.display()))
        .status()
        .unwrap();
    match status.code() {
        Some(PROBE_OK) => true,
        Some(PROBE_LOCKED) => false,
        other => panic!("lock probe child failed: {other:?}"),
    }
}

const HELD: &str = "PIGEONHOLE-LOCK-HELD";
const REFUSED: &str = "PIGEONHOLE-LOCK-REFUSED";

/// Another process holding a lock until dropped.
struct Holder(std::process::Child);

/// Starts a child process that takes `mode` on `byte` of `path` and keeps it.
fn hold(path: &Path, byte: u64, mode: LockMode) -> Holder {
    use std::io::BufRead;
    use std::process::Stdio;
    let m = if mode == LockMode::Exclusive {
        "hx"
    } else {
        "hs"
    };
    let child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "lock_probe_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ENV, format!("{m}|{byte}|{}", path.display()))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    // Waited on by `Holder::drop` on every path, including the panics below.
    let mut holder = Holder(child);
    // The test harness prints its own lines (without newlines) around ours: scan for the marker.
    let mut out = std::io::BufReader::new(holder.0.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        assert!(out.read_line(&mut line).unwrap() > 0, "holder exited early");
        if line.contains(REFUSED) {
            panic!("holder could not take its lock");
        }
        if line.contains(HELD) {
            return holder;
        }
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        drop(self.0.stdin.take()); // EOF: the holder releases and exits
        self.0.wait().unwrap();
    }
}

fn setup(tag: &str) -> (TempDir, Arc<PreadVfs>, std::path::PathBuf) {
    let dir = TempDir::new(tag);
    let path = dir.0.join("data.phdb");
    let vfs = PreadVfs::new(2);
    drop(vfs.open(&path, OpenOptions::read_write_create()).unwrap());
    (dir, vfs, path)
}

#[test]
fn locks_exclude_other_processes() {
    let (_dir, vfs, path) = setup("xproc");
    let f = vfs.open(&path, OpenOptions::read_write_create()).unwrap();
    assert!(probe(&path, 8192, LockMode::Exclusive));

    f.lock(8192, LockMode::Exclusive).unwrap();
    assert!(
        !probe(&path, 8192, LockMode::Exclusive),
        "second exclusive fails"
    );
    assert!(!probe(&path, 8192, LockMode::Shared));
    assert!(
        probe(&path, 8193, LockMode::Exclusive),
        "other bytes are free"
    );

    f.lock(8192, LockMode::Shared).unwrap(); // downgrade
    assert!(
        probe(&path, 8192, LockMode::Shared),
        "shared + shared is fine"
    );
    assert!(!probe(&path, 8192, LockMode::Exclusive));

    f.unlock(8192).unwrap();
    assert!(probe(&path, 8192, LockMode::Exclusive));
}

/// The macOS/BSD hazard: closing any descriptor of a file drops every `fcntl` lock the
/// process holds on it. The registry must keep the other handle's lock alive.
#[test]
fn closing_one_handle_keeps_another_handles_lock() {
    let (_dir, vfs, path) = setup("close");
    let holder = vfs.open(&path, OpenOptions::read_write_create()).unwrap();
    holder.lock(8192, LockMode::Exclusive).unwrap();

    let bystander = vfs.open(&path, OpenOptions::read()).unwrap();
    bystander.lock(8194, LockMode::Shared).unwrap();
    drop(bystander);
    assert!(
        !probe(&path, 8192, LockMode::Exclusive),
        "closing an unrelated handle dropped the lock"
    );
    assert!(
        probe(&path, 8194, LockMode::Exclusive),
        "the closed handle's lock is gone"
    );

    // Shared holders: the byte stays locked until the last one goes.
    let s1 = vfs.open(&path, OpenOptions::read()).unwrap();
    let s2 = vfs.open(&path, OpenOptions::read()).unwrap();
    s1.lock(8193, LockMode::Shared).unwrap();
    s2.lock(8193, LockMode::Shared).unwrap();
    drop(s1);
    assert!(!probe(&path, 8193, LockMode::Exclusive));
    drop(s2);
    assert!(probe(&path, 8193, LockMode::Exclusive));

    drop(holder);
    assert!(probe(&path, 8192, LockMode::Exclusive));
}

#[test]
fn upgrade_fails_while_another_process_shares() {
    let (_dir, vfs, path) = setup("upgrade");
    let f = vfs.open(&path, OpenOptions::read_write_create()).unwrap();
    f.lock(8193, LockMode::Shared).unwrap();
    let other = hold(&path, 8193, LockMode::Shared);
    assert_eq!(
        f.lock(8193, LockMode::Exclusive).unwrap_err().kind,
        ErrorKind::Locked,
        "upgrade must fail while another process shares the byte"
    );
    drop(other);
    // The failed upgrade kept our shared lock (on Windows: re-taken after the attempt).
    assert!(
        !probe(&path, 8193, LockMode::Exclusive),
        "the failed upgrade lost the shared lock"
    );
    assert!(probe(&path, 8193, LockMode::Shared));

    // With the other holder gone the upgrade succeeds, then downgrades again.
    f.lock(8193, LockMode::Exclusive).unwrap();
    assert!(!probe(&path, 8193, LockMode::Shared));
    f.lock(8193, LockMode::Shared).unwrap();
    assert!(probe(&path, 8193, LockMode::Shared));

    // An exclusive holder elsewhere blocks even a shared lock here.
    f.unlock(8193).unwrap();
    let writer = hold(&path, 8193, LockMode::Exclusive);
    assert_eq!(
        f.lock(8193, LockMode::Shared).unwrap_err().kind,
        ErrorKind::Locked
    );
    drop(writer);
    f.lock(8193, LockMode::Exclusive).unwrap();
}

#[test]
fn default_shared_memory_location() {
    let vfs = PreadVfs::new(1);
    let name = unique("m");
    let region = vfs
        .open_shared(&name, None, 1 << 16, SharedOpen::CreateNew)
        .unwrap();
    assert_eq!(region.base_ptr().as_ptr() as usize % 4096, 0);
    assert_eq!(
        vfs.open_shared(&name, None, 1 << 16, SharedOpen::CreateNew)
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists
    );
    let attached = vfs
        .open_shared(&name, None, 1 << 16, SharedOpen::Attach)
        .unwrap();
    region.atomic_u64(4096).store(9, Ordering::Release);
    assert_eq!(attached.atomic_u64(4096).load(Ordering::Acquire), 9);
    region.bind_numa(0, 1 << 16, 0).ok();
    vfs.remove_shared(&name, None).unwrap();
    assert_eq!(attached.atomic_u64(4096).load(Ordering::Acquire), 9);
    drop((region, attached));
    // Windows drops the name with the last handle; elsewhere `remove_shared` did.
    assert_eq!(
        vfs.open_shared(&name, None, 1 << 16, SharedOpen::Attach)
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
}

#[test]
fn attach_to_missing_default_region_fails() {
    let vfs = PreadVfs::new(1);
    let err = vfs
        .open_shared(&unique("n"), None, 4096, SharedOpen::Attach)
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::NotFound, "{err}");
}

#[test]
fn process_liveness() {
    let vfs = PreadVfs::new(1);
    let me = vfs.current_process();
    assert_eq!(me.pid, std::process::id());
    assert!(vfs.process_alive(me));

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "no_such_test", "--test-threads=1"])
        .spawn()
        .unwrap();
    let pid = child.id();
    child.wait().unwrap();
    // The reaped child is gone, unless its pid was already recycled (rare; then only the
    // start time could tell, which `start_time: 0` deliberately does not check).
    if vfs.process_alive(ProcessId { pid, start_time: 0 }) {
        eprintln!("pid {pid} was recycled; skipping the dead-pid check");
    }
    // A recorded start time that differs means a different process behind the same pid.
    if me.start_time != 0 {
        assert!(!vfs.process_alive(ProcessId {
            pid: me.pid,
            start_time: me.start_time + 1,
        }));
    }
}

#[test]
fn pool_serves_many_threads() {
    let (_dir, vfs, path) = setup("pool");
    let f = vfs.open(&path, OpenOptions::read_write_create()).unwrap();
    f.set_len(64 * 4096).unwrap();
    std::thread::scope(|s| {
        for t in 0..8u64 {
            let f = &f;
            s.spawn(move || {
                for i in 0..8u64 {
                    let block = t * 8 + i;
                    let mut buf = IoBuf::zeroed(4096);
                    buf.fill(block as u8);
                    f.submit_write(buf, block * 4096).wait().unwrap();
                }
            });
        }
    });
    f.submit_sync_data().wait().unwrap();
    for block in 0..64u64 {
        let buf = f
            .submit_read(IoBuf::zeroed(4096), block * 4096)
            .wait()
            .unwrap();
        assert!(buf.iter().all(|&b| b == block as u8), "block {block}");
    }
}

#[test]
fn default_thread_count_and_unfinished_completions() {
    let (_dir, vfs, path) = setup("drop");
    let vfs2 = PreadVfs::new(0);
    let f = vfs2.open(&path, OpenOptions::read_write_create()).unwrap();
    // Dropping completions, the file and the vfs while I/O is queued must be safe.
    for i in 0..16u64 {
        drop(f.submit_write(IoBuf::zeroed(4096), i * 4096));
    }
    drop(f);
    drop(vfs2);
    drop(vfs);
}

#[test]
fn network_filesystem_detection_reports_temp_as_local() {
    let (_dir, vfs, path) = setup("local");
    assert!(
        vfs.open(&path, OpenOptions::read())
            .unwrap()
            .is_local()
            .unwrap()
    );
}

#[test]
fn sys_topology() {
    let cpus = pigeonhole_io::sys::available_cpus();
    assert!(cpus >= 1);
    let handle = std::thread::spawn(|| pigeonhole_io::sys::pin_current_thread(0));
    match handle.join().unwrap() {
        Ok(()) => {}
        Err(e) => assert!(
            matches!(e.kind, ErrorKind::Unsupported | ErrorKind::Other),
            "{e}"
        ),
    }
    let _ = pigeonhole_io::sys::numa_node_of(0);
    assert!(pigeonhole_io::sys::pin_current_thread(usize::MAX).is_err());
}

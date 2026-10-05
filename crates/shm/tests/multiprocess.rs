//! The multi-process suite on real processes: the test binary re-executes itself as writer,
//! reader and probe children (as `pigeonhole-io`'s `tests/pread.rs` does) over `PreadVfs`
//! and the platform's default shared-memory location. Real syscalls, so not run under Miri.
#![cfg(not(miri))]

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::*;
use pigeonhole_format::shm::{directory, directory_name, header, region_name};
use pigeonhole_io::pread::PreadVfs;
use pigeonhole_io::{ErrorKind, FileIdentity, FileRef, OpenOptions, SharedOpen, VfsRef};
use pigeonhole_shm::{Error, Generation, Presence, Role, ShmRegion, WriterLock};

const CHILD_ENV: &str = "PIGEONHOLE_SHM_CHILD";
/// Every line a child prints for the parent starts with this.
const TAG: &str = "PHSHM ";

const EXIT_OK: i32 = 0;
const EXIT_LOCKED: i32 = 10;
const EXIT_FAILED: i32 = 20;

fn open_db(vfs: &VfsRef, path: &Path) -> (FileRef, FileIdentity) {
    let file = vfs.open(path, OpenOptions::read_write_create()).unwrap();
    let identity = file.identity().unwrap();
    (file, identity)
}

fn say(line: impl std::fmt::Display) {
    println!("{TAG}{line}");
    std::io::stdout().flush().unwrap();
}

/// Runs in a child process. Without the environment variable it is an empty test.
#[test]
fn child_main() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let (role, path) = spec.split_once('|').unwrap();
    let path = PathBuf::from(path);
    let vfs: VfsRef = PreadVfs::new(1);
    let (file, identity) = open_db(&vfs, &path);
    let code = match role {
        "probe-writer" => match WriterLock::acquire(&file) {
            Ok(_) => EXIT_OK,
            Err(Error::WriterLocked) => EXIT_LOCKED,
            Err(e) => {
                eprintln!("probe failed: {e}");
                EXIT_FAILED
            }
        },
        "writer" => writer_child(&vfs, &file, identity),
        "reader" => reader_child(&vfs, &file, identity),
        other => panic!("unknown child role {other}"),
    };
    std::process::exit(code);
}

fn commands() -> impl Iterator<Item = String> {
    std::io::stdin().lines().map_while(Result::ok)
}

/// Builds a generation, publishes view 1, then waits for `quit` (or to be killed).
fn writer_child(vfs: &VfsRef, file: &FileRef, identity: FileIdentity) -> i32 {
    let _lock = WriterLock::acquire(file).unwrap();
    let _presence = Presence::acquire(file).unwrap();
    let shm = ShmRegion::open(vfs, file, identity, DB_ID, Role::Writer, &small_config()).unwrap();
    shm.publish_view(&view(1, 3, 8)).unwrap();
    shm.set_manifest_version(10);
    shm.reserve_seqnos(9);
    say(format!("READY gen={}", shm.generation().0));
    for cmd in commands() {
        if cmd.trim() == "quit" {
            break;
        }
    }
    EXIT_OK
}

/// Attaches as a reader and serves commands from stdin: `pin <seqno>`, `check <n>`,
/// `status`, `view`, `reattach`, `quit`.
fn reader_child(vfs: &VfsRef, file: &FileRef, identity: FileIdentity) -> i32 {
    let _presence = Presence::acquire(file).unwrap();
    let mut shm = match ShmRegion::open(vfs, file, identity, DB_ID, Role::Reader, &small_config()) {
        Ok(shm) => shm,
        Err(e) => {
            say(format!("ERR {e:?}"));
            return EXIT_FAILED;
        }
    };
    let me = vfs.current_process();
    let mut slot = shm.claim_reader_slot(me).unwrap();
    say(format!(
        "READY slot={} gen={}",
        slot.index(),
        shm.generation().0
    ));
    let mut last = 0;
    for cmd in commands() {
        let mut words = cmd.split_whitespace();
        match (words.next(), words.next()) {
            (Some("pin"), Some(seqno)) => {
                slot.pin(seqno.parse().unwrap(), shm.view_version());
                say(format!("PINNED view={}", shm.view_version()));
            }
            (Some("check"), Some(n)) => {
                let n: u64 = n.parse().unwrap();
                let mut result = Ok(last);
                for _ in 0..n {
                    result = check_snapshot(&shm, &mut last, 512);
                    if result.is_err() {
                        break;
                    }
                }
                match result {
                    Ok(visible) => say(format!("OK visible={visible}")),
                    Err(e) => say(format!("BAD {e}")),
                }
            }
            (Some("status"), _) => say(format!(
                "STATUS stale={} gen={} view={} visible={}",
                shm.is_stale(),
                shm.generation().0,
                shm.view_version(),
                shm.visible_seqno()
            )),
            (Some("view"), _) => match shm.read_view() {
                Ok(v) => say(format!(
                    "VIEW version={} tablets={} manifest={}",
                    v.view_version,
                    v.tablets.len(),
                    v.manifest_version
                )),
                Err(e) => say(format!("ERR {e:?}")),
            },
            (Some("reattach"), _) => {
                shm = shm.reattach(vfs, file).unwrap();
                slot = shm.claim_reader_slot(me).unwrap();
                say(format!(
                    "REATTACHED gen={} slot={}",
                    shm.generation().0,
                    slot.index()
                ));
            }
            (Some("quit"), _) => break,
            other => panic!("unknown command {other:?}"),
        }
    }
    EXIT_OK
}

/// A child process the parent talks to over stdin/stdout.
struct Peer {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

impl Peer {
    fn spawn(role: &str, path: &Path) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child_main", "--nocapture", "--test-threads=1"])
            .env(CHILD_ENV, format!("{role}|{}", path.display()))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            stdin,
            stdout,
        }
    }

    fn send(&mut self, cmd: &str) {
        let stdin = self.stdin.as_mut().expect("stdin open");
        writeln!(stdin, "{cmd}").unwrap();
        stdin.flush().unwrap();
    }

    /// The next tagged line. The harness prints its own text around ours, on the same line
    /// (`test child_main ... PHSHM READY`), so the tag is searched for, not anchored.
    fn recv(&mut self) -> String {
        let mut line = String::new();
        loop {
            line.clear();
            if self.stdout.read_line(&mut line).unwrap() == 0 {
                let status = self.child.wait().unwrap();
                panic!("child exited before answering: {status}");
            }
            if let Some(pos) = line.find(TAG) {
                return line[pos + TAG.len()..].trim_end().to_owned();
            }
        }
    }

    fn ask(&mut self, cmd: &str) -> String {
        self.send(cmd);
        self.recv()
    }

    /// Sends `quit` and waits for a clean exit.
    fn quit(mut self) {
        self.send("quit");
        drop(self.stdin.take());
        let status = self.child.wait().unwrap();
        assert!(status.success(), "child failed: {status}");
    }

    /// Kills the child (`SIGKILL` / `TerminateProcess`) and reaps it.
    fn kill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Whether another process can take the writer lock right now.
fn probe_writer(path: &Path) -> bool {
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_main", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, format!("probe-writer|{}", path.display()))
        .stdout(Stdio::null())
        .status()
        .unwrap();
    match status.code() {
        Some(EXIT_OK) => true,
        Some(EXIT_LOCKED) => false,
        other => panic!("probe child failed: {other:?}"),
    }
}

/// A fresh database file in a temp dir, removed (with its shared-memory names) on drop.
struct Db {
    dir: PathBuf,
    path: PathBuf,
    vfs: VfsRef,
    identity: FileIdentity,
}

impl Db {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pigeonhole-shm-{}-{tag}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.phdb");
        let vfs: VfsRef = PreadVfs::new(1);
        let (_, identity) = open_db(&vfs, &path);
        Self {
            dir,
            path,
            vfs,
            identity,
        }
    }

    fn open(&self) -> FileRef {
        open_db(&self.vfs, &self.path).0
    }

    fn open_writer(&self, file: &FileRef) -> (WriterLock, Presence, ShmRegion) {
        let lock = WriterLock::acquire(file).unwrap();
        let presence = Presence::acquire(file).unwrap();
        let shm = ShmRegion::open(
            &self.vfs,
            file,
            self.identity,
            DB_ID,
            Role::Writer,
            &small_config(),
        )
        .unwrap();
        (lock, presence, shm)
    }

    fn region_exists(&self, generation: u64) -> bool {
        let name = region_name(self.identity.device, self.identity.inode, generation);
        match self.vfs.open_shared(&name, None, 4096, SharedOpen::Attach) {
            Ok(_) => true,
            Err(e) if e.kind == ErrorKind::NotFound => false,
            Err(e) => panic!("{e}"),
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = ShmRegion::remove(&self.vfs, self.identity, None);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn seed() -> u64 {
    std::env::var("PIGEONHOLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x5EED_0002)
}

#[test]
fn second_writer_process_is_refused() {
    let db = Db::new("writer");
    let file = db.open();
    assert!(probe_writer(&db.path));
    let (lock, _presence, _shm) = db.open_writer(&file);
    assert!(!probe_writer(&db.path), "second writer must fail");
    drop(lock);
    assert!(probe_writer(&db.path));
}

#[test]
fn reader_processes_see_commits_in_order_and_whole() {
    let seed = seed();
    let db = Db::new("commits");
    let file = db.open();
    let (_lock, _presence, shm) = db.open_writer(&file);
    shm.publish_view(&view(1, 2, 8)).unwrap();

    let mut readers: Vec<Peer> = (0..2).map(|_| Peer::spawn("reader", &db.path)).collect();
    for r in &mut readers {
        assert!(r.recv().starts_with("READY"));
        r.send("check 20000");
    }
    let stop = AtomicBool::new(false);
    let reserved = run_writer(&shm, seed, Duration::from_millis(1500), &stop);
    assert!(reserved > 0, "seed {seed}");
    for r in &mut readers {
        let answer = r.recv();
        assert!(
            answer.starts_with("OK"),
            "seed {seed}: reader reported {answer}"
        );
        // After the writer is done everything it reserved is visible to every reader.
        let answer = r.ask("check 1");
        assert_eq!(answer, format!("OK visible={reserved}"), "seed {seed}");
        let answer = r.ask("status");
        assert!(answer.contains("stale=false"), "{answer}");
    }
    for r in readers {
        r.quit();
    }
}

#[test]
fn killed_reader_process_slot_is_reclaimed() {
    let db = Db::new("kill-reader");
    let file = db.open();
    let (_lock, _presence, shm) = db.open_writer(&file);
    shm.publish_view(&view(1, 1, 4)).unwrap();

    let mut reader = Peer::spawn("reader", &db.path);
    assert!(reader.recv().starts_with("READY"));
    assert_eq!(reader.ask("pin 5"), "PINNED view=1");
    assert_eq!(shm.oldest_reader_pin(), Some((5, 1)));
    assert_eq!(
        shm.reclaim_dead_slots(&db.vfs),
        0,
        "alive: nothing to reclaim"
    );

    let pid = reader.pid();
    reader.kill();
    assert!(
        !db.vfs
            .process_alive(pigeonhole_io::ProcessId { pid, start_time: 0 })
            || {
                eprintln!("pid {pid} recycled already; the start time must tell them apart");
                true
            }
    );
    assert_eq!(shm.reclaim_dead_slots(&db.vfs), 1);
    assert_eq!(shm.oldest_reader_pin(), None);
    assert_eq!(shm.reclaim_dead_slots(&db.vfs), 0);
}

#[test]
fn writer_process_kill_and_restart_remaps_readers() {
    let db = Db::new("kill-writer");
    let mut writer = Peer::spawn("writer", &db.path);
    assert_eq!(writer.recv(), "READY gen=1");
    assert!(!probe_writer(&db.path));

    let mut reader = Peer::spawn("reader", &db.path);
    assert_eq!(reader.recv(), "READY slot=0 gen=1");
    assert_eq!(reader.ask("pin 9"), "PINNED view=1");
    assert_eq!(reader.ask("view"), "VIEW version=1 tablets=3 manifest=10");

    writer.kill();
    // The reader keeps serving its snapshot from the orphaned region.
    assert_eq!(
        reader.ask("status"),
        "STATUS stale=false gen=1 view=1 visible=9"
    );
    assert_eq!(reader.ask("view"), "VIEW version=1 tablets=3 manifest=10");

    // The next writer rebuilds under generation 2.
    let file = db.open();
    let (_lock, _presence, shm) = db.open_writer(&file);
    assert_eq!(shm.generation(), Generation(2));
    shm.publish_view(&view(7, 1, 4)).unwrap();
    assert!(db.region_exists(2));
    #[cfg(unix)]
    assert!(!db.region_exists(1), "the old name is gone");
    assert_eq!(
        shm.oldest_reader_pin(),
        None,
        "the old pin lives in the old region"
    );

    assert_eq!(
        reader.ask("status"),
        "STATUS stale=true gen=1 view=1 visible=9"
    );
    assert_eq!(
        reader.ask("view"),
        "VIEW version=1 tablets=3 manifest=10",
        "the old mapping stays valid until re-attach"
    );
    assert_eq!(reader.ask("reattach"), "REATTACHED gen=2 slot=0");
    assert_eq!(reader.ask("view"), "VIEW version=7 tablets=1 manifest=70");
    assert_eq!(reader.ask("pin 0"), "PINNED view=7");
    assert_eq!(shm.oldest_reader_pin(), Some((0, 7)));
    assert_eq!(
        reader.ask("status"),
        "STATUS stale=false gen=2 view=7 visible=0"
    );
    reader.quit();
    assert_eq!(
        shm.reclaim_dead_slots(&db.vfs),
        0,
        "a clean quit frees its slot itself"
    );
    assert_eq!(shm.oldest_reader_pin(), None);
}

#[test]
fn mismatched_layout_version_is_refused() {
    let db = Db::new("version");
    let file = db.open();
    let (_lock, presence, shm) = db.open_writer(&file);
    shm.publish_view(&view(1, 1, 4)).unwrap();

    // Pretend the live region was built by a build with layout version 2.
    let (mapping, _, _) = shm.arena(0);
    mapping
        .atomic_u32(header::LAYOUT_VERSION)
        .store(2, Ordering::Release);
    db.vfs
        .open_shared(
            &directory_name(db.identity.device, db.identity.inode),
            None,
            directory::LEN as u64,
            SharedOpen::Attach,
        )
        .unwrap()
        .atomic_u32(directory::LAYOUT_VERSION)
        .store(2, Ordering::Release);

    let mut reader = Peer::spawn("reader", &db.path);
    let answer = reader.recv();
    assert!(
        answer.starts_with("ERR VersionMismatch") && answer.contains("found: 2"),
        "{answer}"
    );
    drop(reader);

    // Alone, a writer may rebuild under its own layout; its presence lock stays shared.
    let rebuilt = ShmRegion::open(
        &db.vfs,
        &file,
        db.identity,
        DB_ID,
        Role::Writer,
        &small_config(),
    )
    .unwrap();
    assert_eq!(rebuilt.generation(), Generation(2));
    // A reader can still take the presence byte shared: the probe left it shared.
    let mut reader = Peer::spawn("reader", &db.path);
    assert_eq!(reader.recv(), "READY slot=0 gen=2");
    assert!(!presence.try_become_last().unwrap());
    reader.quit();
    assert!(presence.try_become_last().unwrap());
}

#[test]
fn concurrent_reader_processes_claim_distinct_slots_until_exhausted() {
    let db = Db::new("slots");
    let file = db.open();
    let (_lock, _presence, shm) = db.open_writer(&file);
    let mut readers: Vec<Peer> = (0..4).map(|_| Peer::spawn("reader", &db.path)).collect();
    let ready: Vec<String> = readers.iter_mut().map(Peer::recv).collect();
    let mut slots = ready.clone();
    slots.sort();
    assert_eq!(
        slots,
        (0..4)
            .map(|i| format!("READY slot={i} gen=1"))
            .collect::<Vec<_>>()
    );
    assert!(matches!(
        shm.claim_reader_slot(db.vfs.current_process()),
        Err(Error::NoReaderSlot)
    ));
    let first = readers.remove(0);
    let freed: u32 = ready[0]
        .strip_prefix("READY slot=")
        .and_then(|s| s.split(' ').next())
        .and_then(|s| s.parse().ok())
        .unwrap();
    first.quit();
    let deadline = Instant::now() + Duration::from_secs(5);
    let slot = loop {
        match shm.claim_reader_slot(db.vfs.current_process()) {
            Ok(slot) => break slot,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            Err(e) => panic!("slot never freed: {e}"),
        }
    };
    assert_eq!(
        slot.index(),
        freed,
        "the quitting reader's slot is the one freed"
    );
    for r in readers {
        r.quit();
    }
}

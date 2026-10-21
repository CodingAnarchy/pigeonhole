//! Multi-process readers on real processes, through the public API (#693's crash matrix):
//! the test binary re-executes itself as writer and reader children (as `pigeonhole-shm`'s
//! `tests/multiprocess.rs` does), talks to them over stdin/stdout, and kills them with
//! `SIGKILL` / `TerminateProcess`. Real files and the platform's shared memory, so not run
//! under Miri.
//!
//! | scenario | expected |
//! |---|---|
//! | a reader killed while it holds a snapshot | the writer frees its slot (#833) and keeps committing past its memtable budget; a new reader attaches |
#![cfg(not(miri))]

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use pigeonhole::{
    Durability, Family, Options, Pigeonhole, PigeonholeReader, ReaderOptions, Snapshot, Table,
};

const CHILD_ENV: &str = "PIGEONHOLE_PROCESS_CHILD";
/// Every line a child prints for the parent starts with this.
const TAG: &str = "PHPROC ";
const VALUE: usize = 1024;

fn say(line: impl std::fmt::Display) {
    println!("{TAG}{line}");
    std::io::stdout().flush().unwrap();
}

fn key(i: u64) -> Vec<u8> {
    format!("row{i:08}").into_bytes()
}

fn writer_options() -> Options {
    Options::default()
        .shards(1)
        .pin_threads(false)
        .memtable_budget(4 << 20)
        .block_cache(4 << 20)
}

fn reader_options() -> ReaderOptions {
    ReaderOptions::default().block_cache(4 << 20)
}

fn table(db: &Pigeonhole) -> Table {
    db.table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap()
}

/// Writes rows `range`, each at `Buffered` (survives a process kill). Stops at the first
/// error and returns it with the row it failed on.
fn put(t: &Table, range: std::ops::Range<u64>) -> Result<(), (u64, pigeonhole::Error)> {
    for i in range {
        t.mutate(&key(i))
            .put("f", b"v", &[(i % 251) as u8; VALUE])
            .durability(Durability::Buffered)
            .commit()
            .map_err(|e| (i, e))?;
    }
    Ok(())
}

/// Runs in a child process. Without the environment variable it is an empty test.
#[test]
fn child_main() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let (role, path) = spec.split_once('|').unwrap();
    match role {
        "writer" => writer_child(Path::new(path)),
        "reader" => reader_child(Path::new(path)),
        other => panic!("unknown child role {other}"),
    }
    std::process::exit(0);
}

fn commands() -> impl Iterator<Item = String> {
    std::io::stdin().lines().map_while(Result::ok)
}

/// A writer that serves `put A B` (rows A..B, then `PUT B`), `flush`, and `quit` (a clean
/// close); or is killed.
fn writer_child(path: &Path) {
    let db = Pigeonhole::open(path, writer_options()).unwrap();
    let t = table(&db);
    say("READY");
    for cmd in commands() {
        let words: Vec<&str> = cmd.split_whitespace().collect();
        match words[..] {
            ["put", a, b] => match put(&t, a.parse().unwrap()..b.parse().unwrap()) {
                Ok(()) => say(format!("PUT {b}")),
                Err((i, e)) => say(format!("ERR {:?} at {i}", e.code())),
            },
            ["flush"] => {
                db.flush().unwrap();
                say("FLUSHED");
            }
            ["quit"] => break,
            _ => panic!("unknown command {cmd:?}"),
        }
    }
    drop(t);
    db.close().unwrap();
}

/// The rows of `t` as of `snapshot` (or now), or the error code the scan failed with.
fn count(t: &pigeonhole::ReadTable, snapshot: Option<&Snapshot>) -> String {
    let scan = t.scan_prefix(b"");
    let scan = match snapshot {
        Some(s) => scan.snapshot(s),
        None => scan,
    };
    let result = scan.iter().and_then(|it| {
        let mut n = 0u64;
        for row in it {
            row?;
            n += 1;
        }
        Ok(n)
    });
    match result {
        Ok(n) => format!("COUNT {n}"),
        Err(e) => format!("ERR {:?}", e.code()),
    }
}

/// A reader that serves `snap` (take a snapshot and hold it, `SNAP`), `count` (rows at the
/// held snapshot), `latest` (rows now), `drop` (release the snapshot), and `quit`; or is
/// killed.
fn reader_child(path: &Path) {
    let r: PigeonholeReader = Pigeonhole::open_reader(path, reader_options()).unwrap();
    let t = r.table("t").unwrap();
    let mut held: Option<Snapshot> = None;
    say("READY");
    for cmd in commands() {
        match cmd.trim() {
            "snap" => {
                held = None;
                match r.snapshot() {
                    Ok(s) => {
                        held = Some(s);
                        say("SNAP");
                    }
                    Err(e) => say(format!("ERR {:?}", e.code())),
                }
            }
            "count" => say(count(&t, Some(held.as_ref().expect("a held snapshot")))),
            "latest" => say(count(&t, None)),
            "drop" => {
                held = None;
                say("DROPPED");
            }
            "quit" => break,
            other => panic!("unknown command {other:?}"),
        }
    }
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
        let mut peer = Self {
            child,
            stdin,
            stdout,
        };
        assert_eq!(peer.recv(), "READY", "{role}");
        peer
    }

    /// The next tagged line. The harness prints its own text around ours, on the same line,
    /// so the tag is searched for, not anchored.
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
        let stdin = self.stdin.as_mut().expect("stdin open");
        writeln!(stdin, "{cmd}").unwrap();
        stdin.flush().unwrap();
        self.recv()
    }

    /// Sends `quit` and waits for a clean exit.
    fn quit(mut self) {
        let stdin = self.stdin.as_mut().expect("stdin open");
        writeln!(stdin, "quit").unwrap();
        drop(self.stdin.take());
        let status = self.child.wait().unwrap();
        assert!(status.success(), "child failed: {status}");
    }

    /// Kills the child (`SIGKILL` / `TerminateProcess`) and reaps it.
    fn kill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A fresh directory for one database, removed on drop.
struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "pigeonhole-proc-{}-{tag}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn db(&self) -> PathBuf {
        self.0.join("data.phdb")
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Rows that need several times the writer's 4 MiB memtable budget: with the first
/// memtables pinned by a reader, the arena cannot take them.
const PAST_BUDGET: u64 = 16 * 1024;

#[test]
fn a_killed_reader_does_not_stall_the_writer() {
    let dir = Dir::new("killed-reader");
    let db = Pigeonhole::open(
        dir.db(),
        writer_options().write_stall_timeout(Duration::from_secs(5)),
    )
    .unwrap();
    let t = table(&db);
    put(&t, 0..1000).unwrap();
    let mut reader = Peer::spawn("reader", &dir.db());
    assert_eq!(reader.ask("snap"), "SNAP");
    assert_eq!(reader.ask("count"), "COUNT 1000");
    reader.kill();
    // #833's symptom: without the slot freed, the dead reader's pin keeps the first
    // memtables, the arena runs out, and these commits stall and then fail `Busy` after the
    // 5 s timeout. With it, a stalled commit gets in after the writer's next liveness check
    // (the real `process_alive`, not the simulator's).
    let start = Instant::now();
    put(&t, 1000..1000 + PAST_BUDGET)
        .unwrap_or_else(|(i, e)| panic!("row {i} after {:?}: {e}", start.elapsed()));
    // A new reader attaches and sees everything.
    let mut next = Peer::spawn("reader", &dir.db());
    assert_eq!(next.ask("latest"), format!("COUNT {}", 1000 + PAST_BUDGET));
    next.quit();
    drop(t);
    db.close().unwrap();
}

//! Multi-process readers on real processes, through the public API (#693's crash matrix):
//! the test binary re-executes itself as writer and reader children (as `pigeonhole-shm`'s
//! `tests/multiprocess.rs` does), talks to them over stdin/stdout, and kills them with
//! `SIGKILL` / `TerminateProcess`. Real files and the platform's shared memory, so not run
//! under Miri.
//!
//! | scenario | expected |
//! |---|---|
//! | a reader killed while it holds a snapshot | the writer frees its slot (#833) and keeps committing past its memtable budget; a new reader attaches |
//! | a live reader holding a snapshot | the writer's commits stall, then fail `Busy` after `write_stall_timeout`; the reader leaving unblocks them |
//! | the writer killed with a reader attached | the reader's snapshot still reads; a new writer recovers; the old snapshot fails `SnapshotExpired`, a new one reads the recovered data |
//! | a reader process's snapshot across writer commits | a held snapshot keeps its count while new snapshots see each commit whole |
//! | a scan held across a writer kill and restart | the scan's next row fails `SnapshotExpired`; nothing from the new generation leaks into it |
//! | the writer killed at random points mid-stream of multi-row commits | readers and the recovered writer only ever see whole commits, and the writer recovers everything readers saw |
//!
//! The seeded SimVfs side of the matrix (reader and writer kills at seeded points, torn
//! state) is `pigeonhole-engine`'s `tests/reader_crash_matrix.rs`.
#![cfg(not(miri))]

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pigeonhole::{
    Durability, ErrorCode, Family, Options, Pigeonhole, PigeonholeReader, ReaderOptions, Snapshot,
    Table,
};

const CHILD_ENV: &str = "PIGEONHOLE_PROCESS_CHILD";
/// Every line a child prints for the parent starts with this.
const TAG: &str = "PHPROC ";
const VALUE: usize = 1024;

fn say(line: impl std::fmt::Display) {
    println!("{TAG}{line}");
    std::io::stdout().flush().unwrap();
}

/// How long one test may run before its watchdog aborts the process. The slowest test takes
/// about a minute on a loaded Windows runner; a hang there once ate the job's whole timeout
/// with no trace of where it was (#1028).
const TEST_LIMIT: Duration = Duration::from_secs(300);

/// Aborts the process if its test outlives [`TEST_LIMIT`], naming the test and the last
/// step it set. It writes to the stderr handle itself, which the harness's output capture
/// does not swallow.
struct Watchdog {
    step: Arc<Mutex<&'static str>>,
    done: Arc<AtomicBool>,
}

impl Watchdog {
    fn new(test: &'static str) -> Self {
        let step = Arc::new(Mutex::new("start"));
        let done = Arc::new(AtomicBool::new(false));
        let (at, finished) = (Arc::clone(&step), Arc::clone(&done));
        std::thread::spawn(move || {
            let started = Instant::now();
            while !finished.load(Ordering::Relaxed) {
                if started.elapsed() > TEST_LIMIT {
                    let _ = writeln!(
                        std::io::stderr(),
                        "{test} hung: still in step {:?} after {TEST_LIMIT:?}; aborting",
                        *at.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                    );
                    std::process::abort();
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        });
        Self { step, done }
    }

    fn step(&self, step: &'static str) {
        *self
            .step
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = step;
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.done.store(true, Ordering::Relaxed);
    }
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
            // Batches of K rows (one commit each) until killed.
            ["stream", k] => {
                let k: u64 = k.parse().unwrap();
                // Until killed: far more batches than any run reaches.
                for b in 0..u64::MAX {
                    let mut wb = db.write_batch();
                    for j in 0..k {
                        wb.put(&t, format!("s{b:08}-{j}").as_bytes(), "f", b"v", &[7; 256]);
                    }
                    wb.commit_with(Durability::Buffered).unwrap();
                    if b == 0 {
                        say("STREAMING");
                    }
                }
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
    let mut scan: Option<pigeonhole::RowIter<'static>> = None;
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
            // An owned scan of the held snapshot: its first row, then the rest.
            "scan-start" => {
                let snap = held.as_ref().expect("a held snapshot");
                let mut it = t
                    .scan_prefix(b"")
                    .snapshot(snap)
                    .into_owned()
                    .iter()
                    .unwrap();
                match it.next() {
                    Some(Ok(row)) => say(format!("ROW {}", String::from_utf8_lossy(row.key()))),
                    other => say(format!("ERR {other:?}")),
                }
                scan = Some(it);
            }
            "scan-rest" => {
                let it = scan.take().expect("a started scan");
                let mut n = 0u64;
                let mut failed = None;
                for row in it {
                    match row {
                        Ok(_) => n += 1,
                        Err(e) => {
                            failed = Some(e.code());
                            break;
                        }
                    }
                }
                match failed {
                    None => say(format!("COUNT {n}")),
                    Some(code) => say(format!("ERR {code:?} after {n}")),
                }
            }
            // The rows now, checked to be whole batches of `k`: `BATCHES n` or the error.
            cmd if cmd.starts_with("batches ") => {
                let k: u64 = cmd[8..].parse().unwrap();
                say(match count(&t, None).strip_prefix("COUNT ") {
                    Some(n) => {
                        let n: u64 = n.parse().unwrap();
                        assert_eq!(n % k, 0, "a reader saw part of a commit: {n} rows");
                        format!("BATCHES {}", n / k)
                    }
                    None => count(&t, None),
                });
            }
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

    /// Sends `quit` and waits for a clean exit, failing (and killing the child, on drop)
    /// if it has not exited within a minute.
    fn quit(mut self) {
        let stdin = self.stdin.as_mut().expect("stdin open");
        writeln!(stdin, "quit").unwrap();
        drop(self.stdin.take());
        let asked = Instant::now();
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(
                asked.elapsed() < Duration::from_secs(60),
                "the child did not exit within 60 s of quit"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
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
    let watchdog = Watchdog::new("a_killed_reader_does_not_stall_the_writer");
    let dir = Dir::new("killed-reader");
    // The default 30 s stall timeout, not a short one: on a loaded Windows runner in debug
    // one flush slice can take over a second and a single commit's room wait over 5 s with
    // the slot freed (#1028's runs). A pin that never lets go still fails the burst.
    let db = Pigeonhole::open(
        dir.db(),
        writer_options().write_stall_timeout(Duration::from_secs(30)),
    )
    .unwrap();
    let t = table(&db);
    put(&t, 0..1000).unwrap();
    watchdog.step("reader");
    let mut reader = Peer::spawn("reader", &dir.db());
    assert_eq!(reader.ask("snap"), "SNAP");
    assert_eq!(reader.ask("count"), "COUNT 1000");
    reader.kill();
    watchdog.step("reclaim");
    // The writer frees the dead reader's slot (its liveness lock released by the real OS,
    // not the simulator) at its next reclaim: at most every 250 ms, run when a flush or a
    // compaction finishes or a commit finds no room. Wait for it, flushing so one runs,
    // before the burst (#887): the burst then measures that the freed pin lets the writer
    // through, not how fast a slow runner flushes its way out of a stall that began before
    // the reclaim (on Windows the killed process read as alive in neither failure: it is
    // dead from `GetExitCodeProcess` at once).
    let killed = Instant::now();
    while db.engine_metrics().reader_slots_reclaimed == 0 {
        assert!(
            killed.elapsed() < Duration::from_secs(30),
            "the killed reader's slot was not reclaimed: {:?}",
            db.engine_metrics()
        );
        // A flush with nothing to write answers at once without a flush task, and so
        // without the reclaim a finished flush runs (#886's run): rewrite row 0 first, so
        // every flush writes and reclaims (the row count stays the same).
        put(&t, 0..1).unwrap();
        db.flush().unwrap();
        std::thread::sleep(Duration::from_millis(300));
    }
    // #833's symptom: without the slot freed, the dead reader's pin keeps the first
    // memtables, the arena runs out, and these commits stall and then fail `Busy` after the
    // timeout. With it freed, they go through.
    watchdog.step("burst");
    let start = Instant::now();
    put(&t, 1000..1000 + PAST_BUDGET)
        .unwrap_or_else(|(i, e)| panic!("row {i} after {:?}: {e}", start.elapsed()));
    // A new reader attaches and sees everything.
    watchdog.step("new reader");
    let mut next = Peer::spawn("reader", &dir.db());
    assert_eq!(next.ask("latest"), format!("COUNT {}", 1000 + PAST_BUDGET));
    next.quit();
    watchdog.step("close");
    drop(t);
    db.close().unwrap();
}

#[test]
fn a_live_readers_snapshot_stalls_the_writer_into_busy_until_it_leaves() {
    let watchdog =
        Watchdog::new("a_live_readers_snapshot_stalls_the_writer_into_busy_until_it_leaves");
    let dir = Dir::new("live-reader");
    let db = Pigeonhole::open(
        dir.db(),
        writer_options().write_stall_timeout(Duration::from_secs(1)),
    )
    .unwrap();
    let t = table(&db);
    put(&t, 0..1000).unwrap();
    watchdog.step("reader");
    let mut reader = Peer::spawn("reader", &dir.db());
    assert_eq!(reader.ask("snap"), "SNAP");
    watchdog.step("put until Busy");
    // The held snapshot pins the first memtables: the writer runs out of arena and its
    // commits fail `Busy` after the stall timeout (D1 of #693: the reader's responsibility,
    // documented).
    let (row, e) = put(&t, 1000..1000 + PAST_BUDGET).expect_err("the writer never stalled");
    assert_eq!(e.code(), ErrorCode::Busy, "row {row}: {e}");
    // The snapshot still reads what it saw.
    watchdog.step("count");
    assert_eq!(reader.ask("count"), "COUNT 1000");
    // The reader leaving releases its slot and the memtables it pinned; the writer goes on.
    // A bounded amount, inside the room that frees: writing the whole rest would depend on
    // flushes keeping pace with the 1 s stall timeout, which a slow CI runner (Windows) does
    // not always (#840's CI). Even the first rows may wait past it there, while the flushes
    // the stall queued run (#1028's runs: one flush slice took over a second), so a `Busy`
    // is retried for up to 30 s; a pin the reader never let go of would still fail.
    watchdog.step("quit");
    reader.quit();
    watchdog.step("put after quit");
    let left = Instant::now();
    let mut next = row;
    while let Err((i, e)) = put(&t, next..row + 1000) {
        assert!(
            e.code() == ErrorCode::Busy && left.elapsed() < Duration::from_secs(30),
            "row {i} {:?} after the reader left: {e}",
            left.elapsed()
        );
        next = i;
    }
    watchdog.step("close");
    drop(t);
    db.close().unwrap();
}

#[test]
fn a_killed_writer_leaves_readers_serving_and_a_new_writer_expires_them() {
    let _watchdog =
        Watchdog::new("a_killed_writer_leaves_readers_serving_and_a_new_writer_expires_them");
    let dir = Dir::new("killed-writer");
    let mut writer = Peer::spawn("writer", &dir.db());
    assert_eq!(writer.ask("put 0 1000"), "PUT 1000");
    assert_eq!(writer.ask("flush"), "FLUSHED");
    // Rows only in the memtable (and the WAL) when the writer dies.
    assert_eq!(writer.ask("put 1000 1500"), "PUT 1500");
    let mut reader = Peer::spawn("reader", &dir.db());
    assert_eq!(reader.ask("snap"), "SNAP");
    assert_eq!(reader.ask("count"), "COUNT 1500");

    writer.kill();
    // The reader keeps serving its snapshot, and new snapshots see the last published
    // state (or `Busy`, if the writer had committed a root it never published: D149).
    assert_eq!(reader.ask("count"), "COUNT 1500");
    let latest = reader.ask("latest");
    assert!(
        latest == "COUNT 1500" || latest == "ERR Busy",
        "after the writer died: {latest}"
    );

    // A new writer recovers the WAL's rows and starts a new generation.
    let db = Pigeonhole::open(dir.db(), writer_options()).unwrap();
    let t = table(&db);
    assert_eq!(t.scan_prefix(b"").iter().unwrap().count(), 1500);
    put(&t, 1500..1600).unwrap();
    // The old snapshot may name extents the new writer freed: it fails, typed.
    assert_eq!(reader.ask("count"), "ERR SnapshotExpired");
    // A new snapshot re-attaches to the new generation.
    assert_eq!(reader.ask("snap"), "SNAP");
    assert_eq!(reader.ask("count"), "COUNT 1600");
    reader.quit();
    drop(t);
    db.close().unwrap();
}

#[test]
fn a_reader_process_sees_commits_whole_and_its_snapshot_stays_put() {
    let _watchdog = Watchdog::new("a_reader_process_sees_commits_whole_and_its_snapshot_stays_put");
    let dir = Dir::new("commits");
    // The reader's held snapshot pins every memtable the writer retires after it (a reader
    // process pins by view version), so the arena must hold all 3,510 rows: about 3.5 MiB
    // of 1 KiB values, plus each chunk's lost tail (13% with D302's 4 KiB chunks here).
    let db = Pigeonhole::open(dir.db(), writer_options().memtable_budget(8 << 20)).unwrap();
    let t = table(&db);
    let mut reader = Peer::spawn("reader", &dir.db());
    assert_eq!(reader.ask("snap"), "SNAP");
    let mut done = 0;
    for step in [10u64, 500, 3000] {
        put(&t, done..done + step).unwrap();
        done += step;
        if step == 500 {
            db.flush().unwrap();
        }
        // Visible once applied (D227): a new snapshot sees every row committed so far.
        assert_eq!(reader.ask("latest"), format!("COUNT {done}"));
        // The held snapshot (taken before any commit) still sees none.
        assert_eq!(reader.ask("count"), "COUNT 0");
    }
    reader.quit();
    drop(t);
    db.close().unwrap();
}

#[test]
fn a_scan_held_across_a_writer_restart_fails_expired() {
    let _watchdog = Watchdog::new("a_scan_held_across_a_writer_restart_fails_expired");
    let dir = Dir::new("scan-restart");
    let mut writer = Peer::spawn("writer", &dir.db());
    assert_eq!(writer.ask("put 0 3000"), "PUT 3000");
    assert_eq!(writer.ask("flush"), "FLUSHED");
    let mut reader = Peer::spawn("reader", &dir.db());
    assert_eq!(reader.ask("snap"), "SNAP");
    assert_eq!(reader.ask("scan-start"), "ROW row00000000");
    writer.kill();
    // A new writer rewrites the file under the scan: deletes, compacts, writes anew.
    let db = Pigeonhole::open(dir.db(), writer_options()).unwrap();
    let t = table(&db);
    // Buffered, as `put` writes: the scan's expiry, not durability, is under test.
    for i in 0..3000 {
        t.mutate(&key(i))
            .delete_row()
            .durability(Durability::Buffered)
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    put(&t, 5000..8000).unwrap();
    db.flush().unwrap();
    // The held scan fails typed, never reads the new generation's pages as its own.
    let rest = reader.ask("scan-rest");
    assert!(rest.starts_with("ERR SnapshotExpired"), "{rest}");
    assert_eq!(reader.ask("snap"), "SNAP");
    assert_eq!(reader.ask("count"), "COUNT 3000");
    reader.quit();
    drop(t);
    db.close().unwrap();
}

#[test]
fn a_writer_killed_mid_stream_leaves_only_whole_commits() {
    let _watchdog = Watchdog::new("a_writer_killed_mid_stream_leaves_only_whole_commits");
    const K: u64 = 8;
    let dir = Dir::new("stream-kill");
    let mut reader: Option<Peer> = None;
    let mut seen = 0u64;
    for (round, delay_ms) in [3u64, 17, 40, 1, 75].into_iter().enumerate() {
        let mut writer = Peer::spawn("writer", &dir.db());
        let r = reader.get_or_insert_with(|| Peer::spawn("reader", &dir.db()));
        let stdin = writer.stdin.as_mut().unwrap();
        writeln!(stdin, "stream {K}").unwrap();
        stdin.flush().unwrap();
        assert_eq!(writer.recv(), "STREAMING");
        // Readers only ever see whole commits while the writer streams.
        for _ in 0..3 {
            let b = r.ask(&format!("batches {K}"));
            assert!(
                b.starts_with("BATCHES") || b == "ERR Busy",
                "round {round}: {b}"
            );
        }
        std::thread::sleep(Duration::from_millis(delay_ms));
        writer.kill();
        // Still whole after the kill (the last published view), or busy.
        let b = r.ask(&format!("batches {K}"));
        assert!(
            b.starts_with("BATCHES") || b == "ERR Busy",
            "round {round}: {b}"
        );
        if let Some(n) = b.strip_prefix("BATCHES ") {
            seen = seen.max(n.parse().unwrap());
        }
        // A new writer recovers whole commits, at least every one a reader saw.
        let db = Pigeonhole::open(dir.db(), writer_options()).unwrap();
        let rows = table(&db).scan_prefix(b"s").iter().unwrap().count() as u64;
        assert_eq!(
            rows % K,
            0,
            "round {round}: recovered part of a commit ({rows} rows)"
        );
        assert!(rows / K >= seen, "round {round}: lost commits a reader saw");
        seen = rows / K;
        // The reader re-attaches to the new writer's generation and agrees. Asked while that
        // writer runs: on Windows a closed writer's shared memory is gone once no process
        // maps it, so a reader can't re-attach to a generation whose writer already left.
        assert_eq!(r.ask(&format!("batches {K}")), format!("BATCHES {seen}"));
        db.close().unwrap();
    }
    reader.take().unwrap().quit();
}

//! Helpers for the CLI tests: a temporary directory, running `phdb` in process, and
//! snapshots with the run-specific parts (paths, the database id, file sizes) masked.

#![allow(dead_code)]

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};

/// A directory under the system temp dir, removed when dropped.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(name: &str) -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("phdb-test-{}-{n}-{name}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A copy in `dir` of the fixture database `build` makes at the path it is given (named
/// `name`), built once per test binary under the target's temporary directory: the files a
/// crash or a clean close left (the data file, and any WAL sidecars), each test with its own
/// (device, inode). Fixtures many tests start from cost one build instead of one each.
pub fn fixture(
    template: &'static OnceLock<PathBuf>,
    name: &str,
    build: impl FnOnce(&Path),
    dir: &TempDir,
) -> PathBuf {
    let template = template.get_or_init(|| {
        let at = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "{}-{}-{name}",
            env!("CARGO_CRATE_NAME"),
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&at);
        std::fs::create_dir_all(&at).unwrap();
        let path = at.join(name);
        build(&path);
        path
    });
    for entry in std::fs::read_dir(template.parent().unwrap()).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), dir.path().join(entry.file_name())).unwrap();
    }
    dir.path().join(name)
}

/// What a `phdb` run produced.
#[derive(Debug)]
pub struct Run {
    pub status: u8,
    pub stdout: String,
    pub stderr: String,
}

/// Runs `phdb args…` in process.
pub fn phdb(args: &[&str]) -> Run {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let status = pigeonhole_cli::run(
        std::iter::once("phdb").chain(args.iter().copied()),
        &mut stdout,
        &mut stderr,
    );
    Run {
        status: status.code(),
        stdout: String::from_utf8(stdout).unwrap(),
        stderr: String::from_utf8(stderr).unwrap(),
    }
}

/// Runs `phdb args…` in process, keeping stdout as bytes (`dump --format arrow`).
pub fn phdb_bytes(args: &[&str]) -> (u8, Vec<u8>, String) {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let status = pigeonhole_cli::run(
        std::iter::once("phdb").chain(args.iter().copied()),
        &mut stdout,
        &mut stderr,
    );
    (status.code(), stdout, String::from_utf8(stderr).unwrap())
}

/// Asserts `text` matches the snapshot `name`, with `dir` (and so every database path in
/// it), the 32-hex-digit database id and the file-level byte counts masked.
pub fn snapshot(name: &str, dir: &TempDir, text: &str) {
    // Paths print with the platform's separators, and JSON escapes Windows' backslashes:
    // mask the directory in both spellings, then normalize the separator after it (only
    // there: other backslashes, such as the shell's \\help, are output).
    let plain = dir.path().display().to_string();
    let escaped = serde_json::to_string(&plain).unwrap();
    let text = text
        .replace(&escaped[1..escaped.len() - 1], "[DIR]")
        .replace(&plain, "[DIR]")
        .replace("[DIR]\\\\", "[DIR]/")
        .replace("[DIR]\\", "[DIR]/");
    let mut settings = insta::Settings::clone_current();
    // Relative to this file: `tests/snapshots/`.
    settings.set_snapshot_path("../snapshots");
    settings.set_prepend_module_to_snapshot(false);
    settings.add_filter(r"\b[0-9a-f]{32}\b", "[DB_ID]");
    // JSON and JSONL: `"file_bytes": 123`; table: `file_bytes      123`.
    settings.add_filter(r#"("?(?:file_bytes|free_bytes)"?:?\s*)\d+"#, "${1}[BYTES]");
    // A snapshot's seqno depends on the writes before it, which tests make differently.
    settings.add_filter(r#"("seqno":\s*)\d+"#, "${1}[SEQNO]");
    settings.bind(|| insta::assert_snapshot!(name, text));
}

/// A writer whose reader has gone away.
pub struct BrokenPipe;

impl Write for BrokenPipe {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::ErrorKind::BrokenPipe.into())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

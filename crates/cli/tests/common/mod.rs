//! Helpers for the CLI tests: a temporary directory, running `phdb` in process, and
//! snapshots with the run-specific parts (paths, the database id, file sizes) masked.

#![allow(dead_code)]

use std::io::{self, Write};
use std::path::{Path, PathBuf};
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

/// Asserts `text` matches the snapshot `name`, with `dir` (and so every database path in
/// it), the 32-hex-digit database id and the file-level byte counts masked.
pub fn snapshot(name: &str, dir: &TempDir, text: &str) {
    // Paths print with the platform's separators, and JSON escapes Windows' backslashes:
    // mask the directory in both spellings, then normalize the separators.
    let plain = dir.path().display().to_string();
    let escaped = serde_json::to_string(&plain).unwrap();
    let text = text
        .replace(&escaped[1..escaped.len() - 1], "[DIR]")
        .replace(&plain, "[DIR]")
        .replace("\\\\", "/")
        .replace('\\', "/");
    let mut settings = insta::Settings::clone_current();
    // Relative to this file: `tests/snapshots/`.
    settings.set_snapshot_path("../snapshots");
    settings.set_prepend_module_to_snapshot(false);
    settings.add_filter(r"\b[0-9a-f]{32}\b", "[DB_ID]");
    // JSON and JSONL: `"file_bytes": 123`; table: `file_bytes      123`.
    settings.add_filter(r#"("?(?:file_bytes|free_bytes)"?:?\s*)\d+"#, "${1}[BYTES]");
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

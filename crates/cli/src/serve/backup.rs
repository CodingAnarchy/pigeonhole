//! `Admin.Backup` and `Admin.BackupDownload` (D313 §9, ICR 0112).
//!
//! - **`Backup`** writes a consistent backup to a path inside `--backup-dir`: relative, plain
//!   components only (no `..`, no root), and not already there. With no `--backup-dir`, the
//!   server takes no backups to disk (`Unsupported`).
//! - **`BackupDownload`** takes a fresh backup into the server's own download directory,
//!   hashes it, and streams its bytes. The file is kept for `KEEP` after its last reader,
//!   so a broken download resumes from an offset with its `backup_id`; then the reaper
//!   deletes it. The last chunk carries the file's size and SHA-256 (perf287's S4).

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, Once, PoisonError, Weak};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use super::errors::ApiError;

/// How long a download's file is kept after its last reader, for a resume.
pub(crate) const KEEP: Duration = Duration::from_secs(300);

/// The most a download chunk carries (ICR 0112: up to 1 MiB).
pub(crate) const CHUNK: usize = 1 << 20;

/// A download's file, kept for resumes.
#[derive(Debug, Clone)]
pub(crate) struct Kept {
    pub path: PathBuf,
    pub total: u64,
    pub sha256: [u8; 32],
}

struct Entry {
    kept: Kept,
    readers: usize,
    last_used: Instant,
}

/// The server's backups: the `--backup-dir` for `Backup`, and the downloads it keeps.
pub(crate) struct Backups {
    dir: Option<PathBuf>,
    downloads: PathBuf,
    kept: Mutex<HashMap<String, Entry>>,
    reaper: Once,
}

impl std::fmt::Debug for Backups {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backups")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

impl Backups {
    /// Downloads go to a directory of the server's own: inside `--backup-dir` when there is
    /// one, else the system's temporary directory, named for the process.
    pub(crate) fn new(dir: Option<PathBuf>) -> Arc<Self> {
        let downloads = match &dir {
            Some(d) => d.join(".phdb-serve-downloads"),
            None => {
                std::env::temp_dir().join(format!("phdb-serve-downloads-{}", std::process::id()))
            }
        };
        Arc::new(Self {
            dir,
            downloads,
            kept: Mutex::new(HashMap::new()),
            reaper: Once::new(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        self.kept.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Where `Backup` writes `relative`: inside `--backup-dir`, and not there yet.
    pub(crate) fn target(&self, relative: &str) -> Result<PathBuf, ApiError> {
        let Some(dir) = &self.dir else {
            return Err(ApiError::unsupported(
                "this server takes no backups to disk: start it with --backup-dir",
            ));
        };
        let rel = Path::new(relative);
        let plain =
            !relative.is_empty() && rel.components().all(|c| matches!(c, Component::Normal(_)));
        if !plain {
            return Err(ApiError::invalid(
                "a backup path is relative to --backup-dir, with no `..`",
            ));
        }
        let path = dir.join(rel);
        if path.exists() {
            return Err(ApiError::invalid(format!(
                "{relative} already exists in --backup-dir"
            )));
        }
        Ok(path)
    }

    /// A new download's file, not yet written: in the download directory, under a fresh id.
    pub(crate) fn new_download(&self) -> Result<(String, PathBuf), ApiError> {
        std::fs::create_dir_all(&self.downloads)
            .map_err(|e| ApiError::internal(format!("the download directory: {e}")))?;
        let id = super::sessions::random_hex()?;
        let path = self.downloads.join(format!("{id}.phdb"));
        Ok((id, path))
    }

    /// Keeps a written download's file under `id`, for one reader now.
    pub(crate) fn keep(self: &Arc<Self>, id: &str, kept: Kept) {
        self.start_reaper();
        self.lock().insert(
            id.to_owned(),
            Entry {
                kept,
                readers: 1,
                last_used: Instant::now(),
            },
        );
    }

    /// The kept download `id`, for one more reader.
    pub(crate) fn open(&self, id: &str) -> Result<Kept, ApiError> {
        let mut kept = self.lock();
        let e = kept.get_mut(id).ok_or_else(|| {
            ApiError::not_found("no such backup download (it expired): start again")
        })?;
        e.readers += 1;
        e.last_used = Instant::now();
        Ok(e.kept.clone())
    }

    /// A reader of `id` is done.
    pub(crate) fn release(&self, id: &str) {
        if let Some(e) = self.lock().get_mut(id) {
            e.readers = e.readers.saturating_sub(1);
            e.last_used = Instant::now();
        }
    }

    /// Deletes the downloads unread for `KEEP` as of `now`.
    pub(crate) fn sweep(&self, now: Instant) {
        let gone: Vec<PathBuf> = {
            let mut kept = self.lock();
            let old: Vec<String> = kept
                .iter()
                .filter(|(_, e)| e.readers == 0 && now.duration_since(e.last_used) >= KEEP)
                .map(|(id, _)| id.clone())
                .collect();
            old.iter()
                .filter_map(|id| kept.remove(id).map(|e| e.kept.path))
                .collect()
        };
        for path in gone {
            let _ = std::fs::remove_file(path);
        }
    }

    fn start_reaper(self: &Arc<Self>) {
        let weak: Weak<Self> = Arc::downgrade(self);
        self.reaper.call_once(|| {
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                rt.spawn(async move {
                    let mut tick = tokio::time::interval(KEEP / 10);
                    loop {
                        tick.tick().await;
                        let Some(b) = weak.upgrade() else { return };
                        b.sweep(Instant::now());
                    }
                });
            }
        });
    }
}

/// A download as both protocols stream it: what it is (known before the first byte, so
/// HTTP can send it as headers), then its chunks.
#[derive(Debug)]
pub(crate) struct Download {
    pub id: String,
    pub total: u64,
    pub sha256: [u8; 32],
    pub offset: u64,
    pub chunks: tokio::sync::mpsc::Receiver<Result<super::proto::BackupChunk, ApiError>>,
}

impl futures_core::Stream for Download {
    type Item = Result<super::proto::BackupChunk, ApiError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.chunks.poll_recv(cx)
    }
}

impl Drop for Backups {
    /// The server is going: the downloads it kept for resumes go with it.
    fn drop(&mut self) {
        for e in self.lock().values() {
            let _ = std::fs::remove_file(&e.kept.path);
        }
    }
}

/// `path`'s size and SHA-256, read once (on the blocking pool).
pub(crate) fn digest(path: &Path) -> Result<(u64, [u8; 32]), ApiError> {
    let mut f = std::fs::File::open(path)
        .map_err(|e| ApiError::internal(format!("reading the backup: {e}")))?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; CHUNK];
    let mut total = 0u64;
    loop {
        let n = f
            .read(&mut buf)
            .map_err(|e| ApiError::internal(format!("reading the backup: {e}")))?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
        total += n as u64;
    }
    Ok((total, h.finalize().into()))
}

/// Up to `CHUNK` bytes of `path` from `offset` (on the blocking pool).
pub(crate) fn read_chunk(path: &Path, offset: u64) -> Result<Vec<u8>, ApiError> {
    let mut f = std::fs::File::open(path)
        .map_err(|e| ApiError::internal(format!("reading the backup: {e}")))?;
    f.seek(SeekFrom::Start(offset))
        .map_err(|e| ApiError::internal(format!("reading the backup: {e}")))?;
    let mut buf = Vec::with_capacity(CHUNK);
    f.take(CHUNK as u64)
        .read_to_end(&mut buf)
        .map_err(|e| ApiError::internal(format!("reading the backup: {e}")))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_paths_stay_inside_the_backup_dir() {
        let dir = std::env::temp_dir().join(format!("phdb-backups-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let b = Backups::new(Some(dir.clone()));
        assert_eq!(b.target("a.phdb").unwrap(), dir.join("a.phdb"));
        assert_eq!(b.target("sub/a.phdb").unwrap(), dir.join("sub/a.phdb"));
        for bad in ["", "../a.phdb", "/etc/a.phdb", "sub/../../a", "./a"] {
            assert_eq!(b.target(bad).unwrap_err().code, "InvalidArgument", "{bad}");
        }
        std::fs::write(dir.join("there.phdb"), b"x").unwrap();
        assert_eq!(b.target("there.phdb").unwrap_err().code, "InvalidArgument");
        let none = Backups::new(None);
        assert_eq!(none.target("a.phdb").unwrap_err().code, "Unsupported");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_download_is_kept_until_unread_for_keep() {
        let dir = std::env::temp_dir().join(format!("phdb-downloads-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let b = Backups::new(Some(dir.clone()));
        let (id, path) = b.new_download().unwrap();
        std::fs::write(&path, b"backup").unwrap();
        let (total, sha256) = digest(&path).unwrap();
        assert_eq!(total, 6);
        b.keep(
            &id,
            Kept {
                path: path.clone(),
                total,
                sha256,
            },
        );
        // Read: kept however long.
        b.sweep(Instant::now() + KEEP * 2);
        assert!(path.exists());
        b.release(&id);
        b.open(&id).unwrap();
        b.release(&id);
        b.sweep(Instant::now());
        assert!(path.exists(), "unread for less than KEEP");
        b.sweep(Instant::now() + KEEP);
        assert!(!path.exists());
        assert_eq!(b.open(&id).unwrap_err().code, "NotFound");
        assert_eq!(
            read_chunk(&dir.join("missing"), 0).unwrap_err().code,
            "Internal"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

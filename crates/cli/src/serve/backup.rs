//! `Admin.Backup` and `Admin.BackupDownload` (D313 §9, ICR 0112).
//!
//! Both need `--backup-dir`. Without it the server takes no backups (`Unsupported`): a
//! download's copy has to go somewhere the operator chose, never the shared temporary
//! directory (simp724's B1, perf287's D3 on #1527).
//! - **`Backup`** writes a consistent backup to a path inside `--backup-dir`: relative, plain
//!   components only (no `..`, no root), not already there, and not resolving outside the
//!   directory through a link in it. `.phdb-serve-downloads` is the server's own.
//! - **`BackupDownload`** takes a fresh backup into `<backup-dir>/.phdb-serve-downloads`
//!   (mode 0700, never a link), hashes it, and streams its bytes. The file is kept for
//!   `KEEP` after its last reader, so a broken download resumes from an offset with its
//!   `backup_id`; then the reaper deletes it. At most `KEPT_MAX` are kept: a fresh download
//!   at the cap evicts the oldest unread one, or is `Overloaded` if all are being read. A
//!   resume is never refused. Leftovers from a crash are deleted at startup.
//! - **Backups run on a thread of their own,** never the blocking pool that scan pages and
//!   `Sync` commits share, and at most `--max-backups` at once (perf287's D1).

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, Once, PoisonError, Weak};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use super::errors::ApiError;

/// How long a download's file is kept after its last reader, for a resume.
pub(crate) const KEEP: Duration = Duration::from_secs(300);

/// The most download copies kept at once (perf287's D2): each is a whole database.
pub(crate) const KEPT_MAX: usize = 2;

/// The most a download chunk carries (ICR 0112: up to 1 MiB).
pub(crate) const CHUNK: usize = 1 << 20;

/// The server's own directory inside `--backup-dir`, for downloads.
pub(crate) const DOWNLOADS: &str = ".phdb-serve-downloads";

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

/// The server's backups: `--backup-dir`, its download directory, and the downloads kept.
pub(crate) struct Backups {
    /// `--backup-dir`, canonical.
    dir: Option<PathBuf>,
    kept: Mutex<HashMap<String, Entry>>,
    /// Backups running at once (`--max-backups`).
    running: Arc<Semaphore>,
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
    /// The backups of a server with `dir` as `--backup-dir` (or none). With one, its download
    /// directory is made (mode 0700) or, if there, checked to be a real directory, and any
    /// downloads a crashed server left in it are deleted (perf287's D4): no id from another
    /// process can be resumed.
    pub(crate) fn new(dir: Option<&Path>, max_backups: usize) -> Result<Arc<Self>, String> {
        let dir = match dir {
            None => None,
            Some(d) => {
                let d = d
                    .canonicalize()
                    .map_err(|e| format!("--backup-dir {}: {e}", d.display()))?;
                prepare_downloads(&d.join(DOWNLOADS))?;
                Some(d)
            }
        };
        Ok(Arc::new(Self {
            dir,
            kept: Mutex::new(HashMap::new()),
            running: Arc::new(Semaphore::new(max_backups.max(1))),
            reaper: Once::new(),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        self.kept.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn dir(&self) -> Result<&Path, ApiError> {
        self.dir.as_deref().ok_or_else(|| {
            ApiError::unsupported("this server takes no backups: start it with --backup-dir")
        })
    }

    /// Where `Backup` writes `relative`, its directory made: inside `--backup-dir` (also
    /// after following any link in it), not the server's download directory, and not there
    /// yet. On the backup's own thread: it touches the filesystem.
    pub(crate) fn prepare_target(&self, relative: &str) -> Result<PathBuf, ApiError> {
        let dir = self.dir()?;
        let rel = Path::new(relative);
        let plain =
            !relative.is_empty() && rel.components().all(|c| matches!(c, Component::Normal(_)));
        if !plain {
            return Err(ApiError::invalid(
                "a backup path is relative to --backup-dir, with no `..`",
            ));
        }
        if rel.components().next() == Some(Component::Normal(DOWNLOADS.as_ref())) {
            return Err(ApiError::invalid(format!(
                "{DOWNLOADS} in --backup-dir is the server's own"
            )));
        }
        let path = dir.join(rel);
        if path.exists() {
            return Err(ApiError::invalid(format!(
                "{relative} already exists in --backup-dir"
            )));
        }
        let parent = path.parent().unwrap_or(dir);
        std::fs::create_dir_all(parent)
            .map_err(|e| ApiError::internal(format!("the backup's directory: {e}")))?;
        let resolved = parent
            .canonicalize()
            .map_err(|e| ApiError::internal(format!("the backup's directory: {e}")))?;
        if !resolved.starts_with(dir) {
            return Err(ApiError::invalid(format!(
                "{relative} leads outside --backup-dir through a link"
            )));
        }
        Ok(path)
    }

    /// A new download's file, not yet written, under a fresh id. At `KEPT_MAX` copies the
    /// oldest unread one goes; with every copy being read, a fresh download is `Overloaded`.
    pub(crate) fn new_download(&self) -> Result<(String, PathBuf), ApiError> {
        let dir = self.dir()?.join(DOWNLOADS);
        {
            let mut kept = self.lock();
            while kept.len() >= KEPT_MAX {
                let oldest = kept
                    .iter()
                    .filter(|(_, e)| e.readers == 0)
                    .min_by_key(|(_, e)| e.last_used)
                    .map(|(id, _)| id.clone());
                let Some(id) = oldest else {
                    return Err(ApiError::overloaded(format!(
                        "the server keeps at most {KEPT_MAX} backup downloads, all being read: \
                         resume one, or try again later"
                    )));
                };
                if let Some(e) = kept.remove(&id) {
                    let _ = std::fs::remove_file(&e.kept.path);
                }
            }
        }
        let id = super::sessions::random_hex()?;
        let path = dir.join(format!("{id}.phdb"));
        Ok((id, path))
    }

    /// Runs `backup` on a thread of its own, holding one of `--max-backups`: never a
    /// blocking-pool thread (perf287's D1). Past the limit, `Overloaded`.
    pub(crate) async fn on_backup_thread<T: Send + 'static>(
        &self,
        backup: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
    ) -> Result<T, ApiError> {
        let Ok(permit) = Arc::clone(&self.running).try_acquire_owned() else {
            return Err(ApiError::overloaded(
                "the server is already taking a backup (--max-backups)",
            ));
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        std::thread::Builder::new()
            .name("phdb-serve-backup".into())
            .spawn(move || {
                let r = backup();
                drop(permit);
                let _ = tx.send(r);
            })
            .map_err(|e| ApiError::internal(format!("a backup thread: {e}")))?;
        rx.await
            .unwrap_or_else(|_| Err(ApiError::internal("the backup's thread ended early")))
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

/// The download directory, ready: made with mode 0700, or a real directory already (never a
/// link: another user's link would send the copies elsewhere), cleared of downloads a
/// crashed server left, and its mode set to 0700.
fn prepare_downloads(path: &Path) -> Result<(), String> {
    let bad = |e: std::io::Error| format!("{}: {e}", path.display());
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.file_type().is_symlink() || !m.is_dir() => {
            return Err(format!(
                "{} must be a directory of the server's own, not a link or a file",
                path.display()
            ));
        }
        Ok(_) => {
            for entry in std::fs::read_dir(path).map_err(bad)? {
                let entry = entry.map_err(bad)?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                let ours = name.len() == 32 + ".phdb".len()
                    && name.ends_with(".phdb")
                    && name[..32].bytes().all(|b| b.is_ascii_hexdigit());
                if ours {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        Err(_) => {
            let mut b = std::fs::DirBuilder::new();
            #[cfg(unix)]
            std::os::unix::fs::DirBuilderExt::mode(&mut b, 0o700);
            b.create(path).map_err(bad)?;
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(bad)?;
    }
    Ok(())
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

    fn fresh(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("phdb-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.canonicalize().unwrap()
    }

    #[test]
    fn backup_paths_stay_inside_the_backup_dir() {
        let dir = fresh("backups");
        let b = Backups::new(Some(&dir), 1).unwrap();
        assert_eq!(b.prepare_target("a.phdb").unwrap(), dir.join("a.phdb"));
        assert_eq!(
            b.prepare_target("sub/a.phdb").unwrap(),
            dir.join("sub/a.phdb")
        );
        for bad in [
            "",
            "../a.phdb",
            "/etc/a.phdb",
            "sub/../../a",
            "./a",
            ".phdb-serve-downloads/x",
        ] {
            assert_eq!(
                b.prepare_target(bad).unwrap_err().code,
                "InvalidArgument",
                "{bad}"
            );
        }
        std::fs::write(dir.join("there.phdb"), b"x").unwrap();
        assert_eq!(
            b.prepare_target("there.phdb").unwrap_err().code,
            "InvalidArgument"
        );
        // A link in --backup-dir that leads outside it is refused (simp724 on #1527).
        #[cfg(unix)]
        {
            let outside = fresh("outside");
            std::os::unix::fs::symlink(&outside, dir.join("out")).unwrap();
            let e = b.prepare_target("out/a.phdb").unwrap_err();
            assert!(e.message.contains("outside"), "{e:?}");
            let _ = std::fs::remove_dir_all(&outside);
        }
        let none = Backups::new(None, 1).unwrap();
        assert_eq!(
            none.prepare_target("a.phdb").unwrap_err().code,
            "Unsupported"
        );
        assert_eq!(none.new_download().unwrap_err().code, "Unsupported");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_download_directory_is_private_and_cleared_at_startup() {
        let dir = fresh("downloads");
        let downloads = dir.join(DOWNLOADS);
        std::fs::create_dir(&downloads).unwrap();
        let leftover = downloads.join(format!("{}.phdb", "a".repeat(32)));
        let theirs = downloads.join("notes.txt");
        std::fs::write(&leftover, b"old").unwrap();
        std::fs::write(&theirs, b"keep").unwrap();
        Backups::new(Some(&dir), 1).unwrap();
        assert!(
            !leftover.exists(),
            "a crashed server's download goes (perf287's D4)"
        );
        assert!(theirs.exists(), "only the server's own files");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&downloads).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700);
            // A link in its place is refused at startup (simp724's B1).
            let other = fresh("elsewhere");
            let linked = fresh("linked");
            std::os::unix::fs::symlink(&other, linked.join(DOWNLOADS)).unwrap();
            assert!(Backups::new(Some(&linked), 1).is_err());
            let _ = std::fs::remove_dir_all(&other);
            let _ = std::fs::remove_dir_all(&linked);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kept_downloads_are_capped_and_kept_until_unread_for_keep() {
        let dir = fresh("kept");
        let b = Backups::new(Some(&dir), 1).unwrap();
        let mut ids = Vec::new();
        for _ in 0..KEPT_MAX {
            let (id, path) = b.new_download().unwrap();
            std::fs::write(&path, b"backup").unwrap();
            let (total, sha256) = digest(&path).unwrap();
            assert_eq!(total, 6);
            b.keep(
                &id,
                Kept {
                    path,
                    total,
                    sha256,
                },
            );
            ids.push(id);
        }
        // All being read: a fresh download is refused, a resume isn't (perf287's D2).
        assert_eq!(b.new_download().unwrap_err().code, "Overloaded");
        b.open(&ids[0]).unwrap();
        // One unread: the fresh download evicts it.
        b.release(&ids[1]);
        let (_, fresh_path) = b.new_download().unwrap();
        assert!(b.open(&ids[1]).is_err(), "the unread copy was evicted");
        drop(fresh_path);
        // Kept while read, however long; then KEEP after the last reader.
        b.sweep(Instant::now() + KEEP * 2);
        assert!(b.open(&ids[0]).is_ok());
        b.release(&ids[0]);
        b.release(&ids[0]);
        b.release(&ids[0]);
        b.sweep(Instant::now());
        assert!(b.open(&ids[0]).is_ok(), "unread for less than KEEP");
        b.release(&ids[0]);
        b.sweep(Instant::now() + KEEP);
        assert_eq!(b.open(&ids[0]).unwrap_err().code, "NotFound");
        assert_eq!(
            read_chunk(&dir.join("missing"), 0).unwrap_err().code,
            "Internal"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn backups_run_one_at_a_time_off_the_blocking_pool() {
        let dir = fresh("running");
        let b = Backups::new(Some(&dir), 1).unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let (go, wait) = std::sync::mpsc::channel::<()>();
            let first = b.on_backup_thread(move || {
                let _ = wait.recv();
                Ok(std::thread::current().name().map(str::to_owned))
            });
            tokio::pin!(first);
            // Start the first; it holds the one --max-backups permit.
            assert!(futures_poll(&mut first).is_none());
            let second = b.on_backup_thread(|| Ok::<_, ApiError>(())).await;
            assert_eq!(second.unwrap_err().code, "Overloaded");
            go.send(()).unwrap();
            let name = first.await.unwrap();
            assert_eq!(name.as_deref(), Some("phdb-serve-backup"));
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Polls `f` once.
    fn futures_poll<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        match std::pin::Pin::new(f).poll(&mut cx) {
            std::task::Poll::Ready(v) => Some(v),
            std::task::Poll::Pending => None,
        }
    }
}

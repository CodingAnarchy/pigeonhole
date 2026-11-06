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
//!   `backup_id`; then the reaper deletes it. At most `KEPT_MAX` are kept, counting copies
//!   still being written (so that's the most on disk at once): a fresh download at the cap
//!   evicts the oldest unread one, or is `Overloaded` if none can go. A resume is never
//!   refused. A copy belongs to this table from the start, not to the request, so a client
//!   that gives up mid-backup leaves an unread copy the reaper deletes. Leftovers from a
//!   crash are deleted at startup.
//! - **Backups run on a thread of their own,** never the blocking pool that scan pages and
//!   `Sync` commits share, and at most `--max-backups` at once (perf287's D1).

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, Once, PoisonError, Weak};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

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
    /// A replica bootstrap's feed token (#1436): what the replica follows the primary from,
    /// returned with every chunk, a resume's too. Empty for an ordinary backup.
    pub feed_token: Vec<u8>,
}

struct Entry {
    kept: Kept,
    /// Written and hashed: until then the backup thread owns it, and it counts against
    /// `KEPT_MAX` but is neither read, evicted nor reaped.
    ready: bool,
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
        let outside = || {
            ApiError::invalid(format!(
                "{relative} leads outside --backup-dir through a link"
            ))
        };
        let io = |e: std::io::Error| ApiError::internal(format!("the backup's directory: {e}"));
        // Before creating anything: the deepest part that exists already must resolve inside
        // the directory, so a link can't make the server create directories elsewhere
        // (simp724's N1 on #1527).
        let mut existing = parent;
        while !existing.exists() {
            existing = existing.parent().unwrap_or(dir);
        }
        if !existing.canonicalize().map_err(io)?.starts_with(dir) {
            return Err(outside());
        }
        std::fs::create_dir_all(parent).map_err(io)?;
        if !parent.canonicalize().map_err(io)?.starts_with(dir) {
            return Err(outside());
        }
        Ok(path)
    }

    /// One of `--max-backups`, or `Overloaded`.
    fn permit(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        Arc::clone(&self.running).try_acquire_owned().map_err(|_| {
            ApiError::overloaded("the server is already taking a backup (--max-backups)")
        })
    }

    /// Runs `backup` on a thread of its own, holding `permit`: never a blocking-pool thread
    /// (perf287's D1).
    fn spawn<T: Send + 'static>(
        permit: OwnedSemaphorePermit,
        backup: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
    ) -> Result<oneshot::Receiver<Result<T, ApiError>>, ApiError> {
        let (tx, rx) = oneshot::channel();
        std::thread::Builder::new()
            .name("phdb-serve-backup".into())
            .spawn(move || {
                let r = backup();
                // The permit before the result: a requester that has its result and asks
                // again at once must find the slot free, not `Overloaded` (#1527's H1 sent
                // first, and a client's next backup could be refused; the flake on #1549).
                // A fresh download's copy needs no permit to stay safe: `finish` already
                // made the requester its reader, so nothing can evict it before the handoff.
                // A requester that's gone drops the result here, which gives that reader up.
                drop(permit);
                let _ = tx.send(r);
            })
            .map_err(|e| ApiError::internal(format!("a backup thread: {e}")))?;
        Ok(rx)
    }

    /// Runs `backup` (an `Admin.Backup`) on its own thread, one of `--max-backups`.
    pub(crate) async fn on_backup_thread<T: Send + 'static>(
        &self,
        backup: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
    ) -> Result<T, ApiError> {
        let rx = Self::spawn(self.permit()?, backup)?;
        rx.await
            .unwrap_or_else(|_| Err(ApiError::internal("the backup's thread ended early")))
    }

    /// A fresh download's copy: `take` writes the backup to the path given, on its own thread,
    /// and the copy is hashed there too. Returns its id and file, with the caller as its
    /// reader.
    ///
    /// The copy belongs to the table from the start, never to the request (perf287's E1,
    /// simp724's N3 on #1527): it's reserved before the backup, counting against `KEPT_MAX`
    /// while it's written (E3), and the backup thread marks it ready, or deletes it if the
    /// backup fails, whether or not the request is still there. A client that gave up leaves
    /// an unread copy, which the reaper deletes after `KEEP`. The `--max-backups` permit is
    /// taken before anything is evicted for the copy (E2).
    pub(crate) async fn fresh_download(
        self: &Arc<Self>,
        take: impl FnOnce(&Path) -> Result<Vec<u8>, ApiError> + Send + 'static,
    ) -> Result<(String, Kept), ApiError> {
        let permit = self.permit()?;
        let (id, path) = self.reserve()?;
        self.start_reaper();
        let me = Arc::clone(self);
        let (tid, tpath) = (id.clone(), path.clone());
        let rx = Self::spawn(permit, move || {
            // Discards the copy unless it's finished: a failed backup, or a panic in one.
            struct Pending(Arc<Backups>, String, bool);
            impl Drop for Pending {
                fn drop(&mut self) {
                    if !self.2 {
                        self.0.discard(&self.1);
                    }
                }
            }
            let mut pending = Pending(me, tid, false);
            let feed_token = take(&tpath)?;
            let (total, sha256) = digest(&tpath)?;
            let kept = pending.0.finish(&pending.1, total, sha256, feed_token);
            pending.2 = true;
            let kept = kept.ok_or_else(|| ApiError::internal("the backup's copy vanished"))?;
            Ok(Handoff {
                backups: Arc::clone(&pending.0),
                id: pending.1.clone(),
                kept: Some(kept),
            })
        })?;
        let handoff = rx
            .await
            .unwrap_or_else(|_| Err(ApiError::internal("the backup's thread ended early")))?;
        Ok((id, handoff.take()))
    }

    /// A new copy's id and file, reserved (not ready) in the table. At `KEPT_MAX` copies,
    /// counting those being written, the oldest ready unread one goes; with none to evict, a
    /// fresh download is `Overloaded`.
    fn reserve(&self) -> Result<(String, PathBuf), ApiError> {
        let dir = self.dir()?.join(DOWNLOADS);
        let id = super::sessions::random_hex()?;
        let path = dir.join(format!("{id}.phdb"));
        let mut kept = self.lock();
        while kept.len() >= KEPT_MAX {
            let oldest = kept
                .iter()
                .filter(|(_, e)| e.ready && e.readers == 0)
                .min_by_key(|(_, e)| e.last_used)
                .map(|(id, _)| id.clone());
            let Some(old) = oldest else {
                return Err(ApiError::overloaded(format!(
                    "the server keeps at most {KEPT_MAX} backup downloads, all in use: \
                     resume one, or try again later"
                )));
            };
            if let Some(e) = kept.remove(&old) {
                let _ = std::fs::remove_file(&e.kept.path);
            }
        }
        kept.insert(
            id.clone(),
            Entry {
                kept: Kept {
                    path: path.clone(),
                    total: 0,
                    sha256: [0; 32],
                    feed_token: Vec::new(),
                },
                ready: false,
                readers: 0,
                last_used: Instant::now(),
            },
        );
        Ok((id, path))
    }

    /// The backup thread wrote and hashed copy `id`: it's ready, and its requester is its
    /// reader from this moment, so no other download can evict it before the requester has
    /// it (simp724's H1, perf287's nit on #1527). `None` if it's gone.
    fn finish(&self, id: &str, total: u64, sha256: [u8; 32], feed_token: Vec<u8>) -> Option<Kept> {
        let mut kept = self.lock();
        let e = kept.get_mut(id)?;
        e.kept.total = total;
        e.kept.sha256 = sha256;
        e.kept.feed_token = feed_token;
        e.ready = true;
        e.readers = 1;
        e.last_used = Instant::now();
        Some(e.kept.clone())
    }

    /// Copy `id` won't be finished: it goes, and its file.
    fn discard(&self, id: &str) {
        if let Some(e) = self.lock().remove(id) {
            let _ = std::fs::remove_file(&e.kept.path);
        }
    }
}

/// A finished copy on its way to its requester, who is its reader. Dropped untaken (the
/// requester went away), it gives that reader up, so the reaper deletes the copy after
/// `KEEP` unread.
struct Handoff {
    backups: Arc<Backups>,
    id: String,
    kept: Option<Kept>,
}

impl Handoff {
    fn take(mut self) -> Kept {
        self.kept.take().expect("taken once")
    }
}

impl Drop for Handoff {
    fn drop(&mut self) {
        if self.kept.is_some() {
            self.backups.release(&self.id);
        }
    }
}

impl Backups {
    /// The kept download `id`, for one more reader.
    pub(crate) fn open(&self, id: &str) -> Result<Kept, ApiError> {
        let mut kept = self.lock();
        let e = kept.get_mut(id).filter(|e| e.ready).ok_or_else(|| {
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
                .filter(|(_, e)| {
                    e.ready && e.readers == 0 && now.duration_since(e.last_used) >= KEEP
                })
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
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                std::fs::DirBuilder::new()
                    .mode(0o700)
                    .create(path)
                    .map_err(bad)?;
            }
            #[cfg(not(unix))]
            std::fs::create_dir(path).map_err(bad)?;
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
            // Nor is a directory made out there on the way (simp724's N1).
            let e = b.prepare_target("out/new/a.phdb").unwrap_err();
            assert!(e.message.contains("outside"), "{e:?}");
            assert!(!outside.join("new").exists(), "nothing created outside");
            let _ = std::fs::remove_dir_all(&outside);
        }
        let none = Backups::new(None, 1).unwrap();
        assert_eq!(
            none.prepare_target("a.phdb").unwrap_err().code,
            "Unsupported"
        );
        assert_eq!(none.reserve().unwrap_err().code, "Unsupported");
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

    /// A fresh download whose "backup" writes `bytes`, run to its end.
    fn download(
        rt: &tokio::runtime::Runtime,
        b: &Arc<Backups>,
        bytes: &'static [u8],
    ) -> Result<(String, Kept), ApiError> {
        rt.block_on(b.fresh_download(move |p| {
            std::fs::write(p, bytes)
                .map(|()| Vec::new())
                .map_err(|e| ApiError::internal(e.to_string()))
        }))
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn kept_downloads_are_capped_and_kept_until_unread_for_keep() {
        let dir = fresh("kept");
        let rt = runtime();
        let b = Backups::new(Some(&dir), 1).unwrap();
        let mut ids = Vec::new();
        for _ in 0..KEPT_MAX {
            let (id, kept) = download(&rt, &b, b"backup").unwrap();
            assert_eq!(kept.total, 6);
            assert_eq!(digest(&kept.path).unwrap().1, kept.sha256);
            ids.push(id);
        }
        // All being read: a fresh download is refused, a resume isn't (perf287's D2).
        assert_eq!(download(&rt, &b, b"x").unwrap_err().code, "Overloaded");
        b.open(&ids[0]).unwrap();
        // One unread: the fresh download evicts it.
        b.release(&ids[1]);
        let (newest, _) = download(&rt, &b, b"x").unwrap();
        assert!(b.open(&ids[1]).is_err(), "the unread copy was evicted");
        b.release(&newest);
        // Kept while read, however long; then KEEP after the last reader.
        b.sweep(Instant::now() + KEEP * 2);
        assert!(b.open(&ids[0]).is_ok());
        for _ in 0..3 {
            b.release(&ids[0]);
        }
        b.sweep(Instant::now());
        assert!(b.open(&ids[0]).is_ok(), "unread for less than KEEP");
        b.release(&ids[0]);
        b.sweep(Instant::now() + KEEP);
        assert_eq!(b.open(&ids[0]).unwrap_err().code, "NotFound");
        // A failed backup leaves nothing behind.
        let e = rt
            .block_on(b.fresh_download(|_| Err(ApiError::internal("disk full"))))
            .unwrap_err();
        assert_eq!(e.message, "disk full");
        // (The sweep above reaped every unread copy; the failed one is discarded, file and all.)
        assert!(b.lock().is_empty(), "a failed backup leaves no copy");
        assert_eq!(std::fs::read_dir(dir.join(DOWNLOADS)).unwrap().count(), 0);
        assert_eq!(
            read_chunk(&dir.join("missing"), 0).unwrap_err().code,
            "Internal"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_finished_copy_is_its_requesters_before_another_download_can_evict_it() {
        // simp724's H1 on #1527: between the backup thread finishing a copy and its
        // requester opening it, another fresh download at KEPT_MAX must not evict it (its
        // requester would get NotFound for its own download). The copy is finished with its
        // requester as reader; a handoff dropped untaken gives that reader up.
        let dir = fresh("handoff");
        let b = Backups::new(Some(&dir), KEPT_MAX + 1).unwrap();
        let mut handoffs = Vec::new();
        for _ in 0..KEPT_MAX {
            let (id, path) = b.reserve().unwrap();
            std::fs::write(&path, b"backup").unwrap();
            let kept = b.finish(&id, 6, [0; 32], Vec::new()).unwrap();
            // Finished, not yet taken by its requester.
            handoffs.push(Handoff {
                backups: Arc::clone(&b),
                id,
                kept: Some(kept),
            });
        }
        assert_eq!(
            b.reserve().unwrap_err().code,
            "Overloaded",
            "nothing finished-but-untaken is evicted"
        );
        // The first requester takes its copy; the second went away.
        let first = handoffs.remove(0);
        let first_id = first.id.clone();
        let kept = first.take();
        drop(handoffs);
        assert_eq!(b.open(&first_id).unwrap().path, kept.path);
        // The given-up copy is unread now: the next fresh download evicts it, not the first.
        let (_, _) = b.reserve().unwrap();
        assert!(b.open(&first_id).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_download_given_up_mid_backup_leaves_a_copy_the_reaper_deletes() {
        // perf287's E1 and simp724's N3 on #1527: the client goes (its request future is
        // dropped) while the backup is still being written. The copy is the table's, not the
        // request's: the backup thread finishes it, nobody reads it, and the reaper deletes it.
        let dir = fresh("given-up");
        let rt = runtime();
        let b = Backups::new(Some(&dir), 1).unwrap();
        let (go, wait) = std::sync::mpsc::channel::<()>();
        let (written, done) = std::sync::mpsc::channel::<PathBuf>();
        rt.block_on(async {
            let request = b.fresh_download(move |p| {
                let _ = wait.recv();
                std::fs::write(p, b"backup").map_err(|e| ApiError::internal(e.to_string()))?;
                let _ = written.send(p.to_owned());
                Ok(Vec::new())
            });
            tokio::pin!(request);
            assert!(
                futures_poll(&mut request).is_none(),
                "the backup is running"
            );
        });
        // The request is gone; the copy is reserved, so it counts against the cap (E3).
        assert_eq!(b.lock().len(), 1);
        go.send(()).unwrap();
        let path = done.recv_timeout(Duration::from_secs(5)).unwrap();
        // The thread marks it ready once it's hashed, just after writing, for its requester;
        // the requester is gone, so the handoff gives that reader up: nobody reads it.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !b.lock().values().any(|e| e.ready && e.readers == 0) {
            assert!(
                Instant::now() < deadline,
                "the copy was never finished and given up"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        b.sweep(Instant::now() + KEEP);
        assert!(b.lock().is_empty());
        assert!(!path.exists(), "the reaper deleted the copy");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_backup_frees_its_slot_before_its_requester_sees_the_result() {
        // The flake on #1549: with --max-backups 1, a client that got its result and asked
        // again at once was refused (`Overloaded`) while the finished backup's thread still
        // held the permit. The permit is released before the result is sent, so the slot is
        // free whenever a requester has its result, every time.
        let dir = fresh("slot");
        let rt = runtime();
        let b = Backups::new(Some(&dir), 1).unwrap();
        for i in 0..200 {
            let (id, _) = download(&rt, &b, b"x").unwrap_or_else(|e| panic!("download {i}: {e:?}"));
            assert_eq!(
                b.running.available_permits(),
                1,
                "download {i}'s slot is free"
            );
            b.release(&id);
            rt.block_on(b.on_backup_thread(|| Ok::<_, ApiError>(())))
                .unwrap_or_else(|e| panic!("backup {i}: {e:?}"));
            assert_eq!(
                b.running.available_permits(),
                1,
                "backup {i}'s slot is free"
            );
            b.sweep(Instant::now() + KEEP);
        }
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

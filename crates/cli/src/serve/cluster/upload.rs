//! `Admin.PartitionUpload` and `Admin.PartitionUploadStatus` (D317: a restore's and a
//! relocation's partition files, streamed in; ICR 0117's "partition upload").
//!
//! A partition is a set of files: the main file, its WAL sidecars and its feed sidecar,
//! named by their suffix of the partition's database path (`""`, `-wal-<n>`, `-feed`). An
//! upload carries one of them, with a header naming the whole set (its manifest) and the
//! identity the set installs with.
//! - **Staged, never served:** the bytes go to `<cluster DIR>/.phdb-incoming/<id>.part`
//!   (the directory mode 0700, never a link). Once every byte arrived and its SHA-256
//!   matched, the file is synced and renamed `<partition>.phdb<suffix>.ready`, beside its
//!   header (`….ready.header`, JSON). A newer upload of the same file replaces it.
//! - **Installed later, elsewhere:** D317 ticket 6's stamp (relocate's first step on the
//!   new node, and restore's) installs the set once every file its manifest names is ready
//!   with one header, checking it against the stamp's partition and epoch, then raises the
//!   epoch. [`ready_set`] is how it reads the staging. The header's epoch is the one the
//!   copy is *of*, the main file's own.
//! - **Accepted only while the partition isn't open here,** with at most one upload of a
//!   file of it at a time, and at most `--max-uploads` uploads at once (more are
//!   `Overloaded`).
//! - **Resumes:** [`Uploads::status`] says how many bytes of an upload id the node holds; the
//!   client sends from there, with the same header. Incomplete uploads not touched for
//!   [`KEEP`] are deleted, and so is every incomplete one at the node's start.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::map;
use crate::serve::errors::ApiError;
use crate::serve::proto::{
    PartitionFile, PartitionUploadChunk, PartitionUploadHeader, PartitionUploadPurpose,
    PartitionUploadReply, PartitionUploadStatusReply,
};

/// The staging directory inside a node's cluster directory.
pub(crate) const INCOMING: &str = ".phdb-incoming";

/// How long an incomplete upload is kept after it was last written to, for a resume.
pub(crate) const KEEP: Duration = Duration::from_secs(300);

/// The most one chunk carries: half of `--max-request-bytes`' default (1 MiB), so a chunk
/// and its framing fit a request under it. A node whose `--max-request-bytes` is lower
/// takes less ([`max_chunk`]), and says so in `PartitionUploadStatus`.
pub(crate) const CHUNK: usize = 512 << 10;

/// Room for a chunk's framing and the first chunk's header under `--max-request-bytes`.
const FRAMING: usize = 16 << 10;

/// The most a chunk may carry on a node with `--max-request-bytes` `max_request_bytes`
/// (cli692's F3 on #1728): at most [`CHUNK`], at least 1 KiB.
pub(crate) fn max_chunk(max_request_bytes: usize) -> usize {
    CHUNK
        .min(max_request_bytes.saturating_sub(FRAMING))
        .max(1 << 10)
}

/// A failed write or rename as an error: a full disk or quota is `NoSpace`
/// (`ResourceExhausted`), resumable once there's room (cli692's F2 on #1728); anything else
/// is `Internal`.
fn io_error(what: &str, path: &Path, e: &std::io::Error) -> ApiError {
    use std::io::ErrorKind;
    if matches!(e.kind(), ErrorKind::StorageFull | ErrorKind::QuotaExceeded) {
        return ApiError::from_code(
            pigeonhole::ErrorCode::NoSpace,
            &format!(
                "{what} {}: {e}: free space, then resume from PartitionUploadStatus",
                path.display()
            ),
        );
    }
    ApiError::internal(format!("{what} {}: {e}", path.display()))
}

#[cfg(test)]
thread_local! {
    /// A test's fault seam: the next this many writes (a chunk's, a header's) fail with a
    /// full disk.
    static FULL_DISK: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// The fault seam's check before a write: `Ok` outside tests.
fn disk_fault() -> std::io::Result<()> {
    #[cfg(test)]
    if FULL_DISK.with(|n| {
        let left = n.get();
        n.set(left.saturating_sub(1));
        left > 0
    }) {
        return Err(std::io::ErrorKind::StorageFull.into());
    }
    Ok(())
}

/// An incomplete upload.
struct Entry {
    header: PartitionUploadHeader,
    received: u64,
    /// A stream is writing it now.
    active: bool,
    last_used: Instant,
}

/// A cluster node's partition uploads.
#[derive(Debug)]
pub(crate) struct Uploads {
    dir: PathBuf,
    permits: Arc<Semaphore>,
    /// [`max_chunk`] of the node's `--max-request-bytes`.
    max_chunk: usize,
    entries: Mutex<HashMap<String, Entry>>,
}

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("received", &self.received)
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

impl Uploads {
    /// The uploads of the node whose cluster directory is `cluster_dir`, at most `max` at
    /// once, chunks of at most `max_chunk` bytes. Incomplete uploads a previous run left are
    /// deleted, and so are half-written headers; ready files are kept.
    pub(crate) fn new(cluster_dir: &Path, max: usize, max_chunk: usize) -> Self {
        let dir = cluster_dir.join(INCOMING);
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if name.ends_with(".part")
                    || name.ends_with(".part.header")
                    || name.ends_with(".tmp")
                {
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
        Self {
            dir,
            permits: Arc::new(Semaphore::new(max.max(1))),
            max_chunk,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// `--max-uploads`, set before the node serves (no upload can be running).
    pub(crate) fn set_max(&mut self, max: usize) {
        self.permits = Arc::new(Semaphore::new(max.max(1)));
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The staging directory, made (mode 0700) if missing; never a link.
    fn staging(&self) -> Result<&Path, ApiError> {
        match std::fs::symlink_metadata(&self.dir) {
            Ok(m) if m.file_type().is_symlink() || !m.is_dir() => {
                return Err(ApiError::internal(format!(
                    "{} isn't a directory",
                    self.dir.display()
                )));
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                #[cfg(unix)]
                let b = {
                    use std::os::unix::fs::DirBuilderExt as _;
                    let mut b = std::fs::DirBuilder::new();
                    b.mode(0o700);
                    b
                };
                #[cfg(not(unix))]
                let b = std::fs::DirBuilder::new();
                b.create(&self.dir).map_err(|e| {
                    ApiError::internal(format!("create {}: {e}", self.dir.display()))
                })?;
            }
            Err(e) => {
                return Err(ApiError::internal(format!("{}: {e}", self.dir.display())));
            }
        }
        Ok(&self.dir)
    }

    /// `Admin.PartitionUploadStatus`: the bytes of `upload_id` the node holds, and whether
    /// it's complete (ready to install).
    pub(crate) fn status(&self, upload_id: &str) -> Result<PartitionUploadStatusReply, ApiError> {
        check_upload_id(upload_id)?;
        if let Some(e) = self.lock().get(upload_id) {
            return Ok(PartitionUploadStatusReply {
                received: e.received,
                complete: false,
                max_chunk: self.max_chunk as u64,
            });
        }
        // Complete: a ready file whose header names it.
        if let Some(h) = self
            .ready_headers()
            .into_iter()
            .find(|h| h.upload_id == upload_id)
        {
            let bytes = h.file.as_ref().map_or(0, |f| f.bytes);
            return Ok(PartitionUploadStatusReply {
                received: bytes,
                complete: true,
                max_chunk: self.max_chunk as u64,
            });
        }
        Ok(PartitionUploadStatusReply {
            max_chunk: self.max_chunk as u64,
            ..Default::default()
        })
    }

    /// The headers of every ready file.
    fn ready_headers(&self) -> Vec<PartitionUploadHeader> {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return Vec::new();
        };
        entries
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".ready.header"))
            .filter_map(|e| read_header(&e.path()).ok())
            .collect()
    }

    /// Deletes the incomplete uploads nobody has written to for [`KEEP`].
    fn reap(&self) {
        let now = Instant::now();
        let mut entries = self.lock();
        let stale: Vec<String> = entries
            .iter()
            .filter(|(_, e)| !e.active && now.duration_since(e.last_used) > KEEP)
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            entries.remove(&id);
            let _ = std::fs::remove_file(self.dir.join(format!("{id}.part")));
            let _ = std::fs::remove_file(self.dir.join(format!("{id}.part.header")));
        }
    }

    /// Starts (or resumes) the upload `header` names: checks it, takes a `--max-uploads`
    /// place, and claims its file. `open` says whether a partition is open on this node.
    pub(crate) fn begin(
        self: &Arc<Self>,
        header: PartitionUploadHeader,
        open: impl Fn(&str) -> bool,
    ) -> Result<Writing, ApiError> {
        check_header(&header)?;
        if open(&header.partition) {
            return Err(ApiError::failed_precondition(format!(
                "partition {:?} is open on this node: its files can't be replaced while it's \
                 served",
                header.partition
            )));
        }
        self.reap();
        let permit = Arc::clone(&self.permits).try_acquire_owned().map_err(|_| {
            ApiError::overloaded(
                "the node is at its concurrent partition-upload limit (--max-uploads)",
            )
        })?;
        self.staging()?;
        let file = header.file.clone().expect("checked");
        let mut entries = self.lock();
        // One upload of a file of a partition at a time.
        if let Some((other, _)) = entries.iter().find(|(id, e)| {
            e.active
                && **id != header.upload_id
                && e.header.partition == header.partition
                && e.header.file.as_ref().map(|f| &f.name) == Some(&file.name)
        }) {
            return Err(ApiError::failed_precondition(format!(
                "another upload ({other}) of partition {:?}'s file {:?} is in progress here",
                header.partition, file.name
            )));
        }
        let part = self.dir.join(format!("{}.part", header.upload_id));
        let received = match entries.get_mut(&header.upload_id) {
            Some(e) if e.header != header => {
                return Err(ApiError::invalid(format!(
                    "upload {} was started with another header: resume it with the same one",
                    header.upload_id
                )));
            }
            Some(e) if e.active => {
                return Err(ApiError::failed_precondition(format!(
                    "upload {} is being written by another stream",
                    header.upload_id
                )));
            }
            Some(e) => {
                e.active = true;
                e.last_used = Instant::now();
                e.received
            }
            None => {
                std::fs::File::create(&part)
                    .map_err(|e| ApiError::internal(format!("create {}: {e}", part.display())))?;
                write_header(
                    &self.dir.join(format!("{}.part.header", header.upload_id)),
                    &header,
                )?;
                entries.insert(
                    header.upload_id.clone(),
                    Entry {
                        header: header.clone(),
                        received: 0,
                        active: true,
                        last_used: Instant::now(),
                    },
                );
                0
            }
        };
        drop(entries);
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&part)
            .map_err(|e| ApiError::internal(format!("open {}: {e}", part.display())))?;
        Ok(Writing {
            uploads: Arc::clone(self),
            header,
            file: Some(f),
            part,
            received,
            _permit: permit,
            done: false,
        })
    }
}

/// An upload being written by one stream. Dropped unfinished, it stays incomplete, to be
/// resumed (or reaped after [`KEEP`]).
pub(crate) struct Writing {
    uploads: Arc<Uploads>,
    header: PartitionUploadHeader,
    /// The `.part` file, open for writing; closed before it's renamed (Windows).
    file: Option<std::fs::File>,
    part: PathBuf,
    received: u64,
    _permit: OwnedSemaphorePermit,
    done: bool,
}

impl Writing {
    /// Writes `chunk`'s bytes, which must start where the node's end.
    pub(crate) fn write(&mut self, chunk: &PartitionUploadChunk) -> Result<(), ApiError> {
        let total = self.header.file.as_ref().map_or(0, |f| f.bytes);
        let max = self.uploads.max_chunk;
        if chunk.data.len() > max {
            return Err(ApiError::invalid(format!(
                "a chunk carries at most {max} bytes here (PartitionUploadStatus's max_chunk)"
            )));
        }
        if chunk.offset != self.received {
            return Err(ApiError::invalid(format!(
                "a chunk at {} where {} was expected: send from PartitionUploadStatus's \
                 `received`",
                chunk.offset, self.received
            )));
        }
        if self.received + chunk.data.len() as u64 > total {
            return Err(ApiError::invalid(format!(
                "more bytes than the file's {total}"
            )));
        }
        let file = self.file.as_mut().expect("open until finish");
        disk_fault()
            .and_then(|()| file.seek(SeekFrom::Start(self.received)))
            .and_then(|_| file.write_all(&chunk.data))
            .map_err(|e| io_error("write", &self.part, &e))?;
        self.received += chunk.data.len() as u64;
        if let Some(e) = self.uploads.lock().get_mut(&self.header.upload_id) {
            e.received = self.received;
            e.last_used = Instant::now();
        }
        Ok(())
    }

    /// The stream ended: if every byte arrived, checks the SHA-256, syncs the file and
    /// stages it as ready. The reply says where the upload stands.
    pub(crate) fn finish(mut self) -> Result<PartitionUploadReply, ApiError> {
        let file = self.header.file.clone().expect("checked");
        if self.received < file.bytes {
            return Ok(PartitionUploadReply {
                received: self.received,
                complete: false,
            });
        }
        let synced = self.file.as_ref().expect("open until finish").sync_all();
        synced.map_err(|e| io_error("sync", &self.part, &e))?;
        // Closed before any rename, which Windows refuses on a file held open.
        drop(self.file.take());
        let sha = digest(&self.part)?;
        if sha[..] != file.sha256[..] {
            // The bytes aren't the file the header names: start over.
            self.forget();
            return Err(ApiError::invalid(format!(
                "partition {:?}'s file {:?} doesn't match its SHA-256: upload it again",
                self.header.partition, file.name
            )));
        }
        let dir = &self.uploads.dir;
        let ready = dir.join(ready_name(&self.header.partition, &file.name));
        let ready_header = path_with(&ready, ".header");
        // The header is the commit marker, written last (feed1248's F1 on #1728): an older
        // ready file's header goes first, so a crash or a failed step in between leaves a
        // ready file with no header, which `ready_set` never reports, rather than this
        // header over the older bytes.
        match std::fs::remove_file(&ready_header) {
            Ok(()) => sync_dir(dir),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(ApiError::internal(format!(
                    "remove {}: {e}",
                    ready_header.display()
                )));
            }
        }
        std::fs::rename(&self.part, &ready).map_err(|e| io_error("stage", &ready, &e))?;
        if let Err(e) = write_header(&ready_header, &self.header) {
            // Back where it was, so a resume (with nothing left to send) finishes it again.
            let _ = std::fs::rename(&ready, &self.part);
            return Err(e);
        }
        sync_dir(dir);
        let _ = std::fs::remove_file(dir.join(format!("{}.part.header", self.header.upload_id)));
        self.uploads.lock().remove(&self.header.upload_id);
        self.done = true;
        Ok(PartitionUploadReply {
            received: self.received,
            complete: true,
        })
    }

    /// Drops the upload and its bytes.
    fn forget(&mut self) {
        self.uploads.lock().remove(&self.header.upload_id);
        let _ = std::fs::remove_file(&self.part);
        let _ = std::fs::remove_file(
            self.uploads
                .dir
                .join(format!("{}.part.header", self.header.upload_id)),
        );
        self.done = true;
    }
}

impl Drop for Writing {
    fn drop(&mut self) {
        if !self.done
            && let Some(e) = self.uploads.lock().get_mut(&self.header.upload_id)
        {
            e.active = false;
            e.last_used = Instant::now();
        }
    }
}

/// A partition's staged set, when every file its manifest names is ready with one header
/// identity (partition, cluster id, epoch, manifest): the header, and each file's staged
/// path by name. `None` while any is missing or the ready files disagree. For the install
/// that D317 ticket 6's open runs.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "D317 ticket 6a PR 1's stamp installs through it (simp724); tested here"
    )
)]
pub(crate) fn ready_set(
    cluster_dir: &Path,
    partition: &str,
) -> Option<(PartitionUploadHeader, Vec<(String, PathBuf)>)> {
    let dir = cluster_dir.join(INCOMING);
    let first = read_header(&path_with(&dir.join(ready_name(partition, "")), ".header")).ok()?;
    let mut files = Vec::with_capacity(first.manifest.len());
    for f in &first.manifest {
        let path = dir.join(ready_name(partition, &f.name));
        let h = read_header(&path_with(&path, ".header")).ok()?;
        let same = h.partition == first.partition
            && h.cluster_id == first.cluster_id
            && h.epoch == first.epoch
            && h.manifest == first.manifest
            && h.file.as_ref() == Some(f);
        if !same || !path.is_file() {
            return None;
        }
        files.push((f.name.clone(), path));
    }
    Some((first, files))
}

/// `<partition>.phdb<suffix>.ready`.
fn ready_name(partition: &str, suffix: &str) -> String {
    format!("{partition}.phdb{suffix}.ready")
}

fn path_with(path: &Path, ext: &str) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(ext);
    PathBuf::from(p)
}

/// Whether `name` is a partition file's suffix: the main file (`""`), a WAL sidecar
/// (`-wal-<n>`) or the feed sidecar (`-feed`).
fn suffix_ok(name: &str) -> bool {
    name.is_empty()
        || name == "-feed"
        || name.strip_prefix("-wal-").is_some_and(|n| {
            !n.is_empty() && n.len() <= 10 && n.bytes().all(|b| b.is_ascii_digit())
        })
}

fn check_upload_id(id: &str) -> Result<(), ApiError> {
    if id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(ApiError::invalid(
            "an upload id is 32 lower-case hex digits",
        ))
    }
}

/// A header's own checks, before anything is written.
fn check_header(h: &PartitionUploadHeader) -> Result<(), ApiError> {
    check_upload_id(&h.upload_id)?;
    if !map::id_ok(&h.partition) || !map::id_ok(&h.cluster_id) {
        return Err(ApiError::invalid(
            "the partition and cluster ids are 1-64 of [a-z0-9_-], not starting with -",
        ));
    }
    if h.epoch == 0 {
        return Err(ApiError::invalid("an epoch is at least 1"));
    }
    if h.purpose == PartitionUploadPurpose::Unspecified as i32
        || PartitionUploadPurpose::try_from(h.purpose).is_err()
    {
        return Err(ApiError::invalid(
            "an upload's purpose is RESTORE or RELOCATE",
        ));
    }
    let Some(file) = &h.file else {
        return Err(ApiError::invalid("the header names no file"));
    };
    if h.manifest.is_empty() {
        return Err(ApiError::invalid("the manifest is empty"));
    }
    let mut names = std::collections::HashSet::new();
    for f in &h.manifest {
        check_file(f)?;
        if !names.insert(f.name.as_str()) {
            return Err(ApiError::invalid(format!(
                "the manifest names {:?} twice",
                f.name
            )));
        }
    }
    if !names.contains("") {
        return Err(ApiError::invalid(
            "the manifest has no main file (name \"\")",
        ));
    }
    if !h.manifest.contains(file) {
        return Err(ApiError::invalid(format!(
            "the file {:?} isn't the manifest's (name, bytes and SHA-256 must match)",
            file.name
        )));
    }
    Ok(())
}

fn check_file(f: &PartitionFile) -> Result<(), ApiError> {
    if !suffix_ok(&f.name) {
        return Err(ApiError::invalid(format!(
            "{:?} isn't a partition file's name: \"\", \"-wal-<n>\" or \"-feed\"",
            f.name
        )));
    }
    if f.sha256.len() != 32 {
        return Err(ApiError::invalid(format!(
            "file {:?}'s SHA-256 is 32 bytes",
            f.name
        )));
    }
    Ok(())
}

fn write_header(path: &Path, h: &PartitionUploadHeader) -> Result<(), ApiError> {
    let json = serde_json::to_vec(h).expect("a header serializes");
    let tmp = path_with(path, ".tmp");
    // Written and synced through one writable handle: Windows refuses to flush a file
    // opened read-only.
    disk_fault()
        .and_then(|()| {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&json)?;
            f.sync_all()
        })
        .and_then(|()| std::fs::rename(&tmp, path))
        .map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            io_error("write", path, &e)
        })
}

fn read_header(path: &Path) -> Result<PartitionUploadHeader, String> {
    let json = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    serde_json::from_slice(&json).map_err(|e| format!("{}: {e}", path.display()))
}

fn digest(path: &Path) -> Result<[u8; 32], ApiError> {
    let mut f = std::fs::File::open(path)
        .map_err(|e| ApiError::internal(format!("read {}: {e}", path.display())))?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f
            .read(&mut buf)
            .map_err(|e| ApiError::internal(format!("read {}: {e}", path.display())))?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(h.finalize().into())
}

fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir() -> PathBuf {
        static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let d = std::env::temp_dir().join(format!(
            "phdb-upload-{}-{}",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn file(name: &str, data: &[u8]) -> PartitionFile {
        PartitionFile {
            name: name.into(),
            bytes: data.len() as u64,
            sha256: Sha256::digest(data).to_vec().into(),
        }
    }

    fn header(id: char, f: &PartitionFile, manifest: &[PartitionFile]) -> PartitionUploadHeader {
        PartitionUploadHeader {
            partition: "p2".into(),
            cluster_id: "c1".into(),
            epoch: 3,
            upload_id: id.to_string().repeat(32),
            purpose: PartitionUploadPurpose::Relocate as i32,
            file: Some(f.clone()),
            manifest: manifest.to_vec(),
        }
    }

    fn chunk(offset: u64, data: &[u8]) -> PartitionUploadChunk {
        PartitionUploadChunk {
            header: None,
            offset,
            data: data.to_vec().into(),
        }
    }

    const CLOSED: fn(&str) -> bool = |_| false;

    #[test]
    fn partition_file_names_are_the_suffix_forms() {
        for ok in ["", "-feed", "-wal-0", "-wal-17"] {
            assert!(suffix_ok(ok), "{ok:?}");
        }
        for bad in [
            "-wal-", "-wal-x", "/etc", "..", "-feed2", ".phdb", "-WAL-1", "x",
        ] {
            assert!(!suffix_ok(bad), "{bad:?}");
        }
    }

    #[test]
    fn an_upload_resumes_and_a_set_is_ready_once_every_file_is() {
        let d = dir();
        let up = Arc::new(Uploads::new(&d, 1, CHUNK));
        let (main, wal) = (b"the main file".as_slice(), b"a wal sidecar".as_slice());
        let manifest = [file("", main), file("-wal-0", wal)];
        let h = header('a', &manifest[0], &manifest);
        // Half, then the stream breaks.
        let mut w = up.begin(h.clone(), CLOSED).unwrap();
        w.write(&chunk(0, &main[..5])).unwrap();
        let r = w.finish().unwrap();
        assert_eq!((r.received, r.complete), (5, false));
        assert_eq!(up.status(&h.upload_id).unwrap().received, 5);
        // Resumed with the same header, from where it stands.
        let mut w = up.begin(h.clone(), CLOSED).unwrap();
        let e = w.write(&chunk(0, main)).unwrap_err();
        assert_eq!(e.code, "InvalidArgument", "{e:?}");
        w.write(&chunk(5, &main[5..])).unwrap();
        assert!(w.finish().unwrap().complete);
        let st = up.status(&h.upload_id).unwrap();
        assert_eq!((st.received, st.complete), (main.len() as u64, true));
        assert_eq!(
            std::fs::read(d.join(INCOMING).join("p2.phdb.ready")).unwrap(),
            main
        );
        // The set isn't ready until its WAL sidecar is too.
        assert!(ready_set(&d, "p2").is_none());
        let mut w = up
            .begin(header('b', &manifest[1], &manifest), CLOSED)
            .unwrap();
        w.write(&chunk(0, wal)).unwrap();
        assert!(w.finish().unwrap().complete);
        let (h0, files) = ready_set(&d, "p2").expect("the whole set");
        assert_eq!((h0.epoch, h0.cluster_id.as_str()), (3, "c1"));
        let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["", "-wal-0"]);
        // A ready file of another set (another epoch) makes it inconsistent.
        let mut other = header('c', &manifest[1], &manifest);
        other.epoch = 4;
        let mut w = up.begin(other, CLOSED).unwrap();
        w.write(&chunk(0, wal)).unwrap();
        w.finish().unwrap();
        assert!(ready_set(&d, "p2").is_none(), "mixed epochs");
    }

    #[test]
    fn a_ready_file_counts_only_with_its_header_written_last() {
        // feed1248's F1 on #1728: a newer upload of a file replaces an older ready one, and a
        // crash between its rename and its header leaves a ready file with no header, which
        // the set never counts.
        let d = dir();
        let up = Arc::new(Uploads::new(&d, 1, CHUNK));
        let (old, new) = (b"old bytes".as_slice(), b"new bytes!".as_slice());
        for (id, data) in [('a', old), ('b', new)] {
            let f = file("", data);
            let mut w = up
                .begin(header(id, &f, std::slice::from_ref(&f)), CLOSED)
                .unwrap();
            w.write(&chunk(0, data)).unwrap();
            assert!(w.finish().unwrap().complete);
        }
        let (h, files) = ready_set(&d, "p2").unwrap();
        assert_eq!(h.upload_id, "b".repeat(32), "the newer upload's header");
        assert_eq!(
            std::fs::read(&files[0].1).unwrap(),
            new,
            "over the newer bytes"
        );
        // The crash: bytes in place, header not yet written.
        std::fs::remove_file(path_with(&files[0].1, ".header")).unwrap();
        assert!(ready_set(&d, "p2").is_none());
    }

    #[test]
    fn a_full_disk_answers_no_space_and_the_upload_resumes() {
        // cli692's F2 on #1728: a full disk, on a chunk's write or on the ready header's, is
        // `NoSpace` (ResourceExhausted), and the upload resumes once there's room.
        let d = dir();
        let up = Arc::new(Uploads::new(&d, 1, CHUNK));
        let data = b"a partition file".as_slice();
        let f = file("", data);
        let h = header('a', &f, std::slice::from_ref(&f));
        let mut w = up.begin(h.clone(), CLOSED).unwrap();
        w.write(&chunk(0, &data[..4])).unwrap();
        FULL_DISK.with(|n| n.set(1));
        let e = w.write(&chunk(4, &data[4..])).unwrap_err();
        assert_eq!(
            (e.code.as_str(), e.grpc),
            ("NoSpace", tonic::Code::ResourceExhausted),
            "{e:?}"
        );
        assert_eq!(up.status(&h.upload_id).unwrap().received, 4, "nothing lost");
        w.write(&chunk(4, &data[4..])).unwrap();
        // The ready header's write fails: the bytes go back, and a resume finishes it.
        FULL_DISK.with(|n| n.set(1));
        let e = w.finish().unwrap_err();
        assert_eq!(e.code, "NoSpace", "{e:?}");
        assert!(ready_set(&d, "p2").is_none());
        let st = up.status(&h.upload_id).unwrap();
        assert_eq!((st.received, st.complete), (data.len() as u64, false));
        let w = up.begin(h, CLOSED).unwrap();
        assert!(w.finish().unwrap().complete);
        assert!(ready_set(&d, "p2").is_some());
    }

    #[test]
    fn a_node_with_a_small_request_limit_takes_smaller_chunks() {
        // cli692's F3 on #1728.
        assert_eq!(max_chunk(1 << 20), CHUNK);
        assert_eq!(max_chunk(256 << 10), (256 << 10) - FRAMING);
        assert_eq!(max_chunk(4 << 10), 1 << 10);
        let d = dir();
        let up = Arc::new(Uploads::new(&d, 1, 4));
        assert_eq!(up.status(&"0".repeat(32)).unwrap().max_chunk, 4);
        let f = file("", b"12345");
        let mut w = up
            .begin(header('a', &f, std::slice::from_ref(&f)), CLOSED)
            .unwrap();
        assert_eq!(
            w.write(&chunk(0, b"12345")).unwrap_err().code,
            "InvalidArgument"
        );
        w.write(&chunk(0, b"1234")).unwrap();
    }

    #[test]
    fn bytes_that_dont_match_the_sha_are_dropped() {
        let d = dir();
        let up = Arc::new(Uploads::new(&d, 1, CHUNK));
        let f = file("", b"what the header says");
        let h = header('a', &f, std::slice::from_ref(&f));
        let mut w = up.begin(h.clone(), CLOSED).unwrap();
        w.write(&chunk(0, b"something else here!")).unwrap();
        let e = w.finish().unwrap_err();
        assert_eq!(e.code, "InvalidArgument", "{e:?}");
        assert_eq!(up.status(&h.upload_id).unwrap().received, 0, "started over");
        assert!(!d.join(INCOMING).join("p2.phdb.ready").exists());
    }

    #[test]
    fn uploads_are_refused_for_an_open_partition_past_the_limit_or_beside_another() {
        let d = dir();
        let up = Arc::new(Uploads::new(&d, 1, CHUNK));
        let f = file("", b"x");
        let h = header('a', &f, std::slice::from_ref(&f));
        let e = up.begin(h.clone(), |p| p == "p2").err().unwrap();
        assert_eq!(e.code, "FailedPrecondition", "{e:?}");
        let held = up.begin(h.clone(), CLOSED).unwrap();
        // --max-uploads 1: a second upload while one runs.
        let g = header('b', &f, std::slice::from_ref(&f));
        let e = up.begin(g.clone(), CLOSED).err().unwrap();
        assert_eq!(e.code, "Overloaded", "{e:?}");
        drop(held);
        // With room, another upload of the same file of P while one is active.
        let up2 = Arc::new(Uploads::new(&dir(), 2, CHUNK));
        let _a = up2.begin(h.clone(), CLOSED).unwrap();
        let e = up2.begin(g, CLOSED).err().unwrap();
        assert_eq!(e.code, "FailedPrecondition", "{e:?}");
        // A resume with another header.
        drop(_a);
        let mut changed = h.clone();
        changed.epoch = 9;
        let e = up2.begin(changed, CLOSED).err().unwrap();
        assert_eq!(e.code, "InvalidArgument", "{e:?}");
    }

    #[test]
    fn a_header_must_name_a_whole_well_formed_set() {
        let main = file("", b"m");
        let ok = header('a', &main, std::slice::from_ref(&main));
        check_header(&ok).unwrap();
        let bad = |f: &dyn Fn(&mut PartitionUploadHeader)| {
            let mut h = ok.clone();
            f(&mut h);
            check_header(&h).unwrap_err()
        };
        bad(&|h| h.upload_id = "XYZ".into());
        bad(&|h| h.epoch = 0);
        bad(&|h| h.purpose = 0);
        bad(&|h| h.manifest.clear());
        bad(&|h| h.manifest = vec![file("-feed", b"f")]);
        bad(&|h| h.file = Some(file("", b"other bytes")));
        bad(&|h| h.manifest.push(file("../x", b"x")));
        bad(&|h| h.manifest.push(main.clone()));
        bad(&|h| h.partition = "P/2".into());
    }

    #[test]
    fn a_restart_drops_incomplete_uploads_and_keeps_ready_ones() {
        let d = dir();
        let up = Arc::new(Uploads::new(&d, 2, CHUNK));
        let (a, b) = (file("", b"done"), file("-feed", b"partial!"));
        let manifest = [a.clone(), b.clone()];
        let ha = header('a', &a, &manifest);
        let mut w = up.begin(ha.clone(), CLOSED).unwrap();
        w.write(&chunk(0, b"done")).unwrap();
        w.finish().unwrap();
        let hb = header('b', &b, &manifest);
        let mut w = up.begin(hb.clone(), CLOSED).unwrap();
        w.write(&chunk(0, b"part")).unwrap();
        drop(w);
        let up = Uploads::new(&d, 2, CHUNK);
        assert!(
            up.status(&ha.upload_id).unwrap().complete,
            "a ready file survives"
        );
        assert_eq!(
            up.status(&hb.upload_id).unwrap().received,
            0,
            "a partial one doesn't"
        );
        assert!(
            !d.join(INCOMING)
                .join(format!("{}.part", hb.upload_id))
                .exists()
        );
    }
}

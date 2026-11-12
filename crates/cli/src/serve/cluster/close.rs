//! `phdb cluster close P --node N` (D317 amendment 2): a fenced partition closed on its
//! node, its files final, so a relocation can copy them.
//!
//! Close answers `CLOSED` at once and hashes the files in the background (perf287's S6):
//! `Status` shows them once hashed, a failed hash sets the partition's `error`, and `close`
//! again retries it. Every file's pages are dropped from the page cache as it's hashed, so
//! a closed partition's files don't hold memory the open ones use.
//!
//! What still holds the partition when it closes: its feed hubs fail (their clients
//! resume from their tokens elsewhere), its bulk load, text rebuilds and vector index
//! threads end, and a call already running on it ends with the database's `Closed`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use super::Node;
use crate::serve::errors::ApiError;
use crate::serve::proto::{LocalPartition, LocalPartitionState, PartitionFile};

/// How long a close waits for the writes a fence admitted to finish.
const DRAIN: Duration = Duration::from_secs(30);

/// A partition closed here: its file's epoch, and its files' hashes.
#[derive(Debug, Clone)]
pub(crate) struct Closed {
    epoch: u64,
    files: Hashing,
}

#[derive(Debug, Clone)]
enum Hashing {
    Running,
    Done(Vec<PartitionFile>),
    Failed(String),
}

impl Closed {
    /// Its line in `Status`.
    pub(crate) fn describe(&self, lp: &mut LocalPartition) {
        lp.set_state(LocalPartitionState::Closed);
        lp.epoch = self.epoch;
        match &self.files {
            Hashing::Running => {}
            Hashing::Done(files) => lp.files.clone_from(files),
            Hashing::Failed(e) => {
                lp.ok = false;
                lp.error = format!("its files' hash failed (close it again to retry): {e}");
            }
        }
    }
}

impl Node {
    /// Closes partition `id`, which must be fenced: once the writes its fence admitted have
    /// finished, it stops being served (its calls answer `PartitionUnavailable`), its users
    /// end, and its database closes. Its files are hashed in the background. Closed already:
    /// a failed hash is retried, and nothing else is done.
    pub(crate) async fn close(self: &Arc<Self>, id: &str) -> Result<(), ApiError> {
        let _one = self.fences.changing.lock().await;
        let _open = self.opening.lock().await;
        if let Some(closed) = self.fences.closed().get(id) {
            if matches!(closed.files, Hashing::Failed(_)) {
                self.hash_soon(id);
            }
            return Ok(());
        }
        let (gate, epoch) = self
            .fences
            .gate(id)
            .ok_or_else(|| super::fence::not_here(id))?;
        if gate.fenced().is_none() {
            return Err(ApiError::failed_precondition(format!(
                "partition {id:?} is serving writes: fence it first (phdb cluster fence)"
            )));
        }
        gate.drained(DRAIN).await.map_err(|n| {
            ApiError::failed_precondition(format!(
                "partition {id:?}: {n} writes admitted before its fence still run after {}s; \
                 close it again once they finish",
                DRAIN.as_secs()
            ))
        })?;
        let shared = self
            .opener
            .get()
            .and_then(|o| o.shared.upgrade())
            .ok_or_else(|| {
                ApiError::unsupported("this node closes no partition while it serves")
            })?;
        let cluster = shared
            .router
            .cluster
            .as_ref()
            .ok_or_else(|| ApiError::internal("not a cluster node"))?;
        // Out of the parts first: no new call reaches it from here.
        let mut parts = (*cluster.parts.load_full()).clone();
        let service = parts.remove(id).ok_or_else(|| super::fence::not_here(id))?;
        cluster.parts.store(Arc::new(parts));
        self.fences.forget(id);
        self.fences.closed().insert(
            id.to_owned(),
            Closed {
                epoch,
                files: Hashing::Running,
            },
        );
        // Its users end and its database closes, off the async threads.
        let closed = tokio::task::spawn_blocking(move || {
            let Some(part) = service.part_opt() else {
                return Ok(());
            };
            part.feeds.fail_all();
            part.bulk.stop();
            part.bulk.close();
            part.texts.close();
            part.indexes.close();
            part.db.clone().close()
        })
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
        if let Err(e) = closed {
            eprintln!("phdb serve: partition {id} closed, and its close reported: {e}");
        }
        eprintln!("phdb serve: partition {id} closed: hashing its files");
        self.hash_soon(id);
        Ok(())
    }

    /// Hashes closed partition `id`'s files in a blocking task, keeping the outcome for
    /// `Status`.
    fn hash_soon(&self, id: &str) {
        let Some(node) = self.me.upgrade() else {
            return;
        };
        if let Some(c) = self.fences.closed().get_mut(id) {
            c.files = Hashing::Running;
        }
        let id = id.to_owned();
        let path = self.dir.join(format!("{id}.phdb"));
        tokio::task::spawn_blocking(move || {
            let files = match hash_files(&path) {
                Ok(files) => Hashing::Done(files),
                Err(e) => {
                    eprintln!("phdb serve: partition {id}: hashing its closed files: {e}");
                    Hashing::Failed(e)
                }
            };
            // Still closed: a reopen meanwhile dropped it, and its files changed.
            if let Some(c) = node.fences.closed().get_mut(&id) {
                c.files = files;
            }
        });
    }
}

/// Every file of the database at `path`, by its suffix, with its size and SHA-256.
fn hash_files(path: &Path) -> Result<Vec<PartitionFile>, String> {
    let files: Vec<PathBuf> = pigeonhole::Pigeonhole::database_files(path)
        .map_err(|e| format!("list {}'s files: {e}", path.display()))?;
    files
        .iter()
        .map(|file| {
            let name = pigeonhole::Pigeonhole::database_file_suffix(path, file)
                .ok_or_else(|| format!("{} isn't one of its files", file.display()))?;
            let (bytes, sha256) = hash_file(file)?;
            Ok(PartitionFile {
                name: name.to_owned(),
                bytes,
                sha256: sha256.to_vec().into(),
            })
        })
        .collect()
}

/// `file`'s size and SHA-256, read through once; its pages are dropped from the page cache
/// as it goes (Linux), since nothing reads a closed partition's files soon.
fn hash_file(file: &Path) -> Result<(u64, [u8; 32]), String> {
    use sha2::{Digest, Sha256};
    use std::io::Read as _;
    let mut f = std::fs::File::open(file).map_err(|e| format!("read {}: {e}", file.display()))?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut bytes = 0u64;
    loop {
        let n = f
            .read(&mut buf)
            .map_err(|e| format!("read {}: {e}", file.display()))?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
        bytes += n as u64;
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd as _;
        // Advice only: a failure leaves the pages to the kernel.
        let _ = nix::fcntl::posix_fadvise(
            f.as_raw_fd(),
            0,
            0,
            nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED,
        );
    }
    Ok((bytes, h.finalize().into()))
}

/// Where `drop-partition` moves a partition's files: inside the cluster directory, so the
/// move is a rename, and never deleted by the node.
pub(crate) const DROPPED: &str = ".phdb-dropped";

impl Node {
    /// `phdb cluster drop-partition P --node N` (D317 ticket 6): moves partition `id`'s files
    /// (and any `.replaced` ones an install moved aside) to
    /// `.phdb-dropped/<P>-<epoch>-<unix time>/`, deleting nothing: the operator deletes that
    /// directory once the move is checked. Refused while the map names this node for it,
    /// or it's open here. The reply's `file` is the directory, relative to the cluster's.
    pub(crate) async fn drop_partition(
        self: &Arc<Self>,
        id: &str,
    ) -> Result<LocalPartition, ApiError> {
        let _one = self.fences.changing.lock().await;
        let _open = self.opening.lock().await;
        if !super::map::id_ok(id) {
            return Err(ApiError::invalid(format!("{id:?} isn't a partition id")));
        }
        if let Ok(map) = self.get_map()
            && map
                .partitions
                .iter()
                .any(|p| p.id == id && p.node == self.id)
        {
            return Err(ApiError::failed_precondition(format!(
                "the cluster map (version {}) still places partition {id:?} on this node: move \
                 it first (phdb cluster relocate)",
                map.map_version
            )));
        }
        if self.part_state(id).is_some() {
            return Err(ApiError::failed_precondition(format!(
                "partition {id:?} is open here: close it first (phdb cluster close)"
            )));
        }
        let dir = self.dir.clone();
        let part = id.to_owned();
        let lp = tokio::task::spawn_blocking(move || move_aside(&dir, &part))
            .await
            .map_err(|e| ApiError::internal(e.to_string()))??;
        self.fences.closed().remove(id);
        eprintln!(
            "phdb serve: partition {id}'s files moved to {}: delete it once checked",
            lp.file
        );
        Ok(lp)
    }
}

/// [`Node::drop_partition`]'s move, on a blocking thread.
fn move_aside(dir: &Path, id: &str) -> Result<LocalPartition, ApiError> {
    let main = dir.join(format!("{id}.phdb"));
    if !main.exists() {
        return Err(ApiError::not_found(format!(
            "this node has no files of partition {id}"
        )));
    }
    // The epoch from the file's identity, read without opening it for writes.
    let epoch = super::identity::read_file(&main)
        .map_err(ApiError::internal)?
        .map_or(0, |i| i.epoch);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let name = format!("{id}-{epoch}-{now}");
    let to = dir.join(DROPPED).join(&name);
    if to.exists() {
        return Err(ApiError::failed_precondition(format!(
            "{} exists already: try again in a second",
            to.display()
        )));
    }
    std::fs::create_dir_all(&to)
        .map_err(|e| ApiError::internal(format!("create {}: {e}", to.display())))?;
    let mut files = pigeonhole::Pigeonhole::database_files(&main)
        .map_err(|e| ApiError::internal(format!("list {}'s files: {e}", main.display())))?;
    let prefix = format!("{id}.phdb");
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let n = e.file_name().to_string_lossy().into_owned();
            if n.starts_with(&prefix) && n.ends_with(".replaced") {
                files.push(e.path());
            }
        }
    }
    for f in &files {
        let Some(n) = f.file_name() else { continue };
        std::fs::rename(f, to.join(n))
            .map_err(|e| ApiError::internal(format!("move {}: {e}", f.display())))?;
    }
    super::upload::sync_dir(&to);
    super::upload::sync_dir(dir);
    let mut lp = LocalPartition {
        id: id.to_owned(),
        epoch,
        file: format!("{DROPPED}/{name}"),
        ok: true,
        ..Default::default()
    };
    lp.set_state(LocalPartitionState::Unspecified);
    Ok(lp)
}

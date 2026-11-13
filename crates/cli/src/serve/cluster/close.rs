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
//! threads end, and a call already running on it answers `PartitionUnavailable`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use super::Node;
use crate::serve::errors::ApiError;
use crate::serve::gate::Fence;
use crate::serve::proto::{LocalPartition, LocalPartitionState, PartitionFile};

/// How long a close waits for the writes a fence admitted to finish.
const DRAIN: Duration = Duration::from_secs(30);

/// A partition closed here: its file's epoch, and its files' hashes.
#[derive(Debug, Clone)]
pub(crate) struct Closed {
    /// The file's epoch when it closed.
    pub(super) epoch: u64,
    files: Hashing,
    /// Which close this is: a hash started for an earlier close of the same partition
    /// (reopened, written, closed again) never lands on this one (cli692's S2 on #1755).
    generation: u64,
    /// The database's own close error, if it reported one (cli692's N4).
    error: Option<String>,
    /// Its database is still closing: until then a read of the file is refused by the
    /// writer lock, which isn't a finding (cli692's N1 and S1 on #1769).
    db_open: bool,
}

/// A hold seam (tests): the next close in this cluster directory waits, after the partition
/// left the parts and before its database closes, until the sender is dropped or sends.
#[cfg(test)]
pub(crate) static HOLD_BEFORE_DB_CLOSE: std::sync::Mutex<
    Option<(std::path::PathBuf, std::sync::mpsc::Receiver<()>)>,
> = std::sync::Mutex::new(None);

/// Each close's [`Closed::generation`].
static CLOSES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Debug, Clone)]
enum Hashing {
    Running,
    Done(Vec<PartitionFile>),
    Failed(String),
}

impl Closed {
    /// File `name` of partition `id` (a suffix after its database path, ICR 0135: `""` the
    /// main file, `-wal-<n>`, `-feed`) as hashed at this close: what a relocation by copy
    /// downloads (D317 ticket 6). Only once the hash is done; a name it didn't list is
    /// `NotFound`.
    pub(crate) fn file(&self, id: &str, name: &str) -> Result<&PartitionFile, ApiError> {
        match &self.files {
            Hashing::Running => Err(ApiError::failed_precondition(format!(
                "partition {id:?} is closed, and its files are still being hashed: wait for \
                 `phdb cluster status` to list them"
            ))),
            Hashing::Failed(e) => Err(ApiError::failed_precondition(format!(
                "partition {id:?}'s files' hash failed (close it again to retry): {e}"
            ))),
            Hashing::Done(files) => files.iter().find(|f| f.name == name).ok_or_else(|| {
                let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
                ApiError::not_found(format!(
                    "partition {id:?} closed here has no file {name:?}: its files are {names:?}"
                ))
            }),
        }
    }

    /// Its line in `Status`.
    pub(crate) fn describe(&self, lp: &mut LocalPartition) {
        lp.set_state(LocalPartitionState::Closed);
        lp.epoch = self.epoch;
        // Between the close's swap and its database's close, a read of the file is still
        // refused by the writer lock: transient, and the partition's identity was the map's
        // while it served (cli692's N1 on #1769). Once it has closed, the file's own read
        // stands: a finding there is real (S1).
        if self.db_open {
            lp.ok = true;
            lp.error.clear();
        }
        if let Some(e) = &self.error {
            lp.ok = false;
            lp.error = format!("its database's close reported: {e}");
        }
        match &self.files {
            Hashing::Running => {}
            Hashing::Done(files) => lp.files.clone_from(files),
            Hashing::Failed(e) => {
                lp.ok = false;
                if !lp.error.is_empty() {
                    lp.error.push_str("; ");
                }
                lp.error.push_str(&format!(
                    "its files' hash failed (close it again to retry): {e}"
                ));
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
        // Its fence is the operator's from here, and persisted, as `fence`'s is (simp724's S1
        // on #1800): a fence since the node started is kept nowhere, and a closed partition
        // reopens at a restart while the map still names this node. A peer's map would then
        // lift that fence, in the middle of a move that copied the closed files.
        gate.fence(Fence::Operator);
        let mut fenced = self.fences.operator().clone();
        if fenced.insert(id.to_owned()) {
            self.persist_fences(fenced).await?;
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
                ApiError::unsupported(
                    "this node can't close a partition: it wasn't started to open and close \
                     partitions while it serves",
                )
            })?;
        let cluster = shared
            .router
            .cluster
            .as_ref()
            .ok_or_else(|| ApiError::internal("not a cluster node"))?;
        // Out of the parts first: no new call reaches it from here.
        let mut parts = (*cluster.parts.load_full()).clone();
        let service = parts.remove(id).ok_or_else(|| super::fence::not_here(id))?;
        cluster.parts.store(Arc::new(parts.clone()));
        self.fences.forget(id);
        self.fences.closed().insert(
            id.to_owned(),
            Closed {
                epoch,
                files: Hashing::Running,
                generation: CLOSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                error: None,
                db_open: true,
            },
        );
        // Its users end and its database closes, off the async threads.
        let id_for_feeds = id.to_owned();
        #[cfg(test)]
        let dir = self.dir.clone();
        let closed = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            {
                let mut armed = HOLD_BEFORE_DB_CLOSE
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if armed.as_ref().is_some_and(|(d, _)| *d == dir) {
                    let (_, wait) = armed.take().expect("armed");
                    drop(armed);
                    let _ = wait.recv();
                }
            }
            let Some(part) = service.part_opt() else {
                return Ok(());
            };
            // Its cluster feed components end as a partition that went away, which a
            // subscription with `allow_partial` waits out and opens again where the map
            // then puts it; not with the engine's `Closed` from the close below, which ends
            // every subscription (audit120's S-C5).
            part.feeds
                .end_parts(ApiError::partition_unavailable(format!(
                    "partition {id_for_feeds:?} closed on this node"
                )));
            part.feeds.fail_all();
            part.bulk.stop();
            part.bulk.close();
            part.texts.close();
            part.indexes.close();
            part.db.clone().close()
        })
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
        if let Some(c) = self.fences.closed().get_mut(id) {
            c.db_open = false;
            if let Err(e) = &closed {
                c.error = Some(e.to_string());
            }
        }
        if let Err(e) = closed {
            eprintln!("phdb serve: partition {id} closed, and its close reported: {e}");
        }
        // Its shards' CPUs are free now: the server's threads take them back.
        if let Some(opener) = self.opener.get() {
            super::open::remask(&opener.placement, &parts, &format!("partition {id} closed"));
        }
        eprintln!("phdb serve: partition {id} closed: hashing its files");
        self.hash_soon(id);
        Ok(())
    }

    /// File `name` of partition `id`, closed here ([`Closed::file`]), and its path: what
    /// `Admin.BackupDownload`'s `fenced_file` streams (D317 ticket 6). The name is one the
    /// close's hash listed, so the path is one of the partition's own files.
    pub(crate) fn closed_file(
        &self,
        id: &str,
        name: &str,
    ) -> Result<(PathBuf, PartitionFile), ApiError> {
        let closed = self.fences.closed();
        let file = closed
            .get(id)
            .ok_or_else(|| {
                ApiError::failed_precondition(format!(
                    "partition {id:?} isn't closed on this node: fence and close it first"
                ))
            })?
            .file(id, name)?
            .clone();
        let mut path = self.dir.join(format!("{id}.phdb")).into_os_string();
        path.push(&file.name);
        Ok((PathBuf::from(path), file))
    }

    /// Hashes closed partition `id`'s files in a blocking task, keeping the outcome for
    /// `Status`.
    fn hash_soon(&self, id: &str) {
        let Some(node) = self.me.upgrade() else {
            return;
        };
        let Some(generation) = self.fences.closed().get_mut(id).map(|c| {
            c.files = Hashing::Running;
            c.generation
        }) else {
            return;
        };
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
            // Still this close: a reopen meanwhile dropped it, and a later close of the same
            // partition has its own hash (cli692's S2).
            if let Some(c) = node.fences.closed().get_mut(&id)
                && c.generation == generation
            {
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
    let mut files = pigeonhole::Pigeonhole::database_files(&main)
        .map_err(|e| ApiError::internal(format!("list {}'s files: {e}", main.display())))?;
    let prefix = format!("{id}.phdb");
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let n = e.file_name().to_string_lossy().into_owned();
            if n.starts_with(&prefix) && n.contains(".replaced") {
                files.push(e.path());
            }
        }
    }
    if files.is_empty() {
        return Err(ApiError::not_found(format!(
            "this node has no files of partition {id}"
        )));
    }
    // The epoch from the file's identity, read without opening it for writes; 0 when a
    // drop that stopped part way already moved the main file.
    let epoch = if main.exists() {
        super::identity::read_file(&main)
            .map_err(ApiError::internal)?
            .map_or(0, |i| i.epoch)
    } else {
        0
    };
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
    // The main file last (cli692's N6 on #1755): a drop that stops part way leaves it, or
    // only sidecars, and a retry lists what's left and finishes.
    files.sort_by_key(|f| *f == main);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn closed(files: Hashing) -> Closed {
        Closed {
            epoch: 3,
            files,
            generation: 0,
            error: None,
            db_open: false,
        }
    }

    #[test]
    fn a_closed_file_is_given_only_once_its_hash_listed_it() {
        let file = |name: &str| PartitionFile {
            name: name.into(),
            bytes: 7,
            sha256: vec![1; 32].into(),
        };
        let e = closed(Hashing::Running).file("p1", "").unwrap_err();
        assert_eq!(e.code, "FailedPrecondition");
        assert!(e.message.contains("still being hashed"), "{}", e.message);
        let e = closed(Hashing::Failed("disk".into()))
            .file("p1", "")
            .unwrap_err();
        assert_eq!(e.code, "FailedPrecondition");
        assert!(e.message.contains("close it again"), "{}", e.message);
        assert!(e.message.contains("disk"), "{}", e.message);
        let done = closed(Hashing::Done(vec![file(""), file("-wal-0"), file("-feed")]));
        assert_eq!(done.file("p1", "").unwrap(), &file(""));
        assert_eq!(done.file("p1", "-feed").unwrap(), &file("-feed"));
        let e = done.file("p1", "-wal-1").unwrap_err();
        assert_eq!(e.code, "NotFound");
        assert!(
            e.message.contains("-wal-0"),
            "names the files it has: {}",
            e.message
        );
    }
}

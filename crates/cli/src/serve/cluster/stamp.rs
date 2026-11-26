//! `Cluster.Stamp` (D317 ticket 6, §1 and §3): on the node a partition moves to (or is
//! restored on), install its staged files, if a complete set is staged, then raise the
//! file's epoch to the one the map is about to give it.
//!
//! The install is resumable (feed1248's F1): before its first rename it writes
//! `.phdb-incoming/<P>.installing` (the set's header and manifest, synced). It moves the
//! partition's other files aside to `<file>.replaced`, renames the staged files in, syncs
//! the directory, and removes the marker last, then the set's leftovers (N2). A stamp or an
//! open that finds the marker rolls the install forward: it renames what's still staged,
//! and checks the files already renamed against the manifest's SHA-256. The partition never
//! opens while the marker is there. Each staged file is checked against the manifest before
//! any rename (F2): a mismatch refuses the install and leaves the staging as it was.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::upload::{self, INCOMING};
use super::{Node, identity};
use crate::serve::errors::ApiError;
use crate::serve::proto::{ClusterMap, PartitionFile, PartitionUploadHeader, StampRequest};

/// A fault seam (tests): the next install in each listed cluster directory fails after its
/// first rename, as a crash there would leave it. Keyed by the directory, one entry each, so
/// other tests' installs run on and tests arming it at once don't take each other's.
#[cfg(test)]
pub(crate) static FAIL_AFTER_FIRST_RENAME: std::sync::Mutex<Vec<PathBuf>> =
    std::sync::Mutex::new(Vec::new());

/// `.phdb-incoming/<P>.installing`.
fn marker(dir: &Path, partition: &str) -> PathBuf {
    dir.join(INCOMING).join(format!("{partition}.installing"))
}

/// A promote's intent (#2156): written and synced before a stamp promotes its copy, and
/// removed once the copy's identity is at the new epoch. The copy's promote and its identity
/// write are two steps, so a stop between them leaves a primary at its bootstrap epoch;
/// this tells that promote, run again, from a file the map has moved past. Beside the
/// partition's files, which a drop moves aside with it.
pub(crate) fn promote_intent(dir: &Path, partition: &str) -> PathBuf {
    dir.join(format!("{partition}.phdb.promoting"))
}

/// What a promote's intent records: the cluster, the partition, the copy's bootstrap epoch
/// and the epoch it's stamped at.
fn intent_of(req: &StampRequest) -> String {
    format!(
        "{}\n{}\n{}\n{}\n",
        req.cluster_id, req.partition, req.replica_epoch, req.to_epoch
    )
}

/// Writes and syncs `req`'s intent before its promote.
fn write_intent(dir: &Path, req: &StampRequest) -> Result<(), ApiError> {
    use std::io::Write as _;
    let path = promote_intent(dir, &req.partition);
    let write = || -> std::io::Result<()> {
        let mut f = std::fs::File::create(&path)?;
        f.write_all(intent_of(req).as_bytes())?;
        f.sync_all()
    };
    write().map_err(|e| ApiError::internal(format!("write {}: {e}", path.display())))?;
    upload::sync_dir(dir);
    Ok(())
}

/// Whether `req` is the promote whose intent is recorded: the same one, run again.
fn intent_matches(dir: &Path, req: &StampRequest) -> bool {
    std::fs::read_to_string(promote_intent(dir, &req.partition)).is_ok_and(|s| s == intent_of(req))
}

/// A promote's fork token beside its intent (D336, #2148): the copy's primary token from
/// before the promote, hex-encoded. A file of its own, so the intent stays what #2156 wrote
/// and a node rolled back to 1.3.1 still matches it (cli692's (1) and (2) on Amendment 4).
/// Written and synced with the intent, removed with it, and moved aside with the
/// partition's files.
pub(crate) fn promote_fork(dir: &Path, partition: &str) -> PathBuf {
    dir.join(format!("{partition}.phdb.promoting.fork"))
}

/// Writes and syncs `token` as `partition`'s promote fork token (synced with the intent by
/// [`write_intent`]'s directory sync, which follows it).
fn write_fork(dir: &Path, partition: &str, token: &[u8]) -> Result<(), ApiError> {
    use std::io::Write as _;
    let path = promote_fork(dir, partition);
    let write = || -> std::io::Result<()> {
        let mut f = std::fs::File::create(&path)?;
        f.write_all(hex(token).as_bytes())?;
        f.sync_all()
    };
    write().map_err(|e| ApiError::internal(format!("write {}: {e}", path.display())))
}

/// The promote fork token beside `partition`'s intent: empty when there's none (a 1.3
/// build's intent) or it can't be read (cli692's (3): never an error).
fn read_fork(dir: &Path, partition: &str) -> Vec<u8> {
    let path = promote_fork(dir, partition);
    match std::fs::read_to_string(&path) {
        Ok(s) => unhex(s.trim()).unwrap_or_else(|| {
            eprintln!(
                "phdb serve: {} isn't hex: the fork token is unknown",
                path.display()
            );
            Vec::new()
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => {
            eprintln!(
                "phdb serve: read {}: {e}: the fork token is unknown",
                path.display()
            );
            Vec::new()
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// A test's fault seam (#2156): a promote's stamp on a node in each listed cluster
/// directory stops after the copy's promote, before its identity, as a crash there would.
#[cfg(test)]
pub(crate) static STOP_AFTER_PROMOTE: std::sync::Mutex<Vec<PathBuf>> =
    std::sync::Mutex::new(Vec::new());

/// Partition `partition`'s main file in `dir`.
fn main_file(dir: &Path, partition: &str) -> PathBuf {
    dir.join(format!("{partition}.phdb"))
}

/// `name`'s file of partition `partition` in `dir` (`name` a suffix: `""`, `-wal-<n>`,
/// `-feed`).
fn target(dir: &Path, partition: &str, name: &str) -> PathBuf {
    dir.join(format!("{partition}.phdb{name}"))
}

/// Whether `file` is `entry` (its size and SHA-256).
fn matches(file: &Path, entry: &PartitionFile) -> Result<bool, ApiError> {
    let bytes = std::fs::metadata(file)
        .map_err(|e| ApiError::internal(format!("read {}: {e}", file.display())))?
        .len();
    Ok(bytes == entry.bytes && upload::digest(file)?.as_slice() == entry.sha256.as_ref())
}

/// Finishes an install a crash or a failure left behind, if its marker is there: before
/// any open of the partition, and before a stamp. The set it installed, if any.
pub(crate) fn finish_install(
    dir: &Path,
    partition: &str,
) -> Result<Option<Vec<PartitionFile>>, ApiError> {
    let marker = marker(dir, partition);
    if !marker.exists() {
        return Ok(None);
    }
    let header = upload::read_header(&marker).map_err(ApiError::internal)?;
    eprintln!("phdb serve: partition {partition}: finishing an interrupted install of its files");
    install(dir, partition, &header).map(Some)
}

/// Moves `file` aside to `<file>.replaced-<unix nanos>`: never deleted, and never over an
/// earlier install's (feed1248's S2 on #1755).
fn move_aside(file: &Path) -> Result<(), ApiError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    std::fs::rename(file, upload::path_with(file, &format!(".replaced-{now}")))
        .map_err(|e| ApiError::internal(format!("move {} aside: {e}", file.display())))
}

/// Installs `header`'s set of partition `partition`, its marker written: the partition's
/// files the set doesn't have moved aside, then each staged file checked against the
/// manifest and renamed in, the file it replaces moved aside first; the marker removed,
/// the leftovers deleted.
fn install(
    dir: &Path,
    partition: &str,
    header: &PartitionUploadHeader,
) -> Result<Vec<PartitionFile>, ApiError> {
    let incoming = dir.join(INCOMING);
    let main = main_file(dir, partition);
    let named: Vec<&str> = header.manifest.iter().map(|f| f.name.as_str()).collect();
    // The partition's files the set doesn't have: aside, never deleted.
    for file in pigeonhole::Pigeonhole::database_files(&main)
        .map_err(|e| ApiError::internal(format!("list {}'s files: {e}", main.display())))?
    {
        let keep = pigeonhole::Pigeonhole::database_file_suffix(&main, &file)
            .is_some_and(|s| named.contains(&s));
        if !keep {
            move_aside(&file)?;
        }
    }
    #[cfg(test)]
    let mut renamed = 0;
    for f in &header.manifest {
        let staged = incoming.join(upload::ready_name(partition, &f.name));
        let to = target(dir, partition, &f.name);
        if staged.exists() {
            // Checked again right before its rename, an interrupted install's included
            // (feed1248's S1): the staging may have changed since the set was checked.
            if !matches(&staged, f)? {
                return Err(ApiError::failed_precondition(format!(
                    "the staged file {:?} of partition {partition} no longer matches the \
                     install's manifest: upload the set again",
                    f.name
                )));
            }
            // The file it replaces (a move back to a node that kept its copy, an in-place
            // restore): aside, never overwritten (feed1248's S2).
            if to.exists() {
                move_aside(&to)?;
            }
            std::fs::rename(&staged, &to)
                .map_err(|e| ApiError::internal(format!("install {}: {e}", to.display())))?;
            #[cfg(test)]
            {
                renamed += 1;
                let mut armed = FAIL_AFTER_FIRST_RENAME
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if renamed == 1
                    && let Some(i) = armed.iter().position(|d| d == dir)
                {
                    armed.swap_remove(i);
                    return Err(ApiError::internal(
                        "the test's fault after the first rename",
                    ));
                }
            }
        } else if !matches(&to, f)? {
            // Renamed by the install this one finishes: it must be the manifest's.
            return Err(ApiError::internal(format!(
                "{} doesn't match its manifest entry: the install can't finish",
                to.display()
            )));
        }
    }
    upload::sync_dir(dir);
    // The set's headers, and the partition's other staged files and headers (N2), before
    // the marker: a stop between the two then leaves the marker, whose finish finds the
    // files renamed and matching, rather than headers that say an upload is complete with
    // no file behind them, which a move's rerun would skip for good (#1848).
    if let Ok(entries) = std::fs::read_dir(&incoming) {
        let prefix = format!("{partition}.phdb");
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            let staged = name.ends_with(".ready") || name.ends_with(".ready.header");
            if staged && name.starts_with(&prefix) {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }
    upload::sync_dir(&incoming);
    std::fs::remove_file(marker(dir, partition))
        .map_err(|e| ApiError::internal(format!("remove the install's marker: {e}")))?;
    upload::sync_dir(&incoming);
    Ok(header.manifest.clone())
}

impl Node {
    /// `Cluster.Stamp`: installs partition `req.partition`'s staged set, if one is complete
    /// (or finishes an interrupted install), then raises its file's epoch from `from_epoch`
    /// to `to_epoch`. Refused while the partition is open here. The set installed, if any,
    /// and a promote's fork token (D336; empty when unknown, and for every other stamp).
    /// Run again once done, it answers success with nothing to do but the token.
    pub(crate) async fn stamp(
        self: &Arc<Self>,
        req: StampRequest,
    ) -> Result<(Vec<PartitionFile>, Vec<u8>), ApiError> {
        let _one = self.fences.changing.lock().await;
        let _open = self.opening.lock().await;
        let p = req.partition.clone();
        if !super::map::id_ok(&p) {
            return Err(ApiError::invalid(format!("{p:?} isn't a partition id")));
        }
        // A stamp may skip epochs: every change that raises P's epoch goes above each epoch a
        // promote of it recorded, so a copy an unfinished or abandoned one stamped is never
        // taken for this history (D330 PR 2, simp724's S2; every change since #2070).
        if req.to_epoch <= req.from_epoch {
            return Err(ApiError::invalid(format!(
                "a stamp raises the epoch: from {} to {}, not {}",
                req.from_epoch,
                req.from_epoch + 1,
                req.to_epoch
            )));
        }
        if req.create {
            return Ok((self.create(req).await?, Vec::new()));
        }
        if self.part_state(&p).is_some() {
            return Err(ApiError::failed_precondition(format!(
                "partition {p:?} is open here: fence and close it first (phdb cluster close)"
            )));
        }
        // A replica copy of P here holds `P.phdb`, which this would stamp or replace with an
        // uploaded set (the one place a set is installed): refused on the node too, whatever
        // map the caller read (D328, simp724's S4 condition on PR 1).
        if self.replica_open(&p) {
            return Err(ApiError::failed_precondition(format!(
                "this node holds an open replica copy of partition {p:?}: set a map without \
                 this node in {p}'s replicas, move the copy aside (phdb cluster \
                 drop-partition {p} here), then run the move, relocate or restore again"
            )));
        }
        let dir = self.dir.clone();
        let restore = req.restore;
        let (installed, fork) =
            tokio::task::spawn_blocking(move || stamp_files(&dir, &req)).await??;
        // An operator's fence here was on the copy this node held before (a move away
        // fences it): a set installed now is another copy, at another epoch. It opens
        // fenced, and serves once the map gives it here. A stamp that installs nothing
        // leaves the copy the operator fenced, and its fence. A restore's partitions keep
        // the operator's fence, which the restore lifts itself once its map is set.
        if !restore && !installed.is_empty() {
            let mut fenced = self.fences.operator().clone();
            if fenced.remove(&p) {
                self.persist_fences(fenced).await?;
            }
        }
        // Its files changed: a close's hashes of them go, and a map that gives it here
        // opens it.
        self.fences.forget_closed(&p);
        Ok((installed, fork))
    }
}

impl Node {
    /// [`Node::stamp`] with `create` (D331 PR 5, audit120's four points): a split's new
    /// partition, created empty with its identity at `to_epoch` and marked for the split's
    /// move and map version. Under the partition-open lock, which the open and the reconcile
    /// of split-created files take too. Refused for anything already under the id here: an
    /// open or closed partition, a replica copy, a staged upload or install, or a file
    /// that isn't this split's empty one. Run again on that empty file, it refreshes the
    /// mark (the driver does before each COPYING attempt).
    async fn create(self: &Arc<Self>, req: StampRequest) -> Result<Vec<PartitionFile>, ApiError> {
        let p = req.partition.clone();
        if req.restore || req.promote || !req.restore_from_cluster_id.is_empty() {
            return Err(ApiError::invalid(
                "a create makes a new, empty partition: no restore and no promotion",
            ));
        }
        if req.create_move.is_empty() || req.create_version == 0 {
            return Err(ApiError::invalid(
                "a create names its split's move and the map version its change carries",
            ));
        }
        // Served, closed (its files maybe hashed for a download) or being opened here: not
        // a new partition (audit120's S1 on #2130).
        if !matches!(self.fences.now(&p, |_| {}), super::fence::Now::Nothing) {
            return Err(ApiError::failed_precondition(format!(
                "partition {p:?} is open, closed or opening here already: a split creates a new \
                 one"
            )));
        }
        // This node's cluster, and an id its map doesn't list: the reconcile skips a listed
        // id, so such a file would stay (audit120's S2).
        if let Ok(map) = self.get_map() {
            if req.cluster_id != map.cluster_id {
                return Err(ApiError::invalid(format!(
                    "the create is for cluster {:?}; this node is in {:?}",
                    req.cluster_id, map.cluster_id
                )));
            }
            if map.partitions.iter().any(|q| q.id == p) {
                return Err(ApiError::failed_precondition(format!(
                    "the cluster map (version {}) lists partition {p:?} already: a split \
                     creates a new one",
                    map.map_version
                )));
            }
        }
        if self.replica_open(&p) {
            return Err(ApiError::failed_precondition(format!(
                "this node holds a replica copy of partition {p:?}: a split creates a new one"
            )));
        }
        let dir = self.dir.clone();
        tokio::task::spawn_blocking(move || create_file(&dir, &req)).await??;
        // An operator fence left on the id by an earlier partition of that name (an aborted
        // split's, whose files were moved aside by the reconcile) isn't this one's (audit120's
        // S3). The stamp holds `changing`.
        self.forget_operator_fence(&p).await?;
        Ok(Vec::new())
    }
}

impl Node {
    /// Brings this node's split-created partition files in line with `map` (D331 PR 5,
    /// cli692's Must 5 and audit120's points 1, 2 and 4), on each map and at the node's
    /// start:
    /// - **a finished split:** a partition open here, which the map places here holding a
    ///   range, whose mark names a move the map no longer has, loses its mark: it's like any
    ///   other partition from then on;
    /// - **an unfinished one:** a file here that isn't open, that the map doesn't place, whose
    ///   mark names a move the map no longer has, from a map past the mark's version, and that
    ///   holds no row, is moved aside into `.phdb-dropped/` (never deleted), under the
    ///   partition-open lock: a split whose COPYING change never landed, or an abort whose
    ///   CLI stopped before its drop-partition.
    pub(crate) async fn reconcile_split_files(self: &Arc<Self>, map: &ClusterMap) {
        let _open = self.opening.lock().await;
        let shared = self.server.get().and_then(|s| s.shared.upgrade());
        let parts = shared
            .as_ref()
            .and_then(|sh| sh.router.cluster.as_ref().map(|c| c.parts.load_full()));
        let moving = |id: &str| map.moves.iter().any(|r| r.id == id);
        let past =
            |mark: &identity::SplitMark| map.map_version > mark.version && !moving(&mark.move_id);
        if let Some(parts) = &parts {
            for (id, service) in parts.iter() {
                if !holds_a_range(map, id, &self.id) {
                    continue;
                }
                let Ok(part) = service.part() else {
                    continue;
                };
                match identity::split_mark(&part.db) {
                    Ok(Some(mark)) if past(&mark) => match identity::clear_split_mark(&part.db) {
                        Ok(()) => eprintln!(
                            "phdb serve: partition {id}: split move {} finished; its \
                                 split-created mark is cleared",
                            mark.move_id
                        ),
                        Err(e) => eprintln!(
                            "phdb serve: partition {id}: clearing its split-created mark: {e}"
                        ),
                    },
                    Ok(_) => {}
                    Err(e) => eprintln!("phdb serve: partition {id}: reading its split mark: {e}"),
                }
            }
        }
        // The files here nothing has open, and that the map doesn't place.
        let mut skip: std::collections::BTreeSet<String> =
            map.partitions.iter().map(|p| p.id.clone()).collect();
        if let Some(parts) = &parts {
            skip.extend(parts.keys().cloned());
        }
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        let candidates: Vec<String> = entries
            .flatten()
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                let id = name.strip_suffix(".phdb")?.to_owned();
                (super::map::id_ok(&id) && !skip.contains(&id)).then_some(id)
            })
            .filter(|id| self.part_state(id).is_none() && !self.replica_open(id))
            .filter(|id| !self.split_unmarked(id))
            .collect();
        for id in candidates {
            let dir = self.dir.clone();
            let map = map.clone();
            let part = id.clone();
            let outcome = tokio::task::spawn_blocking(move || -> Result<Option<String>, String> {
                let main = main_file(&dir, &part);
                if marker(&dir, &part).exists() || staged_any(&dir, &part) {
                    return Ok(None);
                }
                let db = pigeonhole::Pigeonhole::open(
                    &main,
                    pigeonhole::Options::default().read_only(true),
                )
                .map_err(|e| format!("open: {e}"))?;
                let read =
                    identity::split_mark(&db).and_then(|m| Ok((m, identity::holds_no_rows(&db)?)));
                let _ = db.close();
                let (mark, empty) = read?;
                let Some(mark) = mark else {
                    return Ok(Some(String::new()));
                };
                let gone = map.map_version > mark.version
                    && !map.moves.iter().any(|r| r.id == mark.move_id);
                if !gone || !empty {
                    return Ok(None);
                }
                let lp = super::close::move_aside(&dir, &part).map_err(|e| e.message)?;
                Ok(Some(format!("{} (split move {})", lp.file, mark.move_id)))
            })
            .await;
            match outcome {
                // Not split-created: never looked at again while its file stays the same.
                Ok(Ok(Some(moved))) if moved.is_empty() => self.note_unmarked(&id),
                Ok(Ok(Some(moved))) => {
                    self.fences.forget_closed(&id);
                    eprintln!(
                        "phdb serve: partition {id}: an empty split-created file the map doesn't \
                         place, moved to {moved}: delete it once checked"
                    );
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => eprintln!("phdb serve: partition {id}: checking its file: {e}"),
                Err(e) => eprintln!("phdb serve: partition {id}: checking its file: {e}"),
            }
        }
    }

    /// Whether `id`'s file was found unmarked and hasn't changed since.
    fn split_unmarked(&self, id: &str) -> bool {
        let seen = file_seen(&main_file(&self.dir, id));
        self.unmarked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .is_some_and(|s| Some(*s) == seen)
    }

    fn note_unmarked(&self, id: &str) {
        if let Some(seen) = file_seen(&main_file(&self.dir, id)) {
            self.unmarked
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(id.to_owned(), seen);
        }
    }
}

/// A file's size and modification time: whether it changed.
pub(crate) type Seen = (u64, std::time::SystemTime);

fn file_seen(path: &Path) -> Option<Seen> {
    let m = std::fs::metadata(path).ok()?;
    Some((m.len(), m.modified().ok()?))
}

/// Whether `map` places partition `id` on node `node` holding a range of some table.
fn holds_a_range(map: &ClusterMap, id: &str, node: &str) -> bool {
    map.partitions.iter().any(|p| p.id == id && p.node == node)
        && map
            .tables
            .iter()
            .any(|t| super::map::table_ranges(t).iter().any(|(_, _, p)| *p == id))
}

/// [`Node::create`]'s work on the files, on a blocking thread: create-new, never
/// truncating; the same empty, marked file again only takes the new mark.
fn create_file(dir: &Path, req: &StampRequest) -> Result<(), ApiError> {
    let p = &req.partition;
    if marker(dir, p).exists() || upload::ready_set(dir, p).is_some() || staged_any(dir, p) {
        return Err(ApiError::failed_precondition(format!(
            "this node has files of partition {p} staged or installing: a split creates a new \
             partition"
        )));
    }
    let main = main_file(dir, p);
    let files = pigeonhole::Pigeonhole::database_files(&main)
        .map_err(|e| ApiError::internal(format!("list {}'s files: {e}", main.display())))?;
    let mark = identity::SplitMark {
        move_id: req.create_move.clone(),
        version: req.create_version,
    };
    let options = pigeonhole::Options::default()
        .default_shards(1)
        .pin_threads(false)
        .memtable_budget(1 << 20)
        .block_cache(1 << 20)
        .row_cache(0);
    if !files.contains(&main) {
        if !files.is_empty() {
            return Err(ApiError::failed_precondition(format!(
                "this node has files of partition {p} but not its main file: move them aside \
                 (phdb cluster drop-partition {p} on this node)"
            )));
        }
        let db = pigeonhole::Pigeonhole::open(&main, options.create_if_missing(true))
            .map_err(|e| ApiError::internal(format!("create {}: {e}", main.display())))?;
        let made = identity::write(
            &db,
            &identity::Identity {
                cluster_id: req.cluster_id.clone(),
                partition_id: p.clone(),
                epoch: req.to_epoch,
            },
        )
        .and_then(|()| identity::set_split_mark(&db, &mark));
        let closed = db.close();
        made.map_err(ApiError::internal)?;
        closed.map_err(|e| ApiError::internal(format!("close {}: {e}", main.display())))?;
        upload::sync_dir(dir);
        eprintln!(
            "phdb serve: partition {p} created empty for split move {}",
            req.create_move
        );
        return Ok(());
    }
    // There already: this split's empty file (a rerun), or refused. Checked read-only; opened
    // for writes only to refresh the mark (audit120's S1).
    let db = pigeonhole::Pigeonhole::open(&main, pigeonhole::Options::default().read_only(true))
        .map_err(|e| ApiError::internal(format!("open {}: {e}", main.display())))?;
    let checked = (|| {
        let id = identity::read(&db).map_err(ApiError::internal)?;
        let same = id.as_ref().is_some_and(|id| {
            id.cluster_id == req.cluster_id && id.partition_id == *p && id.epoch == req.to_epoch
        });
        // This split's mark: the same move (cli692's nit).
        let marked = identity::split_mark(&db)
            .map_err(ApiError::internal)?
            .is_some_and(|m| m.move_id == req.create_move);
        let empty = identity::holds_no_rows(&db).map_err(ApiError::internal)?;
        if !(same && marked && empty) {
            return Err(ApiError::failed_precondition(format!(
                "{} exists and isn't an empty partition a split created as {p} of cluster {:?} \
                 at epoch {}: a split creates a new partition, so pick another id",
                main.display(),
                req.cluster_id,
                req.to_epoch
            )));
        }
        Ok(())
    })();
    let closed = db.close();
    checked?;
    closed.map_err(|e| ApiError::internal(format!("close {}: {e}", main.display())))?;
    let db = super::open_retrying(&main, &options)
        .map_err(|e| ApiError::internal(format!("open {}: {e}", main.display())))?;
    let marked = identity::set_split_mark(&db, &mark);
    let closed = db.close();
    marked.map_err(ApiError::internal)?;
    closed.map_err(|e| ApiError::internal(format!("close {}: {e}", main.display())))?;
    Ok(())
}

/// Whether `.phdb-incoming` holds any staged file or header of partition `p`, complete or
/// not.
fn staged_any(dir: &Path, p: &str) -> bool {
    let prefix = format!("{p}.phdb");
    std::fs::read_dir(dir.join(INCOMING)).is_ok_and(|entries| {
        entries
            .flatten()
            .any(|e| e.file_name().to_string_lossy().starts_with(&prefix))
    })
}

/// [`Node::stamp`]'s work on the files, on a blocking thread.
/// The set it installed, and a promote's fork token (D336; empty when unknown or not a
/// promote).
fn stamp_files(dir: &Path, req: &StampRequest) -> Result<(Vec<PartitionFile>, Vec<u8>), ApiError> {
    let p = &req.partition;
    if req.promote && (req.restore || upload::ready_set(dir, p).is_some()) {
        return Err(ApiError::invalid(
            "a promotion stamps the replica copy's own file: no restore, and no staged set",
        ));
    }
    let mut installed = if req.promote {
        Vec::new()
    } else {
        finish_install(dir, p)?.unwrap_or_default()
    };
    if installed.is_empty()
        && !req.promote
        && let Some((header, staged)) = upload::ready_set(dir, p)
    {
        let want_cluster = if req.restore_from_cluster_id.is_empty() {
            &req.cluster_id
        } else {
            &req.restore_from_cluster_id
        };
        let epoch_ok = if req.restore {
            header.epoch <= req.from_epoch
        } else {
            header.epoch == req.from_epoch
        };
        if header.partition != *p || header.cluster_id != *want_cluster || !epoch_ok {
            return Err(ApiError::failed_precondition(format!(
                "the staged files are partition {:?} of cluster {:?} at epoch {}: not what this \
                 stamp installs",
                header.partition, header.cluster_id, header.epoch
            )));
        }
        // F2: every staged file against its manifest entry, before any rename.
        for ((name, path), entry) in staged.iter().zip(&header.manifest) {
            if !matches(path, entry)? {
                return Err(ApiError::failed_precondition(format!(
                    "the staged file {name:?} of partition {p} doesn't match its manifest's \
                     size and SHA-256: upload it again"
                )));
            }
        }
        upload::write_header(&marker(dir, p), &header)?;
        upload::sync_dir(&dir.join(INCOMING));
        installed = install(dir, p, &header)?;
    }
    let main = main_file(dir, p);
    if !main.exists() {
        return Err(ApiError::failed_precondition(format!(
            "this node has no files of partition {p}: upload them first"
        )));
    }
    // The file, opened alone and not served, for its identity row: one shard by default
    // (a feed's recorded count is kept), small memory and no pinned threads, since it only
    // writes that row (feed1248's S3).
    let options = pigeonhole::Options::default()
        .default_shards(1)
        .pin_threads(false)
        .memtable_budget(1 << 20)
        .block_cache(1 << 20)
        .row_cache(0);
    // A Status read of the file at this moment holds its lock briefly: retried (#1770).
    let db = super::open_retrying(&main, &options)
        .map_err(|e| ApiError::internal(format!("open {}: {e}", main.display())))?;
    let stamped = (|| {
        let id = identity::read(&db)
            .map_err(ApiError::internal)?
            .ok_or_else(|| {
                ApiError::failed_precondition(format!(
                    "{} has no identity row: it isn't a cluster partition",
                    main.display()
                ))
            })?;
        if id.partition_id != *p {
            return Err(ApiError::failed_precondition(format!(
                "{} is partition {:?}, not {p:?}",
                main.display(),
                id.partition_id
            )));
        }
        // Run again once done: nothing to do but answer a promote's fork token, from the row
        // its identity commit wrote (D336: a promoter whose reply was lost asks again).
        // Copy-move, relocate and restore reruns come here too, and answer none.
        if id.cluster_id == req.cluster_id && id.epoch == req.to_epoch {
            if !req.promote {
                return Ok(Vec::new());
            }
            return Ok(identity::fork_token(&db, req.to_epoch)
                .unwrap_or_else(|e| {
                    eprintln!("phdb serve: partition {p}'s fork token: {e}: unknown");
                    None
                })
                .unwrap_or_default());
        }
        let cluster_ok = if req.restore_from_cluster_id.is_empty() {
            id.cluster_id == req.cluster_id
        } else {
            id.cluster_id == req.restore_from_cluster_id
        };
        // A promotion's copy records the owner's epoch at its bootstrap, `e_b` (D328's
        // amendment): the CLI checked `history_epoch <= e_b <= from_epoch`.
        let epoch_ok = if req.promote {
            id.epoch == req.replica_epoch && req.replica_epoch <= req.from_epoch
        } else if req.restore {
            id.epoch <= req.from_epoch
        } else {
            id.epoch == req.from_epoch
        };
        if !cluster_ok || !epoch_ok {
            return Err(ApiError::failed_precondition(format!(
                "{} is partition {p} of cluster {:?} at epoch {}: the stamp expects cluster {:?} \
                 at epoch {}{}",
                main.display(),
                id.cluster_id,
                id.epoch,
                if req.restore_from_cluster_id.is_empty() {
                    &req.cluster_id
                } else {
                    &req.restore_from_cluster_id
                },
                req.from_epoch,
                if req.restore {
                    " or below (a restore)"
                } else {
                    ""
                }
            )));
        }
        // A replica database takes no local write (R6) until it's a primary: at the last
        // commit it applied, as `phdb promote` makes one. Once promoted it can't follow
        // again, so a promote that stops after this is finished or its file moved aside.
        if req.promote {
            let following = db
                .replicated_seqno()
                .map_err(|e| ApiError::internal(format!("read the copy's state: {e}")))?
                .is_some();
            // Already a primary, and not at `to_epoch` (done, above): this promote, run
            // again after it stopped between its promote and its identity, if its intent says
            // so (#2156), so only the identity is left to write. Otherwise an earlier promote
            // stamped it and the map has moved since (a set-map, relocate or promote):
            // stamping it again would give P this file's history over whatever P took since
            // (simp724's M1 on #2007).
            if following {
                // D336: the copy's primary token, the fork point, before the promote drops
                // it, kept beside the intent (cli692's note: read in this branch).
                let token = db
                    .replicated_token()
                    .map_err(|e| ApiError::internal(format!("read the copy's state: {e}")))?
                    .map(|t| t.as_bytes().to_vec())
                    .unwrap_or_default();
                write_fork(dir, p, &token)?;
                write_intent(dir, req)?;
                db.promote()
                    .map_err(|e| ApiError::internal(format!("promote {}: {e}", main.display())))?;
                #[cfg(test)]
                {
                    let mut armed = STOP_AFTER_PROMOTE
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(i) = armed.iter().position(|d| dir.starts_with(d)) {
                        armed.swap_remove(i);
                        return Err(ApiError::internal(
                            "the test's fault between the copy's promote and its identity",
                        ));
                    }
                }
            } else if !intent_matches(dir, req) {
                return Err(ApiError::failed_precondition(format!(
                    "{} is already a primary, at epoch {}: an earlier promote stopped after \
                     promoting it, and the map has moved on (this promote is from epoch {}). \
                     Stamped again it would replace what {p} took since. Check the map; to \
                     make this node a replica of {p} again, move the file aside (phdb cluster \
                     drop-partition {p} on this node)",
                    main.display(),
                    id.epoch,
                    req.from_epoch
                )));
            }
        }
        let id = identity::Identity {
            cluster_id: req.cluster_id.clone(),
            partition_id: p.clone(),
            epoch: req.to_epoch,
        };
        // D336: a promote's fork token, kept beside its intent before the promote (a first
        // run or a rerun alike), goes in with the identity, so it's durable before the
        // intent is removed.
        let fork = if req.promote {
            read_fork(dir, p)
        } else {
            Vec::new()
        };
        if req.promote {
            identity::write_promoted(&db, &id, &fork)
        } else if req.restore_from_cluster_id.is_empty() {
            identity::write(&db, &id)
        } else {
            identity::restore(&db, &id, &req.restore_from_cluster_id)
        }
        .map_err(ApiError::internal)?;
        // D331 PR 3: a restored file is clipped to its map's ranges when it opens, before it
        // serves (a cluster backup has no common point, so a moved range's rows may be in two
        // partitions' copies).
        if req.restore {
            crate::serve::ranges::mark_clip(&db).map_err(ApiError::internal)?;
        }
        Ok(fork)
    })();
    let closed = db.close();
    let fork = stamped?;
    closed.map_err(|e| ApiError::internal(format!("close {}: {e}", main.display())))?;
    // The identity is at the new epoch and closed: a promote's intent has done its work.
    // Its fork token is in the file now (D336), so its side file goes too, after the intent.
    if req.promote {
        for path in [promote_intent(dir, p), promote_fork(dir, p)] {
            match std::fs::remove_file(&path) {
                Ok(()) => upload::sync_dir(dir),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => eprintln!("phdb serve: remove {}: {e}", path.display()),
            }
        }
    }
    Ok((installed, fork))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::proto::PartitionUploadPurpose;

    fn dir() -> PathBuf {
        let d = crate::test_dir::fresh("phdb-stamp-");
        std::fs::create_dir_all(d.join(INCOMING)).unwrap();
        d
    }

    /// A closed partition `p` in `dir`, stamped as `cluster` at `epoch`.
    fn partition(dir: &Path, p: &str, cluster: &str, epoch: u64) -> PathBuf {
        let main = main_file(dir, p);
        let db = pigeonhole::Pigeonhole::open(
            &main,
            pigeonhole::Options::default()
                .create_if_missing(true)
                .shards(1)
                .pin_threads(false),
        )
        .unwrap();
        identity::write(
            &db,
            &identity::Identity {
                cluster_id: cluster.into(),
                partition_id: p.into(),
                epoch,
            },
        )
        .unwrap();
        db.close().unwrap();
        main
    }

    /// Stages every file of the database at `main` in `dir` as partition `p`'s set, its
    /// header naming `cluster` at `epoch`, as an upload leaves it. The manifest.
    fn stage(dir: &Path, p: &str, main: &Path, cluster: &str, epoch: u64) -> Vec<PartitionFile> {
        let files: Vec<(PartitionFile, PathBuf)> = pigeonhole::Pigeonhole::database_files(main)
            .unwrap()
            .into_iter()
            .map(|f| {
                let data = std::fs::read(&f).unwrap();
                let name = pigeonhole::Pigeonhole::database_file_suffix(main, &f)
                    .unwrap()
                    .to_owned();
                let entry = PartitionFile {
                    name,
                    bytes: data.len() as u64,
                    sha256: <sha2::Sha256 as sha2::Digest>::digest(&data)
                        .to_vec()
                        .into(),
                };
                (entry, f)
            })
            .collect();
        let manifest: Vec<PartitionFile> = files.iter().map(|(e, _)| e.clone()).collect();
        for (i, (entry, from)) in files.iter().enumerate() {
            let ready = dir.join(INCOMING).join(upload::ready_name(p, &entry.name));
            std::fs::copy(from, &ready).unwrap();
            let header = PartitionUploadHeader {
                partition: p.into(),
                cluster_id: cluster.into(),
                epoch,
                upload_id: format!("{i:032x}"),
                purpose: PartitionUploadPurpose::Relocate as i32,
                file: Some(entry.clone()),
                manifest: manifest.clone(),
            };
            upload::write_header(&upload::path_with(&ready, ".header"), &header).unwrap();
        }
        manifest
    }

    fn req(from: u64) -> StampRequest {
        StampRequest {
            cluster_id: "c1".into(),
            partition: "p1".into(),
            from_epoch: from,
            to_epoch: from + 1,
            restore: false,
            restore_from_cluster_id: String::new(),
            promote: false,
            replica_epoch: 0,
            create: false,
            create_move: String::new(),
            create_version: 0,
        }
    }

    fn epoch(dir: &Path) -> (String, u64) {
        let id = identity::read_file(&main_file(dir, "p1")).unwrap().unwrap();
        (id.cluster_id, id.epoch)
    }

    /// A replica copy of partition `p1` in `d`, as D328's bootstrap leaves it: a backup of a
    /// primary at epoch 1, following it from the backup's snapshot. The primary's token.
    fn replica_copy(d: &Path) -> Vec<u8> {
        let src = crate::test_dir::fresh("phdb-stamp-primary-");
        let primary = pigeonhole::Pigeonhole::open(
            main_file(&src, "p1"),
            pigeonhole::Options::default()
                .create_if_missing(true)
                .shards(1)
                .pin_threads(false)
                .change_feed(pigeonhole::FeedRetention::bytes(16 << 20)),
        )
        .unwrap();
        identity::write(
            &primary,
            &identity::Identity {
                cluster_id: "c1".into(),
                partition_id: "p1".into(),
                epoch: 1,
            },
        )
        .unwrap();
        let snap = primary.snapshot().unwrap();
        let token = primary.feed().from_snapshot(&snap).open().unwrap().token();
        primary.backup_at(&snap, main_file(d, "p1")).unwrap();
        drop(snap);
        primary.close().unwrap();
        let copy = pigeonhole::Pigeonhole::open(
            main_file(d, "p1"),
            pigeonhole::Options::default().shards(1).pin_threads(false),
        )
        .unwrap();
        copy.begin_replication(&token).unwrap();
        copy.close().unwrap();
        token.as_bytes().to_vec()
    }

    fn promote(from: u64, to: u64) -> StampRequest {
        StampRequest {
            to_epoch: to,
            promote: true,
            replica_epoch: 1,
            ..req(from)
        }
    }

    fn fork_row(d: &Path, epoch: u64) -> Option<Vec<u8>> {
        let db = pigeonhole::Pigeonhole::open(
            main_file(d, "p1"),
            pigeonhole::Options::default().shards(1).pin_threads(false),
        )
        .unwrap();
        let row = identity::fork_token(&db, epoch).unwrap();
        db.close().unwrap();
        row
    }

    /// D336 Amendment 4: a promote's stamp answers the copy's primary token, its fork point,
    /// and keeps it in the file with the identity, so the intent and its fork file go.
    #[test]
    fn a_promotes_stamp_answers_its_fork_token_and_keeps_it_with_the_identity() {
        let d = dir();
        let token = replica_copy(&d);
        let (installed, fork) = stamp_files(&d, &promote(1, 3)).unwrap();
        assert!(installed.is_empty());
        assert_eq!(fork, token);
        assert_eq!(epoch(&d), ("c1".into(), 3));
        assert_eq!(fork_row(&d, 3), Some(token));
        assert!(!promote_intent(&d, "p1").exists());
        assert!(!promote_fork(&d, "p1").exists());
    }

    /// The coordinator's test for D336 Amendment 4: the stamp's reply is lost after the
    /// identity, its fork row and the intent's removal. The promote's rerun takes the "done"
    /// path and still answers the token.
    #[test]
    fn a_promotes_rerun_after_a_lost_reply_still_answers_its_fork_token() {
        let d = dir();
        let token = replica_copy(&d);
        drop(stamp_files(&d, &promote(1, 3)).unwrap());
        assert!(!promote_intent(&d, "p1").exists(), "the intent is gone");
        let (_, fork) = stamp_files(&d, &promote(1, 3)).unwrap();
        assert_eq!(fork, token);
    }

    /// A promote stopped between the copy's promote and its identity (#2156): the rerun
    /// takes the token from the fork file beside the intent, the replica's state being gone.
    #[test]
    fn a_promote_stopped_after_its_promote_finds_its_fork_token_beside_the_intent() {
        let d = dir();
        let token = replica_copy(&d);
        STOP_AFTER_PROMOTE.lock().unwrap().push(d.clone());
        stamp_files(&d, &promote(1, 3)).unwrap_err();
        assert!(promote_intent(&d, "p1").exists());
        assert!(promote_fork(&d, "p1").exists());
        let (_, fork) = stamp_files(&d, &promote(1, 3)).unwrap();
        assert_eq!(fork, token);
        assert_eq!(fork_row(&d, 3), Some(token));
        assert!(!promote_fork(&d, "p1").exists());
    }

    /// cli692's (3) on D336 Amendment 4: no fork token is never an error. A file at the
    /// promote's epoch with no fork row (a 1.3 build's promote) answers an empty one, and so
    /// does a fork file that isn't hex.
    #[test]
    fn a_promote_without_a_fork_token_answers_an_empty_one() {
        let d = dir();
        partition(&d, "p1", "c1", 3);
        let (_, fork) = stamp_files(&d, &promote(1, 3)).unwrap();
        assert!(fork.is_empty());
        std::fs::write(promote_fork(&d, "p1"), "not hex").unwrap();
        assert!(read_fork(&d, "p1").is_empty());
        // Every other stamp's rerun answers none.
        let (_, fork) = stamp_files(&d, &req(2)).unwrap();
        assert!(fork.is_empty());
    }

    #[test]
    fn hex_round_trips() {
        let bytes: Vec<u8> = (0..=255).collect();
        assert_eq!(unhex(&hex(&bytes)), Some(bytes));
        assert_eq!(unhex("abc"), None);
        assert_eq!(unhex("zz"), None);
    }

    fn replaced(dir: &Path) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(".replaced-")
            })
            .count()
    }

    /// F2: a staged file that doesn't match its manifest refuses the install before any
    /// rename, and leaves the staging as it was.
    #[test]
    fn a_staged_file_that_doesnt_match_refuses_the_install() {
        let (src, d) = (dir(), dir());
        let main = partition(&src, "p1", "c1", 1);
        stage(&d, "p1", &main, "c1", 1);
        let ready = d.join(INCOMING).join(upload::ready_name("p1", ""));
        let mut bytes = std::fs::read(&ready).unwrap();
        bytes.push(0);
        std::fs::write(&ready, &bytes).unwrap();
        let e = stamp_files(&d, &req(1)).unwrap_err();
        assert_eq!(e.code, "FailedPrecondition", "{}", e.message);
        assert!(e.message.contains("doesn't match"), "{}", e.message);
        assert!(ready.exists());
        assert!(!marker(&d, "p1").exists());
        assert!(!main_file(&d, "p1").exists());
    }

    /// S2: the partition's files the new set replaces, or doesn't have, move aside, never
    /// overwritten; a second install keeps the first's.
    #[test]
    fn replaced_files_move_aside_and_a_second_install_keeps_the_first() {
        let (src, d) = (dir(), dir());
        // This node's own copy, at epoch 1, with a stray WAL file the new set doesn't have.
        partition(&d, "p1", "c1", 1);
        std::fs::write(d.join("p1.phdb-wal-7"), b"old").unwrap();
        let main = partition(&src, "p1", "c1", 1);
        let manifest = stage(&d, "p1", &main, "c1", 1);
        assert_eq!(stamp_files(&d, &req(1)).unwrap().0, manifest);
        assert_eq!(epoch(&d), ("c1".into(), 2));
        assert!(!d.join("p1.phdb-wal-7").exists());
        let first = replaced(&d);
        assert!(
            first >= 2,
            "the old main file and the stray WAL file: {first}"
        );
        // Again, from a new copy at epoch 2.
        let main = partition(&src, "p1", "c1", 2);
        stage(&d, "p1", &main, "c1", 2);
        stamp_files(&d, &req(2)).unwrap();
        assert_eq!(epoch(&d), ("c1".into(), 3));
        assert!(
            replaced(&d) > first,
            "the first install's .replaced files are kept"
        );
    }

    /// A restore into another cluster: the staged copy names its old cluster at an older
    /// epoch, and the stamp rewrites it to this one's.
    #[test]
    fn a_restore_rewrites_the_cluster_and_the_epoch() {
        let (src, d) = (dir(), dir());
        let main = partition(&src, "p1", "old", 3);
        stage(&d, "p1", &main, "old", 3);
        let mut r = req(5);
        r.restore = true;
        r.restore_from_cluster_id = "old".into();
        stamp_files(&d, &r).unwrap();
        assert_eq!(epoch(&d), ("c1".into(), 6));
    }

    /// A staged set of another cluster isn't installed.
    #[test]
    fn a_header_of_another_cluster_is_refused() {
        let (src, d) = (dir(), dir());
        let main = partition(&src, "p1", "other", 1);
        stage(&d, "p1", &main, "other", 1);
        let e = stamp_files(&d, &req(1)).unwrap_err();
        assert_eq!(e.code, "FailedPrecondition", "{}", e.message);
        assert!(
            e.message.contains("not what this stamp installs"),
            "{}",
            e.message
        );
        assert!(!main_file(&d, "p1").exists());
    }

    fn create_req(p: &str, move_id: &str, version: u64) -> StampRequest {
        StampRequest {
            partition: p.into(),
            from_epoch: 0,
            to_epoch: 1,
            create: true,
            create_move: move_id.into(),
            create_version: version,
            ..req(0)
        }
    }

    fn mark_of(dir: &Path, p: &str) -> Option<identity::SplitMark> {
        let db = pigeonhole::Pigeonhole::open(
            main_file(dir, p),
            pigeonhole::Options::default().read_only(true),
        )
        .unwrap();
        let mark = identity::split_mark(&db).unwrap();
        db.close().unwrap();
        mark
    }

    /// D331 PR 5 (audit120's point 3): a create makes the file empty, with its identity at
    /// `to_epoch` and the split's mark; run again on that empty file it only refreshes the
    /// mark (the driver's retry at another map version).
    #[test]
    fn a_create_makes_an_empty_marked_file_and_a_rerun_refreshes_its_mark() {
        let d = dir();
        create_file(&d, &create_req("p1-1", "split-1", 5)).unwrap();
        let id = identity::read_file(&main_file(&d, "p1-1"))
            .unwrap()
            .unwrap();
        assert_eq!(
            (id.cluster_id.as_str(), id.partition_id.as_str(), id.epoch),
            ("c1", "p1-1", 1)
        );
        assert_eq!(
            mark_of(&d, "p1-1"),
            Some(identity::SplitMark {
                move_id: "split-1".into(),
                version: 5
            })
        );
        create_file(&d, &create_req("p1-1", "split-1", 7)).unwrap();
        assert_eq!(mark_of(&d, "p1-1").map(|m| m.version), Some(7));
    }

    /// audit120's point 3: create-new. Whatever is already under the id and isn't the
    /// split's own empty file is refused, and left as it was: a partition's file (no mark),
    /// one at another epoch, a marked file holding rows, a WAL file without its main file,
    /// or a staged upload.
    #[test]
    fn a_create_refuses_anything_already_under_the_id() {
        let refused = |d: &Path, r: &StampRequest| {
            let e = create_file(d, r).unwrap_err();
            assert_eq!(e.code, "FailedPrecondition", "{}", e.message);
        };
        // A partition's own file: not split-created.
        let d = dir();
        partition(&d, "p1-1", "c1", 1);
        refused(&d, &create_req("p1-1", "split-1", 5));
        assert_eq!(mark_of(&d, "p1-1"), None);
        // Another epoch.
        let d = dir();
        create_file(&d, &create_req("p1-1", "split-1", 5)).unwrap();
        refused(
            &d,
            &StampRequest {
                from_epoch: 1,
                to_epoch: 2,
                ..create_req("p1-1", "split-1", 6)
            },
        );
        // Rows in it.
        let db = pigeonhole::Pigeonhole::open(main_file(&d, "p1-1"), Default::default()).unwrap();
        let t = db
            .table("users")
            .unwrap()
            .family("f", pigeonhole::Family::default())
            .create_if_missing()
            .unwrap();
        t.mutate(b"d").put("f", b"q", b"v").commit().unwrap();
        db.close().unwrap();
        refused(&d, &create_req("p1-1", "split-1", 6));
        assert_eq!(mark_of(&d, "p1-1").map(|m| m.version), Some(5));
        // A stray WAL file.
        let d = dir();
        std::fs::write(d.join("p1-1.phdb-wal-0"), b"x").unwrap();
        refused(&d, &create_req("p1-1", "split-1", 5));
        assert!(!main_file(&d, "p1-1").exists());
        // A staged upload of it.
        let d = dir();
        std::fs::write(d.join(INCOMING).join("p1-1.phdb.part"), b"x").unwrap();
        refused(&d, &create_req("p1-1", "split-1", 5));
        assert!(!main_file(&d, "p1-1").exists());
    }

    /// An interrupted install whose already-renamed file doesn't match the manifest stops,
    /// rather than opening a mixed set.
    #[test]
    fn an_interrupted_install_with_a_mismatched_renamed_file_stops() {
        let (src, d) = (dir(), dir());
        let main = partition(&src, "p1", "c1", 1);
        stage(&d, "p1", &main, "c1", 1);
        let (header, _) = upload::ready_set(&d, "p1").unwrap();
        upload::write_header(&marker(&d, "p1"), &header).unwrap();
        // The main file "renamed in" already, but not the manifest's bytes.
        std::fs::remove_file(d.join(INCOMING).join(upload::ready_name("p1", ""))).unwrap();
        std::fs::write(main_file(&d, "p1"), b"something else").unwrap();
        let e = finish_install(&d, "p1").unwrap_err();
        assert!(e.message.contains("doesn't match"), "{}", e.message);
        assert!(
            marker(&d, "p1").exists(),
            "the marker stays, so nothing opens it"
        );
    }
}

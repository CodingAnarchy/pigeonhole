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
use crate::serve::proto::{PartitionFile, PartitionUploadHeader, StampRequest};

/// A fault seam (tests): the next install in this cluster directory fails after its first
/// rename, as a crash there would leave it. Keyed by the directory, so other tests'
/// installs run on.
#[cfg(test)]
pub(crate) static FAIL_AFTER_FIRST_RENAME: std::sync::Mutex<Option<PathBuf>> =
    std::sync::Mutex::new(None);

/// `.phdb-incoming/<P>.installing`.
fn marker(dir: &Path, partition: &str) -> PathBuf {
    dir.join(INCOMING).join(format!("{partition}.installing"))
}

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

/// Installs `header`'s set of partition `partition`, its marker written: the staged files
/// renamed in, the others moved aside, the marker removed, the leftovers deleted.
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
            std::fs::rename(&file, upload::path_with(&file, ".replaced"))
                .map_err(|e| ApiError::internal(format!("move {} aside: {e}", file.display())))?;
        }
    }
    #[cfg(test)]
    let mut renamed = 0;
    for f in &header.manifest {
        let staged = incoming.join(upload::ready_name(partition, &f.name));
        let to = target(dir, partition, &f.name);
        if staged.exists() {
            std::fs::rename(&staged, &to)
                .map_err(|e| ApiError::internal(format!("install {}: {e}", to.display())))?;
            #[cfg(test)]
            {
                renamed += 1;
                let mut armed = FAIL_AFTER_FIRST_RENAME
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if renamed == 1 && armed.as_deref() == Some(dir) {
                    *armed = None;
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
    std::fs::remove_file(marker(dir, partition))
        .map_err(|e| ApiError::internal(format!("remove the install's marker: {e}")))?;
    // The set's headers, and the partition's other staged files and headers (N2).
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
    Ok(header.manifest.clone())
}

impl Node {
    /// `Cluster.Stamp`: installs partition `req.partition`'s staged set, if one is complete
    /// (or finishes an interrupted install), then raises its file's epoch from `from_epoch`
    /// to `to_epoch`. Refused while the partition is open here. The set installed, if any.
    /// Run again once done, it answers success with nothing to do.
    pub(crate) async fn stamp(
        self: &Arc<Self>,
        req: StampRequest,
    ) -> Result<Vec<PartitionFile>, ApiError> {
        let _one = self.fences.changing.lock().await;
        let _open = self.opening.lock().await;
        let p = req.partition.clone();
        if !super::map::id_ok(&p) {
            return Err(ApiError::invalid(format!("{p:?} isn't a partition id")));
        }
        if req.to_epoch != req.from_epoch + 1 {
            return Err(ApiError::invalid(format!(
                "a stamp raises the epoch by one: from {} to {}, not {}",
                req.from_epoch,
                req.from_epoch + 1,
                req.to_epoch
            )));
        }
        if self.part_state(&p).is_some() {
            return Err(ApiError::failed_precondition(format!(
                "partition {p:?} is open here: fence and close it first (phdb cluster close)"
            )));
        }
        let dir = self.dir.clone();
        let installed = tokio::task::spawn_blocking(move || stamp_files(&dir, &req))
            .await
            .map_err(|e| ApiError::internal(e.to_string()))??;
        // Its files changed: a close's hashes of them go, and a map that gives it here
        // opens it.
        self.fences.closed().remove(&p);
        Ok(installed)
    }
}

/// [`Node::stamp`]'s work on the files, on a blocking thread.
fn stamp_files(dir: &Path, req: &StampRequest) -> Result<Vec<PartitionFile>, ApiError> {
    let p = &req.partition;
    let mut installed = finish_install(dir, p)?.unwrap_or_default();
    if installed.is_empty()
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
    // The file, opened alone and not served, for its identity row.
    let db = pigeonhole::Pigeonhole::open(&main, pigeonhole::Options::default())
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
        // Run again once done: nothing to do.
        if id.cluster_id == req.cluster_id && id.epoch == req.to_epoch {
            return Ok(());
        }
        let cluster_ok = if req.restore_from_cluster_id.is_empty() {
            id.cluster_id == req.cluster_id
        } else {
            id.cluster_id == req.restore_from_cluster_id
        };
        let epoch_ok = if req.restore {
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
        identity::write(
            &db,
            &identity::Identity {
                cluster_id: req.cluster_id.clone(),
                partition_id: p.clone(),
                epoch: req.to_epoch,
            },
        )
        .map_err(ApiError::internal)
    })();
    let closed = db.close();
    stamped?;
    closed.map_err(|e| ApiError::internal(format!("close {}: {e}", main.display())))?;
    Ok(installed)
}

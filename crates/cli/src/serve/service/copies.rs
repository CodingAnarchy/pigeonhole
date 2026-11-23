use super::*;

/// How often a download sends a heartbeat chunk while its copy is written (#1611): well
/// inside any client's idle limit.
const HEARTBEAT: Duration = Duration::from_secs(1);

/// A backup's partition named on a single node, which has none (D317 ticket 8b).
pub(super) fn single_node_partition() -> ApiError {
    ApiError::invalid("this server isn't a cluster node: a backup names no partition here")
}

/// A flush or compaction's partition named on a single node, which has none (#1852).
pub(super) fn single_node_maintenance() -> ApiError {
    ApiError::invalid(
        "this server isn't a cluster node: a flush or compaction names no partition here",
    )
}

/// A last chunk's identity fields (D317 ticket 8b), empty on a single node.
fn identity_fields(id: Option<&super::super::backup::CopyIdentity>) -> proto::BackupChunk {
    let Some(id) = id else {
        return proto::BackupChunk::default();
    };
    proto::BackupChunk {
        partition: id.partition.clone(),
        cluster_id: id.cluster_id.clone(),
        epoch: id.epoch,
        map_version: id.map_version,
        ..Default::default()
    }
}

/// A download's chunks on their way to its client: what the task of a backup's download and
/// of a closed partition's file sends through and ends with.
struct Chunks {
    tx: mpsc::Sender<Result<proto::BackupChunk, ApiError>>,
    /// `--stream-idle-ms`: the longest one send waits.
    idle: Duration,
    /// What a client that took no chunk for `idle` is told: how it resumes.
    stalled: &'static str,
}

impl Chunks {
    /// Sends `chunk` within the idle limit; `false` once the client went away (it resumes).
    async fn send(&self, chunk: proto::BackupChunk) -> Result<bool, ApiError> {
        match tokio::time::timeout(self.idle, self.tx.send(Ok(chunk))).await {
            Ok(Ok(())) => Ok(true),
            Ok(Err(_)) => Ok(false),
            Err(_) => Err(ApiError::deadline(self.stalled)),
        }
    }

    /// The download's end: counted with its `outcome`, and its error sent. Within the idle
    /// limit, as a chunk: a heartbeat may still sit in the channel, and a dropped error would
    /// end the stream as if it were whole (cli692's R1 on #1628).
    async fn end(&self, outcome: Result<(), ApiError>, metrics: &Metrics, started: Instant) {
        metrics.count(
            DOWNLOAD,
            outcome.as_ref().err().map(|e| e.code.as_str()),
            started.elapsed(),
        );
        if let Err(e) = outcome {
            let _ = tokio::time::timeout(self.idle, self.tx.send(Err(e))).await;
        }
    }
}

/// Sends `kept`'s bytes from `start` in [`backup::CHUNK`](super::super::backup::CHUNK) chunks
/// through `chunks`, the last carrying the total, the SHA-256 and the identity: a download's
/// copy, or a closed partition's file (D317 ticket 6). Stops early, `Ok`, once the client
/// went away.
async fn send_kept(
    kept: &super::super::backup::Kept,
    start: u64,
    id: &str,
    pool: &Arc<Semaphore>,
    chunks: &Chunks,
) -> Result<(), ApiError> {
    let mut at = start;
    loop {
        let path = kept.path.clone();
        let data = blocking(pool, move || super::super::backup::read_chunk(&path, at)).await?;
        // A file that isn't its listed size any more (a closed partition's file, which goes
        // as it lies, changed after its close) fails the download: short, an empty read
        // would never reach `total`; long, bytes past it would go out (#1729).
        let left = kept.total - at;
        if (data.is_empty() && left > 0) || data.len() as u64 > left {
            return Err(ApiError::failed_precondition(format!(
                "the file changed after it was taken: it isn't its {} bytes any more (a \
                 closed partition's file: close it again)",
                kept.total
            )));
        }
        let next = at + data.len() as u64;
        let last = next >= kept.total;
        let chunk = proto::BackupChunk {
            data: data.into(),
            total: if last { kept.total } else { 0 },
            backup_id: id.to_owned(),
            offset: at,
            sha256: if last {
                kept.sha256.to_vec().into()
            } else {
                Default::default()
            },
            feed_token: kept.feed_token.clone().into(),
            ..identity_fields(last.then_some(kept.identity.as_ref()).flatten())
        };
        if !chunks.send(chunk).await? || last {
            return Ok(());
        }
        at = next;
    }
}

/// A download's offset past its copy's end.
pub(crate) fn past_the_copy(offset: u64, total: u64) -> ApiError {
    ApiError::invalid(format!(
        "offset {offset} is past the backup's {total} bytes"
    ))
}

impl Service {
    /// A cluster partition's replicas follow its feed (D328's S1), which is on only with
    /// `--change-feed` on its owner's node: refused, naming that, for their `Follow` and
    /// their bootstrap, which would otherwise fail on the engine's own error. `partition` is
    /// the cluster partition's id; `None` on a single node, whose replicas need no check.
    fn feed_on_for_replicas(partition: Option<&str>, db: &Pigeonhole) -> Result<(), ApiError> {
        if let (Some(partition), Ok(info)) = (partition, db.info())
            && info.feed.is_none()
        {
            return Err(ApiError::failed_precondition(format!(
                "partition {partition}'s change feed is off on this node, so its replicas have \
                 nothing to follow: start the node with --change-feed"
            )));
        }
        Ok(())
    }

    /// `Replication.Follow` (#1436): under a `--max-streams` slot, on a thread of its own.
    pub(super) fn follow(
        &self,
        r: proto::FollowRequest,
    ) -> Result<super::super::replica::FollowBatches, ApiError> {
        if self.replica_info().is_some() {
            // No cascading replicas in 1.2.0 (simp724's P2 on #1436).
            return Err(ApiError::failed_precondition(
                "a replica can't be followed: follow its primary",
            ));
        }
        let part = self.part()?;
        Self::feed_on_for_replicas(self.gate.as_ref().map(|g| g.partition()), &part.db)?;
        let slot = self.stream_slot()?;
        let batches = super::super::replica::follow(
            &part.db,
            r.token.to_vec(),
            slot,
            self.limits.stream_idle,
            Arc::clone(&self.metrics),
            self.placement.clone(),
        )?;
        // A cluster partition's, under its lease (D328's M1).
        match &self.gate {
            Some(gate) => batches.leased(Arc::clone(gate)),
            None => Ok(batches),
        }
    }

    /// `Admin.Backup` of `part`: on a thread of its own, one backup at a time (perf287's D1
    /// on #1527).
    pub(crate) async fn backup(
        &self,
        part: &Partition,
        r: proto::BackupRequest,
    ) -> Result<proto::BackupResponse, ApiError> {
        let (db, backups) = (part.db.clone(), Arc::clone(&part.backups));
        let bytes = part
            .backups
            .on_backup_thread(move || {
                let path = backups.prepare_target(&r.path)?;
                if r.durable {
                    db.backup_durable(&path)?;
                } else {
                    db.backup(&path)?;
                }
                let len = std::fs::metadata(&path).map_or(0, |m| m.len());
                Ok(len)
            })
            .await?;
        Ok(proto::BackupResponse { bytes })
    }

    /// `Admin.BackupDownload`: a fresh backup (hashed, then kept for resumes), or a kept one
    /// from an offset, streamed in chunks of up to 1 MiB on its own task. It holds a
    /// `--max-streams` slot until it ends, and each send waits at most `--stream-idle-ms`.
    ///
    /// It answers at once (#1611): while the copy is written, the task sends a heartbeat (a
    /// chunk with no data, at the requested offset, with the id and, for a replica, the feed
    /// token; `total` 0 and no `sha256`, so a client's last-chunk check never fires on one)
    /// at once and then every `HEARTBEAT`, so a client bounds every wait by its idle
    /// limit and resumes a break during the copy with the id. HTTP waits for the copy
    /// instead: it sends the size and hash as headers.
    ///
    /// `map_version` is a cluster node's (D317 ticket 8b): the copy is of one snapshot, whose
    /// identity row it reads and sends in the last chunk, with the map version.
    pub(crate) async fn backup_download(
        &self,
        part: &Partition,
        r: proto::BackupDownloadRequest,
        map_version: Option<u64>,
    ) -> Result<super::super::backup::Download, ApiError> {
        use super::super::backup::{Copy, CopyIdentity, Taken};
        let slot = self.stream_slot()?;
        if r.replica && self.replica_info().is_some() {
            // No cascading replicas in 1.2.0 (simp724's P2 on #1436): this copy would carry
            // this server's own replication state.
            return Err(ApiError::failed_precondition(
                "a replica can't be a bootstrap source: bootstrap from its primary",
            ));
        }
        // A cluster node's download runs on the node's service, with the partition named.
        if r.replica && map_version.is_some() {
            Self::feed_on_for_replicas(Some(&r.partition), &part.db)?;
        }
        let (id, copy) = if r.backup_id.is_empty() {
            let (db, durable, replica) = (part.db.clone(), r.durable, r.replica);
            // The backup and its hash run on a thread of their own, never the blocking pool
            // (perf287's D1), and the copy is the table's from the start (E1).
            part.backups.fresh_download(move |p, started| {
                if let Some(map_version) = map_version {
                    // A cluster node's copy: one snapshot for the copy and the identity it
                    // names, so the last chunk says exactly what the file holds.
                    // A replica copy's bootstrap (D328): durable, with the token of a feed
                    // of every table from the same snapshot, as a single node's.
                    let snap = if durable || replica {
                        db.durable_snapshot()?
                    } else {
                        db.snapshot()?
                    };
                    let id = super::super::cluster::identity::read_at(&db, &snap)
                        .map_err(ApiError::internal)?
                        .ok_or_else(|| {
                            ApiError::internal("the partition's file has no identity row")
                        })?;
                    let feed_token = if replica {
                        let token = db.feed().from_snapshot(&snap).open()?.token();
                        let token = token.as_bytes().to_vec();
                        started(token.clone());
                        token
                    } else {
                        Vec::new()
                    };
                    db.backup_at(&snap, p)?;
                    return Ok(Taken {
                        feed_token,
                        identity: Some(CopyIdentity {
                            partition: id.partition_id,
                            cluster_id: id.cluster_id,
                            epoch: id.epoch,
                            map_version,
                        }),
                    });
                }
                if replica {
                    // A replica's bootstrap (#1436): a durable snapshot's copy, and the
                    // token of a feed of every table from that same snapshot, sent with the
                    // heartbeats once it's known.
                    let snap = db.durable_snapshot()?;
                    let token = db.feed().from_snapshot(&snap).open()?.token();
                    let token = token.as_bytes().to_vec();
                    started(token.clone());
                    db.backup_at(&snap, p)?;
                    return Ok(Taken {
                        feed_token: token,
                        identity: None,
                    });
                }
                if durable {
                    db.backup_durable(p)?;
                } else {
                    db.backup(p)?;
                }
                Ok(Taken::default())
            })?
        } else {
            (r.backup_id.clone(), part.backups.open(&r.backup_id)?)
        };
        // A kept copy: an offset past it is refused now. One still being written is checked
        // once it's ready.
        if let Copy::Ready(kept) = &*copy.borrow()
            && r.offset > kept.total
        {
            part.backups.release(&id);
            return Err(past_the_copy(r.offset, kept.total));
        }
        let (tx, rx) = mpsc::channel(1);
        let chunks = Chunks {
            tx,
            idle: self.limits.stream_idle,
            stalled: "the client took no backup chunk for --stream-idle-ms; resume with \
                      backup_id and the offset it reached",
        };
        let (backups, pool) = (Arc::clone(&part.backups), Arc::clone(&self.blocking));
        let metrics = Arc::clone(&self.metrics);
        let started = Instant::now();
        let (task_id, start, mut task_copy) = (id.clone(), r.offset, copy.clone());
        let want = r.partition;
        tokio::spawn(async move {
            let outcome: Result<(), ApiError> = async {
                // The copy, with heartbeats while it's written (#1611).
                let kept = loop {
                    let state = task_copy.borrow_and_update().clone();
                    let feed_token = match state {
                        Copy::Ready(kept) => break kept,
                        Copy::Failed(e) => return Err(e),
                        Copy::Writing { feed_token } => feed_token,
                    };
                    let beat = proto::BackupChunk {
                        data: Default::default(),
                        total: 0,
                        backup_id: task_id.clone(),
                        offset: start,
                        sha256: Default::default(),
                        feed_token: feed_token.into(),
                        ..Default::default()
                    };
                    if !chunks.send(beat).await? {
                        return Ok(());
                    }
                    tokio::select! {
                        changed = task_copy.changed() => {
                            // The table dropped the copy without a last state: never expected
                            // (it says why it failed first).
                            if changed.is_err()
                                && matches!(*task_copy.borrow(), Copy::Writing { .. })
                            {
                                return Err(ApiError::internal("the backup's copy vanished"));
                            }
                        }
                        () = tokio::time::sleep(HEARTBEAT) => {}
                    }
                };
                if start > kept.total {
                    return Err(past_the_copy(start, kept.total));
                }
                // A resume names the partition its copy is of (D317 ticket 8b).
                let of = kept.identity.as_ref().map_or("", |i| i.partition.as_str());
                if of != want {
                    return Err(ApiError::invalid(format!(
                        "backup {task_id} is of partition {of:?}, not {want:?}"
                    )));
                }
                send_kept(&kept, start, &task_id, &pool, &chunks).await
            }
            .await;
            chunks.end(outcome, &metrics, started).await;
            backups.release(&task_id);
            drop(slot);
        });
        Ok(super::super::backup::Download {
            id,
            offset: r.offset,
            copy,
            chunks: rx,
        })
    }

    /// `Admin.BackupDownload` of a closed partition's file (D317 ticket 6, `fenced_file`):
    /// `file`'s bytes at `path` as they lie, from `r.offset`, with no backup taken. The
    /// partition is closed, so the file is final, and `file` carries its size and SHA-256 as
    /// hashed at its close. Holds a `--max-streams` slot, as a download does.
    pub(crate) async fn fenced_download(
        &self,
        path: std::path::PathBuf,
        file: proto::PartitionFile,
        r: proto::BackupDownloadRequest,
    ) -> Result<super::super::backup::Download, ApiError> {
        use super::super::backup::{Copy, Kept};
        if r.durable || r.replica || !r.backup_id.is_empty() {
            return Err(ApiError::invalid(
                "a fenced file is sent as it lies: durable, replica and backup_id don't apply",
            ));
        }
        let sha256: [u8; 32] = file.sha256.as_ref().try_into().map_err(|_| {
            ApiError::internal(format!("fenced file {:?} has no SHA-256", file.name))
        })?;
        let kept = Kept {
            path,
            total: file.bytes,
            sha256,
            feed_token: Vec::new(),
            identity: None,
        };
        if r.offset > kept.total {
            return Err(past_the_copy(r.offset, kept.total));
        }
        let slot = self.stream_slot()?;
        let (tx, rx) = mpsc::channel(1);
        let chunks = Chunks {
            tx,
            idle: self.limits.stream_idle,
            stalled: "the client took no chunk for --stream-idle-ms; resume from the offset \
                      it reached",
        };
        let pool = Arc::clone(&self.blocking);
        let metrics = Arc::clone(&self.metrics);
        let started = Instant::now();
        let (start, task_kept) = (r.offset, kept.clone());
        tokio::spawn(async move {
            let outcome = send_kept(&task_kept, start, "", &pool, &chunks).await;
            chunks.end(outcome, &metrics, started).await;
            drop(slot);
        });
        Ok(super::super::backup::Download {
            id: String::new(),
            offset: r.offset,
            copy: watch::channel(Copy::Ready(kept)).1,
            chunks: rx,
        })
    }
}

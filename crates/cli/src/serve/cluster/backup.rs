//! `phdb cluster backup` (D317 ticket 8b, ICR 0117): every partition's backup, each taken
//! through the node the map gives it, plus the map, in one directory.
//!
//! - **Through each owner, never forwarded.** It reads the map from `--node` (version V),
//!   then asks each partition's owner for `Admin.BackupDownload { partition }`. A node
//!   that doesn't hold the partition answers `WrongPartition`.
//! - **Checked against the map.** Each copy's last chunk names the identity the copy holds
//!   (cluster, partition, epoch), read from the snapshot it is of: it must be the map's at
//!   V. At the end the map is read again (V′): its partitions, their owners and epochs, and
//!   the placement of every table both versions hold, must equal V's (ai1249's (c)), or the
//!   backup fails ("the map changed during the backup"). A table created or dropped in
//!   between is allowed, and `map.json` is V′, so a restore reconciles the files against
//!   the map they were taken under (simp724's A2; ai1249's rules for ticket 5).
//! - **Not one point in time.** Each partition's copy is its own snapshot; the manifest says
//!   so (`consistency`).
//! - **Complete only with its manifest.** Copies are written as `<partition>.phdb.partial`
//!   and renamed once checked; `manifest.json` is written last.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use super::cli::{NodeArgs, channel, report, request};
use super::map;
use crate::exit::Failure;
use crate::output::Report;
use crate::serve::proto::{self, BackupDownloadRequest, ClusterMap, GetMapRequest};

/// A copy's stream may break this many times; each break resumes from what was written.
pub(super) const RESUMES: u32 = 5;

/// What the backup of one partition gave.
#[derive(Debug, Clone)]
struct Copied {
    partition: String,
    node: String,
    epoch: u64,
    bytes: u64,
    sha256: String,
    /// The owner's map version when its copy began.
    map_version: u64,
    seconds: f64,
}

/// `phdb cluster backup DIR`.
pub(crate) async fn backup(
    args: &NodeArgs,
    dir: &Path,
    durable: bool,
    parallel: usize,
) -> Result<Report, Failure> {
    backup_then(args, dir, durable, parallel, async || {}).await
}

/// [`backup`], running `copied` once every copy is written, before the map is read again:
/// a test's hold seam, to change the map while a backup runs.
pub(super) async fn backup_then(
    args: &NodeArgs,
    dir: &Path,
    durable: bool,
    parallel: usize,
    copied_all: impl AsyncFnOnce(),
) -> Result<Report, Failure> {
    let (channel_v, token) = channel(args, &args.node).await?;
    let token = token.map(Arc::<str>::from);
    let start = get_map(channel_v.clone(), token.as_deref(), &args.node).await?;
    prepare_dir(dir)?;
    let scheme = if args.node.starts_with("https://") {
        "https"
    } else {
        "http"
    };
    let addresses: BTreeMap<&str, &str> = start
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n.address.as_str()))
        .collect();
    let mut jobs = Vec::new();
    for p in &start.partitions {
        let address = addresses.get(p.node.as_str()).ok_or_else(|| {
            Failure::usage(&format!(
                "the map gives partition {} to node {}, which it doesn't list",
                p.id, p.node
            ))
        })?;
        jobs.push(Job {
            url: format!("{scheme}://{address}"),
            partition: p.id.clone(),
            node: p.node.clone(),
            epoch: p.epoch,
            cluster_id: start.cluster_id.clone(),
        });
    }
    let mut copied = Vec::with_capacity(jobs.len());
    let mut set = tokio::task::JoinSet::new();
    let mut pending = jobs.into_iter();
    let mut failed: Option<Failure> = None;
    loop {
        while failed.is_none() && set.len() < parallel.max(1) {
            let Some(job) = pending.next() else { break };
            let (args, token, dir, durable) =
                (args.clone(), token.clone(), dir.to_path_buf(), durable);
            set.spawn(async move { job.run(&args, token.as_deref(), &dir, durable).await });
        }
        let Some(done) = set.join_next().await else {
            break;
        };
        match done {
            Ok(Ok(c)) => copied.push(c),
            Ok(Err(e)) => {
                failed.get_or_insert(e);
            }
            Err(e) => {
                failed.get_or_insert(Failure::usage(&format!("a partition's backup: {e}")));
            }
        }
    }
    if let Some(e) = failed {
        return Err(e);
    }
    copied_all().await;
    let end = get_map(channel_v, token.as_deref(), &args.node).await?;
    if let Some(why) = moved(&start, &end) {
        return Err(Failure::usage(&format!(
            "the map changed during the backup ({why}): run it again"
        )));
    }
    copied.sort_by(|a, b| a.partition.cmp(&b.partition));
    write_new(&dir.join("map.json"), map::canonical_json(&end).as_bytes())?;
    let manifest = json!({
        "cluster_id": end.cluster_id,
        "map_version": end.map_version,
        "map_version_at_start": start.map_version,
        "durable": durable,
        "consistency": "each partition's copy is a consistent snapshot of its own; the set \
                        is not one point in time across partitions",
        "partitions": copied.iter().map(|c| json!({
            "partition": c.partition,
            "file": format!("{}.phdb", c.partition),
            "node": c.node,
            "epoch": c.epoch,
            "bytes": c.bytes,
            "sha256": c.sha256,
            "node_map_version": c.map_version,
            "seconds": c.seconds,
        })).collect::<Vec<_>>(),
    });
    write_new(
        &dir.join("manifest.json"),
        serde_json::to_string_pretty(&manifest)
            .expect("the manifest serializes")
            .as_bytes(),
    )?;
    let bytes: u64 = copied.iter().map(|c| c.bytes).sum();
    let summary = vec![
        ("dir", json!(dir.display().to_string())),
        ("cluster_id", json!(end.cluster_id)),
        ("map_version", json!(end.map_version)),
        ("partitions", json!(copied.len())),
        ("bytes", json!(bytes)),
    ];
    let mut r = report("cluster backup", summary, Value::Null);
    r.columns = ["partition", "node", "epoch", "bytes", "sha256"]
        .into_iter()
        .map(super::cli::column)
        .collect();
    r.rows = copied
        .iter()
        .map(|c| {
            vec![
                json!(c.partition),
                json!(c.node),
                json!(c.epoch),
                json!(c.bytes),
                json!(c.sha256),
            ]
        })
        .collect();
    r.document.insert("manifest".into(), manifest);
    Ok(r)
}

/// One partition's backup.
struct Job {
    url: String,
    partition: String,
    node: String,
    epoch: u64,
    cluster_id: String,
}

impl Job {
    async fn run(
        self,
        args: &NodeArgs,
        token: Option<&str>,
        dir: &Path,
        durable: bool,
    ) -> Result<Copied, Failure> {
        let started = std::time::Instant::now();
        let partial = dir.join(format!("{}.phdb.partial", self.partition));
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&partial)
            .await
            .map_err(|e| Failure::io(&format!("create {}", partial.display()), &e))?;
        let what = |m: &str| format!("partition {} from node {}: {m}", self.partition, self.node);
        let (channel, _) = channel(args, &self.url).await?;
        let mut admin =
            proto::admin_client::AdminClient::new(channel).max_decoding_message_size(4 << 20);
        let (mut backup_id, mut offset) = (String::new(), 0u64);
        let mut breaks = 0;
        let last = loop {
            let req = request(
                BackupDownloadRequest {
                    fenced_file: None,
                    durable,
                    backup_id: backup_id.clone(),
                    offset,
                    replica: false,
                    partition: self.partition.clone(),
                },
                token,
            )?;
            let got = async {
                let mut stream = admin.backup_download(req).await?.into_inner();
                while let Some(chunk) = stream.message().await? {
                    if backup_id.is_empty() {
                        backup_id.clone_from(&chunk.backup_id);
                    }
                    if !chunk.data.is_empty() {
                        if chunk.offset != offset {
                            return Err(tonic::Status::data_loss(format!(
                                "a chunk at {} where {offset} was expected",
                                chunk.offset
                            )));
                        }
                        file.seek(std::io::SeekFrom::Start(offset))
                            .await
                            .map_err(|e| tonic::Status::internal(e.to_string()))?;
                        file.write_all(&chunk.data)
                            .await
                            .map_err(|e| tonic::Status::internal(e.to_string()))?;
                        offset += chunk.data.len() as u64;
                    }
                    if chunk.total != 0 {
                        return Ok(Some(chunk));
                    }
                }
                Ok(None)
            }
            .await;
            match got {
                Ok(Some(chunk)) => break chunk,
                // A refusal is final; a break resumes from what was written.
                Err(s) if !resumable(&s) => return Err(Failure::usage(&what(s.message()))),
                Ok(None) | Err(_) if breaks < RESUMES && !backup_id.is_empty() => {
                    breaks += 1;
                    tokio::time::sleep(Duration::from_millis(200 * u64::from(breaks))).await;
                }
                Ok(None) => return Err(Failure::usage(&what("the stream ended early"))),
                Err(s) => return Err(Failure::usage(&what(s.message()))),
            }
        };
        file.sync_all()
            .await
            .map_err(|e| Failure::io(&format!("sync {}", partial.display()), &e))?;
        drop(file);
        if offset != last.total {
            return Err(Failure::usage(&what(&format!(
                "{offset} bytes written of {}",
                last.total
            ))));
        }
        let sha = digest(&partial)?;
        if sha[..] != last.sha256[..] {
            return Err(Failure::usage(&what("the copy's SHA-256 doesn't match")));
        }
        // The copy names what it holds: it must be the map's partition at the map's epoch.
        if last.cluster_id != self.cluster_id
            || last.partition != self.partition
            || last.epoch != self.epoch
        {
            return Err(Failure::usage(&what(&format!(
                "the copy is partition {:?} of cluster {:?} at epoch {}, but the map says \
                 partition {:?} of cluster {:?} at epoch {}: the map changed during the \
                 backup, or the node holds another file; run it again",
                last.partition,
                last.cluster_id,
                last.epoch,
                self.partition,
                self.cluster_id,
                self.epoch
            ))));
        }
        let path = dir.join(format!("{}.phdb", self.partition));
        std::fs::rename(&partial, &path)
            .map_err(|e| Failure::io(&format!("rename {}", partial.display()), &e))?;
        Ok(Copied {
            partition: self.partition,
            node: self.node,
            epoch: self.epoch,
            bytes: offset,
            sha256: hex(&sha),
            map_version: last.map_version,
            seconds: started.elapsed().as_secs_f64(),
        })
    }
}

/// Whether a download's error is a break to resume from, not a refusal.
pub(super) fn resumable(s: &tonic::Status) -> bool {
    matches!(
        s.code(),
        tonic::Code::Unavailable
            | tonic::Code::Unknown
            | tonic::Code::Internal
            | tonic::Code::Cancelled
            | tonic::Code::DeadlineExceeded
    )
}

async fn get_map(
    channel: tonic::transport::Channel,
    token: Option<&str>,
    node: &str,
) -> Result<ClusterMap, Failure> {
    proto::cluster_client::ClusterClient::new(channel)
        .get_map(request(GetMapRequest {}, token)?)
        .await
        .map(tonic::Response::into_inner)
        .map_err(|s| Failure::usage(&format!("{node}: {}", s.message())))
}

/// `dir` made, or found empty: a backup never mixes with another's files.
fn prepare_dir(dir: &Path) -> Result<(), Failure> {
    match std::fs::read_dir(dir) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(Failure::usage(&format!(
                    "{} isn't empty: a cluster backup goes in a new or empty directory",
                    dir.display()
                )));
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => std::fs::create_dir_all(dir)
            .map_err(|e| Failure::io(&format!("create {}", dir.display()), &e)),
        Err(e) => Err(Failure::io(&format!("read {}", dir.display()), &e)),
    }
}

fn write_new(path: &PathBuf, bytes: &[u8]) -> Result<(), Failure> {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|e| Failure::io(&format!("create {}", path.display()), &e))?;
    f.write_all(bytes)
        .and_then(|()| f.sync_all())
        .map_err(|e| Failure::io(&format!("write {}", path.display()), &e))
}

pub(super) fn digest(path: &Path) -> Result<[u8; 32], Failure> {
    std::fs::File::open(path)
        .and_then(|mut f| crate::serve::backup::sha256_of(&mut f))
        .map(|(_, sha)| sha)
        .map_err(|e| Failure::io(&format!("read {}", path.display()), &e))
}

pub(super) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Why the map at the end (`end`) can't stand for the copies taken under `start`, if it
/// can't: a partition added, removed, moved or re-epoched, or a table both hold placed
/// differently (ai1249's (c)), or dropped and created again under its name (its `created`
/// differs, simp724's N2 on #1711): some copies may hold the old table and others the new.
/// Tables created or dropped in between are fine: `map.json` is `end`, so a restore's
/// reconcile treats them by ticket 5's rules.
pub(crate) fn moved(start: &ClusterMap, end: &ClusterMap) -> Option<String> {
    if start.cluster_id != end.cluster_id {
        return Some(format!(
            "cluster {:?} became {:?}",
            start.cluster_id, end.cluster_id
        ));
    }
    let parts = |m: &ClusterMap| -> BTreeMap<String, (String, u64)> {
        m.partitions
            .iter()
            .map(|p| (p.id.clone(), (p.node.clone(), p.epoch)))
            .collect()
    };
    let (a, b) = (parts(start), parts(end));
    for (id, (node, epoch)) in &a {
        match b.get(id) {
            None => return Some(format!("partition {id} was removed")),
            Some((n, e)) if n != node || e != epoch => {
                return Some(format!(
                    "partition {id} moved from node {node} at epoch {epoch} to node {n} at \
                     epoch {e}"
                ));
            }
            Some(_) => {}
        }
    }
    if let Some(id) = b.keys().find(|id| !a.contains_key(*id)) {
        return Some(format!("partition {id} was added"));
    }
    for t in &start.tables {
        let Some(u) = end.tables.iter().find(|u| u.name == t.name) else {
            continue;
        };
        if u.created != t.created {
            return Some(format!("table {} was dropped and created again", t.name));
        }
        if u.splits != t.splits || u.partitions != t.partitions || u.group != t.group {
            return Some(format!("table {} was placed differently", t.name));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::proto::{Node, Partition, TablePlacement};

    fn map(version: u64, epoch: u64, splits: &[&str]) -> ClusterMap {
        ClusterMap {
            cluster_id: "c1".into(),
            map_version: version,
            nodes: vec![Node {
                id: "n1".into(),
                address: "a:1".into(),
            }],
            partitions: vec![
                Partition {
                    history_epoch: 0,
                    id: "p1".into(),
                    node: "n1".into(),
                    epoch,
                    memory_bytes: 0,
                },
                Partition {
                    history_epoch: 0,
                    id: "p2".into(),
                    node: "n1".into(),
                    epoch: 1,
                    memory_bytes: 0,
                },
            ],
            tables: vec![TablePlacement {
                name: "t".into(),
                splits: splits
                    .iter()
                    .map(|s| proto::SplitKey {
                        key: Some(proto::split_key::Key::Text((*s).into())),
                    })
                    .collect(),
                partitions: (0..=splits.len()).map(|i| format!("p{}", i + 1)).collect(),
                ..Default::default()
            }],
        }
    }

    #[test]
    fn the_end_map_stands_unless_a_partition_or_a_placement_moved() {
        let a = map(5, 1, &["m"]);
        assert_eq!(
            moved(&a, &map(9, 1, &["m"])),
            None,
            "only the version moved"
        );
        let mut created = map(9, 1, &["m"]);
        created.tables.push(TablePlacement {
            name: "new".into(),
            partitions: vec!["p1".into()],
            ..Default::default()
        });
        assert_eq!(moved(&a, &created), None, "a table created meanwhile");
        let mut dropped = map(9, 1, &["m"]);
        dropped.tables.clear();
        assert_eq!(moved(&a, &dropped), None, "a table dropped meanwhile");
        assert!(moved(&a, &map(9, 2, &["m"])).is_some_and(|w| w.contains("epoch")));
        let mut added = map(9, 1, &["m"]);
        added.partitions.push(Partition {
            history_epoch: 0,
            id: "p3".into(),
            node: "n1".into(),
            epoch: 1,
            memory_bytes: 0,
        });
        assert!(moved(&a, &added).is_some_and(|w| w.contains("added")));
        let mut resplit = map(9, 1, &["m"]);
        resplit.tables[0].splits[0].key = Some(proto::split_key::Key::Text("n".into()));
        assert!(moved(&a, &resplit).is_some_and(|w| w.contains("placed differently")));
        // Dropped and created again under its name, placed the same: a new `created`.
        let mut recreated = map(9, 1, &["m"]);
        recreated.tables[0].created = 7;
        assert!(
            moved(&a, &recreated).is_some_and(|w| w.contains("dropped and created again")),
            "{:?}",
            moved(&a, &recreated)
        );
    }
}

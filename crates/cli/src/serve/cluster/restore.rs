//! `phdb cluster restore DIR` (D317 ticket 8b, §9, ICR 0117): a `phdb cluster backup`
//! put back, each partition's copy uploaded to the node the backup's map gives it, stamped
//! there at a new epoch, and the backup's map installed.
//!
//! - **Checked first.** Every copy's size and SHA-256 against `manifest.json`, the map's
//!   partitions against the manifest's, and every node: no restored partition may be
//!   serving anywhere (fence it, and on its new node close it too), the node each one goes
//!   to must answer, and so must the node the live map gives it, unless `--old-unreachable`.
//! - **Uploaded, then stamped, then mapped.** Each copy goes to its node through
//!   `Admin.PartitionUpload` (resumed after a break). Each node then installs it with
//!   `Cluster.Stamp { restore }` at epoch `e + 1`, where `e` is the highest of the backup's,
//!   the backup map's and the live map's epoch for the partition, so a node still holding
//!   the old file can't write into the restored cluster. Last, the backup's map (or
//!   `--map`'s) is set by amendment 1's forced path, with those epochs: above every
//!   reachable node's version, never checked as a change of the live map, and with every
//!   table's placement and `created` exactly as the backup has them (ai1249's (a) and (b)
//!   on the 8b plan): another `created` would make each node drop the table and make it
//!   again, empty.
//! - **Then served.** Each partition opens on its node with the map, under the fence the
//!   operator set before (persisted), which the restore then lifts.
//! - **History starts over.** A restored partition is its backup's main file alone: its
//!   WAL and feed files are moved aside (`.replaced-<time>`), so feed tokens from before
//!   the restore answer `FeedExpired`.
//! - **Run again** after a failure (simp724's S1 on #1764): `DIR/restore-progress.json`
//!   records each partition stamped (its node and new epoch, under the live map's version)
//!   and the map once set. A run again skips a partition recorded and found at that epoch
//!   on its node (no upload, no second install), and once the map is recorded and the live
//!   map still places every partition so, it only unfences them. An upload not finished
//!   resumes. The file goes once the restore is done, so a later restore from the same
//!   backup starts afresh.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use prost::bytes::Bytes;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::backup::{RESUMES, digest, hex, resumable};
use super::cli::{
    CopyCheck, NodeArgs, Restoring, channel, column, relocate_as, report, request, set_map, url_of,
};
use super::map;
use super::transfer::{Feed, Stopped, upload_file};
use crate::exit::Failure;
use crate::output::Report;
use crate::serve::download::STALL;
use crate::serve::proto::{
    self, ClusterMap, GetMapRequest, LocalPartitionState, PartitionFile, PartitionUploadHeader,
    PartitionUploadPurpose, StampRequest, StatusRequest,
};

/// What `phdb cluster restore` was asked.
#[derive(Debug, Clone)]
pub(crate) struct RestoreArgs {
    /// The backup's directory.
    pub dir: PathBuf,
    /// A map to install in place of the backup's `map.json` (nodes at new addresses).
    pub map: Option<PathBuf>,
    /// The restored cluster's id, when it isn't the backup's.
    pub cluster_id: Option<String>,
    /// The node the live map gives a partition to may be down.
    pub old_unreachable: bool,
    /// Uploads at once.
    pub parallel: usize,
    /// Restore only this partition into the live cluster (#1832).
    pub partition: Option<String>,
    /// With `partition`: the node it goes to, if not the live map's.
    pub to: Option<String>,
}

/// How long a restored partition may take to open on its node once the map is set.
const OPEN_WAIT: Duration = Duration::from_secs(60);

/// The restore's progress, in the backup's directory.
const PROGRESS: &str = "restore-progress.json";

/// A test's fault seam: the next restore from each of these directories fails at its step
/// (`"stamped"`: after every stamp, before the map; `"mapped"`: after the map, before the
/// unfences), as a crash there would leave it. A list, keyed by the directory, so tests
/// running at once don't take or overwrite each other's (#2015).
#[cfg(test)]
pub(crate) static FAIL_AT: std::sync::Mutex<Vec<(PathBuf, &'static str)>> =
    std::sync::Mutex::new(Vec::new());

fn fault(dir: &Path, step: &'static str) -> Result<(), Failure> {
    #[cfg(test)]
    {
        let mut armed = FAIL_AT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(i) = armed.iter().position(|(d, s)| d == dir && *s == step) {
            armed.swap_remove(i);
            return Err(Failure::usage(&format!("the test's fault after {step}")));
        }
    }
    let _ = (dir, step);
    Ok(())
}

/// What a restore from a directory has done so far: under the live map at
/// `live_map_version` (0 for none), each partition stamped (its node and new epoch), and
/// the map's version once set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Progress {
    cluster_id: String,
    live_map_version: u64,
    stamped: BTreeMap<String, Stamped>,
    map_version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Stamped {
    node: String,
    epoch: u64,
}

fn read_progress(dir: &Path) -> Option<Progress> {
    let v: Value = serde_json::from_str(&std::fs::read_to_string(dir.join(PROGRESS)).ok()?).ok()?;
    let mut stamped = BTreeMap::new();
    for (p, st) in v["stamped"].as_object()? {
        stamped.insert(
            p.clone(),
            Stamped {
                node: st["node"].as_str()?.to_owned(),
                epoch: st["epoch"].as_u64()?,
            },
        );
    }
    Some(Progress {
        cluster_id: v["cluster_id"].as_str()?.to_owned(),
        live_map_version: v["live_map_version"].as_u64()?,
        stamped,
        map_version: v["map_version"].as_u64(),
    })
}

/// Records `p` in `dir`, unless an earlier write failed (`unwritable`). A backup that can't
/// be written to (a read-only snapshot) still restores: the first failure is a warning, and
/// a run again after a failure then redoes the whole restore (simp724's S3 on #1764).
fn record(dir: &Path, p: &Progress, unwritable: &mut Option<String>) {
    if unwritable.is_some() {
        return;
    }
    if let Err(e) = write_progress(dir, p) {
        let why = format!(
            "{}: the restore's progress isn't recorded, so a run again after a failure redoes \
             the whole restore",
            e.message()
        );
        eprintln!("phdb cluster restore: warning: {why}");
        *unwritable = Some(why);
    }
}

/// Written whole and renamed in, so a crash leaves the last one.
fn write_progress(dir: &Path, p: &Progress) -> Result<(), Failure> {
    let doc = json!({
        "cluster_id": p.cluster_id,
        "live_map_version": p.live_map_version,
        "stamped": p.stamped.iter().map(|(id, st)| {
            (id.clone(), json!({"node": st.node, "epoch": st.epoch}))
        }).collect::<serde_json::Map<String, Value>>(),
        "map_version": p.map_version,
    });
    let path = dir.join(PROGRESS);
    let tmp = dir.join(format!("{PROGRESS}.tmp"));
    std::fs::write(
        &tmp,
        serde_json::to_vec_pretty(&doc).expect("progress serializes"),
    )
    .and_then(|()| std::fs::rename(&tmp, &path))
    .map_err(|e| Failure::io(&format!("write {}", path.display()), &e))
}

/// One partition's copy in the backup.
#[derive(Debug, Clone)]
pub(super) struct Copy {
    pub(super) partition: String,
    pub(super) path: PathBuf,
    /// The epoch the copy is of.
    pub(super) epoch: u64,
    pub(super) bytes: u64,
    pub(super) sha256: [u8; 32],
}

/// One partition's restore: its copy, the node it goes to and its new epoch.
#[derive(Debug, Clone)]
pub(super) struct Job {
    pub(super) copy: Copy,
    pub(super) node: String,
    pub(super) url: String,
    pub(super) epoch: u64,
    /// Stamped by an earlier run: no upload, no stamp.
    pub(super) earlier: bool,
}

/// `phdb cluster restore DIR`.
pub(crate) async fn restore(args: &NodeArgs, r: &RestoreArgs) -> Result<Report, Failure> {
    if let Some(p) = &r.partition {
        return restore_partition(args, r, p).await;
    }
    if r.to.is_some() {
        return Err(Failure::usage("--to goes with --partition"));
    }
    let (backup_cluster, copies) = read_manifest(&r.dir)?;
    let map_path = r.map.clone().unwrap_or_else(|| r.dir.join("map.json"));
    let mut m = map::parse_json(
        &std::fs::read_to_string(&map_path)
            .map_err(|e| Failure::io(&format!("read {}", map_path.display()), &e))?,
    )
    .map_err(|e| Failure::usage(&format!("{}: {e}", map_path.display())))?;
    if m.cluster_id != backup_cluster {
        return Err(Failure::usage(&format!(
            "{} is cluster {:?}'s map, but the backup is cluster {backup_cluster:?}'s",
            map_path.display(),
            m.cluster_id
        )));
    }
    // An edited map changes where the partitions go, never the tables or the indexes
    // (#1722): another table or index, or another `created`, would drop data or make an
    // index again empty.
    if r.map.is_some() {
        let ours = r.dir.join("map.json");
        let backed_up = map::parse_json(
            &std::fs::read_to_string(&ours)
                .map_err(|e| Failure::io(&format!("read {}", ours.display()), &e))?,
        )
        .map_err(|e| Failure::usage(&format!("{}: {e}", ours.display())))?;
        if backed_up.tables != m.tables {
            return Err(Failure::usage(&format!(
                "{}'s tables aren't the backup's: --map may change the nodes and where the \
                 partitions go, not the tables",
                map_path.display()
            )));
        }
        if backed_up.indexes != m.indexes {
            return Err(Failure::usage(&format!(
                "{}'s indexes aren't the backup's: --map may change the nodes and where the \
                 partitions go, not the indexes",
                map_path.display()
            )));
        }
    }
    let target = r
        .cluster_id
        .clone()
        .unwrap_or_else(|| backup_cluster.clone());
    if !map::id_ok(&target) {
        return Err(Failure::usage(&format!(
            "--cluster-id {target:?} must be 1-64 of [a-z0-9_-]"
        )));
    }
    m.cluster_id.clone_from(&target);
    map::validate(&m, false)
        .map_err(|e| Failure::usage(&format!("{}: {e}", map_path.display())))?;
    let in_map: BTreeSet<&str> = m.partitions.iter().map(|p| p.id.as_str()).collect();
    let in_backup: BTreeSet<&str> = copies.keys().map(String::as_str).collect();
    if in_map != in_backup {
        return Err(Failure::usage(&format!(
            "the map's partitions {in_map:?} aren't the backup's {in_backup:?}"
        )));
    }
    for c in copies.values() {
        check_copy(c)?;
    }

    let (channel_v, token) = channel(args, &args.node).await?;
    let token: Option<Arc<str>> = token.map(Arc::from);
    let live = live_map(channel_v, token.as_deref(), &args.node).await?;
    if let Some(l) = &live
        && l.cluster_id != target
    {
        return Err(Failure::usage(&format!(
            "{} is in cluster {:?}, not {target:?}: a restore under another id goes to nodes \
             of no cluster yet, or of that one (--cluster-id)",
            args.node, l.cluster_id
        )));
    }
    // Every node either map names, by id: the backup map's address first.
    let mut nodes: BTreeMap<String, String> = BTreeMap::new();
    for n in live.iter().flat_map(|l| &l.nodes) {
        nodes.insert(n.id.clone(), url_of(args, &n.address));
    }
    for n in &m.nodes {
        nodes.insert(n.id.clone(), url_of(args, &n.address));
    }
    let live_version = live.as_ref().map_or(0, |l| l.map_version);
    let progress = read_progress(&r.dir).filter(|p| p.cluster_id == target);
    // An earlier run set the map, and the live map still places every partition as it did:
    // only the unfences are left.
    if let (Some(pr), Some(l)) = (&progress, &live)
        && let Some(v) = pr.map_version
        && l.map_version >= v
        && m.partitions.iter().all(|p| {
            pr.stamped.get(&p.id).is_some_and(|st| {
                st.node == p.node
                    && l.partitions
                        .iter()
                        .any(|q| q.id == p.id && q.node == st.node && q.epoch == st.epoch)
            })
        })
    {
        let jobs: Vec<Job> = m
            .partitions
            .iter()
            .map(|p| Job {
                url: nodes[&p.node].clone(),
                node: p.node.clone(),
                epoch: pr.stamped[&p.id].epoch,
                copy: copies[&p.id].clone(),
                earlier: true,
            })
            .collect();
        for job in &jobs {
            serve(args, token.as_deref(), job).await?;
        }
        let _ = std::fs::remove_file(r.dir.join(PROGRESS));
        let mut out = restored_report(&target, &jobs, json!(v), Value::Null);
        out.document
            .insert("resumed".into(), json!("the map was set by an earlier run"));
        return Ok(out);
    }
    // Each node's partitions, after the resume above: `Status` opens each file it lists
    // (read-only, for its identity), which a node opening that partition at the same moment
    // would find holding its lock, so nothing here reads it once a map may be opening them.
    let mut states: BTreeMap<String, Option<Vec<proto::LocalPartition>>> = BTreeMap::new();
    for (id, url) in &nodes {
        let got = async {
            let (c, _) = channel(args, url).await?;
            proto::cluster_client::ClusterClient::new(c)
                .status(request(StatusRequest {}, token.as_deref())?)
                .await
                .map(|r| r.into_inner().partitions)
                .map_err(|s| Failure::usage(s.message()))
        };
        let got = tokio::time::timeout(Duration::from_secs(10), got).await;
        states.insert(id.clone(), got.ok().and_then(Result::ok));
    }
    let progress =
        progress.filter(|p| p.live_map_version == live_version && p.map_version.is_none());
    let mut jobs = Vec::new();
    // Nodes that held a restored partition and don't get it back (simp724's S2).
    let mut old_owners = Vec::new();
    for p in &m.partitions {
        let copy = copies[&p.id].clone();
        for (id, s) in &states {
            let Some(lp) = s.iter().flatten().find(|lp| lp.id == p.id) else {
                continue;
            };
            let state = lp.state();
            if matches!(
                state,
                LocalPartitionState::Serving | LocalPartitionState::Opening
            ) {
                return Err(Failure::usage(&format!(
                    "partition {} is serving on node {id:?}: fence it there first (phdb \
                     cluster fence), and close it on node {:?}, which it's restored to",
                    p.id, p.node
                )));
            }
            if *id == p.node && state == LocalPartitionState::Fenced {
                return Err(Failure::usage(&format!(
                    "partition {} is open (fenced) on node {id:?}, which it's restored to: \
                     close it there first (phdb cluster close). If a restore already set the \
                     map (it places {} on {id:?} at this file's epoch) and its {PROGRESS} is \
                     gone, `phdb cluster unfence {} --node <{id}'s URL>` finishes it instead",
                    p.id, p.id, p.id
                )));
            }
        }
        if !matches!(states.get(&p.node), Some(Some(_))) {
            return Err(Failure::usage(&format!(
                "node {:?}, which partition {} is restored to, can't be reached",
                p.node, p.id
            )));
        }
        let old = live
            .iter()
            .flat_map(|l| &l.partitions)
            .find(|q| q.id == p.id);
        if let Some(o) = old
            && o.node != p.node
            && !r.old_unreachable
            && !matches!(states.get(&o.node), Some(Some(_)))
        {
            return Err(Failure::usage(&format!(
                "node {:?}, which the live map gives partition {} to, can't be reached: pass \
                 --old-unreachable if its process is stopped",
                o.node, p.id
            )));
        }
        if let Some(o) = old
            && o.node != p.node
        {
            old_owners.push(json!({"partition": p.id, "node": o.node}));
        }
        // Past every copy of the partition a node reports too (simp724's N1 on #1832): a
        // move or relocation that failed after its stamp leaves one past the map's epoch.
        // Not this restore's own, recorded by an earlier run, which it reuses.
        let own = progress.as_ref().and_then(|pr| pr.stamped.get(&p.id));
        let floor = states
            .iter()
            .flat_map(|(id, s)| {
                s.iter()
                    .flatten()
                    .filter(move |lp| lp.id == p.id)
                    .filter(move |lp| {
                        own.is_none_or(|st| !(st.node == *id && st.epoch == lp.epoch))
                    })
                    .map(|lp| lp.epoch)
            })
            .max()
            .unwrap_or(0);
        let epoch = 1 + copy
            .epoch
            .max(p.epoch)
            .max(old.map_or(0, |o| o.epoch))
            .max(floor);
        // Stamped by an earlier run under this live map, and its node's file says so.
        let earlier = progress.as_ref().is_some_and(|pr| {
            pr.stamped.get(&p.id)
                == Some(&Stamped {
                    node: p.node.clone(),
                    epoch,
                })
        }) && states
            .get(&p.node)
            .and_then(Option::as_ref)
            .is_some_and(|s| s.iter().any(|lp| lp.id == p.id && lp.epoch == epoch));
        jobs.push(Job {
            url: nodes[&p.node].clone(),
            node: p.node.clone(),
            epoch,
            copy,
            earlier,
        });
    }
    let mut unwritable = None;
    let mut progress = progress.unwrap_or_else(|| Progress {
        cluster_id: target.clone(),
        live_map_version: live_version,
        ..Default::default()
    });
    progress
        .stamped
        .retain(|p, _| jobs.iter().any(|j| j.earlier && j.copy.partition == *p));

    // The uploads, `parallel` at once.
    let mut set = tokio::task::JoinSet::new();
    let mut pending = jobs.clone().into_iter();
    let mut failed: Option<Failure> = None;
    loop {
        while failed.is_none() && set.len() < r.parallel.max(1) {
            let Some(job) = pending.find(|j| !j.earlier) else {
                break;
            };
            let (args, token, cluster) = (args.clone(), token.clone(), backup_cluster.clone());
            set.spawn(async move { upload(&args, token.as_deref(), &cluster, &job, STALL).await });
        }
        let Some(done) = set.join_next().await else {
            break;
        };
        match done {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                failed.get_or_insert(e);
            }
            Err(e) => {
                failed.get_or_insert(Failure::usage(&format!("a partition's upload: {e}")));
            }
        }
    }
    if let Some(e) = failed {
        return Err(e);
    }
    // The stamps: each node installs its copy at the new epoch.
    let restore_from = if target == backup_cluster {
        String::new()
    } else {
        backup_cluster.clone()
    };
    for job in jobs.iter().filter(|j| !j.earlier) {
        let (c, _) = channel(args, &job.url).await?;
        proto::cluster_client::ClusterClient::new(c)
            .stamp(request(
                StampRequest {
                    cluster_id: target.clone(),
                    partition: job.copy.partition.clone(),
                    from_epoch: job.epoch - 1,
                    to_epoch: job.epoch,
                    restore: true,
                    restore_from_cluster_id: restore_from.clone(),
                    promote: false,
                    replica_epoch: 0,
                },
                token.as_deref(),
            )?)
            .await
            .map_err(|s| {
                Failure::usage(&format!(
                    "partition {} on node {}: stamp: {}",
                    job.copy.partition,
                    job.node,
                    s.message()
                ))
            })?;
        progress.stamped.insert(
            job.copy.partition.clone(),
            Stamped {
                node: job.node.clone(),
                epoch: job.epoch,
            },
        );
        record(&r.dir, &progress, &mut unwritable);
    }
    fault(&r.dir, "stamped")?;
    // The map, forced: tables exactly as the backup's.
    for p in &mut m.partitions {
        p.epoch = jobs
            .iter()
            .find(|j| j.copy.partition == p.id)
            .map_or(p.epoch, |j| j.epoch);
    }
    let set = set_map(args, m, true, Vec::new()).await?;
    progress.map_version = set.document.get("map_version").and_then(Value::as_u64);
    record(&r.dir, &progress, &mut unwritable);
    fault(&r.dir, "mapped")?;
    // Each partition opens on its node with the map, still under the fence the operator
    // set before the restore (it's persisted): lifted here once it's open.
    for job in &jobs {
        serve(args, token.as_deref(), job).await?;
    }
    let _ = std::fs::remove_file(r.dir.join(PROGRESS));
    let mut out = restored_report(
        &target,
        &jobs,
        set.document
            .get("map_version")
            .cloned()
            .unwrap_or(Value::Null),
        set.document.get("missed").cloned().unwrap_or(Value::Null),
    );
    if let Some(pushed) = set.document.get("pushed") {
        out.document.insert("pushed".into(), pushed.clone());
    }
    // Their copies stay open there, fenced, and their files until dropped.
    out.document
        .insert("close_and_drop_on".into(), Value::Array(old_owners));
    if let Some(why) = unwritable {
        out.document.insert("warning".into(), json!(why));
    }
    Ok(out)
}

/// One partition's restore progress (#1832), in `DIR/restore-progress-<P>.json`, apart from
/// a whole restore's: the node and epoch its stamp is for, recorded before the stamp, and
/// the map's version once set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PartProgress {
    cluster_id: String,
    partition: String,
    node: String,
    epoch: u64,
    map_version: Option<u64>,
}

fn part_progress_path(dir: &Path, partition: &str) -> PathBuf {
    dir.join(format!("restore-progress-{partition}.json"))
}

fn read_part_progress(dir: &Path, partition: &str) -> Option<PartProgress> {
    let text = std::fs::read_to_string(part_progress_path(dir, partition)).ok()?;
    let v: Value = serde_json::from_str(&text).ok()?;
    Some(PartProgress {
        cluster_id: v["cluster_id"].as_str()?.to_owned(),
        partition: v["partition"].as_str()?.to_owned(),
        node: v["node"].as_str()?.to_owned(),
        epoch: v["epoch"].as_u64()?,
        map_version: v["map_version"].as_u64(),
    })
}

/// Records `p`, as [`record`] does: a backup that can't be written to warns once.
fn record_part(dir: &Path, p: &PartProgress, unwritable: &mut Option<String>) {
    if unwritable.is_some() {
        return;
    }
    let doc = json!({
        "cluster_id": p.cluster_id,
        "partition": p.partition,
        "node": p.node,
        "epoch": p.epoch,
        "map_version": p.map_version,
    });
    let path = part_progress_path(dir, &p.partition);
    let tmp = path.with_extension("json.tmp");
    let written = std::fs::write(&tmp, serde_json::to_vec_pretty(&doc).expect("serializes"))
        .and_then(|()| std::fs::rename(&tmp, &path));
    if let Err(e) = written {
        let why = format!(
            "write {}: {e}: the restore's progress isn't recorded, so a run again after a \
             failure redoes the restore",
            path.display()
        );
        eprintln!("phdb cluster restore: warning: {why}");
        *unwritable = Some(why);
    }
}

/// A key range of a table's: its first key (`None`: the empty key) and its end (`None`:
/// open-ended), as bytes.
type Range = (Option<Vec<u8>>, Option<Vec<u8>>);

/// The ranges `m` gives `partition` in each table and index that has one there, by name
/// (names are unique across both): whether it's a table or an index, its `created`, and the
/// ranges as the splits bound them. An index that follows a table has that table's ranges
/// (#1722).
fn ranges_of(m: &ClusterMap, partition: &str) -> BTreeMap<String, (&'static str, u64, Vec<Range>)> {
    let on = |splits: &[proto::SplitKey], partitions: &[String]| -> Vec<Range> {
        let keys: Vec<Vec<u8>> = splits
            .iter()
            .map(|k| map::split_key_bytes(k).unwrap_or_default())
            .collect();
        partitions
            .iter()
            .enumerate()
            .filter(|(_, q)| *q == partition)
            .map(|(i, _)| {
                (
                    i.checked_sub(1).map(|j| keys[j].clone()),
                    keys.get(i).cloned(),
                )
            })
            .collect()
    };
    let mut out = BTreeMap::new();
    for t in &m.tables {
        let ranges = on(&t.splits, &t.partitions);
        if !ranges.is_empty() {
            out.insert(t.name.clone(), ("table", t.created, ranges));
        }
    }
    for i in &m.indexes {
        let ranges = if i.follows.is_empty() {
            on(&i.splits, &i.partitions)
        } else {
            m.tables
                .iter()
                .find(|t| t.name == i.follows)
                .map(|t| on(&t.splits, &t.partitions))
                .unwrap_or_default()
        };
        if !ranges.is_empty() {
            out.insert(i.name.clone(), ("index", i.created, ranges));
        }
    }
    out
}

/// What the live map's schema does to the backup's copy of `partition` once it opens
/// (D317 ticket 5's reconcile), or why it can't be restored alone: a table or index both
/// maps have (the same `created`) must give it the same ranges, since no reconcile moves
/// rows between partitions (cli692's design Q1). Tables and indexes created on it since
/// come back empty (a text index then fills from its source rows, #1722), and those
/// dropped since go. Their names, in that order.
fn schema_against(
    backup: &ClusterMap,
    live: &ClusterMap,
    partition: &str,
) -> Result<(Vec<String>, Vec<String>), Failure> {
    let (in_backup, in_live) = (ranges_of(backup, partition), ranges_of(live, partition));
    let mut empty = Vec::new();
    for (name, (kind, created, ranges)) in &in_live {
        let before = in_backup
            .get(name)
            .filter(|(k, c, _)| k == kind && c == created);
        let Some((_, _, before)) = before else {
            // Not on this partition in the backup, or another incarnation: the reconcile
            // makes it, empty. One the backup had on other partitions only can't have
            // moved here, since that would change a range it has here.
            if (*kind == "table"
                && backup
                    .tables
                    .iter()
                    .any(|t| t.name == *name && t.created == *created))
                || (*kind == "index"
                    && backup
                        .indexes
                        .iter()
                        .any(|i| i.name == *name && i.created == *created))
            {
                return Err(moved_range(partition, kind, name));
            }
            empty.push(name.clone());
            continue;
        };
        if before != ranges {
            return Err(moved_range(partition, kind, name));
        }
    }
    let dropped = in_backup
        .iter()
        .filter(|(name, (kind, created, _))| {
            !in_live
                .get(*name)
                .is_some_and(|(k, c, _)| k == kind && c == created)
        })
        .map(|(name, _)| name.clone())
        .collect();
    Ok((empty, dropped))
}

/// [`schema_against`]'s refusal: `name`'s range on `partition` changed since the backup.
fn moved_range(partition: &str, kind: &str, name: &str) -> Failure {
    Failure::usage(&format!(
        "partition {partition}'s key range of {kind} {name:?} changed since the backup \
         (its splits or partitions): the copy holds the backup's range, and no reconcile \
         moves rows between partitions. Restore the whole cluster, or put the {kind}'s \
         splits back first"
    ))
}

/// `phdb cluster restore DIR --partition P [--to ID]` (#1832): P's copy put back into the
/// live cluster alone. The other partitions and the tables stay as the live map has them;
/// P gets a new epoch, past every copy of it, and a new history.
///
/// The map goes through a relocation's step ([`relocate_as`] with [`Restoring`]): the same
/// checks and checked map set, with the stamp's `restore` set, so it waits as a relocation
/// does; then a majority of the nodes holding the map ([`majority_holds`]), then P opened
/// and unfenced on its node.
async fn restore_partition(
    args: &NodeArgs,
    r: &RestoreArgs,
    partition: &str,
) -> Result<Report, Failure> {
    if r.map.is_some() || r.cluster_id.is_some() {
        return Err(Failure::usage(
            "--partition restores one partition into the live cluster as its map has it: \
             --map and --cluster-id don't go with it",
        ));
    }
    let (backup_cluster, mut copies) = read_manifest(&r.dir)?;
    let Some(copy) = copies.remove(partition) else {
        return Err(Failure::usage(&format!(
            "the backup has no partition {partition:?}"
        )));
    };
    check_copy(&copy)?;
    let ours = r.dir.join("map.json");
    let backed_up = map::parse_json(
        &std::fs::read_to_string(&ours)
            .map_err(|e| Failure::io(&format!("read {}", ours.display()), &e))?,
    )
    .map_err(|e| Failure::usage(&format!("{}: {e}", ours.display())))?;

    let (channel_v, token) = channel(args, &args.node).await?;
    let token: Option<Arc<str>> = token.map(Arc::from);
    let Some(live) = live_map(channel_v, token.as_deref(), &args.node).await? else {
        return Err(Failure::usage(&format!(
            "{} has no cluster map: --partition restores into a live cluster",
            args.node
        )));
    };
    if live.cluster_id != backup_cluster {
        return Err(Failure::usage(&format!(
            "{} is in cluster {:?}, and the backup is cluster {backup_cluster:?}'s: \
             --partition restores into the cluster it came from",
            args.node, live.cluster_id
        )));
    }
    let Some(now) = live.partitions.iter().find(|q| q.id == partition).cloned() else {
        return Err(Failure::usage(&format!(
            "the live map (version {}) has no partition {partition:?}: --partition restores \
             a partition the map still lists",
            live.map_version
        )));
    };
    let target = r.to.clone().unwrap_or_else(|| now.node.clone());
    let nodes: BTreeMap<String, String> = live
        .nodes
        .iter()
        .map(|n| (n.id.clone(), url_of(args, &n.address)))
        .collect();
    let Some(url) = nodes.get(&target).cloned() else {
        return Err(Failure::usage(&format!(
            "the live map has no node {target:?}"
        )));
    };
    let (empty_since, dropped_since) = schema_against(&backed_up, &live, partition)?;

    let job = |epoch: u64| Job {
        copy: copy.clone(),
        node: target.clone(),
        url: url.clone(),
        epoch,
        earlier: false,
    };
    let progress = read_part_progress(&r.dir, partition)
        .filter(|pr| pr.cluster_id == live.cluster_id && pr.partition == partition);
    let mut unwritable = None;
    // An earlier run set the map, and it still places P so: only the waits are left.
    if let Some(pr) = &progress
        && let Some(v) = pr.map_version
        && live.map_version >= v
        && now.node == pr.node
        && now.epoch == pr.epoch
    {
        super::moving::majority_holds(args, &live, v, Some(OPEN_WAIT)).await?;
        serve(args, token.as_deref(), &job(pr.epoch)).await?;
        let _ = std::fs::remove_file(part_progress_path(&r.dir, partition));
        let mut out = part_report(&job(pr.epoch), v, &empty_since, &dropped_since, None);
        out.document
            .insert("resumed".into(), json!("the map was set by an earlier run"));
        return Ok(out);
    }

    // P on every node that answers: serving nowhere, not open where it goes, and every
    // copy's epoch, which the new one passes.
    let mut target_has = None;
    let mut floor = 0;
    let mut reached = BTreeSet::new();
    // A copy at the epoch an earlier run recorded for its stamp is that run's, which this
    // one reuses: it doesn't raise the floor (simp724's R2 and N1 on the design).
    let mine = progress
        .as_ref()
        .filter(|pr| pr.map_version.is_none())
        .map(|pr| (pr.node.clone(), pr.epoch));
    for (id, node_url) in &nodes {
        let got = async {
            let (c, _) = channel(args, node_url).await?;
            proto::cluster_client::ClusterClient::new(c)
                .status(request(StatusRequest {}, token.as_deref())?)
                .await
                .map(|r| r.into_inner().partitions)
                .map_err(|s| Failure::usage(s.message()))
        };
        let Ok(Ok(listed)) = tokio::time::timeout(Duration::from_secs(10), got).await else {
            if *id == target {
                return Err(Failure::usage(&format!(
                    "node {target:?}, which partition {partition} is restored to, can't be \
                     reached"
                )));
            }
            continue;
        };
        reached.insert(id.clone());
        let Some(lp) = listed.into_iter().find(|lp| lp.id == partition) else {
            continue;
        };
        let state = lp.state();
        if matches!(
            state,
            LocalPartitionState::Serving | LocalPartitionState::Opening
        ) {
            return Err(Failure::usage(&format!(
                "partition {partition} is serving on node {id:?}: fence it there first (phdb \
                 cluster fence), and close it on node {target:?}, which it's restored to"
            )));
        }
        if *id == target && state == LocalPartitionState::Fenced {
            return Err(Failure::usage(&format!(
                "partition {partition} is open (fenced) on node {id:?}, which it's restored \
                 to: close it there first (phdb cluster close)"
            )));
        }
        if mine.as_ref() == Some(&(id.clone(), lp.epoch)) {
            if *id == target {
                target_has = Some(lp.epoch);
            }
            continue;
        }
        floor = floor.max(lp.epoch);
    }
    if now.node != target && !r.old_unreachable && !reached.contains(&now.node) {
        return Err(Failure::usage(&format!(
            "node {:?}, which the live map gives partition {partition} to, can't be reached: \
             pass --old-unreachable if its process is stopped",
            now.node
        )));
    }
    let e = copy.epoch.max(now.epoch).max(floor);
    // Stamped by an earlier run at this epoch still: no upload, no stamp.
    let stamped = mine == Some((target.clone(), e + 1)) && target_has == Some(e + 1);
    let mut pr = PartProgress {
        cluster_id: live.cluster_id.clone(),
        partition: partition.to_owned(),
        node: target.clone(),
        epoch: e + 1,
        map_version: None,
    };
    record_part(&r.dir, &pr, &mut unwritable);
    if !stamped {
        upload(args, token.as_deref(), &backup_cluster, &job(e + 1), STALL).await?;
    }
    let set = relocate_as(
        args,
        partition,
        &target,
        r.old_unreachable,
        CopyCheck::No,
        Some(Restoring {
            from_epoch: e,
            stamped,
        }),
    )
    .await?;
    let version = set
        .document
        .get("map_version")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    pr.map_version = Some(version);
    record_part(&r.dir, &pr, &mut unwritable);
    fault(&r.dir, "mapped")?;
    super::moving::majority_holds(args, &live, version, Some(OPEN_WAIT)).await?;
    serve(args, token.as_deref(), &job(e + 1)).await?;
    let _ = std::fs::remove_file(part_progress_path(&r.dir, partition));
    let old = (now.node != target).then_some(now.node.as_str());
    let mut out = part_report(&job(e + 1), version, &empty_since, &dropped_since, old);
    out.document.insert("uploaded".into(), json!(!stamped));
    if let Some(why) = unwritable {
        out.document.insert("warning".into(), json!(why));
    }
    Ok(out)
}

/// A one-partition restore's report.
fn part_report(
    job: &Job,
    map_version: u64,
    empty_since: &[String],
    dropped_since: &[String],
    old: Option<&str>,
) -> Report {
    let mut out = report(
        "cluster restore",
        vec![
            ("partition", json!(job.copy.partition)),
            ("node", json!(job.node)),
            ("epoch", json!(job.epoch)),
            ("backup_epoch", json!(job.copy.epoch)),
            ("bytes", json!(job.copy.bytes)),
            ("map_version", json!(map_version)),
        ],
        Value::Null,
    );
    out.document
        .insert("tables_empty_since_backup".into(), json!(empty_since));
    out.document
        .insert("tables_dropped_since_backup".into(), json!(dropped_since));
    // Its copy there stays fenced or closed, and its files until dropped.
    out.document.insert(
        "close_and_drop_on".into(),
        json!(
            old.map(|n| vec![json!({"partition": job.copy.partition, "node": n})])
                .unwrap_or_default()
        ),
    );
    out
}

/// The restore's report.
fn restored_report(target: &str, jobs: &[Job], map_version: Value, missed: Value) -> Report {
    let bytes: u64 = jobs
        .iter()
        .filter(|j| !j.earlier)
        .map(|j| j.copy.bytes)
        .sum();
    let summary = vec![
        ("cluster_id", json!(target)),
        ("partitions", json!(jobs.len())),
        ("uploaded_bytes", json!(bytes)),
        ("map_version", map_version),
        ("missed", missed),
    ];
    let mut out = report("cluster restore", summary, Value::Null);
    out.columns = ["partition", "node", "epoch", "bytes", "installed"]
        .into_iter()
        .map(column)
        .collect();
    let installed = |j: &Job| {
        if j.earlier {
            "by an earlier run"
        } else {
            "now"
        }
    };
    out.rows = jobs
        .iter()
        .map(|j| {
            vec![
                json!(j.copy.partition),
                json!(j.node),
                json!(j.epoch),
                json!(j.copy.bytes),
                json!(installed(j)),
            ]
        })
        .collect();
    out.document.insert(
        "restored".into(),
        Value::Array(
            jobs.iter()
                .map(|j| {
                    json!({
                        "partition": j.copy.partition,
                        "node": j.node,
                        "epoch": j.epoch,
                        "backup_epoch": j.copy.epoch,
                        "bytes": j.copy.bytes,
                        "installed": installed(j),
                    })
                })
                .collect(),
        ),
    );
    out
}

/// Waits (up to [`OPEN_WAIT`]) for `job`'s partition to open on its node, and unfences it
/// there (a partition open and not fenced unfences as a no-op).
async fn serve(args: &NodeArgs, token: Option<&str>, job: &Job) -> Result<(), Failure> {
    let p = &job.copy.partition;
    let (ch, _) = channel(args, &job.url).await?;
    let mut c = proto::cluster_client::ClusterClient::new(ch);
    let until = tokio::time::Instant::now() + OPEN_WAIT;
    let mut last = String::from("not open");
    // `Unfence` until it answers: it does once the partition is open here (lifting a fence,
    // if any). Never `Status`, which opens the files it lists to read their identity, and so
    // can make the node's open of this one find its lock held and give up.
    while tokio::time::Instant::now() < until {
        match c
            .unfence(request(
                proto::PartitionRequest {
                    partition: p.clone(),
                    force: false,
                },
                token,
            )?)
            .await
        {
            Ok(_) => return Ok(()),
            Err(s) => last = s.message().to_owned(),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(Failure::usage(&format!(
        "partition {p} is restored and in the map, but isn't serving on node {} yet ({last}): \
         check `phdb cluster status`, then run the restore again, or `phdb cluster unfence {p} \
         --node <its URL>` once it's open. A fence since the node started needs a peer's map to \
         confirm it: if no peer can answer, `--force` only if you know it hasn't moved",
        job.node
    )))
}

/// The backup's cluster and its copies, by partition, from `DIR/manifest.json`.
fn read_manifest(dir: &Path) -> Result<(String, BTreeMap<String, Copy>), Failure> {
    let path = dir.join("manifest.json");
    let text = std::fs::read_to_string(&path).map_err(|e| {
        Failure::io(
            &format!(
                "read {} (a backup is complete only with its manifest)",
                path.display()
            ),
            &e,
        )
    })?;
    let bad = |why: &str| Failure::usage(&format!("{}: {why}", path.display()));
    let v: Value = serde_json::from_str(&text).map_err(|e| bad(&e.to_string()))?;
    let cluster = v["cluster_id"]
        .as_str()
        .ok_or_else(|| bad("no cluster_id"))?
        .to_owned();
    let mut copies = BTreeMap::new();
    for p in v["partitions"]
        .as_array()
        .ok_or_else(|| bad("no partitions"))?
    {
        let partition = p["partition"]
            .as_str()
            .filter(|id| map::id_ok(id))
            .ok_or_else(|| bad("a partition without a valid id"))?
            .to_owned();
        let file = p["file"].as_str().unwrap_or_default();
        // The copy's own name, never a path out of the backup.
        if file != format!("{partition}.phdb") {
            return Err(bad(&format!("partition {partition}'s file is {file:?}")));
        }
        let sha = p["sha256"].as_str().unwrap_or_default();
        let sha256 = unhex(sha).ok_or_else(|| bad(&format!("partition {partition}'s sha256")))?;
        let (Some(epoch), Some(bytes)) = (p["epoch"].as_u64(), p["bytes"].as_u64()) else {
            return Err(bad(&format!("partition {partition}'s epoch or bytes")));
        };
        let copy = Copy {
            path: dir.join(file),
            partition: partition.clone(),
            epoch,
            bytes,
            sha256,
        };
        if copies.insert(partition.clone(), copy).is_some() {
            return Err(bad(&format!("partition {partition} twice")));
        }
    }
    Ok((cluster, copies))
}

/// The copy is the manifest's: its size and SHA-256.
fn check_copy(c: &Copy) -> Result<(), Failure> {
    let bytes = std::fs::metadata(&c.path)
        .map_err(|e| Failure::io(&format!("read {}", c.path.display()), &e))?
        .len();
    if bytes != c.bytes || digest(&c.path)? != c.sha256 {
        return Err(Failure::usage(&format!(
            "{} isn't the manifest's copy of partition {} (its size or SHA-256 differs)",
            c.path.display(),
            c.partition
        )));
    }
    Ok(())
}

fn unhex(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

/// The map a node has, `None` before its first.
async fn live_map(
    channel: tonic::transport::Channel,
    token: Option<&str>,
    node: &str,
) -> Result<Option<ClusterMap>, Failure> {
    match proto::cluster_client::ClusterClient::new(channel)
        .get_map(request(GetMapRequest {}, token)?)
        .await
    {
        Ok(r) => Ok(Some(r.into_inner())),
        Err(s) if s.code() == tonic::Code::NotFound => Ok(None),
        Err(s) => Err(Failure::usage(&format!("{node}: {}", s.message()))),
    }
}

/// Uploads `job`'s copy to its node as a restore's set (the main file alone), resuming
/// from what the node holds after a break. The upload id comes from the copy and the
/// node, so a restore run again resumes it.
///
/// `stall` bounds each of the node's waits ([`upload_file`]): [`STALL`] in a restore.
pub(super) async fn upload(
    args: &NodeArgs,
    token: Option<&str>,
    cluster: &str,
    job: &Job,
    stall: Duration,
) -> Result<(), Failure> {
    let c = &job.copy;
    let what = |m: &str| format!("partition {} to node {}: {m}", c.partition, job.node);
    let file = PartitionFile {
        name: String::new(),
        bytes: c.bytes,
        sha256: c.sha256.to_vec().into(),
    };
    let id = Sha256::digest(format!(
        "phdb restore {cluster} {} {} {}",
        c.partition,
        job.node,
        hex(&c.sha256)
    ));
    let header = PartitionUploadHeader {
        partition: c.partition.clone(),
        cluster_id: cluster.to_owned(),
        epoch: c.epoch,
        upload_id: hex(&id[..16]),
        purpose: PartitionUploadPurpose::Restore as i32,
        file: Some(file.clone()),
        manifest: vec![file],
    };
    let (ch, _) = channel(args, &job.url).await?;
    let mut admin = proto::admin_client::AdminClient::new(ch);
    let mut breaks = 0;
    loop {
        let read = |from: u64, mut feed: Feed| {
            let path = c.path.clone();
            async move {
                let mut f = tokio::fs::File::open(&path).await?;
                f.seek(std::io::SeekFrom::Start(from)).await?;
                // A node can name any chunk size; a read is at most 4 MiB, and `send` cuts
                // it to the node's.
                let mut buf = vec![0u8; feed.max().min(4 << 20)];
                loop {
                    let n = f.read(&mut buf).await?;
                    // The header goes first even with nothing left to send: the node then
                    // finishes the file. The node ending the upload isn't the reader's
                    // error: its answer says why.
                    let sent = feed.send(Bytes::copy_from_slice(&buf[..n])).await;
                    if sent.is_err() || n == 0 {
                        return Ok::<(), std::io::Error>(());
                    }
                }
            }
        };
        // `Ok`: whether the node holds the whole file. `Err(Ok)`: final. `Err(Err)`: the
        // node's status, a refusal or a break.
        let sent = match upload_file(&mut admin, token, header.clone(), stall, read).await {
            // Complete, or an install of the partition's set is in progress there (a run
            // that stopped in it): the stamp finishes it (#1848).
            Ok(None) => Ok(true),
            Ok(Some(((), reply))) => Ok(reply.complete),
            Err(Stopped::Request(f)) => Err(Ok(f)),
            Err(Stopped::Source(e)) => Err(Ok(Failure::io(&what("read the copy"), &e))),
            // The node reads nothing, its connection up (#1950): a break, to resume.
            Err(Stopped::Stalled) => Err(Err(tonic::Status::unavailable(format!(
                "the node took nothing of the file for {} s",
                stall.as_secs()
            )))),
            // The node took every byte and never answered (#1983). Final, as in a move: a
            // resume would send nothing and wait as long again, for a node whose hash or
            // sync has hung (simp724 on #1992). A run again resumes it.
            Err(Stopped::Unanswered(bound)) => Err(Ok(Failure::usage(&what(&format!(
                "the node took the file and gave no answer for {} s: run the restore again",
                bound.as_secs()
            ))))),
            Err(Stopped::Status(s) | Stopped::Upload(s)) => Err(Err(s)),
        };
        match sent {
            Ok(true) => return Ok(()),
            Err(Ok(f)) => return Err(f),
            // A refusal is final; a break (or a stream that ended short) resumes.
            Err(Err(s)) if !resumable(&s) => return Err(Failure::usage(&what(s.message()))),
            Ok(false) | Err(Err(_)) if breaks < RESUMES => {
                breaks += 1;
                tokio::time::sleep(Duration::from_millis(200 * u64::from(breaks))).await;
            }
            Ok(false) => return Err(Failure::usage(&what("the node didn't finish the file"))),
            Err(Err(s)) => return Err(Failure::usage(&what(s.message()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str, created: u64, splits: &[&str], parts: &[&str]) -> proto::TablePlacement {
        proto::TablePlacement {
            name: name.into(),
            created,
            splits: splits
                .iter()
                .map(|k| proto::SplitKey {
                    key: Some(proto::split_key::Key::Text((*k).into())),
                })
                .collect(),
            partitions: parts.iter().map(|p| (*p).into()).collect(),
            ..Default::default()
        }
    }

    fn with(tables: Vec<proto::TablePlacement>) -> ClusterMap {
        ClusterMap {
            tables,
            ..Default::default()
        }
    }

    #[test]
    fn a_partition_restores_alone_only_onto_the_ranges_it_had() {
        // #1832, Q1: p3's range of `users` is [m, end) in both maps. A split moved elsewhere
        // in the table (g to h, between p1 and p2) doesn't change it.
        let backup = with(vec![table("users", 1, &["g", "m"], &["p1", "p2", "p3"])]);
        let moved_elsewhere = with(vec![table("users", 1, &["h", "m"], &["p1", "p2", "p3"])]);
        assert_eq!(
            schema_against(&backup, &moved_elsewhere, "p3").unwrap(),
            (Vec::<String>::new(), Vec::<String>::new())
        );
        // p3's own bound moved (m to n): refused, since no reconcile moves rows.
        let moved = with(vec![table("users", 1, &["g", "n"], &["p1", "p2", "p3"])]);
        let e = schema_against(&backup, &moved, "p3").unwrap_err();
        assert!(
            e.message().contains("key range of table \"users\""),
            "{}",
            e.message()
        );
        // p3 given a second range of the table: refused too.
        let more = with(vec![table("users", 1, &["g", "m"], &["p3", "p2", "p3"])]);
        assert!(schema_against(&backup, &more, "p3").is_err());
    }

    #[test]
    fn tables_created_or_dropped_since_the_backup_are_the_reconciles() {
        // #1832, Q1: a table created on p3 since (or dropped and created again: another
        // `created`) comes back empty; one the live map dropped goes. Neither refuses.
        let backup = with(vec![
            table("users", 1, &["m"], &["p1", "p3"]),
            table("old", 2, &[], &["p3"]),
            table("again", 3, &[], &["p3"]),
        ]);
        let live = with(vec![
            table("users", 1, &["m"], &["p1", "p3"]),
            table("again", 9, &[], &["p3"]),
            table("new", 10, &[], &["p3"]),
        ]);
        assert_eq!(
            schema_against(&backup, &live, "p3").unwrap(),
            (
                vec!["again".to_owned(), "new".to_owned()],
                vec!["again".to_owned(), "old".to_owned()]
            )
        );
    }

    fn index(
        name: &str,
        created: u64,
        follows: &str,
        splits: &[&str],
        parts: &[&str],
    ) -> proto::IndexPlacement {
        let t = table(name, created, splits, parts);
        proto::IndexPlacement {
            name: t.name,
            follows: follows.into(),
            splits: t.splits,
            partitions: t.partitions,
            created,
            ..Default::default()
        }
    }

    #[test]
    fn indexes_count_as_tables_do_when_a_partition_restores_alone() {
        // #1722 (audit120's M2): an index following `users` has its ranges; one of its own
        // ranges has those. Created since (or again) comes back empty, dropped since goes,
        // and one whose range on p3 moved refuses the restore.
        let backup = ClusterMap {
            indexes: vec![
                index("uv", 4, "users", &[], &[]),
                index("own", 5, "", &["k"], &["p1", "p3"]),
                index("gone", 6, "", &[], &["p3"]),
            ],
            ..with(vec![table("users", 1, &["m"], &["p1", "p3"])])
        };
        let live = ClusterMap {
            indexes: vec![
                index("uv", 7, "users", &[], &[]),
                index("own", 5, "", &["k"], &["p1", "p3"]),
                index("ut", 8, "users", &[], &[]),
            ],
            ..with(vec![table("users", 1, &["m"], &["p1", "p3"])])
        };
        assert_eq!(
            schema_against(&backup, &live, "p3").unwrap(),
            (
                vec!["ut".to_owned(), "uv".to_owned()],
                vec!["gone".to_owned(), "uv".to_owned()]
            )
        );
        let moved = ClusterMap {
            indexes: vec![index("own", 5, "", &["j"], &["p1", "p3"])],
            ..with(vec![table("users", 1, &["m"], &["p1", "p3"])])
        };
        let e = schema_against(&backup, &moved, "p3").unwrap_err();
        assert!(
            e.message().contains("key range of index \"own\""),
            "{}",
            e.message()
        );
        // An index on p1 only in the backup, now on p3 too: its rows aren't in p3's copy.
        let spread = ClusterMap {
            indexes: vec![index("own", 5, "", &[], &["p3"])],
            ..with(vec![table("users", 1, &["m"], &["p1", "p3"])])
        };
        let only_p1 = ClusterMap {
            indexes: vec![index("own", 5, "", &[], &["p1"])],
            ..with(vec![table("users", 1, &["m"], &["p1", "p3"])])
        };
        assert!(schema_against(&only_p1, &spread, "p3").is_err());
    }

    #[test]
    fn unhex_reads_a_sha256_and_refuses_anything_else() {
        let h = hex(&[0xab; 32]);
        assert_eq!(unhex(&h), Some([0xab; 32]));
        assert_eq!(unhex(&h[..62]), None);
        assert_eq!(unhex(&"zz".repeat(32)), None);
    }
}

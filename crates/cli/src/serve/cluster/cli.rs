//! `phdb cluster` (D317, ICR 0117): the operator's commands for a partitioned cluster.
//! `init-partition` stamps a partition file offline; `set-map`, `show-map` and `status`
//! talk to a running node (`show-map --file` reads a down node's map file instead).

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};

use super::identity::{self, Identity};
use super::map;
use crate::ExitStatus;
use crate::args::Cli;
use crate::exit::Failure;
use crate::output::{Column, Report};
use crate::serve::proto::{self, ClusterMap, GetMapRequest, SetMapRequest, StatusRequest};

/// How `phdb cluster` reaches a node.
#[derive(Debug, Clone, clap::Args)]
pub(crate) struct NodeArgs {
    /// A node's URL: `http://HOST:PORT` or `https://HOST:PORT`.
    #[arg(long, value_name = "URL")]
    pub node: String,
    /// The bearer token to present (admin scope for `set-map`, read for the others), alone
    /// in the file. Also from `PHDB_TOKEN`. Never a flag's value.
    #[arg(long, value_name = "FILE")]
    pub token_file: Option<PathBuf>,
    /// A PEM file of CA certificates to trust beside the system's roots.
    #[arg(long, value_name = "PEM")]
    pub ca: Option<PathBuf>,
}

/// `phdb cluster`'s subcommands.
#[derive(Debug, Clone, clap::Subcommand)]
pub(crate) enum ClusterCommand {
    /// Stamp a database file as a cluster's partition, offline: its identity row
    /// (`__phdb_cluster`) records the cluster, the partition and its epoch. The file is
    /// created if missing, and must be named `<partition>.phdb` in the node's cluster
    /// directory.
    InitPartition {
        /// The partition's file (`DIR/<partition>.phdb`).
        file: PathBuf,
        #[arg(long, value_name = "ID")]
        cluster_id: String,
        #[arg(long, value_name = "ID")]
        partition: String,
        #[arg(long, value_name = "N", default_value_t = 1)]
        epoch: u64,
    },
    /// Replace the cluster's map from a JSON file: the node validates, persists and pushes
    /// it to the others, and the answer says which took it.
    SetMap {
        /// The map, as JSON (`ClusterMap`, ICR 0117).
        file: PathBuf,
        #[command(flatten)]
        node: NodeArgs,
        /// Recovery when the map's writer (the node with the smallest id) is down: set the
        /// map at `--node` rather than the writer. Its version is put above every reachable
        /// node's, and it must name a reachable writer (remove the down one, or let another
        /// id be the smallest). No partition's epoch may go back, or stay with another node,
        /// against any reachable node's map; unreachable nodes are reported `unchecked`.
        /// Only for a writer that's down: a writer that's up and cut
        /// off can change the map meanwhile, and its change is lost (D317 amendment 1).
        #[arg(long)]
        force: bool,
    },
    /// Print a node's map, or a map file's (a down node's `DIR/cluster-map.json`).
    ShowMap {
        #[arg(long, value_name = "PATH", conflicts_with = "node")]
        file: Option<PathBuf>,
        #[arg(long, value_name = "URL", required_unless_present = "file")]
        node: Option<String>,
        #[arg(long, value_name = "FILE")]
        token_file: Option<PathBuf>,
        #[arg(long, value_name = "PEM")]
        ca: Option<PathBuf>,
    },
    /// Every node's status, from the map one node has: map versions side by side, so a
    /// push that hasn't reached a node shows.
    Status {
        #[command(flatten)]
        node: NodeArgs,
    },
    /// Back up every partition, each through the node the map gives it, plus the map, into
    /// DIR: `map.json`, `manifest.json` and `<partition>.phdb`. Each copy is its own
    /// consistent snapshot; the set is not one point in time across partitions. Fails if
    /// the map moves a partition while it runs.
    Backup {
        /// A new or empty directory.
        dir: PathBuf,
        #[command(flatten)]
        node: NodeArgs,
        /// Copy only what each writer holds durable at its level (`backup_durable`).
        #[arg(long)]
        durable: bool,
        /// Partitions backed up at once.
        #[arg(long, value_name = "N", default_value_t = 4)]
        parallel: usize,
    },
    /// Put a `backup` back: each partition's copy is uploaded to the node the backup's map
    /// (or `--map`) gives it, installed there at a new epoch, and the map set by the forced
    /// path, with every table as the backup has it. Fence every partition first, and close
    /// each on the node it's restored to. Writes since the backup are lost, and feed tokens
    /// from before it expire. Run again after a failure.
    Restore {
        /// A complete backup's directory (`manifest.json`, `map.json`, `<partition>.phdb`).
        dir: PathBuf,
        #[command(flatten)]
        node: NodeArgs,
        /// The map to install in place of the backup's `map.json`: the same partitions and
        /// tables, its nodes at other addresses or other nodes holding them.
        #[arg(long, value_name = "FILE")]
        map: Option<PathBuf>,
        /// Restore under another cluster id (beside the live cluster, onto nodes of no
        /// cluster yet): every file is stamped with it.
        #[arg(long, value_name = "ID")]
        cluster_id: Option<String>,
        /// A node the live map gives a partition to can't be reached (its process is
        /// stopped): skip the check that it isn't serving it.
        #[arg(long)]
        old_unreachable: bool,
        /// Partitions uploaded at once.
        #[arg(long, value_name = "N", default_value_t = 4)]
        parallel: usize,
    },
    /// Fence a partition on the node that serves it (`--node`): its writes answer `Fenced`
    /// through every node until it's unfenced, relocated or closed; reads go on. Answers
    /// once the writes admitted before the fence have finished. Kept across a restart.
    Fence {
        partition: String,
        #[command(flatten)]
        node: NodeArgs,
    },
    /// Lift a partition's fence on `--node` (a closed partition reopens first). Refused
    /// unless the node's map names it for the partition at its file's epoch, and a peer's
    /// map, at least as new, says the same: the node's own map may be behind, and a move
    /// may have passed it by.
    Unfence {
        partition: String,
        #[command(flatten)]
        node: NodeArgs,
        /// Lift a fence that no peer's map confirms: only when no peer can answer and you
        /// know the partition hasn't moved. A peer's newer map may have given it to another
        /// node, and then both would take writes.
        #[arg(long)]
        force: bool,
    },
    /// Close a fenced partition on `--node`: its files become final (for a relocation's
    /// copy), its calls answer `PartitionUnavailable`, and `status` lists the files with
    /// their SHA-256 once hashed. `unfence` reopens it.
    Close {
        partition: String,
        #[command(flatten)]
        node: NodeArgs,
    },
    /// Move a partition to node `--to`, whose staging holds its files (uploaded, or a
    /// closed copy already there): the new node installs them and takes the next epoch,
    /// then the map gives it the partition, and it serves at once. The node serving it now
    /// must have it fenced or closed, unless `--old-unreachable`.
    Relocate {
        partition: String,
        /// The node id it moves to.
        #[arg(long, value_name = "ID")]
        to: String,
        #[command(flatten)]
        node: NodeArgs,
        /// The node serving it now can't be reached (its process is stopped): skip the
        /// check that it's fenced or closed there.
        #[arg(long)]
        old_unreachable: bool,
        /// The files on `--to` are the partition's own, copied whole from the node serving
        /// it (`phdb cluster move` does this): its change-feed history carries over, and
        /// tokens from before the move resume on the new node. Without it the move starts a
        /// new history (a promotion, a restore), and older tokens expire. Not with
        /// `--old-unreachable`: a copy is taken from that node's closed files.
        #[arg(long, conflicts_with = "old_unreachable")]
        copy: bool,
    },
    /// Move a partition to node `--to` with its files copied whole, so its change-feed
    /// history and tokens carry over: fence and close it on the node serving it, copy every
    /// file (the main file, the WAL files a clean close keeps, the feed's) to `--to`, stamp
    /// it there at the next epoch, check the installed set against the closed one (name,
    /// size, SHA-256), set the map (`relocate --copy`) and wait for `--to` to serve it.
    /// Writes answer `Fenced` from the fence until the map moves; reads are down from the
    /// close until `--to` serves. Both grow with the files' size, so with the feed's
    /// retention. Run it again to resume one that stopped. The old node's files stay
    /// unless `--drop-source`.
    Move {
        partition: String,
        /// The node id it moves to.
        #[arg(long, value_name = "ID")]
        to: String,
        #[command(flatten)]
        node: NodeArgs,
        /// Once the copy is checked and `--to` serves the partition, move the old node's
        /// files aside there (`drop-partition`: nothing is deleted).
        #[arg(long)]
        drop_source: bool,
        /// Give up a wait for a node after SECS (at least 1): OLD hashing the closed files, or `--to`
        /// serving the partition once the map gives it to it. The error names the step and
        /// the node's last `status` line for the partition. It bounds each wait, not the
        /// copy, which takes as long as the files do. Without it, a wait goes on as long as
        /// it takes, saying every 10 s what it waits for.
        #[arg(long, value_name = "SECS", value_parser = clap::value_parser!(u64).range(1..))]
        timeout: Option<u64>,
    },
    /// Move a partition's files on `--node` to `DIR/.phdb-dropped/<P>-<epoch>-<time>/`,
    /// once the map no longer places it there and it isn't open (close it first). Nothing
    /// is deleted: delete that directory once the move is checked. It frees no space
    /// until then.
    DropPartition {
        partition: String,
        #[command(flatten)]
        node: NodeArgs,
    },
}

pub(crate) fn run(cli: &Cli, cmd: &ClusterCommand) -> Result<Report, Failure> {
    match cmd {
        ClusterCommand::InitPartition {
            file,
            cluster_id,
            partition,
            epoch,
        } => init_partition(cli, file, cluster_id, partition, *epoch),
        ClusterCommand::SetMap { file, node, force } => {
            let json = std::fs::read_to_string(file)
                .map_err(|e| Failure::io(&format!("read {}", file.display()), &e))?;
            let m = map::parse_json(&json).map_err(|e| Failure::usage(&e))?;
            map::validate(&m, false).map_err(|e| Failure::usage(&e))?;
            runtime()?.block_on(set_map(node, m, *force, Vec::new()))
        }
        ClusterCommand::ShowMap {
            file,
            node,
            token_file,
            ca,
        } => {
            let m = match (file, node) {
                (Some(path), _) => {
                    let json = std::fs::read_to_string(path)
                        .map_err(|e| Failure::io(&format!("read {}", path.display()), &e))?;
                    map::parse_json(&json).map_err(|e| Failure::usage(&e))?
                }
                (None, Some(node)) => {
                    let args = NodeArgs {
                        node: node.clone(),
                        token_file: token_file.clone(),
                        ca: ca.clone(),
                    };
                    runtime()?.block_on(async {
                        let (mut c, token) = connect(&args, &args.node).await?;
                        c.get_map(request(GetMapRequest {}, token.as_deref())?)
                            .await
                            .map(tonic::Response::into_inner)
                            .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))
                    })?
                }
                (None, None) => unreachable!("clap requires one"),
            };
            let doc: Value = serde_json::from_str(&map::canonical_json(&m)).expect("map JSON");
            let summary = vec![
                ("cluster_id", json!(m.cluster_id)),
                ("map_version", json!(m.map_version)),
                ("nodes", json!(m.nodes.len())),
                ("partitions", json!(m.partitions.len())),
                ("tables", json!(m.tables.len())),
            ];
            Ok(report("cluster show-map", summary, doc))
        }
        ClusterCommand::Status { node } => runtime()?.block_on(status(node)),
        ClusterCommand::Backup {
            dir,
            node,
            durable,
            parallel,
        } => runtime()?.block_on(super::backup::backup(node, dir, *durable, *parallel)),
        ClusterCommand::Restore {
            dir,
            node,
            map,
            cluster_id,
            old_unreachable,
            parallel,
        } => runtime()?.block_on(super::restore::restore(
            node,
            &super::restore::RestoreArgs {
                dir: dir.clone(),
                map: map.clone(),
                cluster_id: cluster_id.clone(),
                old_unreachable: *old_unreachable,
                parallel: *parallel,
            },
        )),
        ClusterCommand::Fence { partition, node } => {
            runtime()?.block_on(partition_call(node, partition, Call::Fence))
        }
        ClusterCommand::Unfence {
            partition,
            node,
            force,
        } => runtime()?.block_on(partition_call(node, partition, Call::Unfence(*force))),
        ClusterCommand::Close { partition, node } => {
            runtime()?.block_on(partition_call(node, partition, Call::Close))
        }
        ClusterCommand::Move {
            partition,
            to,
            node,
            drop_source,
            timeout,
        } => runtime()?.block_on(super::moving::run(
            node,
            partition,
            to,
            *drop_source,
            timeout.map(Duration::from_secs),
        )),
        ClusterCommand::DropPartition { partition, node } => {
            runtime()?.block_on(partition_call(node, partition, Call::Drop))
        }
        ClusterCommand::Relocate {
            partition,
            to,
            node,
            old_unreachable,
            copy,
        } => runtime()?.block_on(relocate(node, partition, to, *old_unreachable, *copy)),
    }
}

/// One of the per-partition calls on a node (D317 ticket 6).
#[derive(Clone, Copy)]
enum Call {
    Fence,
    /// With `--force`.
    Unfence(bool),
    Close,
    Drop,
}

/// `phdb cluster fence | unfence | close | drop-partition`: the call on `--node`, and the
/// partition's line there.
async fn partition_call(args: &NodeArgs, partition: &str, call: Call) -> Result<Report, Failure> {
    let (mut c, token) = connect(args, &args.node).await?;
    let req = request(
        proto::PartitionRequest {
            partition: partition.to_owned(),
            force: matches!(call, Call::Unfence(true)),
        },
        token.as_deref(),
    )?;
    let (command, reply) = match call {
        Call::Fence => ("cluster fence", c.fence(req).await),
        Call::Unfence(_) => ("cluster unfence", c.unfence(req).await),
        Call::Close => ("cluster close", c.close(req).await),
        Call::Drop => ("cluster drop-partition", c.drop_partition(req).await),
    };
    let lp = reply
        .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))?
        .into_inner();
    Ok(local_report(command, &args.node, &lp))
}

/// A partition's line on a node as a report.
fn local_report(command: &'static str, node: &str, lp: &proto::LocalPartition) -> Report {
    let summary = vec![
        ("node", json!(node)),
        ("partition", json!(lp.id)),
        ("epoch", json!(lp.epoch)),
        ("state", json!(state_text(lp))),
        ("file", json!(lp.file)),
        ("files", json!(lp.files.len())),
    ];
    report(
        command,
        summary,
        json!({ "local": serde_json::to_value(lp).expect("LocalPartition serializes") }),
    )
}

/// A partition's state for a person: `serving`, `fenced (operator)`, `fenced (awaiting peer
/// map)`, `closed`, `opening`, or empty when the node doesn't hold it open.
pub(super) fn state_text(lp: &proto::LocalPartition) -> String {
    use proto::{FenceReason, LocalPartitionState};
    match (lp.state(), lp.fence_reason()) {
        (LocalPartitionState::Serving, _) => "serving".into(),
        (LocalPartitionState::Fenced, FenceReason::Operator) => "fenced (operator)".into(),
        (LocalPartitionState::Fenced, FenceReason::AwaitingPeerMap) => {
            "fenced (awaiting peer map)".into()
        }
        (LocalPartitionState::Fenced, _) => "fenced".into(),
        (LocalPartitionState::Closed, _) => "closed".into(),
        (LocalPartitionState::Opening, _) => "opening".into(),
        (LocalPartitionState::Unspecified, _) => String::new(),
    }
}

/// Whether a relocation keeps P's feed history, and who checked the copy that lets it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) enum CopyCheck {
    /// Not a copy: P starts a new history on NEW.
    No,
    /// `--copy`: the set NEW's stamp installs must be OLD's closed files, file by file, or
    /// the map doesn't change (the coordinator's call after audit120's F1).
    Check,
    /// `phdb cluster move`'s map step: the move compared the set its own stamp installed
    /// with OLD's closed set, these files.
    Checked(Vec<proto::PartitionFile>),
}

/// `phdb cluster relocate P --to NEW` (D317 ticket 6): stamp NEW from the map's epoch `e`
/// to `e + 1`, then set the map with P on NEW at `e + 1` through the writer, marked as
/// copied (`SetMapRequest.copies`) with `copy`, so P keeps its feed history. Run again once
/// done (the map names NEW, and P isn't closed there), it answers so and does nothing.
pub(crate) async fn relocate(
    args: &NodeArgs,
    partition: &str,
    to: &str,
    old_unreachable: bool,
    copy: bool,
) -> Result<Report, Failure> {
    let copy = if copy {
        CopyCheck::Check
    } else {
        CopyCheck::No
    };
    relocate_as(args, partition, to, old_unreachable, copy).await
}

/// [`relocate`], with who checks a copy ([`CopyCheck`]).
pub(super) async fn relocate_as(
    args: &NodeArgs,
    partition: &str,
    to: &str,
    old_unreachable: bool,
    copy: CopyCheck,
) -> Result<Report, Failure> {
    let checks = copy == CopyCheck::Check;
    let checked = match &copy {
        CopyCheck::Checked(files) => Some(files.clone()),
        _ => None,
    };
    let copy = copy != CopyCheck::No;
    if copy && old_unreachable {
        return Err(Failure::usage(
            "--copy needs the old node's closed files, so it can't be unreachable: a move \
             away from a stopped node is a promotion, which starts a new feed history",
        ));
    }
    let (mut c, token) = connect(args, &args.node).await?;
    let mut m = c
        .get_map(request(GetMapRequest {}, token.as_deref())?)
        .await
        .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))?
        .into_inner();
    let Some(p) = m.partitions.iter().find(|p| p.id == partition).cloned() else {
        return Err(Failure::usage(&format!(
            "the map (version {}) has no partition {partition:?}",
            m.map_version
        )));
    };
    let address = |id: &str| {
        m.nodes
            .iter()
            .find(|n| n.id == id)
            .map(|n| url_of(args, &n.address))
    };
    let Some(new_url) = address(to) else {
        return Err(Failure::usage(&format!("the map has no node {to:?}")));
    };
    let old_url = address(&p.node).unwrap_or_default();
    let old_address = m
        .nodes
        .iter()
        .find(|n| n.id == p.node)
        .map(|n| n.address.clone())
        .unwrap_or_default();
    // The partition on the node serving it now: fenced or closed (cli692's S7). An
    // in-place restore (the same node) needs it closed.
    let probe = async {
        let (mut c, token) = connect(args, &old_url).await?;
        c.status(request(StatusRequest {}, token.as_deref())?)
            .await
            .map(|r| {
                r.into_inner()
                    .partitions
                    .into_iter()
                    .find(|lp| lp.id == partition)
            })
            .map_err(|s| Failure::usage(&format!("{old_url}: {}", s.message())))
    };
    let old_state = if old_unreachable {
        // Still asked, briefly (cli692's S3 on #1755): a node that answers and serves P
        // isn't unreachable, and a move now would give P two writers.
        if let Ok(Ok(Some(lp))) = tokio::time::timeout(Duration::from_secs(3), probe).await
            && lp.state() == proto::LocalPartitionState::Serving
        {
            return Err(Failure::usage(&format!(
                "--old-unreachable, but node {:?} answers and serves partition {partition}: \
                 fence it there first (phdb cluster fence), or stop its process",
                p.node
            )));
        }
        None
    } else {
        Some(probe.await?.map(|lp| lp.state()).unwrap_or_default())
    };
    use proto::LocalPartitionState as S;
    if p.node == to && old_state != Some(S::Closed) {
        let mut r = report(
            "cluster relocate",
            vec![
                ("partition", json!(partition)),
                ("node", json!(to)),
                ("epoch", json!(p.epoch)),
                ("map_version", json!(m.map_version)),
                ("done", json!("the map already gives it this node")),
            ],
            Value::Null,
        );
        r.status = ExitStatus::SUCCESS;
        return Ok(r);
    }
    if let Some(state) = old_state
        && !matches!(state, S::Fenced | S::Closed)
    {
        return Err(Failure::usage(&format!(
            "partition {partition} isn't fenced or closed on node {:?}: fence it there first \
             (phdb cluster fence), or pass --old-unreachable if that node's process is stopped",
            p.node
        )));
    }
    // `--copy`: OLD's closed set, hashed, to check the copy NEW installs against. An open
    // partition's files aren't final, so it must be closed.
    let closed = if checks {
        // In place, a copy keeps nothing an unfence doesn't: the node holds P's closed files
        // already, and its stamp would drop the closed record the last check reads
        // (audit120's question on #1838).
        if p.node == to {
            return Err(Failure::usage(&format!(
                "--copy copies {partition}'s closed files to another node, and node {to:?} \
                 already holds them: phdb cluster unfence {partition} there serves it again, \
                 with its history. A relocation in place without --copy (a restore's files \
                 staged there) starts a new history"
            )));
        }
        if old_state != Some(S::Closed) {
            return Err(Failure::usage(&format!(
                "--copy checks the files node {to:?} installs against the ones node {:?} \
                 closed, and {partition} isn't closed there: close it first (phdb cluster \
                 close), or use phdb cluster move {partition} --to {to}, which fences, closes, \
                 copies and checks it",
                p.node
            )));
        }
        let old = super::moving::At::new(args, &p.node, &old_address).await?;
        Some(super::moving::closed_files(&old, partition, None).await?)
    } else {
        None
    };
    let (mut new, new_token) = connect(args, &new_url).await?;
    let stamped = new
        .stamp(request(
            proto::StampRequest {
                cluster_id: m.cluster_id.clone(),
                partition: partition.to_owned(),
                from_epoch: p.epoch,
                to_epoch: p.epoch + 1,
                restore: false,
                restore_from_cluster_id: String::new(),
            },
            new_token.as_deref(),
        )?)
        .await
        .map_err(|s| Failure::usage(&format!("{new_url}: stamp: {}", s.message())))?
        .into_inner();
    if let Some(closed) = &closed {
        // The map changes only for a copy that is OLD's closed set: a staged set from
        // another close, or a file stamped earlier, would serve without writes OLD
        // acknowledged since, and keep the history all the same.
        let unchanged = format!(
            "The map is unchanged: it still gives {partition} to node {:?} at epoch {}. Use \
             phdb cluster move {partition} --to {to}, which copies the closed files itself and \
             checks them",
            p.node, p.epoch
        );
        if stamped.files.is_empty() {
            return Err(Failure::usage(&format!(
                "node {to:?} installed no staged copy of {partition} at the stamp, so there is \
                 nothing to check against node {:?}'s closed files. {unchanged}",
                p.node
            )));
        }
        super::moving::compare(&stamped.files, closed).map_err(|why| {
            Failure::usage(&format!(
                "node {to:?} installed files that aren't the ones node {:?} closed ({why}). \
                 {unchanged}",
                p.node
            ))
        })?;
    }
    for q in m.partitions.iter_mut().filter(|q| q.id == partition) {
        q.node = to.to_owned();
        q.epoch = p.epoch + 1;
    }
    m.map_version += 1;
    // Last, just before the map: OLD still holds P closed with exactly the files the copy
    // was checked against. A reopen rewrites them (FORMAT §10.1, §10.4), so an unfence and
    // a fence again on OLD since the check, with writes acknowledged between, is caught
    // here, not served from the copy that lacks them (audit120's P2-C3).
    if let Some(expected) = closed.as_ref().or(checked.as_ref()) {
        let unchanged = format!(
            "The map is unchanged: it still gives {partition} to node {:?} at epoch {}",
            p.node, p.epoch
        );
        let now = {
            let (mut c, token) = connect(args, &old_url).await?;
            c.status(request(StatusRequest {}, token.as_deref())?)
                .await
                .map_err(|s| Failure::usage(&format!("{old_url}: {}", s.message())))?
                .into_inner()
                .partitions
                .into_iter()
                .find(|lp| lp.id == partition)
        };
        let still = now.as_ref().is_some_and(|lp| {
            lp.state() == S::Closed && super::moving::compare(&lp.files, expected).is_ok()
        });
        if !still {
            return Err(Failure::usage(&format!(
                "partition {partition} on node {:?} isn't closed with the files the copy on \
                 node {to:?} was checked against any more ({}): it was reopened since. \
                 {unchanged}. Run phdb cluster move {partition} --to {to} again, which copies \
                 its closed files again",
                p.node,
                now.as_ref()
                    .map_or_else(|| "not listed".to_owned(), state_text)
            )));
        }
    }
    let copies = if copy {
        vec![partition.to_owned()]
    } else {
        Vec::new()
    };
    let set = set_map(args, m, false, copies).await?;
    let mut r = report(
        "cluster relocate",
        vec![
            ("partition", json!(partition)),
            ("from", json!(p.node)),
            ("node", json!(to)),
            ("epoch", json!(p.epoch + 1)),
            ("copied", json!(copy)),
            ("installed_files", json!(stamped.files.len())),
            (
                "map_version",
                set.document
                    .get("map_version")
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
        ],
        Value::Null,
    );
    r.document.insert(
        "installed".into(),
        serde_json::to_value(&stamped.files).expect("files serialize"),
    );
    Ok(r)
}

fn runtime() -> Result<tokio::runtime::Runtime, Failure> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| Failure::io("start the runtime", &e))
}

pub(super) fn report(
    command: &'static str,
    summary: Vec<(&'static str, Value)>,
    extra: Value,
) -> Report {
    let mut document: serde_json::Map<String, Value> = summary
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect();
    if let Value::Object(o) = extra {
        document.extend(o);
    }
    Report {
        command,
        summary,
        columns: Vec::<Column>::new(),
        rows: Vec::new(),
        document,
        status: ExitStatus::SUCCESS,
    }
}

fn init_partition(
    cli: &Cli,
    file: &Path,
    cluster_id: &str,
    partition: &str,
    epoch: u64,
) -> Result<Report, Failure> {
    if !map::id_ok(cluster_id) || !map::id_ok(partition) {
        return Err(Failure::usage(
            "--cluster-id and --partition are 1-64 of [a-z0-9_-], not starting with -",
        ));
    }
    if epoch == 0 {
        return Err(Failure::usage("--epoch is at least 1"));
    }
    if file.file_stem().and_then(|s| s.to_str()) != Some(partition)
        || file.extension().and_then(|s| s.to_str()) != Some("phdb")
    {
        return Err(Failure::usage(&format!(
            "partition {partition}'s file is named {partition}.phdb in the node's cluster directory"
        )));
    }
    let dir = match file.parent() {
        Some(d) if !d.as_os_str().is_empty() => d,
        _ => Path::new("."),
    };
    let _lock = super::lock_dir(dir)?;
    let db = crate::open::for_write(cli, file, true)?;
    let written = identity::write(
        &db,
        &Identity {
            cluster_id: cluster_id.to_owned(),
            partition_id: partition.to_owned(),
            epoch,
        },
    )
    .map_err(|e| Failure::usage(&format!("{}: {e}", file.display())));
    let closed = db.close().map_err(|e| Failure::db("close", &e));
    written?;
    closed?;
    let summary = vec![
        ("path", json!(file.display().to_string())),
        ("cluster_id", json!(cluster_id)),
        ("partition", json!(partition)),
        ("epoch", json!(epoch)),
    ];
    Ok(report("cluster init-partition", summary, Value::Null))
}

pub(super) fn column(name: &'static str) -> Column {
    Column { name, brief: true }
}

pub(super) type Client = proto::cluster_client::ClusterClient<tonic::transport::Channel>;

/// A client for the node at `url`, and the token to present.
pub(super) async fn connect(
    args: &NodeArgs,
    url: &str,
) -> Result<(Client, Option<String>), Failure> {
    let (channel, token) = channel(args, url).await?;
    Ok((proto::cluster_client::ClusterClient::new(channel), token))
}

/// A channel to the node at `url`, and the token to present.
pub(super) async fn channel(
    args: &NodeArgs,
    url: &str,
) -> Result<(tonic::transport::Channel, Option<String>), Failure> {
    let token = match &args.token_file {
        Some(path) => Some(
            std::fs::read_to_string(path)
                .map_err(|e| Failure::io(&format!("read {}", path.display()), &e))?
                .trim()
                .to_owned(),
        ),
        None => std::env::var("PHDB_TOKEN").ok(),
    }
    .filter(|t| !t.is_empty());
    let mut endpoint = tonic::transport::Endpoint::from_shared(url.to_owned())
        .map_err(|e| Failure::usage(&format!("{url}: {e}")))?
        .connect_timeout(std::time::Duration::from_secs(10));
    if url.starts_with("https://") {
        let mut tls = tonic::transport::ClientTlsConfig::new().with_native_roots();
        if let Some(path) = &args.ca {
            let pem = std::fs::read(path)
                .map_err(|e| Failure::io(&format!("read {}", path.display()), &e))?;
            tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(pem));
        }
        endpoint = endpoint
            .tls_config(tls)
            .map_err(|e| Failure::usage(&format!("TLS to {url}: {e}")))?;
    } else if !url.starts_with("http://") {
        return Err(Failure::usage(&format!(
            "{url}: a node's URL is http://HOST:PORT or https://HOST:PORT"
        )));
    }
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| Failure::usage(&format!("connecting to {url}: {e}")))?;
    Ok((channel, token))
}

pub(super) fn request<T>(message: T, token: Option<&str>) -> Result<tonic::Request<T>, Failure> {
    let mut req = tonic::Request::new(message);
    if let Some(t) = token {
        req.metadata_mut().insert(
            "authorization",
            format!("Bearer {t}")
                .parse()
                .map_err(|_| Failure::usage("the token isn't a valid header value"))?,
        );
    }
    Ok(req)
}

/// A node's URL from its map address, in the scheme `--node` uses.
pub(super) fn url_of(args: &NodeArgs, address: &str) -> String {
    let scheme = if args.node.starts_with("https://") {
        "https"
    } else {
        "http"
    };
    format!("{scheme}://{address}")
}

pub(crate) async fn set_map(
    args: &NodeArgs,
    mut m: ClusterMap,
    force: bool,
    copies: Vec<String>,
) -> Result<Report, Failure> {
    // A copy is a planned move, through the writer; never part of a recovery.
    debug_assert!(
        !force || copies.is_empty(),
        "a forced map marks nothing copied"
    );
    let (mut c, token) = connect(args, &args.node).await?;
    let current = match c
        .get_map(request(GetMapRequest {}, token.as_deref())?)
        .await
    {
        Ok(r) => Some(r.into_inner()),
        Err(s) if s.code() == tonic::Code::NotFound => None,
        Err(s) => return Err(Failure::usage(&format!("{}: {}", args.node, s.message()))),
    };
    let mut at = args.node.clone();
    // The nodes a forced map's epochs weren't checked against: they couldn't be reached.
    let mut forced_unchecked: Option<Vec<String>> = None;
    let reply = if force {
        // Above every reachable node's version, and naming a writer that's reachable.
        let mut ids: Vec<(String, String)> = Vec::new();
        for n in current.iter().flat_map(|c| &c.nodes).chain(&m.nodes) {
            if !ids.iter().any(|(id, _)| *id == n.id) {
                ids.push((n.id.clone(), n.address.clone()));
            }
        }
        let mut top = current.as_ref().map_or(0, |c| c.map_version);
        let mut live = Vec::new();
        let mut unchecked = Vec::new();
        for (id, address) in &ids {
            let got = async {
                let (mut c, token) = connect(args, &url_of(args, address)).await?;
                let s = c
                    .status(request(StatusRequest {}, token.as_deref())?)
                    .await
                    .map_err(|s| Failure::usage(s.message()))?
                    .into_inner();
                let map = match c
                    .get_map(request(GetMapRequest {}, token.as_deref())?)
                    .await
                {
                    Ok(r) => Some(r.into_inner()),
                    Err(s) if s.code() == tonic::Code::NotFound => None,
                    Err(s) => return Err(Failure::usage(s.message())),
                };
                Ok((s, map))
            }
            .await;
            let Ok((s, map)) = got else {
                unchecked.push(id.clone());
                continue;
            };
            top = top.max(s.map_version);
            live.push(id.clone());
            // No partition's epoch below, or kept with another node than, one a reachable
            // node has: a forced map's histories are then new everywhere it reaches
            // (simp724's F2 on #1729).
            if let Some(theirs) = map {
                super::map::validate_epochs(&theirs, &m).map_err(|e| {
                    Failure::usage(&format!(
                        "node {id:?} has a newer placement than the forced map: {e}; give \
                         each moved partition an epoch above every node's"
                    ))
                })?;
            }
        }
        let w = super::schema::writer(&m).to_owned();
        if !live.contains(&w) {
            return Err(Failure::usage(&format!(
                "the map's writer would be node {w:?} (the smallest id), which can't be \
                 reached: a forced map must name a writer that's up (remove the down node, \
                 or give the writer's role to another id)"
            )));
        }
        m.map_version = m.map_version.max(top + 1);
        forced_unchecked = Some(unchecked);
        c.set_map(request(
            SetMapRequest {
                copies: Vec::new(),
                expected_map_version: 0,
                map: Some(m),
                force: true,
            },
            token.as_deref(),
        )?)
        .await
        .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))?
        .into_inner()
    } else {
        // Compare-and-set against the current version (0 before a first map), at the map's
        // writer (D317 amendment 1).
        let expected = current.as_ref().map_or(0, |c| c.map_version);
        if let Some(cur) = &current {
            let w = super::schema::writer(cur);
            let address = cur
                .nodes
                .iter()
                .find(|n| n.id == w)
                .map(|n| n.address.clone())
                .unwrap_or_default();
            at = url_of(args, &address);
            if at != args.node {
                c = connect(args, &at).await?.0;
            }
        }
        c.set_map(request(
            SetMapRequest {
                copies,
                expected_map_version: expected,
                map: Some(m),
                force: false,
            },
            token.as_deref(),
        )?)
        .await
        .map_err(|s| Failure::usage(&format!("{at}: {}", s.message())))?
        .into_inner()
    };
    let mut summary = vec![
        ("node", json!(at)),
        ("map_version", json!(reply.map_version)),
        (
            "pushed",
            json!(reply.pushed.iter().filter(|p| p.ok).count()),
        ),
        (
            "missed",
            json!(reply.pushed.iter().filter(|p| !p.ok).count()),
        ),
    ];
    if let Some(u) = &forced_unchecked {
        summary.push(("unchecked", json!(u.join(","))));
    }
    let mut r = report("cluster set-map", summary, Value::Null);
    if let Some(u) = forced_unchecked {
        r.document.insert("unchecked".into(), json!(u));
    }
    r.columns = ["node", "ok", "error"].into_iter().map(column).collect();
    r.rows = reply
        .pushed
        .iter()
        .map(|p| vec![json!(p.node), json!(p.ok), json!(p.error)])
        .collect();
    r.document.insert(
        "pushed".into(),
        serde_json::to_value(&reply.pushed).expect("NodePush serializes"),
    );
    Ok(r)
}

pub(super) async fn status(args: &NodeArgs) -> Result<Report, Failure> {
    let (mut c, token) = connect(args, &args.node).await?;
    let m = c
        .get_map(request(GetMapRequest {}, token.as_deref())?)
        .await
        .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))?
        .into_inner();
    let scheme = if args.node.starts_with("https://") {
        "https"
    } else {
        "http"
    };
    let mut rows = Vec::new();
    let mut nodes = Vec::new();
    let mut behind = 0usize;
    for n in &m.nodes {
        let url = format!("{scheme}://{}", n.address);
        let got = async {
            let (mut c, token) = connect(args, &url).await?;
            c.status(request(StatusRequest {}, token.as_deref())?)
                .await
                .map(tonic::Response::into_inner)
                .map_err(|s| Failure::usage(s.message()))
        }
        .await;
        match got {
            Ok(s) => {
                if s.map_version != m.map_version {
                    behind += 1;
                }
                let ok = s.partitions.iter().filter(|p| p.ok).count();
                let node_row = |partition: &str, state: String, error: &str| {
                    vec![
                        json!(n.id),
                        json!(n.address),
                        json!(s.map_version),
                        json!(partition),
                        json!(state),
                        json!(format!("{ok}/{}", s.partitions.len())),
                        json!(s.divergent.join(",")),
                        json!(error),
                    ]
                };
                // A row per partition, with its state on the node (D317 ticket 6); a node
                // that holds none has one row.
                if s.partitions.is_empty() {
                    rows.push(node_row("", String::new(), ""));
                }
                for lp in &s.partitions {
                    rows.push(node_row(&lp.id, state_text(lp), &lp.error));
                }
                nodes.push(serde_json::to_value(&s).expect("NodeStatus serializes"));
            }
            Err(e) => {
                behind += 1;
                rows.push(vec![
                    json!(n.id),
                    json!(n.address),
                    Value::Null,
                    json!(""),
                    json!(""),
                    Value::Null,
                    json!(""),
                    json!(e.message()),
                ]);
                nodes.push(json!({ "node_id": n.id, "error": e.message() }));
            }
        }
    }
    let summary = vec![
        ("cluster_id", json!(m.cluster_id)),
        ("map_version", json!(m.map_version)),
        ("nodes", json!(m.nodes.len())),
        ("not_current", json!(behind)),
    ];
    let mut r = report("cluster status", summary, json!({ "nodes": nodes }));
    r.columns = [
        "node",
        "address",
        "map_version",
        "partition",
        "state",
        "partitions_ok",
        "divergent",
        "error",
    ]
    .into_iter()
    .map(column)
    .collect();
    r.rows = rows;
    Ok(r)
}

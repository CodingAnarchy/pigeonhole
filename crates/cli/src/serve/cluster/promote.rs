//! `phdb cluster promote P --to ID` (D328 PR 3): make the node holding one of P's replica
//! copies P's owner. The copy is closed (a map without ID in P's replicas), promoted to a
//! primary at the last commit it applied and stamped at the map's epoch + 1 on ID, then the
//! map gives ID the partition at that epoch, with a new history. The old owner must be
//! fenced or closed, or unreachable (`--old-unreachable`), and D326's leases keep it from
//! serving once the new owner does. Run again, it finishes a promote that stopped.

use std::time::Duration;

use serde_json::{Value, json};

use super::cli::{NodeArgs, connect, report, request, set_map, url_of};
use crate::exit::Failure;
use crate::output::Report;
use crate::serve::proto::{self, GetMapRequest, StatusRequest};

/// A test's fault seam: the next promote through this `--node` URL stops after its stamp,
/// before its map, as a crash there would leave it.
#[cfg(test)]
pub(crate) static STOP_AFTER_STAMP: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// How long the promote waits for ID to close its copy after the map dropped it.
const CLOSE_WAIT: Duration = Duration::from_secs(60);

/// ID's row for `partition` in its `Status`.
async fn row_on(
    args: &NodeArgs,
    url: &str,
    partition: &str,
) -> Result<Option<proto::LocalPartition>, Failure> {
    let (mut c, token) = connect(args, url).await?;
    c.status(request(StatusRequest {}, token.as_deref())?)
        .await
        .map(|r| {
            r.into_inner()
                .partitions
                .into_iter()
                .find(|lp| lp.id == partition)
        })
        .map_err(|s| Failure::usage(&format!("{url}: {}", s.message())))
}

/// Takes node `to` out of `partition`'s replicas in `m`, through the writer, and waits for
/// it to close its replica copy (D328): what a promotion does first, and what a move,
/// relocate or restore to a node holding P's replica does before its stamp (S4). Nothing if
/// `to` isn't one of P's replicas. Whether it was.
pub(crate) async fn leave_replicas(
    args: &NodeArgs,
    m: &proto::ClusterMap,
    partition: &str,
    to: &str,
) -> Result<bool, Failure> {
    let Some(p) = m.partitions.iter().find(|p| p.id == partition) else {
        return Ok(false);
    };
    if !p.replicas.iter().any(|r| r == to) {
        return Ok(false);
    }
    let Some(url) = m
        .nodes
        .iter()
        .find(|n| n.id == to)
        .map(|n| url_of(args, &n.address))
    else {
        return Err(Failure::usage(&format!("the map has no node {to:?}")));
    };
    let mut m1 = m.clone();
    m1.map_version += 1;
    for q in m1.partitions.iter_mut().filter(|q| q.id == partition) {
        q.replicas.retain(|r| r != to);
    }
    set_map(args, m1, false, Vec::new()).await?;
    let deadline = tokio::time::Instant::now() + CLOSE_WAIT;
    loop {
        let now = row_on(args, &url, partition).await?;
        if now.is_none_or(|lp| lp.role() != proto::PartitionRole::Replica) {
            return Ok(true);
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Failure::usage(&format!(
                "node {to:?} hasn't closed its replica copy of {partition} within {}s of the map \
                 that took it out of {partition}'s replicas: run this again",
                CLOSE_WAIT.as_secs()
            )));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The map as `--node` has it.
async fn map_of(args: &NodeArgs) -> Result<proto::ClusterMap, Failure> {
    let (mut c, token) = connect(args, &args.node).await?;
    Ok(c.get_map(request(GetMapRequest {}, token.as_deref())?)
        .await
        .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))?
        .into_inner())
}

/// `phdb cluster promote`.
pub(crate) async fn promote(
    args: &NodeArgs,
    partition: &str,
    to: &str,
    old_unreachable: bool,
) -> Result<Report, Failure> {
    let m = map_of(args).await?;
    let Some(p) = m.partitions.iter().find(|p| p.id == partition).cloned() else {
        return Err(Failure::usage(&format!(
            "the map (version {}) has no partition {partition:?}",
            m.map_version
        )));
    };
    let url = |id: &str| {
        m.nodes
            .iter()
            .find(|n| n.id == id)
            .map(|n| url_of(args, &n.address))
    };
    let Some(new_url) = url(to) else {
        return Err(Failure::usage(&format!("the map has no node {to:?}")));
    };
    if p.node == to {
        let mut r = report(
            "cluster promote",
            vec![
                ("partition", json!(partition)),
                ("node", json!(to)),
                ("epoch", json!(p.epoch)),
                ("done", json!("the map already gives it this node")),
            ],
            Value::Null,
        );
        r.status = crate::ExitStatus::SUCCESS;
        return Ok(r);
    }
    let old_url = url(&p.node).unwrap_or_default();
    // The owner: fenced or closed, or unreachable (relocate's check, D317's S7 and S3).
    use proto::LocalPartitionState as S;
    let owner = if old_unreachable {
        if let Ok(Ok(Some(lp))) =
            tokio::time::timeout(Duration::from_secs(3), row_on(args, &old_url, partition)).await
            && lp.state() == S::Serving
        {
            return Err(Failure::usage(&format!(
                "--old-unreachable, but node {:?} answers and serves partition {partition}: \
                 fence it there first (phdb cluster fence), or stop its process",
                p.node
            )));
        }
        None
    } else {
        let lp = row_on(args, &old_url, partition).await?;
        if !lp
            .as_ref()
            .is_some_and(|lp| matches!(lp.state(), S::Fenced | S::Closed))
        {
            return Err(Failure::usage(&format!(
                "partition {partition} isn't fenced or closed on its owner, node {:?}: fence it \
                 there first (phdb cluster fence), or pass --old-unreachable if that node's \
                 process is stopped",
                p.node
            )));
        }
        lp
    };
    // ID's copy: open and following (its row a replica's), or closed by an earlier run (its
    // file's own row), or stamped by one (at the map's epoch + 1).
    let row = row_on(args, &new_url, partition).await?.ok_or_else(|| {
        Failure::usage(&format!(
            "node {to:?} has no copy of partition {partition}: add it to {partition}'s \
             replicas (phdb cluster set-map) and let it follow first"
        ))
    })?;
    let replica = row.role() == proto::PartitionRole::Replica;
    if replica && !p.replicas.iter().any(|r| r == to) {
        return Err(Failure::usage(&format!(
            "node {to:?} isn't in {partition}'s replicas in map version {}",
            m.map_version
        )));
    }
    let stamped = !replica && row.epoch == p.epoch + 1;
    // The copy's identity epoch, the owner's at its bootstrap (`e_b`, D328's amendment): its
    // history began at or before it, and the map's epoch is at or past it.
    let e_b = row.epoch;
    if !stamped && !(p.history_epoch <= e_b && e_b <= p.epoch) {
        return Err(Failure::usage(&format!(
            "node {to:?}'s copy of {partition} is at epoch {e_b}, outside its history \
             ({} to the map's {}): it's from before a relocate without copy or a restore, so it \
             bootstraps again first; promote it once it follows",
            p.history_epoch, p.epoch
        )));
    }
    let (applied, primary) = (row.replica_applied_seqno, row.replica_primary_seqno);
    // 1. ID leaves P's replicas, so it closes its copy (D328 PR 1's sync).
    if replica {
        leave_replicas(args, &m, partition, to).await?;
    }
    // 2. The copy, promoted and stamped at the map's epoch + 1.
    if !stamped {
        let (mut c, token) = connect(args, &new_url).await?;
        c.stamp(request(
            proto::StampRequest {
                cluster_id: m.cluster_id.clone(),
                partition: partition.to_owned(),
                from_epoch: p.epoch,
                to_epoch: p.epoch + 1,
                restore: false,
                restore_from_cluster_id: String::new(),
                promote: true,
                replica_epoch: e_b,
            },
            token.as_deref(),
        )?)
        .await
        .map_err(|s| Failure::usage(&format!("{new_url}: stamp: {}", s.message())))?;
    }
    #[cfg(test)]
    {
        let mut armed = STOP_AFTER_STAMP
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if armed.as_deref() == Some(args.node.as_str()) {
            armed.take();
            return Err(Failure::usage("the test's fault after the promote's stamp"));
        }
    }
    // 3. The map: P on ID at the next epoch, with a new history (no `copies`), from the map
    // as it is now (step 1 changed it).
    let mut m2 = map_of(args).await?;
    m2.map_version += 1;
    for q in m2.partitions.iter_mut().filter(|q| q.id == partition) {
        q.node = to.to_owned();
        q.epoch = p.epoch + 1;
        q.replicas.retain(|r| r != to);
    }
    let set = set_map(args, m2, false, Vec::new()).await?;
    let lost = if old_unreachable {
        json!(
            "unknown: the old owner couldn't be asked, so commits it acknowledged after the \
             copy's last one may be lost, however many"
        )
    } else if replica {
        json!(primary.saturating_sub(applied))
    } else {
        Value::Null
    };
    let mut r = report(
        "cluster promote",
        vec![
            ("partition", json!(partition)),
            ("from", json!(p.node)),
            ("node", json!(to)),
            ("epoch", json!(p.epoch + 1)),
            ("applied_seqno", json!(applied)),
            ("owner_durable_seqno", json!(primary)),
            ("lost_commits", lost),
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
    if owner.is_some_and(|lp| lp.state() == S::Fenced) {
        r.document.insert(
            "old_owner".into(),
            json!(
                "still holds its files, fenced: drop them there once checked (phdb cluster \
                 drop-partition)"
            ),
        );
    }
    Ok(r)
}

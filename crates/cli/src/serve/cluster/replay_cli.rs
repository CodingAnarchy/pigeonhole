//! `phdb cluster replay-tail P` (D336 PR 3a, §6): replay a partition's old owner's tail into
//! its owner, or give it up (`--discard`).
//!
//! - **Which tail:** P's oldest PENDING failover event, or the one `--event` names by the
//!   epoch it gave P (`to_epoch`, as `phdb cluster status` lists it).
//! - **The replay** runs on P's owner (`Cluster.ReplayTail`), which reads the old node's file
//!   (`Cluster.ReadTail`). The command waits up to `--timeout` and reports the owner's
//!   progress; the replay goes on past it, and a rerun joins or resumes it.
//! - **DONE** is set through the map's writer once the owner reports the replay finished
//!   (C4): the writer asks the owner again before it takes the map. A dry run, a run that
//!   stopped short (`last_error`, an `in_doubt` share among them) or one still running
//!   leaves the tail PENDING.
//! - **`--discard`** sets DISCARDED with no replay: the old node may then drop its file.

use std::time::Duration;

use serde_json::json;

use super::cli::{NodeArgs, connect, report, request};
use crate::exit::Failure;
use crate::output::Report;
use crate::serve::proto::{self, GetMapRequest, TailState};

/// The command's wait for the owner, without `--timeout`.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// `phdb cluster replay-tail`.
pub(crate) async fn run(
    args: &NodeArgs,
    partition: &str,
    event: Option<u64>,
    dry_run: bool,
    discard: bool,
    timeout: Option<Duration>,
) -> Result<Report, Failure> {
    let (mut c, token) = connect(args, &args.node).await?;
    let map = c
        .get_map(request(GetMapRequest {}, token.as_deref())?)
        .await
        .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))?
        .into_inner();
    let e = pick(&map, partition, event)?;
    if discard {
        let mut m = map.clone();
        set_tail(&mut m, &e, TailState::Discarded, 0);
        let set = super::cli::set_map(args, m, false, Vec::new()).await?;
        return Ok(report(
            "cluster replay-tail",
            vec![
                ("partition", json!(partition)),
                ("event", json!(e.to_epoch)),
                ("tail_state", json!("DISCARDED")),
            ],
            json!({ "set_map": serde_json::Value::Object(set.document) }),
        ));
    }
    let owner = map
        .partitions
        .iter()
        .find(|p| p.id == partition)
        .and_then(|p| map.nodes.iter().find(|n| n.id == p.node))
        .ok_or_else(|| {
            Failure::usage(&format!(
                "the map places partition {partition:?} on no node"
            ))
        })?;
    let wait = timeout.unwrap_or(DEFAULT_TIMEOUT);
    let (mut oc, otoken) = connect(args, &super::cli::url_of(args, &owner.address)).await?;
    let lp = oc
        .replay_tail(request(
            proto::ReplayTailRequest {
                partition: partition.to_owned(),
                from_history_epoch: e.from_history_epoch,
                from_epoch: e.from_epoch,
                dry_run,
                timeout_ms: u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
            },
            otoken.as_deref(),
        )?)
        .await
        .map_err(|s| Failure::usage(&format!("{}: {}", owner.id, s.message())))?
        .into_inner();
    let progress = lp.replay.clone().unwrap_or_default();
    let finished = !progress.running
        && progress.last_error.is_empty()
        && progress.source_end > 0
        && progress.mark >= progress.source_end;
    let mut state = "PENDING";
    let mut set = serde_json::Value::Null;
    if finished && !dry_run {
        // The map as it is now: the replay may have taken a while.
        let (mut c, token) = connect(args, &args.node).await?;
        let mut m = c
            .get_map(request(GetMapRequest {}, token.as_deref())?)
            .await
            .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))?
            .into_inner();
        set_tail(&mut m, &e, TailState::Done, progress.mark);
        set = serde_json::Value::Object(
            super::cli::set_map(args, m, false, Vec::new())
                .await?
                .document,
        );
        state = "DONE";
    }
    let note = if !progress.last_error.is_empty() {
        format!(
            "the replay stopped: {}. The tail stays PENDING: fix the cause and run replay-tail \
             again, which resumes from its mark, or give the tail up with replay-tail \
             {partition} --discard",
            progress.last_error
        )
    } else if progress.running {
        "the replay goes on past --timeout: run replay-tail again to wait for it and set the \
         tail DONE"
            .to_owned()
    } else {
        String::new()
    };
    let summary = vec![
        ("partition", json!(partition)),
        ("event", json!(e.to_epoch)),
        ("old_node", json!(e.from)),
        ("dry_run", json!(dry_run)),
        ("applied", json!(progress.applied)),
        ("skipped", json!(progress.skipped)),
        ("refused", json!(progress.refused)),
        ("mark", json!(progress.mark)),
        ("source_end", json!(progress.source_end)),
        ("running", json!(progress.running)),
        ("tail_state", json!(state)),
        ("note", json!(note)),
    ];
    let out = report(
        "cluster replay-tail",
        summary,
        json!({
            "replay": serde_json::to_value(&progress).expect("ReplayProgress serializes"),
            "set_map": set,
        }),
    );
    Ok(out)
}

/// P's PENDING event: the one at `to_epoch`, or the oldest.
fn pick(
    map: &proto::ClusterMap,
    partition: &str,
    to_epoch: Option<u64>,
) -> Result<proto::FailoverEvent, Failure> {
    map.failovers
        .iter()
        .filter(|e| e.partition == partition && e.tail_state() == TailState::Pending)
        .find(|e| to_epoch.is_none_or(|t| e.to_epoch == t))
        .cloned()
        .ok_or_else(|| {
            Failure::usage(&match to_epoch {
                Some(t) => format!(
                    "partition {partition:?} has no PENDING old owner's tail from its failover \
                     at epoch {t}"
                ),
                None => format!("partition {partition:?} has no PENDING old owner's tail"),
            })
        })
}

/// `m`, a version on, with `e`'s tail set to `state`, its fork token cleared, and
/// `replayed_through`.
fn set_tail(m: &mut proto::ClusterMap, e: &proto::FailoverEvent, state: TailState, through: u64) {
    m.map_version += 1;
    for f in m.failovers.iter_mut().filter(|f| {
        f.kind == e.kind && f.partition == e.partition && f.to == e.to && f.to_epoch == e.to_epoch
    }) {
        f.set_tail_state(state);
        f.fork_token = Default::default();
        f.replayed_through = through;
    }
}

//! D331 PR 6: `phdb cluster merge S --into T` (`.coord/cli692-d331-pr6-design.md`, with its
//! revision 2). A merge is ordinary range moves, one per range S holds, each into T and
//! each a RANGE record of its own (the coordinator's ruling: no merge record), and then
//! S's removal. Every step reads the map afresh, so a half-done merge is a valid map, and
//! a rerun goes on from it: it finishes a running record of S into T, moves what S still
//! holds, then removes S.
//!
//! **An emptied S stays listed** until its node's `LocalPartition.drop_waits` is empty: it
//! serves its feed tail from its moved-away rows (pruned at its own feed retention) and its
//! transaction outcomes, under failover's protection, and nothing new can need it (it owns
//! no key, and a commit point on it is refused). It goes once nothing does, and once no node
//! holds an in-doubt share homed at S (simp724's M1):
//! 0. the map m0 at v0, naming S's owner N0 and epoch;
//! 1. N0's Status: S its owner copy, serving at that epoch, no waits; every node's: no share
//!    homed at S. S fenced already stops the merge, naming the fence (audit120's M2);
//! 2. fence S on N0 (a failure stops, naming the fence: S4);
//! 3. the same reads again (the race with the first);
//! 4. one map change, compare-and-set on v0, removes S and its replicas: a failover that
//!    moved S meanwhile fails it, and the merge starts again (M1), at most [`RESTARTS`] times;
//! 5. close S on N0, drop it there, then on each of m0's replica nodes (a replica still open
//!    there is retried: audit120's S2).
//!
//! A stop after step 4 leaves S's files where they were, unlisted: a rerun finds them
//! through every node's Status (M3), only the files whose identity names this cluster and S
//! (Status's "the map has no such partition"), and drops each, naming it first (R2).

use std::collections::HashMap;
use std::time::Duration;

use super::cli::NodeArgs;
use super::map;
use super::moving::{At, Start, map_now, set_record};
use super::range_move;
use crate::exit::Failure;
use crate::output::Report;
use crate::serve::proto::{
    ClusterMap, FenceReason, LocalPartition, LocalPartitionState, MovePhase, NodeStatus,
    PartitionRole,
};

/// How often the removal starts again when the map moved under it, before giving up.
const RESTARTS: usize = 5;

/// How long a former replica node may take to close its copy before its drop gives up.
const REPLICA_CLOSE: Duration = Duration::from_secs(10);

/// What Status says of a file whose identity names this cluster and a partition the map no
/// longer lists (status.rs): the only files step 6 drops.
const UNLISTED: &str = "the map has no such partition";

/// `phdb cluster merge S --into T`: [`run_as`] with the CLI's records (`range-<ms>`).
/// `fit: false` skips the plan's fit check (`--skip-fit-check`).
pub(super) async fn run(
    args: &NodeArgs,
    from: &str,
    into: &str,
    timeout: Option<Duration>,
    fit: bool,
) -> Result<Report, Failure> {
    run_as(
        args,
        from,
        into,
        timeout,
        fit,
        &Start {
            prefix: "range",
            ..Start::default()
        },
    )
    .await
}

/// How a merge ended: S removed, or S emptied and waiting for what still needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Ended {
    /// S is out of the map; `dropped` names each file moved aside, `node: file`, and
    /// `left` each node or file the removal couldn't finish (a rerun goes on with them).
    Removed {
        dropped: Vec<String>,
        left: Vec<String>,
    },
    /// S holds no range but stays listed: why (`drop_waits`, or a share homed at it).
    Waiting(String),
}

/// A merge of `from` into `into`, each range move started as `start` says.
pub(super) async fn run_as(
    args: &NodeArgs,
    from: &str,
    into: &str,
    timeout: Option<Duration>,
    fit: bool,
    start: &Start<'_>,
) -> Result<Report, Failure> {
    let mut moved = 0usize;
    let mut planned = false;
    let ended = loop {
        let m = map_now(args).await?;
        if !m.partitions.iter().any(|p| p.id == from) {
            let (dropped, left) = unlisted_drops(args, &m, from).await?;
            break Ended::Removed { dropped, left };
        }
        check(&m, from, into)?;
        if let Some(r) = m
            .moves
            .iter()
            .find(|r| [&r.from, &r.to].iter().any(|p| *p == from || *p == into))
        {
            let ours =
                !map::node_move(r) && !range_move::is_split(r) && r.from == from && r.to == into;
            if !ours {
                return Err(range_move::in_flight(r, "merge"));
            }
            if map::phase(r) == MovePhase::Aborting {
                range_move::abort(args, &r.id, timeout).await?;
            } else {
                range_move::resume(args, &r.id, timeout).await?;
                moved += 1;
            }
            continue;
        }
        let Some((table, lo, hi)) = next_range(&m, from) else {
            break remove(args, from).await?;
        };
        if !planned {
            plan(args, &m, from, fit).await?;
            planned = true;
        }
        let spec = range_move::Spec {
            table,
            from: from.to_owned(),
            to: into.to_owned(),
            start: Some(lo),
            end: hi,
        };
        range_move::run_as(args, &spec, timeout, start).await?;
        moved += 1;
    };
    let (state, detail) = match &ended {
        Ended::Removed { dropped, left } if left.is_empty() => ("removed", dropped.join(", ")),
        Ended::Removed { dropped, left } => (
            "removed",
            format!(
                "{}{}not done: {}",
                dropped.join(", "),
                if dropped.is_empty() { "" } else { "; " },
                left.join("; ")
            ),
        ),
        Ended::Waiting(why) => {
            eprintln!(
                "phdb cluster merge: {from} holds no range now; its removal waits for {why}: \
                 run phdb cluster merge again later"
            );
            ("waiting", why.clone())
        }
    };
    Ok(super::cli::report(
        "cluster merge",
        vec![
            ("from", from.into()),
            ("into", into.into()),
            ("ranges_moved", moved.into()),
            ("source", state.into()),
            ("detail", detail.into()),
        ],
        serde_json::json!({}),
    ))
}

/// D331's plan for the merge, once, before its first range: S's whole file is an exact
/// bound for a merge's total (ai1249). As `move-range`'s: `Busy` refuses (retry-safe), a fit
/// refusal unless `fit` is off (`--skip-fit-check`), and an unknown source refuses.
async fn plan(args: &NodeArgs, m: &ClusterMap, from: &str, fit: bool) -> Result<(), Failure> {
    let (node, address) = range_move::node_of(m, from)?;
    let at = At::new(args, node, address).await?;
    // As `move-range`'s: S's write rate is reported once its first load interval ends (30 s
    // at most, from its open), so wait for it rather than refuse a merge just after a start.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(35);
    let st = loop {
        let st = at.status().await?;
        let rated = st
            .partitions
            .iter()
            .any(|lp| lp.id == from && lp.write_bytes_rate.is_some());
        if rated || tokio::time::Instant::now() >= deadline {
            break st;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let statuses = [(node.to_owned(), st)].into();
    match range_move::plan(m, &statuses, from, None) {
        Ok(_) => Ok(()),
        Err(range_move::Skip::Busy(why)) => Err(Failure::coded(
            pigeonhole::ErrorCode::Busy,
            format!("{why}: run the merge again once one finishes"),
        )),
        Err(range_move::Skip::Fit(_)) if !fit => Ok(()),
        Err(range_move::Skip::Fit(why)) => Err(Failure::usage(&format!(
            "{why}: merge with --skip-fit-check to start anyway"
        ))),
        Err(range_move::Skip::Unknown(why)) => Err(Failure::usage(&why)),
    }
}

/// What a merge refuses before it moves anything: the partitions, every table of S's (no
/// co-location group, no index following it), and a placement range moves can't take (the
/// memory store, an index on S).
fn check(m: &ClusterMap, from: &str, into: &str) -> Result<(), Failure> {
    for p in [from, into] {
        if !m.partitions.iter().any(|q| q.id == p) {
            return Err(Failure::usage(&format!("the map has no partition {p:?}")));
        }
    }
    if from == into {
        return Err(Failure::usage("a partition merges into another"));
    }
    if m.memory
        .as_ref()
        .is_some_and(|s| s.partitions.iter().any(|q| q == from))
    {
        return Err(Failure::usage(&format!(
            "partition {from:?} holds the agent memory store's placement, which range moves \
             don't take: move the whole partition instead (phdb cluster move)"
        )));
    }
    if let Some(i) = m
        .indexes
        .iter()
        .find(|i| i.partitions.iter().any(|q| q == from))
    {
        return Err(Failure::usage(&format!(
            "partition {from:?} holds a placement of index {:?}, which range moves don't take",
            i.name
        )));
    }
    for t in m
        .tables
        .iter()
        .filter(|t| t.partitions.iter().any(|q| q == from))
    {
        range_move::check_table(m, &t.name)?;
    }
    Ok(())
}

/// The first range `from` holds, in table then key order: `(table, start, end)`.
fn next_range(m: &ClusterMap, from: &str) -> Option<(String, Vec<u8>, Option<Vec<u8>>)> {
    m.tables.iter().find_map(|t| {
        map::table_ranges(t)
            .into_iter()
            .find(|(_, _, p)| *p == from)
            .map(|(lo, hi, _)| (t.name.clone(), lo, hi))
    })
}

/// The removal of an emptied `s` (the module docs' steps 0-5): `Waiting` while something
/// still needs it.
async fn remove(args: &NodeArgs, s: &str) -> Result<Ended, Failure> {
    let mut moved_by = Vec::new();
    for _ in 0..RESTARTS {
        // 0.
        let m0 = map_now(args).await?;
        let v0 = m0.map_version;
        let Some(p) = m0.partitions.iter().find(|p| p.id == s).cloned() else {
            let (dropped, left) = unlisted_drops(args, &m0, s).await?;
            return Ok(Ended::Removed { dropped, left });
        };
        if map::placed(&m0, s) || m0.moves.iter().any(|r| r.from == s || r.to == s) {
            return Err(Failure::usage(&format!(
                "partition {s:?} holds a placement or a move names it again: run merge again"
            )));
        }
        let (node, address) = range_move::node_of(&m0, s)?;
        let at = At::new(args, node, address).await?;
        // 1.
        let lp = owner_copy(&at, s, node, address, p.epoch, v0).await?;
        if let Some(why) = waits(&lp, args, &m0, s).await? {
            return Ok(Ended::Waiting(why));
        }
        // 2.
        if let Err(f) = at.call(s, "fence").await {
            return Err(fence_left(s, node, address, f.message()));
        }
        // 3. Its Status from a map at v0 or later: an older one may not compute
        // `drop_waits` (audit120's S-c). A failed read leaves the fence named (S-b).
        let (at_version, lp) = local_at(&at, s)
            .await
            .map_err(|f| fence_left(s, node, address, f.message()))?;
        let again = match lp.as_ref() {
            _ if at_version < v0 => Some(format!(
                "node {node} to hold map version {v0} (it holds {at_version})"
            )),
            Some(lp) if lp.epoch == p.epoch && lp.role() == PartitionRole::Owner => {
                waits(lp, args, &m0, s).await?
            }
            _ => Some(format!("{s}'s copy on node {node} changed under the merge")),
        };
        if let Some(why) = again {
            unfence_or_stop(&at, s, node, address).await?;
            return Ok(Ended::Waiting(why));
        }
        #[cfg(test)]
        hold(&args.node).await;
        // 4.
        let moved = std::sync::atomic::AtomicBool::new(false);
        let set = set_record(args, |now: &ClusterMap| {
            if now.map_version != v0 {
                moved.store(true, std::sync::atomic::Ordering::Relaxed);
                return Err(Failure::usage("the map moved under the merge's removal"));
            }
            let mut next = now.clone();
            next.partitions.retain(|q| q.id != s);
            Ok(next)
        })
        .await;
        match set {
            Ok(()) => {}
            Err(_) if moved.load(std::sync::atomic::Ordering::Relaxed) => {
                let now = map_now(args)
                    .await
                    .map_err(|f| fence_left(s, node, address, f.message()))?;
                moved_by.push(now.map_version);
                if now
                    .partitions
                    .iter()
                    .any(|q| q.id == s && q.node == node && q.epoch == p.epoch)
                {
                    unfence_or_stop(&at, s, node, address).await?;
                }
                continue;
            }
            Err(f) => return Err(fence_left(s, node, address, f.message())),
        }
        eprintln!("phdb cluster merge: {s} left the map at version {}", v0 + 1);
        // 5.
        let (mut dropped, mut left) = (Vec::new(), Vec::new());
        at.call(s, "close").await?;
        if let Some(file) = drop_settled(&at, s).await? {
            dropped.push(format!("{node}: {file}"));
        }
        for r in &p.replicas {
            let Some(n) = m0.nodes.iter().find(|n| n.id == *r) else {
                continue;
            };
            let at = match At::new(args, r, &n.address).await {
                Ok(at) => at,
                Err(e) => {
                    left.push(format!(
                        "node {r} (a replica of {s}) didn't answer: {}; run merge again to \
                         drop its copy",
                        e.message()
                    ));
                    continue;
                }
            };
            match drop_settled(&at, s).await {
                Ok(Some(file)) => dropped.push(format!("{r}: {file}")),
                Ok(None) => {}
                Err(e) => left.push(format!(
                    "{s}'s replica copy on node {r}: {}; run merge again to drop it",
                    e.message()
                )),
            }
        }
        for l in &left {
            eprintln!("phdb cluster merge: {l}");
        }
        return Ok(Ended::Removed { dropped, left });
    }
    Err(Failure::usage(&format!(
        "the map kept changing under {s}'s removal (versions {moved_by:?}): run merge again"
    )))
}

/// A test's hold of the removal of the merge talking to node URL `.0`, once, between its
/// re-check (step 3) and its map change (step 4): `.1` is told it got there, and the
/// removal goes on once `.2` is (a hold seam, as `range_move::STOP` is a stop one).
#[cfg(test)]
pub(super) static HOLD: std::sync::Mutex<Vec<Hold>> = std::sync::Mutex::new(Vec::new());

/// One [`HOLD`]: the node URL, then the reached and go signals.
#[cfg(test)]
pub(super) type Hold = (String, Arc<Notify>, Arc<Notify>);

#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use tokio::sync::Notify;

#[cfg(test)]
async fn hold(node: &str) {
    let held = {
        let mut holds = HOLD.lock().unwrap();
        let i = holds.iter().position(|(url, _, _)| url == node);
        i.map(|i| holds.remove(i))
    };
    if let Some((_, reached, go)) = held {
        reached.notify_one();
        go.notified().await;
    }
}

/// The node's map version and its line for `s`, from one Status.
async fn local_at(at: &At, s: &str) -> Result<(u64, Option<LocalPartition>), Failure> {
    let st = at.status().await?;
    Ok((
        st.map_version,
        st.partitions.into_iter().find(|lp| lp.id == s),
    ))
}

/// Step 1's read of `s` on its owner `node`: its owner copy, serving with its lease at
/// `epoch` (audit120's M1), from a Status at map `v0` or later (an older map may not
/// compute `drop_waits`: audit120's S-c). An operator's fence already there stops the
/// merge, naming it (M2).
async fn owner_copy(
    at: &At,
    s: &str,
    node: &str,
    address: &str,
    epoch: u64,
    v0: u64,
) -> Result<LocalPartition, Failure> {
    let (at_version, lp) = local_at(at, s).await?;
    if at_version < v0 {
        return Err(Failure::usage(&format!(
            "node {node} holds map version {at_version}, behind the merge's {v0}: run merge \
             again once it has the map"
        )));
    }
    let lp =
        lp.ok_or_else(|| Failure::usage(&format!("node {node} doesn't list partition {s}")))?;
    // A lapsed lease or one still awaited is no operator's fence: the message below.
    if lp.state() == LocalPartitionState::Closed
        || (lp.state() == LocalPartitionState::Fenced && lp.fence_reason() == FenceReason::Operator)
    {
        return Err(fence_left(
            s,
            node,
            address,
            &format!("it's {:?} there", lp.state()),
        ));
    }
    if lp.role() != PartitionRole::Owner
        || lp.state() != LocalPartitionState::Serving
        || lp.epoch != epoch
        || lp.lease_remaining_ms == 0
    {
        return Err(Failure::usage(&format!(
            "partition {s} isn't serving as node {node}'s owner copy at epoch {epoch} (it's \
             {:?} at epoch {}, lease {} ms): run merge again once it is",
            lp.state(),
            lp.epoch,
            lp.lease_remaining_ms
        )));
    }
    Ok(lp)
}

/// What still needs `s` listed: its owner's `drop_waits`, then an in-doubt share homed at
/// `s` on any node of `m` (simp724's M1), a node that doesn't answer being one.
async fn waits(
    lp: &LocalPartition,
    args: &NodeArgs,
    m: &ClusterMap,
    s: &str,
) -> Result<Option<String>, Failure> {
    if !lp.drop_waits.is_empty() {
        return Ok(Some(lp.drop_waits.clone()));
    }
    let mut statuses: HashMap<String, NodeStatus> = HashMap::new();
    for n in &m.nodes {
        let st = match At::new(args, &n.id, &n.address).await {
            Ok(at) => at.status().await,
            Err(e) => Err(e),
        };
        match st {
            Ok(st) => {
                statuses.insert(n.id.clone(), st);
            }
            Err(e) => {
                return Ok(Some(format!(
                    "node {}'s status, for in-doubt shares homed at {s}: {}",
                    n.id,
                    e.message()
                )));
            }
        }
    }
    Ok(homed_share(&statuses, s))
}

/// An in-doubt share, on any node in `statuses`, of a spanning transaction homed at `s`.
fn homed_share(statuses: &HashMap<String, NodeStatus>, s: &str) -> Option<String> {
    let mut nodes: Vec<&String> = statuses.keys().collect();
    nodes.sort();
    for n in nodes {
        for lp in &statuses[n].partitions {
            if let Some(share) = lp.in_doubt_shares.iter().find(|sh| {
                crate::serve::sessions::spanning_parts(&sh.xid)
                    .is_some_and(|(home, _, _)| home == s)
            }) {
                return Some(format!(
                    "transaction {} homed at {s}, whose share on partition {} (node {n}) is in \
                     doubt: its resolver settles it through {s}",
                    share.xid, lp.id
                ));
            }
        }
    }
    None
}

/// Unfences `s` on its owner after the removal stopped; a failure stops the merge, naming
/// the fence (audit120's M2 and nit).
async fn unfence_or_stop(at: &At, s: &str, node: &str, address: &str) -> Result<(), Failure> {
    match at.call(s, "unfence").await {
        Ok(_) => Ok(()),
        Err(f) => Err(fence_left(s, node, address, f.message())),
    }
}

/// The stop that leaves `s` fenced on `node`: what it is and how to lift it (M2).
fn fence_left(s: &str, node: &str, address: &str, why: &str) -> Failure {
    Failure::usage(&format!(
        "partition {s} may be fenced on node {node} ({why}). Its transaction outcomes and \
         in-doubt shares can't settle under a fence, so the merge stops here: lift it with \
         phdb cluster unfence {s} --node {address}, then run merge again"
    ))
}

/// drop-partition on `at`, "has no files" counting as done: the file moved aside, if any.
async fn drop_at(at: &At, s: &str) -> Result<Option<String>, Failure> {
    match at.call(s, "drop").await {
        Ok(lp) => Ok(Some(lp.file)),
        Err(f) if f.message().contains("has no files of partition") => Ok(None),
        Err(f) => Err(f),
    }
}

/// [`drop_at`] on a node that may not have installed the map without `s` yet: its refusal
/// while it still places `s` (cli692's S1 on #2130), while it still lists itself among
/// `s`'s replicas (ai1249's S1 on #2131), or while a former replica's copy is still open
/// (audit120's S2), is retried for [`REPLICA_CLOSE`].
async fn drop_settled(at: &At, s: &str) -> Result<Option<String>, Failure> {
    let until = tokio::time::Instant::now() + REPLICA_CLOSE;
    loop {
        match drop_at(at, s).await {
            Err(f)
                if (f.message().contains("still open")
                    || f.message().contains("still places partition")
                    || f.message().contains("lists this node among"))
                    && tokio::time::Instant::now() < until =>
            {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            other => return other,
        }
    }
}

/// A rerun after `s` left the map (M3): every node's file of `s` whose identity names this
/// cluster and `s` (Status: exactly [`UNLISTED`], which a split-marked file never gets:
/// audit120's S-a), closed if it's open, then moved aside, each named first (R2). Refused
/// while a record names `s` (a split creating a partition of that name). Returns the files
/// moved aside, and each node or file left as it was, with why.
async fn unlisted_drops(
    args: &NodeArgs,
    m: &ClusterMap,
    s: &str,
) -> Result<(Vec<String>, Vec<String>), Failure> {
    if let Some(r) = m.moves.iter().find(|r| r.from == s || r.to == s) {
        return Err(range_move::in_flight(r, "merge"));
    }
    let (mut dropped, mut left) = (Vec::new(), Vec::new());
    for n in &m.nodes {
        let lp = match At::new(args, &n.id, &n.address).await {
            Ok(at) => at.local(s).await.map(|lp| (at, lp)),
            Err(e) => Err(e),
        };
        let (at, lp) = match lp {
            Ok((at, Some(lp))) => (at, lp),
            Ok((_, None)) => continue,
            Err(e) => {
                left.push(format!(
                    "node {} didn't answer: {}; run merge again for any file of {s} there",
                    n.id,
                    e.message()
                ));
                continue;
            }
        };
        if lp.error != UNLISTED {
            left.push(format!(
                "node {}: {} of {s} left as it is ({}): run merge again once it's closed and \
                 unlisted there",
                n.id,
                lp.file,
                if lp.error.is_empty() {
                    format!("{:?}, {:?}", lp.state(), lp.role())
                } else {
                    lp.error.clone()
                }
            ));
            continue;
        }
        eprintln!(
            "phdb cluster merge: node {}: {} (partition {s}, epoch {}) goes aside",
            n.id, lp.file, lp.epoch
        );
        if matches!(
            lp.state(),
            LocalPartitionState::Serving | LocalPartitionState::Fenced
        ) {
            if lp.state() == LocalPartitionState::Serving {
                at.call(s, "fence").await?;
            }
            at.call(s, "close").await?;
        }
        if let Some(file) = drop_settled(&at, s).await? {
            dropped.push(format!("{}: {file}", n.id));
        }
    }
    for l in &left {
        eprintln!("phdb cluster merge: {l}");
    }
    Ok((dropped, left))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::proto::InDoubtShare;

    fn node(parts: &[(&str, &[&str])]) -> NodeStatus {
        NodeStatus {
            partitions: parts
                .iter()
                .map(|(id, xids)| LocalPartition {
                    id: (*id).into(),
                    in_doubt_shares: xids
                        .iter()
                        .map(|x| InDoubtShare {
                            xid: (*x).into(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    const HEX: &str = "0123456789abcdef0123456789abcdef";

    /// simp724's M1: a share elsewhere whose transaction is homed at S holds S's removal,
    /// named with its node and partition; a share homed elsewhere, or an id that isn't a
    /// spanning one, doesn't.
    #[test]
    fn a_share_homed_at_s_on_any_node_holds_the_removal() {
        let homed = format!("p1:h3-{HEX}");
        let other = format!("p2:h3-{HEX}");
        let statuses: HashMap<String, NodeStatus> = [
            ("a".to_owned(), node(&[("p1", &[])])),
            (
                "b".to_owned(),
                node(&[("p2", &[other.as_str(), "p1:plain"])]),
            ),
        ]
        .into();
        assert_eq!(homed_share(&statuses, "p1"), None);
        let statuses: HashMap<String, NodeStatus> = [
            ("a".to_owned(), node(&[("p1", &[])])),
            ("c".to_owned(), node(&[("p3", &[homed.as_str()])])),
            (
                "b".to_owned(),
                node(&[("p2", &[other.as_str(), homed.as_str()])]),
            ),
        ]
        .into();
        let why = homed_share(&statuses, "p1").unwrap();
        // The first node by id names it, whatever the map's order.
        assert!(why.contains("partition p2 (node b)"), "{why}");
        assert!(why.contains(&homed), "{why}");
        assert_eq!(
            homed_share(&statuses, "p2"),
            Some(format!(
                "transaction {other} homed at p2, whose share on partition p2 (node b) is in \
             doubt: its resolver settles it through p2"
            ))
        );
    }
}

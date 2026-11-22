//! D331 PR 5: the range move's driver, `phdb cluster move-range` (the design note,
//! `.coord/ai1249-d331-pr5-design.md`). It moves the phases; what each phase runs on the
//! nodes is PR 2–4b's and this PR's node side (the fence's seqno, the drops):
//!
//! 0. **Plan:** the partitions, the range inside one of S's, no move naming either, no
//!    co-location group or following index.
//! 1. **COPYING:** one map change adds the record, with S's and T's epochs.
//! 2. **The copy:** until T reports it done.
//! 3. **CATCHING_UP:** until T's catch-up lags S by a second at most, and S holds no share
//!    in doubt (the partition's count, simp724's pre-check).
//! 4. **HANDING_OVER:** S fences the range; once it reports its fence's seqno (its drain
//!    and range-scoped in-doubt wait), the record takes it as `final_seqno`, and T applies
//!    through it. A fence that doesn't report within [`FENCE_BOUND`] aborts the move, so
//!    writes aren't held by a stuck share (simp724).
//! 5. **HANDED_OVER**, the commit point: the placement gives the range to T, and S's epoch
//!    rises past any promote's floor.
//! 6. **Never both** (D331): CLEANING only once S took that map (its node holds it, and S
//!    is at the raised epoch), or S's old lease has provably ended: a majority holds the map
//!    (whose nodes refuse S's renewals at its old epoch) and a majority of the nodes grant
//!    S's partition to S's node no more.
//! 7. **CLEANING:** T serves; S drops its copy and says so (`dropped`); the record goes.
//!
//! Before the commit point a move aborts on S's or T's epoch moving (a failover), T's
//! follower stopping, or S's hold lapsing: the record goes to ABORTING, T drops its copy
//! (its reconcile), and the record goes. Each step reads the record's phase, so a rerun goes
//! on from where the map is. Every map change is the writer's own (`set_map` in process
//! when run by the writer).

use std::time::Duration;

use super::cli::NodeArgs;
use super::map;
use super::moving::{At, Start, Waiting, map_now, set_record};
use crate::exit::Failure;
use crate::output::Report;
use crate::serve::proto::{ClusterMap, LocalPartition, MoveKind, MovePhase, MoveStatus, RangeMove};

/// The most a source's fence may take to report its seqno (its drain, and the shares in
/// doubt on the range) before the move aborts and lifts it (simp724's bounded wait).
pub(super) const FENCE_BOUND: Duration = Duration::from_secs(30);

/// The catch-up's lag at which the source fences the range: the fence stalls the range's
/// writes about this long while the target closes the gap (simp724's bound).
const CATCH_UP_LAG_MS: u64 = 1_000;

/// What to move: `[start, end)` of `table` from partition `from` to partition `to`.
#[derive(Debug, Clone)]
pub(super) struct Spec {
    pub table: String,
    pub from: String,
    pub to: String,
    /// The range's ends as the command gave them (`None`: the end of `from`'s range).
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
}

/// `phdb cluster move-range`, the CLI's (record ids `range-<ms>`).
pub(super) async fn run(
    args: &NodeArgs,
    spec: &Spec,
    timeout: Option<Duration>,
) -> Result<Report, Failure> {
    run_as(
        args,
        spec,
        timeout,
        &Start {
            prefix: "range",
            ..Start::default()
        },
    )
    .await
}

/// `S`'s range of `spec.table` holding `spec`'s range, as `[start, end)` bytes: the
/// command's ends, or that range's own where it gave none. A usage error when the range
/// isn't inside one of `S`'s ranges, or is empty.
pub(super) fn resolve(m: &ClusterMap, spec: &Spec) -> Result<(Vec<u8>, Option<Vec<u8>>), Failure> {
    let t = m
        .tables
        .iter()
        .find(|t| t.name == spec.table)
        .ok_or_else(|| Failure::usage(&format!("the map places no table {:?}", spec.table)))?;
    let ranges = map::table_ranges(t);
    let start = spec.start.clone();
    let holding = ranges.iter().find(|(lo, hi, p)| {
        *p == spec.from
            && start
                .as_deref()
                .is_none_or(|s| s >= lo.as_slice() && hi.as_deref().is_none_or(|h| s < h))
    });
    let Some((lo, hi, _)) = holding else {
        return Err(Failure::usage(&format!(
            "partition {:?} holds no range of table {:?}{}",
            spec.from,
            spec.table,
            start
                .as_ref()
                .map(|s| format!(" at {:?}", String::from_utf8_lossy(s)))
                .unwrap_or_default()
        )));
    };
    let start = start.unwrap_or_else(|| lo.clone());
    let end = spec.end.clone().or_else(|| hi.clone());
    if end.as_deref().is_some_and(|e| e <= start.as_slice()) {
        return Err(Failure::usage(
            "the range is empty: its end isn't after its start",
        ));
    }
    if !map::inside(t, &start, end.as_deref(), &spec.from) {
        return Err(Failure::usage(&format!(
            "the range isn't inside one range of table {:?} that partition {:?} holds",
            spec.table, spec.from
        )));
    }
    Ok((start, end))
}

/// The record of a range move of `spec`'s partitions in `m`, if any: ours when its table,
/// partitions and range are `spec`'s (a rerun goes on with it), else another move in flight.
fn record_for<'m>(
    m: &'m ClusterMap,
    spec: &Spec,
    range: &(Vec<u8>, Option<Vec<u8>>),
) -> Result<Option<&'m RangeMove>, Failure> {
    let Some(r) = m.moves.iter().find(|r| {
        [&r.from, &r.to]
            .iter()
            .any(|p| **p == spec.from || **p == spec.to)
    }) else {
        return Ok(None);
    };
    let ours = !map::node_move(r)
        && r.table == spec.table
        && r.from == spec.from
        && r.to == spec.to
        && map::move_range(r).ok().as_ref() == Some(range)
        && map::phase(r) != MovePhase::Aborting;
    if ours {
        return Ok(Some(r));
    }
    Err(Failure::usage(&format!(
        "move {:?} is in flight ({} of table {:?}, {:?} to {:?}, at {:?}): finish it (run it \
         again), or abort it before its commit point (phdb cluster move-range --abort {}), \
         then move",
        r.id,
        if map::node_move(r) {
            "a whole-partition move"
        } else {
            "a range move"
        },
        r.table,
        r.from,
        r.to,
        map::phase(r),
        r.id
    )))
}

/// The checks before a move starts (step 0), on the map as it is.
fn check(m: &ClusterMap, spec: &Spec) -> Result<(), Failure> {
    for p in [&spec.from, &spec.to] {
        if !m.partitions.iter().any(|q| q.id == *p) {
            return Err(Failure::usage(&format!("the map has no partition {p:?}")));
        }
    }
    if spec.from == spec.to {
        return Err(Failure::usage("a range moves to another partition"));
    }
    let t = m
        .tables
        .iter()
        .find(|t| t.name == spec.table)
        .ok_or_else(|| Failure::usage(&format!("the map places no table {:?}", spec.table)))?;
    if !t.group.is_empty() {
        return Err(Failure::usage(&format!(
            "table {:?} is in co-location group {:?}: its ranges move with the group's, which \
             range moves don't do",
            spec.table, t.group
        )));
    }
    if m.indexes.iter().any(|i| i.follows == spec.table) {
        return Err(Failure::usage(&format!(
            "an index follows table {:?}: its ranges don't move",
            spec.table
        )));
    }
    Ok(())
}

/// A partition's node, as `m` gives it.
fn node_of<'m>(m: &'m ClusterMap, p: &str) -> Result<(&'m str, &'m str), Failure> {
    let node = m
        .partitions
        .iter()
        .find(|q| q.id == p)
        .map(|q| q.node.as_str())
        .ok_or_else(|| Failure::usage(&format!("the map has no partition {p:?}")))?;
    let address = m
        .nodes
        .iter()
        .find(|n| n.id == node)
        .map(|n| n.address.as_str())
        .ok_or_else(|| Failure::usage(&format!("the map has no node {node:?}")))?;
    Ok((node, address))
}

/// Partition `p`'s line on its node, and move `id`'s status there, if it lists them.
async fn side(
    args: &NodeArgs,
    m: &ClusterMap,
    p: &str,
    id: &str,
) -> Result<(Option<LocalPartition>, Option<MoveStatus>), Failure> {
    let (node, address) = node_of(m, p)?;
    let lp = At::new(args, node, address).await?.local(p).await?;
    let ms = lp
        .as_ref()
        .and_then(|lp| lp.moves.iter().find(|s| s.id == id).cloned());
    Ok((lp, ms))
}

/// [`run`], started as `start` says.
pub(super) async fn run_as(
    args: &NodeArgs,
    spec: &Spec,
    timeout: Option<Duration>,
    start: &Start<'_>,
) -> Result<Report, Failure> {
    let m = map_now(args).await?;
    check(&m, spec)?;
    let range = resolve(&m, spec)?;
    let id = match record_for(&m, spec, &range)? {
        Some(r) => r.id.clone(),
        None => begin(args, &m, spec, &range, start).await?,
    };
    drive(args, &id, timeout).await?;
    Ok(super::cli::report(
        "cluster move-range",
        vec![
            ("move", id.into()),
            ("table", spec.table.clone().into()),
            ("from", spec.from.clone().into()),
            ("to", spec.to.clone().into()),
        ],
        serde_json::json!({}),
    ))
}

/// Step 1: the record at COPYING, with S's and T's epochs and `start`'s change.
async fn begin(
    args: &NodeArgs,
    m: &ClusterMap,
    spec: &Spec,
    range: &(Vec<u8>, Option<Vec<u8>>),
    how: &Start<'_>,
) -> Result<String, Failure> {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let id = format!("{}-{ms}", how.prefix);
    let epoch = |m: &ClusterMap, p: &str| {
        m.partitions
            .iter()
            .find(|q| q.id == p)
            .map_or(0, |q| q.epoch)
    };
    let (from_epoch, to_epoch) = (epoch(m, &spec.from), epoch(m, &spec.to));
    let start = |now: &ClusterMap| {
        // The partitions where this run read them, or nothing changes.
        if epoch(now, &spec.from) != from_epoch || epoch(now, &spec.to) != to_epoch {
            return Err(Failure::usage(
                "the map changed under the move's start (a partition's epoch moved): check \
                 the map, then run the move again",
            ));
        }
        record_for(now, spec, range)?;
        let mut next = now.clone();
        let mut r = RangeMove {
            id: id.clone(),
            kind: MoveKind::Range as i32,
            table: spec.table.clone(),
            start: Some(map::canonical_key(&range.0)),
            end: range.1.as_deref().map(map::canonical_key),
            from: spec.from.clone(),
            to: spec.to.clone(),
            from_epoch,
            to_epoch,
            ..RangeMove::default()
        };
        r.set_phase(MovePhase::Copying);
        next.moves.push(r);
        if let Some(with) = how.with {
            with(&mut next)?;
        }
        Ok(next)
    };
    set_record(args, start).await?;
    if let Some(on) = how.on_started {
        on(&id);
    }
    eprintln!("phdb cluster move-range: move {id}: COPYING");
    Ok(id)
}

/// Sets record `id` by `f` in the next map: `Err` naming it when the map no longer has it.
async fn change(
    args: &NodeArgs,
    id: &str,
    f: impl Fn(&ClusterMap, &mut RangeMove) -> Result<(), Failure>,
) -> Result<(), Failure> {
    set_record(args, |now: &ClusterMap| {
        let mut next = now.clone();
        let i = next
            .moves
            .iter()
            .position(|r| r.id == id)
            .ok_or_else(|| gone(id))?;
        let mut r = next.moves[i].clone();
        f(now, &mut r)?;
        next.moves[i] = r;
        Ok(next)
    })
    .await
}

/// Removes record `id` in the next map (done, or an abort finished).
async fn remove(args: &NodeArgs, id: &str) -> Result<(), Failure> {
    set_record(args, |now: &ClusterMap| {
        let mut next = now.clone();
        next.moves.retain(|r| r.id != id);
        Ok(next)
    })
    .await
}

fn gone(id: &str) -> Failure {
    Failure::usage(&format!(
        "move {id}'s record is gone from the map (an abort, a restore or an operator's \
         change ended it): check the map"
    ))
}

/// Why the move must abort, if it must (before its commit point): S's or T's epoch moved,
/// T's follower stopped, or S's hold lapsed.
fn abort_reason(
    m: &ClusterMap,
    r: &RangeMove,
    s: Option<&MoveStatus>,
    t: Option<&MoveStatus>,
) -> Option<String> {
    let epoch = |p: &str| m.partitions.iter().find(|q| q.id == p).map(|q| q.epoch);
    if r.from_epoch != 0 && epoch(&r.from) != Some(r.from_epoch) {
        return Some(format!("its source {} failed over", r.from));
    }
    if r.to_epoch != 0 && epoch(&r.to) != Some(r.to_epoch) {
        return Some(format!("its target {} failed over", r.to));
    }
    if let Some(t) = t
        && !t.follower_error.is_empty()
    {
        return Some(format!(
            "its target's follower stopped: {}",
            t.follower_error
        ));
    }
    if s.is_some_and(|s| s.retention_lapsed) {
        return Some(format!(
            "its source {}'s change-feed hold lapsed (--move-retention-cap)",
            r.from
        ));
    }
    None
}

/// Runs move `id` from the phase its record is at to its end.
async fn drive(args: &NodeArgs, id: &str, timeout: Option<Duration>) -> Result<(), Failure> {
    let mut fenced_since: Option<tokio::time::Instant> = None;
    let mut wait = Waiting::new(
        format!("moving the range (move {id})"),
        timeout,
        format!(
            "The move goes on where the map records it: run the command again, or abort it \
             before its commit point (phdb cluster move-range --abort {id})"
        ),
    );
    let mut said = MovePhase::Unspecified;
    loop {
        let m = map_now(args).await?;
        let Some(r) = m.moves.iter().find(|r| r.id == id).cloned() else {
            // Done (or ended by someone else): done is CLEANING's removal.
            return Ok(());
        };
        let phase = map::phase(&r);
        if phase != said {
            eprintln!("phdb cluster move-range: move {id}: {phase:?}");
            said = phase;
        }
        let (s_lp, s) = wait.ask(side(args, &m, &r.from, id)).await??;
        let (t_lp, t) = wait.ask(side(args, &m, &r.to, id)).await??;
        if map::before_commit(phase)
            && let Some(why) = abort_reason(&m, &r, s.as_ref(), t.as_ref())
        {
            abort_record(args, &r, timeout).await?;
            return Err(Failure::usage(&format!(
                "move {id} aborted: {why}. The range stays on {}; run the move again to retry",
                r.from
            )));
        }
        match phase {
            MovePhase::Copying => {
                if let Some(t) = &t
                    && t.copy_done
                {
                    let snap = t.source_applied_seqno;
                    change(args, id, |_, r| {
                        r.set_phase(MovePhase::CatchingUp);
                        r.snapshot_seqno = snap;
                        Ok(())
                    })
                    .await?;
                    continue;
                }
                let copied = t.as_ref().map_or(0, |t| t.copied_bytes);
                let waiting = t
                    .as_ref()
                    .map(|t| t.follower_waiting.clone())
                    .filter(|w| !w.is_empty());
                wait.tick(&waiting.unwrap_or_else(|| format!("{copied} bytes copied")))
                    .await?;
            }
            MovePhase::CatchingUp => {
                let lag = t_lp.as_ref().map_or(u64::MAX, |lp| lp.move_follower_lag_ms);
                let in_doubt = s_lp.as_ref().map_or(0, |lp| lp.in_doubt);
                if lag <= CATCH_UP_LAG_MS && in_doubt == 0 {
                    change(args, id, |_, r| {
                        r.set_phase(MovePhase::HandingOver);
                        Ok(())
                    })
                    .await?;
                    fenced_since = Some(tokio::time::Instant::now());
                    continue;
                }
                wait.tick(&format!(
                    "the catch-up is {lag} ms behind; {in_doubt} shares in doubt on the source"
                ))
                .await?;
            }
            MovePhase::HandingOver => {
                let since = *fenced_since.get_or_insert_with(tokio::time::Instant::now);
                if r.final_seqno == 0 {
                    let fence = s.as_ref().map_or(0, |s| s.fence_seqno);
                    if fence != 0 {
                        change(args, id, |_, r| {
                            if r.final_seqno == 0 {
                                r.final_seqno = fence;
                            }
                            Ok(())
                        })
                        .await?;
                        continue;
                    }
                    if since.elapsed() >= FENCE_BOUND {
                        abort_record(args, &r, timeout).await?;
                        return Err(Failure::usage(&format!(
                            "move {id} aborted: its source {}'s fence didn't drain within {:?} \
                             (a write still running, or a transaction share in doubt on the \
                             range: phdb cluster status shows them). The range takes writes \
                             again; run the move again to retry",
                            r.from, FENCE_BOUND
                        )));
                    }
                    wait.tick("the source's fence drains").await?;
                    continue;
                }
                let applied = t.as_ref().map_or(0, |t| t.source_applied_seqno);
                if applied >= r.final_seqno {
                    hand_over(args, id).await?;
                    continue;
                }
                wait.tick(&format!(
                    "the target applied through {applied} of {}",
                    r.final_seqno
                ))
                .await?;
            }
            MovePhase::HandedOver => {
                if never_both(args, &m, &r).await? {
                    change(args, id, |_, r| {
                        r.set_phase(MovePhase::Cleaning);
                        Ok(())
                    })
                    .await?;
                    continue;
                }
                wait.tick("waiting for the source to take the hand-over, or its old lease to end")
                    .await?;
            }
            MovePhase::Cleaning => {
                if s.as_ref().is_some_and(|s| s.dropped) {
                    remove(args, id).await?;
                    return Ok(());
                }
                wait.tick("the source drops its copy").await?;
            }
            MovePhase::Aborting => {
                abort_record(args, &r, timeout).await?;
                return Err(Failure::usage(&format!("move {id} was aborted")));
            }
            MovePhase::Unspecified => {
                return Err(Failure::usage(&format!(
                    "move {id}'s phase isn't one this build knows"
                )));
            }
        }
    }
}

/// Step 5: the commit point, in one map change.
async fn hand_over(args: &NodeArgs, id: &str) -> Result<(), Failure> {
    set_record(args, |now: &ClusterMap| {
        let mut next = now.clone();
        let i = next
            .moves
            .iter()
            .position(|r| r.id == id)
            .ok_or_else(|| gone(id))?;
        let r = next.moves[i].clone();
        if map::phase(&r) != MovePhase::HandingOver {
            return Ok(next);
        }
        let (start, end) = map::move_range(&r).map_err(|e| Failure::usage(&e))?;
        for t in next.tables.iter_mut().filter(|t| t.name == r.table) {
            *t = map::handed_over(t, &start, end.as_deref(), &r.to);
        }
        let floor = super::promote::promote_floor(now, &r.from);
        for p in next.partitions.iter_mut().filter(|p| p.id == r.from) {
            p.epoch = p.epoch.max(floor) + 1;
        }
        let m = &mut next.moves[i];
        m.set_phase(MovePhase::HandedOver);
        m.handed_over_term = now.writer_term;
        m.handed_over_version = now.map_version + 1;
        Ok(next)
    })
    .await
}

/// Step 6's rule: S took the hand-over map (its node holds it, S at the raised epoch), or
/// a majority holds the map and a majority of the nodes no longer grant S's partition to
/// S's node (each of them refuses a renewal at S's old epoch).
async fn never_both(args: &NodeArgs, m: &ClusterMap, r: &RangeMove) -> Result<bool, Failure> {
    let (s_node, _) = node_of(m, &r.from)?;
    let raised = m
        .partitions
        .iter()
        .find(|p| p.id == r.from)
        .map_or(0, |p| p.epoch);
    let majority = m.nodes.len() / 2 + 1;
    let (mut holding, mut not_granting) = (0, 0);
    for n in &m.nodes {
        let Ok(at) = At::new(args, &n.id, &n.address).await else {
            continue;
        };
        let Ok(st) = at.status().await else {
            continue;
        };
        if n.id == s_node
            && st.map_version >= r.handed_over_version
            && st
                .partitions
                .iter()
                .any(|lp| lp.id == r.from && lp.epoch >= raised)
        {
            return Ok(true);
        }
        if st.map_version >= r.handed_over_version {
            holding += 1;
        }
        if n.id != s_node
            && !st
                .grants
                .iter()
                .any(|g| g.partition == r.from && g.holder == s_node && g.remaining_ms > 0)
        {
            not_granting += 1;
        }
    }
    Ok(holding >= majority && not_granting >= majority)
}

/// An abort before the commit point: the record to ABORTING, then gone once T has dropped
/// its copy (its reconcile, which also runs at T's next open if T is down: so the record
/// goes after `timeout` anyway, and T cleans up when it's back).
async fn abort_record(
    args: &NodeArgs,
    r: &RangeMove,
    timeout: Option<Duration>,
) -> Result<(), Failure> {
    if map::before_commit(map::phase(r)) {
        change(args, &r.id, |_, r| {
            if map::before_commit(map::phase(r)) {
                r.set_phase(MovePhase::Aborting);
            }
            Ok(())
        })
        .await?;
    }
    let mut wait = Waiting::new(
        format!("waiting for {} to drop move {}'s copy", r.to, r.id),
        Some(timeout.unwrap_or(Duration::from_secs(60))),
        String::new(),
    );
    loop {
        let m = map_now(args).await?;
        let dropped = match side(args, &m, &r.to, &r.id).await {
            Ok((_, Some(t))) => t.dropped,
            _ => false,
        };
        if dropped || wait.tick("the target drops its copy").await.is_err() {
            break;
        }
    }
    remove(args, &r.id).await
}

/// `phdb cluster move-range --abort ID`: move `id` stops before its commit point. Refused
/// from HANDED_OVER on, which only goes forward.
pub(super) async fn abort(
    args: &NodeArgs,
    id: &str,
    timeout: Option<Duration>,
) -> Result<Report, Failure> {
    let m = map_now(args).await?;
    let r = m
        .moves
        .iter()
        .find(|r| r.id == id && !map::node_move(r))
        .cloned()
        .ok_or_else(|| {
            Failure::usage(&format!(
                "the map (version {}) records no range move {id:?}",
                m.map_version
            ))
        })?;
    let phase = map::phase(&r);
    if map::committed(phase) {
        return Err(Failure::usage(&format!(
            "move {id} is at {phase:?}, past its commit point: the map already gives the range \
             to {}. Let it finish: run phdb cluster move-range again",
            r.to
        )));
    }
    abort_record(args, &r, timeout).await?;
    Ok(super::cli::report(
        "cluster move-range --abort",
        vec![
            ("move", id.into()),
            ("table", r.table.clone().into()),
            ("from", r.from.clone().into()),
            ("to", r.to.clone().into()),
        ],
        serde_json::json!({}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::proto::{Partition, SplitKey, TablePlacement, split_key};

    fn text(s: &str) -> SplitKey {
        SplitKey {
            key: Some(split_key::Key::Text(s.into())),
        }
    }

    /// `users` split at "m" over p1 and p2.
    fn map() -> ClusterMap {
        let part = |id: &str| Partition {
            id: id.into(),
            node: "a".into(),
            epoch: 1,
            ..Default::default()
        };
        ClusterMap {
            map_version: 1,
            partitions: vec![part("p1"), part("p2")],
            tables: vec![TablePlacement {
                name: "users".into(),
                splits: vec![text("m")],
                partitions: vec!["p1".into(), "p2".into()],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn spec(start: Option<&str>, end: Option<&str>) -> Spec {
        Spec {
            table: "users".into(),
            from: "p1".into(),
            to: "p2".into(),
            start: start.map(|s| s.as_bytes().to_vec()),
            end: end.map(|s| s.as_bytes().to_vec()),
        }
    }

    #[test]
    fn a_range_resolves_inside_one_of_the_sources_ranges() {
        let m = map();
        assert_eq!(
            resolve(&m, &spec(Some("c"), Some("f"))).unwrap(),
            (b"c".to_vec(), Some(b"f".to_vec()))
        );
        // Ends default to the source's range's.
        assert_eq!(
            resolve(&m, &spec(Some("c"), None)).unwrap(),
            (b"c".to_vec(), Some(b"m".to_vec()))
        );
        assert_eq!(
            resolve(&m, &spec(None, None)).unwrap(),
            (Vec::new(), Some(b"m".to_vec()))
        );
        // Past the source's range, empty, or a range p1 doesn't hold.
        assert!(resolve(&m, &spec(Some("c"), Some("z"))).is_err());
        assert!(resolve(&m, &spec(Some("f"), Some("c"))).is_err());
        assert!(resolve(&m, &spec(Some("n"), None)).is_err());
    }

    #[test]
    fn a_rerun_finds_its_record_and_another_move_in_flight_refuses() {
        let mut m = map();
        let range = (b"c".to_vec(), Some(b"f".to_vec()));
        assert!(
            record_for(&m, &spec(Some("c"), Some("f")), &range)
                .unwrap()
                .is_none()
        );
        let mut r = RangeMove {
            id: "range-1".into(),
            kind: MoveKind::Range as i32,
            table: "users".into(),
            start: Some(text("c")),
            end: Some(text("f")),
            from: "p1".into(),
            to: "p2".into(),
            ..Default::default()
        };
        r.set_phase(MovePhase::CatchingUp);
        m.moves.push(r);
        let found = record_for(&m, &spec(Some("c"), Some("f")), &range).unwrap();
        assert_eq!(found.map(|r| r.id.as_str()), Some("range-1"));
        let other = (b"c".to_vec(), Some(b"e".to_vec()));
        let e = record_for(&m, &spec(Some("c"), Some("e")), &other).unwrap_err();
        assert!(
            e.message().contains("move \"range-1\" is in flight"),
            "{}",
            e.message()
        );
        // An aborting record is never resumed.
        m.moves[0].set_phase(MovePhase::Aborting);
        assert!(record_for(&m, &spec(Some("c"), Some("f")), &range).is_err());
    }

    #[test]
    fn a_move_aborts_on_a_failover_a_stopped_follower_or_a_lapsed_hold() {
        let m = map();
        let r = RangeMove {
            from: "p1".into(),
            to: "p2".into(),
            from_epoch: 1,
            to_epoch: 1,
            ..Default::default()
        };
        assert_eq!(abort_reason(&m, &r, None, None), None);
        let mut moved = m.clone();
        moved.partitions[1].epoch = 2;
        assert!(
            abort_reason(&moved, &r, None, None)
                .unwrap()
                .contains("target p2 failed over")
        );
        moved.partitions[0].epoch = 2;
        assert!(
            abort_reason(&moved, &r, None, None)
                .unwrap()
                .contains("source p1 failed over")
        );
        let t = MoveStatus {
            follower_error: "the source's history expired".into(),
            ..Default::default()
        };
        assert!(
            abort_reason(&m, &r, None, Some(&t))
                .unwrap()
                .contains("history expired")
        );
        let s = MoveStatus {
            retention_lapsed: true,
            ..Default::default()
        };
        assert!(
            abort_reason(&m, &r, Some(&s), None)
                .unwrap()
                .contains("hold lapsed")
        );
        // A record from before to_epoch isn't compared on it.
        let old = RangeMove { to_epoch: 0, ..r };
        let mut moved = m;
        moved.partitions[1].epoch = 2;
        assert_eq!(abort_reason(&moved, &old, None, None), None);
    }
}

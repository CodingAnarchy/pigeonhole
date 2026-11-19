//! `phdb cluster move P --to NEW [--drop-source]` (D317 ticket 6, ICR 0117): a partition's
//! planned move, with its files copied whole, so its change-feed history and tokens carry
//! over.
//!
//! 1. **Fence and close** P on the node serving it (OLD), then wait for the close's hash of
//!    its files: the main file, every WAL file a clean close keeps, and the feed's.
//! 2. **Copy** each file from OLD (`Admin.BackupDownload`'s `fenced_file`) into an upload
//!    on NEW (`Admin.PartitionUpload`, `RELOCATE`, the epoch `e` the copy is of). Each
//!    upload's id comes from the file's name and hash, so a rerun resumes what arrived.
//! 3. **Stamp** NEW from `e` to its next epoch (`e + 1`, or past a promote's floor: #2070) and compare the set it installed with OLD's closed
//!    set, file by file: name, size and SHA-256. A mismatch stops the move before the map
//!    changes, so OLD keeps P.
//! 4. **The map:** `relocate --copy`, which keeps P's `history_epoch`. Its stamp is then a
//!    no-op.
//! 5. **Serving:** wait for NEW to serve P at that epoch.
//! 6. **`--drop-source`:** only after a comparison that matched, OLD's files move aside
//!    (`drop-partition`). Nothing is deleted.
//!
//! A rerun never trusts a copy an earlier run left on NEW: it copies OLD's closed set again
//! (an upload still staged is resumed, one already installed is sent whole), and the stamp
//! installs it over the earlier copy, which moves aside. OLD may have been unfenced and
//! written to between the runs, and nothing else ties NEW's stamped file to what OLD closed
//! this time. So every move that sets the map compared the copy it serves.

use std::time::{Duration, Instant};

use prost::bytes::Bytes;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::cli::{CopyCheck, NodeArgs, channel, connect, relocate_as, report, request, url_of};
use super::transfer::{Feed, Stopped, upload_file};
use crate::exit::Failure;
use crate::output::Report;
use crate::serve::download::STALL;
use crate::serve::proto::{
    self, BackupDownloadRequest, GetMapRequest, LocalPartition, LocalPartitionState, PartitionFile,
    PartitionRequest, PartitionUploadHeader, PartitionUploadPurpose, StatusRequest,
};

/// How often the move asks a node how a step is going.
const POLL: Duration = Duration::from_millis(200);

/// How often a wait says it's still waiting.
const SAY_EVERY: Duration = Duration::from_secs(10);

type Admin = proto::admin_client::AdminClient<tonic::transport::Channel>;

/// A node's channel, as the move uses it.
pub(super) struct At {
    id: String,
    args: NodeArgs,
    token: Option<String>,
    admin: Admin,
}

impl At {
    pub(super) async fn new(args: &NodeArgs, id: &str, address: &str) -> Result<Self, Failure> {
        let args = NodeArgs {
            node: url_of(args, address),
            ..args.clone()
        };
        let (ch, token) = channel(&args, &args.node).await?;
        Ok(Self {
            id: id.to_owned(),
            admin: crate::serve::download::admin(ch),
            args,
            token,
        })
    }

    fn failed(&self, what: &str, s: &tonic::Status) -> Failure {
        Failure::usage(&format!(
            "node {:?} ({}): {what}: {}",
            self.id,
            self.args.node,
            s.message()
        ))
    }

    /// The node's map version.
    async fn map_version(&self) -> Result<u64, Failure> {
        let (mut c, _) = connect(&self.args, &self.args.node).await?;
        Ok(c.get_map(request(GetMapRequest {}, self.token.as_deref())?)
            .await
            .map_err(|s| self.failed("map", &s))?
            .into_inner()
            .map_version)
    }

    /// Partition `p`'s line in the node's `Status`, if it lists it.
    async fn local(&self, p: &str) -> Result<Option<LocalPartition>, Failure> {
        let (mut c, _) = connect(&self.args, &self.args.node).await?;
        let st = c
            .status(request(StatusRequest {}, self.token.as_deref())?)
            .await
            .map_err(|s| self.failed("status", &s))?
            .into_inner();
        Ok(st.partitions.into_iter().find(|lp| lp.id == p))
    }

    /// Fence, close or drop partition `p` here.
    async fn call(&self, p: &str, what: &'static str) -> Result<LocalPartition, Failure> {
        let (mut c, _) = connect(&self.args, &self.args.node).await?;
        let req = request(
            PartitionRequest {
                partition: p.to_owned(),
                force: false,
            },
            self.token.as_deref(),
        )?;
        let reply = match what {
            "fence" => c.fence(req).await,
            "close" => c.close(req).await,
            _ => c.drop_partition(req).await,
        };
        Ok(reply.map_err(|s| self.failed(what, &s))?.into_inner())
    }
}

/// A wait for a node: says every [`SAY_EVERY`] that it goes on, and gives up at
/// `--timeout` (#1784), naming the step, the node's last answer and where that leaves the
/// partition.
struct Waiting {
    what: String,
    since: tokio::time::Instant,
    said: tokio::time::Instant,
    limit: Option<Duration>,
    /// What the node last said (its `status` line for the partition, or the error).
    last: String,
    /// Where the partition stands if the wait gives up, and what to do.
    then: String,
}

impl Waiting {
    fn new(what: String, limit: Option<Duration>, then: String) -> Self {
        let now = tokio::time::Instant::now();
        Self {
            what,
            since: now,
            said: now,
            limit,
            last: "no answer yet".to_owned(),
            then,
        }
    }

    /// The time left, `None` without a limit.
    fn left(&self) -> Option<Duration> {
        self.limit.map(|l| l.saturating_sub(self.since.elapsed()))
    }

    fn gave_up(&self) -> Failure {
        Failure::timed_out(format!(
            "gave up {} after {:?} (--timeout): {}. {}",
            self.what,
            self.limit.unwrap_or_default(),
            self.last,
            self.then
        ))
    }

    /// A call to the node, bounded by the time left: a node that never answers can't hold
    /// the wait past its limit. The outer error is the wait given up; the inner, the
    /// call's own. With no time left (the last poll slept up to the limit), it gives up
    /// at once, with the node's last answer.
    async fn ask<T>(&mut self, call: impl Future<Output = T>) -> Result<T, Failure> {
        match self.left() {
            None => Ok(call.await),
            Some(Duration::ZERO) => Err(self.gave_up()),
            Some(left) => match tokio::time::timeout(left, call).await {
                Ok(r) => Ok(r),
                Err(_) => {
                    self.last = "the node didn't answer".to_owned();
                    Err(self.gave_up())
                }
            },
        }
    }

    /// The node said `now`: wait a poll, or give up at the limit.
    async fn tick(&mut self, now: &str) -> Result<(), Failure> {
        now.clone_into(&mut self.last);
        if self.left() == Some(Duration::ZERO) {
            return Err(self.gave_up());
        }
        if self.said.elapsed() >= SAY_EVERY {
            self.said = tokio::time::Instant::now();
            eprintln!(
                "phdb cluster move: {} ({}s): {now}",
                self.what,
                self.since.elapsed().as_secs(),
            );
        }
        // Less than two polls left: sleep them all, and give up with this answer. Polled
        // once more, the next call could be left a sliver of the limit, too little for a
        // busy node to answer, and the wait would end on "the node didn't answer" rather
        // than on what the node said (#2015, a macOS CI run).
        let sleep = match self.left() {
            Some(l) if l < 2 * POLL => l,
            _ => POLL,
        };
        tokio::time::sleep(sleep).await;
        Ok(())
    }
}

/// A partition's `status` line in a wait's words: its state, then its error if any.
fn said(lp: &LocalPartition) -> String {
    let state = super::cli::state_text(lp);
    let state = if state.is_empty() { "not open" } else { &state };
    if lp.error.is_empty() {
        state.to_owned()
    } else {
        format!("{state}: {}", lp.error)
    }
}

/// `phdb cluster move` (the module docs).
pub(super) async fn run(
    args: &NodeArgs,
    partition: &str,
    to: &str,
    drop_source: bool,
    timeout: Option<Duration>,
) -> Result<Report, Failure> {
    let (mut c, token) = connect(args, &args.node).await?;
    let mut m = c
        .get_map(request(GetMapRequest {}, token.as_deref())?)
        .await
        .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))?
        .into_inner();
    // A node holding P's replica copy (D328) leaves P's replicas and closes its copy before
    // anything is staged there (S4): the installed set then replaces the copy's file.
    if super::promote::leave_replicas(args, &m, partition, to).await? {
        m = c
            .get_map(request(GetMapRequest {}, token.as_deref())?)
            .await
            .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))?
            .into_inner();
    }
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
            .map(|n| n.address.clone())
    };
    let Some(new_address) = address(to) else {
        return Err(Failure::usage(&format!("the map has no node {to:?}")));
    };
    if p.node == to {
        // An earlier run set the map (or P was never elsewhere): it waits, as that run did,
        // for a majority of the nodes to hold the map (#1760).
        majority_holds(args, &m, m.map_version, timeout).await?;
        // An earlier run finished the move (or P was never elsewhere): nothing to copy, and
        // the copy can't be compared again, so nothing is dropped. Where the old copy lies
        // closed, the advice names it.
        let mut closed_on = Vec::new();
        for n in m.nodes.iter().filter(|n| n.id != to) {
            let Ok(at) = At::new(args, &n.id, &n.address).await else {
                continue;
            };
            if let Ok(Some(lp)) = at.local(partition).await
                && lp.state() == LocalPartitionState::Closed
            {
                closed_on.push(format!(
                    "phdb cluster drop-partition {partition} --node {}",
                    at.args.node
                ));
            }
        }
        let advice = if closed_on.is_empty() {
            String::new()
        } else {
            format!(
                ". Its old copy is still closed there; once you've checked node {to}'s, move \
                 it aside: {}",
                closed_on.join("; ")
            )
        };
        let mut r = report(
            "cluster move",
            vec![
                ("partition", json!(partition)),
                ("node", json!(to)),
                ("epoch", json!(p.epoch)),
                ("verified", json!(false)),
                (
                    "done",
                    json!(format!(
                        "the map already gives {partition} to node {to}: an earlier run moved \
                         it, and nothing was dropped{advice}"
                    )),
                ),
            ],
            Value::Null,
        );
        r.status = crate::ExitStatus::SUCCESS;
        return Ok(r);
    }
    let old_address = address(&p.node).unwrap_or_default();
    let old = At::new(args, &p.node, &old_address).await?;
    let new = At::new(args, to, &new_address).await?;
    let e = p.epoch;
    // One above P's epoch and every epoch a promote of it recorded (#2070).
    let next = super::promote::next_epoch(&m, &p);

    // 1. Fenced and closed on OLD, its files hashed.
    let started = Instant::now();
    let unreachable = |f: Failure| {
        Failure::usage(&format!(
            "{} (a move copies the files of the node serving {partition}: if its process is \
             stopped, promote another node with phdb cluster relocate --old-unreachable, \
             which starts a new feed history)",
            f.message()
        ))
    };
    let lp = old
        .local(partition)
        .await
        .map_err(unreachable)?
        .ok_or_else(|| {
            Failure::usage(&format!(
                "node {:?} doesn't list partition {partition}",
                old.id
            ))
        })?;
    // Fenced by this move whenever P is open here, already fenced or not: a fence since
    // the node started isn't persisted, and a restart of OLD mid-move would lift it on a
    // peer's map, which names OLD until step 4.
    if lp.state() == LocalPartitionState::Opening {
        return Err(Failure::usage(&format!(
            "partition {partition} is still opening on node {:?}: run the move again once \
             it's open there (phdb cluster status --node {})",
            old.id, old.args.node
        )));
    }
    let mut fenced_here = None;
    let closed_at = Instant::now();
    if lp.state() != LocalPartitionState::Closed {
        old.call(partition, "fence").await?;
        if lp.state() == LocalPartitionState::Serving {
            fenced_here = Some(Instant::now());
        }
        old.call(partition, "close").await?;
    }
    let files = closed_files(&old, partition, timeout).await?;
    stop_after(args, Step::Closed)?;

    // 2 and 3. Copied, stamped and compared, whatever an earlier run left on NEW (the
    // module docs).
    let mut sent = Vec::new();
    let set = Set {
        cluster: &m.cluster_id,
        p: partition,
        e,
        files: &files,
    };
    for f in &files {
        sent.push(copy_file(&old, &new, &set, f, STALL).await?);
    }
    stop_after(args, Step::Copied)?;
    #[cfg(test)]
    hold_before_stamp(args).await;
    let installed = stamp(&new, &m.cluster_id, partition, e, next).await?;
    if installed.is_empty() {
        // The set this run staged is gone: another run's stamp took it.
        return Err(Failure::usage(&format!(
            "node {to:?} installed nothing at the stamp: another move of {partition} is \
             running. The map still gives {partition} to {:?} at epoch {e}: run the move \
             again once the other has ended",
            old.id
        )));
    }
    compare(&installed, &files).map_err(|why| {
        Failure::usage(&format!(
            "node {to:?} installed files that aren't the ones node {:?} closed ({why}). The \
             map still gives {partition} to {:?} at epoch {e}: run the move again to copy \
             them again, or phdb cluster unfence {partition} on {:?} serves it there again",
            old.id, old.id, old.id
        ))
    })?;

    stop_after(args, Step::Stamped)?;
    // 4. The map, marked as copied.
    let set = relocate_as(
        args,
        partition,
        to,
        false,
        CopyCheck::Checked(files.clone()),
        None,
    )
    .await?;
    let map_at = Instant::now();
    // The map on a majority of its nodes before NEW is waited for (the coordinator's call
    // on #1760's Q4): from then no round can confirm OLD again on an older map, so an
    // unfence of OLD while NEW is down is refused (D317 rule 3). A rerun resumes here.
    let version = set
        .document
        .get("map_version")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    majority_holds(args, &m, version, timeout).await?;
    stop_after(args, Step::Mapped)?;

    // 5. Serving on NEW.
    serving(&new, partition, next, timeout).await?;
    let serving_at = Instant::now();
    stop_after(args, Step::Serving)?;

    // 6. OLD's files aside: the comparison matched.
    let drop_command = format!(
        "phdb cluster drop-partition {partition} --node {}",
        old.args.node
    );
    let dropped = if drop_source {
        Some(old.call(partition, "drop").await?)
    } else {
        None
    };

    let bytes: u64 = files.iter().map(|f| f.bytes).sum();
    let ms = |d: Duration| json!(u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    let mut summary = vec![
        ("partition", json!(partition)),
        ("from", json!(old.id)),
        ("node", json!(to)),
        ("epoch", json!(next)),
        ("files", json!(files.len())),
        ("bytes", json!(bytes)),
        ("verified", json!(true)),
        (
            // Writes answer `Fenced` from the fence until the map gives NEW the partition.
            "writes_down_ms",
            fenced_here.map_or(Value::Null, |at| ms(map_at - at)),
        ),
        ("reads_down_ms", ms(serving_at - closed_at)),
        ("took_ms", ms(started.elapsed())),
        (
            "map_version",
            set.document
                .get("map_version")
                .cloned()
                .unwrap_or(Value::Null),
        ),
    ];
    summary.push((
        "source",
        match &dropped {
            Some(lp) => json!(format!("moved aside on node {}: {}", old.id, lp.file)),
            None => json!(format!("kept on node {}: {drop_command}", old.id)),
        },
    ));
    let mut r = report("cluster move", summary, Value::Null);
    r.document.insert(
        "copied".into(),
        json!(
            files
                .iter()
                .enumerate()
                .map(|(i, f)| json!({
                    "name": f.name,
                    "bytes": f.bytes,
                    "sent": sent.get(i).copied().unwrap_or(0),
                }))
                .collect::<Vec<_>>()
        ),
    );
    if let Some(lp) = dropped {
        r.document.insert(
            "dropped".into(),
            serde_json::to_value(&lp).expect("LocalPartition serializes"),
        );
    }
    Ok(r)
}

/// The steps a move can be stopped after, in the tests: a process killed there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Step {
    Closed,
    Copied,
    Stamped,
    Mapped,
    Serving,
}

/// Moves to stop, each by the `--node` URL it runs with and the step it stops after (a
/// fault seam: each test's nodes have ports of their own).
#[cfg(test)]
pub(super) static STOP: std::sync::Mutex<Vec<(String, Step)>> = std::sync::Mutex::new(Vec::new());

/// A hold seam (tests): a move listed here, by its `--node` URL, says it reached its stamp
/// (the sender) and waits for the release (the receiver, sent or dropped) before it stamps,
/// so a test can run another move's stamp in between. Awaited: no thread blocks.
#[cfg(test)]
pub(super) static HOLD_BEFORE_STAMP: std::sync::Mutex<Vec<HeldStamp>> =
    std::sync::Mutex::new(Vec::new());

/// A [`HOLD_BEFORE_STAMP`] entry: the move's `--node` URL, its "reached" and its release.
#[cfg(test)]
pub(super) type HeldStamp = (
    String,
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);

#[cfg(test)]
async fn hold_before_stamp(args: &NodeArgs) {
    let held = {
        let mut armed = HOLD_BEFORE_STAMP
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        armed
            .iter()
            .position(|(n, _, _)| *n == args.node)
            .map(|i| armed.swap_remove(i))
    };
    if let Some((_, reached, release)) = held {
        let _ = reached.send(());
        let _ = release.await;
    }
}

/// The fault seam: in the tests, a move listed in [`STOP`] ends here, as if killed.
fn stop_after(args: &NodeArgs, step: Step) -> Result<(), Failure> {
    #[cfg(test)]
    {
        let mut stops = STOP
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(i) = stops
            .iter()
            .position(|(n, s)| *n == args.node && *s == step)
        {
            stops.remove(i);
            return Err(Failure::usage(&format!("stopped after {step:?} (test)")));
        }
    }
    let _ = (args, step);
    Ok(())
}

/// Waits for NEW to serve `p`, which the map now gives it at epoch `e`.
pub(super) async fn serving(
    new: &At,
    p: &str,
    e: u64,
    timeout: Option<Duration>,
) -> Result<(), Failure> {
    let mut wait = Waiting::new(
        format!("waiting for node {:?} to serve {p} at epoch {e}", new.id),
        timeout,
        format!(
            "The map already gives {p} to node {:?} at epoch {e}, and its writes go there: \
             check that node (phdb cluster status --node {}). The old node's files are kept",
            new.id, new.args.node
        ),
    );
    loop {
        match wait.ask(new.local(p)).await? {
            // The map's epoch for P is `e` now. `Status` reads a served partition's epoch
            // from its handle (#1767), so a node still serving an older epoch isn't taken
            // for done (simp724's N1 on #1729 and #1787).
            Ok(Some(lp)) if lp.state() == LocalPartitionState::Serving && lp.epoch == e => {
                return Ok(());
            }
            Ok(Some(lp)) if lp.state() == LocalPartitionState::Serving => {
                wait.tick(&format!("serving at epoch {}", lp.epoch)).await?;
            }
            Ok(Some(lp)) => wait.tick(&said(&lp)).await?,
            Ok(None) => wait.tick("not listed yet").await?,
            Err(f) => wait.tick(f.message()).await?,
        }
    }
}

/// The longest [`majority_holds`] waits for one node's answer in a poll.
const PER_NODE: Duration = Duration::from_secs(10);

/// Waits until a majority of `map`'s nodes hold map version `v` or newer (#1760: a move's
/// map step). A node that missed the writer's push takes the map from a peer's round,
/// which pushes it to a peer that answers with an older one (D317 rule 3).
pub(super) async fn majority_holds(
    args: &NodeArgs,
    map: &proto::ClusterMap,
    v: u64,
    timeout: Option<Duration>,
) -> Result<(), Failure> {
    let n = map.nodes.len();
    let mut wait = Waiting::new(
        format!("waiting for a majority of the {n} nodes to take map version {v}"),
        timeout,
        format!(
            "The map is set at version {v}, and a node that missed it takes it from its \
             peers: check them (phdb cluster status --node {}), and run the move again to go \
             on waiting",
            args.node
        ),
    );
    loop {
        // Every node at once (audit120's nit on #1899): the wait is as long as the slowest
        // answer, not their sum. Each is bounded on its own, so a node that's down (a
        // refused connect can take seconds on Windows) counts as missing rather than
        // ending the wait without a count.
        if wait.left() == Some(Duration::ZERO) {
            return Err(wait.gave_up());
        }
        let per = wait.left().map_or(PER_NODE, |l| l.min(PER_NODE));
        let mut asks = tokio::task::JoinSet::new();
        for node in &map.nodes {
            let (args, id, address) = (args.clone(), node.id.clone(), node.address.clone());
            asks.spawn(async move {
                let version = async {
                    let at = At::new(&args, &id, &address).await?;
                    at.map_version().await
                };
                let version = tokio::time::timeout(per, version)
                    .await
                    .unwrap_or_else(|_| {
                        Err(Failure::usage(&format!(
                            "no answer within {}s",
                            per.as_secs_f32()
                        )))
                    });
                (id, version)
            });
        }
        let answers = asks.join_all().await;
        let mut have = 0;
        let mut missing = Vec::new();
        for (id, version) in answers {
            match version {
                Ok(version) if version >= v => have += 1,
                Ok(version) => missing.push(format!("{id} has version {version}")),
                Err(f) => missing.push(format!("{id}: {}", f.message())),
            }
        }
        if have * 2 > n {
            return Ok(());
        }
        wait.tick(&format!(
            "{have} of {n} nodes hold it ({})",
            missing.join("; ")
        ))
        .await?;
    }
}

/// OLD's closed files once their hash is done: a failed hash is retried by closing again.
pub(super) async fn closed_files(
    old: &At,
    p: &str,
    timeout: Option<Duration>,
) -> Result<Vec<PartitionFile>, Failure> {
    let mut wait = Waiting::new(
        format!("waiting for node {:?} to hash {p}'s closed files", old.id),
        timeout,
        format!(
            "The map still gives {p} to node {:?}, closed there: run the move again to go on \
             waiting, or phdb cluster unfence {p} --node {} to serve it there again",
            old.id, old.args.node
        ),
    );
    loop {
        let lp = wait
            .ask(old.local(p))
            .await??
            .ok_or_else(|| Failure::usage(&format!("node {:?} no longer lists {p}", old.id)))?;
        if lp.state() != LocalPartitionState::Closed {
            return Err(Failure::usage(&format!(
                "partition {p} isn't closed on node {:?} any more ({}): something reopened it; \
                 run the move again",
                old.id,
                super::cli::state_text(&lp)
            )));
        }
        if !lp.files.is_empty() {
            if !lp.ok {
                // Its database's own close reported an error: its files may not be whole.
                return Err(Failure::usage(&format!(
                    "partition {p} closed on node {:?}, but: {}. Check it; phdb cluster \
                     unfence {p} and close it again to retry",
                    old.id, lp.error
                )));
            }
            return Ok(lp.files);
        }
        if lp.error.contains("close it again") {
            wait.ask(old.call(p, "close")).await??;
        }
        wait.tick(&said(&lp)).await?;
    }
}

/// The upload id of file `f` of partition `p`'s copy at epoch `e`: the same for a rerun,
/// and another for different bytes.
fn upload_id(cluster: &str, p: &str, e: u64, f: &PartitionFile) -> String {
    let mut h = Sha256::new();
    for part in [cluster.as_bytes(), p.as_bytes(), f.name.as_bytes()] {
        h.update((part.len() as u64).to_le_bytes());
        h.update(part);
    }
    h.update(e.to_le_bytes());
    h.update(&f.sha256);
    h.finalize()[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The closed set a move copies: partition `p`'s files at epoch `e` in `cluster`.
pub(super) struct Set<'a> {
    pub(super) cluster: &'a str,
    pub(super) p: &'a str,
    pub(super) e: u64,
    pub(super) files: &'a [PartitionFile],
}

/// Copies file `f` of `set` from OLD to an upload on NEW, from what NEW holds of it. Either
/// node sending or taking nothing for `stall`, its connection up, fails the copy. Returns
/// the bytes sent.
pub(super) async fn copy_file(
    old: &At,
    new: &At,
    set: &Set<'_>,
    f: &PartitionFile,
    stall: Duration,
) -> Result<u64, Failure> {
    let Set { cluster, p, e, .. } = *set;
    let what = format!("{p}'s file {:?}", f.name);
    let header = PartitionUploadHeader {
        partition: p.to_owned(),
        cluster_id: cluster.to_owned(),
        epoch: e,
        upload_id: upload_id(cluster, p, e, f),
        purpose: PartitionUploadPurpose::Relocate as i32,
        file: Some(f.clone()),
        manifest: set.files.to_vec(),
    };
    let download = |from: u64, mut feed: Feed| {
        let what = &what;
        async move {
            // One pass, with every wait bounded: OLD sends a closed file without pause, so
            // `stall` of nothing is a stalled stream, and the operator runs the move again.
            let stalled = |_| {
                Failure::usage(&format!(
                    "node {:?} sent nothing of {what} for {} s: run the move again to resume",
                    old.id,
                    stall.as_secs()
                ))
            };
            let mut admin = old.admin.clone();
            let asked = admin.backup_download(request(
                BackupDownloadRequest {
                    partition: p.to_owned(),
                    fenced_file: Some(f.name.clone()),
                    offset: from,
                    ..Default::default()
                },
                old.token.as_deref(),
            )?);
            let mut chunks = tokio::time::timeout(stall, asked)
                .await
                .map_err(stalled)?
                .map_err(|s| old.failed(&format!("download {what}"), &s))?
                .into_inner();
            loop {
                let Some(chunk) = tokio::time::timeout(stall, chunks.message())
                    .await
                    .map_err(stalled)?
                    .map_err(|s| old.failed(&format!("download {what}"), &s))?
                else {
                    return Err(Failure::usage(&format!(
                        "node {:?}'s download of {what} ended early: run the move again to resume",
                        old.id
                    )));
                };
                if !chunk.data.is_empty() && chunk.offset != feed.at() {
                    return Err(Failure::usage(&format!(
                        "node {:?} sent {what} from {} where {} was expected",
                        old.id,
                        chunk.offset,
                        feed.at()
                    )));
                }
                if feed.send(chunk.data).await.is_err() {
                    // NEW ended the upload. That isn't the download's error: NEW's answer
                    // says why, and `upload_file` reports it.
                    return Ok(feed.at() - from);
                }
                // The last chunk carries the hash. An empty file's has it too, with a
                // total of 0 (a crash can leave an empty WAL stream file in a set).
                if !chunk.sha256.is_empty() {
                    if chunk.total != f.bytes || chunk.sha256 != f.sha256 {
                        return Err(Failure::usage(&format!(
                            "node {:?} sent {what} as {} bytes with another SHA-256 than its \
                             close listed ({} bytes): close it again and rerun the move",
                            old.id, chunk.total, f.bytes
                        )));
                    }
                    return Ok(feed.at() - from);
                }
            }
        }
    };
    let pass = upload_file(
        &mut new.admin.clone(),
        new.token.as_deref(),
        header,
        stall,
        download,
    )
    .await;
    let (sent, reply) = match pass {
        // NEW holds the whole file, or is installing the set an earlier run staged: nothing
        // is sent. The set it installs is compared with the closed set like any other.
        Ok(None) => return Ok(0),
        Ok(Some(done)) => done,
        Err(Stopped::Request(f) | Stopped::Source(f)) => return Err(f),
        Err(Stopped::Status(s)) => {
            return Err(new.failed(&format!("upload status of {what}"), &s));
        }
        // NEW has the connection up and reads nothing: bounded like OLD's side (#1950).
        Err(Stopped::Stalled) => {
            return Err(Failure::usage(&format!(
                "node {:?} took nothing of {what} for {} s: run the move again to resume",
                new.id,
                stall.as_secs()
            )));
        }
        // NEW took every byte and never answered (#1983).
        Err(Stopped::Unanswered(bound)) => {
            return Err(Failure::usage(&format!(
                "node {:?} took {what} and gave no answer for {} s: run the move again to \
                 resume",
                new.id,
                bound.as_secs()
            )));
        }
        Err(Stopped::Upload(s)) => return Err(new.failed(&format!("upload {what}"), &s)),
    };
    if !reply.complete {
        return Err(Failure::usage(&format!(
            "node {:?} holds {} of {what}'s {} bytes, not complete: run the move again to \
             resume",
            new.id, reply.received, f.bytes
        )));
    }
    Ok(sent)
}

/// Stamps NEW from `e` to `next` ([`super::promote::next_epoch`]): the set it installed,
/// none if it found no set staged.
async fn stamp(
    new: &At,
    cluster: &str,
    p: &str,
    e: u64,
    next: u64,
) -> Result<Vec<PartitionFile>, Failure> {
    let (mut c, _) = connect(&new.args, &new.args.node).await?;
    Ok(c.stamp(request(
        proto::StampRequest {
            cluster_id: cluster.to_owned(),
            partition: p.to_owned(),
            from_epoch: e,
            to_epoch: next,
            restore: false,
            restore_from_cluster_id: String::new(),
            promote: false,
            replica_epoch: 0,
        },
        new.token.as_deref(),
    )?)
    .await
    .map_err(|s| new.failed("stamp", &s))?
    .into_inner()
    .files)
}

/// Whether the set NEW installed is OLD's closed set: the same names, each with the same
/// size and SHA-256. The difference otherwise.
pub(super) fn compare(installed: &[PartitionFile], closed: &[PartitionFile]) -> Result<(), String> {
    let key = |f: &PartitionFile| (f.name.clone(), f.bytes, f.sha256.clone());
    let mut a: Vec<_> = installed.iter().map(key).collect();
    let mut b: Vec<_> = closed.iter().map(key).collect();
    a.sort();
    b.sort();
    if a == b {
        return Ok(());
    }
    let names = |v: &[(String, u64, Bytes)], w: &[(String, u64, Bytes)]| -> String {
        v.iter()
            .filter(|x| !w.contains(x))
            .map(|(n, bytes, _)| format!("{n:?} ({bytes} bytes)"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    Err(format!(
        "installed but not closed: [{}]; closed but not installed: [{}]",
        names(&a, &b),
        names(&b, &a)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paused() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap()
    }

    #[test]
    fn a_wait_gives_up_at_its_limit_naming_the_step_and_the_last_answer() {
        paused().block_on(async {
            let start = tokio::time::Instant::now();
            let mut wait = Waiting::new(
                "waiting for node \"c\" to serve p1 at epoch 2".into(),
                Some(Duration::from_millis(1100)),
                "The map already gives p1 to node \"c\"".into(),
            );
            let mut ticks = 0;
            let e = loop {
                match wait.tick("opening").await {
                    Ok(()) => ticks += 1,
                    Err(e) => break e,
                }
                assert!(ticks < 100, "never gave up");
            };
            // Polled every 200 ms, and with less than two polls left the last sleep runs to
            // the limit (at 800 ms, 300 ms left): given up at it, not at the next poll.
            assert_eq!(start.elapsed(), Duration::from_millis(1100));
            assert_eq!(ticks, 5);
            assert_eq!(e.status(), crate::ExitStatus::ERROR);
            assert_eq!(
                e.message(),
                "gave up waiting for node \"c\" to serve p1 at epoch 2 after 1.1s (--timeout): \
                 opening. The map already gives p1 to node \"c\""
            );
        });
    }

    #[test]
    fn a_call_that_never_answers_ends_the_wait_at_its_limit() {
        paused().block_on(async {
            let start = tokio::time::Instant::now();
            let mut wait = Waiting::new("waiting".into(), Some(Duration::from_secs(3)), "".into());
            wait.tick("closed").await.unwrap();
            let e = tokio::select! {
                r = wait.ask(std::future::pending::<()>()) => r.expect_err("given up"),
                () = tokio::time::sleep(Duration::from_secs(60)) => panic!("never gave up"),
            };
            assert_eq!(start.elapsed(), Duration::from_secs(3));
            assert!(
                e.message().contains("the node didn't answer"),
                "{}",
                e.message()
            );
            // A call that answers gives its own result, an error included.
            let mut wait = Waiting::new("waiting".into(), Some(Duration::from_secs(3)), "".into());
            let r = wait
                .ask(async { Err::<(), _>(Failure::usage("refused")) })
                .await
                .expect("not given up");
            assert_eq!(r.unwrap_err().message(), "refused");
        });
    }

    #[test]
    fn a_wait_that_slept_to_its_limit_gives_up_with_the_node_s_last_answer() {
        // The last poll's sleep ends at the limit: the next call isn't made, and the error
        // keeps what the node said, not that it didn't answer.
        paused().block_on(async {
            let mut wait = Waiting::new(
                "waiting".into(),
                Some(Duration::from_millis(500)),
                "".into(),
            );
            wait.tick("opening").await.unwrap();
            wait.tick("opening: retrying").await.unwrap();
            let e = wait
                .ask(async {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    Ok::<_, Failure>(None::<LocalPartition>)
                })
                .await
                .expect_err("no time left");
            assert_eq!(
                e.message(),
                "gave up waiting after 500ms (--timeout): opening: retrying. "
            );
        });
    }

    #[test]
    fn a_wait_never_leaves_its_last_call_a_sliver_of_the_limit() {
        // #2015: a node that takes 70 ms to answer, polled every 200 ms. A poll at 2.97 s
        // would leave its call 30 ms of a 3 s limit, too little, so the wait would end on
        // "the node didn't answer". Instead the last poll sleeps to the limit, and the wait
        // ends on what the node said.
        paused().block_on(async {
            let start = tokio::time::Instant::now();
            let mut wait = Waiting::new("waiting".into(), Some(Duration::from_secs(3)), "".into());
            let e = loop {
                let answered = wait
                    .ask(async {
                        tokio::time::sleep(Duration::from_millis(70)).await;
                        "serving at epoch 1"
                    })
                    .await;
                let now = match answered {
                    Ok(now) => now,
                    Err(e) => break e,
                };
                if let Err(e) = wait.tick(now).await {
                    break e;
                }
            };
            assert_eq!(start.elapsed(), Duration::from_secs(3));
            assert_eq!(
                e.message(),
                "gave up waiting after 3s (--timeout): serving at epoch 1. "
            );
        });
    }

    #[test]
    fn a_timeout_of_zero_is_refused() {
        let parse = |t: &str| {
            <crate::args::Cli as clap::Parser>::try_parse_from([
                "phdb",
                "cluster",
                "move",
                "p1",
                "--to",
                "c",
                "--node",
                "http://n:1",
                "--timeout",
                t,
            ])
        };
        assert!(
            parse("0").is_err(),
            "--timeout 0 would give up without asking"
        );
        assert!(parse("1").is_ok());
    }

    #[test]
    fn without_a_limit_a_wait_goes_on() {
        paused().block_on(async {
            let mut wait = Waiting::new("waiting".into(), None, "".into());
            for _ in 0..1000 {
                wait.tick("opening").await.unwrap();
            }
            let slow = async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                7
            };
            assert_eq!(wait.ask(slow).await.unwrap(), 7);
        });
    }

    fn file(name: &str, bytes: u64, sha: u8) -> PartitionFile {
        PartitionFile {
            name: name.into(),
            bytes,
            sha256: vec![sha; 32].into(),
        }
    }

    #[test]
    fn the_installed_set_must_be_the_closed_set_file_by_file() {
        let closed = [file("", 10, 1), file("-wal-0", 20, 2), file("-feed", 30, 3)];
        // Any order.
        let mut installed = closed.to_vec();
        installed.reverse();
        assert!(compare(&installed, &closed).is_ok());
        // A different hash, a different size, a missing or an extra file: each refused.
        for bad in [
            vec![file("", 10, 1), file("-wal-0", 20, 9), file("-feed", 30, 3)],
            vec![file("", 11, 1), file("-wal-0", 20, 2), file("-feed", 30, 3)],
            vec![file("", 10, 1), file("-wal-0", 20, 2)],
            vec![
                file("", 10, 1),
                file("-wal-0", 20, 2),
                file("-feed", 30, 3),
                file("-wal-1", 1, 4),
            ],
        ] {
            let why = compare(&bad, &closed).unwrap_err();
            assert!(why.contains("installed but not closed"), "{why}");
        }
        let why = compare(&[file("", 10, 1)], &closed).unwrap_err();
        assert!(
            why.contains("\"-feed\" (30 bytes)"),
            "names what's missing: {why}"
        );
    }

    #[test]
    fn an_upload_id_is_the_same_for_a_rerun_and_another_for_other_bytes() {
        let f = file("-wal-0", 20, 2);
        let id = upload_id("c1", "p1", 4, &f);
        assert_eq!(id.len(), 32);
        assert!(
            id.bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        );
        assert_eq!(id, upload_id("c1", "p1", 4, &f));
        for other in [
            upload_id("c2", "p1", 4, &f),
            upload_id("c1", "p2", 4, &f),
            upload_id("c1", "p1", 5, &f),
            upload_id("c1", "p1", 4, &file("-wal-1", 20, 2)),
            upload_id("c1", "p1", 4, &file("-wal-0", 20, 3)),
        ] {
            assert_ne!(id, other);
        }
    }
}

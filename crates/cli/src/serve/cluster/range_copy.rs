//! D331 PR 4b: a range move's copy, the source's side (`Cluster.Forward`'s `range_copy`).
//!
//! The move's target asks for its range chunk by chunk; the source answers each from a
//! fresh snapshot, as exactly what a compaction would write for it there
//! ([`pigeonhole::Pigeonhole::copy_range`]). The first chunk takes the move's feed retention
//! hold, so the target's catch-up finds the source's history from that chunk's token however
//! long the copy takes, and the move's follower slot (`--max-move-followers`).
//!
//! - **Who's served** (cli692's M3): only this node as the source partition's owner in its
//!   map, for a range move (never a whole partition's, D332's NODE) naming it as `from`, in
//!   COPYING.
//! - **The rate:** every move's copy reads on this node share one token bucket at
//!   `--move-rate-bytes` (cli692): it's the node's copy bandwidth.
//! - **Release** (cli692's M2 and should): the hold and the slot go when the move leaves
//!   COPYING through HANDED_OVER (the target catches up to `final_seqno` then) or the map, when this node stops owning the source, or
//!   when the target stops asking for [`IDLE`]. A lapsed hold is reported in the source's
//!   `MoveStatus.retention_lapsed`, and the move must be aborted (a restart of the source
//!   loses the hold too, so the target's resume finds history gone).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::serve::errors::ApiError;
use crate::serve::proto::{self, MovePhase, RangeCopyCell, RangeCopyRequest, RangeCopyResponse};
use crate::serve::router::Cluster;

/// A target that hasn't asked for this long has stopped: its hold and slot go.
const IDLE: Duration = Duration::from_secs(120);

/// The least a chunk reads on the source (perf287's cost review): a read and a reopen of
/// each family's sources per chunk is paid back over this much.
const MIN_READ: u64 = 2 << 20;

/// Per family, what a chunk reads at least.
const PER_FAMILY: u64 = 128 << 10;

/// The range copies this node serves as a source, and its copy bandwidth.
#[derive(Debug)]
pub(crate) struct Sources {
    copies: Mutex<HashMap<String, Source>>,
    bucket: Mutex<Bucket>,
    /// `--move-rate-bytes`: bytes a second.
    rate: u64,
    /// `--move-retention-cap`: the most history a move's hold keeps.
    cap: u64,
    /// `--max-move-followers`: moves copying from this node at once.
    max_followers: usize,
    /// Copies refused for a full `--max-move-followers` (a metric).
    pub(crate) refused: AtomicU64,
}

/// One move's copy served here.
#[derive(Debug)]
struct Source {
    partition: String,
    hold: pigeonhole::replication::FeedRetentionHold,
    /// The source's feed token at the first chunk's snapshot.
    token: Vec<u8>,
    asked: Instant,
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    at: Instant,
}

impl Sources {
    pub(crate) fn new(rate: u64, cap: u64, max_followers: usize) -> Self {
        Self {
            copies: Mutex::default(),
            bucket: Mutex::new(Bucket {
                tokens: rate as f64,
                at: Instant::now(),
            }),
            rate: rate.max(1),
            cap,
            max_followers: max_followers.max(1),
            refused: AtomicU64::new(0),
        }
    }

    /// How long a read of `bytes` must wait to keep this node's copies at the rate.
    fn take(&self, bytes: u64) -> Duration {
        let mut b = self.bucket.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        let rate = self.rate as f64;
        b.tokens = (b.tokens + now.duration_since(b.at).as_secs_f64() * rate).min(rate);
        b.at = now;
        b.tokens -= bytes as f64;
        if b.tokens >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-b.tokens / rate)
        }
    }

    /// `pigeonhole_cluster_move_copies_refused_total`: range copies refused here for a full
    /// `--max-move-followers`.
    pub(crate) fn render_metrics(&self, text: &mut String) {
        use std::fmt::Write as _;
        let _ = writeln!(
            text,
            "# HELP pigeonhole_cluster_move_copies_refused_total Range move copies this node \
             refused as their source for a full --max-move-followers (D331).\n# TYPE \
             pigeonhole_cluster_move_copies_refused_total counter\n\
             pigeonhole_cluster_move_copies_refused_total {}",
            self.refused.load(Ordering::Relaxed)
        );
    }

    /// Move `id`'s target is still reading (its catch-up stream, which asks for no chunks):
    /// its hold and slot don't go idle.
    pub(crate) fn touch(&self, id: &str) {
        if let Some(s) = self
            .copies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(id)
        {
            s.asked = Instant::now();
        }
    }

    /// Whether move `id`'s hold here lapsed (`MoveStatus.retention_lapsed`).
    pub(crate) fn lapsed(&self, id: &str) -> bool {
        self.copies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .is_some_and(|s| s.hold.lapsed())
    }

    /// Drops the holds and slots of copies this node no longer serves: the move left
    /// COPYING through HANDED_OVER, or the map, or names another source; this node no
    /// longer owns the source, or owns it at another epoch than `from_epoch` (M2); or the
    /// target stopped asking ([`IDLE`]). Each map
    /// install calls it.
    pub(crate) fn release_gone(&self, map: &proto::ClusterMap, me: &str) {
        let now = Instant::now();
        self.copies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|id, s| {
                // Owned here, at the epoch the move began copying at (0: not recorded).
                let owned = |m: &proto::RangeMove| {
                    map.partitions.iter().any(|p| {
                        p.id == s.partition
                            && p.node == me
                            && (m.from_epoch == 0 || p.epoch == m.from_epoch)
                    })
                };
                let live = map.moves.iter().any(|m| {
                    &m.id == id
                        && !super::map::node_move(m)
                        && m.from == s.partition
                        && (MovePhase::Copying..=MovePhase::HandedOver)
                            .contains(&super::map::phase(m))
                        && owned(m)
                });
                live && now.duration_since(s.asked) < IDLE
            });
    }

    /// One chunk of move `r.move_id`'s copy from `partition` (this node's).
    pub(crate) async fn serve(
        &self,
        cluster: &Cluster,
        partition: &str,
        r: RangeCopyRequest,
    ) -> Result<RangeCopyResponse, ApiError> {
        let map = cluster.node.get_map()?;
        let Some(m) = map
            .moves
            .iter()
            .find(|m| m.id == r.move_id && !super::map::node_move(m) && m.from == partition)
        else {
            return Err(ApiError::wrong_partition(
                map.map_version,
                format!(
                    "no range move {:?} from partition {partition:?} here",
                    r.move_id
                ),
            ));
        };
        if super::map::phase(m) != MovePhase::Copying || m.table != r.table {
            return Err(ApiError::failed_precondition(format!(
                "range move {:?} isn't copying table {:?} (it's at {:?})",
                m.id,
                r.table,
                super::map::phase(m)
            )));
        }
        // M3: this node owns the source in its map, and has it open. A source whose epoch
        // moved since COPYING (a failover mid-move) serves no more chunks: `from_epoch`, 0
        // on a record written before it existed (then not compared).
        let Some(p) = map.partitions.iter().find(|p| p.id == partition) else {
            return Err(ApiError::wrong_partition(
                map.map_version,
                format!("no partition {partition:?} in the map"),
            ));
        };
        if m.from_epoch != 0 && p.epoch != m.from_epoch {
            return Err(ApiError::failed_precondition(format!(
                "range move {:?}'s source {partition:?} is at epoch {}, not {} as it was when \
                 copying began",
                m.id, p.epoch, m.from_epoch
            )));
        }
        let owned = p.node == cluster.node.id();
        let Some(service) = cluster.here(partition).filter(|_| owned) else {
            return Err(ApiError::wrong_partition(
                map.map_version,
                format!("partition {partition:?} isn't served here"),
            ));
        };
        let db = service.part()?.db.clone();
        let table = db.table(&m.table)?.open()?;
        let bound = |k: &Option<proto::SplitKey>| {
            k.as_ref()
                .map(|k| super::map::split_key_bytes(k).map_err(ApiError::internal))
                .transpose()
        };
        let (start, end) = (bound(&m.start)?.unwrap_or_default(), bound(&m.end)?);
        let after = if r.after[..] > start[..] {
            r.after.to_vec()
        } else {
            start
        };
        let first = r.after.is_empty();
        let token = if first {
            self.begin(&m.id, partition, &db, &table)?
        } else {
            Vec::new()
        };
        if let Some(s) = self
            .copies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(&m.id)
        {
            s.asked = Instant::now();
        }
        let families = table.families().len() as u64;
        let table = table.clone();
        let read = r.max_bytes.max(MIN_READ).max(families * PER_FAMILY);
        let chunk = tokio::task::spawn_blocking(move || {
            let snap = db.snapshot()?;
            let chunk = db.copy_range(&snap, &table, &after, end.as_deref(), read as usize)?;
            Ok::<_, pigeonhole::Error>((snap.seqno(), chunk))
        })
        .await
        .map_err(|e| ApiError::internal(format!("a range copy's read: {e}")))?;
        let (snapshot_seqno, chunk) = chunk?;
        let bytes: u64 = chunk
            .cells
            .iter()
            .map(|c| (c.row.len() + c.qualifier.len() + c.stored.len()) as u64)
            .sum();
        let wait = self.take(bytes);
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        Ok(RangeCopyResponse {
            cells: chunk
                .cells
                .into_iter()
                .map(|c| RangeCopyCell {
                    family: c.family,
                    kind: logged_kind(c.kind),
                    row: c.row.into(),
                    qualifier: c.qualifier.into(),
                    timestamp: c.ts,
                    stored: c.stored.into(),
                })
                .collect(),
            next: chunk.next.map(Into::into),
            snapshot_seqno,
            token: token.into(),
        })
    }

    /// A copy's first chunk: the move's hold and slot, then its token at a snapshot taken
    /// after the hold (so the hold covers it). Asked again (a target that restarted), the
    /// move keeps its hold and token.
    fn begin(
        &self,
        id: &str,
        partition: &str,
        db: &pigeonhole::Pigeonhole,
        table: &pigeonhole::Table,
    ) -> Result<Vec<u8>, ApiError> {
        let mut copies = self.copies.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(s) = copies.get(id) {
            return Ok(s.token.clone());
        }
        if copies.len() >= self.max_followers {
            self.refused.fetch_add(1, Ordering::Relaxed);
            return Err(ApiError::overloaded(format!(
                "this node serves {} range copies already (--max-move-followers): move {id:?} \
                 waits for one to finish",
                copies.len()
            )));
        }
        let hold = db.hold_feed_retention(self.cap)?;
        let snap = db.snapshot()?;
        let token = table
            .feed()
            .from_snapshot(&snap)
            .open()?
            .token()
            .as_bytes()
            .to_vec();
        copies.insert(
            id.to_owned(),
            Source {
                partition: partition.to_owned(),
                hold,
                token: token.clone(),
                asked: Instant::now(),
            },
        );
        Ok(token)
    }
}

/// A change kind's logged number (FORMAT §2), as `RangeCopyCell.kind` and
/// `RawChange.kind` carry it.
pub(super) fn logged_kind(k: pigeonhole::ChangeKind) -> u32 {
    use pigeonhole::ChangeKind as K;
    match k {
        K::Put => 1,
        K::Merge => 2,
        K::DeleteCell => 3,
        K::DeleteColumn => 4,
        K::DeleteFamily => 5,
        _ => 0,
    }
}

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

/// The most a chunk waits for the node's rate: past it, the chunk is refused `Overloaded`.
const MAX_WAIT: Duration = Duration::from_secs(2);

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
    /// Moves whose target asked for a later chunk with no hold here (this node restarted,
    /// or the target went idle): reported lapsed until the map drops them (cli692's S4 on
    /// #2107).
    lost: Mutex<std::collections::HashSet<String>>,
    /// Whether the idle sweep runs ([`Sources::sweep_idle`]).
    sweeping: std::sync::atomic::AtomicBool,
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
            lost: Mutex::default(),
            sweeping: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// `--move-rate-bytes`, `--move-retention-cap` and `--max-move-followers`, as this node
    /// reports them for a move's plan (D331 PR 5).
    pub(crate) fn limits(&self) -> (u64, u64, usize) {
        (self.rate, self.cap, self.max_followers)
    }

    /// Takes `bytes` from the bucket (negative gives back), and how long the read must wait
    /// to keep the node at its rate; or, past `max_wait` of waiting, takes nothing and says
    /// how long (cli692 on #2107: a chunk is refused `Overloaded` rather than run up a debt
    /// a capped sleep never pays).
    fn reserve(&self, bytes: f64, max_wait: Duration) -> Result<Duration, Duration> {
        self.reserve_at(bytes, max_wait, Instant::now())
    }

    /// [`reserve`](Self::reserve) at `now`. The bucket fills to a second's worth, or to the
    /// read when that's more (cli692 on #2107): else a read over three seconds' worth (a
    /// 2 MiB read under about 683 KiB/s) would wait past `max_wait` from a full bucket, and
    /// every chunk be refused for good. A big read at a slow rate then waits until its bytes
    /// have built up, and the rate holds over time.
    fn reserve_at(
        &self,
        bytes: f64,
        max_wait: Duration,
        now: Instant,
    ) -> Result<Duration, Duration> {
        let mut b = self.bucket.lock().unwrap_or_else(PoisonError::into_inner);
        let rate = self.rate as f64;
        let cap = rate.max(bytes);
        b.tokens = (b.tokens + now.saturating_duration_since(b.at).as_secs_f64() * rate).min(cap);
        b.at = now;
        let left = b.tokens - bytes;
        let wait = if left >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-left / rate)
        };
        if wait > max_wait {
            return Err(wait);
        }
        b.tokens = left;
        Ok(wait)
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

    /// Whether move `id`'s hold here lapsed (`MoveStatus.retention_lapsed`): past its cap,
    /// or lost (its target asked for a later chunk with none here).
    pub(crate) fn lapsed(&self, id: &str) -> bool {
        self.copies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .is_some_and(|s| s.hold.lapsed())
            || self
                .lost
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains(id)
    }

    /// Drops the copies whose target stopped asking ([`IDLE`]), between map installs too
    /// (cli692's S3 on #2107): a target's node that died in a quiet cluster leaves no map
    /// to release them. Runs while any copy is held here.
    fn release_idle(&self) -> bool {
        let now = Instant::now();
        let mut copies = self.copies.lock().unwrap_or_else(PoisonError::into_inner);
        copies.retain(|_, s| now.duration_since(s.asked) < IDLE);
        !copies.is_empty()
    }

    /// Starts the idle sweep if it isn't running: every quarter of [`IDLE`], until no copy
    /// is held.
    fn sweep_idle(self: &std::sync::Arc<Self>) {
        if self.sweeping.swap(true, Ordering::AcqRel) {
            return;
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            self.sweeping.store(false, Ordering::Release);
            return;
        };
        let me = std::sync::Arc::downgrade(self);
        rt.spawn(async move {
            loop {
                tokio::time::sleep(IDLE / 4).await;
                let Some(me) = me.upgrade() else { return };
                if !me.release_idle() {
                    me.sweeping.store(false, Ordering::Release);
                    // A copy begun since the check starts it again.
                    if me
                        .copies
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .is_empty()
                        || me.sweeping.swap(true, Ordering::AcqRel)
                    {
                        return;
                    }
                }
            }
        });
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
                            && super::map::source_epoch_holds(m, p.epoch)
                    })
                };
                let live = map.moves.iter().any(|m| {
                    &m.id == id
                        && !super::map::node_move(m)
                        && m.from == s.partition
                        && super::map::follows(super::map::phase(m))
                        && owned(m)
                });
                live && now.duration_since(s.asked) < IDLE
            });
        self.lost
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|id| map.moves.iter().any(|m| &m.id == id));
    }

    /// One chunk of move `r.move_id`'s copy from `partition` (this node's).
    pub(crate) async fn serve(
        self: &std::sync::Arc<Self>,
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
        if !super::map::source_epoch_holds(m, p.epoch) {
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
        // cli692's S1 on #2107: a stale owner whose lease ran out serves nothing, before or
        // after its read.
        if let Some(g) = service.gate() {
            g.serving()?;
        }
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
            let token = self.begin(&m.id, partition, &db, &table)?;
            self.sweep_idle();
            token
        } else {
            // A later chunk with no hold here (cli692's S4 on #2107): this node restarted,
            // or the target was idle past [`IDLE`]. The source's history from the copy's
            // first token may be gone, so the move can't catch up: refuse, and report it.
            let held = self
                .copies
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains_key(&m.id);
            if !held {
                self.lost
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(m.id.clone());
                return Err(ApiError::move_lapsed(format!(
                    "range move {:?}'s source holds no feed retention for it (the source's node \
                     restarted, or its target stopped asking): abort the move",
                    m.id
                )));
            }
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
        // The node's rate, for the read's worth before it's read: past [`MAX_WAIT`] of debt,
        // `Overloaded` (retry-safe; the target backs off), so the rate holds however slow.
        let wait = self.reserve(read as f64, MAX_WAIT).map_err(|w| {
            ApiError::overloaded(format!(
                "this node's range copies are at --move-rate-bytes: ask again in about {} ms",
                w.as_millis()
            ))
        })?;
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
        if let Some(g) = service.gate() {
            g.serving()?;
        }
        // What was read, not the estimate: the rest back in the bucket (or more out of it).
        let _ = self.reserve(bytes as f64 - read as f64, Duration::MAX);
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
        // The hold first, then the snapshot: a hold covers the tokens of snapshots opened
        // after it, never one opened before (simp724 on #2109).
        let hold = db.hold_feed_retention(self.cap)?;
        let snap = db.snapshot()?;
        debug_assert!(
            !hold.lapsed(),
            "the copy's hold is live at its first snapshot"
        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole::{Family, FeedRetention, Options, Pigeonhole};

    /// A partition's database with a change feed, in small WAL segments so a few
    /// megabytes of writes pass its retention.
    fn db(name: &str) -> (Pigeonhole, pigeonhole::Table) {
        let dir = crate::test_dir::fresh(&format!("phdb-range-copy-{name}-"));
        let db = Pigeonhole::open(
            dir.join("p.phdb"),
            Options::default()
                .create_if_missing(true)
                .shards(1)
                .wal_segment_size(256 << 10)
                .change_feed(FeedRetention::bytes(1 << 20)),
        )
        .unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("f", Family::default())
            .create()
            .unwrap();
        (db, t)
    }

    #[test]
    fn a_full_follower_cap_refuses_a_new_moves_copy_and_counts_it() {
        let (db, t) = db("cap");
        let s = Sources::new(1 << 20, u64::MAX, 2);
        let first = s.begin("m1", "p1", &db, &t).unwrap();
        s.begin("m2", "p1", &db, &t).unwrap();
        let e = s.begin("m3", "p1", &db, &t).unwrap_err();
        assert_eq!(e.code, "Overloaded", "{}", e.message);
        assert!(e.message.contains("--max-move-followers"), "{}", e.message);
        // A target that restarted asks again: its move keeps its slot and its token.
        assert_eq!(s.begin("m1", "p1", &db, &t).unwrap(), first);
        assert_eq!(s.refused.load(Ordering::Relaxed), 1);
        let mut text = String::new();
        s.render_metrics(&mut text);
        assert!(
            text.contains("pigeonhole_cluster_move_copies_refused_total 1"),
            "{text}"
        );
        drop(s);
        db.close().unwrap();
    }

    #[test]
    fn a_read_bigger_than_the_rate_allows_is_taken_once_its_bytes_built_up() {
        // cli692 on #2107: at 512 KiB/s, a 2 MiB read (the least a chunk reads) is refused
        // at first, and taken once enough time has passed, never refused for good.
        let s = Sources::new(512 << 10, u64::MAX, 2);
        let read = f64::from(2 << 20);
        let start = Instant::now();
        assert!(
            s.reserve_at(read, MAX_WAIT, start).is_err(),
            "3 s owed from a full bucket"
        );
        let taken = (1..=8u64)
            .map(|half| start + Duration::from_millis(500 * half))
            .find(|at| s.reserve_at(read, MAX_WAIT, *at).is_ok())
            .expect("taken within 4 s of asking");
        assert!(
            taken - start <= Duration::from_secs(4),
            "{:?}",
            taken - start
        );
    }

    #[test]
    fn the_nodes_moves_share_one_bucket() {
        // cli692's throttle: `--move-rate-bytes` is the node's, across its moves. A second's
        // worth taken by one leaves the next read, of any move, a second's wait.
        let s = Sources::new(1 << 20, u64::MAX, 2);
        let mib = f64::from(1 << 20);
        assert_eq!(
            s.reserve(mib, MAX_WAIT),
            Ok(Duration::ZERO),
            "a full bucket"
        );
        let wait = s.reserve(mib, MAX_WAIT).unwrap();
        assert!(
            wait > Duration::from_millis(900) && wait <= Duration::from_secs(1),
            "{wait:?}"
        );
        // Past `MAX_WAIT` of debt, a read is refused and takes nothing (cli692 on #2107).
        assert!(s.reserve(4.0 * mib, MAX_WAIT).is_err());
        let again = s.reserve(mib, MAX_WAIT).unwrap();
        assert!(
            again <= Duration::from_secs(2) + Duration::from_millis(50),
            "{again:?}"
        );
    }

    #[test]
    fn a_hold_past_its_cap_is_reported_lapsed() {
        let (db, t) = db("lapse");
        let s = Sources::new(1 << 20, 1, 2);
        s.begin("m1", "p1", &db, &t).unwrap();
        assert!(!s.lapsed("m1"));
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut i = 0u32;
        // The cap is enforced by the feed's maintenance, as checkpoints move: values the log
        // keeps inline, flushed as they go (the engine's own lapse test writes so).
        while !s.lapsed("m1") && Instant::now() < deadline {
            for _ in 0..40 {
                t.mutate(format!("r{i}").as_bytes())
                    .put("f", b"q", &[7u8; 20_000])
                    .commit()
                    .unwrap();
                i += 1;
            }
            db.flush().unwrap();
        }
        assert!(s.lapsed("m1"), "the move's hold lapsed past its cap");
        assert!(!s.lapsed("m2"), "no such move");
        drop(s);
        db.close().unwrap();
    }
}

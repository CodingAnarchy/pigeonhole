//! A partition owner's table of its replicas' acknowledgements (D335 PR 1, ICR 0146).
//!
//! A cluster partition's map replica that follows through `Replication.FollowAcked` sends an
//! `Ack` after each apply that stored its token: everything at or before the position is
//! applied and durable in its copy. The owner keeps the last position per replica node, for
//! `Cluster.Status` (`LocalPartition.acked_position`, `.healthy_replicas`) and `/metrics`
//! (`pigeonhole_cluster_replica_acked_seqno`, and `pigeonhole_cluster_replica_ack_wait_seconds`
//! from a write's commit to its acknowledgement).
//!
//! **No write waits here yet:** D335 PR 2 adds the map's `replica_acks` (k), the wait and
//! its timeout. Until then the frontier is the highest position any counted replica
//! acknowledged (k = 1), and the wait histogram is measured to that first acknowledgement.
//!
//! The table lives in the serve layer, never in the engine: a node with no `FollowAcked`
//! stream pays one relaxed load per write ([`ReplicaAcks::committed`]).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use super::proto;

/// A position in a partition's replicated feed (`pigeonhole.v1.Position`): the last commit
/// at or below `seqno`, then `n` more events that aren't commits. Ordered as `(seqno, n)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub(crate) struct Pos {
    pub(crate) seqno: u64,
    pub(crate) n: u32,
}

impl Pos {
    /// A commit's position.
    pub(crate) fn commit(seqno: u64) -> Self {
        Self { seqno, n: 0 }
    }
}

impl From<&proto::Position> for Pos {
    fn from(p: &proto::Position) -> Self {
        Self {
            seqno: p.seqno,
            n: p.n,
        }
    }
}

impl From<Pos> for proto::Position {
    fn from(p: Pos) -> Self {
        Self {
            seqno: p.seqno,
            n: p.n,
        }
    }
}

/// The wait histogram's bucket bounds, in microseconds: 100 µs to 10 s, as the request
/// durations'.
const BUCKETS_US: [u64; 16] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000,
];

/// At most this many commits are timed while unacknowledged; a commit past it isn't
/// timed (a replica far behind, or none acknowledging, would otherwise grow this without
/// bound). Counted in `untimed`.
const PENDING_CAP: usize = 1 << 16;

/// One partition's acknowledgements, on its owner.
#[derive(Debug, Default)]
pub(crate) struct ReplicaAcks {
    /// `FollowAcked` streams attached now. Commits are timed only while one is.
    streams: AtomicUsize,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Per replica node: its live stream's generation, and its last acknowledgement (none
    /// before the stream's first). At most one stream per node counts: a new one replaces
    /// the old one's entry.
    nodes: HashMap<String, Entry>,
    next_generation: u64,
    /// Commits returned and not yet acknowledged, by seqno, with when each returned.
    pending: VecDeque<(u64, Instant)>,
    /// The highest position acknowledged by any stream attached now.
    frontier: Option<Pos>,
    /// The wait histogram: per bucket (not cumulative; the last is past the last bound),
    /// the sum in microseconds, and the commits not timed for `PENDING_CAP`.
    waits: [u64; BUCKETS_US.len() + 1],
    wait_sum_us: u64,
    untimed: u64,
}

#[derive(Debug)]
struct Entry {
    generation: u64,
    acked: Option<Pos>,
}

impl ReplicaAcks {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A `FollowAcked` stream from replica `node` begins: it replaces the node's earlier
    /// stream, if any, whose acknowledgements stop counting. The entry goes when the
    /// returned [`Attached`] is dropped (the stream ended).
    pub(crate) fn attach(self: &Arc<Self>, node: &str) -> Attached {
        let mut inner = self.lock();
        inner.next_generation += 1;
        let generation = inner.next_generation;
        if inner
            .nodes
            .insert(
                node.to_owned(),
                Entry {
                    generation,
                    acked: None,
                },
            )
            .is_none()
        {
            self.streams.fetch_add(1, Ordering::Relaxed);
        }
        inner.refresh_frontier();
        Attached {
            acks: Arc::clone(self),
            node: node.to_owned(),
            generation,
        }
    }

    /// A write on this partition returned committed: `seqno` (or a later seqno, read after
    /// the commit) is timed until a replica acknowledges it. One relaxed load while no
    /// stream is attached; `seqno` is read only when one is.
    #[inline]
    pub(crate) fn committed(&self, seqno: impl FnOnce() -> u64) {
        if self.streams.load(Ordering::Relaxed) == 0 {
            return;
        }
        let seqno = seqno();
        let now = Instant::now();
        let mut inner = self.lock();
        if inner.frontier.is_some_and(|f| f >= Pos::commit(seqno)) {
            // Already acknowledged (the commit was visible, and shipped, before it returned).
            inner.observe(0);
            return;
        }
        if inner.pending.len() >= PENDING_CAP {
            inner.untimed += 1;
            return;
        }
        // Commits return out of seqno order: kept sorted, so an acknowledgement drains a
        // prefix. Most arrive in order, so this inserts at the back.
        let at = inner.pending.partition_point(|&(s, _)| s <= seqno);
        inner.pending.insert(at, (seqno, now));
    }

    /// `Cluster.Status`'s `acked_position` and `healthy_replicas`, counting only the
    /// streams of `replicas` (the partition's map replicas now).
    pub(crate) fn status(&self, replicas: &[String]) -> (Option<Pos>, u32) {
        let inner = self.lock();
        let counted = inner
            .nodes
            .iter()
            .filter(|(node, _)| replicas.iter().any(|r| r == *node));
        let mut healthy = 0;
        let mut acked = None;
        for (_, e) in counted {
            healthy += 1;
            acked = acked.max(e.acked);
        }
        (acked, healthy)
    }

    /// `pigeonhole_cluster_replica_ack_wait_seconds{partition}` and
    /// `pigeonhole_cluster_replica_acked_seqno{partition,replica}`. Nothing for a partition
    /// no replica has acknowledged and none follows with acknowledgements now.
    pub(crate) fn render(&self, partition: &str, waits: &mut String, acked: &mut String) {
        use std::fmt::Write as _;
        let inner = self.lock();
        let timed: u64 = inner.waits.iter().sum();
        if timed > 0 {
            let mut seen = 0;
            for (i, n) in inner.waits.iter().enumerate() {
                seen += n;
                let le = BUCKETS_US
                    .get(i)
                    .map_or_else(|| "+Inf".to_owned(), |&b| (b as f64 / 1e6).to_string());
                let _ = writeln!(
                    waits,
                    "pigeonhole_cluster_replica_ack_wait_seconds_bucket{{partition=\"{partition}\",le=\"{le}\"}} {seen}"
                );
            }
            let _ = writeln!(
                waits,
                "pigeonhole_cluster_replica_ack_wait_seconds_sum{{partition=\"{partition}\"}} {}",
                inner.wait_sum_us as f64 / 1e6
            );
            let _ = writeln!(
                waits,
                "pigeonhole_cluster_replica_ack_wait_seconds_count{{partition=\"{partition}\"}} {seen}"
            );
        }
        let mut nodes: Vec<_> = inner
            .nodes
            .iter()
            .filter_map(|(n, e)| e.acked.map(|p| (n, p)))
            .collect();
        nodes.sort();
        for (node, p) in nodes {
            let _ = writeln!(
                acked,
                "pigeonhole_cluster_replica_acked_seqno{{partition=\"{partition}\",replica=\"{node}\"}} {}",
                p.seqno
            );
        }
    }

    /// The `HELP` and `TYPE` lines for [`render`](Self::render)'s two metrics, followed by
    /// what each partition rendered. Nothing when no partition rendered anything.
    pub(crate) fn render_all(text: &mut String, waits: &str, acked: &str) {
        if !waits.is_empty() {
            text.push_str(
                "# HELP pigeonhole_cluster_replica_ack_wait_seconds From a write's commit on \
                 the partition's owner to a replica's acknowledgement of it (D335; to the \
                 first replica's until the map sets how many a write waits for).\n# TYPE \
                 pigeonhole_cluster_replica_ack_wait_seconds histogram\n",
            );
            text.push_str(waits);
        }
        if !acked.is_empty() {
            text.push_str(
                "# HELP pigeonhole_cluster_replica_acked_seqno The seqno of each replica's \
                 last acknowledgement to the partition's owner (D335).\n# TYPE \
                 pigeonhole_cluster_replica_acked_seqno gauge\n",
            );
            text.push_str(acked);
        }
    }

    #[cfg(test)]
    pub(crate) fn untimed(&self) -> u64 {
        self.lock().untimed
    }

    #[cfg(test)]
    pub(crate) fn timed(&self) -> u64 {
        self.lock().waits.iter().sum()
    }
}

impl Inner {
    fn refresh_frontier(&mut self) {
        self.frontier = self.nodes.values().filter_map(|e| e.acked).max();
    }

    fn observe(&mut self, us: u64) {
        let bucket = BUCKETS_US.partition_point(|&b| b < us);
        self.waits[bucket] += 1;
        self.wait_sum_us += us;
    }
}

/// A `FollowAcked` stream's place in its partition's [`ReplicaAcks`], while it lives.
#[derive(Debug)]
pub(crate) struct Attached {
    acks: Arc<ReplicaAcks>,
    node: String,
    generation: u64,
}

impl Attached {
    /// The stream's acknowledgement of `pos`. Its first replaces the node's entry, even
    /// with a lower position (a re-bootstrapped copy starts from its snapshot); after that
    /// a lower one is ignored, so the entry only rises while the stream lives. Ignored once
    /// a newer stream from the node replaced this one.
    pub(crate) fn ack(&self, pos: Pos) {
        let now = Instant::now();
        let mut inner = self.acks.lock();
        let Some(e) = inner.nodes.get_mut(&self.node) else {
            return;
        };
        if e.generation != self.generation {
            return;
        }
        if e.acked.is_some_and(|a| a >= pos) {
            return;
        }
        e.acked = Some(pos);
        inner.refresh_frontier();
        let Some(frontier) = inner.frontier else {
            return;
        };
        while let Some(&(seqno, at)) = inner.pending.front() {
            if Pos::commit(seqno) > frontier {
                break;
            }
            inner.pending.pop_front();
            let us =
                u64::try_from(now.saturating_duration_since(at).as_micros()).unwrap_or(u64::MAX);
            inner.observe(us);
        }
    }
}

impl Drop for Attached {
    fn drop(&mut self) {
        let mut inner = self.acks.lock();
        if inner
            .nodes
            .get(&self.node)
            .is_some_and(|e| e.generation == self.generation)
        {
            inner.nodes.remove(&self.node);
            self.acks.streams.fetch_sub(1, Ordering::Relaxed);
            inner.refresh_frontier();
            if inner.nodes.is_empty() {
                // Nothing will acknowledge them: not timed.
                let left = inner.pending.len() as u64;
                inner.untimed += left;
                inner.pending.clear();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replicas(of: &[&str]) -> Vec<String> {
        of.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn a_nodes_first_ack_replaces_its_entry_and_later_ones_only_rise() {
        let acks = Arc::new(ReplicaAcks::default());
        let a = acks.attach("a");
        a.ack(Pos::commit(10));
        a.ack(Pos::commit(7));
        assert_eq!(acks.status(&replicas(&["a"])), (Some(Pos::commit(10)), 1));
        // A new stream from the same node: its first ack replaces the entry, even lower
        // (simp724's M2), and the old stream's acks no longer count.
        let a2 = acks.attach("a");
        a2.ack(Pos::commit(5));
        a.ack(Pos::commit(20));
        assert_eq!(acks.status(&replicas(&["a"])), (Some(Pos::commit(5)), 1));
        // The old stream's end leaves the new one's entry.
        drop(a);
        assert_eq!(acks.status(&replicas(&["a"])), (Some(Pos::commit(5)), 1));
        drop(a2);
        assert_eq!(acks.status(&replicas(&["a"])), (None, 0));
    }

    #[test]
    fn only_the_maps_replicas_count() {
        let acks = Arc::new(ReplicaAcks::default());
        let a = acks.attach("a");
        let b = acks.attach("b");
        a.ack(Pos::commit(3));
        b.ack(Pos { seqno: 3, n: 1 });
        assert_eq!(
            acks.status(&replicas(&["a", "b"])),
            (Some(Pos { seqno: 3, n: 1 }), 2)
        );
        assert_eq!(acks.status(&replicas(&["a"])), (Some(Pos::commit(3)), 1));
        assert_eq!(acks.status(&[]), (None, 0));
    }

    #[test]
    fn a_commit_is_timed_to_the_first_ack_at_or_past_it() {
        let acks = Arc::new(ReplicaAcks::default());
        // No stream: nothing timed, and the seqno isn't even read.
        acks.committed(|| unreachable!("no stream is attached"));
        let a = acks.attach("a");
        let b = acks.attach("b");
        acks.committed(|| 5);
        acks.committed(|| 3);
        acks.committed(|| 8);
        a.ack(Pos::commit(4));
        assert_eq!(acks.timed(), 1, "3 only");
        b.ack(Pos::commit(5));
        assert_eq!(acks.timed(), 2, "5 too");
        // Already past the frontier when it returned: a zero wait.
        acks.committed(|| 2);
        assert_eq!(acks.timed(), 3);
        // The last stream's end: what's left can't be timed.
        drop(a);
        drop(b);
        assert_eq!((acks.timed(), acks.untimed()), (3, 1));
        let mut waits = String::new();
        let mut acked = String::new();
        acks.render("p1", &mut waits, &mut acked);
        assert!(
            waits.contains("pigeonhole_cluster_replica_ack_wait_seconds_count{partition=\"p1\"} 3"),
            "{waits}"
        );
        assert!(acked.is_empty(), "no stream now: {acked}");
    }

    #[test]
    fn the_gauge_names_each_acknowledging_replica() {
        let acks = Arc::new(ReplicaAcks::default());
        let a = acks.attach("a");
        let _b = acks.attach("b");
        a.ack(Pos::commit(42));
        let mut waits = String::new();
        let mut acked = String::new();
        acks.render("p1", &mut waits, &mut acked);
        assert_eq!(
            acked,
            "pigeonhole_cluster_replica_acked_seqno{partition=\"p1\",replica=\"a\"} 42\n"
        );
        let mut text = String::new();
        ReplicaAcks::render_all(&mut text, &waits, &acked);
        assert!(text.contains("# TYPE pigeonhole_cluster_replica_acked_seqno gauge"));
        assert!(!text.contains("ack_wait_seconds"), "nothing timed: {text}");
    }
}

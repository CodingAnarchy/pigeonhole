//! D331 PR 4b: a range move's catch-up stream, the source's side (`ForwardSubscribe` with
//! `raw`).
//!
//! The move's target reads the source's history of its range from the copy's first token,
//! and applies it as the source logged it ([`super::range_follow`]). The stream is the
//! target's alone: it opens its own feed of the move's table, never a hub's ring, and never
//! rejoins one.
//!
//! - **Who's served** (feed1248's G1, cli692's M3): only this node as the source's owner in
//!   its map, at the move's `from_epoch`, for a live range move (never D332's NODE) naming
//!   it as `from`, from COPYING through HANDED_OVER, and only for exactly the move's bounds
//!   (an absent bound is open, as in `RangeMove`). HANDED_OVER is included: the target is
//!   still catching up to `final_seqno` then, and a reconnect must find the stream.
//! - **What it sends:** each commit's changes inside the range as a `RawCommit` (kinds,
//!   timestamps and stored values as logged, feed-silent commits included, durable only); a
//!   commit with none in the range, a catalog change, or a range drop outside the range as
//!   `Progress`; a quiet table a `Progress` every `--feed-quiet` interval. A commit
//!   straddling a bound sends only its in-range part (cli692).
//! - **How it ends:** a range drop that removes any of the range ends it with an error, never
//!   `Progress` (feed1248's G2: the target would keep rows the source dropped). Once the
//!   move is handed over and the stream has passed its `final_seqno`, it ends with
//!   `MovedAway`, as a cluster component of a moved table does. A map that no longer gives
//!   the move, or the source, here ends it with `WrongPartition`.

use std::sync::Arc;
use std::time::Duration;

use prost::bytes::Bytes;

use super::Node;
use crate::serve::errors::ApiError;
use crate::serve::feed::{FeedEvents, Out, Sink};
use crate::serve::proto::{self, MovePhase, subscribe_request::Start as S};
use crate::serve::router::Cluster;

/// How often the stream looks at the map for its move's end when the feed is quiet.
const MAP_TICK: Duration = Duration::from_millis(200);

/// A range move's catch-up stream from partition `req.partition`, served here.
pub(crate) async fn serve(
    cluster: &Cluster,
    req: proto::ForwardSubscribeRequest,
) -> Result<FeedEvents, ApiError> {
    let (node, parts) = (&cluster.node, &cluster.parts);
    if !req.raw {
        return Err(ApiError::invalid(
            "a range bound is for a range move's catch-up stream only (raw)",
        ));
    }
    let r = req
        .request
        .as_ref()
        .ok_or_else(|| ApiError::invalid("a ForwardSubscribe carries no request"))?;
    let [table] = &r.tables[..] else {
        return Err(ApiError::invalid(
            "a range move's catch-up stream reads exactly its move's table",
        ));
    };
    let Some(S::Resume(token)) = &r.start else {
        return Err(ApiError::invalid(
            "a range move's catch-up stream starts at its copy's token",
        ));
    };
    if r.allow_partial
        || !r.prefix.is_empty()
        || r.visible
        || r.checkpoint_every != 0
        || !r.skip_through.is_empty()
        || req.skip_through != 0
    {
        return Err(ApiError::invalid(
            "a range move's catch-up stream takes no client options",
        ));
    }
    let m = live_move(node, &req, table)?;
    let (start, end) = super::map::move_range(&m).map_err(ApiError::internal)?;
    let Some(service) = parts.load().get(&req.partition).cloned() else {
        return Err(ApiError::partition_unavailable(format!(
            "the map gives this node partition {:?}, but it isn't open here",
            req.partition
        )));
    };
    // cli692's S1 on #2107: a stale owner whose lease ran out serves no stream.
    let gate = service.gate().cloned();
    if let Some(g) = &gate {
        g.serving()?;
    }
    let db = service.part()?.db.clone();
    let t = db.table(table)?.open()?;
    let mut feed = t
        .feed()
        .resume(&pigeonhole::FeedToken::from_bytes(token.to_vec()))
        .open()?;
    // Feed-silent commits too (the copy's own and other moves' are the range's history),
    // durable only: the target applies nothing the source could still lose.
    feed.include_silent();
    let limits = service.feed_limits();
    let (sink, events, end_tx) = Sink::new(&limits);
    let stream = Stream {
        node: Arc::clone(node),
        sources: Arc::clone(&cluster.range_copies),
        gate,
        partition: req.partition,
        move_id: m.id.clone(),
        table: t,
        range: super::route::PartRange {
            partition: String::new(),
            start,
            end,
        },
        quiet: limits.quiet,
        sink,
    };
    tokio::spawn(async move {
        let outcome = stream.run(feed).await;
        if let Err(e) = outcome
            && !stream.sink.is_closed()
        {
            let _ = end_tx.send(e);
        }
    });
    Ok(events)
}

/// The live move the request names: G1's and M3's checks.
fn live_move(
    node: &Node,
    req: &proto::ForwardSubscribeRequest,
    table: &str,
) -> Result<proto::RangeMove, ApiError> {
    let map = node.get_map()?;
    let m = map
        .moves
        .iter()
        .find(|m| {
            !super::map::node_move(m)
                && m.from == req.partition
                && m.table == table
                && m.start == req.range_start
                && m.end == req.range_end
        })
        .ok_or_else(|| {
            ApiError::wrong_partition(
                map.map_version,
                format!(
                    "no range move of table {table:?} from partition {:?} with those bounds here",
                    req.partition
                ),
            )
        })?;
    if !(MovePhase::Copying..=MovePhase::HandedOver).contains(&super::map::phase(m)) {
        return Err(ApiError::failed_precondition(format!(
            "range move {:?} is at {:?}: it has no catch-up stream",
            m.id,
            super::map::phase(m)
        )));
    }
    source_here(node.id(), &map, m)?;
    Ok(m.clone())
}

/// M3: this node owns `m`'s source in `map`, at the epoch it had when the copy began
/// (`from_epoch`, 0 when the record predates it).
fn source_here(me: &str, map: &proto::ClusterMap, m: &proto::RangeMove) -> Result<(), ApiError> {
    let ok = map
        .partitions
        .iter()
        .any(|p| p.id == m.from && p.node == me && super::map::source_epoch_holds(m, p.epoch));
    if ok {
        Ok(())
    } else {
        Err(ApiError::wrong_partition(
            map.map_version,
            format!(
                "this node no longer serves range move {:?}'s source {:?} at its epoch",
                m.id, m.from
            ),
        ))
    }
}

/// One catch-up stream's state.
struct Stream {
    node: Arc<Node>,
    /// The copies served here: the stream keeps its move's hold from going idle.
    sources: Arc<super::range_copy::Sources>,
    /// The source's gate: the stream ends once its lease does.
    gate: Option<Arc<crate::serve::gate::WriteGate>>,
    partition: String,
    move_id: String,
    table: pigeonhole::Table,
    /// The move's range of its table (`partition` unused).
    range: super::route::PartRange,
    quiet: Duration,
    sink: Sink,
}

/// The most commits one event carries, and about the most bytes (T's commit unit).
const BATCH_COMMITS: usize = 1024;
const BATCH_BYTES: usize = 256 << 10;
/// The most events read for one batch, in range or not (perf287 on #2107): a catch-up deep
/// in a partition's history whose moved range takes few of its writes would otherwise read
/// on without end.
const BATCH_READS: usize = 4 * BATCH_COMMITS;
/// How long a busy stream waits for more commits before it sends a batch (perf287's re-run
/// on #2107: the timer's grain; about 5 commits a message at 5,000 a second).
const LINGER: Duration = Duration::from_millis(1);

/// The commits one event carries (#2108, perf287): what's ready, plus what comes within
/// [`LINGER`] while the stream is busy, oldest first, at most [`BATCH_COMMITS`] and about
/// [`BATCH_BYTES`]. The target applies an event as one
/// commit, and one commit keeps only the last mutation of a cell and timestamp (D34), so no
/// two of a batch's commits touch one key (a second increment at one timestamp would be
/// lost): a commit that would starts the next batch. Its own changes never repeat one (its
/// source commit kept only the last). The keys are hashed: a collision only ends a batch
/// early.
#[derive(Default)]
struct Batch {
    commits: Vec<proto::RawCommit>,
    bytes: usize,
    /// The keys of its commits' changes, once it holds two.
    keys: std::collections::HashSet<u64>,
    /// The feed's events read for it, in range or not.
    reads: usize,
    /// A commit's keys while it's checked against `keys`.
    scratch: Vec<u64>,
}

impl Batch {
    fn full(&self) -> bool {
        self.commits.len() >= BATCH_COMMITS
            || self.bytes >= BATCH_BYTES
            || self.reads >= BATCH_READS
    }

    /// Adds `c`, or hands it back when one of its changes repeats a key the batch holds.
    fn add(&mut self, c: proto::RawCommit) -> Result<(), proto::RawCommit> {
        if !self.commits.is_empty() {
            if self.commits.len() == 1 {
                let first = self.commits[0].changes.iter().map(key_of);
                self.keys.extend(first);
            }
            self.scratch.clear();
            self.scratch.extend(c.changes.iter().map(key_of));
            if self.scratch.iter().any(|k| self.keys.contains(k)) {
                return Err(c);
            }
            self.keys.extend(self.scratch.iter().copied());
        }
        self.bytes += c
            .changes
            .iter()
            .map(|ch| ch.row.len() + ch.qualifier.len() + ch.stored.len() + ch.family.len() + 16)
            .sum::<usize>();
        self.commits.push(c);
        Ok(())
    }
}

/// A change's cell and timestamp, hashed: (family, row, qualifier, timestamp, whether it's a
/// family delete), as D34 keys a commit's mutations.
fn key_of(ch: &proto::RawChange) -> u64 {
    // FNV-1a (perf287: a cheaper hash than SipHash; a collision only ends a batch early),
    // each field's length in, so field boundaries count.
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut eat = |bytes: &[u8]| {
        for &b in (bytes.len() as u64).to_le_bytes().iter().chain(bytes) {
            h = (h ^ u64::from(b)).wrapping_mul(PRIME);
        }
    };
    eat(ch.family.as_bytes());
    eat(&ch.row);
    eat(&ch.qualifier);
    eat(&ch.timestamp.to_le_bytes());
    eat(&[u8::from(ch.kind == 5)]);
    h
}

/// What the stream read next, owned (the feed lends its events).
enum Read {
    /// A commit's changes inside the range (none: the commit is only a position).
    Commit(Option<proto::RawCommit>),
    /// A catalog change, or a range drop outside the range: a position.
    Passed,
    /// A range drop that removes some of the range.
    Dropped(u64),
}

impl Stream {
    /// Sends the range's history until the move ends it, the target goes away, or it fails.
    /// Each event carries a [`Batch`] (#2108): the first commit is waited for, the rest
    /// gathered by [`fill`].
    async fn run(&self, mut feed: pigeonhole::Feed) -> Result<(), ApiError> {
        let mut sent = Bytes::new();
        let mut quiet = tokio::time::Instant::now() + self.quiet;
        let mut looked = tokio::time::Instant::now();
        // A commit read whose changes repeat a key the last batch held: the next one's first.
        let mut carry: Option<proto::RawCommit> = None;
        // Whether the last batch carried commits.
        let mut busy = false;
        loop {
            // A catch-up behind on a backlog finds every read ready: give the worker back
            // when its budget is spent (perf287 on #2107), as S's writes run there too.
            tokio::task::coop::consume_budget().await;
            let mut batch = Batch::default();
            if let Some(c) = carry.take() {
                batch.add(c).expect("an empty batch takes any commit");
            } else {
                let read = tokio::select! {
                    biased;
                    () = self.sink.closed() => return Ok(()),
                    r = tokio::time::timeout(MAP_TICK, next(&mut feed, self)) => r,
                };
                // Quiet (`Err`): the end may have come (the map's, not the feed's).
                if let Ok(read) = read {
                    self.absorb(read?, &mut batch, &mut carry)?;
                }
            }
            // The position after the batch's last commit.
            let mut token = Bytes::copy_from_slice(feed.token().as_bytes());
            // While busy (the last batch carried commits), wait up to [`LINGER`] for more
            // (perf287's re-run: a follower that keeps up is woken by each commit, so what's
            // ready at once is one); after a quiet poll, the first commit goes at once.
            let linger = busy.then(|| tokio::time::Instant::now() + LINGER);
            let mut reads = FeedReads {
                feed: &mut feed,
                stream: self,
            };
            let absorb = |read, batch: &mut Batch, carry: &mut _| self.absorb(read, batch, carry);
            let closed = self.sink.closed();
            if let Some(t) =
                fill(&mut reads, &mut batch, &mut carry, linger, closed, absorb).await?
            {
                token = t;
            }
            busy = !batch.commits.is_empty();
            let now = tokio::time::Instant::now();
            if !batch.commits.is_empty() {
                let event = proto::feed_event::Event::RawCommits(proto::RawCommits {
                    commits: batch.commits,
                });
                self.send(token.clone(), event).await?;
                sent = token.clone();
                quiet = now + self.quiet;
            } else if now >= quiet && token != sent {
                self.send(
                    token.clone(),
                    proto::feed_event::Event::Progress(proto::Progress {}),
                )
                .await?;
                sent = token.clone();
                quiet = now + self.quiet;
            }
            // A map read copies it: at most once a tick.
            if looked.elapsed() < MAP_TICK {
                continue;
            }
            looked = tokio::time::Instant::now();
            if let Some(g) = &self.gate {
                g.serving()?;
            }
            self.sources.touch(&self.move_id);
            if self.ended(&token).await? {
                return Err(ApiError::moved_away(format!(
                    "range move {:?} was handed over, and its catch-up stream passed its last \
                     seqno",
                    self.move_id
                )));
            }
        }
    }

    /// Takes `read` into `batch`: true if the stream's position moved past it, false if it's
    /// left in `carry` for the next batch (it repeats a key the batch holds).
    fn absorb(
        &self,
        read: Read,
        batch: &mut Batch,
        carry: &mut Option<proto::RawCommit>,
    ) -> Result<bool, ApiError> {
        batch.reads += 1;
        match read {
            Read::Commit(Some(c)) => match batch.add(c) {
                Ok(()) => Ok(true),
                Err(c) => {
                    *carry = Some(c);
                    Ok(false)
                }
            },
            Read::Commit(None) | Read::Passed => Ok(true),
            Read::Dropped(seqno) => Err(ApiError::range_dropped(format!(
                "a range drop at seqno {seqno} on partition {:?} removed rows of range move \
                 {:?}'s range: the move must be aborted",
                self.partition, self.move_id
            ))),
        }
    }

    /// Whether the move was handed over and the stream, at `token`, passed its `final_seqno`
    /// (its position then sent, so the target has it). `WrongPartition` when the map no
    /// longer gives the move, or its source, here.
    async fn ended(&self, token: &Bytes) -> Result<bool, ApiError> {
        let map = self.node.get_map()?;
        let Some(m) = map.moves.iter().find(|m| m.id == self.move_id) else {
            return Err(ApiError::wrong_partition(
                map.map_version,
                format!("the map no longer has range move {:?}", self.move_id),
            ));
        };
        source_here(self.node.id(), &map, m)?;
        let done = super::map::phase(m) >= MovePhase::HandedOver
            && m.final_seqno != 0
            && super::token::token_seqno(token).is_some_and(|s| s >= m.final_seqno);
        if done {
            self.send(
                token.clone(),
                proto::feed_event::Event::Progress(proto::Progress {}),
            )
            .await?;
        }
        Ok(done)
    }

    async fn send(&self, token: Bytes, event: proto::feed_event::Event) -> Result<(), ApiError> {
        self.sink
            .put(Out::event(proto::FeedEvent {
                token,
                event: Some(event),
                partition: String::new(),
                xid: Bytes::new(),
            }))
            .await
    }

    /// `ev` as the stream sends it.
    fn read(&self, ev: &pigeonhole::FeedEvent) -> Read {
        match ev {
            pigeonhole::FeedEvent::Commit(c) => {
                let changes: Vec<proto::RawChange> = c
                    .changes()
                    .iter()
                    .filter(|ch| self.range.holds(ch.row()))
                    .map(|ch| proto::RawChange {
                        family: ch.family().to_owned(),
                        kind: super::range_copy::logged_kind(ch.kind()),
                        row: Bytes::copy_from_slice(ch.row()),
                        qualifier: Bytes::copy_from_slice(ch.qualifier()),
                        timestamp: ch.timestamp(),
                        stored: Bytes::copy_from_slice(ch.stored()),
                    })
                    .collect();
                Read::Commit((!changes.is_empty()).then(|| proto::RawCommit {
                    seqno: c.seqno(),
                    timestamp: c.timestamp(),
                    changes,
                }))
            }
            pigeonhole::FeedEvent::RangeDrop(d)
                if d.overlaps(&self.table, &self.range.start, self.range.end.as_deref()) =>
            {
                Read::Dropped(d.seqno())
            }
            _ => Read::Passed,
        }
    }
}

/// What [`fill`] reads from: the stream's feed ([`FeedReads`]), or a test's script.
trait Reads {
    /// The next event, if one is ready now.
    fn ready(&mut self) -> Result<Option<Read>, ApiError>;
    /// The position just after what's been read.
    fn token(&self) -> Bytes;
}

/// The stream's feed, read as the stream reads it.
struct FeedReads<'a> {
    feed: &'a mut pigeonhole::Feed,
    stream: &'a Stream,
}

impl Reads for FeedReads<'_> {
    fn ready(&mut self) -> Result<Option<Read>, ApiError> {
        Ok(self
            .feed
            .try_next_event()
            .map_err(ApiError::from)?
            .map(|ev| self.stream.read(ev)))
    }

    fn token(&self) -> Bytes {
        Bytes::copy_from_slice(self.feed.token().as_bytes())
    }
}

/// Adds to `batch` what `reads` has ready; with `linger` set (the stream is busy), sleeps
/// until then and adds what's ready by then too, until the batch is full or `carry` holds a
/// commit for the next. It sleeps rather than waits on the feed (perf287's last re-run on
/// #2107: waiting on it woke the stream at every commit): one wakeup a batch. `closed` (the
/// target went away) cuts the sleep short. `absorb` takes each event in. The token after
/// the last event taken in, if any.
async fn fill(
    reads: &mut impl Reads,
    batch: &mut Batch,
    carry: &mut Option<proto::RawCommit>,
    linger: Option<tokio::time::Instant>,
    closed: impl std::future::Future<Output = ()>,
    mut absorb: impl FnMut(Read, &mut Batch, &mut Option<proto::RawCommit>) -> Result<bool, ApiError>,
) -> Result<Option<Bytes>, ApiError> {
    let mut token = None;
    let mut drain = |reads: &mut _, batch: &mut Batch, carry: &mut Option<_>| {
        let reads: &mut _ = reads;
        while !batch.full() && carry.is_none() {
            let Some(read) = Reads::ready(reads)? else {
                break;
            };
            if absorb(read, batch, carry)? {
                token = Some(Reads::token(reads));
            }
        }
        Ok::<_, ApiError>(())
    };
    drain(reads, batch, carry)?;
    if let Some(at) = linger
        && !batch.full()
        && carry.is_none()
    {
        tokio::select! {
            biased;
            () = closed => return Ok(token),
            () = tokio::time::sleep_until(at) => {}
        }
        drain(reads, batch, carry)?;
    }
    Ok(token)
}

/// The feed's next event, as the stream reads it.
async fn next(feed: &mut pigeonhole::Feed, s: &Stream) -> Result<Read, ApiError> {
    std::future::poll_fn(|cx| match feed.poll_next_event(cx) {
        std::task::Poll::Ready(Ok(ev)) => std::task::Poll::Ready(Ok(s.read(ev))),
        std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(ApiError::from(e))),
        std::task::Poll::Pending => std::task::Poll::Pending,
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn incr(seqno: u64, row: &str, ts: u64) -> proto::RawCommit {
        proto::RawCommit {
            seqno,
            timestamp: 0,
            changes: vec![proto::RawChange {
                family: "n".into(),
                kind: 2,
                row: Bytes::copy_from_slice(row.as_bytes()),
                qualifier: Bytes::from_static(b"c"),
                timestamp: ts,
                stored: Bytes::from_static(&[1, 1, 0, 0, 0, 0, 0, 0, 0]),
            }],
        }
    }

    #[test]
    fn two_increments_at_one_timestamp_never_share_a_batch() {
        // The coordinator's D34 rule (#2108): one commit on the target keeps only the last
        // mutation of a cell and timestamp, so the second increment starts the next batch.
        let mut b = Batch::default();
        b.add(incr(1, "r", 7)).unwrap();
        b.add(incr(2, "s", 7)).unwrap();
        b.add(incr(3, "r", 8)).unwrap();
        let back = b.add(incr(4, "r", 7)).unwrap_err();
        assert_eq!(back.seqno, 4);
        assert_eq!(
            b.commits.iter().map(|c| c.seqno).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        // It opens the next batch.
        let mut next = Batch::default();
        next.add(back).unwrap();
        assert_eq!(next.commits.len(), 1);
    }

    #[test]
    fn a_batch_is_full_after_its_reads_even_of_commits_out_of_range() {
        // perf287 on #2107: reads that bring nothing count too, so a catch-up deep in a
        // history its range takes few writes of still ends each batch.
        let mut b = Batch::default();
        for _ in 0..BATCH_READS - 1 {
            b.reads += 1;
            assert!(!b.full());
        }
        b.reads += 1;
        assert!(b.full() && b.commits.is_empty());
    }

    /// A script of events, each due at its time.
    struct Script {
        due: std::collections::VecDeque<(tokio::time::Instant, proto::RawCommit)>,
        taken: u64,
        /// The stream's looks at the feed: each wakeup reads until nothing is ready.
        looked: u64,
    }

    impl Reads for Script {
        fn ready(&mut self) -> Result<Option<Read>, ApiError> {
            self.looked += 1;
            let now = tokio::time::Instant::now();
            if !self.due.front().is_some_and(|(at, _)| *at <= now) {
                return Ok(None);
            }
            self.taken += 1;
            Ok(self.due.pop_front().map(|(_, c)| Read::Commit(Some(c))))
        }

        fn token(&self) -> Bytes {
            Bytes::copy_from_slice(&self.taken.to_le_bytes())
        }
    }

    fn script(after_us: &[u64]) -> Script {
        let now = tokio::time::Instant::now();
        Script {
            due: after_us
                .iter()
                .enumerate()
                .map(|(i, us)| {
                    (
                        now + Duration::from_micros(*us),
                        incr(i as u64 + 1, &format!("r{i}"), 1),
                    )
                })
                .collect(),
            taken: 0,
            looked: 0,
        }
    }

    fn absorb(
        read: Read,
        batch: &mut Batch,
        carry: &mut Option<proto::RawCommit>,
    ) -> Result<bool, ApiError> {
        if let Read::Commit(Some(c)) = read
            && let Err(c) = batch.add(c)
        {
            *carry = Some(c);
            return Ok(false);
        }
        Ok(true)
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_stream_gathers_what_comes_within_its_linger() {
        // perf287's re-runs on #2107: commits 0.3 ms apart share a message while the stream
        // is busy, gathered at one wakeup after the linger; one past it waits for the next.
        let mut s = script(&[0, 300, 600, 5_000]);
        let start = tokio::time::Instant::now();
        let mut batch = Batch::default();
        let mut carry = None;
        let closed = std::future::pending();
        let token = fill(
            &mut s,
            &mut batch,
            &mut carry,
            Some(start + LINGER),
            closed,
            absorb,
        )
        .await
        .unwrap();
        assert_eq!(batch.commits.len(), 3);
        assert_eq!(token, Some(Bytes::copy_from_slice(&3u64.to_le_bytes())));
        assert_eq!(start.elapsed(), LINGER, "it slept out its linger");
        // Two looks at the feed (now, and at the linger's end), each read until nothing was
        // ready: 2 + 3 reads, not one per commit as it came.
        assert_eq!(s.looked, 5);
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_stream_sends_its_first_commit_at_once() {
        // After a quiet poll, no linger: what's ready goes, and nothing is waited for.
        let mut s = script(&[0, 300]);
        let start = tokio::time::Instant::now();
        let mut batch = Batch::default();
        let mut carry = None;
        fill(
            &mut s,
            &mut batch,
            &mut carry,
            None,
            std::future::pending(),
            absorb,
        )
        .await
        .unwrap();
        assert_eq!(batch.commits.len(), 1);
        assert_eq!(start.elapsed(), Duration::ZERO, "nothing waited for");
        assert_eq!(s.looked, 2, "one look at the feed");
    }

    #[test]
    fn a_batch_is_full_at_its_commit_count() {
        let mut b = Batch::default();
        for i in 0..BATCH_COMMITS as u64 {
            assert!(!b.full());
            b.add(incr(i, &format!("r{i}"), 1)).unwrap();
        }
        assert!(b.full());
    }
}

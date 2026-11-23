use super::*;

/// Where a stream's events go: its connection's buffered channel, bounded by places and by
/// bytes, and ended by the idle limit when the connection stops taking. A single node's
/// client and a cluster subscription's merge (D317 ticket 7) both send through one.
pub(crate) struct Sink {
    pub(super) tx: mpsc::Sender<(Queued, OwnedSemaphorePermit)>,
    /// The buffer's free bytes: each event or batch in the channel, or a batch the
    /// connection is still taking events from, holds its charge ([`EVENT_FLOOR`]).
    pub(super) budget: Arc<Semaphore>,
    /// When the connection last took an event: a send waiting for room counts its
    /// `--stream-idle-ms` from it (simp724's I1 on #1536).
    taken: Arc<TakeClock>,
    pub(super) client_bytes: usize,
    stream_idle: Duration,
    /// Where the stream starts, sent once it's known ([`Sink::started`], read by
    /// [`FeedEvents::start`]).
    start: Mutex<Option<oneshot::Sender<Bytes>>>,
}

impl Sink {
    /// A stream's sink, the events its connection takes, and where its task's error goes.
    pub(crate) fn new(limits: &FeedLimits) -> (Self, FeedEvents, oneshot::Sender<ApiError>) {
        let (tx, rx) = mpsc::channel(limits.client_events.max(1));
        let client_bytes = limits.client_bytes.clamp(1, Semaphore::MAX_PERMITS);
        let taken = Arc::new(TakeClock::new());
        let (end_tx, end_rx) = oneshot::channel();
        let (start_tx, start_rx) = oneshot::channel();
        let sink = Sink {
            tx,
            budget: Arc::new(Semaphore::new(client_bytes)),
            taken: Arc::clone(&taken),
            client_bytes,
            stream_idle: limits.stream_idle,
            start: Mutex::new(Some(start_tx)),
        };
        let events = FeedEvents(
            EventSource::Sink {
                events: rx,
                draining: None,
                taken,
                end: Some(end_rx),
                start: Some(start_rx),
            },
            None,
            None,
        );
        (sink, events, end_tx)
    }

    /// The stream starts at `token`: for [`FeedEvents::start`], once (later calls do
    /// nothing).
    pub(crate) fn started(&self, token: &Bytes) {
        if let Some(tx) = self
            .start
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            let _ = tx.send(token.clone());
        }
    }

    /// Whether [`Sink::started`] is still to be told.
    pub(crate) fn starting(&self) -> bool {
        self.start
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    /// Whether events wait in the connection's buffer, untaken.
    pub(crate) fn pending(&self) -> bool {
        self.budget.available_permits() < self.client_bytes
    }

    /// When the connection last took an event ([`TakeClock`]: of a batch, its first and last
    /// event, and every event while a send waits).
    pub(crate) fn taken(&self) -> &TakeClock {
        &self.taken
    }

    /// Whether the connection went away.
    pub(crate) fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// Resolves once the connection has gone away.
    pub(crate) async fn closed(&self) {
        self.tx.closed().await;
    }

    /// Queues `event` for the connection: room in its buffer, then a place in its channel,
    /// both inside the idle limit, so a connection that takes nothing fills its buffer and
    /// then times out.
    pub(crate) async fn put(&self, event: Out) -> Result<(), ApiError> {
        let bytes = charge(&event);
        self.queue(Queued::One(event), bytes).await
    }

    /// Queues `events`, in order, as few batches as the buffer allows: each batch is one
    /// place in the channel and one charge against the buffer, and the connection takes its
    /// events without going back to the channel (#1737's F3-0: the per-event channel pop,
    /// buffer release and clock read were about 500 cycles an event on the network workers
    /// at saturation, g129). A batch closes before an event that would take it past the
    /// buffer, so none is larger than the buffer but a single event that is.
    pub(crate) async fn put_all(&self, mut events: Vec<Out>) -> Result<(), ApiError> {
        while !events.is_empty() {
            let mut bytes = 0usize;
            let mut fit = 0;
            for e in &events {
                let c = charge(e);
                if fit > 0 && bytes.saturating_add(c) > self.client_bytes {
                    break;
                }
                bytes = bytes.saturating_add(c);
                fit += 1;
            }
            // The rest, if this batch closes early; usually none, and no allocation.
            let rest = if fit < events.len() {
                events.split_off(fit)
            } else {
                Vec::new()
            };
            let batch = if events.len() == 1 {
                Queued::One(events.pop().expect("one event"))
            } else {
                Queued::Many(events)
            };
            self.queue(batch, bytes).await?;
            events = rest;
        }
        Ok(())
    }

    /// Queues an event or a batch charged `bytes` (at most the whole buffer).
    async fn queue(&self, item: Queued, bytes: usize) -> Result<(), ApiError> {
        let n = u32::try_from(bytes.clamp(1, self.client_bytes)).unwrap_or(u32::MAX);
        // The fast path: room in the buffer and a place in the channel at once, with no
        // timer or waiter registered. Arming them on every send was 64% of the samples in
        // futex calls at 256 subscribers (perf287's profile on #1397).
        let (item, held) = match Arc::clone(&self.budget).try_acquire_many_owned(n) {
            Ok(room) => match self.tx.try_send((item, room)) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full((item, room))) => (item, Some(room)),
                Err(mpsc::error::TrySendError::Closed(_)) => return Err(gone()),
            },
            Err(tokio::sync::TryAcquireError::NoPermits) => (item, None),
            Err(tokio::sync::TryAcquireError::Closed) => return Err(gone()),
        };
        // The slow path, boxed: its timer and waits made this future 1.2 KB, built (and
        // copied) for every event though the fast path needs none of it (#1758's g86c).
        Box::pin(self.wait_to_put(item, held, n)).await
    }

    /// [`Sink::queue`]'s slow path: waits for room in the buffer (unless `held`) and a place
    /// in the channel, within the idle limit.
    async fn wait_to_put(
        &self,
        item: Queued,
        held: Option<OwnedSemaphorePermit>,
        n: u32,
    ) -> Result<(), ApiError> {
        let budget = Arc::clone(&self.budget);
        let queued = async {
            let room = match held {
                Some(room) => room,
                None => budget.acquire_many_owned(n).await.map_err(|_| ())?,
            };
            self.tx.send((item, room)).await.map_err(|_| ())
        };
        tokio::pin!(queued);
        // The idle limit counts from the client's last take, not from the start of the wait:
        // a large event may wait for most of the buffer to drain, and a client taking events
        // all along is slow, not idle (simp724's I1 on #1536). One timer: when it fires, a
        // take since the wait began moves the deadline on, and it waits again. Watched, so
        // takes within a batch are stamped too ([`TakeClock`]), from before the wait begins.
        let _watch = self.taken.watch();
        let idle = self.stream_idle;
        let began = tokio::time::Instant::now();
        let timer = tokio::time::sleep_until(began + idle);
        tokio::pin!(timer);
        loop {
            tokio::select! {
                // The send first: room that is there wins over a timer that fired meanwhile.
                biased;
                sent = &mut queued => {
                    // The client went away: end quietly.
                    return sent.map_err(|()| gone());
                }
                () = self.tx.closed() => return Err(gone()),
                () = &mut timer => {
                    let from = self.taken.last().map_or(began, |t| t.max(began));
                    if from + idle > tokio::time::Instant::now() {
                        timer.as_mut().reset(from + idle);
                        continue;
                    }
                    return Err(idle_error(idle));
                }
            }
        }
    }
}

/// What an event costs a client's buffer: its encoded size, at least [`EVENT_FLOOR`].
fn charge(event: &Out) -> usize {
    event.encoded_len().max(EVENT_FLOOR)
}

/// A place in a client's channel: one event, or a batch of them, in order ([`Sink::put_all`]).
pub(super) enum Queued {
    One(Out),
    /// At least two events.
    Many(Vec<Out>),
}

/// A client's events, then its error if it failed: a single node's, from its [`Sink`]; or
/// a cluster subscription's, merged as the client polls ([`crate::serve::cluster::feed`],
/// #1758's B).
pub(crate) struct FeedEvents(
    EventSource,
    Option<Box<DeliveryLease>>,
    /// A range move's target's component: the barriers it carries (D331 PR 3b).
    Option<Box<Barriers>>,
);

/// A range move's hand-over in its target's cluster component (D331 PR 3b, cli692's (d)):
/// the component hands it out just before its first event past `t_from` (feed1248's M4: any
/// event that moves its position there, progress included), so the entry node's merge holds
/// the component until the source's has passed `final_seqno`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Barrier {
    /// The target's seqno at the hand-over: its feed token's, `token`.
    pub t_from: u64,
    /// The target's D310 feed token at `t_from`: the barrier event's token, so the merge's
    /// position for the component stays there while it holds it (feed1248's M6).
    pub token: Bytes,
    pub move_id: String,
    pub source: String,
    pub final_seqno: u64,
}

/// A component's barriers still to hand out, in `(t_from, move id)` order (feed1248's S4),
/// and an event held back behind one.
#[derive(Default)]
struct Barriers {
    pending: std::collections::VecDeque<Barrier>,
    held: Option<Out>,
    /// A cluster component of a cluster partition (PR 3b): its partition's gate and its
    /// tables, so a hand-over the partition takes while the component runs (its serving
    /// row written at CLEANING) adds that barrier too, not only those known at its open.
    watch: Option<(Arc<super::super::gate::WriteGate>, Vec<String>)>,
    /// The gate's serving rows' version last looked at, the moves already barriered, and the
    /// last seqno handed out (a barrier at or past it still comes in time).
    version: u64,
    known: Vec<String>,
    last: u64,

    /// A component of tables a range move took from its partition whole: it ends, cleanly
    /// for the merge (`MovedAway`), once it has handed out an event at or past this seqno.
    ends_at: Option<u64>,
    /// It has: `MovedAway` next, then nothing.
    done: Option<bool>,
}

/// A hold seam (tests): a one-shot closure a component of the named partition runs between
/// reading an event and loading its gate's serving-row version (D331 PR 3b, feed1248's
/// point 1), so a test can take a hand-over exactly there. A no-op when unset.
#[cfg(test)]
pub(crate) static AFTER_READ: std::sync::Mutex<Option<AfterRead>> = std::sync::Mutex::new(None);

/// [`AFTER_READ`]'s hook: the partition, and what runs there.
#[cfg(test)]
pub(crate) type AfterRead = (String, Box<dyn FnOnce() + Send>);

impl Barriers {
    /// Runs [`AFTER_READ`]'s closure, if it's set for this component's partition.
    #[cfg(test)]
    fn after_read(&self) {
        let Some((gate, _)) = &self.watch else {
            return;
        };
        let hook = {
            let mut slot = AFTER_READ
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match slot.take() {
                Some((p, f)) if p == gate.partition() => Some(f),
                other => {
                    *slot = other;
                    None
                }
            }
        };
        if let Some(f) = hook {
            f();
        }
    }

    /// The barriers a hand-over taken since the last look adds, and the end a move that took
    /// the component's last table here sets (one load while neither changed).
    fn refresh(&mut self) {
        let Some((gate, tables)) = &self.watch else {
            return;
        };
        let version = gate.ranges().rows_version();
        if version == self.version {
            return;
        }
        self.version = version;
        // Every table moved away whole, each with its row (feed1248's M3 on PR 3b): read up to
        // the moves' last seqno, then end, as a component opened after the move does
        // (`moved_end`). Without this, a subscription connected across the move kept the
        // partition in its tokens, and a reconnect after PR 5 pruned the row would expire.
        if self.ends_at.is_none()
            && let Some(state) = gate.ranges().load().as_ref()
            && tables.iter().all(|t| state.moved_whole(t))
        {
            let rows = gate.ranges().moved_rows();
            let ends: Option<Vec<u64>> = tables
                .iter()
                .map(|t| {
                    rows.iter()
                        .filter(|m| &m.table == t)
                        .map(|m| m.final_seqno)
                        .max()
                })
                .collect();
            if let Some(end) = ends.and_then(|e| e.into_iter().max()) {
                self.ends_at = Some(end);
                if self.last >= end {
                    self.done = Some(true);
                }
            }
        }
        let mut added = false;
        for m in gate.ranges().serving_rows() {
            if m.token.is_empty() || !tables.contains(&m.table) || self.known.contains(&m.id) {
                continue;
            }
            self.known.push(m.id.clone());
            // Already past its `t_from` (events of the partition's other ranges, read between
            // the hand-over's token and its row): the range takes writes only once the gate
            // opens, after the row, so the barrier still comes before any of them, now, and
            // without a token, so the merge's position for the component doesn't move
            // (feed1248's point 2: a token past `t_from` checkpointed while held would let a
            // resume start past the barrier).
            let (t_from, token) = if m.seqno < self.last {
                (self.last, Bytes::new())
            } else {
                (m.seqno, m.token.into())
            };
            self.pending.push_back(Barrier {
                t_from,
                token,
                move_id: m.id,
                source: m.source,
                final_seqno: m.final_seqno,
            });
            added = true;
        }
        if added {
            self.pending
                .make_contiguous()
                .sort_by(|a, b| (a.t_from, &a.move_id).cmp(&(b.t_from, &b.move_id)));
        }
    }

    /// `out` is going out: past `ends_at`, the component ends next; and its position.
    fn passed(&mut self, out: &Out) {
        if let Some(s) = crate::serve::cluster::token::token_seqno(out.token()) {
            self.last = self.last.max(s);
        }
        if let Some(end) = self.ends_at
            && crate::serve::cluster::token::token_seqno(out.token()).is_some_and(|s| s >= end)
        {
            self.done = Some(true);
        }
    }

    /// The barrier due before `out`, if one is: the next one, when `out`'s token is past its
    /// `t_from`.
    fn due(&mut self, out: &Out) -> Option<Barrier> {
        let next = self.pending.front()?;
        // A component's commits carry D310 tokens, and its other events one or none
        // (feed1248's S2 and its nit: a tokenless commit past `t_from` is exactly what would
        // slip by unheld). Failing closed (handing the barrier out) isn't safe: its token
        // would move the merge's position to `t_from` ahead of events still to come.
        debug_assert!(
            crate::serve::cluster::token::token_seqno(out.token()).is_some()
                || (out.token().is_empty() && !out.is_commit()),
            "a component's commit without a D310 token, or an event with another token"
        );
        let seqno = crate::serve::cluster::token::token_seqno(out.token())?;
        if seqno > next.t_from {
            self.pending.pop_front()
        } else {
            None
        }
    }
}

impl Barrier {
    fn out(self) -> Out {
        Out::event(proto::FeedEvent {
            token: self.token,
            event: Some(proto::feed_event::Event::MoveBarrier(proto::MoveBarrier {
                move_id: self.move_id,
                source: self.source,
                final_seqno: self.final_seqno,
            })),
            partition: String::new(),
            // A barrier isn't a commit: no transaction's id (D329 2b).
            xid: Default::default(),
        })
    }
}

impl EventSource {
    /// The take clock the sink's idle limit reads.
    fn taken(&self) -> &TakeClock {
        let EventSource::Sink { taken, .. } = self;
        taken
    }
}

enum EventSource {
    Sink {
        events: mpsc::Receiver<(Queued, OwnedSemaphorePermit)>,
        /// The rest of a batch taken from the channel, and its charge: held until its last
        /// event is taken, so the buffer's bound and [`Sink::pending`] count a batch the
        /// connection holds under back-pressure (simp724's (3) on #1737's F3-0).
        draining: Option<(std::vec::IntoIter<Out>, OwnedSemaphorePermit)>,
        /// Each take's time, for a send waiting for room.
        taken: Arc<TakeClock>,
        end: Option<oneshot::Receiver<ApiError>>,
        /// Where the stream starts, once its task knows ([`FeedEvents::start`]).
        start: Option<oneshot::Receiver<Bytes>>,
    },
}

impl std::fmt::Debug for FeedEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.0 {
            EventSource::Sink { .. } => "FeedEvents(sink)",
        })
    }
}

/// An event as it leaves a client: its own, or a ring entry encoded once for every client
/// with no prefix.
pub(crate) enum Out {
    /// Boxed, so `Out` is two `Bytes` wide (`Spliced`), not a whole message: every event is
    /// moved through a `Sink`'s put and channel by value, once per stream it goes to (#1758's
    /// g88: those moves were most of the cluster path's memmove).
    Event(Box<proto::FeedEvent>),
    /// An entry whose `encoded()` is `Some`.
    Shared(Arc<Entry>),
    /// A cluster subscription's event (D317 ticket 7): a component's event without its
    /// token (`body`), then the fields the merge adds (`tail`: the partition, and on a
    /// checkpoint the cluster token). A protobuf message is the concatenation of its fields,
    /// so the component's encoding is sent as it is, not decoded and built again.
    Spliced { body: Bytes, tail: Bytes },
    /// A cluster feed hub's entry (#1798): encoded once for every member, as `Shared` is.
    Merged(Arc<crate::serve::cluster::hub::ClusterEntry>),
}

impl Out {
    pub(crate) fn event(e: proto::FeedEvent) -> Self {
        Out::Event(Box::new(e))
    }

    /// Whether it's a commit (a ring entry always is). Only for the barriers' debug check.
    fn is_commit(&self) -> bool {
        match self {
            Out::Event(e) => matches!(e.event, Some(proto::feed_event::Event::Commit(_))),
            Out::Shared(_) => true,
            Out::Spliced { .. } | Out::Merged(_) => false,
        }
    }

    pub(super) fn token(&self) -> &Bytes {
        static NONE: Bytes = Bytes::new();
        match self {
            Out::Event(e) => &e.token,
            Out::Shared(e) => &e.token,
            Out::Spliced { .. } => &NONE,
            Out::Merged(e) => &e.token,
        }
    }

    /// Its size on the wire, for the client's buffer.
    pub(crate) fn encoded_len(&self) -> usize {
        match self {
            Out::Event(e) => prost::Message::encoded_len(e),
            Out::Shared(e) => e.encoded().map_or(0, Bytes::len),
            Out::Spliced { body, tail } => body.len() + tail.len(),
            Out::Merged(e) => e.body.len() + e.tail.len(),
        }
    }

    /// The event's encoding, as one buffer (a cluster component's event, to splice).
    pub(crate) fn to_bytes(&self) -> Bytes {
        match self {
            Out::Event(e) => Bytes::from(prost::Message::encode_to_vec(e)),
            Out::Shared(e) => e.encoded().cloned().unwrap_or_default(),
            Out::Spliced { body, tail } => [body.as_ref(), tail.as_ref()].concat().into(),
            Out::Merged(e) if e.tail.is_empty() => e.body.clone(),
            Out::Merged(e) => [e.body.as_ref(), e.tail.as_ref()].concat().into(),
        }
    }

    /// The event as a message (HTTP, which renders JSON, and tests).
    pub(crate) fn into_proto(self) -> proto::FeedEvent {
        match self {
            Out::Event(e) => *e,
            Out::Shared(e) => e
                .encoded()
                .and_then(|b| <proto::FeedEvent as prost::Message>::decode(b.clone()).ok())
                .unwrap_or_else(|| {
                    commit_event(e.token.clone(), proto::FeedCommit::default(), b"")
                }),
            Out::Spliced { .. } | Out::Merged(_) => {
                <proto::FeedEvent as prost::Message>::decode(self.to_bytes()).unwrap_or_default()
            }
        }
    }

    /// Writes the event's encoding to `buf` (gRPC): a shared entry's bytes as they are.
    pub(crate) fn encode(
        &self,
        buf: &mut impl prost::bytes::BufMut,
    ) -> Result<(), prost::EncodeError> {
        match self {
            Out::Event(e) => prost::Message::encode(e, buf),
            Out::Shared(e) => {
                if let Some(b) = e.encoded() {
                    buf.put_slice(b);
                }
                Ok(())
            }
            Out::Spliced { body, tail } => {
                buf.put_slice(body);
                buf.put_slice(tail);
                Ok(())
            }
            Out::Merged(e) => {
                buf.put_slice(&e.body);
                buf.put_slice(&e.tail);
                Ok(())
            }
        }
    }
}

impl FeedEvents {
    /// Where the stream starts: a token that resumes after everything it will deliver comes
    /// before (#1915 PR 3: `/mcp`'s `feed_poll` returns it when no commit arrives, as the
    /// library's `Feed::token()` at open does). A single node's is its start's; a cluster
    /// subscription's, once every partition's part has placed it. Waits until then; asked
    /// again, the same token. The stream's error if it ends first.
    pub(crate) async fn start(&mut self) -> Result<Vec<u8>, ApiError> {
        let EventSource::Sink { start, end, .. } = &mut self.0;
        let Some(rx) = start.as_mut() else {
            return Err(ApiError::internal("a stream's start asked for twice"));
        };
        match rx.await {
            Ok(token) => {
                *start = None;
                Ok(token.to_vec())
            }
            Err(_) => {
                // The stream's task ended first: its error, which it sends as it ends.
                *start = None;
                Err(match end.take() {
                    Some(e) => e.await.unwrap_or_else(|_| {
                        ApiError::unavailable("the subscription ended before it started")
                    }),
                    None => ApiError::unavailable("the subscription ended before it started"),
                })
            }
        }
    }

    /// These events, delivered only while cluster partition `gate`'s lease holds (D326 PR
    /// 2): they wait while it has had none since it opened and the node reaches a majority,
    /// and end with `PartitionUnavailable` once it lapses or the node doesn't (feed1248's
    /// F2). Refused at once in that last case, so a subscribe never starts there.
    pub(crate) fn leased(
        mut self,
        gate: Arc<super::super::gate::WriteGate>,
    ) -> Result<Self, ApiError> {
        self.1 = Some(Box::new(DeliveryLease::new(gate)?));
        Ok(self)
    }

    /// The next event as it left the client, then the client's error if it failed.
    ///
    /// On a cluster partition, the lease is checked on every poll ([`DeliveryLease`]): that
    /// replaces a check once the subscribe is in place (feed1248's F3, `still_leased`), since
    /// a lease that lapsed between the subscribe's checks and its first poll ends it here.
    /// Carries `barriers` (D331 PR 3b): each handed out just before the first event past its
    /// `t_from`. Sorted here. Ends past `ends_at`, if set, with `MovedAway`. With `watch`, a
    /// cluster partition's gate and the component's tables, it takes a hand-over's barrier
    /// while it runs too.
    pub(crate) fn with_barriers(
        mut self,
        mut barriers: Vec<Barrier>,
        ends_at: Option<u64>,
        watch: Option<(Arc<super::super::gate::WriteGate>, Vec<String>)>,
    ) -> Self {
        if barriers.is_empty() && ends_at.is_none() && watch.is_none() {
            return self;
        }
        barriers.sort_by(|a, b| (a.t_from, &a.move_id).cmp(&(b.t_from, &b.move_id)));
        let version = watch.as_ref().map_or(0, |(g, _)| g.ranges().rows_version());
        self.2 = Some(Box::new(Barriers {
            known: barriers.iter().map(|b| b.move_id.clone()).collect(),
            pending: barriers.into(),
            held: None,
            watch,
            version,
            last: 0,
            ends_at,
            done: None,
        }));
        self
    }

    /// The next event, a due barrier first (D331 PR 3b): one load while the component
    /// carries none.
    pub(crate) fn poll_out(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Out, ApiError>>> {
        if self.2.is_none() {
            return self.poll_out_inner(cx);
        }
        if let Some(barriers) = self.2.as_mut() {
            match barriers.done {
                Some(true) => {
                    barriers.done = Some(false);
                    return Poll::Ready(Some(Err(ApiError::moved_away(
                        "this component's tables moved to other partitions: it has read up \
                         to their range moves' last seqno",
                    ))));
                }
                Some(false) => return Poll::Ready(None),
                None => {}
            }
            if let Some(held) = barriers.held.take() {
                // Another barrier may be due before the same event.
                if let Some(b) = barriers.due(&held) {
                    barriers.held = Some(held);
                    return Poll::Ready(Some(Ok(b.out())));
                }
                barriers.passed(&held);
                return Poll::Ready(Some(Ok(held)));
            }
        }
        let out = self.as_mut().poll_out_inner(cx);
        if let Poll::Ready(Some(Ok(event))) = out {
            let barriers = self.2.as_mut().expect("checked above");
            // After the read, before the send (feed1248's point 1): the hand-over writes its
            // serving row (bumping the version, Release) before its gate opens the range, so
            // a commit of the range this read returned was published after it, and the
            // reader's Acquire of that commit happens before this load (Acquire), which sees
            // the row. A load before the read could miss it and send the commit unheld.
            #[cfg(test)]
            barriers.after_read();
            barriers.refresh();
            return Poll::Ready(Some(Ok(match barriers.due(&event) {
                Some(b) => {
                    barriers.held = Some(event);
                    b.out()
                }
                None => {
                    barriers.passed(&event);
                    event
                }
            })));
        }
        out
    }

    fn poll_out_inner(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Out, ApiError>>> {
        // Without a lease, the sink's own end comes first (audit120's S-C5), and each look
        // while waiting for a first lease counts as a take (feed1248's S1): see
        // [`DeliveryLease::before`].
        if let Some(mut lease) = self.1.take() {
            let gated = lease.before(cx, &mut *self);
            self.1 = Some(lease);
            if let Some(answer) = gated {
                return answer;
            }
        }
        let out = self.as_mut().poll_events(cx);
        if let Some(lease) = self.1.as_mut() {
            lease.after(out.is_pending(), cx);
        }
        out
    }

    /// The sink's error, if it has ended with one, without taking its events.
    fn poll_end(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<ApiError>> {
        let EventSource::Sink { end, .. } = &mut self.0;
        let Some(rx) = end.as_mut() else {
            return Poll::Pending;
        };
        match Pin::new(rx).poll(cx) {
            Poll::Ready(Ok(e)) => {
                *end = None;
                Poll::Ready(Some(e))
            }
            Poll::Ready(Err(_)) | Poll::Pending => Poll::Pending,
        }
    }

    /// The sink's next event, then its error if it failed. A batch's events come from the
    /// batch, not the channel: one channel pop, one release of its charge (after its last
    /// event) and two clock reads a batch, not one of each an event (#1737's F3-0).
    ///
    /// Each event still spends a unit of the task's Tokio budget, as a channel pop does: the
    /// connection's body task (hyper's `PipeToSendStream`) polls for frames until this goes
    /// `Pending`, so with the batch's events free it would run up to 128 batches on its
    /// network worker before yielding it, and the puts that worker serves would wait (#2027).
    fn poll_events(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Out, ApiError>>> {
        let (events, draining, taken, end) = match &mut self.0 {
            EventSource::Sink {
                events,
                draining,
                taken,
                end,
                ..
            } => (events, draining, taken, end),
        };
        if let Some((rest, _)) = draining.as_mut()
            && rest.len() > 0
        {
            // Out of budget: nothing taken, and the task is woken once it has yielded. The
            // batch's first event paid through the channel's pop.
            let coop = std::task::ready!(tokio::task::coop::poll_proceed(cx));
            let e = rest.next().expect("a batch event left");
            coop.made_progress();
            if rest.len() == 0 {
                // Its last event: the batch's bytes go back to the client's buffer.
                *draining = None;
                taken.took();
            } else {
                taken.took_within();
            }
            return Poll::Ready(Some(Ok(e)));
        }
        *draining = None;
        match events.poll_recv(cx) {
            // Taken: its bytes go back to the client's buffer.
            Poll::Ready(Some((Queued::One(e), room))) => {
                drop(room);
                taken.took();
                return Poll::Ready(Some(Ok(e)));
            }
            // A batch: its bytes stay charged until its last event is taken.
            Poll::Ready(Some((Queued::Many(batch), room))) => {
                taken.took();
                let mut rest = batch.into_iter();
                let e = rest.next().expect("a batch holds at least two events");
                *draining = Some((rest, room));
                return Poll::Ready(Some(Ok(e)));
            }
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => {}
        }
        let Some(rx) = end.as_mut() else {
            return Poll::Ready(None);
        };
        let out = match Pin::new(rx).poll(cx) {
            Poll::Ready(Ok(e)) => Some(Err(e)),
            Poll::Ready(Err(_)) => None,
            Poll::Pending => return Poll::Pending,
        };
        *end = None;
        Poll::Ready(out)
    }
}

impl super::super::lease::LeaseHooks for FeedEvents {
    fn poll_own_end(&mut self, cx: &mut Context<'_>) -> Poll<Option<ApiError>> {
        Pin::new(self).poll_end(cx)
    }

    fn took(&self) {
        self.0.taken().took();
    }
}

impl Stream for FeedEvents {
    type Item = Result<proto::FeedEvent, ApiError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_out(cx)
            .map(|item| item.map(|r| r.map(Out::into_proto)))
    }
}

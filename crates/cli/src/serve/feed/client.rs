use super::*;

/// Where a client starts, as `subscribe` found it.
pub(super) enum Begin {
    /// On the ring at `at`, its position `seqno` and the token with it.
    Ring { at: u64, seqno: u64, token: Bytes },
    /// On its own feed (opened to check the start), and that start as a token. Boxed: the
    /// feed is large, and a client begins once.
    Own(Box<pigeonhole::Feed>, FeedToken),
}

/// How often a client on its own stream looks at its hub's ring for a place to rejoin.
const REJOIN_TICK: Duration = Duration::from_millis(250);

/// How often a client whose hub failed tries the key's current hub.
const HUB_RETRY: Duration = Duration::from_secs(1);

/// Where a client reads.
enum Source {
    /// The hub's ring, from this index.
    Ring(u64),
    /// Its own stream, holding a `--max-catch-up` slot. Boxed: a client changes source
    /// rarely.
    Own(Box<FeedStream>, OwnedSemaphorePermit),
}

pub(super) struct Client {
    pub(super) feeds: Arc<Feeds>,
    /// Its hub: replaced by the key's current one when it fails (F2).
    pub(super) hub: Mutex<Joined>,
    /// For a `Now` start, the database's seqno when it subscribed: commits at or below it
    /// are passed over, as an in-process feed opened then would never deliver them, even
    /// when the hub hadn't read them yet.
    pub(super) after: u64,
    /// Commits up to this seqno are passed over as already handled ([`Spec::skip`]).
    pub(super) skip: u64,
    /// How many of those would have been sent: at most [`SKIP_LIMIT`].
    pub(super) skipped: AtomicU64,
    /// [`Spec::announce`].
    pub(super) announce: bool,
    /// [`Spec::internal`].
    pub(super) internal: bool,
    pub(super) tables: Vec<Table>,
    pub(super) prefix: Vec<u8>,
    pub(super) visible: bool,
    pub(super) sink: Sink,
    /// Where it is, for the dump.
    pub(super) tracker: Arc<Tracker>,
}

/// A client's position: the last commit it covered and the token right after it.
struct Position {
    seqno: Option<u64>,
    token: Bytes,
    /// The token of the last event sent, to tell whether a progress event would say more.
    sent: Bytes,
    last_event: Instant,
}

impl Client {
    fn hub(&self) -> Arc<Hub> {
        Arc::clone(&self.hub.lock().unwrap_or_else(PoisonError::into_inner).0)
    }

    pub(super) async fn run(&self, begin: Begin) -> Result<(), ApiError> {
        let mut pos;
        // Where it starts ([`FeedEvents::start`]): the ring's position it joins at, known
        // now. An own stream's, once its first read has taken the start (`read_own`): a
        // refused resume reports that read's error instead.
        if let Begin::Ring { token, .. } = &begin {
            self.sink.started(token);
        }
        let mut source = match begin {
            Begin::Own(feed, start) => {
                // Opened at subscribe so a bad start fails the call. Waiting for a catch-up
                // slot, it would hold an engine reader (and its shard wakers): drop it, and
                // open it again from its start token once there's a slot.
                let (feed, slot) = match Arc::clone(&self.feeds.catch_up).try_acquire_owned() {
                    Ok(slot) => (*feed, slot),
                    Err(_) => {
                        drop(feed);
                        let slot = self.catch_up_slot().await?;
                        (
                            open(&self.tables, self.visible, &Start::Token(start))?,
                            slot,
                        )
                    }
                };
                let stream = FeedStream::new(feed);
                let token = token_of(&stream);
                // A position from the start: a token or the oldest start can rejoin as soon
                // as the ring covers it, without waiting for a commit (simp724's F1).
                pos = Position {
                    seqno: Some(stream.position().seqno()),
                    sent: token.clone(),
                    token,
                    last_event: Instant::now(),
                };
                Source::Own(Box::new(stream), slot)
            }
            Begin::Ring { at, seqno, token } => {
                pos = Position {
                    seqno: Some(seqno),
                    sent: token.clone(),
                    token,
                    last_event: Instant::now(),
                };
                Source::Ring(at)
            }
        };
        if self.announce {
            let event = proto::FeedEvent {
                token: pos.token.clone(),
                event: Some(proto::feed_event::Event::Progress(proto::Progress {})),
                partition: String::new(),
                xid: Default::default(),
                replayed: None,
            };
            self.send(Out::event(event), &mut pos).await?;
        }
        loop {
            source = match source {
                Source::Ring(at) => self.read_ring(at, &mut pos).await?,
                Source::Own(stream, slot) => self.read_own(stream, slot, &mut pos).await?,
            };
        }
    }

    async fn catch_up_slot(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        let sem = Arc::clone(&self.feeds.catch_up);
        if let Ok(p) = Arc::clone(&sem).try_acquire_owned() {
            return Ok(p);
        }
        self.feeds
            .stats
            .catch_up_waits
            .fetch_add(1, Ordering::Relaxed);
        self.tracker.set(WAITING, 0);
        tokio::select! {
            // A client that leaves while queued gives up its place.
            () = self.sink.closed() => Err(gone()),
            slot = sem.acquire_owned() => {
                slot.map_err(|_| ApiError::internal("the catch-up slots closed"))
            }
        }
    }

    /// Reads the ring from `at` until the client must leave it: fallen off the tail, or the
    /// hub failed. Then its own stream, from its own last token.
    async fn read_ring(&self, mut at: u64, pos: &mut Position) -> Result<Source, ApiError> {
        let hub = self.hub();
        // Subscribed before the first look at the ring, so no commit after it is missed.
        let mut head = hub.head.subscribe();
        // Reused across wakes: one allocation per client, not one per commit.
        let mut batch: Vec<Arc<Entry>> = Vec::with_capacity(64);
        // After a small batch, the head isn't looked at again before this (`Coalesce`, #1901).
        let mut quiet_until: Option<Instant> = None;
        loop {
            // Per delivery batch, as the hub does per read (perf287's P1 on #1755).
            super::super::placement::refresh();
            self.tracker.set(ON_RING, at);
            let (behind, failed) = {
                let r = hub.ring();
                if at < r.base {
                    (true, r.failed)
                } else {
                    let from = ((at - r.base) as usize).min(r.entries.len());
                    batch.extend(r.entries.range(from..).take(64).cloned());
                    (false, r.failed)
                }
            };
            if behind || (failed && batch.is_empty()) {
                if behind {
                    self.feeds.stats.fell_behind.fetch_add(1, Ordering::Relaxed);
                }
                // The slot first, then the feed: a waiting client holds no engine reader
                // (simp724's F3, feed1248's S2 on #1518). The token is its whole position.
                let slot = self.catch_up_slot().await?;
                let feed = open(
                    &self.tables,
                    self.visible,
                    &Start::Token(FeedToken::from_bytes(pos.token.to_vec())),
                )?;
                return Ok(Source::Own(Box::new(FeedStream::new(feed)), slot));
            }
            if batch.is_empty()
                && let Some(until) = quiet_until.take()
                && Instant::now() < until
            {
                tokio::select! {
                    () = self.sink.closed() => return Err(gone()),
                    () = tokio::time::sleep_until(until.into()) => {}
                }
                continue;
            }
            if batch.is_empty() {
                let quiet =
                    tokio::time::sleep_until((pos.last_event + self.feeds.limits.quiet).into());
                tokio::select! {
                    // A client that went away ends here, even one whose prefix never
                    // matches (it would otherwise hold its stream slot until a send failed).
                    () = self.sink.closed() => return Err(gone()),
                    changed = head.changed() => {
                        if changed.is_err() {
                            return Err(ApiError::internal("the feed's hub ended"));
                        }
                    }
                    () = quiet => {
                        // At the head: the hub's feed may have got further than its last
                        // commit (other tables' writes), and its token with it.
                        {
                            let r = hub.ring();
                            if at == r.head() && pos.seqno.is_none_or(|s| r.passed > s) {
                                pos.seqno = Some(r.passed);
                                pos.token = r.passed_token.clone();
                            }
                        }
                        self.progress(pos).await?;
                    }
                }
                continue;
            }
            let n = batch.len() as u32;
            // The pass's events go to the connection as one batch (#1737's F3-0): one place in
            // its channel and one charge against its buffer, not one per event. Its entries
            // are held as `Arc`s, so the ring overrunning while the batch waits for room takes
            // none of them: the next read finds the client behind, and it goes on from
            // `pos.token`, the last commit covered here (simp724's (2)).
            let mut outs = Vec::with_capacity(batch.len());
            for e in batch.drain(..) {
                at += 1;
                if let Some(out) = self.cover_entry(&e, pos)? {
                    outs.push(out);
                }
            }
            self.send_all(outs, pos).await?;
            let client = self.feeds.limits.coalesce.client;
            if n <= SMALL_BATCH && !client.is_zero() {
                quiet_until = Some(Instant::now() + client);
            }
        }
    }

    /// Reads its own stream until it can rejoin the ring.
    async fn read_own(
        &self,
        mut stream: Box<FeedStream>,
        slot: OwnedSemaphorePermit,
        pos: &mut Position,
    ) -> Result<Source, ApiError> {
        let mut hub_retry: Option<Instant> = None;
        // Its stream is read once, without waiting, before the client may rejoin the ring: a
        // resume whose token the engine refused (another history, retention passed) reports
        // that on the first read, and a ring that happens to cover the token's seqno would
        // otherwise take the client on without it (#1805's Windows run: a stale copy's token
        // went on). Nothing ready yet: it may rejoin at once.
        let before = pos.token.clone();
        let first =
            std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut *stream).poll_next(cx))).await;
        if !matches!(first, Poll::Ready(Some(Err(_)))) {
            // The start is taken: where it is ([`FeedEvents::start`]), before any commit.
            self.sink.started(&before);
        }
        match first {
            Poll::Ready(Some(Ok(commit))) => {
                let token = token_of(&stream);
                self.cover(&commit, &token, pos).await?;
                pos.seqno = Some(stream.position().seqno());
            }
            Poll::Ready(Some(Err(e))) => return Err(e.into()),
            Poll::Ready(None) => {
                return Err(ApiError::internal("the feed ended without an error"));
            }
            Poll::Pending => {}
        }
        loop {
            super::super::placement::refresh();
            // A failed hub: take the key's current one, opening a new one if none (F2), at
            // most once a second so a hub that keeps failing isn't reopened in a loop.
            let mut hub = self.hub();
            if hub.ring().failed && hub_retry.is_none_or(|t| t.elapsed() >= HUB_RETRY) {
                hub_retry = Some(Instant::now());
                if let Ok(new) = self.feeds.hub(&self.tables, self.visible) {
                    let joined = new.join(&self.tracker, self.internal);
                    *self.hub.lock().unwrap_or_else(PoisonError::into_inner) = joined;
                    hub = new;
                }
            }
            self.tracker.set(ON_OWN, pos.seqno.unwrap_or(0));
            if let Some(seqno) = pos.seqno {
                let r = hub.ring();
                if !r.failed
                    && let Some(at) = r.rejoin_at(seqno)
                {
                    drop(r);
                    drop(slot);
                    self.feeds.stats.rejoined.fetch_add(1, Ordering::Relaxed);
                    return Ok(Source::Ring(at));
                }
            }
            // Woken by a commit, or every `REJOIN_TICK` to look at the ring again: a caught-up
            // client gives its catch-up slot back as soon as it can rejoin.
            let tick = tokio::time::sleep(REJOIN_TICK.min(self.feeds.limits.quiet));
            tokio::select! {
                () = self.sink.closed() => return Err(gone()),
                item = next(&mut stream) => match item {
                    Some(Ok(commit)) => {
                        let token = token_of(&stream);
                        self.cover(&commit, &token, pos).await?;
                        pos.seqno = Some(stream.position().seqno());
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => return Err(ApiError::internal("the feed ended without an error")),
                },
                () = tick => {
                    // The stream may have read past commits of other tables: its position
                    // moved without a commit, and its token with it.
                    let at = stream.position().seqno();
                    if pos.seqno.is_none_or(|s| at > s) {
                        pos.seqno = Some(at);
                        pos.token = token_of(&stream);
                    }
                }
            }
            // After either arm: commits that keep arriving, none matching the prefix, would
            // otherwise win every round and the token would never advance (audit120).
            if pos.last_event.elapsed() >= self.feeds.limits.quiet {
                self.progress(pos).await?;
            }
        }
    }

    /// Covers one commit of the hub's spec: sends it if a row matches the prefix, else passes
    /// over it (a later progress event carries the position).
    async fn cover(
        &self,
        commit: &Commit,
        token: &Bytes,
        pos: &mut Position,
    ) -> Result<(), ApiError> {
        pos.seqno = Some(commit.seqno());
        pos.token = token.clone();
        if commit.seqno() <= self.after {
            return Ok(());
        }
        if let Some(c) = filtered(commit, &self.prefix) {
            if self.skipping(commit.seqno())? {
                return Ok(());
            }
            let tag = commit.xid().unwrap_or_default();
            self.send(Out::event(commit_event(token.clone(), c, tag)?), pos)
                .await?;
        }
        Ok(())
    }

    /// Whether the commit at `seqno`, which would be sent, is passed over as one the client
    /// already handled ([`Spec::skip`]); `InvalidArgument` past [`SKIP_LIMIT`] of them.
    fn skipping(&self, seqno: u64) -> Result<bool, ApiError> {
        if seqno > self.skip {
            return Ok(false);
        }
        if self.skipped.fetch_add(1, Ordering::Relaxed) >= SKIP_LIMIT {
            return Err(ApiError::invalid(format!(
                "skip_through passes over more than {SKIP_LIMIT} commits after the token \
                 (through seqno {}): a client can't have handled more after a checkpoint",
                self.skip
            )));
        }
        Ok(true)
    }

    /// Covers one entry of the ring. A client with no prefix sends the entry itself: its
    /// event is encoded once, for every such client (perf287's profile on #1536: building
    /// and encoding the same event per subscriber was two thirds of the feed's CPU). The
    /// position moves past the entry here; the event, if any, is the caller's to send
    /// ([`Client::send_all`]).
    fn cover_entry(&self, e: &Arc<Entry>, pos: &mut Position) -> Result<Option<Out>, ApiError> {
        pos.seqno = Some(e.seqno);
        pos.token = e.token.clone();
        if e.seqno <= self.after || e.encoded().is_none() {
            return Ok(None);
        }
        if self.prefix.is_empty() {
            if self.skipping(e.seqno)? {
                return Ok(None);
            }
            return Ok(Some(Out::Shared(Arc::clone(e))));
        }
        // A prefix: the entry's changes to matching rows, if any (a later progress event
        // carries the position past one with none), the shared encoding when they all
        // match (#1780).
        match prefix_out(e, &self.prefix) {
            Some(_) if self.skipping(e.seqno)? => Ok(None),
            out => Ok(out),
        }
    }

    /// A progress event, if the position moved since the last event and the stream was quiet.
    async fn progress(&self, pos: &mut Position) -> Result<(), ApiError> {
        if pos.token == pos.sent {
            // Nothing to say: look again after another quiet interval.
            pos.last_event = Instant::now();
            return Ok(());
        }
        self.send(
            Out::event(proto::FeedEvent {
                token: pos.token.clone(),
                event: Some(proto::feed_event::Event::Progress(proto::Progress {})),
                partition: String::new(),
                xid: Default::default(),
                replayed: None,
            }),
            pos,
        )
        .await
    }

    async fn send(&self, event: Out, pos: &mut Position) -> Result<(), ApiError> {
        let token = event.token().clone();
        self.sink.put(event).await?;
        pos.sent = token;
        pos.last_event = Instant::now();
        Ok(())
    }

    /// Sends `events`, in order, as one batch where the client's buffer allows
    /// ([`Sink::put_all`]). Each event carries its own token, so a resume after any of them
    /// neither skips nor repeats.
    async fn send_all(&self, events: Vec<Out>, pos: &mut Position) -> Result<(), ApiError> {
        let Some(token) = events.last().map(|e| e.token().clone()) else {
            return Ok(());
        };
        self.sink.put_all(events).await?;
        pos.sent = token;
        pos.last_event = Instant::now();
        Ok(())
    }
}

/// The error a feed stream whose client took no event for `idle` (`--stream-idle-ms`) ends
/// with, on a single node or a cluster (#2021): resumable from the last event's token.
pub(crate) fn idle_error(idle: Duration) -> ApiError {
    ApiError::deadline(format!(
        "the client took no feed event for --stream-idle-ms ({} ms); resume from the last \
         event's token",
        idle.as_millis()
    ))
}

/// The error a send to a client that went away ends with.
pub(super) fn gone() -> ApiError {
    ApiError::internal("the client went away")
}

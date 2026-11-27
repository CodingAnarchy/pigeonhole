use super::*;

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct HubKey {
    /// The tables' feed ids, so a table dropped and created again under its name is another
    /// key (feed1248's R2, simp724's F4 on #1518).
    pub(super) tables: Vec<u64>,
    pub(super) visible: bool,
}

/// What a hub's ring holds (#1798): a single node's commits ([`Single`]), or a cluster
/// feed hub's merged events (`cluster::hub`). A type parameter, never a trait object, so
/// the single node's ring compiles as it did before the cluster's shared it (simp724's Q3).
pub(crate) trait RingKind: Sized + Send + Sync + 'static {
    type Entry: Send + Sync + 'static;
    /// What the ring knows beside its entries: where its feed is.
    type State: Send + 'static;
    /// An entry's share of `--feed-ring-bytes`.
    fn bytes(e: &Self::Entry) -> usize;
    /// `e` left the ring's front.
    fn evicted(_state: &mut Self::State, _e: &Self::Entry) {}
}

/// A hub's bounded ring: its entries, the index of the first in the hub's whole sequence,
/// their bytes, and the kind's state (reached through `Deref`).
pub(crate) struct RingOf<K: RingKind> {
    pub(crate) entries: VecDeque<Arc<K::Entry>>,
    /// The index of `entries[0]` in the hub's whole sequence.
    pub(crate) base: u64,
    pub(crate) bytes: usize,
    pub(crate) state: K::State,
}

impl<K: RingKind> std::ops::Deref for RingOf<K> {
    type Target = K::State;

    fn deref(&self) -> &K::State {
        &self.state
    }
}

impl<K: RingKind> std::ops::DerefMut for RingOf<K> {
    fn deref_mut(&mut self) -> &mut K::State {
        &mut self.state
    }
}

impl<K: RingKind> RingOf<K> {
    pub(crate) fn new(state: K::State) -> Self {
        Self {
            entries: VecDeque::new(),
            base: 0,
            bytes: 0,
            state,
        }
    }

    pub(crate) fn head(&self) -> u64 {
        self.base + self.entries.len() as u64
    }

    /// Appends `e`, then drops entries from the front while the ring is over `limit`, always
    /// keeping the newest: a client at the head needs its boundary.
    pub(crate) fn push(&mut self, e: Arc<K::Entry>, limit: usize) {
        self.bytes += K::bytes(&e);
        self.entries.push_back(e);
        while self.bytes > limit && self.entries.len() > 1 {
            if let Some(e) = self.entries.pop_front() {
                self.bytes -= K::bytes(&e);
                self.base += 1;
                K::evicted(&mut self.state, &e);
            }
        }
    }
}

/// A single node's ring ([`RingKind`]): its commits as [`Entry`]s.
pub(crate) struct Single;

impl RingKind for Single {
    type Entry = Entry;
    type State = SingleRing;

    fn bytes(e: &Entry) -> usize {
        e.bytes
    }
}

/// Where a single node's hub feed is, beside its ring.
#[derive(Default)]
pub(crate) struct SingleRing {
    /// The hub feed's position when it opened: with no entry yet, the ring holds every
    /// commit of the spec above it.
    pub(super) start_seqno: u64,
    /// The hub feed's latest position, after its last commit or its last poll that found
    /// none (so it follows other tables' writes too), and the token taken with it: one pair,
    /// from one poll (simp724's F1 on #1518).
    pub(super) passed: u64,
    pub(super) passed_token: Bytes,
    /// The shared feed ended (an error): clients move to their own streams.
    pub(super) failed: bool,
}

pub(super) type Ring = RingOf<Single>;

impl Ring {
    /// Where a client at position `seqno` rejoins: the index of the first entry after it.
    /// The client has covered every commit of the spec up to `seqno` (its own stream's
    /// `position`), and the ring holds every one after the position before its first entry.
    /// So it rejoins only if that position is at most `seqno` (nothing between them was
    /// evicted) and the hub's feed has passed `seqno` (nothing it will add is one the client
    /// has): feed1248's (2) on ticket 5.
    pub(super) fn rejoin_at(&self, seqno: u64) -> Option<u64> {
        let covered_from = self.entries.front().map_or(self.start_seqno, |e| e.prev);
        if covered_from > seqno || self.passed < seqno {
            return None;
        }
        // The entries are in seqno order (the hub pushes its feed's commits as they come),
        // so the first one after `seqno` is found by bisection. A scan from the front walked
        // nearly the whole ring, with its lock held, for a client that rejoins near the
        // head: 2.2 ms at a million entries (audit120's pass 2).
        let i = self.entries.partition_point(|e| e.seqno <= seqno);
        Some(self.base + i as u64)
    }
}

/// A hub: its ring, its task, and its members.
pub(crate) struct HubOf<K: RingKind> {
    ring: Mutex<RingOf<K>>,
    /// The ring's head index, bumped by each push and by a failure.
    pub(crate) head: watch::Sender<u64>,
    /// Its task, once it's spawned after the hub is made ([`set_task`](Self::set_task),
    /// #1926): replaced each time the task moves between runtimes ([`drive`]).
    task: Mutex<Option<tokio::task::AbortHandle>>,
    /// Its clients, on its ring or on their own streams until they rejoin it: a [`JoinedOf`]
    /// each. At most one, a single node's hub task runs on the feed runtime (#1689).
    members: AtomicUsize,
    /// Its members that aren't a cluster hub's merge (#1798). While it has members and none
    /// of these, its task stays on the hub runtime: its readers are merges on the feed
    /// runtime, so running it beside them (#1689) only adds the engine's reads to the
    /// runtime that delivers every cluster subscriber's events (g100).
    external: AtomicUsize,
    /// Bumped by each join, so the task sees a client that joined and left between two of
    /// its looks at `members`.
    joins: AtomicU64,
    /// Whether its task runs on the feed runtime beside its one client (#1689), for the
    /// gauge and the dump.
    pub(super) solo: AtomicBool,
    stats: Arc<FeedStats>,
    /// Live hubs of its kind: `--max-feed-hubs` counts them.
    live: Arc<AtomicU64>,
    /// The node's rings that split `--feed-ring-bytes`: every hub with a member.
    sharers: Arc<AtomicU64>,
    /// Its clients' trackers, for the dump; a gone client's is pruned there.
    pub(super) clients: Mutex<Vec<Weak<Tracker>>>,
    /// Times it told its clients its head moved (an inspection count for tests, #1901).
    #[cfg(test)]
    pub(super) notifies: AtomicU64,
}

pub(crate) type Hub = HubOf<Single>;

impl<K: RingKind> HubOf<K> {
    /// A hub with `ring`, holding a place in `live`. Its task is spawned after it, holding
    /// it, and set with [`set_task`](Self::set_task) (#1926).
    pub(crate) fn new(
        ring: RingOf<K>,
        solo: bool,
        stats: Arc<FeedStats>,
        live: Arc<AtomicU64>,
        sharers: Arc<AtomicU64>,
    ) -> Self {
        let (head, _) = watch::channel(ring.head());
        Self {
            ring: Mutex::new(ring),
            head,
            task: Mutex::new(None),
            members: AtomicUsize::new(0),
            external: AtomicUsize::new(0),
            joins: AtomicU64::new(0),
            solo: AtomicBool::new(solo),
            stats,
            live,
            sharers,
            clients: Mutex::default(),
            #[cfg(test)]
            notifies: AtomicU64::new(0),
        }
    }

    pub(crate) fn ring(&self) -> MutexGuard<'_, RingOf<K>> {
        self.ring.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn task(&self) -> MutexGuard<'_, Option<tokio::task::AbortHandle>> {
        self.task.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sets its task's handle: the task starts after the hub is made, holding it (#1926).
    pub(crate) fn set_task(&self, task: tokio::task::AbortHandle) {
        *self.task() = Some(task);
    }

    /// Makes `tracker`'s client one of this hub's members until the [`JoinedOf`] drops;
    /// `internal`: a cluster hub's merge (#1798), which keeps the task on the hub runtime.
    pub(crate) fn join(self: &Arc<Self>, tracker: &Arc<Tracker>, internal: bool) -> JoinedOf<K> {
        self.track(tracker);
        // `external` before `members` (Release): whoever sees this membership in `members`
        // (Acquire, [`HubOf::internal`]) sees a client's `external` with it (cli692's N1).
        if !internal {
            self.external.fetch_add(1, Ordering::Relaxed);
        }
        if self.members.fetch_add(1, Ordering::Release) == 0 {
            self.sharers.fetch_add(1, Ordering::Relaxed);
        }
        self.joins.fetch_add(1, Ordering::Relaxed);
        JoinedOf(Arc::clone(self), internal)
    }

    /// Its members now.
    pub(crate) fn members(&self) -> usize {
        self.members.load(Ordering::Relaxed)
    }

    /// Its ring's cap: an equal share of `--feed-ring-bytes` (`total`) among the rings with
    /// a member, a cluster hub's and the partition hubs its merges read alike (#1798).
    pub(crate) fn ring_limit(&self, total: usize) -> usize {
        total / self.sharers.load(Ordering::Relaxed).max(1) as usize
    }

    /// Whether its members are all cluster hubs' merges, and there's at least one. `members`
    /// first: a client counted there is counted in `external` too ([`HubOf::join`]). A client
    /// leaving can still read as a merge for a moment, which costs at most one move to the
    /// hub runtime and back after `solo_after`.
    pub(super) fn internal(&self) -> bool {
        self.members.load(Ordering::Acquire) > 0 && self.external.load(Ordering::Relaxed) == 0
    }

    /// Counts `tracker`'s client under this hub, for the dump. Gone clients and those that
    /// moved to another hub are pruned here whenever the list reaches a power of two, so it
    /// stays bounded on a server nobody dumps (perf287's R1 on #1643), and in the dump.
    pub(crate) fn track(&self, tracker: &Arc<Tracker>) {
        let me = std::ptr::from_ref(self) as usize;
        tracker.hub.store(me, Ordering::Relaxed);
        let mut clients = self.clients.lock().unwrap_or_else(PoisonError::into_inner);
        if clients.len() >= 16 && clients.len().is_power_of_two() {
            clients.retain(|w| {
                w.upgrade()
                    .is_some_and(|t| t.hub.load(Ordering::Relaxed) == me)
            });
        }
        clients.push(Arc::downgrade(tracker));
    }

    /// Its live clients' trackers, pruning gone ones and those counted under another hub.
    pub(crate) fn trackers(&self) -> Vec<Arc<Tracker>> {
        let me = std::ptr::from_ref(self) as usize;
        let mut c = self.clients.lock().unwrap_or_else(PoisonError::into_inner);
        c.retain(|w| {
            w.upgrade()
                .is_some_and(|t| t.hub.load(Ordering::Relaxed) == me)
        });
        c.iter().filter_map(Weak::upgrade).collect()
    }
}

impl Hub {
    /// Pushes `commit` onto the ring; `Err` (the hub must fail) for one whose tag is
    /// corrupt (D336 §8): each client meets that on its own stream.
    pub(super) fn push(
        &self,
        commit: Commit,
        token: Bytes,
        position: u64,
        total: usize,
    ) -> Result<(), crate::serve::errors::ApiError> {
        let limit = self.ring_limit(total);
        // Encoded here, on the hub's task and outside the ring's lock.
        let seqno = commit.seqno();
        let mut entry = Entry::new(&commit, token.clone())?;
        let mut r = self.ring();
        entry.prev = r.passed;
        r.passed = position.max(seqno);
        r.passed_token = token;
        r.push(Arc::new(entry), limit);
        Ok(())
    }

    /// Tells the clients the ring's head moved: once per batch of pushes ([`drive`], #1901).
    fn notify(&self) {
        let head = self.ring().head();
        self.head.send_replace(head);
        #[cfg(test)]
        self.notifies.fetch_add(1, Ordering::Relaxed);
    }

    /// The hub's feed found nothing at `position`: the ring holds everything up to it.
    ///
    /// The token is made outside the ring's lock: its cost grows with what the feed holds
    /// unreturned, and clients and subscribes take that lock on network threads (#1397's
    /// fan-out wedge). Only the hub's own task moves `passed`, so it can't move between the
    /// check and the store.
    fn note_passed(&self, position: u64, token: impl FnOnce() -> Bytes) {
        if position <= self.ring().passed {
            return;
        }
        let token = token();
        let mut r = self.ring();
        if position > r.passed {
            r.passed = position;
            r.passed_token = token;
        }
    }

    pub(super) fn fail(&self) {
        self.ring().failed = true;
        self.stats.hub_failures.fetch_add(1, Ordering::Relaxed);
        // Wake every client: each moves to its own stream once it has taken the ring's rest.
        // The head's current value, so the watch says where the ring is (perf2's nit on #1938).
        self.notify();
    }
}

impl<K: RingKind> Drop for HubOf<K> {
    fn drop(&mut self) {
        // The last client left: the hub's feed (and its shard wakers) goes.
        if let Some(task) = self.task.get_mut().unwrap_or_else(PoisonError::into_inner) {
            task.abort();
        }
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A client's membership of its hub, counted in the hub's `members` while held; `.1`:
/// a cluster hub's merge ([`HubOf::join`]).
pub(crate) struct JoinedOf<K: RingKind>(pub(crate) Arc<HubOf<K>>, bool);

pub(super) type Joined = JoinedOf<Single>;

impl<K: RingKind> Drop for JoinedOf<K> {
    fn drop(&mut self) {
        let hub = &self.0;
        if hub.members.fetch_sub(1, Ordering::Relaxed) == 1 {
            hub.sharers.fetch_sub(1, Ordering::Relaxed);
        }
        if !self.1 {
            hub.external.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Where a hub's task runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Place {
    /// The feed runtime, beside its one client's task: a commit wakes the client without a
    /// cross-runtime handoff (#1689, g64's +6.4% CPU per put at one subscriber).
    Feed,
    /// The hub runtime, apart from its clients' delivery, so a hub never queues behind its
    /// clients' tasks (#1397's fan-out wedge).
    Hubs,
}

/// A hub's task: its engine stream, read into its ring.
pub(super) struct Driver {
    pub(super) stream: FeedStream,
    pub(super) hub: Weak<Hub>,
    /// `--feed-ring-bytes`, shared by the live hubs.
    pub(super) ring_bytes: usize,
    /// The server's feed and hub runtimes, when it has both: the task moves between them.
    pub(super) runtimes: Option<(tokio::runtime::Handle, tokio::runtime::Handle)>,
    pub(super) solo_after: Duration,
    pub(super) place: Place,
    /// When the task first saw its hub alone on the hub runtime, and the joins then.
    pub(super) alone: Option<(Instant, u64)>,
    /// `Coalesce::hub`.
    pub(super) coalesce: Duration,
    /// Pushes since the clients were last told ([`NOTIFY_EVERY`]).
    pub(super) unnotified: u32,
    /// Commits read since the feed last had nothing.
    pub(super) batch: u32,
    /// After a small batch, the feed isn't polled again before this ([`Coalesce`]).
    pub(super) quiet_until: Option<Instant>,
    /// Set once the task has tried its hub with its first item (`Feeds::after_hub_spawn`'s
    /// hold seam, #1926).
    #[cfg(test)]
    pub(super) started: Option<Arc<Started>>,
}

/// A drive task's first item handled: what [`Feeds::after_hub_spawn`] waits for (#1926).
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct Started(Mutex<bool>, std::sync::Condvar);

#[cfg(test)]
impl Started {
    fn set(&self) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.1.notify_all();
    }

    /// Waits up to `within` for the task's first item.
    pub(crate) fn wait(&self, within: Duration) -> bool {
        let g = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let (g, _) = self
            .1
            .wait_timeout_while(g, within, |set| !*set)
            .unwrap_or_else(PoisonError::into_inner);
        *g
    }
}

impl Driver {
    /// Where the task should run now, if not where it is: on the hub runtime at once with two
    /// or more clients, or with only cluster hubs' merges ([`HubOf::external`]); back on the
    /// feed runtime after `solo_after` with at most one other client.
    ///
    /// Asked only after a commit, so a hub left alone on the hub runtime by a quiet table
    /// stays there until its next commit. That's harmless: with no commits there's no
    /// handoff to save (perf287's N1 on #1716).
    pub(super) fn moves(&mut self, hub: &Hub) -> Option<Place> {
        self.runtimes.as_ref()?;
        let shared = hub.members.load(Ordering::Relaxed) >= 2 || hub.internal();
        match self.place {
            Place::Feed => shared.then_some(Place::Hubs),
            Place::Hubs if shared => {
                self.alone = None;
                None
            }
            Place::Hubs => {
                let joins = hub.joins.load(Ordering::Relaxed);
                match self.alone {
                    Some((since, j)) if j == joins => {
                        (since.elapsed() >= self.solo_after).then_some(Place::Feed)
                    }
                    _ => {
                        self.alone = Some((Instant::now(), joins));
                        None
                    }
                }
            }
        }
    }
}

/// Runs a hub's task: reads its engine stream into its ring until the hub goes or the stream
/// fails. When the hub's client count calls for the other runtime (#1689), it spawns itself
/// there between two commits, handing over the stream itself, so the ring sees one unbroken
/// sequence of pushes. Boxed: the task spawns its own kind.
pub(super) fn drive(mut d: Driver) -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async move {
        loop {
            // A mask a partition's open or close published (D317 ticket 6): a busy hub
            // may not park for the whole load (perf287's P1 on #1755).
            super::super::placement::refresh();
            // After a small batch (#1901): the commits arriving meanwhile are read together.
            if let Some(until) = d.quiet_until.take()
                && Instant::now() < until
            {
                tokio::time::sleep_until(until.into()).await;
            }
            // A poll that finds nothing records how far the feed got, so the ring's `passed`
            // follows other tables' writes (the engine wakes the hub on each frontier move).
            // It also ends a batch: the clients are told of its pushes, and after a small one
            // the next poll waits (`Coalesce::hub`).
            let item = std::future::poll_fn(|cx| {
                let polled = Pin::new(&mut d.stream).poll_next(cx);
                if polled.is_pending()
                    && let Some(hub) = d.hub.upgrade()
                {
                    let at = d.stream.position().seqno();
                    hub.note_passed(at, || token_of(&d.stream));
                    if d.unnotified > 0 {
                        hub.notify();
                        d.unnotified = 0;
                    }
                    if d.batch > 0 && d.batch <= SMALL_BATCH && !d.coalesce.is_zero() {
                        d.quiet_until = Some(Instant::now() + d.coalesce);
                    }
                    d.batch = 0;
                }
                polled
            })
            .await;
            // `None` only once every client has let the hub go: the task starts after its hub
            // is made (`Feeds::hub`, #1926).
            let hub = d.hub.upgrade();
            #[cfg(test)]
            if let Some(started) = d.started.take() {
                started.set();
            }
            let Some(hub) = hub else { return };
            match item {
                Some(Ok(commit)) => {
                    let at = d.stream.position().seqno();
                    if hub.push(commit, token_of(&d.stream), at, d.ring_bytes).is_err() {
                        hub.fail();
                        return;
                    }
                    d.batch = d.batch.saturating_add(1);
                    d.unnotified += 1;
                    if d.unnotified >= NOTIFY_EVERY {
                        hub.notify();
                        d.unnotified = 0;
                    }
                    if let Some(to) = d.moves(&hub)
                        && let Some((feed, hubs)) = d.runtimes.clone()
                    {
                        d.place = to;
                        d.alone = None;
                        // Its clients hear of every push before the task moves.
                        if d.unnotified > 0 {
                            hub.notify();
                            d.unnotified = 0;
                        }
                        let rt = if to == Place::Feed { feed } else { hubs };
                        // Under the task's lock and holding `hub`: the hub can't drop (and
                        // abort the old handle) before the new task's handle is in place. A
                        // hub failed meanwhile (`fail_all` aborts the handle it finds, this
                        // one, which is past its last await) stops here instead, its place
                        // and the count as they were (perf287's N2 on #1716).
                        let mut task = hub.task();
                        if !hub.ring().failed {
                            hub.solo.store(to == Place::Feed, Ordering::Relaxed);
                            hub.stats.hub_moves.fetch_add(1, Ordering::Relaxed);
                            *task = Some(rt.spawn(drive(d)).abort_handle());
                        }
                        return;
                    }
                    drop(hub);
                    // A feed with a backlog is always ready, and it isn't part of Tokio's
                    // budget: without this, a hub that fell behind keeps its worker, and the
                    // other hubs on its runtime starve (#1397's fan-out wedge; perf287's N1
                    // on #1696).
                    tokio::task::coop::consume_budget().await;
                }
                // An expiry, `Closed`, damage: each client goes on from its own token on its
                // own stream, and the engine decides each (feed1248's (5)).
                _ => {
                    hub.fail();
                    return;
                }
            }
        }
    })
}

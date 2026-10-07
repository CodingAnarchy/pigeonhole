//! Tablet splits, merges and moves, and the balancer that decides them (spec: Tablets and
//! Rebalancing; issue #38).
//!
//! Every change runs on the shard that owns the tablets, one at a time, on the same path:
//!
//! 1. **Start.** The tablets join `moving`. From now on the shard parks single-shard commits
//!    that touch them (unlogged, so they hold no seqno and never block the watermark) and
//!    refuses PREPAREs that touch them with [`PrepareError::Moved`]; the coordinator retries
//!    such a commit once the tablet map has changed. Their memtables are frozen and flushed.
//! 2. **Drain.** The change waits until every memtable of the tablets is in SSTs, no prepared
//!    share and no compaction touches them. Shares decided after the freeze land in a fresh
//!    memtable, which is frozen and flushed again.
//! 3. **Commit.** One manifest request (`ReqKind::Tablets`) retires the old tablets and adds
//!    the new ones (`PutTablet`/`DropTablet`; children reference their parent's SSTs until
//!    compaction rewrites them, D13) and hands out owners. Its view publishes the new tablet
//!    map, memtables and SST set together, so reads are never blocked and never see a
//!    partial change. A shard receiving tablets has its default-timestamp floor raised first
//!    (D11: the floor travels with the tablet).
//! 4. **Forward.** Parked commits are routed again through the new map: to this shard's
//!    queue, to the new owner, or through two-phase commit when their rows now span shards.

use super::*;

use pigeonhole_format::key::{Escaped, row_prefix_len};
use pigeonhole_format::manifest::SstMeta;

use crate::catalog::Catalog;
use crate::manifest::{ReqKind, TabletChange, TabletEdits};
use crate::snapshot::TabletEntry;

/// Row samples kept per tablet and balancer interval (split points for write skew).
const SAMPLES: usize = 64;

/// Weight of the newest interval in the moving averages of write load (#103): one interval's
/// noise never decides a move on its own.
const LOAD_ALPHA: f64 = 0.5;

/// Balancer passes a shard waits after starting a move or a skew split, and a tablet stays
/// put after it arrived on a shard, before write skew moves or splits again (#103): the
/// moving averages catch up with a change before the next one is decided.
const DWELL_PASSES: u64 = 10;

/// The longest balancer interval of an idle shard: passes that find nothing to do, with no
/// writes and no tablet change since the last one, double the interval up to this (10 s).
const BALANCE_IDLE_CAP_NANOS: u64 = 10_000_000_000;

/// Slots reserved on shards receiving tablets (`LoadSlot::reserved`).
type Reserved = Vec<(ShardId, usize)>;

/// A change to tablets this shard owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TabletOpKind {
    /// Split `tablet` at `keys` (sorted, strictly inside its range); child `i` covers
    /// `[keys[i - 1], keys[i])` and goes to `owners[i]`.
    Split {
        tablet: TabletId,
        keys: Vec<Vec<u8>>,
        owners: Vec<ShardId>,
    },
    /// Merge `left` with `right`, its right neighbour of the same table; both on this shard.
    Merge { left: TabletId, right: TabletId },
    /// Hand `tablet` to shard `to`.
    Move { tablet: TabletId, to: ShardId },
}

impl TabletOpKind {
    /// The tablets the change retires (or hands away).
    fn tablets(&self) -> Vec<TabletId> {
        match self {
            TabletOpKind::Split { tablet, .. } | TabletOpKind::Move { tablet, .. } => {
                vec![*tablet]
            }
            TabletOpKind::Merge { left, right } => vec![*left, *right],
        }
    }

    /// The other shards receiving a tablet.
    fn targets(&self, me: ShardId) -> Vec<ShardId> {
        let mut out = match self {
            TabletOpKind::Split { owners, .. } => owners.clone(),
            TabletOpKind::Move { to, .. } => vec![*to],
            TabletOpKind::Merge { .. } => Vec::new(),
        };
        out.retain(|s| *s != me);
        out.sort_unstable();
        out.dedup();
        out
    }
}

/// The change in progress on a shard.
#[derive(Debug)]
pub(crate) struct ActiveOp {
    kind: TabletOpKind,
    reply: Option<Notifier<Result<()>>>,
    /// The tablets as they were when the change started; the commit checks they are still
    /// so (a table drop may have raced it).
    entries: Vec<TabletEntry>,
    /// The manifest request is in flight.
    committing: bool,
    /// Slots reserved on each receiving shard (`LoadSlot::reserved`), released when the
    /// change ends.
    reserved: Reserved,
}

/// Writes to one tablet during the current balancer interval.
#[derive(Debug, Default)]
pub(crate) struct TabletLoad {
    /// Rows written this interval and the one before.
    writes: u64,
    prev_writes: u64,
    /// Rows seen by the reservoir this interval.
    seen: u64,
    /// A uniform sample of the rows written this interval.
    samples: Vec<Vec<u8>>,
    /// Moving average of `writes` over intervals (`LOAD_ALPHA`).
    ewma: f64,
}

/// The balancer settings, copied from `EngineOptions` at open.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BalanceConfig {
    /// `EngineOptions::tablet_changes`: off, nothing splits, merges or moves.
    pub enabled: bool,
    pub interval_nanos: u64,
    pub min_writes: u64,
    pub skew: f64,
    pub split_bytes: u64,
}

/// One shard's load as of its last balancer interval, read by every other shard.
#[derive(Debug, Default)]
#[repr(align(64))]
pub(crate) struct LoadSlot {
    /// Moving average of the rows written per interval.
    pub writes: AtomicU64,
    /// `(tablet, family)` slots promised to changes in flight that will hand this shard
    /// tablets: every change checks them with the slots in the view, so concurrent moves
    /// never take a shard past `max_slots` (#103).
    pub reserved: AtomicUsize,
    /// When it was published (monotonic nanoseconds; 0 = never).
    pub at: AtomicU64,
}

/// Where a member's rows live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Routing {
    Owned,
    /// A row is in a tablet this shard is splitting, merging or moving.
    Moving,
    /// A row belongs to another shard (the router used an older tablet map).
    Elsewhere,
}

impl ShardState {
    // ---- routing at admission ----

    /// Admission routing for one member: returns it if this shard should run it now; parks,
    /// forwards or refuses it otherwise.
    pub(super) fn route_member(
        &mut self,
        m: Member,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) -> Option<Member> {
        if matches!(m.kind, MemberKind::CommitRecord { .. }) {
            return Some(m);
        }
        if self.below_floor(&m) {
            // The coordinator's retry reads this shard's floor again.
            let floor = self.ts_floor();
            self.shared.ts_floors[usize::from(self.id.0)]
                .0
                .fetch_max(floor, Ordering::AcqRel);
            self.refuse_prepare(m, PrepareError::BelowFloor, ctx);
            return None;
        }
        let single = matches!(m.kind, MemberKind::Single);
        if self.moving.is_empty() && m.map_version >= self.lost_version {
            return Some(m);
        }
        let mut routing = self.routing_of(&m);
        if routing == Routing::Elsewhere {
            // Our map may be the stale one.
            self.refresh_tablets();
            routing = self.routing_of(&m);
        }
        if single && routing == Routing::Owned && !self.parked.is_empty() {
            // Per-row submission order: behind a parked commit on any of the same rows.
            let rows = member_rows(&m);
            if rows.iter().any(|h| self.parked_rows.contains(h)) {
                routing = Routing::Moving;
            }
        }
        match (routing, single) {
            (Routing::Owned, _) => Some(m),
            (Routing::Moving, true) => {
                self.parked_rows.extend(member_rows(&m));
                self.parked.push(m);
                self.arm_tablet_timer(ctx);
                None
            }
            (Routing::Elsewhere, true) => self.forward(m, ctx),
            (_, false) => {
                self.refuse_prepare(m, PrepareError::Moved, ctx);
                None
            }
        }
    }

    fn routing_of(&self, m: &Member) -> Routing {
        let prepare = matches!(m.kind, MemberKind::Prepare { .. });
        let mut out = Routing::Owned;
        // A share routed with an older tablet map may name rows this shard no longer owns,
        // and its reads may now belong to a shard outside the commit (nobody would validate
        // them): it runs again through the new map only then (#102). A read another
        // participant owns now is validated there, under that shard's current map.
        let participants: Option<&[ShardId]> = (prepare && m.map_version < self.tablets.version())
            .then(|| {
                self.prepared
                    .get(&m.seqno)
                    .map_or(&[][..], |s| s.participants.as_slice())
            });
        let mut look = |table: TableId, row: &[u8], read_by_others: bool| {
            if let Some((tablet, owner)) = self.tablets.route(table, row) {
                if self.moving.contains(&tablet) {
                    out = Routing::Moving;
                } else if owner != self.id
                    && out == Routing::Owned
                    && (!read_by_others || participants.is_some_and(|p| !p.contains(&owner)))
                {
                    out = Routing::Elsewhere;
                }
            }
        };
        if let Ok(batch) = BatchRef::new(m.bytes.as_slice()) {
            for mu in batch.iter().flatten() {
                look(mu.table, mu.row, false);
            }
        }
        // A PREPARE carries the whole commit's reads; each participant validates its own.
        if let Some((_, reads)) = &m.validate {
            for r in reads {
                look(r.table, &r.row, prepare);
            }
        }
        if let Some((table, row, _)) = &m.predicate {
            look(*table, row, false);
        }
        out
    }

    /// Whether `m` is a PREPARE writing rows at a commit timestamp below a default timestamp
    /// this shard already assigned. The coordinator picked it above the floor it read, but
    /// this shard may have assigned more since (to any of its tablets, a received one
    /// included): applying it would put a write at a lower timestamp after a write at a
    /// higher one, so a tablet's default timestamps would go backwards (D11, issue #105).
    /// Ties are allowed: they never reorder.
    fn below_floor(&self, m: &Member) -> bool {
        if !self.tablets_on() || !matches!(m.kind, MemberKind::Prepare { .. }) {
            return false;
        }
        let raised = self.shared.ts_raises[usize::from(self.id.0)]
            .0
            .load(Ordering::Acquire);
        m.commit_ts < self.ts_floor().max(raised)
            && BatchRef::new(m.bytes.as_slice()).is_ok_and(|b| b.iter().next().is_some())
    }

    /// A PREPARE refused without a record: for rows this shard is moving or no longer owns
    /// (`Moved`: the coordinator retries through the new tablet map), or below its floor
    /// (`BelowFloor`: the coordinator retries with a fresh timestamp).
    fn refuse_prepare(
        &mut self,
        m: Member,
        error: PrepareError,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        let MemberKind::Prepare { coordinator } = m.kind else {
            return;
        };
        if let Some(share) = self.prepared.remove(&m.seqno) {
            if share.tracked {
                self.track_share_rows(share.bytes.batch().as_bytes(), false);
            }
            self.release_room(share.reserved);
        }
        self.release_room(m.reserved);
        self.send(
            coordinator,
            ShardMsg::Prepared {
                seqno: m.seqno,
                from: self.id,
                error: Some(error),
            },
            ctx,
        );
    }

    /// Routes a single-shard member again through the current tablet map. Returns it when it
    /// is this shard's; otherwise submits it to its owner (or to two-phase commit when its
    /// rows now span shards) and returns `None`.
    pub(super) fn forward(
        &mut self,
        m: Member,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) -> Option<Member> {
        let view = self.shared.view.load_full();
        let shards = commit_shards(
            &view,
            m.bytes.as_slice(),
            m.validate.as_ref().map(|(_, r)| r.as_slice()),
            m.predicate.as_ref().map(|(t, r, _)| (*t, r.as_slice())),
        );
        let shards = match shards {
            Ok(s) => s,
            Err(e) => {
                m.reply.resolve(Err(e));
                return None;
            }
        };
        match shards.as_slice() {
            [] => Some(m),
            [s] if *s == self.id => {
                self.refresh_tablets();
                Some(m)
            }
            [s] => {
                let s = *s;
                let mut req = member_req(m);
                req.map_version = view.tablets.version();
                if let Err(pigeonhole_runtime::Error::Closed) =
                    ctx.submitter(s).submit(ShardMsg::Commit(req))
                {
                    // The message (and its reply) is dropped with the closed queue: the
                    // waiter sees `Closed`.
                }
                None
            }
            _ => {
                let req = member_req(m);
                let Reply::Commit(reply) = req.reply else {
                    req.reply.resolve(Err(Error::InvalidArgument(
                        "check_and_mutate touches one row".to_owned(),
                    )));
                    return None;
                };
                let parts = match split_by_shard(&view, &req.bytes, &shards) {
                    Ok(p) => p,
                    Err(e) => {
                        reply.notify(Err(e));
                        return None;
                    }
                };
                let creq = CoordinateReq {
                    parts,
                    durability: req.durability,
                    reply,
                    submitted_at: req.submitted_at,
                    validate: req.validate,
                    map_version: view.tablets.version(),
                    commit_ts: req.commit_ts,
                    epoch: 0,
                    attempts: req.attempts,
                };
                if shards[0] == self.id {
                    self.start_coordination(creq, ctx);
                } else {
                    let _ = ctx.submitter(shards[0]).submit(ShardMsg::Coordinate(creq));
                }
                None
            }
        }
    }

    /// Cross-shard commits refused by a participant moving a tablet: retried once the tablet
    /// map is newer than the one they were routed with.
    pub(super) fn run_retries(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        if self.retries.is_empty() {
            return;
        }
        if self.closing {
            for r in self.retries.drain(..) {
                r.reply.notify(Err(Error::Closed));
            }
            return;
        }
        let version = self.shared.view.load().tablets.version();
        let epoch = self.shared.tablet_epoch.load(Ordering::Acquire);
        let (ready, wait): (Vec<CoordinateReq>, Vec<CoordinateReq>) =
            std::mem::take(&mut self.retries)
                .into_iter()
                .partition(|r| r.map_version < version || r.epoch < epoch);
        self.retries = wait;
        for r in ready {
            self.retry(r, ctx);
        }
    }

    /// Runs a refused cross-shard commit again through the current tablet map.
    pub(super) fn retry(&mut self, r: CoordinateReq, ctx: &mut ShardContext<'_, ShardMsg>) {
        let mut builder = BatchBuilder::new();
        let mut failed = None;
        for (_, part) in &r.parts {
            for mu in part.batch().iter() {
                let pushed = mu.map_err(Error::from).and_then(|mu| {
                    builder
                        .push(
                            mu.table,
                            mu.family,
                            mu.kind,
                            mu.row,
                            mu.qualifier,
                            mu.ts,
                            mu.value,
                        )
                        .map_err(Error::from)
                });
                if let Err(e) = pushed {
                    failed = Some(e);
                }
            }
        }
        if let Some(e) = failed {
            r.reply.notify(Err(e));
            return;
        }
        let m = Member::single(CommitReq {
            bytes: builder,
            durability: r.durability,
            reply: Reply::Commit(r.reply),
            submitted_at: r.submitted_at,
            validate: r.validate,
            predicate: None,
            commit_ts: r.commit_ts,
            map_version: r.map_version,
            attempts: r.attempts,
        });
        if let Some(m) = self.forward(m, ctx) {
            self.pending.push(m);
            let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
        }
    }

    /// Fails with `Busy` every parked commit and every waiting retry that was submitted
    /// `write_stall_timeout_nanos` ago (#102): a change can wait on a long compaction or a
    /// slow flush, and a commit that keeps meeting changes must not wait for ever.
    pub(super) fn expire_tablet_waits(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        if self.parked.is_empty() && self.retries.is_empty() {
            if let Some((_, t)) = self.tablet_timer.take() {
                t.cancel();
            }
            return;
        }
        let now = ctx.now_nanos();
        let timeout = self.shared.write_stall_timeout_nanos;
        let late = |submitted_at: u64| now.saturating_sub(submitted_at) >= timeout;
        if self.parked.iter().any(|m| late(m.submitted_at)) {
            let (expired, kept): (Vec<Member>, Vec<Member>) = std::mem::take(&mut self.parked)
                .into_iter()
                .partition(|m| late(m.submitted_at));
            self.parked = kept;
            self.parked_rows = self.parked.iter().flat_map(member_rows).collect();
            for m in expired {
                trace!("shard {} parked commit times out", self.id.0);
                m.reply.resolve(Err(Error::Busy));
            }
        }
        if self.retries.iter().any(|r| late(r.submitted_at)) {
            let (expired, kept): (Vec<CoordinateReq>, Vec<CoordinateReq>) =
                std::mem::take(&mut self.retries)
                    .into_iter()
                    .partition(|r| late(r.submitted_at));
            self.retries = kept;
            for r in expired {
                trace!("shard {} retried commit times out", self.id.0);
                r.reply.notify(Err(Error::Busy));
            }
        }
        self.arm_tablet_timer(ctx);
        self.try_finish_close(ctx);
    }

    /// Arms (or re-arms) the timer that ends the oldest parked commit or retry.
    pub(super) fn arm_tablet_timer(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        let oldest = self
            .parked
            .iter()
            .map(|m| m.submitted_at)
            .chain(self.retries.iter().map(|r| r.submitted_at))
            .min();
        let Some(oldest) = oldest else {
            if let Some((_, t)) = self.tablet_timer.take() {
                t.cancel();
            }
            return;
        };
        let deadline = oldest.saturating_add(self.shared.write_stall_timeout_nanos);
        // A timer that gave up on a clock still frozen is not armed again: the change's
        // progress releases the commits instead.
        if let Some((at, t)) = &self.tablet_timer
            && ((*at <= deadline && !t.finished()) || t.frozen(ctx.now_nanos()))
        {
            return;
        }
        if let Some((_, t)) = self.tablet_timer.take() {
            t.cancel();
        }
        trace!("shard {} tablet timer armed for {deadline}", self.id.0);
        let state = TimerState::new();
        ctx.spawn(Box::new(ClockTimer::new(
            &self.shared.vfs,
            deadline,
            Arc::clone(&state),
            ctx.submitter(self.id).clone(),
            ShardMsg::Kick,
        )));
        self.tablet_timer = Some((deadline, state));
    }

    // ---- the change ----

    /// Queues a tablet change and starts it if none is running.
    pub(super) fn request_tablet_op(
        &mut self,
        kind: TabletOpKind,
        reply: Option<Notifier<Result<()>>>,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        if !self.tablets_on() {
            if let Some(r) = reply {
                r.notify(Err(Error::Unsupported("tablet changes are off")));
            }
            return;
        }
        if self.closing {
            if let Some(r) = reply {
                r.notify(Err(Error::Closed));
            }
            return;
        }
        self.op_queue.push_back((kind, reply));
        self.start_next_op(ctx);
    }

    fn start_next_op(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        while self.op.is_none() {
            let Some((kind, reply)) = self.op_queue.pop_front() else {
                return;
            };
            self.refresh_tablets();
            match self.validate_op(&kind) {
                Err(e) => {
                    trace!("shard {} tablet change {kind:?} refused: {e}", self.id.0);
                    if let Some(r) = reply {
                        r.notify(Err(e));
                    }
                }
                Ok((entries, reserved)) => {
                    trace!("shard {} tablet change {kind:?} starts", self.id.0);
                    self.moving = kind.tablets().into_iter().collect();
                    self.op = Some(ActiveOp {
                        kind,
                        reply,
                        entries,
                        committing: false,
                        reserved,
                    });
                    if self.freeze(false).is_err() {
                        self.poisoned = true;
                    }
                    self.spawn_flush(ctx);
                    self.progress_op(ctx);
                }
            }
        }
    }

    /// Checks a change against the current tablet map; returns the tablets it touches and the
    /// slots it reserved on the shards receiving tablets.
    fn validate_op(&self, kind: &TabletOpKind) -> Result<(Vec<TabletEntry>, Reserved)> {
        if self.poisoned || self.shared.pager_poisoned.load(Ordering::Acquire) {
            return Err(poisoned_error());
        }
        let view = self.shared.view.load();
        let shards = self.shared.shards;
        let entry = |id: TabletId| -> Result<TabletEntry> {
            view.tablets
                .entry(id)
                .filter(|t| t.shard == self.id)
                .cloned()
                .ok_or_else(|| {
                    Error::InvalidArgument(format!("tablet {} is not on shard {}", id.0, self.id.0))
                })
        };
        match kind {
            TabletOpKind::Split {
                tablet,
                keys,
                owners,
            } => {
                let t = entry(*tablet)?;
                if keys.is_empty()
                    || owners.len() != keys.len() + 1
                    || owners.iter().any(|s| usize::from(s.0) >= shards)
                {
                    return Err(Error::InvalidArgument("malformed split".to_owned()));
                }
                // Above the tablet's start, then above the previous key.
                let mut prev: &[u8] = &t.start;
                for k in keys {
                    if k.as_slice() <= prev {
                        return Err(Error::InvalidArgument(
                            "split keys must increase strictly inside the tablet".to_owned(),
                        ));
                    }
                    prev = k;
                }
                if t.end.as_ref().is_some_and(|e| prev >= e.as_slice()) {
                    return Err(Error::InvalidArgument(
                        "split keys must increase strictly inside the tablet".to_owned(),
                    ));
                }
                // D28: refuse a split whose view would not fit the shared-memory buffer.
                let families = view.catalog.family_ids_of(t.table).len();
                let key_bytes: usize = keys.iter().map(Vec::len).sum();
                let grow = keys.len() * (32 + 24 * families)
                    + 2 * key_bytes
                    + t.start.len()
                    + t.end.as_ref().map_or(0, Vec::len)
                    + 16;
                if view.to_record().encoded_len() + grow > self.shared.view_capacity {
                    return Err(Error::Unsupported(
                        "the tablet map would not fit the shared-memory view buffer (D28)",
                    ));
                }
                // Each shard keeps its slots within its arena's chunks.
                let mut extra: Vec<(ShardId, usize)> = Vec::new();
                for o in owners {
                    if extra.iter().any(|(s, _)| s == o) {
                        continue;
                    }
                    let children = owners.iter().filter(|x| *x == o).count();
                    // The parent's slots leave this shard as its children arrive.
                    let freed = if *o == self.id { families } else { 0 };
                    extra.push((*o, (families * children).saturating_sub(freed)));
                }
                let reserved = self.reserve_slots(&view, &extra)?;
                Ok((vec![t], reserved))
            }
            TabletOpKind::Merge { left, right } => {
                let l = entry(*left)?;
                let r = entry(*right)?;
                if l.table != r.table || l.end.as_deref() != Some(r.start.as_slice()) {
                    return Err(Error::InvalidArgument(
                        "only adjacent tablets of one table merge".to_owned(),
                    ));
                }
                Ok((vec![l, r], Vec::new()))
            }
            TabletOpKind::Move { tablet, to } => {
                let t = entry(*tablet)?;
                if *to == self.id || usize::from(to.0) >= shards {
                    return Err(Error::InvalidArgument(format!(
                        "cannot move tablet {} to shard {}",
                        tablet.0, to.0
                    )));
                }
                let families = view.catalog.family_ids_of(t.table).len();
                let reserved = self.reserve_slots(&view, &[(*to, families)])?;
                Ok((vec![t], reserved))
            }
        }
    }

    /// The most `(tablet, family)` slots a shard should hold: a quarter of its arena's
    /// chunks. Every slot written to takes a memtable chunk (and usually two before it
    /// freezes), and frozen memtables keep theirs until flushed and released.
    fn max_slots(&self) -> usize {
        crate::engine::max_slots(self.arena.region().len(), self.chunk_size)
    }

    /// Slots `shard` holds in `view`.
    fn slots_in(view: &View, shard: ShardId) -> usize {
        view.tablets
            .iter()
            .filter(|t| t.shard == shard)
            .map(|t| view.catalog.family_ids_of(t.table).len())
            .sum()
    }

    /// Whether `shard` can take `extra` more slots under [`ShardState::max_slots`], counting
    /// the slots other changes reserved there.
    fn slots_fit(&self, view: &View, shard: ShardId, extra: usize) -> bool {
        let reserved = self.shared.loads[usize::from(shard.0)]
            .reserved
            .load(Ordering::Acquire);
        Self::slots_in(view, shard) + reserved + extra <= self.max_slots()
    }

    /// Reserves `extra` slots on each shard (all or none), so a change running on another
    /// shard sees them before this one commits (#103).
    fn reserve_slots(&self, view: &View, extra: &[(ShardId, usize)]) -> Result<Reserved> {
        let mut out: Reserved = Vec::new();
        for &(shard, n) in extra {
            if n == 0 {
                continue;
            }
            let slot = &self.shared.loads[usize::from(shard.0)].reserved;
            let before = slot.fetch_add(n, Ordering::AcqRel);
            out.push((shard, n));
            if Self::slots_in(view, shard) + before + n > self.max_slots() {
                self.release_slots(&out);
                return Err(Error::Unsupported(
                    "a shard would hold more tablets than its memtable arena serves",
                ));
            }
        }
        Ok(out)
    }

    fn release_slots(&self, reserved: &[(ShardId, usize)]) {
        for &(shard, n) in reserved {
            self.shared.loads[usize::from(shard.0)]
                .reserved
                .fetch_sub(n, Ordering::AcqRel);
        }
    }

    /// Moves the running change on: commits it once its tablets are drained.
    pub(super) fn progress_op(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        let Some(op) = &self.op else {
            return;
        };
        if op.committing {
            return;
        }
        if self.poisoned || self.shared.pager_poisoned.load(Ordering::Acquire) {
            self.abort_op(poisoned_error(), ctx);
            return;
        }
        let mut busy = false;
        let mut refreeze = false;
        for (key, slot) in &self.memtables {
            if !self.moving.contains(&key.0) {
                continue;
            }
            if !slot.active.table.is_empty() {
                busy = true;
                refreeze = true;
            }
            if !slot.frozen.is_empty() {
                busy = true;
            }
        }
        if self
            .compaction
            .is_some_and(|(t, _)| self.moving.contains(&t))
        {
            busy = true;
        }
        if !busy && self.shares_touch_moving() {
            busy = true;
        }
        trace!(
            "shard {} tablet change waits: busy={busy} refreeze={refreeze} deferred={} flush_running={}",
            self.id.0, self.freeze_deferred, self.flush_running
        );
        if refreeze {
            if self.freeze(false).is_err() {
                self.poisoned = true;
            }
            self.spawn_flush(ctx);
        }
        if busy {
            return;
        }
        self.commit_op(ctx);
    }

    /// Whether a prepared, undecided share writes or reads a tablet being changed.
    fn shares_touch_moving(&self) -> bool {
        self.prepared.values().any(|share| {
            BatchRef::new(share.bytes.batch().as_bytes()).is_ok_and(|b| {
                b.iter().flatten().any(|mu| {
                    self.tablets
                        .route(mu.table, mu.row)
                        .is_some_and(|(t, _)| self.moving.contains(&t))
                })
            })
        })
    }

    fn commit_op(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        let Some(op) = self.op.as_mut() else {
            return;
        };
        op.committing = true;
        let kind = op.kind.clone();
        let entries = op.entries.clone();
        // D11: default timestamps never go backwards on a tablet. A shard receiving one
        // starts above every default timestamp this shard assigned (an upper bound on the
        // tablet's own floor).
        for s in kind.targets(self.id) {
            let i = usize::from(s.0);
            let mut raisers = self.shared.ts_raisers[i]
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let me = if self.id.0 < 64 { 1u64 << self.id.0 } else { 0 };
            if self.ts_floor > raisers.0 {
                *raisers = (self.ts_floor, me);
            } else if self.ts_floor == raisers.0 {
                raisers.1 |= me;
            }
            self.shared.ts_raises[i]
                .0
                .fetch_max(self.ts_floor, Ordering::AcqRel);
        }
        trace!("shard {} tablet change {kind:?} commits", self.id.0);
        let me = self.id;
        let change: TabletChange =
            Box::new(move |catalog| tablet_change(catalog, &kind, &entries, me));
        let submitter = ctx.submitter(self.id).clone();
        let req = ManifestReq {
            kind: ReqKind::Tablets(change),
            readers: Vec::new(),
            flushed_roots: Vec::new(),
            compaction: None,
            rewrite_snapshot: false,
            dropped_ok: Vec::new(),
            reply: Box::new(move |result| {
                let _ = submitter.submit(ShardMsg::TabletOpDone { result });
            }),
        };
        manifest::submit(&self.shared, self.id, req);
    }

    /// The change's manifest commit finished.
    pub(super) fn on_tablet_op_done(
        &mut self,
        result: Result<ManifestVersion>,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        let Some(op) = self.op.take() else {
            return;
        };
        self.finished_op();
        let moving: Vec<TabletId> = std::mem::take(&mut self.moving).into_iter().collect();
        // The view now holds the slots (or the change failed): the reservation goes.
        self.refresh_tablets();
        self.release_slots(&op.reserved);
        match result {
            Ok(_) => {
                trace!("shard {} tablet change {:?} done", self.id.0, op.kind);
                self.refresh_tablets();
                if self.drop_tablets(&moving).is_err() {
                    self.poisoned = true;
                }
                for t in &moving {
                    self.loads.remove(t);
                }
                if !op.kind.targets(self.id).is_empty() {
                    self.lost_version = self.tablets.version();
                }
                let metrics = &self.shared.metrics[usize::from(self.id.0)];
                match op.kind {
                    TabletOpKind::Split { .. } => &metrics.splits,
                    TabletOpKind::Merge { .. } => &metrics.merges,
                    TabletOpKind::Move { .. } => &metrics.moves,
                }
                .fetch_add(1, Ordering::Relaxed);
                self.release_parked(ctx);
                if let Some(r) = op.reply {
                    r.notify(Ok(()));
                }
            }
            Err(e) => {
                trace!(
                    "shard {} tablet change {:?} failed: {e}",
                    self.id.0, op.kind
                );
                self.release_parked(ctx);
                if let Some(r) = op.reply {
                    r.notify(Err(e));
                }
            }
        }
        self.start_next_op(ctx);
        self.try_finish_close(ctx);
    }

    /// Gives up the running change before its commit (a failed flush, a poisoned pager).
    pub(super) fn abort_op(&mut self, e: Error, ctx: &mut ShardContext<'_, ShardMsg>) {
        if self.op.as_ref().is_none_or(|op| op.committing) {
            return;
        }
        let op = self.op.take().expect("checked");
        self.finished_op();
        self.release_slots(&op.reserved);
        self.moving.clear();
        self.release_parked(ctx);
        if let Some(r) = op.reply {
            r.notify(Err(e));
        }
        self.start_next_op(ctx);
    }

    /// A change finished or was given up: commits refused meanwhile may run again.
    fn finished_op(&self) {
        self.shared.tablet_epoch.fetch_add(1, Ordering::AcqRel);
        self.shared.broadcast(|| ShardMsg::Maintain);
    }

    /// Routes every parked commit again, ahead of whatever is pending.
    fn release_parked(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        self.parked_rows.clear();
        let parked = std::mem::take(&mut self.parked);
        let mut mine = Vec::with_capacity(parked.len());
        for m in parked {
            if let Some(m) = self.forward(m, ctx) {
                mine.push(m);
            }
        }
        mine.append(&mut self.pending);
        self.pending = mine;
        if !self.pending.is_empty() {
            let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
        }
    }

    // ---- load tracking and the balancer ----

    /// Counts a row written to `tablet` and samples it (reservoir sampling).
    pub(super) fn note_write(&mut self, tablet: TabletId, row: &[u8]) {
        self.window_writes += 1;
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let r = self.rng;
        let l = self.loads.entry(tablet).or_default();
        l.writes += 1;
        l.seen += 1;
        if l.samples.len() < SAMPLES {
            l.samples.push(row.to_vec());
        } else {
            let j = (r % l.seen) as usize;
            if j < SAMPLES {
                let s = &mut l.samples[j];
                s.clear();
                s.extend_from_slice(row);
            }
        }
    }

    /// Memtable bytes of this shard, and per tablet (empty active memtables count 0).
    fn mem_bytes(&self) -> (u64, HashMap<TabletId, u64>) {
        let mut total = 0;
        let mut per = HashMap::new();
        for (key, slot) in &self.memtables {
            // An empty active memtable holds no writes, whatever it has allocated: its tablet
            // can still be cold (a slot now keeps its memtable after a flush).
            let active = if slot.active.table.is_empty() {
                0
            } else {
                slot.active.table.allocated_bytes() as u64
            };
            let b = active
                + slot
                    .frozen
                    .iter()
                    .map(|m| m.table.allocated_bytes() as u64)
                    .sum::<u64>();
            total += b;
            *per.entry(key.0).or_insert(0) += b;
        }
        (total, per)
    }

    /// Ends a balancer interval when it is due (or at once when `force`): publishes this
    /// shard's load and starts a split, move or merge when one is warranted.
    pub(super) fn maybe_balance(
        &mut self,
        force: bool,
        reply: Option<Notifier<Result<()>>>,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        let cfg = self.shared.balance;
        if !cfg.enabled {
            if let Some(r) = reply {
                r.notify(Err(Error::Unsupported("tablet changes are off")));
            }
            return;
        }
        if !force && cfg.interval_nanos == 0 {
            return;
        }
        let now = ctx.now_nanos();
        let base = cfg.interval_nanos.max(1);
        let epoch = self.shared.tablet_epoch.load(Ordering::Acquire);
        // A write or a tablet change (here or on any shard) ends an idle backoff: the next
        // pass is due one base interval from now.
        let active = self.window_writes > 0
            || self.op.is_some()
            || !self.op_queue.is_empty()
            || epoch != self.balance_epoch;
        if active && self.balance_interval > base {
            self.balance_interval = base;
            let due = now.saturating_add(base);
            if due < self.next_balance {
                self.next_balance = due;
                self.cancel_balance_timer();
                self.arm_balance_timer(ctx);
            }
        }
        if !force && now < self.next_balance {
            return;
        }
        self.next_balance = now.saturating_add(base);
        let wrote = self.window_writes > 0;
        self.balance_pass += 1;
        // Moving averages of the write load: a shard's (published for the other shards) and
        // each tablet's (#103). One interval's swing, or a flush, never decides a move.
        self.load_ewma =
            LOAD_ALPHA * self.window_writes as f64 + (1.0 - LOAD_ALPHA) * self.load_ewma;
        for l in self.loads.values_mut() {
            l.ewma = LOAD_ALPHA * l.writes as f64 + (1.0 - LOAD_ALPHA) * l.ewma;
        }
        self.note_arrivals();
        let (_, mem_per) = self.mem_bytes();
        let slot = &self.shared.loads[usize::from(self.id.0)];
        slot.writes
            .store(self.load_ewma.round() as u64, Ordering::Relaxed);
        slot.at.store(now.max(1), Ordering::Release);
        let idle = self.op.is_none()
            && self.op_queue.is_empty()
            && !self.closing
            && !self.replaying
            && !self.poisoned
            && !self.shared.closing.load(Ordering::Acquire)
            && !self.shared.pager_poisoned.load(Ordering::Acquire)
            && self.shared.full_compactions.load(Ordering::Acquire) == 0;
        // A change waits for a running compaction on its tablets: the balancer leaves a
        // compacting tablet alone and decides again next interval (#102).
        let (decision, cleanups) = if idle {
            let (kind, cleanups) = self.decide(now, &mem_per);
            let kind = kind.filter(|kind| {
                self.compaction
                    .is_none_or(|(t, _)| !kind.tablets().contains(&t))
            });
            (kind, cleanups)
        } else {
            (None, Vec::new())
        };
        self.window_writes = 0;
        for l in self.loads.values_mut() {
            l.prev_writes = l.writes;
            l.writes = 0;
            l.seen = 0;
            l.samples.clear();
        }
        let decision_none = decision.is_none();
        match decision {
            Some(kind) => {
                trace!("shard {} balancer: {kind:?}", self.id.0);
                // Anything but a size split (a move, or a split over shards) starts the
                // shard's dwell: the loads settle before write skew acts again.
                let size_split = matches!(&kind, TabletOpKind::Split { owners, .. }
                    if owners.iter().all(|o| *o == self.id));
                if !size_split {
                    self.skew_pass = Some(self.balance_pass);
                }
                self.request_tablet_op(kind, reply, ctx);
            }
            None => {
                if let Some(r) = reply {
                    r.notify(Ok(()));
                }
            }
        }
        // Idle backoff: a pass that found nothing to do, after no writes and no tablet change,
        // doubles the interval (up to `BALANCE_IDLE_CAP_NANOS`, never below the base), so an
        // idle shard wakes rarely; anything else returns to the base interval.
        let quiet = idle
            && !wrote
            && decision_none
            && cleanups.is_empty()
            && epoch == self.balance_epoch
            && self.op.is_none();
        let current = self.balance_interval.max(base);
        self.balance_interval = if quiet {
            current
                .saturating_mul(2)
                .min(BALANCE_IDLE_CAP_NANOS.max(base))
        } else {
            base
        };
        self.balance_epoch = self.shared.tablet_epoch.load(Ordering::Acquire);
        self.next_balance = now.saturating_add(self.balance_interval);
        if !cleanups.is_empty() {
            for c in cleanups {
                if !self.cleanups.contains(&c) {
                    self.cleanups.push(c);
                }
            }
            self.maintain(ctx);
        }
        self.arm_balance_timer(ctx);
    }

    /// Records the balancer pass at which each tablet this shard owns first appeared here
    /// (the tablets it held at its first pass count as settled).
    fn note_arrivals(&mut self) {
        let view = self.shared.view.load();
        let first = self.balance_pass <= 1;
        let pass = if first { 0 } else { self.balance_pass };
        let owned: HashSet<TabletId> = view
            .tablets
            .iter()
            .filter(|t| t.shard == self.id)
            .map(|t| t.id)
            .collect();
        self.arrived.retain(|t, _| owned.contains(t));
        for t in owned {
            self.arrived.entry(t).or_insert(pass);
        }
    }

    /// Whether write skew may move or split `t` this pass (it has dwelt here long enough).
    fn settled(&self, t: TabletId) -> bool {
        self.arrived
            .get(&t)
            .is_none_or(|p| *p == 0 || *p + DWELL_PASSES <= self.balance_pass)
    }

    /// Wakes the shard for its next balancer pass even when no message arrives (#103), so
    /// an idle shard still merges cold tablets. Like the other clock timers (D126), one that
    /// gave up on a frozen clock is not armed again while the clock stays frozen.
    fn arm_balance_timer(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        let interval = self.shared.balance.interval_nanos;
        if interval == 0 || interval == u64::MAX || self.closing || self.next_balance == u64::MAX {
            return;
        }
        let now = ctx.now_nanos();
        if self
            .balance_timer
            .as_ref()
            .is_some_and(|t| !t.finished() || t.frozen(now))
        {
            return;
        }
        let state = TimerState::new();
        ctx.spawn(Box::new(ClockTimer::new(
            &self.shared.vfs,
            self.next_balance,
            Arc::clone(&state),
            ctx.submitter(self.id).clone(),
            ShardMsg::Kick,
        )));
        self.balance_timer = Some(state);
    }

    /// Cancels the balancer's wakeup (close).
    pub(super) fn cancel_balance_timer(&mut self) {
        if let Some(t) = self.balance_timer.take() {
            t.cancel();
        }
    }

    /// Starts a rewrite of the first queued cleanup slot that still holds an SST sticking out
    /// of its tablet (#95): the rewrite drops the sibling's rows, so a later balancer pass can
    /// merge. Slots that no longer need one, or whose tablet left this shard or is being
    /// changed, are dropped (the balancer queues them again while the merge is refused).
    pub(super) fn start_cleanup(
        &mut self,
        view: &Arc<View>,
        busy: &[SstId],
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        let last = self.picker.options().max_levels.max(2) - 1;
        let now = ctx.now_nanos();
        // Slots waiting out a failed compaction's backoff stay queued (issue #141).
        let mut later = Vec::new();
        while !self.cleanups.is_empty() {
            let key = self.cleanups.remove(0);
            if let Some(&(_, until)) = self.slot_backoff.get(&key)
                && until > now
            {
                later.push(key);
                continue;
            }
            let Some(tablet) = view.tablets.entry(key.0).filter(|t| t.shard == self.id) else {
                continue;
            };
            if self.moving.contains(&key.0)
                || view
                    .catalog
                    .family(key.1)
                    .is_none_or(|m| m.merge == MergeKind::Unknown)
            {
                continue;
            }
            let Some(fam) = view.ssts.family(key.0, key.1) else {
                continue;
            };
            let levels = fam.levels_meta();
            if !sticks_out_of(tablet, &levels) {
                continue;
            }
            let Some(task) = compact::plan_full(tablet, key.1, &levels, last, busy, true) else {
                // Its inputs are busy: try again after the running work.
                self.cleanups.insert(0, key);
                break;
            };
            trace!("shard {} cleanup rewrite of {key:?}", self.id.0);
            if let Err(e) = self.start_compaction(view, key, task, ctx) {
                trace!("shard {} compaction start failed: {e}", self.id.0);
                self.back_off_compaction(key, ctx);
            }
            break;
        }
        self.cleanups.extend(later);
    }

    /// The balancer's choice for this interval, if any: a size split, a write-skew (or
    /// memtable-skew) move or split, or a merge of two small, cold neighbours. With no
    /// choice, also the slots to rewrite so that a refused merge can go through later.
    fn decide(
        &self,
        now: u64,
        mem_per: &HashMap<TabletId, u64>,
    ) -> (Option<TabletOpKind>, Vec<(TabletId, FamilyId)>) {
        let mut cleanups = Vec::new();
        (self.choose(now, mem_per, &mut cleanups), cleanups)
    }

    fn choose(
        &self,
        now: u64,
        mem_per: &HashMap<TabletId, u64>,
        cleanups: &mut Vec<(TabletId, FamilyId)>,
    ) -> Option<TabletOpKind> {
        let cfg = self.shared.balance;
        let view = self.shared.view.load_full();
        let owned: Vec<TabletEntry> = view
            .tablets
            .iter()
            .filter(|t| t.shard == self.id)
            .cloned()
            .collect();
        if owned.is_empty() {
            return None;
        }
        let refs = view.catalog.sst_refs();
        // Every `(tablet, family)` slot written to takes memtable chunks: a shard keeps its
        // slots within `max_slots`, so splits never starve writers of memtables.
        let room_for = |t: &TabletEntry| {
            self.slots_fit(&view, self.id, view.catalog.family_ids_of(t.table).len())
        };
        // 1. Size: a tablet holding `tablet_split_bytes` of SSTs splits in two (not while it
        //    still shares SSTs with a sibling: their bytes would count twice).
        for t in &owned {
            let (bytes, shared) = live_bytes(&view, t, &refs);
            if bytes >= cfg.split_bytes && !shared && !room_for(t) {
                trace!(
                    "shard {} balancer: tablet {} is due a size split but the shard holds \
                     {} of its {} slots",
                    self.id.0,
                    t.id.0,
                    Self::slots_in(&view, self.id),
                    self.max_slots()
                );
            }
            if bytes >= cfg.split_bytes
                && !shared
                && room_for(t)
                && let Some(key) = self.size_split_key(&view, t)
            {
                return Some(TabletOpKind::Split {
                    tablet: t.id,
                    keys: vec![key],
                    owners: vec![self.id, self.id],
                });
            }
        }
        // 2. Skew across shards, on the moving averages of write load (#103; memtable bytes
        //    swing with every flush and no longer count). Not during the shard's dwell after
        //    its last move, and only for tablets that dwelt here long enough.
        let n = self.shared.shards;
        let dwelling = self
            .skew_pass
            .is_some_and(|p| p + DWELL_PASSES > self.balance_pass);
        if n > 1 && !dwelling {
            let stale = cfg.interval_nanos.saturating_mul(3).max(1);
            let mut writes = vec![0u64; n];
            for (i, slot) in self.shared.loads.iter().enumerate() {
                let at = slot.at.load(Ordering::Acquire);
                if at != 0 && now.saturating_sub(at) <= stale {
                    writes[i] = slot.writes.load(Ordering::Relaxed);
                }
            }
            let settled: Vec<TabletEntry> = owned
                .iter()
                .filter(|t| self.settled(t.id))
                .cloned()
                .collect();
            let tablet_writes: HashMap<TabletId, u64> = settled
                .iter()
                .map(|t| {
                    let w = self.loads.get(&t.id).map_or(0.0, |l| l.ewma);
                    (t.id, w.round() as u64)
                })
                .collect();
            let splits_ok = |op: &TabletOpKind| match op {
                TabletOpKind::Split { tablet, .. } => {
                    owned.iter().find(|t| t.id == *tablet).is_some_and(room_for)
                }
                _ => true,
            };
            if let Some(op) = self.skew_op(&view, &settled, &writes, &tablet_writes, cfg.min_writes)
                && splits_ok(&op)
            {
                return Some(op);
            }
        }
        // 3. Merge two adjacent cold tablets of one table that are small together.
        let cold = |id: TabletId| {
            self.loads
                .get(&id)
                .is_none_or(|l| l.writes == 0 && l.prev_writes == 0)
                && mem_per.get(&id).copied().unwrap_or(0) == 0
        };
        for pair in owned.windows(2) {
            let (l, r) = (&pair[0], &pair[1]);
            if l.table != r.table
                || l.end.as_deref() != Some(r.start.as_slice())
                || !cold(l.id)
                || !cold(r.id)
            {
                continue;
            }
            let (lb, _) = live_bytes(&view, l, &refs);
            let (rb, _) = live_bytes(&view, r, &refs);
            if lb + rb >= cfg.split_bytes / 4 {
                continue;
            }
            if merged_ssts(&view.catalog, l, r).is_ok() {
                return Some(TabletOpKind::Merge {
                    left: l.id,
                    right: r.id,
                });
            }
            // Refused while a side still holds an SST inherited from the split, with its
            // sibling's rows: nothing else rewrites a cold tablet's SSTs, so ask for it.
            for t in [l, r] {
                for f in view.catalog.family_ids_of(t.table) {
                    if view
                        .ssts
                        .family(t.id, f)
                        .is_some_and(|fam| sticks_out_of(t, &fam.levels_meta()))
                    {
                        cleanups.push((t.id, f));
                    }
                }
            }
        }
        // 4. Consolidate (#103): a cold tablet whose left neighbour lives on another shard
        //    moves there when the two are small together, so that shard can merge them.
        //    Only right to left, so two shards never swap tablets, and only tablets that
        //    dwelt here (a split's child is cold until its first writes arrive).
        for t in &owned {
            if !cold(t.id) || t.start.is_empty() || !self.settled(t.id) {
                continue;
            }
            let Some(left) = view
                .tablets
                .tablets_of(t.table)
                .iter()
                .find(|l| l.end.as_deref() == Some(t.start.as_slice()))
            else {
                continue;
            };
            if left.shard == self.id {
                continue;
            }
            let (lb, _) = live_bytes(&view, left, &refs);
            let (tb, _) = live_bytes(&view, t, &refs);
            let families = view.catalog.family_ids_of(t.table).len();
            if lb + tb < cfg.split_bytes / 4 && self.slots_fit(&view, left.shard, families) {
                return Some(TabletOpKind::Move {
                    tablet: t.id,
                    to: left.shard,
                });
            }
        }
        None
    }

    /// A move or split that evens out `loads` (per shard), given this shard's tablets'
    /// shares of it (`per_tablet`), when this shard is skewed past the threshold.
    fn skew_op(
        &self,
        view: &View,
        owned: &[TabletEntry],
        loads: &[u64],
        per_tablet: &HashMap<TabletId, u64>,
        min: u64,
    ) -> Option<TabletOpKind> {
        let cfg = self.shared.balance;
        let me = usize::from(self.id.0);
        let my = loads[me];
        let total: u64 = loads.iter().sum();
        let mean = total as f64 / loads.len() as f64;
        if my < min.max(1) || (my as f64) <= cfg.skew * mean {
            return None;
        }
        let (cold, low) = loads
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != me)
            .min_by_key(|(i, l)| (**l, *i))
            .map(|(i, l)| (i, *l))?;
        let gap = my - low;
        let mut tablets: Vec<(TabletId, u64)> = owned
            .iter()
            .map(|t| (t.id, per_tablet.get(&t.id).copied().unwrap_or(0)))
            .filter(|(_, w)| *w > 0)
            .collect();
        tablets.sort_by_key(|(id, w)| (std::cmp::Reverse(*w), *id));
        // A tablet carrying less than the gap moves: the one closest to half of it.
        let to = ShardId(cold as u16);
        let fits = |id: TabletId| {
            owned.iter().find(|t| t.id == id).is_some_and(|t| {
                self.slots_fit(view, to, view.catalog.family_ids_of(t.table).len())
            })
        };
        if let Some((t, _)) = tablets
            .iter()
            .filter(|(id, w)| *w < gap && *w >= gap / 8 && fits(*id))
            .min_by_key(|(id, w)| ((gap / 2).abs_diff(*w), *id))
        {
            return Some(TabletOpKind::Move { tablet: *t, to });
        }
        // One tablet dominates: split it over this shard and the shards below the mean.
        let (hot, _) = *tablets.first()?;
        let entry = owned.iter().find(|t| t.id == hot)?;
        let mut targets: Vec<(u64, usize)> = loads
            .iter()
            .enumerate()
            .filter(|(i, l)| *i != me && (**l as f64) < mean)
            .map(|(i, l)| (*l, i))
            .collect();
        targets.sort_unstable();
        let mut owners = vec![self.id];
        owners.extend(targets.iter().map(|(_, i)| ShardId(*i as u16)));
        let keys = self.skew_split_keys(entry, owners.len());
        if keys.is_empty() {
            return None;
        }
        owners.truncate(keys.len() + 1);
        Some(TabletOpKind::Split {
            tablet: hot,
            keys,
            owners,
        })
    }

    /// Up to `parts - 1` split keys from the rows written to `t` this interval (quantiles of
    /// the sample), strictly inside its range.
    fn skew_split_keys(&self, t: &TabletEntry, parts: usize) -> Vec<Vec<u8>> {
        let Some(load) = self.loads.get(&t.id) else {
            return Vec::new();
        };
        let mut rows: Vec<&[u8]> = load
            .samples
            .iter()
            .map(Vec::as_slice)
            .filter(|r| inside(t, r))
            .collect();
        rows.sort_unstable();
        rows.dedup();
        quantiles(&rows, parts)
    }

    /// A split key near the middle of `t`'s SSTs (their boundary rows) and recent writes.
    fn size_split_key(&self, view: &View, t: &TabletEntry) -> Option<Vec<u8>> {
        let mut rows: Vec<Vec<u8>> = Vec::new();
        for f in view.catalog.family_ids_of(t.table) {
            if let Some(fam) = view.ssts.family(t.id, f) {
                for sst in fam.iter() {
                    for key in [&sst.meta.smallest_key, &sst.meta.largest_key] {
                        if let Some(r) = row_of_key(key) {
                            rows.push(r);
                        }
                    }
                }
            }
        }
        if let Some(l) = self.loads.get(&t.id) {
            rows.extend(l.samples.iter().cloned());
        }
        let mut rows: Vec<&[u8]> = rows
            .iter()
            .map(Vec::as_slice)
            .filter(|r| inside(t, r))
            .collect();
        rows.sort_unstable();
        rows.dedup();
        quantiles(&rows, 2).pop()
    }
}

/// `parts - 1` evenly spaced rows of `rows` (sorted, distinct).
fn quantiles(rows: &[&[u8]], parts: usize) -> Vec<Vec<u8>> {
    let parts = parts.min(rows.len());
    let mut out: Vec<Vec<u8>> = Vec::new();
    for i in 1..parts {
        let r = rows[i * rows.len() / parts];
        if out.last().is_none_or(|l| l.as_slice() < r) {
            out.push(r.to_vec());
        }
    }
    out
}

/// Whether `row` lies strictly inside `t` (a split there leaves both sides non-empty).
fn inside(t: &TabletEntry, row: &[u8]) -> bool {
    row > t.start.as_slice() && t.end.as_ref().is_none_or(|e| row < e.as_slice())
}

/// Whether any SST of a slot of `t` holds rows outside `t`.
fn sticks_out_of(t: &TabletEntry, levels: &pigeonhole_compaction::Levels) -> bool {
    compact::tablet_range(t).is_ok_and(|range| {
        levels
            .levels
            .iter()
            .flatten()
            .any(|m| compact::sticks_out(m, &range))
    })
}

/// The unescaped row of an internal key.
fn row_of_key(key: &[u8]) -> Option<Vec<u8>> {
    let n = row_prefix_len(key).ok()?;
    let mut out = Vec::new();
    Escaped::new(key.get(..n.checked_sub(2)?)?).unescape_into(&mut out);
    Some(out)
}

/// Bytes of SSTs `t` references, and whether any of them is shared with another tablet.
fn live_bytes(view: &View, t: &TabletEntry, refs: &HashMap<SstId, usize>) -> (u64, bool) {
    let mut bytes = 0;
    let mut shared = false;
    for f in view.catalog.family_ids_of(t.table) {
        if let Some(fam) = view.ssts.family(t.id, f) {
            for sst in fam.iter() {
                bytes += sst.meta.len;
                shared |= refs.get(&sst.meta.id).copied().unwrap_or(0) > 1;
            }
        }
    }
    (bytes, shared)
}

/// The rows (hashed with their table) a member writes or reads.
fn member_rows(m: &Member) -> Vec<u64> {
    let mut out = Vec::new();
    if let Ok(batch) = BatchRef::new(m.bytes.as_slice()) {
        for mu in batch.iter().flatten() {
            out.push(hash_row(mu.table, mu.row));
        }
    }
    if let Some((_, reads)) = &m.validate {
        for r in reads {
            out.push(hash_row(r.table, &r.row));
        }
    }
    if let Some((table, row, _)) = &m.predicate {
        out.push(hash_row(*table, row));
    }
    out
}

/// A single-shard member back as the request it came from.
fn member_req(m: Member) -> CommitReq {
    let bytes = match m.bytes {
        Bytes::Own(b) => b,
        Bytes::Shared(b) => (*b).clone(),
        Bytes::Streams(_) => BatchBuilder::new(),
    };
    CommitReq {
        bytes,
        durability: m.durability,
        reply: m.reply,
        submitted_at: m.submitted_at,
        validate: m.validate,
        predicate: m.predicate,
        commit_ts: (m.commit_ts != 0).then_some(Preset {
            ts: m.commit_ts,
            own: m.preset_own,
        }),
        map_version: m.map_version,
        attempts: m.attempts,
    }
}

/// The shards a commit's rows (written and read, and its predicate row) route to through
/// `view`, in first-appearance order.
pub(crate) fn commit_shards(
    view: &View,
    bytes: &[u8],
    reads: Option<&[crate::write::ReadKey]>,
    predicate: Option<(TableId, &[u8])>,
) -> Result<Vec<ShardId>> {
    let mut out: Vec<ShardId> = Vec::new();
    let mut add = |s: ShardId| {
        if !out.contains(&s) {
            out.push(s);
        }
    };
    let mut wrote = false;
    for mu in BatchRef::new(bytes)?.iter() {
        let mu = mu?;
        if let Some((_, s)) = view.tablets.route(mu.table, mu.row) {
            wrote = true;
            add(s);
        }
    }
    if !wrote
        && let Some((table, row)) = predicate
        && let Some((_, s)) = view.tablets.route(table, row)
    {
        add(s);
    }
    for r in reads.unwrap_or_default() {
        if let Some((_, s)) = view.tablets.route(r.table, &r.row) {
            add(s);
        }
    }
    Ok(out)
}

/// Splits a batch into one builder per shard, in `shards` order.
pub(crate) fn split_by_shard(
    view: &View,
    builder: &BatchBuilder,
    shards: &[ShardId],
) -> Result<Vec<(ShardId, Arc<BatchBuilder>)>> {
    let mut parts: Vec<(ShardId, BatchBuilder)> =
        shards.iter().map(|s| (*s, BatchBuilder::new())).collect();
    for m in builder.batch().iter() {
        let m = m?;
        let Some((_, shard)) = view.tablets.route(m.table, m.row) else {
            return Err(Error::TableNotFound(format!("table {}", m.table.0)));
        };
        let Some(part) = parts.iter_mut().find(|(s, _)| *s == shard) else {
            return Err(Error::Corruption(
                "a row routed to an unlisted shard".to_owned(),
            ));
        };
        part.1
            .push(m.table, m.family, m.kind, m.row, m.qualifier, m.ts, m.value)?;
    }
    Ok(parts.into_iter().map(|(s, b)| (s, Arc::new(b))).collect())
}

/// The row-prefix bounds of `[start, end)`.
fn prefix_bounds(start: &[u8], end: Option<&[u8]>) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
    let mut s = Vec::new();
    if !start.is_empty() {
        encode_row_prefix(&mut s, start)?;
    }
    let e = match end {
        Some(e) => {
            let mut k = Vec::new();
            encode_row_prefix(&mut k, e)?;
            Some(k)
        }
        None => None,
    };
    Ok((s, e))
}

/// Whether an SST holds rows within the row-prefix range `[start, end)`.
fn overlaps(meta: &SstMeta, start: &[u8], end: Option<&[u8]>) -> bool {
    let first = crate::snapshot::row_of(&meta.smallest_key);
    let last = crate::snapshot::row_of(&meta.largest_key);
    last >= start && end.is_none_or(|e| first < e)
}

/// The SSTs of a merge of `l` and `r`, per family as `(level, meta)`, each once.
///
/// Refused while a sibling still has to compact its copy of an SST they shared after a split
/// (D13): an SST only one side references must hold none of the other side's rows (the other
/// side rewrote them; the merged tablet would see them twice), and no level of the result may
/// hold overlapping SSTs.
fn merged_ssts(
    catalog: &Catalog,
    l: &TabletEntry,
    r: &TabletEntry,
) -> Result<Vec<(FamilyId, u8, Arc<SstMeta>)>> {
    let (ls, le) = prefix_bounds(&l.start, l.end.as_deref())?;
    let (rs, re) = prefix_bounds(&r.start, r.end.as_deref())?;
    let pending = || {
        Error::InvalidArgument(
            "a sibling has not compacted a shared SST yet; merge after compaction".to_owned(),
        )
    };
    let mut out = Vec::new();
    for f in catalog.family_ids_of(l.table) {
        let ids = |t: &TabletEntry| -> HashSet<SstId> {
            catalog
                .ssts
                .get(&(t.id, f))
                .map(|list| list.iter().map(|(_, m)| m.id).collect())
                .unwrap_or_default()
        };
        let (lids, rids) = (ids(l), ids(r));
        let mut seen: HashSet<SstId> = HashSet::new();
        let mut levels: BTreeMap<u8, Vec<Arc<SstMeta>>> = BTreeMap::new();
        for t in [l, r] {
            if let Some(list) = catalog.ssts.get(&(t.id, f)) {
                for (level, meta) in list {
                    let only_left = lids.contains(&meta.id) && !rids.contains(&meta.id);
                    let only_right = rids.contains(&meta.id) && !lids.contains(&meta.id);
                    if (only_left && overlaps(meta, &rs, re.as_deref()))
                        || (only_right && overlaps(meta, &ls, le.as_deref()))
                    {
                        return Err(pending());
                    }
                    if seen.insert(meta.id) {
                        levels.entry(*level).or_default().push(Arc::clone(meta));
                        out.push((f, *level, Arc::clone(meta)));
                    }
                }
            }
        }
        for (level, list) in &mut levels {
            if *level == 0 {
                continue;
            }
            list.sort_by(|a, b| a.smallest_key.cmp(&b.smallest_key));
            if list
                .windows(2)
                .any(|w| w[0].largest_key >= w[1].smallest_key)
            {
                return Err(Error::InvalidArgument(
                    "the tablets' SSTs overlap within a level; merge after compaction".to_owned(),
                ));
            }
        }
    }
    Ok(out)
}

/// The manifest edits and owners of a tablet change, against the catalog at commit time.
fn tablet_change(
    catalog: &mut Catalog,
    kind: &TabletOpKind,
    entries: &[TabletEntry],
    me: ShardId,
) -> Result<TabletEdits> {
    for e in entries {
        match catalog.tablet(e.id) {
            Some(t) if t.table == e.table && t.start == e.start && t.end == e.end => {}
            _ => {
                return Err(Error::TableNotFound(format!(
                    "tablet {} changed during the split, merge or move",
                    e.id.0
                )));
            }
        }
    }
    let table = entries[0].table;
    let families = catalog.family_ids_of(table);
    match kind {
        TabletOpKind::Move { tablet, to } => Ok((Vec::new(), vec![(*tablet, *to)])),
        TabletOpKind::Split {
            tablet,
            keys,
            owners,
        } => {
            let parent = &entries[0];
            let mut starts: Vec<Vec<u8>> = vec![parent.start.clone()];
            starts.extend(keys.iter().cloned());
            let mut ends: Vec<Option<Vec<u8>>> = keys.iter().cloned().map(Some).collect();
            ends.push(parent.end.clone());
            let mut edits = Vec::new();
            let mut out = Vec::new();
            for (i, (start, end)) in starts.into_iter().zip(ends).enumerate() {
                let id = catalog.alloc_tablet();
                let (s, e) = prefix_bounds(&start, end.as_deref())?;
                edits.push(Edit::PutTablet {
                    tablet: id,
                    table,
                    start,
                    end,
                });
                out.push((id, owners[i]));
                for &f in &families {
                    if let Some(list) = catalog.ssts.get(&(parent.id, f)) {
                        for (level, meta) in list {
                            if overlaps(meta, &s, e.as_deref()) {
                                edits.push(Edit::AddSst {
                                    tablet: id,
                                    family: f,
                                    level: *level,
                                    meta: (**meta).clone(),
                                });
                            }
                        }
                    }
                    if let Some(seqno) = catalog.flushed.get(&(parent.id, f)) {
                        edits.push(Edit::SetFlushed {
                            tablet: id,
                            family: f,
                            seqno: *seqno,
                        });
                    }
                }
            }
            edits.push(Edit::DropTablet { tablet: *tablet });
            Ok((edits, out))
        }
        TabletOpKind::Merge { left, right } => {
            let (l, r) = (&entries[0], &entries[1]);
            let ssts = merged_ssts(catalog, l, r)?;
            let id = catalog.alloc_tablet();
            let mut edits = vec![Edit::PutTablet {
                tablet: id,
                table,
                start: l.start.clone(),
                end: r.end.clone(),
            }];
            for (f, level, meta) in ssts {
                edits.push(Edit::AddSst {
                    tablet: id,
                    family: f,
                    level,
                    meta: (*meta).clone(),
                });
            }
            for &f in &families {
                let seqno = [left, right]
                    .iter()
                    .filter_map(|t| catalog.flushed.get(&(**t, f)).copied())
                    .max();
                if let Some(seqno) = seqno {
                    edits.push(Edit::SetFlushed {
                        tablet: id,
                        family: f,
                        seqno,
                    });
                }
            }
            edits.push(Edit::DropTablet { tablet: *left });
            edits.push(Edit::DropTablet { tablet: *right });
            Ok((edits, vec![(id, me)]))
        }
    }
}

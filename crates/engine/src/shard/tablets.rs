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

/// Why a PREPARE is refused (D245), and the index of its test-hook count
/// (`Engine::refusal_counts`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FloorRefusal {
    MoveRaise = 0,
    Below = 1,
    Above = 2,
    /// Counted by `refuse_for_room` in test-hook builds only.
    #[cfg_attr(not(feature = "test-hooks"), allow(dead_code))]
    Room = 3,
}

/// `ShardState::stamps` drops the entries at or below the visible watermark once it holds
/// this many (D244).
const STAMPS_PRUNE_AT: usize = 32;

/// Weight of the newest interval in the moving averages of write load (#103, D214): one
/// interval's noise never decides a move on its own.
const LOAD_ALPHA: f64 = 0.2;

/// Consecutive balancer passes a shard's load must stay skewed (above `balance_skew` times
/// the mean, and at least `balance_min_writes`) before it starts acting on write skew
/// (D214): short-term noise never moves a tablet.
const SKEW_PERSIST: u64 = 10;

/// Where a shard that is acting on write skew stops (D214): it keeps shedding load after
/// each dwell until its load falls below `1 + SKEW_STOP * (balance_skew - 1)` times the mean
/// (1.1 at the default skew of 1.25), then needs `SKEW_PERSIST` skewed passes to start
/// again.
const SKEW_STOP: f64 = 0.4;

/// Weight of the newest pass in the slow moving average of the mean write load (D224).
const MEAN_SLOW_ALPHA: f64 = 0.05;

/// The mean write load is falling while it is below this fraction of its slow average
/// (D224, #582). Skew that appears because other shards' load fell, not because this shard's
/// rose, is the tail of a closed-loop load: the lighter shards finished their work, and the
/// busy ones look skewed only beside them. Such a pass neither counts toward
/// `SKEW_PERSIST` nor resets it, and moves no tablet. If the others' load stays down, the
/// slow average catches up in a few seconds and the skew counts again.
const MEAN_FALL: f64 = 0.9;

/// How long a tablet must go unwritten, and have been on its shard, before it counts as cold
/// for merges and consolidation (D224): `COLD_PASSES` balancer passes or as many base
/// intervals of clock (10 s at the default), whichever comes first, so an idle shard backing
/// off its passes still merges. Longer than a closed-loop load's phases, so a tablet idle only
/// through a phase's tail (its shard finished its work first) is not merged or moved away.
pub(crate) const COLD_PASSES: u64 = 100;

/// Weight of the newest interval in the slow moving averages of write load (D224): a time
/// constant of about 10 s at the default interval, longer than a closed-loop load's phases, so
/// a shard's slow load follows its share of the work rather than its rate while busy.
const SLOW_ALPHA: f64 = 0.01;

/// Balancer passes the slow skew path waits after a change before deciding again (D224):
/// about the slow averages' time constant, so they show the change first.
const SLOW_DWELL: u64 = 100;

/// Balancer passes a shard waits after starting a move or a skew split, and a tablet stays
/// put after it arrived on a shard, before write skew moves or splits again (#103): the
/// moving averages catch up with a change before the next one is decided.
const DWELL_PASSES: u64 = 10;

/// The longest balancer interval of an idle shard: passes that find nothing to do, with no
/// writes and no tablet change since the last one, double the interval up to this (10 s).
const BALANCE_IDLE_CAP_NANOS: u64 = 10_000_000_000;

/// Packing (D217): the busy fraction each active shard aims for. With `B` the summed busy
/// fraction of all shards, `ceil(B / PACK_TARGET)` shards stay active, so each has headroom
/// before commits queue.
const PACK_TARGET: f64 = 0.5;

/// Packing grows the active shards once `B / k` stays above this for `PACK_GROW_PASSES`.
const PACK_SPREAD: f64 = 0.75;

/// Consecutive passes over `PACK_SPREAD` before more shards become active (1 s): a burst
/// spreads quickly.
const PACK_GROW_PASSES: u64 = 10;

/// Consecutive passes with `B / (k - 1)` below `PACK_TARGET` before fewer shards stay active
/// (5 s): packing is slow, so a pause between bursts does not reshuffle.
const PACK_SHRINK_PASSES: u64 = 50;

/// Packing (D251): an active shard whose thread is busier than this (turn busy, without
/// D248's sync term) also calls for more active shards, whatever the average (`PACK_SPREAD`
/// looks only at `B / k`, which a saturated shard beside an idle one never passes), and while
/// one is, the active shards never shrink.
const PACK_HOT: f64 = 0.9;

/// How much a hot shard's turn busy fraction must fall (millionths) after a hot-triggered
/// growth before another one (D251): shedding helped.
const PACK_HOT_COOLED: u64 = 20_000;

/// The fast write average over the slow one above which the load is still ramping (D255):
/// the shrink counter pauses then (it does not reset: a periodic batch writer reads as
/// ramping after every batch, and a reset would never let it pack). The first pass of a
/// load read its ramp as a light store and cut the active shards from 16 to 2 or 3 on the
/// AX102, and the full load then sat there (#734).
const PACK_RAMP: f64 = 1.25;

/// Consecutive ramping passes after which the shrink counter runs anyway (60 s at the base
/// interval): a load that only ever grows still packs.
const PACK_RAMP_MAX: u64 = 600;

/// A published stall fraction at or above which an active shard counts as stalling for the
/// shrink count (D255): a pass on which any active shard's writers stall is no light pass,
/// whatever the busy sum reads. On the AX102 a write lull in the middle of a 16 GiB load
/// (the loader's writers waiting on room: writes 2,479 against 10–48K around it, busy
/// 0.27) completed the count the ramp had let accrue, and D217 shrank 16 → 2 mid-load.
const PACK_STALLING: u64 = 100_000;

/// Balancer passes after a shrink within which a stall-driven growth undoes it (60 s at
/// the base interval, D255): the growth goes straight back to the count before the shrink.
/// Regrowing one shard per ten passes took most of a 16 GiB load on the AX102 after one
/// mistaken shrink from 16 to 2.
const PACK_SHRINK_UNDO: u64 = 600;

/// A shard reads stalled when its writers stalled on at least `PACK_STALL_PASSES` of the
/// last `PACK_STALL_WINDOW` balancer passes (3 of 10: about 3 stalls a second at the base
/// interval, or one wait spanning 0.3 s, D255). A buffered load's stalls are short and a few
/// per second per shard: a pass-by-pass reading smoothed by the moving average read them at
/// about half and never hot (the buffered riders row on the AX102: thousands of write stalls,
/// the top shards' stall terms at 0), while any single stall must not count as capacity (a
/// light store with one burst would spread). The term is 0 or 1 and the hot test reads it
/// as published, so the decision is this count, not a moving average's tail.
const PACK_STALL_WINDOW: u32 = 10;
const PACK_STALL_PASSES: u32 = 3;

/// Balancer passes with no active shard stalling after which a stall-driven floor expires
/// (30 s at the base interval), and the cap of its doubling after each expiry (D255).
const PACK_FLOOR_QUIET: u64 = 300;
const PACK_FLOOR_QUIET_MAX: u64 = 300 * 16;

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
    /// A uniform sample of the rows written this interval: the first `sampled`. Buffers past
    /// it are earlier intervals' rows, kept for reuse so a new interval's samples do not
    /// allocate (#320).
    samples: Vec<Vec<u8>>,
    sampled: usize,
    /// Moving average of `writes` over intervals (`LOAD_ALPHA`).
    ewma: f64,
    /// Slow moving average of `writes` (`SLOW_ALPHA`, D224).
    slow: f64,
    /// The balancer pass and clock of the last interval with writes (D224).
    last_write: (u64, u64),
}

impl TabletLoad {
    /// This interval's row samples.
    fn live_samples(&self) -> &[Vec<u8>] {
        &self.samples[..self.sampled]
    }
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
    /// `EngineOptions::balance_pack`, engine-owned shards only (D217).
    pub pack: bool,
    /// Unwritten passes before a tablet is cold (`EngineOptions::balance_cold_passes`, D224).
    pub cold_passes: u64,
}

pub(crate) use crate::load_slot::LoadSlot;

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
        match m.kind {
            MemberKind::CommitRecord { .. } => Some(m),
            MemberKind::Prepare { .. } => self.route_prepare(m, ctx),
            MemberKind::Single => self.route_by_map(m, ctx),
        }
    }

    /// A PREPARE's admission routing: refused out of seqno order with what this shard
    /// stamped (D142, D244, D245), else routed as any member and, when it runs here, kept
    /// for later PREPAREs' checks.
    #[inline(never)]
    fn route_prepare(&mut self, m: Member, ctx: &mut ShardContext<'_, ShardMsg>) -> Option<Member> {
        #[cfg(feature = "test-hooks")]
        {
            let forced = &self.shared.metrics[usize::from(self.id.0)]
                .hooks
                .refuse_prepares;
            if forced.load(Ordering::Acquire) > 0 {
                forced.fetch_sub(1, Ordering::AcqRel);
                self.shared.metrics[usize::from(self.id.0)].count_refusal(CrossRefusal::BelowFloor);
                self.refuse_prepare(m, PrepareError::BelowFloor, ctx);
                return None;
            }
        }
        if let Some(why) = self.floor_refusal(&m) {
            // The coordinator's retry reads this shard's floor again, raised to the refused
            // timestamp, or past the earlier-seqno timestamp above it that refused it
            // (D244, D245): the retry's fresh timestamp clears what refused this one.
            #[cfg(feature = "test-hooks")]
            self.shared.hooks.refusals[why as usize].fetch_add(1, Ordering::Relaxed);
            let conflict = match why {
                FloorRefusal::Below => self.floor_below(m.seqno),
                FloorRefusal::MoveRaise | FloorRefusal::Above | FloorRefusal::Room => 0,
            };
            self.raise_ts_floor(m.commit_ts.max(conflict));
            let floor = self.ts_floor();
            self.shared.metrics[usize::from(self.id.0)].count_refusal(match why {
                FloorRefusal::MoveRaise => CrossRefusal::MoveRaise,
                FloorRefusal::Below | FloorRefusal::Above | FloorRefusal::Room => {
                    CrossRefusal::BelowFloor
                }
            });
            self.shared.ts_floors[usize::from(self.id.0)]
                .0
                .fetch_max(floor, Ordering::AcqRel);
            self.refuse_prepare(m, PrepareError::BelowFloor, ctx);
            return None;
        }
        let m = self.route_by_map(m, ctx)?;
        self.note_prepare(&m);
        Some(m)
    }

    /// Routing through the tablet map for a single or a PREPARE: returns it if this shard
    /// should run it now; parks, forwards or refuses it otherwise. Inlined into both callers:
    /// a single's commit path pays no call for it.
    #[inline(always)]
    fn route_by_map(&mut self, m: Member, ctx: &mut ShardContext<'_, ShardMsg>) -> Option<Member> {
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
                self.shared.metrics[usize::from(self.id.0)].count_refusal(CrossRefusal::Moved);
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
        if let Some(reads) = m.validate.as_ref().map(|v| &v.reads) {
            for r in reads {
                look(r.table, &r.row, prepare);
            }
        }
        if let Some((table, row, _)) = m.predicate.as_deref() {
            look(*table, row, false);
        }
        out
    }

    /// Why `m`, a PREPARE writing rows at `(S, T)`, would put this shard's timestamps out of
    /// seqno order (D11, D142, D245), if it would; ties are allowed:
    /// - `MoveRaise`: `T` is below the floor a shard handing this one a tablet raised it to
    ///   (`ts_raises`), seqno-blind, as D142 had it: moves are rare.
    /// - `Below`: this shard gave a seqno below `S` a timestamp above `T` (`floor_below`).
    ///   The coordinator raised this shard's floor before reserving `S` (D245), so only a
    ///   write reserved before `S` and stamped after the coordinator read the floors can.
    /// - `Above`: it gave a seqno above `S` a timestamp below `T`: only a PREPARE whose
    ///   coordinator read the floors before this one's raised them, and reserved after.
    fn floor_refusal(&self, m: &Member) -> Option<FloorRefusal> {
        if !matches!(m.kind, MemberKind::Prepare { .. })
            || !BatchRef::new(m.bytes.as_slice()).is_ok_and(|b| b.iter().next().is_some())
        {
            return None;
        }
        let raised = self.shared.ts_raises[usize::from(self.id.0)]
            .0
            .load(Ordering::Acquire);
        if m.commit_ts < raised {
            Some(FloorRefusal::MoveRaise)
        } else if self.floor_below(m.seqno) > m.commit_ts {
            Some(FloorRefusal::Below)
        } else if self
            .stamps
            .iter()
            .any(|&(seqno, ts)| seqno > m.seqno && ts < m.commit_ts)
        {
            Some(FloorRefusal::Above)
        } else {
            None
        }
    }

    /// The largest timestamp this shard gave a seqno below `seqno` (D245): everything pruned
    /// or stamped before the history, each recent group's first and last single, and each
    /// routed PREPARE. A group's seqnos are contiguous (one reservation), so a group is
    /// wholly below or above `seqno`.
    pub(super) fn floor_below(&self, seqno: Seqno) -> Timestamp {
        self.stamps
            .iter()
            .chain(self.stamps_last.iter())
            .filter(|&&(s, _)| s < seqno)
            .map(|&(_, ts)| ts)
            .fold(self.pruned_max, Timestamp::max)
    }

    /// Folds the coordinators' raises into the floor (D245): once per group, after its
    /// reservation, so the load follows the reservation's read-modify-write on `NEXT_SEQNO`.
    #[inline(never)]
    pub(super) fn fold_coord_raise(&mut self) {
        let raised = self.shared.ts_coord_raises[usize::from(self.id.0)]
            .0
            .load(Ordering::Acquire);
        self.ts_floor = self.ts_floor.max(raised);
    }

    /// A PREPARE that would wait for arena room, refused instead (D245, ICR 0042); its
    /// coordinator hears `RoomFreed` once this shard's room wait ends.
    #[inline(never)]
    pub(super) fn refuse_for_room(
        &mut self,
        m: Member,
        coordinator: ShardId,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        #[cfg(feature = "test-hooks")]
        self.shared.hooks.refusals[FloorRefusal::Room as usize].fetch_add(1, Ordering::Relaxed);
        self.shared.metrics[usize::from(self.id.0)].count_refusal(CrossRefusal::Room);
        if !self.room_refused.contains(&coordinator) {
            self.room_refused.push(coordinator);
        }
        self.room_refused_free = self.arena.free_bytes().saturating_sub(self.reserved);
        self.refuse_prepare(m, PrepareError::Room(self.room_generation), ctx);
    }

    /// Whether room may fit a share this shard refused for room (D245): its room wait ended,
    /// or nothing waits or flushes and more is free than at the latest refusal (after
    /// reclaiming the memtables readers left).
    #[inline(never)]
    pub(super) fn room_freed_since_refusal(&mut self) -> bool {
        if self.room_wait.is_none() {
            return true;
        }
        if !self.pending.is_empty() || self.flush_running || !self.flush_queue.is_empty() {
            return false;
        }
        self.reclaim_retired();
        self.refresh_free();
        self.arena.free_bytes().saturating_sub(self.reserved) > self.room_refused_free
    }

    /// Tells the coordinators this shard refused for room that it has room again (D245).
    #[inline(never)]
    pub(super) fn notify_room_freed(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        self.room_generation += 1;
        let generation = self.room_generation;
        for c in std::mem::take(&mut self.room_refused) {
            trace!(
                "shard {} room freed ({generation}): tells {}",
                self.id.0, c.0
            );
            self.send(
                c,
                ShardMsg::Wake {
                    from: self.id,
                    reason: WakeReason::Room { generation },
                },
                ctx,
            );
        }
    }

    /// Runs the retries that waited for `from`'s room before `generation` (D245).
    pub(super) fn on_room_freed(
        &mut self,
        from: ShardId,
        generation: u64,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        let seen = &mut self.room_freed[usize::from(from.0)];
        *seen = (*seen).max(generation);
        let (ready, wait): (Vec<CoordinateReq>, Vec<CoordinateReq>) =
            std::mem::take(&mut self.retries)
                .into_iter()
                .partition(|r| {
                    r.room_wait
                        .is_some_and(|(p, g)| p == from && g < generation)
                });
        self.retries = wait;
        trace!(
            "shard {} room freed on {} ({generation}): {} retries ready",
            self.id.0,
            from.0,
            ready.len()
        );
        for mut r in ready {
            r.room_wait = None;
            if self.closing {
                r.reply.notify(Err(Error::Closed));
            } else {
                self.retry(r, ctx);
            }
        }
        self.arm_tablet_timer(ctx);
    }

    /// The rows a conditional member writes, reads, or tests (`hash_row`), deduplicated: what
    /// `conflicts_with_group` checks against undecided shares.
    fn member_row_hashes(m: &Member) -> Vec<u64> {
        let mut rows = Vec::new();
        if let Ok(batch) = BatchRef::new(m.bytes.as_slice()) {
            for mu in batch.iter().flatten() {
                rows.push(hash_row(mu.table, mu.row));
            }
        }
        if let Some(v) = &m.validate {
            for r in &v.reads {
                rows.push(hash_row(r.table, &r.row));
            }
        }
        if let Some((table, row, _)) = m.predicate.as_deref() {
            rows.push(hash_row(*table, row));
        }
        rows.sort_unstable();
        rows.dedup();
        rows
    }

    /// A PREPARE overlapping an undecided share, refused rather than held (D249, ICR 0045).
    /// Its coordinator is woken once no undecided share here overlaps it; if one of the
    /// shares it overlaps belongs to an older commit, only once that commit has run its next
    /// attempt here (or its last, or retried elsewhere), so the older one goes first.
    #[cold]
    #[inline(never)]
    pub(super) fn refuse_for_overlap(&mut self, m: Member, ctx: &mut ShardContext<'_, ShardMsg>) {
        let MemberKind::Prepare { coordinator } = m.kind else {
            return;
        };
        let rows = Self::member_row_hashes(&m);
        let priority = self
            .prepared
            .get(&m.seqno)
            .map_or((0, m.seqno), |s| s.priority);
        // The oldest commit among the undecided shares it overlaps.
        let mut oldest: Option<(CommitPriority, Seqno)> = None;
        for (seqno, share) in &self.prepared {
            if *seqno == m.seqno || !share.tracked {
                continue;
            }
            let overlaps = BatchRef::new(share.bytes.batch().as_bytes()).is_ok_and(|b| {
                b.iter()
                    .flatten()
                    .any(|mu| rows.binary_search(&hash_row(mu.table, mu.row)).is_ok())
            });
            if overlaps && oldest.is_none_or(|(p, _)| share.priority < p) {
                oldest = Some((share.priority, *seqno));
            }
        }
        let behind = oldest.filter(|(p, _)| *p < priority);
        trace!(
            "shard {} refuses {} over an undecided overlap (behind {behind:?})",
            self.id.0, m.seqno
        );
        self.shared.metrics[usize::from(self.id.0)].count_refusal(CrossRefusal::PendingOverlap);
        self.overlap_refused.push(OverlapRefusal {
            coordinator,
            priority,
            rows,
            behind,
        });
        self.refuse_prepare(m, PrepareError::PendingOverlap, ctx);
    }

    /// After a decision here (D249): wakes the coordinators refused over an overlap that has
    /// cleared. One waiting behind an older commit is woken by that commit's next attempt's
    /// decision here (a later seqno than the share it met), or its last; any other once no
    /// undecided share here holds its rows.
    #[inline(never)]
    pub(super) fn release_overlaps(
        &mut self,
        seqno: Seqno,
        priority: CommitPriority,
        last: bool,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        let mut i = 0;
        while i < self.overlap_refused.len() {
            let r = &self.overlap_refused[i];
            let go = match r.behind {
                Some((older, met)) => older == priority && (last || seqno > met),
                None => !r.rows.iter().any(|h| self.pending_rows.contains_key(h)),
            };
            if go {
                let r = self.overlap_refused.swap_remove(i);
                self.wake_overlap(r, ctx);
            } else {
                i += 1;
            }
        }
    }

    /// The commit of `priority` retries without this shard (D249): the commits waiting here
    /// behind it are woken.
    pub(super) fn release_overlaps_behind(
        &mut self,
        priority: CommitPriority,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        let mut i = 0;
        while i < self.overlap_refused.len() {
            if self.overlap_refused[i]
                .behind
                .is_some_and(|(older, _)| older == priority)
            {
                let r = self.overlap_refused.swap_remove(i);
                self.wake_overlap(r, ctx);
            } else {
                i += 1;
            }
        }
    }

    fn wake_overlap(&mut self, r: OverlapRefusal, ctx: &mut ShardContext<'_, ShardMsg>) {
        trace!(
            "shard {} overlap cleared for {:?}: tells {}",
            self.id.0, r.priority, r.coordinator.0
        );
        self.send(
            r.coordinator,
            ShardMsg::Wake {
                from: self.id,
                reason: WakeReason::Overlap {
                    priority: r.priority,
                },
            },
            ctx,
        );
    }

    /// A coordinator's side of an overlap wake (D249): the retry of the commit of `priority`
    /// that waited for `from` runs now. One that came before the refused round was decided is
    /// kept for its retry.
    pub(super) fn on_overlap_cleared(
        &mut self,
        from: ShardId,
        priority: CommitPriority,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        if let Some(i) = self.retries.iter().position(|r| {
            r.overlap_wait == Some(from) && (r.submitted_at, r.first_seqno) == priority
        }) {
            let mut r = self.retries.swap_remove(i);
            r.overlap_wait = None;
            if self.closing {
                r.reply.notify(Err(Error::Closed));
            } else {
                self.retry(r, ctx);
            }
            self.arm_tablet_timer(ctx);
        } else if self
            .coord
            .values()
            .any(|c| c.priority == priority && c.overlap == Some(from))
        {
            self.overlap_released.push((from, priority));
        }
    }

    /// A PREPARE routed to this shard (D244): kept, so a PREPARE at another seqno, in this
    /// group or a later one, is checked against it (`floor_refusal`).
    #[inline(never)]
    fn note_prepare(&mut self, m: &Member) {
        if matches!(m.kind, MemberKind::Prepare { .. })
            && BatchRef::new(m.bytes.as_slice()).is_ok_and(|b| b.iter().next().is_some())
        {
            self.note_stamp(m.seqno, m.commit_ts);
        }
    }

    /// An admitted PREPARE (D244): its timestamp raises the floor before the group's
    /// singles are stamped, so none of those, all at later seqnos, lands below it. A PREPARE
    /// that waits for room is refused instead (D245).
    #[inline(never)]
    pub(super) fn raise_for_prepare(&mut self, m: &Member) {
        if BatchRef::new(m.bytes.as_slice()).is_ok_and(|b| b.iter().next().is_some()) {
            self.raise_ts_floor(m.commit_ts);
        }
    }

    /// Drops a refused or aborted PREPARE's entry (D244): it never lands, and it would keep
    /// refusing PREPAREs out of order with it until the watermark passes it.
    pub(super) fn drop_stamp(&mut self, seqno: Seqno) {
        if !self.stamps.is_empty() {
            self.stamps.retain(|&(s, _)| s != seqno);
        }
    }

    /// Keeps a group's first single (D244): the lowest seqno and default timestamp of the
    /// group, since each later member's is at or above both.
    #[inline(never)]
    pub(super) fn note_group_stamps(&mut self, admitted: &[Member]) {
        if let Some(m) = admitted
            .iter()
            .find(|m| matches!(m.kind, MemberKind::Single))
        {
            self.note_stamp(m.seqno, m.commit_ts);
        }
        if let Some(m) = admitted
            .iter()
            .rev()
            .find(|m| matches!(m.kind, MemberKind::Single))
        {
            self.stamps_last.push((m.seqno, m.commit_ts));
        }
    }

    /// Keeps `(seqno, ts)` for `floor_refusal`: a PREPARE that seqno could meet has a lower
    /// seqno still held by its coordinator, so above the visible watermark; entries at or
    /// below it are dropped.
    pub(super) fn note_stamp(&mut self, seqno: Seqno, ts: Timestamp) {
        if self.stamps.len() >= STAMPS_PRUNE_AT || self.stamps_last.len() >= STAMPS_PRUNE_AT {
            let visible = self.shared.shm.visible_seqno();
            let mut pruned = self.pruned_max;
            for v in [&mut self.stamps, &mut self.stamps_last] {
                v.retain(|&(s, t)| {
                    s > visible || {
                        pruned = pruned.max(t);
                        false
                    }
                });
            }
            self.pruned_max = pruned;
        }
        self.stamps.push((seqno, ts));
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
        self.drop_stamp(m.seqno);
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
            m.validate.as_ref().map(|v| v.reads.as_slice()),
            m.predicate.as_deref().map(|(t, r, _)| (*t, r.as_slice())),
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
                let retried = req.retry.map(|r| *r).unwrap_or_default();
                let creq = CoordinateReq {
                    parts,
                    durability: req.durability,
                    reply,
                    submitted_at: req.submitted_at,
                    validate: req.validate,
                    map_version: view.tablets.version(),
                    commit_ts: retried.preset,
                    epoch: 0,
                    room_wait: None,
                    first_seqno: retried.first_seqno,
                    prev_shards: retried.prev_shards,
                    overlap_wait: None,
                };
                if shards[0] == self.id {
                    self.start_coordination(creq, ctx);
                } else {
                    let _ = ctx
                        .submitter(shards[0])
                        .submit(ShardMsg::Coordinate(Box::new(creq)));
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
        // After a shard's death no commit is accepted (D227): one waiting for a participant's
        // room, or for an overlap there to clear, may wait on the dead one, so it ends now
        // rather than at the timeout (D245, D249).
        let waits = |r: &CoordinateReq| r.room_wait.is_some() || r.overlap_wait.is_some();
        if self.shared.shard_died.load(Ordering::Acquire) && self.retries.iter().any(waits) {
            let (dead, keep): (Vec<CoordinateReq>, Vec<CoordinateReq>) =
                std::mem::take(&mut self.retries)
                    .into_iter()
                    .partition(waits);
            self.retries = keep;
            for r in dead {
                r.reply.notify(Err(Error::Closed));
            }
        }
        let version = self.shared.view.load().tablets.version();
        let epoch = self.shared.tablet_epoch.load(Ordering::Acquire);
        let (ready, wait): (Vec<CoordinateReq>, Vec<CoordinateReq>) =
            std::mem::take(&mut self.retries)
                .into_iter()
                .partition(|r| {
                    r.room_wait.is_none()
                        && r.overlap_wait.is_none()
                        && (r.map_version < version || r.epoch < epoch)
                });
        self.retries = wait;
        if !self.retries.is_empty() {
            trace!(
                "shard {} retries waiting: {:?} (map v{version}, epoch {epoch})",
                self.id.0,
                self.retries
                    .iter()
                    .map(|r| (r.room_wait.map(|(s, g)| (s.0, g)), r.map_version, r.epoch))
                    .collect::<Vec<_>>()
            );
        }
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
            retry: Some(Box::new(Retried {
                preset: r.commit_ts,
                first_seqno: r.first_seqno,
                prev_shards: r.prev_shards,
            })),
            map_version: r.map_version,
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
                    if self.freeze_moving().is_err() {
                        self.poison(crate::health::FailureCause::Internal);
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
                    self.shared.metrics[usize::from(self.id.0)]
                        .refused_view
                        .fetch_add(1, Ordering::Relaxed);
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
        let reserved = self.shared.loads[usize::from(shard.0)].reserved();
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
            let before = self.shared.loads[usize::from(shard.0)].reserve(n);
            out.push((shard, n));
            if Self::slots_in(view, shard) + before + n > self.max_slots() {
                self.release_slots(&out);
                self.shared.metrics[usize::from(self.id.0)]
                    .refused_slots
                    .fetch_add(1, Ordering::Relaxed);
                return Err(Error::Unsupported(
                    "a shard would hold more tablets than its memtable arena serves",
                ));
            }
        }
        Ok(out)
    }

    fn release_slots(&self, reserved: &[(ShardId, usize)]) {
        for &(shard, n) in reserved {
            self.shared.loads[usize::from(shard.0)].release(n);
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
            if self.freeze_moving().is_err() {
                self.poison(crate::health::FailureCause::Internal);
            }
            self.spawn_flush(ctx);
        }
        if busy {
            return;
        }
        self.commit_op(ctx);
    }

    /// Freezes the memtables of the tablets being changed (`freeze` freezes them whatever
    /// they hold).
    fn freeze_moving(&mut self) -> Result<()> {
        #[cfg(feature = "test-hooks")]
        if self
            .shared
            .hooks
            .fail_next_tablet_freeze
            .swap(false, Ordering::AcqRel)
        {
            return Err(Error::Busy);
        }
        self.freeze(false)
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
            compactions: Vec::new(),
            rewrite_snapshot: false,
            dropped_ok: Vec::new(),
            on_refusal: Vec::new(),
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
                    self.poison(crate::health::FailureCause::Internal);
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
        if l.sampled < SAMPLES {
            match l.samples.get_mut(l.sampled) {
                Some(s) => {
                    s.clear();
                    s.extend_from_slice(row);
                }
                None => l.samples.push(row.to_vec()),
            }
            l.sampled += 1;
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
        self.load_slow =
            SLOW_ALPHA * self.window_writes as f64 + (1.0 - SLOW_ALPHA) * self.load_slow;
        let (pass, clock) = (self.balance_pass, now);
        for l in self.loads.values_mut() {
            if l.writes > 0 {
                l.last_write = (pass, clock);
            }
            l.ewma = LOAD_ALPHA * l.writes as f64 + (1.0 - LOAD_ALPHA) * l.ewma;
            l.slow = SLOW_ALPHA * l.writes as f64 + (1.0 - SLOW_ALPHA) * l.slow;
        }
        self.note_arrivals(now);
        let (_, mem_per) = self.mem_bytes();
        self.measure_busy(now);
        self.shared.loads[usize::from(self.id.0)]
            .set_turn_busy((self.turn_ewma * 1e6).round() as u64);
        self.shared.loads[usize::from(self.id.0)]
            .set_stall_busy((self.stall_term * 1e6).round() as u64);
        self.shared.loads[usize::from(self.id.0)].publish(
            self.load_ewma.round() as u64,
            self.load_slow.round() as u64,
            (self.busy_ewma * 1e6).round() as u64,
            now.max(1),
            self.balance_pass,
        );
        let store_writes = self.track_pack(now);
        #[cfg(feature = "test-hooks")]
        self.shared.metrics[usize::from(self.id.0)]
            .hooks
            .pack_k
            .store(self.active_shards() as u64 + 1, Ordering::Relaxed);
        self.track_skew(now);
        let idle = self.may_change();
        // A change waits for a running compaction on its tablets (#102: a running change
        // parks their writes, and a compaction can run for minutes). It is decided again as
        // soon as this one ends, before the next one is picked, so back-to-back compactions
        // never starve it.
        self.change_waits = false;
        let (decision, cleanups) = if idle {
            let (kind, cleanups) = self.decide(now, &mem_per);
            let kind = kind.filter(|kind| match self.compaction {
                Some((t, _)) if kind.tablets().contains(&t) => {
                    self.change_waits = true;
                    false
                }
                _ => true,
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
            l.sampled = 0;
        }
        let decision_none = decision.is_none();
        match decision {
            Some(kind) => {
                trace!("shard {} balancer: {kind:?}", self.id.0);
                self.start_change(kind, reply, ctx);
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
        // Shard 0 decides the active shard count for every shard: it keeps the base interval
        // while the store writes, even when it writes nothing itself (D217).
        let quiet = idle
            && !wrote
            && !store_writes
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

    /// Whether the balancer may start a tablet change now.
    fn may_change(&self) -> bool {
        self.op.is_none()
            && self.op_queue.is_empty()
            && !self.closing
            && !self.replaying
            && !self.poisoned
            && !self.shared.closing.load(Ordering::Acquire)
            && !self.shared.pager_poisoned.load(Ordering::Acquire)
            && self.shared.full_compactions.load(Ordering::Acquire) == 0
    }

    /// The compaction a change waited on ended: decides again (on this interval's loads)
    /// and starts the change, before the next compaction is picked.
    pub(super) fn start_waiting_change(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        self.change_waits = false;
        if !self.may_change() {
            return;
        }
        let (_, mem_per) = self.mem_bytes();
        let (kind, _) = self.decide(ctx.now_nanos(), &mem_per);
        if let Some(kind) = kind {
            trace!("shard {} balancer, after a compaction: {kind:?}", self.id.0);
            self.start_change(kind, None, ctx);
        }
    }

    /// Starts a change the balancer chose.
    fn start_change(
        &mut self,
        kind: TabletOpKind,
        reply: Option<Notifier<Result<()>>>,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        // Anything but a size split (a move, or a split over shards) starts the shard's
        // dwell: the loads settle before write skew acts again.
        let size_split = matches!(&kind, TabletOpKind::Split { owners, .. }
            if owners.iter().all(|o| *o == self.id));
        if !size_split {
            self.skew_pass = Some(self.balance_pass);
        }
        if self.chose_slow.get() {
            self.slow_pass = Some(self.balance_pass);
        }
        self.request_tablet_op(kind, reply, ctx);
    }

    /// Records the balancer pass at which each tablet this shard owns first appeared here
    /// (the tablets it held at its first pass count as settled).
    fn note_arrivals(&mut self, now: u64) {
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
        self.arrived_at.retain(|t, _| owned.contains(t));
        for t in owned {
            self.arrived_at
                .entry(t)
                .or_insert(if first { 0 } else { now });
            self.arrived.entry(t).or_insert(pass);
        }
    }

    /// Updates this shard's moving average of how busy it was since the last pass (D217,
    /// D248, D255): the largest of its thread's busy time, its group syncs' utilization and
    /// whether its writers are stalling, each over the time that passed. Utilization is the
    /// syncs' in-flight time (summed over the syncs in flight) over that time, capped at 1,
    /// so a shard whose durable commits keep a sync in flight reads busy although its thread
    /// mostly waits. Not divided by `group_sync_depth`: a shard's concurrent syncs fold into
    /// one journal commit (D221), so they are one server, not two (D248's A/B). The stall
    /// term is 1 when the writers stalled (a wait for room or L0 pacing began or was open) on
    /// at least `PACK_STALL_PASSES` of the last `PACK_STALL_WINDOW` passes, else 0 (D255):
    /// with flush and compaction on dedicated threads (D252) a shard carrying a full write
    /// load runs cheap turns and reads idle, so packing kept whole tables on one shard; a
    /// shard whose writers wait is hot in the sense that counts, more shards would take the
    /// load. `max`, since a turn often runs while a sync is in flight or writers wait. Syncs
    /// and stalls count only while the runtime counts turn time (engine-owned shards): an
    /// application-owned store stays unpacked, as before.
    fn measure_busy(&mut self, now: u64) {
        let me = usize::from(self.id.0);
        let busy = self
            .shared
            .submitters
            .get()
            .and_then(|s| s.get(me))
            .map_or(0, |s| s.busy_nanos());
        let sync = self.sync_busy_nanos(now);
        let elapsed = match self.busy_mark {
            Some((_, _, at)) if now > at => Some((now - at) as f64),
            _ => None,
        };
        // The thread's busy time (none counted for an application-owned shard).
        let turns = match (self.busy_mark, elapsed) {
            (Some((was, _, _)), Some(e)) => Some(busy.saturating_sub(was) as f64 / e),
            _ => None,
        };
        let counted = busy > 0;
        // A chosen turn busy, as an engine-owned shard measures it.
        #[cfg(feature = "test-hooks")]
        let (turns, counted) = match self.shared.metrics[me]
            .hooks
            .busy_override
            .load(Ordering::Relaxed)
        {
            0 => (turns, counted),
            v => (Some((v - 1) as f64 / 1e6), true),
        };
        let syncs = match (self.busy_mark, elapsed) {
            (Some((_, sync_was, _)), Some(e)) if counted => {
                sync.saturating_sub(sync_was) as f64 / e
            }
            _ => 0.0,
        };
        // This pass's stall bit, then the count over the window.
        let events = self.shared.metrics[me].stalls.load(Ordering::Relaxed);
        let stalled_now = events > self.stall_events_mark || self.stall_open();
        self.stall_events_mark = events;
        self.stall_history = (self.stall_history << 1) | u32::from(stalled_now);
        let window = self.stall_history & ((1u32 << PACK_STALL_WINDOW) - 1);
        let stalls = if counted && window.count_ones() >= PACK_STALL_PASSES {
            1.0
        } else {
            0.0
        };
        // A chosen stall term (D255's packing tests).
        #[cfg(feature = "test-hooks")]
        let stalls = match self.shared.metrics[me]
            .hooks
            .stall_override
            .load(Ordering::Relaxed)
        {
            0 => stalls,
            v => (v - 1) as f64 / 1e6,
        };
        let measured = turns.map(|t| t.max(syncs).max(stalls).min(1.0));
        if let Some(frac) = measured {
            self.busy_ewma = LOAD_ALPHA * frac + (1.0 - LOAD_ALPHA) * self.busy_ewma;
        }
        if let Some(t) = turns {
            self.turn_ewma = LOAD_ALPHA * t.min(1.0) + (1.0 - LOAD_ALPHA) * self.turn_ewma;
        }
        self.stall_term = stalls;
        self.busy_mark = Some((busy, sync, now));
        trace!(
            "shard {} balancer: busy turn {:.2} sync {:.2} stall {:.2} (events {}, window {:010b}) -> ewma {:.2}, turn {:.2}",
            self.id.0,
            turns.unwrap_or(0.0),
            syncs,
            stalls,
            events,
            window,
            self.busy_ewma,
            self.turn_ewma
        );
    }

    /// The shards that stay active under packing (D217): `0..k`. Every shard, without
    /// packing or before it has decided.
    pub(super) fn active_shards(&self) -> usize {
        let n = self.shared.shards;
        let k = self.shared.pack_k.load(Ordering::Acquire);
        if self.shared.balance.pack && k > 0 {
            k.min(n)
        } else {
            n
        }
    }

    /// Shard 0 decides how many shards stay active and publishes it; the others read it
    /// (`active_shards`), so every shard uses the same `k` (D217). Each shard keeping its own
    /// hysteresis over loads read at different passes disagreed for good. Returns whether the
    /// store writes enough for packing to act (shard 0 then keeps its base interval).
    fn track_pack(&mut self, now: u64) -> bool {
        if self.id.0 != 0 {
            return false;
        }
        let writing = self.decide_pack(now);
        self.shared.pack_k.store(self.pack_k, Ordering::Release);
        writing
    }

    /// Updates how many shards stay active (D217): shard 0 decides `k` for every shard and
    /// publishes it (`Shared::pack_k`), from the published busy fractions,
    /// `ceil(B / PACK_TARGET)`. It grows (at least by one) after `PACK_GROW_PASSES` passes
    /// with the active shards above `PACK_SPREAD` busy on average, or with one of them above
    /// `PACK_HOT` (D251), and shrinks after `PACK_SHRINK_PASSES` passes with one fewer shard
    /// enough to stay under `PACK_TARGET` and none above `PACK_HOT`. Never fewer than the
    /// shards the tablets' memtable slots need (`max_slots` each), and at once up to that.
    /// Only while the store writes (`balance_min_writes` over all shards) and some shard
    /// reports busy time: an idle store, or a clock that does not move while shards work (the
    /// simulated VFS), keeps its placement.
    fn decide_pack(&mut self, now: u64) -> bool {
        let n = self.shared.shards;
        if !self.shared.balance.pack || n < 2 {
            return false;
        }
        if self.pack_k == 0 {
            self.pack_k = n;
        }
        let stale = self.shared.balance.interval_nanos.saturating_mul(3).max(1);
        let busy: f64 = self
            .shared
            .loads
            .iter()
            .map(|l| l.read_busy_fresh(now, stale).unwrap_or(0) as f64 / 1e6)
            .sum();
        let writes: u64 = self
            .shared
            .loads
            .iter()
            .map(|l| l.read_fresh(now, stale).unwrap_or(0))
            .sum();
        let writes_slow: u64 = self
            .shared
            .loads
            .iter()
            .map(|l| l.read_slow_fresh(now, stale).unwrap_or(0))
            .sum();
        if busy <= 0.0 || writes < self.shared.balance.min_writes.max(1) {
            self.pack_up = 0;
            self.pack_down = 0;
            return false;
        }
        // The active shards must hold every tablet's memtable slots: without the floor, the
        // first full shard strands the rest of the written tablets across the inactive ones.
        let floor = {
            let view = self.shared.view.load();
            let slots: usize = view
                .tablets
                .iter()
                .map(|t| view.catalog.family_ids_of(t.table).len())
                .sum();
            slots.div_ceil(self.max_slots().max(1))
        };
        let k = self.pack_k;
        let want = ((busy / PACK_TARGET).ceil() as usize)
            .max(floor)
            .clamp(1, n);
        if k < floor {
            self.pack_k = want;
            trace!(
                "shard {} balancer: active shards {k} -> {want} (slots need {floor})",
                self.id.0
            );
            self.pack_up = 0;
            self.pack_down = 0;
            return true;
        }
        // D251: an active shard whose thread is saturated (turn busy, not D248's sync term:
        // a shard whose syncs run back to back may not cool however many shards share the
        // load) grows `k` too, and holds off a shrink; so does one whose writers stall
        // (D255: their waits for room and L0 depth do ease once it sheds tablets). A
        // hot-triggered growth repeats only once the hot shard has cooled since the last
        // one (what it shed helped), or another shard is hot: a hot spot more shards cannot
        // cool (one unsplittable tablet) grows `k` once.
        let hottest = self.shared.loads[..k.min(n)]
            .iter()
            .enumerate()
            .map(|(i, l)| {
                let turn = l.read_turn_busy_fresh(now, stale).unwrap_or(0);
                let stall = l.read_stall_busy_fresh(now, stale).unwrap_or(0);
                (turn.max(stall), stall > turn, i)
            })
            .max()
            .filter(|(b, _, _)| *b as f64 / 1e6 > PACK_HOT);
        let hot = hottest.is_some();
        // A stall-driven hot shard grows `k` again while it still has tablets to shed
        // (D255): its stall fraction stays at 1 however much it sheds, so D251's cooled
        // check would hold it to one growth, and on the AX102 a load cut to 3 shards regrew
        // one shard per 20 s. The cooled check keeps holding thread-saturated hot spots.
        let view = self.shared.view.load();
        let hot_grows = hottest.is_some_and(|(b, stalled, h)| {
            let sheds = stalled
                && view
                    .tablets
                    .iter()
                    .filter(|t| usize::from(t.shard.0) == h)
                    .count()
                    > 1;
            sheds
                || self
                    .hot_grew
                    .is_none_or(|(shard, was)| shard != h || b + PACK_HOT_COOLED < was)
        });
        if !hot {
            self.hot_grew = None;
        }
        // D255: the active shards a stall-driven growth reached stay active while the store
        // writes at least half of what it did then. The shards it spread to run cheap turns
        // once their writers stop stalling, so the busy sum alone would call for a shrink,
        // the stalls would come back, and `k` would swing between the two. The floor also
        // expires after a quiet window with no active shard stalling (the cause went away
        // with the load unchanged), each expiry doubling the next window.
        let stall_terms = || {
            self.shared.loads[..k.min(n)]
                .iter()
                .map(|l| l.read_stall_busy_fresh(now, stale).unwrap_or(0))
        };
        let any_stalled = stall_terms().any(|s| s > 0);
        let stalling = stall_terms().any(|s| s >= PACK_STALLING);
        if any_stalled {
            self.floor_quiet = 0;
        } else {
            self.floor_quiet = self.floor_quiet.saturating_add(1);
        }
        let quiet_needed = self.floor_quiet_needed.max(PACK_FLOOR_QUIET);
        let stall_floor = match self.stall_floor {
            Some((floor_k, writes_then))
                if writes.saturating_mul(2) >= writes_then && self.floor_quiet < quiet_needed =>
            {
                floor_k
            }
            Some(_) => {
                self.stall_floor = None;
                if self.floor_quiet >= quiet_needed {
                    // Expired on a quiet window: a floor set again within one window is
                    // the same cause coming back and doubles the next window (below).
                    self.floor_expired_pass = self.balance_pass.max(1);
                } else {
                    // Released by a write drop: the next floor starts from the base window.
                    self.floor_expired_pass = 0;
                    self.floor_quiet_needed = PACK_FLOOR_QUIET;
                }
                0
            }
            None => 0,
        };
        let floor = floor.max(stall_floor.min(n));
        let spread = busy / k as f64 > PACK_SPREAD;
        // No shrink while the load is still ramping (D255, #734): the fast write average
        // above the slow one. The first pass of a load read its ramp as a light store.
        let ramping = writes_slow > 0 && writes as f64 > PACK_RAMP * writes_slow as f64;
        self.ramp_passes = if ramping {
            self.ramp_passes.saturating_add(1)
        } else {
            0
        };
        let ramping = ramping && self.ramp_passes <= PACK_RAMP_MAX;
        trace!(
            "shard {} balancer pass: k {k} busy {busy:.2} writes {writes} (slow {writes_slow}, ramping {ramping}, stalling {stalling}) floor {floor} (quiet {} of {}) hottest {:?} up {} down {}",
            self.id.0,
            self.floor_quiet,
            self.floor_quiet_needed.max(PACK_FLOOR_QUIET),
            hottest,
            self.pack_up,
            self.pack_down
        );
        if k < n && (spread || hot_grows) {
            self.pack_up += 1;
        } else {
            self.pack_up = 0;
        }
        if !hot && !stalling && k > floor.max(1) && busy / ((k - 1) as f64) < PACK_TARGET {
            // A ramping pass pauses the shrink count: a light pass advances it only while
            // the load is not ramping, and a hot, busy or stalling pass resets it ramping
            // or not (ctr274's review: a count carried across a bursty heavy load's bursts
            // would shrink and grow in a slow cycle; a lull whose writers wait on room is
            // no light pass).
            if !ramping {
                self.pack_down += 1;
            }
        } else {
            self.pack_down = 0;
        }
        let why = if k < floor {
            self.pack_k = want.max(floor).min(n);
            "the stall floor"
        } else if self.pack_up >= PACK_GROW_PASSES {
            self.pack_k = want.max(k + 1).min(n);
            // Whatever called for it, a growth with a hot shard counts as its growth.
            match hottest {
                Some((b, stalled, h)) => {
                    self.hot_grew = Some((h, b));
                    if stalled {
                        // A shrink that stalls writers within a minute, with the store
                        // writing twice what it did then, was wrong (the load came back):
                        // back to the count before it in one step (D255).
                        if let Some((from, at, writes_then)) = self.shrunk_from.take()
                            && self.balance_pass.saturating_sub(at) <= PACK_SHRINK_UNDO
                            && writes >= writes_then.saturating_mul(2)
                        {
                            self.pack_k = self.pack_k.max(from.min(n));
                        }
                        if self.stall_floor.is_none() {
                            // A floor set within one quiet window of the last expiry is
                            // the same cause coming back: the window doubles, up to the
                            // cap. One set later starts from the base again.
                            let since = self.balance_pass.saturating_sub(self.floor_expired_pass);
                            self.floor_quiet_needed =
                                if self.floor_expired_pass != 0 && since <= quiet_needed {
                                    (quiet_needed * 2).min(PACK_FLOOR_QUIET_MAX)
                                } else {
                                    PACK_FLOOR_QUIET
                                };
                        }
                        self.stall_floor = Some((self.pack_k, writes));
                        "a shard whose writers stall (D255)"
                    } else {
                        "a shard whose thread is saturated (D251)"
                    }
                }
                None => {
                    debug_assert!(spread);
                    "the busy sum over the active shards (D217)"
                }
            }
        } else if self.pack_down >= PACK_SHRINK_PASSES {
            self.pack_k = want.min(k - 1).max(floor.max(1));
            self.shrunk_from = Some((k, self.balance_pass, writes));
            "a busy sum low enough for one fewer (D217)"
        } else {
            return true;
        };
        trace!(
            "shard {} balancer: active shards {k} -> {} (busy {busy:.2}, floor {floor}, writes {writes}): {why}",
            self.id.0, self.pack_k
        );
        self.pack_up = 0;
        self.pack_down = 0;
        true
    }

    /// Updates whether this shard acts on write skew (D214, with hysteresis): it starts after
    /// `SKEW_PERSIST` consecutive passes above `balance_skew` times the mean (and at least
    /// `balance_min_writes`), and stops once its load falls below the stop ratio or
    /// `balance_min_writes`. Counted on every pass, dwell or not.
    fn track_skew(&mut self, now: u64) {
        let cfg = self.shared.balance;
        // The slow path (D224): skew on the slow averages, whatever the mean does.
        self.slow_run = match self.published_slow_loads(now) {
            Some(slow) => {
                let my = slow[usize::from(self.id.0)] as f64;
                let mean = slow.iter().sum::<u64>() as f64 / slow.len() as f64;
                if my >= cfg.min_writes.max(1) as f64 && my > cfg.skew * mean {
                    self.slow_run + 1
                } else {
                    0
                }
            }
            None => 0,
        };
        let Some((my, mean)) = self.shard_loads(now) else {
            self.skew_run = 0;
            self.skew_active = false;
            self.mean_falling = false;
            return;
        };
        self.mean_slow = MEAN_SLOW_ALPHA * mean + (1.0 - MEAN_SLOW_ALPHA) * self.mean_slow;
        self.mean_falling = mean < MEAN_FALL * self.mean_slow;
        let min = cfg.min_writes.max(1) as f64;
        if self.skew_active {
            if my < min || my < Self::skew_stop(cfg.skew) * mean {
                self.skew_active = false;
                self.skew_run = 0;
            }
            return;
        }
        // A pass while the mean is falling neither counts nor resets the persistence (D224):
        // under a closed-loop load it is a phase's tail, which must not trigger, and a bursty
        // load whose mean dips between bursts must still reach `SKEW_PERSIST`.
        if !self.mean_falling {
            if my >= min && my > cfg.skew * mean {
                self.skew_run += 1;
            } else {
                self.skew_run = 0;
            }
        }
        if self.skew_run >= SKEW_PERSIST {
            self.skew_active = true;
        }
    }

    /// The load ratio below which a shard acting on write skew stops (D214).
    fn skew_stop(skew: f64) -> f64 {
        1.0 + SKEW_STOP * (skew - 1.0)
    }

    /// The published write loads of the active shards (every shard without packing, D217;
    /// stale ones count as 0), or `None` with a single active shard or when this shard is not
    /// active (it only hands its tablets to the active ones).
    fn published_loads(&self, now: u64) -> Option<Vec<u64>> {
        let n = self.active_shards();
        if n < 2 || usize::from(self.id.0) >= n {
            return None;
        }
        let stale = self.shared.balance.interval_nanos.saturating_mul(3).max(1);
        Some(
            self.shared.loads[..n]
                .iter()
                .map(|slot| slot.read_fresh(now, stale).unwrap_or(0))
                .collect(),
        )
    }

    /// The published slow write loads of the active shards (D224), as
    /// [`published_loads`](Self::published_loads). A shard backing off while idle publishes
    /// rarely, so a slow load stays current for twice the longest backoff.
    fn published_slow_loads(&self, now: u64) -> Option<Vec<u64>> {
        let n = self.active_shards();
        if n < 2 || usize::from(self.id.0) >= n {
            return None;
        }
        let stale = BALANCE_IDLE_CAP_NANOS
            .max(self.shared.balance.interval_nanos)
            .saturating_mul(2);
        Some(
            self.shared.loads[..n]
                .iter()
                .map(|slot| slot.read_slow_fresh(now, stale).unwrap_or(0))
                .collect(),
        )
    }

    /// This shard's published load and the mean over the active shards.
    fn shard_loads(&self, now: u64) -> Option<(f64, f64)> {
        let loads = self.published_loads(now)?;
        let mean = loads.iter().sum::<u64>() as f64 / loads.len() as f64;
        Some((loads[usize::from(self.id.0)] as f64, mean))
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
        let last = self.shared.picker.max_levels.max(2) - 1;
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
            let Some(task) = compact::plan_full(
                tablet,
                key.1,
                &levels,
                last,
                busy,
                true,
                self.shared.picker.target_sst_bytes,
            ) else {
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
        // A cleanup passed over for its slot's backoff runs when the backoff ends, even on
        // a shard that is idle by then.
        if let Some(until) = later
            .iter()
            .filter_map(|k| self.slot_backoff.get(k).map(|&(_, until)| until))
            .min()
        {
            self.arm_compaction_retry(until, ctx);
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
                self.shared.metrics[usize::from(self.id.0)]
                    .refused_slots
                    .fetch_add(1, Ordering::Relaxed);
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
        let dwelling = self
            .skew_pass
            .is_some_and(|p| p + DWELL_PASSES > self.balance_pass);
        // Packing (D217): a shard outside the active ones (`0..k`) hands its written tablets,
        // one per dwell, to the least busy active shard with room. Cold tablets stay: they
        // cost no wakeups.
        let active = self.active_shards();
        if usize::from(self.id.0) >= active && !dwelling {
            let stale = cfg.interval_nanos.saturating_mul(3).max(1);
            let mut targets: Vec<(u64, usize)> = self.shared.loads[..active]
                .iter()
                .enumerate()
                .map(|(i, l)| (l.read_busy_fresh(now, stale).unwrap_or(0), i))
                .collect();
            targets.sort_unstable();
            let written = owned
                .iter()
                .filter(|t| self.settled(t.id))
                .filter(|t| self.loads.get(&t.id).is_some_and(|l| l.ewma >= 1.0))
                .max_by(|a, b| {
                    let w = |t: &TabletEntry| self.loads.get(&t.id).map_or(0.0, |l| l.ewma);
                    w(a).total_cmp(&w(b)).then(b.id.cmp(&a.id))
                });
            if let Some(t) = written {
                let families = view.catalog.family_ids_of(t.table).len();
                if let Some((_, to)) = targets
                    .iter()
                    .find(|(_, i)| self.slots_fit(&view, ShardId(*i as u16), families))
                {
                    return Some(TabletOpKind::Move {
                        tablet: t.id,
                        to: ShardId(*to as u16),
                    });
                }
            }
        }
        // 2. Skew across shards, on the moving averages of write load (#103; memtable bytes
        //    swing with every flush and no longer count). Only once the skew persisted
        //    (`track_skew`, D214), not during the shard's dwell after its last move, and only
        //    for tablets that dwelt here long enough. Under packing, only among the active
        //    shards (D217).
        //    The slow path (D224) acts on the slow averages instead, once per `SLOW_DWELL`:
        //    under a closed-loop load the fast averages show skew only in each phase's tail
        //    (where it doesn't count), the slow ones the shards' shares of the work.
        self.chose_slow.set(false);
        let slow_dwelling = self
            .slow_pass
            .is_some_and(|p| p + SLOW_DWELL > self.balance_pass);
        for slow in [false, true] {
            let (ready, loads) = if slow {
                // Only while the fast averages agree this shard is above the stop ratio: once
                // the fast path balanced it, the slow averages still show the old skew for a
                // while, and acting on them would overshoot.
                let agrees = self
                    .shard_loads(now)
                    .is_some_and(|(my, mean)| my > Self::skew_stop(cfg.skew) * mean);
                (
                    self.slow_run >= SKEW_PERSIST && !slow_dwelling && agrees,
                    self.published_slow_loads(now),
                )
            } else {
                (
                    self.skew_active && !self.mean_falling,
                    self.published_loads(now),
                )
            };
            let Some(writes) = loads.filter(|_| ready && !dwelling) else {
                continue;
            };
            let settled: Vec<TabletEntry> = owned
                .iter()
                .filter(|t| self.settled(t.id))
                .cloned()
                .collect();
            let tablet_writes: HashMap<TabletId, u64> = settled
                .iter()
                .map(|t| {
                    let w = self
                        .loads
                        .get(&t.id)
                        .map_or(0.0, |l| if slow { l.slow } else { l.ewma });
                    (t.id, w.round() as u64)
                })
                .collect();
            let splits_ok = |op: &TabletOpKind| match op {
                TabletOpKind::Split { tablet, .. } => {
                    owned.iter().find(|t| t.id == *tablet).is_some_and(room_for)
                }
                _ => true,
            };
            // The slow path makes one change per dwell, only past the trigger itself.
            let ratio = if slow {
                cfg.skew
            } else {
                Self::skew_stop(cfg.skew)
            };
            if let Some(op) = self.skew_op(&view, &settled, &writes, &tablet_writes, ratio, now)
                && splits_ok(&op)
                && self.claim_targets(&op, if slow { SLOW_DWELL } else { DWELL_PASSES }, now)
            {
                self.chose_slow.set(slow);
                return Some(op);
            }
        }
        // 3. Merge two adjacent cold tablets of one table that are small together.
        let cold_nanos = cfg.interval_nanos.saturating_mul(cfg.cold_passes);
        let long_ago = |(pass, clock): (u64, u64)| {
            pass + cfg.cold_passes <= self.balance_pass || clock.saturating_add(cold_nanos) <= now
        };
        let cold = |id: TabletId| {
            self.loads.get(&id).is_none_or(|l| {
                l.writes == 0
                    && l.prev_writes == 0
                    && (l.last_write == (0, 0) || long_ago(l.last_write))
            }) && long_ago((
                self.arrived.get(&id).copied().unwrap_or(0),
                self.arrived_at.get(&id).copied().unwrap_or(0),
            )) && mem_per.get(&id).copied().unwrap_or(0) == 0
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
            // Never onto a shard packing is emptying (D217).
            if left.shard == self.id || usize::from(left.shard.0) >= active {
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

    /// Claims every shard a skew change hands load to (D224), for `dwell` of their passes or
    /// as many base intervals of clock, whichever comes first (the slow path's dwell for its
    /// changes). False if another change claimed one first: the change waits for a later pass.
    /// A claim whose change is then dropped or refused simply runs out.
    fn claim_targets(&self, op: &TabletOpKind, dwell: u64, now: u64) -> bool {
        let dwell_nanos = self.shared.balance.interval_nanos.saturating_mul(dwell);
        let to: Vec<ShardId> = match op {
            TabletOpKind::Move { to, .. } => vec![*to],
            TabletOpKind::Split { owners, .. } => {
                owners.iter().copied().filter(|o| *o != self.id).collect()
            }
            _ => Vec::new(),
        };
        to.iter()
            .all(|s| self.shared.loads[usize::from(s.0)].claim(dwell, dwell_nanos, now))
    }

    /// A move or split that evens out `loads` (per shard), given this shard's tablets'
    /// shares of it (`per_tablet`), when this shard is skewed past the threshold.
    fn skew_op(
        &self,
        view: &View,
        owned: &[TabletEntry],
        loads: &[u64],
        per_tablet: &HashMap<TabletId, u64>,
        ratio: f64,
        now: u64,
    ) -> Option<TabletOpKind> {
        let min = self.shared.balance.min_writes;
        let me = usize::from(self.id.0);
        let my = loads[me];
        let total: u64 = loads.iter().sum();
        let mean = total as f64 / loads.len() as f64;
        if my < min.max(1) || (my as f64) <= ratio * mean {
            return None;
        }
        // A shard another skew change just handed load is not a target until its published
        // load shows it (D224): shards balancing at once would otherwise all shed into the
        // same cold shard.
        let open = |i: usize| self.shared.loads[i].claimable(now);
        let (cold, low) = loads
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != me && open(*i))
            .min_by_key(|(i, l)| (**l, *i))
            .map(|(i, l)| (i, *l))?;
        // With the claimed shards left out, the coldest one left may not be cold at all.
        if low as f64 >= mean {
            return None;
        }
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
        // No tablet carries an eighth of the gap, and none dominates (the hottest is below the
        // gap): many small tablets, as a full shard of equal ones (D251). Splitting one would
        // not help, and needs a slot this shard may not have: the largest that fits moves.
        if tablets.first().is_some_and(|(_, w)| *w < gap)
            && let Some((t, _)) = tablets.iter().find(|(id, w)| *w < gap && fits(*id))
        {
            return Some(TabletOpKind::Move { tablet: *t, to });
        }
        // One tablet dominates: split it over this shard and the coldest shards below the
        // mean, about `my / mean` children (D214): a shard at 1.3x the mean sheds into one
        // other shard, one carrying all the load into every shard.
        let (hot, _) = *tablets.first()?;
        let entry = owned.iter().find(|t| t.id == hot)?;
        let mut targets: Vec<(u64, usize)> = loads
            .iter()
            .enumerate()
            .filter(|(i, l)| *i != me && open(*i) && (**l as f64) < mean)
            .map(|(i, l)| (*l, i))
            .collect();
        if targets.is_empty() {
            // Every shard below the mean is claimed by another skew change (D224).
            return None;
        }
        targets.sort_unstable();
        let parts = ((my as f64 / mean.max(1.0)).round() as usize).clamp(2, targets.len() + 1);
        let mut owners = vec![self.id];
        owners.extend(
            targets
                .iter()
                .take(parts - 1)
                .map(|(_, i)| ShardId(*i as u16)),
        );
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
            .live_samples()
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
            rows.extend(l.live_samples().iter().cloned());
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
    if let Some(reads) = m.validate.as_ref().map(|v| &v.reads) {
        for r in reads {
            out.push(hash_row(r.table, &r.row));
        }
    }
    if let Some((table, row, _)) = m.predicate.as_deref() {
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
        retry: m.retry,
        map_version: m.map_version,
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
                for (level, meta) in list.iter() {
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
                        for (level, meta) in list.iter() {
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

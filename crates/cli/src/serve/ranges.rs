//! A cluster partition's range gate (D331, the per-partition structure of simp724's S2):
//! which of its key ranges a range move has fenced, moved away or not yet given it, beside
//! its [`WriteGate`](super::gate::WriteGate).
//!
//! **Derived, not driven.** Every map install derives each open partition's state from the
//! map's `moves` and the moved-away rows in the partition's own file, and swaps it in whole
//! ([`RangeGate::update`]). Nothing else changes it, so a call reads one consistent state.
//!
//! **The hot path is one load** (perf287's should-fix 3). A partition no move names, with no
//! range moved off it, holds `None`; a call tests that and goes on. Otherwise
//! [`RangeState::check`], out of line and cold, walks the keys the call names
//! ([`ServiceOp::touches`]) against the state's spans. The caller loads the state once and
//! passes it down, so an install between the test and the walk can't mix two states.
//!
//! **What a span answers** (design §3, amendment 1):
//! - **Fenced** (the source, from HANDING_OVER): a write touching it is `Fenced`, reason
//!   `RANGE_MOVING`, retry-safe; reads go on.
//! - **Moved** (the source, from HANDED_OVER, and after the move from its moved-away row):
//!   any call touching it is `WrongPartition` at this node's map version, whoever sent it: a
//!   sender holding a map at or after the hand-over routes the range to the target, so only
//!   one from before it reaches here, and its pull catches it up. The one transient: a range
//!   moved *back* here, and a sender with that newer map reaching this node before it
//!   installs it. It gets a retryable `WrongPartition` until this node catches up, as with
//!   any lagging owner.
//! - **Unserved** (the target, until CLEANING): any call touching it is
//!   `PartitionUnavailable`, retry-safe. The copy's rows are there, but the writer hasn't yet
//!   established that the source can no longer serve the range (the never-both rule).
//!
//! **Transactions** (M3, as simp724 agreed it): serve can't see a transaction's read set, so
//! a commit that writes, of a transaction whose snapshot is below the seqno of the
//! partition's latest hand-over, answers `Conflict` whatever it read (retry-safe; only
//! transactions open across a hand-over meet it). A read-only commit passes: its one
//! snapshot is consistent on its own, taken while the partition still owned the range. Don't
//! extend the rule to it. While a range is fenced, a commit that writes, of a transaction
//! whose snapshot is below the fence's seqno, is `Fenced` too: its earlier `TxnWrite`s may
//! have touched the range before the fence, and nothing tracks which.

use std::ops::{Bound, ControlFlow};
use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwapOption;
use pigeonhole::{ErrorCode, Family, Pigeonhole};

use super::cluster::{identity, map};
use super::errors::ApiError;
use super::proto::{ClusterMap, MovePhase, MoveStatus};
use super::service::{ServiceOp, Touch};

/// The moved-away rows' family in `__phdb_cluster`, and their row prefix (`moved/<move id>`).
const FAMILY: &str = "moved";
const PREFIX: &[u8] = b"moved/";
/// The serving rows' prefix (`serving/<move id>`): a target's ranges ([`read_serving`]).
const SERVING: &[u8] = b"serving/";

/// A range that moved off this partition, as its file records it once the partition takes
/// the hand-over's map. It outlives the map's move record: M3's conflicts and stale
/// forwards need it after the move is done. PR 5's writer prunes it at the later of "every
/// node holds a map at or after the hand-over" and "the transaction lifetime plus the
/// request timeout since it"; a node that never comes back keeps it. It's a plain logged
/// write, so it's in the partition's feed and reaches its replica copies, and a promoted
/// copy keeps answering for it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Moved {
    /// The move's id (`RangeMove.id`).
    pub id: String,
    pub table: String,
    pub start: Vec<u8>,
    /// `None`: to the table's end.
    pub end: Option<Vec<u8>>,
    /// The hand-over map's `(writer_term, map_version)`.
    pub term: u64,
    pub version: u64,
    /// This partition's visible seqno just before it recorded the hand-over: a transaction
    /// with a snapshot at or below it is open across it (M3). The row's own write moves the
    /// visible seqno past it.
    pub seqno: u64,
    /// When this partition recorded it, in milliseconds since the Unix epoch on its clock:
    /// the prune's time bound (the transaction lifetime plus the request timeout since).
    pub at_ms: u64,
    /// The move's `final_seqno` from the map's record (D331 PR 3b, both kinds); a serving
    /// row's (empty on a moved-away row): the move's source, and this partition's D310 feed
    /// token at
    /// `t_from` (`seqno`), taken from a feed of the table before the gate opened the range.
    /// A cluster component that starts at or before it carries a barrier there
    /// ([`super::feed::Barrier`]), and a resume that gained this partition starts there.
    pub source: String,
    pub final_seqno: u64,
    pub token: Vec<u8>,
}

/// The moved-away rows in `db`.
pub(crate) fn read_moved(db: &Pigeonhole) -> Result<Vec<Moved>, String> {
    read_rows(db, PREFIX)
}

/// The serving rows in `db`: the ranges this partition, as a move's target, took over and
/// serves (written as it takes the CLEANING map, before its gate opens them). Each holds the
/// same fields as a moved-away row; its `seqno` is the partition's visible seqno just
/// before it (`t_from`: nothing of the range landed at or below it here but feed-silent
/// copies).
///
/// **Lifetime (cli692):** a serving row gates the target's serving while its move is in the
/// map, and afterwards clears the part of an older moved-away row it covers (a move back).
/// So it lives as long as either holds: PR 5's writer prunes it only once no older
/// moved-away row here overlaps it, never on the feed's retention.
///
/// **Copies:** it's a plain logged write, so a replica copy of the target has it once it
/// has applied past `t_from`. A copy promoted (D330) while the move is in the map can't
/// happen (failover is blocked for a move's partitions until it's done); one promoted after
/// is gated by no move record, so it needs the row only to clear an older moved-away one,
/// which it applied before it.
pub(crate) fn read_serving(db: &Pigeonhole) -> Result<Vec<Moved>, String> {
    read_rows(db, SERVING)
}

/// Records serving row `m` in `db` (see [`read_serving`]).
pub(crate) fn write_serving(db: &Pigeonhole, m: &Moved) -> Result<(), String> {
    write_row(db, SERVING, m)
}

fn read_rows(db: &Pigeonhole, prefix: &[u8]) -> Result<Vec<Moved>, String> {
    let t = match db.table(identity::TABLE).and_then(|t| t.open()) {
        Ok(t) => t,
        Err(e) if e.code() == ErrorCode::TableNotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("read {}: {e}", identity::TABLE)),
    };
    if !t.families().contains(&FAMILY) {
        return Ok(Vec::new());
    }
    let rows = t
        .scan_prefix(prefix)
        .family(FAMILY)
        .iter()
        .map_err(|e| format!("read the moved-away ranges: {e}"))?;
    let mut out = Vec::new();
    for row in rows {
        let row = row.map_err(|e| format!("read the moved-away ranges: {e}"))?;
        let id = String::from_utf8_lossy(&row.key()[prefix.len()..]).into_owned();
        let bytes = |q: &[u8]| row.get(FAMILY, q).map(|c| c.value().to_vec());
        let number = |q: &[u8]| -> Result<u64, String> {
            bytes(q)
                .and_then(|b| std::str::from_utf8(&b).ok()?.parse().ok())
                .ok_or_else(|| format!("the moved-away range of move {id:?} is malformed"))
        };
        out.push(Moved {
            table: String::from_utf8(bytes(b"table").unwrap_or_default())
                .map_err(|_| format!("the moved-away range of move {id:?} is malformed"))?,
            start: bytes(b"start").unwrap_or_default(),
            end: bytes(b"end"),
            term: number(b"term")?,
            version: number(b"version")?,
            seqno: number(b"seqno")?,
            at_ms: number(b"at")?,
            source: String::from_utf8(bytes(b"source").unwrap_or_default()).unwrap_or_default(),
            final_seqno: bytes(b"final")
                .and_then(|b| std::str::from_utf8(&b).ok()?.parse().ok())
                .unwrap_or(0),
            token: bytes(b"token").unwrap_or_default(),
            id,
        });
    }
    Ok(out)
}

/// Records `m` in `db`, durably once the commit returns (the row is the gate's after a
/// restart: S rebuilds its gate from it before it serves).
pub(crate) fn write_moved(db: &Pigeonhole, m: &Moved) -> Result<(), String> {
    write_row(db, PREFIX, m)
}

fn write_row(db: &Pigeonhole, prefix: &[u8], m: &Moved) -> Result<(), String> {
    let t = db
        .table(identity::TABLE)
        .and_then(|b| b.family(FAMILY, Family::default()).create_if_missing())
        .map_err(|e| format!("open the moved-away ranges: {e}"))?;
    let row = [prefix, m.id.as_bytes()].concat();
    let mut w = t
        .mutate(&row)
        .put(FAMILY, b"table", m.table.as_bytes())
        .put(FAMILY, b"start", &m.start)
        .put(FAMILY, b"term", m.term.to_string().as_bytes())
        .put(FAMILY, b"version", m.version.to_string().as_bytes())
        .put(FAMILY, b"seqno", m.seqno.to_string().as_bytes())
        .put(FAMILY, b"at", m.at_ms.to_string().as_bytes());
    if let Some(end) = &m.end {
        w = w.put(FAMILY, b"end", end);
    }
    if m.final_seqno != 0 {
        w = w.put(FAMILY, b"final", m.final_seqno.to_string().as_bytes());
    }
    if !m.source.is_empty() {
        w = w
            .put(FAMILY, b"source", m.source.as_bytes())
            .put(FAMILY, b"token", &m.token);
    }
    w.commit()
        .map(|_| ())
        .map_err(|e| format!("record the moved-away range of move {:?}: {e}", m.id))
}

/// Writes move `id`'s fence marker (`fenced/<move id>`) in `db`, and returns the visible
/// seqno just before it: a transaction with a snapshot at or below that seqno is open
/// across the fence. The marker's own write moves the visible seqno past it, so one begun
/// after the fence on an idle partition isn't refused (simp724's B2).
pub(crate) fn mark_fence(db: &Pigeonhole, id: &str) -> Result<u64, String> {
    let t = db
        .table(identity::TABLE)
        .and_then(|b| b.family(FAMILY, Family::default()).create_if_missing())
        .map_err(|e| format!("open the moved-away ranges: {e}"))?;
    let before = db.visible_seqno();
    t.mutate(&[&b"fenced/"[..], id.as_bytes()].concat())
        .put(FAMILY, b"seqno", before.to_string().as_bytes())
        .commit()
        .map_err(|e| format!("mark move {id:?}'s fence: {e}"))?;
    Ok(before)
}

/// The clip marker's row (D331 PR 3): a restore's stamp leaves it in a restored file, and
/// the file's open clips it to the map's ranges before it serves, then removes it. Left
/// there by a clip that stopped, so the next open runs it again (cli692's F5).
const CLIP: &[u8] = b"clip";

/// Leaves the clip marker in `db` (a restore's stamp).
pub(crate) fn mark_clip(db: &Pigeonhole) -> Result<(), String> {
    let t = db
        .table(identity::TABLE)
        .and_then(|b| b.family(FAMILY, Family::default()).create_if_missing())
        .map_err(|e| format!("open the moved-away ranges: {e}"))?;
    t.mutate(CLIP)
        .put(FAMILY, b"at", db.visible_seqno().to_string().as_bytes())
        .commit()
        .map(|_| ())
        .map_err(|e| format!("mark the restored file for its clip: {e}"))
}

/// Whether `db` holds the clip marker.
pub(crate) fn clip_pending(db: &Pigeonhole) -> Result<bool, String> {
    let t = match db.table(identity::TABLE).and_then(|t| t.open()) {
        Ok(t) => t,
        Err(e) if e.code() == ErrorCode::TableNotFound => return Ok(false),
        Err(e) => return Err(format!("read {}: {e}", identity::TABLE)),
    };
    if !t.families().contains(&FAMILY) {
        return Ok(false);
    }
    t.get(CLIP, FAMILY, b"at")
        .map(|c| c.is_some())
        .map_err(|e| format!("read the clip marker: {e}"))
}

/// Removes the clip marker from `db`, once the clip is done.
pub(crate) fn clear_clip(db: &Pigeonhole) -> Result<(), String> {
    delete_row(db, CLIP)
}

/// Replaces moved-away row `old` in `db` with the pieces `pieces` of its range (a restore's
/// map gives the partition the rest), each as row `<id>~<n>`; none: just deleted.
pub(crate) fn rewrite_moved(db: &Pigeonhole, old: &Moved, pieces: &[Moved]) -> Result<(), String> {
    for p in pieces {
        write_row(db, PREFIX, p)?;
    }
    delete_row(db, &[PREFIX, old.id.as_bytes()].concat())
}

fn delete_row(db: &Pigeonhole, row: &[u8]) -> Result<(), String> {
    let t = db
        .table(identity::TABLE)
        .and_then(|b| b.family(FAMILY, Family::default()).create_if_missing())
        .map_err(|e| format!("open the moved-away ranges: {e}"))?;
    t.mutate(row)
        .delete_row()
        .commit()
        .map(|_| ())
        .map_err(|e| format!("delete {}: {e}", String::from_utf8_lossy(row)))
}

/// What a span refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rule {
    /// Writes: `Fenced` (RANGE_MOVING).
    Fenced,
    /// Everything: `WrongPartition`.
    Moved,
    /// Everything: `PartitionUnavailable`.
    Unserved,
    /// Everything: `PartitionUnavailable`. A range this partition handed away (its row says
    /// so) that the map places here again with no move giving it back (cli692's point on
    /// the PR 2 review: a forced map): its copy here may be cleaned, so serving it would
    /// answer rows as absent.
    Returned,
}

/// One gated range of one table.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Span {
    move_id: String,
    table: String,
    start: Vec<u8>,
    end: Option<Vec<u8>>,
    rule: Rule,
    /// A target's span from a move at CLEANING, waiting for its serving row: kept when the
    /// map drops the move until the row is here (simp724's S3 on PR 3a). An aborted move's
    /// span never is: it never gets a row.
    ///
    /// In memory only, so a copy that restarts after the map dropped the move, and before
    /// the row reached it, rebuilds without the wait. That's safe only through D328's
    /// freshness rule (cli692): the serving row is written at CLEANING, long before PR 5
    /// drops the record after the source's cleanup, so a copy fresh enough to serve already
    /// has the row, and a stale one sends its reads to the owner. Loosening either rule
    /// needs this one to be persisted.
    awaits_row: bool,
}

impl Span {
    /// Whether any key `touch` names may fall in the span. A superset where a bound is
    /// exclusive (an `Excluded(k)` start is taken as `k`), never a subset.
    fn overlaps(&self, touch: Touch<'_>) -> bool {
        let below_end = |k: &[u8]| self.end.as_deref().is_none_or(|e| k < e);
        match touch {
            Touch::Row(k) => self.start.as_slice() <= k && below_end(k),
            // `[p, p's successor)`: a key below the span's end starts with `p` exactly when
            // `p` is below it, and the span's start is below the successor exactly when
            // it's at or below `p`, or starts with it.
            Touch::Prefix(p) => {
                below_end(p) && (self.start.as_slice() <= p || self.start.starts_with(p))
            }
            Touch::Range { start, end } => {
                let lower = match start {
                    Bound::Unbounded => true,
                    Bound::Included(k) | Bound::Excluded(k) => below_end(k),
                };
                let upper = match end {
                    Bound::Unbounded => true,
                    Bound::Included(k) => k >= self.start.as_slice(),
                    Bound::Excluded(k) => k > self.start.as_slice(),
                };
                lower && upper
            }
        }
    }
}

/// One partition's gated ranges, derived from a map and its moved-away rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RangeState {
    partition: String,
    /// The map version it was derived at: a `WrongPartition`'s.
    map_version: u64,
    spans: Vec<Span>,
    /// The partition's visible seqno when its current fence went up: a transaction older
    /// than it may have written the range before it. 0 without a fence.
    fence_seqno: u64,
    /// The latest hand-over's seqno (M3), 0 when none.
    handover_seqno: u64,
}

/// What walks a call's keys calls for each (table or index, key) ([`ServiceOp::touches`]).
type KeyVisitor<'v> = &'v mut dyn FnMut(&str, Touch<'_>) -> ControlFlow<()>;

impl RangeState {
    /// Checks `op`, a write when `write`, against the spans: `Ok` when it touches none it
    /// may not. Out of line: only a partition a move names, or one a range moved off, gets
    /// here.
    #[cold]
    #[inline(never)]
    pub(crate) fn check(&self, op: &ServiceOp, write: bool) -> Result<(), ApiError> {
        self.check_keys(write, &mut |f| op.touches(f))
    }

    /// [`check`](Self::check) of the keys `walk` names, each as a write when `write`.
    fn check_keys(
        &self,
        write: bool,
        walk: &mut dyn FnMut(KeyVisitor<'_>) -> ControlFlow<()>,
    ) -> Result<(), ApiError> {
        let mut refused = None;
        // A touch's name is a table's or, for the index calls, an index's: no move names an
        // index's followed table while `validate_moves` refuses them (#2066 must map an
        // index to its table here when it lifts that).
        let _ = walk(&mut |table, touch| {
            for s in &self.spans {
                if s.table != table || !s.overlaps(touch) {
                    continue;
                }
                refused = match s.rule {
                    Rule::Fenced if !write => continue,
                    Rule::Fenced => Some(ApiError::range_moving(format!(
                        "partition {:?} is handing table {table:?}'s range over to another \
                         partition (move {:?}): it takes no writes to it here, and nothing \
                         was written. Retry: the range takes writes at its new partition \
                         once the hand-over's map lands",
                        self.partition, s.move_id
                    ))),
                    Rule::Moved => Some(ApiError::wrong_partition(
                        self.map_version,
                        format!(
                            "partition {:?} no longer holds this range of table {table:?} \
                             (move {:?} handed it over): route it again",
                            self.partition, s.move_id
                        ),
                    )),
                    Rule::Unserved => Some(ApiError::partition_unavailable(format!(
                        "partition {:?} doesn't serve this range of table {table:?} yet (move \
                         {:?} is handing it over): retry",
                        self.partition, s.move_id
                    ))),
                    Rule::Returned => Some(ApiError::partition_unavailable(format!(
                        "partition {:?} handed this range of table {table:?} away (move {:?}) \
                         and the cluster map gives it back with no move that copied it back: \
                         its rows here may be gone, so it isn't served. Move the range with a \
                         range move, or restore it",
                        self.partition, s.move_id
                    ))),
                };
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
        refused.map_or(Ok(()), Err)
    }

    /// A transaction's commit, with one load of the state: the rows the commit carries are
    /// checked as writes (simp724's B1: `Commit` isn't a write op, so `call` leaves it here),
    /// and then M3 (as simp724 agreed it; see the module's doc): one that writes, with its
    /// snapshot at `snapshot`, is refused when it's open across the fence or a hand-over. A
    /// read-only commit always passes.
    #[cold]
    #[inline(never)]
    pub(crate) fn commit(
        &self,
        rows: &[super::proto::RowWrites],
        snapshot: u64,
        writes: bool,
    ) -> Result<(), ApiError> {
        self.check_keys(true, &mut |f| {
            for k in rows.iter().filter_map(|r| r.row.as_ref()) {
                f(&k.table, Touch::Row(&k.row))?;
            }
            ControlFlow::Continue(())
        })?;
        if !writes {
            return Ok(());
        }
        if self.fence_seqno != 0 && snapshot <= self.fence_seqno {
            return Err(ApiError::range_moving(format!(
                "partition {:?} began handing a range over after this transaction began, and \
                 its writes may be in it: nothing was written. Retry the transaction",
                self.partition
            )));
        }
        if self.handover_seqno != 0 && snapshot <= self.handover_seqno {
            return Err(ApiError::from_code(
                ErrorCode::Conflict,
                &format!(
                    "partition {:?} handed a range over to another partition while this \
                     transaction was open: its reads there can't be validated here. Retry \
                     the transaction",
                    self.partition
                ),
            ));
        }
        Ok(())
    }
}

impl RangeState {
    /// Whether every key of `table` is in a range this partition moved away (feed1248's M3
    /// on PR 3b): its `Moved` spans cover the table from its first key on, unbroken.
    pub(crate) fn moved_whole(&self, table: &str) -> bool {
        let mut spans: Vec<&Span> = self
            .spans
            .iter()
            .filter(|s| s.rule == Rule::Moved && s.table == table)
            .collect();
        spans.sort_by(|a, b| a.start.cmp(&b.start));
        let mut reached: Vec<u8> = Vec::new();
        for s in spans {
            if s.start > reached {
                return false;
            }
            match &s.end {
                None => return true,
                Some(e) if *e > reached => reached = e.clone(),
                Some(_) => {}
            }
        }
        false
    }

    /// Whether a move into this partition still withholds a range of `table` (its target,
    /// before CLEANING or before its serving row is here): a resume that gained the
    /// partition asks again shortly rather than expiring (cli692's M1 on PR 3b).
    pub(crate) fn awaits_hand_over(&self, table: &str) -> bool {
        self.spans
            .iter()
            .any(|s| s.rule == Rule::Unserved && s.table == table)
    }

    /// Whether it fences move `id`'s range.
    pub(crate) fn fences(&self, id: &str) -> bool {
        self.spans
            .iter()
            .any(|s| s.rule == Rule::Fenced && s.move_id == id)
    }
}

/// A partition's range gate: its derived state, and its moved-away rows as its file holds
/// them (loaded when it opens, added to when it records a hand-over).
#[derive(Debug, Default)]
pub(crate) struct RangeGate {
    state: ArcSwapOption<RangeState>,
    moved: Mutex<Vec<Moved>>,
    serving: Mutex<Vec<Moved>>,
    /// The state waits for a row the file doesn't hold yet: a replica copy's follower, which
    /// brings rows in, reloads them after each run it applies while this is set (PR 3).
    pending: std::sync::atomic::AtomicBool,
    /// The visible seqno of the file when its rows here were read ([`load_rows_at`]).
    rows_at: std::sync::atomic::AtomicU64,
    /// Bumped whenever the serving or moved-away rows or the state change (PR 3b): a running
    /// cluster component of the partition looks for a new hand-over's barrier, or for its
    /// tables having moved away whole (feed1248's M3 on PR 3b), when it moves.
    rows_version: std::sync::atomic::AtomicU64,
}

impl RangeGate {
    /// The state, `None` when nothing gates the partition: the hot path's one load (a
    /// guard, no reference count).
    #[inline]
    pub(crate) fn load(&self) -> arc_swap::Guard<Option<Arc<RangeState>>> {
        self.state.load()
    }

    fn moved(&self) -> std::sync::MutexGuard<'_, Vec<Moved>> {
        self.moved.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes the moved-away and serving rows the partition's file holds, as it opens (before
    /// it serves).
    pub(crate) fn load_rows(&self, moved: Vec<Moved>, serving: Vec<Moved>) {
        *self.moved() = moved;
        *self.serving() = serving;
        self.rows_version
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// [`load_rows`](Self::load_rows) for rows read at the file's visible seqno `visible`,
    /// unless rows read later are here already (simp724's N1 on PR 3a: a copy's follower and
    /// a map install each read and load them, and an older read mustn't land last). Whether
    /// it took them.
    pub(crate) fn load_rows_at(
        &self,
        moved: Vec<Moved>,
        serving: Vec<Moved>,
        visible: u64,
    ) -> bool {
        use std::sync::atomic::Ordering;
        let mut m = self.moved();
        if visible < self.rows_at.load(Ordering::Acquire) {
            return false;
        }
        *m = moved;
        *self.serving() = serving;
        self.rows_at.store(visible, Ordering::Release);
        self.rows_version.fetch_add(1, Ordering::Release);
        true
    }

    fn serving(&self) -> std::sync::MutexGuard<'_, Vec<Moved>> {
        self.serving.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds a serving row once it's durable in the file.
    pub(crate) fn add_serving(&self, row: Moved) {
        let mut serving = self.serving();
        serving.retain(|m| m.id != row.id);
        serving.push(row);
        self.rows_version
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// The rows' and state's version ([`rows_version`]): one load.
    #[inline]
    pub(crate) fn rows_version(&self) -> u64 {
        self.rows_version.load(std::sync::atomic::Ordering::Acquire)
    }

    /// The moved-away rows the file holds, for a cluster component of a table moved away
    /// whole (PR 3b).
    pub(crate) fn moved_rows(&self) -> Vec<Moved> {
        self.moved().clone()
    }

    /// The serving rows the file holds, for a cluster component's start and barriers.
    pub(crate) fn serving_rows(&self) -> Vec<Moved> {
        self.serving().clone()
    }

    /// Whether the file records that it serves move `id`'s range.
    pub(crate) fn has_serving(&self, id: &str) -> bool {
        self.serving().iter().any(|m| m.id == id)
    }

    /// Adds a moved-away row once it's durable in the file.
    pub(crate) fn add_moved(&self, row: Moved) {
        let mut moved = self.moved();
        moved.retain(|m| m.id != row.id);
        moved.push(row);
        self.rows_version
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// Whether the file already records move `id`'s hand-over.
    pub(crate) fn has_moved(&self, id: &str) -> bool {
        self.moved().iter().any(|m| m.id == id)
    }

    /// Derives partition `partition`'s state from `map` and its moved-away rows, and swaps
    /// it in; `visible` is the partition's visible seqno now, a new fence's seqno.
    pub(crate) fn update(&self, map: &ClusterMap, partition: &str, visible: u64) {
        let prev = self.state.load_full();
        let (next, pending) = derive(
            map,
            partition,
            &self.moved(),
            &self.serving(),
            prev.as_deref(),
            visible,
        );
        self.state.store(next.map(Arc::new));
        self.pending
            .store(pending, std::sync::atomic::Ordering::Release);
        self.rows_version
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// Whether the state waits for a row the file doesn't hold yet (see `pending`).
    pub(crate) fn pending(&self) -> bool {
        self.pending.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// `partition`'s state under `map`, with its moved-away rows `moved` and serving rows
/// `serving`; `prev` its state before (a fence keeps its seqno while it stands), `visible`
/// its visible seqno now. `None` when nothing gates it; and whether it waits for a row the
/// file doesn't hold yet (a replica copy whose follower hasn't applied it: a target's
/// serving row at CLEANING, or a source's moved-away row once the map dropped its move,
/// whose span it keeps from `prev` meanwhile).
fn derive(
    map: &ClusterMap,
    partition: &str,
    moved: &[Moved],
    serving: &[Moved],
    prev: Option<&RangeState>,
    visible: u64,
) -> (Option<RangeState>, bool) {
    let mut spans = Vec::new();
    let mut handover_seqno = 0;
    let mut pending = false;
    let span = |m: &super::proto::RangeMove, rule| {
        let (start, end) = map::move_range(m).ok()?;
        Some(Span {
            move_id: m.id.clone(),
            table: m.table.clone(),
            start,
            end,
            rule,
            awaits_row: false,
        })
    };
    for m in &map.moves {
        // A NODE move (D332 PR 2) gates no range: the partition's own state does.
        if map::node_move(m) {
            continue;
        }
        let phase = map::phase(m);
        if m.from == partition {
            match phase {
                MovePhase::HandingOver => spans.extend(span(m, Rule::Fenced)),
                MovePhase::HandedOver | MovePhase::Cleaning => {
                    spans.extend(span(m, Rule::Moved));
                    // Until the row records its seqno, every transaction open now is open
                    // across the hand-over.
                    let seqno = moved
                        .iter()
                        .find(|r| r.id == m.id)
                        .map_or(u64::MAX, |r| r.seqno);
                    handover_seqno = handover_seqno.max(seqno);
                }
                _ => {}
            }
        } else if m.to == partition
            && (phase < MovePhase::Cleaning || !serving.iter().any(|r| r.id == m.id))
        {
            // Served from CLEANING on, once its serving row is in the file (written before
            // the gate opens it, so nothing of the range lands here below its `t_from`).
            let cleaning = phase >= MovePhase::Cleaning;
            spans.extend(span(m, Rule::Unserved).map(|s| Span {
                awaits_row: cleaning,
                ..s
            }));
            pending |= cleaning;
        }
    }
    // The rows of moves the map no longer carries: each part of the range a later move
    // gave back (a newer serving row here) is served again; the rest is refused, as moved
    // away, or, where the map places it here anyway, as returned with no copy back.
    for r in moved {
        if map.moves.iter().any(|m| m.id == r.id) {
            continue;
        }
        let mut left = vec![(r.start.clone(), r.end.clone())];
        for back in serving
            .iter()
            .filter(|b| b.table == r.table && (b.term, b.version) > (r.term, r.version))
        {
            left = left
                .into_iter()
                .flat_map(|piece| subtract(&piece, &(back.start.clone(), back.end.clone())))
                .collect();
        }
        if left.is_empty() {
            continue;
        }
        handover_seqno = handover_seqno.max(r.seqno);
        let homes: Vec<(Vec<u8>, Option<Vec<u8>>)> = map
            .tables
            .iter()
            .find(|t| t.name == r.table)
            .map(|t| {
                map::table_ranges(t)
                    .into_iter()
                    .filter(|(_, _, p)| *p == partition)
                    .map(|(a, b, _)| (a, b))
                    .collect()
            })
            .unwrap_or_default();
        for piece in left {
            let mut away = vec![piece.clone()];
            for home in &homes {
                if let Some(both) = intersect(&piece, home) {
                    spans.push(Span {
                        move_id: r.id.clone(),
                        table: r.table.clone(),
                        start: both.0.clone(),
                        end: both.1.clone(),
                        rule: Rule::Returned,
                        awaits_row: false,
                    });
                    away = away.into_iter().flat_map(|a| subtract(&a, &both)).collect();
                }
            }
            spans.extend(away.into_iter().map(|(start, end)| Span {
                move_id: r.id.clone(),
                table: r.table.clone(),
                start,
                end,
                rule: Rule::Moved,
                awaits_row: false,
            }));
        }
    }
    // A move the map dropped whose row hasn't reached this file yet (a replica copy behind
    // its owner): a source's span, or a target's from CLEANING (simp724's S3), kept until
    // the row does.
    if let Some(prev) = prev {
        for s in prev.spans.iter().filter(|s| {
            !map.moves.iter().any(|m| m.id == s.move_id)
                && match s.rule {
                    Rule::Moved => !moved.iter().any(|r| r.id == s.move_id),
                    Rule::Unserved => s.awaits_row && !serving.iter().any(|r| r.id == s.move_id),
                    _ => false,
                }
        }) {
            spans.push(s.clone());
            pending = true;
        }
    }
    if spans.is_empty() {
        return (None, pending);
    }
    let fenced = |s: &RangeState| s.spans.iter().any(|x| x.rule == Rule::Fenced);
    let fence_seqno = if spans.iter().any(|s| s.rule == Rule::Fenced) {
        prev.filter(|p| fenced(p))
            .map_or(visible.max(1), |p| p.fence_seqno)
    } else {
        0
    };
    (
        Some(RangeState {
            partition: partition.to_owned(),
            map_version: map.map_version,
            spans,
            fence_seqno,
            handover_seqno,
        }),
        pending,
    )
}

/// A key range, `[start, end)`; `end` `None`: to the table's end.
pub(crate) type KeyRange = (Vec<u8>, Option<Vec<u8>>);

/// `a` and `b`'s overlap, if they overlap.
pub(crate) fn intersect(a: &KeyRange, b: &KeyRange) -> Option<KeyRange> {
    let start = a.0.clone().max(b.0.clone());
    let end = match (&a.1, &b.1) {
        (None, e) | (e, None) => e.clone(),
        (Some(x), Some(y)) => Some(x.clone().min(y.clone())),
    };
    end.as_ref()
        .is_none_or(|e| start < *e)
        .then_some((start, end))
}

/// What's left of `a` once `b` is taken out: none, one or two ranges.
pub(crate) fn subtract(a: &KeyRange, b: &KeyRange) -> Vec<KeyRange> {
    let Some(both) = intersect(a, b) else {
        return vec![a.clone()];
    };
    let mut out = Vec::new();
    if a.0 < both.0 {
        out.push((a.0.clone(), Some(both.0.clone())));
    }
    if let Some(e) = both.1
        && a.1.as_ref().is_none_or(|ae| e < *ae)
    {
        out.push((e, a.1.clone()));
    }
    out
}

/// The moves naming `partition` as `Status` reports them (`LocalPartition.moves`).
pub(crate) fn statuses(map: &ClusterMap, partition: &str) -> Vec<MoveStatus> {
    map.moves
        .iter()
        .filter(|m| m.from == partition || m.to == partition)
        .map(|m| {
            let phase = map::phase(m);
            let source = m.from == partition;
            // A NODE move (D332 PR 2) fences no range: the partition is closed on its node
            // while it copies, and serves on the target from CLEANING.
            let node = map::node_move(m);
            MoveStatus {
                id: m.id.clone(),
                table: m.table.clone(),
                phase: m.phase,
                source,
                fenced: !node && source && phase == MovePhase::HandingOver,
                serving: if node {
                    phase >= MovePhase::Cleaning
                } else if source {
                    phase < MovePhase::HandedOver
                } else {
                    phase >= MovePhase::Cleaning
                },
                // D331 PR 4b: the copy's progress and the hold's lapse, filled in by the
                // range follower and the source's copy server.
                ..MoveStatus::default()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::*;
    use crate::serve::proto::{
        self, MoveKind, Node, Partition, RangeMove, SplitKey, TablePlacement, split_key,
    };

    fn text(s: &str) -> SplitKey {
        SplitKey {
            key: Some(split_key::Key::Text(s.into())),
        }
    }

    /// `users` split at "m" over p1 and p2; a move of `[c, f)` from p1 to p2 at `phase`.
    fn map(phase: Option<MovePhase>) -> ClusterMap {
        let part = |id: &str| Partition {
            id: id.into(),
            node: "a".into(),
            epoch: 1,
            ..Default::default()
        };
        ClusterMap {
            cluster_id: "c1".into(),
            map_version: 7,
            nodes: vec![Node {
                weight: Default::default(),
                id: "a".into(),
                address: "127.0.0.1:7001".into(),
            }],
            partitions: vec![part("p1"), part("p2")],
            tables: vec![TablePlacement {
                name: "users".into(),
                splits: vec![text("m")],
                partitions: vec!["p1".into(), "p2".into()],
                ..Default::default()
            }],
            moves: phase
                .map(|p| RangeMove {
                    id: "mv1".into(),
                    kind: MoveKind::Range as i32,
                    table: "users".into(),
                    start: Some(text("c")),
                    end: Some(text("f")),
                    from: "p1".into(),
                    to: "p2".into(),
                    phase: p as i32,
                    ..Default::default()
                })
                .into_iter()
                .collect(),
            ..Default::default()
        }
    }

    fn get(row: &str) -> ServiceOp {
        ServiceOp::Get(proto::GetRequest {
            table: "users".into(),
            row: row.to_owned().into(),
            ..Default::default()
        })
    }

    fn put(row: &str) -> ServiceOp {
        ServiceOp::Mutate(proto::MutateRequest {
            writes: Some(proto::RowWrites {
                row: Some(proto::RowKey {
                    table: "users".into(),
                    row: row.to_owned().into(),
                }),
                writes: Vec::new(),
            }),
            ..Default::default()
        })
    }

    fn gate(map: &ClusterMap, partition: &str, moved: Vec<Moved>, visible: u64) -> RangeGate {
        gate_serving(map, partition, moved, Vec::new(), visible)
    }

    fn gate_serving(
        map: &ClusterMap,
        partition: &str,
        moved: Vec<Moved>,
        serving: Vec<Moved>,
        visible: u64,
    ) -> RangeGate {
        let g = RangeGate::default();
        g.load_rows(moved, serving);
        g.update(map, partition, visible);
        g
    }

    fn code(state: &RangeState, op: &ServiceOp) -> Option<String> {
        state
            .check(op, op.writes())
            .err()
            .map(|e| e.code.to_string())
    }

    #[test]
    fn a_span_overlaps_what_may_touch_it() {
        let s = Span {
            move_id: "mv1".into(),
            table: "t".into(),
            start: b"c".to_vec(),
            end: Some(b"f".to_vec()),
            rule: Rule::Moved,
            awaits_row: false,
        };
        assert!(s.overlaps(Touch::Row(b"c")));
        assert!(s.overlaps(Touch::Row(b"e\xff")));
        assert!(!s.overlaps(Touch::Row(b"f")));
        assert!(!s.overlaps(Touch::Row(b"b")));
        // Prefixes: `b` holds keys below `c` only; `` holds every key; `d` and `ca` lie in it.
        assert!(!s.overlaps(Touch::Prefix(b"b")));
        assert!(s.overlaps(Touch::Prefix(b"")));
        assert!(s.overlaps(Touch::Prefix(b"d")));
        assert!(s.overlaps(Touch::Prefix(b"ca")));
        assert!(!s.overlaps(Touch::Prefix(b"f")));
        // A prefix the span's start falls inside: `c` itself starts with "".
        let deep = Span {
            start: b"cab".to_vec(),
            ..s.clone()
        };
        assert!(deep.overlaps(Touch::Prefix(b"ca")));
        assert!(deep.overlaps(Touch::Prefix(b"cb")));
        assert!(!deep.overlaps(Touch::Prefix(b"caa")));
        // Ranges, each bound kind; `..=c` reaches `c`, `..c` doesn't.
        let range = |start, end| Touch::Range { start, end };
        assert!(s.overlaps(range(Bound::Unbounded, Bound::Included(&b"c"[..]))));
        assert!(!s.overlaps(range(Bound::Unbounded, Bound::Excluded(&b"c"[..]))));
        assert!(s.overlaps(range(Bound::Included(&b"e"[..]), Bound::Unbounded)));
        assert!(!s.overlaps(range(Bound::Included(&b"f"[..]), Bound::Unbounded)));
        assert!(s.overlaps(range(Bound::Unbounded, Bound::Unbounded)));
        // An open-ended span.
        let open = Span { end: None, ..s };
        assert!(open.overlaps(Touch::Row(b"zzz")));
        assert!(open.overlaps(range(Bound::Excluded(&b"z"[..]), Bound::Unbounded)));
    }

    #[test]
    fn nothing_gates_a_partition_no_move_names() {
        assert!(gate(&map(None), "p1", Vec::new(), 1).load().is_none());
        // Nor while the move copies, on its source.
        assert!(
            gate(&map(Some(MovePhase::Copying)), "p1", Vec::new(), 1)
                .load()
                .is_none()
        );
    }

    #[test]
    fn each_phase_gates_the_source_and_the_target_as_the_design_says() {
        let at = |phase, p: &str| gate(&map(Some(phase)), p, Vec::new(), 10).state.load_full();
        // The target serves nothing of the range until CLEANING; its other keys go on.
        for phase in [
            MovePhase::Copying,
            MovePhase::CatchingUp,
            MovePhase::HandingOver,
            MovePhase::HandedOver,
        ] {
            let t = at(phase, "p2").expect("gated");
            assert_eq!(code(&t, &get("d")).as_deref(), Some("PartitionUnavailable"));
            assert_eq!(code(&t, &put("d")).as_deref(), Some("PartitionUnavailable"));
            assert_eq!(code(&t, &get("n")), None);
        }
        // At CLEANING it serves the range once its serving row is in its file (written as it
        // takes that map, before the gate opens it: simp724's C2).
        let t = at(MovePhase::Cleaning, "p2").expect("no serving row yet");
        assert_eq!(code(&t, &get("d")).as_deref(), Some("PartitionUnavailable"));
        let serving = Moved {
            id: "mv1".into(),
            table: "users".into(),
            start: b"c".to_vec(),
            end: Some(b"f".to_vec()),
            term: 0,
            version: 5,
            seqno: 30,
            at_ms: 0,
            ..Default::default()
        };
        let cleaning = map(Some(MovePhase::Cleaning));
        assert!(
            gate_serving(&cleaning, "p2", Vec::new(), vec![serving], 31)
                .state
                .load_full()
                .is_none(),
            "T serves it"
        );
        // HANDING_OVER: the source fences the range's writes, retry-safe; reads go on.
        let s = at(MovePhase::HandingOver, "p1").expect("gated");
        let e = s.check(&put("d"), true).unwrap_err();
        assert_eq!(e.code, "Fenced");
        assert!(e.message.contains("move \"mv1\""), "{}", e.message);
        assert_eq!(code(&s, &get("d")), None);
        assert_eq!(code(&s, &put("b")), None);
        // A batch touching the range and another key is refused whole.
        let batch = ServiceOp::Batch(proto::BatchRequest {
            rows: vec![
                proto::RowWrites {
                    row: Some(proto::RowKey {
                        table: "users".into(),
                        row: "a".into(),
                    }),
                    writes: Vec::new(),
                },
                proto::RowWrites {
                    row: Some(proto::RowKey {
                        table: "users".into(),
                        row: "e".into(),
                    }),
                    writes: Vec::new(),
                },
            ],
            ..Default::default()
        });
        assert_eq!(code(&s, &batch).as_deref(), Some("Fenced"));
        // From HANDED_OVER the source answers the range `WrongPartition` at its version.
        for phase in [MovePhase::HandedOver, MovePhase::Cleaning] {
            let s = at(phase, "p1").expect("gated");
            let e = s.check(&get("d"), false).unwrap_err();
            assert_eq!(e.code, "WrongPartition");
            assert!(e.message.contains("version 7"), "{}", e.message);
            assert_eq!(code(&s, &put("d")).as_deref(), Some("WrongPartition"));
            assert_eq!(code(&s, &get("a")), None);
        }
        // Another table's keys are never gated.
        let s = at(MovePhase::HandedOver, "p1").expect("gated");
        let other = ServiceOp::Get(proto::GetRequest {
            table: "orders".into(),
            row: "d".into(),
            ..Default::default()
        });
        assert_eq!(code(&s, &other), None);
    }

    #[test]
    fn a_moved_away_row_outlives_the_move_until_a_move_brings_the_range_home() {
        let row = |id: &str, start: &[u8], end: &[u8], version: u64| Moved {
            id: id.into(),
            table: "users".into(),
            start: start.to_vec(),
            end: Some(end.to_vec()),
            term: 0,
            version,
            seqno: 40,
            at_ms: 0,
            ..Default::default()
        };
        // The move is done and gone from the map, which gives `[c, f)` to p2.
        let mut done = map(None);
        done.tables[0] = map::handed_over(&done.tables[0], b"c", Some(b"f"), "p2");
        let s = gate(&done, "p1", vec![row("mv1", b"c", b"f", 5)], 50)
            .state
            .load_full()
            .expect("gated");
        assert_eq!(code(&s, &get("d")).as_deref(), Some("WrongPartition"));
        assert_eq!(code(&s, &get("a")), None);
        // cli692's point: a map that places the range on p1 again with no move bringing it
        // back (a forced map) doesn't make p1 serve rows it may have cleaned.
        let s = gate(&map(None), "p1", vec![row("mv1", b"c", b"f", 5)], 50)
            .state
            .load_full()
            .expect("still gated");
        let e = s.check(&get("d"), false).unwrap_err();
        assert_eq!(e.code, "PartitionUnavailable");
        assert!(
            e.message.contains("no move that copied it back"),
            "{}",
            e.message
        );
        // A later move that brought `[d, e)` home (p1's newer serving row) serves that part
        // again; the rest stays refused.
        let s = gate_serving(
            &map(None),
            "p1",
            vec![row("mv1", b"c", b"f", 5)],
            vec![row("mv2", b"d", b"e", 9)],
            50,
        )
        .state
        .load_full()
        .expect("partly gated");
        assert_eq!(code(&s, &get("d")), None);
        assert_eq!(code(&s, &get("c")).as_deref(), Some("PartitionUnavailable"));
        assert_eq!(code(&s, &get("e")).as_deref(), Some("PartitionUnavailable"));
        // A serving row from before the hand-over doesn't count.
        let s = gate_serving(
            &map(None),
            "p1",
            vec![row("mv1", b"c", b"f", 5)],
            vec![row("mv0", b"c", b"f", 3)],
            50,
        )
        .state
        .load_full()
        .expect("gated");
        assert_eq!(code(&s, &get("d")).as_deref(), Some("PartitionUnavailable"));
        // Brought home whole: ungated.
        assert!(
            gate_serving(
                &map(None),
                "p1",
                vec![row("mv1", b"c", b"f", 5)],
                vec![row("mv2", b"b", b"g", 9)],
                50,
            )
            .state
            .load_full()
            .is_none()
        );
    }

    #[test]
    fn a_copy_waits_for_the_rows_its_follower_brings() {
        // A copy of the source, whose map already dropped the move but whose file hasn't
        // got the moved-away row: it keeps the span it had, and waits for the row.
        let mut handed = map(Some(MovePhase::HandedOver));
        handed.tables[0] = map::handed_over(&handed.tables[0], b"c", Some(b"f"), "p2");
        let g = gate(&handed, "p1", Vec::new(), 10);
        let mut done = map(None);
        done.tables[0] = handed.tables[0].clone();
        g.update(&done, "p1", 11);
        assert!(g.pending(), "waits for its row");
        let s = g.state.load_full().expect("still gated");
        assert_eq!(code(&s, &get("d")).as_deref(), Some("WrongPartition"));
        // The row arrives: no longer waiting, still gated, now from the row.
        let row = Moved {
            id: "mv1".into(),
            table: "users".into(),
            start: b"c".to_vec(),
            end: Some(b"f".to_vec()),
            term: 0,
            version: 5,
            seqno: 9,
            at_ms: 0,
            ..Default::default()
        };
        g.load_rows(vec![row.clone()], Vec::new());
        g.update(&done, "p1", 12);
        assert!(!g.pending());
        let s = g.state.load_full().expect("gated");
        assert_eq!(code(&s, &get("d")).as_deref(), Some("WrongPartition"));
        // A copy of the target at CLEANING waits for its serving row, then serves.
        let cleaning = map(Some(MovePhase::Cleaning));
        let t = gate(&cleaning, "p2", Vec::new(), 10);
        assert!(t.pending());
        assert!(t.state.load_full().is_some());
        t.load_rows(
            Vec::new(),
            vec![Moved {
                id: "mv1".into(),
                ..row
            }],
        );
        t.update(&cleaning, "p2", 11);
        assert!(!t.pending());
        assert!(t.state.load_full().is_none(), "serves it");
    }

    /// simp724's S3 on PR 3a: a target's copy at CLEANING without its serving row keeps
    /// refusing the range when the map drops the move, until the row arrives; an aborted
    /// move's target span is never kept (it never gets a row).
    #[test]
    fn a_targets_copy_waits_for_its_serving_row_after_the_map_drops_the_move() {
        let g = gate(&map(Some(MovePhase::Cleaning)), "p2", Vec::new(), 10);
        let mut done = map(None);
        done.tables[0] = map::handed_over(&done.tables[0], b"c", Some(b"f"), "p2");
        g.update(&done, "p2", 11);
        assert!(g.pending());
        let s = g.state.load_full().expect("still refusing");
        assert_eq!(code(&s, &get("d")).as_deref(), Some("PartitionUnavailable"));
        g.load_rows(
            Vec::new(),
            vec![Moved {
                id: "mv1".into(),
                table: "users".into(),
                start: b"c".to_vec(),
                end: Some(b"f".to_vec()),
                term: 0,
                version: 5,
                seqno: 9,
                at_ms: 0,
                ..Default::default()
            }],
        );
        g.update(&done, "p2", 12);
        assert!(!g.pending());
        assert!(g.state.load_full().is_none(), "serves it");
        // Aborted at COPYING: the map drops it, and nothing waits.
        let g = gate(&map(Some(MovePhase::Copying)), "p2", Vec::new(), 10);
        g.update(&map(None), "p2", 11);
        assert!(!g.pending());
        assert!(g.state.load_full().is_none());
    }

    #[test]
    fn ranges_intersect_and_subtract() {
        let r = |a: &[u8], b: Option<&[u8]>| (a.to_vec(), b.map(<[u8]>::to_vec));
        assert_eq!(
            intersect(&r(b"c", Some(b"f")), &r(b"d", None)),
            Some(r(b"d", Some(b"f")))
        );
        assert_eq!(intersect(&r(b"c", Some(b"f")), &r(b"f", None)), None);
        assert_eq!(
            subtract(&r(b"c", Some(b"f")), &r(b"d", Some(b"e"))),
            [r(b"c", Some(b"d")), r(b"e", Some(b"f"))]
        );
        assert_eq!(
            subtract(&r(b"c", None), &r(b"d", None)),
            [r(b"c", Some(b"d"))]
        );
        assert!(subtract(&r(b"c", Some(b"f")), &r(b"", None)).is_empty());
        assert_eq!(
            subtract(&r(b"c", Some(b"f")), &r(b"x", None)),
            [r(b"c", Some(b"f"))]
        );
    }

    #[test]
    fn transactions_open_across_a_fence_or_a_hand_over_must_retry() {
        // The fence's seqno is the partition's when it went up, and stays while it stands.
        let g = gate(&map(Some(MovePhase::HandingOver)), "p1", Vec::new(), 20);
        g.update(&map(Some(MovePhase::HandingOver)), "p1", 99);
        let s = g.state.load_full().expect("gated");
        // At or below the fence's seqno: open across it (simp724's B2); the marker's write
        // moves the visible seqno past it, so a transaction begun after is above.
        assert_eq!(s.commit(&[], 20, true).unwrap_err().code, "Fenced");
        assert_eq!(s.commit(&[], 19, true).unwrap_err().code, "Fenced");
        s.commit(&[], 21, true).unwrap();
        // A read-only commit always passes: one snapshot is consistent on its own.
        s.commit(&[], 1, false).unwrap();
        // The rows a commit carries are writes (simp724's B1): into the range, `Fenced`,
        // whatever the snapshot; elsewhere, fine.
        let carried = |r: &str| proto::RowWrites {
            row: Some(proto::RowKey {
                table: "users".into(),
                row: r.to_owned().into(),
            }),
            writes: Vec::new(),
        };
        assert_eq!(
            s.commit(&[carried("d")], 99, true).unwrap_err().code,
            "Fenced"
        );
        s.commit(&[carried("a")], 99, true).unwrap();
        // After the hand-over, before the row records its seqno, every older transaction
        // conflicts; then those below the row's seqno do.
        let s = gate(&map(Some(MovePhase::HandedOver)), "p1", Vec::new(), 30)
            .state
            .load_full()
            .expect("gated");
        assert_eq!(s.commit(&[], 1_000, true).unwrap_err().code, "Conflict");
        let row = Moved {
            id: "mv1".into(),
            table: "users".into(),
            start: b"c".to_vec(),
            end: Some(b"f".to_vec()),
            term: 0,
            version: 7,
            seqno: 40,
            at_ms: 0,
            ..Default::default()
        };
        let s = gate(&map(Some(MovePhase::HandedOver)), "p1", vec![row], 50)
            .state
            .load_full()
            .expect("gated");
        assert_eq!(s.commit(&[], 40, true).unwrap_err().code, "Conflict");
        assert_eq!(s.commit(&[], 39, true).unwrap_err().code, "Conflict");
        s.commit(&[], 41, true).unwrap();
        s.commit(&[], 39, false).unwrap();
        // Rows into the moved range: `WrongPartition`, as any call's.
        assert_eq!(
            s.commit(&[carried("d")], 41, true).unwrap_err().code,
            "WrongPartition"
        );
    }

    #[test]
    fn moved_away_rows_round_trip_through_the_file() {
        let dir = pigeonhole::doc_support::temp_dir();
        let db =
            Pigeonhole::open(dir.join("p.phdb"), pigeonhole::Options::default().shards(1)).unwrap();
        assert!(read_moved(&db).unwrap().is_empty());
        let rows = [
            Moved {
                id: "mv1".into(),
                table: "users".into(),
                start: b"c".to_vec(),
                end: Some(b"f\xff".to_vec()),
                term: 2,
                version: 7,
                seqno: 40,
                at_ms: 0,
                ..Default::default()
            },
            Moved {
                id: "mv2".into(),
                table: "users".into(),
                start: Vec::new(),
                end: None,
                term: 0,
                version: 9,
                seqno: 41,
                at_ms: 0,
                ..Default::default()
            },
        ];
        for r in &rows {
            write_moved(&db, r).unwrap();
        }
        assert_eq!(read_moved(&db).unwrap(), rows);
    }

    #[test]
    fn status_reports_each_side_of_a_move() {
        let s = statuses(&map(Some(MovePhase::HandingOver)), "p1");
        assert_eq!(s.len(), 1);
        assert!(s[0].source && s[0].fenced && s[0].serving);
        let t = statuses(&map(Some(MovePhase::HandingOver)), "p2");
        assert!(!t[0].source && !t[0].fenced && !t[0].serving);
        let t = statuses(&map(Some(MovePhase::Cleaning)), "p2");
        assert!(
            t[0].serving,
            "the phase allows it (the gate also waits for its row)"
        );
        let s = statuses(&map(Some(MovePhase::Cleaning)), "p1");
        assert!(!s[0].serving);
    }
}

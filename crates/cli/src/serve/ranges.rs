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

/// A range that moved off this partition, as its file records it once the partition takes
/// the hand-over's map. It outlives the map's move record: M3's conflicts and stale
/// forwards need it after the move is done. PR 5's writer prunes it at the later of "every
/// node holds a map at or after the hand-over" and "the transaction lifetime plus the
/// request timeout since it"; a node that never comes back keeps it. It's a plain logged
/// write, so it's in the partition's feed and reaches its replica copies, and a promoted
/// copy keeps answering for it.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// This partition's visible seqno when it recorded the hand-over: a transaction with an
    /// older snapshot is open across it (M3).
    pub seqno: u64,
}

/// The moved-away rows in `db`.
pub(crate) fn read_moved(db: &Pigeonhole) -> Result<Vec<Moved>, String> {
    let t = match db.table(identity::TABLE).and_then(|t| t.open()) {
        Ok(t) => t,
        Err(e) if e.code() == ErrorCode::TableNotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("read {}: {e}", identity::TABLE)),
    };
    if !t.families().contains(&FAMILY) {
        return Ok(Vec::new());
    }
    let rows = t
        .scan_prefix(PREFIX)
        .family(FAMILY)
        .iter()
        .map_err(|e| format!("read the moved-away ranges: {e}"))?;
    let mut out = Vec::new();
    for row in rows {
        let row = row.map_err(|e| format!("read the moved-away ranges: {e}"))?;
        let id = String::from_utf8_lossy(&row.key()[PREFIX.len()..]).into_owned();
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
            id,
        });
    }
    Ok(out)
}

/// Records `m` in `db`, durably once the commit returns (the row is the gate's after a
/// restart: S rebuilds its gate from it before it serves).
pub(crate) fn write_moved(db: &Pigeonhole, m: &Moved) -> Result<(), String> {
    let t = db
        .table(identity::TABLE)
        .and_then(|b| b.family(FAMILY, Family::default()).create_if_missing())
        .map_err(|e| format!("open the moved-away ranges: {e}"))?;
    let row = [PREFIX, m.id.as_bytes()].concat();
    let mut w = t
        .mutate(&row)
        .put(FAMILY, b"table", m.table.as_bytes())
        .put(FAMILY, b"start", &m.start)
        .put(FAMILY, b"term", m.term.to_string().as_bytes())
        .put(FAMILY, b"version", m.version.to_string().as_bytes())
        .put(FAMILY, b"seqno", m.seqno.to_string().as_bytes());
    if let Some(end) = &m.end {
        w = w.put(FAMILY, b"end", end);
    }
    w.commit()
        .map(|_| ())
        .map_err(|e| format!("record the moved-away range of move {:?}: {e}", m.id))
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
}

/// One gated range of one table.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Span {
    move_id: String,
    table: String,
    start: Vec<u8>,
    end: Option<Vec<u8>>,
    rule: Rule,
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

impl RangeState {
    /// Checks `op`, a write when `write`, against the spans: `Ok` when it touches none it
    /// may not. Out of line: only a partition a move names, or one a range moved off, gets
    /// here.
    #[cold]
    #[inline(never)]
    pub(crate) fn check(&self, op: &ServiceOp, write: bool) -> Result<(), ApiError> {
        let mut refused = None;
        let _ = op.touches(&mut |table, touch| {
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
                };
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
        refused.map_or(Ok(()), Err)
    }

    /// A transaction's commit (M3, as simp724 agreed it; see the module's doc): one that
    /// writes, with its snapshot at `snapshot`, is refused when it's open across the
    /// fence or a hand-over. A read-only commit always passes.
    #[cold]
    #[inline(never)]
    pub(crate) fn commit(&self, snapshot: u64, writes: bool) -> Result<(), ApiError> {
        if !writes {
            return Ok(());
        }
        if self.fence_seqno != 0 && snapshot < self.fence_seqno {
            return Err(ApiError::range_moving(format!(
                "partition {:?} began handing a range over after this transaction began, and \
                 its writes may be in it: nothing was written. Retry the transaction",
                self.partition
            )));
        }
        if snapshot < self.handover_seqno {
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

/// A partition's range gate: its derived state, and its moved-away rows as its file holds
/// them (loaded when it opens, added to when it records a hand-over).
#[derive(Debug, Default)]
pub(crate) struct RangeGate {
    state: ArcSwapOption<RangeState>,
    moved: Mutex<Vec<Moved>>,
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

    /// Takes the moved-away rows the partition's file holds, as it opens (before it serves).
    pub(crate) fn load_moved(&self, rows: Vec<Moved>) {
        *self.moved() = rows;
    }

    /// Adds a moved-away row once it's durable in the file.
    pub(crate) fn add_moved(&self, row: Moved) {
        let mut moved = self.moved();
        moved.retain(|m| m.id != row.id);
        moved.push(row);
    }

    /// Whether the file already records move `id`'s hand-over.
    pub(crate) fn has_moved(&self, id: &str) -> bool {
        self.moved().iter().any(|m| m.id == id)
    }

    /// Derives partition `partition`'s state from `map` and its moved-away rows, and swaps
    /// it in; `visible` is the partition's visible seqno now, a new fence's seqno.
    pub(crate) fn update(&self, map: &ClusterMap, partition: &str, visible: u64) {
        let prev = self.state.load_full();
        let next = derive(map, partition, &self.moved(), prev.as_deref(), visible);
        self.state.store(next.map(Arc::new));
    }
}

/// `partition`'s state under `map`, with its moved-away rows `moved`; `prev` its state
/// before (a fence keeps its seqno while it stands), `visible` its visible seqno now.
/// `None` when nothing gates it.
fn derive(
    map: &ClusterMap,
    partition: &str,
    moved: &[Moved],
    prev: Option<&RangeState>,
    visible: u64,
) -> Option<RangeState> {
    let mut spans = Vec::new();
    let mut handover_seqno = 0;
    let span = |m: &super::proto::RangeMove, rule| {
        let (start, end) = map::move_range(m).ok()?;
        Some(Span {
            move_id: m.id.clone(),
            table: m.table.clone(),
            start,
            end,
            rule,
        })
    };
    for m in &map.moves {
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
        } else if m.to == partition && phase < MovePhase::Cleaning {
            spans.extend(span(m, Rule::Unserved));
        }
    }
    // The rows of moves the map no longer carries, unless the map gives the range back to
    // this partition (a later move brought it home).
    for r in moved {
        if map.moves.iter().any(|m| m.id == r.id) {
            continue;
        }
        let home = map
            .tables
            .iter()
            .find(|t| t.name == r.table)
            .is_some_and(|t| map::inside(t, &r.start, r.end.as_deref(), partition));
        if home {
            continue;
        }
        spans.push(Span {
            move_id: r.id.clone(),
            table: r.table.clone(),
            start: r.start.clone(),
            end: r.end.clone(),
            rule: Rule::Moved,
        });
        handover_seqno = handover_seqno.max(r.seqno);
    }
    if spans.is_empty() {
        return None;
    }
    let fenced = |s: &RangeState| s.spans.iter().any(|x| x.rule == Rule::Fenced);
    let fence_seqno = if spans.iter().any(|s| s.rule == Rule::Fenced) {
        prev.filter(|p| fenced(p))
            .map_or(visible.max(1), |p| p.fence_seqno)
    } else {
        0
    };
    Some(RangeState {
        partition: partition.to_owned(),
        map_version: map.map_version,
        spans,
        fence_seqno,
        handover_seqno,
    })
}

/// The moves naming `partition` as `Status` reports them (`LocalPartition.moves`).
pub(crate) fn statuses(map: &ClusterMap, partition: &str) -> Vec<MoveStatus> {
    map.moves
        .iter()
        .filter(|m| m.from == partition || m.to == partition)
        .map(|m| {
            let phase = map::phase(m);
            let source = m.from == partition;
            MoveStatus {
                id: m.id.clone(),
                table: m.table.clone(),
                phase: m.phase,
                source,
                fenced: source && phase == MovePhase::HandingOver,
                serving: if source {
                    phase < MovePhase::HandedOver
                } else {
                    phase >= MovePhase::Cleaning
                },
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
        let g = RangeGate::default();
        g.load_moved(moved);
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
        assert!(at(MovePhase::Cleaning, "p2").is_none(), "T serves it");
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
    fn a_moved_away_row_outlives_the_move_until_the_range_comes_home() {
        let row = Moved {
            id: "mv1".into(),
            table: "users".into(),
            start: b"c".to_vec(),
            end: Some(b"f".to_vec()),
            term: 0,
            version: 5,
            seqno: 40,
        };
        // The move is done and gone from the map, which gives `[c, f)` to p2.
        let mut done = map(None);
        done.tables[0] = map::handed_over(&done.tables[0], b"c", Some(b"f"), "p2");
        let s = gate(&done, "p1", vec![row.clone()], 50)
            .state
            .load_full()
            .expect("gated");
        assert_eq!(code(&s, &get("d")).as_deref(), Some("WrongPartition"));
        assert_eq!(code(&s, &get("a")), None);
        // A later map that gives the range back to p1 ungates it.
        assert!(gate(&map(None), "p1", vec![row], 50).load().is_none());
    }

    #[test]
    fn transactions_open_across_a_fence_or_a_hand_over_must_retry() {
        // The fence's seqno is the partition's when it went up, and stays while it stands.
        let g = gate(&map(Some(MovePhase::HandingOver)), "p1", Vec::new(), 20);
        g.update(&map(Some(MovePhase::HandingOver)), "p1", 99);
        let s = g.state.load_full().expect("gated");
        assert_eq!(s.commit(19, true).unwrap_err().code, "Fenced");
        s.commit(20, true).unwrap();
        // A read-only commit always passes: one snapshot is consistent on its own.
        s.commit(1, false).unwrap();
        // After the hand-over, before the row records its seqno, every older transaction
        // conflicts; then those below the row's seqno do.
        let s = gate(&map(Some(MovePhase::HandedOver)), "p1", Vec::new(), 30)
            .state
            .load_full()
            .expect("gated");
        assert_eq!(s.commit(1_000, true).unwrap_err().code, "Conflict");
        let row = Moved {
            id: "mv1".into(),
            table: "users".into(),
            start: b"c".to_vec(),
            end: Some(b"f".to_vec()),
            term: 0,
            version: 7,
            seqno: 40,
        };
        let s = gate(&map(Some(MovePhase::HandedOver)), "p1", vec![row], 50)
            .state
            .load_full()
            .expect("gated");
        assert_eq!(s.commit(39, true).unwrap_err().code, "Conflict");
        s.commit(40, true).unwrap();
        s.commit(39, false).unwrap();
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
            },
            Moved {
                id: "mv2".into(),
                table: "users".into(),
                start: Vec::new(),
                end: None,
                term: 0,
                version: 9,
                seqno: 41,
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
        assert!(t[0].serving);
        let s = statuses(&map(Some(MovePhase::Cleaning)), "p1");
        assert!(!s[0].serving);
    }
}

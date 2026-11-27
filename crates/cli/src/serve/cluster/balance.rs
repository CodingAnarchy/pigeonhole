//! D332's balancer policy (PR 3, `.coord/cli692-design-1485.md` with its amendments): a pure
//! function from the cluster's partitions and nodes, their measured loads and the writer's
//! counters, to at most one action. The writer's loop gathers the inputs from `Status`
//! surveys and the map, and runs the action through the movers; nothing here does I/O.
//!
//! The rules, in priority order within a pass:
//! 1. **Split** a partition too big or too hot (`partition_max_bytes`, `partition_max_load`),
//!    at a tablet boundary that halves it, onto the coldest node holding none of its
//!    replicas. A split whose best halves leave over 80% on one side is a hot key: the range
//!    is unsplittable, and a whole move is considered instead (perf287's should-fix 1). The
//!    upper side, from the key to the end of P's range holding it, goes to a new partition
//!    named `<P>-<v>` ([`split_id`]). The loop then asks D331's `range_move::plan` whether
//!    the copy fits the source's move limits, with the moving tablets' bytes, and its copy
//!    time must also fit in the dwell ([`copy_fits_dwell`]), as a move's must.
//! 2. **Move** one partition off a node over `load_ratio` or `size_ratio` (per unit of
//!    weight, against the mean of the nodes that answered), to the coldest node: the one
//!    whose move brings both nodes closest to the mean without pushing the target over the
//!    bound; ties go to the smaller partition. Its copy must fit in the dwell at
//!    `--move-rate-bytes` (perf287's should-fix 2).
//! 3. **Merge** a partition under `partition_min_bytes` and a tenth of its node's mean load
//!    into a neighbour, one holding a range next to one of its own (only those: ai1249's S2
//!    on #2103, so placements don't fragment),
//!    if the two together stay under half of the max size and load (Amendment 3).
//!
//! Every action waits for its condition to hold `passes` consecutive passes; a node acted on
//! keeps its streak while above `1 + 0.4·(ratio − 1)` (hysteresis, D214); the partitions and
//! nodes an action touched rest for `dwell`. A partition moved whole `max_moves` times
//! within `bounce_window` is auto-pinned instead (its history's PIN entry; it expires a
//! `bounce_window` later, unless an operator's pin cleared it: simp724's S2). Splits and
//! merges don't count toward a bounce: a split keeps P's history, so it doesn't reset P's
//! count either, and the new partition starts with none (simp724's M2 on the PR 5 design).
//! Neither side of a merge split within four dwells, so a split isn't merged straight back.
//! That also keeps P from merging with any neighbour for four dwells after it splits; P's
//! SPLIT entry outlives the 8-entry history cap that long, since P rests a dwell after each
//! action (simp724).
//!
//! **Never a candidate** (simp724's S1, ai1249's (4)): a pinned partition; one in any range
//! move's record, as source or target; one with in-doubt shares; one with an old owner's tail
//! pending (D336, named in the pass's reason); for a split or merge, a
//! table in a co-location group or one an index follows. A partition holding a pinned range
//! (`TablePlacement.pinned_ranges`) never moves or merges, and never splits inside the pin
//! (ai1249's M2 on #2103). A split key lies strictly inside one of P's own ranges in the map,
//! never at a hole a range move or drop left in its file (ai1249's M1).
//!
//! Read holds and open transactions aren't inputs (simp724's M2 on #2103: `Status` doesn't
//! carry them): a move fences and closes P on its node first, so a transaction or pinned
//! snapshot open on it there ends with the old copy, as for `phdb cluster move`. A spanning
//! transaction homed on P keeps its outcome: the move copies P's files whole, history
//! included. A node that didn't answer this pass, or whose numbers are older than
//! a window, is neither a source nor a target, and its streaks reset (Amendment 1's S2).
//! The counters live in the writer's memory only, and start again with each writer (S3).

use std::collections::HashMap;

/// The balancer's bounds (`ClusterMap.balance`), each with its default where the map has 0.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Bounds {
    pub(crate) size_ratio: f64,
    pub(crate) load_ratio: f64,
    pub(crate) partition_max_bytes: u64,
    /// As a fraction of one node's capacity (a node's load is 1.0 when its CPUs or devices
    /// are saturated).
    pub(crate) partition_max_load: f64,
    pub(crate) partition_min_bytes: u64,
    pub(crate) passes: u32,
    pub(crate) dwell_ms: u64,
    pub(crate) max_moves: u32,
    pub(crate) bounce_window_ms: u64,
}

impl Default for Bounds {
    fn default() -> Self {
        let max = 32 << 30;
        Self {
            size_ratio: 1.25,
            load_ratio: 1.25,
            partition_max_bytes: max,
            partition_max_load: 0.5,
            partition_min_bytes: max / 8,
            passes: 10,
            dwell_ms: 30 * 60 * 1000,
            max_moves: 3,
            bounce_window_ms: 24 * 60 * 60 * 1000,
        }
    }
}

/// What a history entry or an action is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Move,
    Split,
    Merge,
    Pin,
}

/// A node, as the pass sees it.
#[derive(Clone, Debug)]
pub(crate) struct NodeView {
    pub(crate) id: String,
    /// `Node.weight` (0 counts as 1).
    pub(crate) weight: f64,
    /// It answered this pass, with numbers no older than a window, for every partition the
    /// map places on it.
    pub(crate) fresh: bool,
    /// Why a node that answered isn't fresh (#2158): a partition the map places on it that
    /// it gave no numbers for (closed, opening, or not open). Its sums would leave that
    /// partition out, and make the node look lighter and colder than it is.
    pub(crate) incomplete: Option<String>,
}

/// One tablet of a table in a partition (ICR 0142's `tablets()`), with its sampled load.
#[derive(Clone, Debug)]
pub(crate) struct Tablet {
    pub(crate) table: String,
    /// Its first key; the table's first tablet has the empty key.
    pub(crate) start: Vec<u8>,
    pub(crate) bytes: u64,
    pub(crate) load: f64,
}

/// A partition, as the pass sees it.
#[derive(Clone, Debug, Default)]
pub(crate) struct PartView {
    pub(crate) id: String,
    pub(crate) node: String,
    pub(crate) bytes: u64,
    /// Its share of its node's capacity (`LocalPartition.load`).
    pub(crate) load: f64,
    pub(crate) replicas: Vec<String>,
    /// `Partition.pinned`.
    pub(crate) pinned: bool,
    /// Its balancer history (`Partition.moves_history`): kind and time.
    pub(crate) history: Vec<(Kind, u64)>,
    /// It's in a range move's record, or has in-doubt shares: never a candidate (S1).
    pub(crate) busy: bool,
    /// D336: an old owner's tail of it is pending: no move, split or merge names it until the
    /// tail is replayed or discarded, and the pass says so.
    pub(crate) tail_pending: bool,
    /// A table here is in a co-location group, or followed by an index: never split or
    /// merged (ai1249's (4)).
    pub(crate) whole_only: bool,
    /// Its tablets, when the loop fetched them (a split candidate's).
    pub(crate) tablets: Vec<Tablet>,
    /// The partitions holding a range next to one of its own, in any table it holds.
    pub(crate) neighbours: Vec<String>,
    /// Its own ranges in the map, of every table it holds.
    pub(crate) ranges: Vec<KeySpan>,
    /// The tables' pinned ranges that overlap its own (`TablePlacement.pinned_ranges`).
    pub(crate) pinned_ranges: Vec<KeySpan>,
}

/// A key range of one table, `[start, end)`: `end` `None` to the table's end.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KeySpan {
    pub(crate) table: String,
    pub(crate) start: Vec<u8>,
    pub(crate) end: Option<Vec<u8>>,
}

impl KeySpan {
    /// Whether `key` lies strictly inside: above the start and below the end.
    fn strictly_holds(&self, table: &str, key: &[u8]) -> bool {
        self.table == table
            && self.start.as_slice() < key
            && self.end.as_deref().is_none_or(|e| key < e)
    }

    /// Whether this span overlaps `[start, end)` of `table` (`end` `None`: to its end).
    fn overlaps(&self, table: &str, start: &[u8], end: Option<&[u8]>) -> bool {
        self.table == table
            && end.is_none_or(|e| self.start.as_slice() < e)
            && self.end.as_deref().is_none_or(|e| start < e)
    }
}

/// An action the policy chose.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Action {
    /// A whole-partition move (PR 2's NODE record).
    Move {
        partition: String,
        from: String,
        to: String,
    },
    /// A split of `partition`'s `table` at `key`: `[key, end)`, the rest of P's range
    /// holding `key` (`end` `None`: to the table's end), goes to a new partition on `node`
    /// (D331 PR 5's split driver, which names it: [`split_id`]).
    Split {
        partition: String,
        table: String,
        key: Vec<u8>,
        end: Option<Vec<u8>>,
        node: String,
        /// The surveyed bytes of the tablets in `[key, end)`: the copy's estimate for
        /// `range_move::plan`.
        bytes: u64,
    },
    /// `from` merged into `into` (D331 PR 6).
    Merge { from: String, into: String },
    /// `partition` auto-pinned: it bounced.
    Pin { partition: String },
    /// `partition`'s auto-pin expired: the balancer unpins it.
    Unpin { partition: String },
}

/// The pass's outcome.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Decision {
    /// Take this action, for this reason.
    Act(Action, String),
    /// Nothing to do, or something to do that can't be done now, and why (for `status`; the
    /// loop writes an event only when the reason changes: simp724's M2, perf287's 1).
    Skip(String),
}

/// The writer's counters: in memory only, new with each writer (S3).
#[derive(Clone, Debug, Default)]
pub(crate) struct Counters {
    /// Consecutive passes each condition held, by key (`split:P`, `move:N`, `merge:P`).
    streak: HashMap<String, u32>,
    /// The node a move was last taken off: its streak survives above the hysteresis line.
    acting_on: Option<String>,
    /// Until when each partition or node rests after an action, in ms.
    rest: HashMap<String, u64>,
}

impl Counters {
    /// Records that `action` was taken at `now`: what it touched rests for the dwell, and
    /// the streak that led to it starts again (a node acted on keeps its own: hysteresis).
    pub(crate) fn took(&mut self, action: &Action, now: u64, b: &Bounds) {
        let until = now.saturating_add(b.dwell_ms);
        let mut rest = |k: &str| {
            self.rest.insert(k.to_owned(), until);
        };
        match action {
            Action::Move {
                partition,
                from,
                to,
            } => {
                rest(partition);
                rest(from);
                rest(to);
                self.acting_on = Some(from.clone());
            }
            Action::Split {
                partition, node, ..
            } => {
                rest(partition);
                rest(node);
                self.streak.remove(&format!("split:{partition}"));
            }
            Action::Merge { from, into } => {
                rest(from);
                rest(into);
                self.streak.remove(&format!("merge:{from}"));
            }
            Action::Pin { .. } | Action::Unpin { .. } => {}
        }
    }

    /// `key`'s streak so far: the passes in a row its condition held, before this one.
    fn streak_of(&self, key: &str) -> u32 {
        self.streak.get(key).copied().unwrap_or(0)
    }

    fn resting(&self, key: &str, now: u64) -> bool {
        self.rest.get(key).is_some_and(|&t| now < t)
    }

    /// The whole seconds `key` still rests at `now`, rounded up (#2158's skip reasons).
    fn rest_left_s(&self, key: &str, now: u64) -> u64 {
        self.rest
            .get(key)
            .map_or(0, |&t| t.saturating_sub(now).div_ceil(1000))
    }

    /// Advances `key`'s streak if `held`, else resets it; whether it reached `passes`.
    fn hold(&mut self, key: &str, held: bool, passes: u32) -> bool {
        if !held {
            self.streak.remove(key);
            return false;
        }
        let n = self.streak.entry(key.to_owned()).or_insert(0);
        *n = n.saturating_add(1);
        *n >= passes.max(1)
    }
}

/// How many times `p` was moved whole within the bounce window before `now`: splits and
/// merges don't count (simp724's M2 on the PR 5 design).
fn bounces(p: &PartView, now: u64, b: &Bounds) -> u32 {
    let since = now.saturating_sub(b.bounce_window_ms);
    let n = p
        .history
        .iter()
        .filter(|(k, at)| *k == Kind::Move && *at >= since)
        .count();
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// Whether `p`'s pin is the balancer's own (its latest PIN history entry stands: an
/// operator's pin cleared them, S2), and has expired.
fn auto_pin_expired(p: &PartView, now: u64, b: &Bounds) -> bool {
    p.pinned
        && p.history
            .iter()
            .filter(|(k, _)| *k == Kind::Pin)
            .map(|(_, at)| *at)
            .max()
            .is_some_and(|at| now >= at.saturating_add(b.bounce_window_ms))
}

/// Whether a balancer split's copy, `copy` long at its source's move rate (D331's
/// `range_move::plan`), ends within the dwell (perf287's should-fix 2 on #2103; simp724 on
/// the PR 5 split: a balancer setting, so the balancer checks it, not the split driver).
pub(crate) fn copy_fits_dwell(copy: std::time::Duration, b: &Bounds) -> bool {
    copy.as_millis() <= u128::from(b.dwell_ms)
}

/// When `p` split last, if it did.
fn split_at(p: &PartView) -> Option<u64> {
    p.history
        .iter()
        .filter(|(k, _)| *k == Kind::Split)
        .map(|(_, at)| *at)
        .max()
}

/// The id of `parent`'s new partition in a split whose move is first recorded at map
/// version `version` (the coordinator, 2026-11-22, on simp724's review): `<P>-<v>`. Never
/// reused, as map versions only grow, so an old feed token or history can't name it.
/// `None` if it isn't a valid id ([`super::map::id_ok`]: over 64 characters). The split
/// driver names the partition at the version its start lands at; the balancer checks the
/// next version's before starting one.
pub(crate) fn split_id(parent: &str, version: u64) -> Option<String> {
    Some(format!("{parent}-{version}")).filter(|id| super::map::id_ok(id))
}

/// Where a split cuts (see [`split_point`]).
#[derive(Debug)]
struct Cut {
    table: String,
    key: Vec<u8>,
    /// The end of P's range holding `key`: `[key, end)` moves.
    end: Option<Vec<u8>>,
    /// The bytes of the tablets that move.
    moving: u64,
    /// The larger side's share, for the hot-key guard.
    larger: f64,
}

/// The split of `p` that best halves it: a tablet boundary of one table (not the table's
/// first key) where the load (or the bytes, with no load measured) below is closest to half
/// of the partition's. `None` with no boundary to split at.
/// The split candidate whose tablets a pass asks for (D332 PR 5's design §1), through the
/// split loop's own gates so that one partition that can't split doesn't hold up the rest
/// (perf2's and ai1249's S1 on #2137): over `partition_max_bytes` or `partition_max_load`;
/// not pinned, busy or moving only whole; on a node that answered; not resting nor
/// bouncing; with a node to split onto (one that answered, other than its own, holding
/// none of its replicas and not resting: the split loop's `coldest`, perf2 on #2137); its
/// condition held `passes` passes counting this one, so a survey isn't wasted on a streak
/// not yet complete; and not set aside in `no_cut` (by partition, until when, in ms: one
/// whose last survey found no cut, or whose split couldn't start). Of those, the one
/// furthest over, by the larger of its two ratios.
pub(crate) fn split_candidate(
    nodes: &[NodeView],
    parts: &[PartView],
    b: &Bounds,
    counters: &Counters,
    now: u64,
    no_cut: &HashMap<String, u64>,
) -> Option<usize> {
    parts
        .iter()
        .enumerate()
        .filter(|(_, p)| {
            !p.pinned
                && !p.busy
                && !p.whole_only
                && nodes.iter().any(|n| n.fresh && n.id == p.node)
                && nodes.iter().any(|n| {
                    n.fresh
                        && n.id != p.node
                        && !p.replicas.contains(&n.id)
                        && !counters.resting(&n.id, now)
                })
                && !counters.resting(&p.id, now)
                && bounces(p, now, b) < b.max_moves
                && counters.streak_of(&format!("split:{}", p.id)) + 1 >= b.passes.max(1)
                && no_cut.get(&p.id).is_none_or(|&until| now >= until)
        })
        .map(|(i, p)| {
            let size = p.bytes as f64 / b.partition_max_bytes.max(1) as f64;
            let load = if b.partition_max_load > 0.0 {
                p.load / b.partition_max_load
            } else {
                0.0
            };
            (i, size.max(load))
        })
        .filter(|(_, over)| *over > 1.0)
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(i, _)| i)
}

/// Why surveyed `p` has no cut, whatever the pass: no tablet boundary inside its own
/// ranges outside a pin, or a best cut that leaves over 80% on one side (a hot key).
/// `None` when it has one; whether a node is free to take it is the pass's.
pub(crate) fn unsplittable(p: &PartView) -> Option<String> {
    match split_point(p) {
        None => Some(format!(
            "{} has no tablet boundary inside its own ranges, outside a pin, to split at",
            p.id
        )),
        Some(cut) if cut.larger > 0.8 => Some(format!(
            "{}'s best split leaves {:.0}% on one side: a hot key or a narrow hot range, \
             unsplittable",
            p.id,
            cut.larger * 100.0
        )),
        Some(_) => None,
    }
}

fn split_point(p: &PartView) -> Option<Cut> {
    let use_load = p.tablets.iter().any(|t| t.load > 0.0);
    let weight = |t: &Tablet| {
        if use_load { t.load } else { t.bytes as f64 }
    };
    let total: f64 = p.tablets.iter().map(weight).sum();
    if total <= 0.0 {
        return None;
    }
    let mut by_table: HashMap<&str, Vec<&Tablet>> = HashMap::new();
    for t in &p.tablets {
        by_table.entry(t.table.as_str()).or_default().push(t);
    }
    let mut best: Option<Cut> = None;
    for (table, mut ts) in by_table {
        ts.sort_by(|a, b| a.start.cmp(&b.start));
        // Everything outside this table stays on the lower side, with the table's part below
        // the key.
        let outside = total - ts.iter().map(|t| weight(t)).sum::<f64>();
        let mut below = outside;
        for t in ts.iter() {
            // Strictly inside one of P's own ranges (ai1249's M1 on #2103), and the part
            // that moves, from the key to that range's end, overlapping no pin (M2): a key
            // below a pin in the same range would move the pin too.
            let moving = p
                .ranges
                .iter()
                .find(|r| r.strictly_holds(table, &t.start))
                .map(|r| r.end.as_deref());
            let ok = moving.is_some_and(|end| {
                !p.pinned_ranges
                    .iter()
                    .any(|pin| pin.overlaps(table, &t.start, end))
            });
            if let (true, Some(end)) = (ok, moving) {
                let larger = below.max(total - below) / total;
                if best.as_ref().is_none_or(|c| larger < c.larger) {
                    let moving = ts
                        .iter()
                        .filter(|u| {
                            u.start >= t.start && end.is_none_or(|e| u.start.as_slice() < e)
                        })
                        .map(|u| u.bytes)
                        .sum();
                    best = Some(Cut {
                        table: table.to_owned(),
                        key: t.start.clone(),
                        end: end.map(<[u8]>::to_vec),
                        moving,
                        larger,
                    });
                }
            }
            below += weight(t);
        }
    }
    best
}

/// One tablet as its owner reports it (`TabletInfo`): its table, range start, bytes, write
/// load and the partition's sampled reads that fall in it.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct TabletReport {
    pub(crate) table: String,
    pub(crate) start: Vec<u8>,
    pub(crate) bytes: u64,
    pub(crate) write_load: f64,
    pub(crate) reads: u64,
}

/// The policy's tablets for a partition of `load`, from its owner's report (perf287 on the
/// D332 PR 5 design): each tablet's load is `load` times its share, the larger of its share
/// of the sampled reads and its share of the write load, renormalised so the shares sum to
/// one. A measure that's zero across the partition is left out, so a write-hot partition's
/// tablets aren't judged by reads alone, nor a read-hot one's by writes. With neither
/// measured, every load is 0 and [`split_point`] weighs by bytes.
pub(crate) fn tablet_loads(report: &[TabletReport], load: f64) -> Vec<Tablet> {
    let reads: u64 = report.iter().map(|t| t.reads).sum();
    let writes: f64 = report.iter().map(|t| t.write_load.max(0.0)).sum();
    let share = |t: &TabletReport| {
        let r = if reads > 0 {
            t.reads as f64 / reads as f64
        } else {
            0.0
        };
        let w = if writes > 0.0 {
            t.write_load.max(0.0) / writes
        } else {
            0.0
        };
        r.max(w)
    };
    let total: f64 = report.iter().map(share).sum();
    report
        .iter()
        .map(|t| Tablet {
            table: t.table.clone(),
            start: t.start.clone(),
            bytes: t.bytes,
            load: if total > 0.0 {
                load * share(t) / total
            } else {
                0.0
            },
        })
        .collect()
}

/// A node's load and size per unit of weight.
fn per_weight(n: &NodeView, parts: &[PartView]) -> (f64, f64) {
    let (mut load, mut bytes) = (0.0, 0.0);
    for p in parts.iter().filter(|p| p.node == n.id) {
        load += p.load;
        bytes += p.bytes as f64;
    }
    let w = if n.weight > 0.0 { n.weight } else { 1.0 };
    (load / w, bytes / w)
}

/// The pass: at most one action (the module doc), with `counters` advanced.
/// `move_rate` is `--move-rate-bytes` per second.
pub(crate) fn decide(
    nodes: &[NodeView],
    parts: &[PartView],
    b: &Bounds,
    counters: &mut Counters,
    now: u64,
    move_rate: u64,
) -> Decision {
    let fresh: Vec<&NodeView> = nodes.iter().filter(|n| n.fresh).collect();
    // A node missing from the pass resets its streaks (S2).
    for n in nodes.iter().filter(|n| !n.fresh) {
        counters.streak.remove(&format!("move:{}", n.id));
        for p in parts.iter().filter(|p| p.node == n.id) {
            counters.streak.remove(&format!("split:{}", p.id));
            counters.streak.remove(&format!("merge:{}", p.id));
        }
    }
    // A node that answered without numbers for one of its partitions (#2158), named when
    // nothing else is.
    let incomplete = nodes.iter().find_map(|n| n.incomplete.clone());
    if fresh.len() < 2 {
        return Decision::Skip(match incomplete {
            Some(why) => format!("fewer than two nodes have complete numbers this pass: {why}"),
            None => "fewer than two nodes answered this pass".into(),
        });
    }
    let is_fresh = |id: &str| fresh.iter().any(|n| n.id == id);
    // Auto-pins that expired come first: they're bookkeeping, not a move.
    if let Some(p) = parts.iter().find(|p| auto_pin_expired(p, now, b)) {
        return Decision::Act(
            Action::Unpin {
                partition: p.id.clone(),
            },
            "its auto-pin expired".into(),
        );
    }
    let stats: Vec<(&NodeView, f64, f64)> = fresh
        .iter()
        .map(|n| {
            let (l, s) = per_weight(n, parts);
            (*n, l, s)
        })
        .collect();
    let count = stats.len() as f64;
    let mean_load = stats.iter().map(|s| s.1).sum::<f64>() / count;
    let mean_size = stats.iter().map(|s| s.2).sum::<f64>() / count;
    let eligible = |p: &PartView| !p.pinned && !p.busy && !p.tail_pending && is_fresh(&p.node);
    // The coldest fresh node by the measure, other than `not`, holding none of `avoid`.
    let coldest = |counters: &Counters, by_load: bool, not: &str, avoid: &[String]| {
        stats
            .iter()
            .filter(|(n, _, _)| {
                n.id != not && !avoid.contains(&n.id) && !counters.resting(&n.id, now)
            })
            .min_by(|a, b| {
                let (x, y) = if by_load { (a.1, b.1) } else { (a.2, b.2) };
                x.total_cmp(&y)
            })
            .map(|(n, _, _)| n.id.clone())
    };
    let mut skip: Option<String> = None;
    // D336 (ai1249 on #2239): a partition with an old owner's tail pending is no source or
    // target of anything (`eligible`), and the pass names it, as a merge into it would be
    // refused at its first move.
    if let Some(p) = parts.iter().find(|p| p.tail_pending) {
        skip = Some(format!(
            "{} has an old owner's tail pending: no split, move or merge names it until the \
             tail is replayed or discarded",
            p.id
        ));
    }

    // 1. Splits.
    for p in parts.iter().filter(|p| eligible(p)) {
        let key = format!("split:{}", p.id);
        let too_big = p.bytes > b.partition_max_bytes;
        let too_hot = p.load > b.partition_max_load;
        let what = if too_big { "big" } else { "hot" };
        if !counters.hold(&key, too_big || too_hot, b.passes) {
            if too_big || too_hot {
                skip.get_or_insert_with(|| {
                    format!(
                        "{} is too {what}: {} of {} passes so far",
                        p.id,
                        counters.streak_of(&key),
                        b.passes.max(1)
                    )
                });
            }
            continue;
        }
        if counters.resting(&p.id, now) {
            skip.get_or_insert_with(|| {
                format!(
                    "{} is too {what}, but rests {} s more",
                    p.id,
                    counters.rest_left_s(&p.id, now)
                )
            });
            continue;
        }
        if bounces(p, now, b) >= b.max_moves {
            return Decision::Act(
                Action::Pin {
                    partition: p.id.clone(),
                },
                "bouncing".into(),
            );
        }
        if p.whole_only {
            skip.get_or_insert_with(|| {
                format!(
                    "{} is too {} but holds a grouped or indexed table: it moves only whole",
                    p.id,
                    if too_big { "big" } else { "hot" }
                )
            });
            continue;
        }
        // Only the pass's one surveyed candidate has tablets ([`split_candidate`]).
        if p.tablets.is_empty() {
            skip.get_or_insert_with(|| {
                format!(
                    "{} is too {}, but its tablets weren't surveyed this pass",
                    p.id,
                    if too_big { "big" } else { "hot" }
                )
            });
            continue;
        }
        let Some(cut) = split_point(p) else {
            skip.get_or_insert_with(|| {
                format!(
                    "{} has no tablet boundary inside its own ranges, outside a pin, to split at",
                    p.id
                )
            });
            continue;
        };
        if cut.larger > 0.8 {
            skip.get_or_insert_with(|| {
                format!(
                    "{}'s best split leaves {:.0}% on one side: a hot key or a narrow hot range, \
                     unsplittable",
                    p.id,
                    cut.larger * 100.0
                )
            });
            continue;
        }
        // The range copy must fit in the dwell, as a move's (perf287's should-fix 2).
        let Some(node) = coldest(counters, too_hot, &p.node, &p.replicas) else {
            skip.get_or_insert_with(|| {
                format!("{}'s split has no node free of its replicas", p.id)
            });
            continue;
        };
        return Decision::Act(
            Action::Split {
                partition: p.id.clone(),
                table: cut.table,
                key: cut.key,
                end: cut.end,
                node,
                bytes: cut.moving,
            },
            format!("{} is too {}", p.id, if too_big { "big" } else { "hot" }),
        );
    }

    // 2. Whole moves off a node over a bound.
    let line = |r: f64| 1.0 + 0.4 * (r - 1.0);
    for (n, load, size) in &stats {
        let lr = if mean_load > 0.0 {
            load / mean_load
        } else {
            1.0
        };
        let sr = if mean_size > 0.0 {
            size / mean_size
        } else {
            1.0
        };
        let acting = counters.acting_on.as_deref() == Some(n.id.as_str());
        let over = lr > b.load_ratio || sr > b.size_ratio;
        let by_load = lr - b.load_ratio >= sr - b.size_ratio;
        let at = || {
            format!(
                "{:.2}x the mean {}",
                if by_load { lr } else { sr },
                if by_load { "load" } else { "size" }
            )
        };
        let above_line = lr > line(b.load_ratio) || sr > line(b.size_ratio);
        let key = format!("move:{}", n.id);
        let ready = if acting && above_line {
            // Hysteresis: a node acted on keeps going while above the line.
            true
        } else {
            if acting {
                counters.acting_on = None;
            }
            counters.hold(&key, over, b.passes)
        };
        if !ready {
            if over {
                skip.get_or_insert_with(|| {
                    format!(
                        "node {} is over its bound ({}): {} of {} passes so far",
                        n.id,
                        at(),
                        counters.streak_of(&key),
                        b.passes.max(1)
                    )
                });
            }
            continue;
        }
        if counters.resting(&n.id, now) {
            skip.get_or_insert_with(|| {
                format!(
                    "node {} is over its bound ({}), but rests {} s more",
                    n.id,
                    at(),
                    counters.rest_left_s(&n.id, now)
                )
            });
            continue;
        }
        let mut best: Option<(&PartView, String, f64)> = None;
        for p in parts
            .iter()
            .filter(|p| p.node == n.id && eligible(p) && p.pinned_ranges.is_empty())
        {
            if counters.resting(&p.id, now) {
                continue;
            }
            if bounces(p, now, b) >= b.max_moves {
                return Decision::Act(
                    Action::Pin {
                        partition: p.id.clone(),
                    },
                    "bouncing".into(),
                );
            }
            // The copy must fit in the dwell (perf287's should-fix 2).
            if move_rate > 0 && p.bytes / move_rate > b.dwell_ms / 1000 {
                continue;
            }
            // The target: the coldest by the measure that the move keeps within its bound
            // on both measures (#2158). Else a load move could take it over on size, or the
            // reverse, and once the dwell ended a move would take the partition back.
            let nw = if n.weight > 0.0 { n.weight } else { 1.0 };
            let within = |after: f64, mean: f64, bound: f64| mean <= 0.0 || after / mean <= bound;
            let mut targets: Vec<&(&NodeView, f64, f64)> = stats
                .iter()
                .filter(|(t, _, _)| {
                    t.id != n.id && !p.replicas.contains(&t.id) && !counters.resting(&t.id, now)
                })
                .collect();
            targets.sort_by(|a, b| {
                if by_load {
                    a.1.total_cmp(&b.1)
                } else {
                    a.2.total_cmp(&b.2)
                }
            });
            let Some((to, to_load, to_size)) = targets.into_iter().find_map(|(t, tl, ts)| {
                let tw = if t.weight > 0.0 { t.weight } else { 1.0 };
                let (load_after, size_after) = (tl + p.load / tw, ts + p.bytes as f64 / tw);
                (within(load_after, mean_load, b.load_ratio)
                    && within(size_after, mean_size, b.size_ratio))
                .then(|| (t.id.clone(), load_after, size_after))
            }) else {
                continue;
            };
            let (from_after, to_after, mean) = if by_load {
                (load - p.load / nw, to_load, mean_load)
            } else {
                (size - p.bytes as f64 / nw, to_size, mean_size)
            };
            let distance = (from_after - mean).abs() + (to_after - mean).abs();
            let better = best.as_ref().is_none_or(|(q, _, d)| {
                distance < *d - 1e-12 || ((distance - *d).abs() <= 1e-12 && p.bytes < q.bytes)
            });
            if better {
                best = Some((p, to, distance));
            }
        }
        if let Some((p, to, _)) = best {
            return Decision::Act(
                Action::Move {
                    partition: p.id.clone(),
                    from: n.id.clone(),
                    to,
                },
                format!("node {} is at {}", n.id, at()),
            );
        }
        skip.get_or_insert_with(|| {
            format!(
                "node {} is over its bound, but no partition on it can move within the bounds",
                n.id
            )
        });
    }

    // 3. Merges of small, cold partitions.
    for p in parts
        .iter()
        .filter(|p| eligible(p) && !p.whole_only && p.pinned_ranges.is_empty())
    {
        let key = format!("merge:{}", p.id);
        let node_mean = stats.iter().find(|s| s.0.id == p.node).map_or(0.0, |s| s.1);
        // Cold: under a tenth of the mean, or under a hundredth of `partition_max_load` (0.5%
        // of a node by default) whatever the mean: in an idle cluster every load is near 0,
        // the mean too, and nothing was under a tenth of it, so nothing ever merged.
        let cold = (node_mean.max(mean_load) / 10.0).max(b.partition_max_load / 100.0);
        let small = p.bytes < b.partition_min_bytes && p.load < cold;
        if !counters.hold(&key, small, b.passes) || counters.resting(&p.id, now) {
            continue;
        }
        // Neither side within four dwells of its own split: P keeps its SPLIT entry, and its
        // new partition starts with none, so a split isn't merged straight back either way.
        // A split aborted before its hand-over keeps its entry too, so P isn't merged for
        // those four dwells either (simp724 on #2137: conservative, and kept).
        let split_lately =
            |q: &PartView| split_at(q).is_some_and(|at| now < at.saturating_add(4 * b.dwell_ms));
        if split_lately(p) {
            continue;
        }
        let fits = |q: &PartView| {
            q.id != p.id
                && !split_lately(q)
                && eligible(q)
                && !q.whole_only
                && q.pinned_ranges.is_empty()
                && !counters.resting(&q.id, now)
                && ((p.bytes + q.bytes) as f64) < b.partition_max_bytes as f64 / 2.0
                && p.load + q.load < b.partition_max_load / 2.0
        };
        let target = p
            .neighbours
            .iter()
            .filter_map(|id| parts.iter().find(|q| q.id == *id))
            .find(|q| fits(q));
        if let Some(q) = target {
            return Decision::Act(
                Action::Merge {
                    from: p.id.clone(),
                    into: q.id.clone(),
                },
                format!("{} is small and cold", p.id),
            );
        }
    }
    Decision::Skip(
        skip.or(incomplete)
            .unwrap_or_else(|| "within bounds".into()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: u64 = 60 * 1000;

    fn node(id: &str) -> NodeView {
        NodeView {
            id: id.into(),
            weight: 1.0,
            fresh: true,
            incomplete: None,
        }
    }

    fn part(id: &str, node: &str, load: f64, bytes: u64) -> PartView {
        PartView {
            id: id.into(),
            node: node.into(),
            load,
            bytes,
            ..PartView::default()
        }
    }

    /// Runs `passes` passes at 30 s apart from `t`, and the last decision.
    fn run(
        nodes: &[NodeView],
        parts: &[PartView],
        b: &Bounds,
        c: &mut Counters,
        t: &mut u64,
        passes: u32,
    ) -> Decision {
        let mut d = Decision::Skip(String::new());
        for _ in 0..passes {
            *t += 30 * 1000;
            d = decide(nodes, parts, b, c, *t, 64 << 20);
        }
        d
    }

    fn bounds() -> Bounds {
        Bounds {
            partition_min_bytes: 0,
            ..Bounds::default()
        }
    }

    /// D336 (ai1249 on #2239): a small, cold partition next to one with an old owner's tail
    /// pending isn't merged into it, nor is the tail's partition merged, moved or split; the
    /// pass says why.
    #[test]
    fn a_partition_with_a_pending_tail_is_no_merge_or_move_end() {
        let nodes = [node("a"), node("b")];
        let b = Bounds::default();
        let mut small = part("p1", "a", 0.001, 1 << 20);
        small.neighbours = vec!["p2".into()];
        // One on each node, equally loaded: no node is over its bound.
        let mut held = part("p2", "b", 0.001, 1 << 20);
        held.neighbours = vec!["p1".into()];
        held.tail_pending = true;
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(
            &nodes,
            &[small.clone(), held.clone()],
            &b,
            &mut c,
            &mut t,
            10,
        );
        let Decision::Skip(why) = d else {
            panic!("{d:?}");
        };
        assert!(why.contains("p2 has an old owner's tail pending"), "{why}");
        // Once it isn't pending, the merge goes ahead.
        held.tail_pending = false;
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &[small, held], &b, &mut c, &mut t, 10);
        assert!(matches!(d, Decision::Act(Action::Merge { .. }, _)), "{d:?}");
    }

    #[test]
    fn a_uniform_cluster_takes_no_action() {
        let nodes = [node("a"), node("b"), node("c")];
        let parts = [
            part("p1", "a", 0.2, 1 << 30),
            part("p2", "b", 0.2, 1 << 30),
            part("p3", "c", 0.2, 1 << 30),
        ];
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &bounds(), &mut c, &mut t, 30);
        assert_eq!(d, Decision::Skip("within bounds".into()));
    }

    #[test]
    fn a_hot_node_moves_one_partition_after_its_passes_then_rests() {
        let nodes = [node("a"), node("b"), node("c")];
        let parts = [
            part("p1", "a", 0.3, 1 << 30),
            // Small, so b stays within its size bound too (#2158: at 2 GiB, b would be at
            // 1.8x the mean size, and the move is refused).
            part("p2", "a", 0.2, 1 << 28),
            part("p3", "b", 0.05, 1 << 30),
            part("p4", "c", 0.1, 1 << 30),
        ];
        let b = bounds();
        let (mut c, mut t) = (Counters::default(), 0);
        // Nine passes: not yet.
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 9);
        assert!(matches!(d, Decision::Skip(_)), "{d:?}");
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 1);
        let Decision::Act(a, _) = d else {
            panic!("{d:?}")
        };
        assert_eq!(
            a,
            Action::Move {
                partition: "p2".into(),
                from: "a".into(),
                to: "b".into()
            },
            "the move that brings both closest to the mean, onto the coldest node"
        );
        // The dwell: a and b rest, so nothing within it on the same inputs.
        c.took(&a, t, &b);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 20);
        assert!(matches!(d, Decision::Skip(_)), "{d:?}");
    }

    /// Node a holds four partitions at 0.1 each; b and c 0.1 each: a is at 2x the mean, and
    /// any one partition moved off it lands within the bound.
    /// `users`' range `[start, end)`.
    fn span(start: &str, end: Option<&str>) -> KeySpan {
        KeySpan {
            table: "users".into(),
            start: start.as_bytes().to_vec(),
            end: end.map(|e| e.as_bytes().to_vec()),
        }
    }

    #[test]
    fn a_split_key_lies_strictly_inside_ps_own_ranges_and_outside_every_pin() {
        // ai1249's M1 and M2 on #2103: P holds users' [, g) and [m, ), with a hole at
        // [g, m) a range move left; its file keeps tablet edges at g and m. Neither is
        // strictly inside one of P's ranges, so the split takes t; with [s, ) pinned, none.
        let nodes = [node("a"), node("b")];
        let tab = |start: &str, load: f64| Tablet {
            table: "users".into(),
            start: start.as_bytes().to_vec(),
            bytes: 1 << 20,
            load,
        };
        let mut p = part("p1", "a", 0.8, 1 << 30);
        p.tablets = vec![tab("", 0.3), tab("g", 0.0), tab("m", 0.3), tab("t", 0.2)];
        p.ranges = vec![span("", Some("g")), span("m", None)];
        let parts = [p.clone(), part("p2", "b", 0.1, 1 << 30)];
        let b = bounds();
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        assert!(
            matches!(&d, Decision::Act(Action::Split { key, .. }, _) if key == b"t"),
            "{d:?}"
        );
        p.pinned_ranges = vec![span("s", None)];
        let parts = [p, part("p2", "b", 0.1, 1 << 30)];
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        let Decision::Skip(why) = d else {
            panic!("{d:?}")
        };
        assert!(
            why.contains("inside its own ranges, outside a pin"),
            "{why}"
        );
    }

    #[test]
    fn a_split_never_moves_a_pin_above_its_key_in_the_same_range() {
        // ai1249 on #2103 round 1: P owns users' [m, ), with [p, r) pinned. A split at n
        // would send [n, ) to the new partition, the pin with it: passed over. One at s
        // leaves the pin behind.
        let nodes = [node("a"), node("b")];
        let tab = |start: &str, load: f64| Tablet {
            table: "users".into(),
            start: start.as_bytes().to_vec(),
            bytes: 1 << 20,
            load,
        };
        let mut p = part("p1", "a", 0.8, 1 << 30);
        p.tablets = vec![tab("m", 0.4), tab("n", 0.2), tab("s", 0.2)];
        p.ranges = vec![span("m", None)];
        p.pinned_ranges = vec![span("p", Some("r"))];
        let parts = [p, part("p2", "b", 0.1, 1 << 30)];
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &bounds(), &mut c, &mut t, 10);
        assert!(
            matches!(&d, Decision::Act(Action::Split { key, .. }, _) if key == b"s"),
            "{d:?}"
        );
    }

    #[test]
    fn a_partition_holding_a_pinned_range_never_moves_or_merges() {
        // ai1249's M2 on #2103: a is hot, but every partition on it holds a pinned range.
        let nodes = [node("a"), node("b"), node("c")];
        let mut parts = spread();
        for p in parts.iter_mut().filter(|p| p.node == "a") {
            p.pinned_ranges = vec![span("", None)];
        }
        let b = bounds();
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        let Decision::Skip(why) = d else {
            panic!("{d:?}")
        };
        assert!(why.contains("no partition on it can move"), "{why}");
        // Small and cold, next to p2, but holding a pin: not merged.
        let mut p1 = part("p1", "a", 0.001, 1 << 20);
        p1.neighbours = vec!["p2".into()];
        p1.pinned_ranges = vec![span("", None)];
        let parts = [
            p1,
            part("p2", "a", 0.01, 1 << 20),
            part("p3", "b", 0.3, 8 << 30),
        ];
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes[..2], &parts, &Bounds::default(), &mut c, &mut t, 10);
        assert!(
            !matches!(&d, Decision::Act(Action::Merge { from, .. }, _) if from == "p1"),
            "{d:?}"
        );
    }

    #[test]
    fn a_split_with_no_tablets_surveyed_waits() {
        // The owner didn't answer the survey (or the pass asked another candidate's): the
        // split waits, and the status names no PR (audit120's N7 on 1.3.0).
        let nodes = [node("a"), node("b")];
        let mut p = part("p1", "a", 0.8, 1 << 30);
        p.ranges = vec![span("", None)];
        let parts = [p, part("p2", "b", 0.75, 1 << 30)];
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &bounds(), &mut c, &mut t, 10);
        let Decision::Skip(why) = d else {
            panic!("{d:?}")
        };
        assert!(why.contains("its tablets weren't surveyed"), "{why}");
    }

    #[test]
    fn a_small_partition_merges_only_into_a_neighbour() {
        // ai1249's S2 on #2103: p2 would fit, but it isn't next to p1.
        let nodes = [node("a"), node("b")];
        let parts = [
            part("p1", "a", 0.001, 1 << 20),
            part("p2", "a", 0.01, 1 << 20),
            part("p3", "b", 0.3, 8 << 30),
        ];
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &Bounds::default(), &mut c, &mut t, 10);
        assert!(
            !matches!(&d, Decision::Act(Action::Merge { .. }, _)),
            "{d:?}"
        );
    }

    fn spread() -> Vec<PartView> {
        let mut parts: Vec<PartView> = (0..4)
            .map(|i| part(&format!("p{i}"), "a", 0.1, (1 << 30) + i as u64))
            .collect();
        // Sizes even enough that load, not size, is the skew; p0 the smallest.
        parts.push(part("q1", "b", 0.1, (1 << 30) + 100));
        parts.push(part("q2", "c", 0.1, (1 << 30) + 100));
        parts
    }

    #[test]
    fn a_move_never_lands_on_a_replicas_node_or_takes_a_pinned_or_busy_partition() {
        let nodes = [node("a"), node("b"), node("c")];
        let b = bounds();
        // p0, the smallest, wins the tie; its replica on b sends it to c.
        let mut parts = spread();
        parts[0].replicas = vec!["b".into()];
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        let Decision::Act(Action::Move { partition, to, .. }, _) = d else {
            panic!("{d:?}")
        };
        assert_eq!((partition.as_str(), to.as_str()), ("p0", "c"));
        // Pinned or busy, p0 is never the one: the next smallest goes.
        for f in [
            |p: &mut PartView| p.pinned = true,
            |p: &mut PartView| p.busy = true,
        ] {
            let mut ps = spread();
            f(&mut ps[0]);
            let (mut c, mut t) = (Counters::default(), 0);
            let d = run(&nodes, &ps, &b, &mut c, &mut t, 10);
            let Decision::Act(Action::Move { partition, .. }, _) = d else {
                panic!("{d:?}")
            };
            assert_eq!(partition, "p1");
        }
        // A node with one hot partition can't be helped by a move (the target would go
        // over): that's a split's case.
        let parts = [
            part("p1", "a", 0.45, 1 << 30),
            part("p2", "b", 0.05, 1 << 30),
            part("p3", "c", 0.1, 1 << 30),
        ];
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        assert!(matches!(d, Decision::Skip(_)), "{d:?}");
    }

    #[test]
    fn a_hot_partition_splits_at_the_tablet_boundary_that_halves_it() {
        let nodes = [node("a"), node("b")];
        let mut p = part("p1", "a", 0.8, 1 << 30);
        let tab = |start: &str, load: f64| Tablet {
            table: "users".into(),
            start: start.as_bytes().to_vec(),
            bytes: 1 << 20,
            load,
        };
        p.tablets = vec![tab("", 0.2), tab("g", 0.2), tab("m", 0.3), tab("t", 0.1)];
        p.ranges = vec![span("", None)];
        let parts = [p, part("p2", "b", 0.1, 1 << 30)];
        let b = bounds();
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        assert_eq!(
            d,
            Decision::Act(
                Action::Split {
                    partition: "p1".into(),
                    table: "users".into(),
                    key: b"m".to_vec(),
                    end: None,
                    node: "b".into(),
                    bytes: 2 << 20
                },
                "p1 is too hot".into()
            )
        );
    }

    #[test]
    fn a_splits_new_id_is_the_parent_and_the_map_version_that_records_it() {
        // The coordinator, 2026-11-22: `<P>-<v>`, never reused; ids stay `[a-z0-9_-]`, 1-64
        // characters.
        assert_eq!(split_id("p3", 1742).as_deref(), Some("p3-1742"));
        // A split of a split.
        assert_eq!(split_id("p3-1742", 1800).as_deref(), Some("p3-1742-1800"));
        let at_60 = "p".repeat(60);
        assert_eq!(split_id(&at_60, 999), Some(format!("{at_60}-999")));
        // 65 characters: no id.
        assert_eq!(split_id(&at_60, 1000), None);
    }

    #[test]
    fn a_split_moves_the_rest_of_the_range_holding_its_key_and_counts_its_bytes() {
        // P holds users' [, p) and [x, ): a cut at m moves [m, p) only, and only the tablets
        // there are copied.
        let nodes = [node("a"), node("b")];
        let mut p = part("p1", "a", 0.8, 1 << 30);
        let tab = |start: &str, load: f64, bytes: u64| Tablet {
            table: "users".into(),
            start: start.as_bytes().to_vec(),
            bytes,
            load,
        };
        p.tablets = vec![
            tab("", 0.3, 1 << 20),
            tab("m", 0.3, 3 << 20),
            tab("x", 0.2, 5 << 20),
        ];
        p.ranges = vec![span("", Some("p")), span("x", None)];
        let parts = [p, part("p2", "b", 0.1, 1 << 30)];
        let b = bounds();
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        let Decision::Act(Action::Split { key, end, .. }, _) = &d else {
            panic!("{d:?}")
        };
        assert_eq!(
            (key.as_slice(), end.as_deref()),
            (&b"m"[..], Some(&b"p"[..]))
        );
        let Some(cut) = split_point(&parts[0]) else {
            panic!("no cut")
        };
        assert_eq!(cut.moving, 3 << 20);
        let Decision::Act(Action::Split { bytes, .. }, _) = &d else {
            panic!("{d:?}")
        };
        assert_eq!(*bytes, 3 << 20);
        // The dwell bounds the copy plan() reckons, to the millisecond.
        let b = bounds();
        let dwell = std::time::Duration::from_millis(b.dwell_ms);
        assert!(copy_fits_dwell(dwell, &b));
        assert!(!copy_fits_dwell(
            dwell + std::time::Duration::from_millis(1),
            &b
        ));
    }

    #[test]
    fn splits_neither_count_toward_a_bounce_nor_reset_it() {
        // simp724's M2 on the PR 5 design: P keeps its history through a split, whose entry
        // isn't a move; the new partition starts with none.
        let nodes = [node("a"), node("b"), node("c")];
        let b = bounds();
        let moves = |n: u32| (1..=u64::from(n)).map(|at| (Kind::Move, at));
        // Node a is at 1.36x the mean load (and under the mean size); p1 can move to b or c
        // within their bounds.
        let hot = |history: Vec<(Kind, u64)>| {
            let mut p1 = part("p1", "a", 0.1, 1 << 20);
            p1.history = history;
            [
                p1,
                part("p0", "a", 0.4, 1 << 29),
                part("p2", "b", 0.3, 1 << 30),
                part("p3", "c", 0.3, 1 << 30),
            ]
        };
        // max_moves − 1 moves and a split: it moves, not pinned.
        let parts = hot(moves(b.max_moves - 1).chain([(Kind::Split, 100)]).collect());
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        assert!(
            matches!(&d, Decision::Act(Action::Move { partition, .. }, _) if partition == "p1"),
            "{d:?}"
        );
        // max_moves moves and then a split: still pinned.
        let parts = hot(moves(b.max_moves).chain([(Kind::Split, 100)]).collect());
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        assert_eq!(
            d,
            Decision::Act(
                Action::Pin {
                    partition: "p1".into()
                },
                "bouncing".into()
            )
        );
    }

    #[test]
    fn a_splits_new_partition_isnt_merged_straight_back_into_its_parent() {
        // p1 split at t; p1-1 starts with no history, small and cold, next to p1 and p4.
        let nodes = [node("a"), node("b")];
        let b = Bounds::default();
        let mut p1 = part("p1", "a", 0.01, 1 << 20);
        let mut child = part("p1-1", "a", 0.001, 1 << 20);
        child.neighbours = vec!["p1".into()];
        let parts_at = |p1: &PartView, child: &PartView| {
            [
                p1.clone(),
                child.clone(),
                part("p4", "a", 0.01, 1 << 20),
                // b is over its size bound, but its one partition can't move.
                part("p3", "b", 0.3, 8 << 30),
            ]
        };
        let mut t = 0;
        p1.history = vec![(Kind::Split, t)];
        let mut c = Counters::default();
        let d = run(&nodes, &parts_at(&p1, &child), &b, &mut c, &mut t, 10);
        assert!(
            !matches!(&d, Decision::Act(Action::Merge { from, into }, _)
                if from == "p1-1" && into == "p1"),
            "{d:?}"
        );
        // Another neighbour takes it.
        child.neighbours = vec!["p1".into(), "p4".into()];
        let mut c = Counters::default();
        let d = run(&nodes, &parts_at(&p1, &child), &b, &mut c, &mut t, 10);
        assert_eq!(
            d,
            Decision::Act(
                Action::Merge {
                    from: "p1-1".into(),
                    into: "p4".into()
                },
                "p1-1 is small and cold".into()
            )
        );
    }

    #[test]
    fn a_hot_key_is_unsplittable_and_a_grouped_table_never_splits() {
        let nodes = [node("a"), node("b")];
        let mut p = part("p1", "a", 0.8, 1 << 30);
        let tab = |start: &str, load: f64| Tablet {
            table: "users".into(),
            start: start.as_bytes().to_vec(),
            bytes: 1 << 20,
            load,
        };
        p.tablets = vec![tab("", 0.01), tab("k", 0.9), tab("l", 0.01)];
        p.ranges = vec![span("", None)];
        let parts = [p.clone(), part("p2", "b", 0.75, 1 << 30)];
        let b = bounds();
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        let Decision::Skip(why) = d else {
            panic!("{d:?}")
        };
        assert!(why.contains("unsplittable"), "{why}");
        let mut grouped = p;
        grouped.tablets.clear();
        grouped.whole_only = true;
        let parts = [grouped, part("p2", "b", 0.75, 1 << 30)];
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        let Decision::Skip(why) = d else {
            panic!("{d:?}")
        };
        assert!(why.contains("moves only whole"), "{why}");
    }

    #[test]
    fn a_bouncing_partition_is_auto_pinned_and_unpinned_once_it_expires() {
        let nodes = [node("a"), node("b"), node("c")];
        let mut p1 = part("p1", "a", 0.5, 1 << 30);
        p1.history = vec![(Kind::Move, 1), (Kind::Move, 2), (Kind::Move, 3)];
        let parts = [
            p1.clone(),
            part("p2", "b", 0.05, 1 << 30),
            part("p3", "c", 0.1, 1 << 30),
        ];
        let b = bounds();
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        assert_eq!(
            d,
            Decision::Act(
                Action::Pin {
                    partition: "p1".into()
                },
                "bouncing".into()
            )
        );
        // Pinned by the balancer at t: it expires a bounce window later.
        let mut pinned = p1.clone();
        pinned.pinned = true;
        pinned.history.push((Kind::Pin, t));
        let parts = [
            pinned.clone(),
            part("p2", "b", 0.05, 1 << 30),
            part("p3", "c", 0.1, 1 << 30),
        ];
        let d = decide(&nodes, &parts, &b, &mut c, t + MIN, 64 << 20);
        assert!(matches!(d, Decision::Skip(_)), "{d:?}");
        let d = decide(&nodes, &parts, &b, &mut c, t + b.bounce_window_ms, 64 << 20);
        assert_eq!(
            d,
            Decision::Act(
                Action::Unpin {
                    partition: "p1".into()
                },
                "its auto-pin expired".into()
            )
        );
        // An operator's pin cleared the balancer's PIN entries (S2): it never expires.
        let mut operator = pinned;
        operator.history.retain(|(k, _)| *k != Kind::Pin);
        let parts = [
            operator,
            part("p2", "b", 0.05, 1 << 30),
            part("p3", "c", 0.1, 1 << 30),
        ];
        let d = decide(
            &nodes,
            &parts,
            &b,
            &mut c,
            t + 10 * b.bounce_window_ms,
            64 << 20,
        );
        assert!(matches!(d, Decision::Skip(_)), "{d:?}");
    }

    #[test]
    fn small_cold_partitions_merge_into_a_neighbour_but_not_back_after_a_split() {
        let nodes = [node("a"), node("b")];
        let b = Bounds::default();
        let mut p1 = part("p1", "a", 0.001, 1 << 20);
        p1.neighbours = vec!["p2".into()];
        let parts = [
            p1.clone(),
            part("p2", "a", 0.01, 1 << 20),
            part("p3", "b", 0.3, 8 << 30),
        ];
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        assert_eq!(
            d,
            Decision::Act(
                Action::Merge {
                    from: "p1".into(),
                    into: "p2".into()
                },
                "p1 is small and cold".into()
            )
        );
        // Split at t: not merged back within four dwells.
        let mut split = p1;
        split.history = vec![(Kind::Split, t)];
        let parts = [
            split,
            part("p2", "a", 0.01, 1 << 20),
            part("p3", "b", 0.3, 8 << 30),
        ];
        let mut c = Counters::default();
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        // p1 isn't merged away (p2 may still merge into it).
        assert!(
            !matches!(&d, Decision::Act(Action::Merge { from, .. }, _) if from == "p1"),
            "{d:?}"
        );
    }

    #[test]
    fn a_hot_key_candidate_is_set_aside_and_the_next_one_over_is_surveyed() {
        // perf2's and ai1249's S1 on #2137: p1 is the furthest over, but its load is all in
        // one tablet (a hot key); p2 is too big and splittable.
        let nodes = [node("a"), node("b"), node("c")];
        let b = Bounds {
            partition_max_bytes: 1 << 30,
            passes: 1,
            ..bounds()
        };
        let r = |start: &str, write_load: f64| TabletReport {
            table: "users".into(),
            start: start.as_bytes().to_vec(),
            bytes: 1 << 20,
            write_load,
            reads: 0,
        };
        let mut p1 = part("p1", "a", 0.9, 4 << 30);
        p1.ranges = vec![span("", Some("g"))];
        let mut p2 = part("p2", "b", 0.1, 2 << 30);
        p2.ranges = vec![span("g", None)];
        let mut parts = vec![p1, p2, part("p3", "c", 0.0, 1 << 20)];
        let (c, now) = (Counters::default(), 1_000);
        let mut no_cut = HashMap::new();
        assert_eq!(
            split_candidate(&nodes, &parts, &b, &c, now, &no_cut),
            Some(0)
        );
        parts[0].tablets = tablet_loads(&[r("", 100.0), r("c", 0.0)], 0.9);
        let why = unsplittable(&parts[0]).expect("a hot key");
        assert!(why.contains("hot key"), "{why}");
        no_cut.insert("p1".to_owned(), now + b.dwell_ms);
        // The next pass surveys p2, which splits.
        assert_eq!(
            split_candidate(&nodes, &parts, &b, &c, now + 30_000, &no_cut),
            Some(1)
        );
        parts[0].tablets.clear();
        parts[1].tablets = tablet_loads(&[r("g", 1.0), r("p", 1.0)], 0.1);
        assert_eq!(unsplittable(&parts[1]), None);
        let mut c = Counters::default();
        let d = decide(&nodes, &parts, &b, &mut c, now + 30_000, 64 << 20);
        assert!(
            matches!(&d, Decision::Act(Action::Split { partition, .. }, _) if partition == "p2"),
            "{d:?}"
        );
        // Once its time is up, p1 is surveyed again.
        assert_eq!(
            split_candidate(&nodes, &parts, &b, &c, now + b.dwell_ms, &no_cut),
            Some(0)
        );
    }

    #[test]
    fn a_resting_stale_or_unripe_partition_isnt_surveyed() {
        let mut nodes = vec![node("a"), node("b"), node("c")];
        let b = Bounds {
            partition_max_bytes: 1 << 30,
            passes: 3,
            ..bounds()
        };
        let parts = [part("p1", "a", 0.1, 4 << 30), part("p2", "b", 0.1, 2 << 30)];
        let mut c = Counters::default();
        let no_cut = HashMap::new();
        // Its condition hasn't held `passes` passes yet: no survey.
        assert_eq!(split_candidate(&nodes, &parts, &b, &c, 0, &no_cut), None);
        c.hold("split:p1", true, 3);
        c.hold("split:p1", true, 3);
        c.hold("split:p2", true, 3);
        c.hold("split:p2", true, 3);
        assert_eq!(split_candidate(&nodes, &parts, &b, &c, 0, &no_cut), Some(0));
        // p1 resting: p2.
        c.rest.insert("p1".into(), 10_000);
        assert_eq!(split_candidate(&nodes, &parts, &b, &c, 0, &no_cut), Some(1));
        // p2's node didn't answer: none.
        nodes[1].fresh = false;
        assert_eq!(split_candidate(&nodes, &parts, &b, &c, 0, &no_cut), None);
    }

    #[test]
    fn a_candidate_with_no_node_to_split_onto_isnt_surveyed() {
        // perf2 on #2137: p1, the furthest over, has its replicas on every other node, so
        // its split has nowhere to go; p2 is surveyed instead. Then with every other node
        // resting, neither is.
        let nodes = [node("a"), node("b"), node("c")];
        let b = Bounds {
            partition_max_bytes: 1 << 30,
            passes: 1,
            ..bounds()
        };
        let mut p1 = part("p1", "a", 0.1, 4 << 30);
        p1.replicas = vec!["b".into(), "c".into()];
        let parts = [p1, part("p2", "b", 0.1, 2 << 30)];
        let no_cut = HashMap::new();
        let mut c = Counters::default();
        assert_eq!(split_candidate(&nodes, &parts, &b, &c, 0, &no_cut), Some(1));
        c.rest.insert("a".into(), 10_000);
        c.rest.insert("c".into(), 10_000);
        assert_eq!(split_candidate(&nodes, &parts, &b, &c, 0, &no_cut), None);
    }

    #[test]
    fn an_idle_clusters_small_partitions_are_cold() {
        // Every load near 0 (no load measured, or background work only), so the mean is too:
        // under a hundredth of `partition_max_load` is cold whatever the mean.
        let nodes = [node("a"), node("b")];
        let b = Bounds::default();
        let mut p1 = part("p1", "a", 0.001, 1 << 20);
        p1.neighbours = vec!["p2".into()];
        let parts = [p1, part("p2", "b", 0.001, 1 << 20)];
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        assert_eq!(
            d,
            Decision::Act(
                Action::Merge {
                    from: "p1".into(),
                    into: "p2".into()
                },
                "p1 is small and cold".into()
            )
        );
    }

    #[test]
    fn a_node_that_didnt_answer_is_neither_source_nor_target_and_resets_its_streak() {
        let mut nodes = vec![node("a"), node("b"), node("c")];
        let parts = spread();
        let b = bounds();
        let (mut c, mut t) = (Counters::default(), 0);
        run(&nodes, &parts, &b, &mut c, &mut t, 9);
        nodes[0].fresh = false;
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 1);
        assert!(matches!(d, Decision::Skip(_)), "{d:?}");
        nodes[0].fresh = true;
        // Its streak started again: nine more passes aren't enough.
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 9);
        assert!(matches!(d, Decision::Skip(_)), "{d:?}");
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 1);
        assert!(matches!(d, Decision::Act(Action::Move { .. }, _)), "{d:?}");
    }

    #[test]
    fn a_move_whose_copy_outlasts_the_dwell_isnt_taken() {
        let nodes = [node("a"), node("b"), node("c")];
        // 200 GiB at 64 MiB/s: over 50 min, past the 30 min dwell.
        let parts = [
            part("p1", "a", 0.5, 200 << 30),
            part("p2", "b", 0.05, 1 << 30),
            part("p3", "c", 0.1, 1 << 30),
        ];
        let b = Bounds {
            partition_max_bytes: 1 << 50,
            ..bounds()
        };
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 10);
        let Decision::Skip(why) = d else {
            panic!("{d:?}")
        };
        assert!(why.contains("no partition on it can move"), "{why}");
    }
    #[test]
    fn a_tablets_load_is_its_larger_share_of_reads_or_writes() {
        // perf287 on the D332 PR 5 design: a write-hot tablet the reads never touch is still
        // the hot side, and a read-hot one with no writes too; shares renormalise to the
        // partition's load.
        let r = |start: &str, write_load: f64, reads: u64| TabletReport {
            table: "users".into(),
            start: start.as_bytes().to_vec(),
            bytes: 1 << 20,
            write_load,
            reads,
        };
        let t = tablet_loads(&[r("", 0.0, 10), r("g", 90.0, 0), r("m", 10.0, 10)], 0.8);
        let loads: Vec<f64> = t.iter().map(|t| t.load).collect();
        assert!((loads.iter().sum::<f64>() - 0.8).abs() < 1e-9, "{loads:?}");
        assert!(
            loads[1] > loads[0] && loads[1] > loads[2],
            "write-hot g: {loads:?}"
        );
        // No measure at all: every load 0, so the split weighs by bytes.
        let t = tablet_loads(&[r("", 0.0, 0), r("g", 0.0, 0)], 0.8);
        assert!(t.iter().all(|t| t.load == 0.0));
    }

    #[test]
    fn a_write_only_hot_tablet_is_the_splits_hot_side() {
        // perf287: P's reads are spread, but its writes all go to [m, t): the split cuts at
        // m or t, not where the reads alone would halve it.
        let nodes = [node("a"), node("b")];
        let r = |start: &str, write_load: f64, reads: u64| TabletReport {
            table: "users".into(),
            start: start.as_bytes().to_vec(),
            bytes: 1 << 20,
            write_load,
            reads,
        };
        let mut p = part("p1", "a", 0.8, 1 << 30);
        p.tablets = tablet_loads(
            &[
                r("", 0.0, 10),
                r("g", 0.0, 10),
                r("m", 100.0, 10),
                r("t", 0.0, 10),
            ],
            0.8,
        );
        p.ranges = vec![span("", None)];
        let parts = [p, part("p2", "b", 0.1, 1 << 30)];
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &bounds(), &mut c, &mut t, 10);
        assert!(
            matches!(&d, Decision::Act(Action::Split { key, .. }, _) if key == b"m" || key == b"t"),
            "{d:?}"
        );
    }

    /// #2158 item 2: a load move's target stays within its size bound too. By load, b is the
    /// coldest node, but p1's bytes would take b over its size bound: the move goes to c,
    /// within both, where it went to b before.
    #[test]
    fn a_load_move_never_takes_its_target_over_its_size_bound() {
        const G: u64 = 1 << 30;
        let nodes = [node("a"), node("b"), node("c"), node("d")];
        let parts = [
            part("p1", "a", 0.05, 4 * G / 10),
            part("p2", "a", 0.55, G / 10),
            part("pb", "b", 0.0, 3 * G / 2),
            part("pc", "c", 0.2, G / 2),
            part("pd", "d", 0.2, G / 2),
        ];
        let b = Bounds {
            partition_max_load: 1.0,
            passes: 1,
            ..bounds()
        };
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 1);
        assert!(
            matches!(&d, Decision::Act(Action::Move { partition, to, .. }, _)
                if partition == "p1" && to == "c"),
            "{d:?}"
        );
    }

    /// #2158 item 2, the reverse: a size move's target stays within its load bound. No node
    /// would, so nothing moves, and the pass says why.
    #[test]
    fn a_size_move_never_takes_its_target_over_its_load_bound() {
        const G: u64 = 1 << 30;
        let nodes = [node("a"), node("b"), node("c")];
        // a is over on size; b and c are small, but p1 alone would take either over on load.
        let parts = [
            part("p1", "a", 0.3, G),
            part("p2", "a", 0.0, 2 * G),
            part("pb", "b", 0.15, G / 2),
            part("pc", "c", 0.15, G / 2),
        ];
        let b = Bounds {
            partition_max_load: 1.0,
            passes: 1,
            ..bounds()
        };
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 1);
        assert_eq!(
            d,
            Decision::Skip(
                "node a is over its bound, but no partition on it can move within the bounds"
                    .into()
            )
        );
    }

    /// #2158 item 3: a node over its bound that is still counting its passes, then resting,
    /// says so, not "within bounds".
    #[test]
    fn a_node_over_its_bound_but_counting_or_resting_says_so() {
        const G: u64 = 1 << 30;
        let nodes = [node("a"), node("b"), node("c")];
        let parts = [
            part("p1", "a", 0.0, 3 * G),
            part("p2", "b", 0.0, G),
            part("p3", "c", 0.0, G),
        ];
        let b = Bounds {
            passes: 3,
            ..bounds()
        };
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 2);
        assert_eq!(
            d,
            Decision::Skip(
                "node a is over its bound (1.80x the mean size): 2 of 3 passes so far".into()
            )
        );
        c.rest.insert("a".into(), t + 30 * 1000 + 90 * 1000);
        let d = run(&nodes, &parts, &b, &mut c, &mut t, 1);
        assert_eq!(
            d,
            Decision::Skip(
                "node a is over its bound (1.80x the mean size), but rests 90 s more".into()
            )
        );
    }

    /// #2158 item 3: a split candidate counting its passes, then resting, says so.
    #[test]
    fn a_split_candidate_counting_or_resting_says_so() {
        let nodes = [node("a"), node("b")];
        let parts = [part("p1", "a", 0.0, 2 << 20), part("p2", "b", 0.0, 2 << 20)];
        let b = Bounds {
            partition_max_bytes: 3 << 20,
            size_ratio: 10.0,
            passes: 3,
            ..bounds()
        };
        let mut big = parts.clone();
        big[0].bytes = 4 << 20;
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &big, &b, &mut c, &mut t, 1);
        assert_eq!(
            d,
            Decision::Skip("p1 is too big: 1 of 3 passes so far".into())
        );
        c.rest.insert("p1".into(), t + 30 * 1000 + 61 * 1000);
        let d = run(&nodes, &big, &b, &mut c, &mut t, 2);
        assert_eq!(
            d,
            Decision::Skip("p1 is too big, but rests 31 s more".into())
        );
    }

    /// #2158 item 1: a node that answered without numbers for one of its partitions is
    /// neither source nor target, and the pass names it when nothing else is.
    #[test]
    fn a_node_with_a_partition_it_gave_no_numbers_for_is_left_out_and_named() {
        let why = "node a's numbers are incomplete: partition p1 is closed there".to_owned();
        let mut a = node("a");
        a.fresh = false;
        a.incomplete = Some(why.clone());
        let parts = [
            part("p1", "a", 0.0, 0),
            part("p2", "b", 0.2, 1 << 30),
            part("p3", "c", 0.2, 1 << 30),
        ];
        let b = Bounds {
            passes: 1,
            ..bounds()
        };
        let (mut c, mut t) = (Counters::default(), 0);
        // Fresh, a would be a's p1's lighter node, and b's and c's target.
        let d = run(
            &[a.clone(), node("b"), node("c")],
            &parts,
            &b,
            &mut c,
            &mut t,
            1,
        );
        assert_eq!(d, Decision::Skip(why.clone()));
        let d = run(&[a, node("b")], &parts, &b, &mut c, &mut t, 1);
        assert_eq!(
            d,
            Decision::Skip(format!(
                "fewer than two nodes have complete numbers this pass: {why}"
            ))
        );
    }
}

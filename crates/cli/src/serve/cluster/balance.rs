//! D332's balancer policy (PR 3, `.coord/cli692-design-1485.md` with its amendments): a pure
//! function from the cluster's partitions and nodes, their measured loads and the writer's
//! counters, to at most one action. The writer's loop gathers the inputs from `Status`
//! surveys and the map, and runs the action through the movers; nothing here does I/O.
//!
//! The rules, in priority order within a pass:
//! 1. **Split** a partition too big or too hot (`partition_max_bytes`, `partition_max_load`),
//!    at a tablet boundary that halves it, onto the coldest node holding none of its
//!    replicas. A split whose best halves leave over 80% on one side is a hot key: the range
//!    is unsplittable, and a whole move is considered instead (perf287's should-fix 1).
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
//! nodes an action touched rest for `dwell`. A partition moved `max_moves` times within
//! `bounce_window` is auto-pinned instead (its history's PIN entry; it expires a
//! `bounce_window` later, unless an operator's pin cleared it: simp724's S2).
//!
//! **Never a candidate** (simp724's S1, ai1249's (4)): a pinned partition; one in any range
//! move's record, as source or target; one with in-doubt shares; for a split or merge, a
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
    /// It answered this pass, with numbers no older than a window.
    pub(crate) fresh: bool,
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
    /// A split of `partition`'s `table` at `key`, the upper side going to a new partition on
    /// `node` (D331 PR 5's driver).
    Split {
        partition: String,
        table: String,
        key: Vec<u8>,
        node: String,
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

    fn resting(&self, key: &str, now: u64) -> bool {
        self.rest.get(key).is_some_and(|&t| now < t)
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

/// How many times `p` was moved, split or merged within the bounce window before `now`.
fn bounces(p: &PartView, now: u64, b: &Bounds) -> u32 {
    let since = now.saturating_sub(b.bounce_window_ms);
    let n = p
        .history
        .iter()
        .filter(|(k, at)| *k != Kind::Pin && *at >= since)
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

/// The split of `p` that best halves it: a tablet boundary of one table (not the table's
/// first key) where the load (or the bytes, with no load measured) below is closest to half
/// of the partition's. `None` with no boundary to split at. The second value is the larger
/// side's share, for the hot-key guard.
fn split_point(p: &PartView) -> Option<(String, Vec<u8>, f64)> {
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
    let mut best: Option<(String, Vec<u8>, f64)> = None;
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
            if ok {
                let larger = below.max(total - below) / total;
                if best.as_ref().is_none_or(|(_, _, s)| larger < *s) {
                    best = Some((table.to_owned(), t.start.clone(), larger));
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
    if fresh.len() < 2 {
        return Decision::Skip("fewer than two nodes answered this pass".into());
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
    let eligible = |p: &PartView| !p.pinned && !p.busy && is_fresh(&p.node);
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

    // 1. Splits.
    for p in parts.iter().filter(|p| eligible(p)) {
        let key = format!("split:{}", p.id);
        let too_big = p.bytes > b.partition_max_bytes;
        let too_hot = p.load > b.partition_max_load;
        if !counters.hold(&key, too_big || too_hot, b.passes) {
            continue;
        }
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
        // The loop surveys no tablets until D331 PR 5's split (ai1249's S1 on #2103).
        if p.tablets.is_empty() {
            skip.get_or_insert_with(|| {
                format!(
                    "{} is too {}: a split waits for D331 PR 5",
                    p.id,
                    if too_big { "big" } else { "hot" }
                )
            });
            continue;
        }
        let Some((table, at, larger)) = split_point(p) else {
            skip.get_or_insert_with(|| {
                format!(
                    "{} has no tablet boundary inside its own ranges, outside a pin, to split at",
                    p.id
                )
            });
            continue;
        };
        if larger > 0.8 {
            skip.get_or_insert_with(|| {
                format!(
                    "{}'s best split leaves {:.0}% on one side: a hot key or a narrow hot range, \
                     unsplittable",
                    p.id,
                    larger * 100.0
                )
            });
            continue;
        }
        let Some(node) = coldest(counters, too_hot, &p.node, &p.replicas) else {
            skip.get_or_insert_with(|| {
                format!("{}'s split has no node free of its replicas", p.id)
            });
            continue;
        };
        return Decision::Act(
            Action::Split {
                partition: p.id.clone(),
                table,
                key: at,
                node,
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
        if !ready || counters.resting(&n.id, now) {
            continue;
        }
        let by_load = lr - b.load_ratio >= sr - b.size_ratio;
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
            let Some(to) = coldest(counters, by_load, &n.id, &p.replicas) else {
                continue;
            };
            let Some((_, tl, ts)) = stats.iter().find(|s| s.0.id == to) else {
                continue;
            };
            let tw = stats
                .iter()
                .find(|s| s.0.id == to)
                .map_or(1.0, |s| if s.0.weight > 0.0 { s.0.weight } else { 1.0 });
            let nw = if n.weight > 0.0 { n.weight } else { 1.0 };
            let (from_after, to_after, mean) = if by_load {
                (load - p.load / nw, tl + p.load / tw, mean_load)
            } else {
                (
                    size - p.bytes as f64 / nw,
                    ts + p.bytes as f64 / tw,
                    mean_size,
                )
            };
            let bound = if by_load { b.load_ratio } else { b.size_ratio };
            if mean > 0.0 && to_after / mean > bound {
                continue;
            }
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
                format!(
                    "node {} is at {:.2}x the mean {}",
                    n.id,
                    if by_load { lr } else { sr },
                    if by_load { "load" } else { "size" }
                ),
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
        let small = p.bytes < b.partition_min_bytes && p.load < node_mean.max(mean_load) / 10.0;
        if !counters.hold(&key, small, b.passes) || counters.resting(&p.id, now) {
            continue;
        }
        // Not within four dwells of its own split.
        let split_at = p
            .history
            .iter()
            .filter(|(k, _)| *k == Kind::Split)
            .map(|(_, at)| *at)
            .max();
        if split_at.is_some_and(|at| now < at.saturating_add(4 * b.dwell_ms)) {
            continue;
        }
        let fits = |q: &PartView| {
            q.id != p.id
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
    Decision::Skip(skip.unwrap_or_else(|| "within bounds".into()))
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
            part("p2", "a", 0.2, 2 << 30),
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
    fn a_split_with_no_tablets_surveyed_waits_for_the_split_driver() {
        // ai1249's S1 on #2103: the loop surveys no tablets until D331 PR 5.
        let nodes = [node("a"), node("b")];
        let mut p = part("p1", "a", 0.8, 1 << 30);
        p.ranges = vec![span("", None)];
        let parts = [p, part("p2", "b", 0.75, 1 << 30)];
        let (mut c, mut t) = (Counters::default(), 0);
        let d = run(&nodes, &parts, &bounds(), &mut c, &mut t, 10);
        let Decision::Skip(why) = d else {
            panic!("{d:?}")
        };
        assert!(why.contains("a split waits for D331 PR 5"), "{why}");
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
                    node: "b".into()
                },
                "p1 is too hot".into()
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
}

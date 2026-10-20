//! The shares a shard applied whose coordinators have not yet been told they are in SSTs
//! (D24, D254), indexed so a report touches only the shares it reports (#777).
//!
//! A share is reported once every slot it writes is flushed through the seqno it applied at.
//! Shares are grouped by the slots they write and ordered by that seqno, so a report reads
//! each group's flushed-through seqnos once and pops the group's flushed prefix. The flat
//! list it replaces was scanned whole at every flush install: about 98K shares, 185 ns each,
//! held a 16-shard cross-shard row's `Flushed` turn for 17-21 ms (AX102, 2026-10-20).

use std::collections::{BTreeMap, HashMap};

use pigeonhole_format::{FamilyId, Seqno, TabletId};
use pigeonhole_runtime::ShardId;

/// A `(tablet, family)` slot.
pub(super) type Slot = (TabletId, FamilyId);

/// One slot set's shares: `(flush seqno, id)` to the commit's coordinator.
type Shares = BTreeMap<(Seqno, Seqno), ShardId>;

/// A slot some unreported share writes: how many, and the flushed-through seqno the last
/// report saw for it (`None` until a report ran with the slot listed). A share resolves only
/// when a slot's flushed-through seqno moves (or its tablet goes), so a report that finds
/// every listed slot where the last one left it has nothing to report and skips: every
/// manifest commit anywhere (an SST, a checkpoint, a flushed seqno, a tablet change) tells
/// every shard to report, and on a 16-shard store running cross-shard commits that was
/// 20-60 scans a second per shard over 30-40K shares, 1-3 ms inside a turn each, with the
/// visible watermark held for every client meanwhile (the 0.3.0 gate's cross-shard row after
/// a packed load regrew: 398K against 590K ops/s).
#[derive(Debug, Default)]
pub(super) struct UnreportedSlot {
    shares: usize,
    seen: Option<Option<Seqno>>,
}

/// The unreported shares (see the module docs).
#[derive(Debug, Default)]
pub(super) struct Unreported {
    /// By the slots the shares write (sorted, deduplicated), then by `(flush seqno, id)`:
    /// each share's coordinator. Ordered maps, so a report's sends keep a seed replay's
    /// message order.
    groups: BTreeMap<Box<[Slot]>, Shares>,
    /// Shares per slot, with what the last report saw.
    slots: HashMap<Slot, UnreportedSlot>,
    len: usize,
    /// The last report stopped at its budget: the next one runs even if no slot moved.
    more: bool,
}

impl Unreported {
    /// Adds the share `seqno` (its commit's id), applied at `flush_seqno`, of `coordinator`'s
    /// commit, writing `slots`.
    pub(super) fn push(
        &mut self,
        seqno: Seqno,
        flush_seqno: Seqno,
        coordinator: ShardId,
        slots: &[Slot],
    ) {
        let mut key: Vec<Slot> = slots.to_vec();
        key.sort_unstable();
        key.dedup();
        for s in &key {
            self.slots.entry(*s).or_default().shares += 1;
        }
        self.groups
            .entry(key.into_boxed_slice())
            .or_default()
            .insert((flush_seqno, seqno), coordinator);
        self.len += 1;
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn clear(&mut self) {
        *self = Self::default();
    }

    /// Whether a report has anything to do: a listed slot's flushed-through seqno (`current`,
    /// `None` once its tablet is gone) differs from what the last report saw, a share writes
    /// no slot (it is due at once), or the last report stopped at its budget. Records what it
    /// sees.
    pub(super) fn due(&mut self, current: impl Fn(&Slot) -> Option<Seqno>) -> bool {
        let mut moved = false;
        for (key, slot) in self.slots.iter_mut() {
            let now = Some(current(key));
            if slot.seen != now {
                slot.seen = now;
                moved = true;
            }
        }
        moved || self.more || self.groups.contains_key(&[][..])
    }

    /// Takes up to `budget` shares whose every slot is flushed through the seqno the share
    /// applied at (`current` as for [`due`](Self::due); a gone tablet's slot counts as
    /// flushed), group by group in flush order, handing each to `report(id, coordinator)`.
    /// Returns whether it stopped at the budget with such a share left.
    pub(super) fn take_flushed(
        &mut self,
        budget: usize,
        current: impl Fn(&Slot) -> Option<Seqno>,
        mut report: impl FnMut(Seqno, ShardId),
    ) -> bool {
        let mut taken = 0;
        let mut emptied = Vec::new();
        self.more = false;
        for (key, shares) in self.groups.iter_mut() {
            let through = key
                .iter()
                .map(|s| current(s).unwrap_or(Seqno::MAX))
                .min()
                .unwrap_or(Seqno::MAX);
            let mut k = 0;
            while let Some(entry) = shares.first_entry() {
                if entry.key().0 > through {
                    break;
                }
                if taken == budget {
                    self.more = true;
                    break;
                }
                let ((_, seqno), coordinator) = entry.remove_entry();
                report(seqno, coordinator);
                taken += 1;
                k += 1;
            }
            if k > 0 {
                self.len -= k;
                for s in key.iter() {
                    let slot = self.slots.get_mut(s).expect("counted");
                    slot.shares -= k;
                    if slot.shares == 0 {
                        self.slots.remove(s);
                    }
                }
                if shares.is_empty() {
                    emptied.push(key.clone());
                }
            }
            if self.more {
                break;
            }
        }
        for key in emptied {
            self.groups.remove(&key);
        }
        self.more
    }

    /// The slots of every share whose commit id is at most `through`.
    pub(super) fn slots_through(&self, through: Seqno) -> impl Iterator<Item = Slot> + '_ {
        self.groups
            .iter()
            .filter(move |(_, shares)| shares.keys().any(|(_, id)| *id <= through))
            .flat_map(|(key, _)| key.iter().copied())
    }

    /// Checks the per-slot counts against the groups (debug builds).
    pub(super) fn debug_check(&self) {
        if cfg!(debug_assertions) {
            let mut counts: HashMap<Slot, usize> = HashMap::new();
            let mut len = 0;
            for (key, shares) in &self.groups {
                assert!(!shares.is_empty(), "empty group left");
                len += shares.len();
                for s in key.iter() {
                    *counts.entry(*s).or_default() += shares.len();
                }
            }
            assert_eq!(len, self.len, "unreported share count drifted");
            assert_eq!(counts.len(), self.slots.len(), "unreported slots drifted");
            for (s, n) in counts {
                assert_eq!(self.slots[&s].shares, n, "unreported slot counts drifted");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(t: u64, f: u32) -> Slot {
        (TabletId(t), FamilyId(f))
    }

    #[test]
    fn a_report_runs_only_when_a_listed_slot_flushed_further() {
        let mut u = Unreported::default();
        u.push(1, 1, ShardId(0), &[slot(1, 1)]);
        u.push(2, 2, ShardId(0), &[slot(2, 1)]);
        let flushed = std::cell::Cell::new((10, 20));
        let current = |s: &Slot| {
            Some(if s.0 == TabletId(1) {
                flushed.get().0
            } else {
                flushed.get().1
            })
        };
        // Unseen slots: the first call reports.
        assert!(u.due(current));
        // Nothing moved: every later call skips, however many manifest commits arrive.
        for _ in 0..5 {
            assert!(!u.due(current));
        }
        // One slot flushed further: report once, then skip again.
        flushed.set((11, 20));
        assert!(u.due(current));
        assert!(!u.due(current));
        // A tablet that went (its flushed-through reads `None`) counts as a move too.
        assert!(u.due(|_| None));
        assert!(!u.due(|_| None));
        // A slot a new share lists starts unseen and forces the next report.
        u.push(3, 3, ShardId(0), &[slot(3, 1)]);
        assert!(u.due(|_| None));
        // A share that writes no slot is due at once, every time until taken.
        u.push(4, 4, ShardId(1), &[]);
        assert!(u.due(|_| None));
        assert!(u.due(|_| None));
    }

    /// The indexed report takes exactly the shares the old full scan did: every slot
    /// flushed through the share's flush seqno, a gone tablet counting as flushed.
    #[test]
    fn takes_what_a_full_scan_would() {
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..200 {
            let mut u = Unreported::default();
            let mut flat: Vec<(Seqno, Seqno, ShardId, Vec<Slot>)> = Vec::new();
            for id in 1..=(next() % 300) {
                let n = (next() % 3) as usize;
                let slots: Vec<Slot> = (0..n)
                    .map(|_| slot(next() % 4, (next() % 2) as u32))
                    .collect();
                let flush = 1 + next() % 1000;
                let coord = ShardId((next() % 5) as u16);
                u.push(id, flush, coord, &slots);
                flat.push((id, flush, coord, slots));
            }
            u.debug_check();
            // Flushed-through per slot; tablet 3 is gone.
            let through: HashMap<Slot, Seqno> = (0..4)
                .flat_map(|t| (0..2).map(move |f| slot(t, f)))
                .map(|s| (s, next() % 1100))
                .collect();
            let current = |s: &Slot| (s.0 != TabletId(3)).then(|| through[s]);
            let mut want: Vec<(Seqno, ShardId)> = flat
                .iter()
                .filter(|(_, flush, _, slots)| {
                    slots.iter().all(|s| current(s).is_none_or(|f| f >= *flush))
                })
                .map(|(id, _, c, _)| (*id, *c))
                .collect();
            want.sort_unstable();
            // In budgets of 1..=7 until done.
            let budget = 1 + round % 7;
            let mut got = Vec::new();
            while u.take_flushed(budget, current, |id, c| got.push((id, c))) {
                u.debug_check();
            }
            u.debug_check();
            got.sort_unstable();
            assert_eq!(got, want, "round {round}");
            assert_eq!(u.len(), flat.len() - want.len());
            // Nothing more is taken without a move.
            assert!(!u.take_flushed(usize::MAX, current, |_, _| panic!("taken twice")));
        }
    }

    #[test]
    fn slots_through_lists_the_slots_of_shares_at_or_below() {
        let mut u = Unreported::default();
        u.push(5, 9, ShardId(0), &[slot(1, 1)]);
        u.push(7, 8, ShardId(0), &[slot(2, 1), slot(1, 1)]);
        let mut got: Vec<Slot> = u.slots_through(5).collect();
        got.sort_unstable();
        assert_eq!(got, vec![slot(1, 1)]);
        let mut got: Vec<Slot> = u.slots_through(7).collect();
        got.sort_unstable();
        got.dedup();
        assert_eq!(got, vec![slot(1, 1), slot(2, 1)]);
    }
}

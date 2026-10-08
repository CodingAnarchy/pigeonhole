//! The in-memory free-space map: a buddy allocator over 64 KiB units.
//!
//! A *unit* is the smallest extent (64 KiB, 16 pages); unit `u` covers pages `16u..16u+16`.
//! An extent of size class `c` is `2^c` units aligned to `2^c` units, so its buddy is
//! `u ^ 2^c`. Unit 0 holds the superblocks, the lock page and the reserved pages and is never
//! free. Free blocks are kept per class in ordered sets (lowest address first) and coalesce
//! with their buddy on free, so the map never holds two free buddies.
//!
//! Every unit below `frontier` (the file's end) is in exactly one of: unit 0, a free block,
//! or a used extent (live or retired). Nothing here does I/O; the pager grows and truncates
//! the file around these calls.

use std::collections::{BTreeMap, BTreeSet};

use pigeonhole_format::ManifestVersion;

use crate::Extent;

/// Pages per unit (64 KiB / 4 KiB).
pub(crate) const UNIT_PAGES: u64 = 16;
/// Bytes per unit.
pub(crate) const UNIT_BYTES: u64 = 64 * 1024;
/// Number of size classes (0..=10).
const CLASSES: usize = Extent::MAX_CLASS as usize + 1;

/// A used extent: live, or retired at a manifest version and awaiting reclamation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Used {
    class: u8,
    retired: Option<ManifestVersion>,
    /// Known to be referenced by a durable root: loaded at open, or named by a committed
    /// root. A live extent that is not is *pending* (allocated in this session); only
    /// pending extents may be abandoned or trimmed. An SST published in this session stays
    /// pending here, since the pager never sees the manifest's contents.
    published: bool,
}

/// Why a set of live extents cannot be loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoadError {
    /// Not page 16 or later, unknown class, or misaligned.
    Invalid,
    /// Two live extents overlap without being equal.
    Overlap,
    /// A live extent ends past the file.
    PastEnd,
}

#[derive(Debug, Clone)]
pub(crate) struct Alloc {
    free: [BTreeSet<u64>; CLASSES],
    used: BTreeMap<u64, Used>,
    /// `(superseded_at, unit)` of every retired extent.
    retired: BTreeSet<(ManifestVersion, u64)>,
    /// End of the file, in units (at least 1).
    frontier: u64,
    used_units: u64,
    retired_units: u64,
}

/// A region `Alloc::clear_for` could clear: occupied units, first unit, and occupants as
/// `(unit, class)`, largest first.
type Candidate = (u64, u64, Vec<(u64, u8)>);

/// Units in an extent of `class`.
fn units(class: u8) -> u64 {
    1 << class
}

/// The extent's first unit, if it is a well-formed extent.
pub(crate) fn unit_of(e: Extent) -> Option<u64> {
    if e.size_class > Extent::MAX_CLASS || !e.page.is_multiple_of(UNIT_PAGES) {
        return None;
    }
    let u = e.page / UNIT_PAGES;
    (u != 0 && u.is_multiple_of(units(e.size_class))).then_some(u)
}

fn extent(unit: u64, class: u8) -> Extent {
    Extent {
        page: unit * UNIT_PAGES,
        size_class: class,
    }
}

impl Alloc {
    /// An empty map over a file of `frontier` units (at least 1).
    pub(crate) fn empty(frontier: u64) -> Self {
        let mut a = Self {
            free: Default::default(),
            used: BTreeMap::new(),
            retired: BTreeSet::new(),
            frontier: frontier.max(1),
            used_units: 0,
            retired_units: 0,
        };
        a.free_range(1, a.frontier);
        a
    }

    /// A map over a file of `frontier` units in which exactly `live` is used. Exact duplicates
    /// in `live` are accepted (tablets may share an SST after a split).
    pub(crate) fn load(
        frontier: u64,
        live: impl IntoIterator<Item = Extent>,
    ) -> Result<Self, LoadError> {
        let frontier = frontier.max(1);
        let mut extents: Vec<(u64, u8)> = Vec::new();
        for e in live {
            let u = unit_of(e).ok_or(LoadError::Invalid)?;
            extents.push((u, e.size_class));
        }
        extents.sort_unstable();
        extents.dedup();
        let mut a = Self {
            free: Default::default(),
            used: BTreeMap::new(),
            retired: BTreeSet::new(),
            frontier,
            used_units: 0,
            retired_units: 0,
        };
        let mut next = 1;
        for (u, class) in extents {
            if u < next {
                return Err(LoadError::Overlap);
            }
            let end = u + units(class);
            if end > frontier {
                return Err(LoadError::PastEnd);
            }
            a.free_range(next, u);
            a.used.insert(
                u,
                Used {
                    class,
                    retired: None,
                    published: true,
                },
            );
            a.used_units += units(class);
            next = end;
        }
        a.free_range(next, frontier);
        Ok(a)
    }

    /// Adds `[from, to)` to the free map as maximal aligned blocks, coalescing with free
    /// neighbors.
    fn free_range(&mut self, mut from: u64, to: u64) {
        while from < to {
            let mut class = 0u8;
            while class < Extent::MAX_CLASS
                && from.is_multiple_of(units(class + 1))
                && from + units(class + 1) <= to
            {
                class += 1;
            }
            self.free_block(from, class);
            from += units(class);
        }
    }

    /// Returns one block to the free map, merging it with its buddy while the buddy is free.
    fn free_block(&mut self, mut unit: u64, mut class: u8) {
        while class < Extent::MAX_CLASS {
            let buddy = unit ^ units(class);
            if !self.free[class as usize].remove(&buddy) {
                break;
            }
            unit = unit.min(buddy);
            class += 1;
        }
        self.free[class as usize].insert(unit);
    }

    /// Takes `block` (free, of class `from`) out of the free map and splits it down to
    /// `class`, returning the lower halves' upper buddies to the map. Returns its unit.
    fn take_split(&mut self, unit: u64, from: u8, class: u8) -> u64 {
        self.free[from as usize].remove(&unit);
        let mut k = from;
        while k > class {
            k -= 1;
            self.free[k as usize].insert(unit + units(k));
        }
        unit
    }

    fn mark_used(&mut self, unit: u64, class: u8) -> Extent {
        self.used.insert(
            unit,
            Used {
                class,
                retired: None,
                published: false,
            },
        );
        self.used_units += units(class);
        extent(unit, class)
    }

    /// Allocates from free space: the smallest free class that fits, lowest address within
    /// it. `None` if the file must grow.
    pub(crate) fn alloc_free(&mut self, class: u8) -> Option<Extent> {
        let from = (class..=Extent::MAX_CLASS).find(|&k| !self.free[k as usize].is_empty())?;
        let unit = *self.free[from as usize].first().expect("non-empty");
        let unit = self.take_split(unit, from, class);
        Some(self.mark_used(unit, class))
    }

    /// Allocates the lowest-addressed free block that fits, regardless of class (for
    /// relocation toward the start of the file). `None` if no free block fits.
    pub(crate) fn alloc_lowest(&mut self, class: u8) -> Option<Extent> {
        let (unit, from) = (class..=Extent::MAX_CLASS)
            .filter_map(|k| self.free[k as usize].first().map(|&u| (u, k)))
            .min()?;
        let unit = self.take_split(unit, from, class);
        Some(self.mark_used(unit, class))
    }

    /// Where growing the file would place an extent of `class`, and the new frontier.
    pub(crate) fn grow_target(&self, class: u8) -> (u64, u64) {
        let unit = self.frontier.next_multiple_of(units(class));
        (unit, unit + units(class))
    }

    /// Records a growth planned by [`grow_target`](Self::grow_target) once the file has been
    /// extended: the alignment gap becomes free and the extent is used.
    pub(crate) fn alloc_grown(&mut self, class: u8) -> Extent {
        let (unit, end) = self.grow_target(class);
        let old = self.frontier;
        self.frontier = end;
        self.free_range(old, unit);
        self.mark_used(unit, class)
    }

    /// Whether `e` is exactly a pending extent: live and not known to be published.
    pub(crate) fn is_pending(&self, e: Extent) -> bool {
        unit_of(e)
            .and_then(|u| self.used.get(&u))
            .is_some_and(|u| u.class == e.size_class && u.retired.is_none() && !u.published)
    }

    /// Records that a durable root references the live extent `e`. Returns false if `e` is
    /// not live.
    pub(crate) fn publish(&mut self, e: Extent) -> bool {
        let Some(unit) = unit_of(e) else { return false };
        match self.used.get_mut(&unit) {
            Some(u) if u.class == e.size_class && u.retired.is_none() => {
                u.published = true;
                true
            }
            _ => false,
        }
    }

    /// Frees a pending extent at once. Returns false (and changes nothing) if `e` is not
    /// pending.
    pub(crate) fn release_live(&mut self, e: Extent) -> bool {
        let Some(unit) = unit_of(e) else { return false };
        if !self.is_pending(e) {
            return false;
        }
        self.used.remove(&unit);
        self.used_units -= units(e.size_class);
        self.free_block(unit, e.size_class);
        true
    }

    /// Shrinks a pending extent to `class` in place, freeing its upper halves at once.
    /// Returns false (and changes nothing) if `e` is not pending or `class` is larger than
    /// its class.
    pub(crate) fn shrink_live(&mut self, e: Extent, class: u8) -> bool {
        if class > e.size_class || !self.is_pending(e) {
            return false;
        }
        let unit = unit_of(e).expect("pending extent is well formed");
        self.used.insert(
            unit,
            Used {
                class,
                retired: None,
                published: false,
            },
        );
        self.used_units -= units(e.size_class) - units(class);
        // Each upper half's buddy is the lower part, still used, so none coalesces.
        for k in class..e.size_class {
            self.free_block(unit + units(k), k);
        }
        true
    }

    /// Marks a live extent retired at `at`. Returns false if `e` is not live.
    pub(crate) fn retire(&mut self, e: Extent, at: ManifestVersion) -> bool {
        let Some(unit) = unit_of(e) else { return false };
        match self.used.get_mut(&unit) {
            Some(u) if u.class == e.size_class && u.retired.is_none() => {
                u.retired = Some(at);
                self.retired.insert((at, unit));
                self.retired_units += units(e.size_class);
                true
            }
            _ => false,
        }
    }

    /// Frees every retired extent with `superseded_at <= oldest_live`. Returns how many.
    pub(crate) fn reclaim(&mut self, oldest_live: ManifestVersion) -> usize {
        let keep = match oldest_live.checked_add(1) {
            Some(v) => self.retired.split_off(&(v, 0)),
            None => BTreeSet::new(),
        };
        let done = std::mem::replace(&mut self.retired, keep);
        for &(_, unit) in &done {
            let used = self.used.remove(&unit).expect("retired extent is used");
            let n = units(used.class);
            self.used_units -= n;
            self.retired_units -= n;
            self.free_block(unit, used.class);
        }
        done.len()
    }

    /// Whether `e` is exactly a live (used, not retired) extent.
    pub(crate) fn is_live(&self, e: Extent) -> bool {
        unit_of(e)
            .and_then(|u| self.used.get(&u))
            .is_some_and(|u| u.class == e.size_class && u.retired.is_none())
    }

    /// Whether `e` is exactly a used extent (live or retired).
    pub(crate) fn is_used(&self, e: Extent) -> bool {
        unit_of(e)
            .and_then(|u| self.used.get(&u))
            .is_some_and(|u| u.class == e.size_class)
    }

    /// End of the last used extent, in units (1 if none).
    pub(crate) fn used_end(&self) -> u64 {
        self.used
            .last_key_value()
            .map_or(1, |(&u, used)| u + units(used.class))
    }

    /// Drops every free block at or past `end` and makes `end` the frontier. `end` must be at
    /// least [`used_end`](Self::used_end), so no free block straddles it.
    pub(crate) fn truncate(&mut self, end: u64) {
        debug_assert!(end >= self.used_end());
        for set in &mut self.free {
            set.split_off(&end);
        }
        self.frontier = end;
    }

    /// Live extents past the point the live set would end at if packed toward the start of
    /// the file, largest first. Retired extents are not moved: reclaiming them frees their
    /// space.
    pub(crate) fn shrink_plan(&self) -> Vec<Extent> {
        let mut classes: Vec<u8> = self
            .used
            .values()
            .filter(|u| u.retired.is_none())
            .map(|u| u.class)
            .collect();
        classes.sort_unstable_by(|a, b| b.cmp(a));
        let mut packed = Alloc::empty(1);
        for class in classes {
            if packed.alloc_free(class).is_none() {
                packed.alloc_grown(class);
            }
        }
        let target = packed.frontier;
        let mut plan: Vec<Extent> = self
            .used
            .iter()
            .filter(|(u, used)| used.retired.is_none() && **u + units(used.class) > target)
            .map(|(&u, used)| extent(u, used.class))
            .collect();
        plan.sort_unstable_by(|a, b| b.size_class.cmp(&a.size_class).then(b.page.cmp(&a.page)));
        plan
    }

    /// Clears room for `big`, a live extent no free block of its class lies below. A
    /// candidate is a region of its class and alignment below it whose occupants are all
    /// live, smaller and `movable`, and can each move to a free block outside the region and
    /// below `big` (tried on a copy of the map, largest first). Of the first `tries`
    /// candidates, the one with the fewest occupied units wins (the lowest on a tie). Its
    /// free blocks are reserved as pending extents, so nothing else is allocated there, and
    /// returned with the occupants to move out.
    pub(crate) fn clear_for(
        &mut self,
        big: Extent,
        movable: &dyn Fn(Extent) -> bool,
        tries: usize,
    ) -> Option<(Vec<Extent>, Vec<Extent>)> {
        let limit = unit_of(big)?;
        let size = units(big.size_class);
        let mut tried = 0;
        // (occupied units, region, occupants as (unit, class), largest first).
        let mut best: Option<Candidate> = None;
        let mut region = size;
        while region + size <= limit && tried < tries {
            let at = region;
            region += size;
            // Inside a used extent that starts below it: nothing to clear.
            if self
                .used
                .range(..at)
                .next_back()
                .is_some_and(|(&u, used)| u + units(used.class) > at)
            {
                continue;
            }
            let occupants: Vec<(u64, Used)> = self
                .used
                .range(at..at + size)
                .map(|(&u, used)| (u, *used))
                .collect();
            if occupants.is_empty()
                || occupants.iter().any(|(u, used)| {
                    used.retired.is_some()
                        || used.class >= big.size_class
                        || !movable(extent(*u, used.class))
                })
            {
                continue;
            }
            tried += 1;
            let mut trial = self.clone();
            trial.reserve_free_in(at, at + size);
            let mut by_size: Vec<(u64, u8)> =
                occupants.iter().map(|(u, used)| (*u, used.class)).collect();
            by_size.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            let fits = by_size
                .iter()
                .all(|&(_, class)| trial.alloc_lowest(class).is_some_and(|t| t.page < big.page));
            if !fits {
                continue;
            }
            let cost: u64 = by_size.iter().map(|&(_, c)| units(c)).sum();
            if best.as_ref().is_none_or(|(c, _, _)| cost < *c) {
                best = Some((cost, at, by_size));
            }
        }
        let (_, at, occupants) = best?;
        let reserved = self.reserve_free_in(at, at + size);
        let occupants = occupants.into_iter().map(|(u, c)| extent(u, c)).collect();
        Some((reserved, occupants))
    }

    /// Marks every free block inside `[from, to)` used (pending) and returns them.
    fn reserve_free_in(&mut self, from: u64, to: u64) -> Vec<Extent> {
        let mut blocks = Vec::new();
        for class in 0..CLASSES {
            let inside: Vec<u64> = self.free[class].range(from..to).copied().collect();
            for u in inside {
                self.free[class].remove(&u);
                blocks.push(self.mark_used(u, class as u8));
            }
        }
        blocks
    }

    pub(crate) fn frontier(&self) -> u64 {
        self.frontier
    }

    pub(crate) fn used_units(&self) -> u64 {
        self.used_units
    }

    pub(crate) fn retired_units(&self) -> u64 {
        self.retired_units
    }

    /// Checks every invariant (tests only): the free blocks, used extents and unit 0 tile
    /// `[0, frontier)` exactly, and no two free blocks are buddies.
    #[cfg(test)]
    pub(crate) fn check(&self) {
        let mut spans: Vec<(u64, u64)> = vec![(0, 1)];
        for (k, set) in self.free.iter().enumerate() {
            for &u in set {
                assert!(u.is_multiple_of(1 << k), "misaligned free block {u}/{k}");
                if k < CLASSES - 1 {
                    assert!(
                        !set.contains(&(u ^ (1 << k))),
                        "uncoalesced buddies {u}/{k}"
                    );
                }
                spans.push((u, u + (1 << k)));
            }
        }
        let (mut used, mut retired) = (0, 0);
        for (&u, x) in &self.used {
            spans.push((u, u + units(x.class)));
            used += units(x.class);
            if let Some(v) = x.retired {
                assert!(self.retired.contains(&(v, u)));
                retired += units(x.class);
            }
        }
        assert_eq!(used, self.used_units);
        assert_eq!(retired, self.retired_units);
        spans.sort_unstable();
        let mut next = 0;
        for (s, e) in spans {
            assert_eq!(s, next, "gap or overlap at unit {s}");
            next = e;
        }
        assert_eq!(next, self.frontier);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ext(unit: u64, class: u8) -> Extent {
        extent(unit, class)
    }

    #[test]
    fn empty_file_grows_with_alignment() {
        let mut a = Alloc::empty(1);
        assert_eq!(a.alloc_free(0), None);
        assert_eq!(a.alloc_grown(0), ext(1, 0));
        // Class 2 is 4 units aligned to 4: units 2-3 become a free class-1 block.
        assert_eq!(a.alloc_grown(2), ext(4, 2));
        a.check();
        assert_eq!(a.frontier(), 8);
        assert_eq!(a.alloc_free(1), Some(ext(2, 1)));
        a.check();
    }

    #[test]
    fn free_coalesces_back_to_one_block() {
        let mut a = Alloc::empty(2048);
        a.check();
        let xs: Vec<_> = (0..20).map(|_| a.alloc_free(0).unwrap()).collect();
        a.check();
        for x in xs {
            assert!(a.release_live(x));
        }
        a.check();
        assert_eq!(a.free[10].len(), 1, "units 1024..2048 whole again");
    }

    #[test]
    fn shrink_live_frees_the_upper_halves() {
        let mut a = Alloc::empty(1);
        let x = a.alloc_grown(3);
        assert_eq!(x, ext(8, 3));
        assert!(!a.shrink_live(x, 4), "cannot grow");
        assert!(a.shrink_live(x, 0));
        a.check();
        assert_eq!(a.used_units(), 1);
        assert!(a.is_live(ext(8, 0)) && !a.is_live(x));
        assert!(!a.shrink_live(x, 0), "no longer live at its old class");
        // The freed halves (9, 10-11, 12-15) are free blocks, and coalesce once the rest goes.
        assert!(a.free[0].contains(&9) && a.free[1].contains(&10) && a.free[2].contains(&12));
        assert!(a.retire(ext(8, 0), 1));
        assert!(
            !a.shrink_live(ext(8, 0), 0),
            "retired extents are not trimmed"
        );
        assert_eq!(a.reclaim(1), 1);
        a.check();
        assert_eq!(a.used_units(), 0);
        assert!(a.free[3].contains(&8), "units 8..16 whole again");
    }

    #[test]
    fn published_extents_are_neither_released_nor_shrunk() {
        let mut a = Alloc::load(32, [ext(16, 4)]).unwrap();
        let loaded = ext(16, 4);
        assert!(a.is_live(loaded) && !a.is_pending(loaded));
        assert!(!a.release_live(loaded), "loaded at open: published");
        assert!(!a.shrink_live(loaded, 0));
        let fresh = a.alloc_free(2).unwrap();
        assert!(a.is_pending(fresh));
        assert!(a.publish(fresh));
        assert!(!a.release_live(fresh), "named by a committed root");
        assert!(!a.shrink_live(fresh, 0));
        // Published extents still retire and reclaim.
        assert!(a.retire(fresh, 1) && a.retire(loaded, 1));
        assert_eq!(a.reclaim(1), 2);
        a.check();
        assert_eq!(a.used_units(), 0);
    }

    #[test]
    fn load_rejects_bad_sets() {
        assert_eq!(
            Alloc::load(8, [ext(4, 2), ext(5, 0)]).unwrap_err(),
            LoadError::Overlap
        );
        assert_eq!(Alloc::load(8, [ext(8, 0)]).unwrap_err(), LoadError::PastEnd);
        let bad = Extent {
            page: 16,
            size_class: 1,
        };
        assert_eq!(Alloc::load(8, [bad]).unwrap_err(), LoadError::Invalid);
        let a = Alloc::load(8, [ext(4, 2), ext(4, 2), ext(1, 0)]).unwrap();
        a.check();
        assert_eq!(a.used_units(), 5);
    }

    #[test]
    fn retire_then_reclaim_by_version() {
        let mut a = Alloc::empty(1);
        let x = a.alloc_grown(0);
        let y = a.alloc_grown(0);
        assert!(a.retire(x, 5));
        assert!(!a.retire(x, 6), "already retired");
        assert!(a.retire(y, 7));
        assert!(!a.release_live(x), "retired extents are not abandoned");
        assert_eq!(a.reclaim(4), 0);
        assert_eq!(a.reclaim(6), 1);
        assert!(a.is_used(y) && !a.is_used(x));
        assert_eq!(a.reclaim(u64::MAX), 1);
        a.check();
        assert_eq!(a.used_units(), 0);
    }

    #[test]
    fn truncate_drops_tail_free_blocks() {
        let mut a = Alloc::empty(1);
        let xs: Vec<_> = (0..8).map(|_| a.alloc_grown(0)).collect();
        for x in &xs[2..] {
            a.release_live(*x);
        }
        a.truncate(a.used_end());
        a.check();
        assert_eq!(a.frontier(), 3);
    }

    /// Issue #314's layout (64 KiB units): every 16-aligned hole below the 1 MiB extent at
    /// 96 holds a small extent, so it cannot move until one region is cleared.
    fn fragmented() -> (Alloc, Extent) {
        let live = [
            (2, 1),
            (8, 3),
            (16, 4),
            (32, 2),
            (36, 1),
            (38, 0),
            (39, 0),
            (40, 0),
            (42, 0),
            (44, 0),
            (46, 0),
            (48, 0),
            (52, 2),
            (64, 5),
            (96, 4),
        ]
        .map(|(u, c)| extent(u, c));
        let a = Alloc::load(112, live).unwrap();
        (a, extent(96, 4))
    }

    #[test]
    fn clearing_a_region_lets_a_large_extent_move_down() {
        let (mut a, big) = fragmented();
        assert!(a.clone().alloc_lowest(4).is_none_or(|t| t.page > big.page));
        let (reserved, occupants) = a.clear_for(big, &|_| true, 16).unwrap();
        // 32..48 could be cleared too, but 48..64 moves less: the SST at 48 and the manifest
        // log at 52, into the holes at 41 and 4..8.
        assert_eq!(occupants, vec![extent(52, 2), extent(48, 0)]);
        assert!(
            reserved
                .iter()
                .all(|e| (48 * UNIT_PAGES..64 * UNIT_PAGES).contains(&e.page))
        );
        a.check();
        for o in &occupants {
            let t = a.alloc_lowest(o.size_class).unwrap();
            assert!(t.page < 48 * UNIT_PAGES, "{t:?}");
            // Published at load: the move's commit retires it, a later reclaim frees it.
            assert!(a.retire(*o, 1));
        }
        for r in reserved {
            assert!(a.release_live(r));
        }
        a.reclaim(u64::MAX);
        let moved = a.alloc_lowest(4).unwrap();
        assert_eq!(moved, extent(48, 4));
        a.check();
    }

    #[test]
    fn a_region_with_an_occupant_that_cannot_move_is_skipped() {
        let (mut a, big) = fragmented();
        // The SST at 48 is pinned: 32..48 is cleared instead, moving more.
        let pinned = extent(48, 0);
        let (_, occupants) = a.clear_for(big, &|e| e != pinned, 16).unwrap();
        assert_eq!(occupants.len(), 8, "{occupants:?}");
        assert!(
            occupants
                .iter()
                .all(|e| (32 * UNIT_PAGES..48 * UNIT_PAGES).contains(&e.page))
        );
        a.check();
        // With the manifest snapshot at 39 pinned too, no region can be cleared.
        let (mut a, big) = fragmented();
        let used = a.used_units;
        let pinned = [extent(48, 0), extent(39, 0)];
        assert!(a.clear_for(big, &|e| !pinned.contains(&e), 16).is_none());
        assert_eq!(a.used_units, used, "nothing reserved");
        a.check();
    }

    #[test]
    fn shrink_plan_names_tail_extents() {
        let mut a = Alloc::empty(1);
        let xs: Vec<_> = (0..16).map(|_| a.alloc_grown(0)).collect();
        for x in &xs[..12] {
            a.release_live(*x);
        }
        // Four live units pack into units 1..5.
        let plan = a.shrink_plan();
        assert_eq!(plan.len(), 4);
        for e in plan {
            let moved = a.alloc_lowest(0).unwrap();
            assert!(moved.page < e.page);
            a.release_live(e);
        }
        a.truncate(a.used_end());
        a.check();
        assert_eq!(a.frontier(), 5);
    }
}

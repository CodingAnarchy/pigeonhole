//! The row cache (#404, D201): the newest visible version of each cell of small, hot family
//! rows, served to latest row reads without merging their sources.
//!
//! **The epoch protocol.** `lw` is a table of write watermarks indexed by a hash of
//! `(table, row)`. It lives in the shared-memory region ([`RowEpochs`], D281), so a reader
//! process's row cache runs the same protocol against the writer's watermarks. A shard raises its slot to the commit's seqno (`fetch_max`, Release) when it
//! applies the commit's first write to the row, before the write is inserted into the memtable
//! and so before it can become visible. A latest read takes its read point (view and seqno
//! `S`) and then loads the slot, `e` (Acquire):
//!
//! - every write to the row at or below `S` is applied, so `e` is at least its seqno;
//! - a fill stores the row it read at `S` under epoch `e`, and only if `e <= S` (a larger `e`
//!   is a write applied but not yet visible);
//! - a lookup hits only if the entry's epoch equals the slot now and the slot is at most the
//!   reader's `S`.
//!
//! Any later write to the row raises the slot above the entry's epoch, so the entry misses
//! from then on: no invalidation, and no lock on the write path. A slot shared by two rows
//! costs a false miss, never a wrong row (the cache verifies family and row).
//!
//! Flush, compaction and blob GC never change the newest visible version of a cell, and an
//! entry holds only those, so they need no epoch change. TTL does: an entry stores the
//! earliest moment one of its cells expires and misses from then on.

use std::sync::atomic::{AtomicU64, Ordering};

use std::ops::Bound;
use std::sync::Arc;

use pigeonhole_cache::{RowCache, RowHandle};
use pigeonhole_format::scan::QualifierFilter;
use pigeonhole_format::{FamilyId, Seqno, TableId, Timestamp};
use pigeonhole_shm::RowEpochTable;

use crate::Result;
use crate::read::{CellData, ReadSpec, RowData, RowSink, read_row_into};
use crate::snapshot::View;

/// Bytes of row cache per write-watermark slot.
const BYTES_PER_SLOT: usize = 256;
/// The fewest slots the watermark table has.
const MIN_SLOTS: usize = 4096;
/// The row-cache size the shared watermark table is sized for at least (D281): reader
/// processes' default cache (64 MiB) gets its slots whatever the writer's own cache is.
const MIN_TABLE_BYTES: usize = 64 << 20;

/// Slots of the shared row-epoch table for a writer whose row cache is `bytes` (D281): one
/// per 256 bytes of `max(bytes, 64 MiB)`, a power of two, at least 4,096 and at most the
/// region's limit. What the engine passes the region at open (for footprint estimates).
pub fn table_slots(bytes: usize) -> u32 {
    let slots = (bytes.max(MIN_TABLE_BYTES) / BYTES_PER_SLOT)
        .max(MIN_SLOTS)
        .next_power_of_two();
    u32::try_from(slots)
        .unwrap_or(u32::MAX)
        .min(pigeonhole_shm::MAX_ROW_EPOCH_SLOTS)
}

/// The region's write watermarks and the hash that picks a row's slot (D201, D281). A writer
/// with its row cache on raises a row's slot for every write and says so in the header
/// (`ROW_EPOCHS_MAINTAINED`); a reader caches only then (perf287: a reader caching against
/// slots nobody raises would serve stale rows for ever). With the writer's cache off the
/// commit path pays nothing (the owner's ruling, after callgrind).
pub(crate) struct RowEpochs {
    table: RowEpochTable,
    seed: u64,
}

impl std::fmt::Debug for RowEpochs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowEpochs")
            .field("slots", &self.table.slots())
            .finish()
    }
}

impl RowEpochs {
    /// The epochs of `table`, hashed with its generation's seed.
    pub fn new(table: RowEpochTable) -> Self {
        let seed = table.seed();
        Self { table, seed }
    }

    /// The row's slot hash: FORMAT §11.7's, the same in every build of every process.
    #[inline]
    fn hash(&self, table: TableId, row: &[u8]) -> u64 {
        pigeonhole_format::shm::row_epoch_hash(self.seed, table.0, row)
    }

    /// Whether the writer raises the slots (a reader caches only then).
    pub fn maintained(&self) -> bool {
        self.table.maintained()
    }

    /// A commit at `seqno` writes `row` (before the write is inserted, so before it can be
    /// visible).
    #[inline]
    #[cfg(test)]
    pub fn note_write(&self, table: TableId, row: &[u8], seqno: Seqno) {
        self.table.note_write(self.hash(table, row), seqno);
    }

    /// As [`note_write`](Self::note_write), returning the slot's value before the raise
    /// (D290's write-through: `p` below, the commit's own seqno, or above).
    #[inline]
    pub fn note_write_prev(&self, table: TableId, row: &[u8], seqno: Seqno) -> Seqno {
        self.table.note_write_prev(self.hash(table, row), seqno)
    }

    /// How many slots the table has.
    pub fn slots(&self) -> usize {
        self.table.slots()
    }
}

/// The row cache, its write watermarks and its admission rules (writer process only).
pub(crate) struct RowCaches {
    cache: RowCache,
    lw: Arc<RowEpochs>,
    /// Per slot, the tag of the family row filled last: a lookup whose tag differs misses
    /// without taking the cache's lock. A hint only: the cache itself verifies a hit.
    present: Box<[AtomicU64]>,
    /// Per slot, the family row (whatever its epoch) that missed last: a miss fills only when
    /// the row missed before, so rows read once are not stored, and a hot row that was
    /// written refills on its next read.
    ghost: Box<[AtomicU64]>,
    mask: usize,
    /// Largest encoded family row filled.
    pub max_row: usize,
    /// Whether a get that misses twice fills the row (D290's get fill); off, gets only read
    /// what is cached. For A/Bs (#1059).
    pub get_fill: bool,
    /// `(table, family)` names the cache serves; empty means every family.
    families: Vec<(String, String)>,
    hits: AtomicU64,
    misses: AtomicU64,
    fills: AtomicU64,
    /// Entries a commit moved forward (D290's write-through).
    patches: AtomicU64,
    /// The newest timestamp of any delete the file may hold (D290): the largest of the SSTs'
    /// at open and every delete applied since (replay included). A delete hides a put by
    /// timestamp whatever their seqnos (a cell delete exactly at its timestamp, a column or
    /// family delete at or below), and a cached row doesn't record the deletes behind it, so
    /// the write-through overlays only puts above this.
    delete_ceiling: AtomicU64,
}

/// What the row cache did since open ([`Engine::row_cache_stats`](crate::Engine::row_cache_stats)).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowCacheStats {
    /// Family rows and gets answered from the cache.
    pub hits: u64,
    /// Lookups that found nothing usable (absent, older epoch, expired).
    pub misses: u64,
    /// Family rows stored.
    pub fills: u64,
}

impl std::fmt::Debug for RowCaches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowCaches")
            .field("cache", &self.cache)
            .field("epochs", &self.lw)
            .field("max_row", &self.max_row)
            .field("families", &self.families)
            .finish()
    }
}

impl RowCaches {
    /// A row cache of `bytes` over the watermarks `lw`, or `None` when `bytes` is 0.
    pub fn new(
        bytes: usize,
        max_row: usize,
        families: Vec<(String, String)>,
        lw: Arc<RowEpochs>,
    ) -> Option<Self> {
        if bytes == 0 {
            return None;
        }
        let slots = lw.slots();
        let table = || (0..slots).map(|_| AtomicU64::new(0)).collect();
        Some(Self {
            cache: RowCache::new(bytes),
            lw,
            present: table(),
            ghost: table(),
            mask: slots - 1,
            max_row,
            get_fill: true,
            families,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            fills: AtomicU64::new(0),
            patches: AtomicU64::new(0),
            delete_ceiling: AtomicU64::new(0),
        })
    }

    /// The row's epoch for a read at `seqno`, loaded after the read point was taken: `None`
    /// when a write to the row is applied but not visible at `seqno` (no lookup can hit, and
    /// no fill may store).
    #[inline]
    pub fn epoch(&self, table: TableId, row: &[u8], seqno: Seqno) -> Option<RowKey> {
        let hash = self.lw.hash(table, row);
        let epoch = self.lw.table.epoch(hash, seqno)?;
        Some(RowKey { hash, epoch })
    }

    #[inline]
    fn slot(&self, tag: u64) -> usize {
        tag as usize & self.mask
    }

    /// Whether a family row that just missed should be stored: it missed before (at any
    /// epoch), until another row's miss takes the slot.
    #[inline]
    pub fn admit(&self, family: FamilyId, key: RowKey) -> bool {
        let tag = key.row_tag(family);
        self.ghost[self.slot(tag)].swap(tag, Ordering::Relaxed) == tag
    }

    /// Whether the cache serves `family` of `table` (by name, as the options list them).
    pub fn serves(&self, table: &str, family: &str) -> bool {
        self.families.is_empty() || self.families.iter().any(|(t, f)| t == table && f == family)
    }

    /// The family row at `epoch`: cached and not expired at `now`, known to be over
    /// `max_row`, or neither.
    #[inline]
    pub fn get(&self, family: FamilyId, row: &[u8], key: RowKey, now: Timestamp) -> Lookup {
        let tag = key.tag(family);
        let found = if self.present[self.slot(tag)].load(Ordering::Relaxed) != tag {
            Lookup::Miss
        } else {
            match self.cache.get(u64::from(family.0), row, key.epoch) {
                Some(h) if Encoded::deadline(&h) == Encoded::OVERSIZE => Lookup::Oversize,
                Some(h) if now < Encoded::deadline(&h) => Lookup::Hit(h),
                _ => Lookup::Miss,
            }
        };
        let counter = if matches!(found, Lookup::Hit(_)) {
            &self.hits
        } else {
            &self.misses
        };
        counter.fetch_add(1, Ordering::Relaxed);
        found
    }

    /// The counters.
    /// Drops every cached row (a closed instance releasing its memory, D265). The watermark
    /// and hint tables are fixed allocations and stay.
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Bytes the rows and their encodings take (the `memory_release` test hook).
    #[cfg(feature = "test-hooks")]
    pub fn usage(&self) -> usize {
        self.cache.usage()
    }

    pub fn stats(&self) -> RowCacheStats {
        RowCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            fills: self.fills.load(Ordering::Relaxed),
        }
    }

    /// Records that the family row at `epoch` is over `max_row`, so reads until the next
    /// write to it skip the fill instead of encoding it again.
    pub fn insert_oversize(&self, family: FamilyId, row: &[u8], key: RowKey) {
        let marker = Encoded::OVERSIZE.to_le_bytes().to_vec();
        self.store(family, row, key, marker);
    }

    /// Stores an encoded family row read at the key's epoch.
    pub fn insert(&self, family: FamilyId, row: &[u8], key: RowKey, encoded: Vec<u8>) {
        self.fills.fetch_add(1, Ordering::Relaxed);
        self.store(family, row, key, encoded);
    }

    /// A delete at `ts` is applied (or the file holds one): before it can be visible, and
    /// before any later write to its row applies (D290).
    #[inline]
    pub fn note_delete(&self, ts: Timestamp) {
        self.delete_ceiling.fetch_max(ts, Ordering::Relaxed);
    }

    /// Entries a commit moved forward since open (D290's write-through).
    pub fn patches(&self) -> u64 {
        self.patches.load(Ordering::Relaxed)
    }

    /// D290's write-through, on the shard that applied the commit, after its `apply`
    /// returned Ok and before the commit can be visible. The commit at `c` wrote `family` of
    /// `row` with `cells` only, all plain puts (`(qualifier, ts, stored)`), and the row's
    /// slot was `p < c` at the commit's first write to the row. If the family row is cached
    /// at `p`, no write to the row lies in `(p, c)` (it would have moved the slot), so the
    /// entry with `cells` overlaid is the family row at `c`: it moves to `c`. A reader hits it
    /// only at an `S >= c`, so once `c` is visible. Anything that stops the overlay
    /// (`overlay_fits`) leaves the entry stale at `p`, where it misses (the slot is at `c`)
    /// and shows, unread, that the row is written between its reads (`get_through`).
    #[allow(clippy::too_many_arguments)]
    pub fn write_through<'a>(
        &self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        p: Seqno,
        c: Seqno,
        ttl: u64,
        cells: impl Iterator<Item = (&'a [u8], Timestamp, &'a [u8])> + Clone,
    ) {
        let hash = self.lw.hash(table, row);
        let from = RowKey { hash, epoch: p }.tag(family);
        // The tag says whether a lookup at `p` could hit: two atomic loads for a row the
        // cache doesn't hold.
        if self.present[self.slot(from)].load(Ordering::Relaxed) != from {
            return;
        }
        let max_row = self.max_row;
        // Relaxed: a delete to this row applied on this shard thread, in order; another
        // row's delete only makes the bound stricter.
        let deletes = self.delete_ceiling.load(Ordering::Relaxed);
        let fits = cells.clone();
        if self.cache.update(
            u64::from(family.0),
            row,
            p,
            c,
            |encoded| overlay_fits(encoded, fits, deletes, max_row),
            |encoded| overlay(encoded, cells, ttl),
        ) {
            self.patches.fetch_add(1, Ordering::Relaxed);
            let to = RowKey { hash, epoch: c }.tag(family);
            self.present[self.slot(to)].store(to, Ordering::Relaxed);
        }
    }

    /// Drops `row`'s cached `families`: a commit applied to it after a commit with a higher
    /// seqno (D290), so a write-through by that later commit may lack this one's write.
    pub fn invalidate(&self, row: &[u8], families: impl Iterator<Item = FamilyId>) {
        for family in families {
            self.cache.invalidate(u64::from(family.0), row);
        }
    }

    fn store(&self, family: FamilyId, row: &[u8], key: RowKey, encoded: Vec<u8>) {
        let _ = self
            .cache
            .insert(u64::from(family.0), row, key.epoch, encoded);
        let tag = key.tag(family);
        self.present[self.slot(tag)].store(tag, Ordering::Relaxed);
    }
}

/// A row's hash and epoch for one read.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RowKey {
    hash: u64,
    pub epoch: u64,
}

impl RowKey {
    /// A nonzero tag for the family row at this epoch (zero is an empty slot).
    #[inline]
    fn tag(self, family: FamilyId) -> u64 {
        mix(self.hash ^ u64::from(family.0).rotate_left(32) ^ self.epoch)
    }

    /// A nonzero tag for the family row at any epoch.
    #[inline]
    fn row_tag(self, family: FamilyId) -> u64 {
        mix(self.hash ^ u64::from(family.0).rotate_left(32))
    }
}

#[inline]
fn mix(mut h: u64) -> u64 {
    h = (h ^ (h >> 33)).wrapping_mul(0xff51_afd7_ed55_8ccd);
    (h ^ (h >> 29)) | 1
}

/// A row cache lookup.
pub(crate) enum Lookup {
    /// The cached family row.
    Hit(RowHandle),
    /// The family row is over `max_row` (until the next write to the row).
    Oversize,
    /// Nothing usable.
    Miss,
}

/// An encoded family row: the earliest expiry (`u64` LE), then per cell, in read order, the
/// qualifier (`u32` LE length, bytes), the timestamp (`u64` LE) and the stored value (`u32`
/// LE length, bytes).
pub(crate) struct Encoded;

impl Encoded {
    /// The header of a marker entry: the row is over `max_row` (no real row expires at 0).
    pub const OVERSIZE: u64 = 0;

    /// A fresh encoding, expiring never until a cell says otherwise.
    #[cfg(test)]
    pub fn start(buf: &mut Vec<u8>) {
        buf.clear();
        buf.extend_from_slice(&u64::MAX.to_le_bytes());
    }

    /// Bytes a cell takes beyond its qualifier and value.
    pub const CELL_OVERHEAD: usize = 16;

    /// Appends a cell; `expires` is when it expires (`u64::MAX` for never).
    #[cfg(test)]
    pub fn push(buf: &mut Vec<u8>, qualifier: &[u8], ts: Timestamp, stored: &[u8], expires: u64) {
        let deadline = Self::deadline(buf).min(expires);
        buf[..8].copy_from_slice(&deadline.to_le_bytes());
        Self::push_cell(buf, qualifier, ts, stored);
    }

    /// Appends a cell, leaving the header alone.
    #[inline]
    pub fn push_cell(buf: &mut Vec<u8>, qualifier: &[u8], ts: Timestamp, stored: &[u8]) {
        buf.extend_from_slice(&(qualifier.len() as u32).to_le_bytes());
        buf.extend_from_slice(qualifier);
        buf.extend_from_slice(&ts.to_le_bytes());
        buf.extend_from_slice(&(stored.len() as u32).to_le_bytes());
        buf.extend_from_slice(stored);
    }

    /// When the first of the row's cells expires.
    #[inline]
    pub fn deadline(encoded: &[u8]) -> u64 {
        u64::from_le_bytes(
            encoded[..8]
                .try_into()
                .expect("an encoded row has a header"),
        )
    }

    /// The offset of the cell with `qualifier` (its qualifier length), if the row has one.
    fn find(encoded: &[u8], qualifier: &[u8]) -> Option<usize> {
        let mut at = 8;
        while at < encoded.len() {
            let qlen = u32::from_le_bytes(encoded.get(at..at + 4)?.try_into().ok()?) as usize;
            let q = encoded.get(at + 4..at + 4 + qlen)?;
            let len_at = at + 4 + qlen + 8;
            let vlen = u32::from_le_bytes(encoded.get(len_at..len_at + 4)?.try_into().ok()?);
            if q == qualifier {
                return Some(at);
            }
            at = len_at + 4 + vlen as usize;
        }
        None
    }

    /// The cells, in read order: `(qualifier, ts, stored)`.
    pub fn cells(encoded: &[u8]) -> Cells<'_> {
        Cells {
            rest: &encoded[8..],
        }
    }
}

/// Whether a commit's puts can be overlaid on a cached family row (D290's write-through),
/// without changing it: not for a marker, a put at or below `deletes` (a delete the entry
/// doesn't record could hide it), a put to a qualifier the row lacks, or a row the overlay
/// would grow past `max_row`. A refusal leaves the entry stale where it is, as any write
/// does without the write-through (D201), so a later get sees that it went unread.
fn overlay_fits<'a>(
    encoded: &[u8],
    cells: impl Iterator<Item = (&'a [u8], Timestamp, &'a [u8])>,
    deletes: Timestamp,
    max_row: usize,
) -> bool {
    if Encoded::deadline(encoded) == Encoded::OVERSIZE {
        return false;
    }
    // Sized as `overlay` applies the cells, in order: a column written twice in one commit
    // (two timestamps) is compared with what the first put left, not with the cached cell
    // (simp724's review: sizing both against the cached cell underflowed).
    let mut seen: smallvec::SmallVec<[(&[u8], Timestamp, usize); 4]> = smallvec::SmallVec::new();
    let mut len = encoded.len();
    for (qualifier, ts, stored) in cells {
        if ts <= deletes {
            return false;
        }
        let current = match seen.iter_mut().find(|(q, _, _)| *q == qualifier) {
            Some(cell) => cell,
            None => {
                let Some(at) = Encoded::find(encoded, qualifier) else {
                    return false;
                };
                let (cached, old) = cell_at(encoded, at, qualifier);
                seen.push((qualifier, cached, old));
                seen.last_mut().expect("just pushed")
            }
        };
        if current.1 <= ts {
            let Some(next) = len
                .checked_sub(current.2)
                .and_then(|l| l.checked_add(stored.len()))
            else {
                return false;
            };
            len = next;
            *current = (qualifier, ts, stored.len());
        }
    }
    len <= max_row
}

/// A cell's timestamp and value length, given its offset ([`Encoded::find`]).
fn cell_at(encoded: &[u8], at: usize, qualifier: &[u8]) -> (Timestamp, usize) {
    let ts_at = at + 4 + qualifier.len();
    let ts = u64::from_le_bytes(
        encoded[ts_at..ts_at + 8]
            .try_into()
            .expect("an encoded cell has a timestamp"),
    );
    let len = u32::from_le_bytes(
        encoded[ts_at + 8..ts_at + 12]
            .try_into()
            .expect("an encoded cell has a value length"),
    ) as usize;
    (ts, len)
}

/// Overlays a commit's puts on a cached family row that [`overlay_fits`] accepted: each
/// replaces the cell with its qualifier unless the cached version is newer (`ts`; on a tie
/// the commit, with the higher seqno, wins), and the row's expiry is recomputed.
fn overlay<'a>(
    encoded: &mut Vec<u8>,
    cells: impl Iterator<Item = (&'a [u8], Timestamp, &'a [u8])>,
    ttl: u64,
) {
    for (qualifier, ts, stored) in cells {
        let Some(at) = Encoded::find(encoded, qualifier) else {
            debug_assert!(false, "overlay_fits checked every qualifier");
            continue;
        };
        let (cached, old) = cell_at(encoded, at, qualifier);
        if cached > ts {
            continue;
        }
        let ts_at = at + 4 + qualifier.len();
        encoded[ts_at..ts_at + 8].copy_from_slice(&ts.to_le_bytes());
        let len_at = ts_at + 8;
        let value = len_at + 4..len_at + 4 + old;
        if old == stored.len() {
            encoded[value].copy_from_slice(stored);
        } else {
            encoded[len_at..len_at + 4].copy_from_slice(&(stored.len() as u32).to_le_bytes());
            encoded.splice(value, stored.iter().copied());
        }
    }
    if ttl != 0 {
        let deadline = Encoded::cells(encoded)
            .map(|(_, ts, _)| ts.saturating_add(ttl))
            .min()
            .unwrap_or(u64::MAX);
        encoded[..8].copy_from_slice(&deadline.to_le_bytes());
    }
}

/// The cells of an [`Encoded`] row.
pub(crate) struct Cells<'a> {
    rest: &'a [u8],
}

impl<'a> Iterator for Cells<'a> {
    type Item = (&'a [u8], Timestamp, &'a [u8]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.is_empty() {
            return None;
        }
        let take = |rest: &mut &'a [u8], n: usize| {
            let (a, b) = rest.split_at(n);
            *rest = b;
            a
        };
        let rest = &mut self.rest;
        let qlen = u32::from_le_bytes(take(rest, 4).try_into().ok()?) as usize;
        let qualifier = take(rest, qlen);
        let ts = u64::from_le_bytes(take(rest, 8).try_into().ok()?);
        let vlen = u32::from_le_bytes(take(rest, 4).try_into().ok()?) as usize;
        let stored = take(rest, vlen);
        Some((qualifier, ts, stored))
    }
}

/// Whether a read with `spec` may be served from the row cache: the newest version, no time
/// range. Qualifier selections, column limits and value predicates are applied to the cached
/// cells (D22: a value predicate tests the newest visible value).
#[inline]
pub(crate) fn serves_spec(spec: &ReadSpec) -> bool {
    spec.versions == 1 && spec.time_range.is_none()
}

/// Whether `spec` reads the whole family row, so its result can be stored.
fn unprojected(spec: &ReadSpec) -> bool {
    spec.qualifiers == QualifierFilter::All && spec.columns_per_row == 0 && spec.value.is_none()
}

/// A latest row read through the row cache (D201): each family is served from the cache,
/// or read as [`read_row_into`] reads it and then stored. `spec` must pass [`serves_spec`].
#[allow(clippy::too_many_arguments)]
pub(crate) fn read_row_cached(
    rc: &RowCaches,
    view: &Arc<View>,
    seqno: Seqno,
    table: TableId,
    row: &[u8],
    families: &[FamilyId],
    spec: &ReadSpec,
    now: Timestamp,
    cache_only: bool,
    sink: &mut impl RowSink,
    still_valid: impl Fn() -> bool,
) -> Result<bool> {
    let uncached = |families: &[FamilyId], sink: &mut _| {
        read_row_into(
            view, seqno, table, row, families, spec, now, cache_only, sink,
        )
    };
    // A write to the row applied but not yet visible at `seqno`: neither hit nor fill.
    let Some(key) = rc.epoch(table, row, seqno) else {
        return uncached(families, sink);
    };
    let info = if rc.families.is_empty() {
        None
    } else {
        view.catalog.table(table)
    };
    let store = unprojected(spec);
    let mut any = false;
    for family in families {
        let family = std::slice::from_ref(family);
        let id = family[0];
        let served = match info {
            None => true,
            Some(info) => info
                .families
                .iter()
                .find(|f| f.id == id)
                .is_some_and(|f| rc.serves(&info.name, &f.name)),
        };
        if !served {
            any |= uncached(family, sink)?;
            continue;
        }
        match rc.get(id, row, key, now) {
            Lookup::Hit(hit) => {
                any |= push_cached(&hit, id, spec, sink);
                continue;
            }
            Lookup::Oversize => {
                any |= uncached(family, sink)?;
                continue;
            }
            Lookup::Miss => {}
        }
        let mark = sink.cell_count();
        any |= uncached(family, sink)?;
        // `still_valid`: a reader process's snapshot still protects what it read (checked
        // after the read, seqlock style), so a row read from reclaimed memory is never
        // stored (D281). Always true in the writer process.
        if store
            && let Some(mark) = mark
            && rc.admit(id, key)
            && still_valid()
            && let Some(meta) = view.catalog.family(id)
        {
            match encode(sink, mark, meta.options.ttl_micros, rc.max_row) {
                Some(encoded) => rc.insert(id, row, key, encoded),
                None => rc.insert_oversize(id, row, key),
            }
        }
    }
    Ok(any)
}

/// A latest point get answered from a cached family row (D201), which gets consult but never
/// fill: `Some(None)` when the cached row has no such qualifier, `None` on a miss.
#[inline(never)]
#[allow(clippy::too_many_arguments, clippy::option_option)]
pub(crate) fn get_cached(
    rc: &RowCaches,
    view: &View,
    seqno: Seqno,
    now: Timestamp,
    table: TableId,
    family: FamilyId,
    row: &[u8],
    qualifier: &[u8],
) -> Option<Option<CellData>> {
    let key = rc.epoch(table, row, seqno)?;
    if !rc.families.is_empty() {
        let info = view.catalog.table(table)?;
        let f = info.families.iter().find(|f| f.id == family)?;
        if !rc.serves(&info.name, &f.name) {
            return None;
        }
    }
    let Lookup::Hit(hit) = rc.get(family, row, key, now) else {
        return None;
    };
    Some(
        Encoded::cells(&hit)
            .find(|(q, _, _)| *q == qualifier)
            .map(|(_, ts, stored)| CellData::copied(ts, stored)),
    )
}

/// A point get at `seqno` through the writer's row cache (D290): answered from the cached
/// family row under D201's hit rule, which holds at any visible `seqno` (so snapshot and
/// transaction gets too: the read point is the snapshot's), or, on a miss of a row that
/// missed before (D201's ghost tag), from the family row read whole at `seqno` and stored.
/// `Some(None)` when the row has no such qualifier; `None`: read the cell as usual.
///
/// Families with a merge operator (counters among them) are served but never filled by a
/// get: every increment moves their rows past what D290's write-through can follow, so a
/// fill would be a family-row read per get after each write.
#[inline(never)]
#[allow(clippy::too_many_arguments, clippy::option_option)]
pub(crate) fn get_through(
    rc: &RowCaches,
    view: &Arc<View>,
    seqno: Seqno,
    now: Timestamp,
    table: TableId,
    family: FamilyId,
    row: &[u8],
    qualifier: &[u8],
) -> Option<Result<Option<CellData>>> {
    let key = rc.epoch(table, row, seqno)?;
    if !rc.families.is_empty() {
        let info = view.catalog.table(table)?;
        let f = info.families.iter().find(|f| f.id == family)?;
        if !rc.serves(&info.name, &f.name) {
            return None;
        }
    }
    match rc.get(family, row, key, now) {
        Lookup::Hit(hit) => return Some(Ok(cell_of(&hit, qualifier))),
        Lookup::Oversize => return None,
        Lookup::Miss => {}
    }
    if !rc.get_fill {
        return None;
    }
    let meta = view.catalog.family(family)?;
    if meta.merge != crate::catalog::MergeKind::None {
        return None;
    }
    // A row whose last cached copy went stale unread is written between its reads (by
    // commits the write-through can't follow): refilling it on every get read the whole
    // family row each time for nothing. The skip is one-shot (simp724's review): the stale
    // copy goes with it, so the next miss fills under the ghost rule. Otherwise a copy
    // refilled and then written before any hit would refuse every later fill, leaving a hot
    // row uncached until evicted (a fill is not a read). Such a row then fills at most every
    // other get.
    let cache_family = u64::from(family.0);
    if rc.cache.stale_unused(cache_family, row, key.epoch) {
        rc.cache.invalidate(cache_family, row);
        return None;
    }
    if !rc.admit(family, key) {
        return None;
    }
    // Read only as far as the row fits the cache: past `max_row` it is oversize, and the get
    // reads its cell as usual (#1059: a wide row read whole cost a get a row read's p99).
    let mut sink = CappedFill {
        row: RowData::default(),
        len: 8,
        cap: rc.max_row,
    };
    let spec = ReadSpec {
        versions: 1,
        ..ReadSpec::default()
    };
    if let Err(e) = read_row_into(
        view,
        seqno,
        table,
        row,
        std::slice::from_ref(&family),
        &spec,
        now,
        false,
        &mut sink,
    ) {
        return Some(Err(e));
    }
    if sink.full() {
        rc.insert_oversize(family, row, key);
        return None;
    }
    match encode(&sink, 0, meta.options.ttl_micros, rc.max_row) {
        Some(encoded) => {
            let got = cell_of(&encoded, qualifier);
            rc.insert(family, row, key, encoded);
            Some(Ok(got))
        }
        None => {
            rc.insert_oversize(family, row, key);
            None
        }
    }
}

/// A get's fill (#1059): the family row as [`encode`] sizes it, full once it is past the
/// cache's row cap, so the read stops there rather than read an oversize row whole.
struct CappedFill {
    row: RowData,
    /// The row's encoded length so far, as `encode` counts it.
    len: usize,
    cap: usize,
}

impl CappedFill {
    fn count_last(&mut self) {
        if let Some(n) = self.row.cell_count()
            && let Some(i) = n.checked_sub(1)
            && let Some((qualifier, data)) = self.row.cell(i)
        {
            self.len += qualifier.len() + data.stored().len() + Encoded::CELL_OVERHEAD;
        }
    }
}

impl RowSink for CappedFill {
    fn qualifiers(&mut self) -> &mut Vec<u8> {
        self.row.qualifiers()
    }

    fn push(&mut self, family: FamilyId, qualifier: std::ops::Range<usize>, data: CellData) {
        self.row.push(family, qualifier, data);
        self.count_last();
    }

    fn push_inline(
        &mut self,
        family: FamilyId,
        qualifier: std::ops::Range<usize>,
        ts: Timestamp,
        stored: &[u8],
    ) {
        self.row.push_inline(family, qualifier, ts, stored);
        self.count_last();
    }

    fn full(&self) -> bool {
        self.len > self.cap
    }

    fn cell_count(&self) -> Option<usize> {
        self.row.cell_count()
    }

    fn cell(&self, i: usize) -> Option<(&[u8], &CellData)> {
        self.row.cell(i)
    }
}

/// The cell with `qualifier` of an encoded family row, copied.
fn cell_of(encoded: &[u8], qualifier: &[u8]) -> Option<CellData> {
    Encoded::cells(encoded)
        .find(|(q, _, _)| *q == qualifier)
        .map(|(_, ts, stored)| CellData::copied(ts, stored))
}

/// The cells `sink` holds from `mark` on, encoded, or `None` if they exceed `max_row` (or the
/// sink stops reporting them).
fn encode(sink: &impl RowSink, mark: usize, ttl: u64, max_row: usize) -> Option<Vec<u8>> {
    let end = sink.cell_count()?;
    // Sized first: an oversize row is refused before anything is copied.
    let mut len = 8;
    let mut deadline = u64::MAX;
    for i in mark..end {
        let (qualifier, data) = sink.cell(i)?;
        len += qualifier.len() + data.stored().len() + Encoded::CELL_OVERHEAD;
        if len > max_row {
            return None;
        }
        if ttl != 0 {
            deadline = deadline.min(data.timestamp().saturating_add(ttl));
        }
    }
    let mut buf = Vec::with_capacity(len);
    buf.extend_from_slice(&deadline.to_le_bytes());
    for i in mark..end {
        let (qualifier, data) = sink.cell(i)?;
        Encoded::push_cell(&mut buf, qualifier, data.timestamp(), data.stored());
    }
    Some(buf)
}

/// Pushes a cached family row's cells that `spec` selects; returns whether it pushed any.
fn push_cached(hit: &[u8], family: FamilyId, spec: &ReadSpec, sink: &mut impl RowSink) -> bool {
    let mut columns = 0;
    let mut any = false;
    for (qualifier, ts, stored) in Encoded::cells(hit) {
        if !selects(&spec.qualifiers, qualifier) {
            continue;
        }
        if spec.columns_per_row != 0 && columns == spec.columns_per_row {
            break;
        }
        if let Some(p) = &spec.value
            && !p.matches(stored)
        {
            continue;
        }
        columns += 1;
        let qualifiers = sink.qualifiers();
        let start = qualifiers.len();
        qualifiers.extend_from_slice(qualifier);
        let range = start..qualifiers.len();
        if stored.len() <= CellData::INLINE_MAX {
            sink.push_inline(family, range, ts, stored);
        } else {
            sink.push(family, range, CellData::copied(ts, stored));
        }
        any = true;
    }
    any
}

/// Whether `filter` keeps the (unescaped) `qualifier`.
fn selects(filter: &QualifierFilter, qualifier: &[u8]) -> bool {
    match filter {
        QualifierFilter::All => true,
        QualifierFilter::Prefix(p) => qualifier.starts_with(p),
        QualifierFilter::Range(lo, hi) => {
            (match lo {
                Bound::Included(b) => qualifier >= b.as_slice(),
                Bound::Excluded(b) => qualifier > b.as_slice(),
                Bound::Unbounded => true,
            }) && (match hi {
                Bound::Included(b) => qualifier <= b.as_slice(),
                Bound::Excluded(b) => qualifier < b.as_slice(),
                Bound::Unbounded => true,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 1 MiB row cache over a private region's watermarks.
    fn caches(families: Vec<(String, String)>) -> RowCaches {
        let shm = pigeonhole_shm::ShmRegion::in_memory([1; 16], &pigeonhole_shm::ShmConfig::new(1));
        let lw = Arc::new(RowEpochs::new(shm.row_epochs()));
        RowCaches::new(1 << 20, 4096, families, lw).unwrap()
    }

    #[test]
    fn an_encoded_row_round_trips_and_keeps_the_earliest_expiry() {
        let mut buf = Vec::new();
        Encoded::start(&mut buf);
        assert_eq!(Encoded::deadline(&buf), u64::MAX);
        Encoded::push(&mut buf, b"a", 7, b"\x01x", 500);
        Encoded::push(&mut buf, b"", 9, b"", u64::MAX);
        Encoded::push(&mut buf, b"long", 3, &[2; 300], 200);
        assert_eq!(Encoded::deadline(&buf), 200);
        let cells: Vec<_> = Encoded::cells(&buf).collect();
        assert_eq!(
            cells,
            vec![
                (&b"a"[..], 7, &b"\x01x"[..]),
                (&b""[..], 9, &b""[..]),
                (&b"long"[..], 3, &[2u8; 300][..]),
            ]
        );
    }

    #[test]
    fn a_capped_fill_is_full_exactly_where_encode_refuses_the_row() {
        // #1059: the fill stops reading at the first cell that takes the row past the cap,
        // and that is where `encode` refuses it (so no row that fits is cut short).
        for cap in [64usize, 200, 1000, 4096] {
            let mut sink = CappedFill {
                row: RowData::default(),
                len: 8,
                cap,
            };
            let mut stopped = None;
            for i in 0..1000usize {
                if sink.full() {
                    stopped = Some(i);
                    break;
                }
                let q = format!("q{i:04}");
                let qs = sink.qualifiers();
                let start = qs.len();
                qs.extend_from_slice(q.as_bytes());
                let end = qs.len();
                sink.push_inline(FamilyId(1), start..end, 1, &[0u8; 7]);
                assert_eq!(
                    sink.full(),
                    encode(&sink, 0, 0, cap).is_none(),
                    "cap {cap}, after cell {i}"
                );
            }
            assert!(stopped.is_some(), "cap {cap}: never full");
        }
    }

    #[test]
    fn a_write_after_a_fill_makes_the_entry_miss() {
        let rc = caches(Vec::new());
        let (t, f) = (TableId(1), FamilyId(2));
        rc.lw.note_write(t, b"r", 5);
        // A read at 10 sees the write at 5 and fills under epoch 5.
        let k = rc.epoch(t, b"r", 10).unwrap();
        assert_eq!(k.epoch, 5);
        let mut buf = Vec::new();
        Encoded::start(&mut buf);
        rc.insert(f, b"r", k, buf);
        assert!(matches!(
            rc.get(f, b"r", rc.epoch(t, b"r", 10).unwrap(), 0),
            Lookup::Hit(_)
        ));
        // A write at 11, applied: a read at 10 may neither hit nor fill; a read at 11 misses.
        rc.lw.note_write(t, b"r", 11);
        assert!(rc.epoch(t, b"r", 10).is_none());
        let k = rc.epoch(t, b"r", 11).unwrap();
        assert!(matches!(rc.get(f, b"r", k, 0), Lookup::Miss));
    }

    /// Rows sharing a watermark slot cost misses, never each other's rows (D201, D281): a
    /// one-slot table.
    #[test]
    fn rows_sharing_a_slot_miss_and_never_cross() {
        let mut config = pigeonhole_shm::ShmConfig::new(1);
        config.row_epoch_slots = 1;
        let shm = pigeonhole_shm::ShmRegion::in_memory([1; 16], &config);
        let lw = Arc::new(RowEpochs::new(shm.row_epochs()));
        let rc = RowCaches::new(1 << 20, 4096, Vec::new(), lw).unwrap();
        let (t, f) = (TableId(1), FamilyId(2));
        let fill = |row: &[u8], at: Seqno| {
            let k = rc.epoch(t, row, at).unwrap();
            let mut buf = Vec::new();
            Encoded::start(&mut buf);
            Encoded::push_cell(&mut buf, b"q", 1, row);
            rc.insert(f, row, k, buf);
        };
        fill(b"a", 10);
        fill(b"b", 10);
        let hit = |row: &[u8], at: Seqno| match rc.get(f, row, rc.epoch(t, row, at).unwrap(), 0) {
            Lookup::Hit(h) => Some(Encoded::cells(&h).next().unwrap().2.to_vec()),
            _ => None,
        };
        // `b`, filled last, hits; `a` shares `b`'s presence hint too, so it may miss, but
        // never answers with `b`'s row.
        assert_eq!(hit(b"b", 10).as_deref(), Some(&b"b"[..]));
        assert!(matches!(hit(b"a", 10).as_deref(), None | Some(b"a")));
        fill(b"a", 10);
        assert_eq!(hit(b"a", 10).as_deref(), Some(&b"a"[..]));
        // A write to `b` raises the shared slot: `a` misses too, and nothing crosses.
        rc.lw.note_write(t, b"b", 11);
        assert_eq!(hit(b"a", 11), None);
        assert_eq!(hit(b"b", 11), None);
    }

    #[test]
    fn a_row_is_admitted_on_its_second_miss() {
        let rc = caches(Vec::new());
        let (t, f) = (TableId(1), FamilyId(2));
        let k = rc.epoch(t, b"r", 1).unwrap();
        assert!(!rc.admit(f, k));
        assert!(rc.admit(f, k));
        // After a write, the row has been seen: its next miss fills.
        rc.lw.note_write(t, b"r", 2);
        let k = rc.epoch(t, b"r", 2).unwrap();
        assert!(rc.admit(f, k));
        // Another family of the row has not.
        assert!(!rc.admit(FamilyId(3), k));
    }

    #[test]
    fn an_entry_misses_once_a_cell_expires() {
        let rc = caches(Vec::new());
        let (t, f) = (TableId(1), FamilyId(1));
        let k = rc.epoch(t, b"r", 0).unwrap();
        let mut buf = Vec::new();
        Encoded::start(&mut buf);
        Encoded::push(&mut buf, b"q", 1, b"v", 100);
        rc.insert(f, b"r", k, buf);
        assert!(matches!(rc.get(f, b"r", k, 99), Lookup::Hit(_)));
        assert!(matches!(rc.get(f, b"r", k, 100), Lookup::Miss));
        rc.insert_oversize(f, b"r", k);
        assert!(matches!(rc.get(f, b"r", k, 0), Lookup::Oversize));
    }

    #[test]
    fn the_family_list_selects_by_name() {
        let all = caches(Vec::new());
        assert!(all.serves("t", "f"));
        let some = caches(vec![("t".into(), "f".into())]);
        assert!(some.serves("t", "f"));
        assert!(!some.serves("t", "g"));
        assert!(!some.serves("u", "f"));
        assert!(RowCaches::new(0, 4096, Vec::new(), Arc::clone(&all.lw)).is_none());
    }
}

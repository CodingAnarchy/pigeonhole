//! Choosing compaction work: levels, tasks and the leveled, tiered and FIFO-by-time pickers.

use std::sync::Arc;

use pigeonhole_format::key::row_prefix_len;
use pigeonhole_format::manifest::{CompactionStyle, SstMeta};
use pigeonhole_format::{BlobFileId, FamilyId, SstId, TabletId, Timestamp};

/// The SSTs of one `(tablet, family)` by level. Level 0 is ordered newest first and may
/// overlap; deeper levels are sorted by key and disjoint.
#[derive(Debug, Clone, Default)]
pub struct Levels {
    /// `levels[n]` is level `n`.
    pub levels: Vec<Vec<Arc<SstMeta>>>,
}

impl Levels {
    /// Bytes in level `n` (0 if absent).
    pub fn level_bytes(&self, n: usize) -> u64 {
        self.levels
            .get(n)
            .map_or(0, |l| l.iter().map(|s| s.len).sum())
    }
}

/// Tuning for the pickers.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PickerOptions {
    /// L0 file count that triggers an L0 compaction.
    pub l0_trigger: u32,
    /// Target size of level 1 in bytes.
    pub level_base_bytes: u64,
    /// Size ratio between adjacent levels.
    pub level_multiplier: u32,
    /// Number of levels.
    pub max_levels: u8,
    /// Target output SST size.
    pub target_sst_bytes: u64,
    /// Tiered: a merge of the L0 runs takes in the next level's run while that run is at
    /// most this many percent larger than everything taken so far.
    pub tiered_size_ratio_percent: u32,
    /// Tiered: once the runs above the oldest one hold more than this many percent of its
    /// bytes, every run is merged into the last level.
    pub tiered_max_space_amp_percent: u32,
    /// FIFO-by-time: once a family's SSTs hold more than this many bytes, the ones with the
    /// oldest newest timestamps are dropped, expired or not; 0 (the default) never drops by
    /// size.
    pub fifo_max_bytes: u64,
}

impl Default for PickerOptions {
    /// 4 L0 files, 256 MiB at L1, ×10 per level, 7 levels, 64 MiB SSTs; tiered merges take
    /// in runs up to 1% larger and cap space amplification at 200%; FIFO has no size cap.
    fn default() -> Self {
        Self {
            l0_trigger: 4,
            level_base_bytes: 256 << 20,
            level_multiplier: 10,
            max_levels: 7,
            target_sst_bytes: 64 << 20,
            tiered_size_ratio_percent: 1,
            tiered_max_space_amp_percent: 200,
            fifo_max_bytes: 0,
        }
    }
}

impl PickerOptions {
    /// Target bytes of level `n >= 1`.
    pub fn level_target(&self, n: usize) -> u64 {
        let mut t = self.level_base_bytes.max(1);
        for _ in 1..n {
            t = t.saturating_mul(u64::from(self.level_multiplier.max(1)));
        }
        t
    }
}

/// A range of internal keys, `[start, end)` in byte order. Built from row bounds with
/// `pigeonhole_format::key::encode_row_prefix`, so it never splits a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    /// Inclusive start; `None` is unbounded.
    pub start: Option<Vec<u8>>,
    /// Exclusive end; `None` is unbounded.
    pub end: Option<Vec<u8>>,
}

impl KeyRange {
    /// Every key.
    pub fn all() -> Self {
        Self {
            start: None,
            end: None,
        }
    }

    /// The intersection with `other`.
    pub fn intersect(&self, other: &KeyRange) -> KeyRange {
        let start = match (&self.start, &other.start) {
            (Some(a), Some(b)) => Some(a.max(b).clone()),
            (a, b) => a.clone().or_else(|| b.clone()),
        };
        let end = match (&self.end, &other.end) {
            (Some(a), Some(b)) => Some(a.min(b).clone()),
            (a, b) => a.clone().or_else(|| b.clone()),
        };
        KeyRange { start, end }
    }
}

/// One unit of compaction work.
///
/// The picker fills `range` with [`KeyRange::all`]; the engine narrows `range` (and
/// `subranges`) to the tablet's row range before running it, and turns a
/// [`TaskKind::TrivialMove`] of an SST shared with a sibling tablet (D13) into a
/// [`TaskKind::Rewrite`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionTask {
    /// Tablet.
    pub tablet: TabletId,
    /// Family.
    pub family: FamilyId,
    /// Keys the task may read and write: the tablet's row range. Inputs shared with a sibling
    /// tablet after a split (D13) are read only within it, and outputs never leave it.
    pub range: KeyRange,
    /// Disjoint pieces of `range`, in order, that run as independent subcompactions; one
    /// piece equal to `range` means no split.
    pub subranges: Vec<KeyRange>,
    /// Input SSTs by level.
    pub inputs: Vec<(u8, Vec<SstId>)>,
    /// Level the outputs go to.
    pub output_level: u8,
    /// How to carry it out.
    pub kind: TaskKind,
}

/// How a task changes the tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskKind {
    /// Merge inputs into new SSTs.
    Rewrite,
    /// Move one SST down a level without rewriting it (only when its key range lies inside
    /// `range` and no sibling shares it).
    TrivialMove,
    /// Drop whole SSTs (FIFO-by-time expiry): no I/O.
    Drop,
    /// Blob GC (Phase 2): rewrite the inputs, copying values still live in these blob files
    /// into new blob files, so the old ones can be dropped.
    BlobGc {
        /// Blob files to empty.
        blob_files: Vec<BlobFileId>,
    },
}

/// A picked task: inputs by level, output level and kind.
type Picked = (Vec<(u8, Vec<SstId>)>, u8, TaskKind);

/// Whether every entry of `s` has expired at `now` under `ttl_micros` (0: no TTL): its
/// newest timestamp has.
fn expired(s: &SstMeta, now: Timestamp, ttl_micros: u64) -> bool {
    ttl_micros != 0 && s.ts_range.1.saturating_add(ttl_micros) <= now
}

/// The row prefix of an internal key (the whole key if it has none).
fn row_of(key: &[u8]) -> &[u8] {
    &key[..row_prefix_len(key).unwrap_or(key.len())]
}

/// Whether `s` holds any row in `[lo, hi]` (row prefixes).
fn overlaps(s: &SstMeta, lo: &[u8], hi: &[u8]) -> bool {
    row_of(&s.smallest_key) <= hi && lo <= row_of(&s.largest_key)
}

/// Picks compaction work for one `(tablet, family)`, by the family's [`CompactionStyle`].
///
/// Leveled: L0 compacts into L1 once it holds `l0_trigger` files; level `n >= 1`
/// compacts into `n + 1` once it outgrows `level_base_bytes × level_multiplier^(n-1)`,
/// choosing the file whose rewrite costs least (fewest overlapping bytes below per byte
/// moved). Inputs and overlaps are whole rows: an SST sharing an edge row with a neighbour
/// is taken together with it, in the input level and in the level below (to a fixpoint), so
/// every row of a level moves down at once and GC always sees all of a row's data at and
/// below the input level. The task's `range` stays [`KeyRange::all`], which covers that
/// expansion; the engine narrows it only to the tablet's rows.
///
/// Tiered (universal): each L0 file and each non-empty deeper level is a sorted run, newest
/// first. Once L0 holds `l0_trigger` files, all of them merge with the following runs while
/// each is at most `tiered_size_ratio_percent` larger than what was taken so far; once the
/// runs above the oldest hold more than `tiered_max_space_amp_percent` of its bytes, every
/// run merges into the last level. Outputs go just above the first run not taken, so runs
/// stay ordered newest first down the levels, and whole runs move, so no row is split. The
/// score is the larger of L0 depth over its trigger and space amplification over its cap;
/// the write stall uses only the first ([`stall_score`](Self::stall_score)).
///
/// FIFO-by-time: whole SSTs whose newest timestamp has expired (`ts + ttl <= now`) are
/// dropped with no I/O ([`TaskKind::Drop`]); every entry in them is expired, and so is
/// everything a tombstone among them hides (a delete hides only older timestamps). Past
/// `fifo_max_bytes`, the SSTs with the oldest newest timestamps are dropped too. Otherwise,
/// once `l0_trigger` adjacent L0 files fit in `target_sst_bytes` together, they merge into
/// one L0 file, so the file count stays near the data size over the target while each file
/// still covers a short span of time.
///
/// ```
/// use std::sync::Arc;
/// use pigeonhole_compaction::{CompactionPicker, Levels, PickerOptions, TaskKind};
/// use pigeonhole_format::key::{Kind, encode_key};
/// use pigeonhole_format::manifest::{CompactionStyle, SstMeta};
/// use pigeonhole_format::superblock::ExtentRef;
/// use pigeonhole_format::{FamilyId, SstId, TabletId};
///
/// let sst = |id: u64, row: &[u8]| {
///     let mut k = Vec::new();
///     encode_key(&mut k, row, b"q", 1, id, Kind::Put).unwrap();
///     Arc::new(SstMeta {
///         id: SstId(id),
///         extent: ExtentRef { page: 16 * id, size_class: 0 },
///         len: 1000,
///         smallest_key: k.clone(),
///         largest_key: k,
///         seqno_range: (id, id),
///         ts_range: (1, 1),
///         entries: 1,
///         deletes: 0,
///     })
/// };
/// let picker = CompactionPicker::new(CompactionStyle::Leveled, PickerOptions::default());
/// let mut levels = Levels { levels: vec![vec![sst(2, b"b"), sst(1, b"a")]] };
/// assert!(picker.score(&levels) < 1.0);
/// assert!(picker.pick(TabletId(1), FamilyId(1), &levels, &[], 0, 0).is_none());
///
/// levels.levels[0].insert(0, sst(4, b"d"));
/// levels.levels[0].insert(0, sst(5, b"e"));
/// assert!(picker.score(&levels) >= 1.0);
/// let task = picker.pick(TabletId(1), FamilyId(1), &levels, &[], 0, 0).unwrap();
/// assert_eq!((task.output_level, task.kind), (1, TaskKind::Rewrite));
/// assert_eq!(task.inputs, [(0, vec![SstId(5), SstId(4), SstId(2), SstId(1)])]);
/// ```
#[derive(Debug, Clone)]
pub struct CompactionPicker {
    style: CompactionStyle,
    options: PickerOptions,
}

impl CompactionPicker {
    /// A picker for `style`.
    pub fn new(style: CompactionStyle, options: PickerOptions) -> Self {
        Self { style, options }
    }

    /// The options.
    pub fn options(&self) -> &PickerOptions {
        &self.options
    }

    /// The deepest level.
    fn last_level(&self) -> usize {
        usize::from(self.options.max_levels.max(2)) - 1
    }

    fn level_score(&self, levels: &Levels, n: usize) -> f64 {
        if n == 0 {
            let files = levels.levels.first().map_or(0, Vec::len);
            files as f64 / f64::from(self.options.l0_trigger.max(1))
        } else if n < self.last_level() {
            levels.level_bytes(n) as f64 / self.options.level_target(n) as f64
        } else {
            0.0
        }
    }

    /// The L0 write stall's score (D119): L0 depth over `l0_trigger`, so writers are paced
    /// only while flushes outrun compaction (never for a deeper level, tiered space
    /// amplification or FIFO expiry, which only drive picking). FIFO keeps its files in L0,
    /// so its depth is the longest window of small L0 files a merge would relieve.
    pub fn stall_score(&self, levels: &Levels) -> f64 {
        match self.style {
            CompactionStyle::Leveled | CompactionStyle::Tiered => self.level_score(levels, 0),
            CompactionStyle::FifoByTime => {
                self.fifo_window(levels, &[]).1 as f64 / self.fifo_trigger() as f64
            }
        }
    }

    /// Tiered: how far the runs above the oldest one exceed the space-amplification cap
    /// (`>= 1.0` once they hold more than `tiered_max_space_amp_percent` of its bytes). The
    /// runs are the L0 files and each non-empty deeper level; with fewer than two there is
    /// nothing to merge.
    fn space_amp_score(&self, levels: &Levels) -> f64 {
        let mut runs = levels.levels.first().map_or(0, Vec::len);
        let mut oldest = levels
            .levels
            .first()
            .and_then(|l0| l0.last())
            .map_or(0, |s| s.len);
        let mut total = levels.level_bytes(0);
        for n in 1..levels.levels.len() {
            let bytes = levels.level_bytes(n);
            if !levels.levels[n].is_empty() {
                runs += 1;
                oldest = bytes;
                total += bytes;
            }
        }
        if runs < 2 {
            return 0.0;
        }
        let amp = (total - oldest) as f64 * 100.0 / oldest.max(1) as f64;
        amp / f64::from(self.options.tiered_max_space_amp_percent.max(1))
    }

    /// FIFO: the longest run of adjacent L0 files, none of them `busy`, whose bytes fit in
    /// `target_sst_bytes` together, as `(start, len)`; the newest on ties.
    fn fifo_window(&self, levels: &Levels, busy: &[SstId]) -> (usize, usize) {
        let Some(l0) = levels.levels.first() else {
            return (0, 0);
        };
        let target = self.options.target_sst_bytes;
        let mut best = (0, 0);
        let (mut start, mut bytes) = (0, 0u64);
        for (i, s) in l0.iter().enumerate() {
            if busy.contains(&s.id) {
                // A busy file splits the windows: one cannot cross it.
                (start, bytes) = (i + 1, 0);
                continue;
            }
            bytes += s.len;
            while bytes > target && start <= i {
                bytes -= l0[start].len;
                start += 1;
            }
            if i + 1 - start > best.1 {
                best = (start, i + 1 - start);
            }
        }
        best
    }

    /// FIFO: the L0 file count that starts a merge (at least two: one file merges into
    /// itself).
    fn fifo_trigger(&self) -> usize {
        self.options.l0_trigger.max(2) as usize
    }

    /// FIFO-by-time: when the next SST expires (the earliest `ts_range.1 + ttl_micros` of
    /// the family's SSTs, possibly already past), or `None` for other styles, without a TTL
    /// or without SSTs. The engine arms a timer for it, so an idle family drops expired SSTs
    /// without waiting for its next flush.
    pub fn next_expiry(&self, levels: &Levels, ttl_micros: u64) -> Option<Timestamp> {
        if self.style != CompactionStyle::FifoByTime || ttl_micros == 0 {
            return None;
        }
        levels
            .levels
            .iter()
            .flatten()
            .map(|s| s.ts_range.1.saturating_add(ttl_micros))
            .min()
    }

    /// Urgency: `>= 1.0` means compaction is due. The engine services the highest score
    /// first; it throttles writes on [`stall_score`](Self::stall_score). The same as
    /// [`score_at`](Self::score_at) with no expiry.
    pub fn score(&self, levels: &Levels) -> f64 {
        self.score_at(levels, 0, 0)
    }

    /// [`score`](Self::score) at time `now` for a family whose TTL is `ttl_micros` (0:
    /// none). FIFO-by-time scores at least 1.0 while an SST has expired or its bytes exceed
    /// `fifo_max_bytes`, exactly when [`pick`](Self::pick) with the same `now` has work
    /// unless that work's SSTs are busy (as for every style: the score does not see `busy`,
    /// and the engine moves on to the next due slot). The other styles ignore the time.
    pub fn score_at(&self, levels: &Levels, now: Timestamp, ttl_micros: u64) -> f64 {
        match self.style {
            CompactionStyle::Leveled => (0..self.last_level())
                .map(|n| self.level_score(levels, n))
                .fold(0.0, f64::max),
            CompactionStyle::Tiered => self
                .level_score(levels, 0)
                .max(self.space_amp_score(levels)),
            CompactionStyle::FifoByTime => {
                let mut score = self.fifo_window(levels, &[]).1 as f64 / self.fifo_trigger() as f64;
                if levels
                    .levels
                    .iter()
                    .flatten()
                    .any(|s| expired(s, now, ttl_micros))
                {
                    score = score.max(1.0);
                }
                let cap = self.options.fifo_max_bytes;
                let total: u64 = (0..levels.levels.len())
                    .map(|n| levels.level_bytes(n))
                    .sum();
                // Due only past the cap, where `pick` drops something.
                if cap != 0 && total > cap {
                    score = score.max((total as f64 / cap as f64).max(1.0));
                }
                score
            }
        }
    }

    /// The next task, or `None`. `busy` lists SSTs already in a running job. `now` and the
    /// family's `ttl_micros` (0: none) drive FIFO expiry.
    pub fn pick(
        &self,
        tablet: TabletId,
        family: FamilyId,
        levels: &Levels,
        busy: &[SstId],
        now: Timestamp,
        ttl_micros: u64,
    ) -> Option<CompactionTask> {
        let (inputs, output_level, kind) = match self.style {
            CompactionStyle::Leveled => self.pick_leveled(levels, busy)?,
            CompactionStyle::Tiered => self.pick_tiered(levels, busy)?,
            CompactionStyle::FifoByTime => self.pick_fifo(levels, busy, now, ttl_micros)?,
        };
        Some(CompactionTask {
            tablet,
            family,
            range: KeyRange::all(),
            subranges: vec![KeyRange::all()],
            inputs,
            output_level,
            kind,
        })
    }

    /// Leveled: the most urgent level's best task.
    fn pick_leveled(&self, levels: &Levels, busy: &[SstId]) -> Option<Picked> {
        let mut due: Vec<(f64, usize)> = (0..self.last_level())
            .map(|n| (self.level_score(levels, n), n))
            .filter(|&(s, _)| s >= 1.0)
            .collect();
        due.sort_by(|a, b| b.0.total_cmp(&a.0));
        due.into_iter().find_map(|(_, n)| {
            let (inputs, below, kind) = if n == 0 {
                self.pick_l0(levels, busy)?
            } else {
                self.pick_level(levels, n, busy)?
            };
            let mut by_level = vec![(n as u8, inputs)];
            if !below.is_empty() {
                by_level.push((n as u8 + 1, below));
            }
            Some((by_level, n as u8 + 1, kind))
        })
    }

    /// FIFO-by-time: drop every expired SST (and, past the size cap, the oldest others), or
    /// else merge the longest window of small adjacent L0 files into L0.
    fn pick_fifo(
        &self,
        levels: &Levels,
        busy: &[SstId],
        now: Timestamp,
        ttl_micros: u64,
    ) -> Option<Picked> {
        let mut dropped: Vec<Vec<SstId>> = levels
            .levels
            .iter()
            .map(|files| {
                files
                    .iter()
                    .filter(|s| expired(s, now, ttl_micros) && !busy.contains(&s.id))
                    .map(|s| s.id)
                    .collect()
            })
            .collect();
        let cap = self.options.fifo_max_bytes;
        let mut left: u64 = levels
            .levels
            .iter()
            .zip(&dropped)
            .flat_map(|(files, gone)| files.iter().filter(|s| !gone.contains(&s.id)))
            .map(|s| s.len)
            .sum();
        if cap != 0 && left > cap {
            // Oldest data first: by newest timestamp, then newest seqno.
            let mut rest: Vec<(usize, &Arc<SstMeta>)> = levels
                .levels
                .iter()
                .enumerate()
                .flat_map(|(n, files)| files.iter().map(move |s| (n, s)))
                .filter(|(n, s)| !dropped[*n].contains(&s.id) && !busy.contains(&s.id))
                .collect();
            rest.sort_by_key(|(_, s)| (s.ts_range.1, s.seqno_range.1, s.id.0));
            for (n, s) in rest {
                if left <= cap {
                    break;
                }
                dropped[n].push(s.id);
                left -= s.len;
            }
        }
        let dropped: Vec<(u8, Vec<SstId>)> = dropped
            .into_iter()
            .enumerate()
            .filter(|(_, ids)| !ids.is_empty())
            .map(|(n, ids)| (n as u8, ids))
            .collect();
        if !dropped.is_empty() {
            return Some((dropped, 0, TaskKind::Drop));
        }
        // The longest window of files not busy, so a busy file does not hold up a merge
        // elsewhere in L0 (#232).
        let (start, len) = self.fifo_window(levels, busy);
        if len < self.fifo_trigger() {
            return None;
        }
        let window = &levels.levels.first()?[start..start + len];
        Some((
            vec![(0, window.iter().map(|s| s.id).collect())],
            0,
            TaskKind::Rewrite,
        ))
    }

    /// Tiered: every L0 file, plus the following level runs while each is at most
    /// `tiered_size_ratio_percent` larger than everything taken so far (level 1 always, so
    /// the output has a level above the next run to go to), or every run once space
    /// amplification passes its cap. The output goes just above the first run not taken, or
    /// to the last level. Whole runs move, so rows are never split.
    fn pick_tiered(&self, levels: &Levels, busy: &[SstId]) -> Option<Picked> {
        let full = self.space_amp_score(levels) >= 1.0;
        if !full && self.level_score(levels, 0) < 1.0 {
            return None;
        }
        let ids = |files: &[Arc<SstMeta>]| files.iter().map(|s| s.id).collect::<Vec<_>>();
        let mut inputs = Vec::new();
        if let Some(l0) = levels.levels.first().filter(|l| !l.is_empty()) {
            inputs.push((0u8, ids(l0)));
        }
        let ratio = u128::from(self.options.tiered_size_ratio_percent);
        let mut taken = levels.level_bytes(0);
        let mut next = None;
        for (n, files) in levels.levels.iter().enumerate().skip(1) {
            if files.is_empty() {
                continue;
            }
            let bytes = levels.level_bytes(n);
            if full || n == 1 || u128::from(bytes) * 100 <= u128::from(taken) * (100 + ratio) {
                inputs.push((n as u8, ids(files)));
                taken += bytes;
            } else {
                next = Some(n);
                break;
            }
        }
        if inputs
            .iter()
            .flat_map(|(_, ids)| ids)
            .any(|id| busy.contains(id))
        {
            return None;
        }
        let deepest = usize::from(inputs.last()?.0);
        let output = next.map_or(self.last_level().max(deepest), |n| n - 1);
        let files: usize = inputs.iter().map(|(_, ids)| ids.len()).sum();
        let kind = if files == 1 && deepest != output {
            TaskKind::TrivialMove
        } else {
            TaskKind::Rewrite
        };
        Some((inputs, output as u8, kind))
    }

    /// Files of `level` holding rows in `[lo, hi]`, expanded to a clean cut (neighbours
    /// sharing an edge row come along, to a fixpoint), or `None` if one is busy. Without the
    /// expansion a row split across two SSTs of the level could be rewritten half at a time,
    /// and a bottommost run could purge a family marker that still hides the other half.
    fn overlapping(
        levels: &Levels,
        level: usize,
        lo: &[u8],
        hi: &[u8],
        busy: &[SstId],
    ) -> Option<(Vec<SstId>, u64)> {
        let Some(files) = levels.levels.get(level) else {
            return Some((Vec::new(), 0));
        };
        // Deeper levels are sorted and disjoint, so the overlap is one contiguous run.
        let Some(mut first) = files.iter().position(|s| overlaps(s, lo, hi)) else {
            return Some((Vec::new(), 0));
        };
        let mut last = files.iter().rposition(|s| overlaps(s, lo, hi))?;
        while first > 0
            && row_of(&files[first - 1].largest_key) == row_of(&files[first].smallest_key)
        {
            first -= 1;
        }
        while last + 1 < files.len()
            && row_of(&files[last].largest_key) == row_of(&files[last + 1].smallest_key)
        {
            last += 1;
        }
        let run = &files[first..=last];
        if run.iter().any(|s| busy.contains(&s.id)) {
            return None;
        }
        Some((
            run.iter().map(|s| s.id).collect(),
            run.iter().map(|s| s.len).sum(),
        ))
    }

    fn pick_l0(
        &self,
        levels: &Levels,
        busy: &[SstId],
    ) -> Option<(Vec<SstId>, Vec<SstId>, TaskKind)> {
        let l0 = levels.levels.first()?;
        if l0.is_empty() || l0.iter().any(|s| busy.contains(&s.id)) {
            return None;
        }
        let lo = l0.iter().map(|s| row_of(&s.smallest_key)).min()?;
        let hi = l0.iter().map(|s| row_of(&s.largest_key)).max()?;
        let (below, _) = Self::overlapping(levels, 1, lo, hi, busy)?;
        let kind = if l0.len() == 1 && below.is_empty() {
            TaskKind::TrivialMove
        } else {
            TaskKind::Rewrite
        };
        Some((l0.iter().map(|s| s.id).collect(), below, kind))
    }

    fn pick_level(
        &self,
        levels: &Levels,
        n: usize,
        busy: &[SstId],
    ) -> Option<(Vec<SstId>, Vec<SstId>, TaskKind)> {
        let files = levels.levels.get(n)?;
        let mut best: Option<(f64, usize, usize, Vec<SstId>)> = None;
        let mut i = 0;
        while i < files.len() {
            // Expand to a clean cut: neighbours sharing an edge row come along.
            let start = i;
            let mut end = i;
            while end + 1 < files.len()
                && row_of(&files[end].largest_key) == row_of(&files[end + 1].smallest_key)
            {
                end += 1;
            }
            i = end + 1;
            let group = &files[start..=end];
            if group.iter().any(|s| busy.contains(&s.id)) {
                continue;
            }
            let lo = row_of(&group[0].smallest_key);
            let hi = row_of(&group[group.len() - 1].largest_key);
            let Some((below, below_bytes)) = Self::overlapping(levels, n + 1, lo, hi, busy) else {
                continue;
            };
            let bytes: u64 = group.iter().map(|s| s.len).sum();
            let ratio = below_bytes as f64 / bytes.max(1) as f64;
            if best.as_ref().is_none_or(|b| ratio < b.0) {
                best = Some((ratio, start, end, below));
            }
        }
        let (_, start, end, below) = best?;
        let kind = if start == end && below.is_empty() {
            TaskKind::TrivialMove
        } else {
            TaskKind::Rewrite
        };
        Some((
            files[start..=end].iter().map(|s| s.id).collect(),
            below,
            kind,
        ))
    }
}

//! Choosing compaction work: levels, tasks and the leveled and tiered pickers.

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
}

impl Default for PickerOptions {
    /// 4 L0 files, 256 MiB at L1, ×10 per level, 7 levels, 64 MiB SSTs; tiered merges take
    /// in runs up to 1% larger and cap space amplification at 200%.
    fn default() -> Self {
        Self {
            l0_trigger: 4,
            level_base_bytes: 256 << 20,
            level_multiplier: 10,
            max_levels: 7,
            target_sst_bytes: 64 << 20,
            tiered_size_ratio_percent: 1,
            tiered_max_space_amp_percent: 200,
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
/// score is the larger of L0 depth over its trigger and space amplification over its cap,
/// so the engine's L0 write stall works as with leveled.
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
    /// A picker for `style`. `FifoByTime` returns no work yet.
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

    /// Urgency: `>= 1.0` means compaction is due. The engine services the highest score
    /// first and throttles writes on L0 depth.
    pub fn score(&self, levels: &Levels) -> f64 {
        match self.style {
            CompactionStyle::Leveled => (0..self.last_level())
                .map(|n| self.level_score(levels, n))
                .fold(0.0, f64::max),
            CompactionStyle::Tiered => self
                .level_score(levels, 0)
                .max(self.space_amp_score(levels)),
            CompactionStyle::FifoByTime => 0.0,
        }
    }

    /// The next task, or `None`. `busy` lists SSTs already in a running job. `now` drives
    /// FIFO expiry.
    pub fn pick(
        &self,
        tablet: TabletId,
        family: FamilyId,
        levels: &Levels,
        busy: &[SstId],
        now: Timestamp,
        ttl_micros: u64,
    ) -> Option<CompactionTask> {
        // FIFO-by-time (which uses `now` and the TTL) is not implemented yet.
        let _ = (now, ttl_micros);
        let (inputs, output_level, kind) = match self.style {
            CompactionStyle::Leveled => self.pick_leveled(levels, busy)?,
            CompactionStyle::Tiered => self.pick_tiered(levels, busy)?,
            CompactionStyle::FifoByTime => return None,
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

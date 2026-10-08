//! The leveled, tiered and FIFO-by-time pickers under random flush workloads.
#![allow(clippy::field_reassign_with_default)]

mod common;

use std::sync::Arc;

use common::cases;
use pigeonhole_compaction::{CompactionPicker, Levels, PickerOptions, TaskKind};
use pigeonhole_format::key::{Kind, encode_key, row_prefix_len};
use pigeonhole_format::manifest::{CompactionStyle, SstMeta};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{FamilyId, SstId, TabletId};
use pigeonhole_sim::Rng;
use proptest::prelude::*;

const ROWS: u64 = 100_000;

fn row_key(row: u64, qualifier: &[u8]) -> Vec<u8> {
    let mut k = Vec::new();
    encode_key(
        &mut k,
        format!("r{row:08}").as_bytes(),
        qualifier,
        1,
        1,
        Kind::Put,
    )
    .unwrap();
    k
}

fn row_num(key: &[u8]) -> u64 {
    let n = row_prefix_len(key).unwrap();
    std::str::from_utf8(&key[1..n - 2])
        .unwrap()
        .parse()
        .unwrap()
}

fn sst(id: &mut u64, lo: u64, hi: u64, len: u64) -> Arc<SstMeta> {
    sst_cut(id, (lo, b"a"), (hi, b"z"), len)
}

/// An SST from `(row, qualifier)` to `(row, qualifier)`: two SSTs can share an edge row,
/// one ending at qualifier `m` and the next starting at `n`.
fn sst_cut(id: &mut u64, lo: (u64, &[u8]), hi: (u64, &[u8]), len: u64) -> Arc<SstMeta> {
    *id += 1;
    Arc::new(SstMeta {
        id: SstId(*id),
        extent: ExtentRef {
            page: 16,
            size_class: 0,
        },
        len,
        smallest_key: row_key(lo.0, lo.1),
        largest_key: row_key(hi.0, hi.1),
        seqno_range: (*id, *id),
        ts_range: (1, 1),
        entries: len / 100,
        deletes: 0,
    })
}

/// Applies a task the way a job would: inputs out, their rows rewritten into target-sized
/// SSTs at the output level (a fraction of the bytes is garbage collected).
fn apply(
    levels: &mut Levels,
    inputs: &[(u8, Vec<SstId>)],
    out: u8,
    kind: &TaskKind,
    target: u64,
    id: &mut u64,
    rng: &mut Rng,
) {
    assert_clean_cut(levels, inputs);
    rewrite(levels, inputs, out, kind, target, id, rng);
}

/// Inputs out, their rows rewritten into target-sized SSTs at the output level, keeping the
/// inputs' seqno range.
fn rewrite(
    levels: &mut Levels,
    inputs: &[(u8, Vec<SstId>)],
    out: u8,
    kind: &TaskKind,
    target: u64,
    id: &mut u64,
    rng: &mut Rng,
) {
    let mut taken = Vec::new();
    for (level, ids) in inputs {
        let l = &mut levels.levels[*level as usize];
        for sid in ids {
            let i = l.iter().position(|s| s.id == *sid).expect("input exists");
            taken.push(l.remove(i));
        }
    }
    let out = out as usize;
    if levels.levels.len() <= out {
        levels.levels.resize(out + 1, Vec::new());
    }
    let seqnos = (
        taken.iter().map(|s| s.seqno_range.0).min().unwrap(),
        taken.iter().map(|s| s.seqno_range.1).max().unwrap(),
    );
    let new: Vec<Arc<SstMeta>> = if *kind == TaskKind::TrivialMove {
        taken
    } else {
        let lo = taken
            .iter()
            .map(|s| row_num(&s.smallest_key))
            .min()
            .unwrap();
        let hi = taken.iter().map(|s| row_num(&s.largest_key)).max().unwrap();
        let bytes: u64 = taken.iter().map(|s| s.len).sum::<u64>() * 9 / 10;
        let n = bytes.div_ceil(target).clamp(1, hi - lo + 1);
        // Chunks sometimes share an edge row (a row too big for one SST is split).
        let mut start = (lo, &b"a"[..]);
        (0..n)
            .map(|i| {
                let b = lo + (hi - lo + 1) * (i + 1) / n - 1;
                let shared = i + 1 < n && rng.below(3) == 0;
                let end = if shared {
                    (b + 1, &b"m"[..])
                } else {
                    (b, &b"z"[..])
                };
                let mut s = (*sst_cut(id, start, end, bytes / n)).clone();
                s.seqno_range = seqnos;
                start = if shared {
                    (b + 1, &b"n"[..])
                } else {
                    (b + 1, &b"a"[..])
                };
                Arc::new(s)
            })
            .collect()
    };
    let l = &mut levels.levels[out];
    l.extend(new);
    l.sort_by(|a, b| a.smallest_key.cmp(&b.smallest_key));
    for w in l.windows(2) {
        assert!(w[0].largest_key < w[1].smallest_key, "level {out} overlaps");
    }
}

/// First and last row of an SST.
type RowRange = (Vec<u8>, Vec<u8>);

fn row_of(key: &[u8]) -> &[u8] {
    &key[..row_prefix_len(key).unwrap()]
}

/// No SST left behind in the input level shares a row with a taken one of that level, and
/// none left in the level below shares a row with a taken one of either level.
fn assert_clean_cut(levels: &Levels, inputs: &[(u8, Vec<SstId>)]) {
    let top = inputs[0].0 as usize;
    let taken = |s: &SstMeta| inputs.iter().any(|(_, ids)| ids.contains(&s.id));
    let rows_of = |levels_: &[usize]| -> Vec<RowRange> {
        levels_
            .iter()
            .flat_map(|&l| levels.levels[l].iter())
            .filter(|s| taken(s))
            .map(|s| {
                (
                    row_of(&s.smallest_key).to_vec(),
                    row_of(&s.largest_key).to_vec(),
                )
            })
            .collect()
    };
    let checks: Vec<(usize, Vec<RowRange>)> = if top == 0 {
        vec![(1, rows_of(&[0, 1]))]
    } else {
        vec![(top, rows_of(&[top])), (top + 1, rows_of(&[top, top + 1]))]
    };
    for (level, rows) in checks {
        for s in levels.levels[level].iter().filter(|s| !taken(s)) {
            let (lo, hi) = (row_of(&s.smallest_key), row_of(&s.largest_key));
            assert!(
                rows.iter()
                    .all(|(a, b)| hi < a.as_slice() || b.as_slice() < lo),
                "level {level}: SST {:?} shares rows with the inputs {inputs:?}",
                s.id
            );
        }
    }
}

fn check(seed: u64, flushes: usize) {
    let mut rng = Rng::new(seed);
    let mut options = PickerOptions::default();
    options.l0_trigger = 4;
    options.level_base_bytes = 10 << 20;
    options.level_multiplier = 4 + rng.below(7) as u32;
    options.max_levels = 5;
    options.target_sst_bytes = 2 << 20;
    let picker = CompactionPicker::new(CompactionStyle::Leveled, options.clone());
    let mut levels = Levels::default();
    levels
        .levels
        .resize(options.max_levels as usize, Vec::new());
    let mut id = 0;
    let mut compactions = 0;
    let mut flushed = 0u64;
    for _ in 0..flushes {
        // A flush: a random row range, 1–3 MiB.
        let lo = rng.below(ROWS);
        let hi = (lo + rng.below(ROWS / 2)).min(ROWS - 1);
        let len = (1 << 20) + rng.below(2 << 20);
        flushed += len;
        let s = sst(&mut id, lo, hi, len);
        levels.levels[0].insert(0, s);
        let mut guard = 0;
        while picker.score(&levels) >= 1.0 {
            let task = picker
                .pick(TabletId(1), FamilyId(1), &levels, &[], 0, 0)
                .unwrap_or_else(|| panic!("seed {seed}: score >= 1 but no task"));
            apply(
                &mut levels,
                &task.inputs,
                task.output_level,
                &task.kind,
                options.target_sst_bytes,
                &mut id,
                &mut rng,
            );
            compactions += 1;
            guard += 1;
            assert!(guard < 1000, "seed {seed}: compaction does not converge");
        }
        // Within the fan-out after every flush: L0 below its trigger, every level within its
        // target, except the last.
        assert!(
            (levels.levels[0].len() as u32) < options.l0_trigger,
            "seed {seed}"
        );
        for n in 1..options.max_levels as usize - 1 {
            assert!(
                levels.level_bytes(n) <= options.level_target(n),
                "seed {seed}: level {n} holds {} > {}",
                levels.level_bytes(n),
                options.level_target(n)
            );
        }
    }
    let total: u64 = (0..levels.levels.len())
        .map(|n| levels.level_bytes(n))
        .sum();
    assert!(total <= flushed, "seed {seed}");
    assert!(compactions > 0 || flushes < 4);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases(64)))]

    /// Done-when (4): level sizes stay within the target fan-out under random flushes.
    #[test]
    fn leveled_picker_keeps_levels_within_fan_out(seed in any::<u64>()) {
        check(seed, if cfg!(miri) { 20 } else { 300 });
    }
}

/// The sorted runs, newest first: each L0 file, then each non-empty deeper level, as
/// `(level, bytes, seqno range)`.
fn runs(levels: &Levels) -> Vec<(usize, u64, (u64, u64))> {
    let mut runs: Vec<_> = levels.levels[0]
        .iter()
        .map(|s| (0, s.len, s.seqno_range))
        .collect();
    for (n, files) in levels.levels.iter().enumerate().skip(1) {
        if !files.is_empty() {
            let lo = files.iter().map(|s| s.seqno_range.0).min().unwrap();
            let hi = files.iter().map(|s| s.seqno_range.1).max().unwrap();
            runs.push((n, levels.level_bytes(n), (lo, hi)));
        }
    }
    runs
}

fn check_tiered(seed: u64, flushes: usize) {
    let mut rng = Rng::new(seed);
    let mut options = PickerOptions::default();
    options.l0_trigger = 2 + rng.below(4) as u32;
    options.max_levels = 3 + rng.below(5) as u8;
    options.target_sst_bytes = 2 << 20;
    options.tiered_size_ratio_percent = rng.below(50) as u32;
    options.tiered_max_space_amp_percent = 50 + rng.below(250) as u32;
    let what = format!("seed {seed}: {options:?}");
    let picker = CompactionPicker::new(CompactionStyle::Tiered, options.clone());
    let mut levels = Levels::default();
    levels
        .levels
        .resize(options.max_levels as usize, Vec::new());
    let last = options.max_levels as usize - 1;
    let mut id = 0;
    for _ in 0..flushes {
        let lo = rng.below(ROWS);
        let hi = (lo + rng.below(ROWS / 2)).min(ROWS - 1);
        let len = (1 << 20) + rng.below(2 << 20);
        let s = sst(&mut id, lo, hi, len);
        levels.levels[0].insert(0, s);
        let mut guard = 0;
        while picker.score(&levels) >= 1.0 {
            let task = picker
                .pick(TabletId(1), FamilyId(1), &levels, &[], 0, 0)
                .unwrap_or_else(|| panic!("{what}: score >= 1 but no task"));
            // Whole runs, newest first, and the output lies between the deepest input and
            // the first run left out.
            let before = runs(&levels);
            let taken: Vec<usize> = task
                .inputs
                .iter()
                .flat_map(|(l, ids)| {
                    let files = &levels.levels[*l as usize];
                    assert_eq!(ids.len(), files.len(), "{what}: level {l} not whole");
                    let n = if *l == 0 { ids.len() } else { 1 };
                    std::iter::repeat_n(*l as usize, n)
                })
                .collect();
            let in_runs = taken.len();
            assert_eq!(
                taken,
                before[..in_runs].iter().map(|r| r.0).collect::<Vec<_>>(),
                "{what}: not the newest runs"
            );
            let deepest = task.inputs.last().unwrap().0;
            assert!(task.output_level >= deepest.max(1), "{what}: {task:?}");
            if let Some(next) = before.get(in_runs) {
                assert_eq!(task.output_level as usize, next.0 - 1, "{what}: {task:?}");
            } else {
                assert_eq!(task.output_level as usize, last, "{what}: {task:?}");
            }
            rewrite(
                &mut levels,
                &task.inputs,
                task.output_level,
                &task.kind,
                options.target_sst_bytes,
                &mut id,
                &mut rng,
            );
            guard += 1;
            assert!(guard < 100, "{what}: compaction does not converge");
        }
        // Within bounds after every flush: L0 below its trigger, at most one run per deeper
        // level, space amplification within its cap, and runs ordered newest first.
        let r = runs(&levels);
        assert!(
            (levels.levels[0].len() as u32) < options.l0_trigger,
            "{what}"
        );
        assert!(r.len() < options.l0_trigger as usize + last, "{what}");
        if r.len() >= 2 {
            let oldest = r.last().unwrap().1;
            let above: u64 = r[..r.len() - 1].iter().map(|r| r.1).sum();
            assert!(
                above * 100 <= oldest * u64::from(options.tiered_max_space_amp_percent),
                "{what}: space amplification {above}/{oldest}"
            );
        }
        for w in r.windows(2) {
            assert!(w[0].2.0 > w[1].2.1, "{what}: runs out of order: {r:?}");
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases(64)))]

    /// Issue #31: the run count and space amplification stay within their bounds under
    /// random flushes, and runs stay ordered newest first down the levels.
    #[test]
    fn tiered_picker_keeps_runs_and_space_amp_within_bounds(seed in any::<u64>()) {
        check_tiered(seed, if cfg!(miri) { 20 } else { 300 });
    }
}

#[test]
fn tiered_merges_l0_with_runs_of_similar_size() {
    let mut id = 0;
    let mut options = PickerOptions::default();
    options.l0_trigger = 2;
    options.max_levels = 5;
    options.tiered_size_ratio_percent = 10;
    let picker = CompactionPicker::new(CompactionStyle::Tiered, options);
    let pick = |levels: &Levels, busy: &[SstId]| {
        picker
            .pick(TabletId(1), FamilyId(1), levels, busy, 0, 0)
            .map(|t| (t.inputs, t.output_level, t.kind))
    };
    // No levels yet: two L0 files go to the last level.
    let mut levels = Levels {
        levels: vec![vec![sst(&mut id, 0, 9, 100), sst(&mut id, 5, 20, 100)]],
    };
    assert!(picker.score(&levels) >= 1.0);
    assert_eq!(
        pick(&levels, &[]),
        Some((vec![(0, vec![SstId(1), SstId(2)])], 4, TaskKind::Rewrite))
    );
    assert_eq!(pick(&levels, &[SstId(2)]), None);
    // L0 (200 bytes) takes in L2 (210, within 10%) but not L4 (1000); the output goes just
    // above L4.
    levels.levels.resize(5, Vec::new());
    levels.levels[2] = vec![sst(&mut id, 0, 50, 210)];
    levels.levels[4] = vec![sst(&mut id, 0, 99, 1000)];
    assert_eq!(
        pick(&levels, &[]),
        Some((
            vec![(0, vec![SstId(1), SstId(2)]), (2, vec![SstId(3)])],
            3,
            TaskKind::Rewrite
        ))
    );
    // A larger L2 stays: the merge goes to L1.
    levels.levels[2] = vec![sst(&mut id, 0, 50, 230)];
    assert_eq!(
        pick(&levels, &[]),
        Some((vec![(0, vec![SstId(1), SstId(2)])], 1, TaskKind::Rewrite))
    );
    // L1 is always taken: nothing can go above it.
    levels.levels[1] = vec![sst(&mut id, 0, 50, 5000)];
    assert_eq!(pick(&levels, &[]).unwrap().0[1].0, 1);
    // One L0 file below its trigger, but the runs above the oldest hold more than twice
    // its bytes: everything merges into the last level.
    levels.levels[0] = vec![sst(&mut id, 0, 9, 10)];
    levels.levels[1] = vec![sst(&mut id, 0, 50, 1500)];
    levels.levels[2] = vec![];
    levels.levels[4] = vec![sst(&mut id, 0, 99, 700)];
    assert!(picker.score(&levels) >= 1.0);
    let (inputs, output, kind) = pick(&levels, &[]).unwrap();
    assert_eq!(
        inputs.iter().map(|(l, _)| *l).collect::<Vec<_>>(),
        [0, 1, 4]
    );
    assert_eq!((output, kind), (4, TaskKind::Rewrite));
    // Three equal L0 files under the default trigger of four: space amplification reaches
    // its cap (200%) and makes a merge due, but the write stall follows L0 depth only.
    let mut options = PickerOptions::default();
    options.l0_trigger = 4;
    let p = CompactionPicker::new(CompactionStyle::Tiered, options);
    let three = Levels {
        levels: vec![(0..3).map(|i| sst(&mut id, i, i + 5, 100)).collect()],
    };
    assert!(p.score(&three) >= 1.0);
    assert!((p.stall_score(&three) - 0.75).abs() < 1e-9);
    // A lone L0 file over an empty tree moves down whole.
    let mut options = PickerOptions::default();
    options.l0_trigger = 1;
    let picker = CompactionPicker::new(CompactionStyle::Tiered, options);
    let levels = Levels {
        levels: vec![vec![sst(&mut id, 0, 9, 100)]],
    };
    let task = picker
        .pick(TabletId(1), FamilyId(1), &levels, &[], 0, 0)
        .unwrap();
    assert_eq!((task.output_level, task.kind), (6, TaskKind::TrivialMove));
}

/// An SST holding timestamps `ts`, `len` bytes, as flush number `id`.
fn timed(id: &mut u64, row: u64, ts: (u64, u64), len: u64) -> Arc<SstMeta> {
    let mut s = (*sst(id, row, row + 10, len)).clone();
    s.ts_range = ts;
    Arc::new(s)
}

fn check_fifo(seed: u64, flushes: usize) {
    let mut rng = Rng::new(seed);
    let mut options = PickerOptions::default();
    options.l0_trigger = 2 + rng.below(4) as u32;
    options.target_sst_bytes = (1 << 20) + rng.below(8 << 20);
    options.fifo_max_bytes = [0, 40 << 20][rng.below(2) as usize];
    let ttl = [0, 1_000, 10_000][rng.below(3) as usize];
    let what = format!("seed {seed}: ttl {ttl}, {options:?}");
    let picker = CompactionPicker::new(CompactionStyle::FifoByTime, options.clone());
    let trigger = options.l0_trigger as u64;
    let mut levels = Levels {
        levels: vec![Vec::new()],
    };
    let (mut id, mut now) = (0, 0u64);
    for _ in 0..flushes {
        // Time-ordered flushes, with now and then a late write at an old timestamp.
        let back = if rng.below(10) == 0 { 5_000 } else { 50 };
        let start = now.saturating_sub(rng.below(back));
        now += 1 + rng.below(300);
        let len = (64 << 10) + rng.below(2 << 20);
        let s = timed(&mut id, rng.below(ROWS), (start, now), len);
        levels.levels[0].insert(0, s);
        let mut guard = 0;
        while picker.score_at(&levels, now, ttl) >= 1.0 {
            let task = picker
                .pick(TabletId(1), FamilyId(1), &levels, &[], now, ttl)
                .unwrap_or_else(|| panic!("{what}: score >= 1 but no task"));
            let ids = &task.inputs[0].1;
            assert_eq!(task.inputs.len(), 1, "{what}");
            let l0 = &mut levels.levels[0];
            if task.kind == TaskKind::Drop {
                for s in l0.iter().filter(|s| ids.contains(&s.id)) {
                    assert!(
                        s.ts_range.1 + ttl <= now && ttl != 0 || options.fifo_max_bytes != 0,
                        "{what}: dropped {s:?} unexpired"
                    );
                }
                l0.retain(|s| !ids.contains(&s.id));
            } else {
                assert_eq!((task.kind, task.output_level), (TaskKind::Rewrite, 0));
                let at = l0.iter().position(|s| s.id == ids[0]).unwrap();
                let window: Vec<_> = l0.drain(at..at + ids.len()).collect();
                assert_eq!(
                    window.iter().map(|s| s.id).collect::<Vec<_>>(),
                    *ids,
                    "{what}: not adjacent"
                );
                let bytes: u64 = window.iter().map(|s| s.len).sum();
                assert!(bytes <= options.target_sst_bytes, "{what}");
                let mut merged = (*window[0]).clone();
                id += 1;
                merged.id = SstId(id);
                merged.len = bytes;
                merged.ts_range = (
                    window.iter().map(|s| s.ts_range.0).min().unwrap(),
                    window.iter().map(|s| s.ts_range.1).max().unwrap(),
                );
                l0.insert(at, Arc::new(merged));
            }
            guard += 1;
            assert!(guard < 100, "{what}: compaction does not converge");
        }
        // Not due means nothing to pick.
        assert!(
            picker
                .pick(TabletId(1), FamilyId(1), &levels, &[], now, ttl)
                .is_none(),
            "{what}: work picked at score < 1"
        );
        // Nothing expired is left, the size cap holds, and no `trigger` adjacent files fit
        // in one target: fewer than `trigger` files per target-sized slice of the data.
        let l0 = &levels.levels[0];
        let total: u64 = l0.iter().map(|s| s.len).sum();
        if ttl != 0 {
            assert!(l0.iter().all(|s| s.ts_range.1 + ttl > now), "{what}");
        }
        if options.fifo_max_bytes != 0 {
            assert!(total <= options.fifo_max_bytes, "{what}");
        }
        assert!(
            (l0.len() as u64) < trigger * (total / options.target_sst_bytes + 1),
            "{what}: {} files, {total} bytes",
            l0.len()
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases(64)))]

    /// Issue #32: under time-ordered flushes, expired SSTs go, the size cap holds, and small
    /// L0 files merge so the file count tracks the data size.
    #[test]
    fn fifo_picker_drops_expired_files_and_bounds_l0(seed in any::<u64>()) {
        check_fifo(seed, if cfg!(miri) { 20 } else { 300 });
    }
}

#[test]
fn fifo_drops_expired_ssts_at_any_level_without_io() {
    let mut id = 0;
    let mut options = PickerOptions::default();
    options.l0_trigger = 3;
    let picker = CompactionPicker::new(CompactionStyle::FifoByTime, options.clone());
    // L0 newest first; an older run in L2 (after a full compaction).
    let levels = Levels {
        levels: vec![
            vec![
                timed(&mut id, 0, (300, 400), 100),
                timed(&mut id, 0, (150, 250), 100),
            ],
            vec![],
            vec![
                timed(&mut id, 0, (0, 100), 100),
                timed(&mut id, 50, (90, 200), 100),
            ],
        ],
    };
    let pick = |busy: &[SstId], now, ttl| {
        picker
            .pick(TabletId(1), FamilyId(1), &levels, busy, now, ttl)
            .map(|t| (t.inputs, t.kind))
    };
    // Nothing has expired: two small L0 files are below the trigger of three.
    assert!(picker.score_at(&levels, 199, 100) < 1.0);
    assert_eq!(pick(&[], 199, 100), None);
    // At 200, the SST whose newest timestamp is 100 expires (100 + 100 <= 200), not the
    // others; without a TTL nothing does.
    assert!(picker.score_at(&levels, 200, 100) >= 1.0);
    assert!(picker.score(&levels) < 1.0);
    assert_eq!(
        pick(&[], 200, 100),
        Some((vec![(2, vec![SstId(3)])], TaskKind::Drop))
    );
    assert_eq!(pick(&[], 200, 0), None);
    // #232: the engine's timer goes off when the first SST expires.
    assert_eq!(picker.next_expiry(&levels, 100), Some(200));
    assert_eq!(picker.next_expiry(&levels, 0), None);
    let leveled = CompactionPicker::new(CompactionStyle::Leveled, options.clone());
    assert_eq!(leveled.next_expiry(&levels, 100), None);
    // Later, across levels; busy ones wait.
    assert_eq!(
        pick(&[SstId(4)], 350, 100),
        Some((
            vec![(0, vec![SstId(2)]), (2, vec![SstId(3)])],
            TaskKind::Drop
        ))
    );
    // A size cap drops the oldest by newest timestamp, expired or not.
    let mut capped = options.clone();
    capped.fifo_max_bytes = 250;
    let picker = CompactionPicker::new(CompactionStyle::FifoByTime, capped);
    assert!(picker.score(&levels) >= 1.0);
    assert_eq!(
        picker
            .pick(TabletId(1), FamilyId(1), &levels, &[], 0, 0)
            .map(|t| (t.inputs, t.kind)),
        Some((vec![(2, vec![SstId(3), SstId(4)])], TaskKind::Drop))
    );
    // At the cap exactly, nothing is due and nothing is picked.
    let mut at_cap = options.clone();
    at_cap.fifo_max_bytes = 400;
    let picker = CompactionPicker::new(CompactionStyle::FifoByTime, at_cap);
    assert!(picker.score(&levels) < 1.0);
    assert!(
        picker
            .pick(TabletId(1), FamilyId(1), &levels, &[], 0, 0)
            .is_none()
    );
}

#[test]
fn fifo_merges_the_longest_window_of_small_l0_files() {
    let mut id = 0;
    let mut options = PickerOptions::default();
    options.l0_trigger = 2;
    options.target_sst_bytes = 1000;
    let picker = CompactionPicker::new(CompactionStyle::FifoByTime, options);
    // Sizes newest first: 900 | 300 300 300 | 800: the three 300s fit in 1000 together.
    let l0: Vec<_> = [900, 300, 300, 300, 800]
        .iter()
        .enumerate()
        .map(|(i, &len)| timed(&mut id, 0, (i as u64, i as u64), len))
        .collect();
    let levels = Levels { levels: vec![l0] };
    assert!((picker.score(&levels) - 1.5).abs() < 1e-9);
    // The stall follows that window, not the five L0 files FIFO keeps by design.
    assert!((picker.stall_score(&levels) - 1.5).abs() < 1e-9);
    let big = Levels {
        levels: vec![(0..5).map(|i| timed(&mut id, 0, (i, i), 900)).collect()],
    };
    assert!(picker.stall_score(&big) < 1.0);
    let task = picker
        .pick(TabletId(1), FamilyId(1), &levels, &[], 0, 0)
        .unwrap();
    assert_eq!(task.inputs, [(0, vec![SstId(2), SstId(3), SstId(4)])]);
    assert_eq!((task.output_level, task.kind), (0, TaskKind::Rewrite));
    assert!(
        picker
            .pick(TabletId(1), FamilyId(1), &levels, &[SstId(3)], 0, 0)
            .is_none()
    );
    // #232: a busy file splits the windows, and the longest one without it merges.
    let l0: Vec<_> = (0..6u64).map(|i| timed(&mut id, 0, (i, i), 100)).collect();
    let ids: Vec<SstId> = l0.iter().map(|s| s.id).collect();
    let levels = Levels { levels: vec![l0] };
    let task = picker
        .pick(TabletId(1), FamilyId(1), &levels, &[ids[2]], 0, 0)
        .unwrap();
    assert_eq!(task.inputs, [(0, ids[3..].to_vec())]);
}

#[test]
fn busy_inputs_are_not_picked() {
    let mut id = 0;
    let mut levels = Levels::default();
    levels.levels = vec![
        (0..4)
            .map(|i| sst(&mut id, i * 10, i * 10 + 5, 1000))
            .collect(),
    ];
    let picker = CompactionPicker::new(CompactionStyle::Leveled, PickerOptions::default());
    assert!(
        picker
            .pick(TabletId(1), FamilyId(1), &levels, &[SstId(2)], 0, 0)
            .is_none()
    );
    assert!(
        picker
            .pick(TabletId(1), FamilyId(1), &levels, &[], 0, 0)
            .is_some()
    );
}

#[test]
fn clean_cuts_take_neighbours_sharing_a_row() {
    let mut id = 0;
    let mut options = PickerOptions::default();
    options.level_base_bytes = 1000;
    let picker = CompactionPicker::new(CompactionStyle::Leveled, options);
    // L1: [0..5] [5..9] share row 5; [20..30] alone. L2 holds row 7.
    let l1 = vec![
        sst(&mut id, 0, 5, 800),
        sst(&mut id, 5, 9, 800),
        sst(&mut id, 20, 30, 800),
    ];
    let l2 = vec![sst(&mut id, 7, 7, 100)];
    let levels = Levels {
        levels: vec![vec![], l1, l2],
    };
    let task = picker
        .pick(TabletId(1), FamilyId(1), &levels, &[], 0, 0)
        .unwrap();
    // The lone file with no overlap below costs least: a trivial move.
    assert_eq!(task.kind, TaskKind::TrivialMove);
    assert_eq!(task.inputs, [(1, vec![SstId(3)])]);
    // With it busy, the two files sharing row 5 go together, with the overlap below.
    let task = picker
        .pick(TabletId(1), FamilyId(1), &levels, &[SstId(3)], 0, 0)
        .unwrap();
    assert_eq!(
        task.inputs,
        [(1, vec![SstId(1), SstId(2)]), (2, vec![SstId(4)])]
    );
    assert_eq!((task.output_level, task.kind), (2, TaskKind::Rewrite));
}

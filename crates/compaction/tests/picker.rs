//! The leveled picker under a random flush workload.
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
                let s = sst_cut(id, start, end, bytes / n);
                start = if shared {
                    (b + 1, &b"n"[..])
                } else {
                    (b + 1, &b"a"[..])
                };
                s
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

#[test]
fn busy_inputs_are_not_picked_and_other_styles_wait() {
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
    for style in [CompactionStyle::Tiered, CompactionStyle::FifoByTime] {
        let p = CompactionPicker::new(style, PickerOptions::default());
        assert_eq!(p.score(&levels), 0.0);
        assert!(
            p.pick(TabletId(1), FamilyId(1), &levels, &[], 0, 0)
                .is_none()
        );
    }
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

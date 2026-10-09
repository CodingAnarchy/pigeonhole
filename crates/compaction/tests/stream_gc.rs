//! `StreamGc` hands out the same entries however its steps and drains interleave: kept
//! entries of a step may point into the GC's group buffer, so a step taken before the last
//! one's entries were drained must keep them intact.

use pigeonhole_compaction::{GcPolicy, StreamGc, VecCursor};
use pigeonhole_format::Cursor;
use pigeonhole_format::key::{Kind, encode_key};
use pigeonhole_format::manifest::FamilyOptions;
use proptest::prelude::*;

type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// Runs `entries` through a fresh GC, draining after every `drain_every` steps.
fn run(entries: &Entries, snapshots: &[u64], drain_every: usize) -> Entries {
    let policy = GcPolicy::new(snapshots.to_vec(), 0, false);
    let family = FamilyOptions::default().max_versions(1);
    let mut gc = StreamGc::new(&policy, &family, None);
    let mut cursor = VecCursor::new(entries.clone());
    cursor.seek_to_first().unwrap();
    let mut out = Vec::new();
    let mut steps = 0;
    loop {
        let more = gc.step(&mut cursor).unwrap();
        steps += 1;
        if !more || steps % drain_every == 0 {
            gc.drain(|k, v| {
                out.push((k.to_vec(), v.to_vec()));
                Ok::<(), ()>(())
            })
            .unwrap();
        }
        if !more {
            return out;
        }
    }
}

proptest! {
    #[test]
    fn steps_before_a_drain_keep_their_entries(
        cells in prop::collection::vec((0u8..4, 0u8..3, 1u64..6, 0u8..3), 1..60),
        snapshots in prop::collection::vec(1u64..80, 0..3),
        drain_every in 2usize..6,
    ) {
        let mut entries: Entries = cells
            .iter()
            .enumerate()
            .map(|(i, &(row, q, ts, kind))| {
                let kind = [Kind::Put, Kind::Put, Kind::CellDelete][usize::from(kind)];
                let mut key = Vec::new();
                encode_key(&mut key, &[b'r', row], &[b'q', q], ts, i as u64 + 1, kind).unwrap();
                let value = if kind == Kind::Put { vec![0, row, q, i as u8] } else { Vec::new() };
                (key, value)
            })
            .collect();
        entries.sort();
        let mut snapshots = snapshots;
        snapshots.sort_unstable();
        snapshots.dedup();
        prop_assert_eq!(run(&entries, &snapshots, drain_every), run(&entries, &snapshots, 1));
    }
}

//! Compaction jobs on SSTs over `SimVfs`: reads at every live snapshot are unchanged, dead
//! data is reclaimed, and interrupted jobs leave their inputs intact.
#![allow(clippy::field_reassign_with_default)]

mod common;

use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};

use common::*;
use pigeonhole_cache::BlockCache;
use pigeonhole_compaction::{
    CompactionJob, CompactionOutput, CompactionPicker, CompactionTask, GcPolicy, I64Add,
    JobContext, JobPoll, KeyRange, Levels, PickerOptions, TaskKind,
};
use pigeonhole_format::key::{Kind, encode_key, encode_marker_key, split_suffix};
use pigeonhole_format::manifest::{CompactionStyle, FamilyOptions, SstMeta};
use pigeonhole_format::{FamilyId, Seqno, SstId, TableId, TabletId, Timestamp};
use pigeonhole_io::sim::{CrashKind, SimVfs};
use pigeonhole_io::{Vfs, VfsRef};
use pigeonhole_pager::Pager;
use pigeonhole_sim::{ModelPurge, Rng};
use pigeonhole_sst::SstReader;
use proptest::prelude::*;

/// An entry as written to an SST.
type KeyValue = (Vec<u8>, Vec<u8>);

struct Db {
    vfs: Arc<SimVfs>,
    pager: Arc<Pager>,
    cache: Arc<BlockCache>,
    next_id: u64,
    /// Output SST ids: shared by every job, as the engine's counter is (the block cache is
    /// keyed by SST id).
    sst_ids: Arc<AtomicU64>,
}

impl Db {
    fn new(seed: u64) -> Self {
        let vfs = SimVfs::new(seed);
        let dyn_vfs: VfsRef = vfs.clone();
        let pager = Arc::new(Pager::create(&dyn_vfs, "/db/data.phdb".as_ref()).unwrap());
        Self {
            vfs,
            pager,
            cache: Arc::new(BlockCache::new(4 << 20, 2)),
            next_id: 1,
            sst_ids: Arc::new(AtomicU64::new(1000)),
        }
    }

    fn sst(
        &mut self,
        family: &FamilyOptions,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> (SstMeta, Arc<SstReader>) {
        let meta = write_sst(&self.pager, self.next_id, family, entries);
        self.next_id += 1;
        let reader = open_sst(&self.pager, &self.cache, &meta);
        (meta, reader)
    }

    fn context(&self, family: FamilyOptions, gc: GcPolicy) -> JobContext {
        let mut ctx = JobContext::new(
            TableId(1),
            family,
            self.pager.clone(),
            self.cache.clone(),
            self.sst_ids.clone(),
            Arc::new(AtomicU32::new(1)),
            gc,
        );
        ctx.merge = Some(Arc::new(I64Add));
        ctx.target_sst_bytes = 64 << 10;
        let clock: VfsRef = self.vfs.clone();
        ctx.clock = Some(clock);
        ctx
    }
}

fn task(inputs: Vec<(u8, Vec<SstId>)>, output_level: u8) -> CompactionTask {
    CompactionTask {
        tablet: TabletId(1),
        family: FamilyId(1),
        range: KeyRange::all(),
        subranges: vec![KeyRange::all()],
        inputs,
        output_level,
        kind: TaskKind::Rewrite,
    }
}

/// Runs a job in time slices that always hit the deadline (simulated time stands still).
fn run_sliced(db: &Db, job: &mut CompactionJob) -> usize {
    let mut slices = 0;
    loop {
        slices += 1;
        match job.run(db.vfs.monotonic_nanos()).unwrap() {
            JobPoll::Pending => continue,
            JobPoll::Done => return slices,
        }
    }
}

/// Which compaction `check_compaction` runs.
#[derive(Clone, Copy, Debug)]
enum Choose {
    /// A random leveled-shaped one.
    Random,
    /// The tiered picker's (issue #31).
    Tiered,
}

fn check_compaction(seed: u64, commits: usize, choose: Choose) {
    let mut h = random_history(seed, common::commits(commits));
    let mut rng = Rng::new(seed ^ 0xc0c0);
    let mut db = Db::new(seed);
    let family = family_options(&h);
    let max = h.model.snapshot();

    // Cut the history into seqno batches: the oldest goes to L2, the next to L1, the rest
    // are L0 flushes (newest first).
    let mut cuts: Vec<Seqno> = (0..1 + rng.below(4)).map(|_| 1 + rng.below(max)).collect();
    cuts.push(0);
    cuts.push(max);
    cuts.sort_unstable();
    cuts.dedup();
    let batches: Vec<Vec<(Vec<u8>, Vec<u8>)>> = cuts
        .windows(2)
        .map(|w| {
            let mut b: Vec<_> = h
                .entries
                .iter()
                .filter(|e| e.2 > w[0] && e.2 <= w[1])
                .map(|e| (e.0.clone(), e.1.clone()))
                .collect();
            b.sort();
            b
        })
        .filter(|b| !b.is_empty())
        .collect();
    // levels[n] = SSTs (meta, reader); deeper levels split at a row boundary.
    let mut levels: Vec<Vec<(SstMeta, Arc<SstReader>)>> = vec![Vec::new(), Vec::new(), Vec::new()];
    for (i, batch) in batches.iter().enumerate() {
        let level = match i {
            0 => 2,
            1 => 1,
            _ => 0,
        };
        if level == 0 {
            let s = db.sst(&family, batch);
            levels[0].insert(0, s);
        } else {
            let split = row_prefix(b"b");
            let (lo, hi): (Vec<_>, Vec<_>) = batch.iter().cloned().partition(|e| e.0 < split);
            for part in [lo, hi] {
                if !part.is_empty() {
                    let s = db.sst(&family, &part);
                    levels[level].push(s);
                }
            }
        }
    }

    let mut snapshots: Vec<Seqno> = (0..rng.below(4)).map(|_| 1 + rng.below(max)).collect();
    snapshots.sort_unstable();
    snapshots.dedup();
    let gc_now = h.last_ts + rng.below(300);

    // Choose the compaction.
    let choice = rng.below(3);
    let (from, to): (Vec<usize>, u8) = match choose {
        Choose::Random => match choice {
            0 => (vec![0, 1], 1),
            1 => (vec![1, 2], 2),
            _ => (vec![0, 1, 2], 2),
        },
        Choose::Tiered => {
            let mut options = PickerOptions::default();
            options.l0_trigger = 1;
            options.max_levels = 3;
            options.tiered_size_ratio_percent = rng.below(200) as u32;
            options.tiered_max_space_amp_percent = 1 + rng.below(300) as u32;
            let picker = CompactionPicker::new(CompactionStyle::Tiered, options);
            let metas = Levels {
                levels: levels
                    .iter()
                    .map(|l| l.iter().map(|s| Arc::new(s.0.clone())).collect())
                    .collect(),
            };
            let Some(t) = picker.pick(TabletId(1), FamilyId(1), &metas, &[], 0, 0) else {
                return;
            };
            let from = t.inputs.iter().map(|(l, _)| usize::from(*l)).collect();
            (from, t.output_level)
        }
    };
    let bottommost = to == 2 || levels[2].is_empty();
    let mut inputs = Vec::new();
    let mut by_level = Vec::new();
    for &l in &from {
        by_level.push((
            l as u8,
            levels[l].iter().map(|s| s.0.id).collect::<Vec<_>>(),
        ));
        inputs.extend(levels[l].iter().map(|s| s.1.clone()));
    }
    let before: Vec<Arc<SstReader>> = levels.iter().flatten().map(|s| s.1.clone()).collect();
    // The smallest timestamp left above the inputs bounds bottommost purges.
    let mut gc = GcPolicy::new(snapshots.clone(), gc_now, bottommost);
    gc.min_ts_above = levels
        .iter()
        .enumerate()
        .filter(|(l, _)| !from.contains(l))
        .flat_map(|(_, s)| s.iter().map(|s| s.0.ts_range.0))
        .min()
        .unwrap_or(u64::MAX);
    if rng.below(4) == 0 {
        gc.min_ts_above = 0;
    }
    // What the model drops when this compaction purges (owner decision: HBase semantics).
    let purge = ModelPurge {
        table: TABLE.into(),
        family: FAMILY.into(),
        rows: (Bound::Unbounded, Bound::Unbounded),
        snapshots: snapshots.clone(),
        now: gc_now,
        min_ts_above: gc.min_ts_above,
        max_seqno: from
            .iter()
            .flat_map(|&l| &levels[l])
            .map(|s| s.0.seqno_range.1)
            .max()
            .unwrap_or(0),
    };
    let mut job = CompactionJob::new(task(by_level, to), inputs, db.context(family.clone(), gc));
    run_sliced(&db, &mut job);
    let read = job.entries_read();
    let out = job.finish().unwrap();
    let input_entries: usize = from
        .iter()
        .flat_map(|&l| &levels[l])
        .map(|s| s.0.entries as usize)
        .sum();
    assert_eq!(
        read as usize, input_entries,
        "seed {seed}: every input entry is read"
    );

    let mut after: Vec<Arc<SstReader>> = levels
        .iter()
        .enumerate()
        .filter(|(l, _)| !from.contains(l))
        .flat_map(|(_, s)| s.iter().map(|s| s.1.clone()))
        .collect();
    for (level, meta) in &out.added {
        assert_eq!(*level, to);
        after.push(open_sst(&db.pager, &db.cache, meta));
    }
    assert_disjoint(&out);

    // Under Miri, latest only (each read point costs seconds there).
    let mut points = if cfg!(miri) {
        Vec::new()
    } else {
        snapshots.clone()
    };
    points.push(max);
    for &s in &points {
        for now in [gc_now, gc_now + 500]
            .into_iter()
            .take(if cfg!(miri) { 1 } else { 2 })
        {
            let what = format!(
                "seed {seed}: choice {choice}, snapshots {snapshots:?}, gc_now {gc_now}, read at {s}/{now}"
            );
            let expected = model_reads(&h, s, now);
            let b = resolver_reads(&h, s, now, |o| sst_resolver(&before, o));
            assert_same(&format!("before, {what}"), &expected, &b);
            let a = resolver_reads(&h, s, now, |o| sst_resolver(&after, o));
            assert_same(&format!("after, {what}"), &expected, &a);
        }
    }

    // A bottommost compaction may purge deletes and versions (HBase semantics): apply the
    // same purge to the model, which leaves every read at the live snapshots unchanged.
    if bottommost {
        h.model.purge(&purge);
    }
    for &s in &points {
        let what = format!("seed {seed}: choice {choice}, purged model, read at {s}/{gc_now}");
        let expected = model_reads(&h, s, gc_now);
        let a = resolver_reads(&h, s, gc_now, |o| sst_resolver(&after, o));
        assert_same(&what, &expected, &a);
    }

    // Any writes after the compaction, explicit older timestamps and cell deletes included,
    // flushed on top, read as in the (purged) model at the old live snapshots and the new
    // latest one.
    let n_later = 1 + rng.below(8) as usize;
    let later = extend_history(&mut h, &mut rng, n_later, false);
    let mut flush: Vec<_> = later.iter().map(|e| (e.0.clone(), e.1.clone())).collect();
    flush.sort();
    after.push(db.sst(&family, &flush).1);
    let now = gc_now.max(h.last_ts) + rng.below(100);
    let old_points = if cfg!(miri) { &[][..] } else { &snapshots[..] };
    for s in old_points.iter().copied().chain([max, h.model.snapshot()]) {
        let what = format!("seed {seed}: choice {choice}, later writes, read at {s}/{now}");
        let expected = model_reads(&h, s, now);
        let a = resolver_reads(&h, s, now, |o| sst_resolver(&after, o));
        assert_same(&what, &expected, &a);
    }
}

/// Output SSTs are sorted and disjoint.
fn assert_disjoint(out: &CompactionOutput) {
    for w in out.added.windows(2) {
        assert!(w[0].1.largest_key < w[1].1.smallest_key);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases(128)))]

    /// Done-when (2): no compaction changes any read result at any live snapshot.
    #[test]
    fn compaction_preserves_reads_at_live_snapshots(seed in any::<u64>(), commits in 1usize..40) {
        check_compaction(seed, commits, Choose::Random);
    }

    /// Issue #31: the same for the tiered picker's compactions.
    #[test]
    fn tiered_compaction_preserves_reads_at_live_snapshots(seed in any::<u64>(), commits in 1usize..40) {
        check_compaction(seed, commits, Choose::Tiered);
    }
}

#[test]
fn compaction_preserves_reads_fixed_seeds() {
    for seed in 0..if cfg!(miri) { 1 } else { 48 } {
        check_compaction(seed, 35, Choose::Random);
        check_compaction(seed, 35, Choose::Tiered);
    }
}

/// Issue #32: the FIFO-by-time picker's drops of expired SSTs, and its merges of L0 files,
/// leave every read at every live snapshot unchanged from the picker's `now` on.
fn check_fifo(seed: u64, commits: usize) {
    let h = random_history(seed, common::commits(commits));
    let ttl = h.family.ttl_micros;
    if ttl == 0 {
        return;
    }
    let mut rng = Rng::new(seed ^ 0xf1f0);
    let mut db = Db::new(seed);
    let family = family_options(&h);
    let max = h.model.snapshot();
    // L0 flushes by seqno, newest first.
    let mut cuts: Vec<Seqno> = (0..rng.below(6)).map(|_| 1 + rng.below(max)).collect();
    cuts.extend([0, max]);
    cuts.sort_unstable();
    cuts.dedup();
    let mut l0: Vec<(SstMeta, Arc<SstReader>)> = Vec::new();
    for w in cuts.windows(2) {
        let mut b: Vec<KeyValue> = h
            .entries
            .iter()
            .filter(|e| e.2 > w[0] && e.2 <= w[1])
            .map(|e| (e.0.clone(), e.1.clone()))
            .collect();
        if !b.is_empty() {
            b.sort();
            l0.insert(0, db.sst(&family, &b));
        }
    }
    let mut snapshots: Vec<Seqno> = (0..rng.below(4)).map(|_| 1 + rng.below(max)).collect();
    snapshots.push(max);
    snapshots.sort_unstable();
    snapshots.dedup();
    let now = h.last_ts.saturating_sub(ttl) + rng.below(2 * ttl);
    let reads_match = |what: &str, ssts: &[(SstMeta, Arc<SstReader>)]| {
        let readers: Vec<Arc<SstReader>> = ssts.iter().map(|s| s.1.clone()).collect();
        for &s in &snapshots {
            for at in [now, now + 1 + ttl / 2] {
                let what = format!("seed {seed}: {what}, picked at {now}, read at {s}/{at}");
                let expected = model_reads(&h, s, at);
                let got = resolver_reads(&h, s, at, |o| sst_resolver(&readers, o));
                assert_same(&what, &expected, &got);
            }
        }
    };
    reads_match("before", &l0);

    let mut options = PickerOptions::default();
    options.l0_trigger = 2;
    options.target_sst_bytes = 1 + rng.below(4 << 10);
    let picker = CompactionPicker::new(CompactionStyle::FifoByTime, options);
    let levels = |l0: &[(SstMeta, Arc<SstReader>)]| Levels {
        levels: vec![l0.iter().map(|s| Arc::new(s.0.clone())).collect()],
    };
    let expired = l0.iter().filter(|s| s.0.ts_range.1 + ttl <= now).count();
    if let Some(task) = picker.pick(TabletId(1), FamilyId(1), &levels(&l0), &[], now, ttl)
        && task.kind == TaskKind::Drop
    {
        assert_eq!(task.inputs[0].1.len(), expired, "seed {seed}");
        l0.retain(|s| !task.inputs[0].1.contains(&s.0.id));
        reads_match("after the drop", &l0);
    } else {
        assert_eq!(expired, 0, "seed {seed}");
    }
    // A merge of adjacent L0 files is never bottommost unless it takes them all (the engine's
    // rule): the files left out may be older.
    let Some(task) = picker.pick(TabletId(1), FamilyId(1), &levels(&l0), &[], now, ttl) else {
        return;
    };
    assert_eq!(task.kind, TaskKind::Rewrite);
    assert_eq!(task.output_level, 0);
    let ids = &task.inputs[0].1;
    let all = ids.len() == l0.len();
    let mut gc = GcPolicy::new(snapshots.clone(), now, all);
    gc.min_ts_above = l0
        .iter()
        .filter(|s| !ids.contains(&s.0.id))
        .map(|s| s.0.ts_range.0)
        .min()
        .unwrap_or(u64::MAX);
    let inputs = l0
        .iter()
        .filter(|s| ids.contains(&s.0.id))
        .map(|s| s.1.clone())
        .collect();
    let mut job = CompactionJob::new(task.clone(), inputs, db.context(family.clone(), gc));
    run_sliced(&db, &mut job);
    let out = job.finish().unwrap();
    assert_disjoint(&out);
    let at = l0.iter().position(|s| ids.contains(&s.0.id)).unwrap();
    l0.retain(|s| !ids.contains(&s.0.id));
    for (level, meta) in &out.added {
        assert_eq!(*level, 0);
        let reader = open_sst(&db.pager, &db.cache, meta);
        l0.insert(at, (meta.clone(), reader));
    }
    reads_match("after the merge", &l0);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases(128)))]

    /// Issue #32: expired SSTs are dropped and reads at every live snapshot are unchanged.
    #[test]
    fn fifo_drops_and_merges_preserve_reads(seed in any::<u64>(), commits in 1usize..40) {
        check_fifo(seed, commits);
    }
}

// ---------------------------------------------------------------------------------------
// Focused cases
// ---------------------------------------------------------------------------------------

/// A policy for inputs with nothing above them.
fn policy(snapshots: Vec<Seqno>, now: Timestamp, bottommost: bool) -> GcPolicy {
    let mut gc = GcPolicy::new(snapshots, now, bottommost);
    gc.min_ts_above = u64::MAX;
    gc
}

fn key(row: &[u8], q: &[u8], ts: Timestamp, seqno: Seqno, kind: Kind) -> Vec<u8> {
    let mut k = Vec::new();
    encode_key(&mut k, row, q, ts, seqno, kind).unwrap();
    k
}

fn compact(
    db: &Db,
    family: &FamilyOptions,
    inputs: &[(SstMeta, Arc<SstReader>)],
    gc: GcPolicy,
) -> (CompactionOutput, Vec<KeyValue>) {
    let ids = inputs.iter().map(|s| s.0.id).collect();
    let mut job = CompactionJob::new(
        task(vec![(1, ids)], 2),
        inputs.iter().map(|s| s.1.clone()).collect(),
        db.context(family.clone(), gc),
    );
    run_sliced(db, &mut job);
    let out = job.finish().unwrap();
    let mut entries = Vec::new();
    for (_, meta) in &out.added {
        entries.extend(sst_entries(&open_sst(&db.pager, &db.cache, meta)));
    }
    (out, entries)
}

fn kinds(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<(Timestamp, Seqno, Kind)> {
    entries
        .iter()
        .map(|(k, _)| {
            let (_, ts, seqno, kind) = split_suffix(k).unwrap();
            (ts, seqno, kind)
        })
        .collect()
}

/// Done-when (3): expired and deleted data is gone after a bottommost compaction with no
/// snapshot needing it, and kept while a snapshot can still see it.
#[test]
#[cfg_attr(
    miri,
    ignore = "each SimVfs pager costs ~25 s under Miri; covered natively"
)]
fn ttl_and_tombstones_are_reclaimed_at_the_bottom() {
    let mut db = Db::new(3);
    let mut family = FamilyOptions::default();
    family.ttl_micros = 1_000;
    family.max_versions = 2;
    let mut e = Vec::new();
    // Column `old`: every version expired at now = 10_000.
    for (i, ts) in [100u64, 200, 300].into_iter().enumerate() {
        e.push((key(b"r", b"old", ts, i as u64 + 1, Kind::Put), stored(b"x")));
    }
    // Column `del`: two puts under a column delete.
    e.push((key(b"r", b"del", 9_500, 4, Kind::Put), stored(b"a")));
    e.push((key(b"r", b"del", 9_600, 5, Kind::Put), stored(b"b")));
    e.push((key(b"r", b"del", 9_700, 6, Kind::ColumnDelete), vec![]));
    // Column `ver`: four live versions, two over max_versions.
    for (i, ts) in [9_100u64, 9_200, 9_300, 9_400].into_iter().enumerate() {
        e.push((key(b"r", b"ver", ts, i as u64 + 7, Kind::Put), stored(b"v")));
    }
    // A family marker hiding row `s` entirely.
    e.push((key(b"s", b"q", 9_000, 11, Kind::Put), stored(b"gone")));
    let mut m = Vec::new();
    encode_marker_key(&mut m, b"s", 9_900, 12).unwrap();
    e.push((m, vec![]));
    e.sort();
    let input = db.sst(&family, &e);

    let (out, kept) = compact(
        &db,
        &family,
        std::slice::from_ref(&input),
        policy(vec![], 10_000, true),
    );
    assert_eq!(out.removed, [input.0.id]);
    let k = kinds(&kept);
    assert_eq!(
        k,
        [(9_400, 10, Kind::Put), (9_300, 9, Kind::Put)],
        "only the two newest versions survive"
    );

    // A snapshot before the column delete keeps the delete and what it hides from later
    // snapshots; a non-bottommost run keeps every tombstone and every version.
    let (_, kept) = compact(
        &db,
        &family,
        std::slice::from_ref(&input),
        policy(vec![5], 10_000, true),
    );
    assert!(kinds(&kept).contains(&(9_700, 6, Kind::ColumnDelete)));
    assert!(kinds(&kept).contains(&(9_600, 5, Kind::Put)));
    let (_, kept) = compact(&db, &family, &[input], policy(vec![], 10_000, false));
    let k = kinds(&kept);
    assert!(k.contains(&(9_700, 6, Kind::ColumnDelete)));
    assert!(k.contains(&(9_900, 12, Kind::FamilyDelete)));
    assert!(
        !k.contains(&(9_600, 5, Kind::Put)),
        "hidden at every snapshot: dropped anyway"
    );
    assert_eq!(
        k.iter()
            .filter(|x| x.2 == Kind::Put && x.0 < 9_500 && x.0 > 9_000)
            .count(),
        4
    );
    assert!(
        !k.iter().any(|x| x.0 <= 300),
        "expired data is dropped at any level"
    );
}

/// #25: a cell delete at `T` keeps a later put at `T` hidden through compactions.
#[test]
#[cfg_attr(
    miri,
    ignore = "each SimVfs pager costs ~25 s under Miri; covered natively"
)]
fn cell_delete_hides_a_later_put_through_compaction() {
    let mut db = Db::new(4);
    let family = FamilyOptions::default();
    let e = vec![
        (key(b"r", b"q", 50, 3, Kind::Put), stored(b"later")),
        (key(b"r", b"q", 50, 2, Kind::CellDelete), vec![]),
        (key(b"r", b"q", 50, 1, Kind::Put), stored(b"first")),
    ];
    let input = db.sst(&family, &e);
    let latest = |entries: &[(Vec<u8>, Vec<u8>)], snapshot| {
        let mut r = pigeonhole_compaction::CellResolver::new(
            pigeonhole_compaction::VecCursor::new(entries.to_vec()),
            pigeonhole_compaction::ResolveOptions::new(snapshot, 100),
        );
        r.seek_column(b"r", b"q").unwrap();
        r.next_cell().unwrap().map(|c| c.value.to_vec())
    };
    assert_eq!(latest(&e, 3), None);
    for bottommost in [false, true] {
        let (_, kept) = compact(
            &db,
            &family,
            std::slice::from_ref(&input),
            policy(vec![], 100, bottommost),
        );
        assert_eq!(latest(&kept, 3), None, "bottommost {bottommost}");
        if bottommost {
            assert!(kept.is_empty(), "the delete goes with everything it covers");
        } else {
            assert_eq!(kinds(&kept), [(50, 2, Kind::CellDelete)]);
        }
        // A snapshot at 1 still sees the first put, so it and the delete stay.
        let (_, kept) = compact(
            &db,
            &family,
            std::slice::from_ref(&input),
            policy(vec![1], 100, bottommost),
        );
        assert_eq!(latest(&kept, 1), Some(stored(b"first")));
        assert_eq!(latest(&kept, 3), None);
    }
}

/// #21: a bad base is never folded into a zero; the read keeps failing after compaction.
#[test]
#[cfg_attr(
    miri,
    ignore = "each SimVfs pager costs ~25 s under Miri; covered natively"
)]
fn a_bad_merge_base_stays_a_merge_failure() {
    let mut db = Db::new(5);
    let family = FamilyOptions::default();
    let i64v = |v: i64| stored(&v.to_le_bytes());
    let e = vec![
        (key(b"r", b"n", 30, 3, Kind::Merge), i64v(2)),
        (key(b"r", b"n", 20, 2, Kind::Merge), i64v(1)),
        (key(b"r", b"n", 10, 1, Kind::Put), stored(b"abc")),
    ];
    let input = db.sst(&family, &e);
    let (_, kept) = compact(&db, &family, &[input], policy(vec![], 100, true));
    assert_eq!(
        kept, e,
        "operands are not folded across timestamps or onto a base"
    );
    let mut o = pigeonhole_compaction::ResolveOptions::new(9, 100);
    o.merge = Some(Arc::new(I64Add));
    let mut r =
        pigeonhole_compaction::CellResolver::new(pigeonhole_compaction::VecCursor::new(kept), o);
    r.seek_column(b"r", b"n").unwrap();
    assert!(matches!(
        r.next_cell(),
        Err(pigeonhole_compaction::Error::Merge(_))
    ));
}

/// Operands at one timestamp and stripe combine into one operand.
#[test]
#[cfg_attr(
    miri,
    ignore = "each SimVfs pager costs ~25 s under Miri; covered natively"
)]
fn operands_combine_within_a_timestamp() {
    let mut db = Db::new(6);
    let family = FamilyOptions::default();
    let i64v = |v: i64| stored(&v.to_le_bytes());
    let e = vec![
        (key(b"r", b"n", 10, 3, Kind::Merge), i64v(4)),
        (key(b"r", b"n", 10, 2, Kind::Merge), i64v(2)),
        (key(b"r", b"n", 10, 1, Kind::Merge), i64v(1)),
    ];
    let input = db.sst(&family, &e);
    let (_, kept) = compact(
        &db,
        &family,
        std::slice::from_ref(&input),
        policy(vec![], 100, false),
    );
    assert_eq!(kept, [(key(b"r", b"n", 10, 3, Kind::Merge), i64v(7))]);
    // A snapshot at 2 separates the newest operand from the older two.
    let (_, kept) = compact(&db, &family, &[input], policy(vec![2], 100, false));
    assert_eq!(
        kept,
        [
            (key(b"r", b"n", 10, 3, Kind::Merge), i64v(4)),
            (key(b"r", b"n", 10, 2, Kind::Merge), i64v(3))
        ]
    );
}

/// Large inputs cut into several outputs at row boundaries.
#[test]
#[cfg_attr(miri, ignore = "writes several 64 KiB SSTs: too slow under Miri")]
fn outputs_are_cut_near_the_target_between_rows() {
    let mut db = Db::new(7);
    let mut family = FamilyOptions::default();
    family.compression = pigeonhole_format::compress::Compression::None;
    let mut e = Vec::new();
    for row in 0..400u32 {
        for q in 0..3u8 {
            e.push((
                key(format!("row{row:05}").as_bytes(), &[q], 10, 1, Kind::Put),
                stored(&[q; 300]),
            ));
        }
    }
    let input = db.sst(&family, &e);
    let (out, kept) = compact(&db, &family, &[input], policy(vec![], 100, true));
    assert_eq!(kept, e);
    assert!(out.added.len() > 2, "{} outputs", out.added.len());
    assert_disjoint(&out);
    for w in out.added.windows(2) {
        let row = |k: &[u8]| {
            pigeonhole_format::decode_key(k)
                .unwrap()
                .row
                .as_escaped()
                .to_vec()
        };
        assert_ne!(
            row(&w[0].1.largest_key),
            row(&w[1].1.smallest_key),
            "a row straddles outputs"
        );
    }
}

/// A small compaction under a large target takes an extent sized to its output, not a
/// target-size one, and leaves the file small (#106).
#[test]
#[cfg_attr(
    miri,
    ignore = "each SimVfs pager costs ~25 s under Miri; covered natively"
)]
fn small_outputs_take_small_extents() {
    let mut db = Db::new(106);
    let family = FamilyOptions::default();
    let mut inputs = Vec::new();
    for ts in [10, 20] {
        let e: Vec<_> = (0..50u32)
            .map(|row| {
                let k = key(format!("row{row:05}").as_bytes(), b"q", ts, ts, Kind::Put);
                (k, stored(&[ts as u8; 100]))
            })
            .collect();
        inputs.push(db.sst(&family, &e));
    }
    let before = db.pager.stats();
    for round in 0..20 {
        let mut ctx = db.context(family.clone(), policy(vec![], 100, true));
        ctx.target_sst_bytes = 64 << 20;
        let ids = inputs.iter().map(|s| s.0.id).collect();
        let mut job = CompactionJob::new(
            task(vec![(1, ids)], 2),
            inputs.iter().map(|s| s.1.clone()).collect(),
            ctx,
        );
        run_sliced(&db, &mut job);
        let out = job.finish().unwrap();
        assert_eq!(out.added.len(), 1, "round {round}");
        let meta = &out.added[0].1;
        assert_eq!(
            meta.extent.size_class, 0,
            "{meta:?} not trimmed to its length"
        );
        assert!(meta.len <= meta.extent.len());
        // Both versions of each cell are kept.
        assert_eq!(
            sst_entries(&open_sst(&db.pager, &db.cache, meta)).len(),
            100
        );
        // Nothing publishes the output; give it back as an aborted job would.
        db.pager.abandon(meta.extent);
    }
    let after = db.pager.stats();
    assert_eq!(after.allocated_bytes, before.allocated_bytes);
    assert!(
        after.file_bytes <= before.file_bytes + (1 << 20),
        "file grew from {} to {}",
        before.file_bytes,
        after.file_bytes
    );
}

/// Slices run before interrupting a job (each ends at the deadline after 64 groups).
const SLICES: usize = if cfg!(miri) { 2 } else { 12 };

fn inputs_for_interrupt(db: &mut Db) -> (FamilyOptions, Vec<(SstMeta, Arc<SstReader>)>) {
    let family = FamilyOptions::default();
    let mut ssts = Vec::new();
    for s in 0..3u64 {
        let mut e = Vec::new();
        for row in 0..if cfg!(miri) { 100 } else { 300u32 } {
            e.push((
                key(
                    format!("row{row:05}").as_bytes(),
                    b"q",
                    10 + s,
                    1 + s,
                    Kind::Put,
                ),
                stored(&[s as u8; 200]),
            ));
        }
        ssts.push(db.sst(&family, &e));
    }
    (family, ssts)
}

/// Done-when (5): an aborted job returns its space and leaves the inputs as they were.
#[test]
fn abort_leaves_inputs_intact() {
    let mut db = Db::new(8);
    let (family, inputs) = inputs_for_interrupt(&mut db);
    let contents: Vec<_> = inputs.iter().map(|s| sst_entries(&s.1)).collect();
    let allocated = db.pager.stats().allocated_bytes;
    let ids = inputs.iter().map(|s| s.0.id).collect();
    let mut job = CompactionJob::new(
        task(vec![(1, ids)], 2),
        inputs.iter().map(|s| s.1.clone()).collect(),
        db.context(family, policy(vec![], 100, true)),
    );
    // A few slices: some outputs are finished, one is open.
    for _ in 0..SLICES {
        assert_eq!(job.run(db.vfs.monotonic_nanos()).unwrap(), JobPoll::Pending);
    }
    assert!(db.pager.stats().allocated_bytes > allocated);
    job.abort();
    assert_eq!(db.pager.stats().allocated_bytes, allocated);
    for (s, c) in inputs.iter().zip(&contents) {
        assert_eq!(&sst_entries(&s.1), c);
    }
}

/// Done-when (5): a crash mid-job leaves the (durable) inputs readable and unchanged.
#[test]
fn crash_mid_job_leaves_inputs_intact() {
    let mut db = Db::new(9);
    let (family, inputs) = inputs_for_interrupt(&mut db);
    db.pager.file().sync_all().unwrap();
    let contents: Vec<_> = inputs.iter().map(|s| sst_entries(&s.1)).collect();
    let ids = inputs.iter().map(|s| s.0.id).collect();
    let mut job = CompactionJob::new(
        task(vec![(1, ids)], 2),
        inputs.iter().map(|s| s.1.clone()).collect(),
        db.context(family, policy(vec![], 100, true)),
    );
    for _ in 0..SLICES {
        assert_eq!(job.run(db.vfs.monotonic_nanos()).unwrap(), JobPoll::Pending);
    }
    db.vfs.crash(CrashKind::Power);
    drop(job);

    let vfs: VfsRef = db.vfs.clone();
    let opened = Pager::open(&vfs, "/db/data.phdb".as_ref(), true).unwrap();
    let pager = opened.finish(inputs.iter().map(|s| s.0.extent)).unwrap();
    let cache = Arc::new(BlockCache::new(1 << 20, 1));
    for ((meta, _), c) in inputs.iter().zip(&contents) {
        let reader = open_sst(&pager, &cache, meta);
        assert_eq!(&sst_entries(&reader), c);
    }
}

/// `TrivialMove` and `Drop` tasks need no I/O.
#[test]
#[cfg_attr(
    miri,
    ignore = "each SimVfs pager costs ~25 s under Miri; covered natively"
)]
fn non_rewrite_tasks_finish_empty() {
    let mut db = Db::new(10);
    let (family, inputs) = inputs_for_interrupt(&mut db);
    let mut t = task(vec![(1, vec![inputs[0].0.id])], 2);
    t.kind = TaskKind::TrivialMove;
    let mut job = CompactionJob::new(
        t,
        vec![inputs[0].1.clone()],
        db.context(family, policy(vec![], 0, true)),
    );
    assert_eq!(job.run(0).unwrap(), JobPoll::Done);
    let out = job.finish().unwrap();
    assert!(out.added.is_empty() && out.removed.is_empty());
}

/// The cursor stays within the task's subranges.
#[test]
#[cfg_attr(
    miri,
    ignore = "each SimVfs pager costs ~25 s under Miri; covered natively"
)]
fn subranges_bound_the_inputs() {
    let mut db = Db::new(11);
    let family = FamilyOptions::default();
    let e: Vec<_> = (0..10u8)
        .map(|r| (key(&[b'a' + r], b"q", 10, 1, Kind::Put), stored(b"v")))
        .collect();
    let input = db.sst(&family, &e);
    let mut t = task(vec![(1, vec![input.0.id])], 2);
    t.range = KeyRange {
        start: Some(row_prefix(b"b")),
        end: Some(row_prefix(b"h")),
    };
    t.subranges = vec![
        KeyRange {
            start: None,
            end: Some(row_prefix(b"d")),
        },
        KeyRange {
            start: Some(row_prefix(b"f")),
            end: None,
        },
    ];
    let mut job = CompactionJob::new(
        t,
        vec![input.1],
        db.context(family, policy(vec![], 100, true)),
    );
    run_sliced(&db, &mut job);
    let out = job.finish().unwrap();
    let mut rows = Vec::new();
    for (_, m) in &out.added {
        for (k, _) in sst_entries(&open_sst(&db.pager, &db.cache, m)) {
            rows.push(
                pigeonhole_format::decode_key(&k)
                    .unwrap()
                    .row
                    .as_escaped()
                    .to_vec(),
            );
        }
    }
    assert_eq!(
        rows,
        [b"b".to_vec(), b"c".to_vec(), b"f".to_vec(), b"g".to_vec()]
    );
}

/// A put at one timestamp shadows older entries at that timestamp in its stripe.
#[test]
#[cfg_attr(
    miri,
    ignore = "each SimVfs pager costs ~25 s under Miri; covered natively"
)]
fn same_timestamp_puts_shadow_older_ones() {
    let mut db = Db::new(12);
    let family = FamilyOptions::default();
    let e = vec![
        (key(b"r", b"q", 10, 3, Kind::Put), stored(b"c")),
        (
            key(b"r", b"q", 10, 2, Kind::Merge),
            stored(&1i64.to_le_bytes()),
        ),
        (key(b"r", b"q", 10, 1, Kind::Put), stored(b"a")),
    ];
    let input = db.sst(&family, &e);
    let (_, kept) = compact(
        &db,
        &family,
        std::slice::from_ref(&input),
        policy(vec![], 100, false),
    );
    assert_eq!(kept, e[..1]);
    // A snapshot at 2 still needs the operand and its base.
    let (_, kept) = compact(&db, &family, &[input], policy(vec![2], 100, false));
    assert_eq!(kept, e);
}

/// A row split across two bottom-level SSTs: X ends with row `m`'s family marker, Y starts
/// with the cells it hides. An L1 file overlapping only X must still bring Y along, or the
/// bottommost rewrite of X purges the marker and Y's cells come back.
#[test]
fn a_row_split_across_bottom_ssts_moves_together() {
    let mut db = Db::new(13);
    let family = FamilyOptions::default();
    let mut marker = Vec::new();
    encode_marker_key(&mut marker, b"m", 50, 5).unwrap();
    let x = db.sst(
        &family,
        &[
            (key(b"c", b"q", 10, 2, Kind::Put), stored(b"c")),
            (marker, vec![]),
        ],
    );
    let y = db.sst(
        &family,
        &[
            (key(b"m", b"b", 10, 1, Kind::Put), stored(b"hidden")),
            (key(b"z", b"q", 10, 3, Kind::Put), stored(b"z")),
        ],
    );
    let f = db.sst(
        &family,
        &[
            (key(b"a", b"q", 60, 6, Kind::Put), stored(b"a")),
            (key(b"d", b"q", 60, 7, Kind::Put), stored(b"d")),
        ],
    );
    let all = [&f, &x, &y];

    let get = |ssts: &[Arc<SstReader>], row: &[u8], q: &[u8]| {
        let mut r = sst_resolver(ssts, pigeonhole_compaction::ResolveOptions::new(99, 100));
        r.seek_column(row, q).unwrap();
        r.next_cell().unwrap().map(|c| c.value.to_vec())
    };
    let before: Vec<_> = all.iter().map(|s| s.1.clone()).collect();
    assert_eq!(get(&before, b"m", b"b"), None);

    let mut options = PickerOptions::default();
    options.level_base_bytes = 1;
    options.max_levels = 3;
    let picker = CompactionPicker::new(CompactionStyle::Leveled, options);
    let levels = Levels {
        levels: vec![
            vec![],
            vec![Arc::new(f.0.clone())],
            vec![Arc::new(x.0.clone()), Arc::new(y.0.clone())],
        ],
    };
    let t = picker
        .pick(TabletId(1), FamilyId(1), &levels, &[], 0, 0)
        .unwrap();
    assert_eq!(t.inputs, [(1, vec![f.0.id]), (2, vec![x.0.id, y.0.id])]);

    let inputs: Vec<_> = all.iter().map(|s| s.1.clone()).collect();
    let mut job = CompactionJob::new(t, inputs, db.context(family, policy(vec![], 100, true)));
    run_sliced(&db, &mut job);
    let out = job.finish().unwrap();
    let after: Vec<_> = out
        .added
        .iter()
        .map(|(_, m)| open_sst(&db.pager, &db.cache, m))
        .collect();
    assert_eq!(get(&after, b"m", b"b"), None);
    assert_eq!(get(&after, b"c", b"q"), Some(stored(b"c")));
    assert_eq!(get(&after, b"z", b"q"), Some(stored(b"z")));
}

/// Commits `before`, compacts all of it as the bottommost level with nothing above, purges
/// the model the same way, then commits `later` on top and compares every read with the
/// model at the live snapshots and the latest one.
fn check_purge_scenario(
    seed: u64,
    family: pigeonhole_sim::ModelFamily,
    before: &[(Vec<pigeonhole_sim::ModelOp>, Timestamp)],
    snapshots: Vec<Seqno>,
    later: &[(Vec<pigeonhole_sim::ModelOp>, Timestamp)],
) {
    let mut db = Db::new(seed);
    let mut h = History::new(family);
    for (ops, ts) in before {
        h.commit(ops, *ts);
    }
    let fam = family_options(&h);
    let mut entries: Vec<_> = h
        .entries
        .iter()
        .map(|e| (e.0.clone(), e.1.clone()))
        .collect();
    entries.sort();
    let input = db.sst(&fam, &entries);
    let now = h.last_ts + 1;
    let gc = policy(snapshots.clone(), now, true);
    let (out, _) = compact(&db, &fam, std::slice::from_ref(&input), gc);
    h.model.purge(&ModelPurge {
        table: TABLE.into(),
        family: FAMILY.into(),
        rows: (Bound::Unbounded, Bound::Unbounded),
        snapshots: snapshots.clone(),
        now,
        min_ts_above: u64::MAX,
        max_seqno: h.model.snapshot(),
    });
    let max = h.model.snapshot();
    let mut ssts: Vec<_> = out
        .added
        .iter()
        .map(|(_, m)| open_sst(&db.pager, &db.cache, m))
        .collect();
    let mut flushed = Vec::new();
    for (ops, ts) in later {
        flushed.extend(h.commit(ops, *ts).into_iter().map(|e| (e.0, e.1)));
    }
    flushed.sort();
    if !flushed.is_empty() {
        ssts.push(db.sst(&fam, &flushed).1);
    }
    let read_now = h.last_ts + 1;
    for s in snapshots.iter().copied().chain([max, h.model.snapshot()]) {
        let expected = model_reads(&h, s, read_now);
        let actual = resolver_reads(&h, s, read_now, |o| sst_resolver(&ssts, o));
        assert_same(&format!("scenario {seed}, read at {s}"), &expected, &actual);
    }
}

/// Owner decision (HBase semantics): a purge may uncover later writes with older explicit
/// timestamps; the model's purge hook removes exactly what compaction does, including the
/// deletes it keeps because they are newer than the one purged at the same timestamp.
#[test]
#[cfg_attr(
    miri,
    ignore = "each SimVfs pager costs ~25 s under Miri; covered natively"
)]
fn purges_match_the_model_purge_hook() {
    use pigeonhole_sim::{ModelFamily, ModelOp};
    let fam = |max_versions| ModelFamily {
        name: FAMILY.into(),
        max_versions,
        ttl_micros: 0,
        i64_add: true,
    };
    let (t, f, r, q) = (
        TABLE.to_string(),
        FAMILY.to_string(),
        b"r".to_vec(),
        b"q".to_vec(),
    );
    let put = |ts: Option<Timestamp>, v: &[u8]| ModelOp::Put {
        table: t.clone(),
        row: r.clone(),
        family: f.clone(),
        qualifier: q.clone(),
        ts,
        value: v.to_vec(),
    };
    let del_cell = |ts| ModelOp::DeleteCell {
        table: t.clone(),
        row: r.clone(),
        family: f.clone(),
        qualifier: q.clone(),
        ts,
    };
    let del_col = || ModelOp::DeleteColumn {
        table: t.clone(),
        row: r.clone(),
        family: f.clone(),
        qualifier: q.clone(),
    };
    let del_fam = || ModelOp::DeleteFamily {
        table: t.clone(),
        row: r.clone(),
        family: f.clone(),
    };
    // Two cell deletes at one timestamp on either side of a snapshot: the older is purged,
    // the newer stays and keeps hiding a later put at that timestamp.
    check_purge_scenario(
        1,
        fam(0),
        &[
            (vec![put(Some(5), b"a")], 10),
            (vec![del_cell(5)], 20),
            (vec![del_cell(5)], 30),
        ],
        vec![2],
        &[(vec![put(Some(5), b"later")], 40)],
    );
    // The same for column deletes and family markers at one timestamp.
    check_purge_scenario(
        2,
        fam(0),
        &[
            (vec![put(Some(40), b"a")], 10),
            (vec![del_col()], 50),
            (vec![del_col()], 50),
        ],
        vec![2],
        &[(vec![put(Some(45), b"later")], 60)],
    );
    check_purge_scenario(
        3,
        fam(0),
        &[
            (vec![put(Some(40), b"a")], 10),
            (vec![del_fam()], 50),
            (vec![del_fam()], 50),
        ],
        vec![2],
        &[(vec![put(Some(45), b"later")], 60)],
    );
    // A purged marker takes the column delete it covers with it: the later put is visible.
    check_purge_scenario(
        4,
        fam(0),
        &[
            (vec![put(Some(40), b"a")], 10),
            (vec![del_fam(), del_col()], 50),
        ],
        vec![],
        &[(vec![put(Some(45), b"later")], 60)],
    );
    // A cell delete at a purged column delete's timestamp goes with it, even when newer.
    check_purge_scenario(
        6,
        fam(0),
        &[
            (vec![put(Some(40), b"a")], 10),
            (vec![del_col()], 50),
            (vec![del_cell(50)], 60),
        ],
        vec![2],
        &[(vec![put(Some(50), b"later")], 70)],
    );
    // A version purged over max_versions does not come back when the newest is deleted.
    check_purge_scenario(
        5,
        fam(1),
        &[
            (vec![put(Some(10), b"old")], 10),
            (vec![put(Some(20), b"new")], 20),
        ],
        vec![],
        &[(vec![del_cell(20)], 30)],
    );
}

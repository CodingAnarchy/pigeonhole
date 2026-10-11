//! The durability matrix under the simulator: a returned `Buffered` commit survives a process
//! kill, a returned `GroupSync` or `Sync` commit survives power loss with torn and reordered
//! writes, crashing at every mutating operation; replay is always an in-order prefix of the
//! appended records after the checkpoint, and never includes stale data from recycled slots.
//! Every seeded test names its seed in its failure message.

mod common;

use common::*;
use pigeonhole_format::wal::FRAME_SIZE;
use pigeonhole_format::{Durability, Lsn};
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};
use pigeonhole_io::{ErrorKind, OpenOptions, VfsRef};
use pigeonhole_wal::{CommitTicket, Error, Wal, WalOptions, WalStream};

/// SplitMix64, so the workload is reproducible from its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// What the workload knew when it stopped.
#[derive(Default)]
struct Trace {
    /// Every appended record with its ticket, in order.
    appended: Vec<(CommitTicket, Rec)>,
    /// Tickets whose commit was acknowledged (their level was met when a call returned).
    acked: Vec<CommitTicket>,
    /// The latest checkpoint handed to the stream (what the manifest would hold).
    checkpoint: Lsn,
    /// Set when an operation failed with something other than `Crashed`.
    unexpected: Option<String>,
}

const GROUPS: u64 = 14;

/// Runs group commits until the simulator crashes (or `GROUPS` groups are done).
fn workload(vfs: &VfsRef, opts: WalOptions, seed: u64) -> Trace {
    let mut rng = Rng(seed);
    let mut trace = Trace::default();
    let mut wal = match WalStream::create(vfs, db(), STREAM, DB_ID, opts) {
        Ok(w) => w,
        Err(e) => {
            note(&mut trace, e);
            return trace;
        }
    };
    let mut seqno = 0;
    'groups: for _ in 0..GROUPS {
        let n = 1 + rng.below(4);
        let mut group = Vec::new();
        let mut strongest = Durability::Buffered;
        for _ in 0..n {
            seqno += 1;
            let size = match rng.below(4) {
                0 => rng.below(64) as usize,
                1 => FRAME_SIZE - 40 + rng.below(80) as usize,
                _ => rng.below(60_000) as usize,
            };
            let rec = match rng.below(6) {
                0 => prepare(seqno),
                1 => commit(seqno, &[STREAM]),
                _ => batch(seqno, size),
            };
            let level = [
                Durability::Buffered,
                Durability::GroupSync,
                Durability::Sync,
            ][rng.below(3) as usize];
            strongest = strongest.max(level);
            match wal.append(&rec.record(), level) {
                Ok(t) => {
                    group.push(t);
                    trace.appended.push((t, rec));
                }
                Err(e) => {
                    note(&mut trace, e);
                    break 'groups;
                }
            }
        }
        let synced = match strongest {
            Durability::Buffered => wal.write().map(|_| false),
            Durability::Sync => wal.sync().map(|_| true),
            _ => wal
                .submit_sync()
                .and_then(|c| c.wait().map_err(Error::from))
                .map(|_| true),
        };
        match synced {
            Ok(synced) => {
                for t in &group {
                    if synced || t.durability == Durability::Buffered {
                        assert!(wal.satisfies(t), "seed {seed}: {t:?} not satisfied");
                        trace.acked.push(*t);
                    }
                }
            }
            Err(e) => {
                note(&mut trace, e);
                break;
            }
        }
        // The engine checkpoints positions whose data was flushed, synced or not.
        if rng.below(3) == 0 {
            let cp = group[rng.below(group.len() as u64) as usize].end;
            if cp > trace.checkpoint {
                if let Err(e) = wal.checkpoint(cp) {
                    note(&mut trace, e);
                    break;
                }
                trace.checkpoint = cp;
            }
        }
    }
    trace
}

fn note(trace: &mut Trace, e: Error) {
    if !matches!(&e, Error::Io(io) if io.kind == ErrorKind::Crashed) {
        trace.unexpected = Some(e.to_string());
    }
}

/// Checks a replay against the trace: an in-order prefix of what was appended after the
/// checkpoint, containing every record `must_survive` admits.
fn check(vfs: &VfsRef, trace: &Trace, must_survive: impl Fn(&CommitTicket) -> bool, ctx: &str) {
    let (got, r) = replay(vfs, trace.checkpoint).unwrap_or_else(|e| panic!("{ctx}: {e}"));
    let expected: Vec<&(CommitTicket, Rec)> = trace
        .appended
        .iter()
        .filter(|(t, _)| t.end > trace.checkpoint)
        .collect();
    assert!(
        got.records.len() <= expected.len(),
        "{ctx}: more records than appended"
    );
    for ((end, bytes), (t, rec)) in got.records.iter().zip(&expected) {
        assert_eq!(*end, t.end, "{ctx}: record position");
        assert_eq!(*bytes, rec.bytes(), "{ctx}: record bytes");
    }
    // A record must survive if a commit at or after it was acknowledged at a level the crash
    // respects: the stream is ordered, so that acknowledgement covers everything before it.
    let required_prefix = expected
        .iter()
        .take_while(|(t, _)| {
            trace
                .acked
                .iter()
                .any(|a| a.end >= t.end && must_survive(a))
        })
        .count();
    assert!(
        got.records.len() >= required_prefix,
        "{ctx}: acknowledged commits lost: replayed {} of {} (required {required_prefix})\n\
         appended: {:?}\nacked: {:?}\nreplayed: {:?}\ncheckpoint {:?}",
        got.records.len(),
        expected.len(),
        trace
            .appended
            .iter()
            .map(|(t, _)| (t.end, t.durability))
            .collect::<Vec<_>>(),
        trace.acked.iter().map(|t| t.end).collect::<Vec<_>>(),
        got.records.iter().map(|(l, _)| *l).collect::<Vec<_>>(),
        trace.checkpoint,
    );
    // The log ends at the last record, or at the start of an empty successor segment whose
    // header was synced before its records were lost.
    if let Some((end, _)) = got.records.last() {
        assert!(
            got.end == *end
                || (got.end.epoch() > end.epoch() && got.end.offset() == FRAME_SIZE as u32),
            "{ctx}: end {:?} vs last record {end:?}",
            got.end
        );
    }
    let max_seen = got.seqnos().into_iter().max().unwrap_or(0);
    assert_eq!(got.max_seqno, max_seen, "{ctx}: max_seqno");

    // Recovery never appends to the torn segment and the result stays replayable.
    let old_end = got.end;
    let mut wal = r
        .into_stream(opts(4, 1))
        .unwrap_or_else(|e| panic!("{ctx}: {e}"));
    assert!(
        wal.written().epoch() > old_end.epoch(),
        "{ctx}: fresh epoch"
    );
    let extra = batch(1_000_000, 100);
    let t = wal
        .append(&extra.record(), Durability::GroupSync)
        .unwrap_or_else(|e| panic!("{ctx}: {e}"));
    wal.sync().unwrap_or_else(|e| panic!("{ctx}: {e}"));
    drop(wal);
    let (again, _) = replay(vfs, trace.checkpoint).unwrap_or_else(|e| panic!("{ctx}: {e}"));
    assert_eq!(
        again.records.len(),
        got.records.len() + 1,
        "{ctx}: after reopen"
    );
    assert_eq!(
        again.records[..got.records.len()],
        got.records[..],
        "{ctx}: after reopen"
    );
    assert_eq!(
        again.records.last().unwrap(),
        &(t.end, extra.bytes()),
        "{ctx}"
    );
    assert_eq!(again.end, t.end, "{ctx}");
}

/// Reopens after a crash and crashes again at every operation of `into_stream` and the first
/// group after it (torn header in a recycled or fresh slot included), then recovers once more.
#[test]
fn crash_during_reopen_at_every_operation() {
    let opts = opts(4, 1);
    for seed in 300..306u64 {
        // A workload, a power loss, and a reopen crashed after `crash_at` of its operations.
        let run = |crash_at: Option<u64>| {
            let mut plan = FaultPlan::none();
            plan.torn_writes = true;
            let sim = SimVfs::with_faults(seed, plan.clone());
            let vfs: VfsRef = sim.clone();
            let trace = workload(&vfs, opts, seed);
            sim.crash(CrashKind::Power);
            let (before, r) = replay(&vfs, trace.checkpoint).unwrap();
            let ops_before = sim.mutating_ops();
            plan.crash_after_ops = crash_at.map(|n| ops_before + n);
            sim.set_faults(plan);
            let reopened = r.into_stream(opts).and_then(|mut wal| {
                let extra = batch(2_000_000, 100);
                let t = wal.append(&extra.record(), Durability::GroupSync)?;
                wal.sync()?;
                Ok((t, extra.bytes()))
            });
            (sim, vfs, trace, before, reopened, ops_before)
        };
        let (sim, _, _, _, reopened, ops_before) = run(None);
        assert!(reopened.is_ok(), "seed {seed}: clean reopen");
        let reopen_ops = sim.mutating_ops() - ops_before;
        assert!(reopen_ops >= 4, "seed {seed}: {reopen_ops} ops");
        for n in 1..=reopen_ops {
            let (sim, vfs, trace, before, reopened, ops_before) = run(Some(n));
            let ctx = format!("seed {seed} reopen crash after op {n}");
            assert!(sim.mutating_ops() >= ops_before + n, "{ctx}: never crashed");
            match &reopened {
                Ok(_) => {}
                Err(Error::Io(e)) => assert_eq!(e.kind, ErrorKind::Crashed, "{ctx}"),
                Err(e) => panic!("{ctx}: {e}"),
            }
            // Whatever the crash tore, the records replayed before it are still there, in
            // order, followed at most by the one record the reopened stream acknowledged.
            let (after, r) =
                replay(&vfs, trace.checkpoint).unwrap_or_else(|e| panic!("{ctx}: {e}"));
            assert!(
                after.records.len() >= before.records.len(),
                "{ctx}: records lost"
            );
            assert_eq!(
                after.records[..before.records.len()],
                before.records[..],
                "{ctx}"
            );
            match &reopened {
                Ok((t, bytes)) => {
                    assert_eq!(after.records.len(), before.records.len() + 1, "{ctx}");
                    assert_eq!(
                        after.records.last().unwrap(),
                        &(t.end, bytes.clone()),
                        "{ctx}"
                    );
                }
                Err(_) => assert!(after.records.len() <= before.records.len() + 1, "{ctx}"),
            }
            // And the stream can be reopened once more.
            let mut wal = r.into_stream(opts).unwrap_or_else(|e| panic!("{ctx}: {e}"));
            assert!(wal.written().epoch() > after.end.epoch(), "{ctx}");
            wal.append(&batch(3_000_000, 10).record(), Durability::GroupSync)
                .unwrap_or_else(|e| panic!("{ctx}: {e}"));
            wal.sync().unwrap_or_else(|e| panic!("{ctx}: {e}"));
        }
    }
}

fn durable_levels(t: &CommitTicket) -> bool {
    t.durability >= Durability::GroupSync
}

#[test]
fn group_sync_and_sync_survive_power_loss_at_every_operation() {
    let opts = opts(4, 1);
    for seed in 0..6u64 {
        // Size the sweep from a crash-free run.
        let dry = SimVfs::new(seed);
        let dry_vfs: VfsRef = dry.clone();
        let trace = workload(&dry_vfs, opts, seed);
        assert_eq!(trace.unexpected, None, "seed {seed}");
        check(&dry_vfs, &trace, |_| true, &format!("seed {seed} no crash"));
        let total = dry.mutating_ops();
        assert!(total > 20, "seed {seed}: {total} ops");

        for n in 1..=total {
            for (torn, reorder) in [(false, false), (true, false), (true, true)] {
                let mut plan = FaultPlan::none();
                plan.torn_writes = torn;
                plan.reorder_unsynced = reorder;
                plan.crash_after_ops = Some(n);
                let sim = SimVfs::with_faults(seed, plan);
                let vfs: VfsRef = sim.clone();
                let trace = workload(&vfs, opts, seed);
                let ctx = format!("seed {seed} crash after op {n} torn={torn} reorder={reorder}");
                assert_eq!(trace.unexpected, None, "{ctx}");
                if sim.mutating_ops() < n {
                    continue; // never crashed (the sweep ran past the workload)
                }
                // A crash before the stream file was durable means nothing to recover.
                if !vfs.exists(&path()).unwrap() {
                    assert!(trace.acked.is_empty(), "{ctx}: acked without a file");
                    continue;
                }
                check(&vfs, &trace, durable_levels, &ctx);
            }
        }
    }
}

#[test]
fn buffered_survives_process_kill() {
    let opts = opts(4, 1);
    for seed in 100..130u64 {
        let sim = SimVfs::new(seed);
        let vfs: VfsRef = sim.clone();
        let trace = workload(&vfs, opts, seed);
        assert_eq!(trace.unexpected, None, "seed {seed}");
        sim.crash(CrashKind::Process);
        // Nothing handed to the kernel is lost: every acknowledged commit, at any level.
        check(&vfs, &trace, |_| true, &format!("seed {seed} process kill"));
        let (got, _) = replay(&vfs, trace.checkpoint).unwrap();
        let written: Vec<_> = trace
            .appended
            .iter()
            .filter(|(t, _)| t.end > trace.checkpoint && trace.acked.contains(t))
            .map(|(t, _)| t.end)
            .collect();
        let replayed: Vec<_> = got.records.iter().map(|(l, _)| *l).collect();
        assert!(
            replayed.starts_with(&written),
            "seed {seed}: {replayed:?} vs {written:?}"
        );
    }
}

#[test]
fn buffered_may_be_lost_to_power_loss_but_never_out_of_order() {
    // Not a durability promise, just the shape of what survives: a prefix.
    let opts = opts(4, 1);
    let mut lost_something = false;
    for seed in 200..240u64 {
        let mut plan = FaultPlan::none();
        plan.torn_writes = true;
        let sim = SimVfs::with_faults(seed, plan);
        let vfs: VfsRef = sim.clone();
        let trace = workload(&vfs, opts, seed);
        sim.crash(CrashKind::Power);
        check(
            &vfs,
            &trace,
            durable_levels,
            &format!("seed {seed} power loss"),
        );
        let (got, _) = replay(&vfs, trace.checkpoint).unwrap();
        let appended = trace
            .appended
            .iter()
            .filter(|(t, _)| t.end > trace.checkpoint)
            .count();
        lost_something |= got.records.len() < appended;
    }
    assert!(lost_something, "no seed lost an unsynced record");
}

/// A process killed between a slot's `allocate` and its `sync_all` leaves the slot in the
/// page cache but not on disk. Recovery adopts it as blank, so it must make the length
/// durable before the new stream writes there: appends sync with `sync_data` (issue #72).
#[test]
fn a_slot_grown_before_a_process_crash_survives_a_later_power_loss() {
    let sim = SimVfs::new(1);
    let vfs: VfsRef = sim.clone();
    let opts = opts(4, 0);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts).unwrap();
    let t1 = wal
        .append(&batch(1, 100).record(), Durability::GroupSync)
        .unwrap();
    wal.sync().unwrap();
    drop(wal);
    // The interrupted growth.
    let mut rw = OpenOptions::read();
    rw.write = true;
    let file = vfs.open(&path(), rw).unwrap();
    file.allocate(opts.segment_size, opts.segment_size).unwrap();
    drop(file);
    sim.crash(CrashKind::Process);

    let (_, r) = replay(&vfs, Lsn::default()).unwrap();
    let mut wal = r.into_stream(opts).unwrap();
    let t2 = wal
        .append(&batch(2, 100).record(), Durability::GroupSync)
        .unwrap();
    wal.sync().unwrap();
    drop(wal);

    sim.crash(CrashKind::Power);
    let (got, _) = replay(&vfs, Lsn::default()).unwrap();
    assert_eq!(got.seqnos(), [1, 2]);
    assert_eq!(got.end, t2.end);
    assert!(t2.end > t1.end);
}

#[test]
fn a_successor_whose_header_was_lost_over_a_recycled_slot_never_replays_after_a_reopen() {
    // #508's power-loss form, in 0.2.0 too: a successor's header frame and first records go
    // out in one unsynced write into a recycled slot, and a power loss keeps the records'
    // sectors but not the header's (a device does not order the sectors of one write). This
    // builds that outcome (the slot's old header frame put back) and kills the process; the
    // reopened stream must not take the lost header's epoch in that slot (D209).
    let opts = opts(4, 1);
    let seg = opts.segment_size;
    let mut hits = 0;
    for seed in 0..64u64 {
        let sim = SimVfs::new(seed);
        let vfs: VfsRef = sim.clone();
        let mut rng = Rng(seed);
        let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts).unwrap();
        let mut checkpoint = Lsn::default();
        let mut found = None;
        for seqno in 1..=300u64 {
            let before = headers(&vfs, seg);
            let f = vfs.open(&path(), OpenOptions::read()).unwrap();
            let old: Vec<Vec<u8>> = (0..before.len() as u64)
                .map(|slot| {
                    let mut b = vec![0u8; FRAME_SIZE];
                    f.read_at(&mut b, slot * seg).unwrap();
                    b
                })
                .collect();
            drop(f);
            let rec = batch(seqno, rng.below(20_000) as usize);
            let t = wal.append(&rec.record(), Durability::Buffered).unwrap();
            wal.write().unwrap();
            let after = headers(&vfs, seg);
            let recycled = (0..before.len()).find(|&i| match (&before[i], after.get(i)) {
                (Some(b), Some(Some(a))) => a.epoch > b.epoch,
                _ => false,
            });
            if let Some(slot) = recycled
                && wal.written().offset() as usize > FRAME_SIZE
            {
                found = Some((slot, old[slot].clone()));
                break;
            }
            wal.sync().unwrap();
            if rng.below(2) == 0 {
                wal.checkpoint(t.end).unwrap();
                checkpoint = t.end;
            }
        }
        let Some((slot, old)) = found else { continue };
        hits += 1;
        let mut rw = OpenOptions::read();
        rw.write = true;
        let f = vfs.open(&path(), rw).unwrap();
        f.write_at(&old, slot as u64 * seg).unwrap();
        drop(f);
        sim.crash(CrashKind::Process);
        drop(wal);
        let ctx = format!("seed {seed}, successor in slot {slot}");
        let (got, r) = replay(&vfs, checkpoint).unwrap_or_else(|e| panic!("{ctx}: {e}"));
        drop(r.into_stream(opts).unwrap_or_else(|e| panic!("{ctx}: {e}")));
        let (again, _) = replay(&vfs, checkpoint).unwrap_or_else(|e| panic!("{ctx}: {e}"));
        assert_eq!(
            again.records.iter().map(|(l, _)| *l).collect::<Vec<_>>(),
            got.records.iter().map(|(l, _)| *l).collect::<Vec<_>>(),
            "{ctx}: records appeared after a reopen that appended nothing"
        );
    }
    assert!(hits > 0, "no successor started in a recycled slot");
}

/// Loads `fixtures/NAME` (the stream file's length, then `(offset u64, len u32, bytes)` for
/// each run of non-zero bytes) as the stream file.
fn load_fixture(vfs: &VfsRef, name: &str) {
    let dump = std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name),
    )
    .unwrap();
    let u64_at = |i: usize| u64::from_le_bytes(dump[i..i + 8].try_into().unwrap());
    let f = vfs.open(&path(), OpenOptions::read_write_create()).unwrap();
    f.set_len(u64_at(0)).unwrap();
    let mut i = 8;
    while i < dump.len() {
        let offset = u64_at(i);
        let len = u32::from_le_bytes(dump[i + 8..i + 12].try_into().unwrap()) as usize;
        f.write_at(&dump[i + 12..i + 12 + len], offset).unwrap();
        i += 12 + len;
    }
    f.sync_all().unwrap();
    // The file's name too, as the database that wrote it had made it durable.
    vfs.sync_dir(path().parent().unwrap()).unwrap();
}

#[test]
fn a_stream_written_before_d209_recovers_and_reopens() {
    // `wal_reopened_0_2.bin` was written by 0.2.0 (opts(4, 0) is not it: two-frame slots,
    // no spares): records 1 to 3, a process kill, a reopen at max seen + 1 (epoch 2, the
    // spacing before D209), records 4 to 10 rolling over to epoch 3, record 11, and a kill.
    let opts = opts(2, 0);
    let sim = SimVfs::new(1);
    let vfs: VfsRef = sim.clone();
    load_fixture(&vfs, "wal_reopened_0_2.bin");
    let epochs = |vfs: &VfsRef| -> Vec<Option<u32>> {
        headers(vfs, opts.segment_size)
            .iter()
            .map(|h| h.as_ref().map(|h| h.epoch))
            .collect()
    };
    assert_eq!(epochs(&vfs), [Some(1), Some(2), Some(3)]);
    let (got, r) = replay(&vfs, Lsn::default()).unwrap();
    assert_eq!(got.seqnos(), (1..=11).collect::<Vec<_>>());
    let mut wal = r.into_stream(opts).unwrap();
    assert_eq!(wal.written().epoch(), 6, "max seen (3) + 3");
    let t = wal
        .append(&batch(12, 100).record(), Durability::GroupSync)
        .unwrap();
    wal.sync().unwrap();
    drop(wal);
    sim.crash(CrashKind::Power);
    let (got, r) = replay(&vfs, Lsn::default()).unwrap();
    assert_eq!(got.seqnos(), (1..=12).collect::<Vec<_>>());
    assert_eq!(got.end, t.end);
    // And once more from a checkpoint inside the old file's segments.
    drop(r.into_stream(opts).unwrap());
    let (again, _) = replay(&vfs, Lsn::default()).unwrap();
    assert_eq!(again.seqnos(), (1..=12).collect::<Vec<_>>());
}

/// Buffered appends until done or a crash: nothing syncs but the rollovers, so a segment's
/// header and first records, written in one write, stay unsynced until the next rollover
/// (#508). Checkpoints follow the durable position. Returns the last checkpoint.
fn buffered_rollovers(sim: &std::sync::Arc<SimVfs>, opts: WalOptions, seed: u64) -> Lsn {
    let vfs: VfsRef = sim.clone();
    let mut rng = Rng(seed);
    let mut checkpoint = Lsn::default();
    let Ok(mut wal) = WalStream::create(&vfs, db(), STREAM, DB_ID, opts) else {
        return checkpoint;
    };
    let mut ends = Vec::new();
    for seqno in 1..=GROUPS * 6 {
        let rec = batch(seqno, rng.below(30_000) as usize);
        let Ok(t) = wal.append(&rec.record(), Durability::Buffered) else {
            break;
        };
        if wal.write().is_err() {
            break;
        }
        ends.push(t.end);
        if rng.below(2) == 0
            && let Some(&cp) = ends.iter().rev().find(|&&e| e <= wal.durable())
            && cp > checkpoint
        {
            if wal.checkpoint(cp).is_err() {
                break;
            }
            checkpoint = cp;
        }
    }
    checkpoint
}

#[test]
fn reordering_power_losses_amid_buffered_rollovers_never_replay_new_records_after_a_reopen() {
    // #508: a power loss at every operation of a Buffered workload, unsynced writes reordered
    // (and torn): a segment's header may be lost while its records survive.
    // Recovery, a reopen that appends nothing, and a second replay must agree.
    let opts = opts(4, 1);
    let mut cases = 0;
    // A seed range of its own under the sweep workflow (`PIGEONHOLE_SEED`, `PIGEONHOLE_SEEDS`);
    // by default a few seeds and 361, whose torn header-and-records write reproduced #508 here.
    let env = |k: &str| std::env::var(k).ok().and_then(|s| s.parse::<u64>().ok());
    let seeds: Vec<u64> = match (env("PIGEONHOLE_SEED"), env("PIGEONHOLE_SEEDS")) {
        (None, None) => (0..6).chain([361]).collect(),
        (first, n) => {
            let first = first.unwrap_or(0);
            (first..first + n.unwrap_or(6)).collect()
        }
    };
    for seed in seeds {
        let dry = SimVfs::new(seed);
        buffered_rollovers(&dry, opts, seed);
        let total = dry.mutating_ops();
        for n in 1..=total {
            for torn in [false, true] {
                let mut plan = FaultPlan::none();
                plan.torn_writes = torn;
                plan.reorder_unsynced = true;
                plan.crash_after_ops = Some(n);
                let sim = SimVfs::with_faults(seed, plan);
                let checkpoint = buffered_rollovers(&sim, opts, seed);
                let vfs: VfsRef = sim.clone();
                if sim.mutating_ops() < n || !vfs.exists(&path()).unwrap() {
                    continue;
                }
                sim.set_faults(FaultPlan::none());
                let ctx = format!("seed {seed} power loss after op {n} torn={torn}");
                let (got, r) = replay(&vfs, checkpoint).unwrap_or_else(|e| panic!("{ctx}: {e}"));
                drop(r.into_stream(opts).unwrap_or_else(|e| panic!("{ctx}: {e}")));
                let (again, _) = replay(&vfs, checkpoint).unwrap_or_else(|e| panic!("{ctx}: {e}"));
                cases += 1;
                assert_eq!(
                    again.records.iter().map(|(l, _)| *l).collect::<Vec<_>>(),
                    got.records.iter().map(|(l, _)| *l).collect::<Vec<_>>(),
                    "{ctx}: records appeared after a reopen that appended nothing"
                );
            }
        }
    }
    assert!(cases > 0);
    eprintln!("{cases} crash cases");
}

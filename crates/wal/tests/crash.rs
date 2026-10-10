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
            // As the engine does: Buffered records a held segment header covers (#19) are
            // not handed over by `write`, so their group is resolved through a sync.
            Durability::Buffered => wal.write().and_then(|_| {
                if group.iter().all(|t| wal.satisfies(t)) {
                    Ok(false)
                } else {
                    wal.submit_sync()
                        .and_then(|c| c.wait().map_err(Error::from))
                        .map(|_| true)
                }
            }),
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

// ---- crash points inside a rollover's window (#19) ----

/// What the deferred workload saw: the trace, and the held header (epoch, stale epoch) when
/// it stopped, if a crash hit inside a rollover's window.
struct DeferredRun {
    trace: Trace,
    held_at_crash: Option<(u32, Option<u32>)>,
}

/// Completes deferred I/O until `done` holds or nothing is in flight.
fn drive(sim: &SimVfs, done: impl Fn() -> bool) {
    while !done() && sim.complete_io() {}
}

/// The workload with deferred I/O: syncs complete only when the harness completes them, in
/// a seeded order, so a rollover's sync is often still in flight while the next records are
/// written under the successor's held-back header. The workload checks `blocked` before each
/// append and completes I/O while it holds, as the engine waits for `notify_unblocked`.
fn deferred_workload(sim: &std::sync::Arc<SimVfs>, opts: WalOptions, seed: u64) -> DeferredRun {
    deferred_workload_until(sim, opts, seed, u64::MAX)
}

/// [`deferred_workload`], stopping before its append number `appends` (mid-group) with
/// whatever is in flight still in flight (no final completion).
fn deferred_workload_until(
    sim: &std::sync::Arc<SimVfs>,
    opts: WalOptions,
    seed: u64,
    appends: u64,
) -> DeferredRun {
    sim.set_deferred_io(true);
    let vfs: VfsRef = sim.clone();
    let mut rng = Rng(seed);
    let mut trace = Trace::default();
    let mut held_at_crash = None;
    let mut wal = match WalStream::create(&vfs, db(), STREAM, DB_ID, opts) {
        Ok(w) => w,
        Err(e) => {
            note(&mut trace, e);
            return DeferredRun {
                trace,
                held_at_crash,
            };
        }
    };
    let mut seqno = 0;
    'groups: for _ in 0..GROUPS * 3 {
        let n = 1 + rng.below(4);
        let mut group = Vec::new();
        let mut strongest = Durability::Buffered;
        for _ in 0..n {
            seqno += 1;
            let rec = batch(seqno, rng.below(30_000) as usize);
            let level = [
                Durability::Buffered,
                Durability::GroupSync,
                Durability::Sync,
            ][rng.below(3) as usize];
            strongest = strongest.max(level);
            if seqno > appends {
                return DeferredRun {
                    held_at_crash: wal.held_header(),
                    trace,
                };
            }
            drive(sim, || !wal.blocked());
            let appended = wal.append(&rec.record(), level);
            if appended.is_err() {
                held_at_crash = wal.held_header();
            }
            match appended {
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
        let written = wal.write();
        let synced = written.and_then(|_| {
            if strongest == Durability::Buffered && group.iter().all(|t| wal.satisfies(t)) {
                return Ok(false);
            }
            let c = wal.submit_sync()?;
            drive(sim, || c.is_ready());
            c.wait().map_err(Error::from).map(|_| true)
        });
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
                held_at_crash = wal.held_header();
                note(&mut trace, e);
                break;
            }
        }
        // Leave a seeded number of operations in flight (a rollover's sync among them).
        for _ in 0..rng.below(3) {
            sim.complete_io();
        }
        // Checkpoint often, so slots are recycled and successors land on stale headers.
        if rng.below(2) == 0 {
            let cp = group[rng.below(group.len() as u64) as usize].end;
            if cp > trace.checkpoint && cp <= wal.durable() {
                if let Err(e) = wal.checkpoint(cp) {
                    note(&mut trace, e);
                    break;
                }
                trace.checkpoint = cp;
            }
        }
        if held_at_crash.is_none() {
            held_at_crash = wal.held_header();
        }
    }
    // The run ended without a crash (or with one between calls): what is held now.
    sim.complete_all_io();
    DeferredRun {
        trace,
        held_at_crash,
    }
}

#[test]
fn crashes_inside_a_rollovers_window_lose_no_acknowledged_commit() {
    // FORMAT §10.1 rule 1 lets a successor's records be written before its header, which
    // waits for the full segment's sync: under the slot's stale header (a recycled slot's
    // older epoch) they must never replay, and every acknowledged commit must survive.
    let opts = opts(16, 1);
    let (mut in_window, mut over_recycled) = (0, 0);
    let mut closest_stale = u32::MAX;
    for seed in 0..4u64 {
        let dry = SimVfs::new(seed);
        let run = deferred_workload(&dry, opts, seed);
        assert_eq!(run.trace.unexpected, None, "seed {seed}");
        let total = dry.mutating_ops();
        for n in 1..=total {
            for (torn, reorder) in [(false, false), (true, true)] {
                let mut plan = FaultPlan::none();
                plan.torn_writes = torn;
                plan.reorder_unsynced = reorder;
                plan.crash_after_ops = Some(n);
                let sim = SimVfs::with_faults(seed, plan);
                let run = deferred_workload(&sim, opts, seed);
                let ctx = format!(
                    "seed {seed} crash after op {n} torn={torn} reorder={reorder} held {:?}",
                    run.held_at_crash
                );
                assert_eq!(run.trace.unexpected, None, "{ctx}");
                if sim.mutating_ops() < n {
                    continue;
                }
                if let Some((epoch, stale)) = run.held_at_crash {
                    in_window += 1;
                    if let Some(stale) = stale {
                        over_recycled += 1;
                        closest_stale = closest_stale.min(epoch - stale);
                    }
                }
                let vfs: VfsRef = sim.clone();
                if !vfs.exists(&path()).unwrap() {
                    assert!(run.trace.acked.is_empty(), "{ctx}: acked without a file");
                    continue;
                }
                check(&vfs, &run.trace, durable_levels, &ctx);
            }
        }
    }
    // The sweep did crash inside the window, over recycled slots too.
    assert!(
        in_window > 0 && over_recycled > 0,
        "{in_window} in the window, {over_recycled} over a recycled slot"
    );
    eprintln!(
        "{in_window} crashes in a rollover window, {over_recycled} over a recycled slot; closest stale epoch {closest_stale} below the held one"
    );
}

#[test]
fn buffered_survives_a_process_kill_inside_a_rollovers_window() {
    // Records written under a held-back header are not handed over (`written` stays at the
    // full segment's end), so no Buffered commit is acknowledged on them until the header
    // is written: a process kill then loses no acknowledged commit at any level.
    let opts = opts(16, 1);
    let mut in_window = 0;
    for seed in 200..212u64 {
        for appends in 1..GROUPS * 6 {
            let sim = SimVfs::new(seed);
            let run = deferred_workload_until(&sim, opts, seed, appends);
            assert_eq!(run.trace.unexpected, None, "seed {seed}");
            in_window += usize::from(run.held_at_crash.is_some());
            sim.crash(CrashKind::Process);
            let vfs: VfsRef = sim.clone();
            check(
                &vfs,
                &run.trace,
                |_| true,
                &format!(
                    "seed {seed} process kill after {appends} appends, held {:?}",
                    run.held_at_crash
                ),
            );
        }
    }
    assert!(in_window > 0, "no kill landed inside a rollover's window");
    eprintln!("{in_window} process kills inside a rollover window");
}

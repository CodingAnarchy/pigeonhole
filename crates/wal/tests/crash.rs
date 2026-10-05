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
use pigeonhole_io::{ErrorKind, VfsRef};
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

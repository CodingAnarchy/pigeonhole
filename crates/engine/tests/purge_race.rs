//! #316: a bottommost compaction samples `GcPolicy::min_ts_above` when it plans and purges
//! below it (D70). A write committed into another source before the compaction installs, at
//! an explicit timestamp below that sample, is not in the sample. A cell delete of the newest
//! version then exposes an older one that the compaction purges, so a read changes when the
//! compaction installs, with no write in between. The compaction runs under a guard (as a
//! flush's purge does, #287): such a write voids it before its install, or waits while it
//! installs. A slot voided three times in a row compacts once without the purge.

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use common::{poll_commit, value_bytes};
use pigeonhole_engine::{
    CommitInfo, Durability, Engine, EngineShard, FamilyId, FamilyOptions, FlushGcMutation,
    PendingCommit, TableId, ValueRef, WriteBatch,
};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";

/// `PURGE_VOID_LIMIT` in the engine.
const VOID_LIMIT: u64 = 3;

#[test]
fn a_delete_below_min_ts_above_during_a_bottommost_compaction_keeps_reads() {
    // Versions at 10 and 20 (max_versions 1: the compaction purges 10); a cell delete at 20
    // exposes 10.
    for cross in Cross::ALL {
        check(&Case {
            seed: 316,
            first: |wb, t, f| wb.put(t, f, b"r", b"q", Some(10), ValueRef::Bytes(b"old")),
            second: |wb, t, f| wb.put(t, f, b"r", b"q", Some(20), ValueRef::Bytes(b"new")),
            raise_floor: false,
            during: |wb, t, f| wb.delete_cell(t, f, b"r", b"q", 20),
            cross,
        });
    }
}

#[test]
fn a_put_below_min_ts_above_during_a_bottommost_compaction_keeps_reads() {
    // A column delete at 15 over a version at 12 (the compaction purges both: the delete is
    // visible at every read point); a put at 5 is hidden by the delete until it is purged.
    for cross in Cross::ALL {
        check(&Case {
            seed: 317,
            first: |wb, t, f| wb.put(t, f, b"r", b"q", Some(12), ValueRef::Bytes(b"old")),
            second: |wb, t, f| wb.delete_column(t, f, b"r", b"q", Some(15)),
            raise_floor: false,
            during: |wb, t, f| wb.put(t, f, b"r", b"q", Some(5), ValueRef::Bytes(b"late")),
            cross,
        });
    }
}

#[test]
fn a_share_at_a_lagging_default_timestamp_voids_the_purge() {
    // With tablet changes off a share keeps its coordinator's commit timestamp even below
    // the participant's floor (D11 refuses that only with tablet changes on). Here the other
    // shard's clock-based timestamp is the first default one, below the inputs: a column
    // delete at the second over a put at the first. The share's put lands at the first
    // timestamp, hidden by the delete until it is purged. It counts at its commit
    // timestamp, not the participant's floor (raised above every input by a write to
    // another table on the shard).
    check(&Case {
        seed: 321,
        first: |wb, t, f| wb.put(t, f, b"r", b"q", None, ValueRef::Bytes(b"old")),
        second: |wb, t, f| wb.delete_column(t, f, b"r", b"q", None),
        raise_floor: true,
        during: |wb, t, f| wb.put(t, f, b"r", b"q", None, ValueRef::Bytes(b"late")),
        cross: Cross::There,
    });
}

#[test]
fn a_write_above_the_inputs_does_not_void_the_purge() {
    // Max_versions 1 over versions at 10 and 20; a put at 30 is above every input.
    for cross in Cross::ALL {
        let case = Case {
            seed: 318,
            first: |wb, t, f| wb.put(t, f, b"r", b"q", Some(10), ValueRef::Bytes(b"old")),
            second: |wb, t, f| wb.put(t, f, b"r", b"q", Some(20), ValueRef::Bytes(b"new")),
            raise_floor: false,
            during: |wb, t, f| wb.put(t, f, b"r", b"q", Some(30), ValueRef::Bytes(b"newer")),
            cross,
        };
        for at in [Stage::Plan, Stage::Install] {
            let r = race(&case, at, FlushGcMutation::None);
            assert_untouched(&r, at, cross);
            assert_eq!(r.after, Some((30, b"newer".to_vec())));
        }
    }
}

#[test]
fn a_write_at_a_default_timestamp_does_not_void_the_purge() {
    // Inputs at default timestamps, the newest at the shard's floor: a default-timestamp
    // write lands above every input, as a single commit (above the floor) or as a share
    // the compaction's shard coordinates (its commit timestamp is above its floor).
    for cross in [Cross::No, Cross::Here] {
        let case = Case {
            seed: 319,
            first: |wb, t, f| wb.put(t, f, b"r", b"q", None, ValueRef::Bytes(b"old")),
            second: |wb, t, f| wb.put(t, f, b"r", b"q", None, ValueRef::Bytes(b"new")),
            raise_floor: false,
            during: |wb, t, f| wb.put(t, f, b"r", b"q", None, ValueRef::Bytes(b"newer")),
            cross,
        };
        for at in [Stage::Plan, Stage::Install] {
            let r = race(&case, at, FlushGcMutation::None);
            assert_untouched(&r, at, cross);
            assert_eq!(r.after.map(|(_, v)| v), Some(b"newer".to_vec()));
        }
    }
}

#[test]
fn a_backfill_newest_to_oldest_cannot_starve_a_bottommost_compaction() {
    // Each write of the backfill is below the bound the compaction last sampled (the
    // previous write, now in the memtable) and among the inputs' timestamps, and arrives
    // before the job runs: it voids every guarded attempt.
    let mut rig = Rig::open(320, FlushGcMutation::None);
    rig.flushed(|wb, t, f| wb.put(t, f, b"r", b"q", Some(1_000), ValueRef::Bytes(b"a")));
    rig.flushed(|wb, t, f| wb.put(t, f, b"r", b"q", Some(2_000), ValueRef::Bytes(b"b")));
    let voids = rig.voids();
    rig.db.hold_compactions(true);
    let mut compaction = rig.db.compact_pending(Some(rig.t)).unwrap();
    rig.run(|db| db.compaction_held());
    let mut attempts = 0;
    let mut ts = 999;
    let done = loop {
        assert!(rig.db.compaction_held(), "attempt {attempts} is held");
        attempts += 1;
        let mut wb = WriteBatch::new();
        wb.put(rig.t, rig.f, b"r", b"q", Some(ts), ValueRef::Bytes(b"x"))
            .unwrap();
        ts -= 1;
        let mut pc = rig.db.submit(wb, Some(Durability::Buffered)).unwrap();
        rig.commit(&mut pc).expect("the backfill write commits");
        // The attempt runs: voided, it plans the next (held again); unguarded, it installs.
        rig.db.release_held_compaction();
        rig.run(|db| db.compaction_held());
        if let Poll::Ready(r) = poll(&mut compaction) {
            break r;
        }
        assert!(attempts <= VOID_LIMIT, "the compaction never installed");
    };
    done.unwrap();
    assert_eq!(attempts, VOID_LIMIT + 1);
    assert_eq!(rig.voids() - voids, VOID_LIMIT);
    // The last attempt purged nothing below its bound: the version at 1000 is kept, and
    // reads are unchanged.
    rig.db.hold_compactions(false);
    assert_eq!(rig.read().map(|(ts, _)| ts), Some(2_000));
}

type Write = fn(&mut WriteBatch, TableId, FamilyId) -> pigeonhole_engine::Result<()>;

/// Whether the write is a two-shard commit (the compaction's shard then sees it as a
/// PREPARE), and which shard coordinates it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cross {
    No,
    /// The compaction's shard coordinates: its floor is raised to the commit timestamp.
    Here,
    /// The other shard coordinates, at a timestamp from its own floor.
    There,
}

impl Cross {
    const ALL: [Self; 3] = [Self::No, Self::Here, Self::There];
}

struct Case {
    seed: u64,
    /// Committed and flushed, each to an SST of its own: the compaction's inputs.
    first: Write,
    second: Write,
    /// Then a default-timestamp write to another table on the compaction's shard, which
    /// raises its floor above the inputs.
    raise_floor: bool,
    /// Committed while the compaction runs.
    during: Write,
    cross: Cross,
}

/// The race at both stages: the write arrives before the compaction's job runs (it voids
/// the purge and commits at once) or while the compaction's commit installs (it waits).
/// Reads hold either way, and fail without the guard.
fn check(case: &Case) {
    let cross = case.cross;
    for at in [Stage::Plan, Stage::Install] {
        let r = race(case, at, FlushGcMutation::None);
        assert!(r.keeps_reads(), "{at:?} {cross:?}: {r:?}");
        assert_eq!(
            r.committed_before_install,
            at == Stage::Plan,
            "{at:?} {cross:?}: {r:?}"
        );
        assert_eq!(r.voided, at == Stage::Plan, "{at:?} {cross:?}: {r:?}");
        let r = race(case, at, FlushGcMutation::IgnoreVoids);
        assert!(
            !r.keeps_reads(),
            "{at:?} {cross:?}: the mutation must change a read: {r:?}"
        );
    }
}

/// The write neither voided the compaction nor waited for its install.
fn assert_untouched(r: &Outcome, at: Stage, cross: Cross) {
    assert!(
        r.committed_before_install,
        "{at:?} {cross:?}: waited: {r:?}"
    );
    assert!(!r.voided, "{at:?} {cross:?}: voided the purge: {r:?}");
    assert_eq!(r.after, r.before, "{at:?} {cross:?}");
}

/// Where the compaction is when the write arrives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    /// Started (its plan sampled `min_ts_above`), its job not yet run.
    Plan,
    /// Its commit installed and is held before the shard learns of it.
    Install,
}

type Read = Option<(u64, Vec<u8>)>;

#[derive(Debug)]
struct Outcome {
    initial: Read,
    before: Read,
    after: Read,
    committed_before_install: bool,
    /// A write voided the compaction, and it compacted again.
    voided: bool,
}

impl Outcome {
    /// No read changes without a write: what a reader saw after the write stays until
    /// another write changes it. (If the write waited for the install, no reader saw it
    /// before.)
    fn keeps_reads(&self) -> bool {
        if self.committed_before_install {
            self.after == self.before
        } else {
            self.before == self.initial
        }
    }
}

/// The case's inputs, then a full (bottommost) compaction run to `at`, with `during`
/// committed meanwhile. Returns the reads before the compaction, right after the write, and
/// once both are done.
fn race(case: &Case, at: Stage, mutation: FlushGcMutation) -> Outcome {
    let mut rig = Rig::open(case.seed, mutation);
    rig.flushed(case.first);
    rig.flushed(case.second);
    if case.raise_floor {
        let mut wb = WriteBatch::new();
        wb.put(rig.u, rig.uf, b"r", b"q", None, ValueRef::Bytes(b"u"))
            .unwrap();
        let mut pc = rig.db.submit(wb, Some(Durability::Sync)).unwrap();
        rig.commit(&mut pc).expect("the write commits");
        // Flushed, so the compaction starts at once (its table only: `u` stays as it is).
        let mut flush = rig.db.flush_pending().unwrap();
        rig.settle(&mut flush).unwrap();
    }
    let initial = rig.read();

    // The full compaction is bottommost and samples min_ts_above = u64::MAX (nothing above
    // it), so it purges below any timestamp.
    let voids = rig.voids();
    rig.db.park_manifest_commits(true);
    let mut compaction = rig.db.compact_pending(Some(rig.t)).unwrap();
    match at {
        Stage::Plan => {
            rig.poke();
            assert!(!rig.db.manifest_commit_parked());
        }
        Stage::Install => {
            rig.run(|db| db.manifest_commit_parked());
            assert!(
                rig.db.manifest_commit_parked(),
                "the compaction reached its install"
            );
        }
    }

    let mut pc = rig.submit(case.during, case.cross);
    if at == Stage::Plan {
        // Admitted (a share prepared) before the compaction's job runs a slice.
        rig.poke();
    }
    let committed_before_install = rig.commit(&mut pc).is_some();
    let before = rig.read();

    rig.db.park_manifest_commits(false);
    // A compaction voided by the write may report it; either way it is over.
    let _ = rig.settle(&mut compaction);
    if !committed_before_install {
        rig.commit(&mut pc)
            .expect("the write commits after the install");
    }
    rig.run(|_| false);
    let after = rig.read();
    Outcome {
        initial,
        before,
        after,
        committed_before_install,
        voided: rig.voids() > voids,
    }
}

fn poll<F: Future + Unpin>(f: &mut F) -> Poll<F::Output> {
    Pin::new(f).poll(&mut Context::from_waker(Waker::noop()))
}

/// Two shards: tables `t` (family `f`, max_versions 1) and `u` on one, table `o` on the
/// other.
struct Rig {
    db: Arc<Engine>,
    shards: Vec<EngineShard>,
    t: TableId,
    f: FamilyId,
    o: TableId,
    of: FamilyId,
    u: TableId,
    uf: FamilyId,
}

impl Rig {
    fn open(seed: u64, mutation: FlushGcMutation) -> Self {
        let vfs = SimVfs::new(seed);
        let mut o = common::options(Arc::clone(&vfs), 2, 4 << 20);
        o.pin_threads = false;
        o.tablet_changes = false;
        o.compaction_threads = 0;
        // Only the explicit full compaction.
        o.compaction.l0_trigger = u32::MAX;
        o.compaction.level_base_bytes = u64::MAX;
        let (db, shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
        db.mutate_flush_gc(mutation);
        let fam = FamilyOptions::default().max_versions(1);
        let t = db.create_table("t", &[("f".into(), fam)]).unwrap();
        let o = db
            .create_table("o", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        let u = db
            .create_table("u", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        Self {
            db,
            shards,
            t: t.id,
            f: t.families[0].id,
            o: o.id,
            of: o.families[0].id,
            u: u.id,
            uf: u.families[0].id,
        }
    }

    /// Runs the shards until `until` holds or they go idle (two passes in a row with
    /// nothing to do: a message one shard sends another in a pass may not count as work).
    fn run(&mut self, until: impl Fn(&Engine) -> bool) {
        let mut idle = 0;
        for _ in 0..100_000 {
            let mut more = false;
            for s in &mut self.shards {
                more |= s.run_once(u64::MAX);
            }
            idle = if more { 0 } else { idle + 1 };
            if until(&self.db) || idle == 2 {
                return;
            }
        }
        panic!("the shards never went idle");
    }

    /// Passes with a deadline already passed: the shards handle their messages (requests,
    /// commits, a share's PREPARE) but run no task slice.
    fn poke(&mut self) {
        for _ in 0..4 {
            for s in &mut self.shards {
                s.run_once(0);
            }
        }
    }

    /// The commit's result, or `None` if it is still pending once the shards are idle (it
    /// waits on something the test holds).
    fn commit(&mut self, pc: &mut PendingCommit) -> Option<CommitInfo> {
        if let Poll::Ready(r) = poll_commit(pc) {
            return Some(r.unwrap());
        }
        self.run(|_| false);
        match poll_commit(pc) {
            Poll::Ready(r) => Some(r.unwrap()),
            Poll::Pending => None,
        }
    }

    fn settle<F: Future + Unpin>(&mut self, f: &mut F) -> F::Output {
        loop {
            if let Poll::Ready(r) = poll(f) {
                return r;
            }
            self.run(|_| false);
        }
    }

    /// Submits `w` (with a put into the other shard's table, first or last, if `cross`).
    fn submit(&self, w: Write, cross: Cross) -> PendingCommit {
        let other = |wb: &mut WriteBatch| {
            wb.put(self.o, self.of, b"r", b"q", None, ValueRef::Bytes(b"o"))
                .unwrap();
        };
        let mut wb = WriteBatch::new();
        // The shard of the batch's first row coordinates.
        if cross == Cross::There {
            other(&mut wb);
        }
        w(&mut wb, self.t, self.f).unwrap();
        if cross == Cross::Here {
            other(&mut wb);
        }
        self.db.submit(wb, Some(Durability::Sync)).unwrap()
    }

    /// Commits `w` and flushes it to an SST of its own.
    fn flushed(&mut self, w: Write) {
        let mut pc = self.submit(w, Cross::No);
        self.commit(&mut pc).expect("the write commits");
        let mut flush = self.db.flush_pending().unwrap();
        self.settle(&mut flush).unwrap();
    }

    fn read(&self) -> Read {
        let snap = self.db.snapshot().unwrap();
        self.db
            .get(&snap, self.t, self.f, b"r", b"q")
            .unwrap()
            .map(|c| (c.timestamp(), value_bytes(c.value())))
    }

    /// Voided compactions on every shard.
    fn voids(&self) -> u64 {
        (0..self.shards.len())
            .map(|s| self.db.compaction_purge_voids(s))
            .sum()
    }
}

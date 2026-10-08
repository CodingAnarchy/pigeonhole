//! Each family compacts by its own `CompactionStyle` (issue #31): a tiered family's L0
//! merges go to the last level as whole sorted runs while a leveled one fills L1, under one
//! application-owned shard over `SimVfs`.

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::manifest::CompactionStyle;
use pigeonhole_format::{Durability, TableId};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";

struct Rig {
    db: Arc<Engine>,
    shard: EngineShard,
}

impl Rig {
    fn open(o: EngineOptions) -> Self {
        let (db, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
        let mut shard = shards.remove(0);
        while shard.run_once(u64::MAX) {}
        Self { db, shard }
    }

    /// Runs the shard until `f` resolves.
    fn wait<F: Future + Unpin>(&mut self, mut f: F) -> F::Output {
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..100_000 {
            if let Poll::Ready(r) = Pin::new(&mut f).poll(&mut cx) {
                return r;
            }
            self.shard.run_once(u64::MAX);
        }
        panic!("the shard never resolved the request");
    }

    /// Runs the shard until it has no work left (background compactions included).
    fn idle(&mut self) {
        while self.shard.run_once(u64::MAX) {}
    }

    fn put(&mut self, t: &TableInfo, row: u32, value: &[u8]) {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            t.families[0].id,
            format!("row{row:04}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(value),
        )
        .unwrap();
        let pending = self.db.submit(wb, Some(Durability::Buffered)).unwrap();
        self.wait(pending).unwrap();
    }

    fn get(&self, t: &TableInfo, row: u32) -> Option<Vec<u8>> {
        let snap = self.db.snapshot().unwrap();
        self.db
            .get(
                &snap,
                t.id,
                t.families[0].id,
                format!("row{row:04}").as_bytes(),
                b"q",
            )
            .unwrap()
            .map(|c| common::value_bytes(c.value()))
    }

    /// The levels holding `t`'s SSTs, with how many each holds.
    fn levels(&self, t: TableId) -> Vec<(u8, usize)> {
        let mut levels: Vec<(u8, usize)> = Vec::new();
        let mut ssts = self.db.sst_levels();
        ssts.sort_unstable();
        for (_, level, _) in ssts.into_iter().filter(|s| s.0 == t) {
            match levels.last_mut() {
                Some((l, n)) if *l == level => *n += 1,
                _ => levels.push((level, 1)),
            }
        }
        levels
    }
}

#[test]
fn families_compact_by_their_own_style() {
    let vfs = SimVfs::new(31);
    let mut o = common::options(vfs, 1, 16 << 20);
    o.tablet_changes = false;
    o.compaction.l0_trigger = 2;
    o.compaction.max_levels = 4;
    o.compaction.level_base_bytes = u64::MAX;
    let mut rig = Rig::open(o);
    let table = |rig: &Rig, name: &str, compaction| {
        let family = FamilyOptions {
            compaction,
            ..FamilyOptions::default()
        };
        rig.db.create_table(name, &[("f".into(), family)]).unwrap()
    };
    let leveled = table(&rig, "leveled", CompactionStyle::Leveled);
    let tiered = table(&rig, "tiered", CompactionStyle::Tiered);
    for round in 0..6u32 {
        let value = round.to_le_bytes();
        for row in round * 5..round * 5 + 20 {
            rig.put(&leveled, row, &value);
            rig.put(&tiered, row, &value);
        }
        let flush = rig.db.flush_pending().unwrap();
        rig.wait(flush).unwrap();
        rig.idle();
        // Leveled merges L0 into L1 (whose target is never reached); tiered merges L0 into
        // whole runs, starting at the last level.
        let l = rig.levels(leveled.id);
        let t = rig.levels(tiered.id);
        assert!(
            l.iter().all(|&(level, _)| level <= 1),
            "round {round}: {l:?}"
        );
        assert!(
            t.iter().all(|&(level, n)| level != 0 || n < 2),
            "round {round}: {t:?}"
        );
        if round > 0 {
            assert!(
                t.iter().any(|&(level, _)| level == 3),
                "round {round}: {t:?}"
            );
        }
    }
    for row in 0..45 {
        let newest = (0..6u32)
            .rev()
            .find(|r| (r * 5..r * 5 + 20).contains(&row))
            .map(|r| r.to_le_bytes().to_vec());
        assert_eq!(rig.get(&leveled, row), newest, "row {row}");
        assert_eq!(rig.get(&tiered, row), newest, "row {row}");
    }
}

/// Review of #227: a FIFO-by-time family still compacts (leveled, until its own picker
/// lands in #32), so its L0 never grows without bound.
#[test]
fn fifo_by_time_families_still_compact() {
    let vfs = SimVfs::new(32);
    let mut o = common::options(vfs, 1, 16 << 20);
    o.tablet_changes = false;
    o.compaction.l0_trigger = 2;
    let mut rig = Rig::open(o);
    let family = FamilyOptions {
        compaction: CompactionStyle::FifoByTime,
        ..FamilyOptions::default()
    };
    let t = rig
        .db
        .create_table("fifo", &[("f".into(), family)])
        .unwrap();
    for round in 0..6u32 {
        for row in 0..20 {
            rig.put(&t, row, &round.to_le_bytes());
        }
        let flush = rig.db.flush_pending().unwrap();
        rig.wait(flush).unwrap();
        rig.idle();
        let l0 = rig
            .levels(t.id)
            .iter()
            .find(|l| l.0 == 0)
            .map_or(0, |l| l.1);
        assert!(l0 < 2, "round {round}: {:?}", rig.levels(t.id));
    }
    assert_eq!(rig.get(&t, 7), Some(5u32.to_le_bytes().to_vec()));
}

/// Review of #227: the write stall follows L0 depth only (D119). Tiered space
/// amplification over its cap makes a compaction due but never paces writers, even while
/// that compaction cannot commit.
#[test]
fn tiered_space_amp_does_not_stall_writers() {
    let vfs = SimVfs::new(33);
    let mut o = common::options(vfs, 1, 16 << 20);
    o.tablet_changes = false;
    o.compaction.l0_trigger = 64;
    o.compaction.tiered_max_space_amp_percent = 10;
    let mut rig = Rig::open(o);
    let family = FamilyOptions {
        compaction: CompactionStyle::Tiered,
        ..FamilyOptions::default()
    };
    let t = rig
        .db
        .create_table("tiered", &[("f".into(), family)])
        .unwrap();
    // Two equal flushes: space amplification 100%, far over 10%, with L0 at 2 of 64.
    for round in 0..2u32 {
        for row in round * 20..round * 20 + 20 {
            rig.put(&t, row, &[7; 512]);
        }
        if round == 1 {
            // The compaction this flush makes due runs but cannot publish.
            rig.db.park_manifest_commits(true);
        }
        let flush = rig.db.flush_pending().unwrap();
        rig.wait(flush).unwrap();
    }
    rig.idle();
    for row in 100..400 {
        rig.put(&t, row, &[8; 64]);
    }
    assert_eq!(rig.db.metrics().stalls.0, 0, "{:?}", rig.db.metrics());
    rig.db.park_manifest_commits(false);
    rig.idle();
    let m = rig.db.metrics();
    assert!(m.compactions > 0, "{m:?}");
    assert_eq!(m.stalls.0, 0, "{m:?}");
    assert!(
        rig.levels(t.id).iter().all(|l| l.0 != 0),
        "{:?}",
        rig.levels(t.id)
    );
}

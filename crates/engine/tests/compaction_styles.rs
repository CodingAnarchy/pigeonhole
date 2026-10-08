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

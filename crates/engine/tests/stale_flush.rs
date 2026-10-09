//! The experimental stale trigger (`EngineOptions::memtable_stale_share`, #287): a memtable
//! that is mostly other versions of columns it holds flushes before it reaches
//! `memtable_freeze_bytes`; one of distinct columns does not; and with the share at 0 (the
//! default) nothing changes.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{Engine, EngineOptions, EngineShard, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

/// One application-owned shard driven by the test.
struct Rig {
    db: Arc<Engine>,
    shard: EngineShard,
}

impl Rig {
    fn open(seed: u64, stale_share: f64) -> Self {
        Self::open_with(seed, stale_share, 0)
    }

    fn open_with(seed: u64, stale_share: f64, stale_count: u64) -> Self {
        let vfs = SimVfs::new(seed);
        let mut o = EngineOptions::new(vfs);
        o.create_if_missing = true;
        o.shards = 1;
        o.memtable_budget = 16 << 20;
        // Size alone freezes at 4 MiB; the stale trigger from 1 MiB.
        o.memtable_freeze_bytes = 4 << 20;
        o.memtable_stale_share = stale_share;
        o.memtable_stale_count = stale_count;
        o.memtable_stale_min_bytes = 1 << 20;
        o.compaction.l0_trigger = u32::MAX;
        o.compaction.level_base_bytes = u64::MAX;
        let (db, mut shards) =
            Engine::open_application_owned(Path::new("/db/stale.phdb"), o).unwrap();
        let mut rig = Self {
            db,
            shard: shards.remove(0),
        };
        while rig.shard.run_once(u64::MAX) {}
        rig
    }

    fn wait<F: Future + Unpin>(&mut self, mut f: F) -> F::Output {
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(r) = Pin::new(&mut f).poll(&mut cx) {
                return r;
            }
            self.shard.run_once(u64::MAX);
        }
    }

    /// Writes about 2 MiB (below the size threshold) as 1 KiB cells of `columns` columns,
    /// then lets the shard finish, and returns the flushes it ran.
    fn write(&mut self, columns: u32) -> u64 {
        self.write_family(columns, 1)
    }

    /// As `write`, in a family keeping `max_versions` (0 keeps every version).
    fn write_family(&mut self, columns: u32, max_versions: u32) -> u64 {
        let mut options = FamilyOptions::default();
        options.max_versions = max_versions;
        let t = self.db.create_table("t", &[("f".into(), options)]).unwrap();
        let f = t.families[0].id;
        for i in 0..2_000u32 {
            let mut wb = WriteBatch::new();
            let q = format!("q{:05}", i % columns);
            wb.put(
                t.id,
                f,
                b"hot",
                q.as_bytes(),
                None,
                ValueRef::Bytes(&[7u8; 1000]),
            )
            .unwrap();
            let p = self.db.submit(wb, Some(Durability::Buffered)).unwrap();
            self.wait(p).unwrap();
        }
        while self.shard.run_once(u64::MAX) {}
        self.db.metrics().flushes
    }

    fn close(mut self) {
        self.db.close().unwrap();
        loop {
            self.shard.run_once(u64::MAX);
            if let Some(r) = self.shard.closed() {
                r.unwrap();
                return;
            }
        }
    }
}

#[test]
fn a_memtable_of_overwritten_versions_flushes_early() {
    let mut rig = Rig::open(1, 0.5);
    // 2,000 writes to 10 columns: nearly all are other versions of a column.
    let flushes = rig.write(10);
    assert!(
        flushes >= 1,
        "the stale trigger did not flush ({flushes} flushes)"
    );
    rig.close();
}

#[test]
fn distinct_columns_do_not_trigger_it() {
    let mut rig = Rig::open(2, 0.5);
    let flushes = rig.write(u32::MAX);
    assert_eq!(flushes, 0, "a memtable of distinct columns flushed early");
    rig.close();
}

#[test]
fn off_by_default() {
    // (Sweeps may turn the trigger on for every test through the environment.)
    if std::env::var_os("PIGEONHOLE_TEST_STALE_SHARE").is_none() {
        assert_eq!(EngineOptions::new(SimVfs::new(3)).memtable_stale_share, 0.0);
    }
    let mut rig = Rig::open(3, 0.0);
    let flushes = rig.write(10);
    assert_eq!(flushes, 0, "with the trigger off, a 2 MiB memtable flushed");
    rig.close();
}

#[test]
fn the_test_environment_knob_turns_it_on() {
    // What sweeps use: `PIGEONHOLE_TEST_STALE_SHARE` reaches the default options of every
    // engine test (they build with `test-hooks`).
    if let Ok(v) = std::env::var("PIGEONHOLE_TEST_STALE_SHARE") {
        let share: f64 = v.parse().unwrap();
        assert_eq!(
            EngineOptions::new(SimVfs::new(4)).memtable_stale_share,
            share
        );
    }
}

#[test]
fn a_family_keeping_every_version_never_triggers_it() {
    // Every version of such a family is live: a flush drops none of them, so flushing early
    // would only cost writes.
    let mut rig = Rig::open(5, 0.5);
    let flushes = rig.write_family(10, 0);
    assert_eq!(
        flushes, 0,
        "the trigger fired for a family that keeps every version"
    );
    rig.close();
}

#[test]
fn the_count_trigger_fires_on_overwrites_alone() {
    // 2,000 writes to 1,000 columns: half are other versions, a share of 0.5 at most, but a
    // count of 500 is reached.
    let mut rig = Rig::open_with(6, 0.0, 500);
    let flushes = rig.write(1_000);
    assert!(
        flushes >= 1,
        "the count trigger did not flush ({flushes} flushes)"
    );
    rig.close();
    let mut rig = Rig::open_with(7, 0.0, 500);
    let flushes = rig.write(u32::MAX);
    assert_eq!(flushes, 0, "distinct columns reached the count trigger");
    rig.close();
}

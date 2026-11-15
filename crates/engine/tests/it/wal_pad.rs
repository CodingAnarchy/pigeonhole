//! D327 (#1708, ICR 0137): an open with `wal.pad_unit` set flags the file before any WAL
//! write, in both superblock pages and for good; a bad unit is refused before the file is
//! touched; and padded commits, with syncs left in flight, survive a power loss.

use crate::common;

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    CommitInfo, Engine, EngineOptions, EngineShard, Error, FamilyOptions, PendingCommit, TableInfo,
    ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_format::superblock::{SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock};
use pigeonhole_format::wal::INCOMPAT_WAL_PAD;
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};
use pigeonhole_io::{OpenOptions, Vfs, VfsRef};

const DB: &str = "/db/wal_pad.phdb";
const PAGE: u64 = 4096;

fn options(vfs: &Arc<SimVfs>, pad: u32) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), 2, 16 << 20);
    o.wal.pad_unit = pad;
    o
}

fn incompat(vfs: &SimVfs) -> [u64; 2] {
    let f = vfs.open(Path::new(DB), OpenOptions::read()).unwrap();
    [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B].map(|p| {
        let mut buf = [0u8; PAGE as usize];
        f.read_at(&mut buf, p * PAGE).unwrap();
        Superblock::decode(&buf).unwrap().features.incompat
    })
}

#[test]
fn an_open_that_pads_flags_the_file_in_both_pages_for_good() {
    let vfs = SimVfs::new(327);
    let db = Engine::open(Path::new(DB), options(&vfs, 0)).unwrap();
    db.close().unwrap();
    assert_eq!(incompat(&vfs), [0; 2], "off: not flagged");
    let db = Engine::open(Path::new(DB), options(&vfs, 4096)).unwrap();
    assert_eq!(incompat(&vfs), [INCOMPAT_WAL_PAD; 2], "flagged at the open");
    db.close().unwrap();
    // Sticky: an open without the option keeps it (retained feed segments keep pads).
    let db = Engine::open(Path::new(DB), options(&vfs, 0)).unwrap();
    db.close().unwrap();
    assert_eq!(incompat(&vfs), [INCOMPAT_WAL_PAD; 2]);
}

#[test]
fn a_bad_pad_unit_is_refused_before_the_file_is_flagged() {
    let vfs = SimVfs::new(328);
    let db = Engine::open(Path::new(DB), options(&vfs, 0)).unwrap();
    db.close().unwrap();
    for unit in [16, 100, 65_536] {
        match Engine::open(Path::new(DB), options(&vfs, unit)) {
            Err(Error::InvalidArgument(what)) => assert!(what.contains("pad unit"), "{what}"),
            Ok(_) => panic!("{unit}: opened"),
            Err(e) => panic!("{unit}: {e:?}"),
        }
        assert_eq!(incompat(&vfs), [0; 2], "{unit}: the file isn't flagged");
    }
}

/// GroupSync commits on one hand-driven shard with deferred I/O: a group's sync is still in
/// flight when the next group appends, so groups pad; every commit acknowledged before a
/// power loss with torn, reordered writes is there after the reopen.
#[test]
fn padded_commits_acknowledged_before_a_power_loss_survive_it() {
    for seed in 0..4u64 {
        let mut plan = FaultPlan::none();
        plan.torn_writes = true;
        plan.reorder_unsynced = true;
        let vfs = SimVfs::with_faults(330 + seed, plan);
        let (engine, mut shards) = open(&vfs);
        let shard = &mut shards[0];
        let t = engine
            .create_table("t", &[("f".to_owned(), FamilyOptions::default())])
            .unwrap();
        settle(&vfs, shard);
        vfs.set_deferred_io(true);
        let mut pending = Vec::new();
        let mut acked = Vec::new();
        for i in 0..300usize {
            pending.push((
                i,
                engine
                    .submit(put(&t, i), Some(Durability::GroupSync))
                    .unwrap(),
            ));
            while shard.run_once(u64::MAX) {}
            // Complete some I/O now and then, leaving a sync in flight in between.
            if i % (2 + (seed as usize % 3)) == 0 {
                vfs.complete_io();
                while shard.run_once(u64::MAX) {}
            }
            pending.retain_mut(|(i, pc)| match poll(pc) {
                Poll::Ready(r) => {
                    r.unwrap();
                    acked.push(*i);
                    false
                }
                Poll::Pending => true,
            });
        }
        let pads = engine.metrics().wal_pads;
        assert!(pads.0 > 0, "seed {seed}: no group padded: {pads:?}");
        assert!(!acked.is_empty(), "seed {seed}");
        vfs.crash(CrashKind::Power);
        drop(pending);
        drop(shards);
        drop(engine);
        vfs.set_deferred_io(false);
        let (engine, mut shards) = open(&vfs);
        settle(&vfs, &mut shards[0]);
        let t = engine.table("t").expect("the table survived");
        for i in &acked {
            let got = engine
                .get_latest(
                    t.id,
                    t.families[0].id,
                    format!("row{i:04}").as_bytes(),
                    b"q",
                )
                .unwrap();
            assert!(got.is_some(), "seed {seed}: acknowledged row{i:04} lost");
        }
        drop(shards);
        drop(engine);
    }
}

fn open(vfs: &Arc<SimVfs>) -> (Arc<Engine>, Vec<EngineShard>) {
    let mut o = EngineOptions::new(Arc::clone(vfs) as VfsRef);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.compaction_threads = 0;
    o.memtable_budget = 4 << 20;
    o.wal.segment_size = 256 << 10;
    o.wal.spare_segments = 1;
    o.wal.pad_unit = 4096;
    Engine::open_application_owned(Path::new(DB), o).unwrap()
}

fn put(t: &TableInfo, i: usize) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        format!("row{i:04}").as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(&[7u8; 300]),
    )
    .unwrap();
    wb
}

fn poll(pc: &mut PendingCommit) -> Poll<Result<CommitInfo, Error>> {
    Pin::new(pc).poll(&mut Context::from_waker(Waker::noop()))
}

/// Runs the shard and completes I/O until neither has anything left.
fn settle(vfs: &SimVfs, shard: &mut EngineShard) {
    loop {
        while shard.run_once(u64::MAX) {}
        if !vfs.complete_io() && !shard.run_once(u64::MAX) {
            return;
        }
    }
}

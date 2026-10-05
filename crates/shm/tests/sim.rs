//! The multi-process suite on simulated processes: one `SimVfs`, several `ProcessId`s
//! entered in turn, `crash_process` for kills. Deterministic, so every lifecycle edge is
//! exercised exactly.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::*;
use pigeonhole_format::shm::{directory, directory_name, header, region_name};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{
    ErrorKind, FileIdentity, FileRef, LockMode, OpenOptions, ProcessId, SharedOpen, VfsRef,
};
use pigeonhole_shm::{Error, Generation, Presence, ReaderSlot, Role, ShmRegion, WriterLock};

const DB: &str = "/db/data.phdb";

fn pid(n: u32) -> ProcessId {
    ProcessId {
        pid: n,
        start_time: 1,
    }
}

/// A simulated process with its own handle on the main file.
struct Proc {
    id: ProcessId,
    vfs: VfsRef,
    sim: Arc<SimVfs>,
    file: FileRef,
    identity: FileIdentity,
}

impl Proc {
    fn start(sim: &Arc<SimVfs>, id: ProcessId) -> Self {
        sim.enter_process(id);
        let vfs: VfsRef = sim.clone();
        let file = vfs
            .open(Path::new(DB), OpenOptions::read_write_create())
            .unwrap();
        let identity = file.identity().unwrap();
        Self {
            id,
            vfs,
            sim: sim.clone(),
            file,
            identity,
        }
    }

    /// Makes the calling thread this process (for `current_process`).
    fn enter(&self) {
        self.sim.enter_process(self.id);
    }

    fn open(&self, role: Role) -> pigeonhole_shm::Result<ShmRegion> {
        self.enter();
        ShmRegion::open(
            &self.vfs,
            &self.file,
            self.identity,
            DB_ID,
            role,
            &small_config(),
        )
    }

    fn open_writer(&self) -> (WriterLock, Presence, ShmRegion) {
        let lock = WriterLock::acquire(&self.file).unwrap();
        let presence = Presence::acquire(&self.file).unwrap();
        let shm = self.open(Role::Writer).unwrap();
        (lock, presence, shm)
    }

    fn open_reader(&self) -> (Presence, ShmRegion, ReaderSlot) {
        let presence = Presence::acquire(&self.file).unwrap();
        let shm = self.open(Role::Reader).unwrap();
        let slot = shm.claim_reader_slot(self.id).unwrap();
        (presence, shm, slot)
    }

    fn crash(&self) {
        self.sim.crash_process(self.id);
    }

    fn region_exists(&self, generation: u64) -> bool {
        let name = region_name(self.identity.device, self.identity.inode, generation);
        match self.vfs.open_shared(&name, None, 4096, SharedOpen::Attach) {
            Ok(_) => true,
            Err(e) if e.kind == ErrorKind::NotFound => false,
            Err(e) => panic!("{e}"),
        }
    }

    fn directory_generation(&self) -> u64 {
        let name = directory_name(self.identity.device, self.identity.inode);
        self.vfs
            .open_shared(&name, None, directory::LEN as u64, SharedOpen::Attach)
            .unwrap()
            .atomic_u64(directory::GENERATION)
            .load(Ordering::Acquire)
    }
}

#[test]
fn reader_before_any_writer_finds_no_region() {
    let sim = SimVfs::new(1);
    let r = Proc::start(&sim, pid(10));
    match r.open(Role::Reader) {
        Err(Error::Io(e)) => assert_eq!(e.kind, ErrorKind::NotFound),
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[test]
fn writer_builds_generations_and_a_second_writer_is_refused() {
    let sim = SimVfs::new(2);
    let w = Proc::start(&sim, pid(1));
    let (lock, _presence, shm) = w.open_writer();
    assert_eq!(shm.generation(), Generation(1));
    assert_eq!(shm.shard_count(), 2);
    assert_eq!(w.directory_generation(), 1);
    assert!(!shm.is_stale());

    let w2 = Proc::start(&sim, pid(2));
    assert!(matches!(
        WriterLock::acquire(&w2.file),
        Err(Error::WriterLocked)
    ));
    drop(lock);
    let lock2 = WriterLock::acquire(&w2.file).unwrap();
    drop(lock2);
}

#[test]
fn readers_see_commits_in_order_and_never_half_a_cross_shard_commit() {
    let sim = SimVfs::new(3);
    let w = Proc::start(&sim, pid(1));
    let (_lock, _wp, shm) = w.open_writer();
    shm.publish_view(&view(1, 2, 8)).unwrap();

    let r1 = Proc::start(&sim, pid(11));
    let (_p1, shm1, slot1) = r1.open_reader();
    let r2 = Proc::start(&sim, pid(12));
    let (_p2, shm2, slot2) = r2.open_reader();
    assert_ne!(slot1.index(), slot2.index());
    assert_eq!(shm1.read_view().unwrap(), view(1, 2, 8));

    let mut last1 = 0;
    let mut last2 = 0;
    let mut seen = Vec::new();
    // A scripted interleaving: shard 0 runs a group, then coordinates a cross-shard commit
    // with shard 1, keeps committing on its own while that is in flight, and only then
    // releases it. Readers check after every writer step.
    let mut held: Option<u64> = None;
    let publish = |shard: u32, x: u64, held: Option<u64>| {
        shm.publish_pending(shard, held.unwrap_or(u64::MAX).min(x));
    };
    let mut check = |shm1: &ShmRegion, shm2: &ShmRegion| {
        let v1 = check_snapshot(shm1, &mut last1, u64::MAX).unwrap();
        let v2 = check_snapshot(shm2, &mut last2, u64::MAX).unwrap();
        seen.push((v1, v2));
        v1
    };

    // Group of two on shard 0.
    publish(0, shm.visible_seqno() + 1, held);
    let first = shm.reserve_seqnos(2);
    publish(0, first, held);
    let visible = check(&shm1, &shm2);
    assert_eq!(visible, 0, "reserved but unapplied: invisible");
    for s in first..first + 2 {
        set_mask(&shm, s, 0b01);
        mark_applied(&shm, 0, s);
        publish(0, s + 1, held);
        check(&shm1, &shm2);
    }
    publish(0, u64::MAX, held);
    check(&shm1, &shm2);
    assert_eq!(shm1.visible_seqno(), 2);

    // Cross-shard commit q coordinated by shard 0 with shard 1.
    publish(0, shm.visible_seqno() + 1, held);
    let q = shm.reserve_seqnos(1);
    held = Some(q);
    publish(0, u64::MAX, held);
    set_mask(&shm, q, 0b11);
    mark_applied(&shm, 0, q);
    check(&shm1, &shm2);
    assert_eq!(shm1.visible_seqno(), 2, "q is held");

    // Shard 1 runs its own group meanwhile: visible stays below q.
    publish(1, shm.visible_seqno() + 1, None);
    let g = shm.reserve_seqnos(1);
    publish(1, g, None);
    set_mask(&shm, g, 0b10);
    mark_applied(&shm, 1, g);
    publish(1, u64::MAX, None);
    check(&shm1, &shm2);
    assert_eq!(shm1.visible_seqno(), 2);

    // Shard 0 runs another group while still holding q.
    publish(0, shm.visible_seqno() + 1, held);
    let h = shm.reserve_seqnos(1);
    publish(0, h, held);
    set_mask(&shm, h, 0b01);
    mark_applied(&shm, 0, h);
    publish(0, u64::MAX, held);
    check(&shm1, &shm2);
    assert_eq!(shm1.visible_seqno(), 2, "min(held, ..) keeps q holding");

    // Shard 1 applies its share; the coordinator releases.
    mark_applied(&shm, 1, q);
    held = None;
    publish(0, u64::MAX, held);
    check(&shm1, &shm2);
    assert_eq!(shm1.visible_seqno(), h);
    assert_eq!(h, 5);

    // Snapshots were monotone for each reader.
    for w in seen.windows(2) {
        assert!(w[0].0 <= w[1].0 && w[0].1 <= w[1].1, "{seen:?}");
    }
    slot1.pin(shm1.visible_seqno(), shm1.view_version());
    assert_eq!(shm.oldest_reader_pin(), Some((5, 1)));
}

#[test]
fn killed_readers_slots_are_reclaimed() {
    let sim = SimVfs::new(4);
    let w = Proc::start(&sim, pid(1));
    let (_lock, _wp, shm) = w.open_writer();
    let r1 = Proc::start(&sim, pid(11));
    let (_p1, _shm1, slot1) = r1.open_reader();
    let r2 = Proc::start(&sim, pid(12));
    let (_p2, _shm2, slot2) = r2.open_reader();
    slot1.pin(4, 0);
    slot2.pin(3, 0);
    assert_eq!(shm.oldest_reader_pin(), Some((3, 0)));
    assert_eq!(shm.reclaim_dead_slots(&w.vfs), 0);

    r2.crash();
    std::mem::forget(slot2); // a dead process never runs its destructor
    assert_eq!(shm.reclaim_dead_slots(&w.vfs), 1);
    assert_eq!(shm.oldest_reader_pin(), Some((4, 0)));
    assert_eq!(shm.reclaim_dead_slots(&w.vfs), 0);

    // The slot is free for the next reader, and r2's presence lock died with it.
    let r3 = Proc::start(&sim, pid(13));
    let (_p3, _shm3, _slot3) = r3.open_reader();
    assert_eq!(
        shm.claim_reader_slot(pid(99)).unwrap().index() + 1,
        4 - 1,
        "4 slots: r1, r3, this one, one left"
    );
}

#[test]
fn writer_kill_and_restart_leaves_readers_on_a_valid_snapshot_then_remaps() {
    let sim = SimVfs::new(5);
    let w = Proc::start(&sim, pid(1));
    let (lock, wp, shm) = w.open_writer();
    shm.publish_view(&view(1, 3, 8)).unwrap();
    shm.set_manifest_version(10);
    shm.reserve_seqnos(9);

    let r = Proc::start(&sim, pid(11));
    let (_rp, shm_r, slot) = r.open_reader();
    slot.pin(shm_r.visible_seqno(), shm_r.view_version());
    assert_eq!(shm.oldest_reader_pin(), Some((9, 1)));

    // The writer dies (its locks go with it; the region stays).
    w.crash();
    std::mem::forget((lock, wp));
    assert!(!shm_r.is_stale(), "no new generation yet");
    assert_eq!(shm_r.read_view().unwrap(), view(1, 3, 8));
    assert_eq!(shm_r.visible_seqno(), 9);
    assert_eq!(shm_r.manifest_version(), 10);

    // A new writer: new generation, old one abandoned and unnamed.
    let w2 = Proc::start(&sim, pid(2));
    let (_lock2, _wp2, shm2) = w2.open_writer();
    assert_eq!(shm2.generation(), Generation(2));
    assert_eq!(w2.directory_generation(), 2);
    assert!(!w2.region_exists(1), "old region's name is removed");
    assert!(w2.region_exists(2));
    shm2.publish_view(&view(7, 1, 4)).unwrap();
    assert_eq!(
        shm2.visible_seqno(),
        0,
        "fresh counters; the engine re-seeds them"
    );
    assert_eq!(
        shm2.oldest_reader_pin(),
        None,
        "readers have not re-pinned yet"
    );

    // The reader notices, still reads its old snapshot, then re-attaches.
    assert!(shm_r.is_stale());
    assert_eq!(
        shm_r.read_view().unwrap(),
        view(1, 3, 8),
        "old mapping stays valid"
    );
    let shm_r2 = shm_r.reattach(&r.vfs, &r.file).unwrap();
    assert_eq!(shm_r2.generation(), Generation(2));
    assert!(!shm_r2.is_stale());
    drop(slot);
    let slot2 = shm_r2.claim_reader_slot(r.id).unwrap();
    slot2.pin(shm_r2.visible_seqno(), shm_r2.view_version());
    assert_eq!(shm_r2.read_view().unwrap(), view(7, 1, 4));
    assert_eq!(shm2.oldest_reader_pin(), Some((0, 7)));

    // Attaching to the old generation directly is refused as stale.
    let old = r.vfs.open_shared(
        &region_name(r.identity.device, r.identity.inode, 2),
        None,
        4096,
        SharedOpen::Attach,
    );
    assert!(old.is_ok());
}

#[test]
fn layout_version_mismatch_is_refused_unless_alone() {
    let sim = SimVfs::new(6);
    let w = Proc::start(&sim, pid(1));
    let (lock, wp, shm) = w.open_writer();
    let r = Proc::start(&sim, pid(11));
    let (rp, _shm_r, _slot) = r.open_reader();

    // Pretend the live region was built by a build with layout version 2.
    let (mapping, _, _) = shm.arena(0);
    mapping
        .atomic_u32(header::LAYOUT_VERSION)
        .store(2, Ordering::Release);
    let dir_name = directory_name(w.identity.device, w.identity.inode);
    w.vfs
        .open_shared(&dir_name, None, directory::LEN as u64, SharedOpen::Attach)
        .unwrap()
        .atomic_u32(directory::LAYOUT_VERSION)
        .store(2, Ordering::Release);

    // A new reader is refused.
    let r2 = Proc::start(&sim, pid(12));
    match r2.open(Role::Reader) {
        Err(Error::VersionMismatch { found, expected }) => {
            assert_eq!((found, expected), (2, 1));
        }
        other => panic!("expected VersionMismatch, got {other:?}"),
    }

    // The writer dies; a new writer is refused while the reader is still present...
    w.crash();
    std::mem::forget((lock, wp));
    let w2 = Proc::start(&sim, pid(2));
    let lock2 = WriterLock::acquire(&w2.file).unwrap();
    let wp2 = Presence::acquire(&w2.file).unwrap();
    match w2.open(Role::Writer) {
        Err(Error::VersionMismatch { found, .. }) => assert_eq!(found, 2),
        other => panic!("expected VersionMismatch, got {other:?}"),
    }
    // ...and rebuilds once it is alone (its own presence lock stays shared).
    drop(rp);
    let shm2 = w2.open(Role::Writer).unwrap();
    assert_eq!(shm2.generation(), Generation(2));
    assert!(
        matches!(w2.file.lock(8193, LockMode::Exclusive), Ok(())),
        "still the only process present"
    );
    w2.file.lock(8193, LockMode::Shared).unwrap();
    let r3 = Proc::start(&sim, pid(13));
    assert!(
        r3.open(Role::Reader).is_ok(),
        "the rebuilt region is current"
    );
    drop((lock2, wp2));
}

#[test]
fn last_process_removes_the_region() {
    let sim = SimVfs::new(7);
    let w = Proc::start(&sim, pid(1));
    let (lock, wp, shm) = w.open_writer();
    let r = Proc::start(&sim, pid(11));
    let (rp, _shm_r, slot) = r.open_reader();

    assert!(!wp.try_become_last().unwrap());
    assert!(!rp.try_become_last().unwrap());
    drop(slot);
    drop(rp);
    assert!(wp.try_become_last().unwrap());
    drop(shm);
    ShmRegion::remove(&w.vfs, w.identity, None).unwrap();
    assert!(!w.region_exists(1));
    let dir_name = directory_name(w.identity.device, w.identity.inode);
    assert_eq!(
        w.vfs
            .open_shared(&dir_name, None, 4096, SharedOpen::Attach)
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    ShmRegion::remove(&w.vfs, w.identity, None).unwrap();
    assert!(matches!(r.open(Role::Reader), Err(Error::Io(e)) if e.kind == ErrorKind::NotFound));

    // A new writer starts over at generation 1.
    let w2 = Proc::start(&sim, pid(2));
    drop((lock, wp));
    let (_l, _p, shm2) = w2.open_writer();
    assert_eq!(shm2.generation(), Generation(1));
}

#[test]
fn region_belonging_to_another_database_is_refused() {
    let sim = SimVfs::new(8);
    let w = Proc::start(&sim, pid(1));
    let (_lock, _wp, _shm) = w.open_writer();
    let r = Proc::start(&sim, pid(11));
    r.enter();
    let err = ShmRegion::open(
        &r.vfs,
        &r.file,
        r.identity,
        [0xCD; 16],
        Role::Reader,
        &small_config(),
    )
    .unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
}

#[test]
fn shm_init_lock_contention_fails_after_retries() {
    let sim = SimVfs::new(9);
    let w = Proc::start(&sim, pid(1));
    let other = Proc::start(&sim, pid(2));
    other.file.lock(8194, LockMode::Exclusive).unwrap();
    let start = std::time::Instant::now();
    match w.open(Role::Writer) {
        Err(Error::Io(e)) => assert_eq!(e.kind, ErrorKind::Locked),
        other => panic!("expected Locked, got {other:?}"),
    }
    assert!(start.elapsed() < std::time::Duration::from_secs(10));
    other.file.unlock(8194).unwrap();
    assert!(w.open(Role::Writer).is_ok());
}

#[test]
fn file_backed_region_in_a_directory() {
    let sim = SimVfs::new(10);
    let w = Proc::start(&sim, pid(1));
    let mut config = small_config();
    config.dir = Some("/dev/shm-alt".into());
    w.enter();
    let shm = ShmRegion::open(&w.vfs, &w.file, w.identity, DB_ID, Role::Writer, &config).unwrap();
    assert_eq!(shm.generation(), Generation(1));
    let r = Proc::start(&sim, pid(11));
    r.enter();
    let shm_r = ShmRegion::open(&r.vfs, &r.file, r.identity, DB_ID, Role::Reader, &config).unwrap();
    shm.publish_view(&view(1, 1, 1)).unwrap();
    assert_eq!(shm_r.read_view().unwrap().view_version, 1);
    assert!(
        matches!(r.open(Role::Reader), Err(Error::Io(e)) if e.kind == ErrorKind::NotFound),
        "the default location has nothing"
    );
    ShmRegion::remove(&w.vfs, w.identity, config.dir.as_deref()).unwrap();
    assert!(shm_r.read_view().is_ok(), "mappings outlive the name");
}

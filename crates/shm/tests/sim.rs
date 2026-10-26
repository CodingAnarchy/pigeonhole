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
use pigeonhole_shm::{
    Error, Generation, Presence, ReaderSlot, Role, ShmConfig, ShmRegion, WriterLock,
};

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
        self.open_with(role, &small_config())
    }

    fn open_with(&self, role: Role, config: &ShmConfig) -> pigeonhole_shm::Result<ShmRegion> {
        self.enter();
        ShmRegion::open(&self.vfs, &self.file, self.identity, DB_ID, role, config)
    }

    fn open_writer(&self) -> (WriterLock, Presence, ShmRegion) {
        // The writer's order (decision D37): writer lock, region, then presence.
        let lock = WriterLock::acquire(&self.file).unwrap();
        let shm = self.open(Role::Writer).unwrap();
        let presence = Presence::acquire(&self.file).unwrap();
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
    // ICR 0057: typed, not an I/O error.
    match r.open(Role::Reader) {
        Err(Error::NoWriter) => {}
        other => panic!("expected NoWriter, got {other:?}"),
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
    publish(0, shm.next_seqno(), held);
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
    publish(0, shm.next_seqno(), held);
    let q = shm.reserve_seqnos(1);
    held = Some(q);
    publish(0, u64::MAX, held);
    set_mask(&shm, q, 0b11);
    mark_applied(&shm, 0, q);
    check(&shm1, &shm2);
    assert_eq!(shm1.visible_seqno(), 2, "q is held");

    // Shard 1 runs its own group meanwhile: visible stays below q.
    publish(1, shm.next_seqno(), None);
    let g = shm.reserve_seqnos(1);
    publish(1, g, None);
    set_mask(&shm, g, 0b10);
    mark_applied(&shm, 1, g);
    publish(1, u64::MAX, None);
    check(&shm1, &shm2);
    assert_eq!(shm1.visible_seqno(), 2);

    // Shard 0 runs another group while still holding q.
    publish(0, shm.next_seqno(), held);
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

    // A new writer: new generation, old one abandoned and unnamed. It recovered a seqno
    // ceiling of 9 and seeds the counter past it (ICR 0002).
    let w2 = Proc::start(&sim, pid(2));
    let _lock2 = WriterLock::acquire(&w2.file).unwrap();
    let mut config = small_config();
    config.first_seqno = 10;
    let shm2 = w2.open_with(Role::Writer, &config).unwrap();
    let _wp2 = Presence::acquire(&w2.file).unwrap();
    assert_eq!(shm2.generation(), Generation(2));
    assert_eq!(w2.directory_generation(), 2);
    assert!(!w2.region_exists(1), "old region's name is removed");
    assert!(w2.region_exists(2));
    shm2.publish_view(&view(7, 1, 4)).unwrap();
    assert_eq!(shm2.next_seqno(), 10);
    assert_eq!(shm2.visible_seqno(), 9, "never below what readers saw");
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
    assert!(shm_r2.visible_seqno() >= 9, "monotone across the restart");
    assert_eq!(
        slot2.pin(shm_r2.visible_seqno(), shm_r2.view_version()),
        (9, 7)
    );
    assert_eq!(shm_r2.read_view().unwrap(), view(7, 1, 4));
    assert_eq!(shm2.oldest_reader_pin(), Some((9, 7)));

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
    // The previous layout (an older writer's region left behind, D277's upgrade: perf287's
    // P3) and a newer one.
    for other in [3, 5] {
        mismatch_refused_unless_alone(other);
    }
}

fn mismatch_refused_unless_alone(other: u32) {
    let current = pigeonhole_format::ShmLayoutVersion::CURRENT.0;
    let sim = SimVfs::new(6);
    let w = Proc::start(&sim, pid(1));
    let (lock, wp, shm) = w.open_writer();
    let r = Proc::start(&sim, pid(11));
    let (rp, _shm_r, _slot) = r.open_reader();

    // Pretend the live region was built by a build with another layout version.
    let (mapping, _, _) = shm.arena(0);
    mapping
        .atomic_u32(header::LAYOUT_VERSION)
        .store(other, Ordering::Release);
    let dir_name = directory_name(w.identity.device, w.identity.inode);
    w.vfs
        .open_shared(&dir_name, None, directory::LEN as u64, SharedOpen::Attach)
        .unwrap()
        .atomic_u32(directory::LAYOUT_VERSION)
        .store(other, Ordering::Release);

    // A new reader is refused.
    let r2 = Proc::start(&sim, pid(12));
    match r2.open(Role::Reader) {
        Err(Error::VersionMismatch { found, expected }) => {
            assert_eq!((found, expected), (other, current));
        }
        other => panic!("expected VersionMismatch, got {other:?}"),
    }

    // The writer dies; a new writer is refused while the reader is still present...
    w.crash();
    std::mem::forget((lock, wp));
    let w2 = Proc::start(&sim, pid(2));
    let lock2 = WriterLock::acquire(&w2.file).unwrap();
    match w2.open(Role::Writer) {
        Err(Error::VersionMismatch { found, .. }) => assert_eq!(found, other),
        other => panic!("expected VersionMismatch, got {other:?}"),
    }
    // ...and rebuilds once it is alone. The probe ran with no presence lock held (D37) and
    // left the byte shared, so taking `Presence` afterwards is a no-op.
    drop(rp);
    let shm2 = w2.open(Role::Writer).unwrap();
    let wp2 = Presence::acquire(&w2.file).unwrap();
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

    // The writer is not last while the reader is present; a failed upgrade releases its
    // presence byte (#150 item 4.7), so the reader, closing next, is.
    assert!(!wp.try_become_last().unwrap());
    drop(slot);
    assert!(rp.try_become_last().unwrap());
    drop(rp);
    // The writer re-takes its byte only to test removal below.
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
    assert!(matches!(r.open(Role::Reader), Err(Error::NoWriter)));

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
        matches!(r.open(Role::Reader), Err(Error::NoWriter)),
        "the default location has nothing"
    );
    ShmRegion::remove(&w.vfs, w.identity, config.dir.as_deref()).unwrap();
    assert!(shm_r.read_view().is_ok(), "mappings outlive the name");
}

/// The writer is present once its open publishes a generation, before it takes `Presence`:
/// a reader closing in that window cannot pass the last-one-out check and remove the region.
#[test]
fn writer_open_is_present_before_presence_is_taken() {
    let sim = SimVfs::new(14);
    let r = Proc::start(&sim, pid(11));
    let w = Proc::start(&sim, pid(1));
    let _lock = WriterLock::acquire(&w.file).unwrap();
    let shm = w.open(Role::Writer).unwrap();
    // A reader that was present (and opens nothing more) now closes.
    let rp = Presence::acquire(&r.file).unwrap();
    assert!(!rp.try_become_last().unwrap(), "the writer is present");
    drop(rp);
    let _wp = Presence::acquire(&w.file).unwrap();
    assert_eq!(w.directory_generation(), shm.generation().0);
    assert!(w.region_exists(shm.generation().0));
}

/// #844 (ICR 0057): a reader that finds no writer's region to attach to gets `NoWriter`, not
/// an I/O error: before any writer built one, and on re-attach when the generation the
/// directory names is gone (on Windows a closed writer's mapping disappears with its last
/// handle; here its name is removed by hand to the same effect).
#[test]
fn a_reader_with_no_writer_region_gets_no_writer() {
    let sim = SimVfs::new(844);
    // No writer has built a region yet.
    let r = Proc::start(&sim, pid(11));
    assert!(matches!(r.open(Role::Reader), Err(Error::NoWriter)));

    let w = Proc::start(&sim, pid(1));
    let (lock, wp, shm) = w.open_writer();
    shm.publish_view(&view(1, 3, 8)).unwrap();
    let (_rp, shm_r, _slot) = r.open_reader();

    // A new writer builds generation 2, then goes away and its region with it.
    w.crash();
    std::mem::forget((lock, wp, shm));
    let w2 = Proc::start(&sim, pid(2));
    let (lock2, wp2, shm2) = w2.open_writer();
    assert_eq!(shm2.generation(), Generation(2));
    drop((shm2, wp2, lock2));
    let name = region_name(w2.identity.device, w2.identity.inode, 2);
    let _ = pigeonhole_io::Vfs::remove_shared(&*sim, &name, None);
    assert!(!w2.region_exists(2));

    // The reader sees the generation change and re-attaches: nothing to attach to.
    r.enter();
    assert!(shm_r.is_stale());
    assert!(matches!(
        shm_r.reattach(&r.vfs, &r.file),
        Err(Error::NoWriter)
    ));
}

/// #693 PR 3: liveness by each slot's byte-range lock, not the pid. A live reader is never
/// judged dead (even one whose pid the writer reads as gone, as from another pid namespace),
/// a killed reader's slot is freed, and so is a slot left in `claiming` by a claimer that
/// died before recording itself.
#[test]
fn reader_liveness_is_judged_by_the_slots_lock() {
    use pigeonhole_format::superblock::READER_LOCK_BASE;
    let sim = SimVfs::new(693);
    let w = Proc::start(&sim, pid(1));
    let (_lock, _wp, shm) = w.open_writer();

    // A live reader: its handle holds its slot's lock, so it is kept whatever its pid says
    // (the probe never looks at the pid; a reader in another pid namespace is the same).
    let hidden = Proc::start(&sim, pid(21));
    let rs = hidden.open(Role::Reader).unwrap();
    let kept = rs
        .claim_reader_slot_locked(hidden.id, &hidden.file)
        .unwrap();
    assert_eq!(
        shm.reclaim_dead_slots_locked(&w.file),
        0,
        "a live reader's slot was freed"
    );
    assert!(kept.is_owned());
    // Its byte is held: another handle cannot take it, so a second claim skips the slot.
    assert!(
        w.file
            .lock(
                READER_LOCK_BASE + u64::from(kept.index()),
                LockMode::Exclusive
            )
            .is_err()
    );

    // A killed reader (its handles closed by the crash, so its lock released): freed.
    let doomed = Proc::start(&sim, pid(22));
    let ds = doomed.open(Role::Reader).unwrap();
    let dslot = ds
        .claim_reader_slot_locked(doomed.id, &doomed.file)
        .unwrap();
    let dindex = dslot.index();
    doomed.crash();
    std::mem::forget(dslot);
    std::mem::forget(ds);
    sim.enter_process(pid(1));
    assert_eq!(shm.reclaim_dead_slots_locked(&w.file), 1);
    assert_eq!(
        shm.reader_slot_usage().0,
        1,
        "only the live reader's slot is claimed"
    );

    // The freed slot can be claimed again, and holds its new owner's lock.
    let next = Proc::start(&sim, pid(23));
    let ns = next.open(Role::Reader).unwrap();
    let nslot = ns.claim_reader_slot_locked(next.id, &next.file).unwrap();
    assert_eq!(nslot.index(), dindex);
    sim.enter_process(pid(1));
    assert_eq!(shm.reclaim_dead_slots_locked(&w.file), 0);

    // Dropping a slot frees it and releases its byte, in that order.
    drop(nslot);
    assert!(
        next.file
            .lock(READER_LOCK_BASE + u64::from(dindex), LockMode::Exclusive)
            .is_ok()
    );
    next.file
        .unlock(READER_LOCK_BASE + u64::from(dindex))
        .unwrap();
    drop(kept);
    assert_eq!(shm.reader_slot_usage().0, 0);
}

#[test]
fn an_abandoned_slot_leaves_a_later_claim_through_its_handle_alone() {
    use pigeonhole_format::superblock::READER_LOCK_BASE;
    let sim = SimVfs::new(694);
    let w = Proc::start(&sim, pid(1));
    let (_lock, _wp, shm) = w.open_writer();
    let r = Proc::start(&sim, pid(31));
    let rs = r.open(Role::Reader).unwrap();
    let old = rs.claim_reader_slot_locked(r.id, &r.file).unwrap();
    let index = old.index();
    let byte = READER_LOCK_BASE + u64::from(index);
    let ownership = old.ownership();

    // Abandoned: no longer this handle's, and its byte is released at once (so the writer
    // may free the slot), but the slot itself is left as it was.
    old.abandon();
    assert!(!old.is_owned() && !ownership.is_current());
    sim.enter_process(pid(1));
    assert_eq!(shm.reclaim_dead_slots_locked(&w.file), 1);

    // The same handle claims the same slot again (locks never conflict within a handle).
    sim.enter_process(pid(31));
    let new = rs.claim_reader_slot_locked(r.id, &r.file).unwrap();
    assert_eq!(new.index(), index);
    assert_eq!(new.pin(5, 0), (5, 0));
    // The abandoned handle stores nothing, and its drop neither frees the new claim (which
    // names the same process) nor releases the byte under it.
    old.raise_pin(9, 9);
    old.unpin();
    drop(old);
    assert!(new.is_owned() && !ownership.is_current());
    assert_eq!(shm.oldest_reader_pin(), Some((5, 0)));
    sim.enter_process(pid(1));
    assert!(
        w.file.lock(byte, LockMode::Exclusive).is_err(),
        "the new claim lost its lock"
    );
    assert_eq!(shm.reclaim_dead_slots_locked(&w.file), 0);
    sim.enter_process(pid(31));
    drop(new);
    assert_eq!(shm.reader_slot_usage().0, 0);
}

/// perf287's review of #898, S2: a claim through a handle that already holds a live slot's
/// byte skips that slot instead of locking and then unlocking the byte (locks never conflict
/// within a handle), so the live slot keeps its lock.
#[test]
fn a_claim_through_a_handle_holding_a_slots_byte_leaves_that_lock_alone() {
    use pigeonhole_format::superblock::READER_LOCK_BASE;
    let sim = SimVfs::new(695);
    let w = Proc::start(&sim, pid(1));
    let (_lock, _wp, shm) = w.open_writer();
    let r = Proc::start(&sim, pid(41));
    let rs = r.open(Role::Reader).unwrap();
    let first = rs.claim_reader_slot_locked(r.id, &r.file).unwrap();
    let byte = READER_LOCK_BASE + u64::from(first.index());
    assert!(r.file.holds(byte).is_some());

    // A second claim through the same handle, the first slot not abandoned: another slot.
    let second = rs.claim_reader_slot_locked(r.id, &r.file).unwrap();
    assert_ne!(second.index(), first.index());
    assert!(
        r.file.holds(byte).is_some(),
        "the claim released the first slot's byte"
    );
    sim.enter_process(pid(1));
    assert!(w.file.lock(byte, LockMode::Exclusive).is_err());
    assert_eq!(shm.reclaim_dead_slots_locked(&w.file), 0);
    sim.enter_process(pid(41));
    assert!(first.is_owned() && second.is_owned());
    drop(second);
    drop(first);
    assert_eq!(shm.reader_slot_usage().0, 0);
}

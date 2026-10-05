//! The in-process protocol suite on the heap-backed region: view publication by version and
//! overflow, reader slots and the pin handshake, and the FORMAT §11.3 seqno and watermark
//! protocol (including the `min(held, ..)` rule) under deterministic interleavings
//! (proptest) and real threads.

mod common;

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use common::*;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{OpenOptions, ProcessId, VfsRef};
use pigeonhole_shm::{Error, Generation, ShmRegion};
use proptest::prelude::*;

fn proc(pid: u32) -> ProcessId {
    ProcessId { pid, start_time: 1 }
}

/// Seed from `PIGEONHOLE_SEED`, or a fixed default; printed on failure.
fn seed() -> u64 {
    std::env::var("PIGEONHOLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x5EED_0001)
}

// ---- views ----

#[test]
fn views_publish_by_version_and_alternate_buffers() {
    let shm = ShmRegion::in_memory(DB_ID, &small_config());
    assert_eq!(shm.view_version(), 0);
    for version in 1..=5 {
        let v = view(version, version as usize, 8);
        shm.publish_view(&v).unwrap();
        assert_eq!(shm.view_version(), version);
        assert_eq!(shm.read_view().unwrap(), v);
    }
    shm.set_manifest_version(77);
    assert_eq!(shm.manifest_version(), 77);
}

#[test]
fn oversized_view_is_refused_and_nothing_changes() {
    let mut config = small_config();
    config.view_buffer_bytes = 4096;
    let shm = ShmRegion::in_memory(DB_ID, &config);
    let v1 = view(1, 2, 8);
    shm.publish_view(&v1).unwrap();

    let big = view(2, 100, 64);
    let needed = big.encoded_len();
    assert!(needed > 4096);
    match shm.publish_view(&big) {
        Err(Error::ViewTooLarge {
            needed: n,
            capacity,
        }) => {
            assert_eq!(n, needed);
            assert_eq!(capacity, 4096);
        }
        other => panic!("expected ViewTooLarge, got {other:?}"),
    }
    assert_eq!(shm.view_version(), 1, "nothing was published");
    assert_eq!(shm.read_view().unwrap(), v1);

    // Exactly full is fine.
    let mut fits = view(2, 1, 0);
    let slack = 4096 - fits.encoded_len();
    fits.tablets[0].start = vec![1; slack / 2];
    fits.tablets[0].end = Some(vec![2; slack - slack / 2]);
    assert_eq!(fits.encoded_len(), 4096);
    shm.publish_view(&fits).unwrap();
    assert_eq!(shm.read_view().unwrap(), fits);
}

#[test]
fn concurrent_readers_never_decode_a_torn_view() {
    let shm = ShmRegion::in_memory(DB_ID, &small_config());
    let stop = AtomicBool::new(false);
    let reads = AtomicU64::new(0);
    let expected = |version: u64| view(version, (version % 7) as usize + 1, 16);
    shm.publish_view(&expected(1)).unwrap();
    std::thread::scope(|s| {
        s.spawn(|| {
            let mut version = 1;
            while !stop.load(Ordering::Relaxed) {
                version += 1;
                shm.publish_view(&expected(version)).unwrap();
            }
        });
        for _ in 0..3 {
            s.spawn(|| {
                let mut last = 0;
                while !stop.load(Ordering::Relaxed) {
                    let v = shm.read_view().unwrap();
                    assert!(v.view_version >= last, "views went backwards");
                    last = v.view_version;
                    assert_eq!(v, expected(v.view_version), "torn or mixed view");
                    reads.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        std::thread::sleep(Duration::from_millis(300));
        stop.store(true, Ordering::Relaxed);
    });
    assert!(reads.load(Ordering::Relaxed) > 100);
}

// ---- reader slots ----

#[test]
fn reader_slots_claim_pin_release_and_exhaustion() {
    let shm = ShmRegion::in_memory(DB_ID, &small_config());
    let slots: Vec<_> = (0..4)
        .map(|i| shm.claim_reader_slot(proc(100 + i)).unwrap())
        .collect();
    let mut indexes: Vec<u32> = slots.iter().map(|s| s.index()).collect();
    indexes.sort_unstable();
    assert_eq!(indexes, vec![0, 1, 2, 3]);
    assert!(matches!(
        shm.claim_reader_slot(proc(200)),
        Err(Error::NoReaderSlot)
    ));

    assert_eq!(shm.oldest_reader_pin(), None);
    shm.publish_view(&view(3, 1, 4)).unwrap();
    slots[0].pin(30, 3);
    slots[1].pin(20, 3);
    slots[2].pin(25, 2); // older than the current view: the pin moves up to 3
    assert_eq!(shm.oldest_reader_pin(), Some((20, 3)));
    slots[1].unpin();
    assert_eq!(shm.oldest_reader_pin(), Some((25, 3)));

    let freed = slots[2].index();
    drop(slots);
    assert_eq!(shm.oldest_reader_pin(), None, "dropping a slot unpins it");
    let again = shm.claim_reader_slot(proc(300)).unwrap();
    assert!(again.index() <= freed, "freed slots are reused");
}

#[test]
fn pin_moves_up_when_the_writer_published_a_newer_view() {
    let shm = ShmRegion::in_memory(DB_ID, &small_config());
    let slot = shm.claim_reader_slot(proc(1)).unwrap();
    slot.pin(3, 0);
    assert_eq!(shm.oldest_reader_pin(), Some((3, 0)), "no view yet: view 0");

    shm.publish_view(&view(1, 1, 4)).unwrap();
    let v = shm.view_version();
    // The writer publishes between the reader's load of the version and its pin.
    shm.publish_view(&view(2, 1, 4)).unwrap();
    slot.pin(5, v);
    assert_eq!(
        shm.oldest_reader_pin(),
        Some((5, 2)),
        "the pin lands on the newer view, never on one that may be reclaimed"
    );
    let read = shm.read_view().unwrap();
    assert!(read.view_version >= 2);
}

#[test]
fn dead_reader_slots_are_reclaimed_by_liveness() {
    let sim = SimVfs::new(1);
    let vfs: VfsRef = sim.clone();
    let shm = ShmRegion::in_memory(DB_ID, &small_config());
    let alive = shm.claim_reader_slot(proc(10)).unwrap();
    let dead = shm.claim_reader_slot(proc(11)).unwrap();
    let recycled = shm.claim_reader_slot(proc(12)).unwrap();
    alive.pin(5, 0);
    dead.pin(2, 0);
    recycled.pin(3, 0);
    assert_eq!(shm.oldest_reader_pin(), Some((2, 0)));
    assert_eq!(shm.reclaim_dead_slots(&vfs), 0);

    sim.kill_process(proc(11));
    // Same pid, different start time: the recorded process is gone.
    sim.kill_process(proc(12));
    assert_eq!(shm.reclaim_dead_slots(&vfs), 2);
    assert_eq!(shm.reclaim_dead_slots(&vfs), 0, "nothing left to reclaim");
    assert_eq!(shm.oldest_reader_pin(), Some((5, 0)));
    let (a, b) = (
        shm.claim_reader_slot(proc(13)).unwrap(),
        shm.claim_reader_slot(proc(14)).unwrap(),
    );
    assert_ne!(a.index(), b.index());
    assert_ne!(a.index(), alive.index());
    // The dropped `ReaderSlot`s of dead processes write into slots now owned by others; the
    // writer reset them, so the originals must not free them again.
    std::mem::forget(dead);
    std::mem::forget(recycled);
}

#[test]
fn in_memory_region_is_never_stale() {
    let vfs: VfsRef = SimVfs::new(1);
    let file = vfs
        .open(Path::new("/db/data.phdb"), OpenOptions::read_write_create())
        .unwrap();
    let shm = ShmRegion::in_memory(DB_ID, &small_config());
    assert!(!shm.is_stale());
    assert_eq!(shm.generation(), Generation(1));
    let again = shm.reattach(&vfs, &file).unwrap();
    assert_eq!(again.generation(), Generation(1));
    shm.reserve_seqnos(5);
    assert_eq!(again.visible_seqno(), shm.visible_seqno(), "same mapping");
}

// ---- seqnos and watermarks ----

#[test]
fn idle_shards_never_hold_back_snapshots() {
    let shm = ShmRegion::in_memory(DB_ID, &small_config());
    assert_eq!(shm.visible_seqno(), 0);
    assert_eq!(shm.reserve_seqnos(4), 1, "seqnos start at 1");
    // Reserved without a prior lower bound: visible at once (the caller broke the protocol,
    // but idle shards publish `u64::MAX`).
    assert_eq!(shm.visible_seqno(), 4);
    shm.publish_pending(1, 3);
    assert_eq!(shm.visible_seqno(), 2);
    shm.publish_pending(1, u64::MAX);
    assert_eq!(shm.visible_seqno(), 4);
}

/// The protocol of FORMAT §11.3 step by step for a cross-shard commit, and what happens
/// without the `min(held, ..)` rule.
#[test]
fn cross_shard_commit_is_all_or_nothing_only_with_min_held() {
    for with_min_held in [true, false] {
        let shm = ShmRegion::in_memory(DB_ID, &small_config());
        let mut last = 0;
        // Shard 0 coordinates seqno q with shard 1 as participant.
        shm.publish_pending(0, shm.visible_seqno() + 1);
        let q = shm.reserve_seqnos(1);
        assert_eq!(q, 1);
        let held = q;
        shm.publish_pending(0, held);
        set_mask(&shm, q, 0b11);
        mark_applied(&shm, 0, q);
        assert_eq!(shm.visible_seqno(), 0);
        check_snapshot(&shm, &mut last, u64::MAX).unwrap();

        // Shard 0's next group while shard 1 has not applied q.
        let min = |x: u64| if with_min_held { held.min(x) } else { x };
        shm.publish_pending(0, min(shm.visible_seqno() + 1));
        let first = shm.reserve_seqnos(2);
        shm.publish_pending(0, min(first));
        for s in first..first + 2 {
            set_mask(&shm, s, 0b01);
            mark_applied(&shm, 0, s);
            shm.publish_pending(0, min(s + 1));
        }
        shm.publish_pending(0, min(u64::MAX));

        let result = check_snapshot(&shm, &mut last, u64::MAX);
        if with_min_held {
            assert_eq!(shm.visible_seqno(), 0, "q holds the watermark");
            result.unwrap();
        } else {
            assert_eq!(shm.visible_seqno(), 3, "half of q is exposed");
            let err = result.unwrap_err();
            assert!(err.contains("half of a cross-shard commit"), "{err}");
            continue;
        }

        // Shard 1 applies; the coordinator releases q.
        mark_applied(&shm, 1, q);
        shm.publish_pending(0, u64::MAX);
        assert_eq!(shm.visible_seqno(), 3);
        check_snapshot(&shm, &mut last, u64::MAX).unwrap();
    }
}

/// A single-shard group's state in the step machine.
#[derive(Clone, Copy)]
enum Phase {
    Idle,
    Bounded,
    Reserved { first: u64, n: u64, next: u64 },
}

struct ShardSm {
    phase: Phase,
    held: std::collections::BTreeSet<u64>,
    inbox: Vec<u64>,
}

struct Commit {
    seqno: u64,
    coordinator: u32,
    participants: Vec<u32>,
    applied: Vec<u32>,
    released: bool,
}

struct Machine {
    shm: ShmRegion,
    shards: Vec<ShardSm>,
    commits: Vec<Commit>,
    last: u64,
    reserved: u64,
}

impl Machine {
    fn new(shards: u32) -> Self {
        let mut config = small_config();
        config.shards = shards;
        Self {
            shm: ShmRegion::in_memory(DB_ID, &config),
            shards: (0..shards)
                .map(|_| ShardSm {
                    phase: Phase::Idle,
                    held: Default::default(),
                    inbox: Vec::new(),
                })
                .collect(),
            commits: Vec::new(),
            last: 0,
            reserved: 0,
        }
    }

    fn publish(&self, shard: u32) {
        let sm = &self.shards[shard as usize];
        let held = sm.held.first().copied().unwrap_or(u64::MAX);
        let group = match sm.phase {
            Phase::Idle => u64::MAX,
            Phase::Bounded => self.shm.visible_seqno() + 1,
            Phase::Reserved { next, .. } => next,
        };
        self.shm.publish_pending(shard, held.min(group));
    }

    /// One step chosen by `choice`; returns whether it did anything.
    fn step(&mut self, choice: u8) -> bool {
        let shards = self.shards.len() as u32;
        let shard = u32::from(choice) % shards;
        let action = (u32::from(choice) / shards) % 4;
        let s = shard as usize;
        match action {
            0 => match self.shards[s].phase {
                Phase::Idle => {
                    self.shards[s].phase = Phase::Bounded;
                    self.publish(shard);
                }
                Phase::Bounded => {
                    let n = 1 + u64::from(choice >> 6);
                    let first = self.shm.reserve_seqnos(n);
                    self.reserved += n;
                    for q in first..first + n {
                        set_mask(&self.shm, q, 1 << shard);
                    }
                    self.shards[s].phase = Phase::Reserved {
                        first,
                        n,
                        next: first,
                    };
                    self.publish(shard);
                }
                Phase::Reserved { first, n, next } => {
                    mark_applied(&self.shm, shard, next);
                    self.shards[s].phase = if next + 1 < first + n {
                        Phase::Reserved {
                            first,
                            n,
                            next: next + 1,
                        }
                    } else {
                        Phase::Idle
                    };
                    self.publish(shard);
                }
            },
            1 => {
                if shards == 1 || !matches!(self.shards[s].phase, Phase::Idle) {
                    return false;
                }
                self.shards[s].phase = Phase::Bounded;
                self.publish(shard);
                let seqno = self.shm.reserve_seqnos(1);
                self.reserved += 1;
                self.shards[s].held.insert(seqno);
                self.shards[s].phase = Phase::Idle;
                self.publish(shard);
                let mut participants: Vec<u32> = (0..shards)
                    .filter(|&o| o != shard && (choice >> 2) & (1 << (o % 6)) != 0)
                    .collect();
                if participants.is_empty() {
                    participants.push((shard + 1) % shards);
                }
                let mask = participants
                    .iter()
                    .fold(1u64 << shard, |m, &p| m | (1 << p));
                set_mask(&self.shm, seqno, mask);
                mark_applied(&self.shm, shard, seqno);
                for &p in &participants {
                    self.shards[p as usize].inbox.push(seqno);
                }
                self.commits.push(Commit {
                    seqno,
                    coordinator: shard,
                    participants,
                    applied: Vec::new(),
                    released: false,
                });
            }
            2 => {
                if self.shards[s].inbox.is_empty() {
                    return false;
                }
                let seqno = self.shards[s].inbox.remove(0);
                mark_applied(&self.shm, shard, seqno);
                let c = self.commits.iter_mut().find(|c| c.seqno == seqno).unwrap();
                c.applied.push(shard);
            }
            _ => {
                let Some(c) = self.commits.iter_mut().find(|c| {
                    c.coordinator == shard && !c.released && c.applied.len() == c.participants.len()
                }) else {
                    return false;
                };
                c.released = true;
                let seqno = c.seqno;
                self.shards[s].held.remove(&seqno);
                self.publish(shard);
            }
        }
        true
    }

    fn check(&mut self) -> Result<(), TestCaseError> {
        check_snapshot(&self.shm, &mut self.last, u64::MAX).map_err(TestCaseError::fail)?;
        prop_assert!(self.shm.visible_seqno() <= self.reserved);
        Ok(())
    }

    /// Finishes every group and commit; afterwards everything reserved is visible.
    fn drain(&mut self) -> Result<(), TestCaseError> {
        for _ in 0..10_000 {
            let mut progressed = false;
            for shard in 0..self.shards.len() as u32 {
                for action in [0u8, 2, 3] {
                    if action == 0 && matches!(self.shards[shard as usize].phase, Phase::Idle) {
                        continue; // never start a new group while draining
                    }
                    let choice = action * self.shards.len() as u8 + shard as u8;
                    progressed |= self.step(choice);
                    self.check()?;
                }
            }
            if !progressed {
                break;
            }
        }
        prop_assert_eq!(self.shm.visible_seqno(), self.reserved);
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES").ok().and_then(|s| s.parse().ok()).unwrap_or(64),
        ..ProptestConfig::default()
    })]

    /// Under any interleaving of protocol steps across shards, a snapshot never includes a
    /// reserved-but-unapplied seqno or half of a cross-shard commit, never goes backwards,
    /// and once everything is applied and released, everything is visible.
    #[test]
    fn watermark_protocol_holds_under_any_interleaving(
        shards in 1u32..=4,
        choices in prop::collection::vec(any::<u8>(), 0..300),
    ) {
        let mut m = Machine::new(shards);
        m.check()?;
        for c in choices {
            m.step(c);
            m.check()?;
        }
        m.drain()?;
    }
}

#[test]
fn threaded_writer_keeps_concurrent_readers_consistent() {
    let seed = seed();
    let mut config = small_config();
    config.shards = 4;
    let shm = ShmRegion::in_memory(DB_ID, &config);
    let stop = AtomicBool::new(false);
    let checks = AtomicU64::new(0);
    let reserved = std::thread::scope(|s| {
        let readers: Vec<_> = (0..2)
            .map(|_| {
                s.spawn(|| {
                    let mut last = 0;
                    while !stop.load(Ordering::Relaxed) {
                        if let Err(e) = check_snapshot(&shm, &mut last, 512) {
                            panic!("seed {seed}: {e}");
                        }
                        checks.fetch_add(1, Ordering::Relaxed);
                    }
                    last
                })
            })
            .collect();
        let reserved = run_writer(&shm, seed, Duration::from_millis(400), &stop);
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }
        reserved
    });
    assert!(reserved > 0, "seed {seed}");
    assert_eq!(
        shm.visible_seqno(),
        reserved,
        "seed {seed}: all applied in the end"
    );
    let mut last = 0;
    check_snapshot(&shm, &mut last, u64::MAX).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    assert!(checks.load(Ordering::Relaxed) > 0);
}

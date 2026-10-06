//! Shared by the integration tests: a small region configuration, a writer workload that
//! follows the FORMAT §11.3 protocol (single-shard groups and cross-shard commits with the
//! `min(held, ..)` rule) and records what it applied in the arenas, and a reader-side checker
//! that verifies a snapshot is consistent with those records.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pigeonhole_format::shm::{ViewRecord, ViewTablet};
use pigeonhole_format::{TableId, TabletId};
use pigeonhole_shm::{ShmConfig, ShmRegion};

/// Database id every test uses.
pub const DB_ID: [u8; 16] = [0xAB; 16];

/// A region small enough to build many times: 2 shards, 2 MiB arenas, 64 KiB view
/// buffers, 4 reader slots.
pub fn small_config() -> ShmConfig {
    let mut config = ShmConfig::new(2);
    config.arena_bytes = 2 << 20;
    config.view_buffer_bytes = 64 << 10;
    config.reader_slots = 4;
    config
}

/// A view with `tablets` tablets of `key_len`-byte boundaries.
pub fn view(version: u64, tablets: usize, key_len: usize) -> ViewRecord {
    ViewRecord {
        view_version: version,
        manifest_version: version * 10,
        tablets: (0..tablets)
            .map(|i| ViewTablet {
                tablet: TabletId(i as u64 + 1),
                table: TableId(1),
                shard: (i % 2) as u16,
                start: vec![i as u8; key_len],
                end: Some(vec![i as u8 + 1; key_len]),
            })
            .collect(),
        memtables: Vec::new(),
    }
}

// ---- arena records ----
//
// Shard `s`'s arena holds a bitmap at `APPLIED_OFF`: bit `q` set means seqno `q` is applied
// on shard `s`. Shard 0's arena also holds, at `MASK_OFF`, one u64 per seqno: the set of
// shards that seqno belongs to, written before the seqno can become visible.

/// Seqnos a workload may use (bounds the bitmaps).
pub const MAX_SEQNO: u64 = 1 << 16;
const APPLIED_OFF: usize = 64;
const MASK_OFF: usize = 64 + (MAX_SEQNO as usize / 8) + 64;

fn applied_word(shm: &ShmRegion, shard: u32, seqno: u64) -> (pigeonhole_io::SharedRegion, usize) {
    let (region, base, _) = shm.arena(shard);
    (region, base + APPLIED_OFF + (seqno / 64) as usize * 8)
}

/// Marks `seqno` applied on `shard`.
pub fn mark_applied(shm: &ShmRegion, shard: u32, seqno: u64) {
    let (region, off) = applied_word(shm, shard, seqno);
    region
        .atomic_u64(off)
        .fetch_or(1 << (seqno % 64), Ordering::Release);
}

/// Whether `seqno` is applied on `shard`.
pub fn is_applied(shm: &ShmRegion, shard: u32, seqno: u64) -> bool {
    let (region, off) = applied_word(shm, shard, seqno);
    region.atomic_u64(off).load(Ordering::Acquire) & (1 << (seqno % 64)) != 0
}

/// Records which shards `seqno` belongs to.
pub fn set_mask(shm: &ShmRegion, seqno: u64, mask: u64) {
    let (region, base, _) = shm.arena(0);
    region
        .atomic_u64(base + MASK_OFF + seqno as usize * 8)
        .store(mask, Ordering::Release);
}

/// Which shards `seqno` belongs to (0 = not yet assigned).
pub fn mask(shm: &ShmRegion, seqno: u64) -> u64 {
    let (region, base, _) = shm.arena(0);
    region
        .atomic_u64(base + MASK_OFF + seqno as usize * 8)
        .load(Ordering::Acquire)
}

// ---- reader-side checker ----

/// Checks one snapshot: the visible seqno never goes backwards for this checker, and every
/// seqno it covers (within `window` of it) is assigned and applied on every shard it belongs
/// to. Returns the visible seqno.
pub fn check_snapshot(shm: &ShmRegion, last: &mut u64, window: u64) -> Result<u64, String> {
    let visible = shm.visible_seqno();
    if visible < *last {
        return Err(format!(
            "visible seqno went backwards: {} -> {visible}",
            *last
        ));
    }
    *last = visible;
    let from = visible.saturating_sub(window).max(1);
    for seqno in from..=visible {
        let mask = mask(shm, seqno);
        if mask == 0 {
            return Err(format!(
                "seqno {seqno} is visible (<= {visible}) but unassigned"
            ));
        }
        for shard in 0..shm.shard_count() {
            if mask & (1 << shard) != 0 && !is_applied(shm, shard, seqno) {
                return Err(format!(
                    "seqno {seqno} is visible (<= {visible}) but not applied on shard {shard}: \
                     half of a cross-shard commit (mask {mask:#b})"
                ));
            }
        }
    }
    Ok(visible)
}

// ---- writer workload ----

/// One shard's protocol state: the next unapplied seqno of its current group, and the
/// cross-shard seqnos it coordinates and has not released (`held`).
struct ShardState {
    shard: u32,
    group: Option<u64>,
    held: BTreeSet<u64>,
}

impl ShardState {
    /// `pending[shard].store(min(held, group))`, FORMAT §11.3.
    fn publish(&self, shm: &ShmRegion) {
        let held = self.held.first().copied().unwrap_or(u64::MAX);
        let group = self.group.unwrap_or(u64::MAX);
        shm.publish_pending(self.shard, held.min(group));
    }
}

/// A cross-shard commit a coordinator handed to a participant.
struct Share {
    seqno: u64,
    done: Arc<AtomicU32>,
}

/// Seeded SplitMix64, so a workload replays from its seed.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next()) * u128::from(n)) >> 64) as u64
    }
}

/// Runs one thread per shard for `duration` (or until the seqno budget is spent), each
/// doing single-shard groups and, now and then, coordinating a cross-shard commit. Returns
/// the number of seqnos reserved. Stops early if `stop` is set.
pub fn run_writer(shm: &ShmRegion, seed: u64, duration: Duration, stop: &AtomicBool) -> u64 {
    let shards = shm.shard_count();
    let (senders, receivers): (Vec<Sender<Share>>, Vec<Receiver<Share>>) =
        (0..shards).map(|_| channel()).unzip();
    let receivers: Vec<Mutex<Receiver<Share>>> = receivers.into_iter().map(Mutex::new).collect();
    let reserved = AtomicU32::new(0);
    // Shards that stopped issuing new commits; a shard exits only once every shard has, so
    // no share it must apply is left behind.
    let stopped = AtomicU32::new(0);
    let deadline = Instant::now() + duration;
    std::thread::scope(|scope| {
        for shard in 0..shards {
            let senders = &senders;
            let receivers = &receivers;
            let reserved = &reserved;
            let stopped = &stopped;
            let mut rng = Rng(seed ^ (u64::from(shard) << 32));
            scope.spawn(move || {
                let inbox = receivers[shard as usize].lock().unwrap();
                let mut me = ShardState {
                    shard,
                    group: None,
                    held: BTreeSet::new(),
                };
                // Cross-shard commits this shard coordinates and waits on.
                let mut coordinating: Vec<(u64, Arc<AtomicU32>, u32)> = Vec::new();
                let mut i_stopped = false;
                loop {
                    // Participant duties: apply shares handed to us.
                    while let Ok(share) = inbox.try_recv() {
                        mark_applied(shm, shard, share.seqno);
                        share.done.fetch_add(1, Ordering::AcqRel);
                    }
                    // Coordinator duties: release commits every participant applied.
                    coordinating.retain(|(seqno, done, participants)| {
                        if done.load(Ordering::Acquire) == *participants {
                            me.held.remove(seqno);
                            me.publish(shm);
                            false
                        } else {
                            true
                        }
                    });
                    let done_issuing = Instant::now() >= deadline
                        || stop.load(Ordering::Relaxed)
                        || reserved.load(Ordering::Relaxed) as u64 + 8 >= MAX_SEQNO;
                    if i_stopped {
                        if stopped.load(Ordering::Acquire) == shards {
                            while let Ok(share) = inbox.try_recv() {
                                mark_applied(shm, shard, share.seqno);
                                share.done.fetch_add(1, Ordering::AcqRel);
                            }
                            break;
                        }
                        std::thread::yield_now();
                        continue;
                    }
                    if done_issuing {
                        if coordinating.is_empty() {
                            i_stopped = true;
                            stopped.fetch_add(1, Ordering::AcqRel);
                        } else {
                            std::thread::yield_now();
                        }
                        continue;
                    }
                    if shards > 1 && rng.below(8) == 0 {
                        // Cross-shard commit: reserve one seqno, hold it, hand shares out.
                        me.group = Some(shm.next_seqno());
                        me.publish(shm);
                        let seqno = shm.reserve_seqnos(1);
                        reserved.fetch_add(1, Ordering::Relaxed);
                        me.held.insert(seqno);
                        me.group = None;
                        me.publish(shm);
                        let mut mask = 1u64 << shard;
                        let mut others = Vec::new();
                        for other in 0..shards {
                            if other != shard && rng.below(2) == 0 {
                                mask |= 1 << other;
                                others.push(other);
                            }
                        }
                        if others.is_empty() {
                            let other = (shard + 1) % shards;
                            mask |= 1 << other;
                            others.push(other);
                        }
                        set_mask(shm, seqno, mask);
                        mark_applied(shm, shard, seqno);
                        let done = Arc::new(AtomicU32::new(0));
                        for other in &others {
                            senders[*other as usize]
                                .send(Share {
                                    seqno,
                                    done: done.clone(),
                                })
                                .unwrap();
                        }
                        coordinating.push((seqno, done, others.len() as u32));
                    } else {
                        // Single-shard group of 1..=4 commits.
                        let n = 1 + rng.below(4);
                        me.group = Some(shm.next_seqno());
                        me.publish(shm);
                        let first = shm.reserve_seqnos(n);
                        reserved.fetch_add(n as u32, Ordering::Relaxed);
                        me.group = Some(first);
                        me.publish(shm);
                        for seqno in first..first + n {
                            set_mask(shm, seqno, 1 << shard);
                            if rng.below(4) == 0 {
                                std::thread::yield_now();
                            }
                            mark_applied(shm, shard, seqno);
                            me.group = if seqno + 1 < first + n {
                                Some(seqno + 1)
                            } else {
                                None
                            };
                            me.publish(shm);
                        }
                    }
                    if rng.below(16) == 0 {
                        std::thread::yield_now();
                    }
                }
                // Idle: hold nothing back.
                assert!(me.held.is_empty());
                me.group = None;
                me.publish(shm);
            });
        }
    });
    u64::from(reserved.load(Ordering::Relaxed))
}

//! D324: what a direct stream writes. Its records go as whole aligned blocks, and a buffered
//! write never lands in a segment's record area: a direct write over a dirty buffered page
//! would make the kernel write that page back first (`filemap_write_and_wait_range`), the
//! wait D324 removes. The only buffered writes are a held segment header (#19), alone in the
//! segment's first frame (32 KiB, frame-aligned, so no record block shares its pages), and
//! spare slots' zero-fill, synced before a segment uses the slot (D157).

mod common;

use common::*;
use pigeonhole_format::Durability;
use pigeonhole_format::wal::FRAME_SIZE;
use pigeonhole_io::VfsRef;
use pigeonhole_io::sim::{SimOp, SimVfs};
use pigeonhole_wal::{Wal, WalStream};

/// The direct-I/O alignment the simulator enforces.
const ALIGN: u64 = 4096;

#[test]
fn a_direct_stream_writes_its_record_areas_only_in_aligned_blocks() {
    let frame = FRAME_SIZE as u64;
    let frames = 8;
    let segment = frames * frame;
    for seed in 0..8u64 {
        for deferred in [false, true] {
            let sim = SimVfs::new(seed);
            sim.set_deferred_io(deferred);
            let vfs: VfsRef = sim.clone();
            let mut o = opts(frames, 1);
            o.direct = true;
            let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, o).expect("create");
            assert!(
                wal.counters().direct(),
                "seed {seed}: the simulator takes direct I/O"
            );
            sim.record_ops();
            let mut seqno = 0;
            let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut ends = Vec::new();
            for _ in 0..400 {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                seqno += 1;
                let rec = batch(seqno, (rng % 6000) as usize);
                while wal.blocked() && sim.complete_io() {}
                let level = if rng % 3 == 0 {
                    Durability::GroupSync
                } else {
                    Durability::Buffered
                };
                let t = wal.append(&rec.record(), level).expect("append");
                ends.push(t.end);
                let c = if level == Durability::GroupSync || wal.header_held() {
                    wal.submit_sync().expect("sync")
                } else {
                    wal.submit_write().expect("write")
                };
                while !c.is_ready() && sim.complete_io() {}
                c.wait().expect("lands");
                assert!(wal.satisfies(&t), "seed {seed}: {t:?}");
                // Recycle slots, so segments land on stale ones too.
                if rng % 5 == 0 {
                    let cp = ends[ends.len() / 2];
                    if cp <= wal.durable() {
                        wal.checkpoint(cp).expect("checkpoint");
                    }
                }
            }
            while sim.complete_io() {}
            let node_writes: Vec<(u64, u64)> = sim
                .recorded_ops()
                .into_iter()
                .filter_map(|op| match op {
                    SimOp::Write { offset, len, .. } => Some((offset, len)),
                    _ => None,
                })
                .collect();
            let mut records = 0;
            let mut slots = std::collections::BTreeSet::new();
            for (offset, len) in node_writes {
                let in_slot = offset % segment;
                slots.insert(offset / segment);
                if in_slot + len <= frame {
                    continue; // within a header frame: a held header's own write
                }
                if len >= segment {
                    continue; // a spare slot's zero-fill, synced before use
                }
                records += 1;
                // A write reaching a record area (a segment the open started sends its header
                // and first records as one direct write). Alignment stands in for "went through
                // the direct handle": the simulator's recorded writes don't name their handle,
                // and a buffered write that happened to be block-aligned would pass. A buffered
                // record write (the `write_buf` path) frames records of any length, so it shows
                // here as misaligned.
                assert!(
                    offset % ALIGN == 0 && len % ALIGN == 0,
                    "seed {seed} deferred={deferred}: an unaligned (buffered) write at {offset} \
                     ({len} B) in a record area"
                );
            }
            assert!(records > 50, "seed {seed}: {records} record writes");
            assert!(slots.len() > 2, "seed {seed}: no rollover ({slots:?})");
        }
    }
}

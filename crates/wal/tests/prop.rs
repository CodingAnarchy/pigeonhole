//! Property tests: any sequence of records in any segment size replays exactly, from any
//! checkpoint, and a power loss at any point leaves an in-order prefix.

mod common;

use common::*;
use pigeonhole_format::wal::FRAME_SIZE;
use pigeonhole_format::{Durability, Lsn};
use pigeonhole_io::VfsRef;
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};
use pigeonhole_wal::{Wal, WalStream};
use proptest::collection::vec;
use proptest::prelude::*;

fn record_size() -> impl Strategy<Value = usize> {
    prop_oneof![
        4 => 0usize..200,
        1 => (FRAME_SIZE - 80)..(FRAME_SIZE + 80),
        // Up to three data frames less the fragment headers and the record's own overhead,
        // so it fits the smallest segment used (4 frames).
        1 => 1usize..(3 * FRAME_SIZE - 3 * 12 - 128),
    ]
}

/// (record size, durability index, write after this record?)
fn ops() -> impl Strategy<Value = Vec<(usize, u8, bool)>> {
    vec((record_size(), 0u8..3, any::<bool>()), 1..40)
}

fn config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|c| c.parse().ok())
        .unwrap_or(64);
    ProptestConfig {
        cases,
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn every_sequence_replays_exactly(ops in ops(), frames in 4u64..8, cp in any::<prop::sample::Index>()) {
        let vfs = sim(1);
        let o = opts(frames, 1);
        let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, o).unwrap();
        let mut tickets = Vec::new();
        let mut recs = Vec::new();
        for (i, (size, level, write)) in ops.iter().enumerate() {
            let rec = batch(i as u64 + 1, *size);
            let level = [Durability::Buffered, Durability::GroupSync, Durability::Sync][*level as usize];
            tickets.push(wal.append(&rec.record(), level).unwrap());
            recs.push(rec);
            if *write {
                wal.write().unwrap();
            }
        }
        wal.sync().unwrap();
        drop(wal);
        let cp = cp.index(tickets.len() + 1);
        let checkpoint = if cp == 0 { Lsn::default() } else { tickets[cp - 1].end };
        let (got, r) = replay(&vfs, checkpoint).unwrap();
        let want: Vec<_> = tickets[cp..].iter().zip(&recs[cp..]).map(|(t, r)| (t.end, r.bytes())).collect();
        prop_assert_eq!(&got.records, &want);
        prop_assert_eq!(got.end, tickets.last().unwrap().end);
        prop_assert_eq!(got.max_seqno, if cp < tickets.len() { tickets.len() as u64 } else { 0 });
        let wal = r.into_stream(o).unwrap();
        prop_assert!(wal.written().epoch() > got.end.epoch());
    }

    /// Power loss with torn, reordered unsynced writes after a random group: the synced
    /// prefix survives whole, what follows is an in-order prefix of the rest.
    #[test]
    fn power_loss_leaves_an_ordered_prefix(ops in ops(), frames in 4u64..8, synced_upto in any::<prop::sample::Index>(), seed in any::<u64>()) {
        let mut plan = FaultPlan::none();
        plan.torn_writes = true;
        plan.reorder_unsynced = true;
        let sim = SimVfs::with_faults(seed, plan);
        let vfs: VfsRef = sim.clone();
        let o = opts(frames, 1);
        let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, o).unwrap();
        let synced_upto = synced_upto.index(ops.len());
        let mut tickets = Vec::new();
        let mut recs = Vec::new();
        for (i, (size, _, write)) in ops.iter().enumerate() {
            let rec = batch(i as u64 + 1, *size);
            tickets.push(wal.append(&rec.record(), Durability::GroupSync).unwrap());
            recs.push(rec);
            if i == synced_upto {
                wal.sync().unwrap();
            } else if *write {
                wal.write().unwrap();
            }
        }
        wal.write().unwrap();
        sim.crash(CrashKind::Power);
        let (got, _) = replay(&vfs, Lsn::default()).unwrap();
        prop_assert!(got.records.len() > synced_upto, "seed {seed}: synced records lost");
        for ((end, bytes), (t, r)) in got.records.iter().zip(tickets.iter().zip(&recs)) {
            prop_assert_eq!(end, &t.end, "seed {}", seed);
            prop_assert_eq!(bytes, &r.bytes(), "seed {}", seed);
        }
        // The log ends at the last record, or at the start of an empty successor segment.
        let last = got.records.last().unwrap().0;
        prop_assert!(
            got.end == last || (got.end.epoch() > last.epoch() && got.end.offset() == FRAME_SIZE as u32),
            "seed {seed}: end {:?} vs last record {last:?}", got.end
        );
    }
}

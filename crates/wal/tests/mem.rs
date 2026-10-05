//! The in-memory mock keeps appended, written and synced records apart.

mod common;

use common::*;
use pigeonhole_format::{Durability, Lsn, StreamId};
use pigeonhole_wal::{MemWal, Wal};

#[test]
fn crash_keeps_written_or_synced_records() {
    let mut wal = MemWal::new(StreamId(2));
    assert_eq!(wal.stream(), StreamId(2));
    let t1 = wal
        .append(&batch(1, 10).record(), Durability::GroupSync)
        .unwrap();
    assert!(!wal.satisfies(&t1));
    let lsn = wal.submit_sync().unwrap().wait().unwrap();
    assert_eq!(lsn, t1.end);
    assert_eq!(wal.durable(), t1.end);
    assert!(wal.satisfies(&t1));

    let t2 = wal
        .append(&batch(2, 10).record(), Durability::Buffered)
        .unwrap();
    let t3 = wal
        .append(&batch(3, 10).record(), Durability::GroupSync)
        .unwrap();
    assert!(t1.end < t2.end && t2.end < t3.end);
    assert!(!wal.satisfies(&t2));
    assert_eq!(wal.write().unwrap(), t3.end);
    assert!(wal.satisfies(&t2));
    assert!(!wal.satisfies(&t3));
    let t4 = wal
        .append(&batch(4, 10).record(), Durability::None)
        .unwrap();
    assert!(wal.satisfies(&t4), "None is always met");
    let foreign = pigeonhole_wal::CommitTicket {
        stream: StreamId(5),
        ..t1
    };
    assert!(!wal.satisfies(&foreign));

    assert_eq!(wal.records().len(), 4);
    wal.crash(false);
    let survived: Vec<Lsn> = wal.records().iter().map(|(l, _)| *l).collect();
    assert_eq!(survived, [t1.end, t2.end, t3.end]);
    assert_eq!(wal.records()[1].1, batch(2, 10).bytes());
    assert!(
        wal.written().epoch() > t3.end.epoch(),
        "a crash starts a new epoch"
    );

    wal.crash(true);
    let survived: Vec<Lsn> = wal.records().iter().map(|(l, _)| *l).collect();
    assert_eq!(survived, [t1.end]);

    let t5 = wal
        .append(&batch(5, 10).record(), Durability::Sync)
        .unwrap();
    assert!(t5.end > t3.end);
    wal.sync().unwrap();
    wal.checkpoint(t1.end).unwrap();
    let survived: Vec<Lsn> = wal.records().iter().map(|(l, _)| *l).collect();
    assert_eq!(survived, [t5.end]);
    wal.crash(true);
    assert_eq!(wal.records().len(), 1);
    Box::new(wal).remove().unwrap();
    assert_eq!(MemWal::default().stream(), StreamId(0));
}

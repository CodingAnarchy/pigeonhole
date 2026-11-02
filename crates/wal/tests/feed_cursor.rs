//! The change feed's stream cursor (D310 PR 3): it follows a live stream across rollovers,
//! resumes from any record boundary, names each segment's feed session, and reports a
//! recycled segment as gone, never as a short log.

mod common;

use common::*;
use pigeonhole_format::wal::WalRecord;
use pigeonhole_format::{Durability, Lsn, Seqno};
use pigeonhole_wal::{FeedCursor, Wal, WalOptions, WalStream};

fn seqno_of(record: &WalRecord<'_>) -> Seqno {
    match record {
        WalRecord::Batch { seqno, .. } => *seqno,
        other => panic!("unexpected record {other:?}"),
    }
}

/// Reads until the stream's readable end: the seqnos and the last record's end, or `None`
/// if the cursor's segment is gone.
fn drain(cursor: &mut FeedCursor) -> Option<(Vec<Seqno>, Option<Lsn>)> {
    let mut got = Vec::new();
    let mut last = None;
    loop {
        match cursor.step().unwrap() {
            pigeonhole_wal::Step::Record { end, record, .. } => {
                got.push(seqno_of(&record));
                last = Some(end);
            }
            pigeonhole_wal::Step::End => return Some((got, last)),
            pigeonhole_wal::Step::Gone => return None,
        }
    }
}

fn append(wal: &mut WalStream, seqno: Seqno) -> Lsn {
    let t = wal
        .append(&batch(seqno, 20_000).record(), Durability::GroupSync)
        .unwrap();
    wal.sync().unwrap();
    t.end
}

fn fed(frames: u64, spare: u32, retain: u32) -> WalOptions {
    let mut o = opts(frames, spare);
    o.retain_segments = retain;
    o.feed_session = 0xfeed_5e55;
    o
}

#[test]
fn the_cursor_follows_a_live_stream_across_rollovers() {
    let vfs = sim(1);
    let o = fed(4, 2, 8);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, o).unwrap();
    wal.spares().prepare(2).unwrap();
    let mut cursor = FeedCursor::open(&vfs, db(), STREAM, DB_ID, Lsn::default()).unwrap();
    let mut seen = Vec::new();
    let mut seqno = 1;
    // A few records at a time, reading between: every record once, in order, as the
    // stream rolls over several segments (a 20 KB record per 32 KB frame, 4 frames each).
    for round in 0..10 {
        for _ in 0..3 {
            append(&mut wal, seqno);
            seqno += 1;
        }
        let (got, _) = drain(&mut cursor).expect("retained, never gone");
        seen.extend(got);
        assert_eq!(seen, (1..seqno).collect::<Vec<_>>(), "round {round}");
        assert_eq!(cursor.epoch(), wal.written().epoch(), "round {round}");
        wal.spares().prepare(2).unwrap();
    }
    assert!(wal.written().epoch() > 5, "the stream rolled over");
}

#[test]
fn the_cursor_resumes_from_a_record_boundary_and_names_the_session() {
    let vfs = sim(2);
    let o = fed(4, 2, 8);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, o).unwrap();
    wal.spares().prepare(2).unwrap();
    let mut ends = Vec::new();
    for s in 1..=12 {
        ends.push(append(&mut wal, s));
        wal.spares().prepare(2).unwrap();
    }
    // From after record 5, in another segment than the end: records 6..=12.
    let mut cursor = FeedCursor::open(&vfs, db(), STREAM, DB_ID, ends[4]).unwrap();
    assert_eq!(cursor.position(), ends[4]);
    let mut sessions = Vec::new();
    let mut got = Vec::new();
    while let pigeonhole_wal::Step::Record {
        start,
        end,
        session,
        record,
    } = cursor.step().unwrap()
    {
        let s = seqno_of(&record);
        // Each record's start is the previous one's end, or a segment's first record.
        assert!(start == ends[s as usize - 2] || start.epoch() > ends[s as usize - 2].epoch());
        assert_eq!(end, ends[s as usize - 1]);
        got.push(s);
        sessions.push(session);
    }
    assert_eq!(got, (6..=12).collect::<Vec<_>>());
    assert!(sessions.iter().all(|&s| s == 0xfeed_5e55));

    // Without the feed, segments carry no session.
    let vfs = sim(3);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts(4, 2)).unwrap();
    append(&mut wal, 1);
    let mut cursor = FeedCursor::open(&vfs, db(), STREAM, DB_ID, Lsn::default()).unwrap();
    assert!(matches!(
        cursor.step().unwrap(),
        pigeonhole_wal::Step::Record { session: 0, .. }
    ));
}

#[test]
fn a_recycled_segment_is_gone_never_a_short_log() {
    let vfs = sim(4);
    // No retention: a segment is recycled once the checkpoint passes it.
    let o = opts(4, 2);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, o).unwrap();
    wal.spares().prepare(2).unwrap();
    let first = append(&mut wal, 1);
    wal.checkpoint(first).unwrap();
    let mut cursor = FeedCursor::open(&vfs, db(), STREAM, DB_ID, Lsn::default()).unwrap();
    assert_eq!(drain(&mut cursor).unwrap().0, [1]);
    // The writer runs far ahead, checkpointing as it goes: the slot the cursor is in is
    // recycled several times.
    for s in 2..=40 {
        let end = append(&mut wal, s);
        wal.checkpoint(end).unwrap();
        wal.spares().prepare(2).unwrap();
    }
    // Whatever the cursor still reads of its own segment is that segment's records, then
    // it is gone: it never skips to records of another segment as if they followed.
    let mut got = Vec::new();
    let gone = loop {
        match cursor.step().unwrap() {
            pigeonhole_wal::Step::Record { record, .. } => got.push(seqno_of(&record)),
            pigeonhole_wal::Step::End => break false,
            pigeonhole_wal::Step::Gone => break true,
        }
    };
    assert!(gone, "read {got:?} then stopped short");
    assert!(
        got.windows(2).all(|w| w[1] == w[0] + 1) && got.first().is_none_or(|&s| s == 2),
        "{got:?}"
    );
    // A cursor opened at a position whose segment was recycled is gone at once.
    let mut cursor = FeedCursor::open(&vfs, db(), STREAM, DB_ID, first).unwrap();
    assert!(matches!(cursor.step().unwrap(), pigeonhole_wal::Step::Gone));
}

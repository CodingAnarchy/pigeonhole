//! The change feed's stream cursor (D310 PR 3): it follows a live stream across rollovers,
//! resumes from any record boundary, names each segment's feed session, and reports a
//! recycled segment as gone, never as a short log.

mod common;

use common::*;
use pigeonhole_format::wal::WalRecord;
use pigeonhole_format::{Durability, Lsn, Seqno};
use pigeonhole_wal::{FeedCursor, Recovery, Wal, WalOptions, WalStream};

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
            pigeonhole_wal::Step::Gone | pigeonhole_wal::Step::Cut => return None,
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
            pigeonhole_wal::Step::Cut => panic!("a recycled slot is gone, not cut"),
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

#[test]
fn the_cursor_stops_at_a_hole_where_recovery_did_then_follows_the_next_session() {
    // simp724's Q2 on #1316: a power loss kept a later frame of an unsynced segment and
    // lost an earlier one. Recovery stops at the hole and the next session writes its own
    // segment; the cursor must stop at the same place, never deliver the later frame, and
    // move on to the new session's segment.
    let vfs = sim(5);
    let o = fed(8, 2, 8);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, o).unwrap();
    let ends: Vec<Lsn> = (1..=4).map(|s| append(&mut wal, s)).collect();
    assert!(
        ends.iter().all(|e| e.epoch() == ends[0].epoch()),
        "one segment"
    );
    drop(wal);
    // The hole: record 3's first bytes, with record 4's frame intact after it.
    let slot = headers(&vfs, o.segment_size)
        .iter()
        .position(|h| h.as_ref().is_some_and(|h| h.epoch == ends[0].epoch()))
        .unwrap() as u64;
    let f = vfs
        .open(&path(), {
            let mut w = pigeonhole_io::OpenOptions::read();
            w.write = true;
            w
        })
        .unwrap();
    f.write_at(
        &[0xFF; 16],
        slot * o.segment_size + u64::from(ends[1].offset()) + 8,
    )
    .unwrap();
    f.sync_data().unwrap();
    drop(f);

    // Recovery's view, then the next session's own segment.
    let mut r = Recovery::open(&vfs, db(), STREAM, DB_ID, Lsn::default()).unwrap();
    let mut recovered = Vec::new();
    while let Some((_, rec)) = r.next_record().unwrap() {
        recovered.push(seqno_of(&rec));
    }
    assert_eq!(
        recovered,
        [1, 2],
        "recovery stops at the hole, before record 4's frame"
    );
    let mut next = fed(8, 2, 8);
    next.feed_session = 0x5e55_0002;
    let mut wal = r.into_stream(next).unwrap();
    let after: Vec<Seqno> = (10..=11).collect();
    for &s in &after {
        append(&mut wal, s);
    }

    let mut cursor = FeedCursor::open(&vfs, db(), STREAM, DB_ID, Lsn::default()).unwrap();
    let mut got = Vec::new();
    let mut sessions = Vec::new();
    loop {
        match cursor.step().unwrap() {
            pigeonhole_wal::Step::Record {
                session, record, ..
            } => {
                got.push(seqno_of(&record));
                sessions.push(session);
            }
            pigeonhole_wal::Step::End => break,
            pigeonhole_wal::Step::Gone | pigeonhole_wal::Step::Cut => {
                panic!("retained and intact, never gone or cut")
            }
        }
    }
    let mut want = recovered.clone();
    want.extend(&after);
    assert_eq!(
        got, want,
        "the cursor reads what recovery kept, then the next session"
    );
    assert_eq!(sessions.last(), Some(&0x5e55_0002));
    assert_eq!(cursor.epoch(), wal.written().epoch());
}

#[test]
fn a_cursor_past_where_recovery_cut_its_segment_is_cut_never_corrupt() {
    // A power loss took records a cursor had already read (a visible feed delivered them):
    // the next session's recovery stops before them and its segment names that earlier end.
    // The cursor, live or reopened at its old position, reports `Cut` (an expiry), never a
    // chain corruption (#1248: a token resumed after a power loss failed with "predecessor
    // end mismatch").
    let vfs = sim(6);
    let o = fed(8, 2, 8);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, o).unwrap();
    let ends: Vec<Lsn> = (1..=4).map(|s| append(&mut wal, s)).collect();
    drop(wal);
    // A live cursor reads all four.
    let mut live = FeedCursor::open(&vfs, db(), STREAM, DB_ID, Lsn::default()).unwrap();
    assert_eq!(drain(&mut live).unwrap().0, [1, 2, 3, 4]);
    // The loss: record 3 damaged, so recovery keeps [1, 2].
    let slot = headers(&vfs, o.segment_size)
        .iter()
        .position(|h| h.as_ref().is_some_and(|h| h.epoch == ends[0].epoch()))
        .unwrap() as u64;
    let f = vfs
        .open(&path(), {
            let mut w = pigeonhole_io::OpenOptions::read();
            w.write = true;
            w
        })
        .unwrap();
    f.write_at(
        &[0xFF; 16],
        slot * o.segment_size + u64::from(ends[1].offset()) + 8,
    )
    .unwrap();
    f.sync_data().unwrap();
    drop(f);
    let mut r = Recovery::open(&vfs, db(), STREAM, DB_ID, Lsn::default()).unwrap();
    let mut recovered = Vec::new();
    while let Some((_, rec)) = r.next_record().unwrap() {
        recovered.push(seqno_of(&rec));
    }
    assert_eq!(recovered, [1, 2]);
    let mut next = fed(8, 2, 8);
    next.feed_session = 0x5e55_0003;
    let mut wal = r.into_stream(next).unwrap();
    append(&mut wal, 10);

    // The live cursor, past the cut: `Cut`.
    assert!(
        matches!(live.step().unwrap(), pigeonhole_wal::Step::Cut),
        "a cursor past the cut is cut"
    );
    // Reopened where it was (a token's position): `Cut` too.
    let mut resumed = FeedCursor::open(&vfs, db(), STREAM, DB_ID, ends[3]).unwrap();
    let step = loop {
        match resumed.step().unwrap() {
            pigeonhole_wal::Step::Record { .. } => continue,
            other => break format!("{other:?}"),
        }
    };
    assert_eq!(step, "Cut");
    // A cursor at or before the cut follows the next session.
    let mut before = FeedCursor::open(&vfs, db(), STREAM, DB_ID, ends[1]).unwrap();
    assert_eq!(drain(&mut before).unwrap().0, [10]);
}

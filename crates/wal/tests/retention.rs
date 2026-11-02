//! Change-feed retention (D310): the slot life cycle live → retained → recycled, the age
//! trim, the never-stall fallback, adoption at reopen, and the feed session in each header.

mod common;

use common::*;
use pigeonhole_format::{Durability, Lsn};
use pigeonhole_wal::{Wal, WalOptions, WalStream};

/// `(floor, retained bytes)` from the stream's feed marks.
fn history(wal: &WalStream) -> (u32, u64) {
    let m = wal.feed_marks().unwrap();
    (m.floor(), m.retained_bytes())
}

fn fed(frames: u64, spare: u32, retain: u32) -> WalOptions {
    let mut o = opts(frames, spare);
    o.retain_segments = retain;
    o.feed_session = 0x5e55_1011;
    o
}

/// Appends `n` records from `first`, syncing each and checkpointing to its end, with spares
/// prepared after each when `prepare` says so. Returns the next seqno.
fn run(wal: &mut WalStream, first: u64, n: u64, prepare: bool) -> u64 {
    for i in first..first + n {
        let t = wal
            .append(&batch(i, 20_000).record(), Durability::GroupSync)
            .unwrap();
        wal.sync().unwrap();
        wal.checkpoint(t.end).unwrap();
        if prepare {
            wal.spares().prepare(wal.spares().target()).unwrap();
        }
    }
    first + n
}

#[test]
fn retention_deepens_the_ring_then_recycles_it() {
    let vfs = sim(1);
    let o = fed(4, 2, 3);
    let seg = o.segment_size;
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, o).unwrap();
    wal.spares().prepare(2).unwrap();
    let next = run(&mut wal, 1, 40, true);
    // Without retention the ring is 3 slots (passed, current, spare; see `stream.rs`); with
    // 3 retained it is 3 deeper, and the steady state recycles: the file stops growing.
    let settled = file_len(&vfs);
    assert_eq!(settled, 6 * seg);
    run(&mut wal, next, 60, true);
    assert_eq!(file_len(&vfs), settled, "the steady state recycles");
    assert_eq!(wal.inline_grows(), 0);
    assert_eq!(wal.counters().retained_taken(), 0, "spares kept up");
    let (oldest, retained) = history(&wal);
    assert_eq!(retained, 3 * seg);
    let current = wal.written().epoch();
    // The three retained segments are the three before the current one.
    assert_eq!(oldest, current - 3);
    assert_eq!(wal.feed_marks().unwrap().head(), current);
    let hs = headers(&vfs, seg);
    let mut epochs: Vec<u32> = hs.iter().flatten().map(|h| h.epoch).collect();
    epochs.sort_unstable();
    assert!(epochs.ends_with(&[current - 3, current - 2, current - 1, current]));
    assert!(
        hs.iter().flatten().all(|h| h.feed_session == 0x5e55_1011),
        "every header names the feed session"
    );
}

#[test]
fn without_retention_nothing_is_retained() {
    let vfs = sim(2);
    let o = opts(4, 2);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, o).unwrap();
    wal.spares().prepare(2).unwrap();
    run(&mut wal, 1, 40, true);
    assert_eq!(file_len(&vfs), 3 * o.segment_size);
    // Without the feed the marks' floor and retained bytes aren't kept.
    assert_eq!(history(&wal), (0, 0));
    let hs = headers(&vfs, o.segment_size);
    assert!(hs.iter().flatten().all(|h| h.feed_session == 0));
}

#[test]
fn the_age_trim_drops_old_retained_segments() {
    let vfs = sim(3);
    let o = fed(4, 2, 100);
    let seg = o.segment_size;
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, o).unwrap();
    wal.spares().prepare(2).unwrap();
    let next = run(&mut wal, 1, 20, true);
    wal.trim_retained(1_000, Some(500));
    let (_, before) = history(&wal);
    assert!(before >= 4 * seg, "{before}");
    // Not old enough yet.
    wal.trim_retained(1_400, Some(500));
    assert_eq!(history(&wal).1, before);
    // Every segment stamped at 1000 is now older than the age.
    wal.trim_retained(1_501, Some(500));
    assert_eq!(history(&wal).1, 0);
    // With a trim between records and time moving faster than the age, at most the
    // segments sealed since the last trim stay retained: the ring stays shallow and the file
    // stops growing.
    let len = file_len(&vfs);
    let mut now = 2_000;
    for i in next..next + 40 {
        let t = wal
            .append(&batch(i, 20_000).record(), Durability::GroupSync)
            .unwrap();
        wal.sync().unwrap();
        wal.checkpoint(t.end).unwrap();
        wal.trim_retained(now, Some(500));
        wal.spares().prepare(2).unwrap();
        now += 1_000;
        assert!(history(&wal).1 <= seg, "record {i}");
    }
    assert_eq!(file_len(&vfs), len);
    assert_eq!(wal.inline_grows(), 0);
}

#[test]
fn retention_never_stalls_a_rollover_without_spares() {
    let vfs = sim(4);
    let o = fed(4, 2, 3);
    let seg = o.segment_size;
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, o).unwrap();
    // No spare is ever prepared: the first rollovers grow inline, then, once segments are
    // retained, a rollover takes the oldest retained slot instead of growing or waiting.
    run(&mut wal, 1, 60, false);
    assert!(wal.counters().retained_taken() > 0);
    let len = file_len(&vfs);
    run(&mut wal, 61, 60, false);
    assert_eq!(file_len(&vfs), len, "no growth once history is taken");
    assert!(len <= 5 * seg, "{len}");
    // Near a segment's end the stream is not blocked: a retained slot is ready.
    assert!(!wal.blocked());
}

#[test]
fn a_reopen_adopts_retained_segments_without_replaying_them() {
    let vfs = sim(5);
    let o = fed(4, 2, 3);
    let seg = o.segment_size;
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, o).unwrap();
    wal.spares().prepare(2).unwrap();
    run(&mut wal, 1, 40, true);
    // A clean close: everything checkpointed to the end.
    let end = wal.written();
    wal.checkpoint(end).unwrap();
    let (_, kept) = history(&wal);
    assert_eq!(kept, 3 * seg);
    drop(wal);

    let (got, r) = replay(&vfs, end).unwrap();
    assert!(
        got.records.is_empty(),
        "nothing past the checkpoint replays"
    );
    let mut wal = r.into_stream(o).unwrap();
    // The open's segment took a new slot, never a retained one.
    assert_eq!(wal.counters().retained_taken(), 0);
    assert_eq!(history(&wal).1, kept);
    let first = wal.written();
    assert!(first.epoch() >= end.epoch() + 3);
    // Once the checkpoint passes the old session's last segment, it is retained and the
    // oldest of the old ones becomes recyclable: still three.
    wal.checkpoint(Lsn::new(first.epoch(), first.offset()))
        .unwrap();
    assert_eq!(history(&wal).1, 3 * seg);
    assert_eq!(history(&wal).0, end.epoch() - 2);
}

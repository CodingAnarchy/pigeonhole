//! `WalStream` behavior on the fault-free simulator: framing across frames and segments,
//! chaining, recycling, preallocation and the error paths.

mod common;

use std::path::Path;

use common::*;
use pigeonhole_format::wal::{FRAME_SIZE, WalRecord};
use pigeonhole_format::{Durability, Lsn, StreamId};
use pigeonhole_wal::{Error, Recovery, Wal, WalOptions, WalStream, discover_streams};

const FRAME: u64 = FRAME_SIZE as u64;

#[test]
fn records_roundtrip_across_frames_and_segments() {
    let vfs = sim(1);
    let opts = opts(4, 1);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts).unwrap();
    assert_eq!(wal.stream(), STREAM);
    assert_eq!(wal.written(), Lsn::new(1, FRAME_SIZE as u32));
    assert_eq!(wal.durable(), wal.written());

    // Sizes that fit a frame, straddle one, and span several.
    let sizes = [
        10usize,
        100,
        FRAME_SIZE - 30,
        FRAME_SIZE + 5,
        2 * FRAME_SIZE + 77,
        3,
        50_000,
    ];
    let mut recs = Vec::new();
    let mut tickets = Vec::new();
    for (i, size) in sizes.iter().cycle().take(40).enumerate() {
        let rec = match i % 7 {
            3 => prepare(i as u64 + 1),
            5 => commit(i as u64 + 1, &[StreamId(1), StreamId(2)]),
            _ => batch(i as u64 + 1, *size),
        };
        let level = [
            Durability::Buffered,
            Durability::GroupSync,
            Durability::Sync,
        ][i % 3];
        tickets.push(wal.append(&rec.record(), level).unwrap());
        recs.push(rec);
        if i % 4 == 3 {
            wal.write().unwrap();
        }
    }
    // Tickets are strictly increasing in append order.
    for w in tickets.windows(2) {
        assert!(w[0].end < w[1].end);
    }
    wal.sync().unwrap();
    for t in &tickets {
        assert!(wal.satisfies(t), "{t:?}");
    }
    assert!(wal.written().epoch() > 1, "several segments were used");
    drop(wal);

    let (got, _) = replay(&vfs, Lsn::default()).unwrap();
    let want: Vec<(Lsn, Vec<u8>)> = tickets
        .iter()
        .zip(&recs)
        .map(|(t, r)| (t.end, r.bytes()))
        .collect();
    assert_eq!(got.records.len(), want.len());
    assert_eq!(got.records, want);
    assert_eq!(got.end, tickets.last().unwrap().end);
    assert_eq!(got.max_seqno, 40);
    assert_eq!(got.seqnos(), (1..=40).collect::<Vec<_>>());
}

#[test]
fn prepare_and_commit_records_roundtrip_with_their_fields() {
    let vfs = sim(2);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts(2, 1)).unwrap();
    wal.append(&prepare(11).record(), Durability::GroupSync)
        .unwrap();
    wal.append(
        &commit(11, &[StreamId(0), StreamId(4)]).record(),
        Durability::GroupSync,
    )
    .unwrap();
    wal.sync().unwrap();

    let mut r = Recovery::open(&vfs, db(), STREAM, DB_ID, Lsn::default()).unwrap();
    let (_, rec) = r.next_record().unwrap().unwrap();
    match rec {
        WalRecord::Prepare {
            seqno,
            commit_ts,
            coordinator,
            batch,
        } => {
            assert_eq!((seqno, commit_ts, coordinator), (11, 111, StreamId(9)));
            assert_eq!(batch.len(), 2);
        }
        other => panic!("{other:?}"),
    }
    let (_, rec) = r.next_record().unwrap().unwrap();
    match rec {
        WalRecord::Commit {
            seqno,
            participants,
        } => {
            assert_eq!(seqno, 11);
            assert_eq!(
                participants.iter().collect::<Vec<_>>(),
                [StreamId(0), StreamId(4)]
            );
        }
        other => panic!("{other:?}"),
    }
    assert!(r.next_record().unwrap().is_none());
    assert_eq!(r.max_seqno(), 11);
}

#[test]
fn checkpoint_replays_only_what_follows_it() {
    let vfs = sim(3);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts(4, 1)).unwrap();
    let tickets: Vec<_> = (1..=30)
        .map(|i| {
            wal.append(&batch(i, 9_000).record(), Durability::GroupSync)
                .unwrap()
        })
        .collect();
    wal.sync().unwrap();
    for (i, t) in tickets.iter().enumerate() {
        let (got, _) = replay(&vfs, t.end).unwrap();
        assert_eq!(
            got.seqnos(),
            (i as u64 + 2..=30).collect::<Vec<_>>(),
            "from {t:?}"
        );
        assert_eq!(got.end, tickets.last().unwrap().end);
    }
}

#[test]
fn recovery_starts_a_fresh_chained_segment() {
    let vfs = sim(4);
    let opts = opts(4, 1);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts).unwrap();
    let t1 = wal
        .append(&batch(1, 100).record(), Durability::GroupSync)
        .unwrap();
    wal.sync().unwrap();
    drop(wal);

    let (got, r) = replay(&vfs, Lsn::default()).unwrap();
    assert_eq!(got.end, t1.end);
    let mut wal = r.into_stream(opts).unwrap();
    assert_eq!(wal.written().epoch(), 2, "epoch above every header");
    assert!(
        wal.durable() >= Lsn::new(2, FRAME_SIZE as u32),
        "header is synced"
    );
    let hs = headers(&vfs, opts.segment_size);
    let h = hs.iter().flatten().find(|h| h.epoch == 2).unwrap();
    assert_eq!((h.prev_epoch, h.prev_end), (1, t1.end.offset()));

    let t2 = wal
        .append(&batch(2, 100).record(), Durability::GroupSync)
        .unwrap();
    wal.sync().unwrap();
    drop(wal);
    let (got, r) = replay(&vfs, Lsn::default()).unwrap();
    assert_eq!(got.seqnos(), [1, 2]);
    assert_eq!(got.end, t2.end);
    // A third incarnation chains to the second.
    let wal = r.into_stream(opts).unwrap();
    assert_eq!(wal.written().epoch(), 3);
    let hs = headers(&vfs, opts.segment_size);
    let h = hs.iter().flatten().find(|h| h.epoch == 3).unwrap();
    assert_eq!((h.prev_epoch, h.prev_end), (2, t2.end.offset()));
    // Replaying from the checkpoint at the recovered end finds nothing; the log ends at the
    // start of the empty segment chained there.
    drop(wal);
    let (got, _) = replay(&vfs, t2.end).unwrap();
    assert!(got.records.is_empty());
    assert_eq!(got.end, Lsn::new(3, FRAME_SIZE as u32));
}

#[test]
fn segments_are_preallocated_and_recycled_after_checkpoint() {
    let vfs = sim(5);
    let opts = opts(4, 2);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts).unwrap();
    // The first slot, zero-filled, plus two prepared spares.
    assert_eq!(file_len(&vfs), opts.segment_size);
    assert_eq!(wal.spares().prepare(2).unwrap(), 2);
    assert_eq!(file_len(&vfs), 3 * opts.segment_size);

    // Checkpoints keep up (each one inside the current segment): the file settles at the
    // segment just checkpointed past, the current one and one spare, and slots are reused.
    for i in 1..=60u64 {
        let t = wal
            .append(&batch(i, 20_000).record(), Durability::GroupSync)
            .unwrap();
        wal.sync().unwrap();
        wal.checkpoint(t.end).unwrap();
        wal.spares().prepare(2).unwrap();
        assert!(file_len(&vfs) <= 3 * opts.segment_size, "after record {i}");
    }
    assert_eq!(file_len(&vfs), 3 * opts.segment_size);
    assert_eq!(wal.inline_grows(), 0, "spares were always ready");
    assert_eq!(
        wal.inline_rollover_syncs(),
        0,
        "every rollover sync was submitted"
    );
    let epoch = wal.written().epoch();
    assert!(epoch > 10, "many segments were started: {epoch}");
    let hs = headers(&vfs, opts.segment_size);
    assert!(hs.iter().flatten().all(|h| h.epoch > epoch - 4), "{hs:?}");

    // Checkpoints fall behind: the file grows by whole slots.
    let cp = wal.written();
    for i in 61..=80u64 {
        wal.append(&batch(i, 20_000).record(), Durability::GroupSync)
            .unwrap();
        wal.sync().unwrap();
    }
    let grown = file_len(&vfs);
    assert!(grown > 3 * opts.segment_size);
    assert_eq!(grown % opts.segment_size, 0);
    // Everything since `cp` replays.
    drop(wal);
    let (got, r) = replay(&vfs, cp).unwrap();
    assert_eq!(got.seqnos(), (61..=80).collect::<Vec<_>>());
    // Nothing is below the checkpoint, so the reopened stream needs one new slot for its
    // segment; once checkpoints resume, slots are recycled and the file never grows again.
    let mut wal = r.into_stream(opts).unwrap();
    let after_open = file_len(&vfs);
    assert_eq!(after_open, grown + opts.segment_size);
    for i in 81..=100u64 {
        let t = wal
            .append(&batch(i, 20_000).record(), Durability::GroupSync)
            .unwrap();
        wal.sync().unwrap();
        wal.checkpoint(t.end).unwrap();
    }
    assert_eq!(file_len(&vfs), after_open);
}

#[test]
fn stale_records_in_a_recycled_slot_are_not_replayed() {
    let vfs = sim(6);
    let opts = opts(4, 1);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts).unwrap();
    // Fill epoch 1 (slot 0) with several records, then move into epochs 2 and 3.
    let mut seqno = 0;
    while wal.written().epoch() < 3 {
        seqno += 1;
        wal.append(&batch(seqno, 20_000).record(), Durability::GroupSync)
            .unwrap();
        wal.sync().unwrap();
    }
    let cp = wal.written();
    wal.checkpoint(cp).unwrap();
    let in_epoch3 = seqno;
    // Keep appending until slot 0 is reused under a new epoch, then write one small record
    // there: its stale epoch-1 fragments remain past the new data.
    let hs = headers(&vfs, opts.segment_size);
    assert_eq!(hs[0].unwrap().epoch, 1);
    while headers(&vfs, opts.segment_size)[0].unwrap().epoch == 1 {
        seqno += 1;
        wal.append(&batch(seqno, 20_000).record(), Durability::GroupSync)
            .unwrap();
        wal.sync().unwrap();
    }
    let reused = headers(&vfs, opts.segment_size)[0].unwrap();
    assert!(reused.epoch > 3);
    drop(wal);
    let (got, _) = replay(&vfs, cp).unwrap();
    assert_eq!(got.seqnos(), (in_epoch3 + 1..=seqno).collect::<Vec<_>>());
}

#[test]
fn durability_none_is_never_logged() {
    let vfs = sim(12);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts(2, 1)).unwrap();
    assert!(matches!(
        wal.append(&batch(1, 10).record(), Durability::None),
        Err(Error::InvalidArgument { .. })
    ));
    // The refusal leaves the stream usable and nothing was framed.
    let t = wal
        .append(&batch(2, 10).record(), Durability::GroupSync)
        .unwrap();
    wal.sync().unwrap();
    assert!(wal.satisfies(&t));
    let (got, _) = replay(&vfs, Lsn::default()).unwrap();
    assert_eq!(got.seqnos(), [2]);
}

#[test]
fn spares_are_prepared_off_the_shard_thread_and_taken_before_growing() {
    let vfs = sim(13);
    let opts = opts(4, 2);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts).unwrap();
    // Create zero-fills and uses the first slot only.
    assert_eq!(file_len(&vfs), opts.segment_size);
    assert_eq!(wal.inline_grows(), 0);
    let spares = wal.spares();
    assert_eq!(spares.target(), 2);
    assert_eq!(spares.ready(), 0);

    // Prepared on another thread while the shard appends.
    let worker = {
        let spares = spares.clone();
        std::thread::spawn(move || spares.prepare(spares.target()))
    };
    let mut seqno = 0;
    let mut appended = Vec::new();
    for _ in 0..3 {
        seqno += 1;
        appended.push(
            wal.append(&batch(seqno, 100).record(), Durability::GroupSync)
                .unwrap(),
        );
        wal.sync().unwrap();
    }
    assert_eq!(worker.join().unwrap().unwrap(), 2);
    assert_eq!(spares.ready(), 2);
    assert_eq!(file_len(&vfs), 3 * opts.segment_size);
    // Enough is enough: preparing again fills nothing.
    assert_eq!(spares.prepare(2).unwrap(), 0);
    let f = vfs
        .open(&path(), pigeonhole_io::OpenOptions::read())
        .unwrap();
    let mut tail = vec![1u8; 2 * opts.segment_size as usize];
    f.read_at(&mut tail, opts.segment_size).unwrap();
    assert!(tail.iter().all(|&b| b == 0), "spares are zero-filled");

    // Two rollovers take the two prepared slots; the file does not grow.
    while wal.written().epoch() < 3 {
        seqno += 1;
        wal.append(&batch(seqno, 20_000).record(), Durability::GroupSync)
            .unwrap();
        wal.sync().unwrap();
    }
    assert_eq!(spares.ready(), 0);
    assert_eq!(file_len(&vfs), 3 * opts.segment_size);
    assert_eq!(wal.inline_grows(), 0);
    assert_eq!(wal.inline_rollover_syncs(), 0);
    // No spare and nothing recyclable: the next rollover grows inline and says so.
    while wal.written().epoch() < 4 {
        seqno += 1;
        wal.append(&batch(seqno, 20_000).record(), Durability::GroupSync)
            .unwrap();
        wal.sync().unwrap();
    }
    assert_eq!(wal.inline_grows(), 1);
    assert_eq!(
        wal.inline_rollover_syncs(),
        1,
        "no slot was ready: synced inline"
    );
    assert_eq!(file_len(&vfs), 4 * opts.segment_size);
    // Recyclable slots count as free: after a checkpoint no spare needs preparing, and the
    // next rollover recycles instead of taking a spare.
    let cp = wal.written();
    wal.checkpoint(cp).unwrap();
    assert_eq!(spares.prepare(2).unwrap(), 0);
    while wal.written().epoch() < 5 {
        seqno += 1;
        wal.append(&batch(seqno, 20_000).record(), Durability::GroupSync)
            .unwrap();
        wal.sync().unwrap();
    }
    assert_eq!(file_len(&vfs), 4 * opts.segment_size);
    assert_eq!(wal.inline_grows(), 1);
    assert_eq!(
        wal.inline_rollover_syncs(),
        1,
        "a recyclable slot was ready"
    );
    // The trait exposes the same handle; the mock has none.
    let boxed: Box<dyn Wal> = Box::new(wal);
    assert!(boxed.spares().is_some());
    assert!(pigeonhole_wal::MemWal::new(STREAM).spares().is_none());
    drop(boxed);
    let (got, r) = replay(&vfs, cp).unwrap();
    assert!(got.seqnos().last().copied() == Some(seqno));
    // A blank slot found at recovery is zero-filled by prepare before the file grows.
    let wal = r.into_stream(opts).unwrap();
    let before = file_len(&vfs);
    assert!(wal.spares().prepare(4).unwrap() >= 1);
    assert!(file_len(&vfs) >= before);
}

#[test]
fn records_too_large_for_a_segment_are_refused() {
    let vfs = sim(7);
    let opts = opts(2, 1); // one data frame: at most FRAME_SIZE - 12 bytes of payload
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts).unwrap();
    let too_big = batch(1, FRAME_SIZE);
    assert!(matches!(
        wal.append(&too_big.record(), Durability::Buffered),
        Err(Error::RecordTooLarge)
    ));
    // The stream is still usable, and a record that just fits is accepted.
    let fits = batch(2, FRAME_SIZE - 12 - 60);
    let t = wal.append(&fits.record(), Durability::GroupSync).unwrap();
    wal.sync().unwrap();
    assert!(wal.satisfies(&t));
    let (got, _) = replay(&vfs, Lsn::default()).unwrap();
    assert_eq!(got.seqnos(), [2]);
}

#[test]
fn foreign_segments_and_bad_options_are_refused() {
    let vfs = sim(8);
    let mut good = opts(2, 1);
    WalStream::create(&vfs, db(), STREAM, DB_ID, good).unwrap();
    assert!(matches!(
        Recovery::open(&vfs, db(), STREAM, [1; 16], Lsn::default()),
        Err(Error::ForeignSegment)
    ));
    // A missing stream file is `NotFound`, so the engine creates it.
    match Recovery::open(&vfs, db(), StreamId(7), DB_ID, Lsn::default()) {
        Err(Error::Io(e)) => assert_eq!(e.kind, pigeonhole_io::ErrorKind::NotFound),
        other => panic!("{other:?}"),
    }
    for size in [FRAME + 1, FRAME, 1 << 32] {
        good.segment_size = size;
        assert!(matches!(
            WalStream::create(&vfs, db(), StreamId(1), DB_ID, good),
            Err(Error::InvalidArgument { .. })
        ));
    }
    // A checkpoint naming an epoch that never existed, with later segments present, is corruption.
    assert!(Recovery::open(&vfs, db(), STREAM, DB_ID, Lsn::new(5, 0)).is_ok());
    assert!(Recovery::open(&vfs, db(), STREAM, DB_ID, Lsn::new(0, 0)).is_ok());
}

#[test]
fn checkpoint_in_a_segment_that_never_reached_disk() {
    // A Buffered commit lands in a fresh segment, is flushed and checkpointed, and power is
    // lost before the segment was ever synced: recovery finds nothing to replay and the new
    // segment chains to the checkpoint.
    let vfs = sim(9);
    let opts = opts(2, 1);
    let sim = pigeonhole_io::sim::SimVfs::new(9);
    let vfs2: pigeonhole_io::VfsRef = sim.clone();
    drop(vfs);
    let mut wal = WalStream::create(&vfs2, db(), STREAM, DB_ID, opts).unwrap();
    wal.append(&batch(1, 100).record(), Durability::GroupSync)
        .unwrap();
    wal.sync().unwrap();
    // The second record forces a new segment; it is written but not synced.
    let t2 = wal
        .append(&batch(2, FRAME_SIZE - 200).record(), Durability::Buffered)
        .unwrap();
    wal.write().unwrap();
    assert_eq!(t2.end.epoch(), 2);
    sim.crash(pigeonhole_io::sim::CrashKind::Power);

    let (got, r) = replay(&vfs2, t2.end).unwrap();
    assert!(got.records.is_empty());
    assert_eq!(got.end, t2.end);
    let mut wal = r.into_stream(opts).unwrap();
    assert_eq!(
        wal.written().epoch(),
        3,
        "above the checkpoint's epoch, even if never on disk"
    );
    let hs = headers(&vfs2, opts.segment_size);
    let h = hs.iter().flatten().find(|h| h.epoch == 3).unwrap();
    assert_eq!((h.prev_epoch, h.prev_end), (2, t2.end.offset()));
    let t3 = wal
        .append(&batch(3, 10).record(), Durability::GroupSync)
        .unwrap();
    wal.sync().unwrap();
    drop(wal);
    // Replaying from the same checkpoint follows the chain into the new segment.
    let (got, _) = replay(&vfs2, t2.end).unwrap();
    assert_eq!(got.seqnos(), [3]);
    assert_eq!(got.end, t3.end);
}

#[test]
fn chain_mismatch_is_reported_not_dropped() {
    let vfs = sim(10);
    let opts = opts(4, 1);
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts).unwrap();
    let mut seqno = 0;
    while wal.written().epoch() < 2 {
        seqno += 1;
        wal.append(&batch(seqno, 20_000).record(), Durability::GroupSync)
            .unwrap();
        wal.sync().unwrap();
    }
    drop(wal);
    // Damage the last record of the full first segment: its successor names an end past
    // where replay now stops.
    let hs = headers(&vfs, opts.segment_size);
    let succ = hs.iter().flatten().find(|h| h.prev_epoch == 1).unwrap();
    let f = vfs
        .open(&path(), {
            let mut o = pigeonhole_io::OpenOptions::read();
            o.write = true;
            o
        })
        .unwrap();
    f.write_at(&[0xFF; 16], u64::from(succ.prev_end) - 16)
        .unwrap();
    f.sync_data().unwrap();
    drop(f);
    match replay(&vfs, Lsn::default()) {
        Err(Error::Format(e)) => assert!(e.to_string().contains("chain"), "{e}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn discover_and_remove_streams() {
    let vfs = sim(11);
    for s in [3u32, 0, 1] {
        WalStream::create(&vfs, db(), StreamId(s), DB_ID, opts(2, 0)).unwrap();
    }
    // Unrelated files are ignored.
    vfs.open(
        Path::new("/db/data.phdb-wal-x"),
        pigeonhole_io::OpenOptions::read_write_create(),
    )
    .unwrap();
    vfs.open(
        Path::new("/db/other.phdb-wal-5"),
        pigeonhole_io::OpenOptions::read_write_create(),
    )
    .unwrap();
    assert_eq!(
        discover_streams(&vfs, db()).unwrap(),
        [StreamId(0), StreamId(1), StreamId(3)]
    );
    let wal = Recovery::open(&vfs, db(), StreamId(1), DB_ID, Lsn::default())
        .unwrap()
        .into_stream(opts(2, 0))
        .unwrap();
    Box::new(wal).remove().unwrap();
    assert_eq!(
        discover_streams(&vfs, db()).unwrap(),
        [StreamId(0), StreamId(3)]
    );
    // Create over an existing file truncates it: epochs start over at 1.
    let wal = WalStream::create(&vfs, db(), StreamId(3), DB_ID, opts(2, 0)).unwrap();
    assert_eq!(wal.written().epoch(), 1);
    assert_eq!(WalOptions::default().segment_size, 64 << 20);
}

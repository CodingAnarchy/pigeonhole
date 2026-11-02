//! The change feed's M1 depends on D200 (D310 PR 4): a rollover's successor header waits for
//! its predecessor's sync (FORMAT §10.1 rule 1) while its records are written, and D200 keeps
//! their groups hidden until a sync writes that header. The feed's frontier settles on the
//! same `Wal::written`, so it never passes a record behind a held header, and a feed that
//! reads every stream to its readable end has read everything at or below it. No head mark
//! is needed: this test pins the invariant instead. If D200 ever showed such a group early,
//! the feed would deliver a later commit past the held segment and this test would fail.
//! Here the sync is held for real (the gate VFS), and the shard confirms its header is held
//! (a test hook), so the test can't pass vacuously.

use crate::gate;

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, FeedOptions, FeedSpec, FeedStart, Next,
    TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::{Durability, Seqno};
use pigeonhole_io::VfsRef;

const DB: &str = "/db/head.phdb";

fn options(vfs: VfsRef) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.compaction_threads = 0;
    o.memtable_budget = 16 << 20;
    o.tablet_changes = false;
    // Small segments, so a few commits roll stream 0 over.
    o.wal.segment_size = 4 * 32 * 1024;
    o.wal.spare_segments = 2;
    let mut f = FeedOptions::default();
    f.retention_bytes = 16 << 20;
    o.feed = Some(f);
    o
}

fn wal(stream: u32) -> PathBuf {
    pigeonhole_wal::stream_path(Path::new(DB), pigeonhole_format::StreamId(stream))
}

fn batch(t: &TableInfo, row: &[u8], value: &[u8]) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row,
        b"q",
        None,
        ValueRef::Bytes(value),
    )
    .unwrap();
    wb
}

fn settle(shards: &mut [EngineShard]) {
    for _ in 0..10_000 {
        let mut more = false;
        for s in shards.iter_mut() {
            more |= s.run_once(u64::MAX);
        }
        if !more {
            return;
        }
    }
}

#[test]
fn the_frontier_never_passes_a_record_behind_a_held_segment_header() {
    let (vfs, gate) = gate::vfs(4201);
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options(vfs)).unwrap();
    let f = || vec![("f".into(), FamilyOptions::default())];
    let a = (*db.create_table("a", &f()).unwrap()).clone();
    let b = (*db.create_table("b", &f()).unwrap()).clone();
    settle(&mut shards);
    // The two tables' shards (tablet n is on shard n % 2): `a`'s stream is held.
    let sa = db.shard_of(a.id, b"A").unwrap();
    let sb = db.shard_of(b.id, b"B").unwrap();
    assert_ne!(sa, sb, "the tables are on different shards");
    let mut feed = db
        .feed(FeedSpec {
            tables: vec![a.id, b.id],
            prefix: Vec::new(),
            durable: false,
            start: FeedStart::Now,
        })
        .unwrap();

    // `a`'s stream's syncs are held: its next rollover's header waits behind the
    // predecessor's. Commits are submitted without waiting: one whose record is behind the
    // held header stays unanswered (and hidden) until the sync lands.
    gate.hold(&wal(sa as u32));
    let big = vec![7u8; 20_000];
    let mut pending = Vec::new();
    let mut i = 0u32;
    while !shards[sa].wal_header_held() {
        pending.push(
            db.submit(
                batch(&a, format!("fill{i:03}").as_bytes(), &big),
                Some(Durability::Buffered),
            )
            .unwrap(),
        );
        for _ in 0..50 {
            for s in shards.iter_mut() {
                s.run_once(u64::MAX);
            }
        }
        i += 1;
        assert!(i < 200, "the stream never rolled over behind a held sync");
    }
    // Into the held segment, then a later commit on the other stream.
    pending.push(
        db.submit(batch(&a, b"A", b"a"), Some(Durability::Buffered))
            .unwrap(),
    );
    pending.push(
        db.submit(batch(&b, b"B", b"b"), Some(Durability::Buffered))
            .unwrap(),
    );
    for _ in 0..200 {
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
    }
    assert!(
        shards[sa].wal_header_held(),
        "the successor's header is still held: the test is not vacuous"
    );

    // Inside the window nothing past the held segment is delivered: neither `A`, whose record
    // is behind the header (its group stays hidden, D200), nor `B`, a later commit on the
    // other stream. The frontier never passes a record behind a held header (`Wal::written`
    // excludes it), and the head check stops the feed there too.
    let rows = |c: &pigeonhole_engine::Commit| -> Vec<Vec<u8>> {
        c.changes.iter().map(|ch| ch.row.clone()).collect()
    };
    let mut got: Vec<Vec<u8>> = Vec::new();
    let mut early: Vec<Seqno> = Vec::new();
    while let Next::Commit(c) = feed.try_next().unwrap() {
        got.extend(rows(c));
        early.push(c.seqno);
    }
    assert!(
        !got.iter().any(|r| r == b"A" || r == b"B"),
        "a commit delivered past a held segment header: {:?}",
        got.iter()
            .map(|r| String::from_utf8_lossy(r).into_owned())
            .collect::<Vec<_>>()
    );

    // The sync lands and the header follows: every commit answers, and the feed delivers
    // `A` before `B`, none skipped.
    gate.release();
    let mut cx = Context::from_waker(Waker::noop());
    let mut seqnos = Vec::new();
    for mut pc in pending {
        let seqno = loop {
            for s in shards.iter_mut() {
                s.run_once(u64::MAX);
            }
            if let Poll::Ready(r) = Pin::new(&mut pc).poll(&mut cx) {
                break r.unwrap().seqno;
            }
        };
        seqnos.push(seqno);
    }
    // Every commit of the window, in seqno order: `A` may take its seqno after `B`'s (its
    // stream holds groups back while the header waits), so order is by seqno, not by
    // submission.
    let last = *seqnos.iter().max().unwrap();
    let mut rest: Vec<Seqno> = early;
    for _ in 0..10_000 {
        settle(&mut shards);
        while let Next::Commit(c) = feed.try_next().unwrap() {
            rest.push(c.seqno);
        }
        if rest.contains(&last) {
            break;
        }
    }
    assert!(
        rest.windows(2).all(|w| w[0] < w[1]),
        "in seqno order: {rest:?}"
    );
    for s in &seqnos {
        assert!(
            rest.contains(s),
            "commit {s} skipped: {rest:?} (made {seqnos:?})"
        );
    }
    drop(feed);
    let closer = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || db.close())
    };
    while !closer.is_finished() {
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
        std::thread::yield_now();
    }
    closer.join().unwrap().unwrap();
}

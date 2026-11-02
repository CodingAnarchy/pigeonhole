//! The change feed's writer side (D310 PR 1): WAL retention kept across a clean close and
//! adopted by the next open, the `<db>-feed` sidecar's sessions and sequence chain, the
//! feed session in every segment header, and blob GC's horizon.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyOptions, FeedOptions, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::feed::{FeedEntry, FeedHeader, FeedTables, decode_entries};
use pigeonhole_format::wal::{SEGMENT_HEADER_LEN, SegmentHeader};
use pigeonhole_format::{Durability, StreamId};
use pigeonhole_io::sim::{CrashKind, SimVfs};
use pigeonhole_io::{OpenOptions, Vfs, VfsRef};

const DB: &str = "/db/data.phdb";
const SIDECAR: &str = "/db/data.phdb-feed";
/// `common::options`' WAL segment size.
const SEGMENT: u64 = 8 * 32 * 1024;

fn options(vfs: &Arc<SimVfs>, feed: Option<FeedOptions>) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), 1, 16 << 20);
    o.compaction.l0_trigger = u32::MAX;
    o.compaction.level_base_bytes = u64::MAX;
    o.feed = feed;
    o
}

/// A feed retaining `segments` WAL segments (one shard).
fn feed(segments: u64) -> FeedOptions {
    let mut f = FeedOptions::default();
    f.retention_bytes = segments * SEGMENT;
    f
}

fn open(vfs: &Arc<SimVfs>, feed: Option<FeedOptions>) -> Arc<Engine> {
    Engine::open(Path::new(DB), options(vfs, feed)).unwrap()
}

fn table(db: &Engine) -> TableInfo {
    match db.table("t") {
        Some(t) => (*t).clone(),
        None => {
            // One version, so an overwrite leaves the old value's blob file dead.
            let mut f = FamilyOptions::default();
            f.max_versions = 1;
            (*db.create_table("t", &[("f".into(), f)]).unwrap()).clone()
        }
    }
}

/// Writes `n` rows of `size`-byte values from row `first`, flushing every `flush_every`
/// rows so checkpoints move and segments roll into retention.
fn write(db: &Engine, t: &TableInfo, first: u32, n: u32, size: usize, flush_every: u32) {
    for i in first..first + n {
        let mut wb = WriteBatch::new();
        let v = vec![(i % 251) as u8; size];
        wb.put(
            t.id,
            t.families[0].id,
            format!("row{i:06}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&v),
        )
        .unwrap();
        db.commit(wb, Some(Durability::Buffered)).unwrap();
        if (i + 1 - first).is_multiple_of(flush_every) {
            db.flush().unwrap();
        }
    }
    db.flush().unwrap();
}

fn sidecar(vfs: &Arc<SimVfs>) -> Vec<FeedEntry> {
    let f = vfs.open(Path::new(SIDECAR), OpenOptions::read()).unwrap();
    let mut bytes = vec![0u8; f.len().unwrap() as usize];
    f.read_at(&mut bytes, 0).unwrap();
    FeedHeader::decode(&bytes).unwrap();
    let (entries, end) = decode_entries(&bytes[FeedHeader::LEN..]);
    assert_eq!(FeedHeader::LEN + end, bytes.len(), "no torn tail");
    entries
}

fn wal_path() -> std::path::PathBuf {
    pigeonhole_wal::stream_path(Path::new(DB), StreamId(0))
}

/// The feed session in each valid segment header of stream 0.
fn header_sessions(vfs: &Arc<SimVfs>) -> Vec<u64> {
    let f = vfs.open(&wal_path(), OpenOptions::read()).unwrap();
    let len = f.len().unwrap();
    (0..len / SEGMENT)
        .filter_map(|slot| {
            let mut buf = [0u8; SEGMENT_HEADER_LEN];
            f.read_at(&mut buf, slot * SEGMENT).unwrap();
            SegmentHeader::decode(&buf).ok().map(|h| h.feed_session)
        })
        .collect()
}

fn session_of(e: &FeedEntry) -> Option<u64> {
    match e {
        FeedEntry::Session { session, .. } => Some(*session),
        _ => None,
    }
}

#[test]
fn a_clean_close_keeps_the_history_and_the_reopen_adopts_it() {
    let vfs = SimVfs::new(1248);
    let db = open(&vfs, Some(feed(3)));
    let t = table(&db);
    write(&db, &t, 0, 600, 2_000, 50);
    let info = db.info().unwrap().feed.unwrap();
    assert_eq!(info.retention_bytes, 3 * SEGMENT);
    assert_eq!(info.retained_wal_bytes, 3 * SEGMENT, "the ring is full");
    db.close().unwrap();

    // The WAL file stays, every header naming the one session.
    let vfs_ref: VfsRef = vfs.clone();
    assert!(vfs_ref.exists(&wal_path()).unwrap());
    let entries = sidecar(&vfs);
    let FeedEntry::Session {
        session, tables, ..
    } = &entries[0]
    else {
        panic!("{entries:?}");
    };
    assert_eq!(*tables, FeedTables::All);
    let marked = header_sessions(&vfs);
    assert!(marked.len() >= 4, "{marked:?}");
    assert!(marked.iter().all(|s| s == session), "{marked:?}");

    // The reopen replays nothing and keeps the retained segments.
    let db = open(&vfs, Some(feed(3)));
    let info = db.info().unwrap().feed.unwrap();
    assert_eq!(info.retained_wal_bytes, 3 * SEGMENT);
    let snap = db.snapshot().unwrap();
    let got = db
        .get(&snap, t.id, t.families[0].id, b"row000599", b"q")
        .unwrap()
        .unwrap();
    assert_eq!(got.value(), ValueRef::Bytes(&[(599 % 251) as u8; 2_000]));
    drop(snap);
    db.close().unwrap();

    // Two sessions, the second chained to the first: its open saw the sequence the first
    // recorded last.
    let entries = sidecar(&vfs);
    let sessions: Vec<usize> = (0..entries.len())
        .filter(|&i| session_of(&entries[i]).is_some())
        .collect();
    assert_eq!(sessions.len(), 2, "{entries:?}");
    let last_root = entries[..sessions[1]]
        .iter()
        .rev()
        .find_map(|e| match e {
            FeedEntry::Root { sequence, .. } => Some(*sequence),
            _ => None,
        })
        .unwrap();
    let FeedEntry::Session { opened_at, .. } = entries[sessions[1]] else {
        unreachable!()
    };
    assert_eq!(opened_at, last_root, "{entries:?}");
}

#[test]
fn a_feed_off_session_ends_the_history() {
    let vfs = SimVfs::new(1249);
    let db = open(&vfs, Some(feed(2)));
    let t = table(&db);
    write(&db, &t, 0, 200, 2_000, 50);
    db.close().unwrap();

    // Off: "fed: none" goes into the sidecar, nothing is retained, and its clean close
    // removes the WAL files as without a feed.
    let db = open(&vfs, None);
    assert!(db.info().unwrap().feed.is_none());
    write(&db, &t, 200, 10, 100, 1_000);
    db.close().unwrap();
    let vfs_ref: VfsRef = vfs.clone();
    assert!(!vfs_ref.exists(&wal_path()).unwrap());
    let entries = sidecar(&vfs);
    assert!(
        matches!(entries.last(), Some(FeedEntry::Unfed { .. })),
        "{entries:?}"
    );

    // The next feeding session starts its own history: new headers, a new session.
    let db = open(&vfs, Some(feed(2)));
    write(&db, &t, 300, 10, 100, 1_000);
    db.close().unwrap();
    let entries = sidecar(&vfs);
    let ids: Vec<u64> = entries.iter().filter_map(session_of).collect();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    assert!(header_sessions(&vfs).iter().all(|s| *s == ids[1]));
}

#[test]
fn a_root_another_build_commits_shows_in_the_sequence_check() {
    let vfs = SimVfs::new(1250);
    let db = open(&vfs, Some(feed(2)));
    let t = table(&db);
    write(&db, &t, 0, 50, 500, 25);
    db.close().unwrap();
    let last = sidecar(&vfs)
        .iter()
        .rev()
        .find_map(|e| match e {
            FeedEntry::Root { sequence, .. } => Some(*sequence),
            _ => None,
        })
        .unwrap();

    // An older build's session: it never touches the sidecar, but its open commits a root.
    let vfs_ref: VfsRef = vfs.clone();
    let pager = pigeonhole_pager::Pager::open(&vfs_ref, Path::new(DB), true)
        .unwrap()
        .finish([])
        .unwrap();
    pager.commit_root(pager.root()).unwrap();
    drop(pager);

    let db = open(&vfs, Some(feed(2)));
    db.close().unwrap();
    let entries = sidecar(&vfs);
    let opened_at = entries
        .iter()
        .rev()
        .find_map(|e| match e {
            FeedEntry::Session { opened_at, .. } => Some(*opened_at),
            _ => None,
        })
        .unwrap();
    assert!(opened_at > last, "{opened_at} vs {last}: the gap shows");
}

#[test]
fn feed_tables_must_name_existing_tables() {
    let vfs = SimVfs::new(1251);
    let db = open(&vfs, None);
    table(&db);
    db.close().unwrap();
    let mut f = feed(2);
    f.tables = Some(vec!["t".into(), "missing".into()]);
    let err = Engine::open(Path::new(DB), options(&vfs, Some(f))).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
    let mut f = feed(2);
    f.tables = Some(vec!["t".into()]);
    let db = open(&vfs, Some(f));
    db.close().unwrap();
    let entries = sidecar(&vfs);
    assert!(
        matches!(&entries[0], FeedEntry::Session { tables: FeedTables::Only(t), .. } if t.len() == 1),
        "{entries:?}"
    );
}

#[test]
fn blob_files_stay_until_the_history_passes_them() {
    let vfs = SimVfs::new(1252);
    let db = open(&vfs, Some(feed(2)));
    db.set_inline_value_limit(200);
    let t = table(&db);
    // Large values, then overwritten and compacted: without the feed their files would go.
    write(&db, &t, 0, 20, 1_000, 1_000);
    let files = db.blob_files().len();
    assert_eq!(files, 20);
    write(&db, &t, 0, 20, 100, 1_000);
    db.compact(None).unwrap();
    db.check_blob_accounting().unwrap();
    assert_eq!(db.blob_files().len(), files, "held for the horizon");
    let held = db.info().unwrap().feed.unwrap().held_blob_bytes;
    assert!(held > 20_000, "{held}");

    // Once the history rolls past the segments holding those records, maintenance drops
    // them (inline values from here, so the roll adds no blob files).
    db.set_inline_value_limit(1 << 20);
    write(&db, &t, 1_000, 600, 2_000, 50);
    for _ in 0..2_000 {
        if db.blob_files().is_empty() {
            break;
        }
        db.flush().unwrap();
        std::thread::yield_now();
    }
    assert_eq!(db.blob_files().len(), 0);
    assert_eq!(db.info().unwrap().feed.unwrap().held_blob_bytes, 0);
    db.check_blob_accounting().unwrap();
    db.close().unwrap();
}

#[test]
fn held_blob_files_are_held_again_after_a_reopen() {
    let vfs = SimVfs::new(1253);
    let db = open(&vfs, Some(feed(2)));
    db.set_inline_value_limit(200);
    let t = table(&db);
    write(&db, &t, 0, 10, 1_000, 1_000);
    write(&db, &t, 0, 10, 100, 1_000);
    db.compact(None).unwrap();
    assert_eq!(db.blob_files().len(), 10);
    db.close().unwrap();

    // The open neither sweeps them (no live bytes, but held) nor forgets them.
    let db = open(&vfs, Some(feed(2)));
    db.set_inline_value_limit(200);
    assert_eq!(db.blob_files().len(), 10);
    assert!(db.info().unwrap().feed.unwrap().held_blob_bytes > 0);
    db.set_inline_value_limit(1 << 20);
    write(&db, &t, 1_000, 600, 2_000, 50);
    for _ in 0..2_000 {
        if db.blob_files().is_empty() {
            break;
        }
        db.flush().unwrap();
        std::thread::yield_now();
    }
    assert_eq!(db.blob_files().len(), 0);
    db.close().unwrap();

    // Without the feed, the open sweeps what nothing points into, as before.
    let db = open(&vfs, Some(feed(2)));
    db.set_inline_value_limit(200);
    write(&db, &t, 0, 5, 1_000, 1_000);
    write(&db, &t, 0, 5, 100, 1_000);
    db.compact(None).unwrap();
    assert_eq!(db.blob_files().len(), 5);
    db.close().unwrap();
    let db = open(&vfs, None);
    assert!(db.blob_files().is_empty());
    db.close().unwrap();
}

#[test]
fn a_shard_count_change_shows_in_the_sidecar() {
    let vfs = SimVfs::new(1254);
    let mut o = options(&vfs, Some(feed(4)));
    o.shards = 2;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = table(&db);
    write(&db, &t, 0, 100, 500, 50);
    db.close().unwrap();
    let vfs_ref: VfsRef = vfs.clone();
    let second = pigeonhole_wal::stream_path(Path::new(DB), StreamId(1));
    assert!(vfs_ref.exists(&second).unwrap(), "kept by the clean close");

    let db = open(&vfs, Some(feed(4)));
    db.close().unwrap();
    assert!(!vfs_ref.exists(&second).unwrap(), "the extra stream goes");
    let streams: Vec<u32> = sidecar(&vfs)
        .iter()
        .filter_map(|e| match e {
            FeedEntry::Session { streams, .. } => Some(*streams),
            _ => None,
        })
        .collect();
    assert_eq!(streams, [2, 1]);
}

#[test]
fn a_crash_keeps_the_history_and_the_sidecar_readable() {
    let vfs = SimVfs::new(1255);
    let db = open(&vfs, Some(feed(3)));
    let t = table(&db);
    write(&db, &t, 0, 600, 2_000, 50);
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        b"last",
        b"q",
        None,
        ValueRef::Bytes(b"durable"),
    )
    .unwrap();
    db.commit(wb, Some(Durability::Sync)).unwrap();
    vfs.crash(CrashKind::Power);
    drop(db);

    let db = open(&vfs, Some(feed(3)));
    let snap = db.snapshot().unwrap();
    let got = db
        .get(&snap, t.id, t.families[0].id, b"last", b"q")
        .unwrap()
        .unwrap();
    assert_eq!(got.value(), ValueRef::Bytes(b"durable"));
    drop(snap);
    assert!(db.info().unwrap().feed.unwrap().retained_wal_bytes > 0);
    db.close().unwrap();
    // The sidecar decodes to the end (a torn append was cut off at the reopen). The root
    // appends are not synced, so the power loss may have taken them: the last sequence
    // the first session recorded (a root's, or its open's) is behind the second open's,
    // and a reader expires, the safe direction (D310 §2.2). A kept append can only make
    // them equal.
    let entries = sidecar(&vfs);
    let second = entries
        .iter()
        .rposition(|e| session_of(e).is_some())
        .unwrap();
    assert!(second > 0);
    let recorded = entries[..second]
        .iter()
        .rev()
        .find_map(|e| match e {
            FeedEntry::Root { sequence, .. } => Some(*sequence),
            FeedEntry::Session { opened_at, .. } => Some(*opened_at),
            _ => None,
        })
        .unwrap();
    let FeedEntry::Session { opened_at, .. } = entries[second] else {
        unreachable!()
    };
    assert!(opened_at >= recorded, "{opened_at} < {recorded}");
}

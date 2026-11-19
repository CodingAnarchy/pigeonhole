//! D331 (ICR 0141): the engine calls a range move needs. A feed-silent batch is logged,
//! durable and applied like any commit, never delivered to a subscription, and delivered
//! marked to a reader that includes them (a replica's follower); the first one sets
//! `incompat` bit 3 in both superblock pages, for good, and a marked batch without the bit is
//! corrupt. `raise_ts_floor` survives a crash. A retention hold keeps the feed's history
//! from when it's taken until it's dropped or its cap passes.

use crate::common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pigeonhole_engine::{
    Engine, EngineOptions, FamilyOptions, FeedOptions, FeedReader, FeedSpec, FeedStart, Next,
    TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::superblock::{SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock};
use pigeonhole_format::wal::INCOMPAT_FEED_SILENT;
use pigeonhole_format::{Durability, Seqno};
use pigeonhole_io::sim::{CrashKind, SimVfs};
use pigeonhole_io::{OpenOptions, Vfs};

const DB: &str = "/db/silent.phdb";
const PAGE: u64 = 4096;
const WAIT: Duration = Duration::from_secs(10);

fn feed(retention: u64) -> FeedOptions {
    let mut f = FeedOptions::default();
    f.retention_bytes = retention;
    f
}

fn options(vfs: &Arc<SimVfs>, retention: u64) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), 2, 16 << 20);
    o.feed = Some(feed(retention));
    o
}

fn open(vfs: &Arc<SimVfs>) -> Arc<Engine> {
    Engine::open(Path::new(DB), options(vfs, 1 << 20)).unwrap()
}

/// Two tables on different shards: a batch over both is a cross-shard commit.
fn tables(db: &Engine) -> (Arc<TableInfo>, Arc<TableInfo>) {
    let f = || vec![("f".into(), FamilyOptions::default())];
    match (db.table("a"), db.table("b")) {
        (Some(a), Some(b)) => (a, b),
        _ => (
            db.create_table("a", &f()).unwrap(),
            db.create_table("b", &f()).unwrap(),
        ),
    }
}

fn put(wb: &mut WriteBatch, t: &TableInfo, row: &[u8]) {
    wb.put(
        t.id,
        t.families[0].id,
        row,
        b"q",
        None,
        ValueRef::Bytes(row),
    )
    .unwrap();
}

fn commit(db: &Engine, rows: &[(&TableInfo, &[u8])], silent: bool) -> Seqno {
    let mut wb = WriteBatch::new();
    if silent {
        wb.mark_feed_silent();
    }
    for (t, row) in rows {
        put(&mut wb, t, row);
    }
    db.commit(wb, Some(Durability::GroupSync)).unwrap().seqno
}

fn spec(tables: &[&TableInfo], start: FeedStart) -> FeedSpec {
    FeedSpec {
        tables: tables.iter().map(|t| t.id).collect(),
        prefix: Vec::new(),
        durable: true,
        start,
    }
}

/// Every commit delivered until nothing more comes within a short wait, with whether the
/// reader marked it silent.
fn drain(r: &mut FeedReader) -> Vec<(Seqno, bool)> {
    let mut got = Vec::new();
    loop {
        let seqno = match r.wait(Duration::from_millis(300)).unwrap() {
            Next::Commit(c) => c.seqno,
            Next::Pending => return got,
            Next::Expired(e) => panic!("expired: {e:?}"),
            Next::Catalog(e) => panic!("a catalog event in a per-table feed: {e:?}"),
        };
        got.push((seqno, r.silent()));
    }
}

fn has(db: &Engine, t: &TableInfo, row: &[u8]) -> bool {
    db.get_latest(t.id, t.families[0].id, row, b"q")
        .unwrap()
        .is_some()
}

/// Both superblock pages' `incompat` sets.
fn incompat(vfs: &SimVfs) -> [u64; 2] {
    let f = vfs.open(Path::new(DB), OpenOptions::read()).unwrap();
    [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B].map(|p| {
        let mut buf = [0u8; PAGE as usize];
        f.read_at(&mut buf, p * PAGE).unwrap();
        Superblock::decode(&buf).unwrap().features.incompat
    })
}

fn silent_bit(vfs: &SimVfs) -> [bool; 2] {
    incompat(vfs).map(|i| i & INCOMPAT_FEED_SILENT != 0)
}

/// Rewrites both superblock pages with `edit` applied.
fn edit_superblocks(vfs: &SimVfs, edit: impl Fn(&mut Superblock)) {
    let mut opts = OpenOptions::read();
    opts.write = true;
    let f = vfs.open(Path::new(DB), opts).unwrap();
    for p in [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B] {
        let mut buf = [0u8; PAGE as usize];
        f.read_at(&mut buf, p * PAGE).unwrap();
        let mut sb = Superblock::decode(&buf).unwrap();
        edit(&mut sb);
        sb.encode(&mut buf);
        f.write_at(&buf, p * PAGE).unwrap();
    }
    f.sync_all().unwrap();
}

/// A subscriber sees the ordinary commits around silent ones, single- and cross-shard, and
/// its position passes the silent ones; a reader that includes them sees every commit, the
/// silent ones marked. The silent writes are applied and readable.
#[test]
fn a_silent_commit_is_applied_but_never_delivered_to_a_subscriber() {
    let vfs = SimVfs::new(3310);
    let db = open(&vfs);
    let (a, b) = tables(&db);
    let mut sub = db.feed(spec(&[&a, &b], FeedStart::Now)).unwrap();
    let mut all = db.feed(spec(&[&a, &b], FeedStart::Now)).unwrap();
    all.include_silent();

    let one = commit(&db, &[(&a, b"one")], false);
    let two = commit(&db, &[(&a, b"two")], true);
    let three = commit(&db, &[(&a, b"three"), (&b, b"three")], true);
    let four = commit(&db, &[(&b, b"four")], false);

    assert_eq!(drain(&mut sub), [(one, false), (four, false)]);
    assert!(sub.delivered() >= four);
    assert_eq!(
        drain(&mut all),
        [(one, false), (two, true), (three, true), (four, false)]
    );
    for (t, row) in [(&a, &b"two"[..]), (&a, b"three"), (&b, b"three")] {
        assert!(has(&db, t, row), "{row:?} applied");
    }
    db.close().unwrap();
}

/// The first silent commit sets `incompat` bit 3 in both pages; ordinary commits don't. It
/// stays across a clean reopen and a process crash; the crash's replay applies the silent
/// batch again; and a feed from the oldest history still skips it.
#[test]
fn the_first_silent_commit_sets_incompat_bit_3_for_good() {
    let vfs = SimVfs::new(3311);
    let db = open(&vfs);
    let (a, b) = tables(&db);
    commit(&db, &[(&a, b"plain")], false);
    assert_eq!(silent_bit(&vfs), [false, false]);
    commit(&db, &[(&a, b"quiet"), (&b, b"quiet")], true);
    assert_eq!(silent_bit(&vfs), [true, true]);
    db.close().unwrap();

    let db = open(&vfs);
    assert_eq!(silent_bit(&vfs), [true, true], "sticky across a reopen");
    let (a, b) = tables(&db);
    let crashed = commit(&db, &[(&a, b"after"), (&b, b"after")], true);
    vfs.crash(CrashKind::Process);
    drop(db);

    let db = open(&vfs);
    assert_eq!(silent_bit(&vfs), [true, true], "sticky across a crash");
    let (a, b) = tables(&db);
    assert!(has(&db, &a, b"after") && has(&db, &b, b"after"), "replayed");
    let mut r = db.feed(spec(&[&a, &b], FeedStart::Oldest)).unwrap();
    let got = drain(&mut r);
    assert!(
        got.iter().all(|&(s, silent)| !silent && s != crashed),
        "{got:?}"
    );
    assert!(!got.is_empty(), "the plain commit is still delivered");
    db.close().unwrap();
}

/// A WAL holding a marked batch, in a file whose superblock lacks bit 3 (a stray mark, or a
/// bit lost), doesn't open: the mark is never taken without its bit.
#[test]
fn a_marked_batch_without_its_bit_is_corrupt() {
    let vfs = SimVfs::new(3312);
    let db = open(&vfs);
    let (a, _) = tables(&db);
    commit(&db, &[(&a, b"quiet")], true);
    vfs.crash(CrashKind::Process);
    drop(db);
    edit_superblocks(&vfs, |sb| sb.features.incompat &= !INCOMPAT_FEED_SILENT);
    let err = Engine::open(Path::new(DB), options(&vfs, 1 << 20)).unwrap_err();
    assert!(
        err.to_string().contains("incompat bit 3"),
        "refused as corrupt: {err}"
    );
}

/// The floor `raise_ts_floor` sets is committed before it returns: after a process crash,
/// a write with the default timestamp sorts above it.
#[test]
fn a_raised_ts_floor_survives_a_crash() {
    let vfs = SimVfs::new(3313);
    let db = open(&vfs);
    let (a, _) = tables(&db);
    // Far past the sim clock: a copy from a node whose clock runs ahead.
    let ahead = 1u64 << 60;
    db.raise_ts_floor(ahead).unwrap();
    vfs.crash(CrashKind::Process);
    drop(db);

    let db = open(&vfs);
    let (a2, _) = tables(&db);
    assert_eq!(a2.id, a.id);
    commit(&db, &[(&a2, b"new")], false);
    let cell = db
        .get_latest(a2.id, a2.families[0].id, b"new", b"q")
        .unwrap()
        .unwrap();
    assert!(cell.timestamp() > ahead, "{} <= {ahead}", cell.timestamp());
    db.close().unwrap();
}

/// Far more than the retention, in values the log keeps inline, flushed as it goes so the
/// checkpoints move on (as `retention_passing_a_token_expires_it`).
fn write_past_retention(db: &Engine, t: &TableInfo, tag: u8) {
    let value = vec![tag; 20_000];
    for i in 0..400u32 {
        let mut wb = WriteBatch::new();
        let mut row = vec![tag];
        row.extend(i.to_be_bytes());
        wb.put(
            t.id,
            t.families[0].id,
            &row,
            b"q",
            None,
            ValueRef::Bytes(&value),
        )
        .unwrap();
        db.commit(wb, Some(Durability::GroupSync)).unwrap();
        if i % 40 == 39 {
            db.flush().unwrap();
        }
    }
}

/// Whether a feed from `token` reads to the end without expiring, and how many commits.
fn resumes(db: &Engine, t: &TableInfo, token: Vec<u8>) -> Option<usize> {
    let mut r = db.feed(spec(&[t], FeedStart::Token(token))).unwrap();
    let mut n = 0;
    loop {
        match r.wait(Duration::from_millis(300)).unwrap() {
            Next::Commit(_) => n += 1,
            Next::Catalog(_) => {}
            Next::Pending => return Some(n),
            Next::Expired(_) => return None,
        }
    }
}

/// A hold keeps the history from when it was taken past the retention, so a token from
/// then resumes; without one (or once it's dropped) the same writes expire it. A hold whose
/// cap the retained history passes lapses, and retention trims again.
#[test]
fn a_retention_hold_keeps_the_history_until_dropped_or_its_cap() {
    let vfs = SimVfs::new(3314);
    // The smallest retention: two segments per stream.
    let db = Engine::open(Path::new(DB), options(&vfs, 1)).unwrap();
    let (a, _) = tables(&db);
    let mut r = db.feed(spec(&[&a], FeedStart::Now)).unwrap();

    // Held: every write since the token is still there.
    let hold = db.hold_feed_retention(u64::MAX).unwrap();
    let token = r.token();
    write_past_retention(&db, &a, 1);
    assert_eq!(resumes(&db, &a, token), Some(400), "the hold kept it");
    assert!(!hold.lapsed());

    // Dropped: the next writes trim it again.
    drop(hold);
    drain(&mut r);
    let token = r.token();
    write_past_retention(&db, &a, 2);
    write_past_retention(&db, &a, 3);
    assert_eq!(
        resumes(&db, &a, token),
        None,
        "released, retention passed it"
    );

    // A small cap: the hold lapses once the retained history passes it.
    drain(&mut r);
    let hold = db.hold_feed_retention(1 << 20).unwrap();
    let token = r.token();
    let deadline = Instant::now() + WAIT;
    let mut tag = 4;
    while !hold.lapsed() && Instant::now() < deadline {
        write_past_retention(&db, &a, tag);
        tag += 1;
    }
    assert!(hold.lapsed(), "past its cap");
    write_past_retention(&db, &a, tag);
    assert_eq!(resumes(&db, &a, token), None, "lapsed, retention passed it");
    drop(hold);
    db.close().unwrap();
}

/// On a replica database a direct silent write is refused like any other; a replica takes
/// silent batches only as its primary's logged ones.
#[test]
fn a_replica_refuses_its_own_silent_write() {
    let vfs = SimVfs::new(3315);
    let db = open(&vfs);
    let (a, _) = tables(&db);
    db.begin_replication(b"state", true).unwrap();
    let mut wb = WriteBatch::new();
    wb.mark_feed_silent();
    put(&mut wb, &a, b"mine");
    assert!(db.commit(wb, Some(Durability::GroupSync)).is_err());
    assert_eq!(
        silent_bit(&vfs),
        [false, false],
        "a refused write sets nothing"
    );
    db.close().unwrap();
}

/// The commit paths that rebuild a batch keep its mark: counter merges (prepared at the
/// counter timestamp) and a value over the inline limit (separated into a blob file).
#[test]
fn a_rebuilt_silent_batch_stays_silent() {
    use pigeonhole_format::manifest::FamilyKind;
    let vfs = SimVfs::new(3316);
    let db = open(&vfs);
    let counter = FamilyOptions::default()
        .merge_operator("pigeonhole.i64_add")
        .kind(FamilyKind::Counter);
    let c = db.create_table("c", &[("n".into(), counter)]).unwrap();
    let (a, _) = tables(&db);
    let mut sub = db.feed(spec(&[&a, &c], FeedStart::Now)).unwrap();
    let mut all = db.feed(spec(&[&a, &c], FeedStart::Now)).unwrap();
    all.include_silent();

    let mut wb = WriteBatch::new();
    wb.mark_feed_silent();
    for _ in 0..2 {
        wb.merge(c.id, c.families[0].id, b"hits", b"q", ValueRef::I64(1))
            .unwrap();
    }
    let counted = db.commit(wb, Some(Durability::GroupSync)).unwrap().seqno;

    // Over the inline limit (the WAL segment less 64 KiB): separated at commit.
    let big = vec![9u8; 300 << 10];
    let mut wb = WriteBatch::new();
    wb.mark_feed_silent();
    wb.put(
        a.id,
        a.families[0].id,
        b"big",
        b"q",
        None,
        ValueRef::Bytes(&big),
    )
    .unwrap();
    let separated = db.commit(wb, Some(Durability::GroupSync)).unwrap().seqno;
    let plain = commit(&db, &[(&a, b"plain")], false);

    assert_eq!(drain(&mut sub), [(plain, false)]);
    assert_eq!(
        drain(&mut all),
        [(counted, true), (separated, true), (plain, false)]
    );
    assert!(has(&db, &a, b"big"));
    db.close().unwrap();
}

/// A reader that includes silent commits notes each one only where it keeps it, so its notes
/// are bounded by what's ready or held: none once it has drained. Silent commits it reads
/// but passes (at or below where it starts, or in a round it drops) leave no note behind,
/// where #2055's reader kept one for good.
#[test]
fn silent_notes_are_bounded_by_what_the_reader_keeps() {
    let vfs = SimVfs::new(3317);
    let db = open(&vfs);
    let (a, b) = tables(&db);
    // Before the start: single commits and cross-shard rounds, silent, then a plain one.
    for i in 0..4u8 {
        commit(&db, &[(&a, &[b'p', i])], true);
        commit(&db, &[(&a, &[b'x', i]), (&b, &[b'y', i])], true);
    }
    let start = commit(&db, &[(&a, b"start")], false);
    let mut all = db.feed(spec(&[&a, &b], FeedStart::After(start))).unwrap();
    all.include_silent();
    assert!(drain(&mut all).is_empty());
    assert_eq!(all.notes(), 0, "silent commits read below the start");
    // After it: delivered silent commits, single and round, and one of another table only.
    let c = db
        .create_table("c", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let s1 = commit(&db, &[(&a, b"s1")], true);
    let r1 = commit(&db, &[(&a, b"r-a"), (&b, b"r-b")], true);
    commit(&db, &[(&c, b"other")], true);
    let p = commit(&db, &[(&b, b"plain")], false);
    assert_eq!(drain(&mut all), [(s1, true), (r1, true), (p, false)]);
    assert_eq!(all.notes(), 0, "delivered and passed-over silent commits");
    drop(all);
    db.close().unwrap();
}

/// The same under the read-ahead cap (D320), which evicts kept commits and reads them again
/// later: an evicted single's note goes with it (ai1249's nit on #2068), so after every
/// delivery each note is a commit the reader still keeps (`notes` checks it).
#[test]
fn a_capped_readers_silent_notes_follow_its_evictions() {
    let vfs = SimVfs::new(3318);
    let db = open(&vfs);
    let (a, b) = tables(&db);
    let mut all = db.feed(spec(&[&a, &b], FeedStart::Now)).unwrap();
    all.include_silent();
    // About one record: each pass keeps a commit or two, and evicts the rest.
    all.set_read_ahead(300);
    let mut want = Vec::new();
    for i in 0..40u8 {
        let silent = i % 4 != 3;
        let s = if i % 5 == 0 {
            commit(&db, &[(&a, &[b'x', i]), (&b, &[b'y', i])], silent)
        } else {
            commit(
                &db,
                &[(if i % 2 == 0 { &a } else { &b }, &[b's', i])],
                silent,
            )
        };
        want.push((s, silent));
    }
    let mut got = Vec::new();
    while got.len() < want.len() {
        let c = match all.wait(WAIT).unwrap() {
            Next::Commit(c) => c.seqno,
            other => panic!("{other:?}"),
        };
        got.push((c, all.silent()));
        all.notes();
    }
    assert_eq!(got, want);
    assert!(all.stream_reads() > 5, "the cap never bit");
    assert_eq!(all.notes(), 0);
    drop(all);
    db.close().unwrap();
}

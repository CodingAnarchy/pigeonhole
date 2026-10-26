//! D281 (#926): a reader process's row cache. It keys on the writer's write watermarks in
//! shared memory, so every write the reader can see makes its cached copy miss: puts,
//! family deletes, row deletes and counter increments alike (perf287's PR point). And it
//! belongs to one writer generation: a writer reopen (clean, or through the handover a
//! recovery uses) builds a new region with zero watermarks, and the reader's cache starts
//! empty there, so an entry filled against the old generation never hits against the new
//! one's slots (perf287's M2). Deterministic: one application-owned shard driven by hand, a
//! simulated reader process.

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyKind, FamilyOptions, ReadSpec, RowData, TableInfo,
    ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/reader_row_cache.phdb";

const READER: pigeonhole_io::ProcessId = pigeonhole_io::ProcessId {
    pid: 4343,
    start_time: 1,
};

struct Writer {
    db: Arc<Engine>,
    shard: EngineShard,
    t: Arc<TableInfo>,
}

fn options(vfs: &Arc<SimVfs>) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), 1, 16 << 20);
    o.tablet_changes = false;
    // The writer's own row cache on: only then does it raise the slots (D281).
    o.row_cache_bytes = 1 << 20;
    o
}

impl Writer {
    fn open(vfs: &Arc<SimVfs>) -> Self {
        let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options(vfs)).unwrap();
        let mut shard = shards.remove(0);
        while shard.run_once(u64::MAX) {}
        let t = match db.table("t") {
            Some(t) => t,
            None => db
                .create_table(
                    "t",
                    &[
                        ("f".into(), FamilyOptions::default()),
                        (
                            "c".into(),
                            FamilyOptions::default()
                                .merge_operator("pigeonhole.i64_add")
                                .kind(FamilyKind::Counter),
                        ),
                    ],
                )
                .unwrap(),
        };
        Self { db, shard, t }
    }

    fn commit(&mut self, f: impl FnOnce(&mut WriteBatch, &TableInfo)) {
        self.commit_at(Durability::Sync, f);
    }

    fn commit_at(&mut self, durability: Durability, f: impl FnOnce(&mut WriteBatch, &TableInfo)) {
        let mut wb = WriteBatch::new();
        f(&mut wb, &self.t);
        let mut pending = self.db.submit(wb, Some(durability)).unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(r) = Pin::new(&mut pending).poll(&mut cx) {
                r.unwrap();
                return;
            }
            self.shard.run_once(u64::MAX);
        }
    }

    fn close(self) {
        let Writer { db, mut shard, .. } = self;
        db.close().unwrap();
        while shard.closed().is_none() {
            shard.run_once(u64::MAX);
        }
    }
}

/// The row's cells, comparable: (family, qualifier, value bytes).
fn latest(db: &Engine, t: &TableInfo, row: &[u8]) -> Vec<(u32, Vec<u8>, Vec<u8>)> {
    let mut spec = ReadSpec::default();
    spec.versions = 1;
    let families: Vec<_> = t.families.iter().map(|f| f.id).collect();
    let mut data = RowData::default();
    db.read_row_latest_into(t.id, row, &families, &spec, &mut data)
        .unwrap();
    data.cells
        .iter()
        .map(|c| {
            (
                c.family.0,
                data.qualifier(c).to_vec(),
                common::value_bytes(c.data.value()),
            )
        })
        .collect()
}

fn open_reader(vfs: &Arc<SimVfs>) -> Arc<Engine> {
    let mut o = options(vfs);
    o.row_cache_bytes = 1 << 20;
    Engine::open_reader(Path::new(DB), o).unwrap()
}

#[test]
fn a_reader_process_caches_hot_rows_and_misses_after_every_kind_of_write() {
    let vfs = SimVfs::new(926);
    let writer_process = pigeonhole_io::Vfs::current_process(&*vfs);
    let mut w = Writer::open(&vfs);
    let (f, c) = (w.t.families[0].id, w.t.families[1].id);
    w.commit(|wb, t| {
        wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(b"v1"))
            .unwrap();
        wb.merge(t.id, c, b"r", b"n", ValueRef::I64(1)).unwrap();
    });

    vfs.enter_process(READER);
    let reader = open_reader(&vfs);
    let t = Arc::clone(&w.t);
    // A row is stored on its second miss, and served from then on.
    let first = latest(&reader, &t, b"r");
    assert_eq!(latest(&reader, &t, b"r"), first);
    let filled = reader.row_cache_stats();
    assert_eq!(latest(&reader, &t, b"r"), first);
    let after = reader.row_cache_stats();
    assert!(filled.fills >= 1, "never filled: {filled:?}");
    assert!(after.hits > filled.hits, "never hit: {after:?}");

    // Each kind of write: the reader's next read is the writer's, never the cached copy.
    type Write = Box<dyn Fn(&mut WriteBatch, &TableInfo)>;
    let writes: Vec<(&str, Write)> = vec![
        (
            "put",
            Box::new(move |wb, t| {
                wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(b"v2"))
                    .unwrap();
            }),
        ),
        (
            "counter increment",
            Box::new(move |wb, t| {
                wb.merge(t.id, c, b"r", b"n", ValueRef::I64(5)).unwrap();
            }),
        ),
        (
            "family delete",
            Box::new(move |wb, t| wb.delete_family(t.id, f, b"r", None).unwrap()),
        ),
        (
            "put after the delete",
            Box::new(move |wb, t| {
                wb.put(t.id, f, b"r", b"q2", None, ValueRef::Bytes(b"v3"))
                    .unwrap();
            }),
        ),
        (
            "row delete",
            Box::new(|wb, t| wb.delete_row(t.id, b"r", None).unwrap()),
        ),
    ];
    for (what, write) in writes {
        // Cached before the write (two reads: a fill, then a hit).
        latest(&reader, &t, b"r");
        latest(&reader, &t, b"r");
        vfs.enter_process(writer_process);
        w.commit(|wb, t| write(wb, t));
        let want = latest(&w.db, &t, b"r");
        vfs.enter_process(READER);
        assert_eq!(latest(&reader, &t, b"r"), want, "after the {what}");
        assert_eq!(
            latest(&reader, &t, b"r"),
            want,
            "after the {what}, read again"
        );
    }
    drop(reader);
    vfs.enter_process(writer_process);
    w.close();
}

/// perf287's M2: an entry filled at epoch 0 against one writer generation must not hit
/// against the next generation's slots, which start at 0 again. The reader fills the row,
/// the writer changes it durably (the reader does not read it again, so its entry stays at
/// epoch 0) and reopens: on the next generation's zero slots the old entry would hit, and
/// serve the row from before the write. `reopen` is a clean reopen or the handover a
/// recovery uses; both build a new generation.
fn an_entry_never_outlives_its_generation(reopen: fn(Writer, &Arc<SimVfs>) -> Writer) {
    let vfs = SimVfs::new(281);
    let writer_process = pigeonhole_io::Vfs::current_process(&*vfs);
    let mut w = Writer::open(&vfs);
    let f = w.t.families[0].id;
    w.commit(|wb, t| {
        wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(b"before"))
            .unwrap();
    });
    vfs.enter_process(READER);
    let reader = open_reader(&vfs);
    let t = Arc::clone(&w.t);
    for _ in 0..3 {
        latest(&reader, &t, b"r");
    }
    assert!(reader.row_cache_stats().hits > 0, "the row is cached");

    vfs.enter_process(writer_process);
    w.commit(|wb, t| {
        wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(b"after"))
            .unwrap();
    });
    let w = reopen(w, &vfs);
    let want = latest(&w.db, &t, b"r");
    assert_eq!(want[0].2, b"after");

    vfs.enter_process(READER);
    for _ in 0..3 {
        assert_eq!(
            latest(&reader, &t, b"r"),
            want,
            "a stale row from the old generation"
        );
    }
    // And the new generation's writes reach the reader (its cache keys on the new slots,
    // not on the old region's frozen ones).
    vfs.enter_process(writer_process);
    let mut w = w;
    w.commit(|wb, t| {
        wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(b"later"))
            .unwrap();
    });
    let want = latest(&w.db, &t, b"r");
    vfs.enter_process(READER);
    for _ in 0..3 {
        assert_eq!(
            latest(&reader, &t, b"r"),
            want,
            "a write of the new generation missed"
        );
    }
    drop(reader);
    vfs.enter_process(writer_process);
    w.close();
}

#[test]
fn an_entry_never_outlives_its_generation_across_a_reopen() {
    an_entry_never_outlives_its_generation(|w, vfs| {
        w.close();
        Writer::open(vfs)
    });
}

/// perf287's M2 for the handover a recovery uses (D265): `close_for_reopen` and `open_with`
/// keep the writer lock but build a new region generation, so a reader's entries from the
/// old one never hit. The reader is a handle opened inside the writer process: it reads
/// through shared memory exactly as a reader process does.
#[test]
fn an_entry_never_outlives_its_generation_across_a_handover() {
    let vfs = SimVfs::new(265);
    let mut o = options(&vfs);
    o.pin_threads = false;
    o.compaction_threads = 0;
    let db = Engine::open(Path::new(DB), o.clone()).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    let put = |db: &Engine, v: &[u8]| {
        let mut wb = WriteBatch::new();
        wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(v))
            .unwrap();
        db.commit(wb, Some(Durability::Sync)).unwrap();
    };
    put(&db, b"before");
    let reader = open_reader(&vfs);
    for _ in 0..3 {
        latest(&reader, &t, b"r");
    }
    assert!(reader.row_cache_stats().hits > 0, "the row is cached");
    put(&db, b"after");
    let (closed, handover) = db.close_for_reopen();
    closed.unwrap();
    let db = Engine::open_with(Path::new(DB), o, handover.unwrap())
        .map_err(|(e, _)| e)
        .unwrap();
    let want = latest(&db, &t, b"r");
    assert_eq!(want[0].2, b"after");
    for _ in 0..3 {
        assert_eq!(
            latest(&reader, &t, b"r"),
            want,
            "a stale row across the handover"
        );
    }
    put(&db, b"later");
    let want = latest(&db, &t, b"r");
    for _ in 0..3 {
        assert_eq!(
            latest(&reader, &t, b"r"),
            want,
            "a write after the handover missed"
        );
    }
    drop(reader);
    db.close().unwrap();
}

/// The generation binding end to end (perf287's PR point): a write the reader saw, and
/// cached, that its writer's crash lost (`Durability::None`: visible, never logged) is never
/// served after the writer restarts, and the restarted writer's writes reach the reader.
#[test]
fn a_write_lost_with_its_writer_is_never_served_from_a_reader_cache() {
    let vfs = SimVfs::new(927);
    let first = pigeonhole_io::Vfs::current_process(&*vfs);
    let mut w = Writer::open(&vfs);
    let f = w.t.families[0].id;
    w.commit(|wb, t| {
        wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(b"kept"))
            .unwrap();
    });
    w.commit_at(Durability::None, |wb, t| {
        wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(b"lost"))
            .unwrap();
    });
    vfs.enter_process(READER);
    let reader = open_reader(&vfs);
    let t = Arc::clone(&w.t);
    for _ in 0..3 {
        assert_eq!(
            latest(&reader, &t, b"r")[0].2,
            b"lost",
            "the reader sees the write"
        );
    }
    assert!(reader.row_cache_stats().hits > 0, "and caches it");

    // The writer's process dies: the `None` write was never logged, so it is gone.
    vfs.crash_process(first);
    std::mem::forget(w);
    let second = pigeonhole_io::ProcessId {
        pid: first.pid,
        start_time: first.start_time + 1,
    };
    vfs.enter_process(second);
    let mut w = Writer::open(&vfs);
    let want = latest(&w.db, &t, b"r");
    assert_eq!(want[0].2, b"kept", "the lost write is gone");
    vfs.enter_process(READER);
    for _ in 0..3 {
        assert_eq!(
            latest(&reader, &t, b"r"),
            want,
            "a lost write served from the cache"
        );
    }
    vfs.enter_process(second);
    w.commit(|wb, t| {
        wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(b"new"))
            .unwrap();
    });
    let want = latest(&w.db, &t, b"r");
    vfs.enter_process(READER);
    for _ in 0..3 {
        assert_eq!(
            latest(&reader, &t, b"r"),
            want,
            "a write of the new writer missed"
        );
    }
    drop(reader);
    vfs.enter_process(second);
    w.close();
}

/// D281 after callgrind (the owner's ruling): a writer with its row cache off raises no slot
/// and says so in the header, and a reader process then keeps no cache, so its reads stay
/// right instead of keying on slots that stay 0.
#[test]
fn a_writer_without_a_row_cache_gives_readers_none() {
    let vfs = SimVfs::new(928);
    let writer_process = pigeonhole_io::Vfs::current_process(&*vfs);
    let mut o = options(&vfs);
    o.row_cache_bytes = 0;
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
    let mut shard = shards.remove(0);
    while shard.run_once(u64::MAX) {}
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let mut w = Writer { db, shard, t };
    let f = w.t.families[0].id;
    w.commit(|wb, t| {
        wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(b"v1"))
            .unwrap();
    });
    vfs.enter_process(READER);
    let reader = open_reader(&vfs);
    let t = Arc::clone(&w.t);
    for _ in 0..3 {
        latest(&reader, &t, b"r");
    }
    vfs.enter_process(writer_process);
    w.commit(|wb, t| {
        wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(b"v2"))
            .unwrap();
    });
    let want = latest(&w.db, &t, b"r");
    vfs.enter_process(READER);
    assert_eq!(
        latest(&reader, &t, b"r"),
        want,
        "a stale row from slots nobody raised"
    );
    let stats = reader.row_cache_stats();
    assert_eq!(
        (stats.hits, stats.fills),
        (0, 0),
        "a cache keyed on unraised slots: {stats:?}"
    );
    drop(reader);
    vfs.enter_process(writer_process);
    w.close();
}

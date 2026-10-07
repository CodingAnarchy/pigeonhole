//! Reader-process snapshots against a live writer (issue #140): a snapshot taken before a
//! writer restart fails with `SnapshotExpired` instead of reading extents the new writer
//! reused, and a reader builds a view only from the catalog of the view record's own
//! manifest version.

mod common;

use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use common::poll_commit;
use pigeonhole_engine::{
    Engine, EngineShard, Error, FamilyOptions, PendingMaintenance, ScanSpec, Snapshot, TableInfo,
    ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::ProcessId;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";
const WRITER: ProcessId = ProcessId {
    pid: 1,
    start_time: 1,
};
const READER: ProcessId = ProcessId {
    pid: 2,
    start_time: 1,
};

/// An application-owned writer the test drives on its own thread.
struct Writer {
    db: Arc<Engine>,
    shards: Vec<EngineShard>,
}

impl Writer {
    fn open(vfs: &Arc<SimVfs>) -> Self {
        let (db, shards) = Engine::open_application_owned(
            Path::new(DB),
            common::options(Arc::clone(vfs), 1, 4 << 20),
        )
        .unwrap();
        Writer { db, shards }
    }

    fn run(&mut self) -> bool {
        let mut more = false;
        for s in &mut self.shards {
            more |= s.run_once(u64::MAX);
        }
        more
    }

    fn commit(&mut self, wb: WriteBatch) {
        let mut pc = self.db.submit(wb, Some(Durability::Buffered)).unwrap();
        for _ in 0..100_000 {
            if let Poll::Ready(r) = poll_commit(&mut pc) {
                r.unwrap();
                return;
            }
            self.run();
        }
        panic!("commit did not finish");
    }

    fn drive(&mut self, mut m: PendingMaintenance) {
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..200_000 {
            if let Poll::Ready(r) = Pin::new(&mut m).poll(&mut cx) {
                r.unwrap();
                return;
            }
            self.run();
        }
        panic!("maintenance did not finish");
    }

    fn flush(&mut self) {
        let m = self.db.flush_pending().unwrap();
        self.drive(m);
    }

    fn compact(&mut self) {
        let m = self.db.compact_pending(None).unwrap();
        self.drive(m);
    }

    fn close(mut self) {
        self.db.close().unwrap();
        while self.run() {}
    }
}

fn value(tag: &str, i: u32) -> Vec<u8> {
    let mut v = format!("{tag}-{i:05}-").into_bytes();
    v.resize(2000, b'.');
    v
}

fn write_rows(w: &mut Writer, t: &TableInfo, prefix: &str, tag: &str, rows: std::ops::Range<u32>) {
    let f = t.family("f").unwrap().id;
    for chunk in rows.collect::<Vec<_>>().chunks(20) {
        let mut wb = WriteBatch::new();
        for &i in chunk {
            let row = format!("{prefix}{i:05}");
            wb.put(
                t.id,
                f,
                row.as_bytes(),
                b"q",
                None,
                ValueRef::Bytes(&value(tag, i)),
            )
            .unwrap();
        }
        w.commit(wb);
    }
    w.flush();
}

fn get(db: &Engine, snap: &Snapshot, t: &TableInfo, row: &str) -> Result<Option<Vec<u8>>, Error> {
    let f = t.family("f").unwrap().id;
    db.get(snap, t.id, f, row.as_bytes(), b"q")
        .map(|c| c.map(|c| common::value_bytes(c.value())))
}

/// F7-1: the new writer frees every extent its recovered root does not name, including the
/// SSTs an old-generation snapshot reads (its pin lives in the abandoned region). Reads
/// through that snapshot used to fail with `Corruption("sst footer")` or return `Ok(None)`
/// for rows that exist; they must fail with `SnapshotExpired`.
#[test]
fn a_snapshot_from_before_a_writer_restart_expires() {
    let vfs = SimVfs::new(11);
    vfs.enter_process(WRITER);
    let mut w = Writer::open(&vfs);
    let t =
        w.db.create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
    write_rows(&mut w, &t, "row", "old", 0..200);
    write_rows(&mut w, &t, "row", "old", 200..400);

    vfs.enter_process(READER);
    let reader =
        Engine::open_reader(Path::new(DB), common::options(Arc::clone(&vfs), 1, 4 << 20)).unwrap();
    let rt = reader.table("t").unwrap();
    let snap = reader.snapshot().unwrap();

    // The writer closes: with no new writer the snapshot keeps serving.
    vfs.enter_process(WRITER);
    w.close();
    vfs.enter_process(READER);
    assert_eq!(
        get(&reader, &snap, &rt, "row00007").unwrap(),
        Some(value("old", 7))
    );

    // A new writer recovers, compacts the old SSTs away and reuses their extents.
    vfs.enter_process(WRITER);
    let mut w = Writer::open(&vfs);
    let t = w.db.table("t").unwrap();
    w.compact();
    for round in 0..6u32 {
        write_rows(&mut w, &t, "zzz", "NEW", round * 200..round * 200 + 200);
    }

    vfs.enter_process(READER);
    // Before the fix: `Corruption("block")` for row 150, `Ok(None)` for rows 250 and 399.
    let mut unexpired = Vec::new();
    for i in [0u32, 1, 150, 250, 399] {
        match get(&reader, &snap, &rt, &format!("row{i:05}")) {
            Err(Error::SnapshotExpired) => {}
            other => unexpired.push((
                i,
                other.map(|v| v.map(|v| String::from_utf8_lossy(&v[..12]).into_owned())),
            )),
        }
    }
    assert!(
        unexpired.is_empty(),
        "reads at the pre-restart snapshot: {unexpired:?}"
    );
    let mut cursor = reader
        .scan(
            &snap,
            rt.id,
            ScanSpec::new(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        )
        .unwrap();
    assert!(matches!(cursor.next_row(), Err(Error::SnapshotExpired)));
    drop(cursor);
    drop(snap);

    // A new snapshot re-attaches and reads everything.
    let snap = reader.snapshot().unwrap();
    for i in [0u32, 1, 150, 250, 399] {
        assert_eq!(
            get(&reader, &snap, &rt, &format!("row{i:05}")).unwrap(),
            Some(value("old", i)),
            "row{i:05}"
        );
    }
    assert_eq!(
        get(&reader, &snap, &rt, "zzz01199").unwrap(),
        Some(value("NEW", 1199))
    );
    drop(snap);
    reader.close().unwrap();
    vfs.enter_process(WRITER);
    w.close();
}

/// F7-2: a reader read the view record (which lists memtable *m*) and then loaded the
/// catalog of the current durable root, which a flush in between had moved past it (it
/// holds *m*'s SST): *m*'s counter operands were counted twice. The reader must use the
/// catalog of the record's own manifest version, re-reading the record when the root moved.
#[test]
fn a_reader_view_never_mixes_a_record_with_another_versions_catalog() {
    let vfs = SimVfs::new(77);
    vfs.enter_process(WRITER);
    let w = Arc::new(Mutex::new(Writer::open(&vfs)));
    let (t, f) = {
        let w = w.lock().unwrap();
        let fo = FamilyOptions {
            merge_operator: "pigeonhole.i64_add".to_owned(),
            ..FamilyOptions::default()
        };
        let t = w.db.create_table("t", &[("f".into(), fo)]).unwrap();
        let f = t.family("f").unwrap().id;
        (t, f)
    };
    let add = |w: &Arc<Mutex<Writer>>, n: u32| {
        let mut w = w.lock().unwrap();
        for _ in 0..n {
            let mut wb = WriteBatch::new();
            wb.merge(t.id, f, b"ctr", b"q", ValueRef::I64(1)).unwrap();
            w.commit(wb);
        }
    };
    add(&w, 10);

    vfs.enter_process(READER);
    let reader =
        Engine::open_reader(Path::new(DB), common::options(Arc::clone(&vfs), 1, 4 << 20)).unwrap();
    let counter = |snap: &Snapshot| match reader.get(snap, t.id, f, b"ctr", b"q").unwrap() {
        Some(c) => match c.value() {
            ValueRef::I64(v) => v,
            other => panic!("{other:?}"),
        },
        None => 0,
    };
    let snap = reader.snapshot().unwrap();
    assert_eq!(counter(&snap), 10);
    drop(snap);

    for round in 1..=3i64 {
        add(&w, 10);
        // The writer flushes between the reader's record read and its catalog load.
        let (hw, hv) = (Arc::clone(&w), Arc::clone(&vfs));
        reader.on_reader_view_record(Box::new(move || {
            hv.enter_process(WRITER);
            hw.lock().unwrap().flush();
            hv.enter_process(READER);
        }));
        let snap = reader.snapshot().unwrap();
        assert_eq!(counter(&snap), 10 + round * 10, "round {round}");
        drop(snap);
    }
    reader.close().unwrap();
    vfs.enter_process(WRITER);
    Arc::try_unwrap(w)
        .ok()
        .unwrap()
        .into_inner()
        .unwrap()
        .close();
}

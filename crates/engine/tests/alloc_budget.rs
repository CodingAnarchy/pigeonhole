//! Allocations per operation on the engine's hot paths (#320): a counting global allocator
//! measures each path in steady state (after a warm-up) and prints a table; budgets assert
//! upper bounds on the hottest ones, so a regression fails here.
//!
//! Deterministic: one shard driven by the test thread (application-owned), the simulated
//! file system (I/O completes inline), and counters per thread, so only the calling
//! thread's allocations count. Flushes and compactions run as tasks of that shard, on this
//! thread. "Bytes" is the size of every allocation (and of every reallocation's new size);
//! for a value copied into owned memory it counts one value's worth per copy, so bytes per
//! value byte is a cheap stand-in for the copies a path makes (copies into reused buffers
//! or the memtable arena are not counted).
//!
//! The counting allocator needs `unsafe` (`GlobalAlloc`); it lives in this test binary
//! only, as in `pigeonhole`'s `tests/alloc.rs`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell as StdCell;
use std::future::Future;
use std::ops::Bound;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, ReadSpec, ScanSpec, TableInfo, ValueRef,
    WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

struct Counting;

thread_local! {
    static ALLOCS: StdCell<u64> = const { StdCell::new(0) };
    static BYTES: StdCell<u64> = const { StdCell::new(0) };
}

fn bump(bytes: usize) {
    // `try_with`: the allocator can run during thread-local teardown.
    let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
    let _ = BYTES.try_with(|n| n.set(n.get() + bytes as u64));
}

// SAFETY: forwards every call to `System` unchanged; only counts calls on this thread.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump(layout.size());
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract, which we pass through.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        bump(layout.size());
        // SAFETY: as for `alloc`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump(new_size);
        // SAFETY: as for `alloc`; `ptr` came from this allocator, i.e. from `System`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from this allocator, i.e. from `System`, with `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Allocations (alloc and realloc calls) and bytes allocated by `f` on this thread.
fn counted<R>(f: impl FnOnce() -> R) -> (u64, u64, R) {
    let (a, b) = (ALLOCS.with(StdCell::get), BYTES.with(StdCell::get));
    let r = f();
    (
        ALLOCS.with(StdCell::get) - a,
        BYTES.with(StdCell::get) - b,
        r,
    )
}

/// One measured path: allocations and bytes per operation (or per cell or entry).
struct Row {
    path: &'static str,
    unit: &'static str,
    allocs: f64,
    bytes: f64,
}

/// Measures `ops` runs of `f` after one warm-up run: per-op averages.
fn per_op(ops: u64, mut f: impl FnMut(u64)) -> (f64, f64) {
    f(u64::MAX);
    let (a, b, ()) = counted(|| (0..ops).for_each(&mut f));
    (a as f64 / ops as f64, b as f64 / ops as f64)
}

struct Rig {
    db: Arc<Engine>,
    shard: EngineShard,
    t: TableInfo,
    /// Qualifiers `q000`.., made once so batches don't allocate them.
    quals: Vec<Vec<u8>>,
}

impl Rig {
    fn open(name: &str) -> Self {
        let vfs = SimVfs::new(320);
        let mut o = EngineOptions::new(vfs);
        o.create_if_missing = true;
        o.shards = 1;
        o.pin_threads = false;
        o.memtable_budget = 16 << 20;
        o.memtable_freeze_bytes = 8 << 20;
        o.reader_slots = 8;
        // 256 KiB segments: the inline limit is the segment payload, 192 KiB.
        o.wal.segment_size = 256 << 10;
        o.wal.spare_segments = 1;
        // Explicit flushes and compactions only.
        o.compaction.l0_trigger = u32::MAX;
        o.compaction.level_base_bytes = u64::MAX;
        let (db, mut shards) =
            Engine::open_application_owned(Path::new(&format!("/db/{name}.phdb")), o).unwrap();
        let mut shard = shards.remove(0);
        while shard.run_once(u64::MAX) {}
        let family = FamilyOptions::default().blob_threshold(u32::MAX);
        let t = (*db.create_table("t", &[("f".into(), family)]).unwrap()).clone();
        let quals = (0..64).map(|c| format!("q{c:03}").into_bytes()).collect();
        Self {
            db,
            shard,
            t,
            quals,
        }
    }

    fn idle(&mut self) {
        while self.shard.run_once(u64::MAX) {}
    }

    fn wait<F: Future + Unpin>(&mut self, mut f: F) -> F::Output {
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(r) = Pin::new(&mut f).poll(&mut cx) {
                return r;
            }
            self.shard.run_once(u64::MAX);
        }
    }

    fn batch(&self, row: &[u8], cells: u32, value: &[u8]) -> WriteBatch {
        let mut wb = WriteBatch::new();
        for q in &self.quals[..cells as usize] {
            wb.put(
                self.t.id,
                self.t.families[0].id,
                row,
                q,
                None,
                ValueRef::Bytes(value),
            )
            .unwrap();
        }
        wb
    }

    fn commit(&mut self, wb: WriteBatch) {
        let p = self.db.submit(wb, Some(Durability::Buffered)).unwrap();
        self.wait(p).unwrap();
    }

    fn put_rows(&mut self, rows: std::ops::Range<u32>, cells: u32, value: &[u8]) {
        for r in rows {
            let wb = self.batch(&row(r), cells, value);
            self.commit(wb);
        }
    }

    fn flush(&mut self) {
        let p = self.db.flush_pending().unwrap();
        self.wait(p).unwrap();
        self.idle();
    }

    fn compact(&mut self) {
        let p = self.db.compact_pending(None).unwrap();
        self.wait(p).unwrap();
        self.idle();
    }

    fn close(mut self) {
        self.db.close().unwrap();
        loop {
            self.shard.run_once(u64::MAX);
            if let Some(r) = self.shard.closed() {
                r.unwrap();
                return;
            }
        }
    }
}

fn row(i: u32) -> Vec<u8> {
    format!("row{i:05}").into_bytes()
}

/// Row keys `from..to`, made before a measurement so it doesn't count them.
fn rows(from: u32, to: u32) -> Vec<Vec<u8>> {
    (from..to).map(row).collect()
}

/// Point reads: memtable and SST, through a snapshot and `get_latest`.
fn gets(out: &mut Vec<Row>) {
    let mut rig = Rig::open("gets");
    let (t, f) = (rig.t.id, rig.t.families[0].id);
    let value = [7u8; 100];
    rig.put_rows(0..64, 1, &value);
    let keys = rows(0, 64);
    let key = |i: u64| &keys[(i % 64) as usize];
    let snap = rig.db.snapshot().unwrap();
    let (allocs, bytes) = per_op(64, |i| {
        assert!(rig.db.get(&snap, t, f, key(i), b"q000").unwrap().is_some());
    });
    out.push(Row {
        path: "get, memtable (snapshot)",
        unit: "get",
        allocs,
        bytes,
    });
    drop(snap);
    let (allocs, bytes) = per_op(64, |i| {
        assert!(rig.db.get_latest(t, f, key(i), b"q000").unwrap().is_some());
    });
    out.push(Row {
        path: "get_latest, memtable",
        unit: "get",
        allocs,
        bytes,
    });
    let (allocs, bytes) = per_op(64, |i| {
        assert!(
            rig.db
                .get_latest(t, f, key(i), b"missing")
                .unwrap()
                .is_none()
        );
    });
    out.push(Row {
        path: "get_latest, memtable miss",
        unit: "get",
        allocs,
        bytes,
    });

    // Rows 1000.. flushed: 4 KiB values, so each row's cell has a data block to itself
    // and a row read the first time misses the block cache.
    let big = [9u8; 4 << 10];
    rig.put_rows(1000..1128, 1, &big);
    rig.flush();
    let flushed = rows(1000, 1128);
    let mut next = 0;
    let (allocs, bytes) = per_op(64, |_| {
        next += 1;
        assert!(
            rig.db
                .get_latest(t, f, &flushed[next], b"q000")
                .unwrap()
                .is_some()
        );
    });
    out.push(Row {
        path: "get_latest, SST, block not cached",
        unit: "get",
        allocs,
        bytes,
    });
    let cached = |i: u64| &flushed[1 + (i % 64) as usize];
    let (allocs, bytes) = per_op(64, |i| {
        assert!(
            rig.db
                .get_latest(t, f, cached(i), b"q000")
                .unwrap()
                .is_some()
        );
    });
    out.push(Row {
        path: "get_latest, SST, block cached",
        unit: "get",
        allocs,
        bytes,
    });
    let snap = rig.db.snapshot().unwrap();
    let (allocs, bytes) = per_op(64, |i| {
        assert!(
            rig.db
                .get(&snap, t, f, cached(i), b"q000")
                .unwrap()
                .is_some()
        );
    });
    out.push(Row {
        path: "get, SST, block cached (snapshot)",
        unit: "get",
        allocs,
        bytes,
    });
    drop(snap);
    rig.close();
}

/// Row reads and scans: per row (rows of one cell) and per extra cell (rows of 33 cells),
/// from the memtable and from cached SST blocks. Returns the bytes a row read allocates per
/// value byte (rows of 33 cells of 1 KiB against 33 of 64 bytes): the copies of each value.
fn rows_and_scans(out: &mut Vec<Row>) -> f64 {
    let mut rig = Rig::open("rows");
    let t = rig.t.id;
    let value = [5u8; 64];
    // Rows 0..32 have one cell, rows 100..132 have 33, rows 200..232 have 33 of 1 KiB.
    rig.put_rows(0..32, 1, &value);
    rig.put_rows(100..132, 33, &value);
    rig.put_rows(200..232, 33, &[6u8; 1024]);
    let mut copies = 0.0;
    for flush in [false, true] {
        if flush {
            rig.flush();
        }
        let snap = rig.db.snapshot().unwrap();
        let spec = ReadSpec::default();
        let read = |base: u32, rig: &Rig| {
            let keys = rows(base, base + 32);
            per_op(32, |i| {
                let r = &keys[(i % 32) as usize];
                assert!(rig.db.read_row(&snap, t, r, &spec).unwrap().is_some());
            })
        };
        let one = read(0, &rig);
        let many = read(100, &rig);
        let large = read(200, &rig);
        copies = (large.1 - many.1) / 33.0 / (1024.0 - 64.0);
        out.push(Row {
            path: if flush {
                "read_row, SST, cached"
            } else {
                "read_row, memtable"
            },
            unit: "row",
            allocs: one.0,
            bytes: one.1,
        });
        out.push(Row {
            path: if flush {
                "read_row, SST, cached"
            } else {
                "read_row, memtable"
            },
            unit: "extra cell",
            allocs: (many.0 - one.0) / 32.0,
            bytes: (many.1 - one.1) / 32.0,
        });
        // A scan of rows base..base+32, counting from the second row (the first sizes the
        // cursor's buffers).
        let scan = |base: u32, rig: &Rig| {
            let spec = ScanSpec::new(Bound::Included(row(base)), Bound::Excluded(row(base + 32)));
            let mut cur = rig.db.scan(&snap, t, spec).unwrap();
            assert!(cur.next_row().unwrap());
            while cur.next_cell().unwrap().is_some() {}
            let (a, b, cells) = counted(|| {
                let mut cells = 0u64;
                while cur.next_row().unwrap() {
                    while let Some(c) = cur.next_cell().unwrap() {
                        cells += c.stored.len().min(1) as u64;
                    }
                }
                cells
            });
            (a as f64 / 31.0, b as f64 / 31.0, cells)
        };
        let one = scan(0, &rig);
        let many = scan(100, &rig);
        assert_eq!((one.2, many.2), (31, 31 * 33));
        out.push(Row {
            path: if flush {
                "scan, SST, cached"
            } else {
                "scan, memtable"
            },
            unit: "row",
            allocs: one.0,
            bytes: one.1,
        });
        out.push(Row {
            path: if flush {
                "scan, SST, cached"
            } else {
                "scan, memtable"
            },
            unit: "extra cell",
            allocs: (many.0 - one.0) / 32.0,
            bytes: (many.1 - one.1) / 32.0,
        });
        drop(snap);
    }
    rig.close();
    copies
}

/// Commits: building the batch, submitting it and driving the shard until it resolves.
fn commits(out: &mut Vec<Row>) {
    let mut rig = Rig::open("commits");
    let value = [3u8; 100];
    let keys = rows(0, 512);
    let mut next = 0usize;
    for cells in [1u32, 16] {
        let (allocs, bytes) = per_op(64, |_| {
            next += 1;
            let wb = rig.batch(&keys[next], cells, &value);
            rig.commit(wb);
        });
        out.push(Row {
            path: if cells == 1 {
                "commit, 1 cell"
            } else {
                "commit, 16 cells"
            },
            unit: "commit",
            allocs,
            bytes,
        });
    }
    // Batched: 8 commits of 16 cells submitted before the shard runs, so they share one
    // group commit.
    let (allocs, bytes) = per_op(8, |_| {
        // The vector of pending commits is the test's, not the engine's: reserved
        // outside the count would not help (it is per op), so it is one allocation of 8
        // per group, a 1/8 allocation per commit.
        let pending: Vec<_> = (0..8)
            .map(|_| {
                next += 1;
                let wb = rig.batch(&keys[next], 16, &value);
                rig.db.submit(wb, Some(Durability::Buffered)).unwrap()
            })
            .collect();
        for p in pending {
            rig.wait(p).unwrap();
        }
    });
    out.push(Row {
        path: "commit, 16 cells, 8 per group",
        unit: "commit",
        allocs: allocs / 8.0,
        bytes: bytes / 8.0,
    });
    rig.close();
}

/// A put above the inline limit (192 KiB here): separated into a blob file as it commits.
fn large_put(out: &mut Vec<Row>) -> f64 {
    let mut rig = Rig::open("large");
    let value = vec![1u8; 1 << 20];
    // Steady state: the file already has the room. A dropped table's large values leave
    // free extents, so the measured puts don't grow the simulated file (whose in-memory
    // growth would count here, though a real file grows without allocating).
    let scratch = rig
        .db
        .create_table("scratch", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for key in rows(0, 8) {
        let mut wb = WriteBatch::new();
        wb.put(
            scratch.id,
            scratch.families[0].id,
            &key,
            b"q",
            None,
            ValueRef::Bytes(&value),
        )
        .unwrap();
        rig.commit(wb);
    }
    rig.db.drop_table(scratch.id).unwrap();
    rig.idle();
    let keys = rows(0, 8);
    let mut next = 0usize;
    let (allocs, bytes) = per_op(4, |_| {
        next += 1;
        let wb = rig.batch(&keys[next], 1, &value);
        rig.commit(wb);
    });
    out.push(Row {
        path: "commit, one 1 MiB put (separated)",
        unit: "commit",
        allocs,
        bytes,
    });
    rig.close();
    bytes / value.len() as f64
}

/// Flush and full compaction, per entry written.
fn maintenance(out: &mut Vec<Row>) {
    let mut rig = Rig::open("maint");
    let value = [4u8; 100];
    let entries = 4_000u64;
    // Warm-up: one flush and one compaction of other rows.
    rig.put_rows(50_000..50_100, 1, &value);
    rig.flush();
    rig.compact();
    rig.put_rows(0..4_000, 1, &value);
    let (a, b, ()) = counted(|| rig.flush());
    out.push(Row {
        path: "flush",
        unit: "entry",
        allocs: a as f64 / entries as f64,
        bytes: b as f64 / entries as f64,
    });
    // Overwrite the same rows, flush, then compact both runs together.
    rig.put_rows(0..4_000, 1, &value);
    rig.flush();
    let (a, b, ()) = counted(|| rig.compact());
    let input = 2 * entries + 100;
    out.push(Row {
        path: "compaction (full)",
        unit: "input entry",
        allocs: a as f64 / input as f64,
        bytes: b as f64 / input as f64,
    });
    rig.close();
}

/// Allocations per unit allowed on the hottest paths: today's values with a little headroom
/// (#320). Lower one when a change improves its path; a path over budget fails the test.
const BUDGETS: &[(&str, &str, f64)] = &[
    ("get, memtable (snapshot)", "get", 9.0),
    ("get_latest, memtable", "get", 9.0),
    ("get_latest, memtable miss", "get", 7.0),
    ("get_latest, SST, block cached", "get", 10.0),
    ("get, SST, block cached (snapshot)", "get", 10.0),
    ("read_row, memtable", "row", 16.0),
    ("read_row, memtable", "extra cell", 0.5),
    ("read_row, SST, cached", "row", 17.0),
    ("read_row, SST, cached", "extra cell", 0.5),
    ("scan, memtable", "row", 0.0),
    ("scan, memtable", "extra cell", 0.0),
    ("scan, SST, cached", "row", 0.0),
    ("scan, SST, cached", "extra cell", 0.0),
    ("commit, 1 cell", "commit", 15.0),
    ("commit, 16 cells", "commit", 18.0),
    ("commit, 16 cells, 8 per group", "commit", 16.0),
    ("flush", "entry", 0.15),
    ("compaction (full)", "input entry", 0.15),
];

/// Bytes allocated per value byte allowed: a large put (separated at commit) and a row read
/// from cached SST blocks. A large put copies the value once, into the batch (#320; it was
/// three times with the simulated file's growth), and the simulated file keeps one more
/// copy of every write it has not synced (a real file does not), so about 2.0. A row read
/// copies each value once into a buffer that grows by doubling, so its reallocations move
/// the bytes again (about 2.4 today).
const LARGE_PUT_COPIES: f64 = 2.1;
const ROW_READ_COPIES: f64 = 2.6;

#[test]
fn allocations_per_operation() {
    let mut out = Vec::new();
    gets(&mut out);
    let row_copies = rows_and_scans(&mut out);
    commits(&mut out);
    let large_copies = large_put(&mut out);
    maintenance(&mut out);
    println!("| Path | Per | Allocations | Bytes |");
    println!("|---|---|--:|--:|");
    for r in &out {
        println!(
            "| {} | {} | {:.2} | {:.0} |",
            r.path, r.unit, r.allocs, r.bytes
        );
    }
    println!();
    println!(
        "Bytes allocated per value byte: large put {large_copies:.2}, row read {row_copies:.2}"
    );
    let mut over = Vec::new();
    for &(path, unit, budget) in BUDGETS {
        let r = out
            .iter()
            .find(|r| r.path == path && r.unit == unit)
            .unwrap_or_else(|| panic!("no measurement for budget {path} / {unit}"));
        if r.allocs > budget {
            over.push(format!(
                "{path}: {:.2} allocations per {unit}, budget {budget}",
                r.allocs
            ));
        }
    }
    if large_copies > LARGE_PUT_COPIES {
        over.push(format!(
            "large put: {large_copies:.2} bytes per value byte, budget {LARGE_PUT_COPIES}"
        ));
    }
    if row_copies > ROW_READ_COPIES {
        over.push(format!(
            "row read: {row_copies:.2} bytes per value byte, budget {ROW_READ_COPIES}"
        ));
    }
    assert!(
        over.is_empty(),
        "over the allocation budget (#320):\n{}",
        over.join("\n")
    );
}

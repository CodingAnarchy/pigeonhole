//! Issue #185: a database at rest after `compact` and `shrink` is about as large as its
//! data. Outputs below L0 are cut into power-of-two pieces of at most half the remaining
//! stream (`pigeonhole_compaction::output_piece_bytes`), so they pack below one another;
//! before, a file was at least twice its largest extent (1.2–3.8× its data on these
//! shapes). File sizes only, on `SimVfs` and an application-owned shard (so no
//! background work races the shrink).

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{Engine, EngineOptions, EngineShard, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{OpenOptions, Vfs};

const DB: &str = "/db/footprint.phdb";

/// The header unit, a 64 KiB manifest snapshot and the 256 KiB manifest log extent.
const METADATA: u64 = 384 << 10;

/// Runs `shard` until `f` resolves.
fn wait<F: Future + Unpin>(shard: &mut EngineShard, mut f: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(r) = Pin::new(&mut f).poll(&mut cx) {
            return r;
        }
        shard.run_once(u64::MAX);
    }
}

/// Loads `mib` MiB of 1 KiB incompressible values, keeps one row in `100 / keep_pct`,
/// compacts and shrinks; returns `(file length, live SST bytes, SSTs)`.
fn at_rest(mib: u64, keep_pct: u64) -> (u64, u64, usize) {
    let vfs = SimVfs::new(185);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.tablet_changes = false;
    o.memtable_budget = 64 << 20;
    o.wal.segment_size = 4 << 20;
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
    let mut shard = shards.remove(0);
    while shard.run_once(u64::MAX) {}
    let commit = |shard: &mut EngineShard, wb| {
        let pending = db.submit(wb, Some(Durability::None)).unwrap();
        wait(shard, pending).unwrap();
    };
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let rows = mib * 1024;
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let mut i = 0;
    while i < rows {
        let mut wb = WriteBatch::new();
        for _ in 0..64.min(rows - i) {
            let v: Vec<u8> = (0..1024)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x as u8
                })
                .collect();
            let row = format!("r{i:09}");
            wb.put(
                t.id,
                t.families[0].id,
                row.as_bytes(),
                b"q",
                None,
                ValueRef::Bytes(&v),
            )
            .unwrap();
            i += 1;
        }
        commit(&mut shard, wb);
    }
    if keep_pct < 100 {
        let every = 100 / keep_pct;
        let mut wb = WriteBatch::new();
        for r in (0..rows).filter(|r| r % every != 0) {
            wb.delete_row(t.id, format!("r{r:09}").as_bytes(), None)
                .unwrap();
            if wb.len() >= 256 {
                commit(&mut shard, std::mem::take(&mut wb));
            }
        }
        if !wb.is_empty() {
            commit(&mut shard, wb);
        }
    }
    wait(&mut shard, db.compact_pending(None).unwrap()).unwrap();
    while shard.run_once(u64::MAX) {}
    db.shrink().unwrap();
    let ssts = db.sst_lens();
    let live: u64 = ssts.iter().map(|s| s.1).sum();
    let len = vfs
        .open(Path::new(DB), OpenOptions::read())
        .unwrap()
        .len()
        .unwrap();
    db.close().unwrap();
    while shard.closed().is_none() {
        shard.run_once(u64::MAX);
    }
    (len, live, ssts.len())
}

#[test]
#[cfg_attr(miri, ignore = "loads tens of MiB")]
fn a_compacted_and_shrunk_file_is_about_its_data() {
    for (mib, keep_pct) in [(5, 100), (20, 100), (50, 100), (50, 10), (50, 1)] {
        let (len, live, ssts) = at_rest(mib, keep_pct);
        let ratio = len as f64 / live as f64;
        eprintln!(
            "load {mib} MiB, kept {keep_pct}%: file {len}, live {live} ({ratio:.2}x), {ssts} SSTs"
        );
        // The last piece of a stream rounds up to a power of two: small data has the most
        // slack (0.5 MiB of data: about 1.2x). Besides the SSTs the file holds a fixed
        // `METADATA`: the header unit, the manifest snapshot and the manifest log's extent.
        let bound = if live < 1 << 20 { 1.3 } else { 1.2 };
        assert!(
            len as f64 <= live as f64 * bound + METADATA as f64,
            "load {mib} MiB, kept {keep_pct}%: the file is {ratio:.2}x its data ({len} for {live})"
        );
    }
}

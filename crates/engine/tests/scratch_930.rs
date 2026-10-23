//! #930 scratch (not for merge): reader-process vs writer-process row reads of the same
//! hot rows, on real files. Prints time per read and block-cache lookups per read.
#![cfg(unix)]

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, FamilyOptions, ReadSpec, RowData, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;

fn options(vfs: VfsRef) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.compaction_threads = std::env::var("R930_COMPACT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    o.memtable_budget = 64 << 20;
    o.row_cache_bytes = 0;
    o
}

#[test]
fn reader_vs_writer_row_reads() {
    let dir = std::env::temp_dir().join(format!("phdb-930-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("s.phdb");
    let vfs: VfsRef = pigeonhole_io::pread::PreadVfs::new(2);
    let db = Engine::open(&path, options(Arc::clone(&vfs))).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let rows: Vec<Vec<u8>> = (0..16u32)
        .map(|i| format!("user{i:020}").into_bytes())
        .collect();
    let rounds: u32 = std::env::var("R930_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40);
    for round in 0..rounds {
        for r in &rows {
            for f in 0..10u32 {
                let mut wb = WriteBatch::new();
                wb.put(
                    t.id,
                    t.families[0].id,
                    r,
                    format!("field{f}").as_bytes(),
                    None,
                    ValueRef::Bytes(&[round as u8; 100]),
                )
                .unwrap();
                db.commit(wb, Some(Durability::Buffered)).unwrap();
            }
        }
        if round % 8 == 7 {
            db.flush().unwrap();
        }
    }
    // A larger key space, so the database holds many SSTs (as the bench's 1M rows do).
    let keys: u32 = std::env::var("R930_KEYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    for k in 0..keys {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            t.families[0].id,
            format!("key{k:020}").as_bytes(),
            b"field0",
            None,
            ValueRef::Bytes(&[7u8; 100]),
        )
        .unwrap();
        db.commit(wb, Some(Durability::Buffered)).unwrap();
    }
    db.flush().unwrap();
    let reader = Engine::open_reader(Path::new(&path), options(Arc::clone(&vfs))).unwrap();
    let mut spec = ReadSpec::default();
    spec.versions = 1;
    let n = 200_000u32;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let churn = std::env::var("R930_CHURN").is_ok();
    let writer_thread = churn.then(|| {
        let (db, t, rows, stop) = (
            Arc::clone(&db),
            Arc::clone(&t),
            rows.clone(),
            Arc::clone(&stop),
        );
        let keys = keys;
        std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let mut wb = WriteBatch::new();
                let cold;
                let r: &[u8] = if i % 2 == 0 {
                    &rows[(i / 2 % 16) as usize]
                } else {
                    cold = format!("key{:020}", (i.wrapping_mul(2654435761)) % u64::from(keys));
                    cold.as_bytes()
                };
                wb.put(
                    t.id,
                    t.families[0].id,
                    r,
                    format!("field{}", i % 10).as_bytes(),
                    None,
                    ValueRef::Bytes(&[i as u8; 100]),
                )
                .unwrap();
                db.commit(wb, Some(Durability::Buffered)).unwrap();
                i += 1;
            }
            i
        })
    });
    for (name, e) in [
        ("writer", &db),
        ("reader", &reader),
        ("writer2", &db),
        ("reader2", &reader),
    ] {
        let mut sink = RowData::default();
        for i in 0..20_000u32 {
            e.read_row_latest_into(t.id, &rows[(i % 16) as usize], &[], &spec, &mut sink)
                .unwrap();
        }
        let (h0, m0) = e.metrics().block_cache;
        let start = std::time::Instant::now();
        for i in 0..n {
            sink = RowData::default();
            assert!(
                e.read_row_latest_into(t.id, &rows[(i % 16) as usize], &[], &spec, &mut sink)
                    .unwrap()
            );
        }
        let el = start.elapsed();
        let (h1, m1) = e.metrics().block_cache;
        eprintln!(
            "{name}: {:.0} ns/read, block cache lookups/read {:.2} (hits {:.2}, misses {:.3}), cells {}",
            el.as_nanos() as f64 / n as f64,
            ((h1 - h0) + (m1 - m0)) as f64 / n as f64,
            (h1 - h0) as f64 / n as f64,
            (m1 - m0) as f64 / n as f64,
            sink.cells.len()
        );
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    if let Some(w) = writer_thread {
        eprintln!("writes during the run: {}", w.join().unwrap());
    }
    drop(reader);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

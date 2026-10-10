//! A point get skips the marker seek on memtables whose writer never inserted a delete
//! marker (ICR 0020). These tests race deletes against gets: a get that starts after a row
//! delete or family delete returned must never see the deleted cell, even in the window
//! where the memtable's has-markers flag is first set, and reader threads must agree with
//! the sync result at every step.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pigeonhole::{Family, Options, Pigeonhole};

fn row(i: u64) -> Vec<u8> {
    format!("row{i:08}").into_bytes()
}

/// Phases: `2i + 1` once row `i` is written, `2i + 2` once it is deleted. Readers check
/// that a get started in phase `2i + 2` or later sees no cell in row `i`. The writer flushes
/// often, so many fresh memtables (flag clear) see their first delete while readers read.
fn race(delete_family: bool) {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("m.phdb"), Options::default().shards(2)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .family("g", Family::default())
        .create_if_missing()
        .unwrap();
    let phase = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    std::thread::scope(|s| {
        for _ in 0..3 {
            let (t, phase, stop) = (&t, Arc::clone(&phase), Arc::clone(&stop));
            s.spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    let p = phase.load(Ordering::SeqCst);
                    if p >= 2 && p % 2 == 0 {
                        let i = p / 2 - 1;
                        let got = t.get(&row(i), "f", b"q").unwrap();
                        assert!(got.is_none(), "row {i} read after its delete (phase {p})");
                    }
                }
            });
        }
        for i in 0..2_000u64 {
            t.mutate(&row(i))
                .put("f", b"q", b"v")
                .put("g", b"q", b"v")
                .commit()
                .unwrap();
            phase.store(2 * i + 1, Ordering::SeqCst);
            let m = t.mutate(&row(i));
            let m = if delete_family {
                m.delete_family("f")
            } else {
                m.delete_row()
            };
            m.commit().unwrap();
            phase.store(2 * i + 2, Ordering::SeqCst);
            if i % 50 == 49 {
                db.flush().unwrap();
            }
        }
        stop.store(true, Ordering::Release);
    });
    drop(t);
    db.close().unwrap();
}

#[test]
fn a_get_never_sees_a_row_deleted_before_it_started() {
    race(false);
}

#[test]
fn a_get_never_sees_a_family_deleted_before_it_started() {
    race(true);
}

/// Rows without deletes in a memtable that has some: the marker seek runs and still finds
/// the right cells; rows with deletes in older memtables or SSTs are still hidden.
#[test]
fn markers_in_other_sources_still_hide_cells() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("m.phdb"), Options::default().shards(1)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    t.mutate(b"a").put("f", b"q", b"1").commit().unwrap();
    t.mutate(b"a").delete_family("f").commit().unwrap();
    db.flush().unwrap(); // the marker is in an SST now; the new memtable has none
    assert!(t.get(b"a", "f", b"q").unwrap().is_none());
    t.mutate(b"b").put("f", b"q", b"2").commit().unwrap(); // a memtable without markers
    assert_eq!(t.get(b"b", "f", b"q").unwrap().unwrap().value(), b"2");
    assert!(t.get(b"a", "f", b"q").unwrap().is_none());
    // A put older than an SST's family delete, re-written into the fresh memtable at an
    // explicit timestamp below the delete: still hidden by the SST's marker.
    t.mutate(b"a")
        .put_at("f", b"q", 1, b"old")
        .commit()
        .unwrap();
    assert!(t.get(b"a", "f", b"q").unwrap().is_none());
    drop(t);
    db.close().unwrap();
}

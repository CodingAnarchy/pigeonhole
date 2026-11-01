//! D306 PR 2 (#490 D, ICR 0100): bulk loading, and background rebalancing. A bulk load into
//! an empty index lands every vector (a repeated id keeps its last) with the invariants and
//! an exact search; it refuses an index that holds lists. With `Rebalance::Background`,
//! writes only mark the lists they push out of bounds: the crate's thread
//! (`Index::open`) or an application's `Rebalancer` (`open_with_rebalancer`) splits and
//! merges them, including after a crash left markers behind.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use common::{Rng, V, check, exact, open, specs};
use pigeonhole::{Element, Metric, VectorSpec};
use pigeonhole_vector::{Error, Index, IndexOptions, Rebalance};

fn out_of_bounds(index: &Index) -> bool {
    let lens = index.verify().unwrap().list_lens;
    lens.iter()
        .any(|n| *n > index.max_list_len() || (lens.len() > 1 && *n < index.min_list_len()))
}

fn vectors(spec: &VectorSpec, n: u32, seed: u64) -> Vec<(Vec<u8>, V)> {
    let mut rng = Rng::new(seed);
    (0..n)
        .map(|i| {
            let c = rng.below(8);
            (i.to_be_bytes().to_vec(), V::random(&mut rng, spec, c))
        })
        .collect()
}

fn bulk(index: &Index, expected: u64, items: &[(Vec<u8>, V)]) -> pigeonhole_vector::Result<u64> {
    let stats = match &items.first().map(|i| &i.1) {
        Some(V::I8(_)) => index.bulk_load_i8(
            expected,
            items.iter().map(|(k, v)| match v {
                V::I8(x) => (k, x.clone()),
                _ => unreachable!(),
            }),
        ),
        Some(V::Bits(_)) => index.bulk_load_bits(
            expected,
            items.iter().map(|(k, v)| match v {
                V::Bits(x) => (k, x.clone()),
                _ => unreachable!(),
            }),
        ),
        _ => index.bulk_load_f32(
            expected,
            items.iter().map(|(k, v)| match v {
                V::F32(x) => (k, x.clone()),
                _ => unreachable!(),
            }),
        ),
    }?;
    Ok(stats.vectors)
}

#[test]
fn a_bulk_load_lands_every_vector_within_bounds_and_searches_exactly() {
    for spec in specs() {
        let ctx = format!("{spec:?}");
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let index =
            Index::create(&db, "idx", spec, IndexOptions::new().target_list_len(16)).unwrap();
        let mut items = vectors(&spec, 1500, 9);
        // Repeats: the last vector of an id wins.
        let again = vectors(&spec, 50, 10);
        items.extend(again.iter().cloned());
        assert_eq!(bulk(&index, 1500, &items).unwrap(), 1550, "{ctx}");
        let model: BTreeMap<Vec<u8>, V> = items.into_iter().collect();
        check(&index, &model, &ctx);
        let mut rng = Rng::new(11);
        for _ in 0..5 {
            let c = rng.below(8);
            let q = V::random(&mut rng, &spec, c);
            exact(&index, &model, &q, 10, &ctx);
        }
        // A second bulk load is refused: the index holds lists.
        assert!(matches!(
            bulk(&index, 10, &vectors(&spec, 10, 12)),
            Err(Error::InvalidArgument(_))
        ));
        db.close().unwrap();
    }
}

#[test]
fn a_bulk_load_refuses_a_vector_of_another_shape() {
    let spec = VectorSpec::new(4, Element::F32, Metric::L2).unwrap();
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec, IndexOptions::new().target_list_len(8)).unwrap();
    let bad: Vec<(Vec<u8>, Vec<f32>)> = vec![(b"a".to_vec(), vec![1.0, 2.0])];
    assert!(matches!(
        index.bulk_load_f32(1, bad),
        Err(Error::InvalidArgument(_))
    ));
    let none: Vec<(Vec<u8>, Vec<f32>)> = Vec::new();
    assert_eq!(index.bulk_load_f32(0, none).unwrap().vectors, 0);
    db.close().unwrap();
}

fn background() -> IndexOptions {
    IndexOptions::new()
        .target_list_len(8)
        .rebalance(Rebalance::Background)
}

#[test]
fn the_crates_thread_rebalances_in_the_background() {
    let spec = VectorSpec::new(4, Element::F32, Metric::L2).unwrap();
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec, background()).unwrap();
    assert_eq!(index.options().rebalance_mode(), Rebalance::Background);
    let mut model = BTreeMap::new();
    for (id, v) in vectors(&spec, 400, 3) {
        v.insert(&index, &id).unwrap();
        model.insert(id, v);
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while out_of_bounds(&index) {
        assert!(Instant::now() < deadline, "the rebalancer never caught up");
        std::thread::sleep(Duration::from_millis(20));
    }
    check(&index, &model, "after the background rebalance");
    let q = V::random(&mut Rng::new(4), &spec, 2);
    exact(&index, &model, &q, 10, "after the background rebalance");
    drop(index);
    db.close().unwrap();
}

#[test]
fn an_applications_rebalancer_splits_what_the_writes_marked() {
    let spec = VectorSpec::new(4, Element::F32, Metric::L2).unwrap();
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (index, mut rebalancer) =
        Index::create_with_rebalancer(&db, "idx", spec, background()).unwrap();
    let woken = Arc::new(AtomicUsize::new(0));
    let w = Arc::clone(&woken);
    rebalancer.set_wakeup(Box::new(move || {
        w.fetch_add(1, Ordering::Relaxed);
    }));
    let mut model = BTreeMap::new();
    for (id, v) in vectors(&spec, 200, 5) {
        v.insert(&index, &id).unwrap();
        model.insert(id, v);
    }
    // Nothing split them: the application hasn't run its rebalancer.
    assert!(out_of_bounds(&index), "writes only marked");
    assert!(woken.load(Ordering::Relaxed) > 0, "the wakeup ran");
    // As the doc's loop: call again while it says so.
    while rebalancer.run_once(Duration::from_millis(50)).unwrap() {}
    check(&index, &model, "after the application's rebalancer");
    assert!(!rebalancer.closed());
    drop(index);
    assert!(rebalancer.closed(), "the last handle dropped");
    assert!(!rebalancer.run_once(Duration::from_millis(50)).unwrap());
    db.close().unwrap();
}

#[test]
fn marks_a_crash_left_are_rebalanced_after_the_reopen() {
    let spec = VectorSpec::new(4, Element::F32, Metric::L2).unwrap();
    let dir = pigeonhole::doc_support::temp_dir();
    let mut model = BTreeMap::new();
    {
        let db = open(&dir);
        let (index, rebalancer) =
            Index::create_with_rebalancer(&db, "idx", spec, background()).unwrap();
        for (id, v) in vectors(&spec, 200, 6) {
            v.insert(&index, &id).unwrap();
            model.insert(id, v);
        }
        assert!(out_of_bounds(&index));
        // The process stops before its rebalancer ran.
        drop((index, rebalancer));
        db.close().unwrap();
    }
    let db = open(&dir);
    let (index, mut rebalancer) = Index::open_with_rebalancer(&db, "idx").unwrap();
    // A zero budget is a machine too slow to finish any list in it (Windows CI on #1271):
    // each call still does one list, and says to call again while any is left.
    let mut calls = 0;
    while rebalancer.run_once(Duration::ZERO).unwrap() {
        calls += 1;
        assert!(calls < 10_000, "run_once never finished");
    }
    check(&index, &model, "after the reopen's rebalance");
    db.close().unwrap();
}

/// simp724's R1 on #1271: an application that drops its handles without `close()` and
/// reopens isn't held off by the rebalancer. Once the last `Index` handle is gone the
/// rebalancer stops without taking the index again, so it holds no database handle.
#[test]
fn a_rebalancer_holds_no_database_handle_after_the_last_index_drops() {
    let spec = VectorSpec::new(4, Element::F32, Metric::L2).unwrap();
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (index, mut rebalancer) =
        Index::create_with_rebalancer(&db, "idx", spec, background()).unwrap();
    for (id, v) in vectors(&spec, 100, 7) {
        v.insert(&index, &id).unwrap();
    }
    assert!(out_of_bounds(&index), "lists are marked");
    drop(index);
    assert!(rebalancer.closed());
    assert!(
        !rebalancer.run_once(Duration::from_secs(1)).unwrap(),
        "it stops"
    );
    // No close(): dropping the last handle releases the database, so it reopens at once.
    drop(db);
    let db = open(&dir);
    let (index, mut rebalancer) = Index::open_with_rebalancer(&db, "idx").unwrap();
    while rebalancer.run_once(Duration::ZERO).unwrap() {}
    assert!(
        !out_of_bounds(&index),
        "the reopen's rebalancer did the marked work"
    );
    drop((index, rebalancer));
    db.close().unwrap();
}

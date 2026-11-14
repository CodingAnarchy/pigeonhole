//! D306's races and crash windows (§8, rev 3 M3, rev 4 N1), held deterministically with the
//! `__unstable-hooks` seams. The seams are process-wide, so these tests run one at a time.

mod common;

use std::collections::BTreeMap;
use std::sync::Mutex;

use common::{V, check, exact, open};
use pigeonhole::{Durability, Element, Metric, Options, Pigeonhole, RetryPolicy, VectorSpec};
use pigeonhole_vector::hooks::{self, Hold};
use pigeonhole_vector::{Error, Index, IndexOptions, Placed, Rebalance};

static SERIAL: Mutex<()> = Mutex::new(());

fn spec() -> VectorSpec {
    VectorSpec::new(2, Element::F32, Metric::L2).unwrap()
}

/// Rev 4 N1: a delete and an in-place update committed between a split's 2-means snapshot
/// and its transaction. The split skips the deleted id and moves the updated value, never
/// the snapshot's.
#[test]
fn a_split_moves_what_its_own_reads_return() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec(), IndexOptions::new().target_list_len(4)).unwrap();
    let mut model = BTreeMap::new();
    // Two groups in the one list, so the split moves one of them whole.
    for i in 0..8u32 {
        let v = V::F32(vec![if i % 2 == 0 { 0.0 } else { 100.0 }, i as f32 * 0.01]);
        v.insert(&index, &i.to_be_bytes()).unwrap();
        model.insert(i.to_be_bytes().to_vec(), v);
    }
    let held = hooks::arm(Hold::SplitBeforeCommit);
    let splitter = {
        let index = index.clone();
        std::thread::spawn(move || index.insert_f32(&8u32.to_be_bytes(), &[100.0, 0.08]))
    };
    held.wait();
    model.insert(8u32.to_be_bytes().to_vec(), V::F32(vec![100.0, 0.08]));
    // Both groups lose one: whichever moves, the split meets a delete and an update.
    for i in [0u32, 1] {
        assert!(index.delete(&i.to_be_bytes()).unwrap());
        model.remove(i.to_be_bytes().as_slice());
    }
    for i in [2u32, 3] {
        let v = V::F32(vec![if i % 2 == 0 { 0.5 } else { 100.5 }, 7.0]);
        v.insert(&index, &i.to_be_bytes()).unwrap();
        model.insert(i.to_be_bytes().to_vec(), v);
    }
    held.release();
    splitter.join().unwrap().unwrap();
    assert!(index.verify().unwrap().list_lens.len() >= 2, "it split");
    check(&index, &model, "after the held split");
    for i in [2u32, 3] {
        exact(
            &index,
            &model,
            &model[&i.to_be_bytes().to_vec()].clone(),
            1,
            &format!("updated {i}"),
        );
    }
    db.close().unwrap();
}

/// Rev 3 M3: a write commits and the process dies before its split or merge. Reopening
/// queues the lists outside their bounds, and the next write rebalances them.
#[test]
fn reopening_rebalances_what_a_crash_left_out_of_bounds() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = pigeonhole::doc_support::temp_dir();
    let mut model = BTreeMap::new();
    {
        let db = open(&dir);
        let index =
            Index::create(&db, "idx", spec(), IndexOptions::new().target_list_len(4)).unwrap();
        for i in 0..40u32 {
            let v = V::F32(vec![i as f32, 0.0]);
            v.insert(&index, &i.to_be_bytes()).unwrap();
            model.insert(i.to_be_bytes().to_vec(), v);
        }
        hooks::skip_rebalance(true);
        // Over a bound in one list, under it in another.
        for i in 100..120u32 {
            let v = V::F32(vec![0.5, i as f32 * 0.001]);
            v.insert(&index, &i.to_be_bytes()).unwrap();
            model.insert(i.to_be_bytes().to_vec(), v);
        }
        for i in 30..40u32 {
            index.delete(&i.to_be_bytes()).unwrap();
            model.remove(i.to_be_bytes().as_slice());
        }
        hooks::skip_rebalance(false);
        let stats = index.verify().unwrap();
        assert!(
            stats.list_lens.iter().any(|n| *n > index.max_list_len())
                && stats.list_lens.iter().any(|n| *n < index.min_list_len()),
            "the crash window: {:?}",
            stats.list_lens
        );
        db.close().unwrap();
    }
    let db = open(&dir);
    let index = Index::open(&db, "idx").unwrap();
    // The first write after the reopen runs the queued work, whatever list it touches.
    let v = V::F32(vec![15.0, 1.0]);
    v.insert(&index, b"after").unwrap();
    model.insert(b"after".to_vec(), v);
    check(&index, &model, "after the reopen's first write");
    db.close().unwrap();
}

/// One-dimensional points, so placement and splits are worked out by hand.
fn put(index: &Index, model: &mut BTreeMap<Vec<u8>, V>, id: &str, x: f32) {
    let v = V::F32(vec![x, 0.0]);
    v.insert(index, id.as_bytes()).unwrap();
    model.insert(id.as_bytes().to_vec(), v);
}

/// Builds, with target 4 (lists split above 8) and one neighbour reassigned:
/// - list 0 = {6, x = 4} (centroid 6), list 1 = {a0..a7 at 0.0..0.7} (centroid 0.35) and
///   list 2 = {b0..b7 at 10.0..10.7} (centroid 10.35);
///
/// so that the next insert of `z` at 3.0 splits list 1 into {z} (centroid 3) and a new list
/// of the a's, and phase 2 then moves x (now nearer 3 than 6) out of list 0.
fn three_lists(index: &Index, model: &mut BTreeMap<Vec<u8>, V>) {
    for i in 0..8 {
        put(index, model, &format!("a{i}"), i as f32 * 0.1);
    }
    for i in 0..8 {
        put(index, model, &format!("b{i}"), 10.0 + i as f32 * 0.1);
    }
    put(index, model, "y", 6.0);
    put(index, model, "x", 4.0);
}

fn reassigning() -> IndexOptions {
    IndexOptions::new()
        .target_list_len(4)
        .reassign_neighbours(1)
}

/// #1268: the opposite commit order to N1's test. A delete, then an in-place update, sit
/// between their reads and their commits while a split of their list commits. Each
/// conflicts (the split bumped the list's version and rewrote the moved ids' mappings),
/// retries, and lands against the new layout.
#[test]
fn an_op_held_across_a_split_of_its_list_retries_against_the_new_layout() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for hold in [Hold::DeleteBeforeCommit, Hold::PlaceBeforeCommit] {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let index =
            Index::create(&db, "idx", spec(), IndexOptions::new().target_list_len(4)).unwrap();
        let mut model = BTreeMap::new();
        for i in 0..8 {
            put(&index, &mut model, &format!("a{i}"), i as f32 * 0.1);
        }
        let conflicts = db.engine_metrics().transaction_conflicts;
        let held = hooks::arm(hold);
        let op = {
            let index = index.clone();
            std::thread::spawn(move || match hold {
                Hold::DeleteBeforeCommit => index.delete(b"a3").map(|_| ()),
                _ => index.insert_f32(b"a3", &[0.33, 0.0]),
            })
        };
        assert!(
            held.wait_for(std::time::Duration::from_secs(10)),
            "{hold:?} reached"
        );
        // The ninth vector splits the one list while the op is held.
        put(&index, &mut model, "b0", 10.0);
        assert!(
            index.verify().unwrap().list_lens.len() >= 2,
            "{hold:?}: it split"
        );
        held.release();
        op.join().unwrap().unwrap();
        match hold {
            Hold::DeleteBeforeCommit => {
                model.remove(b"a3".as_slice());
            }
            _ => {
                model.insert(b"a3".to_vec(), V::F32(vec![0.33, 0.0]));
            }
        }
        assert!(
            db.engine_metrics().transaction_conflicts > conflicts,
            "{hold:?}: the held op conflicted"
        );
        check(&index, &model, &format!("{hold:?}"));
        exact(
            &index,
            &model,
            &V::F32(vec![0.33, 0.0]),
            3,
            &format!("{hold:?}"),
        );
        db.close().unwrap();
    }
}

/// #1268: phase 2 against a split of its destination. Phase 2 of list 1's split is held
/// with its move of x into list 1 written; a split of the other destination (the new list)
/// commits meanwhile; phase 2's commit then conflicts on that list's version and is
/// skipped, and every invariant holds.
#[test]
fn phase_2_against_a_split_of_its_destination_is_skipped() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec(), reassigning()).unwrap();
    let mut model = BTreeMap::new();
    three_lists(&index, &mut model);
    let held = hooks::arm(Hold::ReassignBeforeCommit);
    let splitter = {
        let index = index.clone();
        std::thread::spawn(move || index.insert_f32(b"z", &[3.0, 0.0]))
    };
    assert!(
        held.wait_for(std::time::Duration::from_secs(10)),
        "phase 2 reached"
    );
    model.insert(b"z".to_vec(), V::F32(vec![3.0, 0.0]));
    let lists = index.verify().unwrap().list_lens.len();
    // The a's are the new list now; one more near them splits it.
    put(&index, &mut model, "a8", 0.25);
    assert!(
        index.verify().unwrap().list_lens.len() > lists,
        "the destination split"
    );
    let conflicts = db.engine_metrics().transaction_conflicts;
    held.release();
    splitter.join().unwrap().unwrap();
    assert!(
        db.engine_metrics().transaction_conflicts > conflicts,
        "phase 2 conflicted"
    );
    check(&index, &model, "after phase 2's conflict");
    exact(
        &index,
        &model,
        &V::F32(vec![4.0, 0.0]),
        4,
        "after phase 2's conflict",
    );
    db.close().unwrap();
}

/// #1268: a crash between a split's commit and its phase 2. The split is consistent on its
/// own; after a reopen every invariant holds and a search probing every list is exact.
#[test]
fn a_crash_between_a_split_and_its_phase_2_leaves_a_consistent_index() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = pigeonhole::doc_support::temp_dir();
    let mut model = BTreeMap::new();
    {
        let db = open(&dir);
        let index = Index::create(&db, "idx", spec(), reassigning()).unwrap();
        three_lists(&index, &mut model);
        hooks::fail_after_split(true);
        put(&index, &mut model, "z", 3.0);
        hooks::fail_after_split(false);
        db.close().unwrap();
    }
    let db = open(&dir);
    let index = Index::open(&db, "idx").unwrap();
    check(&index, &model, "after the reopen");
    for q in [0.3f32, 3.0, 4.0, 6.0, 10.3] {
        exact(
            &index,
            &model,
            &V::F32(vec![q, 0.0]),
            5,
            &format!("query {q}"),
        );
    }
    db.close().unwrap();
}

/// Counts a list row as no bytes while it lives, so a split's whole-read bound is four split
/// lengths and a test's long list is split a page at a time (#1879: the bound is what a
/// split holds in 16 MiB otherwise).
struct PagedSplits;

impl PagedSplits {
    fn on() -> Self {
        hooks::split_row_bytes(Some(0));
        PagedSplits
    }
}

impl Drop for PagedSplits {
    fn drop(&mut self) {
        hooks::split_row_bytes(None);
    }
}

/// #1824: a list far longer than its split length (a skewed bulk load, or a list that outgrew
/// a background rebalancer; here, writes made while rebalancing was skipped) is split from a
/// sample and moved a page at a time. No read of the split or its phase 2 holds more than
/// four split lengths ([`PagedSplits`]), and the index ends within its bounds holding exactly the model.
/// The long list's vectors: two groups, or all one point, which leaves the split nothing to
/// separate (split by order).
///
/// #1879: with the real bound, 16 MiB held, the same list (about 50 KiB held) is read whole,
/// so it's split in one transaction rather than a page of eight vectors at a time.
#[derive(Clone, Copy)]
enum Shape {
    TwoGroups,
    OnePoint,
}

fn a_long_list_splits_without_reading_it_whole(shape: Shape) {
    a_long_list_splits(shape, true);
}

fn a_long_list_splits(shape: Shape, paged: bool) {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let _paged = paged.then(PagedSplits::on);
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (index, mut rebalancer) = Index::create_with_rebalancer(
        &db,
        "idx",
        spec(),
        reassigning().rebalance(Rebalance::Background),
    )
    .unwrap();
    let drain = |r: &mut pigeonhole_vector::Rebalancer| {
        while r.run_once(std::time::Duration::from_secs(60)).unwrap() {}
    };
    let mut model = BTreeMap::new();
    for i in 0..40 {
        put(&index, &mut model, &format!("s{i:03}"), i as f32 * 10.0);
    }
    drain(&mut rebalancer);
    let lists = index.verify().unwrap().list_lens.len();
    let max = index.max_list_len();
    hooks::skip_rebalance(true);
    // 60 split lengths into the list nearest 200.
    for i in 0..60 * max {
        let x = match (shape, i % 2) {
            (Shape::OnePoint, _) => 200.0,
            (Shape::TwoGroups, 0) => 199.0 + (i % 7) as f32 * 0.01,
            (Shape::TwoGroups, _) => 201.0 + (i % 5) as f32 * 0.01,
        };
        put(&index, &mut model, &format!("l{i:05}"), x);
    }
    hooks::skip_rebalance(false);
    let lens = index.verify().unwrap().list_lens;
    assert_eq!(lens.len(), lists);
    assert!(
        lens.iter().any(|n| *n > 60 * max),
        "one long list: {lens:?}"
    );
    hooks::take_split_held();
    // The next write into it marks it for the rebalancer.
    put(&index, &mut model, "trigger", 200.0);
    drain(&mut rebalancer);
    let held = hooks::take_split_held();
    assert!(held > 0, "a split ran");
    if paged {
        assert!(held as u64 <= 4 * max, "a read held {held} > {}", 4 * max);
    } else {
        assert!(held as u64 > 60 * max, "read whole: held {held}");
    }
    check(&index, &model, "after the long list's split");
    for q in [0.0, 200.0, 199.0, 201.0, 390.0] {
        exact(
            &index,
            &model,
            &V::F32(vec![q, 0.0]),
            5,
            &format!("query {q}"),
        );
    }
    drop(rebalancer);
    db.close().unwrap();
}

#[test]
fn a_long_list_of_two_groups_splits_without_reading_it_whole() {
    a_long_list_splits_without_reading_it_whole(Shape::TwoGroups);
}

#[test]
fn a_long_list_of_one_point_splits_without_reading_it_whole() {
    a_long_list_splits_without_reading_it_whole(Shape::OnePoint);
}

#[test]
fn a_long_but_small_list_splits_whole() {
    a_long_list_splits(Shape::TwoGroups, false);
}

/// Inline mode until every list is within its bounds: each write runs a bounded number of
/// queued splits and merges, so write (into `model` too) until none is left.
fn settle(index: &Index, model: &mut BTreeMap<Vec<u8>, V>, ctx: &str) {
    let (min, max) = (index.min_list_len(), index.max_list_len());
    for n in 0..500 {
        let lens = index.verify().unwrap().list_lens;
        let several = lens.len() > 1;
        if lens.iter().all(|l| *l <= max && (*l >= min || !several)) {
            return;
        }
        put(
            index,
            model,
            &format!("settle{n:03}"),
            10.0 + n as f32 * 0.001,
        );
    }
    panic!("{ctx}: not settled");
}

/// #1824, inside a paged split (audit120's S2 on #1861): held at its second page with
/// movers, after the first page committed the split. Mid-pass the index holds every vector
/// exactly once (`verify`, and exact searches over every list). A copy taken there (the
/// state a crash leaves) opens and rebalances to within bounds. And an inline writer's
/// insert into the new list, short with the first page's one mover, doesn't merge it away
/// (S1: the pass claims it).
#[test]
fn a_paged_split_is_consistent_between_its_pages() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let _paged = PagedSplits::on();
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let opts = IndexOptions::new()
        .target_list_len(16)
        .reassign_neighbours(1);
    let index = Index::create(&db, "idx", spec(), opts).unwrap();
    let max = index.max_list_len();
    let mut model = BTreeMap::new();
    // One list (the bootstrap's, centred on 0.0) of 30 split lengths: group A near 0.0 and
    // group B near 10.0, half each, except that the first page in id order holds one A.
    // The split's first centroid is the vector farthest from 0.0, so B stays and A moves:
    // the first page's commit leaves the new list with one vector.
    let a = |i: u64| i == 5 || (i >= max && i.is_multiple_of(2));
    let x = |i: u64| {
        if a(i) {
            i as f32 * 0.0001
        } else {
            10.0 + i as f32 * 0.0001
        }
    };
    hooks::skip_rebalance(true);
    put(&index, &mut model, "p00005", x(5));
    for i in (0..30 * max).filter(|i| *i != 5) {
        put(&index, &mut model, &format!("p{i:05}"), x(i));
    }
    hooks::skip_rebalance(false);
    assert_eq!(index.verify().unwrap().list_lens.len(), 1);
    let held = hooks::arm(Hold::ReassignBeforeCommit);
    model.insert(b"trigger".to_vec(), V::F32(vec![10.5, 0.0]));
    let splitter = {
        let index = index.clone();
        std::thread::spawn(move || index.insert_f32(b"trigger", &[10.5, 0.0]))
    };
    assert!(
        held.wait_for(std::time::Duration::from_secs(30)),
        "the pass reached its second page with movers"
    );
    let stats = index.verify().unwrap();
    assert_eq!(stats.vectors, model.len() as u64, "mid-pass: vectors");
    assert_eq!(stats.list_lens.len(), 2, "mid-pass: {:?}", stats.list_lens);
    assert!(
        stats.list_lens.contains(&1),
        "the new list holds the first page's one mover: {:?}",
        stats.list_lens
    );
    for q in [0.0, 0.05, 5.0, 10.0, 10.5] {
        exact(
            &index,
            &model,
            &V::F32(vec![q, 0.0]),
            5,
            &format!("mid-pass {q}"),
        );
    }
    let copy = dir.join("copy.phdb");
    db.backup(&copy).unwrap();
    let mut copied = model.clone();
    // S1: an insert into the short new list, whose inline merge would undo the pass.
    put(&index, &mut model, "late", 0.0);
    let lens = index.verify().unwrap().list_lens;
    assert_eq!(lens.len(), 2, "the new list survived the insert: {lens:?}");
    held.release();
    splitter.join().unwrap().unwrap();
    settle(&index, &mut model, "after the pass");
    check(&index, &model, "after the pass");
    for q in [0.0, 5.0, 10.0] {
        exact(
            &index,
            &model,
            &V::F32(vec![q, 0.0]),
            5,
            &format!("after {q}"),
        );
    }
    db.close().unwrap();

    // The copy: the crash state between pages opens, verifies, and rebalances.
    let db = pigeonhole::Pigeonhole::open(&copy, pigeonhole::Options::default().shards(2)).unwrap();
    let index = Index::open(&db, "idx").unwrap();
    let stats = index.verify().unwrap();
    assert_eq!(stats.vectors, copied.len() as u64, "the copy: vectors");
    settle(&index, &mut copied, "the copy");
    check(&index, &copied, "the copy");
    exact(&index, &copied, &V::F32(vec![0.0, 0.0]), 5, "the copy");
    db.close().unwrap();
}

/// audit120's S1 on #1881: a long list of small vectors, within the bound a split reads
/// whole, whose whole move is more than the engine takes in one transaction at a small
/// memtable budget (`BatchTooLarge`). The split and phase 2 fall back to moving it a page
/// at a time, so the write that triggers it succeeds and the index ends within its bounds.
#[test]
fn a_whole_split_too_large_for_one_transaction_moves_a_page_at_a_time() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(
        dir.join("idx.phdb"),
        Options::default()
            .shards(1)
            .memtable_budget(1 << 20)
            .durability(Durability::Buffered),
    )
    .unwrap();
    let opts = IndexOptions::new()
        .target_list_len(64)
        .reassign_neighbours(1)
        .rebalance(Rebalance::Background);
    let (index, mut rebalancer) = Index::create_with_rebalancer(&db, "idx", spec(), opts).unwrap();
    let drain = |r: &mut pigeonhole_vector::Rebalancer| {
        while r.run_once(std::time::Duration::from_secs(60)).unwrap() {}
    };
    let mut model = BTreeMap::new();
    for i in 0..400 {
        put(&index, &mut model, &format!("s{i:03}"), i as f32);
    }
    drain(&mut rebalancer);
    let lists = index.verify().unwrap().list_lens.len();
    hooks::skip_rebalance(true);
    // 10,000 vectors (80 KiB of them) into the list nearest 200, 500 per transaction.
    for chunk in 0..20u32 {
        let (placed, _) = db
            .transact(&RetryPolicy::default(), |tx| {
                let mut p = Placed::new();
                for j in 0..500u32 {
                    let i = chunk * 500 + j;
                    let x = 200.0 + (i % 2) as f32 * 0.5 + (i % 7) as f32 * 0.01;
                    index.insert_f32_in(tx, &mut p, format!("l{i:05}").as_bytes(), &[x, 0.0])?;
                }
                Ok::<Placed, Error>(p)
            })
            .unwrap();
        index.after_commit(placed).unwrap();
        for j in 0..500u32 {
            let i = chunk * 500 + j;
            let x = 200.0 + (i % 2) as f32 * 0.5 + (i % 7) as f32 * 0.01;
            model.insert(format!("l{i:05}").into_bytes(), V::F32(vec![x, 0.0]));
        }
    }
    hooks::skip_rebalance(false);
    let lens = index.verify().unwrap().list_lens;
    assert_eq!(lens.len(), lists);
    assert!(lens.iter().any(|n| *n > 10_000), "one long list: {lens:?}");
    // The next write into it marks it; the rebalancer's splits must not fail.
    put(&index, &mut model, "trigger", 200.0);
    drain(&mut rebalancer);
    check(&index, &model, "after the long list's splits");
    for q in [0.0, 200.0, 200.5, 390.0] {
        exact(
            &index,
            &model,
            &V::F32(vec![q, 0.0]),
            5,
            &format!("query {q}"),
        );
    }
    drop(rebalancer);
    db.close().unwrap();
}

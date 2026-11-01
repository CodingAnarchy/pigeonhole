//! D306's races and crash windows (§8, rev 3 M3, rev 4 N1), held deterministically with the
//! `__unstable-hooks` seams. The seams are process-wide, so these tests run one at a time.

mod common;

use std::collections::BTreeMap;
use std::sync::Mutex;

use common::{V, check, exact, open};
use pigeonhole::{Element, Metric, VectorSpec};
use pigeonhole_vector::hooks::{self, Hold};
use pigeonhole_vector::{Index, IndexOptions};

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

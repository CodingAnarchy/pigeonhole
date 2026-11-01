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

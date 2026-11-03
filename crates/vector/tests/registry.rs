//! #1268 item 8 (ICR 0110): the per-process registry. A second open of an index this process
//! has open shares the first handle's state; another database, the same file reopened, a
//! table dropped and created again, and an index whose handles all dropped are opened afresh.

mod common;

use std::sync::{Arc, Barrier};

use common::open;
use pigeonhole::{Element, ErrorCode, Metric, VectorSpec};
use pigeonhole_vector::hooks::shares_state;
use pigeonhole_vector::{Index, IndexOptions, Rebalance};

fn spec() -> VectorSpec {
    VectorSpec::new(2, Element::F32, Metric::L2).unwrap()
}

fn opts() -> IndexOptions {
    IndexOptions::new().target_list_len(4)
}

#[test]
fn a_second_open_shares_the_first() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let a = Index::create(&db, "idx", spec(), opts()).unwrap();
    let b = Index::open(&db, "idx").unwrap();
    assert!(shares_state(&a, &b));
    // Through another clone of the database handle, and with a rebalancer handed out.
    let (c, mut rebalancer) = Index::open_with_rebalancer(&db.clone(), "idx").unwrap();
    assert!(shares_state(&a, &c));
    // One cache: an insert through one handle is in the layout every handle searches.
    for i in 0..20u32 {
        a.insert_f32(&i.to_be_bytes(), &[i as f32, 0.0]).unwrap();
    }
    let hits = c.search_f32(&[3.0, 0.0], 1).nprobe(u32::MAX).run().unwrap();
    assert_eq!(hits[0].id, 3u32.to_be_bytes());
    assert!(
        !rebalancer
            .run_once(std::time::Duration::from_secs(1))
            .unwrap()
    );
    // The rebalancer sees the application's handles, whichever open made them.
    drop((a, b));
    assert!(!rebalancer.closed());
    drop(c);
    assert!(rebalancer.closed());
    db.close().unwrap();
}

#[test]
fn concurrent_first_opens_get_one_index() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    drop(Index::create(&db, "idx", spec(), opts()).unwrap());
    let barrier = Arc::new(Barrier::new(4));
    let handles: Vec<Index> = std::thread::scope(|s| {
        let ts: Vec<_> = (0..4)
            .map(|_| {
                let (db, barrier) = (&db, Arc::clone(&barrier));
                s.spawn(move || {
                    barrier.wait();
                    Index::open(db, "idx").unwrap()
                })
            })
            .collect();
        ts.into_iter().map(|t| t.join().unwrap()).collect()
    });
    assert!(handles.iter().all(|h| shares_state(&handles[0], h)));
    drop(handles);
    db.close().unwrap();
}

#[test]
fn another_database_or_a_recreated_table_is_opened_afresh() {
    let (d1, d2) = (
        pigeonhole::doc_support::temp_dir(),
        pigeonhole::doc_support::temp_dir(),
    );
    let (db1, db2) = (open(&d1), open(&d2));
    let a = Index::create(&db1, "idx", spec(), opts()).unwrap();
    let b = Index::create(&db2, "idx", spec(), opts()).unwrap();
    assert!(!shares_state(&a, &b), "same name, another database");
    a.insert_f32(b"one", &[1.0, 1.0]).unwrap();
    assert!(b.search_f32(&[1.0, 1.0], 1).run().unwrap().is_empty());

    // Dropped and created again under the same name while `a` is still held: a new index.
    db1.drop_table("idx").unwrap();
    let c = Index::create(&db1, "idx", spec(), opts()).unwrap();
    assert!(!shares_state(&a, &c));
    assert!(c.search_f32(&[1.0, 1.0], 1).run().unwrap().is_empty());
    assert!(shares_state(&c, &Index::open(&db1, "idx").unwrap()));
    drop((a, b, c));
    db2.close().unwrap();

    // The same file reopened after a close, while an old handle is still held.
    let old = Index::open(&db1, "idx").unwrap();
    db1.close().unwrap();
    let db1 = open(&d1);
    let fresh = Index::open(&db1, "idx").unwrap();
    assert!(!shares_state(&old, &fresh));
    assert_eq!(
        old.insert_f32(b"x", &[0.0, 0.0]).unwrap_err().code(),
        ErrorCode::Closed
    );
    fresh.insert_f32(b"x", &[0.0, 0.0]).unwrap();
    drop((old, fresh));
    db1.close().unwrap();
}

#[test]
fn background_opens_share_one_rebalancer_and_reopen_after_the_last_handle() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let bg = opts().rebalance(Rebalance::Background);
    let a = Index::create(&db, "idx", spec(), bg).unwrap();
    let b = Index::open(&db, "idx").unwrap();
    assert!(shares_state(&a, &b));
    for i in 0..60u32 {
        b.insert_f32(&i.to_be_bytes(), &[i as f32, 0.0]).unwrap();
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while a
        .verify()
        .unwrap()
        .list_lens
        .iter()
        .any(|n| *n > a.max_list_len())
    {
        assert!(std::time::Instant::now() < deadline, "never rebalanced");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    drop((a, b));
    // Every handle gone: the next open is a new index state, with its vectors.
    let c = Index::open(&db, "idx").unwrap();
    assert_eq!(c.verify().unwrap().vectors, 60);
    drop(c);
    db.close().unwrap();
}

//! D311 T8 (#1342): the index's writes inside a caller's transaction (`insert_*_in`,
//! `delete_in`, `after_commit`) and the prefixed search (`Search::id_prefix`, `max_probe`).
//! The writes commit and roll back with the caller's own, a transaction whose snapshot
//! predates a split conflicts rather than writing a list that changed, a stale centroid
//! cache reloads, and a prefixed search widens until it has `k` hits, and is exact for
//! the prefix once it has probed every list.

mod common;

use std::collections::BTreeMap;

use common::{V, check, open};
use pigeonhole::{ErrorCode, Family, Metric, RetryPolicy, Table, VectorSpec};
use pigeonhole_vector::{Error, Index, IndexOptions, Placed};

fn small() -> IndexOptions {
    IndexOptions::new().target_list_len(8).nprobe(2)
}

fn spec() -> VectorSpec {
    VectorSpec::new(2, pigeonhole::Element::F32, Metric::L2).unwrap()
}

/// A point of cluster `c` (of 6, 10 apart on the x axis), nudged by `i`.
fn point(c: u32, i: u32) -> Vec<f32> {
    vec![
        c as f32 * 10.0 + (i % 97) as f32 * 0.01,
        (i % 13) as f32 * 0.01,
    ]
}

fn other(db: &pigeonhole::Pigeonhole) -> Table {
    db.table("other")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap()
}

#[test]
fn writes_in_a_callers_transaction_commit_and_roll_back_with_it() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec(), small()).unwrap();
    let t = other(&db);
    let mut model = BTreeMap::new();
    for i in 0..120u32 {
        let id = i.to_be_bytes();
        let v = point(i % 6, i);
        let (placed, _) = db
            .transact(&RetryPolicy::default(), |tx| {
                let mut p = Placed::new();
                index.insert_f32_in(tx, &mut p, &id, &v)?;
                tx.put(&t, &id, "f", b"q", b"v");
                Ok::<Placed, Error>(p)
            })
            .unwrap();
        index.after_commit(placed).unwrap();
        model.insert(id.to_vec(), V::F32(v));
    }
    check(&index, &model, "after the inserts");
    assert!(index.verify().unwrap().list_lens.len() > 6, "lists split");

    // A body that fails after its index write leaves neither write.
    let r = db.transact(&RetryPolicy::default(), |tx| {
        index.insert_f32_in(tx, &mut Placed::new(), b"gone", &point(1, 1))?;
        tx.put(&t, b"gone", "f", b"q", b"v");
        Err::<(), Error>(Error::InvalidArgument("rolled back".into()))
    });
    assert!(matches!(r, Err(Error::InvalidArgument(_))));
    assert!(t.get(b"gone", "f", b"q").unwrap().is_none());
    check(&index, &model, "after a rolled-back insert");

    // An upsert in a transaction moves the vector to the list nearest its new value.
    let id = 7u32.to_be_bytes();
    let moved = point(5, 7);
    let (p, _) = db
        .transact(&RetryPolicy::default(), |tx| {
            let mut p = Placed::new();
            index.insert_f32_in(tx, &mut p, &id, &moved).map(|()| p)
        })
        .unwrap();
    index.after_commit(p).unwrap();
    model.insert(id.to_vec(), V::F32(moved));
    check(&index, &model, "after an upsert");

    // Deletes: an absent id writes nothing; a rolled-back delete keeps the vector.
    let (none, _) = db
        .transact(&RetryPolicy::default(), |tx| {
            let mut p = Placed::new();
            let deleted = index.delete_in(tx, &mut p, b"absent")?;
            Ok::<_, Error>((deleted, p))
        })
        .unwrap();
    assert!(!none.0, "nothing to delete");
    index.after_commit(none.1).unwrap();
    let r = db.transact(&RetryPolicy::default(), |tx| {
        index.delete_in(tx, &mut Placed::new(), &3u32.to_be_bytes())?;
        Err::<(), Error>(Error::InvalidArgument("rolled back".into()))
    });
    assert!(r.is_err());
    check(&index, &model, "after a rolled-back delete");
    for i in 0..100u32 {
        let id = i.to_be_bytes();
        let (p, _) = db
            .transact(&RetryPolicy::default(), |tx| {
                let mut p = Placed::new();
                assert!(index.delete_in(tx, &mut p, &id)?, "present");
                Ok::<_, Error>(p)
            })
            .unwrap();
        index.after_commit(p).unwrap();
        model.remove(id.as_slice());
    }
    check(&index, &model, "after the deletes merged lists");
    db.close().unwrap();
}

#[test]
fn without_after_commit_a_list_waits_for_its_next_write() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec(), small()).unwrap();
    let mut model = BTreeMap::new();
    for i in 0..40u32 {
        let id = i.to_be_bytes();
        let v = point(i % 2, i);
        // Dropped unused: no split yet.
        let _ = db
            .transact(&RetryPolicy::default(), |tx| {
                let mut p = Placed::new();
                index.insert_f32_in(tx, &mut p, &id, &v).map(|()| p)
            })
            .unwrap();
        model.insert(id.to_vec(), V::F32(v));
    }
    assert_eq!(index.verify().unwrap().list_lens, vec![40]);
    // The next ordinary write to that list splits it.
    let v = point(0, 40);
    index.insert_f32(&40u32.to_be_bytes(), &v).unwrap();
    model.insert(40u32.to_be_bytes().to_vec(), V::F32(v));
    check(&index, &model, "after the next write");
    db.close().unwrap();
}

#[test]
fn a_transaction_older_than_a_split_conflicts_and_its_retry_lands() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let a = Index::create(&db, "idx", spec(), small()).unwrap();
    let t = other(&db);
    t.mutate(b"x").put("f", b"q", b"1").commit().unwrap();
    let mut model = BTreeMap::new();
    for i in 0..5u32 {
        let v = point(0, i);
        a.insert_f32(&i.to_be_bytes(), &v).unwrap();
        model.insert(i.to_be_bytes().to_vec(), V::F32(v));
    }
    // `b` caches the one-list layout, and `tx` reads before the split, fixing its snapshot.
    let b = Index::open(&db, "idx").unwrap();
    let mut tx = db.transaction().unwrap();
    assert!(tx.get(&t, b"x", "f", b"q").unwrap().is_some());
    for i in 5..60u32 {
        let v = point(i % 3, i);
        a.insert_f32(&i.to_be_bytes(), &v).unwrap();
        model.insert(i.to_be_bytes().to_vec(), V::F32(v));
    }
    assert!(a.verify().unwrap().list_lens.len() > 1, "list 0 split");
    let v = point(0, 99);
    b.insert_f32_in(&mut tx, &mut Placed::new(), b"late", &v)
        .unwrap();
    assert_eq!(tx.commit().unwrap_err().code(), ErrorCode::Conflict);
    check(&a, &model, "the conflicting write left nothing");

    // Retried at a fresh snapshot through `b`'s stale cache: it reloads and lands.
    let (p, _) = db
        .transact(&RetryPolicy::default(), |tx| {
            let mut p = Placed::new();
            b.insert_f32_in(tx, &mut p, b"late", &v).map(|()| p)
        })
        .unwrap();
    b.after_commit(p).unwrap();
    model.insert(b"late".to_vec(), V::F32(v));
    check(&a, &model, "after the retry");
    db.close().unwrap();
}

/// Ids `a/…` sit far from the query and `b/…` crowd around it, so the lists nearest the
/// query hold only `b`s.
fn prefixed() -> (
    pigeonhole::doc_support::TempDir,
    pigeonhole::Pigeonhole,
    Index,
    BTreeMap<Vec<u8>, V>,
) {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec(), small()).unwrap();
    let mut model = BTreeMap::new();
    for i in 0..300u32 {
        let id = [b"b/".as_slice(), &i.to_be_bytes()].concat();
        let v = point(i % 5, i);
        index.insert_f32(&id, &v).unwrap();
        model.insert(id, V::F32(v));
    }
    for i in 0..6u32 {
        let id = [b"a/".as_slice(), &i.to_be_bytes()].concat();
        let v = vec![100.0 + i as f32, 50.0];
        index.insert_f32(&id, &v).unwrap();
        model.insert(id, V::F32(v));
    }
    (dir, db, index, model)
}

#[test]
fn a_prefixed_search_widens_until_k_hits_pass_and_is_exact_once_it_probes_every_list() {
    let (_dir, db, index, model) = prefixed();
    let q = [0.0f32, 0.0];
    let lists = index.verify().unwrap().list_lens.len() as u32;
    assert!(lists > 4, "{lists} lists");

    // Unprefixed: nprobe lists, every hit a `b`.
    let (hits, stats) = index.search_f32(&q, 3).nprobe(1).run_with_stats().unwrap();
    assert_eq!(stats.lists_probed, 1);
    assert_eq!(stats.lists, lists);
    assert!(hits.iter().all(|h| h.id.starts_with(b"b/")));

    // Prefixed with k above the prefix's count: it widens to every list, and then it's
    // exact for the prefix (all six, nearest first).
    let (hits, stats) = index
        .search_f32(&q, 10)
        .nprobe(1)
        .id_prefix(b"a/")
        .run_with_stats()
        .unwrap();
    assert_eq!(stats.lists_probed, lists);
    let spec = index.spec();
    let mut want: Vec<(f64, &Vec<u8>)> = model
        .iter()
        .filter(|(id, _)| id.starts_with(b"a/"))
        .map(|(id, v)| (V::F32(q.to_vec()).distance(&spec, v), id))
        .collect();
    want.sort_by(|a, b| a.0.total_cmp(&b.0));
    let got: Vec<(f64, &Vec<u8>)> = hits.iter().map(|h| (h.distance, &h.id)).collect();
    assert_eq!(got, want);

    // `k` hits found early: it stops there, short of every list.
    let (hits, stats) = index
        .search_f32(&[100.0, 50.0], 2)
        .nprobe(1)
        .id_prefix(b"a/")
        .run_with_stats()
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert!(stats.lists_probed < lists, "{stats:?}");

    // `max_probe` caps the widening, never below nprobe.
    let (hits, stats) = index
        .search_f32(&q, 3)
        .nprobe(1)
        .id_prefix(b"a/")
        .max_probe(1)
        .run_with_stats()
        .unwrap();
    assert_eq!(stats.lists_probed, 1);
    assert!(hits.is_empty(), "the nearest list holds only b/: {hits:?}");
    let (_, stats) = index
        .search_f32(&q, 3)
        .nprobe(2)
        .id_prefix(b"a/")
        .max_probe(1)
        .run_with_stats()
        .unwrap();
    assert_eq!(stats.lists_probed, 2);
    db.close().unwrap();
}

#[test]
fn a_stale_cache_never_writes_a_merged_away_list_inside_a_transaction() {
    use pigeonhole_vector::hooks::{Hold, arm};
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec(), small()).unwrap();
    let mut model = BTreeMap::new();
    // Two clusters far apart: 20 vectors split into a list for each.
    let at = |c: u32, i: u32| vec![c as f32 * 1000.0 + i as f32 * 0.01, 0.0];
    for c in 0..2u32 {
        for i in 0..10u32 {
            let id = (c * 100 + i).to_be_bytes();
            index.insert_f32(&id, &at(c, i)).unwrap();
            model.insert(id.to_vec(), V::F32(at(c, i)));
        }
    }
    assert_eq!(index.verify().unwrap().list_lens, vec![10, 10]);
    // Cluster 1's list down to its bound (2), then one more delete merges it, held
    // between the merge's commit and its cache reload.
    for i in 0..8u32 {
        let id = (100 + i).to_be_bytes();
        index.delete(&id).unwrap();
        model.remove(id.as_slice());
    }
    let held = arm(Hold::MergeBeforeReload);
    let merger = {
        let index = index.clone();
        std::thread::spawn(move || index.delete(&108u32.to_be_bytes()).unwrap())
    };
    held.wait();
    model.remove(108u32.to_be_bytes().as_slice());
    // The cache still names cluster 1's list: the write reloads and lands in the live one.
    let v = at(1, 50);
    let (p, _) = db
        .transact(&RetryPolicy::default(), |tx| {
            let mut p = Placed::new();
            index.insert_f32_in(tx, &mut p, b"late", &v).map(|()| p)
        })
        .unwrap();
    held.release();
    assert!(merger.join().unwrap());
    index.after_commit(p).unwrap();
    model.insert(b"late".to_vec(), V::F32(v));
    check(&index, &model, "after the merge and the write");
    db.close().unwrap();
}

#[test]
fn a_widened_search_equals_one_read_of_the_lists_it_probed() {
    for seed in common::seeds(&[1, 2, 3]) {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let index = Index::create(&db, "idx", spec(), small()).unwrap();
        let mut rng = common::Rng::new(seed);
        // Ids under 8 prefixes, the rare ones scattered.
        for i in 0..400u32 {
            let pre = if rng.below(10) == 0 {
                rng.below(4) as u8
            } else {
                4 + rng.below(4) as u8
            };
            let id = [&[pre][..], &i.to_be_bytes()].concat();
            let v = [rng.unit() * 50.0, rng.unit() * 50.0];
            index.insert_f32(&id, &v).unwrap();
        }
        for _ in 0..20 {
            let q = [rng.unit() * 50.0, rng.unit() * 50.0];
            let pre = [rng.below(8) as u8];
            let k = 1 + rng.below(12) as usize;
            let (got, stats) = index
                .search_f32(&q, k)
                .nprobe(1)
                .id_prefix(&pre)
                .run_with_stats()
                .unwrap();
            // The same lists in one read: an unprefixed-widening search at nprobe = probed.
            let once = index
                .search_f32(&q, k)
                .nprobe(stats.lists_probed)
                .id_prefix(&pre)
                .max_probe(stats.lists_probed)
                .run()
                .unwrap();
            let ctx = format!("seed {seed}, prefix {pre:?}, k {k}, {stats:?}");
            let d = |h: &[pigeonhole_vector::Hit]| h.iter().map(|h| h.distance).collect::<Vec<_>>();
            assert_eq!(d(&got), d(&once), "{ctx}: distances");
            let set = |h: &[pigeonhole_vector::Hit]| {
                h.iter()
                    .map(|h| h.id.clone())
                    .collect::<std::collections::BTreeSet<_>>()
            };
            // Ties at the k-th distance may pick different ids; below it the sets agree.
            if got.len() < k
                || got.last().map(|h| h.distance)
                    != got.get(k.saturating_sub(2)).map(|h| h.distance)
            {
                assert_eq!(set(&got), set(&once), "{ctx}: ids");
            }
        }
        db.close().unwrap();
    }
}

#[test]
fn several_ids_in_one_transaction_each_land_once() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec(), small()).unwrap();
    let mut model = BTreeMap::new();
    for batch in 0..10u32 {
        let (placed, _) = db
            .transact(&RetryPolicy::default(), |tx| {
                let mut p = Placed::new();
                for j in 0..12u32 {
                    let i = batch * 12 + j;
                    index.insert_f32_in(tx, &mut p, &i.to_be_bytes(), &point(i % 4, i))?;
                }
                if batch > 0 {
                    // And a delete of an id an earlier transaction wrote.
                    let id = ((batch - 1) * 12).to_be_bytes();
                    assert!(index.delete_in(tx, &mut p, &id)?);
                }
                Ok::<_, Error>(p)
            })
            .unwrap();
        index.after_commit(placed).unwrap();
        for j in 0..12u32 {
            let i = batch * 12 + j;
            model.insert(i.to_be_bytes().to_vec(), V::F32(point(i % 4, i)));
        }
        if batch > 0 {
            model.remove(((batch - 1) * 12).to_be_bytes().as_slice());
        }
        check(&index, &model, &format!("batch {batch}"));
    }
    db.close().unwrap();
}

/// cli692's B1 on #1412: a transaction's reads don't see its own writes, so a second write
/// of an id in one transaction must act on the first (through `Placed`), not the snapshot.
#[test]
fn writing_one_id_twice_in_a_transaction_acts_on_the_first_write() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec(), small()).unwrap();
    let mut model = BTreeMap::new();
    let one = |body: &dyn Fn(&mut pigeonhole::Transaction, &mut Placed) -> Result<(), Error>| {
        let (p, _) = db
            .transact(&RetryPolicy::default(), |tx| {
                let mut p = Placed::new();
                body(tx, &mut p)?;
                Ok::<_, Error>(p)
            })
            .unwrap();
        index.after_commit(p).unwrap();
    };
    // From empty: two inserts of one id and one of another, all in the bootstrap's
    // transaction.
    one(&|tx, p| {
        index.insert_f32_in(tx, p, b"x", &point(0, 1))?;
        index.insert_f32_in(tx, p, b"x", &point(0, 2))?;
        index.insert_f32_in(tx, p, b"y", &point(0, 3))
    });
    model.insert(b"x".to_vec(), V::F32(point(0, 2)));
    model.insert(b"y".to_vec(), V::F32(point(0, 3)));
    check(&index, &model, "bootstrap transaction");
    for i in 0..90u32 {
        let v = point(i % 6, i);
        index.insert_f32(&i.to_be_bytes(), &v).unwrap();
        model.insert(i.to_be_bytes().to_vec(), V::F32(v));
    }
    assert!(index.verify().unwrap().list_lens.len() > 3);
    let id = |i: u32| i.to_be_bytes();
    // A new id inserted near cluster 0, then moved to cluster 5.
    one(&|tx, p| {
        index.insert_f32_in(tx, p, b"new", &point(0, 7))?;
        index.insert_f32_in(tx, p, b"new", &point(5, 7))
    });
    model.insert(b"new".to_vec(), V::F32(point(5, 7)));
    check(&index, &model, "insert, then move");
    // An existing id moved away and back.
    one(&|tx, p| {
        index.insert_f32_in(tx, p, &id(3), &point(4, 3))?;
        index.insert_f32_in(tx, p, &id(3), &point(3, 3))
    });
    model.insert(id(3).to_vec(), V::F32(point(3, 3)));
    check(&index, &model, "move away and back");
    // Insert then delete: absent. Delete then insert: present. Delete twice: once.
    one(&|tx, p| {
        index.insert_f32_in(tx, p, b"brief", &point(2, 9))?;
        assert!(index.delete_in(tx, p, b"brief")?);
        assert!(!index.delete_in(tx, p, b"brief")?);
        assert!(index.delete_in(tx, p, &id(10))?);
        index.insert_f32_in(tx, p, &id(10), &point(1, 10))?;
        assert!(index.delete_in(tx, p, &id(11))?);
        assert!(!index.delete_in(tx, p, &id(11))?);
        Ok(())
    });
    model.insert(id(10).to_vec(), V::F32(point(1, 10)));
    model.remove(id(11).as_slice());
    check(&index, &model, "deletes and reinserts");
    db.close().unwrap();
}

/// cli692's B2 on #1412: in-transaction writes racing inline splits and merges never
/// surface a body error (a merge is one transaction, so no snapshot shows a mapping to a
/// merged-away list), and the index stays consistent.
#[test]
fn in_transaction_writes_racing_splits_and_merges_stay_consistent() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec(), small()).unwrap();
    std::thread::scope(|s| {
        for t in 0..4u32 {
            let (db, index) = (&db, &index);
            s.spawn(move || {
                let mut rng = common::Rng::new(u64::from(t) + 7);
                for _ in 0..150 {
                    let id = (rng.below(60) as u32).to_be_bytes();
                    let v = point(rng.below(4) as u32, rng.below(1000) as u32);
                    let del = rng.below(3) == 0;
                    let (p, _) = db
                        .transact(&RetryPolicy::default(), |tx| {
                            let mut p = Placed::new();
                            if del {
                                index.delete_in(tx, &mut p, &id)?;
                            } else {
                                index.insert_f32_in(tx, &mut p, &id, &v)?;
                            }
                            Ok::<_, Error>(p)
                        })
                        .expect("no body error escapes");
                    index.after_commit(p).expect("after_commit");
                }
            });
        }
    });
    let stats = index.verify().unwrap();
    assert!(stats.vectors <= 60, "{stats:?}");
    db.close().unwrap();
}

/// cli692's C1 on #1412: a `Placed` belongs to the index its first write used.
#[test]
fn a_placed_belongs_to_one_index() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let a = Index::create(&db, "a", spec(), small()).unwrap();
    let b = Index::create(&db, "b", spec(), small()).unwrap();
    let r = db.transact(&RetryPolicy::default(), |tx| {
        let mut p = Placed::new();
        a.insert_f32_in(tx, &mut p, b"x", &point(0, 1))?;
        // A clone (or another open) of `a` shares its state: the same index.
        a.clone().insert_f32_in(tx, &mut p, b"y", &point(0, 2))?;
        let other = b.insert_f32_in(tx, &mut p, b"z", &point(0, 3));
        assert!(matches!(other, Err(Error::InvalidArgument(_))), "{other:?}");
        let other = b.delete_in(tx, &mut p, b"x");
        assert!(matches!(other, Err(Error::InvalidArgument(_))), "{other:?}");
        Ok::<_, Error>(p)
    });
    let (p, _) = r.unwrap();
    assert!(matches!(
        b.after_commit(p.clone()),
        Err(Error::InvalidArgument(_))
    ));
    a.after_commit(p).unwrap();
    assert_eq!(a.verify().unwrap().vectors, 2);
    assert_eq!(b.verify().unwrap().vectors, 0);
    db.close().unwrap();
}

//! D306 (#490 D, ICR 0100): the IVF index through its public API. A seeded model of inserts,
//! upserts, deletes and searches over every element type and metric holds the invariants
//! after every operation (§8): the rows agree, the index holds exactly the model's
//! vectors, every list is within its bounds, and a search probing every list equals a brute
//! force by `VectorSpec`'s distances. Also: reopening, two handles with separate centroid
//! caches (a stale cache never writes a merged-away list, rev 3 M1), concurrent inserts into
//! one splitting list, and a recall floor on clustered data.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::{Rng, V, check, exact, open, seeds, specs};
use pigeonhole::{Element, Metric, VectorSpec};
use pigeonhole_vector::{Error, Index, IndexOptions};

fn small() -> IndexOptions {
    IndexOptions::new().target_list_len(8).nprobe(2)
}

#[test]
fn a_seeded_model_holds_the_invariants_after_every_operation() {
    for seed in seeds(&[1, 2, 3]) {
        for spec in specs() {
            let ctx = format!("seed {seed}, {spec:?}");
            let dir = pigeonhole::doc_support::temp_dir();
            let db = open(&dir);
            let index = Index::create(&db, "idx", spec, small()).expect("create");
            let mut rng = Rng::new(seed);
            let mut model: BTreeMap<Vec<u8>, V> = BTreeMap::new();
            for op in 0..400 {
                let id = rng.below(120).to_be_bytes().to_vec();
                let ctx = format!("{ctx}, op {op}");
                match rng.below(10) {
                    // Inserts and upserts (an id inserted again moves or stays: M2).
                    0..=5 => {
                        let v = {
                            let c = rng.below(4);
                            V::random(&mut rng, &spec, c)
                        };
                        v.insert(&index, &id)
                            .unwrap_or_else(|e| panic!("{ctx}: insert: {e}"));
                        model.insert(id, v);
                    }
                    6..=8 => {
                        let had = index
                            .delete(&id)
                            .unwrap_or_else(|e| panic!("{ctx}: delete: {e}"));
                        assert_eq!(had, model.remove(&id).is_some(), "{ctx}: delete's answer");
                    }
                    _ => {
                        let q = {
                            let c = rng.below(4);
                            V::random(&mut rng, &spec, c)
                        };
                        exact(&index, &model, &q, 5, &ctx);
                    }
                }
                check(&index, &model, &ctx);
            }
            assert!(
                index.verify().unwrap().list_lens.len() > 2,
                "{ctx}: splits happened"
            );
            db.close().expect("close");
        }
    }
}

#[test]
fn an_index_reopens_with_its_vectors_and_options() {
    let spec = VectorSpec::new(4, Element::F32, Metric::L2).unwrap();
    let dir = pigeonhole::doc_support::temp_dir();
    let mut model = BTreeMap::new();
    let mut rng = Rng::new(7);
    {
        let db = open(&dir);
        let index = Index::create(&db, "idx", spec, small().nprobe(3)).expect("create");
        for i in 0..100u32 {
            let v = V::random(&mut rng, &spec, u64::from(i % 3));
            v.insert(&index, &i.to_be_bytes()).unwrap();
            model.insert(i.to_be_bytes().to_vec(), v);
        }
        db.close().unwrap();
    }
    let db = open(&dir);
    let index = Index::open(&db, "idx").expect("reopen");
    assert_eq!(index.options(), small().nprobe(3));
    assert_eq!(index.spec(), spec);
    check(&index, &model, "reopened");
    let q = V::random(&mut rng, &spec, 1);
    exact(&index, &model, &q, 10, "reopened");
    // Creating it again is refused; a plain table isn't an index.
    assert!(Index::create(&db, "idx", spec, small()).is_err());
    db.table("plain")
        .unwrap()
        .family("f", pigeonhole::Family::default())
        .create()
        .unwrap();
    assert!(matches!(
        Index::open(&db, "plain"),
        Err(Error::InvalidArgument(_))
    ));
    db.close().unwrap();
}

#[test]
fn writes_and_searches_refuse_another_shape() {
    let spec = VectorSpec::new(4, Element::F32, Metric::Cosine).unwrap();
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec, small()).unwrap();
    assert!(matches!(
        index.insert_f32(b"a", &[1.0, 2.0]),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        index.insert_i8(b"a", &[1, 2, 3, 4]),
        Err(Error::InvalidArgument(_))
    ));
    // An all-zero cosine vector is the database's refusal (D303).
    assert!(index.insert_f32(b"a", &[0.0; 4]).is_err());
    assert!(index.search_f32(&[1.0], 3).run().is_err());
    // An empty index searches to nothing.
    assert!(
        index
            .search_f32(&[1.0, 0.0, 0.0, 0.0], 3)
            .run()
            .unwrap()
            .is_empty()
    );
    assert!(!index.delete(b"nobody").unwrap());
    assert!(IndexOptions::new().target_list_len(2).nprobe(1) != IndexOptions::new());
    assert!(Index::create(&db, "tiny", spec, IndexOptions::new().target_list_len(3)).is_err());
    db.close().unwrap();
}

#[test]
fn a_search_at_a_snapshot_sees_the_index_as_of_it() {
    let spec = VectorSpec::new(2, Element::F32, Metric::L2).unwrap();
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec, small()).unwrap();
    for i in 0..30u32 {
        index
            .insert_f32(&i.to_be_bytes(), &[i as f32, 0.0])
            .unwrap();
    }
    let before = db.snapshot().unwrap();
    // Many more inserts split lists and change the layout.
    for i in 30..200u32 {
        index
            .insert_f32(&i.to_be_bytes(), &[i as f32, 0.0])
            .unwrap();
    }
    index.delete(&5u32.to_be_bytes()).unwrap();
    let old = index
        .search_f32(&[5.2, 0.0], 1)
        .nprobe(1000)
        .snapshot(&before)
        .run()
        .unwrap();
    assert_eq!(old[0].id, 5u32.to_be_bytes(), "5 was there then");
    let far = index
        .search_f32(&[150.0, 0.0], 1)
        .nprobe(1000)
        .snapshot(&before)
        .run()
        .unwrap();
    assert_eq!(far[0].id, 29u32.to_be_bytes(), "150 wasn't there then");
    let now = index.search_f32(&[5.2, 0.0], 1).nprobe(1000).run().unwrap();
    assert_ne!(now[0].id, 5u32.to_be_bytes());
    db.close().unwrap();
}

#[test]
fn a_stale_handle_never_writes_a_merged_away_list() {
    let spec = VectorSpec::new(2, Element::F32, Metric::L2).unwrap();
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let a = Index::create(&db, "idx", spec, small()).unwrap();
    let mut model = BTreeMap::new();
    for i in 0..60u32 {
        let v = V::F32(vec![(i % 6) as f32 * 10.0 + (i as f32) * 0.01, 0.0]);
        v.insert(&a, &i.to_be_bytes()).unwrap();
        model.insert(i.to_be_bytes().to_vec(), v);
    }
    // `b` caches the layout now; `a` then merges lists away and splits others.
    let b = Index::open(&db, "idx").unwrap();
    let lists_before = b.verify().unwrap().list_lens.len();
    for i in 0..50u32 {
        a.delete(&i.to_be_bytes()).unwrap();
        model.remove(i.to_be_bytes().as_slice());
    }
    assert!(
        a.verify().unwrap().list_lens.len() < lists_before,
        "lists merged"
    );
    // `b`'s cache names lists that are gone: its inserts reload and land in live lists.
    for i in 100..140u32 {
        let v = V::F32(vec![(i % 6) as f32 * 10.0, 1.0]);
        v.insert(&b, &i.to_be_bytes()).unwrap();
        model.insert(i.to_be_bytes().to_vec(), v);
        check(&b, &model, &format!("stale insert {i}"));
    }
    check(&a, &model, "after");
    db.close().unwrap();
}

#[test]
fn concurrent_inserts_into_one_list_lose_and_duplicate_nothing() {
    let spec = VectorSpec::new(4, Element::F32, Metric::L2).unwrap();
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Arc::new(Index::create(&db, "idx", spec, small()).unwrap());
    // Every thread inserts near one point, so they all hit the list that is splitting.
    let threads: Vec<_> = (0..8u32)
        .map(|t| {
            let index = Arc::clone(&index);
            std::thread::spawn(move || {
                let mut rng = Rng::new(u64::from(t) + 100);
                for i in 0..60u32 {
                    let id = (t * 1000 + i).to_be_bytes();
                    let v: Vec<f32> = (0..4).map(|_| rng.unit()).collect();
                    index.insert_f32(&id, &v).expect("insert");
                }
            })
        })
        .collect();
    for t in threads {
        t.join().expect("thread");
    }
    let stats = index.verify().unwrap();
    assert_eq!(stats.vectors, 480);
    assert!(
        stats.list_lens.len() > 8,
        "splits completed under load: {:?}",
        stats.list_lens
    );
    db.close().unwrap();
}

#[test]
fn clustered_data_reaches_its_recall_floor() {
    let spec = VectorSpec::new(16, Element::F32, Metric::L2).unwrap();
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(
        &db,
        "idx",
        spec,
        IndexOptions::new().target_list_len(32).nprobe(4),
    )
    .unwrap();
    // 200 overlapping clusters: centres in [-1, 1]^16, members within ±0.5 of them, so the
    // probed lists miss some true neighbours and recall measures the clustering.
    let mut rng = Rng::new(42);
    let centres: Vec<Vec<f32>> = (0..200)
        .map(|_| (0..16).map(|_| rng.unit()).collect())
        .collect();
    let point = |rng: &mut Rng| -> V {
        let c = &centres[rng.below(200) as usize];
        V::F32(c.iter().map(|x| x + 0.5 * rng.unit()).collect())
    };
    let mut model = BTreeMap::new();
    for i in 0..4000u32 {
        let v = point(&mut rng);
        v.insert(&index, &i.to_be_bytes()).unwrap();
        model.insert(i.to_be_bytes().to_vec(), v);
    }
    let lists = index.verify().unwrap().list_lens.len();
    let (mut found, mut total) = (0usize, 0usize);
    for _ in 0..100 {
        let q = point(&mut rng);
        let mut all: Vec<(f64, &Vec<u8>)> = model
            .iter()
            .map(|(id, v)| (q.distance(&spec, v), id))
            .collect();
        all.sort_by(|a, b| a.0.total_cmp(&b.0));
        let truth: Vec<&Vec<u8>> = all.iter().take(10).map(|(_, id)| *id).collect();
        let got = q.search(&index, 10, 8);
        found += got.iter().filter(|h| truth.contains(&&h.id)).count();
        total += 10;
    }
    let recall = found as f64 / total as f64;
    println!("recall@10 at nprobe 8 of {lists} lists: {recall:.3}");
    assert!(recall >= FLOOR, "recall@10 {recall} below {FLOOR}");
}

/// Pinned a little below the measured recall (seed 42), so clustering regressions fail.
const FLOOR: f64 = 0.94;

//! ICR 0123 (#1479 PR 2): the single-attempt forms a caller commits itself (`link_in`,
//! `supersede_in`, `mark_used_in`) do what the sync methods do, their in-transaction reads
//! still conflict (link against a concurrent delete, perf287 on the addendum), and `settle`
//! hands an IVF store's parked writes on.

use pigeonhole::{Element, ErrorCode, Options, Pigeonhole};
use pigeonhole_memory::{
    EmbeddingModel, Error, IvfOptions, LinkType, Memory, MemoryId, Store, StoreOptions,
};

fn open() -> (pigeonhole::doc_support::TempDir, Pigeonhole) {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("mem.phdb"), Options::default().shards(2)).unwrap();
    (dir, db)
}

fn put(store: &Store, text: &str) -> MemoryId {
    store.remember("n", Memory::new(text)).unwrap().id().clone()
}

#[test]
fn the_in_forms_do_what_the_sync_methods_do() {
    let (_dir, db) = open();
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let (a, b, c) = (
        put(&store, "alpha"),
        put(&store, "beta"),
        put(&store, "gamma"),
    );

    let mut txn = db.transaction().unwrap();
    store
        .link_in(&mut txn, &a, &b, LinkType::SIMILAR_TO, 0.5)
        .unwrap();
    store.supersede_in(&mut txn, &c, &b).unwrap();
    let m = store
        .mark_used_in(&mut txn, &[a.clone(), a.clone()])
        .unwrap();
    assert_eq!(
        (m.marked, m.missing.len()),
        (1, 0),
        "a repeated id counts once"
    );
    // Nothing is visible before the caller commits.
    assert!(store.get(&a).unwrap().unwrap().links.is_empty());
    txn.commit().unwrap();

    let ma = store.get(&a).unwrap().unwrap();
    assert_eq!(ma.links.len(), 1);
    assert_eq!(ma.uses, 1);
    assert_eq!(
        store.get(&c).unwrap().unwrap().superseded_by,
        Some(b.clone())
    );

    // The argument checks are the sync methods' own.
    let mut txn = db.transaction().unwrap();
    assert!(matches!(
        store.link_in(&mut txn, &a, &b, LinkType::SIMILAR_TO, f32::NAN),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        store.supersede_in(&mut txn, &a, &a),
        Err(Error::InvalidArgument(_))
    ));
    // Superseding again with the same memory does nothing; with another, it's refused.
    store.supersede_in(&mut txn, &c, &b).unwrap();
    assert!(matches!(
        store.supersede_in(&mut txn, &c, &a),
        Err(Error::InvalidArgument(_))
    ));
    drop(txn);
    db.close().unwrap();
}

#[test]
fn a_link_in_against_a_concurrent_delete_conflicts_then_finds_it_gone() {
    let (_dir, db) = open();
    let store = Store::create(&db, "notes", StoreOptions::new()).unwrap();
    let (a, b) = (put(&store, "alpha"), put(&store, "beta"));
    let mut txn = db.transaction().unwrap();
    store
        .link_in(&mut txn, &a, &b, LinkType::SIMILAR_TO, 1.0)
        .unwrap();
    // `b` goes between the link's reads and its commit.
    assert!(store.delete(&b).unwrap());
    let e = txn.commit().unwrap_err();
    assert_eq!(e.code(), ErrorCode::Conflict);
    // The retry's reads see it gone, as `link` reports it.
    let mut txn = db.transaction().unwrap();
    let e = store
        .link_in(&mut txn, &a, &b, LinkType::SIMILAR_TO, 1.0)
        .unwrap_err();
    assert!(matches!(e, Error::NotFound(_)), "{e:?}");
    assert!(matches!(
        store.link(&a, &b, LinkType::SIMILAR_TO, 1.0),
        Err(Error::NotFound(_))
    ));
    drop(txn);
    db.close().unwrap();
}

#[test]
fn settle_hands_on_what_remember_in_parked() {
    let (_dir, db) = open();
    let model = EmbeddingModel::new("m", 4, Element::F32).unwrap();
    let store = Store::create(
        &db,
        "notes",
        StoreOptions::new()
            .embedding(model)
            .ivf(IvfOptions::new().min_namespace_len(1).target_list_len(4)),
    )
    .unwrap();
    for i in 0..12u32 {
        let mut txn = db.transaction().unwrap();
        let v = [i as f32, 1.0, 0.0, 0.0];
        store
            .remember_in(&mut txn, "n", Memory::new(format!("note {i}")).embedding(v))
            .unwrap();
        txn.commit().unwrap();
    }
    assert!(
        pigeonhole_memory::hooks::ivf_pending(&store) > 0,
        "remember_in parks its IVF writes for the caller to settle"
    );
    store.settle();
    assert_eq!(pigeonhole_memory::hooks::ivf_pending(&store), 0);
    // Settling twice, or with nothing parked, is harmless.
    store.settle();
    assert!(store.verify().unwrap().is_consistent());
    db.close().unwrap();
}

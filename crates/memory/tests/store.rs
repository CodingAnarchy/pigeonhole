//! D311 T1 (#1335, ICR 0107), the store's first PR: create and open with the pinned
//! embedding model, `remember` with exact dedup, `get`, `delete`, `link`, `supersede`,
//! `mark_used`, a write's atomicity, and S3's races, each ending in `verify`.

use pigeonhole::{Element, ErrorCode, Options, Pigeonhole, RetryPolicy};
use pigeonhole_memory::{
    EmbeddingModel, Error, Kind, LinkType, Memory, MemoryId, Remembered, Store, StoreOptions,
};

const MODEL: &str = "test-embed-v1";

fn open(dir: &pigeonhole::doc_support::TempDir) -> Pigeonhole {
    Pigeonhole::open(dir.join("mem.phdb"), Options::default().shards(2)).expect("open")
}

fn pinned(db: &Pigeonhole) -> Store {
    let model = EmbeddingModel::new(MODEL, 4, Element::F32).unwrap();
    Store::create(db, "notes", StoreOptions::new().embedding(model)).unwrap()
}

fn emb(seed: f32) -> Vec<f32> {
    vec![1.0, seed, 0.5, -seed]
}

fn check(store: &Store, ctx: &str) {
    let st = store.verify().unwrap();
    assert!(st.is_consistent(), "{ctx}: {st:?}");
}

#[test]
fn a_memory_round_trips_with_its_metadata() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    let r = store
        .remember(
            "agent:alice",
            Memory::new("The March meeting chose Postgres.")
                .embedding(emb(0.1))
                .kind(Kind::Semantic)
                .importance(0.8)
                .source("call-17")
                .session("s1")
                .pinned(true),
        )
        .unwrap();
    let Remembered::New(id) = r else {
        panic!("{r:?}")
    };
    assert_eq!(id.namespace(), "agent:alice");
    let m = store.get(&id).unwrap().unwrap();
    assert_eq!(m.text, "The March meeting chose Postgres.");
    assert_eq!(
        (m.kind, m.importance, m.uses, m.pinned),
        (Kind::Semantic, 0.8, 0, true)
    );
    assert_eq!(m.session.as_deref(), Some("s1"));
    assert_eq!(m.sources.len(), 1);
    assert_eq!(m.sources[0].name, "call-17");
    assert_eq!(m.embedding.as_deref(), Some(&emb(0.1)[..]));
    // The id's text form round-trips, and created is the rid's time.
    assert_eq!(id.to_string().parse::<MemoryId>().unwrap(), id);
    assert!(id.created_us() > 0);
    check(&store, "one memory");
    db.close().unwrap();
}

#[test]
fn the_same_text_merges_and_its_variants_do_not() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    let ns = "agent:alice";
    let a = store
        .remember(
            ns,
            Memory::new("We chose  Postgres.")
                .embedding(emb(0.1))
                .importance(0.3),
        )
        .unwrap();
    // Whitespace differences merge; the larger importance and the new source stay.
    let b = store
        .remember(
            ns,
            Memory::new("  We chose Postgres. \n")
                .embedding(emb(0.2))
                .importance(0.9)
                .source("call-2"),
        )
        .unwrap();
    assert_eq!(b, Remembered::Merged(a.id().clone()));
    let m = store.get(a.id()).unwrap().unwrap();
    assert_eq!((m.importance, m.uses), (0.9, 1));
    assert_eq!(m.sources.len(), 1);
    // Case differs: no case folding, so a new memory.
    let c = store
        .remember(ns, Memory::new("we chose postgres.").embedding(emb(0.3)))
        .unwrap();
    assert!(matches!(c, Remembered::New(_)));
    // Another namespace: exact and flat, so a new memory.
    let d = store
        .remember(
            "agent:alice2",
            Memory::new("We chose Postgres.").embedding(emb(0.1)),
        )
        .unwrap();
    assert!(matches!(d, Remembered::New(_)));
    check(&store, "dedup");
    db.close().unwrap();
}

#[test]
fn a_deleted_or_superseded_memory_is_not_a_duplicate() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    let ns = "agent:alice";
    let text = "The deploy window is Tuesday.";
    let a = store
        .remember(ns, Memory::new(text).embedding(emb(0.1)))
        .unwrap();
    assert!(store.delete(a.id()).unwrap());
    assert!(!store.delete(a.id()).unwrap());
    assert_eq!(store.get(a.id()).unwrap(), None);
    // Deleted: the dedup row went with it, so the text is new again.
    let b = store
        .remember(ns, Memory::new(text).embedding(emb(0.1)))
        .unwrap();
    assert!(matches!(b, Remembered::New(_)));
    // Superseded: the old text recurring is a new statement, and the dedup row moves to it.
    let newer = store
        .remember(
            ns,
            Memory::new("The deploy window is Thursday.").embedding(emb(0.2)),
        )
        .unwrap();
    store.supersede(b.id(), newer.id()).unwrap();
    let old = store.get(b.id()).unwrap().unwrap();
    assert_eq!(old.superseded_by.as_ref(), Some(newer.id()));
    let n = store.get(newer.id()).unwrap().unwrap();
    assert_eq!(n.links.len(), 1);
    assert_eq!(
        (&n.links[0].id, n.links[0].ty, n.links[0].weight),
        (b.id(), LinkType::SUPERSEDES, 1.0)
    );
    let again = store
        .remember(ns, Memory::new(text).embedding(emb(0.1)))
        .unwrap();
    assert!(matches!(again, Remembered::New(_)));
    let fourth = store
        .remember(ns, Memory::new(text).embedding(emb(0.1)))
        .unwrap();
    assert_eq!(fourth, Remembered::Merged(again.id().clone()));
    check(&store, "delete and supersede");
    db.close().unwrap();
}

#[test]
fn links_go_both_ways_and_a_delete_removes_both() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    let ns = "agent:alice";
    let a = store
        .remember(ns, Memory::new("a").embedding(emb(0.1)))
        .unwrap();
    let b = store
        .remember(
            ns,
            Memory::new("b").embedding(emb(0.2)).link(
                a.id().clone(),
                LinkType::custom(16).unwrap(),
                0.5,
            ),
        )
        .unwrap();
    let c = store
        .remember(ns, Memory::new("c").embedding(emb(0.3)))
        .unwrap();
    store
        .link(c.id(), a.id(), LinkType::custom(17).unwrap(), 0.25)
        .unwrap();
    let am = store.get(a.id()).unwrap().unwrap();
    let mut back: Vec<_> = am.backlinks.iter().map(|l| (l.id.clone(), l.ty)).collect();
    back.sort();
    let mut want = vec![
        (b.id().clone(), LinkType::custom(16).unwrap()),
        (c.id().clone(), LinkType::custom(17).unwrap()),
    ];
    want.sort();
    assert_eq!(back, want);
    check(&store, "links");
    // A link to a missing memory is NotFound, and writes nothing.
    let gone = store
        .remember(ns, Memory::new("gone").embedding(emb(0.4)))
        .unwrap();
    store.delete(gone.id()).unwrap();
    let r = store.remember(
        ns,
        Memory::new("d").embedding(emb(0.5)).link(
            gone.id().clone(),
            LinkType::custom(16).unwrap(),
            1.0,
        ),
    );
    assert!(matches!(r, Err(Error::NotFound(_))), "{r:?}");
    assert!(matches!(
        store.link(c.id(), gone.id(), LinkType::custom(16).unwrap(), 1.0),
        Err(Error::NotFound(_))
    ));
    // Deleting a removes b's and c's links to it.
    store.delete(a.id()).unwrap();
    assert!(store.get(b.id()).unwrap().unwrap().links.is_empty());
    assert!(store.get(c.id()).unwrap().unwrap().links.is_empty());
    check(&store, "after the delete");
    db.close().unwrap();
}

#[test]
fn mark_used_counts_live_memories_and_skips_deleted_ones() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    let ns = "agent:alice";
    let a = store
        .remember(ns, Memory::new("a").embedding(emb(0.1)))
        .unwrap();
    let b = store
        .remember(ns, Memory::new("b").embedding(emb(0.2)))
        .unwrap();
    let m = store
        .mark_used(&[a.id().clone(), a.id().clone(), b.id().clone()])
        .unwrap();
    assert_eq!((m.marked, m.missing.len()), (2, 0));
    store.delete(b.id()).unwrap();
    // S3: mark_used after a delete skips it and never resurrects the row.
    let m = store.mark_used(&[a.id().clone(), b.id().clone()]).unwrap();
    assert_eq!(m.marked, 1);
    assert_eq!(m.missing, vec![b.id().clone()]);
    assert_eq!(store.get(b.id()).unwrap(), None);
    let m = store.get(a.id()).unwrap().unwrap();
    assert_eq!(m.uses, 2);
    assert!(m.last_used_us.is_some());
    check(&store, "mark_used");
    db.close().unwrap();
}

#[test]
fn the_pinned_model_is_checked_at_open_and_on_every_embedding() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    let ns = "agent:alice";
    let a = store
        .remember(ns, Memory::new("a").embedding(emb(0.1)))
        .unwrap();
    drop(store);
    // Another model: Mismatch (ids compare byte for byte).
    assert!(matches!(
        Store::open(&db, "notes", "test-embed-V1"),
        Err(Error::Mismatch(_))
    ));
    let store = Store::open(&db, "notes", MODEL).unwrap();
    assert_eq!(store.embedding_model().unwrap().dim(), 4);
    // A wrong dimension, or no embedding at all, in a pinned store.
    for m in [Memory::new("x").embedding(vec![1.0, 2.0]), Memory::new("x")] {
        let r = store.remember(ns, m);
        assert!(matches!(r, Err(Error::InvalidArgument(_))), "{r:?}");
    }
    // A handle without the model: no content writes, but delete, link and mark_used.
    let bare = Store::open_without_embeddings(&db, "notes").unwrap();
    let r = bare.remember(ns, Memory::new("y").embedding(emb(0.2)));
    assert!(matches!(r, Err(Error::InvalidArgument(_))), "{r:?}");
    let b = store
        .remember(ns, Memory::new("b").embedding(emb(0.2)))
        .unwrap();
    assert!(matches!(
        bare.supersede(a.id(), b.id()),
        Err(Error::InvalidArgument(_))
    ));
    bare.link(a.id(), b.id(), LinkType::custom(16).unwrap(), 1.0)
        .unwrap();
    assert_eq!(bare.mark_used(&[a.id().clone()]).unwrap().marked, 1);
    assert!(bare.delete(b.id()).unwrap());
    check(&bare, "a model-less handle");
    // A store without a model takes no embeddings, and opens only without one.
    let plain = Store::create(&db, "plain", StoreOptions::new()).unwrap();
    assert!(matches!(
        plain.remember(ns, Memory::new("z").embedding(emb(0.1))),
        Err(Error::InvalidArgument(_))
    ));
    plain.remember(ns, Memory::new("z")).unwrap();
    assert!(matches!(
        Store::open(&db, "plain", MODEL),
        Err(Error::InvalidArgument(_))
    ));
    Store::open_without_embeddings(&db, "plain")
        .unwrap()
        .remember(ns, Memory::new("w"))
        .unwrap();
    // I8 and Bit models are refused, as is an empty id.
    assert!(EmbeddingModel::new(MODEL, 4, Element::I8).is_err());
    assert!(EmbeddingModel::new("", 4, Element::F32).is_err());
    db.close().unwrap();
}

#[test]
fn namespaces_are_checked_and_errors_have_codes() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    for ns in ["", "a\0b"] {
        let e = store
            .remember(ns, Memory::new("x").embedding(emb(0.1)))
            .unwrap_err();
        assert_eq!(e.code(), ErrorCode::InvalidArgument, "{ns:?}");
    }
    assert!(matches!(
        Store::create(&db, "notes", StoreOptions::new()),
        Err(Error::Db(e)) if e.code() == ErrorCode::TableExists
    ));
    db.close().unwrap();
}

#[test]
fn a_write_given_up_in_the_callers_transaction_leaves_nothing() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    let ns = "agent:alice";
    let a = store
        .remember(ns, Memory::new("a").embedding(emb(0.1)))
        .unwrap();
    let r = db.transact(&RetryPolicy::default(), |txn| {
        store.remember_in(
            txn,
            ns,
            Memory::new("b").embedding(emb(0.2)).link(
                a.id().clone(),
                LinkType::custom(16).unwrap(),
                1.0,
            ),
        )?;
        Err::<(), _>(Error::InvalidArgument("the application gave up".into()))
    });
    assert!(r.is_err());
    // Nothing of b: no backlink on a, no second memory, and verify agrees.
    assert!(store.get(a.id()).unwrap().unwrap().backlinks.is_empty());
    let st = store.verify().unwrap();
    assert_eq!(st.memories, 1);
    assert!(st.is_consistent(), "{st:?}");
    db.close().unwrap();
}

#[test]
fn a_merge_racing_a_delete_of_its_target_conflicts_then_inserts_as_new() {
    // S3: the merge reads the target's content:text, so a delete committed in between
    // conflicts with it; the retry inserts as new.
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    let ns = "agent:alice";
    let text = "Racing text.";
    let a = store
        .remember(ns, Memory::new(text).embedding(emb(0.1)))
        .unwrap();
    let mut txn = db.transaction().unwrap();
    let r = store
        .remember_in(&mut txn, ns, Memory::new(text).embedding(emb(0.1)))
        .unwrap();
    assert_eq!(r, Remembered::Merged(a.id().clone()));
    store.delete(a.id()).unwrap();
    let e = txn.commit().unwrap_err();
    assert_eq!(e.code(), ErrorCode::Conflict);
    let again = store
        .remember(ns, Memory::new(text).embedding(emb(0.1)))
        .unwrap();
    assert!(matches!(again, Remembered::New(_)));
    check(&store, "merge racing a delete");
    db.close().unwrap();
}

#[test]
fn a_link_racing_a_delete_of_its_target_conflicts() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    let ns = "agent:alice";
    let a = store
        .remember(ns, Memory::new("a").embedding(emb(0.1)))
        .unwrap();
    let mut txn = db.transaction().unwrap();
    store
        .remember_in(
            &mut txn,
            ns,
            Memory::new("b").embedding(emb(0.2)).link(
                a.id().clone(),
                LinkType::custom(16).unwrap(),
                1.0,
            ),
        )
        .unwrap();
    store.delete(a.id()).unwrap();
    assert_eq!(txn.commit().unwrap_err().code(), ErrorCode::Conflict);
    check(&store, "link racing a delete");
    db.close().unwrap();
}

#[test]
fn a_merge_racing_a_supersede_of_its_target_conflicts_then_inserts_as_new() {
    // simp724 on #1362: supersede writes superseded_by without touching the dedup row, so
    // only the merge's in-transaction read of the target (M1) makes the two conflict.
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    let ns = "agent:alice";
    let text = "The window is Tuesday.";
    let a = store
        .remember(ns, Memory::new(text).embedding(emb(0.1)))
        .unwrap();
    let b = store
        .remember(
            ns,
            Memory::new("The window is Thursday.").embedding(emb(0.2)),
        )
        .unwrap();
    let mut txn = db.transaction().unwrap();
    let r = store
        .remember_in(&mut txn, ns, Memory::new(text).embedding(emb(0.1)))
        .unwrap();
    assert_eq!(r, Remembered::Merged(a.id().clone()));
    store.supersede(a.id(), b.id()).unwrap();
    assert_eq!(txn.commit().unwrap_err().code(), ErrorCode::Conflict);
    // The retry sees a superseded target, so the text is a new statement of it.
    let again = store
        .remember(ns, Memory::new(text).embedding(emb(0.1)))
        .unwrap();
    assert!(matches!(again, Remembered::New(_)));
    check(&store, "merge racing a supersede");
    db.close().unwrap();
}

#[test]
fn two_remembers_of_one_new_text_conflict_and_the_retry_merges() {
    // The first reads the dedup row absent; the second commits it; the first's absent-cell
    // read conflicts, and its retry merges.
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    let ns = "agent:alice";
    let text = "Both at once.";
    let mut txn = db.transaction().unwrap();
    let first = store
        .remember_in(&mut txn, ns, Memory::new(text).embedding(emb(0.1)))
        .unwrap();
    assert!(matches!(first, Remembered::New(_)));
    let second = store
        .remember(ns, Memory::new(text).embedding(emb(0.1)))
        .unwrap();
    assert!(matches!(second, Remembered::New(_)));
    assert_eq!(txn.commit().unwrap_err().code(), ErrorCode::Conflict);
    let retry = store
        .remember(ns, Memory::new(text).embedding(emb(0.1)))
        .unwrap();
    assert_eq!(retry, Remembered::Merged(second.id().clone()));
    assert_eq!(store.verify().unwrap().memories, 1);
    check(&store, "two remembers");
    db.close().unwrap();
}

#[test]
fn a_memory_is_superseded_once_and_recalled_again_when_its_superseder_goes() {
    // simp724's G1 and G2 on #1362.
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    let ns = "agent:alice";
    let old = store
        .remember(ns, Memory::new("v1").embedding(emb(0.1)))
        .unwrap();
    let v2 = store
        .remember(ns, Memory::new("v2").embedding(emb(0.2)))
        .unwrap();
    let v3 = store
        .remember(ns, Memory::new("v3").embedding(emb(0.3)))
        .unwrap();
    store.supersede(old.id(), v2.id()).unwrap();
    // Again with the same superseder: nothing changes.
    store.supersede(old.id(), v2.id()).unwrap();
    assert_eq!(store.get(v2.id()).unwrap().unwrap().links.len(), 1);
    // With another: refused, naming the current one.
    let e = store.supersede(old.id(), v3.id()).unwrap_err();
    assert!(
        matches!(&e, Error::InvalidArgument(m) if m.contains(&v2.id().to_string())),
        "{e}"
    );
    // Two concurrent supersedes of one memory: the second reads superseded_by and refuses
    // (in a transact retry it would conflict first, then refuse).
    check(&store, "superseded once");
    // Deleting the superseder clears old's superseded_by, so old is current again.
    store.delete(v2.id()).unwrap();
    let o = store.get(old.id()).unwrap().unwrap();
    assert_eq!(o.superseded_by, None);
    check(&store, "superseder deleted");
    // And it can be superseded again.
    store.supersede(old.id(), v3.id()).unwrap();
    check(&store, "superseded again");
    db.close().unwrap();
}

#[test]
fn link_types_and_weights_are_checked() {
    assert!(LinkType::custom(15).is_err());
    assert_eq!(LinkType::custom(16).unwrap().get(), 16);
    assert_eq!(LinkType::SUPERSEDES.get(), 2);
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let store = pinned(&db);
    let ns = "agent:alice";
    let a = store
        .remember(ns, Memory::new("a").embedding(emb(0.1)))
        .unwrap();
    let b = store
        .remember(ns, Memory::new("b").embedding(emb(0.2)))
        .unwrap();
    let ty = LinkType::custom(16).unwrap();
    for w in [-0.1, 1.5, f32::NAN] {
        assert!(
            matches!(
                store.link(a.id(), b.id(), ty, w),
                Err(Error::InvalidArgument(_))
            ),
            "{w}"
        );
    }
    store.link(a.id(), b.id(), ty, 1.0).unwrap();
    store.link(a.id(), b.id(), ty, 0.0).unwrap();
    check(&store, "weights");
    db.close().unwrap();
}

//! D311 T3 (#1337): near-duplicate dedup. The three policies, the threshold, a superseded
//! match, exact dedup's precedence, the persisted setting, `near_duplicate`, and
//! `remember_in` (which doesn't check), each ending in `verify`.

use pigeonhole::{Element, Options, Pigeonhole};
use pigeonhole_memory::{
    DedupPolicy, EmbeddingModel, Error, LinkType, Memory, NearDedup, Remembered, Store,
    StoreOptions,
};

fn open(dir: &pigeonhole::doc_support::TempDir) -> Pigeonhole {
    Pigeonhole::open(dir.join("mem.phdb"), Options::default().shards(2)).expect("open")
}

fn store(db: &Pigeonhole, near: Option<NearDedup>) -> Store {
    let model = EmbeddingModel::new("e", 2, Element::F32).unwrap();
    let mut o = StoreOptions::new().embedding(model);
    if let Some(n) = near {
        o = o.near_dedup(n);
    }
    Store::create(db, "m", o).unwrap()
}

fn check(store: &Store, ctx: &str) {
    let st = store.verify().unwrap();
    assert!(st.is_consistent(), "{ctx}: {st:?}");
}

fn mem(text: &str, e: [f32; 2]) -> Memory {
    Memory::new(text).embedding(e.to_vec())
}

#[test]
fn link_skip_and_merge_handle_a_near_duplicate() {
    for policy in [DedupPolicy::Link, DedupPolicy::Skip, DedupPolicy::Merge] {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let s = store(&db, Some(NearDedup::new(0.95).policy(policy)));
        let first = s
            .remember(
                "a",
                mem("the deploy runs at noon", [1.0, 0.0]).importance(0.2),
            )
            .unwrap();
        let first = first.id().clone();
        // cos ≈ 0.995: near.
        let r = s
            .remember(
                "a",
                mem("deploys happen at 12", [0.995, 0.0998])
                    .importance(0.7)
                    .source("call-9"),
            )
            .unwrap();
        let m = s.get(&first).unwrap().unwrap();
        match policy {
            DedupPolicy::Link => {
                let Remembered::New(id) = &r else {
                    panic!("{r:?}")
                };
                let n = s.get(id).unwrap().unwrap();
                let l = n
                    .links
                    .iter()
                    .find(|l| l.ty == LinkType::SIMILAR_TO)
                    .unwrap();
                assert_eq!(&l.id, &first);
                assert!((l.weight - 0.995).abs() < 0.01, "{l:?}");
                assert!(m.backlinks.iter().any(|b| &b.id == id));
            }
            DedupPolicy::Skip => {
                assert_eq!(r, Remembered::Similar(first.clone()));
                assert_eq!(m.uses, 0);
            }
            _ => {
                assert_eq!(r, Remembered::Merged(first.clone()));
                assert_eq!((m.importance, m.uses), (0.7, 1));
                assert_eq!(m.sources[0].name, "call-9");
                assert_eq!(m.text, "the deploy runs at noon");
            }
        }
        // Far: written as new, unlinked, under every policy.
        let far = s.remember("a", mem("lunch is at one", [0.0, 1.0])).unwrap();
        let Remembered::New(id) = far else {
            panic!("{far:?}")
        };
        assert!(s.get(&id).unwrap().unwrap().links.is_empty());
        // Another namespace isn't searched.
        let other = s
            .remember("b", mem("deploys happen at 12", [1.0, 0.0]))
            .unwrap();
        assert!(matches!(other, Remembered::New(_)), "{other:?}");
        check(&s, &format!("{policy:?}"));
        db.close().unwrap();
    }
}

#[test]
fn an_exact_duplicate_merges_and_a_superseded_match_is_ignored() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let s = store(&db, Some(NearDedup::new(0.9).policy(DedupPolicy::Skip)));
    let a = s
        .remember("a", mem("the deploy runs at noon", [1.0, 0.0]))
        .unwrap()
        .id()
        .clone();
    // The same text: exact dedup wins over Skip.
    let r = s
        .remember("a", mem("the  deploy runs at noon", [1.0, 0.0]))
        .unwrap();
    assert_eq!(r, Remembered::Merged(a.clone()));
    // Superseded, the match no longer counts (M1's rule).
    let b = s
        .remember("a", mem("the deploy moved", [0.0, 1.0]))
        .unwrap()
        .id()
        .clone();
    s.supersede(&a, &b).unwrap();
    let r = s
        .remember("a", mem("deploys happen at 12", [0.99, 0.1]))
        .unwrap();
    assert!(matches!(r, Remembered::New(_)), "{r:?}");
    check(&s, "exact and superseded");
    db.close().unwrap();
}

#[test]
fn the_setting_persists_and_near_duplicate_reports_what_remember_would_find() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let s = store(&db, None);
    assert_eq!(s.near_dedup().unwrap(), None);
    // Off: near duplicates are just new memories, and near_duplicate refuses.
    let a = s
        .remember("a", mem("the deploy runs at noon", [1.0, 0.0]))
        .unwrap()
        .id()
        .clone();
    let probe = mem("deploys happen at 12", [0.99, 0.1]);
    assert!(matches!(
        s.near_duplicate("a", &probe),
        Err(Error::InvalidArgument(_))
    ));
    let n = NearDedup::new(0.95).policy(DedupPolicy::Skip);
    s.set_near_dedup(Some(n)).unwrap();
    assert_eq!(s.near_dedup().unwrap(), Some(n));
    let (id, sim) = s.near_duplicate("a", &probe).unwrap().unwrap();
    assert_eq!(id, a);
    assert!(sim > 0.99, "{sim}");
    assert_eq!(s.near_duplicate("a", &mem("x", [0.0, 1.0])).unwrap(), None);
    // remember_in doesn't check: the caller's transaction can't contain the check.
    let mut txn = db.transaction().unwrap();
    let r = s.remember_in(&mut txn, "a", probe.clone()).unwrap();
    txn.commit().unwrap();
    assert!(matches!(r, Remembered::New(_)), "{r:?}");
    // Reopened, the setting is still there; then off again.
    drop(s);
    let s = Store::open(&db, "m", "e").unwrap();
    assert_eq!(s.near_dedup().unwrap(), Some(n));
    s.set_near_dedup(None).unwrap();
    assert_eq!(s.near_dedup().unwrap(), None);
    // Refused: a similarity outside −1..1, or a store without a model.
    assert!(s.set_near_dedup(Some(NearDedup::new(1.5))).is_err());
    assert!(s.set_near_dedup(Some(NearDedup::new(f32::NAN))).is_err());
    let plain = Store::create(&db, "p", StoreOptions::new()).unwrap();
    assert!(matches!(
        plain.set_near_dedup(Some(NearDedup::new(0.9))),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        Store::create(
            &db,
            "q",
            StoreOptions::new().near_dedup(NearDedup::new(0.9))
        ),
        Err(Error::InvalidArgument(_))
    ));
    check(&s, "setting");
    db.close().unwrap();
}

/// simp724's F1 on #1386: the match is re-read in `remember`'s transaction under every
/// policy. Deleted or superseded between the check and the transaction (held there), it's
/// ignored: the memory is written as new, and `Skip` doesn't return a gone id.
#[test]
fn a_match_gone_after_the_check_is_ignored_under_every_policy() {
    use pigeonhole_memory::hooks::{Hold, arm};
    for policy in [DedupPolicy::Skip, DedupPolicy::Merge, DedupPolicy::Link] {
        for how in ["deleted", "superseded"] {
            let dir = pigeonhole::doc_support::temp_dir();
            let db = open(&dir);
            let s = store(&db, Some(NearDedup::new(0.95).policy(policy)));
            let m = s
                .remember("a", mem("the deploy runs at noon", [1.0, 0.0]))
                .unwrap()
                .id()
                .clone();
            let held = arm(Hold::RememberAfterNear);
            let r = std::thread::scope(|sc| {
                let w = sc.spawn(|| s.remember("a", mem("deploys happen at 12", [0.99, 0.1])));
                held.wait();
                if how == "deleted" {
                    assert!(s.delete(&m).unwrap());
                } else {
                    let newer = s
                        .remember("a", mem("the deploy moved", [0.0, 1.0]))
                        .unwrap()
                        .id()
                        .clone();
                    s.supersede(&m, &newer).unwrap();
                }
                held.release();
                w.join().unwrap().unwrap()
            });
            let ctx = format!("{policy:?}, {how}");
            let Remembered::New(id) = &r else {
                panic!("{ctx}: {r:?}")
            };
            assert!(
                s.get(id).unwrap().unwrap().links.is_empty(),
                "{ctx}: linked to the gone match"
            );
            check(&s, &ctx);
            db.close().unwrap();
        }
    }
}

#[test]
fn a_similarity_of_zero_or_below_is_refused() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let s = store(&db, None);
    for sim in [0.0, -0.5] {
        assert!(matches!(
            s.set_near_dedup(Some(NearDedup::new(sim))),
            Err(Error::InvalidArgument(_))
        ));
    }
    s.set_near_dedup(Some(NearDedup::new(1.0))).unwrap();
    db.close().unwrap();
}

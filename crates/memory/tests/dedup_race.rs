//! D311 T3 (#1337): a near-duplicate match that's deleted or superseded between the check
//! and the transaction is ignored under every policy (simp724's F1 on #1386).
//!
//! On its own in this binary: a hold is process-wide, so another test's `remember` running
//! in parallel would take it.

use pigeonhole::{Element, Options, Pigeonhole};
use pigeonhole_memory::{
    DedupPolicy, EmbeddingModel, Memory, NearDedup, Remembered, Store, StoreOptions,
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

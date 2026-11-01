//! #1268 item 6: `Search::run_async` (on D296 PR 5's `Nearest::run_async`) returns what
//! `run` returns: for every element and metric, from a cold cache after a reopen, at the
//! current snapshot and at an older one, and across a layout the async path loads itself.
#![cfg(feature = "async")]

mod common;

use std::collections::BTreeMap;

use common::{Rng, V, check, open, specs};
use pigeonhole::doc_support::block_on;
use pigeonhole_vector::{Index, IndexOptions, Search};

fn both<'a>(search: impl Fn() -> Search<'a>, ctx: &str) {
    let want = search().run().unwrap();
    let got = block_on(search().run_async()).unwrap();
    assert_eq!(got, want, "{ctx}");
}

fn query<'a>(index: &'a Index, q: &'a V, k: usize, nprobe: u32) -> Search<'a> {
    match q {
        V::F32(x) => index.search_f32(x, k),
        V::I8(x) => index.search_i8(x, k),
        V::Bits(x) => index.search_bits(x, k),
    }
    .nprobe(nprobe)
}

#[test]
fn an_async_search_returns_what_the_sync_one_returns() {
    for spec in specs() {
        let ctx = format!("{spec:?}");
        let dir = pigeonhole::doc_support::temp_dir();
        let mut model = BTreeMap::new();
        {
            let db = open(&dir);
            let index =
                Index::create(&db, "idx", spec, IndexOptions::new().target_list_len(8)).unwrap();
            let mut rng = Rng::new(21);
            for i in 0..300u32 {
                let c = rng.below(6);
                let v = V::random(&mut rng, &spec, c);
                v.insert(&index, &i.to_be_bytes()).unwrap();
                model.insert(i.to_be_bytes().to_vec(), v);
            }
            db.flush().unwrap();
            drop(index);
            db.close().unwrap();
        }
        // Cold: a reopen, then the async search first, so it loads the layout and reads the
        // lists' blocks itself.
        let db = open(&dir);
        let index = &Index::open(&db, "idx").unwrap();
        check(index, &model, &ctx);
        let mut rng = Rng::new(22);
        let qs: Vec<V> = (0..4)
            .map(|_| {
                let c = rng.below(6);
                V::random(&mut rng, &spec, c)
            })
            .collect();
        for (i, q) in qs.iter().enumerate() {
            let cold = block_on(query(index, q, 10, 3).run_async()).unwrap();
            assert_eq!(
                cold,
                query(index, q, 10, 3).run().unwrap(),
                "{ctx} cold {i}"
            );
            for (k, nprobe) in [(1, 1), (10, 3), (50, 1000)] {
                both(
                    || query(index, q, k, nprobe),
                    &format!("{ctx} q{i} k{k} p{nprobe}"),
                );
            }
        }
        // An older snapshot, then a layout change the async path must load itself.
        let before = &db.snapshot().unwrap();
        let mut rng = Rng::new(23);
        for i in 300..500u32 {
            let c = rng.below(6);
            V::random(&mut rng, &spec, c)
                .insert(index, &i.to_be_bytes())
                .unwrap();
        }
        for q in &qs {
            both(
                || query(index, q, 10, 1000).snapshot(before),
                &format!("{ctx} older"),
            );
            both(|| query(index, q, 10, 4), &format!("{ctx} after splits"));
        }
        db.close().unwrap();
    }
}

/// `run_async`'s future is `Send` (compile-time).
#[allow(dead_code)]
fn run_async_is_send(index: &Index, q: &[f32]) {
    fn send<T: Send>(_: T) {}
    send(index.search_f32(q, 3).run_async());
}

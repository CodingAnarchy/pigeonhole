//! #1268 item 6: `Search::run_async` (on D296 PR 5's `Nearest::run_async`) returns what
//! `run` returns: for every element and metric, from a cold cache after a reopen (the
//! counters show its fetches were asynchronous), at the current snapshot and at an older one,
//! and across a layout change a stale handle's async search loads itself.
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
        V::F16(x) => index.search_f16(x, k),
        V::BF16(x) => index.search_bf16(x, k),
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
        // Cold: a reopen, and the async search is the first read of the lists (Index::open
        // reads only the C, N and Q rows). The counters prove the async path ran, as #1269
        // does: the search missed the cache, and fewer of its reads were synchronous than it
        // missed, so the rest were fetched asynchronously.
        let db = open(&dir);
        let index = &Index::open(&db, "idx").unwrap();
        let mut rng = Rng::new(22);
        let qs: Vec<V> = (0..4)
            .map(|_| {
                let c = rng.below(6);
                V::random(&mut rng, &spec, c)
            })
            .collect();
        let before = db.engine_metrics();
        let cold = block_on(query(index, &qs[0], 10, 3).run_async()).unwrap();
        let after = db.engine_metrics();
        let misses = after.block_cache_misses - before.block_cache_misses;
        let sync_reads = after.async_sync_reads - before.async_sync_reads;
        assert!(misses > 0, "{ctx}: the cold search missed the cache");
        assert!(
            sync_reads < misses,
            "{ctx}: some of {misses} missed blocks were fetched asynchronously ({sync_reads} sync)"
        );
        assert_eq!(
            cold,
            query(index, &qs[0], 10, 3).run().unwrap(),
            "{ctx} cold"
        );
        check(index, &model, &ctx);
        for (i, q) in qs.iter().enumerate() {
            for (k, nprobe) in [(1, 1), (10, 3), (50, 1000)] {
                both(
                    || query(index, q, k, nprobe),
                    &format!("{ctx} q{i} k{k} p{nprobe}"),
                );
            }
        }
        // A layout change that `stale` must load itself: it's opened before the inserts that
        // split lists, so its centroid cache predates them, and its `run_async` goes first.
        let stale = &Index::open(&db, "idx").unwrap();
        let older = &db.snapshot().unwrap();
        let lists = index.verify().unwrap().list_lens.len();
        let mut rng = Rng::new(23);
        for i in 300..500u32 {
            let c = rng.below(6);
            V::random(&mut rng, &spec, c)
                .insert(index, &i.to_be_bytes())
                .unwrap();
        }
        assert!(
            index.verify().unwrap().list_lens.len() > lists,
            "{ctx}: the inserts split lists, so the layout changed"
        );
        for (i, q) in qs.iter().enumerate() {
            let got = block_on(query(stale, q, 10, 4).run_async()).unwrap();
            let want = query(index, q, 10, 4).run().unwrap();
            assert_eq!(
                got, want,
                "{ctx} q{i}: a stale handle's async search loads the layout"
            );
            both(
                || query(index, q, 10, 1000).snapshot(older),
                &format!("{ctx} older"),
            );
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

/// D311 T8: a prefixed search widens the same way async (`run_async` against `run`).
#[test]
fn a_prefixed_async_search_widens_as_the_sync_one_does() {
    let spec =
        pigeonhole::VectorSpec::new(2, pigeonhole::Element::F32, pigeonhole::Metric::L2).unwrap();
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let index = Index::create(&db, "idx", spec, IndexOptions::new().target_list_len(8)).unwrap();
    for i in 0..300u32 {
        let id = [b"b/".as_slice(), &i.to_be_bytes()].concat();
        let v = [(i % 5) as f32 * 10.0 + (i % 97) as f32 * 0.01, 0.0];
        index.insert_f32(&id, &v).unwrap();
    }
    for i in 0..6u32 {
        let id = [b"a/".as_slice(), &i.to_be_bytes()].concat();
        index.insert_f32(&id, &[100.0 + i as f32, 50.0]).unwrap();
    }
    let q = [0.0f32, 0.0];
    for (k, max) in [(3, None), (10, None), (3, Some(2))] {
        both(
            || {
                let s = index.search_f32(&q, k).nprobe(1).id_prefix(b"a/");
                match max {
                    Some(m) => s.max_probe(m),
                    None => s,
                }
            },
            &format!("k {k}, max_probe {max:?}"),
        );
    }
    let hits = block_on(
        index
            .search_f32(&q, 10)
            .nprobe(1)
            .id_prefix(b"a/")
            .run_async(),
    )
    .unwrap();
    assert_eq!(hits.len(), 6, "widened to every list");
    db.close().unwrap();
}

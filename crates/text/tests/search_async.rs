//! #1560 PR 2 (ICR 0124): `Search::run_async` and `run_with_stats_async` return exactly what
//! `run` and `run_with_stats` do, for every query kind (terms, a hub term, several terms,
//! fuzzy terms, phrases at slop 0 and 2, `id_prefix`, highlighting), from a cold block cache,
//! without reading a block synchronously, and their futures are `Send`.
//!
//! The store is on the simulated device with its I/O deferred and completed after each
//! `Pending`: every block the search misses is fetched, so the core is resumed at its read
//! points throughout (and at its step budget's yields), and no read completes at submit,
//! which would count as synchronous on real files.
#![cfg(feature = "async")]

mod common;

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use common::{Rng, seeds, source, text, word};
use pigeonhole::{Family, Options, Pigeonhole};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_text::{Fuzziness, Index, IndexOptions, Query};

const PATH: &str = "/db/text.phdb";

fn opts(vfs: &Arc<SimVfs>) -> Options {
    Options::default().vfs(Arc::clone(vfs) as _).shards(2)
}

/// A corpus of `n` documents (skewed words: "the" is a hub term), flushed and compacted into
/// several SST blocks, a few more in a memtable; reopened, so the block cache is cold.
fn build(vfs: &Arc<SimVfs>, n: u32, seed: u64) -> Pigeonhole {
    let db = Pigeonhole::open(PATH, opts(vfs)).expect("open");
    let docs = db
        .table("docs")
        .unwrap()
        .family("body", Family::default())
        .create_if_missing()
        .unwrap();
    let index = Index::create(
        &db,
        "docs_text",
        source(&docs),
        IndexOptions::new().positions(true),
    )
    .expect("create");
    let mut rng = Rng::new(seed);
    for i in 0..n {
        let doc = format!("d{i:05}").into_bytes();
        index.put(&doc, &text(&mut rng, 30)).unwrap();
        if i % 1000 == 999 {
            db.flush().unwrap();
        }
    }
    db.compact().unwrap();
    for i in n..n + 20 {
        let doc = format!("d{i:05}").into_bytes();
        index.put(&doc, &text(&mut rng, 30)).unwrap();
    }
    drop(index);
    db.close().unwrap();
    Pigeonhole::open(PATH, opts(vfs)).expect("reopen")
}

/// Polls `fut` to completion with the device's I/O deferred, completing it after each
/// `Pending` (a step-budget yield has none in flight, and is simply polled again).
fn drive<F: Future>(vfs: &SimVfs, fut: F) -> (F::Output, u32) {
    let mut fut = std::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    vfs.set_deferred_io(true);
    let mut pending = 0;
    let out = loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => break v,
            Poll::Pending => {
                pending += 1;
                assert!(pending < 1_000_000, "never resolved");
                vfs.complete_all_io();
            }
        }
    };
    vfs.set_deferred_io(false);
    (out, pending)
}

const QUERIES: &[&str] = &[
    "memory",
    "the",
    "the memory store",
    "agent postgres meeting march chose index search phrase",
    "postgress~",
    "serch~1 the",
    "\"the memory\"",
    "\"memory the\"~2",
    "\"the store\" agent~",
];

#[test]
fn an_async_search_returns_what_the_sync_one_returns() {
    for seed in [1u64, 1560] {
        let vfs = SimVfs::new(seed);
        let db = build(&vfs, 3000, seed);
        let index = Index::open(&db, "docs_text").expect("open index");
        let mut pendings = 0;
        for q in QUERIES {
            for (k, prefix, highlight) in [
                (10, None, false),
                (100, None, false),
                (3, Some(&b"d01"[..]), true),
            ] {
                let search = || {
                    let mut s = index.search(*q, k).highlight(highlight);
                    if let Some(p) = prefix {
                        s = s.id_prefix(p);
                    }
                    s
                };
                let before = db.engine_metrics().async_sync_reads;
                let (got, pending) = drive(&vfs, search().run_with_stats_async());
                let reads = db.engine_metrics().async_sync_reads - before;
                pendings += pending;
                let (got, got_stats) = got.expect("run_with_stats_async");
                let (want, want_stats) = search().run_with_stats().expect("run_with_stats");
                let ctx = format!("seed {seed}, {q:?}, k {k}, prefix {prefix:?}");
                assert_eq!(got, want, "{ctx}");
                assert_eq!(format!("{got_stats:?}"), format!("{want_stats:?}"), "{ctx}");
                assert_eq!(reads, 0, "{ctx}: synchronous reads");
                let (hits, _) = drive(&vfs, search().run_async());
                assert_eq!(hits.expect("run_async"), want, "{ctx}");
            }
        }
        // The first searches started cold: their blocks were fetched.
        assert!(pendings > 0, "seed {seed}: nothing was fetched");
        drop(index);
        vfs.complete_all_io();
        db.close().unwrap();
    }
}

#[test]
fn async_search_futures_are_send() {
    fn send<T: Send>(_: &T) {}
    let vfs = SimVfs::new(7);
    let db = build(&vfs, 50, 7);
    let index = Index::open(&db, "docs_text").expect("open index");
    let run = index.search("memory", 3).run_async();
    send(&run);
    drop(run);
    let stats = index.search("memory~", 3).run_with_stats_async();
    send(&stats);
    drop(stats);
    drop(index);
    db.close().unwrap();
}

/// A word with one edit (a dropped, swapped or replaced letter): a fuzzy term's input.
fn misspell(rng: &mut Rng) -> String {
    let mut w: Vec<u8> = word(rng).as_bytes().to_vec();
    let i = rng.below(w.len() as u64) as usize;
    match rng.below(3) {
        0 if w.len() > 2 => {
            w.remove(i);
        }
        1 if i + 1 < w.len() => w.swap(i, i + 1),
        _ => w[i] = b'a' + rng.below(26) as u8,
    }
    String::from_utf8(w).expect("ascii")
}

/// A random disjunction of every clause kind: terms, text, phrases (slop 0 to 3), quoted
/// phrases and fuzzy terms (every fuzziness).
fn random_query(rng: &mut Rng) -> Query {
    let mut q = Query::new();
    for _ in 0..1 + rng.below(4) {
        q = match rng.below(6) {
            0 => q.term(word(rng)),
            1 => q.text(format!("{} {}", word(rng), word(rng))),
            2 => {
                let words: Vec<&str> = (0..2 + rng.below(2)).map(|_| word(rng)).collect();
                q.phrase_with_slop(words, rng.below(4) as u16)
            }
            3 => q.quoted_phrase(format!("{} {}", word(rng), word(rng)), rng.below(3) as u16),
            4 => {
                let f = [Fuzziness::Auto, Fuzziness::One, Fuzziness::Two][rng.below(3) as usize];
                q.fuzzy(misspell(rng), f)
            }
            _ => q.fuzzy(word(rng), Fuzziness::Auto),
        };
    }
    q
}

/// The condition perf287 and the coordinator set on #1560 PR 2's two document loops (the
/// sync `top_k` and the async `TopK::run`): a seeded property over every query kind. Random
/// corpora and queries (mixes of every clause kind, `k` from 1 to 100, `id_prefix`,
/// highlighting, `max_expansions` and `prefix_length`), with deletes and new documents
/// between rounds: the async search equals the sync one, hits and stats, and reads nothing
/// synchronously. `TEXT_SEED=<n>` runs one seed (a long sweep passes its seeds that way).
#[test]
fn async_equals_sync_over_random_corpora_and_queries() {
    for seed in seeds(&[11, 12, 13]) {
        let vfs = SimVfs::new(seed);
        let db = build(&vfs, 600, seed);
        let index = Index::open(&db, "docs_text").expect("open index");
        let mut rng = Rng::new(seed ^ 0x1560);
        let mut next_doc = 620;
        for round in 0..3 {
            if round > 0 {
                for _ in 0..40 {
                    let doc = format!("d{:05}", rng.below(next_doc));
                    index.delete(doc.as_bytes()).unwrap();
                }
                for _ in 0..20 {
                    let doc = format!("d{next_doc:05}");
                    index.put(doc.as_bytes(), &text(&mut rng, 30)).unwrap();
                    next_doc += 1;
                }
            }
            for i in 0..40 {
                let query = random_query(&mut rng);
                let k = [1, 3, 10, 100][rng.below(4) as usize];
                let prefix = (rng.below(4) == 0).then(|| format!("d00{}", rng.below(6)));
                let highlight = rng.below(2) == 0;
                let max = [1, 3, 50][rng.below(3) as usize];
                let prefix_length = rng.below(3) as usize;
                let search = || {
                    let mut s = index
                        .search(query.clone(), k)
                        .highlight(highlight)
                        .max_expansions(max)
                        .prefix_length(prefix_length);
                    if let Some(p) = &prefix {
                        s = s.id_prefix(p.as_bytes());
                    }
                    s
                };
                let ctx = format!(
                    "seed {seed}, round {round}, query {i}: {query:?}, k {k}, prefix {prefix:?}, \
                     max {max}, prefix_length {prefix_length}"
                );
                let before = db.engine_metrics().async_sync_reads;
                let (got, _) = drive(&vfs, search().run_with_stats_async());
                let reads = db.engine_metrics().async_sync_reads - before;
                let (got, got_stats) = got.unwrap_or_else(|e| panic!("{ctx}: {e}"));
                let (want, want_stats) = search().run_with_stats().expect("run_with_stats");
                assert_eq!(got, want, "{ctx}");
                assert_eq!(format!("{got_stats:?}"), format!("{want_stats:?}"), "{ctx}");
                assert_eq!(reads, 0, "{ctx}: synchronous reads");
            }
        }
        drop(index);
        vfs.complete_all_io();
        db.close().unwrap();
    }
}

/// ai1249's R1 on #1642: the loops outside MaxScore spend one shared step budget, so a warm
/// search (no read to wait for) still yields. Three phrases over the hub word walk its
/// position row three times and read their lead terms' lengths, more than a poll's budget
/// together (less than one each, so a budget per loop wouldn't yield), and so yield with no
/// I/O in flight; and they still return what the sync search does.
#[test]
fn a_warm_phrase_search_yields_without_io() {
    let vfs = SimVfs::new(5);
    let db = build(&vfs, 3000, 5);
    let index = Index::open(&db, "docs_text").expect("open index");
    let search = || index.search("\"the memory\" \"the store\" \"the agent\" memory~", 10);
    // Warm the cache.
    let (warm, _) = drive(&vfs, search().run_async());
    let want = search().run().expect("run");
    assert_eq!(warm.expect("run_async"), want);
    // Warm now: every `Pending` is a yield, with nothing in flight. In a block: the pinned
    // future borrows the index until its scope ends.
    let (got, yields) = {
        let mut fut = std::pin::pin!(search().run_async());
        let mut cx = Context::from_waker(Waker::noop());
        let mut yields = 0;
        let got = loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(v) => break v,
                Poll::Pending => {
                    assert_eq!(vfs.io_in_flight(), 0, "a warm search waited on I/O");
                    yields += 1;
                    assert!(yields < 10_000, "never resolved");
                }
            }
        };
        (got, yields)
    };
    assert_eq!(got.expect("run_async"), want);
    assert!(yields > 0, "a warm phrase over the hub word never yielded");
    drop(index);
    db.close().unwrap();
}

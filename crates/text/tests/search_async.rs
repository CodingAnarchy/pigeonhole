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
///
/// With `packed` (#1585) the index is packed once the corpus is in: a version 2 index, its
/// postings in blocks, then some deletes (tombstones over the blocks) and the last few
/// documents as cells over them.
fn build(vfs: &Arc<SimVfs>, n: u32, seed: u64, packed: bool) -> Pigeonhole {
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
    if packed {
        index.pack().unwrap();
        db.flush().unwrap();
        for i in (0..n).step_by(17) {
            index.delete(format!("d{i:05}").as_bytes()).unwrap();
        }
        for i in (0..n).step_by(17) {
            let doc = format!("d{i:05}").into_bytes();
            index.put(&doc, &text(&mut rng, 30)).unwrap();
        }
        for i in (5..n).step_by(29) {
            index.delete(format!("d{i:05}").as_bytes()).unwrap();
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
    // A repeated word: one stream of positions for both of its terms (#1586).
    "\"the the\"",
    "\"the memory the\"~2",
    "\"memory the memory\"",
    // A rare lead beside the hub term: the hub's positions come by point reads, and the
    // lead's lengths by point reads and then its walk (ai1249's S2 on #1997). The phrases
    // above take the hub's walk from the start.
    "\"term the\"",
    "\"the term\"~2",
    "\"rank the term\"~3",
];

#[test]
fn an_async_search_returns_what_the_sync_one_returns() {
    async_equals_sync(false);
}

/// The same over a version 2 index (#1585): blocks, tombstones over them and cells over
/// them, each tier read without blocking.
#[test]
fn an_async_search_of_a_packed_index_returns_what_the_sync_one_returns() {
    async_equals_sync(true);
}

fn async_equals_sync(packed: bool) {
    for seed in [1u64, 1560] {
        let vfs = SimVfs::new(seed);
        let db = build(&vfs, 3000, seed, packed);
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
                let ctx = format!("seed {seed}, packed {packed}, {q:?}, k {k}, prefix {prefix:?}");
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
    let db = build(&vfs, 50, 7, false);
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
    random_rounds(false);
}

/// The property over a version 2 index (#1585), packed again part way between rounds, and
/// against a twin that is never packed and gets the same writes: the packed index's hits
/// and stats are the twin's, for every random query.
#[test]
fn a_packed_index_equals_its_twin_and_async_equals_sync_over_random_queries() {
    random_rounds(true);
}

fn random_rounds(packed: bool) {
    for seed in seeds(&[11, 12, 13]) {
        let vfs = SimVfs::new(seed);
        let db = build(&vfs, 600, seed, packed);
        let index = Index::open(&db, "docs_text").expect("open index");
        // The twin: the same corpus and the same writes, never packed.
        let twin = packed.then(|| {
            let vfs = SimVfs::new(seed);
            let db = build(&vfs, 600, seed, false);
            let index = Index::open(&db, "docs_text").expect("open the twin");
            // What `build` does to a packed index after its pack, in the same order.
            let mut rng = Rng::new(seed);
            for _ in 0..600 {
                text(&mut rng, 30);
            }
            for i in (0..600).step_by(17) {
                index.delete(format!("d{i:05}").as_bytes()).unwrap();
            }
            for i in (0..600).step_by(17) {
                let doc = format!("d{i:05}").into_bytes();
                index.put(&doc, &text(&mut rng, 30)).unwrap();
            }
            for i in (5..600).step_by(29) {
                index.delete(format!("d{i:05}").as_bytes()).unwrap();
            }
            // The last documents, with the texts the packed build drew for them.
            for i in 600..620 {
                let doc = format!("d{i:05}").into_bytes();
                index.put(&doc, &text(&mut rng, 30)).unwrap();
            }
            (db, index)
        });
        let mut rng = Rng::new(seed ^ 0x1560);
        let mut next_doc = 620;
        for round in 0..3 {
            if round > 0 {
                for _ in 0..40 {
                    let doc = format!("d{:05}", rng.below(next_doc));
                    index.delete(doc.as_bytes()).unwrap();
                    if let Some((_, twin)) = &twin {
                        twin.delete(doc.as_bytes()).unwrap();
                    }
                }
                for _ in 0..20 {
                    let doc = format!("d{next_doc:05}");
                    let t = text(&mut rng, 30);
                    index.put(doc.as_bytes(), &t).unwrap();
                    if let Some((_, twin)) = &twin {
                        twin.put(doc.as_bytes(), &t).unwrap();
                    }
                    next_doc += 1;
                }
                if packed && round == 1 {
                    // Part of a pack: some terms' tombstones and cells are merged, the rest
                    // wait.
                    let st = index
                        .pack_with(pigeonhole_text::PackOptions::new(), |st| {
                            if st.terms >= 6 {
                                std::ops::ControlFlow::Break(())
                            } else {
                                std::ops::ControlFlow::Continue(())
                            }
                        })
                        .unwrap();
                    assert!(st.stopped, "{st:?}");
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
                if let Some((_, twin)) = &twin {
                    let mut s = twin
                        .search(query.clone(), k)
                        .highlight(highlight)
                        .max_expansions(max)
                        .prefix_length(prefix_length);
                    if let Some(p) = &prefix {
                        s = s.id_prefix(p.as_bytes());
                    }
                    let (plain, plain_stats) = s.run_with_stats().expect("the twin");
                    assert_eq!(want, plain, "{ctx}: against the twin");
                    assert_eq!(
                        format!("{want_stats:?}"),
                        format!("{plain_stats:?}"),
                        "{ctx}: against the twin"
                    );
                }
            }
        }
        if let Some((db, twin)) = twin {
            drop(twin);
            db.close().unwrap();
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
    let db = build(&vfs, 3000, 5, false);
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

/// #2010: a fuzzy expansion that is walked again mid-search steps past the last document
/// decided (`Walk::pass`, #1363). Nothing failed when it stopped on that document instead:
/// the document then became a candidate a second time and was returned twice.
///
/// The search is `wa~1 zeta` at top 2. `wa` expands to `wa` (boost 1) and `wb` (boost 0.5):
///
/// | documents | text | fuzzy clause | `zeta` |
/// |---|---|---|---|
/// | `d000`, `d001` | `wa` and 4 pads | `weak` | |
/// | `d002` | `wa` × 4 | `strong` | |
/// | `d002x` | `wb` and 8 pads | little | |
/// | `d003` | `zeta` × 2, `wb` × 3, 4 pads | `wb` (the row's bound) | `zeta` (the row's bound) |
/// | `d004` to `d023` | `zeta` and 12 pads | | less |
/// | `e000` to `e030` | `wb` and 8 pads | little | |
/// | `f000` to `f399` | 6 pads | | |
///
/// 1. `d000` and `d001` fill the top 2, so the threshold is `weak`. `wb`'s bound is under
///    it (`wb < weak`): `wb` stops being walked, standing on `d002x`, its first posting.
///    `zeta` stays essential (`weak < zeta`).
/// 2. `d002` enters. `d003` is a candidate through `zeta`, scores `zeta + wb` and enters:
///    the threshold is `strong`. `wb`'s part came by a point read (its row has 30 more
///    postings, so reading is cheaper than stepping), which leaves its walk on `d002x`.
/// 3. `zeta` turns non-essential (`zeta < strong`), and what the fuzzy clause must bring
///    alone falls to `strong − zeta`, under `wb`'s bound: `wb` is walked again, from
///    `d002x`, with `d003` the last document decided. It must step to `e000`.
/// 4. If it stops on `d003`, that is the fuzzy clause's next candidate. `zeta`'s lookup is a
///    point read too (20 postings are left in its row), so it scores `zeta + wb` again,
///    above the worst held (`strong`): the hits are `d003` twice.
///
/// **The walk must start before `d003`.** A walk standing on the document itself always
/// moves off it (every advance steps at least once), which is why a first version of this
/// test, with `d003` as `wb`'s only posting, passed with all three mutants.
///
/// **On a packed index step 4 shows in no hit.** There a lookup doesn't look behind its
/// walk, so `zeta`, already past `d003`, adds nothing the second time and `d003` scores too
/// little to enter twice. What fails is the document loop's own check that candidates
/// ascend (a `debug_assert`, in both loops), which the tests' profile has on.
///
/// The four inequalities are asserted from the index's own scores, so a change to the
/// corpus or to BM25 that takes the search off this path fails here rather than passing
/// quietly. Sync and async, unpacked and packed: the sync walk, the async walk's seek and
/// the merged walk each have their own "past", and a mutant of each fails this test.
#[test]
fn a_fuzzy_expansion_walked_again_steps_past_the_last_decided_document() {
    for packed in [false, true] {
        let vfs = SimVfs::new(2010);
        let db = Pigeonhole::open(PATH, opts(&vfs)).expect("open");
        let docs = db
            .table("docs")
            .unwrap()
            .family("body", Family::default())
            .create_if_missing()
            .unwrap();
        let index =
            Index::create(&db, "docs_text", source(&docs), IndexOptions::new()).expect("create");
        let pads = |n: usize| " pad".repeat(n);
        let put = |id: String, text: String| index.put(id.as_bytes(), &text).unwrap();
        put("d000".into(), format!("wa{}", pads(4)));
        put("d001".into(), format!("wa{}", pads(4)));
        put("d002".into(), "wa wa wa wa".into());
        put("d002x".into(), format!("wb{}", pads(8)));
        put("d003".into(), format!("zeta zeta wb wb wb{}", pads(4)));
        for i in 4..24 {
            put(format!("d{i:03}"), format!("zeta{}", pads(12)));
        }
        for i in 0..31 {
            put(format!("e{i:03}"), format!("wb{}", pads(8)));
        }
        for i in 0..400 {
            put(format!("f{i:03}"), pads(6));
        }
        if packed {
            index.pack().unwrap();
        }
        // From a cold cache, as the other tests here: the async search then yields at its
        // reads.
        drop(index);
        db.close().unwrap();
        let db = Pigeonhole::open(PATH, opts(&vfs)).expect("reopen");
        let index = Index::open(&db, "docs_text").expect("open index");

        // The scenario's scores, from the index itself.
        let fuzzy = index
            .search(Query::new().fuzzy("wa", Fuzziness::One), 40)
            .run()
            .unwrap();
        let of = |id: &[u8]| {
            fuzzy
                .iter()
                .find(|h| h.id == id)
                .unwrap_or_else(|| panic!("{:?} isn't a fuzzy match", id))
                .score
        };
        let (weak, strong, wb) = (of(b"d000"), of(b"d002"), of(b"d003"));
        let zeta = index.search(Query::new().term("zeta"), 1).run().unwrap();
        assert_eq!(zeta[0].id, b"d003");
        let zeta = zeta[0].score;
        let ctx = format!("packed {packed}: weak {weak}, strong {strong}, wb {wb}, zeta {zeta}");
        assert!(
            wb < weak,
            "{ctx}: wb must go unwalked at the first threshold"
        );
        assert!(weak < zeta, "{ctx}: zeta must stay essential then");
        assert!(zeta < strong, "{ctx}: zeta must turn non-essential");
        assert!(strong - zeta < wb, "{ctx}: wb must be walked again");

        let query = || Query::new().fuzzy("wa", Fuzziness::One).term("zeta");
        let ids = |hits: &[pigeonhole_text::Hit]| -> Vec<String> {
            hits.iter()
                .map(|h| String::from_utf8_lossy(&h.id).into_owned())
                .collect()
        };
        let got = index.search(query(), 2).run().unwrap();
        assert_eq!(ids(&got), ["d003", "d002"], "{ctx}: sync");
        assert!((got[0].score - (zeta + wb)).abs() < 1e-9, "{ctx}: {got:?}");
        assert!((got[1].score - strong).abs() < 1e-9, "{ctx}: {got:?}");
        let (hits, _) = drive(&vfs, index.search(query(), 2).run_async());
        let hits = hits.expect("run_async");
        assert_eq!(ids(&hits), ["d003", "d002"], "{ctx}: async");
        assert_eq!(hits, got, "{ctx}: async against sync");

        drop(index);
        vfs.complete_all_io();
        db.close().unwrap();
    }
}

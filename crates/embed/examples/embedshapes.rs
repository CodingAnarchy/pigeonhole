//! The local embedder's instruction counts (#1828), for `scripts/instructions-per-cell.sh`:
//! `embedshapes SHAPE ITERATIONS DIR`.
//!
//! | shape | measured | units |
//! |---|---|---|
//! | `embed-query` | 4 queries per iteration, one `embed_query` each (about 16 tokens) | texts |
//! | `embed-passages` | one `embed_passages` of 2 passages (about 32 tokens each) per iteration | texts |
//!
//! The model is pigeonhole-embed's pinned `BgeSmallEnV15`, from the cache
//! (`$PIGEONHOLE_EMBED_CACHE`, downloaded there unless `PIGEONHOLE_EMBED_OFFLINE` is set).
//! Loading it is setup.
//!
//! **What's counted:** an embedder runs every call on its own thread pool
//! (`LoadOptions::threads`), not on the caller's thread, and callgrind's collection toggle
//! (`support/measure.rs`) counts one thread. So the pool has one thread, and
//! `shape_pool_thread`, its start hook, turns counting on for that thread's whole life: it
//! runs nothing but the measured calls. The caller only waits. candle's own barrier pool is
//! one thread too (`CANDLE_NUM_THREADS=1`, set first thing), so it runs inline there. A setup-only run
//! (`SHAPE_SETUP_ONLY=1`) installs no hook and makes no call, so it counts nothing.
//! `DIR` is unused (the harness passes it to every shape binary).
#[allow(
    unexpected_cfgs,
    reason = "support/measure.rs is the bench crate's (`fixed-seed`)"
)]
#[path = "../../bench/examples/support/measure.rs"]
mod measure;
use measure::Measured;

use pigeonhole_embed::{LoadOptions, LocalEmbedder, Model};

/// Queries per `embed-query` iteration.
const QUERIES: usize = 4;
/// Passages in an `embed-passages` call: 2, since one batch of 8 took over an hour
/// under valgrind (the attention grows with the batch's tokens).
const PASSAGES: usize = 2;

/// A text of about `tokens` tokens (short common words, one token each), varied by `seed`
/// so no two texts of a run are the same.
fn text(tokens: usize, seed: usize) -> String {
    const WORDS: [&str; 16] = [
        "the", "meeting", "moved", "to", "thursday", "after", "lunch", "again", "dana", "said",
        "she", "prefers", "tabs", "over", "spaces", "today",
    ];
    (0..tokens.saturating_sub(2))
        .map(|i| WORDS[(i * 7 + seed * 3) % WORDS.len()])
        .collect::<Vec<_>>()
        .join(" ")
}

/// The pool thread's start hook: counts everything the thread does from here on. It runs
/// only the embedder's calls, all of them measured.
fn shape_pool_thread() {
    std::mem::forget(Measured::start());
}

fn main() {
    // candle (0.11) also splits large operations over a process-wide `BarrierPool` of its
    // own, sized by `CANDLE_NUM_THREADS` (every core by default), not by
    // `LoadOptions::threads`: its workers' share went uncounted and the measured thread spun
    // in `BarrierGuard::drop` for a timing-dependent while (#2060's first run: 344M to 370M
    // on identical code). One thread runs those operations inline, so every instruction is
    // on the measured thread.
    // SAFETY: first thing in `main`, before any other thread exists or reads the variable.
    unsafe { std::env::set_var("CANDLE_NUM_THREADS", "1") };
    measure::fix_hash_seeds();
    let mut args = std::env::args().skip(1);
    let usage = "usage: embedshapes SHAPE ITERATIONS DIR";
    let shape = args.next().expect(usage);
    let iters: usize = args.next().expect(usage).parse().expect("ITERATIONS");
    let _dir = args.next().expect(usage);
    let setup_only = std::env::var_os("SHAPE_SETUP_ONLY").is_some();
    let mut options = LoadOptions::new().threads(1);
    if !setup_only {
        options = options.on_thread_start(shape_pool_thread);
    }
    let embedder = LocalEmbedder::open(Model::BgeSmallEnV15, &options).expect("load the model");
    let units = match shape.as_str() {
        "embed-query" => {
            let queries: Vec<String> = (0..QUERIES * iters).map(|i| text(16, i)).collect();
            if !setup_only {
                for q in &queries {
                    std::hint::black_box(embedder.embed_query(q).expect("embed"));
                }
            }
            queries.len()
        }
        "embed-passages" => {
            let passages: Vec<Vec<String>> = (0..iters)
                .map(|i| (0..PASSAGES).map(|j| text(32, i * PASSAGES + j)).collect())
                .collect();
            if !setup_only {
                for batch in &passages {
                    let batch: Vec<&str> = batch.iter().map(String::as_str).collect();
                    std::hint::black_box(embedder.embed_passages(&batch).expect("embed"));
                }
            }
            iters * PASSAGES
        }
        other => panic!("unknown shape {other}: embed-query, embed-passages"),
    };
    eprintln!("units {}", if setup_only { 0 } else { units });
}

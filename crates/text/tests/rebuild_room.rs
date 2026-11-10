//! #1514: `Index::repair` and `Index::rebuild` commit while they work through the index. They
//! read what to visit at a snapshot first: `repair` the keys it deletes, `rebuild` the
//! documents it re-indexes. Held across those commits, the snapshot (and the scan's
//! iterator) pinned the memtables it saw, so a commit that had to wait for arena room waited
//! on room only its own snapshot kept, until the write-stall timeout. That's the shape #1513
//! fixed for the IVF index. Each now drops its snapshot before it commits: `repair` per
//! prefix, `rebuild` per page of documents.
//!
//! A small arena on one shard and a short stall timeout: a repair and a rebuild that each
//! rewrite the whole index finish without `Busy`, and the index verifies.

mod common;

use std::time::Duration;

use common::*;
use pigeonhole::{Options, Pigeonhole};
use pigeonhole_text::{Index, IndexOptions};

const TIMEOUT: Duration = Duration::from_secs(4);

#[test]
fn repair_and_rebuild_that_wait_for_arena_room_never_wait_out_the_stall_timeout() {
    for seed in seeds(&[1514, 1515]) {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = Pigeonhole::open(
            dir.join("rebuild-room.phdb"),
            Options::default()
                .shards(1)
                .memtable_budget(2 << 20)
                .write_stall_timeout(TIMEOUT),
        )
        .expect("open");
        let t = docs(&db);
        let index = Index::create(&db, "docs_text", source(&t), IndexOptions::new()).unwrap();
        let mut rng = Rng::new(seed);
        // About 600 documents of up to 300 words: postings and forward rows several times
        // the 2 MiB arena, so the repair's deletes and re-index fill it more than once, past
        // a page of 256 documents.
        let n = 600u32;
        for i in 0..n {
            index
                .put(&i.to_be_bytes(), &text(&mut rng, 300))
                .unwrap_or_else(|e| panic!("seed {seed}: put {i}: {e}"));
        }
        // A commit that waited out the stall timeout fails with `Busy`: the regression shows
        // as this error, never as a timing check, which a loaded runner could trip with no
        // stall at all (perf287 on #1524).
        let repaired = index
            .repair()
            .unwrap_or_else(|e| panic!("seed {seed}: repair: {e}"));
        assert_eq!(repaired.documents, u64::from(n), "seed {seed}");
        // New text written around the index, so the rebuild re-indexes every document.
        for i in 0..n {
            t.mutate(&i.to_be_bytes())
                .put("body", b"text", text(&mut rng, 300).as_bytes())
                .commit()
                .unwrap();
        }
        let rebuilt = index
            .rebuild()
            .unwrap_or_else(|e| panic!("seed {seed}: rebuild: {e}"));
        assert_eq!(rebuilt.documents, u64::from(n), "seed {seed}");
        assert!(
            rebuilt.changed > 0,
            "seed {seed}: the rebuild re-indexed nothing"
        );
        let v = index.verify().unwrap();
        assert!(v.is_consistent(), "seed {seed}: {v:?}");
        drop(index);
        drop(t);
        db.close().unwrap();
    }
}

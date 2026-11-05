//! #1497: a split, and its phase 2, run on the writing thread inside the insert that made
//! the list too long (`Rebalance::Inline`), and commit transactions of their own. They read
//! the list's members at a snapshot first. Held across those commits, that snapshot pinned
//! the memtables it saw, so a commit that had to wait for arena room waited on room only its
//! own snapshot kept: nothing freed it until the write-stall timeout (30 s by default). On
//! the AX102 an IVF memory store after a large build stalled 30 s that way. The snapshot is
//! now dropped once the members are copied out.
//!
//! A small arena on one shard and a short stall timeout: inserts that keep splitting lists
//! while the arena fills never fail with `Busy`, and the index verifies.

mod common;

use std::time::Duration;

use pigeonhole::{Element, Metric, Options, Pigeonhole, VectorSpec};
use pigeonhole_vector::{Index, IndexOptions};

const TIMEOUT: Duration = Duration::from_secs(4);

#[test]
fn splits_that_wait_for_arena_room_never_wait_out_the_stall_timeout() {
    for seed in common::seeds(&[1497, 1498]) {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = Pigeonhole::open(
            dir.join("split-room.phdb"),
            Options::default()
                .shards(1)
                .memtable_budget(2 << 20)
                .write_stall_timeout(TIMEOUT),
        )
        .expect("open");
        let dim = 256usize;
        let spec = VectorSpec::new(dim as u32, Element::F32, Metric::L2).unwrap();
        // Lists of up to 2 x 512 vectors of 1 KiB: a split moves about 512 KiB, a quarter
        // of the arena, in a commit made while its 2-means snapshot pins what it read.
        let index =
            Index::create(&db, "v", spec, IndexOptions::new().target_list_len(512)).unwrap();
        let mut rng = common::Rng::new(seed);
        for i in 0..1_600u32 {
            let v: Vec<f32> = (0..dim).map(|_| rng.unit()).collect();
            index
                .insert_f32(&i.to_be_bytes(), &v)
                .unwrap_or_else(|e| panic!("seed {seed}: insert {i}: {e}"));
        }
        let stats = index
            .verify()
            .unwrap_or_else(|e| panic!("seed {seed}: verify: {e}"));
        assert_eq!(stats.vectors, 1_600, "seed {seed}");
        assert!(stats.list_lens.len() > 1, "seed {seed}: no list ever split");
        // No wall-clock bound: on main the first split's commit fails with `Busy` above, and
        // a bound would only flake on a loaded runner (simp724 on #1513).
        drop(index);
        db.close().unwrap();
    }
}

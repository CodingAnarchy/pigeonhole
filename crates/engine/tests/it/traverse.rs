//! D304: a traversal fanned out over tablets (scoped threads, every hop past the work floor)
//! equals the sequential one, hop by hop and parent by parent, under one snapshot.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, FamilyOptions, KeySet, TraverseSpec, ValueRef, WriteBatch};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/traverse.phdb";
const NODES: u32 = 400;

fn node(i: u32) -> Vec<u8> {
    format!("n{i:04}").into_bytes()
}

#[test]
fn a_fanned_out_traversal_equals_the_sequential_one() {
    let vfs = SimVfs::new(304);
    let db = Engine::open(
        Path::new(DB),
        common::options(Arc::clone(&vfs), 2, 64 << 20),
    )
    .unwrap();
    let t = db
        .create_table("g", &[("out".into(), FamilyOptions::default())])
        .unwrap();
    let out = t.families[0].id;
    // Each node points at a few others, with hubs at multiples of 50.
    let mut x: u64 = 0x5EED_0304;
    for i in 0..NODES {
        let mut wb = WriteBatch::new();
        let degree = if i % 50 == 0 { 120 } else { 1 + i % 5 };
        for _ in 0..degree {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let d = (x % u64::from(NODES)) as u32;
            wb.put(t.id, out, &node(i), &node(d), None, ValueRef::Bytes(b""))
                .unwrap();
        }
        db.commit(wb, None).unwrap();
        if i == NODES / 2 {
            db.flush().unwrap();
        }
    }
    // Four tablets over both shards.
    db.split_table(t.id, (1..4).map(|i| node(i * 100)).collect(), &[0, 1, 0, 1])
        .unwrap();
    let snap = db.snapshot().unwrap();
    let seeds = KeySet::from_unsorted((0..NODES).step_by(9).map(node));
    for (per_node, hop_limit) in [(0, 0), (4, 0), (0, 30), (2, 7)] {
        let mut spec = TraverseSpec::new(out);
        spec.hops = 4;
        spec.per_node_limit = per_node;
        spec.hop_limit = hop_limit;
        let seq = db.traverse(&snap, t.id, &seeds, &spec).unwrap();
        spec.parallelism = 4;
        spec.parallel_floor = 0;
        let par = db.traverse(&snap, t.id, &seeds, &spec).unwrap();
        assert!(
            !seq.hops.is_empty(),
            "per_node {per_node}, hop_limit {hop_limit}"
        );
        assert!(par.stats.threads > 1, "fanned out: {:?}", par.stats);
        assert_eq!(
            par.hops, seq.hops,
            "per_node {per_node}, hop_limit {hop_limit}"
        );
    }
    db.close().unwrap();
}

//! D304: a traversal fanned out over tablets (scoped threads, every hop past the work floor)
//! equals the sequential one, hop by hop and parent by parent, under one snapshot. D308: a
//! byte budget keeps each hop's in-budget prefix in first-edge order, and the traversal ends
//! with the hop it cuts, sequential, fanned out and async alike.

use crate::common;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    BUDGET_ITEM_OVERHEAD, Engine, FamilyOptions, KeySet, Snapshot, TraverseSpec, ValueRef,
    WriteBatch,
};
use pigeonhole_format::{FamilyId, TableId};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/traverse.phdb";
const NODES: u32 = 400;

fn node(i: u32) -> Vec<u8> {
    format!("n{i:04}").into_bytes()
}

/// Each node's out-edges, in memory.
type Adjacency = BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>>;

/// The graph, and its edges in memory: each node points at a few others, with hubs at
/// multiples of 50, over four tablets on two shards.
fn graph() -> (Arc<Engine>, TableId, FamilyId, Adjacency) {
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
    let mut adj = Adjacency::new();
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
            adj.entry(node(i)).or_default().insert(node(d));
        }
        db.commit(wb, None).unwrap();
        if i == NODES / 2 {
            db.flush().unwrap();
        }
    }
    // Four tablets over both shards.
    db.split_table(t.id, (1..4).map(|i| node(i * 100)).collect(), &[0, 1, 0, 1])
        .unwrap();
    (db, t.id, out, adj)
}

#[test]
fn a_fanned_out_traversal_equals_the_sequential_one() {
    let (db, table, out, _) = graph();
    let snap = db.snapshot().unwrap();
    let seeds = Arc::new(KeySet::from_unsorted((0..NODES).step_by(9).map(node)));
    for (per_node, hop_limit) in [(0, 0), (4, 0), (0, 30), (2, 7)] {
        let mut spec = TraverseSpec::new(out);
        spec.hops = 4;
        spec.per_node_limit = per_node;
        spec.hop_limit = hop_limit;
        let seq = db
            .traverse(&snap, table, Arc::clone(&seeds), &spec)
            .unwrap();
        spec.parallelism = 4;
        spec.parallel_floor = 0;
        let par = db
            .traverse(&snap, table, Arc::clone(&seeds), &spec)
            .unwrap();
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

fn block_on<F: Future + Unpin>(mut f: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = Pin::new(&mut f).poll(&mut cx) {
            return v;
        }
        std::thread::yield_now();
    }
}

/// D308 by hand: a breadth-first search in memory, each hop's new nodes ranked by first edge
/// (source in the frontier's order, then destination), cut to the budget left; the hop the
/// budget cuts is the last. Each hop's nodes sorted, as the engine returns them.
fn by_hand(
    adj: &Adjacency,
    seeds: &KeySet,
    hops: u32,
    budget: u64,
    extra: u64,
) -> (Vec<Vec<Vec<u8>>>, bool) {
    let mut visited: HashSet<Vec<u8>> = seeds.iter().map(<[u8]>::to_vec).collect();
    let mut frontier: Vec<Vec<u8>> = seeds.iter().map(<[u8]>::to_vec).collect();
    let mut left = budget;
    let mut out = Vec::new();
    for _ in 0..hops {
        let mut seen = HashSet::new();
        let ranking: Vec<Vec<u8>> = frontier
            .iter()
            .flat_map(|src| adj.get(src).into_iter().flatten())
            .filter(|d| !visited.contains(*d) && seen.insert((*d).clone()))
            .cloned()
            .collect();
        let mut kept = Vec::new();
        let mut cut = false;
        for d in ranking {
            let size = d.len() as u64 + BUDGET_ITEM_OVERHEAD + extra;
            if size > left {
                cut = true;
                break;
            }
            left -= size;
            kept.push(d);
        }
        kept.sort();
        if cut {
            if !kept.is_empty() {
                out.push(kept);
            }
            return (out, true);
        }
        if kept.is_empty() {
            break;
        }
        visited.extend(kept.iter().cloned());
        frontier = kept.clone();
        out.push(kept);
    }
    (out, false)
}

fn nodes_of(t: &pigeonhole_engine::Traversal) -> Vec<Vec<Vec<u8>>> {
    t.hops
        .iter()
        .map(|h| h.nodes.iter().map(<[u8]>::to_vec).collect())
        .collect()
}

#[test]
fn a_byte_budget_keeps_each_hops_first_edge_prefix_and_ends_with_the_hop_it_cuts() {
    let (db, table, out, adj) = graph();
    let snap: Snapshot = db.snapshot().unwrap();
    let seeds = Arc::new(KeySet::from_unsorted((0..NODES).step_by(37).map(node)));
    // The unbudgeted traversal agrees with the brute force.
    let mut spec = TraverseSpec::new(out);
    spec.hops = 3;
    let full = db
        .traverse(&snap, table, Arc::clone(&seeds), &spec)
        .unwrap();
    assert_eq!(nodes_of(&full), by_hand(&adj, &seeds, 3, u64::MAX, 0).0);
    let node_size = node(0).len() as u64 + BUDGET_ITEM_OVERHEAD;
    let mut cut_later_hop = false;
    let first_hop = full.hops[0].nodes.len() as u64;
    for (budget, parents) in [
        (1, false),
        (node_size, false),
        (node_size * 3 + 5, false),
        (node_size * first_hop, false),
        (node_size * (first_hop + 7), true),
        (node_size * (first_hop + 40), false),
        (u64::MAX / 2, false),
    ] {
        let extra = if parents { 4 } else { 0 };
        let (want, want_cut) = by_hand(&adj, &seeds, 3, budget, extra);
        if want_cut && want.len() >= 2 {
            cut_later_hop = true;
        }
        let mut spec = TraverseSpec::new(out);
        spec.hops = 3;
        spec.byte_budget = budget;
        spec.parents_in_budget = parents;
        let seq = db
            .traverse(&snap, table, Arc::clone(&seeds), &spec)
            .unwrap();
        assert_eq!(nodes_of(&seq), want, "budget {budget}");
        assert_eq!(seq.stats.cut_by_budget, want_cut, "budget {budget}");
        let async_run = block_on(
            db.traverse_async(&snap, table, Arc::clone(&seeds), spec.clone())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            async_run.hops, seq.hops,
            "budget {budget}: async equals sync"
        );
        assert_eq!(async_run.stats.cut_by_budget, want_cut, "budget {budget}");
        spec.parallelism = 4;
        spec.parallel_floor = 0;
        let par = db
            .traverse(&snap, table, Arc::clone(&seeds), &spec)
            .unwrap();
        assert_eq!(
            par.hops, seq.hops,
            "budget {budget}: fan-out equals sequential"
        );
        assert_eq!(par.stats.cut_by_budget, want_cut, "budget {budget}");
    }
    assert!(cut_later_hop, "some budget cut a hop after the first");
    // With hop_limit too, both prefixes of the same ranking: the shorter wins.
    let mut spec = TraverseSpec::new(out);
    spec.hops = 1;
    spec.hop_limit = 3;
    spec.byte_budget = node_size * 5;
    let both = db
        .traverse(&snap, table, Arc::clone(&seeds), &spec)
        .unwrap();
    assert_eq!(both.hops[0].nodes.len(), 3);
    assert!(!both.stats.cut_by_budget);
    db.close().unwrap();
}

//! D304 (ICR 0099): `Table::traverse` returns exactly what a client-side BFS over
//! `Table::keys` returns under the same snapshot, with D304 §2.1's semantics written out
//! independently below: each hop reads the frontier's rows in key order and each row's edges
//! in qualifier order (the newest version, the selection applied), takes the first
//! `per_node_limit` of each row, and keeps each destination not visited before once, ranked
//! by its first edge, at most `hop_limit` of them and `max_nodes` in all. The graph has hubs,
//! self-loops, cycles, deleted edges, rows and families, several SSTs and levels, and tablets
//! on two shards.

use pigeonhole::{ErrorCode, Family, KeySet, Options, Pigeonhole, Snapshot, Table, ValueFilter};

/// A deterministic xorshift; the seed is printed on failure.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

const NODES: u32 = 600;
const SEED: u64 = 0xD304_0001;

fn node(i: u32) -> Vec<u8> {
    format!("n{i:04}").into_bytes()
}

/// Per hop: the nodes, sorted, with their parents; and whether a limit cut the hop.
type Hops = Vec<(Vec<Vec<u8>>, Vec<u32>, bool)>;

#[derive(Clone, Copy, Debug)]
struct Limits {
    hops: u32,
    per_node: u32,
    per_hop: u64,
    max_nodes: u64,
}

/// The client BFS (the oracle).
fn client_bfs(
    t: &Table,
    s: &Snapshot,
    seeds: &KeySet,
    l: Limits,
    select: &dyn Fn(pigeonhole::KeysRead<'_>) -> pigeonhole::KeysRead<'_>,
) -> Hops {
    let mut visited: std::collections::HashSet<Vec<u8>> =
        seeds.iter().map(<[u8]>::to_vec).collect();
    let mut frontier = seeds.clone();
    let mut out = Vec::new();
    for _ in 0..l.hops {
        let room = l.max_nodes.saturating_sub(visited.len() as u64);
        if room == 0 {
            break;
        }
        let limit = if l.per_hop == 0 {
            room
        } else {
            l.per_hop.min(room)
        };
        let mut kept: Vec<(Vec<u8>, u32)> = Vec::new();
        let mut full = false;
        let mut rows = select(t.keys(&frontier).snapshot(s)).read().expect("keys");
        'rows: while let Some((i, row)) = rows.next().expect("next") {
            for (n, e) in row.iter().enumerate() {
                if l.per_node != 0 && n as u32 >= l.per_node {
                    break;
                }
                let d = e.qualifier.to_vec();
                if visited.contains(&d) || kept.iter().any(|(k, _)| *k == d) {
                    continue;
                }
                if kept.len() as u64 == limit {
                    full = true;
                    break 'rows;
                }
                kept.push((d, i as u32));
            }
        }
        if kept.is_empty() {
            break;
        }
        kept.sort();
        visited.extend(kept.iter().map(|(k, _)| k.clone()));
        frontier = KeySet::from_unsorted(kept.iter().map(|(k, _)| k));
        out.push((
            kept.iter().map(|(k, _)| k.clone()).collect(),
            kept.iter().map(|(_, p)| *p).collect(),
            full,
        ));
    }
    out
}

fn engine_bfs(
    t: &Table,
    s: &Snapshot,
    seeds: &KeySet,
    l: Limits,
    parallel: usize,
    select: &dyn Fn(pigeonhole::Traverse<'_>) -> pigeonhole::Traverse<'_>,
) -> (Hops, pigeonhole::TraverseStats) {
    let r = select(
        t.traverse(seeds)
            .family("out")
            .snapshot(s)
            .hops(l.hops)
            .per_node_limit(l.per_node)
            .hop_limit(l.per_hop)
            .max_nodes(l.max_nodes)
            .parents(true)
            .parallel(parallel),
    )
    .run()
    .expect("traverse");
    let hops = r
        .hops()
        .iter()
        .map(|h| {
            (
                h.nodes().iter().map(<[u8]>::to_vec).collect(),
                h.parents().expect("asked for").to_vec(),
                false,
            )
        })
        .collect();
    (hops, r.stats())
}

/// The graph: power-law-ish out-degrees, three hubs of 400 edges, self-loops, values that
/// say the edge's kind, written in rounds with flushes, then deletes of every scope.
fn build(db: &Pigeonhole, t: &Table, rng: &mut Rng) {
    for round in 0..3u32 {
        for i in (round..NODES).step_by(3) {
            let degree = match i {
                7 | 300 | 555 => 400,
                _ => (1 + rng.below(6)) * (1 + rng.below(3)),
            };
            let mut m = t.mutate(&node(i));
            for _ in 0..degree {
                let d = rng.below(u64::from(NODES)) as u32;
                let kind: &[u8] = if rng.below(4) == 0 {
                    b"weak"
                } else {
                    b"strong"
                };
                m = m.put("out", &node(d), kind);
            }
            if i % 50 == 0 {
                m = m.put("out", &node(i), b"self");
            }
            m.commit().expect("commit");
        }
        db.flush().expect("flush");
    }
    for i in (0..NODES).step_by(37) {
        t.mutate(&node(i))
            .delete_row()
            .commit()
            .expect("delete row");
    }
    for i in (5..NODES).step_by(41) {
        t.mutate(&node(i))
            .delete_family("out")
            .commit()
            .expect("delete family");
    }
    for i in (3..NODES).step_by(13) {
        t.mutate(&node(i))
            .delete_column("out", &node((i * 7) % NODES))
            .commit()
            .expect("delete column");
    }
}

fn open() -> (pigeonhole::doc_support::TempDir, Pigeonhole, Table) {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("g.phdb"), Options::default().shards(2)).expect("open");
    let t = db
        .table("g")
        .expect("table")
        .family("out", Family::default())
        .family("props", Family::default())
        .create_if_missing()
        .expect("create");
    (dir, db, t)
}

fn hops_only(h: &Hops) -> Vec<(Vec<Vec<u8>>, Vec<u32>)> {
    h.iter().map(|(n, p, _)| (n.clone(), p.clone())).collect()
}

#[test]
fn a_traversal_returns_what_a_client_bfs_returns() {
    let (_dir, db, t) = open();
    let mut rng = Rng(SEED);
    build(&db, &t, &mut rng);
    // Tablets on both shards and deeper levels, then writes after the snapshot.
    t.split_over_shards((1..6).map(|i| node(i * 100)).collect())
        .expect("split");
    db.compact().expect("compact");
    t.mutate(&node(1))
        .put("out", &node(2), b"m")
        .commit()
        .expect("commit");
    let s = db.snapshot().expect("snapshot");
    t.mutate(&node(2))
        .put("out", &node(599), b"late")
        .commit()
        .expect("commit");
    let seed_sets = [
        KeySet::from_unsorted([node(1)]),
        KeySet::from_unsorted([node(7)]), // a hub
        KeySet::from_unsorted((0..NODES).step_by(97).map(node).chain([b"absent".to_vec()])),
    ];
    let limits = [
        Limits {
            hops: 3,
            per_node: 0,
            per_hop: 0,
            max_nodes: u64::MAX,
        },
        Limits {
            hops: 4,
            per_node: 3,
            per_hop: 0,
            max_nodes: u64::MAX,
        },
        Limits {
            hops: 4,
            per_node: 0,
            per_hop: 10,
            max_nodes: u64::MAX,
        },
        Limits {
            hops: 5,
            per_node: 5,
            per_hop: 25,
            max_nodes: 120,
        },
        Limits {
            hops: 2,
            per_node: 1,
            per_hop: 1,
            max_nodes: 3,
        },
    ];
    for (si, seeds) in seed_sets.iter().enumerate() {
        for l in limits {
            let want = client_bfs(&t, &s, seeds, l, &|r| r.family("out"));
            for parallel in [1, 4] {
                let (got, stats) = engine_bfs(&t, &s, seeds, l, parallel, &|r| r);
                assert_eq!(
                    hops_only(&got),
                    hops_only(&want),
                    "seed {SEED:#x}, seeds {si}, {l:?}, parallel {parallel}"
                );
                let cut = want.iter().filter(|h| h.2).count() as u32;
                assert_eq!(stats.hops_cut, cut, "seeds {si}, {l:?}");
            }
        }
    }
    db.close().expect("close");
}

#[test]
fn the_selection_applies_to_edges() {
    let (_dir, db, t) = open();
    let mut rng = Rng(SEED ^ 1);
    build(&db, &t, &mut rng);
    let s = db.snapshot().expect("snapshot");
    let seeds = KeySet::from_unsorted([node(4), node(300)]);
    let l = Limits {
        hops: 3,
        per_node: 4,
        per_hop: 0,
        max_nodes: u64::MAX,
    };
    // Only strong edges.
    let want = client_bfs(&t, &s, &seeds, l, &|r| {
        r.family("out")
            .value_filter(ValueFilter::Equals(b"strong".to_vec()))
    });
    let (got, _) = engine_bfs(&t, &s, &seeds, l, 1, &|r| {
        r.value_filter(ValueFilter::Equals(b"strong".to_vec()))
    });
    assert_eq!(hops_only(&got), hops_only(&want));
    // Only destinations n01xx.
    let want = client_bfs(&t, &s, &seeds, l, &|r| {
        r.family("out").qualifier_prefix(b"n01")
    });
    let (got, _) = engine_bfs(&t, &s, &seeds, l, 1, &|r| r.qualifier_prefix(b"n01"));
    assert_eq!(hops_only(&got), hops_only(&want));
    db.close().expect("close");
}

#[test]
fn limits_and_arguments() {
    let (_dir, db, t) = open();
    let mut rng = Rng(SEED ^ 2);
    build(&db, &t, &mut rng);
    // Empty seeds: no hops.
    let r = t
        .traverse(&KeySet::default())
        .family("out")
        .run()
        .expect("empty");
    assert!(r.hops().is_empty());
    // max_nodes counts the seeds: room for 2 more.
    let r = t
        .traverse(&KeySet::from_unsorted([node(7)]))
        .family("out")
        .hops(3)
        .max_nodes(3)
        .run()
        .expect("traverse");
    assert_eq!(r.hops().iter().map(|h| h.nodes().len()).sum::<usize>(), 2);
    assert!(r.stats().cut_by_max_nodes);
    // Parents are off unless asked for.
    assert!(r.hops()[0].parents().is_none());
    let invalid = |e: pigeonhole::Error| assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
    let seeds = KeySet::from_unsorted([node(1)]);
    invalid(t.traverse(&seeds).run().unwrap_err());
    invalid(
        t.traverse(&seeds)
            .family("out")
            .family("props")
            .run()
            .unwrap_err(),
    );
    invalid(t.traverse(&seeds).family("out").hops(0).run().unwrap_err());
    invalid(t.traverse(&seeds).family("out").hops(65).run().unwrap_err());
    let e = t.traverse(&seeds).family("nope").run().unwrap_err();
    assert_eq!(e.code(), ErrorCode::FamilyNotFound, "{e}");
    db.close().expect("close");
}

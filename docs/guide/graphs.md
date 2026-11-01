# Graphs

> **Status:** this guide describes `main`; the current release on crates.io is 1.0.0, and [`changelog.d/`](../../changelog.d/README.md) lists what `main` adds after it. Wide-row walks (D297), multi-get (D299) and traversal (D304) are new in 1.1.0. Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup).

Pigeonhole stores a graph as adjacency rows: one row per node, one column per edge. Four reads cover the usual access patterns:
- a node's neighbours: one row read;
- a hub's neighbours: a row walked cell by cell or in pages;
- a frontier's neighbours: one multi-get;
- several hops of a breadth-first search: one traversal that runs inside the engine.

The sections below cover modelling, those reads, and the patterns that replace features Pigeonhole doesn't build in.

## Modelling: adjacency rows
- **A node is a row.** Its key is the node id. Fixed-width big-endian ids (`n:` and a `u64` as 8 big-endian bytes, say) sort numerically and keep a frontier's keys close together, which multi-get and traversal read faster.
- **An edge is a column of an edge family.** The qualifier is the destination's row key, so a node's out-edges are its columns in that family, in destination order. Use one family per edge type (`out`, `in`, `follows`, ...).
- **Edge properties go in the value** (a weight, a label, a timestamp), or in another family keyed the same way. A traversal can filter on the value (`value_filter`), and on the qualifier, by prefix or range.
- **Node properties go in another family** of the same row, or in another table keyed by node id.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let g = db
    .table("graph")?
    .family("out", Family::default().bloom_bits(10).max_versions(1))
    .family("in", Family::default().bloom_bits(10).max_versions(1))
    .family("deg", Family::counter())
    .family("props", Family::default())
    .create_if_missing()?;

// Edge a -> b with its reverse edge, in one atomic batch (the rows may be on different
// shards).
let mut wb = db.write_batch();
wb.put(&g, b"n:a", "out", b"n:b", b"w=3")
  .put(&g, b"n:b", "in", b"n:a", b"w=3");
wb.commit()?;

// a's neighbours: one row read of one family.
let row = g.row(b"n:a").family("out").read()?.expect("a has edges");
let neighbours: Vec<&[u8]> = row.iter().map(|e| e.qualifier).collect();
assert_eq!(neighbours, [&b"n:b"[..]]);
# Ok::<(), pigeonhole::Error>(())
```

## Patterns instead of built-ins
These were considered for 1.1 and left to the application (#489). Each is a few lines:
- **Reverse edges.** Write the `in` edge in the same `WriteBatch` as the `out` edge, as above. The batch is atomic across shards, so a reader never sees one direction without the other. Delete both the same way.
- **Degree.** Keep it in a counter family, incremented in the same commit as the edge. To count an edge only when it's new, make the commit conditional on the edge being absent: re-putting an existing edge then changes nothing.
- **Compact edge storage.** Use short, fixed-width node ids. Qualifiers are stored with prefix compression inside a block, so ids that share a prefix (`n:` and the high bytes of a `u64`) cost little each.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let g = db.table("graph")?.family("out", Family::default().max_versions(1))
#     .family("deg", Family::counter()).create_if_missing()?;
// Add a -> c and count it in a's out-degree, unless the edge is already there.
let new_edge = Condition::Absent { family: "out".into(), qualifier: b"n:c".to_vec() };
for _ in 0..2 {
    g.mutate(b"n:a")
        .put("out", b"n:c", b"")
        .incr("deg", b"out", 1)
        .commit_if(&new_edge)?;
}
let degree = g.get(b"n:a", "deg", b"out")?.and_then(|c| c.as_i64());
assert_eq!(degree, Some(1)); // the second commit's condition failed: nothing changed
# Ok::<(), pigeonhole::Error>(())
```

## Hubs: walking a wide row
A super-node with millions of edges is one wide row. `read()` builds the whole row in memory, about 410 bytes per edge, so a 2M-edge hub costs 824 MB and about 280 ms. Walk it instead:
- **`cells()`** lends one edge at a time, allocating nothing per edge.
- **`pages(n)`** lends pages of at most `n` edges from one reused buffer. A page always ends on a column boundary, so all of a column's versions are on one page.
- **A page's `position()`** is a resume token. `resume_after(&position)` continues the walk in a later read, and `to_bytes`/`from_bytes` make the token storable: hand it to a client, and pick up the next page from it.
- **`column_limit(n)`** reads only the first `n` edges, to sample a hub.

As a rule of thumb, walk any row that may hold more than about 10,000 edges, and read smaller rows whole.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let g = db.table("graph")?.family("out", Family::default()).create_if_missing()?;
let mut m = g.mutate(b"n:hub");
for i in 0u64..2_500 {
    m = m.put("out", &i.to_be_bytes(), b"");
}
m.commit()?;

// The whole hub, one edge at a time.
let mut degree = 0;
let mut cells = g.row(b"n:hub").family("out").cells()?;
while cells.next()?.is_some() {
    degree += 1;
}
assert_eq!(degree, 2_500);

// One page of 1,000 now, and the next one later from its token.
let mut pages = g.row(b"n:hub").family("out").pages(1_000)?;
let first = pages.next()?.expect("a page").len();
let token = pages.position().expect("a page was read").to_bytes();
drop(pages);
let resume = RowPosition::from_bytes(&token)?;
let mut pages = g.row(b"n:hub").family("out").resume_after(&resume).pages(1_000)?;
let second = pages.next()?.expect("a page").len();
assert_eq!((first, second), (1_000, 1_000));
# Ok::<(), pigeonhole::Error>(())
```

## Frontiers: many nodes at once
`Table::keys(&KeySet)` reads the rows of many keys in key order, under one snapshot. Each tablet's read is set up once for all of its keys, and moves forward from key to key, so a frontier of sorted ids costs well under a row read per key. `KeySet::from_unsorted` sorts and deduplicates. Rows are lent one at a time, with each key's index in the set, and keys with no cell the selection keeps are skipped. `next_async` and `into_owned_stream` are the async forms ([Async](async.md)).

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let g = db.table("graph")?.family("out", Family::default()).create_if_missing()?;
# g.mutate(b"n:a").put("out", b"n:b", b"").put("out", b"n:c", b"").commit()?;
# g.mutate(b"n:c").put("out", b"n:d", b"").commit()?;
let frontier = KeySet::from_unsorted([&b"n:c"[..], b"n:a", b"n:zz"]);
let mut next = Vec::new();
let mut rows = g.keys(&frontier).family("out").read()?;
while let Some((i, row)) = rows.next()? {
    assert_ne!(frontier.get(i), b"n:zz"); // no row: skipped
    next.extend(row.iter().map(|e| e.qualifier.to_vec()));
}
assert_eq!(next.len(), 3);
# Ok::<(), pigeonhole::Error>(())
```

## Traversal: hops inside the engine
`Table::traverse(&seeds)` runs a breadth-first search over one edge family, hop by hop, inside the engine and under one snapshot. Each hop keeps every node not visited before, once. No row is materialised, duplicate and already-visited nodes never leave the engine, and a capped hub costs only its first edges, read before a seek past the rest.

| Method | Effect |
|---|---|
| `family("out")` | The edge family (required; exactly one). |
| `hops(k)` | Hops to take, 1 to 64 (default 1). |
| `per_node_limit(m)` | At most the first `m` edges of each node per hop, in qualifier order; the rest of the row is skipped with a seek. Edges to visited nodes count; edges the selection drops (a value filter's misses, say) don't. |
| `hop_limit(n)` | At most `n` new nodes per hop: the ones whose first edge comes first, by source row and then qualifier. Deterministic, not a sample. |
| `max_nodes(t)` | At most `t` nodes visited in all, the seeds included (default 10,000,000). |
| `parents(true)` | Each node's parent: the index, in the previous hop's nodes (or the seeds, for hop 1), of the source of the first edge that reached it. |
| `byte_budget(bytes)` | At most `bytes` of nodes in all, over the hops in order (D308): a node counts its key and 16 bytes, plus 4 with `parents`; the seeds don't count. The hop the budget cuts keeps the in-budget prefix of its ranking (the nodes `hop_limit` would keep first), and the traversal ends with it (`stats().cut_by_budget`). |
| `parallel(n)` | Read each hop on up to `n` threads when its frontier spans several tablets with at least 1,000 keys per thread. |
| `qualifier_prefix`, `qualifier_range`, `qualifier_bounds`, `time_range`, `value_filter`, `snapshot` | The edges' selection, as for a row read. |

`run()` returns the hops in order. Each hop's `nodes()` is a sorted `KeySet`, ready for `keys()` to read the nodes' properties. A traversal ends early when a hop finds no new node, or when `max_nodes` is reached (`stats().cut_by_max_nodes`).

The visited set is held in memory, about the key length plus 48 bytes per node, which is what `max_nodes` bounds.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let g = db.table("graph")?.family("out", Family::default()).create_if_missing()?;
for (a, b) in [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("d", "e"), ("d", "a")] {
    g.mutate(format!("n:{a}").as_bytes())
        .put("out", format!("n:{b}").as_bytes(), b"")
        .commit()?;
}
let seeds = KeySet::from_unsorted([b"n:a"]);
let t = g.traverse(&seeds).family("out").hops(3).parents(true).run()?;
let hops: Vec<Vec<&[u8]>> = t.hops().iter().map(|h| h.nodes().iter().collect()).collect();
assert_eq!(hops, [vec![&b"n:b"[..], b"n:c"], vec![&b"n:d"[..]], vec![&b"n:e"[..]]]);

// One shortest path to e: follow the parents back to the seed.
let mut path = vec![b"n:e".to_vec()];
let mut at = 0; // e's index in the last hop
for h in (0..t.hops().len()).rev() {
    let parent = t.hops()[h].parents().expect("asked for")[at] as usize;
    let prev = if h == 0 { &seeds } else { t.hops()[h - 1].nodes() };
    path.push(prev.get(parent).to_vec());
    at = parent;
}
path.reverse();
assert_eq!(path, [b"n:a".to_vec(), b"n:b".to_vec(), b"n:d".to_vec(), b"n:e".to_vec()]);
# Ok::<(), pigeonhole::Error>(())
```

**`run_async()`** is the async form. Each hop's rows are positioned from the block cache, and the blocks they and the next rows need are fetched asynchronously. A row's edges past its first block are read synchronously if they aren't cached, so cap hubs with `per_node_limit`. A poll also yields after 4,096 edges, so a hub never holds an executor thread for long. It gives the same hops and parents as `run()`, reads each hop on the polling task, and never fans out.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let g = db.table("graph")?.family("out", Family::default()).create_if_missing()?;
# g.mutate(b"n:a").put("out", b"n:b", b"").commit()?;
let seeds = KeySet::from_unsorted([b"n:a"]);
let t = pigeonhole::doc_support::block_on(
    g.traverse(&seeds).family("out").per_node_limit(1_000).run_async(),
)?;
assert_eq!(t.hops().len(), 1);
# Ok::<(), pigeonhole::Error>(())
```

## Adjacency sets
> **TODO (#488 PR 5):** placeholder. This section documents the `Set` family once #488 PR 5 lands it; nothing named here exists yet.

A hub's edges can also be kept as one set-valued cell: the `Set` aggregate family of #488, a compressed bitmap of `u32` node ids. With it, membership is one lookup, degree is the set's cardinality, and common neighbours of two hubs are one intersection. Until it lands, adjacency rows (above) and a degree counter cover the same needs.

## Choosing a read
| Need | Read |
|---|---|
| One node's neighbours, small degree | `row(k).family("out").read()` |
| One node's neighbours, any degree | `row(k).family("out").cells()` or `.pages(n)` |
| Neighbours of many nodes | `keys(&set).family("out").read()` |
| Several hops, deduplicated, capped | `traverse(&seeds).family("out").hops(k)` |
| Is there an edge a -> b | `get(a, "out", b)` (bloom filters make misses cheap) |
| Degree | a counter family, kept with the edges |

# Vectors

> **Status:** this guide describes `main`; the current release on crates.io is 1.0.0, and [`changelog.d/`](../../changelog.d/README.md) lists what `main` adds after it. Vector families (D303), `nearest_*` (D296) and the `pigeonhole-vector` IVF index (D306) are new in 1.1.0. Code samples run as doctests of the `pigeonhole-vector` crate (lines starting with `#` are hidden setup).

Pigeonhole stores embeddings in **vector families** and finds the vectors nearest a query in two ways:
- **Exact search** (`nearest_f32` and its forms) scores every vector a read covers, inside the engine. It's right for a few thousand vectors per query, or a set already narrowed by a row range or a key set.
- **The IVF index** (`pigeonhole-vector`) clusters the vectors into lists and searches only the lists nearest the query. It's right for millions, at a recall you choose.

## Vector families
A vector family fixes the element type, the dimension and the metric. Every value in it is a vector of that shape, checked when the write is built.

```rust
# use pigeonhole::*;
# fn main() -> Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let spec = VectorSpec::new(4, Element::F32, Metric::Cosine)?;
let docs = db
    .table("docs")?
    .family("emb", Family::vector(spec).max_versions(1))
    .family("text", Family::default())
    .create_if_missing()?;
docs.mutate(b"doc:1")
    .put_vector_f32("emb", b"", &[0.1, 0.9, 0.0, 0.2])
    .put("text", b"", b"a short note")
    .commit()?;
# db.close()?;
# Ok(())
# }
```

- **Elements:** `F32`, `I8` (raw signed bytes; you quantise), and `Bit` (packed bits, Hamming only).
- **Metrics:** `L2` (squared Euclidean), `InnerProduct` (negated, so the largest product ranks first), `Cosine` (`1 − cosine similarity`), and `Hamming`. Smaller is always nearer.
- **What a write refuses:**
  - another element type or dimension;
  - an `f32` component that isn't finite, or exceeds 2^56 in magnitude;
  - under cosine, a vector whose norm is zero.

  These rules guarantee every distance is finite, so every stored vector can be found.
- **Reads:** a vector cell reads back as `Value::Vector`, or `CellRef::as_vector()`.
- **Scoring yourself:** `VectorSpec::distance_f32` and its forms give the distance the engine ranks by, bit for bit.

## Exact search
`nearest_f32(query, k)` (and `_i8`, `_bits`) on a scan or a multi-get returns the `k` nearest vectors of the read's one vector family, nearest first. Ties go by row, then qualifier.

```rust
# use pigeonhole::*;
# fn main() -> Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let spec = VectorSpec::new(2, Element::F32, Metric::L2)?;
# let t = db.table("pts")?.family("emb", Family::vector(spec)).create_if_missing()?;
# for (row, v) in [(b"a", [0.0, 0.0]), (b"b", [1.0, 0.0]), (b"c", [5.0, 5.0])] {
#     t.mutate(row).put_vector_f32("emb", b"", &v).commit()?;
# }
let near = t.scan_prefix(b"").family("emb").nearest_f32(&[0.9, 0.0], 2).run()?;
assert_eq!(near[0].row, b"b");
assert_eq!(near[1].row, b"a");
# db.close()?;
# Ok(())
# }
```

- **The read's selection applies:** a key range or prefix, a key set (`table.keys(&set)`), qualifiers, a time range, and a value filter.
- **`parallel(n)`** spreads a large read over `n` threads the call owns, and the result is the same.
- **`run_async()`** never blocks on the device, and yields within a wide row.
- **The cost is linear in the vectors read:** about 140 ns per 128-dimension `f32` vector on the reference machine, with the data in the block cache. So it's the right tool up to tens of thousands of vectors per query at interactive latency.

## The IVF index (`pigeonhole-vector`)
For a large collection, the index keeps the vectors in **lists**, each a wide row around a centroid. A search reads only the `nprobe` lists whose centroids are nearest the query. The index is ordinary rows and transactions in a table of its own, so it's as durable and consistent as the rest of the database.

```rust
# use pigeonhole::*;
# use pigeonhole_vector::{Index, IndexOptions};
# fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let spec = VectorSpec::new(2, Element::F32, Metric::L2)?;
let index = Index::create(&db, "points", spec, IndexOptions::new().target_list_len(16))?;
for i in 0..200u32 {
    let x = i as f32;
    index.insert_f32(&i.to_be_bytes(), &[x, x * 0.5])?;
}
let hits = index.search_f32(&[42.0, 21.0], 5).nprobe(4).run()?;
assert_eq!(hits[0].id, 42u32.to_be_bytes());
assert!(index.delete(&42u32.to_be_bytes())?);
# drop(index);
# db.close()?;
# Ok(())
# }
```

- **Ids are bytes,** at most a row key's length. An insert of an existing id replaces its vector (an upsert).
- **Every insert and delete is one transaction.** Lists split above twice `target_list_len` (default 1,000) and merge below a quarter of it, each move a transaction too. A search reads one snapshot, so a concurrent split never shows a vector twice or hides it.
- **`search_f32(q, k)`** returns `Hit { id, distance }`, nearest first. Options:
  - `nprobe(n)`: the lists probed. It overrides the index's own default, `IndexOptions::nprobe`: 24 for an index created now. Options are fixed when an index is created, so an older index keeps the default it was created with;
  - `snapshot(&s)`;
  - `parallel(n)`;
  - `run_async()`.

  With every list probed, a search is exact: it equals `nearest_*` over the index's vectors.
- **Ties:** hits at an equal distance come back by the list that holds them, then by id. A split can move a tied id to another list, so which of several ids tied at the k-th distance makes the cut can change between layouts.
- **`verify()`** checks the index's rows against each other. It's for tests and repair tools.

### Recall against latency
On SIFT1M (1M × 128-dimension `f32`, L2, 10,000 queries; the reference machine, one thread, one query at a time), built with `bulk_load`, default list length, the index held in the block cache:

| `nprobe` | recall@10 | p50 | p99 |
|--:|--:|--:|--:|
| 16 | 92.2% | 2.30 ms | 3.94 ms |
| 20 | 94.3% | 2.84 ms | 4.67 ms |
| **24 (default)** | **95.8%** | **3.38 ms** | **5.39 ms** |
| 28 | 96.7% | 3.92 ms | 6.08 ms |
| 32 | 97.4% | 4.47 ms | 6.75 ms |

- **Recall depends on the data,** not only on `nprobe`: how clustered your vectors are, and how your queries relate to them. Measure on a sample of your own corpus (exact search gives the ground truth) and pick `nprobe` from that. The default suits data like SIFT1M.
- **On SIFT1M, recall** follows `nprobe` and the clustering. It matched Faiss IVF-Flat's (1,000 lists) at every `nprobe` measured, from 4 to 128.
- **Latency** is about linear in `nprobe`: about `nprobe` × list length × the exact search's per-vector cost.
- **Faiss** answers the same queries 9–10× faster at equal recall with this index cached (0.50 ms at `nprobe` 32), and up to 22× faster when it isn't (the default 256 MiB cache). Most of the gap is the engine's per-cell read path, not the distance arithmetic; [#1283](https://github.com/CodingAnarchy/pigeonhole/issues/1283) works on it.
- **The `ivfsift` bench** (`crates/bench/examples/ivfsift.rs`) measures your own build.

### Sizing the block cache
A search reads `nprobe` whole lists, about `nprobe` × list length × the vector size: 12 MB per query at the defaults for 128-dimension `f32`. For the latencies above, the block cache must hold the index: about the number of vectors × the vector's bytes, plus a little.
- **SIFT1M is 512 MB,** so set `Options::block_cache(1 << 30)`.
- **Under the default 256 MiB cache,** half of each search's lists come from the page cache or the device. On the same machine the same searches took 2.4× as long (10.8 ms at `nprobe` 32).
- **Cold, after a reopen:** p99 at `nprobe` 24 reached 9.7 ms with direct I/O, while the lists loaded.

### Building it
- **Inserting** costs one transaction per vector. At the default durability (`GroupSync`) each one waits for its group's sync. To load a large set, open with `Durability::Buffered` or use a bulk load. On the reference machine, 1M inserts at `Buffered` took 67 s (insert p50 56 µs, p99 93 µs, splits included).
- **`bulk_load_f32(expected, vectors)`** loads an empty index in one pass:
  - it clusters the first 50,000 vectors with k-means into `expected / target_list_len` lists;
  - it writes each vector to its nearest list, a batch per transaction;
  - it then splits or merges any list left out of bounds.

  1M vectors took 138 s. The sample is the input's first vectors, so shuffle an input sorted by cluster. `expected` only chooses the starting number of lists: a wrong estimate costs time, not correctness.

```rust
# use pigeonhole::*;
# use pigeonhole_vector::{Index, IndexOptions};
# fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let spec = VectorSpec::new(2, Element::F32, Metric::L2)?;
let index = Index::create(&db, "points", spec, IndexOptions::new().target_list_len(16))?;
let vectors = (0..500u32).map(|i| (i.to_be_bytes(), [i as f32, (i % 7) as f32]));
let stats = index.bulk_load_f32(500, vectors)?;
assert_eq!(stats.vectors, 500);
# drop(index);
# db.close()?;
# Ok(())
# }
```

### Who rebalances
- **`Rebalance::Inline`** (the default): the insert or delete that takes a list out of bounds splits or merges it before returning. That's simple, and a split costs that one writer a few milliseconds.
- **`Rebalance::Background`:** writes only mark the list, and a `Rebalancer` splits and merges.
  - `Index::open` runs one on a thread of the crate's own.
  - An application that runs its own threads, such as an application-owned database, opens with `Index::open_with_rebalancer` and drives the `Rebalancer` itself: `run_once(budget)` until it returns `false`, then `wait`.
  - Until it runs, a list can be longer than its bound, which slows the searches that probe it, never their results.

### Things to know
- **Open an index once per process,** and clone the handle. Clones share the centroid cache and the rebalancer.
- **To reopen a database in the same process, `close()` it first.** A rebalancing thread can hold a handle for up to one split after the last `Index` drops.
- **The index's table holds its rows only.** Don't write to it directly.
- **Vector elements:** `f32`, `i8` and bit vectors. Half-precision elements ([#1263](https://github.com/CodingAnarchy/pigeonhole/issues/1263)) come to the index once vector families have them.

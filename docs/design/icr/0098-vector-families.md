# 0098: vector families, `Family::vector`, `put_vector_*`, `Value::Vector`

**Status:** Approved (number from the coordinator, 2026-10-31; owner accepted D303 at revision 2 on #490, c6544874035). D303 PR 1 (#1230) landed the format and is internal. D303 PR 2 adds the public API below. D296 PR 4 adds the typed `nearest_*`, which this ICR covers too.

## PR 2: families, writes and reads
- **pigeonhole:**
  - `VectorSpec` (`Copy + Eq + Hash`): `new(dim: u32, Element, Metric) -> Result<VectorSpec>` (`InvalidArgument` unless `dim` is 1 to 4096, a multiple of 8 for bit, and the metric takes the element), `dim`, `element`, `metric`, and `distance_f32(&[f32], VectorRef) -> Result<f64>`, `distance_i8(&[i8], VectorRef)`, `distance_bits(&[u8], VectorRef)`. These score as the engine ranks: the same kernels, exact for `i8` and bit vectors, and the `f32` kernels' result widened exactly. A query that doesn't fit the spec, or a stored vector of another spec, is `InvalidArgument`.
  - `Element` (`#[non_exhaustive]`): `F32`, `I8`, `Bit`.
  - `Metric` (`#[non_exhaustive]`): `L2`, `InnerProduct`, `Cosine`, `Hamming`.
  - `Family::vector(VectorSpec) -> Family`.
  - `FamilyInfo::vector: Option<VectorSpec>`.
  - `put_vector_f32`, `put_vector_i8`, `put_vector_bits`, each with an `_at(…, ts, …)` form, on `RowMutation` (`(family, qualifier, value)`), `WriteBatch` and `Transaction` (`(table, row, family, qualifier, value)`). Each is checked when the write is built: the family must be a vector family, the vector of its element type and dimension, with finite `f32` components of at most 2^56 in magnitude, and under cosine a nonzero norm (an `f32` squared norm of at least `f32::MIN_POSITIVE`), so every distance is finite (perf287's B1). Otherwise the batch fails with `InvalidArgument` naming the family's spec, as #1213 does for typed aggregate inputs.
  - `Value::Vector(VectorRef<'a>)` (`Value` is `#[non_exhaustive]`), and `CellRef::as_vector`/`Cell::as_vector -> Option<VectorRef<'_>>`.
  - `VectorRef<'a>` (`Copy + PartialEq + Debug`): `element`, `dim`, `as_bytes` (the raw payload), and `to_f32_vec -> Option<Vec<f32>>` (exact for `f32` and `i8`, `None` for bits). A vector cell's `value()` is the element byte and the dimension varint followed by `as_bytes()`, and a `ValueFilter` on a vector family compares those bytes (D303 N1).
- **Internal crates** (no promise):
  - **pigeonhole-format:** `vector::Query` (`F32`, `I8`, `Bits`), `Params::{check, payload, distance}`, `VectorRef::check_components`, and the `i8` kernels generic over `SignedByte` (`i8`, or a stored byte).
  - **pigeonhole-engine:** vector families can be created (`Unsupported` in PR 1). Any merge operator on one is `InvalidArgument`, and `Corruption` at catalog decode. The commit gate checks every vector put (its family, element, dimension and components) and refuses a vector anywhere but a vector family. `WriteBatch::merge` refuses a vector operand.

**Not done as D303 rev 0 worded it:** a plain put (`put`, `put_i64`, …) into a vector family is refused at commit by the engine's gate, not when the write is built. That's how a plain put into an aggregate family is refused, and it keeps the plain `put`'s hot path free of a kind check. The typed `put_vector_*` are checked when built, as rev 2 (N2) says.

## D296 PR 4: `nearest_*`
- **pigeonhole:** `Scan::nearest_f32(self, &[f32], k: usize) -> Nearest<'t>`, `nearest_i8(&[i8], k)` and `nearest_bits(&[u8], k)`, and the same three on `KeysRead`.
  - `Nearest` (`#[must_use]`): `parallel(n)` (default 1; on a key set an explicit `n > 1` fans out over its tablet groups whatever their count, #490 D's seam, c6548060686), and `run(self) -> Result<Vec<Neighbour>>`.
  - `Neighbour` (`#[non_exhaustive]`): `row`, `qualifier`, `distance: f64`, `ts`.
  - `Vector` (`Clone + PartialEq + Debug`), an owned vector for ones that aren't cells (#490 D's centroids and means): `from_f32(&[f32])`, `from_i8(&[i8])`, `from_bits(dim, &[u8])` (each `InvalidArgument` outside 1 to 4096 dimensions, bits a multiple of 8), `element`, `dim`, and `as_ref() -> VectorRef<'_>` for `VectorSpec::distance_*`. Its components aren't checked against a family; a write or a query is.
  - The result is the `k` nearest of the read's one vector family, in (distance, row, qualifier) order, the same at any `parallel`. The read's selection applies, at the newest version of each cell. Its distances equal `VectorSpec::distance_*`'s to the bit. A read naming no family or several, a non-vector family, or a query that doesn't fit the spec is `InvalidArgument`.
- **pigeonhole-engine** (no promise): `KnnVisitor::typed(Params, KnnQuery, Arc<KnnSkips>) -> Result<KnnVisitor>`, `KnnQuery` (`F32`, `I8`, `Bits`), and `Score` holding an `f64`.
- **Next:** `run_async` on `Nearest` (a key set's async cursor, as D304's `PendingTraversal`) follows in its own PR, and D308's `run_within` after that.

## Why
D303 (#490 B): embeddings stored as `Bytes` carry no shape. A typed family fixes element, dimension and metric, so every stored vector has a finite, ordered distance to every valid query (D296).

## Callers changed
None: additive.

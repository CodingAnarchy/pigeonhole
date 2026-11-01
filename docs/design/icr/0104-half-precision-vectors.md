# 0104: half-precision vector elements (`f16`, `bf16`)

**Status:** Approved (number from the coordinator, 2026-10-31; the owner approved both elements for 1.1.0 in #1263, amending D303). The API shape was agreed with perf287 on #1263 (c6551564500 and its five points).

## The change
- **pigeonhole:**
  - `Element::F16` and `Element::BF16` (the enum is `#[non_exhaustive]`). `VectorSpec::new(dim, Element::F16 | BF16, Metric::L2 | InnerProduct | Cosine)`; Hamming stays bit-only (`InvalidArgument`).
  - **Writes, as raw bits:** `put_vector_f16(family, qualifier, &[u16])` and `put_vector_bf16(…, &[u16])`, with `_at` forms, on `RowMutation`, `WriteBatch` and `Transaction` (`(table, row, family, qualifier, value)` on the last two). Each `u16` is an IEEE binary16 or bfloat16 bit pattern, stored and read back bit for bit (a `half::f16` user passes `to_bits()`). Refused when built for another element or dimension, a component that isn't finite (and for `bf16` one above 2^56 in magnitude), or under cosine a zero norm.
  - **Writes, from `f32`:** `put_vector_f32` into an `f16` or `bf16` family rounds each component to nearest, ties to even. A component that isn't finite or overflows the element is refused, and the rounded vector is checked as stored, so one that rounds to a zero vector under cosine is refused. `put_vector_i8`/`put_vector_bits` into a half family stay refused.
  - **Queries and distances stay `f32`:** `nearest_f32` and `VectorSpec::distance_f32` accept a half family. A half vector scores exactly as its widened `f32` vector. There is no `nearest_f16`.
  - **Reads:** `VectorRef::to_f32_vec()` widens `f16`/`bf16` exactly; `as_bytes()` is the raw little-endian `u16`s. `Vector::from_f16_bits(&[u16])` and `Vector::from_bf16_bits(&[u16])` make owned ones.
- **Python** (`pigeonhole-db`, ICR 0102): `Element.F16` and `Element.BF16` (`"f16"`, `"bf16"`), and `VectorSpec.element` reports them. `put_vector` into a half family takes what an `f32` family takes (a `float32` or `float64` buffer, or a sequence of numbers), written through `put_vector_f32`; `Cell.as_vector()` widens to `array.array('f')`; `Table.nearest` reads the query as `f32`. There's no raw-bits write in Python: NumPy's `float16` converts exactly through the `f32` path.
- **Internal crates** (no promise):
  - **pigeonhole-format:** `vector::Element::{F16 = 4, BF16 = 5}` and `Element::is_half`, `vector::Query::{F16, BF16}`, `half::{f16_to_f32, bf16_to_f32, f32_to_f16, f32_to_bf16}`, and `distance::{l2, dot_norm, norm2}_{f16, bf16}_le`. `half` is a dev-dependency (tests only).

## Why
#1263: half precision halves an embedding's bytes, on disk and in a KNN scan, with less loss than `i8` and no quantisation scale. `f16` is the common stored-embedding format, and `bf16` is what GPU-run models often emit. Both ship in 1.1.0 under its feature bit; after 1.1.0 they would need a new bit.

## Callers changed
None: additive.

# 0108: vector cells and specs in `pigeonhole-arrow` and `phdb` (D303 PR 3)

**Status:** Approved (number from the coordinator, 2026-11-02, for #1309). D303's PR plan named "3, the Arrow and `phdb` mappings"; this records them.

## The change
- **pigeonhole-arrow** (semver-checked):
  - **`value_type`** gains `vector` (dictionary key 4).
  - **`value`** carries a vector cell's stored encoding: its element byte, `dim` as a varint, then the payload (`CellRef::value`). Vector cells were refused with `NotYetImplemented` before; now no vector is refused.
  - **`ArrowOptions::vector_dim(dim: u32) -> Self`** adds the nullable column `value_vector: FixedSizeList<Float32, dim>` (item field `item`, non-null `Float32`), with `dim` clamped to 1–4096.
    - Each vector cell's components go there: `f32`, `f16` and `bf16` widened exactly, `i8` as exact floats.
    - Other cells, and bit vectors (no float form), are null.
    - A vector of another dimension fails its batch with `ArrowError::InvalidArgumentError` naming the row. `schema(&options)` is known before the scan, so one export has one `dim`: select families of one dimension.
- **phdb** (`dump`, `load`, `info`, `shell`, `mcp`):
  - **A family's options gain `"vector": {"dim", "element", "metric"}`** (null for other families). Elements are `f32`, `i8`, `bit`, `f16` or `bf16`; metrics are `l2`, `inner_product`, `cosine` or `hamming`. `load` and the MCP `create_table` read it back, so a dump recreates its vector families.
  - **A vector value is `{"vector": {"dim", "element", "values": [...]}}`.** Floats are the `f64`s they widen to exactly, `i8` components are integers, and a bit vector has `"base64"` in place of `values`. A dump and load round-trips every stored bit: a half is written widened, and `put_vector_f32` rounds it back to the same bits.
  - **`load`** checks each vector against its family's element and dim (and refuses a non-finite or out-of-range component, or a vector in a non-vector family), naming the line. The MCP `write` tool's `put` takes the same shape, with the same check.
  - **CSV:** `type` is `vector`, and the value is the JSONL form as text.
  - **MCP reads:** a vector whose payload is over `max_value_bytes` comes back as `{"omitted_vector": {"element", "dim"}}`.

## Why
#1309: before this, `phdb dump` wrote vector cells as `{"unknown_type": true}`, so a dump lost every vector, and an Arrow export of a vector family failed. 1.1.0 ships vector families, so both have to round-trip.

## Callers changed
None. It's additive; a dump written before this has no `vector` keys, and loads as before.

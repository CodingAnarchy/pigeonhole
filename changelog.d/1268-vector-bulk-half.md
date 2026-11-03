### Added
- `pigeonhole-vector`: `Index::bulk_load_f16`/`bulk_load_bf16` load an empty `f16` or `bf16` index from bit patterns, stored exactly (#1268, ICR 0100). An `f32` vector written into a half index is now placed by its rounded, stored value.

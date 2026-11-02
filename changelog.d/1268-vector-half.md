### Added
- `pigeonhole-vector`: `f16` and `bf16` IVF indexes (#1268, ICR 0100). `insert_f16`/`insert_bf16` store bit patterns exactly, and `search_f16`/`search_bf16` (on `Index` and `ReadIndex`) search as `f32`. A half index takes `insert_f32`/`search_f32` too, rounding written vectors to the element. Centroids stay `f32`.

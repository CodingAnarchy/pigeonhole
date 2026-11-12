### Added
- **`phdb serve`: the IVF vector index** (`Vectors.CreateIndex`, `Insert`, `Delete`, `Search`, `Verify`; D313, ICR 0100 and 0112, #1397), over gRPC and HTTP/JSON, in all five element types. A background-rebalancing index rebalances on a server thread, which shutdown waits for.

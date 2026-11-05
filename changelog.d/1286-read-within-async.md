### Added
- **`Scan::read_within_async` and `KeysRead::read_within_async`** (the `async` feature, #1286, ICR 0122): `read_within`'s byte-budgeted page (D308) as a future that never blocks on a block it can fetch. A poll yields after a bounded number of cells or value bytes. In `pigeonhole-engine`, `Engine::reduce_async` drives any visitor and reducer this way (`PendingReduce`).

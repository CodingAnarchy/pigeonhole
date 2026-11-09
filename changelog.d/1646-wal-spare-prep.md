### Changed
- A durable commit no longer waits behind a WAL spare segment's zero-fill: the zeros are written back a chunk at a time as they're prepared, and a stream leaves its first, never-written segment as soon as a spare is ready. At `GroupSync` this removes the 16–64 ms WAL syncs that remained under steady load (D319, #1621).

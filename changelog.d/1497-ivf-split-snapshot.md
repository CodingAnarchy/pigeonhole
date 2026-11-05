### Fixed
- An IVF vector index (`pigeonhole-vector`) could make an insert, or a memory store's `remember`, wait the whole write-stall timeout and fail with `Busy` under a write load that filled the memtable arena. A list split held the snapshot it read the list at while its own transactions committed, so the memory those commits waited for was held by the split itself. The split now drops it once the list's members are copied out (#1497).

### Changed
- The user guide's transactions page and `Pigeonhole::snapshot`'s docs say not to hold a snapshot, an iterator or a read value across a commit on the same thread (#1497).

### Changed
- A shard that receives a large flush report from another shard no longer stalls for tens of milliseconds counting it (D254). It counts at most 4096 entries per turn and finishes the rest in later turns.

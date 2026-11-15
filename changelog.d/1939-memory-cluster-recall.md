### Added
- `phdb serve --cluster`: agent memory calls across partitions (#1915, ICR 0139).
  - **`Memory.Recall`** over namespaces on several partitions runs in steps from the node you called. It returns what one store holding every namespace would, except for the keyword signal (each partition's BM25 statistics are its own), while nothing writes to those namespaces.
  - **`Memory.MarkUsed` and `Memory.Forget`** are split by partition, and their counts summed. An error names the partitions that applied, and a part that may have applied answers `OutcomeUnknown`.
  - `Memory.Watch` on a cluster node is still to come.
- `pigeonhole-memory`: a recall in steps, for recalls over several stores. `Recall::gather` gives a `RecallPool`, which `merge` and `rank` turn into a `RecallRanking`. `RecallRanking::walk` gives a graph expansion hop by hop (`RecallWalk`, `RecallHop`, `Recall::hop`). `Recall::bodies` and `RecallRanking::finish` produce the hits. Each step has versioned bytes. `Recall::run` runs the same steps in process.

### Changed
- `pigeonhole-memory`: each namespace in a recall doubles its own candidate count when filters leave a list short. Before, one count was shared by the whole recall. A namespace's IVF candidates now depend only on that namespace, not on the other namespaces the recall names. Exact lists, and recalls over one namespace, are unchanged.

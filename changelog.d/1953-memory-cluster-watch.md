### Added
- `phdb serve --cluster`: `Memory.Watch` across partitions (#1915, ICR 0139). A watch of namespaces on several partitions, or of every namespace, merges each partition's events on the node you called.
  - **Order:** each partition's commits arrive in order, and a commit's memories arrive together.
  - **Tokens:** every event's token resumes the whole watch through any node.
  - **Ending:** each partition delivers only under its lease, and a close or a move ends the watch with a token to resume from.

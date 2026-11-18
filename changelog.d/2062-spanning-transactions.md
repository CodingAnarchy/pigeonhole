### Added
- **A cluster transaction can read and write rows of several partitions** (D329, ICR 0140), when it opts in with `BeginRequest.spans_partitions`. Its home is the partition its `route` names. Each other partition it touches is a branch on that partition's owner, and they all commit atomically through a two-phase commit whose commit point is a decision row on the home partition.
  - A commit that writes runs at `GroupSync` or stronger. `TxnCommitResponse.parts` lists each partition's share, the home first, and marks any that its partition hasn't applied yet.
  - A read-only one writes nothing: each partition holds its reads until a release round confirms that every hold lasted, or answers `Conflict`.
  - Every node must keep the new `spanning_txns` map feature. `--txn-max-branches` (default 16) bounds the branches of one transaction.
  - Without the opt-in, a row of another partition is still `CrossPartition`.
  - Resolving branches left in doubt by a home that fails (the resolver, `Transactions.Outcome`, the forced decide) comes in the next release step, together with this one: until then such a branch stays in doubt.
- **The engine and `pigeonhole` (behind `__unstable-txn-prepare`):**
  - An external prepare with reads and no writes logs its reads and holds them.
  - `Transaction::hold_reads()` holds a read-only transaction's reads in memory until `ReadHold::release()`. `release()` answers whether the hold lasted, and is refused on a thread that drives a shard.

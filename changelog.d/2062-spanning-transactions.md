### Changed
- `phdb serve --cluster`: `Replication.Follow` on a cluster node serves a partition only to a node the map lists in that partition's `replicas`. An outside follower is refused with `FailedPrecondition`. A cluster node already refused `--replica-of`, so no documented setup followed a cluster partition from outside its map.

### Added
- **A cluster transaction can read and write rows of several partitions** (D329, ICR 0140), when it opts in with `BeginRequest.spans_partitions`. Its home is the partition its `route` names. Each other partition it touches is a branch on that partition's owner, and they all commit atomically through a two-phase commit whose commit point is a decision row on the home partition.
  - A commit that writes runs at `GroupSync` or stronger. `TxnCommitResponse.parts` lists each partition's share, the home first, and marks any that its partition hasn't applied yet.
  - A read-only one writes nothing: each partition holds its reads until a release round confirms that every hold lasted, or answers `Conflict`.
  - Every node must keep the new `spanning_txns` map feature. `--txn-max-branches` (default 16) bounds the branches of one transaction.
  - Without the opt-in, a row of another partition is still `CrossPartition`.
  - **A branch left in doubt by a home that fails is resolved on its own:**
    - each node's resolver asks the home what was decided once a share has been in doubt for `--txn-resolve-delay` (default: the request timeout);
    - with no decision recorded and the home's history unchanged, the home records an abort, so a late commit can't follow;
    - across a history change the share stays in doubt, with the reason in `Status` (`LocalPartition.in_doubt`, `in_doubt_shares`).
  - **`Transactions.Outcome`** answers what became of a transaction: in progress, committed (with its parts), aborted, unknown, or not found once its record has gone. Records are kept for `--txn-outcome-retention` (default 24 h) after every branch has applied the outcome.
  - **`phdb cluster txn decide <xid> --partition P --commit|--abort`** decides a share by hand. It's refused while the home answers, unless `--force`, which can break the transaction's atomicity.
  - **Limits:** `--txn-max-in-doubt` and `--txn-max-in-doubt-wal-bytes` (default 16 MiB) bound what a partition holds in doubt.
  - **The change feed:** each partition's commit of a spanning transaction carries the transaction's id (`FeedEvent.xid`), the same on every share, so a subscriber can group them.
- **The engine and `pigeonhole` (behind `__unstable-txn-prepare`):**
  - An external prepare with reads and no writes logs its reads and holds them.
  - `Transaction::hold_reads()` holds a read-only transaction's reads in memory until `ReadHold::release()`. `release()` answers whether the hold lasted, and is refused on a thread that drives a shard.

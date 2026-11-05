### Added
- **`phdb serve`: `Transactions` and `Snapshots`** (D313 ticket 4, ICR 0112, #1397). A transaction lives on the server between calls under a `txn_id` bound to its token, one call at a time (`SessionBusy` otherwise).
  - `Commit` runs to its outcome even if the client disconnects. A `Commit` sent again answers the first one's outcome without committing twice; other calls then answer `TransactionFinished`.
  - Idle transactions expire (`--txn-idle-ms`, `--txn-lifetime-ms`). Before any `Commit`, that's `TransactionExpired`, which is safe to retry from `Begin`.
  - Pinned snapshots (`Snapshots.Open`, `Release`) let `Get`, `MultiGet`, `ReadRow` and `Scan` read at one point in time across requests, under their own lease (`--snapshot-idle-ms`, `--snapshot-lifetime-ms`).
  - `--max-sessions` and `--max-sessions-per-token` cap the sessions held. `/metrics` reports them by kind, with the oldest one's age.
- **`Transaction::snapshot_seqno`** (ICR 0118): the seqno of the snapshot a transaction reads at.

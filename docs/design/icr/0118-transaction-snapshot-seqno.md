# 0118: `Transaction::snapshot_seqno`

**Status:** Approved (number from the coordinator, 2026-11-04: option (a) for D313 ticket 4).

## The change
- **pigeonhole** (semver, additive): `Transaction::snapshot_seqno(&self) -> u64`, the sequence number of the snapshot the transaction reads at. It's the engine transaction's snapshot seqno, the same number `Snapshot::seqno` gives for a snapshot taken at that moment. It never fails and doesn't count as a read.

## Why
ICR 0112's `Transactions.Begin` answers `snapshot_seqno`, the seqno the transaction reads at, so a client can relate it to a pinned snapshot's `seqno` (`Snapshots.Open`) or a commit's. `phdb serve` builds on the public API only (#692), and nothing public exposed it.

## Callers changed
None: additive. `phdb serve`'s `Transactions.Begin` (D313 ticket 4) uses it.

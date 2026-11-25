### Fixed
- **A change feed started after a restart no longer expires when a spanning transaction in doubt from before the restart commits** (#2189). On a `phdb serve` cluster partition that restarted while a spanning transaction's share there was in doubt, the following ended with `FeedExpired` ("history has a gap") when that transaction committed, though nothing was lost:
  - a feed subscription opened after the restart without a token, or from a snapshot;
  - a replica bootstrapped after the restart, which then copied the partition again;
  - a range move started after the restart, whose follower failed.

  The same held for an embedded `Pigeonhole` feed started from now, a snapshot or a seqno after a reopen with an externally prepared transaction in doubt. Such a start now begins before the transaction's share. A feed resumed from a token taken before the restart was never affected.

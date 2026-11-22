### Fixed
- **A spanning transaction's resolver no longer stalls behind a dead home** (#2078). It asks up to eight homes at once, and a home that gives no answer costs one ask a pass instead of one per share in doubt. So the other homes' shares, and the collection of decided records, go on beside it.
- **A commit decided by hand now lets its home's record be collected** (#2078). `phdb cluster txn decide --commit` tells the home, as the resolver does. A failed ack is retried until the home takes it, rather than lost. If the branch's node restarts before then, the home's record stays (#2098).

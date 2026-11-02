### Added
- The change feed's subscriber API (D310, #1248, ICR 0106). `Table::feed()` builds a `Feed` of whole commits, once each, in commit order, from the writer process:
  - over one or more tables, optionally only rows under a prefix;
  - durable by default, or `visible()` for sooner;
  - starting after now, after a snapshot, at the oldest retained history, or from a stored `FeedToken`, in this open or a later one.
  - Each `Commit` carries its `Change`s, with table and family names, the `ChangeKind`, row, qualifier, timestamp and typed value.
  - When the history a feed needs is gone, `next` fails with the new `ErrorCode::FeedExpired` (31), `pigeonhole.FeedExpired` in Python.
  - A durable feed follows an automatic recovery.
  - Reader processes can't subscribe yet.

### Fixed
- A change feed opened `from_oldest()` after the database was closed and reopened delivered none of the history kept from before the reopen, though a token taken before the close still resumed through it (D310, #1465). It now starts at the oldest earlier open whose history every shard still keeps whole. Unreleased; found before 1.1.0.

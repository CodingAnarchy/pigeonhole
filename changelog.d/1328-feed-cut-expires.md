### Fixed
- A change feed resumed from a token after a power loss could fail with `Corruption` ("wal segment chain: predecessor end mismatch") when the power loss had taken commits the feed already delivered, which only a `visible()` feed does. It now expires with `FeedExpired`, as the guide describes for that case (D310, #1248).

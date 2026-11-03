### Fixed
- A durable change feed (the default) under a steady stream of `Buffered` commits was delivered nothing until the load stopped: records logged while a sync was in flight kept each shard's synced hold from moving (D310, #1415). The hold now moves past what each sync covered. Unreleased; found before 1.1.0.

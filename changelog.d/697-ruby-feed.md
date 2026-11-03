### Added
- Ruby bindings, PR 2b (#697, ICR 0111): the change feed. `Pigeonhole.open(change_feed:, change_feed_off:, feed_age:, feed_tables:)`, `Table#feed` (with a block form), and `Feed` with `next`, `try_next`, `each`, `token` and `close`, delivering frozen `Commit` and `Change` objects. A wait stays interruptible (`Thread#raise`, `Timeout`, Ctrl-C).

### Added
- `Feed::progress` and `FeedProgress`: a feed's position, readable from another thread, which keeps moving past commits that carry nothing for the feed. And `Pigeonhole::visible_seqno`. `pigeonhole-otel`'s `Registration::watch_feed(name, progress)` reports a named feed's lag as `pigeonhole.feed.lag` (D316 A4, ICR 0116).

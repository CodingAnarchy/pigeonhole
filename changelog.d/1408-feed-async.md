### Added
- Async change feeds (D310, #1248): `Feed::poll_next`, and `nonblocking::FeedStream`, a `Stream` of owned commits for any executor (the `async` feature). A pending feed is woken, never polled in a loop, when a commit it waits for becomes durable (or visible, for a `visible()` feed). In a reader process the async form returns `Unsupported`: use the blocking `next` on a thread of its own there (#1407).
- A guide page for the change feed, [`docs/guide/change-feed.md`](docs/guide/change-feed.md).

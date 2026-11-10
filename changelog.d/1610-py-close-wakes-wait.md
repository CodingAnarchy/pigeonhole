### Fixed
- **Python: closing a feed or memory watch ends a pending `next_async`** (#1610). An `await feed.next_async()` or `await watch.next_async()` already waiting when another task closed the stream stayed pending until the next commit, or for ever. It now fails with `Closed` as soon as the stream is closed.

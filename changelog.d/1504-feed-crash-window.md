### Fixed
- The change-feed guide and the `FeedExpired` row of the errors guide now say that a process crash, not only a power loss, can expire a durable feed in the short window after a root commit (a flush's, a checkpoint's or a compaction's) and before the feed records it (#1504).

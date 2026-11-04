### Fixed
- The change-feed guide and the `FeedExpired` row of the errors guide now say that a process crash, not only a power loss, can expire a durable feed in the short window after a metadata commit (such as a flush's, a checkpoint's, a compaction's, a schema change's or a shrink's) and before the feed records it (#1504).

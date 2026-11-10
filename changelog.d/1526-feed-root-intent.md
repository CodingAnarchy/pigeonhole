### Fixed
- A process crash right after a metadata commit (a flush's, a checkpoint's, a compaction's, a schema change's or a shrink's) no longer expires durable change feeds: each such commit records its intent in the feed's sidecar before it writes, and the next open finishes the record from the commit itself (#1504, D315 amendment). Only a power loss in that window can still expire a feed.

### Changed
- Upgrade note: after a 1.2 session that changed the schema (created, dropped or extended a table) with the change feed on, a 1.1 build opening the database expires the feeds whose history runs across that change (`FeedExpired`), since 1.1 doesn't know the feed records 1.2 writes for schema changes. Nothing is skipped and no data is affected; start those feeds again from a snapshot. Without schema changes, 1.1 resumes a 1.2 feed.

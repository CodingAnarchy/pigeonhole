### Fixed
- A process crash right after a metadata commit (a flush's, a checkpoint's, a compaction's, a schema change's or a shrink's) no longer expires durable change feeds: each such commit records its intent in the feed's sidecar before it writes, and the next open finishes the record from the commit itself (#1504, D315 amendment). Only a power loss in that window can still expire a feed.


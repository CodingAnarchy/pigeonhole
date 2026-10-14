### Performance
- A flush that puts many cross-shard commits in SSTs no longer stalls its shard for tens of milliseconds (D254). Each shard now tells each other shard about them in one message, not one message per commit.

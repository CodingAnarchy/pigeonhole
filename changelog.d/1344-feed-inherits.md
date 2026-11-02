### Changed
- An open that doesn't mention the change feed now inherits it from the database's last open: the same retention, age and fed tables. With `Options::shards` unset, it also takes the recorded shard count. So a tool that opens the file between two runs of an app (`phdb load`, say) keeps the app's feed history and tokens, where it used to end them silently (D310, #1248).
### Added
- `Options::change_feed_off()` turns the change feed off for an open, ending its history. A new database, or one last opened with the feed off, stays off without it.

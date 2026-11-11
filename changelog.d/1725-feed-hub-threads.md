### Changed
- `phdb serve`: the threads that read the change feed into its shared subscriptions are as many as `--feed-threads` by default, rather than a quarter as many, and the new `--feed-hub-threads` sets them (#1719). With one such thread, 16 busy subscriptions fell seconds behind and delivered about half their events. `/metrics` gains `pigeonhole_serve_feed_hub_threads`.

### Added
- Reader processes follow the change feed in Python and `phdb mcp` (#1361, D310 PR 4): `Table.feed(...)` on a reader's table (`open_reader(path).table(...)`), and `phdb mcp --read-only`'s `feed_poll` beside a running application. If the writer restarts mid-wait, it's `SnapshotExpired`: resubscribe from the token.

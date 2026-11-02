### Changed
- `phdb mcp`'s `traverse` cursors page from the server's cached result instead of re-running the traversal (#1254). Each page costs only that page, and all pages come from one consistent result, so writes between pages no longer shift or repeat nodes. The server keeps one traversal, up to 64 MiB of node keys. A cursor whose result is gone runs the traversal again, as before.

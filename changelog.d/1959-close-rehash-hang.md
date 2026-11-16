### Fixed
- `phdb cluster close` on a partition that is already closed and whose file hash failed (the way to retry the hash) never returned, and the node could then no longer fence, unfence, open, close or stamp any partition until it was restarted. It now takes the hash again and returns (backport of #1956).

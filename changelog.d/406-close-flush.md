### Fixed
- A close whose flush failed after a WAL write error no longer restarts that flush indefinitely, rewriting the same SST each time and growing the file without bound. The close gives up flushing, as it does after any flush failure: the WAL keeps the data, and the close is unclean.
- A `flush` or `compact` call that races a close whose flush already failed now gets `Closed` at once, instead of waiting until the close ends.

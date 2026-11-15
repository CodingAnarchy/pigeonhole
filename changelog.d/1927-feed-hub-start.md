### Fixed
- `phdb serve`: a change-feed subscription opened while its tables were being written could receive nothing (only quiet heartbeats, no error) until every subscriber to those tables disconnected. A new fan-out hub's task could stop at its first commit, before the hub was ready; it now starts after the hub is made (#1926).

### Fixed
- A feed of every table (`Pigeonhole::feed()`) delivers a catalog change (a table created, a family added, a table dropped) at once on an idle database; it waited for the next commit before (#1539).

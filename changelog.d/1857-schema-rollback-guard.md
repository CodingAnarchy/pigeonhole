### Fixed
- A feed of every table resumed across a 1.1.x session that dropped a table (after a rollback from 1.2) answers `FeedExpired` when it reaches a commit to the dropped table. Before, it delivered that commit with an empty table name and went on without the drop (#1853).
- `phdb serve --replica-of`: a replica whose apply is refused because its tables are no longer its primary's bootstraps again. Before, it retried from the same token for ever. This happens after a schema change the primary's feed didn't record, such as one made while the primary ran 1.1.x (#1853).

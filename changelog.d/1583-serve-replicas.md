### Added
- **Read replicas for `phdb serve`** (#1436, ICR 0112 amendment): `phdb serve --replica-of URL` serves a copy that follows another `phdb serve`.
  - It bootstraps from the primary's backup of one durable snapshot (`Admin.BackupDownload` with `replica`), then follows the primary's durable commits (`Replication.Follow`), applying them in runs (`--replica-run`).
  - Reads see a consistent prefix of the primary's commits. Writes, schema changes and vector-index writes fail with `ReadOnly`, naming the primary.
  - A bootstrap download that stalls resumes from its offset on a new connection, with the stall in `last_error`.
  - Lag is in `Admin.Info`'s `replica` section and in `/metrics` (`pigeonhole_replica_*`).
  - A restart resumes where the replica stopped. A replica that falls behind the primary's feed retention, or whose URL now serves another database, bootstraps again while serving its old copy.
  - `Admin.Promote` (or `phdb promote <file>` on a stopped replica) makes it a primary. Nothing fences the old primary.
- **`phdb serve --change-feed BYTES`** turns the database's change feed on: replicas and `Feed.Subscribe` read it.
- **The `FailedPrecondition` server code** (409, not applied, don't retry): the server's role refuses the call, for example `Admin.Promote` on a server that isn't a replica.

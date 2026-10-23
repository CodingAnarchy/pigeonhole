### Fixed
- `pigeonhole`: with `Options::auto_recover` on, a commit that met a failed shard just before the recovery began closing it now gets `Busy` (a plain commit) or `SnapshotExpired` (a transaction, which `transact` retries) once the database has recovered, as a commit refused during the recovery already did; it used to get `Io` (#925, #921). Without `auto_recover` it is still `Io`.

### Added
- `pigeonhole-engine`: `Error::ShardFailed`, a commit refused before anything of it was logged by a shard whose WAL stream failed earlier (it was `Error::Io`; the message is unchanged) (#925).

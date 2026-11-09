### Fixed
- `phdb serve --replica-of`: a bootstrap download that stalls no longer hangs the replica. Each chunk waits at most `--stream-idle-ms` (the first answer to a fresh download, which comes once the primary has written its whole copy, at most 10 minutes); a stall is recorded in `last_error` and the download resumes from its offset on a new connection.

### Added
- `Admin.Info`'s replica section reports `bootstrap_phase` and `bootstrap_bytes` while a replica bootstraps (#1595, ICR 0112 amendment).

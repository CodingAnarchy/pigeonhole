### Added
- **`phdb serve`: `Schema` and `Admin`** (D313 ticket 6, ICR 0112, #1397).
  - `Schema.ListTables` (also `GET /v1/tables`), `CreateTable` (every family option, and `if_missing` to add the families a table lacks), `DescribeTable` (every option as stored) and `DropTable` (admin scope).
  - A table dropped and created again under its name is a new table to the server's later calls and feed streams.
  - `Admin.Health` (no token), `Info`, `Metrics`, `Flush` and `Compact`.
  - `Admin.Backup` writes a consistent backup inside `--backup-dir`, which is off without the flag.
  - `Admin.BackupDownload` streams one: gRPC chunks ending with its SHA-256, or the file over HTTP with its id and SHA-256 in headers. A broken download resumes from an offset for 5 minutes.

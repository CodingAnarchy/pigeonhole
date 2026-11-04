### Added
- **Sharing a database across processes and hosts** (#1397 step 1, D313): the guide's new page, `docs/guide/serving.md`, and a runnable example, `examples/serving`, in which one process owns the database and serves a small HTTP API to workers on other hosts.
  - The example covers paging by byte budget with stateless positions, writes that happen once, leases with fencing tokens on a TTL family, a transfer in `transact` made safe to retry by an operation id, and the change feed as server-sent events that resume with `Last-Event-ID`.
  - The page covers which errors make a write safe to retry, security in front of the server, backups and restore, and the single-node limits. `phdb serve` follows in 1.2.0.

### Added
- **The cluster map's writer holds a lease, and maps carry a writer term** (D330 PR 1, #1484).
  - The writer changes the map only while a majority grants its writer lease. A map with a higher term replaces one with a lower term, whatever their versions.
  - With the new `failover` map field on (off by default, set with `phdb cluster set-map`), a down writer is replaced by an elected one after `--cluster-failover-after-ms` (default twice `--cluster-lease-ms`).
  - `phdb cluster set-map --force` raises the term, so a writer it replaced can't change the map when it comes back.
  - `phdb cluster status` shows the writer, its term, its lease and `failover`.

### Fixed
- `phdb cluster status` shows a partition that is opening on a node as open only once writes route to it there (#2113). Before, it could show it `fenced`, awaiting its peers, a moment early, while a write still answered `PartitionUnavailable`. A reopened partition stays `closed` until then. An open that fails partway no longer leaves the partition shown as open on that node until a restart.
- A closed partition's files (`Admin.BackupDownload` with `fenced_file`) are refused while it's being reopened (`FailedPrecondition`), since its engine is already writing them.

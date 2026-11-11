### Added
- A `phdb serve --cluster` node takes a partition's files in through `Admin.PartitionUpload`, resumable via `Admin.PartitionUploadStatus` (D317, ICR 0117). It stages them for a restore or a move by file copy, which later steps install. `--max-uploads` caps uploads at once.

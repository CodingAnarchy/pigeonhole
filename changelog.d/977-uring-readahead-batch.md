### Added
- `pigeonhole_io::File::submit_reads_ahead`: submits reads nothing waits on yet (a scan's readahead) as one batch. The default submits each read in turn; a backend may hand the batch to the kernel at once (ICR 0077, #977).

### Changed
- io_uring backend: a scan's readahead goes to the kernel as one batch, each read run by a kernel worker thread, rather than one system call per block on the scanning thread. Concurrent scans no longer queue behind each other's page-cache copies on the shared ring. The kernel's worker threads per submitting thread are capped at 4 (#974, #977).

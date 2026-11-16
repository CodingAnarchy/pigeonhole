### Fixed
- `phdb cluster move` and `phdb cluster restore`: a node that takes a file's last byte and then never answers no longer hangs the command for ever. The wait for its answer is bounded by 60 s plus the file's size at 8 MiB/s; past that the move or the restore fails with a message saying to run it again, and the rerun resumes from what the node holds (#1983).

### Fixed

- `pigeonhole-otel` 0.1.2: the `pigeonhole.file.holes` description says the gauge reads 0 after a reopen until punch passes run again, and `pigeonhole.file.punch.refusals` says when punching resumes after a failed punch (#2181).
- Docs: `Options::punch_freed` says that punched space is no longer reserved on the filesystem (on a full disk a write into it can fail, which without punching only the file's growth could on an in-place filesystem such as ext4 or XFS; a failed root commit stops writes until reopen), and how the punch metrics read after a reopen (#2181).

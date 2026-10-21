### Added
- `ErrorCode::NoWriter` (29): a reader process found no running writer to attach to (ICR 0057, #844). `open_reader` with no writer running returns it, as does a reader's next read when the writer generation it must re-attach to is gone (on Windows, a writer that opened and closed meanwhile). Both used to fail with an untyped `Io`. Open the reader again once a writer runs.

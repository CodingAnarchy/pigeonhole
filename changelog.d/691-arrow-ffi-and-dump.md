### Added
- `pigeonhole-arrow` feature `ffi`: `ArrowScan::into_ffi_stream` hands an owned export over as an Arrow C stream (`FFI_ArrowArrayStream`), for consumers on another arrow-rs major or in another language (#691).
- `phdb dump --format arrow --table T`: one table as an Arrow IPC stream, in the Arrow export's schema with `value_i64` and `value_f64`, every retained version at one snapshot (#691). A cell an Arrow batch can't hold, such as a timestamp above `i64::MAX`, fails the dump with status 6 (`Unsupported`). CI reads such a dump with Polars.

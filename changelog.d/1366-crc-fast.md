### Changed
- The WAL's CRC32C uses the `crc-fast` crate instead of `crc32c`: about a third of the instructions for a typical small frame on x86 (115 against 322 for 160 bytes), with the same checksums and file format (#1366).

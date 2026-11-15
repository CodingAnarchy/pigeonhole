### Fixed
- `phdb cluster restore` keeps the chunk limit a node reports instead of raising it to 1 KiB, so a node with a smaller limit (a `--max-request-bytes` under about 4 KiB) no longer refuses every chunk. A node that reports no limit (one from before `max_chunk`) is sent 512 KiB chunks, as `phdb cluster move` sends, where it was sent 1 KiB chunks (#1830).

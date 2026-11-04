### Fixed
- A commit held back while its shard's WAL segment was nearly full (#19) could wait for ever when the segment header's sync landed before a spare segment was ready and no other commit came: the shard registered its wake once per hold and never again. It now registers again whenever the stream is still blocked (#1409).

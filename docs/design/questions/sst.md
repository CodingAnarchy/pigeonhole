# pigeonhole-sst questions

Each entry carries the coordinator's resolution from the PR #30 review (2026-10-06), to be folded into `decisions.md` at merge.

## Proposed decision: `SstWriterOptions::created_micros`
The properties block records `created_micros`, but `SstWriter` gets no clock (it has a `FileRef`, not a `Vfs`). Reading `SystemTime` would make simulated runs nondeterministic.

**Behavior:** an added public field `SstWriterOptions::created_micros: u64` (default 0 from `for_family`). Additive under D33. The engine sets it from `Vfs::now_micros` on flush and compaction.

**Resolution:** confirmed.

## Proposed decision: block-cache namespaces for SSTs and blob files
`BlockKey::file` must keep SST and blob-file ids apart, and the engine must pass the same values to `BlockCache::erase_files`.

**Behavior:** `pigeonhole_sst::sst_cache_file(SstId)` is the id with bit 63 clear, and `blob_cache_file(BlobFileId)` sets bit 63. SST ids must stay below 2^63: `sst_cache_file` has a `debug_assert`, and `interfaces.md` states the invariant. Blob ids are `u32`, so they are checked at compile time.

**Resolution:** confirmed, with the assert.

## Proposed decision: classifying errors
Callers (engine error mapping, robustness tests) need to tell bad bytes from I/O failures without matching on `format::Error`'s variants.

**Behavior:**
- `Error::is_corruption()` is true for truncated, bad-magic, checksum and corrupt errors.
- `Error::is_unsupported()` is true for an unsupported format version or codec.
- `KeyTooLarge` is a caller error and is in neither.

**Resolution:** changed (split) as above.

## Q: what does `SstReader::open` promise about an SST whose build was interrupted?
**Behavior:** the footer is written last, in its own write.
- With in-order (possibly torn) loss of unsynced writes, `open` accepts the SST only if every byte survived.
- With a disk that reorders unsynced writes, the footer can survive without earlier blocks. `open` still checks the footer, top index, filters and properties, but it does **not** checksum the data blocks or index partitions: that would read the whole SST. A lost block fails its checksum when it is read, which is a corruption error, never wrong data.

This is safe because nothing references an SST until its manifest commit is durable, and the root commit syncs the SST first.

**Resolution:** confirmed; documented on `SstReader::open`.

## Q: SST block decoding lived in `sst`, not `format::block::BlockIter`
**Resolution:** changed (ICR 0004). `format::block` now has an O(1) `Block::new`, `Block::validate`, `BlockIter::reset` and an inline key buffer, and `sst` uses `BlockIter<BlockHandle>`. The private cursor is gone.

## Q: `unsafe` in an sst test
The allocation test needs a counting `GlobalAlloc`, which is `unsafe impl`.

**Behavior:** `crates/sst/tests/alloc.rs` has the same forwarding allocator as `pigeonhole-cache`'s, with `// SAFETY:` comments. The library keeps `#![forbid(unsafe_code)]`.

**Resolution:** confirmed. CONTRIBUTING's `unsafe` rule now allows a counting `GlobalAlloc` in test and bench binaries only.

## Q: index partition size and readahead
**Behavior:**
- Index partitions target `min(block_size, 4 KiB)`.
- `ReadOptions::readahead_blocks` reads up to that many adjacent, uncached data blocks in one read during forward movement (never on seeks), within the current index partition.
- With `fill_cache = false` the read-ahead blocks are kept in the cursor, and every seek drops them.

**Resolution:** confirmed.

## Q: blob record caching and `BlobWriter::finish`'s byte count
**Behavior:**
- `BlobReader` caches verified records at `Priority::Low`, except records larger than `min(1 MiB, cache capacity / 8)`, which are returned pinned but uncached.
- Each extent's header (magic, version, checksum, blob file, position) is verified the first time a read touches that extent.
- `BlobWriter::finish` returns the logical length: record headers plus values, without extent headers.

**Resolution:** caching changed to the size cap above; the logical length is confirmed.

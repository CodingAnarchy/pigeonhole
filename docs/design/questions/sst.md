# pigeonhole-sst questions

## Proposed decision: `SstWriterOptions::created_micros`
The properties block records `created_micros`, but `SstWriter` gets no clock (it has a `FileRef`, not a `Vfs`). Reading `SystemTime` would make simulated runs nondeterministic.

**Interim behavior:** an added public field `SstWriterOptions::created_micros: u64` (default 0 from `for_family`). Additive under D33 (the struct is `#[non_exhaustive]`). The engine should set it from `Vfs::now_micros` on flush and compaction.

## Proposed decision: block-cache namespaces for SSTs and blob files
`BlockKey::file` must keep SST and blob-file ids apart ("tagged by the caller"), and the engine must pass the same values to `BlockCache::erase_files`.

**Interim behavior:** added `pigeonhole_sst::sst_cache_file(SstId) -> u64` (the id with bit 63 clear) and `blob_cache_file(BlobFileId) -> u64` (bit 63 set). SST ids are assumed below 2^63. Both are additive public functions; the engine and compaction should call them, not build keys themselves.

## Proposed decision: `Error::is_corruption`
Callers (engine error mapping, the robustness tests) need to tell bad bytes from I/O failures without matching on `format::Error`'s variants.

**Interim behavior:** added `Error::is_corruption()`: true for `Format` errors that mean stored bytes are bad (truncated, magic, checksum, corrupt, version, codec, oversized key part).

## Q: what does `SstReader::open` promise about an SST whose build was interrupted?
The writer never syncs (the pager's root commit does), and the SST is referenced only after `finish` returns and the manifest commit is durable. Still, the brief asks that a crash mid-write leave nothing `open` accepts.

**Interim behavior:** the footer is written last, in its own write. With in-order (possibly torn) loss of unsynced writes, `open` accepts the SST only if every byte survived. With a disk that reorders unsynced writes, the footer can survive without earlier blocks; `open` then still checks the top index, filters and properties, and any lost data or index block fails its checksum on read (a corruption error, never wrong data). `open` does not checksum every block up front, since that would read the whole SST.

## Q: SST block decoding lives in `sst`, not `format::block::BlockIter`
`format::block::BlockIter` starts each block with an empty `Vec` key buffer, so a point lookup allocates once per block visited, and `Block::new` checks both offset tables up front (O(entries) per block opened). The brief requires an allocation-free hot point lookup.

**Interim behavior:** `sst` has a private cursor over the same FORMAT §4.2 layout. It is reset onto each block, keeps keys of up to 128 bytes inline, and checks offsets lazily as it uses them (still bounds-checked; it never panics). `format`'s `BlockBuilder` still writes the blocks. If `format` later gains a reusable-buffer `BlockIter`, the sst cursor can switch to it.

## Q: `unsafe` in an sst test
The allocation test needs a counting `GlobalAlloc`, which is `unsafe impl`. CONTRIBUTING allows `unsafe` only in io, cache and memtable.

**Interim behavior:** `crates/sst/tests/alloc.rs` contains the same forwarding allocator as `pigeonhole-cache`'s `tests/alloc.rs`, with `// SAFETY:` comments. The library keeps `#![forbid(unsafe_code)]`.

## Q: index partition size, readahead and blob caching
These are not in the spec.

**Interim behavior:**
- Index partitions target `min(block_size, 4 KiB)`.
- `ReadOptions::readahead_blocks` reads up to that many adjacent, uncached data blocks in one read during forward movement (never on seeks), within the current index partition. With `fill_cache = false` the blocks are kept in the cursor instead of the cache.
- `BlobReader` caches whole records at `Priority::Low`.
- `BlobWriter::finish`'s byte count is the logical length (record headers plus values, without extent headers).

# Engine questions (blob separation, #33)

## Proposed decision (refines D29): separated values are pinned, not copied
D29 lists "blob reads" among the values `CellData` copies.

**Interim behavior:** a separated value read from its blob file is returned like an SST value: pinned in the block cache (`BlobReader` caches records up to `min(1 MiB, capacity / 8)` at low priority, D69, and hands larger ones out pinned but uncached), and copied only when it is at most `CellData::INLINE_MAX` (128) bytes. Separated values are at least `blob_threshold` bytes, so a copy would cost a large memcpy on every read.

## Proposed decision: format version 2
A 0.1.0 build reads a `Blob`-tagged value as empty bytes (`decode_value` falls back), so it would return wrong values from a file with blob files instead of refusing it.

**Interim behavior:** `FormatVersion::CURRENT` is 2 (`MIN_READABLE` stays 1): every structure is written with version 2, so 0.1.0 refuses the file at the superblock with `UnsupportedFormat`, and this build reads 0.1.0 files (upgraded to version 2 at the first commit). FORMAT §12 and the changelog say so; the golden files were regenerated. A per-file feature flag (version 2 only once a blob file exists) would keep untouched files openable by 0.1.0, but the pager writes the superblock version without knowing the catalog, and pre-1.0 the format may change in any release.

## Proposed decision: `drop_table` drops the table's blob files
FORMAT §9.3's `DropTable` drops the table's tablets and SSTs; blob files belong to a family, and nothing dropped them.

**Interim behavior:** `drop_table` commits a `DropBlobFile` for every blob file of the table's families in the same manifest commit as its `DropTable`, so their extents are retired at that version. FORMAT §7 says so. No implicit rule was added to `DropTable` itself.

## Proposed decision: what holds a blob file's extents after it is dropped
**Interim behavior:** as for SSTs. A dropped file's extents are retired at the commit's version and reclaimed once no view (in-process or reader process, D61) can reach them; a view keeps the file's `BlobReader` (opened lazily, shared across versions by blob id, `OpenBlob`), so a snapshot taken before a blob GC keeps reading the old file. Its cached records are erased at the drop; an old snapshot that reads the retired file again re-caches records under the dead id until the cache evicts them (harmless: blob ids are never reused, so nothing else reads them).

## Q: shrink and blob extents
D160's `shrink` relocates SST extents only. Blob extents past the shrink point are treated like in-flight output and skipped, so they set a floor on how far the file shrinks.

**Interim behavior:** skipped. Relocating them is #231.

## Q: values larger than D16's write-time limit
Blob files can hold values up to `2^32 - 1` bytes, but every value still passes through one WAL record and one memtable entry before a flush separates it.

**Interim behavior:** D16's limit stays (`ValueTooLarge` above `min(WAL segment payload, 64 MiB, half the arena)`). Lifting it is #230.

## Proposed decision (amends D120): `backup` copies the values the snapshot references
D120 refused `backup` of a database with blob files (`Unsupported`) until #58.

**Interim behavior:** with the two-phase backup (#262, #268), phase 1 copies the memtables into temporary SSTs with every value inline: blob files written there would belong to the copy, while phase 2 reads pointers through the source's blob files, and the temporary extents are freed after the merge. Phase 2 reads each source pointer through the snapshot's SST view (its pinned manifest version keeps the source blob files' extents) and writes the values, with the large values of the temporary SSTs, through the same separating sink a flush uses. The copy gets its own blob files (fresh ids from 1) holding exactly the values its SSTs reference, all live. Blob files are not copied extent by extent: that would also copy garbage and old files' layout. Each slot's merge (source and temporary SSTs) is clamped to its tablet's rows: after a split, children share SSTs that hold their siblings' rows too, and before this each child's copy repeated them.

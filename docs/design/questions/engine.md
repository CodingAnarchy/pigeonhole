# Engine questions (blob separation, #33)

## Proposed decision (refines D29): separated values are pinned, not copied
D29 lists "blob reads" among the values `CellData` copies.

**Interim behavior:** a separated value read from its blob file is returned like an SST value: pinned in the block cache (`BlobReader` caches records up to `min(1 MiB, capacity / 8)` at low priority, D69, and hands larger ones out pinned but uncached), and copied only when it is at most `CellData::INLINE_MAX` (128) bytes. Separated values are at least `blob_threshold` bytes, so a copy would cost a large memcpy on every read.

## Proposed decision: `drop_table` drops the table's blob files
FORMAT §9.3's `DropTable` drops the table's tablets and SSTs; blob files belong to a family, and nothing dropped them.

**Interim behavior:** `drop_table` commits a `DropBlobFile` for every blob file of the table's families in the same manifest commit as its `DropTable`, so their extents are retired at that version. FORMAT §7 says so. No implicit rule was added to `DropTable` itself.

## Proposed decision: what holds a blob file's extents after it is dropped
**Interim behavior:** as for SSTs. A dropped file's extents are retired at the commit's version and reclaimed once no view (in-process or reader process, D61) can reach them; a view keeps the file's `BlobReader` (opened lazily, shared across versions by blob id, `OpenBlob`), so a snapshot taken before a blob GC keeps reading the old file. Its cached records are erased at the drop.

## Q: shrink and blob extents
D160's `shrink` relocates SST extents only. Blob extents past the shrink point are treated like in-flight output and skipped, so they set a floor on how far the file shrinks.

**Interim behavior:** skipped. Relocating them is #231.

## Q: values larger than D16's write-time limit
Blob files can hold values up to `2^32 - 1` bytes, but every value still passes through one WAL record and one memtable entry before a flush separates it.

**Interim behavior:** D16's limit stays (`ValueTooLarge` above `min(WAL segment payload, 64 MiB, half the arena)`). Lifting it is #230.

# FORMAT

Every on-disk and shared-memory byte layout. Written during the interface freeze (build step 2). The executable form is the `pigeonhole-format` crate; where this file and the crate disagree, this file wins and the crate has a bug.

Format version **1**, shared-memory layout version **1**. Nothing here is promised stable before 1.0 (spec: "promise forward compatibility only at 1.0"), but every structure carries a magic or a version so it can evolve.

## 1. Conventions

- **Byte order.** Every integer is little-endian, except the ordering fields inside internal keys (§2), which are big-endian. The shared-memory structures (§11) are accessed as native-endian atomics, so only little-endian targets are supported; `pigeonhole-shm` and `pigeonhole-memtable` refuse to build elsewhere (decision D56).
- **Varint.** Unsigned LEB128, at most 10 bytes for a `u64`. A *bytes* field is a varint length followed by that many bytes.
- **Checksums.** The main file uses **xxh3-64** (seed 0). The WAL uses **CRC32C** (Castagnoli), unmasked.
- **Offsets** are byte offsets from the start of the structure being described unless stated otherwise.
- **Reserved** bytes are written as zero and ignored on read.
- **Page size** is 4096 bytes.
- **Magic numbers:**

| Structure | Magic (8 bytes unless noted) | Where |
|---|---|---|
| Superblock | `PHDBSUPR` | offset 0 of pages 0 and 1 |
| Manifest block | `PHDBMANI` | offset 0 of the block |
| SST | `PHDBSST\x01` | last 8 bytes of the SST |
| Blob extent | `PHDBBLOB` | offset 0 of each blob extent |
| WAL segment | `PHDBWALS` | offset 0 of each segment |
| Shared-memory region | `PHDBSHM\0` | offset 0 of the region |
| Shared-memory directory | `PHDBSHMD` | offset 0 of the directory region |
| Memtable header | `MEMT` (u32 `0x544D454D`) | offset 0 of each memtable header |

- **Versions.** `FormatVersion` (u32, currently 2; see §12) is stored in the superblock, every manifest block, every SST footer, every blob extent header and every WAL segment header. `ShmLayoutVersion` (u32, currently 1) is stored in the shared-memory header and must match exactly. Blocks, filters and WAL records carry kind/tag bytes whose numbering is frozen; new kinds take new numbers.

## 2. Internal key

Each family of each tablet is its own LSM tree; table and family ids are not part of the key.

```text
cell:   [row, escaped][00 01][qualifier, escaped][00 01][!ts: u64 BE][!seqno: u64 BE][kind: u8]
marker: [row, escaped][00 01][00 00]                    [!ts: u64 BE][!seqno: u64 BE][kind: u8]
```

- **Escaping.** Each `0x00` byte of the row or qualifier is written as `00 FF`. Every other byte is written as is. The terminator `00 01` sorts before any escaped continuation (`00 FF` or a byte `>= 01`), so a row that is a prefix of another sorts first, and byte order of encoded keys equals logical order. Row keys and qualifiers are each at most 65,536 unescaped bytes.
- **Family marker.** A family-in-row delete puts `00 00` where the escaped qualifier and its terminator would be. `00 00` never occurs inside an escaped string and sorts before `00 01` (the empty qualifier) and before every non-empty qualifier, so a reader meets a row's markers before any of its cells.
- **Suffix** (17 bytes): `!ts` is `u64::MAX - timestamp` big-endian, so newer timestamps sort first; `!seqno` is `u64::MAX - seqno` big-endian, so at equal timestamp the later commit sorts first; `kind` is one byte.
- **Comparison** is plain `memcmp` everywhere (memtables, blocks, indexes, merges).
- **Seek keys** use kind byte `0x00`, which sorts before every real kind: `[row][00 01][qual][00 01][!T][!S][00]` is the first possible entry for column `(row, qual)` with timestamp `<= T` and, at `T`, seqno `<= S`.

| Kind | Value | Meaning |
|---|---|---|
| `Put` | `0x01` | A value (inline or blob pointer) for this version |
| `Merge` | `0x02` | A merge operand; resolved at read and compaction time |
| `CellDelete` | `0x03` | Deletes every version with exactly this timestamp, whatever its seqno |
| `ColumnDelete` | `0x04` | Deletes every version of the column with timestamp `<=` this one |
| `FamilyDelete` | `0x05` | Marker key only: deletes every cell of the row in this family with timestamp `<=` this one |

**Delete rule** (BigTable semantics, decisions D9 and D38). A `ColumnDelete` or `FamilyDelete` with timestamp `T` hides every version in its scope with timestamp `<= T`, **regardless of seqno**: a put committed later with an older timestamp stays hidden. A `CellDelete` with timestamp `T` hides every version with exactly timestamp `T`, also **regardless of seqno**: a put or merge operand at `T` committed after the delete stays hidden. Seqnos decide only which entries a snapshot can see (a snapshot taken before the delete still sees the versions it hides).

Timestamps are microseconds since the Unix epoch by convention (decision D11). Seqnos are global, start at 1, and are unique per commit; every cell of one commit carries the commit's seqno.

## 3. Values

A stored value is a one-byte tag followed by the payload. Delete kinds have an empty value (no tag).

| Tag | Value | Payload |
|---|---|---|
| `Bytes` | `0x00` | the bytes |
| `I64` | `0x01` | 8 bytes, `i64` LE |
| `F64` | `0x02` | 8 bytes, IEEE-754 bit pattern LE |
| `Varint` | `0x03` | zigzag-LEB128 `i64` |
| `Blob` | `0x80` | 16-byte blob pointer |

The largest value is `2^32 - 1` bytes (decision D16).

**Blob pointer** (16 bytes):

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | `blob_file` u32: logical blob file id |
| 4 | 4 | `len` u32: length of the stored value the pointer replaced (tag byte included) |
| 8 | 8 | `offset` u64: logical offset of the blob record within the blob file (§7) |

## 4. Blocks

### 4.1 Physical block

Every block in an SST is stored as `payload ++ trailer`. A block address (`BlockAddr`) is the block's offset from the SST start and its physical length including the trailer.

Trailer (16 bytes, at the end of the physical block):

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | `kind`: 1 Data, 2 Index, 3 TopIndex, 4 Filter, 5 Properties |
| 1 | 1 | `compression`: 0 None, 1 LZ4 (block format, no frame), 2 zstd (one zstd frame, standard format, no dictionary) |
| 2 | 2 | reserved |
| 4 | 4 | `uncompressed_len` u32 |
| 8 | 8 | `checksum` u64: xxh3-64 of `payload ++ trailer[0..8]` |

The checksum is verified on every read from disk and skipped on cache hits. A logical block is at most 128 MiB (twice the largest extent); a reader rejects a larger `uncompressed_len`, and an LZ4 one above 255 times the payload length. A zstd block decodes into a buffer of exactly `uncompressed_len` bytes and is rejected if its frame holds more or less. The zstd level is a writer setting (the family's `compression_level`) and is not stored per block. A writer stores a block uncompressed when compression saves less than 1/8 of its size.

### 4.2 Logical data and index block

```text
entry*  restarts: u32 x R  row_starts: u32 x S  R: u32  S: u32
entry = shared: varint | unshared: varint | value_len: varint | key[shared..]: unshared bytes | value: value_len bytes
```

- `shared` is the length of the prefix shared with the previous entry's key; it is 0 at restart points.
- `restarts` holds the offset of every restart entry, ascending; the first entry is always a restart. Data blocks restart every 16 entries by default; index blocks restart at every entry (interval 1).
- `row_starts` (data blocks only; `S = 0` in index blocks) holds the offset of every entry whose row differs from the previous entry's row (the first entry of the block included if it starts a row). A row-start entry need not be a restart: its shared prefix lies within the previous key's row prefix (escaped row including its terminator), which a scanner skipping the current row already knows, so the entry decodes without the skipped cells.
- Offsets in both tables are from the start of the logical block.
- Default target size of a data block is 16 KiB uncompressed (per family). A block holds at least one entry, so one huge cell may exceed the target.

**Index entries.** Key: a separator `K_i` with `last_key(block_i) <= K_i < first_key(block_i+1)` (byte order; a separator need not be a valid internal key). Value: the target's `BlockAddr` as `offset: varint, len: varint`. To find the block that may hold key `k`, take the first entry whose separator is `>= k`.

## 5. SST

An SST occupies the start of one extent (§8.2); its length is recorded in the manifest.

```text
[data block]* [index partition]* [top index] [row filter] [column filter] [properties] [footer]
```

- **Partitioned index.** Each *index partition* (kind Index) indexes a run of data blocks. The *top index* (kind TopIndex) indexes the partitions. The top index and both filters are read at open and pinned in memory, so a point lookup costs at most one cached partition lookup and one data-block read.

### 5.1 Footer (104 bytes, at the end of the SST)

Each address is 16 bytes: `offset` u64, `len` u32, reserved u32. A zero-length address means "absent".

| Offset | Size | Field |
|---|---|---|
| 0 | 16 | top index address |
| 16 | 16 | row filter address (absent if the family has no filter) |
| 32 | 16 | column filter address (absent if no filter) |
| 48 | 16 | properties address |
| 64 | 16 | compression dictionary address (reserved for zstd dictionaries; absent in v1) |
| 80 | 4 | `format_version` u32 |
| 84 | 4 | `flags` u32 (reserved, 0) |
| 88 | 8 | `checksum` u64: xxh3-64 of bytes 0..88 |
| 96 | 8 | magic `PHDBSST\x01` |

### 5.2 Properties block (kind Properties)

Fields in order, fixed-width little-endian unless marked *bytes* (varint length + bytes):

`table` u32, `family` u32, `tablet` u64, `entries` u64, `rows` u64, `deletes` u64, `merges` u64, `raw_key_bytes` u64, `raw_value_bytes` u64, `data_blocks` u32, `index_partitions` u32, `min_seqno` u64, `max_seqno` u64, `min_ts` u64, `max_ts` u64, `created_micros` u64, `smallest_key` *bytes*, `largest_key` *bytes*, `merge_operator` *bytes* (UTF-8, empty if none).

Readers ignore trailing bytes after the last known field, so fields can be appended.

## 6. Filters

Two filters per SST: a **row filter** keyed by the escaped row bytes (no terminator), and a **column filter** keyed by the column prefix (escaped row, `00 01`, escaped qualifier, `00 01`). For every family marker, the column filter also gets the key `escaped row ++ 00 01 ++ 00 00`; a point get probes both its column key and that marker key, so skipping an SST can never hide a family delete.

Logical filter block, kind byte 1 (**blocked bloom**):

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | `filter_kind` = 1 |
| 1 | 1 | `probes` k (1..=16): `clamp(floor(bits_per_key * 0.69), 1, 16)` |
| 2 | 2 | reserved |
| 4 | 4 | `num_lines` u32 (>= 1): `ceil(keys * bits_per_key / 512)` |
| 8 | 64 x `num_lines` | bit lines |

Probing key hash `h = xxh3_64(key)`: let `h1 = h as u32`, `h2 = (h >> 32) as u32`, `line = (h2 as u64 * num_lines as u64) >> 32`, `delta = h1.rotate_right(17)`, `x = h1`. For each of the `k` probes: test (or set) bit `x & 511` of the line, then `x = x.wrapping_add(delta)`. Bit `b` of a line is bit `b % 8` (LSB first) of byte `b / 8`. A new filter algorithm (ribbon) gets a new `filter_kind`.

## 7. Blob extents

A logical blob file is a list of extents of equal size class, listed in the manifest (`PutBlobFile`). Each extent starts with a 64-byte header; the payload areas (`extent_len - 64` bytes each) concatenate into one logical address space. Logical offset `L` is extent `L / P`, byte `64 + L % P`, where `P = extent_len - 64`. Records may span extents.

Blob extent header (64 bytes):

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `PHDBBLOB` |
| 8 | 4 | `format_version` u32 |
| 12 | 4 | `blob_file` u32 |
| 16 | 4 | `extent_index` u32 |
| 20 | 36 | reserved |
| 56 | 8 | `checksum` u64: xxh3-64 of bytes 0..56 |

Blob record at a logical offset: `len` u64, `checksum` u64 (xxh3-64 of the value), then `len` bytes of value. The value is the stored value (§3) that the pointer replaced, tag byte included, so a reader returns the record as the stored value. The pointer's `len` must equal the record's `len`. Because offsets are logical, a value may span several extents; this is how values larger than one 64 MiB extent can be stored. A value larger than `min(WAL segment payload, 64 MiB, half the shard's memtable arena)` is still rejected at write time with `ValueTooLarge` (decision D16): it must pass through the WAL and a memtable before it is separated.

**Separation.** A put whose stored value has tag `Bytes` and a payload longer than its family's `blob_threshold` (`u32::MAX`: never) is separated when it is written to an SST by a flush or a compaction: the value is appended to a new blob file and the SST holds the `Blob` tag and the pointer. Typed values and merge operands are never separated. Blob records are stored uncompressed: the family's codec (§4.1) applies to SST blocks, which hold the pointers, so nothing is compressed twice. A blob file belongs to one family; every extent of a file has the same size class, and a file of one extent may use any class that holds it.

**Live bytes.** `PutBlobFile.total_bytes` is the file's logical length (record headers plus values); `live_bytes` is the part still referenced: the sum of `16 + len` over the pointers that the SSTs hold within their tablets' rows (an SST shared after a split counts once per tablet, for that tablet's rows). A compaction lowers it for every pointer it drops, and a blob GC for every value it copies to a new file; a file is dropped (`DropBlobFile`) once it reaches zero, and with its table (`DropTable` is accompanied by a `DropBlobFile` per blob file of the table's families).

## 8. Main file

### 8.1 Pages and extents

```text
page 0      superblock A
page 1      superblock B
page 2      lock page (never read or written; byte-range locks only)
pages 3-15  reserved (zero)
page 16..   extents
```

An **extent** is `64 KiB << size_class` bytes (`size_class` 0..=10: 64 KiB to 64 MiB), aligned to its own size (buddy allocation), starting at page 16 or later. Persisted as `page` u64 plus `size_class` u8. SSTs, blob extents and manifest blocks each live in their own extents. Free space is not persisted: at open, everything not reachable from the manifest is free (decision D8).

### 8.2 Superblock (pages 0 and 1)

The first 128 bytes of the page; the rest is zero.

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `PHDBSUPR` |
| 8 | 4 | `format_version` u32 |
| 12 | 4 | `page_size` u32 (4096) |
| 16 | 8 | `sequence` u64: incremented by every root commit |
| 24 | 16 | `db_id`: random at creation |
| 40 | 8 | `snapshot_page` u64: manifest snapshot block (0 = empty database) |
| 48 | 1 | `snapshot_size_class` u8 |
| 49 | 3 | reserved |
| 52 | 4 | `snapshot_len` u32 |
| 56 | 8 | `manifest_version` u64 |
| 64 | 8 | `file_pages` u64: file high-water mark, in pages |
| 72 | 8 | `flags` u64: bit 0 = last writer closed cleanly |
| 80 | 8 | `log_page` u64: manifest delta log (0 = none) |
| 88 | 1 | `log_size_class` u8 |
| 89 | 3 | reserved |
| 92 | 4 | `log_len` u32: live bytes of the delta log |
| 96 | 24 | reserved |
| 120 | 8 | `checksum` u64: xxh3-64 of bytes 0..120 |

A superblock is valid if magic, checksum and version check out. The valid one with the higher `sequence` is current. **Root commit:** sync all data written since the last commit, write the new superblock (sequence + 1) over the *non-current* slot, sync. This is the only in-place write of live data in the main file. Reader processes re-read both superblocks to pick up a new root; they never write.

### 8.3 Lock page (page 2)

Byte-range locks on single bytes at absolute file offsets (decision D3; refined by D21):

| Offset | Byte | Held |
|---|---|---|
| 8192 | writer | exclusive by the one writer; a second writer fails with `WriterLocked` |
| 8193 | presence | shared by every process with the database open; a process that can upgrade it to exclusive at close is the last one |
| 8194 | shm-init | exclusive while a process creates, validates or rebuilds the shared-memory region |

## 9. Manifest

The manifest is a **snapshot block** plus a **delta log** (decision D7). The snapshot block, in its own extent, holds every edit needed to rebuild the state from empty. The delta log is one 256 KiB extent (size class 2) holding consecutive **delta blocks**, one per manifest commit, with versions `snapshot version + 1, + 2, ...`. The superblock names the snapshot, the log and the log's live length.

- **Commit:** append the new delta block at `log_len` (bytes past `log_len` are not live, so nothing live is overwritten), then commit a root with the larger `log_len` and the new version. The root commit's first sync covers the delta.
- **Compaction of the manifest:** when the delta would not fit in the log extent, or the live log exceeds the snapshot's length, the writer writes a new snapshot block and a fresh, empty log (new extents) and commits that root. The old snapshot and log extents are retired at that version.
- **Open:** read the superblocks, the snapshot block and the live log: three reads, independent of history.
- **Reader catch-up:** a reader at version `v` re-reads the superblock and, if the snapshot is unchanged, only the log bytes after its last position.

### 9.1 Block header (64 bytes)

Snapshot and delta blocks share this header.

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `PHDBMANI` |
| 8 | 4 | `format_version` u32 |
| 12 | 1 | `kind`: 1 Snapshot, 2 Delta |
| 13 | 3 | reserved |
| 16 | 8 | `manifest_version` u64 (this block's) |
| 24 | 16 | reserved |
| 40 | 4 | `edit_count` u32 |
| 44 | 4 | `body_len` u32 |
| 48 | 8 | `checksum` u64: xxh3-64 of header bytes 0..48 followed by the body |
| 56 | 8 | reserved |

The body (`body_len` bytes) follows at offset 64: `edit_count` edits. In the log, the next delta starts right after the previous body.

### 9.2 Edit encoding

Each edit is `tag: u8, body_len: varint, body`. The length prefix lets a reader skip tags it does not know. Field types: fixed-width LE integers; *bytes* = varint length + bytes; *string* = *bytes* of UTF-8; *opt-bytes* = `u8` (0 absent, 1 present) then *bytes* if present; *extent* = `page` u64 + `size_class` u8.

*FamilyOptions* = `compression` u8, `compression_level` i8, `block_size` u32, `bloom_bits` u8, `max_versions` u32, `ttl_micros` u64, `blob_threshold` u32, `merge_operator` string, `cache_priority` u8 (0 Low, 1 Normal, 2 High), `compaction` u8 (0 Leveled, 1 Tiered, 2 FifoByTime).

*SstMeta* = `id` u64, `extent` extent, `len` u64, `smallest_key` bytes, `largest_key` bytes, `min_seqno` u64, `max_seqno` u64, `min_ts` u64, `max_ts` u64, `entries` u64, `deletes` u64.

### 9.3 Edits

| Tag | Edit | Body |
|---|---|---|
| 1 | `CreateTable` | `table` u32, `name` string |
| 2 | `DropTable` | `table` u32 |
| 3 | `PutFamily` | `table` u32, `family` u32, `name` string, FamilyOptions |
| 4 | `PutTablet` | `tablet` u64, `table` u32, `start` bytes, `end` opt-bytes |
| 5 | `DropTablet` | `tablet` u64 |
| 6 | `AddSst` | `tablet` u64, `family` u32, `level` u8, SstMeta |
| 7 | `RemoveSst` | `tablet` u64, `family` u32, `sst` u64 |
| 8 | `SetFlushed` | `tablet` u64, `family` u32, `seqno` u64 |
| 9 | `WalCheckpoint` | `stream` u32, `lsn` u64 |
| 10 | `PutBlobFile` | `blob_file` u32, `family` u32, `count` u32, `count` x extent, `total_bytes` u64, `live_bytes` u64 |
| 11 | `DropBlobFile` | `blob_file` u32 |
| 12 | `Counters` | `next_table` u32, `next_family` u32, `next_tablet` u64, `next_sst` u64, `next_blob_file` u32, `seqno_ceiling` u64, `ts_floor` u64 |

Rules: family ids are unique across the database, so `(tablet, family)` names one tree. After a split both child tablets may reference the same SST; an SST's extent is retired when no tablet references it. A snapshot block contains `Counters`, every live table, family, tablet, SST, flushed seqno, blob file and stream checkpoint. The live extents at open are: the snapshot and log extents, every referenced SST extent, and every blob-file extent. `ts_floor` is at least every default timestamp assigned before the commit (decision D11).

## 10. WAL

### 10.1 Files and segments

Stream `N` of database `data.phdb` is the file `data.phdb-wal-N` (decimal `N`). Stream numbers are independent of shard numbers. A stream file is a sequence of equal-size **slots** (default 64 MiB, a multiple of 32 KiB, at most 4 GiB − 32 KiB so that `prev_end` always fits a `u32`; decision D43) at offsets `k x segment_size`. Slots are preallocated and recycled after checkpoint, never deleted while the database is open. Each use of a slot is a **segment** with its own **epoch** (u32): one more than the largest epoch in any segment header of the stream, so epochs never repeat.

An **LSN** is `(epoch << 32) | offset_within_segment`. LSNs increase monotonically within a stream. The manifest records each stream's checkpoint LSN.

**Chaining** (decision D25). Each segment header names its predecessor: `prev_epoch` and `prev_end`, the offset where the predecessor's valid data ends. Rules for the writer:
1. When a segment fills, write nothing more to it, **sync it**, and only then write the next segment's header with `prev_end` = the offset where the full segment's last record ends (its stop offset, §10.2; the zero tail after it is not part of `prev_end`). The sync may be submitted to the I/O backend; the header write waits for it to complete (decision D30).
2. After recovery, **never append to the last replayed segment**: start a new segment (epoch = max seen + 1) with `prev_epoch`/`prev_end` = where replay ended.

Replay starts at the segment whose epoch is the checkpoint's, at the checkpoint's offset, and reads until the segment's data stops (§10.2). It then looks for the segment whose header has `prev_epoch` = this epoch:
- none: **end of log**; the stop point is the torn tail.
- one, with `prev_end` = the stop offset: **end of segment**; continue there.
- one, with a different `prev_end`: synced data is missing; recovery fails with a corruption error instead of dropping it.

### 10.2 Frames

A segment is made of 32 KiB **frames**. Frame 0 holds the segment header; the rest of frame 0 is zero.

Segment header (offset 0 of frame 0):

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `PHDBWALS` |
| 8 | 4 | `format_version` u32 |
| 12 | 4 | `stream` u32 |
| 16 | 4 | `epoch` u32 |
| 20 | 4 | `prev_epoch` u32 (0 = first segment of a new stream) |
| 24 | 16 | `db_id` (must match the superblock) |
| 40 | 8 | `segment_size` u64 |
| 48 | 4 | `frame_size` u32 (32768) |
| 52 | 4 | `prev_end` u32: offset where the predecessor's data ends |
| 56 | 4 | `crc` u32: CRC32C of bytes 0..56 |

Frames 1.. hold **fragments**. Fragment header (12 bytes):

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | `crc` u32: CRC32C of header bytes 4..12 followed by the payload |
| 4 | 4 | `epoch` u32: must equal the segment's epoch |
| 8 | 2 | `len` u16: payload length |
| 10 | 1 | `type`: 1 Full, 2 First, 3 Middle, 4 Last (0 = unused space) |
| 11 | 1 | reserved |

A record that fits in the rest of the current frame is one Full fragment; otherwise it is split into First, Middle..., Last fragments across frames. Fragments never span frames. If 12 bytes or fewer remain in a frame (no room for a header and any payload), the writer zero-pads them, the reader skips them whatever they contain, and the next fragment starts at the next frame. So every fragment carries payload except the Full fragment of an empty record. A record never spans segments: if it does not fit, the writer moves to a new segment. A record larger than a segment's payload fails with `RecordTooLarge`.

**Where a segment's data stops.** The stop offset is always the end of the last complete record (the starting offset if there is none), never a position after a skipped frame tail; a writer whose segment fills records exactly this offset (where its last record ended) as the successor's `prev_end`. Replay of a segment stops at the first fragment header (read only where more than 12 bytes remain in the frame) that has type 0, an epoch different from the segment's (stale data from a previous use of the slot), a bad CRC, or a First/Middle whose Last never arrives before the segment ends. Whether that stop is the end of the segment or the end of the log is decided by chaining (§10.1), never by the stop condition itself. Data after the end of the log is never read again: the next segment starts fresh.

### 10.3 Records

The payload of a reassembled record:

| Type | Byte 0 | Fields after the type byte |
|---|---|---|
| Batch | `1` | `seqno` u64, `commit_ts` u64, batch |
| Prepare | `2` | `seqno` u64, `commit_ts` u64, `coordinator_stream` u32, batch |
| Commit | `3` | `seqno` u64, `count` u16, `count` x `participant_stream` u32 |

**Batch** encoding (shared with the engine's `WriteBatch`, so commit never re-encodes): `count` u32, then `count` mutations:

| Field | Encoding |
|---|---|
| `table` | u32 |
| `family` | u32 |
| `kind_flags` | u8: bits 0-6 = kind (§2); bit 7 = explicit timestamp present |
| `ts` | u64, only if bit 7 is set; otherwise the record's `commit_ts` |
| `row` | bytes (unescaped) |
| `qualifier` | bytes (unescaped; empty for FamilyDelete) |
| `value` | bytes: a stored value (§3); empty for deletes |

**Cross-shard commits.** The commit's id is its seqno, reserved once by the coordinator, so it is unique for the life of the database (decision D26). Each participant writes a Prepare with its share of the mutations. The coordinator writes a Commit to its own stream once every Prepare meets the requested durability. At recovery, a Prepare is applied if and only if its coordinator's stream holds a Commit with the same seqno. Recovery sets `next_seqno` above every seqno in every replayed record, **including discarded Prepares**, so no seqno is ever reused. A stream's checkpoint never advances past a Commit record while any of its participants' Prepares may still need replay (decision D24).

A `Durability::None` commit's record is appended to the shard's in-memory stream buffer with no I/O of its own; the next `write()` or fsync, triggered by a stronger commit on the same stream, carries it to the file. So a stronger commit also makes every earlier `None` commit of its stream durable (the mixed-levels rule), and a `None` commit that no stronger commit follows survives nothing past the last flush.

## 11. Shared-memory region

Two objects (decision D27), placed at `/dev/shm/<name>` on Linux, POSIX `shm_open("/<name>")` on macOS and BSD, a `Local\<name>` pagefile-backed mapping on Windows, or `<shm_dir>/<name>.phdb-shm` when `shm_dir` is set:

- **Directory** `phdb-<h>`, where `<h>` is 16 lowercase hex digits of `xxh3_64(device LE ++ inode LE)` of the main file. One page whose layout never changes: magic `PHDBSHMD` @0, `format` u32 @8 (=1), `generation` u64 atomic @16 (current region, 0 = none), `layout_version` u32 atomic @24.
- **Region** `phdb-<h>-<generation in hex>` (at most 30 bytes: macOS's `shm_open` limit is 31 including the leading `/`; decision D44), laid out below.

A writer always builds a new generation: create the new region, store `state = abandoned` in the old one, then store the new generation in the directory. Because the name changes with the generation, a mapping some process still holds (Windows keeps named mappings alive while any handle is open) is never reused. Readers that see `state = abandoned` or a different directory generation re-attach. The header also stores the device, inode and `db_id`, which an attaching process verifies.

```text
[header 4096][watermarks 64 x shards][view buffer 0][view buffer 1][reader slots 64 x n][pad to 2 MiB][arena 0]...[arena shards-1]
```

### 11.1 Header (4096 bytes)

"atomic" fields change while the region is live and are accessed with atomic loads and stores; all others are written once while the shm-init lock is held, before `state` becomes ready.

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `PHDBSHM\0` |
| 8 | 4 | `layout_version` u32 |
| 12 | 4 | `header_len` u32 (4096) |
| 16 | 8 | `region_len` u64 |
| 24 | 16 | `db_id` |
| 40 | 8 | `generation` u64: this region's generation (also in its name) |
| 48 | 4 | `state` u32, atomic: 0 initializing, 1 ready, 2 abandoned (replaced by a newer generation) |
| 52 | 4 | `shard_count` u32 |
| 56 | 4 | `reader_slot_count` u32 |
| 60 | 4 | `view_buffer_len` u32 |
| 64 | 8 | `manifest_version` u64, atomic |
| 72 | 8 | `view_pointer` u64, atomic: `(view_version << 1) \| buffer_index`; view versions start at 1, 0 = none yet |
| 80 | 4 | `writer_pid` u32 |
| 84 | 4 | reserved |
| 88 | 8 | `writer_start_time` u64 |
| 96 | 8 | `watermarks_off` u64 |
| 104 | 8 | `views_off` u64 |
| 112 | 8 | `reader_slots_off` u64 |
| 120 | 8 | `arenas_off` u64 (2 MiB aligned) |
| 128 | 8 | `arena_len` u64 (per shard, multiple of 2 MiB) |
| 136 | 8 | `next_seqno` u64, atomic: next global seqno to reserve |
| 144 | 8 | `file_device` u64 |
| 152 | 8 | `file_inode` u64 |
| 160 | 3936 | reserved |

### 11.2 Watermarks

One 64-byte line per shard at `watermarks_off + 64 x shard`. Offset 0: `pending` u64, atomic: the lowest seqno this shard holds unapplied, or `u64::MAX` when idle (an idle shard never holds back snapshots). The rest of the line is padding (no false sharing).

### 11.3 Seqno reservation and snapshot protocol

Each shard tracks `held`: the seqnos of cross-shard commits it coordinates that are not yet applied everywhere. Every store to `pending` publishes `min(held, x)`:

1. `pending[shard].store(min(held, next_seqno.load()))` (Release): a lower bound, published before reserving.
2. `first = next_seqno.fetch_add(n)` (AcqRel). A cross-shard commit reserves `n = 1` and adds its seqno to `held`.
3. `pending[shard].store(min(held, first))`; apply the group; then store `min(held, next unapplied seqno)`, which is `u64::MAX` when nothing is held or pending.
4. When every participant has applied a cross-shard commit, remove it from `held` and store `pending` again.

Without the `min(held, ..)` in steps 1 and 3, a coordinator's next group would overwrite `pending` and expose half of a cross-shard commit.

Snapshot (writer threads and reader processes alike): `n = next_seqno.load()` (Acquire), then `m = min over shards of pending.load()` (Acquire); the snapshot seqno is `min(n, m) - 1`. Every seqno below `n` was reserved by a `fetch_add` that published a lower bound first, so it is either applied or holds `m` at or below it.

### 11.4 View buffers

Two buffers of `view_buffer_len` bytes (default 4 MiB) at `views_off` and `views_off + view_buffer_len`. A view that does not fit is never published: the writer gets `ViewTooLarge` and refuses the change that grew it (for example a tablet split) (decision D28). The writer encodes a new view into the buffer not named by `view_pointer`, then stores `view_pointer` (Release). A reader loads `view_pointer` (Acquire), copies the named buffer, re-loads `view_pointer`, and retries if it changed or the CRC fails.

View record:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `view_version` u64 |
| 8 | 8 | `manifest_version` u64: the SST set this view uses |
| 16 | 4 | `byte_len` u32: total record length |
| 20 | 4 | `tablet_count` u32 |
| 24 | 4 | `memtable_count` u32 |
| 28 | 4 | `crc` u32: CRC32C of the record with this field zeroed |
| 32 | ... | `tablet_count` tablet entries, then `memtable_count` memtable entries |

Tablet entry (8-byte aligned): `tablet` u64, `table` u32, `shard` u16, `flags` u16 (bit 0: has end), `start_len` u32, `end_len` u32, `start` bytes, `end` bytes, zero padding to 8 bytes.

Memtable entry (24 bytes): `tablet` u64, `family` u32, `shard` u16, `age` u8 (0 active, then 1, 2, ... frozen, newest first), reserved u8, `root` u32 (memtable header offset within the shard's arena), reserved u32.

The tablet map and the memtable list travel together in one view record (decision D23).

### 11.5 Reader slots

`reader_slot_count` slots of 64 bytes at `reader_slots_off`:

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | `state` u32, atomic: 0 free, 1 claiming, 2 active |
| 4 | 4 | `pid` u32 |
| 8 | 8 | `start_time` u64 (platform-defined process start time) |
| 16 | 8 | `pinned_seqno` u64, atomic (0 = none) |
| 24 | 8 | `pinned_view` u64, atomic (0 = none) |
| 32 | 8 | `generation` u64: region generation at claim |
| 40 | 24 | reserved |

Claim: CAS `state` 0 to 1, write `pid`, `start_time`, `generation`, store `state` = 2. Pin: store `pinned_view = v` (SeqCst), store `pinned_seqno = s`, then re-load `view_pointer`; if the view version moved past `v`, take a fresh snapshot seqno `s' >= s` and pin again with the new version and `s'` (the seqno and the view always move as a pair, and the reader uses the pair it ended up pinning). The writer frees a view's memtables, and the extents its manifest version uses, only after publishing a newer view and finding no slot pinning that view or an older one. A slot whose process is gone (`pid` absent or `start_time` changed) is reset to free by the writer. Readers write nothing in the region except their own slot.

### 11.6 Arenas and memtables

Shard `i`'s arena is `arena_len` bytes at `arenas_off + i x arena_len`. All arena offsets are `u32` relative to the arena base; `0` is null and the first 64 bytes of every arena are reserved. How the writer carves the arena into chunks is private to the writer process (a new writer rebuilds the region).

Memtable header (64 bytes, at the `root` offset named in the view):

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | magic `MEMT` |
| 4 | 2 | `version` u16 (1) |
| 6 | 1 | `flags` u8: bit 0 frozen |
| 7 | 1 | reserved |
| 8 | 4 | `head` u32: head node (height 16) |
| 12 | 4 | `count` u32, atomic |
| 16 | 8 | `bytes` u64, atomic: arena bytes used |
| 24 | 8 | `max_seqno` u64, atomic |
| 32 | 8 | `min_seqno` u64, atomic |
| 40 | 24 | reserved |

Skiplist node (4-byte aligned; maximum height 16):

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | `key_len` u32 |
| 4 | 4 | `value_len` u32 |
| 8 | 1 | `height` u8 |
| 9 | 3 | reserved |
| 12 | 4 x height | `next[i]` u32, atomic |
| 12 + 4 x height | `key_len` | internal key (§2) |
| ... | `value_len` | stored value (§3) |

The writer fully writes a node, then links it bottom-up with Release stores to each predecessor's `next[i]`; readers load `next[i]` with Acquire. Nodes are never modified after linking and never freed while any view listing their memtable is pinned.

## 12. Evolution

- A reader rejects a `FormatVersion` above what it supports and refuses a `ShmLayoutVersion` that differs at all.
- **Version 2** adds blob files (§7): separated values, `PutBlobFile`/`DropBlobFile` edits in use, and the `Blob` value tag in SSTs. A version 1 build (0.1.0) would read a blob pointer as an empty value, so every structure is now written with version 2 and a version 1 build refuses the file (version 2 also covers zstd blocks, codec 2 in §4.1, which a version 1 build cannot decode). This build still reads version 1 files; a file it writes to (any commit rewrites the superblock) is version 2 from then on.
- Manifest edits and the properties block are length-delimited, so fields and edit tags can be added without breaking older readers within the same major format.
- Block, filter, WAL record, value tag and key kind numbers are frozen; new variants take new numbers.
- Golden files for every structure are frozen at 1.0 (format brief).

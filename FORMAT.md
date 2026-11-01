# FORMAT

Every on-disk and shared-memory byte layout. Written during the interface freeze (build step 2). The executable form is the `pigeonhole-format` crate; where this file and the crate disagree, this file wins and the crate has a bug.

Format version **4**, shared-memory layout version **6**. Format 4 is the format every 1.x build reads and writes (D269): later 1.x features use the feature bits and reserved room described here, never a new version. §13 states the compatibility promise, which takes effect with 1.0.0. The shared-memory layout (§11) is outside it.

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

- **Versions.** `FormatVersion` (u32, currently 4; see §12) is stored in the superblock, every manifest block, every SST footer, every blob extent header and every WAL segment header. `ShmLayoutVersion` (u32, currently 4) is stored in the shared-memory header and must match exactly. Blocks, filters and WAL records carry kind/tag bytes whose numbering is frozen; new kinds take new numbers.

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
| `Vector` | `0x04` | a vector (§3.7; only in a vector family, D303) |
| `Blob` | `0x80` | 16-byte blob pointer |

The largest value is `2^32 - 1` bytes (decision D16).

**Blob pointer** (16 bytes):

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | `blob_file` u32: logical blob file id |
| 4 | 4 | `len` u32: length of the stored value the pointer replaced (tag byte included) |
| 8 | 8 | `offset` u64: logical offset of the blob record within the blob file (§7) |

### 3.1 HyperLogLog sketch

The value of an HLL family's cell (§9.2 `kind` 4, D294) is a `Bytes` value holding a sketch: 4096 registers (precision 12, about 1.6% standard error). Inputs are `i64` or bytes elements. An `i64` element's hash is xxh3-64 with seed 0 over its 8 little-endian bytes. A bytes element's hash (D298) is xxh3-64 over the domain tag `0x01` followed by its bytes, so the two forms never collide. The hash's top 12 bits pick the register, and the rank is one plus the number of leading zeros of the remaining 52 bits, at most 53. A register keeps the largest rank merged into it, so merging is the register-wise maximum: associative, commutative and idempotent.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | encoding: 1 (anything else is malformed) |
| 1 | 1 | precision: 12 |
| 2 | 1 | representation: 0 sparse, 1 dense |
| 3 | 1 | reserved: 0 |
| 4 | 4·n | sparse: `n` (1..=767) `u32` LE entries `index << 8 \| rank`, strictly increasing by index, rank 1..=53 |
| 4 | 3072 | dense: 4096 six-bit registers packed little-endian, register `i` in bits `6i .. 6i+6` |

- **Canonical:** a sketch is sparse if and only if at most 767 registers are nonzero, so equal registers have equal bytes. A dense sketch with 767 or fewer nonzero registers is malformed. The largest sketch is 3076 bytes; an HLL family's `blob_threshold` is at least that, so sketches are never separated.
- **Estimate:** Ertl's improved estimator (2017, algorithm 6) over the register histogram `C`, with `m = 4096` and `q = 52`: `z = m·τ(1 − C[q+1]/m)`, then `z = (z + C[k])/2` for `k = q` down to 1, then `z += m·σ(C[0]/m)`, and the estimate is `α∞·m²/z` rounded, with `α∞ = 1/(2 ln 2)` taken as the `f64` bit pattern `0x3fe71547652b82fe`. `σ` and `τ` are Ertl's series, iterated until they stop changing; they use only multiplication, halving, addition and `√`, so an estimate is the same on every platform. No register set: no sketch (the cell is absent).
- **Merge:** registers merge in place; a sparse sketch turns dense when the union passes 767 registers, and never back.

### 3.2 Exact f64 sum

The value of an exact-sum family's cell (§9.2 `kind` 10, D298) is a `Bytes` value holding a sum state. Every finite `f64` is `±m·2^e` with an integer `m < 2^53`, so it is an integer number of 2^-1074 units, fewer than 2^2098 of them. The state holds Σx in those units, exactly, as a 2176-bit two's-complement integer (34 `u64` limbs, room for 2^64 inputs). Adding an input is a shifted add of its significand; merging two states adds their integers. Both are exact, so a state does not depend on the order or grouping of its inputs.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | encoding: 2 |
| 1 | 1 | `n`: limbs that follow (0..=34) |
| 2 | 8·n | Σx, `u64` LE limbs, least significant first |

**Canonical form** (two's complement, sign-extended; perf287's N3 on #1220):
- While `n ≥ 2`, the top limb is dropped if it is 0 and the limb below it has bit 63 clear, or if it is `u64::MAX` and the limb below it has bit 63 set.
- A lone limb (`n = 1`) is dropped only if it is 0, so zero is `n = 0` and −1 is the lone limb `u64::MAX`.
- For example, +2^63 is the two limbs `[2^63, 0]`, and −2^63 the one limb `[2^63]`.
- Reading a state sign-extends its top limb's bit 63 through the missing limbs, to 34 limbs. A state that is not canonical, or has more than 34 limbs, is malformed.

**Read:** the integer times 2^-1074, rounded once to the nearest `f64` (ties to even), or ±∞ past the range. Inputs that cancel exactly read +0.0. A state is at most 274 bytes. Inputs are finite: NaN and ±∞ are refused at write.

### 3.3 Mean and variance

The value of a mean-variance family's cell (§9.2 `kind` 11, D298) holds the inputs' count, Σx (as §3.2) and Σx². A square `m²·2^2e` is an integer number of 2^-2148 units below 2^4196, so Σx² is a 4288-bit two's-complement integer (67 limbs) in those units, never negative.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | encoding: 3 |
| 1 | varint | count, at least 1 |
| … | | Σx, encoded as §3.2's limbs (count byte, then at most 34 limbs) |
| … | | Σx², the same with at most 67 limbs |

**Read:** each figure is computed exactly and rounded once, to nearest with ties to even:
- `mean = Σx / n`;
- `variance = (n·Σx² − (Σx)²) / n²` (population);
- `sample variance = (n·Σx² − (Σx)²) / (n(n−1))`, for `n ≥ 2`.

A state is at most 821 bytes. The encoding bytes 2 and 3 keep these states apart from an HLL sketch (§3.1, encoding 1).

### 3.4 Histogram

The value of a histogram family's cell (§9.2 `kind` 12, D298) counts its inputs in the buckets the family's bounds give (§9.2.1 tag 1). `n` bounds `b0 < … < b(n-1)` make `n + 1` buckets, `(-∞, b0)`, `[b0, b1)`, …, `[b(n-1), +∞)`. An input's bucket is the number of bounds at or below it by IEEE `<=`, so `-0.0` and `0.0` share a bucket. NaN is refused and no bound is `-0.0`, so the comparison is total and deterministic (perf287's S1 on #1221). Infinities fall in the end buckets; NaN is refused at write.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | encoding: 4 |
| 1 | varint | bucket count `n + 1` (the family's) |
| … | varints | the counts, first bucket first, trailing zero counts dropped (so the last is nonzero) |

Merging adds the counts. A state with the wrong bucket count, a trailing zero, or no input is malformed.

### 3.5 Quantile sketch

The value of a quantile family's cell (§9.2 `kind` 13, D298) is a log-linear sketch of its finite inputs, with the family's mantissa bits `p` (1–12, default 7) and bucket limit (16–4096 per store, default 2048; §9.2.1 tags 1 and 2).
- **Index:** a positive `x` maps to `bits(x) >> (52 − p)`, its binary exponent and top `p` mantissa bits. A bucket's values then lie within a factor `1 + 2^-p`, with no logarithm computed.
- **Stores:** negative inputs map their magnitude into a second store, and zeros (either sign) are counted apart.
- **Collapse** (D298 revision 1's S3): when a store's indexes span more than the limit, set `t = max_index − limit + 1`; every bucket below `t` folds into bucket `t`, which is created if absent. `t` depends only on the store's largest index, which only grows, so collapsing after every merge gives what one collapse at the end would. Merges are associative and commutative. Indexes grow with magnitude, so a collapse folds the **smallest magnitudes** in both stores: in a signed distribution it coarsens the quantiles nearest zero, not the low tail.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | encoding: 5 |
| 1 | 1 | `p` (the family's) |
| 2 | varint | the zero count |
| … | | the positive store, then the negative store: each a bucket count `k` (varint), then `k` pairs (varints): the first bucket's index and count, then each later bucket's index gap (at least 1) and count (at least 1) |

**Malformed:** a state is malformed if any of these hold:
- its `p` is not the family's;
- a store is not collapsed;
- a count is zero;
- an index lies past the `f64` range (`≥ 2^(11 + p)`);
- it holds no input.

**Read:** the `q`-quantile is the midpoint of the bucket holding the input of rank `⌊q·(n−1)⌋`, in increasing order. The midpoint is the bucket's lower edge's bits with the bit below the kept ones set, which is exact. It is within 2^-p of the input while its store has not collapsed. A zero reads `0.0`, a negative bucket its midpoint negated.

### 3.6 Bloom filter

The value of a bloom family's cell (§9.2 `kind` 14, D298) is a filter of `m` bits (§9.2.1 tag 1: a power of two, 2^6 to 2^24) with `k` positions per element (tag 2: 1 to 16).
- **Hashing:** an element's hash `h` is the HLL family's (§3.1: `i64` or bytes). Its positions are `(h1 + i·h2) mod m` for `i` in `0..k`, with `h1` the hash's low 32 bits and `h2` its high 32 bits with bit 0 set (Kirsch–Mitzenmacher double hashing). They are exact integers, so every platform sets the same bits.
- **Merge:** a bitwise OR, which is associative, commutative and idempotent.
- **Read:** a lookup never misses an element that was added. Its false-positive rate is about `(1 − e^(−kn/m))^k` for `n` elements.
- **Inputs:** an input becomes the one-element filter of its element before the WAL.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | encoding: 6 |
| 1 | 1 | `log2 m` (the family's) |
| 2 | 1 | `k` (the family's) |
| 3 | 1 | representation: 0 sparse, 1 dense |
| 4 | 4·n | sparse: the `n` set bits, `u32` LE, strictly increasing |
| 4 | m/8 | dense: the bitmap, bit `j` in byte `j / 8` at bit `j % 8` |

**Canonical form:** sparse if and only if `4·n < m/8` (the smaller form). A state that holds no bit, is dense below that threshold, has unsorted or out-of-range bits, or carries other parameters than the family's is malformed. A filter only gains bits, so it turns dense once and never back.

### 3.7 Vector

The value of a vector family's cell (§9.2 `kind` 16, D303) is a vector of the family's element type and dimension (§9.2.1 tags 1 and 2). Tag `0x04` is followed by:

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | element: 1 `f32`, 2 `i8`, 3 bit, 4 `f16`, 5 `bf16` |
| 1 | varint | `dim`: 1 to 4096, a multiple of 8 for bit |
| 1 + varint | payload | `dim` × 4 bytes of IEEE-754 `f32` LE, `dim` bytes of `i8`, `dim / 8` bytes of bits (bit `j` in byte `j / 8` at bit `j % 8`), or `dim` × 2 bytes of IEEE binary16 (`f16`) or bfloat16 (`bf16`) bit patterns LE |

There is no alignment padding. A payload whose length does not match the element and `dim`, a `dim` out of range, an overlong varint, or an unknown element is `Corrupt` (as in `kind_params`: a new element would take a new `incompat` bit). A writer refuses an element or dimension other than the family's, an `f32` component that isn't finite or exceeds 2^56 in magnitude, and in a cosine family a vector with a zero norm (all zeros, or an `f32` squared norm below `f32::MIN_POSITIVE`), so every distance is finite. Half-precision components (#1263) are checked as they widen to `f32`: finite (an `f16`'s largest, 65,504, needs no other bound; a `bf16` keeps `f32`'s 2^56 bound, having its range), and under cosine a squared norm, summed over the widened components, of at least `f32::MIN_POSITIVE`. An `f32` vector written to a half-precision family is rounded to nearest, ties to even; a component that overflows the element is refused, and the rounded vector is checked as stored. Distances widen each stored half component exactly to `f32` in the `f32` kernels' order, so a half-precision vector scores exactly as its widened `f32` vector. The largest value is 16 KiB plus 4 bytes (tag included).

**Storage:** the values of the kinds listed by `FamilyKind::values_stay_inline` (every aggregate kind so far, and Vector) are never separated into blob files, at flush, compaction or commit (D298, D303): aggregate operators fold stored values, never blob pointers, and a reader takes a vector in place, so a family's `blob_threshold` does not apply to them. #488 PR 5 takes Bloom out of the list once its operator folds a separated base. A whole state larger than one WAL record (a dense bloom filter of 2^21 bits or more) cannot be put in a commit until then.

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
| 84 | 4 | `flags` u32: the SST's feature bits (format 4; none defined, so 0). A reader refuses an SST with a bit it does not know (`UnsupportedFeature`) |
| 88 | 8 | `checksum` u64: xxh3-64 of bytes 0..88 |
| 96 | 8 | magic `PHDBSST\x01` |

### 5.2 Properties block (kind Properties)

Fields in order, fixed-width little-endian unless marked *bytes* (varint length + bytes):

`table` u32, `family` u32, `tablet` u64, `entries` u64, `rows` u64, `deletes` u64, `merges` u64, `raw_key_bytes` u64, `raw_value_bytes` u64, `data_blocks` u32, `index_partitions` u32, `min_seqno` u64, `max_seqno` u64, `min_ts` u64, `max_ts` u64, `created_micros` u64, `smallest_key` *bytes*, `largest_key` *bytes*, `merge_operator` *bytes* (UTF-8, empty if none), `purge_flags` u32, `purged_max_versions` u32.

`purge_flags` bit 0 (**fully purged**, D268): a bottommost compaction wrote the SST with nothing holding its purge back (no live snapshot, nothing above its inputs, and for a counter family every other source listed), so a rewrite would drop only what expires afterwards. `purged_max_versions` is the family's `max_versions` at the time (0: every version kept); a later policy keeping fewer versions voids the flag. Other bits are reserved and ignored. A block written before these two fields (format 3 before D268) ends after `merge_operator`, and reads as not fully purged.

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

Blob record at a logical offset: `len` u64, `checksum` u64 (xxh3-64 of the value), then `len` bytes of value. The value is the stored value (§3) that the pointer replaced, tag byte included, so a reader returns the record as the stored value. The pointer's `len` must equal the record's `len`. Because offsets are logical, a value may span several extents; this is how values larger than one 64 MiB extent can be stored. A put whose value is larger than the inline limit, `min(WAL segment payload, 64 MiB, half the shard's memtable arena)` (decision D16), is separated when its commit is routed: the value goes to a new blob file, committed in the manifest before the commit's WAL record is written, and the WAL record and memtable entry hold the pointer. A stored value can be up to `2^32 - 1` bytes (the pointer's `len`). A merge operand above the inline limit is rejected with `ValueTooLarge`. At open, after WAL replay, a blob file that no SST's references (`SstBlobRefs`), no SST of its family without references, and no recovered memtable entry points into is dropped: only a commit-time separation whose commit never became durable leaves one.

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

The first 128 bytes of the page, then the extension area (bytes 128..4088; format 4); the rest of the page is zero.

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
| 96 | 8 | `compat_features` u64 (format 4; reserved before) |
| 104 | 8 | `ro_compat_features` u64 (format 4; reserved before) |
| 112 | 8 | `incompat_features` u64 (format 4; reserved before) |
| 120 | 8 | `checksum` u64: xxh3-64 of bytes 0..120 |

**Feature bits** (format 4, D269). A build that starts using a feature sets its bit in both pages before it writes anything that uses it; a build that stops using an `incompat` or `ro_compat` feature may clear the bit in both pages the same way (a bit cleared in one page only leaves the other page's stale bit to refuse the file), and a feature may instead keep its bit for good. Defined bits:

| Set | Bit | Feature |
|---|---|---|
| `incompat` | 0 | Aggregate families of kinds Min, Max and Hll (D294; §9.2 `kind` 2, 3 and 4), and of the kinds #488 adds (D298; `kind` 5 to 14 so far, and a family's `kind_params`, §9.2.1), and vector families with value tag `0x04` (D303; `kind` 16, §3.7). Set, in both pages, before the manifest commit that writes the first such family; never cleared, even once no such family remains (sticky). |

A reader handles a bit it does not know by its set: an unknown `incompat` bit refuses the file (`UnsupportedFeature`), in either page, like a newer version; an unknown `ro_compat` bit allows a read-only open only (a reader process, or a writer with `read_only_fallback`, ICR 0059, which then writes nothing to any of the database's files); an unknown `compat` bit is ignored and kept by every commit. A file written before format 4 has zero in all three.

**Extension area** (format 4, D269). Bytes 128..4088 are reserved for later 1.x features and are zero until one uses them. Every format 4 build copies them unchanged from the current page into each root commit. The superblock checksum does not cover them: a feature that stores data there brings its own checksum. A file written before format 4 has zeros there.

**What a compat feature may rely on.** A build that does not know a `compat` feature keeps its bit and the extension area, but it commits new roots, and it drops manifest edits with skippable tags (§9.3) at its next manifest snapshot. So a compat feature's data must stay correct, or be detectably stale, after such a build commits; data tied to the root (in the extension area or in skippable edits) goes stale with the next root, and the feature must detect that, for example by recording the `sequence` or `manifest_version` it was written at. A feature that cannot tolerate this is `ro_compat`, so builds that do not know it cannot write the file.

A superblock is valid if magic, checksum and version check out. The valid one with the higher `sequence` is current. **Root commit:** sync all data written since the last commit, write the new superblock (sequence + 1) over the *non-current* slot, sync. This is the only in-place write of live data in the main file. Reader processes re-read both superblocks to pick up a new root; they never write.

### 8.3 Lock page (page 2)

Byte-range locks on single bytes at absolute file offsets (decision D3; refined by D21):

| Offset | Byte | Held |
|---|---|---|
| 8192 | writer | exclusive by the one writer; a second writer fails with `WriterLocked` |
| 8193 | presence | shared by every process with the database open; a process that can upgrade it to exclusive at close is the last one. A process that cannot releases its shared lock and tries the exclusive lock a few more times (1, 2, 4 and 8 ms later), so two processes closing at once do not both conclude another is still open (#150 item 4.7). Both may conclude they are last; the cleanup that follows is idempotent |
| 8194 | shm-init | exclusive while a process creates, validates or rebuilds the shared-memory region |
| 2^62 + i | reader slot *i* | exclusive by the reader process that owns shared-memory reader slot *i* (§11.5), from before it claims the slot until after it frees it (layout version 3, #693). Far beyond any file's end, so it never covers data (Windows' locks are mandatory) |

Locks are held per open handle (OFD locks on Linux, a per-handle registry over `fcntl` on macOS and BSD, `LockFileEx` on Windows), so two handles in one process conflict like two processes. Two limits:
- **macOS and BSD:** `fcntl` locks belong to the process, and closing *any* descriptor of the file drops all of them. The registry defers this crate's own closes while a lock is held, but a `close` of the file by other code in the process drops the writer, presence and reader slot locks (a reader's slot is then freed under it).
- **Linux:** a forked child shares the parent's open file description and so its OFD locks. A reader slot handle does nothing in a process other than the one that claimed it (§11.5), and a reader refuses snapshots and slot claims there, so a child never releases the parent's lock or takes a slot through the shared description; a child that keeps the descriptor keeps the lock held after the parent exits. The process is told apart by a cached id that a `pthread_atfork` child handler resets, so only `fork(3)` is covered: `_Fork`, `vfork` and a raw `clone` skip the handlers.

Network filesystems are refused at open: their lock managers don't give these guarantees.

## 9. Manifest

The manifest is a **snapshot block** plus a **delta log** (decision D7). The snapshot block, in its own extent, holds every edit needed to rebuild the state from empty. The delta log is one 256 KiB extent (size class 2) holding consecutive **delta blocks**, one per manifest commit, with versions `snapshot version + 1, + 2, ...`. The superblock names the snapshot, the log and the log's live length.

- **Commit:** append the new delta block at `log_len` (bytes past `log_len` are not live, so nothing live is overwritten), then commit a root with the larger `log_len` and the new version. The root commit's first sync covers the delta.
- **Compaction of the manifest:** when the delta would not fit in the log extent, or the live log exceeds the snapshot's length, the writer writes a new snapshot block and a fresh, empty log (new extents) and commits that root. The old snapshot and log extents are retired at that version. A shrink also rewrites them to move them toward the start of the file, but only into free space below them: where there is none, its delta goes into the log even past the snapshot's length (a reader bounds the log only by its extent).
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

Each edit is `tag: u8, body_len: varint, body`. The length prefix lets a reader skip tags it does not know, where §9.3's tag ranges allow it. Field types: fixed-width LE integers; *bytes* = varint length + bytes; *string* = *bytes* of UTF-8; *opt-bytes* = `u8` (0 absent, 1 present) then *bytes* if present; *extent* = `page` u64 + `size_class` u8.

*FamilyOptions* = `compression` u8, `compression_level` i8, `block_size` u32, `bloom_bits` u8, `max_versions` u32, `ttl_micros` u64, `blob_threshold` u32, `merge_operator` string, `cache_priority` u8 (0 Low, 1 Normal, 2 High), `compaction` u8 (0 Leveled, 1 Tiered, 2 FifoByTime), `kind` u8 (0 Standard, 1 Counter, decision D179; 2 Min, 3 Max, 4 Hll, decision D294; 5 BitOr, 6 BitAnd, 7 BitXor, 8 MinF64, 9 MaxF64, 10 SumF64, 11 MeanVar, 12 Histogram, 13 Quantiles, 14 Bloom, decision D298; 16 Vector, decision D303), then `kind_params` *bytes*, written only when the family has parameters or a later appended field needs its slot (§9.2.1). Every kind but Standard and Vector is an aggregate family, whose `merge_operator` is fixed by the kind: `pigeonhole.i64_add` (Counter), `pigeonhole.i64_min` (Min), `pigeonhole.i64_max` (Max), `pigeonhole.hll` (Hll, its values sketches, §3.1), `pigeonhole.u64_or`, `pigeonhole.u64_and`, `pigeonhole.u64_xor` (BitOr, BitAnd, BitXor: `I64` values read as the `u64` with the same bits), `pigeonhole.f64_min`, `pigeonhole.f64_max` (MinF64, MaxF64: `F64` values, never NaN, ordered by IEEE 754 `totalOrder`, so `-0.0 < +0.0`), `pigeonhole.f64_sum`, `pigeonhole.f64_mean_var` (SumF64, MeanVar: finite `F64` inputs, values the states of §3.2 and §3.3), `pigeonhole.f64_histogram`, `pigeonhole.f64_quantiles` (Histogram, Quantiles: `F64` inputs, values the states of §3.4 and §3.5, built from the family's `kind_params`), `pigeonhole.bloom` (Bloom: `Bytes` filter states of §3.6, from its `kind_params`); another name is `Corrupt`. A Vector family's cells version and delete as a Standard family's; its values are the vectors of §3.7. Kinds 2 to 14 and 16 need `incompat` feature bit 0 (§8.2): a family of such a kind in a file whose superblock lacks the bit is `Corrupt`, and a build that does not know the bit refuses the file at the superblock, before any `PutFamily` is decoded. `kind` was appended after the 0.1.0 release: a body that ends before it (a file 0.1.0 wrote) reads as Standard, and an unknown value is `UnsupportedFeature` (ICR 0058; reached only if a file lacks the bit its kind needs). It relies on format version 2 (D181): a 0.1.0 reader ignores trailing body bytes, so without the bump it would open a file with counter families, read them as Standard and fold their fixed-timestamp increments wrongly; version 2 makes it refuse the file.

#### 9.2.1 Kind parameters

`kind_params` (D298) holds a kind's parameters as a list of entries, each `tag` u8, `len` varint, then `len` bytes. The list is canonical: tags strictly increasing (each at most once), tag 0 never written, every length within the field. A list that breaks any of these is `Corrupt`.

**No parameters** is the empty list, and it has two encodings, each used in one case:
- **Absent:** when nothing follows the field. The body ends at `kind`, as before the field existed.
- **A present field of length 0:** only when a later appended `FamilyOptions` field needs this slot.

A reader takes both as no parameters, and ignores bytes after the field (later appended fields), so a field can still be appended after `kind_params` (perf287's S1 on #1213).

- Tags 1 to 127 are must-understand: a build whose kind doesn't define the tag refuses the family with `UnsupportedFeature`, the backstop behind the kind's feature bit.
- Tags 128 to 255 are skippable hints, ignored by a build that doesn't know them.
- A kind fixes its tags and their value encodings when it lands. A later must-understand tag on an existing kind takes a new `incompat` bit.

The tags defined so far (D298):
- **Histogram** (`kind` 12): tag 1, its bounds, as `n` `f64` LE (1 ≤ `n` ≤ 1023; finite, strictly increasing, never `-0.0`). Required.
- **Quantiles** (`kind` 13): tag 1, the mantissa bits `p` (u8, 1–12, default 7); tag 2, the bucket limit per store (u16 LE, 16–4096, default 2048). Both optional.
- **Bloom** (`kind` 14): tag 1, the bits `m` (u32 LE, a power of two, 2^6 to 2^24); tag 2, the positions per element `k` (u8, 1–16). Both required.
- **Vector** (`kind` 16, D303): tag 1, the element (u8: 1 `f32`, 2 `i8`, 3 bit, 4 `f16`, 5 `bf16`, the last two from #1263); tag 2, the dimension (u32 LE, 1–4096, a multiple of 8 for bit); tag 3, the metric (u8: 1 L2, 2 inner product, 3 cosine, 4 Hamming). All three required. L2, inner product and cosine take `f32`, `i8`, `f16` or `bf16`; Hamming takes bit, and bit takes only Hamming.

Values out of range are `Corrupt` at read. A writer refuses them, and any tag the family's kind doesn't define.

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
| 13 | `SstBlobRefs` | `sst` u64, `count` u32, `count` x (`blob_file` u32, `bytes` u64): the blob files the SST's puts point into, ascending, and the bytes (`16 + len` per pointer, §7) each references |

**Tag ranges** (format 4, D269). Tags 1..=127 are *must-understand*: a reader that meets one it does not know fails with `UnsupportedFeature` rather than skip an edit that changes what the manifest means. Tags 128..=255 are *skippable*: a reader that does not know one skips it, and as a writer it drops the edit at its next manifest snapshot (§8.2 says what a compat feature may rely on). Tag 0 is never written; reading it is `Corrupt`. Tag 13 predates the ranges; every build since knows it, and older builds skip it.

Rules: family ids are unique across the database, so `(tablet, family)` names one tree. After a split both child tablets may reference the same SST; an SST's extent is retired when no tablet references it. A snapshot block contains `Counters`, every live table, family, tablet, SST, flushed seqno, blob file, SST blob references and stream checkpoint. Every commit that adds a new SST (a flush, compaction, backup or shrink copy) adds its `SstBlobRefs`, with an empty list if it holds no pointer; an SST without one (written before tag 13 existed) may point into any blob file of its family. References of an SST no tablet references are dropped from the next snapshot. A build that does not know tag 13 skips it. The live extents at open are: the snapshot and log extents, every referenced SST extent, and every blob-file extent. `ts_floor` is at least every default timestamp assigned before the commit (decision D11).

## 10. WAL

### 10.1 Files and segments

Stream `N` of database `data.phdb` is the file `data.phdb-wal-N` (decimal `N`). Stream numbers are independent of shard numbers. A stream file is a sequence of equal-size **slots** (default 64 MiB, a multiple of 32 KiB, at most 4 GiB − 32 KiB so that `prev_end` always fits a `u32`; decision D43) at offsets `k x segment_size`. Slots are preallocated and recycled after checkpoint, never deleted while the database is open. Each use of a slot is a **segment** with its own **epoch** (u32), above the largest epoch in any segment header of the stream: one more at a rollover (rule 1), three more when a stream reopens after recovery (rule 2). So epochs never repeat, including the epochs of successors whose headers never reached the disk (decision D209).

An **LSN** is `(epoch << 32) | offset_within_segment`. LSNs increase monotonically within a stream. The manifest records each stream's checkpoint LSN. **No checkpoint may name a stream whose file creation is not durable** (its file and directory syncs finished; decision D211): otherwise a power loss could take the file's name and leave the checkpoint to a stream the next open creates afresh, past that stream's first records.

**Chaining** (decision D25). Each segment header names its predecessor: `prev_epoch` and `prev_end`, the offset where the predecessor's valid data ends. Rules for the writer:
1. When a segment fills, write nothing more to it, **sync it**, and only then write the next segment's header with `prev_end` = the offset where the full segment's last record ends (its stop offset, §10.2; the zero tail after it is not part of `prev_end`). The sync may be submitted to the I/O backend; the header write waits for it to complete (decisions D30, D200).

   The successor's **records** (its frames after the header frame) may be written before its header, while that sync is in flight. Until the header is written, the slot still holds a stale header: zeros (a blank or prepared slot), or the header of an older segment that used the slot (a recycled slot, whose epoch is below the checkpoint's, so at least two below the new segment's). Neither can be chained to the full segment (a stale header's `prev_epoch` is below the full segment's epoch), and replay of a segment stops at the first fragment of another epoch (§10.2), so the early frames are never read. A writer acknowledges none of them before the header is written: their bytes count as handed to the kernel only from then. If the header never reaches the disk (a crash in this window, or a power loss that keeps the frames of a header-and-records write but not its header), the frames stay in the slot under the epoch the header would have had. At most two headers are ever not durable at once, because a header is written only after its predecessor is durable: the full segment's own header (written once its predecessor's sync completed, durable only with this sync) and the held one (written only once this sync completes). A power loss that reorders unsynced writes can drop both and keep the held segment's frames, two epochs above the largest header on disk. Rule 2 keeps any later segment from taking such an epoch (D209).
2. After recovery, **never append to the last replayed segment**: start a new segment (epoch = max seen + 3, where max seen covers every segment header and the replayed end) with `prev_epoch`/`prev_end` = where replay ended. Max seen + 1 and + 2 are skipped: they may be the epochs of successors whose records lie, without their headers, in the recycled slot the new segment takes (rule 1), which would then replay as the new segment's (decision D209, #508). Files written before D209 used max seen + 1 and read the same way: chaining follows `prev_epoch`, never consecutive epochs.

Replay starts at the segment whose epoch is the checkpoint's, at the checkpoint's offset, and reads until the segment's data stops (§10.2). It then looks for the segment whose header has `prev_epoch` = this epoch:
- none: **end of log**; the stop point is the torn tail.
- one, with `prev_end` = the stop offset: **end of segment**; continue there.
- one, with a different `prev_end`: synced data is missing; recovery fails with a corruption error instead of dropping it.

### 10.2 Frames

A segment is made of 32 KiB **frames**. Frame 0 holds the segment header; the rest of frame 0 is zero.

Segment header (offset 0 of frame 0). From format 4 it is 128 bytes; a segment written before format 4 has the 60-byte header (`crc` at 56, over bytes 0..56) and is still read. The `format_version` field picks the layout. A header whose version is above this build's is refused (never skipped as torn) if the CRC of either layout passes. That rule only recognises a newer header that keeps its CRC at 56 or 124; the real gate is the superblock: both pages carry a newer version or `incompat` bit before a newer build writes any WAL segment (§12), so a later format may lay its header out freely:

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
| 56 | 4 | `features` u32: the segment's feature bits (none defined, so 0). A reader refuses a segment with a bit it does not know (`UnsupportedFeature`) |
| 60 | 16 | reserved for a per-database salt (#176), zero |
| 76 | 48 | reserved |
| 124 | 4 | `crc` u32: CRC32C of bytes 0..124 |

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
| Decision | `4` | `seqno` u64, `apply_seqno` u64, `commit_ts` u64, `count` u16, `count` x `participant_stream` u32 |

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

**Cross-shard commits** (decision D263, ICR 0046). A commit's **id** is a seqno its coordinator reserves once, at the start (`NEXT_SEQNO`, §11.3), and never applies: nothing is ever written at it, so it is never held, and it is unique for the life of the database (D26). Each participant writes a Prepare carrying the id in `seqno` and its share of the mutations; the Prepare's `commit_ts` is 0, since the timestamp is not picked yet, and every mutation without an explicit timestamp takes the Decision's. Once every Prepare meets the requested durability, the coordinator picks the commit timestamp above every participant's floor, reserves the seqno the commit **applies at** (held from then until every participant applied, §11.3), and writes a Decision to its own stream: the id in `seqno`, then `apply_seqno` and `commit_ts`. At recovery, a Prepare is applied if and only if its coordinator's stream holds a Decision (or a Commit, below) with the same id, and it is applied at the Decision's `apply_seqno` and `commit_ts`. Recovery sets `next_seqno` above every seqno in every replayed record, **including discarded Prepares and every Decision's `apply_seqno`**, so no seqno is ever reused. A stream's checkpoint never advances past a Decision or Commit record while any of its participants' Prepares may still need replay (decision D24).

A **Commit** (type 3) is the decision as format version 2 (0.2.x, 0.3.x) wrote it: its Prepares carry the commit's own seqno and timestamp, and are applied at those. This build replays it; it writes only Decisions.

A `Durability::None` commit's record is appended to the shard's in-memory stream buffer with no I/O of its own; the next `write()` or fsync, triggered by a stronger commit on the same stream, carries it to the file. So a stronger commit also makes every earlier `None` commit of its stream durable (the mixed-levels rule), and a `None` commit that no stronger commit follows survives nothing past the last flush.

## 11. Shared-memory region

**Not part of the compatibility promise** (§13, D269 D9). The layout may change in any release, and `ShmLayoutVersion` must match exactly: stop reader processes before upgrading the writer.

Two objects (decision D27), placed at `/dev/shm/<name>` on Linux, POSIX `shm_open("/<name>")` on macOS and BSD, a `Local\<name>` pagefile-backed mapping on Windows, or `<shm_dir>/<name>.phdb-shm` when `shm_dir` is set:

- **Directory** `phdb-<h>`, where `<h>` is 16 lowercase hex digits of `xxh3_64(device LE ++ inode LE)` of the main file. One page whose layout never changes: magic `PHDBSHMD` @0, `format` u32 @8 (=1), `generation` u64 atomic @16 (current region, 0 = none), `layout_version` u32 atomic @24.
- **Region** `phdb-<h>-<generation in hex>` (at most 30 bytes: macOS's `shm_open` limit is 31 including the leading `/`; decision D44), laid out below.

A writer always builds a new generation: create the new region, store `state = abandoned` in the old one, then store the new generation in the directory. Because the name changes with the generation, a mapping some process still holds (Windows keeps named mappings alive while any handle is open) is never reused. Readers that see `state = abandoned` or a different directory generation re-attach. The header also stores the device, inode and `db_id`, which an attaching process verifies.

```text
[header 4096][watermarks 64 x shards][view buffer 0][view buffer 1][reader slots 64 x n][row epochs 8 x slots][pad to 2 MiB][arena 0]...[arena shards-1]
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
| 160 | 8 | `row_epochs_off` u64 (64-aligned; layout version 5, §11.7) |
| 168 | 4 | `row_epoch_slots` u32, a nonzero power of two |
| 172 | 4 | `row_epoch_hash` u32: which hash picks a row's slot; 1 = xxh3 (§11.7), any other value refused at attach |
| 176 | 8 | `row_epoch_seed` u64: the slot hash's seed, random per generation |
| 184 | 4 | `row_epoch_flags` u32: bit 0, the writer raises the slots (its row cache is on) |
| 188 | 4 | padding, so `cross_held` is 8-aligned |
| 192 | 8 | `cross_held` u64, atomic (layout version 6, D278): the lowest decided cross-shard seqno not yet applied on every participant, `u64::MAX` when none (§11.3) |
| 200 | 3896 | reserved |

### 11.2 Watermarks

One 64-byte line per shard at `watermarks_off + 64 x shard`. The rest of the line is padding (no false sharing).
- **Offset 0: `pending`,** u64, atomic. The lowest seqno this shard holds unapplied, or `u64::MAX` when idle (an idle shard never holds back snapshots).
- **Offset 8: `durable`,** u64, atomic (layout version 2, D227). The lowest seqno this shard holds that is not yet durable at its commit's level: every applied group whose sync has not returned (its cross-shard seqnos too, before layout version 6; since then `cross_held` holds them, §11.3). `u64::MAX` when idle. A shard stores `durable` before `pending` whenever both change.

### 11.3 Seqno reservation and snapshot protocol

A group's seqnos are held back by its shard's `pending`; a cross-shard commit's decided seqno (the seqno a Decision applies at, §10.3) by the header's `cross_held` (layout version 6, D278). A commit's id is reserved like any seqno and never held, since nothing is written at it.

1. Under the writer process's one allocator lock (D271): `first` = the allocator's next seqno; `pending[shard].store(min(pending, first))` (Release), the group's hold, where `pending` is the shard's lowest unapplied seqno before this group (the value `pending[shard]` already holds, or `u64::MAX`, so the `min` never lowers it); then the `n` seqnos and the group's default timestamps are reserved.
2. After the lock, `next_seqno.fetch_max(first + n)` (AcqRel): `next_seqno` is a mirror of the allocator and never passes a reservation, and every `fetch_max` past `first`, by any thread, comes after the hold.
3. Apply the group; then store the next unapplied seqno, which is `u64::MAX` when nothing is pending.

The hold is stored under the lock, not as a bound read from `next_seqno` beforehand (D300, #1208). A bound loaded from the mirror and stored later could land below a seqno another shard reserved, applied and released in between, already visible, and the snapshot seqno would go back. `first`, read under the lock, is above every seqno any reader can see, so the store never lowers the snapshot seqno: snapshots never go back.

A cross-shard commit's decision reserves `n = 1` the same way, but holds it in the header rather than its shard's `pending`. The allocator keeps the set of decided seqnos not yet applied on every participant, and under its lock it adds the new seqno to the set and stores `cross_held = min(set)` (Release), before the `fetch_max` of step 2. Once every participant has applied its share, the one that applied last removes the seqno under the lock and stores `cross_held` again (`u64::MAX` when the set is empty). A participant that cannot apply never removes it: the seqno stays held for good, and nothing of the commit becomes visible.

Snapshot (writer threads and reader processes alike): `n = next_seqno.load()` (Acquire) **first**, then `c = cross_held.load()` (Acquire), then `m = min over shards of pending.load()` (Acquire); the snapshot seqno is `min(n, c, m) - 1`. A *durable* snapshot (D227) loads each shard's `pending` and then its `durable` (Acquire both), and takes `min(n, c, every pending, every durable) - 1`. A shard stores `durable` first, so a reader that sees a raised `pending` also sees the `durable` covering what is applied but not yet synced.

The order matters. Every seqno below `n` was reserved by a shard that stored its hold before the mirror passed it (step 1), or is a decision whose hold was stored the same way. So it is either applied, or held at or below it in `pending` or `cross_held`. A reader that loaded `cross_held` before `next_seqno` could see no hold, then a mirror past a decision made in between, and include the held seqno (perf287's M1 on D278; the engine's `clock_loom` model checks both orders).

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

Claim: take the slot's lock byte (§8.3, `2^62 + i`) exclusive, CAS `state` 0 to 1, write `pid`, `start_time`, `generation`, store `state` = 2; the reader holds the byte until after it frees the slot (clear the pins, CAS `state` 2 to 0), then releases it. A byte another handle holds means the slot is taken: try the next one. Pin: store `pinned_view = v` (SeqCst), store `pinned_seqno = s`, then re-load `view_pointer`; if the view version moved past `v`, take a fresh snapshot seqno `s' >= s` and pin again with the new version and `s'` (the seqno and the view always move as a pair, and the reader uses the pair it ended up pinning). **What a slot pins** (#693 PR 2): its seqno pin is its process's oldest live snapshot's seqno and its view pin that process's oldest live snapshot's view, each the minimum over the process's live snapshots; with none alive both are 0 (the slot pins nothing). A new snapshot pins as above when the process holds none, otherwise only lowers `pinned_seqno` (a durable snapshot, D227); a dropped one raises them (`fetch_max`) to the oldest left, or clears them. The writer frees a view's memtables, and the extents its manifest version uses, only after publishing a newer view and finding no slot pinning that view or an older one. **Liveness** (layout version 3, #693): the writer judges a slot by its lock byte, not by `pid`. It CASes `state` 2 (or finds it 1) to 1, tries the byte exclusive without waiting, and if it gets it (no process holds the slot) clears the pins, stores `state` = 0 and releases the byte; if the byte is held it puts `state` back as it was. A live reader is never judged dead, whatever its pid namespace, and a claimer that died between its CAS and its `state` = 2 store is reclaimed too. `pid` and `start_time` are diagnostic. Since a slot is freed only by a handle holding its byte, every store a reader makes to its slot is to its own slot. A claim skips a byte its own handle already holds (locks don't conflict within a handle, so locking it would succeed and the failed claim's unlock would release it), and a slot handle used in a process other than the one that claimed it (a forked child) stores nothing, frees nothing and unlocks nothing. Readers write nothing in the region except their own slot.

### 11.6 Arenas and memtables

Shard `i`'s arena is `arena_len` bytes at `arenas_off + i x arena_len`. All arena offsets are `u32` relative to the arena base; `0` is null and the first 64 bytes of every arena are reserved. How the writer carves the arena into chunks is private to the writer process (a new writer rebuilds the region).

Memtable header (64 bytes, at the `root` offset named in the view):

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | magic `MEMT` |
| 4 | 2 | `version` u16 (2) |
| 6 | 1 | `flags` u8: bit 0 frozen; bit 1 the writer records stale tails in this memtable's nodes (D277) |
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
| 12 | 4 | `tail` u32: a later node of the same column, or 0 (D194, D277) |
| 16 | 4 x height | `next[i]` u32, atomic |
| 16 + 4 x height | `key_len` | internal key (§2) |
| ... | `value_len` | stored value (§3) |

The writer fully writes a node, then links it bottom-up with Release stores to each predecessor's `next[i]`; readers load `next[i]` with Acquire. `tail` is written with the rest of the node, before the links (0 when it has none, also when the writer records no tails), and names a node linked before this one, so a reader that acquired this node's link sees the tail's target fully written too. A reader follows a tail only after its column is finished (D194). A reader process (any reader that opens the memtable from shared memory) validates it as a node whose key is in that column, and treats anything else as corruption; only the writer's own in-process reader, which wrote the tails, follows them unchecked. Nodes are never modified after linking and never freed while any view listing their memtable is pinned.

### 11.7 Row epochs

`row_epoch_slots` atomic `u64`s at `row_epochs_off`, between the reader slots and the arenas, zero when the region is built (layout version 5, D281). They are the write watermarks of every process's row cache (D201). The slot of a row is `xxh3_64_with_seed(row_epoch_seed + table, row bytes) & (row_epoch_slots − 1)`, the seed plus the table id wrapping in `u64` (`row_epoch_hash` = 1): specified bytes and algorithm, so every build of every process picks the same slot. A reader looking at a slot the writer never raises would serve a stale row for ever.

- **The writer**, when its row cache is on, sets `row_epoch_flags` bit 0 when it builds the region and raises the slot of every row a commit writes to the commit's seqno (`fetch_max`, Release) before the write is inserted into a memtable, so before it can be visible: every mutation kind (puts, deletes and markers, merge operands, counters, both shares of a cross-shard commit). Not during replay: the new generation's slots start at zero, and a reader reads the replayed state. With its row cache off it raises nothing and leaves the bit clear.
- **A reader process** keeps a row cache only when bit 0 is set.
- **A reader**, in any process, takes its read point `S` (a snapshot's seqno), then loads the slot `e` (Acquire). It stores a row read at `S` under epoch `e` only if `e ≤ S`, and serves a stored row only if its epoch equals the slot now and the slot is at most its `S`. Any later write to the row raises the slot past the entry's epoch, so the entry misses from then on. Rows sharing a slot cost misses, never each other's rows (a cache verifies the row and family).
- **Generations:** a reader's cache belongs to one region generation and is emptied when the reader re-attaches. A new generation's slots start at zero again, and a writer crash can lose visible `None` writes whose seqnos the next session reuses.
- **Validation:** an attaching process refuses a header whose table is not 64-aligned, whose slot count is not a nonzero power of two, whose table overlaps the reader slots or the arenas or leaves the region, or whose `row_epoch_hash` it doesn't know (`Corrupt`), so a validated slot index never leaves the table and every process hashes alike.

## 12. Evolution

- A reader rejects a `FormatVersion` above what it supports and refuses a `ShmLayoutVersion` that differs at all.
- **Version 4** is the format 1.x is frozen at (D269). It reserves room to grow without a version bump: feature bits in the superblock (§8.2: compat, ro_compat, incompat), in the SST footer's `flags` (§5.1) and in the WAL segment header, which grows to 128 bytes (§10.2); the superblock's extension area; and the manifest's must-understand and skippable tag ranges (§9.3). Feature data follows §8.2's rules for compat features. Nothing else changes, and no bit was defined at the freeze, so the upgrade from version 3 is the same as from version 2: a writable open commits the root twice, both pages carry version 4 before any WAL write, and new WAL segments get the 128-byte header while the older segments keep theirs until their slots are reused. A version 3 build (never released: main between 0.3.0 and format 4) or an older one refuses the file (`UnsupportedFormat(4)`). This build still reads versions 1 to 3.
- **Shared-memory layout version 4** gives every memtable node a `tail` field at offset 12 (the node header grows from 12 to 16 bytes, so the tower moves to offset 16) and raises the memtable header's `version` to 2, with flag bit 1 (D277). The writer stores there the stale tail D194 computes at insert: a later node of the same column, when the node was linked in front of an older version of its column. Reader processes then pass a finished column's superseded versions in one jump, as the writer does. The field is written on every insert, before the node is linked, so a reused chunk never shows an earlier memtable's tail; a reader validates a tail as a node in the same column before following it, and reports `Corrupt` otherwise. A version 3 process would misread every node's tower and key, so writer and readers must run the same build: a version 3 reader refuses a version 4 region, and a version 4 writer rebuilds a version 3 region left behind once no other process has the database open.
- **Shared-memory layout version 5** adds the row-epoch table (§11.7, D281): the write watermarks of every process's row cache, which the writer raises for every written row, so reader processes cache hot rows as the writer does. Header fields `row_epochs_off` @160, `row_epoch_slots` @168, `row_epoch_hash` @172 (1 = xxh3), `row_epoch_seed` @176 and `row_epoch_flags` @184 (bit 0: the writer raises the slots, set when its row cache is on); the table lies between the reader slots and the arenas. A version 4 writer would not raise the slots, so writer and readers must run the same build: a version 4 reader refuses a version 5 region, and a version 5 writer rebuilds a version 4 region left behind once no other process has the database open.
- **Shared-memory layout version 6** moves the hold on a decided cross-shard seqno from its coordinator's `pending` to the header's `cross_held` word at offset 192 (D278, §11.3). Whichever participant applies last then releases it, without a message back to the coordinator. A version 5 reader ignores the word and would see a cross-shard commit before every share of it is applied, so a version 5 reader refuses a version 6 region, and a version 6 writer rebuilds a version 5 region left behind once no other process has the database open.
- **Shared-memory layout version 3** judges reader liveness by each slot's lock byte (§8.3, §11.5, #693) instead of `pid` and `start_time`. The region's bytes are unchanged, but a version 2 reader holds no slot lock, so a version 3 writer would free its slot at once; hence the bump, and a version 2 process cannot attach to a version 3 region, nor the reverse.
- **Shared-memory layout version 2** adds each shard's `durable` watermark (§11.2, D227). A version 1 process cannot attach to a version 2 region, nor the reverse, so writer and reader processes must run builds with the same layout.
- **Version 3** adds the WAL's Decision record (§10.3, D263, ICR 0046). A version 2 build (0.2.x, 0.3.x) would fail on it as corruption, and would skip a version 3 segment header as torn, dropping every record from there on. So a writable open of an older file first commits its current root twice (`OpenedPager::finish`), and both superblock pages carry version 3 before this build writes any WAL record; a version 2 build then refuses the file (`UnsupportedFormat(3)`) instead of falling back to the page it can still decode. A crash between the two commits leaves one older page naming the same root, and the next writable open finishes the upgrade. This build still reads version 1 and 2 files and replays their Commit records.
- **Version 2** adds blob files (§7): separated values, `PutBlobFile`/`DropBlobFile` edits in use, and the `Blob` value tag in SSTs. A version 1 build (0.1.0) would read a blob pointer as an empty value, so every structure is now written with version 2 and a version 1 build refuses the file (version 2 also covers zstd blocks, codec 2 in §4.1, which a version 1 build cannot decode). This build still reads version 1 files; a file it writes to (any commit rewrites the superblock) is version 2 from then on.
- Manifest edits and the properties block are length-delimited, so fields and edit tags can be added without breaking older readers within the same major format.
- Block, filter, WAL record, value tag and key kind numbers are frozen; new variants take new numbers.
- Golden files for every structure are frozen at 1.0 (format brief).
- **Version 4 is the last 1.x version.** A later 1.x feature takes a feature bit (§8.2) and the room §13.1 names for it (§13.2 lists the paths for tracked features); 2.0 is format 5.

## 13. Compatibility promise

From 1.0.0 (D269):

- **Older files.** Every 1.x build opens every file written by 0.1.0 or later, including any older 1.x build's (`MIN_READABLE` stays 1). A writable open upgrades an older file to format 4 before it writes anything an older build could misread (both superblock pages first, §12), so the upgrade is one-way: the older build then refuses the file with `UnsupportedFormat`. Back up a file before opening it writable with a newer build if you may need to go back. A read-only open (a reader process, or `read_only_fallback`) never upgrades.
- **Newer files.** A 1.x build opens a file a newer 1.x build wrote, read-write, unless the file uses a feature it lacks:
  - an unknown `incompat` bit refuses the open with `UnsupportedFeature`, naming the bits;
  - an unknown `ro_compat` bit refuses a writable open with `UnsupportedFeature`; reader processes open it, and a writer opens it read-only with `read_only_fallback` (ICR 0059), writing nothing;
  - unknown `compat` bits are ignored and kept; §8.2 says what a compat feature's data may rely on.
- **Features are flagged only once used.** A bit is set in both superblock pages before the first structure that uses the feature is written, so a file that doesn't use a new feature stays readable and writable by every older 1.x build.
- **Unknown values are never corruption.** A value of a frozen enum that this build doesn't know, in bytes whose checksum passes, decodes to `UnsupportedFeature` (ICR 0058): key kind, value tag, compression, block, filter and manifest block kinds, family settings, WAL record and fragment types, must-understand edit tags (§9.3), SST and WAL segment feature bits. With the file-level bit set first, an older build normally refuses at open; the per-structure error is the backstop.
- **What isn't promised:** the shared-memory layout (§11), the API (semver covers it separately), and files written by 2.0 (format 5), which no 1.x build opens. 2.0 comes with upgrade tooling.
- **How it's checked:** golden files for every structure (`crates/format/tests/golden`, frozen at 1.0 by the append-only hash list `crates/format/tests/golden-frozen.txt`), a corpus of files written by the 1.0 release that every later build opens in CI, and, once 1.0.0 is published, a job that opens files from `main` with 1.0.0.

### 13.1 Where each structure has room

| Structure | Room for later 1.x features |
|---|---|
| Superblock (§8.2) | `compat`, `ro_compat`, `incompat` feature sets; the extension area, bytes 128..4088. The first feature that uses the area defines its record layout (D269 D6: TLV records with an xxh3 at 4088) behind an `incompat` bit |
| Manifest edits (§9.2, §9.3) | new must-understand tags 14..=127 behind an `incompat` bit; skippable tags 128..=255 for data an older writer may drop; fields appended to an edit body when a bit says they're there |
| SST (§5) | footer `flags` as per-SST feature bits; properties block fields appended (length-delimited); new block kinds |
| Values and keys (§2, §3) | new value tags (0x04.., 0x81..) and key kinds, each behind an `incompat` bit |
| Blocks (§4) | new compression codes (3..) and block kinds, behind an `incompat` bit |
| WAL (§10) | segment header `features` bits and reserved bytes (the salt slot at 60..76); new record types behind an `incompat` bit |
| Blob extents (§7) | reserved header bytes 20..56; new blob pointer tags behind a bit |

### 13.2 Paths for tracked features

How each feature tracked for after 1.0 fits format 4 (D269 §3). The bit numbers are assigned when a feature lands.

| Feature | Path |
|---|---|
| Min, Max and HLL families (#487) | `FamilyKind` 2, 3 and 4 behind `incompat` bit 0, the first real feature bit (D294, §8.2); an HLL value is a `Bytes` sketch with a fixed encoding (§3.1) |
| More aggregations (#488) | more `FamilyKind` values under `incompat` bit 0 (5 to 14: bitwise, `f64` min/max, exact sum, mean/variance, histograms, quantiles and bloom filters, D298), plus the optional `kind_params` field appended to `PutFamily` (§9.2.1) |
| Adjacency sets, roaring (#489) | a `FamilyKind` value and a value encoding, behind a bit |
| Vector values (#490) | a value tag (0x04) behind an `incompat` bit; payload alignment as a data-block kind (6) or an entry flag under the same bit; dimension and metric in `kind_params` |
| Time-series codec (#491) | a compression code (3..) or a block kind, behind an `incompat` bit |
| Content-addressed blob dedupe (#491) | a blob-pointer value tag (0x81) and a must-understand refcount edit, behind an `incompat` bit |
| Encryption at rest (#493) | an `incompat` bit; key ids in the superblock extension area; a per-block nonce and MAC as a trailer form under the bit; WAL segment feature bits |
| Zone maps (#493), per-block time ranges (#535) | a meta block kind addressed from an appended properties field: `compat` (derived data an older writer drops when it rewrites the SST) |
| Zero-copy clones and branches (#493) | shared extents as a must-understand edit, behind an `incompat` bit (free space is rebuilt from the manifest, D8) |
| Read replicas, WAL shipping (#493, #176) | the WAL framing frozen in format 4; the per-database salt slot in the segment header |
| Multi-process readers (#39, #397) | the shared-memory layout version (§11): outside the promise |

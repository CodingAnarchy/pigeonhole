//! Byte-for-byte checks written by hand from FORMAT.md, independent of the encoders (the
//! golden files only freeze what the encoders produce). Offsets here are the literal numbers
//! from the FORMAT tables, not the crate's constants.

use pigeonhole_format::blob::{BlobExtentHeader, encode_record_header};
use pigeonhole_format::block::{Block, BlockAddr, BlockBuilder, BlockKind, seal};
use pigeonhole_format::checksum::{crc32c, xxh3_64};
use pigeonhole_format::compress::Compression;
use pigeonhole_format::filter::FilterBuilder;
use pigeonhole_format::key::{Kind, encode_key, encode_marker_key, encode_seek_key};
use pigeonhole_format::manifest::{Edit, ManifestBlockKind, ManifestHeader, encode_block};
use pigeonhole_format::shm::{ShmHeader, ViewMemtable, ViewRecord, ViewTablet};
use pigeonhole_format::sst::Footer;
use pigeonhole_format::superblock::{ExtentRef, Superblock};
use pigeonhole_format::value::{BlobPointer, ValueRef, encode_value};
use pigeonhole_format::wal::{FRAME_SIZE, FrameEncoder, SegmentHeader};
use pigeonhole_format::{BlobFileId, FamilyId, FormatVersion, Lsn, StreamId, TableId, TabletId};

const FF7: [u8; 7] = [0xFF; 7];

fn le16(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(b[o..o + 2].try_into().unwrap())
}
fn le32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn le64(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

#[test]
fn hash_test_vectors() {
    // Published check values: XXH3_64bits("") with seed 0, and CRC-32C("123456789").
    assert_eq!(xxh3_64(b""), 0x2D06_8005_38D3_94C2);
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
}

#[test]
fn internal_keys() {
    // §2: row "a\0" escapes to 61 00 FF; terminators 00 01; !ts and !seqno are MAX - x, BE.
    let mut k = Vec::new();
    encode_key(&mut k, b"a\0", b"", 1, 1, Kind::Put).unwrap();
    let mut want = vec![0x61, 0x00, 0xFF, 0x00, 0x01, 0x00, 0x01];
    want.extend(FF7);
    want.push(0xFE);
    want.extend(FF7);
    want.push(0xFE);
    want.push(0x01);
    assert_eq!(k, want);

    // Marker: row, 00 01, 00 00, !ts(0) = FF.., !seqno(MAX) = 00.., kind 05.
    let mut k = Vec::new();
    encode_marker_key(&mut k, b"r", 0, u64::MAX).unwrap();
    let mut want = vec![0x72, 0x00, 0x01, 0x00, 0x00];
    want.extend([0xFF; 8]);
    want.extend([0x00; 8]);
    want.push(0x05);
    assert_eq!(k, want);

    // Seek key: kind byte 00.
    let mut k = Vec::new();
    encode_seek_key(&mut k, b"", b"q", 2, 0).unwrap();
    let mut want = vec![0x00, 0x01, 0x71, 0x00, 0x01];
    want.extend(FF7);
    want.push(0xFD);
    want.extend([0xFF; 8]);
    want.push(0x00);
    assert_eq!(k, want);
}

#[test]
fn values() {
    let enc = |v| {
        let mut out = Vec::new();
        encode_value(&mut out, v);
        out
    };
    assert_eq!(enc(ValueRef::Bytes(b"x")), [0x00, 0x78]);
    assert_eq!(
        enc(ValueRef::I64(-2)),
        [0x01, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
    );
    assert_eq!(
        enc(ValueRef::F64(1.0)),
        [0x02, 0, 0, 0, 0, 0, 0, 0xF0, 0x3F]
    );
    assert_eq!(enc(ValueRef::Varint(-1)), [0x03, 0x01]); // zigzag(-1) = 1
    assert_eq!(enc(ValueRef::Varint(64)), [0x03, 0x80, 0x01]); // zigzag(64) = 128
    let p = BlobPointer {
        blob_file: BlobFileId(3),
        len: 5,
        offset: 0x0102,
    };
    assert_eq!(
        enc(ValueRef::Blob(p)),
        [0x80, 3, 0, 0, 0, 5, 0, 0, 0, 0x02, 0x01, 0, 0, 0, 0, 0, 0]
    );
}

#[test]
fn physical_block_trailer() {
    // §4.1: payload ++ [kind, codec, 0, 0, uncompressed_len u32] ++ xxh3(payload ++ trailer[0..8]).
    let mut out = Vec::new();
    seal(BlockKind::Index, Compression::None, b"abc", &mut out).unwrap();
    let head = [b'a', b'b', b'c', 2, 0, 0, 0, 3, 0, 0, 0];
    assert_eq!(out[..11], head);
    assert_eq!(le64(&out, 11), xxh3_64(&head));
    assert_eq!(out.len(), 19);
    // Index value: offset varint, len varint.
    let mut v = Vec::new();
    BlockAddr {
        offset: 300,
        len: 5,
    }
    .encode_varint(&mut v);
    assert_eq!(v, [0xAC, 0x02, 0x05]);
}

#[test]
fn logical_data_block() {
    // §4.2: two entries of different rows, restart interval 16.
    let mut k1 = Vec::new();
    encode_key(&mut k1, b"a", b"x", 0, 0, Kind::Put).unwrap();
    let mut k2 = Vec::new();
    encode_key(&mut k2, b"ab", b"x", 0, 0, Kind::Put).unwrap();
    let mut b = BlockBuilder::data(16);
    b.add(&k1, b"V").unwrap();
    b.add(&k2, b"").unwrap();
    let block = b.finish().to_vec();
    let mut want = vec![0, k1.len() as u8, 1];
    want.extend(&k1);
    want.push(b'V');
    let off2 = want.len() as u32;
    // k1 and k2 share "a" (k1 continues 00 01, k2 continues 'b'), but k2 starts a row, and a
    // row start stores its whole key.
    want.extend([0, k2.len() as u8, 0]);
    want.extend(&k2);
    for v in [0u32, 0, off2, 1, 2] {
        // restarts [0]; row_starts [0, off2]; R = 1; S = 2
        want.extend(v.to_le_bytes());
    }
    assert_eq!(block, want);
    assert_eq!(Block::new(block.as_slice()).unwrap().row_start_count(), 2);
}

#[test]
fn filter_probing() {
    // §6, re-implemented from the text: bits_per_key 10 gives k = floor(6.9) = 6 and
    // num_lines = ceil(1 * 10 / 512) = 1.
    let h = 0x0123_4567_89AB_CDEFu64;
    let mut b = FilterBuilder::new(10);
    b.add_hash(h);
    let mut out = Vec::new();
    b.finish(&mut out);
    let mut want = vec![1u8, 6, 0, 0, 1, 0, 0, 0];
    let mut line = [0u8; 64];
    let h1 = h as u32;
    let delta = h1.rotate_right(17);
    let mut x = h1;
    for _ in 0..6 {
        let bit = (x & 511) as usize;
        line[bit / 8] |= 1 << (bit % 8);
        x = x.wrapping_add(delta);
    }
    want.extend(line);
    assert_eq!(out, want);
}

#[test]
fn sst_footer() {
    let f = Footer {
        top_index: BlockAddr {
            offset: 0x1111,
            len: 0x22,
        },
        row_filter: BlockAddr {
            offset: 0x3333,
            len: 0x44,
        },
        column_filter: BlockAddr::default(),
        properties: BlockAddr {
            offset: 0x5555,
            len: 0x66,
        },
        compression_dict: BlockAddr::default(),
        version: FormatVersion(1),
        flags: 0,
    };
    let b = f.encode();
    assert_eq!((le64(&b, 0), le32(&b, 8), le32(&b, 12)), (0x1111, 0x22, 0));
    assert_eq!((le64(&b, 16), le32(&b, 24)), (0x3333, 0x44));
    assert_eq!(b[32..48], [0; 16]);
    assert_eq!((le64(&b, 48), le32(&b, 56)), (0x5555, 0x66));
    assert_eq!(b[64..80], [0; 16]);
    assert_eq!((le32(&b, 80), le32(&b, 84)), (1, 0));
    assert_eq!(le64(&b, 88), xxh3_64(&b[..88]));
    assert_eq!(&b[96..], b"PHDBSST\x01");
}

#[test]
fn blob_headers() {
    let h = BlobExtentHeader {
        version: FormatVersion(1),
        blob_file: BlobFileId(7),
        extent_index: 9,
    };
    let b = h.encode();
    assert_eq!(&b[..8], b"PHDBBLOB");
    assert_eq!((le32(&b, 8), le32(&b, 12), le32(&b, 16)), (1, 7, 9));
    assert_eq!(b[20..56], [0; 36]);
    assert_eq!(le64(&b, 56), xxh3_64(&b[..56]));
    let r = encode_record_header(b"hello");
    assert_eq!((le64(&r, 0), le64(&r, 8)), (5, xxh3_64(b"hello")));
}

#[test]
fn superblock_offsets() {
    let sb = Superblock {
        version: FormatVersion(1),
        page_size: 4096,
        sequence: 0x0A0B,
        db_id: [0xD1; 16],
        snapshot: Some(ExtentRef {
            page: 32,
            size_class: 1,
        }),
        snapshot_len: 1000,
        log: Some(ExtentRef {
            page: 64,
            size_class: 2,
        }),
        log_len: 2000,
        manifest_version: 0x0C0D,
        file_pages: 0x0E0F,
        flags: 1,
        features: Default::default(),
    };
    let mut p = [0u8; 4096];
    sb.encode(&mut p);
    assert_eq!(&p[0..8], b"PHDBSUPR");
    assert_eq!((le32(&p, 8), le32(&p, 12), le64(&p, 16)), (1, 4096, 0x0A0B));
    assert_eq!(p[24..40], [0xD1; 16]);
    assert_eq!((le64(&p, 40), p[48], le32(&p, 52)), (32, 1, 1000));
    assert_eq!(
        (le64(&p, 56), le64(&p, 64), le64(&p, 72)),
        (0x0C0D, 0x0E0F, 1)
    );
    assert_eq!((le64(&p, 80), p[88], le32(&p, 92)), (64, 2, 2000));
    assert_eq!(p[96..120], [0; 24]);
    assert_eq!(le64(&p, 120), xxh3_64(&p[..120]));
}

#[test]
fn manifest_header_and_edit() {
    let header = ManifestHeader {
        version: FormatVersion(1),
        kind: ManifestBlockKind::Delta,
        manifest_version: 0x55,
        edit_count: 0,
        body_len: 0,
    };
    let mut b = Vec::new();
    encode_block(&header, &[Edit::DropTable { table: TableId(4) }], &mut b);
    // §9.1 header, then the edit: tag 2, body_len varint 4, table u32.
    assert_eq!(&b[0..8], b"PHDBMANI");
    assert_eq!((le32(&b, 8), b[12], le64(&b, 16)), (1, 2, 0x55));
    assert_eq!((le32(&b, 40), le32(&b, 44)), (1, 6));
    assert_eq!(b[64..], [2, 4, 4, 0, 0, 0]);
    assert_eq!(le64(&b, 48), xxh3_64(&[&b[..48], &b[64..]].concat()));
    assert!(
        b[13..16]
            .iter()
            .chain(&b[24..40])
            .chain(&b[56..64])
            .all(|&x| x == 0)
    );
}

#[test]
fn wal_segment_header_and_fragment() {
    let h = SegmentHeader {
        version: FormatVersion(1),
        stream: StreamId(3),
        epoch: 7,
        prev_epoch: 6,
        prev_end: 0x8000,
        db_id: [0xD2; 16],
        segment_size: 1 << 26,
        features: 0,
    };
    let mut f = Box::new([0u8; FRAME_SIZE]);
    h.encode(&mut f);
    assert_eq!(&f[0..8], b"PHDBWALS");
    assert_eq!(
        (
            le32(&f[..], 8),
            le32(&f[..], 12),
            le32(&f[..], 16),
            le32(&f[..], 20)
        ),
        (1, 3, 7, 6)
    );
    assert_eq!(f[24..40], [0xD2; 16]);
    assert_eq!(
        (le64(&f[..], 40), le32(&f[..], 48), le32(&f[..], 52)),
        (1 << 26, 32768, 0x8000)
    );
    assert_eq!(le32(&f[..], 56), crc32c(&f[..56]));
    // Format 4: features u32 at 56, 60..124 reserved (zero), CRC32C at 124.
    let mut f = Box::new([0u8; FRAME_SIZE]);
    SegmentHeader {
        version: FormatVersion(4),
        features: 0x0102_0304,
        ..h
    }
    .encode(&mut f);
    assert_eq!(le32(&f[..], 8), 4);
    assert_eq!(le32(&f[..], 56), 0x0102_0304);
    assert_eq!(f[60..124], [0; 64]);
    assert_eq!(le32(&f[..], 124), crc32c(&f[..124]));

    // §10.2 fragment: crc, epoch, len u16, type, reserved, payload.
    let mut out = Vec::new();
    FrameEncoder::new(7, FRAME_SIZE as u64).encode(b"hi", &mut out);
    assert_eq!(out[4..14], [7, 0, 0, 0, 2, 0, 1, 0, b'h', b'i']);
    assert_eq!(le32(&out, 0), crc32c(&out[4..]));
    assert_eq!(Lsn::new(7, 0x8000).0, (7 << 32) | 0x8000);
}

/// §11.7 (D281, perf287's review of #947): a row's row-epoch slot is
/// `xxh3_64_with_seed(seed + table, row) & (slots − 1)` (the seed plus the table id, wrapping)
/// in every build of every process. Golden vectors: a dependency or compiler change that
/// moves the hash fails here (a reader and a writer disagreeing on a slot would serve stale
/// rows for ever).
#[test]
fn row_epoch_hash_vectors() {
    use pigeonhole_format::shm::row_epoch_hash;
    let long: Vec<u8> = (0..300u32).map(|i| (i * 7) as u8).collect();
    assert_eq!(
        row_epoch_hash(0x5eed_5eed, 7, b"com.example/a"),
        0xc8e3_a972_e90b_591a
    );
    assert_eq!(row_epoch_hash(0, 0, b""), 0x2d06_8005_38d3_94c2);
    // The seed wraps: u64::MAX + u32::MAX.
    assert_eq!(
        row_epoch_hash(u64::MAX, u32::MAX, &long),
        0xc952_63d4_13d4_ab8a
    );
    for len in [0, 16, 17, 128, 129, 240, 241, 300] {
        assert_eq!(
            row_epoch_hash(3, 9, &long[..len]),
            twox_hash::XxHash3_64::oneshot_with_seed(12, &long[..len]),
            "row of {len} bytes"
        );
    }
}

#[test]
fn shm_layout() {
    let h = ShmHeader::layout([0xD3; 16], 2, 3, 100, 1, 64, 0x77, 0x88);
    let mut p = [0u8; 4096];
    h.encode(&mut p);
    assert_eq!(&p[0..8], b"PHDBSHM\0");
    assert_eq!(
        (le32(&p, 8), le32(&p, 12), le64(&p, 16)),
        (6, 4096, h.region_len)
    );
    // Layout version 6 (D278): the cross-shard hold, a live word the writer initializes.
    assert_eq!(pigeonhole_format::shm::header::CROSS_HELD, 192);
    // §8.3: reader slot `i`'s liveness lock is byte 2^62 + i (layout version 3, #693).
    assert_eq!(pigeonhole_format::superblock::READER_LOCK_BASE, 1 << 62);
    // §11.2: each shard's 64-byte watermark line holds `pending` at 0 and `durable` at 8
    // (layout version 2, D227).
    assert_eq!(
        (
            pigeonhole_format::shm::watermark::PENDING,
            pigeonhole_format::shm::watermark::DURABLE
        ),
        (0, 8)
    );
    assert_eq!(p[24..40], [0xD3; 16]);
    assert_eq!((le32(&p, 52), le32(&p, 56), le32(&p, 60)), (2, 3, 100));
    // Watermarks right after the header, views after 2 x 64-byte lines, slots after the two
    // view buffers (rounded to 64), arenas 2 MiB aligned, arena length rounded to 2 MiB.
    assert_eq!(
        (le64(&p, 96), le64(&p, 104), le64(&p, 112)),
        (4096, 4096 + 128, 4096 + 128 + 256)
    );
    assert_eq!((le64(&p, 120), le64(&p, 128)), (2 << 20, 2 << 20));
    assert_eq!((le64(&p, 144), le64(&p, 152)), (0x77, 0x88));
    // Layout version 5 (D281): the row-epoch table after the reader slots (64-aligned), its
    // slot count, its hash (1 = xxh3), its seed (0 from `layout`; the builder sets it) and
    // its flags (bit 0: the writer maintains the slots; 0 from `layout`).
    assert_eq!(
        (
            le64(&p, 160),
            le32(&p, 168),
            le32(&p, 172),
            le64(&p, 176),
            le32(&p, 184)
        ),
        (4096 + 128 + 256 + 3 * 64, 64, 1, 0, 0)
    );
    assert_eq!(h.region_len, 6 << 20);

    let v = ViewRecord {
        view_version: 9,
        manifest_version: 8,
        tablets: vec![ViewTablet {
            tablet: TabletId(5),
            table: TableId(4),
            shard: 1,
            start: b"m".to_vec(),
            end: None,
        }],
        memtables: vec![ViewMemtable {
            tablet: TabletId(5),
            family: FamilyId(6),
            shard: 1,
            age: 2,
            root: 64,
        }],
    };
    let mut b = Vec::new();
    v.encode(&mut b);
    // 32-byte header, tablet entry 24 + 1 byte padded to 32, memtable entry 24.
    assert_eq!(b.len(), 88);
    assert_eq!(
        (
            le64(&b, 0),
            le64(&b, 8),
            le32(&b, 16),
            le32(&b, 20),
            le32(&b, 24)
        ),
        (9, 8, 88, 1, 1)
    );
    let mut zeroed = b.clone();
    zeroed[28..32].fill(0);
    assert_eq!(le32(&b, 28), crc32c(&zeroed));
    assert_eq!(
        (le64(&b, 32), le32(&b, 40), le16(&b, 44), le16(&b, 46)),
        (5, 4, 1, 0)
    );
    assert_eq!((le32(&b, 48), le32(&b, 52), b[56]), (1, 0, b'm'));
    assert_eq!(b[57..64], [0; 7]);
    assert_eq!(
        (le64(&b, 64), le32(&b, 72), le16(&b, 76), b[78], b[79]),
        (5, 6, 1, 2, 0)
    );
    assert_eq!((le32(&b, 80), le32(&b, 84)), (64, 0));
}

/// No two shm header fields overlap, every one is inside the header page, and every `u64` is
/// 8-aligned (most are atomics). A new field must be added here, beside FORMAT §11.1.
#[test]
fn shm_header_fields_do_not_overlap() {
    use pigeonhole_format::shm::{HEADER_LEN, header as h};
    let mut fields = [
        ("magic", h::MAGIC, 8),
        ("layout_version", h::LAYOUT_VERSION, 4),
        ("header_len", h::HEADER_LEN, 4),
        ("region_len", h::REGION_LEN, 8),
        ("db_id", h::DB_ID, 16),
        ("generation", h::GENERATION, 8),
        ("state", h::STATE, 4),
        ("shard_count", h::SHARD_COUNT, 4),
        ("reader_slot_count", h::READER_SLOT_COUNT, 4),
        ("view_buffer_len", h::VIEW_BUFFER_LEN, 4),
        ("manifest_version", h::MANIFEST_VERSION, 8),
        ("view_pointer", h::VIEW_POINTER, 8),
        ("writer_pid", h::WRITER_PID, 4),
        ("writer_start_time", h::WRITER_START_TIME, 8),
        ("watermarks_off", h::WATERMARKS_OFF, 8),
        ("views_off", h::VIEWS_OFF, 8),
        ("reader_slots_off", h::READER_SLOTS_OFF, 8),
        ("arenas_off", h::ARENAS_OFF, 8),
        ("arena_len", h::ARENA_LEN, 8),
        ("next_seqno", h::NEXT_SEQNO, 8),
        ("file_device", h::FILE_DEVICE, 8),
        ("file_inode", h::FILE_INODE, 8),
        ("row_epochs_off", h::ROW_EPOCHS_OFF, 8),
        ("row_epoch_slots", h::ROW_EPOCH_SLOTS, 4),
        ("row_epoch_hash", h::ROW_EPOCH_HASH, 4),
        ("row_epoch_seed", h::ROW_EPOCH_SEED, 8),
        ("row_epoch_flags", h::ROW_EPOCH_FLAGS, 4),
        ("cross_held", h::CROSS_HELD, 8),
    ];
    fields.sort_by_key(|&(_, off, _)| off);
    for w in fields.windows(2) {
        let ((a, a_off, a_len), (b, b_off, _)) = (w[0], w[1]);
        assert!(
            a_off + a_len <= b_off,
            "{a} (@{a_off}, {a_len} bytes) overlaps {b} (@{b_off})"
        );
    }
    for (name, off, len) in fields {
        assert!(off + len <= HEADER_LEN, "{name} is past the header");
        if len == 8 && name != "magic" {
            assert_eq!(off % 8, 0, "{name} (@{off}) is not 8-aligned");
        }
    }
}

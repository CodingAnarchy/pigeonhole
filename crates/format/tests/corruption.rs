//! Decoder corruption branches that no other test reaches (coverage run of 2026-10-28, #695
//! item (c)): crafted bytes, with checksums recomputed where one guards the branch, each
//! decoding to the error the branch returns.

use pigeonhole_format::block::{Block, BlockBuilder};
use pigeonhole_format::checksum::{crc32c, xxh3_64};
use pigeonhole_format::compress::{Compression, decompress};
use pigeonhole_format::cursor::Cursor;
use pigeonhole_format::key::{
    Kind, MAX_KEY_PART, SUFFIX_LEN, TERMINATOR, decode_key, decode_key_in_row,
    encode_column_prefix, encode_key, encode_marker_key,
};
use pigeonhole_format::manifest::{
    Edit, ManifestBlockKind, ManifestHeader, decode_block, encode_block,
};
use pigeonhole_format::shm::{ShmHeader, ViewRecord, ViewTablet};
use pigeonhole_format::superblock::Superblock;
use pigeonhole_format::wal::{FRAME_SIZE, SegmentHeader};
use pigeonhole_format::{Error, FormatVersion, StreamId, TableId, TabletId};

fn key(row: &[u8], qualifier: &[u8], kind: Kind) -> Vec<u8> {
    let mut k = Vec::new();
    encode_key(&mut k, row, qualifier, 5, 7, kind).unwrap();
    k
}

/// `body` (an escaped row, its terminator, and what follows) with `suffix_of`'s suffix.
fn with_suffix(body: &[u8], suffix_of: &[u8]) -> Vec<u8> {
    [body, &suffix_of[suffix_of.len() - SUFFIX_LEN..]].concat()
}

fn body(k: &[u8]) -> &[u8] {
    &k[..k.len() - SUFFIX_LEN]
}

#[test]
fn a_key_part_over_64_kib_is_too_large() {
    let small = key(b"r", b"q", Kind::Put);
    let long = vec![b'a'; MAX_KEY_PART + 1];
    // A row past the limit (no build writes one), then a qualifier past it.
    let big_row = [&long[..], &small[1..]].concat();
    assert_eq!(decode_key(&big_row), Err(Error::KeyTooLarge));
    let big_qualifier = with_suffix(
        &[&b"r"[..], &TERMINATOR, &long, &TERMINATOR].concat(),
        &small,
    );
    assert_eq!(decode_key(&big_qualifier), Err(Error::KeyTooLarge));
    assert_eq!(
        decode_key_in_row(&big_qualifier, 1),
        Err(Error::KeyTooLarge)
    );
}

#[test]
fn the_in_row_decoder_checks_everything_after_the_row() {
    let k = key(b"row", b"q", Kind::Put);
    assert!(decode_key_in_row(&k, 3).is_ok());
    // A row length that does not end at the row's terminator.
    assert!(matches!(
        decode_key_in_row(&k, 2),
        Err(Error::Corrupt {
            what: "key: row terminator"
        })
    ));
    assert!(matches!(
        decode_key_in_row(&k, 1000),
        Err(Error::Corrupt {
            what: "key: row terminator"
        })
    ));
    // Bytes after the qualifier's terminator.
    let trailing = with_suffix(&[body(&k), b"x"].concat(), &k);
    assert!(matches!(
        decode_key_in_row(&trailing, 3),
        Err(Error::Corrupt {
            what: "key: trailing bytes"
        })
    ));
    // A put's suffix on a family-delete marker, and a marker's suffix on a put.
    let mut marker = Vec::new();
    encode_marker_key(&mut marker, b"row", 5, 7).unwrap();
    for bad in [
        with_suffix(body(&marker), &k),
        with_suffix(body(&k), &marker),
    ] {
        let corrupt = Err(Error::Corrupt {
            what: "key: marker kind",
        });
        assert_eq!(decode_key_in_row(&bad, 3), corrupt);
        assert_eq!(decode_key(&bad), corrupt);
    }
}

#[test]
fn an_uncompressed_payload_must_fill_the_block() {
    let mut out = [0u8; 4];
    assert!(matches!(
        decompress(Compression::None, b"abc", &mut out),
        Err(Error::Corrupt {
            what: "uncompressed block length"
        })
    ));
    assert!(decompress(Compression::None, b"abcd", &mut out).is_ok());
}

/// A key in the column `prefix`, or longer than a column key if `extra` isn't empty.
fn column_key(prefix: &[u8], fill: u8, extra: &[u8]) -> Vec<u8> {
    [prefix, extra, &[fill; SUFFIX_LEN]].concat()
}

#[test]
fn a_column_skip_steps_over_a_malformed_shared_prefix() {
    // The entry after the column shares more than the column with the last one in it, which
    // no well-formed block has (it is not a key of the column, so it must differ inside the
    // column). The skip rebuilds the passed keys by stepping and stops on it.
    let mut column = Vec::new();
    encode_column_prefix(&mut column, b"r", b"q").unwrap();
    let first = column_key(&column, 0x10, b"");
    let second = column_key(&column, 0x20, b"");
    let stop = column_key(&column, 0x30, &[0x20; 5]);
    let mut b = BlockBuilder::data(16);
    for k in [&first, &second, &stop] {
        b.add(k, b"\0v").unwrap();
    }
    let block = Block::new(b.finish().to_vec()).unwrap();
    let mut it = block.into_cursor();
    it.seek_to_first().unwrap();
    assert_eq!(it.key(), &first[..]);
    assert!(it.skip_column(&column).unwrap());
    assert!(it.valid());
    assert_eq!(it.key(), &stop[..]);
    it.next().unwrap();
    assert!(!it.valid());
}

/// A cursor over fixed entries that keeps every default method.
struct Fixed {
    keys: Vec<Vec<u8>>,
    at: usize,
}

impl Cursor for Fixed {
    type Error = Error;
    fn valid(&self) -> bool {
        self.at < self.keys.len()
    }
    fn key(&self) -> &[u8] {
        &self.keys[self.at]
    }
    fn value(&self) -> &[u8] {
        b""
    }
    fn seek_to_first(&mut self) -> Result<(), Error> {
        self.at = 0;
        Ok(())
    }
    fn seek(&mut self, target: &[u8]) -> Result<(), Error> {
        self.at = self
            .keys
            .iter()
            .position(|k| &k[..] >= target)
            .unwrap_or(self.keys.len());
        Ok(())
    }
    fn next(&mut self) -> Result<(), Error> {
        self.at += 1;
        Ok(())
    }
}

#[test]
fn the_default_row_skip_steps_one_entry_past_a_malformed_key() {
    let next = key(b"s", b"q", Kind::Put);
    let mut c = Fixed {
        keys: vec![b"not a key".to_vec(), next.clone()],
        at: 0,
    };
    c.skip_row().unwrap();
    assert_eq!(c.key(), &next[..]);
}

fn manifest_block(edits: &[Edit]) -> Vec<u8> {
    let header = ManifestHeader {
        version: FormatVersion::CURRENT,
        kind: ManifestBlockKind::Delta,
        manifest_version: 3,
        edit_count: 0,
        body_len: 0,
    };
    let mut out = Vec::new();
    encode_block(&header, edits, &mut out);
    out
}

#[test]
fn a_manifest_body_longer_than_its_edits_is_corrupt() {
    let edit = Edit::CreateTable {
        table: TableId(1),
        name: "t".into(),
    };
    let mut b = manifest_block(&[edit]);
    assert!(decode_block(&b).is_ok());
    // Two bytes after the last edit, inside the checksummed body.
    b.extend([0, 0]);
    let body_len = (b.len() - 64) as u32;
    b[44..48].copy_from_slice(&body_len.to_le_bytes());
    let sum = xxh3_64(&[&b[..48], &b[64..]].concat());
    b[48..56].copy_from_slice(&sum.to_le_bytes());
    assert!(matches!(
        decode_block(&b),
        Err(Error::Corrupt {
            what: "manifest block body length"
        })
    ));
}

fn superblock(version: FormatVersion, page_size: u32) -> [u8; 4096] {
    let sb = Superblock {
        version,
        page_size,
        sequence: 1,
        db_id: [3; 16],
        snapshot: None,
        snapshot_len: 0,
        log: None,
        log_len: 0,
        manifest_version: 1,
        file_pages: 64,
        flags: 0,
        features: Default::default(),
    };
    let mut page = [0u8; 4096];
    sb.encode(&mut page);
    page
}

#[test]
fn a_superblock_for_another_page_size_is_corrupt() {
    assert!(Superblock::decode(&superblock(FormatVersion::CURRENT, 4096)).is_ok());
    assert!(matches!(
        Superblock::decode(&superblock(FormatVersion::CURRENT, 8192)),
        Err(Error::Corrupt {
            what: "superblock page size"
        })
    ));
}

#[test]
fn a_newer_slot_b_beside_a_damaged_slot_a_is_a_newer_file() {
    let mut a = superblock(FormatVersion::CURRENT, 4096);
    a[20] ^= 1;
    let b = superblock(FormatVersion(99), 4096);
    let (a, b) = (Superblock::decode(&a), Superblock::decode(&b));
    assert!(matches!(a, Err(Error::Checksum { .. })));
    assert!(matches!(
        Superblock::choose(a, b),
        Err(Error::UnsupportedVersion { found: 99, .. })
    ));
}

fn segment_header(segment_size: u64) -> Box<[u8; FRAME_SIZE]> {
    let h = SegmentHeader {
        version: FormatVersion::CURRENT,
        stream: StreamId(2),
        epoch: 4,
        prev_epoch: 3,
        prev_end: 0,
        db_id: [3; 16],
        segment_size,
        features: 0,
    };
    let mut frame = Box::new([0u8; FRAME_SIZE]);
    h.encode(&mut frame);
    frame
}

#[test]
fn a_wal_segment_header_checks_its_crc_and_geometry() {
    let good = 4 * FRAME_SIZE as u64;
    assert!(SegmentHeader::decode(&segment_header(good)[..]).is_ok());
    let mut torn = segment_header(good);
    torn[12] ^= 1;
    assert!(matches!(
        SegmentHeader::decode(&torn[..]),
        Err(Error::Checksum { .. })
    ));
    // With a valid CRC: a partial frame, a single frame, and over 4 GiB.
    for size in [good + 1, FRAME_SIZE as u64, (1 << 32) + FRAME_SIZE as u64] {
        assert!(
            matches!(
                SegmentHeader::decode(&segment_header(size)[..]),
                Err(Error::Corrupt { .. })
            ),
            "segment size {size}"
        );
    }
}

#[test]
fn a_shm_header_with_a_foreign_magic_is_refused() {
    // The golden file is the header page's first 188 bytes; the rest is zero.
    let mut golden = include_bytes!("golden/shm_header.bin").to_vec();
    golden.resize(4096, 0);
    assert!(ShmHeader::decode(&golden).is_ok());
    let mut bad = golden;
    bad[0] ^= 0xFF;
    assert!(matches!(
        ShmHeader::decode(&bad),
        Err(Error::BadMagic { .. })
    ));
}

/// Re-seals a view record's CRC after an edit, as the publisher computes it.
fn reseal(view: &mut [u8]) {
    view[28..32].fill(0);
    let crc = crc32c(view);
    view[28..32].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn a_view_tablet_with_end_bytes_but_no_end_is_corrupt() {
    let record = ViewRecord {
        view_version: 1,
        manifest_version: 1,
        tablets: vec![ViewTablet {
            tablet: TabletId(1),
            table: TableId(1),
            shard: 0,
            start: b"a".to_vec(),
            end: Some(b"z".to_vec()),
        }],
        memtables: Vec::new(),
    };
    let mut view = Vec::new();
    record.encode(&mut view);
    reseal(&mut view);
    assert_eq!(ViewRecord::decode(&view), Ok(record));
    // Clear the tablet's has-end flag (after the 32-byte header: id, table, shard, flags).
    view[32 + 14] &= !1;
    reseal(&mut view);
    assert!(matches!(
        ViewRecord::decode(&view),
        Err(Error::Corrupt { .. })
    ));
}

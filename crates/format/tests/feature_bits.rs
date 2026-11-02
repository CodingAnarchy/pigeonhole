//! #496 PR 3 (D269): format 4's feature bits. A superblock with an incompatible bit this
//! build does not know is refused; an unknown read-only-compatible bit refuses writing only;
//! a WAL segment or SST with an unknown feature bit is refused; a manifest edit with an
//! unknown tag is refused below 128 and skipped from 128.

use pigeonhole_format::checksum::xxh3_64;
use pigeonhole_format::manifest::{
    Edit, MANIFEST_HEADER_LEN, ManifestBlockKind, ManifestHeader, decode_block, encode_block,
};
use pigeonhole_format::superblock::{Features, SUPERBLOCK_PAGE_A, Superblock};
use pigeonhole_format::version::FormatVersion;
use pigeonhole_format::wal::{FRAME_SIZE, SegmentHeader};
use pigeonhole_format::{Error, StreamId, TableId, varint};

#[track_caller]
fn unsupported<T: std::fmt::Debug>(r: Result<T, Error>, what: &str, value: u64) {
    match r {
        Err(Error::UnsupportedFeature { what: w, value: v }) => {
            assert_eq!((w, v), (what, value));
        }
        other => panic!("expected UnsupportedFeature {{ {what}, {value} }}, got {other:?}"),
    }
}

fn sb(sequence: u64, features: Features) -> Superblock {
    Superblock {
        version: FormatVersion::CURRENT,
        page_size: 4096,
        sequence,
        db_id: [7; 16],
        snapshot: None,
        snapshot_len: 0,
        log: None,
        log_len: 0,
        manifest_version: 0,
        file_pages: 16,
        flags: 0,
        features,
    }
}

fn decoded(s: &Superblock) -> pigeonhole_format::Result<Superblock> {
    let mut page = [0u8; 4096];
    s.encode(&mut page);
    Superblock::decode(&page)
}

#[test]
fn superblock_feature_bits_roundtrip_at_their_offsets() {
    let f = Features {
        compat: 0x11,
        ro_compat: 0x22,
        incompat: 0x33,
    };
    let mut page = [0u8; 4096];
    sb(1, f).encode(&mut page);
    let le = |at: usize| u64::from_le_bytes(page[at..at + 8].try_into().unwrap());
    assert_eq!((le(96), le(104), le(112)), (0x11, 0x22, 0x33));
    assert!(page[128..4088].iter().all(|&b| b == 0), "extension area");
    assert_eq!(Superblock::decode(&page).unwrap().features, f);
}

#[test]
fn an_unknown_incompat_bit_refuses_the_file() {
    let plain = Features::default();
    let unknown = Features {
        incompat: 1 << 40,
        ..plain
    };
    // In either slot, at a higher or lower sequence: never passed over.
    for (a, b) in [
        (sb(5, plain), sb(6, unknown)),
        (sb(7, plain), sb(6, unknown)),
        (sb(6, unknown), sb(5, plain)),
        (sb(6, unknown), sb(6, unknown)),
    ] {
        unsupported(
            Superblock::choose(decoded(&a), decoded(&b)),
            "incompatible feature bits",
            1 << 40,
        );
    }
}

#[test]
fn unknown_compat_and_ro_compat_bits_still_read() {
    let f = Features {
        compat: 1 << 3,
        ro_compat: 1 << 9,
        incompat: 0,
    };
    let (chosen, page) = Superblock::choose(decoded(&sb(5, f)), decoded(&sb(4, f))).unwrap();
    assert_eq!((chosen.features, page), (f, SUPERBLOCK_PAGE_A));
    assert!(f.check_read().is_ok());
    // Writing is refused for the unknown ro_compat bit only.
    unsupported(
        f.check_write(),
        "read-only-compatible feature bits (the file can be opened read-only)",
        1 << 9,
    );
    assert!(Features { ro_compat: 0, ..f }.check_write().is_ok());
}

fn segment(features: u32) -> Box<[u8; FRAME_SIZE]> {
    let mut f = Box::new([0u8; FRAME_SIZE]);
    SegmentHeader {
        version: FormatVersion::CURRENT,
        stream: StreamId(0),
        epoch: 3,
        prev_epoch: 2,
        prev_end: 0,
        db_id: [7; 16],
        segment_size: 4 * FRAME_SIZE as u64,
        features,
        feed_session: 0,
    }
    .encode(&mut f);
    f
}

#[test]
fn an_unknown_wal_segment_feature_bit_is_refused() {
    assert_eq!(SegmentHeader::decode(&segment(0)[..]).unwrap().features, 0);
    unsupported(
        SegmentHeader::decode(&segment(1 << 5)[..]),
        "wal segment feature bits",
        1 << 5,
    );
}

/// A delta block holding a `DropTable` edit, then an edit with `tag` and an opaque body.
fn block_with_tag(tag: u8) -> Vec<u8> {
    let header = ManifestHeader {
        version: FormatVersion::CURRENT,
        kind: ManifestBlockKind::Delta,
        manifest_version: 5,
        edit_count: 0,
        body_len: 0,
    };
    let known = Edit::DropTable { table: TableId(4) };
    let mut out = Vec::new();
    encode_block(&header, std::slice::from_ref(&known), &mut out);
    out.push(tag);
    varint::put_bytes(&mut out, b"from a newer build");
    let body_len = (out.len() - MANIFEST_HEADER_LEN) as u32;
    out[40..44].copy_from_slice(&2u32.to_le_bytes());
    out[44..48].copy_from_slice(&body_len.to_le_bytes());
    let sum = xxh3_64(&[&out[..48], &out[MANIFEST_HEADER_LEN..]].concat());
    out[48..56].copy_from_slice(&sum.to_le_bytes());
    out
}

#[test]
fn manifest_edit_tags_below_128_must_be_understood_and_tag_0_is_corrupt() {
    assert!(matches!(
        decode_block(&block_with_tag(0)),
        Err(Error::Corrupt { .. })
    ));
    unsupported(decode_block(&block_with_tag(20)), "manifest edit tag", 20);
    unsupported(decode_block(&block_with_tag(127)), "manifest edit tag", 127);
    for tag in [128, 200, 255] {
        let (_, edits, len) = decode_block(&block_with_tag(tag)).unwrap();
        assert_eq!(edits, [Edit::DropTable { table: TableId(4) }], "tag {tag}");
        assert_eq!(len, block_with_tag(tag).len());
    }
}

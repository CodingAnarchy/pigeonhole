//! Decoders reject well-checksummed but implausible input (review B2 to B5 and the nits).

use pigeonhole_format::block::verify;
use pigeonhole_format::checksum::xxh3_64;
use pigeonhole_format::manifest::decode_block;
use pigeonhole_format::superblock::{ExtentRef, Superblock};
use pigeonhole_format::wal::{BatchRef, FRAME_SIZE, FrameDecoder, FrameEncoder, StreamList};
use pigeonhole_format::{Error, FormatVersion, StreamId, varint};

/// A physical block with a valid checksum over whatever trailer fields we choose.
fn physical(payload: &[u8], codec: u8, uncompressed_len: u32) -> Vec<u8> {
    let mut b = payload.to_vec();
    b.extend([1, codec, 0, 0]);
    b.extend(uncompressed_len.to_le_bytes());
    let sum = xxh3_64(&b);
    b.extend(sum.to_le_bytes());
    b
}

#[test]
fn block_uncompressed_len_is_bounded() {
    // LZ4 cannot expand 10 bytes to 4 GiB: rejected before anyone allocates.
    assert!(matches!(
        verify(&physical(&[0; 10], 1, u32::MAX)),
        Err(Error::Corrupt { .. })
    ));
    assert!(matches!(
        verify(&physical(&[0; 10], 1, 2551)),
        Err(Error::Corrupt { .. })
    ));
    assert!(verify(&physical(&[0; 10], 1, 2550)).is_ok());
    // Even a large payload cannot claim more than the 128 MiB block cap.
    let big = vec![0; 1 << 20];
    assert!(matches!(
        verify(&physical(&big, 1, (128 << 20) + 1)),
        Err(Error::Corrupt { .. })
    ));
    assert!(verify(&physical(&big, 1, 128 << 20)).is_ok());
    // Uncompressed blocks must match exactly.
    assert!(verify(&physical(b"abc", 0, 4)).is_err());
}

fn superblock(
    snapshot: Option<ExtentRef>,
    snapshot_len: u32,
    log: Option<ExtentRef>,
    log_len: u32,
) -> Superblock {
    Superblock {
        version: FormatVersion::CURRENT,
        page_size: 4096,
        sequence: 1,
        db_id: [0; 16],
        snapshot,
        snapshot_len,
        log,
        log_len,
        manifest_version: 1,
        file_pages: 64,
        flags: 0,
    }
}

#[test]
fn superblock_lengths_fit_their_extents() {
    let e = ExtentRef {
        page: 16,
        size_class: 0,
    }; // 64 KiB
    let decode = |sb: Superblock| {
        let mut page = [0u8; 4096];
        sb.encode(&mut page);
        Superblock::decode(&page)
    };
    assert!(decode(superblock(Some(e), 65536, Some(e), 0)).is_ok());
    assert!(decode(superblock(Some(e), 65537, None, 0)).is_err());
    assert!(decode(superblock(None, 0, Some(e), 65537)).is_err());
    assert!(
        decode(superblock(None, 1, None, 0)).is_err(),
        "no snapshot but a length"
    );
    assert!(
        decode(superblock(None, 0, None, 1)).is_err(),
        "no log but a length"
    );
}

#[test]
fn manifest_edit_count_is_bounded() {
    // A checksummed block claiming more edits than its body can hold.
    let mut b = vec![0u8; 64];
    b[..8].copy_from_slice(b"PHDBMANI");
    b[8..12].copy_from_slice(&1u32.to_le_bytes());
    b[12] = 2;
    b[40..44].copy_from_slice(&u32::MAX.to_le_bytes());
    b[44..48].copy_from_slice(&4u32.to_le_bytes());
    b.extend([2, 2, 0, 0]);
    let sum = xxh3_64(&[&b[..48], &b[64..]].concat());
    b[48..56].copy_from_slice(&sum.to_le_bytes());
    assert!(matches!(decode_block(&b), Err(Error::Corrupt { .. })));
}

/// One encoded mutation with arbitrary fields, wrapped as a one-mutation batch.
fn batch(kind: u8, row_len: usize, qual: &[u8], value: &[u8]) -> Vec<u8> {
    let mut b = 1u32.to_le_bytes().to_vec();
    b.extend(1u32.to_le_bytes());
    b.extend(2u32.to_le_bytes());
    b.push(kind);
    varint::put_bytes(&mut b, &vec![b'r'; row_len]);
    varint::put_bytes(&mut b, qual);
    varint::put_bytes(&mut b, value);
    b
}

#[test]
fn batch_decode_applies_push_rules() {
    assert!(BatchRef::new(&batch(1, 3, b"q", b"\0v")).is_ok());
    assert_eq!(
        BatchRef::new(&batch(1, 65537, b"q", b"")),
        Err(Error::KeyTooLarge)
    );
    assert!(
        BatchRef::new(&batch(3, 1, b"q", b"\0v")).is_err(),
        "delete with a value"
    );
    assert!(
        BatchRef::new(&batch(5, 1, b"q", b"")).is_err(),
        "family delete with a qualifier"
    );
    assert!(BatchRef::new(&batch(5, 1, b"", b"")).is_ok());
}

#[test]
fn varints_are_canonical() {
    assert_eq!(varint::get_u64(&[0x00]), Ok((0, 1)));
    assert!(varint::get_u64(&[0x80, 0x00]).is_err());
    assert!(varint::get_u64(&[0xAC, 0x82, 0x00]).is_err());
    assert_eq!(varint::get_u64(&[0xAC, 0x02]), Ok((300, 2)));
    let mut max = Vec::new();
    varint::put_u64(&mut max, u64::MAX);
    assert_eq!(varint::get_u64(&max), Ok((u64::MAX, 10)));
}

#[test]
fn decoder_clamps_to_frame_one() {
    // An offset inside frame 0 starts at frame 1, like the encoder.
    let mut out = Vec::new();
    FrameEncoder::new(4, 10).encode(b"rec", &mut out);
    let mut frame = vec![0u8; FRAME_SIZE];
    frame[..out.len()].copy_from_slice(&out);
    let mut dec = FrameDecoder::new(4, 10);
    assert!(matches!(
        dec.decode(&frame),
        Ok(Some(pigeonhole_format::wal::Decoded::Record {
            offset: 32768
        }))
    ));
    assert_eq!(dec.record(), b"rec");
}

#[test]
#[should_panic(expected = "65535")]
fn stream_list_refuses_too_many_streams() {
    let streams = vec![StreamId(0); 65536];
    StreamList::encode(&streams, &mut Vec::new());
}

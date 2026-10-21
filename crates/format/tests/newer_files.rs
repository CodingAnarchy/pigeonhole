//! #496 PR 2: an older build never silently misreads what a newer build wrote. A superblock
//! slot whose checksum passes but whose version is newer is never passed over for the other
//! slot; a WAL fragment of the segment's epoch whose CRC passes but whose type is unknown is
//! an `UnsupportedFeature`, not a torn tail.

use pigeonhole_format::Error;
use pigeonhole_format::checksum::{crc32c, crc32c_append};
use pigeonhole_format::superblock::{SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock};
use pigeonhole_format::version::FormatVersion;
use pigeonhole_format::wal::{FRAGMENT_HEADER_LEN, FRAME_SIZE, FrameDecoder, FrameEncoder};

fn sb(version: u32, sequence: u64) -> Superblock {
    Superblock {
        version: FormatVersion(version),
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
    }
}

fn decoded(s: &Superblock) -> pigeonhole_format::Result<Superblock> {
    let mut page = [0u8; 4096];
    s.encode(&mut page);
    Superblock::decode(&page)
}

#[test]
fn a_newer_superblock_slot_is_never_passed_over() {
    let current = FormatVersion::CURRENT.0;
    let newer = current + 1;
    // Either slot newer, at a higher or a lower sequence than the other: refused.
    for (a, b) in [
        (sb(current, 5), sb(newer, 6)),
        (sb(current, 7), sb(newer, 6)),
        (sb(newer, 6), sb(current, 5)),
        (sb(newer, 4), sb(current, 5)),
    ] {
        match Superblock::choose(decoded(&a), decoded(&b)) {
            Err(Error::UnsupportedVersion { found, .. }) => assert_eq!(found, newer),
            other => panic!("a newer slot was passed over: {other:?}"),
        }
    }
    // Both readable: the higher sequence, as before.
    let (chosen, page) = Superblock::choose(decoded(&sb(current, 5)), decoded(&sb(2, 6))).unwrap();
    assert_eq!((chosen.sequence, page), (6, SUPERBLOCK_PAGE_B));
    // A torn (bad checksum) slot is still passed over.
    let mut torn = [0u8; 4096];
    sb(current, 9).encode(&mut torn);
    torn[30] ^= 1;
    let (chosen, page) =
        Superblock::choose(decoded(&sb(current, 5)), Superblock::decode(&torn)).unwrap();
    assert_eq!((chosen.sequence, page), (5, SUPERBLOCK_PAGE_A));
}

/// One frame holding `record`'s fragments as `FrameEncoder` writes them for `epoch`, the
/// first fragment's type set to `ty` with its CRC recomputed (or not).
fn frame_with_type(epoch: u32, ty: u8, fix_crc: bool) -> Vec<u8> {
    let mut enc = FrameEncoder::new(epoch, FRAME_SIZE as u64);
    let mut out = Vec::new();
    enc.encode(b"a record", &mut out);
    out.resize(FRAME_SIZE, 0);
    let h = 0;
    out[h + 10] = ty;
    if fix_crc {
        let len = usize::from(u16::from_le_bytes([out[h + 8], out[h + 9]]));
        let crc = crc32c_append(
            crc32c(&out[h + 4..h + FRAGMENT_HEADER_LEN]),
            &out[h + FRAGMENT_HEADER_LEN..h + FRAGMENT_HEADER_LEN + len],
        );
        out[h..h + 4].copy_from_slice(&crc.to_le_bytes());
    }
    out
}

#[test]
fn an_intact_fragment_of_an_unknown_type_is_unsupported_not_a_torn_tail() {
    let epoch = 3;
    match FrameDecoder::new(epoch, FRAME_SIZE as u64).decode(&frame_with_type(epoch, 9, true)) {
        Err(Error::UnsupportedFeature { what, value }) => {
            assert_eq!((what, value), ("wal fragment type", 9));
        }
        other => panic!("expected UnsupportedFeature, got {other:?}"),
    }
    // A failing CRC or another epoch is a torn or recycled frame: still a stop.
    for (frame, epoch) in [
        (frame_with_type(epoch, 9, false), epoch),
        (frame_with_type(epoch, 9, true), epoch + 1),
    ] {
        let d = FrameDecoder::new(epoch, FRAME_SIZE as u64).decode(&frame);
        assert!(
            matches!(d, Ok(Some(pigeonhole_format::wal::Decoded::Stop { .. }))),
            "{d:?}"
        );
    }
}

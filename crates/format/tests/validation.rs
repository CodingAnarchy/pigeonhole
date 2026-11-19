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
    // Even a large payload cannot claim more than the 128 MiB block cap. (Hashing 1 MiB is
    // too slow under Miri, which only needs the small cases above.)
    if cfg!(miri) {
        return;
    }
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
        features: Default::default(),
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
fn stream_list_refuses_too_many_streams() {
    let mut out = Vec::new();
    let err = StreamList::encode(&vec![StreamId(0); 65536], &mut out).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument { .. }));
    assert!(out.is_empty(), "nothing written on failure");
    if !cfg!(miri) {
        StreamList::encode(&vec![StreamId(0); 65535], &mut out).unwrap();
        assert_eq!(out.len(), 2 + 4 * 65535);
    }
}

#[test]
fn caller_misuse_is_invalid_argument() {
    use pigeonhole_format::block::BlockBuilder;
    use pigeonhole_format::key::encode_key;
    use pigeonhole_format::wal::BatchBuilder;
    use pigeonhole_format::{FamilyId, Kind, TableId};
    let invalid = |r: Result<(), Error>| matches!(r, Err(Error::InvalidArgument { .. }));
    assert!(invalid(encode_key(
        &mut Vec::new(),
        b"r",
        b"",
        1,
        1,
        Kind::FamilyDelete
    )));
    let mut b = BatchBuilder::new();
    assert!(invalid(b.push(
        TableId(1),
        FamilyId(1),
        Kind::ColumnDelete,
        b"r",
        b"q",
        None,
        b"\0v"
    )));
    assert!(invalid(b.push(
        TableId(1),
        FamilyId(1),
        Kind::FamilyDelete,
        b"r",
        b"q",
        None,
        b""
    )));
    let mut blk = BlockBuilder::index();
    blk.add(b"b", b"").unwrap();
    assert!(invalid(blk.add(b"a", b"")), "out of order");
    blk.finish();
    assert!(invalid(blk.add(b"c", b"")), "after finish");
    // The same shape violations read from bytes are corruption, not caller error.
    assert!(matches!(
        BatchRef::new(&batch(3, 1, b"q", b"\0v")),
        Err(Error::Corrupt { .. })
    ));
}

/// D331 (ICR 0141): a feed-silent batch's mark is the count's bit 31. A builder carries it
/// into the record, `decode_with(.., true)` takes it, and `decode` (a database without
/// `incompat` bit 3, or a build before D331) refuses it as corrupt, so a stray bit is never
/// read as the mark.
#[test]
fn a_silent_batch_is_read_only_with_its_bit() {
    use pigeonhole_format::wal::{BatchBuilder, SILENT_BATCH, WalRecord};
    use pigeonhole_format::{FamilyId, Kind, TableId};

    let mut b = BatchBuilder::new();
    b.push(
        TableId(1),
        FamilyId(2),
        Kind::Put,
        b"row",
        b"q",
        Some(7),
        b"\x00v",
    )
    .unwrap();
    b.mark_silent();
    b.push(
        TableId(1),
        FamilyId(2),
        Kind::Put,
        b"row2",
        b"q",
        None,
        b"\x00w",
    )
    .unwrap();
    let bytes = b.batch().as_bytes().to_vec();
    let raw = u32::from_le_bytes(bytes[..4].try_into().unwrap());
    assert_eq!(raw, SILENT_BATCH | 2, "the mark is in the encoded count");
    let mut rec = Vec::new();
    WalRecord::Batch {
        seqno: 9,
        commit_ts: 100,
        batch: b.batch(),
    }
    .encode(&mut rec);

    let taken = WalRecord::decode_with(&rec, true).unwrap();
    let batch = taken.batch().unwrap();
    assert!(batch.is_silent());
    assert_eq!(batch.len(), 2);
    assert_eq!(batch.iter().count(), 2);
    assert_eq!(
        taken,
        WalRecord::Batch {
            seqno: 9,
            commit_ts: 100,
            batch: b.batch()
        }
    );

    for refused in [WalRecord::decode(&rec), WalRecord::decode_with(&rec, false)] {
        assert!(matches!(refused, Err(Error::Corrupt { .. })), "{refused:?}");
    }

    // As a build before D331 reads it: the whole count as mutations, so the batch ends
    // long before two billion of them.
    let mut old = bytes.clone();
    let plain = raw.to_le_bytes();
    old[..4].copy_from_slice(&plain);
    let mut r = &old[4..];
    let mut parsed = 0u64;
    while !r.is_empty() && parsed < u64::from(raw) {
        // Skip one mutation by re-encoding it as a lone batch would: table, family, kind
        // (and ts), then three varint-length fields.
        let flags = r[8];
        r = &r[9 + if flags & 0x80 != 0 { 8 } else { 0 }..];
        for _ in 0..3 {
            let (len, used) = varint::get_u64(r).unwrap();
            r = &r[used + len as usize..];
        }
        parsed += 1;
    }
    assert!(
        parsed < u64::from(raw),
        "an old build runs out of bytes: corrupt"
    );

    // Cleared, the builder's next batch is ordinary.
    b.clear();
    assert!(!b.is_silent());
    b.push(
        TableId(1),
        FamilyId(2),
        Kind::Put,
        b"r",
        b"q",
        None,
        b"\x00v",
    )
    .unwrap();
    assert!(!b.batch().is_silent());
    let mut rec = Vec::new();
    WalRecord::Batch {
        seqno: 10,
        commit_ts: 100,
        batch: b.batch(),
    }
    .encode(&mut rec);
    assert!(WalRecord::decode(&rec).is_ok());
}

/// D329 2b (ICR 0140 amendment 2): a batch's xid trailer, flagged by the count's bit 30.
/// It reads back with the mutations as they were, beside the silent mark or without it.
/// Every count reader masks both bits, and a reader that knows the file lacks `incompat`
/// bit 2 refuses it.
#[test]
fn an_xid_trailer_reads_back_beside_the_silent_mark_and_is_refused_without_its_bit() {
    use pigeonhole_format::wal::{BatchBuilder, BatchRef, SILENT_BATCH, WalRecord, XID_BATCH};
    use pigeonhole_format::{FamilyId, Kind, TableId};

    let batch = |silent: bool, xid: Option<&[u8]>| {
        let mut b = BatchBuilder::new();
        for row in [&b"a"[..], b"b", b"c"] {
            b.push(
                TableId(1),
                FamilyId(2),
                Kind::Put,
                row,
                b"q",
                Some(5),
                b"\x00v",
            )
            .unwrap();
        }
        if silent {
            b.mark_silent();
        }
        if let Some(x) = xid {
            b.tag_xid(x).unwrap();
        }
        b
    };
    let xid = b"p2:h7-0123456789abcdef0123456789abcdef";
    for silent in [false, true] {
        let b = batch(silent, Some(xid));
        let bytes = b.batch().as_bytes().to_vec();
        let raw = u32::from_le_bytes(bytes[..4].try_into().unwrap());
        assert_eq!(raw & XID_BATCH, XID_BATCH, "the trailer's bit");
        assert_eq!(raw & SILENT_BATCH != 0, silent, "the silent mark beside it");
        assert_eq!(
            *bytes.last().unwrap() as usize,
            xid.len(),
            "its length last"
        );
        let parsed = BatchRef::new(&bytes).unwrap();
        assert_eq!(parsed.len(), 3, "the count masks both bits");
        assert_eq!(parsed.iter().count(), 3, "the walk stops at the trailer");
        assert_eq!(parsed.xid(), Some(&xid[..]));
        assert_eq!(parsed.is_silent(), silent);
        // In a record: taken with the bit, refused without it.
        let mut rec = Vec::new();
        WalRecord::Batch {
            seqno: 9,
            commit_ts: 100,
            batch: b.batch(),
        }
        .encode(&mut rec);
        let taken = WalRecord::decode_flags(&rec, true, true).unwrap();
        assert_eq!(taken.batch().unwrap().xid(), Some(&xid[..]));
        assert!(WalRecord::decode_flags(&rec, true, false).is_err());
        let mut rec = Vec::new();
        WalRecord::Prepare {
            seqno: 9,
            commit_ts: 100,
            coordinator: pigeonhole_format::StreamId(0),
            batch: b.batch(),
        }
        .encode(&mut rec);
        assert!(WalRecord::decode_flags(&rec, true, false).is_err());
        assert_eq!(
            WalRecord::decode_flags(&rec, true, true)
                .unwrap()
                .batch()
                .unwrap()
                .xid(),
            Some(&xid[..])
        );
    }
    // An untagged batch has none; a second tag, an empty xid or one over 255 bytes is refused.
    let plain = batch(false, None);
    assert_eq!(plain.batch().xid(), None);
    let mut b = batch(false, Some(xid));
    assert!(b.tag_xid(b"again").is_err());
    let mut b = batch(false, None);
    assert!(b.tag_xid(b"").is_err());
    assert!(b.tag_xid(&[b'x'; 256]).is_err());
    // A trailer whose length runs past the batch, or a zero length, is corrupt.
    let mut bytes = batch(false, Some(xid)).batch().as_bytes().to_vec();
    *bytes.last_mut().unwrap() = 250;
    assert!(BatchRef::new(&bytes).is_err());
    *bytes.last_mut().unwrap() = 0;
    assert!(BatchRef::new(&bytes).is_err());
    // Bytes between the last mutation and the trailer are still trailing bytes.
    let mut b = batch(false, None);
    let mut bytes = b.batch().as_bytes().to_vec();
    bytes.push(0xEE);
    bytes.extend_from_slice(xid);
    bytes.push(xid.len() as u8);
    bytes[3] |= (XID_BATCH >> 24) as u8;
    assert!(BatchRef::new(&bytes).is_err());
    b.clear();
    assert_eq!(b.batch().xid(), None, "clear drops the tag");
}

/// A count near the mask's cap round-trips with both flags set and with either, and a push
/// past the cap is refused before it reaches a flag bit (perf2's point 4 on 2b).
#[test]
fn a_count_near_its_cap_keeps_its_flags_apart() {
    use pigeonhole_format::wal::{COUNT_FLAGS, COUNT_MASK, SILENT_BATCH, XID_BATCH};
    let cap = COUNT_MASK & !COUNT_FLAGS;
    assert_eq!(cap, (1 << 30) - 1);
    for flags in [0, SILENT_BATCH, XID_BATCH, SILENT_BATCH | XID_BATCH] {
        let raw = cap | flags;
        assert_eq!(raw & COUNT_MASK, cap, "{flags:#x}");
        assert_eq!(raw & COUNT_FLAGS, flags);
    }
}

/// perf2's S1 on #2076: the xid trailer is a batch's last step; a push after it would land
/// past the trailer and drop its flag, so debug builds refuse it.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "a push after the batch's xid trailer")]
fn a_push_after_the_xid_trailer_is_refused_in_debug_builds() {
    use pigeonhole_format::wal::BatchBuilder;
    use pigeonhole_format::{FamilyId, Kind, TableId};
    let mut b = BatchBuilder::new();
    b.push(
        TableId(1),
        FamilyId(2),
        Kind::Put,
        b"a",
        b"q",
        None,
        b"\x00v",
    )
    .unwrap();
    b.tag_xid(b"p1:h1-aa").unwrap();
    let _ = b.push(
        TableId(1),
        FamilyId(2),
        Kind::Put,
        b"b",
        b"q",
        None,
        b"\x00v",
    );
}

/// feed1248's condition 5 on #2076: an External Prepare's batch never carries an xid
/// trailer (its share names the xid): its share, and the record holding it, are `Corrupt`
/// on every read, whatever bits the database has.
#[test]
fn an_external_prepares_batch_with_an_xid_trailer_is_corrupt() {
    use pigeonhole_format::wal::{BatchBuilder, ExternalShare, WalRecord};
    use pigeonhole_format::{FamilyId, Kind, StreamId, TableId};
    let share = |tagged: bool| {
        let mut b = BatchBuilder::new();
        b.push(
            TableId(1),
            FamilyId(2),
            Kind::Put,
            b"a",
            b"q",
            None,
            b"\x00v",
        )
        .unwrap();
        if tagged {
            b.tag_xid(b"p1:h3-00ff").unwrap();
        }
        let mut body = Vec::new();
        ExternalShare::encode(
            b"p1:h3-00ff",
            &[StreamId(0), StreamId(5)],
            std::iter::empty(),
            Some(b.batch()),
            &mut body,
        )
        .unwrap();
        body
    };
    let (plain, tagged) = (share(false), share(true));
    assert!(ExternalShare::decode(&plain, true).is_ok());
    assert!(ExternalShare::decode(&tagged, true).is_err(), "the share");
    // The whole record: an untagged one's head, the tagged share's body.
    let mut rec = Vec::new();
    WalRecord::ExternalPrepare {
        seqno: 104,
        coordinator: StreamId(0),
        share: ExternalShare::decode(&plain, true).unwrap(),
    }
    .encode(&mut rec);
    assert!(WalRecord::decode_flags(&rec, true, true).is_ok());
    rec.truncate(rec.len() - plain.len());
    rec.extend_from_slice(&tagged);
    for (silent_ok, xid_ok) in [(true, true), (true, false), (false, false)] {
        assert!(
            WalRecord::decode_flags(&rec, silent_ok, xid_ok).is_err(),
            "decode_flags({silent_ok}, {xid_ok}) took a tagged External Prepare"
        );
    }
}

/// D331 PR 4a: a range drop's bounds are checked on both sides: an end at or before the
/// start, or a row over `MAX_KEY_PART`, is refused when encoding and `Corrupt` when
/// decoding (a hand-built body), and a body with bytes left over is `Corrupt`.
#[test]
fn a_dropped_range_checks_its_bounds() {
    use pigeonhole_format::key::MAX_KEY_PART;
    use pigeonhole_format::wal::DroppedRange;
    use pigeonhole_format::{Error, TableId};
    let mut out = Vec::new();
    for (start, end) in [(&b"m"[..], &b"m"[..]), (b"m", b"a")] {
        assert!(matches!(
            DroppedRange::encode(&mut out, TableId(1), start, Some(end)),
            Err(Error::InvalidArgument { .. })
        ));
    }
    let long = vec![b'k'; MAX_KEY_PART + 1];
    assert!(matches!(
        DroppedRange::encode(&mut out, TableId(1), &long, None),
        Err(Error::KeyTooLarge)
    ));
    assert!(out.is_empty(), "a refused range writes nothing");
    // Hand-built: table, start "m", end flag 1, end "a" (before the start).
    let backwards = [1, 0, 0, 0, 1, b'm', 1, 1, b'a'];
    let flag = [1, 0, 0, 0, 0, 2];
    let mut trailing = Vec::new();
    DroppedRange::encode(&mut trailing, TableId(1), b"a", None).unwrap();
    trailing.push(0);
    for body in [&backwards[..], &flag[..], &trailing[..]] {
        assert!(
            matches!(DroppedRange::decode(body), Err(Error::Corrupt { .. })),
            "{body:?}"
        );
    }
}

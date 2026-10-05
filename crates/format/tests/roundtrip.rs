//! Every encoded type decodes to what was encoded.

mod common;

use common::{bytes, config, edit, extent};
use pigeonhole_format::blob::{BlobExtentHeader, encode_record_header, verify_record};
use pigeonhole_format::block::BlockAddr;
use pigeonhole_format::filter::{Filter, FilterBuilder};
use pigeonhole_format::manifest::{
    Edit, ManifestBlockKind, ManifestHeader, decode_block, encode_block,
};
use pigeonhole_format::shm::{ShmHeader, ViewMemtable, ViewRecord, ViewTablet};
use pigeonhole_format::sst::{Footer, Properties};
use pigeonhole_format::superblock::Superblock;
use pigeonhole_format::value::{BlobPointer, ValueRef, decode_value, encode_value};
use pigeonhole_format::varint;
use pigeonhole_format::wal::{
    BatchBuilder, BatchRef, FRAME_SIZE, SegmentHeader, StreamList, WalRecord,
};
use pigeonhole_format::{BlobFileId, FamilyId, FormatVersion, Kind, StreamId, TableId, TabletId};
use proptest::collection::vec;
use proptest::prelude::*;

fn addr() -> impl Strategy<Value = BlockAddr> {
    (any::<u64>(), any::<u32>()).prop_map(|(offset, len)| BlockAddr { offset, len })
}

fn value() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        bytes(30).prop_map(|b| {
            let mut v = Vec::new();
            encode_value(&mut v, ValueRef::Bytes(&b));
            v
        }),
        any::<i64>().prop_map(|i| enc(ValueRef::I64(i))),
        any::<f64>().prop_map(|f| enc(ValueRef::F64(f))),
        any::<i64>().prop_map(|i| enc(ValueRef::Varint(i))),
        (any::<u32>(), any::<u32>(), any::<u64>()).prop_map(|(b, len, offset)| enc(
            ValueRef::Blob(BlobPointer {
                blob_file: BlobFileId(b),
                len,
                offset
            })
        )),
    ]
}

fn enc(v: ValueRef<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value(&mut out, v);
    out
}

/// A mutation as pushed into a `BatchBuilder`.
type Mut = (u32, u32, Kind, Vec<u8>, Vec<u8>, Option<u64>, Vec<u8>);

fn mutation() -> impl Strategy<Value = Mut> {
    (
        any::<u32>(),
        any::<u32>(),
        common::kind(),
        common::part(),
        common::part(),
        proptest::option::of(any::<u64>()),
        value(),
    )
        .prop_map(|(t, f, kind, row, qual, ts, v)| {
            let qual = if kind == Kind::FamilyDelete {
                Vec::new()
            } else {
                qual
            };
            let v = if kind.is_delete() { Vec::new() } else { v };
            (t, f, kind, row, qual, ts, v)
        })
}

fn batch(muts: &[Mut]) -> BatchBuilder {
    let mut b = BatchBuilder::new();
    for (t, f, kind, row, qual, ts, v) in muts {
        b.push(TableId(*t), FamilyId(*f), *kind, row, qual, *ts, v)
            .unwrap();
    }
    b
}

proptest! {
    #![proptest_config(config(500))]

    #[test]
    fn varints(v in any::<u64>(), b in bytes(300)) {
        let mut out = Vec::new();
        varint::put_u64(&mut out, v);
        prop_assert!(out.len() <= varint::MAX_VARINT_LEN);
        prop_assert_eq!(varint::get_u64(&out).unwrap(), (v, out.len()));
        prop_assert_eq!(varint::get_u32(&out).is_ok(), v <= u64::from(u32::MAX));
        let mut out = Vec::new();
        varint::put_bytes(&mut out, &b);
        prop_assert_eq!(varint::get_bytes(&out).unwrap(), (b.as_slice(), out.len()));
    }

    #[test]
    fn values(v in value()) {
        let decoded = decode_value(&v).unwrap();
        // Compare via re-encoding so NaN payloads compare bit for bit.
        prop_assert_eq!(enc(decoded), v);
    }

    #[test]
    fn block_addr(a in addr()) {
        let mut out = Vec::new();
        a.encode_varint(&mut out);
        prop_assert_eq!(BlockAddr::decode_varint(&out).unwrap(), a);
    }

    #[test]
    fn footer(a in vec(addr(), 5), flags in any::<u32>()) {
        let f = Footer { top_index: a[0], row_filter: a[1], column_filter: a[2], properties: a[3], compression_dict: a[4], version: FormatVersion::CURRENT, flags };
        let bytes = f.encode();
        prop_assert_eq!(Footer::decode(&bytes).unwrap(), f);
        // Decoding the tail of a longer buffer works too.
        let sst = [vec![1; 7], bytes.to_vec()].concat();
        prop_assert_eq!(Footer::decode(&sst).unwrap(), f);
    }

    #[test]
    fn properties(n in any::<[u64; 14]>(), small in bytes(40), large in bytes(40), op in "\\PC{0,12}", extra in bytes(8)) {
        let p = Properties {
            table: TableId(n[0] as u32), family: FamilyId(n[1] as u32), tablet: TabletId(n[2]),
            entries: n[3], rows: n[4], deletes: n[5], merges: n[6], raw_key_bytes: n[7], raw_value_bytes: n[8],
            data_blocks: n[9] as u32, index_partitions: (n[9] >> 32) as u32,
            seqno_range: (n[10], n[11]), ts_range: (n[12], n[13]), created_micros: n[0] ^ n[13],
            smallest_key: small, largest_key: large, merge_operator: op,
        };
        let mut out = Vec::new();
        p.encode(&mut out);
        prop_assert_eq!(&Properties::decode(&out).unwrap(), &p);
        out.extend_from_slice(&extra); // appended future fields are ignored
        prop_assert_eq!(Properties::decode(&out).unwrap(), p);
    }

    #[test]
    fn filter_has_no_false_negatives(hashes in vec(any::<u64>(), 0..400), bpk in 0u8..30) {
        let mut b = FilterBuilder::new(bpk);
        for &h in &hashes {
            b.add_hash(h);
        }
        let mut block = Vec::new();
        b.finish(&mut block);
        let f = Filter::new(block.as_slice()).unwrap();
        for &h in &hashes {
            prop_assert!(f.may_contain(h));
        }
    }

    #[test]
    fn blob(file in any::<u32>(), index in any::<u32>(), value in bytes(100)) {
        let h = BlobExtentHeader { version: FormatVersion::CURRENT, blob_file: BlobFileId(file), extent_index: index };
        prop_assert_eq!(BlobExtentHeader::decode(&h.encode()).unwrap(), h);
        let rh = encode_record_header(&value);
        prop_assert!(verify_record(&rh, &value, value.len() as u32).is_ok());
        prop_assert!(verify_record(&rh, &value, value.len() as u32 + 1).is_err());
    }

    #[test]
    fn superblock(n in any::<[u64; 5]>(), db_id in any::<[u8; 16]>(), lens in any::<(u32, u32)>(), snap in proptest::option::of(extent()), log in proptest::option::of(extent())) {
        let sb = Superblock {
            version: FormatVersion::CURRENT, page_size: 4096, sequence: n[0], db_id,
            snapshot: snap, snapshot_len: lens.0, log, log_len: lens.1,
            manifest_version: n[1], file_pages: n[2], flags: n[3],
        };
        let mut page = [0xEEu8; 4096];
        sb.encode(&mut page);
        prop_assert!(page[128..].iter().all(|&b| b == 0));
        prop_assert_eq!(Superblock::decode(&page).unwrap(), sb);
    }

    #[test]
    fn manifest_edits_and_blocks(edits in vec(edit(), 0..20), version in any::<u64>(), snapshot in any::<bool>()) {
        for e in &edits {
            let mut out = Vec::new();
            e.encode(&mut out);
            prop_assert_eq!(Edit::decode(&out).unwrap(), (e.clone(), out.len()));
        }
        let kind = if snapshot { ManifestBlockKind::Snapshot } else { ManifestBlockKind::Delta };
        let header = ManifestHeader { version: FormatVersion::CURRENT, kind, manifest_version: version, edit_count: 0, body_len: 0 };
        // Two consecutive blocks in one log.
        let mut log = Vec::new();
        encode_block(&header, &edits, &mut log);
        let first_len = log.len();
        encode_block(&ManifestHeader { manifest_version: version.wrapping_add(1), ..header }, &edits[..edits.len() / 2], &mut log);
        let (h, back, n) = decode_block(&log).unwrap();
        prop_assert_eq!((h.kind, h.manifest_version, h.edit_count as usize, n), (kind, version, edits.len(), first_len));
        prop_assert_eq!(&back, &edits);
        let (h2, back2, n2) = decode_block(&log[n..]).unwrap();
        prop_assert_eq!(h2.manifest_version, version.wrapping_add(1));
        prop_assert_eq!(&back2[..], &edits[..edits.len() / 2]);
        prop_assert_eq!(n + n2, log.len());
    }

    #[test]
    fn segment_header(n in any::<[u32; 5]>(), db_id in any::<[u8; 16]>(), frames in 2u64..=131072) {
        let h = SegmentHeader {
            version: FormatVersion::CURRENT, stream: StreamId(n[0]), epoch: n[1], prev_epoch: n[2], prev_end: n[3],
            db_id, segment_size: frames * FRAME_SIZE as u64,
        };
        let mut frame = Box::new([0xEEu8; FRAME_SIZE]);
        h.encode(&mut frame);
        prop_assert!(frame[60..].iter().all(|&b| b == 0));
        prop_assert_eq!(SegmentHeader::decode(&frame[..]).unwrap(), h);
    }

    #[test]
    fn wal_records(muts in vec(mutation(), 0..10), seqno in any::<u64>(), ts in any::<u64>(), coord in any::<u32>(), streams in vec(any::<u32>(), 0..8)) {
        let b = batch(&muts);
        prop_assert_eq!(b.len(), muts.len());
        let decoded: Vec<_> = b.batch().iter().map(Result::unwrap).collect();
        prop_assert_eq!(decoded.len(), muts.len());
        for (m, (t, f, kind, row, qual, mts, v)) in decoded.iter().zip(&muts) {
            prop_assert_eq!((m.table.0, m.family.0, m.kind, m.row, m.qualifier, m.ts, m.value), (*t, *f, *kind, row.as_slice(), qual.as_slice(), *mts, v.as_slice()));
        }
        prop_assert_eq!(BatchRef::new(b.batch().as_bytes()).unwrap(), b.batch());

        let streams: Vec<_> = streams.into_iter().map(StreamId).collect();
        let mut list = Vec::new();
        StreamList::encode(&streams, &mut list);
        let list = StreamList::new(&list).unwrap();
        prop_assert_eq!(list.iter().collect::<Vec<_>>(), streams);

        for rec in [
            WalRecord::Batch { seqno, commit_ts: ts, batch: b.batch() },
            WalRecord::Prepare { seqno, commit_ts: ts, coordinator: StreamId(coord), batch: b.batch() },
            WalRecord::Commit { seqno, participants: list },
        ] {
            let mut out = Vec::new();
            rec.encode(&mut out);
            prop_assert_eq!(WalRecord::decode(&out).unwrap(), rec);
        }
    }

    #[test]
    fn shm_header(db_id in any::<[u8; 16]>(), shards in 1u32..64, slots in 0u32..512, view in 0u32..(8 << 20), arena in 1u64..(256 << 20), dev in any::<u64>(), ino in any::<u64>()) {
        let h = ShmHeader::layout(db_id, shards, slots, view, arena, dev, ino);
        prop_assert!(h.arena_len >= arena && h.region_len == h.arenas_off + h.arena_len * u64::from(shards));
        let mut page = [0xEEu8; 4096];
        h.encode(&mut page);
        prop_assert_eq!(ShmHeader::decode(&page).unwrap(), h);
    }

    #[test]
    fn view_record(
        versions in any::<(u64, u64)>(),
        tablets in vec((any::<u64>(), any::<u32>(), any::<u16>(), bytes(20), proptest::option::of(bytes(20))), 0..10),
        memtables in vec((any::<u64>(), any::<u32>(), any::<u16>(), any::<u8>(), any::<u32>()), 0..10),
    ) {
        let v = ViewRecord {
            view_version: versions.0,
            manifest_version: versions.1,
            tablets: tablets.into_iter().map(|(tablet, table, shard, start, end)| ViewTablet { tablet: TabletId(tablet), table: TableId(table), shard, start, end }).collect(),
            memtables: memtables.into_iter().map(|(tablet, family, shard, age, root)| ViewMemtable { tablet: TabletId(tablet), family: FamilyId(family), shard, age, root }).collect(),
        };
        let mut out = vec![1, 2, 3]; // encode appends; alignment is relative to the record
        v.encode(&mut out);
        prop_assert_eq!(out.len() - 3, v.encoded_len());
        prop_assert_eq!((out.len() - 3) % 8, 0);
        let mut buffer = out[3..].to_vec();
        buffer.resize(buffer.len() + 100, 0xEE); // a whole view buffer
        prop_assert_eq!(ViewRecord::decode(&buffer).unwrap(), v);
    }
}

#[test]
fn lsn_and_names() {
    let lsn = pigeonhole_format::Lsn::new(u32::MAX, 7);
    assert_eq!((lsn.epoch(), lsn.offset()), (u32::MAX, 7));
    let dir = pigeonhole_format::shm::directory_name(1, 2);
    assert!(dir.starts_with("phdb-") && dir.len() == 21);
    assert!(
        dir[5..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    );
    assert!(pigeonhole_format::shm::region_name(1, 2, (1 << 36) - 1).len() <= 31);
}

#[test]
fn manifest_skips_unknown_edits() {
    let header = ManifestHeader {
        version: FormatVersion::CURRENT,
        kind: ManifestBlockKind::Delta,
        manifest_version: 3,
        edit_count: 0,
        body_len: 0,
    };
    let known = Edit::DropTable { table: TableId(4) };
    let mut block = Vec::new();
    encode_block(&header, &[known.clone(), known.clone()], &mut block);
    // Replace the first edit with an unknown tag 200 of the same length, then fix the checksum.
    block[64] = 200;
    let body = block[64..].to_vec();
    let mut fixed = Vec::new();
    encode_block(&header, &[], &mut fixed);
    fixed.truncate(64);
    fixed[40..44].copy_from_slice(&2u32.to_le_bytes());
    fixed[44..48].copy_from_slice(&(body.len() as u32).to_le_bytes());
    fixed.extend_from_slice(&body);
    let sum = pigeonhole_format::checksum::xxh3_64(&[&fixed[..48], &fixed[64..]].concat());
    fixed[48..56].copy_from_slice(&sum.to_le_bytes());
    let (_, edits, _) = decode_block(&fixed).unwrap();
    assert_eq!(edits, [known]);
}

#[test]
fn versions_are_checked() {
    let f = Footer {
        top_index: BlockAddr::default(),
        row_filter: BlockAddr::default(),
        column_filter: BlockAddr::default(),
        properties: BlockAddr::default(),
        compression_dict: BlockAddr::default(),
        version: FormatVersion(2),
        flags: 0,
    };
    assert_eq!(
        Footer::decode(&f.encode()),
        Err(pigeonhole_format::Error::UnsupportedVersion {
            what: "sst footer",
            found: 2
        })
    );
    let mut page = [0u8; 4096];
    let h = ShmHeader {
        layout_version: pigeonhole_format::ShmLayoutVersion(2),
        ..ShmHeader::layout([0; 16], 1, 1, 64, 1, 0, 0)
    };
    h.encode(&mut page);
    assert!(matches!(
        ShmHeader::decode(&page),
        Err(pigeonhole_format::Error::UnsupportedVersion { found: 2, .. })
    ));
    // A newer superblock is reported as such even if the other copy is garbage.
    let sb = Superblock {
        version: FormatVersion(9),
        page_size: 4096,
        sequence: 1,
        db_id: [0; 16],
        snapshot: None,
        snapshot_len: 0,
        log: None,
        log_len: 0,
        manifest_version: 0,
        file_pages: 16,
        flags: 0,
    };
    let mut page = [0u8; 4096];
    sb.encode(&mut page);
    let err =
        Superblock::choose(Superblock::decode(&[0; 4096]), Superblock::decode(&page)).unwrap_err();
    assert!(matches!(
        err,
        pigeonhole_format::Error::UnsupportedVersion { found: 9, .. }
    ));
}

#[test]
fn batch_builder_validates() {
    let mut b = BatchBuilder::default();
    assert!(b.is_empty());
    assert_eq!(b.batch().as_bytes(), [0, 0, 0, 0]);
    assert!(
        b.push(
            TableId(1),
            FamilyId(1),
            Kind::CellDelete,
            b"r",
            b"q",
            None,
            b"\0v"
        )
        .is_err()
    );
    assert!(
        b.push(
            TableId(1),
            FamilyId(1),
            Kind::FamilyDelete,
            b"r",
            b"q",
            None,
            b""
        )
        .is_err()
    );
    let big = vec![0; pigeonhole_format::key::MAX_KEY_PART + 1];
    assert_eq!(
        b.push(TableId(1), FamilyId(1), Kind::Put, &big, b"", None, b""),
        Err(pigeonhole_format::Error::KeyTooLarge)
    );
    b.push(
        TableId(1),
        FamilyId(1),
        Kind::Put,
        b"r",
        b"q",
        Some(5),
        b"\0v",
    )
    .unwrap();
    assert_eq!(b.len(), 1);
    b.clear();
    assert!(b.is_empty());
    assert_eq!(b.batch().as_bytes(), [0, 0, 0, 0]);
}

//! Golden files: one per record type, in `tests/golden/`. A test fails if any encoding
//! changes, and the frozen bytes must still decode to the same values.
//!
//! After an intentional format change (with a FORMAT.md update and a version bump where
//! needed), regenerate with `UPDATE_GOLDEN=1 cargo test -p pigeonhole-format --test golden`.

use std::path::PathBuf;

use pigeonhole_format::blob::{BlobExtentHeader, encode_record_header};
use pigeonhole_format::block::{Block, BlockAddr, BlockBuilder, BlockKind, seal, verify};
use pigeonhole_format::compress::Compression;
use pigeonhole_format::filter::{Filter, FilterBuilder, column_hash, row_hash};
use pigeonhole_format::key::{Kind, decode_key, encode_key, encode_marker_key, encode_seek_key};
use pigeonhole_format::manifest::{
    Edit, FamilyOptions, ManifestBlockKind, ManifestHeader, SstMeta, decode_block, encode_block,
};
use pigeonhole_format::shm::{
    ShmHeader, ViewMemtable, ViewRecord, ViewTablet, directory_name, region_name,
};
use pigeonhole_format::sst::{Footer, Properties};
use pigeonhole_format::superblock::{ExtentRef, Superblock};
use pigeonhole_format::value::{BlobPointer, ValueRef, decode_value, encode_value};
use pigeonhole_format::wal::{
    BatchBuilder, FRAME_SIZE, FrameEncoder, SegmentHeader, StreamList, WalRecord,
};
use pigeonhole_format::{
    BlobFileId, FamilyId, FormatVersion, Lsn, SstId, StreamId, TableId, TabletId,
};

fn check(name: &str, bytes: &[u8]) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        return;
    }
    let want = std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    assert!(
        want == bytes,
        "{name}: encoding changed (see the module docs to regenerate)"
    );
}

/// Strips a fixed-size buffer to its meaningful prefix after checking the rest is zero.
fn prefix(bytes: &[u8], len: usize) -> &[u8] {
    assert!(bytes[len..].iter().all(|&b| b == 0));
    &bytes[..len]
}

fn key(row: &[u8], qual: &[u8], ts: u64, seqno: u64, kind: Kind) -> Vec<u8> {
    let mut k = Vec::new();
    encode_key(&mut k, row, qual, ts, seqno, kind).unwrap();
    k
}

fn sample_keys() -> Vec<Vec<u8>> {
    let mut marker = Vec::new();
    encode_marker_key(
        &mut marker,
        b"com.example\x00page",
        1_700_000_000_000_000,
        41,
    )
    .unwrap();
    let mut keys = vec![
        marker,
        key(
            b"com.example\x00page",
            b"",
            1_700_000_000_000_000,
            42,
            Kind::Put,
        ),
        key(
            b"com.example\x00page",
            b"links:a\xFF",
            1_700_000_000_000_001,
            43,
            Kind::Merge,
        ),
        key(
            b"com.example\x00page",
            b"links:a\xFF",
            1_700_000_000_000_000,
            44,
            Kind::CellDelete,
        ),
        key(
            b"com.example\x00page",
            b"links:b",
            7,
            45,
            Kind::ColumnDelete,
        ),
        key(b"com.example\x00page\x00", b"q", 0, 1, Kind::Put),
        key(b"com.example/", b"q", u64::MAX, u64::MAX, Kind::Put),
    ];
    keys.sort();
    keys
}

#[test]
fn keys() {
    let mut out = Vec::new();
    for k in sample_keys() {
        pigeonhole_format::varint::put_bytes(&mut out, &k);
    }
    let mut seek = Vec::new();
    encode_seek_key(&mut seek, b"r\x00w", b"q", 10, 20).unwrap();
    pigeonhole_format::varint::put_bytes(&mut out, &seek);
    check("keys.bin", &out);
    for k in sample_keys() {
        decode_key(&k).unwrap();
    }
}

#[test]
fn values() {
    let mut out = Vec::new();
    let ptr = BlobPointer {
        blob_file: BlobFileId(3),
        len: 100_000,
        offset: 0x0102_0304_0506,
    };
    for v in [
        ValueRef::Bytes(b"hello\x00world"),
        ValueRef::I64(-2),
        ValueRef::F64(-1.5),
        ValueRef::Varint(-300),
        ValueRef::Blob(ptr),
    ] {
        let mut one = Vec::new();
        encode_value(&mut one, v);
        assert_eq!(decode_value(&one).unwrap(), v);
        pigeonhole_format::varint::put_bytes(&mut out, &one);
    }
    check("values.bin", &out);
    check("blob_pointer.bin", &ptr.encode());
}

fn data_block() -> Vec<u8> {
    let mut b = BlockBuilder::data(2);
    for (i, k) in sample_keys().iter().enumerate() {
        let mut v = Vec::new();
        encode_value(&mut v, ValueRef::I64(i as i64));
        b.add(k, &v).unwrap();
    }
    b.finish().to_vec()
}

#[test]
fn blocks() {
    let data = data_block();
    check("data_block.bin", &data);
    let block = Block::new(data.as_slice()).unwrap();
    assert_eq!((block.restart_count(), block.row_start_count()), (4, 3));

    let mut index = BlockBuilder::index();
    let mut addr = Vec::new();
    BlockAddr {
        offset: 0,
        len: 4096,
    }
    .encode_varint(&mut addr);
    index
        .add(b"com.example\x00\xFFpage\x00\x01", &addr)
        .unwrap();
    addr.clear();
    BlockAddr {
        offset: 4096,
        len: 300,
    }
    .encode_varint(&mut addr);
    index.add(b"com.example/\x00\x01q", &addr).unwrap();
    check("index_block.bin", index.finish());

    let mut sealed = Vec::new();
    seal(BlockKind::Data, Compression::None, &data, &mut sealed).unwrap();
    check("sealed_block_none.bin", &sealed);
    let mut sealed = Vec::new();
    seal(
        BlockKind::Index,
        Compression::Lz4,
        &[b'x'; 300],
        &mut sealed,
    )
    .unwrap();
    check("sealed_block_lz4.bin", &sealed);
    assert_eq!(verify(&sealed).unwrap().0.compression, Compression::Lz4);
}

#[test]
fn filter() {
    let mut b = FilterBuilder::new(10);
    for i in 0..100u32 {
        b.add_hash(row_hash(&i.to_le_bytes()));
        b.add_hash(column_hash(&i.to_be_bytes()));
    }
    let mut out = Vec::new();
    b.finish(&mut out);
    check("filter.bin", &out);
    let f = Filter::new(out.as_slice()).unwrap();
    assert!((0..100u32).all(|i| f.may_contain(row_hash(&i.to_le_bytes()))));
}

#[test]
fn sst() {
    let footer = Footer {
        top_index: BlockAddr {
            offset: 65536,
            len: 1234,
        },
        row_filter: BlockAddr {
            offset: 66770,
            len: 600,
        },
        column_filter: BlockAddr {
            offset: 67370,
            len: 1200,
        },
        properties: BlockAddr {
            offset: 68570,
            len: 150,
        },
        compression_dict: BlockAddr::default(),
        version: FormatVersion::CURRENT,
        flags: 0,
    };
    check("sst_footer.bin", &footer.encode());
    assert_eq!(Footer::decode(&footer.encode()).unwrap(), footer);
    let keys = sample_keys();
    let props = Properties {
        table: TableId(1),
        family: FamilyId(2),
        tablet: TabletId(3),
        entries: 7,
        rows: 3,
        deletes: 3,
        merges: 1,
        raw_key_bytes: 200,
        raw_value_bytes: 63,
        data_blocks: 1,
        index_partitions: 1,
        seqno_range: (1, u64::MAX),
        ts_range: (0, u64::MAX),
        created_micros: 1_700_000_000_000_000,
        smallest_key: keys[0].clone(),
        largest_key: keys[keys.len() - 1].clone(),
        merge_operator: "pigeonhole.i64_add".into(),
    };
    let mut out = Vec::new();
    props.encode(&mut out);
    check("sst_properties.bin", &out);
    assert_eq!(Properties::decode(&out).unwrap(), props);
}

#[test]
fn blob() {
    let h = BlobExtentHeader {
        version: FormatVersion::CURRENT,
        blob_file: BlobFileId(5),
        extent_index: 2,
    };
    check("blob_extent_header.bin", &h.encode());
    check(
        "blob_record_header.bin",
        &encode_record_header(b"a large value"),
    );
}

#[test]
fn superblock() {
    let sb = Superblock {
        version: FormatVersion::CURRENT,
        page_size: 4096,
        sequence: 77,
        db_id: *b"0123456789abcdef",
        snapshot: Some(ExtentRef {
            page: 32,
            size_class: 0,
        }),
        snapshot_len: 900,
        log: Some(ExtentRef {
            page: 64,
            size_class: 2,
        }),
        log_len: 1500,
        manifest_version: 12,
        file_pages: 4096,
        flags: 1,
    };
    let mut page = [0u8; 4096];
    sb.encode(&mut page);
    check("superblock.bin", prefix(&page, 128));
    assert_eq!(Superblock::decode(&page).unwrap(), sb);
}

fn all_edits() -> Vec<Edit> {
    let keys = sample_keys();
    vec![
        Edit::Counters {
            next_table: 2,
            next_family: 3,
            next_tablet: 4,
            next_sst: 5,
            next_blob_file: 6,
            seqno_ceiling: 1000,
            ts_floor: 1_700_000_000_000_000,
        },
        Edit::CreateTable {
            table: TableId(1),
            name: "pages".into(),
        },
        Edit::PutFamily {
            table: TableId(1),
            family: FamilyId(2),
            name: "links".into(),
            options: FamilyOptions {
                merge_operator: "pigeonhole.i64_add".into(),
                ..FamilyOptions::default()
            },
        },
        Edit::PutTablet {
            tablet: TabletId(3),
            table: TableId(1),
            start: Vec::new(),
            end: Some(b"m".to_vec()),
        },
        Edit::PutTablet {
            tablet: TabletId(4),
            table: TableId(1),
            start: b"m".to_vec(),
            end: None,
        },
        Edit::AddSst {
            tablet: TabletId(3),
            family: FamilyId(2),
            level: 1,
            meta: SstMeta {
                id: SstId(9),
                extent: ExtentRef {
                    page: 128,
                    size_class: 3,
                },
                len: 70000,
                smallest_key: keys[0].clone(),
                largest_key: keys[keys.len() - 1].clone(),
                seqno_range: (1, 45),
                ts_range: (0, u64::MAX),
                entries: 7,
                deletes: 3,
            },
        },
        Edit::RemoveSst {
            tablet: TabletId(3),
            family: FamilyId(2),
            sst: SstId(8),
        },
        Edit::SetFlushed {
            tablet: TabletId(3),
            family: FamilyId(2),
            seqno: 45,
        },
        Edit::WalCheckpoint {
            stream: StreamId(0),
            lsn: Lsn::new(4, 65536),
        },
        Edit::PutBlobFile {
            blob_file: BlobFileId(5),
            family: FamilyId(2),
            extents: vec![
                ExtentRef {
                    page: 1024,
                    size_class: 4,
                },
                ExtentRef {
                    page: 2048,
                    size_class: 4,
                },
            ],
            total_bytes: 2_000_000,
            live_bytes: 1_500_000,
        },
        Edit::DropBlobFile {
            blob_file: BlobFileId(4),
        },
        Edit::DropTablet {
            tablet: TabletId(2),
        },
        Edit::DropTable { table: TableId(0) },
    ]
}

#[test]
fn manifest() {
    let edits = all_edits();
    let header = |kind, manifest_version| ManifestHeader {
        version: FormatVersion::CURRENT,
        kind,
        manifest_version,
        edit_count: 0,
        body_len: 0,
    };
    let mut snapshot = Vec::new();
    encode_block(
        &header(ManifestBlockKind::Snapshot, 10),
        &edits,
        &mut snapshot,
    );
    check("manifest_snapshot.bin", &snapshot);
    let (_, back, _) = decode_block(&snapshot).unwrap();
    assert_eq!(back, edits);
    let mut log = Vec::new();
    encode_block(
        &header(ManifestBlockKind::Delta, 11),
        &edits[5..7],
        &mut log,
    );
    encode_block(
        &header(ManifestBlockKind::Delta, 12),
        &edits[7..8],
        &mut log,
    );
    check("manifest_delta_log.bin", &log);
}

fn batch() -> BatchBuilder {
    let mut b = BatchBuilder::new();
    let mut v = Vec::new();
    encode_value(&mut v, ValueRef::Bytes(b"<html>"));
    b.push(
        TableId(1),
        FamilyId(2),
        Kind::Put,
        b"com.example\x00page",
        b"body",
        None,
        &v,
    )
    .unwrap();
    v.clear();
    encode_value(&mut v, ValueRef::I64(1));
    b.push(
        TableId(1),
        FamilyId(3),
        Kind::Merge,
        b"com.example\x00page",
        b"hits",
        Some(99),
        &v,
    )
    .unwrap();
    b.push(
        TableId(1),
        FamilyId(4),
        Kind::FamilyDelete,
        b"com.example\x00page",
        b"",
        Some(98),
        b"",
    )
    .unwrap();
    b
}

#[test]
fn wal() {
    let header = SegmentHeader {
        version: FormatVersion::CURRENT,
        stream: StreamId(2),
        epoch: 9,
        prev_epoch: 8,
        prev_end: 1_048_000,
        db_id: *b"0123456789abcdef",
        segment_size: 64 << 20,
    };
    let mut frame = Box::new([0u8; FRAME_SIZE]);
    header.encode(&mut frame);
    check("wal_segment_header.bin", prefix(&frame[..], 60));

    let b = batch();
    let mut participants = Vec::new();
    StreamList::encode(&[StreamId(0), StreamId(5)], &mut participants).unwrap();
    let records = [
        (
            "wal_record_batch.bin",
            WalRecord::Batch {
                seqno: 100,
                commit_ts: 1_700_000_000_000_000,
                batch: b.batch(),
            },
        ),
        (
            "wal_record_prepare.bin",
            WalRecord::Prepare {
                seqno: 101,
                commit_ts: 1_700_000_000_000_001,
                coordinator: StreamId(0),
                batch: b.batch(),
            },
        ),
        (
            "wal_record_commit.bin",
            WalRecord::Commit {
                seqno: 101,
                participants: StreamList::new(&participants).unwrap(),
            },
        ),
    ];
    let mut frames = Vec::new();
    let mut enc = FrameEncoder::new(9, FRAME_SIZE as u64 * 2 - 100);
    for (name, rec) in &records {
        let mut out = Vec::new();
        rec.encode(&mut out);
        check(name, &out);
        assert_eq!(&WalRecord::decode(&out).unwrap(), rec);
        enc.encode(&out, &mut frames);
    }
    // Starting 100 bytes before a frame end, so the first record is split First/Last.
    check("wal_fragments.bin", &frames);
}

#[test]
fn shm() {
    let h = ShmHeader::layout(
        *b"0123456789abcdef",
        4,
        64,
        4 << 20,
        64 << 20,
        0x42,
        0x1234_5678,
    );
    let mut page = [0u8; 4096];
    h.encode(&mut page);
    check("shm_header.bin", prefix(&page, 160));
    let view = ViewRecord {
        view_version: 3,
        manifest_version: 12,
        tablets: vec![
            ViewTablet {
                tablet: TabletId(3),
                table: TableId(1),
                shard: 0,
                start: Vec::new(),
                end: Some(b"m".to_vec()),
            },
            ViewTablet {
                tablet: TabletId(4),
                table: TableId(1),
                shard: 1,
                start: b"m".to_vec(),
                end: None,
            },
        ],
        memtables: vec![
            ViewMemtable {
                tablet: TabletId(3),
                family: FamilyId(2),
                shard: 0,
                age: 0,
                root: 64,
            },
            ViewMemtable {
                tablet: TabletId(3),
                family: FamilyId(2),
                shard: 0,
                age: 1,
                root: 4096,
            },
        ],
    };
    let mut out = Vec::new();
    view.encode(&mut out);
    check("shm_view_record.bin", &out);
    assert_eq!(ViewRecord::decode(&out).unwrap(), view);
    let names = format!(
        "{}\n{}\n",
        directory_name(0x42, 0x1234_5678),
        region_name(0x42, 0x1234_5678, 7)
    );
    check("shm_names.txt", names.as_bytes());
}

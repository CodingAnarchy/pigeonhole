//! #496 (ICR 0058): a value of a persisted enum that this build does not know decodes to
//! `Error::UnsupportedFeature`, naming the enum and the value, never `Corrupt`. The bytes are
//! intact (each structure's checksum passes); a newer build wrote them.

use pigeonhole_format::block::{BlockKind, seal, verify};
use pigeonhole_format::checksum::xxh3_64;
use pigeonhole_format::compress::Compression;
use pigeonhole_format::filter::Filter;
use pigeonhole_format::manifest::{
    CachePriority, CompactionStyle, Edit, FamilyKind, FamilyOptions, ManifestBlockKind,
    ManifestHeader, decode_block, encode_block,
};
use pigeonhole_format::value::decode_value;
use pigeonhole_format::version::FormatVersion;
use pigeonhole_format::wal::WalRecord;
use pigeonhole_format::{Error, FamilyId, Kind, TableId};

#[track_caller]
fn unsupported<T: std::fmt::Debug>(r: Result<T, Error>, what: &str, value: u64) {
    match r {
        Err(Error::UnsupportedFeature { what: w, value: v }) => {
            assert_eq!((w, v), (what, value));
        }
        other => panic!("expected UnsupportedFeature {{ {what}, {value} }}, got {other:?}"),
    }
}

#[test]
fn key_kind_and_value_tag() {
    unsupported(Kind::from_u8(0x06), "key kind", 6);
    unsupported(Kind::from_u8(0xff), "key kind", 0xff);
    unsupported(decode_value(&[0x05, 1, 2, 3]), "value tag", 5);
    unsupported(decode_value(&[0x04, 9, 1, 0]), "vector element", 9);
    unsupported(decode_value(&[0x81]), "value tag", 0x81);
}

#[test]
fn block_kind_with_a_valid_checksum() {
    let mut physical = Vec::new();
    seal(
        BlockKind::Data,
        Compression::None,
        b"payload",
        &mut physical,
    )
    .unwrap();
    // A kind this build does not know, the checksum recomputed: intact bytes.
    let t = physical.len() - 16;
    physical[t] = 9;
    let sum = xxh3_64(&physical[..t + 8]);
    physical[t + 8..].copy_from_slice(&sum.to_le_bytes());
    unsupported(verify(&physical), "block kind", 9);
}

#[test]
fn filter_kind() {
    let mut bytes = vec![0u8; 8 + 64];
    bytes[0] = 2;
    bytes[1] = 7;
    bytes[4] = 1;
    unsupported(Filter::new(bytes), "filter kind", 2);
}

#[test]
fn manifest_block_kind_with_a_valid_checksum() {
    let header = ManifestHeader {
        version: FormatVersion::CURRENT,
        kind: ManifestBlockKind::Delta,
        manifest_version: 5,
        edit_count: 0,
        body_len: 0,
    };
    let mut out = Vec::new();
    encode_block(&header, &[], &mut out);
    out[12] = 3;
    let body_len = u32::from_le_bytes(out[44..48].try_into().unwrap()) as usize;
    let sum = xxh3_64(&[&out[..48], &out[64..64 + body_len]].concat());
    out[48..56].copy_from_slice(&sum.to_le_bytes());
    unsupported(decode_block(&out), "manifest block kind", 3);
}

/// A `PutFamily` edit with one family field set to a value no build defines: the byte that
/// differs between two encodings with different values of that field is the field.
fn family_edit_with(
    a: FamilyOptions,
    b: FamilyOptions,
    unknown: u8,
) -> Result<(Edit, usize), Error> {
    let encode = |options| {
        let mut out = Vec::new();
        Edit::PutFamily {
            table: TableId(1),
            family: FamilyId(2),
            name: "f".into(),
            options,
        }
        .encode(&mut out);
        out
    };
    let (x, y) = (encode(a), encode(b));
    let at = (0..x.len())
        .find(|&i| x[i] != y[i])
        .expect("the encodings differ in the field");
    let mut bytes = x;
    bytes[at] = unknown;
    Edit::decode(&bytes)
}

#[test]
fn family_settings() {
    let with = |set: &dyn Fn(&mut FamilyOptions)| {
        let mut o = FamilyOptions::default();
        set(&mut o);
        o
    };
    unsupported(
        family_edit_with(
            with(&|o| o.kind = FamilyKind::Standard),
            with(&|o| o.kind = FamilyKind::Counter),
            99,
        ),
        "family kind",
        99,
    );
    unsupported(
        family_edit_with(
            with(&|o| o.compaction = CompactionStyle::Leveled),
            with(&|o| o.compaction = CompactionStyle::Tiered),
            9,
        ),
        "family compaction style",
        9,
    );
    unsupported(
        family_edit_with(
            with(&|o| o.cache_priority = CachePriority::Low),
            with(&|o| o.cache_priority = CachePriority::High),
            5,
        ),
        "family cache priority",
        5,
    );
    unsupported(
        family_edit_with(
            with(&|o| o.compression = Compression::Lz4),
            with(&|o| o.compression = Compression::Zstd),
            8,
        ),
        "family compression",
        8,
    );
}

#[test]
fn wal_record_type() {
    unsupported(WalRecord::decode(&[9, 0, 0, 0]), "wal record type", 9);
}

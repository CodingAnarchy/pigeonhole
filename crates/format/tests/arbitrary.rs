//! Decoders never panic on arbitrary input, nor on valid encodings with damage.

mod common;

use common::{cell, config, edit, harness, sized};
use pigeonhole_format::block::{BlockBuilder, BlockKind, seal};
use pigeonhole_format::compress::Compression;
use pigeonhole_format::manifest::{ManifestBlockKind, ManifestHeader, encode_block};
use pigeonhole_format::wal::{BatchBuilder, FRAME_SIZE, FrameEncoder, WalRecord};
use pigeonhole_format::{FamilyId, FormatVersion, Kind, TableId};
use proptest::collection::vec;
use proptest::prelude::*;

/// Arbitrary bytes, biased towards the special values decoders branch on.
fn input() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        vec(any::<u8>(), 0..sized(600, 64)),
        vec(
            prop_oneof![Just(0u8), Just(1), Just(0x80), Just(0xFF), any::<u8>()],
            0..sized(600, 64)
        ),
    ]
}

/// Applies a few byte edits to a valid encoding.
fn damage(mut bytes: Vec<u8>, edits: &[(prop::sample::Index, u8)]) -> Vec<u8> {
    if !bytes.is_empty() {
        for (i, b) in edits {
            let i = i.index(bytes.len());
            bytes[i] = *b;
        }
    }
    bytes
}

proptest! {
    #![proptest_config(config(3000))]

    #[test]
    fn arbitrary_bytes(data in input()) {
        harness::all(&data);
    }

    #[test]
    fn damaged_keys_and_blocks(
        cells in vec(cell(), 1..sized(40, 4)),
        edits in vec((any::<prop::sample::Index>(), any::<u8>()), 1..4),
        interval in 1usize..8,
    ) {
        let mut keys: Vec<_> = cells.iter().map(|c| c.encode()).collect();
        keys.sort();
        keys.dedup();
        for k in &keys {
            harness::key(&damage(k.clone(), &edits));
        }
        let mut b = BlockBuilder::data(interval);
        for k in &keys {
            b.add(k, b"\x00value").unwrap();
        }
        let logical = b.finish().to_vec();
        harness::block(&damage(logical.clone(), &edits));
        let mut physical = Vec::new();
        seal(BlockKind::Data, Compression::Lz4, &logical, &mut physical).unwrap();
        harness::block(&damage(physical, &edits));
    }

    #[test]
    fn damaged_manifest_and_wal(
        edits_in in vec(edit(), 0..sized(10, 3)),
        edits in vec((any::<prop::sample::Index>(), any::<u8>()), 1..4),
    ) {
        let header = ManifestHeader { version: FormatVersion::CURRENT, kind: ManifestBlockKind::Snapshot, manifest_version: 1, edit_count: 0, body_len: 0 };
        let mut block = Vec::new();
        encode_block(&header, &edits_in, &mut block);
        harness::manifest(&damage(block, &edits));

        let mut batch = BatchBuilder::new();
        for e in &edits_in {
            let mut v = vec![0];
            e.encode(&mut v);
            batch.push(TableId(1), FamilyId(2), Kind::Put, &v, b"q", Some(3), &v).unwrap();
        }
        let mut rec = Vec::new();
        WalRecord::Batch { seqno: 1, commit_ts: 2, batch: batch.batch() }.encode(&mut rec);
        harness::wal(&damage(rec.clone(), &edits));
        let mut frames = Vec::new();
        FrameEncoder::new(7, FRAME_SIZE as u64).encode(&rec, &mut frames);
        harness::wal(&damage(frames, &edits));
    }
}

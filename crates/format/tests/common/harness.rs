//! Decoder harnesses that must never panic, whatever the bytes. Shared by the proptest
//! arbitrary-input tests and the cargo-fuzz targets in `crates/format/fuzz`.
//!
//! Checksummed structures are also fed with their checksum recomputed over the arbitrary
//! bytes, so the parser behind the checksum gets exercised, not just the checksum check.
#![allow(dead_code)]

use std::ops::Bound;

use pigeonhole_format::Cursor;
use pigeonhole_format::block::{Block, BlockAddr, verify};
use pigeonhole_format::checksum::{crc32c, xxh3_64};
use pigeonhole_format::compress::decompress;
use pigeonhole_format::filter::Filter;
use pigeonhole_format::key::{self, Escaped};
use pigeonhole_format::manifest::{Edit, decode_block};
use pigeonhole_format::scan::{QualifierFilter, ScanFilter};
use pigeonhole_format::wal::{
    BatchRef, Decoded, FRAGMENT_HEADER_LEN, FRAME_SIZE, FrameDecoder, SegmentHeader, StreamList,
    WalRecord,
};
use pigeonhole_format::{blob, shm, sst, superblock, value, varint};

/// Keys, values and the scan filter.
pub fn key(data: &[u8]) {
    if let Ok(p) = key::decode_key(data) {
        let mut out = Vec::new();
        p.row.unescape_into(&mut out);
        let _ = p.row.eq_raw(&out);
        if let Some(q) = p.qualifier {
            q.unescape_into(&mut out);
        }
    }
    let _ = key::split_suffix(data);
    let _ = key::row_prefix_len(data);
    let _ = key::column_prefix_len(data);
    let e = Escaped::new(data);
    let _ = (e.is_verbatim(), e.eq_raw(data));
    let split = data.len() / 2;
    let filters = [
        ScanFilter::all(),
        {
            let mut f = ScanFilter::all();
            f.qualifiers = QualifierFilter::Prefix(data[..split.min(3)].to_vec());
            f.time_range = Some((1, 1 << 40));
            f
        },
        {
            let mut f = ScanFilter::all();
            f.qualifiers = QualifierFilter::Range(
                Bound::Excluded(data[..split.min(2)].to_vec()),
                Bound::Included(data[split..].to_vec()),
            );
            f
        },
    ];
    for f in &filters {
        let _ = f.admits(data);
        let mut hint = Vec::new();
        let _ = f.next_admissible(data, &mut hint);
    }
    let _ = value::decode_value(data);
    let _ = value::BlobPointer::decode(data);
    let _ = varint::get_u64(data);
    let _ = varint::get_u32(data);
    let _ = varint::get_bytes(data);
    let _ = BlockAddr::decode_varint(data);
}

/// Logical blocks (cursor operations included), physical blocks, filters, SST footer and
/// properties.
pub fn block(data: &[u8]) {
    if let Ok(b) = Block::new(data) {
        let _ = (b.restart_count(), b.row_start_count(), b.validate());
        let mut it = b.into_cursor();
        let _ = it.seek_to_first();
        for _ in 0..64 {
            if !it.valid() {
                break;
            }
            let _ = (it.key(), it.value(), it.value_range(), it.entry_offset());
            if it.next().is_err() {
                break;
            }
        }
        let _ = it.seek_to_first();
        let _ = it.skip_row();
        let _ = it.seek(&data[..data.len().min(9)]);
        let _ = it.skip_row();
        let _ = it.next();
    }
    physical(data);
    let mut sealed = data.to_vec();
    if sealed.len() >= 8 {
        // Recompute the checksum so the trailer and payload are parsed.
        let n = sealed.len() - 8;
        let sum = xxh3_64(&sealed[..n]);
        sealed[n..].copy_from_slice(&sum.to_le_bytes());
        physical(&sealed);
    }
    if let Ok(f) = Filter::new(data) {
        let _ = f.may_contain(xxh3_64(data));
    }
    let mut filter = data.to_vec();
    if filter.len() >= 8 {
        // A well-formed header over arbitrary lines.
        filter[0] = 1;
        filter[1] = filter[1] % 16 + 1;
        let lines = ((filter.len() - 8) / 64) as u32;
        filter.truncate(8 + 64 * lines as usize);
        filter[4..8].copy_from_slice(&lines.to_le_bytes());
        if let Ok(f) = Filter::new(filter.as_slice()) {
            let _ = f.may_contain(xxh3_64(data));
        }
    }
    let _ = sst::Footer::decode(data);
    let _ = sst::Properties::decode(data);
}

fn physical(data: &[u8]) {
    if let Ok((t, payload)) = verify(data) {
        // Bound the buffer the way a reader would (blocks are far below this).
        if t.uncompressed_len <= 1 << 20 {
            let mut out = vec![0; t.uncompressed_len as usize];
            let _ = decompress(t.compression, payload, &mut out);
        }
    }
}

/// WAL segment headers, frames (with valid-looking fragment headers) and records.
pub fn wal(data: &[u8]) {
    let _ = SegmentHeader::decode(data);
    let _ = WalRecord::decode(data);
    let _ = BatchRef::new(data).map(|b| b.iter().count());
    let _ = StreamList::new(data).map(|l| l.iter().count());
    // Raw bytes as a frame.
    let mut frame = vec![0u8; FRAME_SIZE];
    let n = data.len().min(FRAME_SIZE);
    frame[..n].copy_from_slice(&data[..n]);
    decode_frames(&frame, 0);
    // The same bytes chopped into fragments with correct CRCs, so reassembly runs.
    let mut frame = vec![0u8; FRAME_SIZE];
    let mut pos = 0;
    let mut rest = data;
    while rest.len() >= 2 && pos + FRAGMENT_HEADER_LEN < FRAME_SIZE {
        let (ctl, tail) = rest.split_at(2);
        let len = usize::from(ctl[0])
            .min(tail.len())
            .min(FRAME_SIZE - pos - FRAGMENT_HEADER_LEN);
        let h = &mut frame[pos..pos + FRAGMENT_HEADER_LEN];
        h[4..8].copy_from_slice(&7u32.to_le_bytes());
        h[8..10].copy_from_slice(&(len as u16).to_le_bytes());
        h[10] = ctl[1] % 6;
        let mut covered = h[4..].to_vec();
        covered.extend_from_slice(&tail[..len]);
        let crc = crc32c(&covered);
        frame[pos..pos + 4].copy_from_slice(&crc.to_le_bytes());
        frame[pos + FRAGMENT_HEADER_LEN..pos + FRAGMENT_HEADER_LEN + len]
            .copy_from_slice(&tail[..len]);
        pos += FRAGMENT_HEADER_LEN + len;
        rest = &tail[len..];
    }
    decode_frames(&frame, 7);
}

fn decode_frames(frame: &[u8], epoch: u32) {
    let mut dec = FrameDecoder::new(epoch, FRAME_SIZE as u64);
    // The same frame twice: as frame 1 and as frame 2.
    for _ in 0..2 {
        // Every record is at least a fragment header long, so this bounds the loop.
        for _ in 0..FRAME_SIZE / FRAGMENT_HEADER_LEN + 1 {
            match dec.decode(frame) {
                Ok(Some(Decoded::Record { .. })) => {
                    let _ = WalRecord::decode(dec.record());
                }
                // `Stop` is sticky: once the data stops, nothing more will decode.
                Ok(Some(Decoded::Stop { .. })) => return,
                Ok(None) | Err(_) => break,
            }
        }
    }
}

/// Manifest edits and blocks, superblocks, blob headers, shared-memory structures.
pub fn manifest(data: &[u8]) {
    let _ = Edit::decode(data);
    let _ = decode_block(data);
    let mut block = data.to_vec();
    if block.len() >= 64 {
        // Fix the body length and checksum so the edits are parsed.
        let body = (block.len() - 64) as u32;
        block[44..48].copy_from_slice(&body.to_le_bytes());
        block[..8].copy_from_slice(b"PHDBMANI");
        block[8..12].copy_from_slice(&1u32.to_le_bytes());
        let sum = xxh3_64(&[&block[..48], &block[64..]].concat());
        block[48..56].copy_from_slice(&sum.to_le_bytes());
        let _ = decode_block(&block);
    }
    let _ = superblock::Superblock::choose(
        superblock::Superblock::decode(data),
        superblock::Superblock::decode(data.get(1..).unwrap_or_default()),
    );
    let _ = blob::BlobExtentHeader::decode(data);
    let _ = blob::verify_record(data, data.get(16..).unwrap_or_default(), data.len() as u32);
    let _ = shm::ShmHeader::decode(data);
    let _ = shm::ViewRecord::decode(data);
    let mut view = data.to_vec();
    if view.len() >= 32 {
        // Fix the length and CRC so the entries are parsed.
        let len = view.len() as u32;
        view[16..20].copy_from_slice(&len.to_le_bytes());
        view[28..32].fill(0);
        let crc = crc32c(&view);
        view[28..32].copy_from_slice(&crc.to_le_bytes());
        let _ = shm::ViewRecord::decode(&view);
    }
}

/// Every harness.
pub fn all(data: &[u8]) {
    key(data);
    block(data);
    wal(data);
    manifest(data);
}

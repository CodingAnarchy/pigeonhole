//! WAL fragmentation: records survive the trip through frames, torn tails stop exactly at the
//! end of the last complete record, and stale fragments from an older epoch are never read.

mod common;

use common::config;
use pigeonhole_format::wal::{
    Decoded, FRAGMENT_HEADER_LEN, FRAME_SIZE, FrameDecoder, FrameEncoder,
};
use proptest::collection::vec;
use proptest::prelude::*;

const FRAMES: usize = 8;

/// Records of sizes that fit in a frame, straddle one, or span several.
fn record() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        4 => vec(any::<u8>(), 0..64),
        1 => (FRAME_SIZE - 40..FRAME_SIZE + 40).prop_map(|n| vec![0x5A; n]),
        1 => (1usize..3 * FRAME_SIZE).prop_map(|n| (0..n).map(|i| i as u8).collect()),
    ]
}

/// Lays `records` out in a segment of `FRAMES` frames from `start`; returns the segment and
/// the end offset of every record. Stops before a record that would not fit.
fn write(epoch: u32, start: u64, records: &[Vec<u8>], fill: u8) -> (Vec<u8>, Vec<(u64, u64)>) {
    let mut seg = vec![fill; FRAMES * FRAME_SIZE];
    let mut enc = FrameEncoder::new(epoch, start);
    let mut pos = start;
    let mut spans = Vec::new();
    for r in records {
        let need = enc.encoded_len(r.len());
        if pos as usize + need > seg.len() {
            break;
        }
        let mut out = Vec::new();
        let end = enc.encode(r, &mut out);
        assert_eq!(out.len(), need);
        assert_eq!(end, pos + need as u64);
        seg[pos as usize..end as usize].copy_from_slice(&out);
        // The record's first fragment starts after any skipped frame tail.
        let first = if FRAME_SIZE - (pos as usize % FRAME_SIZE) < FRAGMENT_HEADER_LEN {
            pos.next_multiple_of(FRAME_SIZE as u64)
        } else {
            pos
        };
        spans.push((first, end));
        pos = end;
    }
    (seg, spans)
}

/// Replays a segment from `start`: the records with their offsets, and the stop offset (or
/// `None` if the frames ran out first).
fn read(seg: &[u8], epoch: u32, start: u64) -> (Vec<(u64, Vec<u8>)>, Option<u64>) {
    let mut dec = FrameDecoder::new(epoch, start);
    let mut out = Vec::new();
    let first_frame = start as usize / FRAME_SIZE;
    for frame in seg.chunks(FRAME_SIZE).skip(first_frame) {
        loop {
            match dec.decode(frame).unwrap() {
                Some(Decoded::Record { offset }) => out.push((offset, dec.record().to_vec())),
                Some(Decoded::Stop { offset }) => {
                    assert_eq!(
                        dec.decode(frame).unwrap(),
                        Some(Decoded::Stop { offset }),
                        "stop is sticky"
                    );
                    return (out, Some(offset));
                }
                None => break,
            }
        }
    }
    (out, None)
}

fn start_offset() -> impl Strategy<Value = u64> {
    prop_oneof![
        Just(FRAME_SIZE as u64),
        (1u64..20).prop_map(|k| 2 * FRAME_SIZE as u64 - k), // near a frame end
        (FRAME_SIZE as u64..3 * FRAME_SIZE as u64),
    ]
}

proptest! {
    #![proptest_config(config(300))]

    #[test]
    fn records_roundtrip(records in vec(record(), 0..12), epoch in 1u32.., start in start_offset()) {
        let (seg, spans) = write(epoch, start, &records, 0);
        let (got, stop) = read(&seg, epoch, start);
        prop_assert_eq!(got.len(), spans.len());
        for ((off, rec), (want, (first, _))) in got.iter().zip(records.iter().zip(&spans)) {
            prop_assert_eq!(rec, want);
            prop_assert_eq!(*off, *first);
        }
        let end = spans.last().map_or(start, |s| s.1);
        if let Some(stop) = stop {
            prop_assert_eq!(stop, end);
        }
    }

    /// Cutting the segment anywhere (zeros or stale bytes after the cut) yields a prefix of the
    /// records and a stop at the end of the last complete one.
    #[test]
    fn torn_tail_stops_at_last_complete_record(
        records in vec(record(), 1..10),
        epoch in 2u32..,
        start in start_offset(),
        cut in any::<prop::sample::Index>(),
        stale in any::<bool>(),
    ) {
        let (seg, spans) = write(epoch, start, &records, 0);
        let end = spans.last().map_or(start, |s| s.1) as usize;
        let cut = start as usize + cut.index(end - start as usize + 1);
        // What lies past the cut: zeros, or a valid-looking log from the previous epoch.
        let (old, _) = write(epoch - 1, FRAME_SIZE as u64, &[vec![7; 5000], vec![8; 50_000], vec![9; 10]], 0);
        let mut torn = seg.clone();
        for i in cut..torn.len() {
            torn[i] = if stale { old[i] } else { 0 };
        }
        let (got, stop) = read(&torn, epoch, start);
        let complete = spans.iter().take_while(|s| s.1 as usize <= cut).count();
        prop_assert_eq!(got.len(), complete);
        for ((_, rec), want) in got.iter().zip(&records) {
            prop_assert_eq!(rec, want);
        }
        let want_stop = if complete == 0 { start } else { spans[complete - 1].1 };
        prop_assert_eq!(stop, Some(want_stop));
    }

    /// A flipped bit inside any fragment stops replay at or before that record.
    #[test]
    fn corruption_is_detected(records in vec(vec(any::<u8>(), 1..3000), 1..20), bit in any::<prop::sample::Index>()) {
        let start = FRAME_SIZE as u64;
        let (mut seg, spans) = write(1, start, &records, 0);
        let end = spans.last().unwrap().1 as usize;
        let i = start as usize + bit.index(end - start as usize);
        seg[i] ^= 1;
        let (got, _) = read(&seg, 1, start);
        let hit = spans.iter().position(|s| (i as u64) < s.1).unwrap();
        // Everything before the damaged record survives and nothing damaged is returned,
        // unless the bit landed in a skipped frame tail (padding before a record).
        let in_padding = (i as u64) < spans[hit].0;
        prop_assert_eq!(got.len(), if in_padding { records.len() } else { hit });
        for ((_, rec), want) in got.iter().zip(&records) {
            prop_assert_eq!(rec, want);
        }
    }
}

#[test]
fn frame_tail_too_short_for_a_header_is_skipped() {
    // Leave exactly 11 bytes in frame 1, then write a record: it must start in frame 2.
    let start = 2 * FRAME_SIZE as u64 - 11;
    let mut enc = FrameEncoder::new(1, start);
    let mut out = Vec::new();
    assert_eq!(enc.encoded_len(3), 11 + FRAGMENT_HEADER_LEN + 3);
    let end = enc.encode(b"abc", &mut out);
    assert_eq!(end, 2 * FRAME_SIZE as u64 + FRAGMENT_HEADER_LEN as u64 + 3);
    assert!(out[..11].iter().all(|&b| b == 0));
    // The decoder skips the tail whatever it holds.
    let mut seg = vec![0u8; 3 * FRAME_SIZE];
    seg[start as usize..end as usize].copy_from_slice(&out);
    seg[start as usize..start as usize + 11].fill(0xFF);
    let (got, stop) = read(&seg, 1, start);
    assert_eq!(got, [(2 * FRAME_SIZE as u64, b"abc".to_vec())]);
    assert_eq!(stop, Some(end));
    // A frame of the wrong size is an error, not a panic.
    assert!(FrameDecoder::new(1, start).decode(&[0; 10]).is_err());
}

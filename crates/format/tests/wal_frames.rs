//! WAL fragmentation: records survive the trip through frames, torn tails stop exactly at the
//! end of the last complete record, and stale fragments from an older epoch are never read.

mod common;

use common::{config, sized};
use pigeonhole_format::wal::{
    Decoded, FRAGMENT_HEADER_LEN, FRAME_SIZE, FrameDecoder, FrameEncoder, Within,
};
use proptest::collection::vec;
use proptest::prelude::*;

/// Frames per test segment (fewer under Miri; records spanning frames are still covered).
const FRAMES: usize = sized(8, 3);

/// Records of sizes that fit in a frame, straddle one, or span several.
fn record() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        4 => vec(any::<u8>(), 0..64),
        1 => (FRAME_SIZE - 40..FRAME_SIZE + 40).prop_map(|n| vec![0x5A; n]),
        1 => (1usize..sized(3 * FRAME_SIZE, FRAME_SIZE + 100)).prop_map(|n| (0..n).map(|i| i as u8).collect()),
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
        let first = if FRAME_SIZE - (pos as usize % FRAME_SIZE) <= FRAGMENT_HEADER_LEN {
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
    fn records_roundtrip(records in vec(record(), 0..sized(12, 4)), epoch in 1u32.., start in start_offset()) {
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
        records in vec(record(), 1..sized(10, 4)),
        epoch in 2u32..,
        start in start_offset(),
        cut in any::<prop::sample::Index>(),
        stale in any::<bool>(),
    ) {
        let (seg, spans) = write(epoch, start, &records, 0);
        let end = spans.last().map_or(start, |s| s.1) as usize;
        let cut = start as usize + cut.index(end - start as usize + 1);
        // What lies past the cut: zeros, or a valid-looking log from the previous epoch.
        let (old, _) = write(epoch - 1, FRAME_SIZE as u64, &[vec![7; sized(5000, 500)], vec![8; sized(50_000, FRAME_SIZE + 100)], vec![9; 10]], 0);
        let mut torn = seg.clone();
        if stale {
            torn[cut..].copy_from_slice(&old[cut..]);
        } else {
            torn[cut..].fill(0);
        }
        let (got, stop) = read(&torn, epoch, start);
        // A record is complete if its bytes survived: usually it ended before the cut, but the
        // bytes written past the cut may happen to equal the originals (a zero or stale byte
        // matching a reserved byte), in which case the record is genuinely intact.
        let complete = spans
            .iter()
            .take_while(|s| torn[start as usize..s.1 as usize] == seg[start as usize..s.1 as usize])
            .count();
        prop_assert_eq!(got.len(), complete);
        for ((_, rec), want) in got.iter().zip(&records) {
            prop_assert_eq!(rec, want);
        }
        let want_stop = if complete == 0 { start } else { spans[complete - 1].1 };
        prop_assert_eq!(stop, Some(want_stop));
    }

    /// A flipped bit inside any fragment stops replay at or before that record.
    #[test]
    fn corruption_is_detected(records in vec(vec(any::<u8>(), 1..sized(3000, 300)), 1..sized(20, 4)), bit in any::<prop::sample::Index>()) {
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
    // Exactly 12 bytes left is also skipped: no zero-payload First fragment is written.
    let start12 = 2 * FRAME_SIZE as u64 - 12;
    let mut out = Vec::new();
    let end12 = FrameEncoder::new(1, start12).encode(b"abc", &mut out);
    assert_eq!(end12, end);
    assert!(out[..12].iter().all(|&b| b == 0));
    let mut seg = vec![0u8; 3 * FRAME_SIZE];
    seg[start12 as usize..end12 as usize].copy_from_slice(&out);
    assert_eq!(
        read(&seg, 1, start12).0,
        [(2 * FRAME_SIZE as u64, b"abc".to_vec())]
    );
    // With 13 bytes left a record starts there, carrying one payload byte.
    let start13 = 2 * FRAME_SIZE as u64 - 13;
    let mut out = Vec::new();
    FrameEncoder::new(1, start13).encode(b"abc", &mut out);
    assert_eq!(
        (out[8], out[10]),
        (1, 2),
        "First fragment with a 1-byte payload"
    );
    // A frame of the wrong size is an error, not a panic.
    assert!(FrameDecoder::new(1, start).decode(&[0; 10]).is_err());
}

proptest! {
    #![proptest_config(config(300))]

    /// A record framed from parts (`encode_parts`, #320) is byte for byte the record framed
    /// whole, wherever the parts split it (empty parts included) and wherever in a frame it
    /// starts: the same fragments, headers and checksums.
    #[test]
    fn framing_from_parts_matches_framing_whole(
        r in record(),
        cuts in vec(any::<prop::sample::Index>(), 0..4),
        start in FRAME_SIZE as u64..(3 * FRAME_SIZE) as u64,
    ) {
        let mut at: Vec<usize> = cuts.iter().map(|c| c.index(r.len() + 1)).collect();
        at.sort_unstable();
        let mut parts = Vec::new();
        let mut from = 0;
        for &to in &at {
            parts.push(&r[from..to]);
            from = to;
        }
        parts.push(&r[from..]);
        let mut whole = Vec::new();
        let whole_end = FrameEncoder::new(9, start).encode(&r, &mut whole);
        let mut framed = vec![7u8];
        let parts_end = FrameEncoder::new(9, start).encode_parts(&parts, &mut framed);
        prop_assert_eq!(parts_end, whole_end);
        prop_assert_eq!(&framed[1..], &whole[..]);
    }
}

/// D327 (#1708): lays out `records` like [`write`], but a record with a unit is preceded by a
/// pad to that unit, framed into the same buffer as it is written. Returns the segment and
/// each record's (first fragment, end) span.
fn write_padded(
    epoch: u32,
    start: u64,
    records: &[(Option<usize>, Vec<u8>)],
) -> (Vec<u8>, Vec<(u64, u64)>) {
    let mut seg = vec![0; FRAMES * FRAME_SIZE];
    let mut enc = FrameEncoder::new(epoch, start);
    let mut pos = start;
    let mut spans = Vec::new();
    for (unit, r) in records {
        let pad = unit.map_or(0, |u| enc.pad_len(u));
        let mut out = Vec::new();
        if let Some(u) = unit {
            assert_eq!(enc.pad(*u, &mut out), pad);
        }
        let need = pad + enc.encoded_len(r.len());
        if pos as usize + need > seg.len() {
            break;
        }
        let end = enc.encode(r, &mut out);
        assert_eq!(out.len(), need);
        seg[pos as usize..end as usize].copy_from_slice(&out);
        let after_pad = pos + pad as u64;
        let first = if FRAME_SIZE - (after_pad as usize % FRAME_SIZE) <= FRAGMENT_HEADER_LEN {
            after_pad.next_multiple_of(FRAME_SIZE as u64)
        } else {
            after_pad
        };
        if let Some(u) = unit {
            assert_eq!(first % *u as u64, 0, "a padded record starts on its unit");
        }
        spans.push((first, end));
        pos = end;
    }
    (seg, spans)
}

fn pad_unit() -> impl Strategy<Value = Option<usize>> {
    prop_oneof![
        2 => Just(None),
        1 => Just(Some(32)),
        1 => Just(Some(512)),
        2 => Just(Some(4096)),
        1 => Just(Some(FRAME_SIZE)),
    ]
}

proptest! {
    #![proptest_config(config(300))]

    /// Pads between records are stepped over: every record comes back at its offset, and the
    /// stop is the end of the last record, never past a pad.
    #[test]
    fn padded_records_roundtrip(
        records in vec((pad_unit(), record()), 0..sized(12, 4)),
        epoch in 1u32..,
        start in start_offset(),
    ) {
        let (seg, spans) = write_padded(epoch, start, &records);
        let (got, stop) = read(&seg, epoch, start);
        prop_assert_eq!(got.len(), spans.len());
        for ((off, rec), ((_, want), (first, _))) in got.iter().zip(records.iter().zip(&spans)) {
            prop_assert_eq!(rec, want);
            prop_assert_eq!(*off, *first);
        }
        if let Some(stop) = stop {
            prop_assert_eq!(stop, spans.last().map_or(start, |s| s.1));
        }
    }

    /// A cut anywhere, in a pad or in the record after it, stops at the end of the last
    /// complete record: a pad whose record was torn off is not data (D327, §4 of the design).
    #[test]
    fn a_torn_padded_tail_stops_at_the_last_complete_record(
        records in vec((pad_unit(), record()), 1..sized(10, 4)),
        epoch in 1u32..,
        start in start_offset(),
        cut in any::<prop::sample::Index>(),
    ) {
        let (seg, spans) = write_padded(epoch, start, &records);
        let end = spans.last().map_or(start, |s| s.1) as usize;
        let cut = start as usize + cut.index(end - start as usize + 1);
        let mut torn = seg.clone();
        torn[cut..].fill(0);
        let (got, stop) = read(&torn, epoch, start);
        let complete = spans
            .iter()
            .take_while(|s| torn[start as usize..s.1 as usize] == seg[start as usize..s.1 as usize])
            .count();
        prop_assert_eq!(got.len(), complete);
        let want_stop = if complete == 0 { start } else { spans[complete - 1].1 };
        prop_assert_eq!(stop, Some(want_stop));
    }
}

#[test]
fn a_pad_reaches_the_next_unit_and_needs_room_for_its_header() {
    let f = FRAME_SIZE as u64;
    // Aligned already: nothing.
    assert_eq!(FrameEncoder::new(1, f + 4096).pad_len(4096), 0);
    // A gap with room for a header: just the gap.
    assert_eq!(FrameEncoder::new(1, f + 100).pad_len(4096), 3996);
    // A gap too short for a header (under 12 bytes): to the boundary after. A gap of
    // exactly 12 is a header-only pad.
    assert_eq!(FrameEncoder::new(1, f + 4090).pad_len(4096), 6 + 4096);
    assert_eq!(FrameEncoder::new(1, f + 4085).pad_len(4096), 11 + 4096);
    assert_eq!(FrameEncoder::new(1, f + 4084).pad_len(4096), 12);
    // A frame tail too short for any fragment: the next record starts the next frame anyway.
    assert_eq!(FrameEncoder::new(1, 2 * f - 12).pad_len(4096), 0);
    // A frame's last unit: the pad ends exactly at the frame's end.
    assert_eq!(FrameEncoder::new(1, 2 * f - 100).pad_len(4096), 100);
    let mut out = Vec::new();
    let mut enc = FrameEncoder::new(1, f + 100);
    assert_eq!(enc.pad(4096, &mut out), 3996);
    assert_eq!(out.len(), 3996);
    assert_eq!(out[10], 5, "fragment type Pad");
    assert!(
        out[FRAGMENT_HEADER_LEN..].iter().all(|&b| b == 0),
        "a pad's payload is zeros"
    );
    assert_eq!(enc.pad(4096, &mut out), 0, "aligned after a pad");
}

#[test]
#[should_panic(expected = "a pad unit is a power of two")]
fn a_pad_unit_must_divide_a_frame() {
    FrameEncoder::new(1, FRAME_SIZE as u64 + 1).pad_len(3000);
}

/// A pad is only valid between records: one inside a First…Last sequence is out of sequence,
/// so replay stops before that record, as for any fragment out of order.
#[test]
fn a_pad_inside_a_record_stops_replay() {
    let start = FRAME_SIZE as u64;
    let big = vec![0xA5; FRAME_SIZE + 100];
    let (mut seg, spans) = write(3, start, &[vec![1; 10], big, vec![2; 10]], 0);
    // The big record's second fragment (its Last, at the start of frame 2): retype it a pad.
    let at = 2 * FRAME_SIZE;
    assert_eq!(seg[at + 10], 4, "the big record's Last fragment");
    seg[at + 10] = 5;
    let len = usize::from(u16::from_le_bytes([seg[at + 8], seg[at + 9]]));
    let crc = pigeonhole_format::checksum::crc32c(&seg[at + 4..at + FRAGMENT_HEADER_LEN + len]);
    seg[at..at + 4].copy_from_slice(&crc.to_le_bytes());
    let (got, stop) = read(&seg, 3, start);
    assert_eq!(got.len(), 1);
    assert_eq!(stop, Some(spans[0].1));
}

/// A reader that starts at a pad (a feed cursor resuming at the end of the last record it
/// read, D310) steps over it to the record after.
#[test]
fn a_decoder_starting_at_a_pad_reads_the_record_after_it() {
    let start = FRAME_SIZE as u64;
    let (seg, spans) = write_padded(4, start, &[(None, vec![1; 100]), (Some(4096), vec![2; 50])]);
    let resume = spans[0].1;
    assert!(spans[1].0 > resume, "a pad between them");
    let (got, _) = read(&seg, 4, resume);
    assert_eq!(got, [(spans[1].0, vec![2; 50])]);
}

/// Replays `truth` from `start` as a feed cursor that re-read only part of one frame (#1901):
/// in the frame holding segment offset `cut`, the bytes from `cut` on are `stale` ones (left
/// from an earlier read) until the decoder asks for more, and then the whole true frame is
/// fresh. Every other frame is read whole. What it returns must be what [`read`] returns.
fn read_with_stale_tail(
    truth: &[u8],
    stale: &[u8],
    epoch: u32,
    start: u64,
    cut: u64,
) -> (Vec<(u64, Vec<u8>)>, Option<u64>) {
    let mut dec = FrameDecoder::new(epoch, start);
    let mut out = Vec::new();
    let first_frame = start as usize / FRAME_SIZE;
    let cut_frame = cut as usize / FRAME_SIZE;
    for (i, frame) in truth.chunks(FRAME_SIZE).enumerate().skip(first_frame) {
        let mut mixed;
        let mut view: &[u8] = frame;
        let mut fresh = FRAME_SIZE;
        if i == cut_frame {
            let at = cut as usize % FRAME_SIZE;
            mixed = frame.to_vec();
            mixed[at..].copy_from_slice(&stale[i * FRAME_SIZE + at..(i + 1) * FRAME_SIZE]);
            view = &mixed;
            fresh = at;
        }
        loop {
            match dec.decode_within(view, fresh).unwrap() {
                Within::More => {
                    // The cursor reads the rest of the frame: all of it is now fresh.
                    assert!(fresh < FRAME_SIZE, "more with the whole frame fresh");
                    mixed = frame.to_vec();
                    view = &mixed;
                    fresh = FRAME_SIZE;
                }
                Within::Decoded(Some(Decoded::Record { offset })) => {
                    out.push((offset, dec.record().to_vec()));
                }
                Within::Decoded(Some(Decoded::Stop { offset })) => {
                    return (out, Some(offset));
                }
                Within::Decoded(None) => break,
            }
        }
    }
    (out, None)
}

/// What a stale tail holds: unwritten zeros, junk, or real fragments of this epoch at the
/// wrong offsets (the bytes of another part of the segment).
fn stale_kind() -> impl Strategy<Value = u8> {
    0u8..3
}

proptest! {
    #![proptest_config(config(300))]

    /// #1901: a decoder told that only a frame's first bytes are fresh decides nothing on
    /// the rest. Whatever the stale tail holds, a replay that asks for more when it reaches it
    /// returns exactly what replaying the true segment does, pads included.
    #[test]
    fn nothing_is_decided_on_bytes_past_the_fresh_ones(
        records in vec((pad_unit(), record()), 0..sized(12, 4)),
        epoch in 1u32..,
        start in start_offset(),
        cut_at in 0.0f64..1.0,
        kind in stale_kind(),
        junk in any::<u64>(),
    ) {
        let (truth, _) = write_padded(epoch, start, &records);
        let cut = start + ((truth.len() as u64 - start) as f64 * cut_at) as u64;
        let stale: Vec<u8> = match kind {
            0 => vec![0; truth.len()],
            1 => (0..truth.len()).map(|i| (junk.wrapping_mul(i as u64 + 1) >> 13) as u8).collect(),
            _ => {
                // The segment rotated by a few hundred bytes: CRC-valid fragments of this
                // epoch, at offsets where they don't belong.
                let k = 1 + (junk as usize % 997);
                (0..truth.len()).map(|i| truth[(i + k) % truth.len()]).collect()
            }
        };
        let (want, want_stop) = read(&truth, epoch, start);
        let (got, got_stop) = read_with_stale_tail(&truth, &stale, epoch, start, cut);
        prop_assert_eq!(got, want);
        prop_assert_eq!(got_stop, want_stop);
    }
}

/// #1901: `decode_within` returns `More` for a header, then for a payload, past the fresh
/// bytes, consuming nothing; with the whole frame fresh it decodes as `decode` does.
#[test]
fn a_fragment_past_the_fresh_bytes_asks_for_more_and_consumes_nothing() {
    let start = FRAME_SIZE as u64;
    let mut enc = FrameEncoder::new(7, start);
    let mut frame = Vec::new();
    enc.encode(&[1u8; 100], &mut frame);
    let second = frame.len();
    enc.encode(&[2u8; 100], &mut frame);
    frame.resize(FRAME_SIZE, 0);
    let mut dec = FrameDecoder::new(7, start);
    // The header itself isn't fresh.
    assert_eq!(dec.decode_within(&frame, 4).unwrap(), Within::More);
    // The header is, the payload isn't.
    assert_eq!(
        dec.decode_within(&frame, FRAGMENT_HEADER_LEN + 50).unwrap(),
        Within::More
    );
    // The first record whole, the second's header only.
    assert_eq!(
        dec.decode_within(&frame, second + FRAGMENT_HEADER_LEN)
            .unwrap(),
        Within::Decoded(Some(Decoded::Record { offset: start }))
    );
    assert_eq!(dec.record(), &[1u8; 100][..]);
    assert_eq!(
        dec.decode_within(&frame, second + FRAGMENT_HEADER_LEN)
            .unwrap(),
        Within::More
    );
    // Fresh to the end: the second record, then the stop just past it.
    assert_eq!(
        dec.decode_within(&frame, FRAME_SIZE).unwrap(),
        Within::Decoded(Some(Decoded::Record {
            offset: start + second as u64
        }))
    );
    assert_eq!(dec.record(), &[2u8; 100][..]);
    assert!(matches!(
        dec.decode_within(&frame, FRAME_SIZE).unwrap(),
        Within::Decoded(Some(Decoded::Stop { .. }))
    ));
}

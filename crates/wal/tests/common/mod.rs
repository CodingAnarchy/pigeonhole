//! Helpers shared by the integration tests.
// Shared by several test binaries, each using a subset of it.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use pigeonhole_format::wal::{BatchBuilder, FRAME_SIZE, SegmentHeader, StreamList, WalRecord};
use pigeonhole_format::{FamilyId, Kind, Lsn, Seqno, StreamId, TableId};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{OpenOptions, VfsRef};
use pigeonhole_wal::{Recovery, Result, WalOptions, stream_path};

pub const DB: &str = "/db/data.phdb";
pub const DB_ID: [u8; 16] = [0xAB; 16];
pub const STREAM: StreamId = StreamId(0);

pub fn db() -> &'static Path {
    Path::new(DB)
}

pub fn sim(seed: u64) -> VfsRef {
    SimVfs::new(seed)
}

/// Options with `frames` frames per segment and `spare` preallocated spares.
pub fn opts(frames: u64, spare: u32) -> WalOptions {
    let mut o = WalOptions::default();
    o.segment_size = frames * FRAME_SIZE as u64;
    o.spare_segments = spare;
    o
}

/// A record whose bytes are owned here, since `WalRecord` borrows its batch.
#[derive(Debug, Clone)]
pub struct Rec {
    pub seqno: Seqno,
    kind: u8,
    builder: BatchBuilder,
    participants: Vec<u8>,
}

/// A Batch record with one put whose value has `size` bytes derived from `seqno`.
pub fn batch(seqno: Seqno, size: usize) -> Rec {
    let mut b = BatchBuilder::new();
    let value: Vec<u8> = (0..size).map(|i| (i as u64 ^ seqno) as u8).collect();
    b.push(
        TableId(1),
        FamilyId(2),
        Kind::Put,
        format!("row{seqno}").as_bytes(),
        b"q",
        Some(seqno),
        &value,
    )
    .unwrap();
    Rec {
        seqno,
        kind: 1,
        builder: b,
        participants: Vec::new(),
    }
}

/// A Prepare record with two puts.
pub fn prepare(seqno: Seqno) -> Rec {
    let mut b = BatchBuilder::new();
    b.push(
        TableId(1),
        FamilyId(2),
        Kind::Put,
        b"r1",
        b"a",
        None,
        b"\x00x",
    )
    .unwrap();
    b.push(
        TableId(1),
        FamilyId(3),
        Kind::CellDelete,
        b"r1",
        b"b",
        Some(7),
        b"",
    )
    .unwrap();
    Rec {
        seqno,
        kind: 2,
        builder: b,
        participants: Vec::new(),
    }
}

/// A Commit record naming `streams`.
pub fn commit(seqno: Seqno, streams: &[StreamId]) -> Rec {
    let mut participants = Vec::new();
    StreamList::encode(streams, &mut participants).unwrap();
    Rec {
        seqno,
        kind: 3,
        builder: BatchBuilder::new(),
        participants,
    }
}

impl Rec {
    pub fn record(&self) -> WalRecord<'_> {
        match self.kind {
            1 => WalRecord::Batch {
                seqno: self.seqno,
                commit_ts: self.seqno * 10,
                batch: self.builder.batch(),
            },
            2 => WalRecord::Prepare {
                seqno: self.seqno,
                commit_ts: self.seqno * 10 + 1,
                coordinator: StreamId(9),
                batch: self.builder.batch(),
            },
            _ => WalRecord::Commit {
                seqno: self.seqno,
                participants: StreamList::new(&self.participants).unwrap(),
            },
        }
    }

    pub fn bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.record().encode(&mut out);
        out
    }
}

/// Everything a replay produced.
#[derive(Debug, PartialEq, Eq)]
pub struct Replayed {
    pub records: Vec<(Lsn, Vec<u8>)>,
    pub end: Lsn,
    pub max_seqno: Seqno,
}

impl Replayed {
    pub fn seqnos(&self) -> Vec<Seqno> {
        self.records
            .iter()
            .map(|(_, b)| match WalRecord::decode(b).unwrap() {
                WalRecord::Batch { seqno, .. }
                | WalRecord::Prepare { seqno, .. }
                | WalRecord::Commit { seqno, .. } => seqno,
            })
            .collect()
    }
}

/// Replays `stream` from `checkpoint`, re-encoding each record.
pub fn replay(vfs: &VfsRef, checkpoint: Lsn) -> Result<(Replayed, Recovery)> {
    let mut r = Recovery::open(vfs, db(), STREAM, DB_ID, checkpoint)?;
    let mut records = Vec::new();
    while let Some((end, rec)) = r.next_record()? {
        let mut bytes = Vec::new();
        rec.encode(&mut bytes);
        records.push((end, bytes));
    }
    Ok((
        Replayed {
            records,
            end: r.end(),
            max_seqno: r.max_seqno(),
        },
        r,
    ))
}

pub fn path() -> PathBuf {
    stream_path(db(), STREAM)
}

/// The valid segment headers in the stream file, by slot.
pub fn headers(vfs: &VfsRef, segment_size: u64) -> Vec<Option<SegmentHeader>> {
    let f = vfs.open(&path(), OpenOptions::read()).unwrap();
    let len = f.len().unwrap();
    (0..len / segment_size)
        .map(|slot| {
            let mut buf = [0u8; 64];
            f.read_at(&mut buf, slot * segment_size).unwrap();
            SegmentHeader::decode(&buf).ok()
        })
        .collect()
}

pub fn file_len(vfs: &VfsRef) -> u64 {
    vfs.open(&path(), OpenOptions::read())
        .unwrap()
        .len()
        .unwrap()
}

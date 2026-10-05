//! A toy store built directly on `pigeonhole_io`: an append-only log plus an in-memory
//! index, with one fsync policy per durability level.
//!
//! It exists to exercise the crash/replay checker, not to be a database. The in-memory index
//! is a [`Model`] (the semantics are tested on their own in the model's unit tests); what
//! the toy adds, and what the checker probes, is everything around it: record framing and
//! checksums, write and fsync ordering per durability level, recovery, and directory syncs.
//!
//! [`Variant`] selects a correct store or one of several deliberately broken ones.

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use pigeonhole_format::{Durability, Seqno, Timestamp};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{FileRef, OpenOptions, Result, Vfs};
use pigeonhole_sim::{Model, ModelCell, ModelFamily, ModelOp};

pub const TABLE: &str = "t";
const DIR: &str = "/db";
const LOG: &str = "/db/log";

/// A correct toy store, or one with a known bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Variant {
    Correct,
    /// `GroupSync` and `Sync` commits skip the fsync.
    NoFsync,
    /// The fsync is issued before the write it is meant to cover.
    SyncBeforeWrite,
    /// `Buffered` commits stay in a user-space buffer instead of reaching the kernel.
    BufferedNotWritten,
    /// Recovery does not verify record checksums, so torn records are applied.
    NoChecksum,
    /// Earlier unwritten (`None`) commits are flushed after the commit that carries them.
    ReorderedFlush,
    /// Delete operations are dropped when applied.
    IgnoresDeletes,
    /// Recovery silently drops one commit out of five (but keeps the seqno).
    LosesMiddleCommit,
    /// Reads return the versions of a column oldest first.
    WrongVersionOrder,
    /// A commit is written as one record per mutation, and recovery applies a trailing
    /// incomplete commit.
    PartialCommit,
    /// The log's directory entry is never synced, so power loss can unlink the file.
    NoDirSync,
}

impl Variant {
    pub const BROKEN: [Variant; 10] = [
        Variant::NoFsync,
        Variant::SyncBeforeWrite,
        Variant::BufferedNotWritten,
        Variant::NoChecksum,
        Variant::ReorderedFlush,
        Variant::IgnoresDeletes,
        Variant::LosesMiddleCommit,
        Variant::WrongVersionOrder,
        Variant::PartialCommit,
        Variant::NoDirSync,
    ];
}

pub struct Toy {
    variant: Variant,
    file: FileRef,
    families: Vec<ModelFamily>,
    index: Model,
    /// Where the next record is written.
    end: u64,
    /// Encoded records not yet handed to the kernel.
    unflushed: Vec<u8>,
}

impl Toy {
    /// Opens (creating or recovering) the store through `vfs`.
    pub fn open(vfs: &Arc<SimVfs>, variant: Variant, families: &[ModelFamily]) -> Result<Self> {
        let file = vfs.open(Path::new(LOG), OpenOptions::read_write_create())?;
        if variant != Variant::NoDirSync {
            vfs.sync_dir(Path::new(DIR))?;
        }
        let mut index = Model::new();
        index.create_table(TABLE, families.to_vec());
        let mut toy = Toy {
            variant,
            file,
            families: families.to_vec(),
            index,
            end: 0,
            unflushed: Vec::new(),
        };
        toy.recover()?;
        Ok(toy)
    }

    pub fn alive(&self) -> bool {
        self.file.len().is_ok()
    }

    pub fn last_seqno(&self) -> Seqno {
        self.index.snapshot()
    }

    fn recover(&mut self) -> Result<()> {
        let len = self.file.len()?;
        let mut data = vec![0u8; len as usize];
        self.file.read_at(&mut data, 0)?;
        let mut pos = 0usize;
        // The group being assembled: consecutive records with one seqno.
        let mut group: Option<(Seqno, Timestamp, Vec<ModelOp>)> = None;
        while let Some((rec, next)) = self.read_record(&data, pos) {
            pos = next;
            match &mut group {
                Some((seqno, _, ops)) if *seqno == rec.seqno => ops.extend(rec.ops),
                _ => {
                    if let Some(g) = group.replace((rec.seqno, rec.commit_ts, rec.ops)) {
                        self.replay(g);
                    }
                }
            }
        }
        // Only `PartialCommit` can leave an incomplete trailing group, and it applies it.
        if let Some(g) = group.take() {
            self.replay(g);
        }
        self.end = pos as u64;
        if self.end < len {
            self.file.set_len(self.end)?;
            self.file.sync_data()?;
        }
        Ok(())
    }

    fn replay(&mut self, (seqno, commit_ts, ops): (Seqno, Timestamp, Vec<ModelOp>)) {
        let ops = if self.variant == Variant::LosesMiddleCommit && seqno % 5 == 0 {
            Vec::new()
        } else {
            self.effective(ops)
        };
        self.index.commit(&ops, commit_ts, Durability::Sync);
    }

    fn effective(&self, ops: Vec<ModelOp>) -> Vec<ModelOp> {
        if self.variant == Variant::IgnoresDeletes {
            ops.into_iter()
                .filter(|o| matches!(o, ModelOp::Put { .. } | ModelOp::Incr { .. }))
                .collect()
        } else {
            ops
        }
    }

    fn read_record(&self, data: &[u8], pos: usize) -> Option<(Record, usize)> {
        let header = data.get(pos..pos + 12)?;
        let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let sum = u64::from_le_bytes(header[4..].try_into().unwrap());
        if len == 0 {
            return None;
        }
        let payload = data.get(pos + 12..pos + 12 + len)?;
        if self.variant != Variant::NoChecksum && checksum(payload) != sum {
            return None;
        }
        Some((decode(payload, &self.families)?, pos + 12 + len))
    }

    /// Commits `ops` at `commit_ts`, returning once durable at `durability`.
    pub fn commit(
        &mut self,
        ops: &[ModelOp],
        commit_ts: Timestamp,
        durability: Durability,
    ) -> Result<Seqno> {
        let seqno = self.index.snapshot() + 1;
        let mut bytes = Vec::new();
        if self.variant == Variant::PartialCommit {
            for op in ops {
                bytes.extend(encode(seqno, commit_ts, std::slice::from_ref(op)));
            }
        } else {
            bytes.extend(encode(seqno, commit_ts, ops));
        }
        let hold = durability == Durability::None
            || (self.variant == Variant::BufferedNotWritten && durability == Durability::Buffered);
        if hold {
            self.unflushed.extend(bytes);
        } else {
            // A commit also makes every earlier, still-unwritten record durable.
            let out = if self.variant == Variant::ReorderedFlush {
                [bytes, std::mem::take(&mut self.unflushed)].concat()
            } else {
                [std::mem::take(&mut self.unflushed), bytes].concat()
            };
            let strong = durability >= Durability::GroupSync && self.variant != Variant::NoFsync;
            if strong && self.variant == Variant::SyncBeforeWrite {
                self.file.sync_data()?;
            }
            if self.variant == Variant::PartialCommit {
                // One write per record, so a crash can land between them.
                for chunk in split_records(&out) {
                    self.file.write_at(chunk, self.end)?;
                    self.end += chunk.len() as u64;
                }
            } else {
                self.file.write_at(&out, self.end)?;
                self.end += out.len() as u64;
            }
            if strong && self.variant != Variant::SyncBeforeWrite {
                self.file.sync_data()?;
            }
        }
        let ops = self.effective(ops.to_vec());
        Ok(self.index.commit(&ops, commit_ts, durability))
    }

    pub fn get(
        &self,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        snapshot: Seqno,
        now: Timestamp,
    ) -> Option<ModelCell> {
        self.index.get(TABLE, row, family, qualifier, snapshot, now)
    }

    pub fn scan(
        &self,
        start: &[u8],
        end: &[u8],
        snapshot: Seqno,
        now: Timestamp,
    ) -> Vec<(Vec<u8>, Vec<ModelCell>)> {
        let mut rows = self.index.scan(
            TABLE,
            Bound::Included(start),
            Bound::Excluded(end),
            &[],
            snapshot,
            now,
        );
        if self.variant == Variant::WrongVersionOrder {
            for (_, cells) in &mut rows {
                oldest_first(cells);
            }
        }
        rows
    }

    /// Every version of every row at `snapshot`.
    pub fn dump(&self, snapshot: Seqno, now: Timestamp) -> Vec<(Vec<u8>, Vec<ModelCell>)> {
        self.index
            .scan(
                TABLE,
                Bound::Unbounded,
                Bound::Unbounded,
                &[],
                snapshot,
                now,
            )
            .into_iter()
            .map(|(row, _)| {
                let mut cells = self.index.read_row(TABLE, &row, &[], 0, snapshot, now);
                if self.variant == Variant::WrongVersionOrder {
                    oldest_first(&mut cells);
                }
                (row, cells)
            })
            .collect()
    }
}

/// Reverses each run of versions of one column.
fn oldest_first(cells: &mut [ModelCell]) {
    let mut i = 0;
    while i < cells.len() {
        let mut j = i + 1;
        while j < cells.len()
            && cells[j].family == cells[i].family
            && cells[j].qualifier == cells[i].qualifier
        {
            j += 1;
        }
        cells[i..j].reverse();
        i = j;
    }
}

fn split_records(mut bytes: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    while !bytes.is_empty() {
        let len = 12 + u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let (head, rest) = bytes.split_at(len);
        out.push(head);
        bytes = rest;
    }
    out
}

struct Record {
    seqno: Seqno,
    commit_ts: Timestamp,
    ops: Vec<ModelOp>,
}

fn checksum(bytes: &[u8]) -> u64 {
    // FNV-1a: enough to detect torn and misplaced bytes in a test store.
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
    })
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend((b.len() as u32).to_le_bytes());
    out.extend(b);
}

fn encode(seqno: Seqno, commit_ts: Timestamp, ops: &[ModelOp]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend(seqno.to_le_bytes());
    p.extend(commit_ts.to_le_bytes());
    p.extend((ops.len() as u32).to_le_bytes());
    for op in ops {
        match op {
            ModelOp::Put {
                row,
                family,
                qualifier,
                ts,
                value,
                ..
            } => {
                p.push(1);
                put_bytes(&mut p, row);
                put_bytes(&mut p, family.as_bytes());
                put_bytes(&mut p, qualifier);
                p.extend(ts.map_or([0; 9], |t| {
                    let mut b = [1; 9];
                    b[1..].copy_from_slice(&t.to_le_bytes());
                    b
                }));
                put_bytes(&mut p, value);
            }
            ModelOp::Incr {
                row,
                family,
                qualifier,
                delta,
                ..
            } => {
                p.push(2);
                put_bytes(&mut p, row);
                put_bytes(&mut p, family.as_bytes());
                put_bytes(&mut p, qualifier);
                p.extend(delta.to_le_bytes());
            }
            ModelOp::DeleteCell {
                row,
                family,
                qualifier,
                ts,
                ..
            } => {
                p.push(3);
                put_bytes(&mut p, row);
                put_bytes(&mut p, family.as_bytes());
                put_bytes(&mut p, qualifier);
                p.extend(ts.to_le_bytes());
            }
            ModelOp::DeleteColumn {
                row,
                family,
                qualifier,
                ..
            } => {
                p.push(4);
                put_bytes(&mut p, row);
                put_bytes(&mut p, family.as_bytes());
                put_bytes(&mut p, qualifier);
            }
            ModelOp::DeleteFamily { row, family, .. } => {
                p.push(5);
                put_bytes(&mut p, row);
                put_bytes(&mut p, family.as_bytes());
            }
            ModelOp::DeleteRow { row, .. } => {
                p.push(6);
                put_bytes(&mut p, row);
            }
        }
    }
    let mut out = Vec::with_capacity(p.len() + 12);
    out.extend((p.len() as u32).to_le_bytes());
    out.extend(checksum(&p).to_le_bytes());
    out.extend(p);
    out
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (head, rest) = (self.0.get(..n)?, self.0.get(n..)?);
        self.0 = rest;
        Some(head)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
    fn bytes(&mut self) -> Option<Vec<u8>> {
        let n = self.u32()? as usize;
        Some(self.take(n)?.to_vec())
    }
    fn family(&mut self, families: &[ModelFamily]) -> Option<String> {
        let name = String::from_utf8(self.bytes()?).ok()?;
        families.iter().any(|f| f.name == name).then_some(name)
    }
}

/// Decodes a payload, rejecting anything structurally invalid (so a store that skips the
/// checksum still never panics on garbage).
fn decode(payload: &[u8], families: &[ModelFamily]) -> Option<Record> {
    let mut r = Reader(payload);
    let seqno = r.u64()?;
    let commit_ts = r.u64()?;
    let n = r.u32()?;
    let mut ops = Vec::new();
    for _ in 0..n {
        let tag = r.u8()?;
        let table = TABLE.to_owned();
        let row = r.bytes()?;
        ops.push(match tag {
            1 => {
                let family = r.family(families)?;
                let qualifier = r.bytes()?;
                let ts = match r.u8()? {
                    0 => {
                        r.take(8)?;
                        None
                    }
                    1 => Some(r.u64()?),
                    _ => return None,
                };
                ModelOp::Put {
                    table,
                    row,
                    family,
                    qualifier,
                    ts,
                    value: r.bytes()?,
                }
            }
            2 => {
                let family = r.family(families)?;
                // The model rejects `Incr` on a family without the merge operator.
                if !families.iter().any(|f| f.name == family && f.i64_add) {
                    return None;
                }
                let qualifier = r.bytes()?;
                ModelOp::Incr {
                    table,
                    row,
                    family,
                    qualifier,
                    delta: r.u64()? as i64,
                }
            }
            3 => {
                let family = r.family(families)?;
                let qualifier = r.bytes()?;
                ModelOp::DeleteCell {
                    table,
                    row,
                    family,
                    qualifier,
                    ts: r.u64()?,
                }
            }
            4 => {
                let family = r.family(families)?;
                ModelOp::DeleteColumn {
                    table,
                    row,
                    family,
                    qualifier: r.bytes()?,
                }
            }
            5 => ModelOp::DeleteFamily {
                table,
                row,
                family: r.family(families)?,
            },
            6 => ModelOp::DeleteRow { table, row },
            _ => return None,
        });
    }
    r.0.is_empty().then_some(Record {
        seqno,
        commit_ts,
        ops,
    })
}

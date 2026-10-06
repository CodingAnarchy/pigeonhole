//! Shared test harness: random model histories, their stored entries, and read comparison
//! between `pigeonhole_sim::Model` and `CellResolver`.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_compaction::{
    CellResolver, Error, I64Add, MergeError, MergingCursor, ResolveOptions, VecCursor,
};
use pigeonhole_format::key::{Kind, TERMINATOR, encode_key, encode_marker_key, escape_into};
use pigeonhole_format::manifest::{FamilyOptions, SstMeta};
use pigeonhole_format::value::ValueTag;
use pigeonhole_format::{Cursor, Durability, FamilyId, Seqno, SstId, TableId, TabletId, Timestamp};
use pigeonhole_pager::Pager;
use pigeonhole_sim::{Model, ModelError, ModelFamily, ModelOp, Rng};
use pigeonhole_sst::{ReadOptions, ScanFilter, SstIter, SstReader, SstWriter, SstWriterOptions};

pub const TABLE: &str = "t";
pub const FAMILY: &str = "f";
pub const ROWS: [&[u8]; 4] = [b"a", b"b", b"b\x00x", b"c"];
pub const QUALS: [&[u8]; 3] = [b"", b"q", b"r\x00"];

/// Proptest cases: honors `PROPTEST_CASES`, scaled down under Miri.
pub fn cases(default: u32) -> u32 {
    let base = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default);
    if cfg!(miri) { base.min(4) } else { base }
}

/// One stored entry: internal key, stored value, seqno.
pub type Entry = (Vec<u8>, Vec<u8>, Seqno);

/// A model history and the same history as stored entries.
pub struct History {
    pub model: Model,
    pub family: ModelFamily,
    pub entries: Vec<Entry>,
    pub last_ts: Timestamp,
}

/// The stored form of a model value: 8-byte values as `i64`, everything else as bytes.
pub fn stored(value: &[u8]) -> Vec<u8> {
    let tag = if value.len() == 8 {
        ValueTag::I64
    } else {
        ValueTag::Bytes
    };
    let mut v = vec![tag as u8];
    v.extend_from_slice(value);
    v
}

/// The model form of a stored value.
pub fn unstored(value: &[u8]) -> Vec<u8> {
    value[1..].to_vec()
}

fn pick<'a, T: ?Sized>(rng: &mut Rng, xs: &[&'a T]) -> &'a T {
    xs[rng.below(xs.len() as u64) as usize]
}

/// A random history of `commits` commits on one family.
pub fn random_history(seed: u64, commits: usize) -> History {
    let mut rng = Rng::new(seed);
    let family = ModelFamily {
        name: FAMILY.into(),
        max_versions: [0, 0, 1, 2, 3][rng.below(5) as usize],
        ttl_micros: [0, 0, 120, 300][rng.below(4) as usize],
        i64_add: true,
    };
    let mut model = Model::new();
    model.create_table(TABLE, vec![family.clone()]);
    let mut entries: Vec<Entry> = Vec::new();
    let mut used_ts: Vec<Timestamp> = vec![5];
    let mut last_ts = 0;
    for c in 0..commits {
        let commit_ts = 10 * (c as u64 + 1);
        last_ts = commit_ts;
        let n_ops = 1 + rng.below(3) as usize;
        let mut ops = Vec::new();
        for _ in 0..n_ops {
            let row = pick(&mut rng, &ROWS).to_vec();
            let qualifier = pick(&mut rng, &QUALS).to_vec();
            let (table, family_name) = (TABLE.to_string(), FAMILY.to_string());
            let op = match rng.below(20) {
                0..=6 => {
                    let ts = match rng.below(3) {
                        0 => Some(used_ts[rng.below(used_ts.len() as u64) as usize]),
                        _ => None,
                    };
                    let value = match rng.below(10) {
                        0..=2 => (rng.below(1000) as i64).to_le_bytes().to_vec(),
                        3 => vec![b'L'; 5000 + rng.below(100) as usize],
                        4 => b"abc".to_vec(),
                        _ => {
                            let len = rng.below(12) as usize;
                            (0..len).map(|_| rng.below(256) as u8).collect()
                        }
                    };
                    ModelOp::Put {
                        table,
                        row,
                        family: family_name,
                        qualifier,
                        ts,
                        value,
                    }
                }
                7..=11 => ModelOp::Incr {
                    table,
                    row,
                    family: family_name,
                    qualifier,
                    delta: rng.below(100) as i64 - 50,
                },
                12..=14 => ModelOp::DeleteCell {
                    table,
                    row,
                    family: family_name,
                    qualifier,
                    ts: used_ts[rng.below(used_ts.len() as u64) as usize],
                },
                15..=16 => ModelOp::DeleteColumn {
                    table,
                    row,
                    family: family_name,
                    qualifier,
                },
                17..=18 => ModelOp::DeleteFamily {
                    table,
                    row,
                    family: family_name,
                },
                _ => ModelOp::DeleteRow { table, row },
            };
            ops.push(op);
        }
        let seqno = model.commit(&ops, commit_ts, Durability::Sync);
        // The same commit as stored entries, collapsed like the model (D34).
        let mut cells: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut collapse: BTreeMap<(Vec<u8>, Vec<u8>, Timestamp), Vec<u8>> = BTreeMap::new();
        for op in &ops {
            let mut key = Vec::new();
            let (row, q, ts, kind, value) = match op {
                ModelOp::Put {
                    row,
                    qualifier,
                    ts,
                    value,
                    ..
                } => (
                    row,
                    qualifier,
                    ts.unwrap_or(commit_ts),
                    Kind::Put,
                    stored(value),
                ),
                ModelOp::Incr {
                    row,
                    qualifier,
                    delta,
                    ..
                } => (
                    row,
                    qualifier,
                    commit_ts,
                    Kind::Merge,
                    stored(&delta.to_le_bytes()),
                ),
                ModelOp::DeleteCell {
                    row, qualifier, ts, ..
                } => (row, qualifier, *ts, Kind::CellDelete, Vec::new()),
                ModelOp::DeleteColumn { row, qualifier, .. } => {
                    (row, qualifier, commit_ts, Kind::ColumnDelete, Vec::new())
                }
                ModelOp::DeleteFamily { row, .. } | ModelOp::DeleteRow { row, .. } => {
                    encode_marker_key(&mut key, row, commit_ts, seqno).unwrap();
                    cells.insert(key, Vec::new());
                    continue;
                }
            };
            encode_key(&mut key, row, q, ts, seqno, kind).unwrap();
            // A later op on the same (row, qualifier, ts) replaces the earlier one.
            if let Some(old) = collapse.insert((row.clone(), q.clone(), ts), key.clone()) {
                cells.remove(&old);
            }
            cells.insert(key, value);
            used_ts.push(ts);
        }
        used_ts.push(commit_ts);
        entries.extend(cells.into_iter().map(|(k, v)| (k, v, seqno)));
    }
    History {
        model,
        family,
        entries,
        last_ts,
    }
}

/// Resolve options for a history's family.
pub fn options(h: &History, snapshot: Seqno, now: Timestamp, versions: u32) -> ResolveOptions {
    let mut o = ResolveOptions::new(snapshot, now);
    o.ttl_micros = h.family.ttl_micros;
    o.versions = match (versions, h.family.max_versions) {
        (0, m) => m,
        (v, 0) => v,
        (v, m) => v.min(m),
    };
    o.merge = Some(Arc::new(I64Add));
    o
}

/// A read result in model terms; a merge failure is `Err(())`.
pub type Cells = Result<Vec<(Vec<u8>, Timestamp, Vec<u8>)>, ()>;

fn model_cells(r: Result<Vec<pigeonhole_sim::ModelCell>, ModelError>) -> Cells {
    match r {
        Ok(cells) => Ok(cells
            .into_iter()
            .map(|c| (c.qualifier, c.ts, c.value))
            .collect()),
        Err(ModelError::MergeFailed(_)) => Err(()),
        Err(e) => panic!("unexpected model error {e}"),
    }
}

pub fn row_prefix(row: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    escape_into(&mut p, row);
    p.extend_from_slice(&TERMINATOR);
    p
}

/// The first key after every key of `row`.
pub fn row_end(row: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    escape_into(&mut p, row);
    p.extend_from_slice(&[0x00, 0x02]);
    p
}

/// Unescaped qualifier of an internal key.
fn qualifier(key: &[u8]) -> Vec<u8> {
    let parts = pigeonhole_format::decode_key(key).unwrap();
    let mut q = Vec::new();
    parts.qualifier.unwrap().unescape_into(&mut q);
    q
}

fn row_of(key: &[u8]) -> Vec<u8> {
    let parts = pigeonhole_format::decode_key(key).unwrap();
    let mut r = Vec::new();
    parts.row.unescape_into(&mut r);
    r
}

/// Drains a resolver into cells, stopping at the first error.
fn drain<C: Cursor<Error = Error>>(r: &mut CellResolver<C>) -> Cells {
    let mut out = Vec::new();
    loop {
        match r.next_cell() {
            Ok(Some(c)) => out.push((c.key.to_vec(), c.ts, unstored(c.value))),
            Ok(None) => return Ok(out),
            Err(Error::Merge(_)) => return Err(()),
            Err(e) => panic!("read failed: {e}"),
        }
    }
}

/// Every read the oracle checks at one snapshot, run through `resolver_for`. Returns a
/// description of the first mismatch with the model, or with `other` when given.
pub struct Reads {
    pub gets: Vec<Cells>,
    pub rows: Vec<Cells>,
    pub scan: Cells,
}

pub fn model_reads(h: &History, snapshot: Seqno, now: Timestamp) -> Reads {
    let m = &h.model;
    let mut gets = Vec::new();
    let mut rows = Vec::new();
    for row in ROWS {
        for q in QUALS {
            gets.push(model_cells(
                m.try_get(TABLE, row, FAMILY, q, snapshot, now)
                    .map(|c| c.into_iter().collect()),
            ));
        }
        for versions in [0, 1, 2] {
            rows.push(model_cells(m.try_read_row(
                TABLE,
                row,
                &[FAMILY],
                versions,
                snapshot,
                now,
            )));
        }
    }
    let scan = match m.try_scan(
        TABLE,
        Bound::Unbounded,
        Bound::Unbounded,
        &[FAMILY],
        snapshot,
        now,
    ) {
        Ok(rows) => Ok(rows
            .into_iter()
            .flat_map(|(row, cells)| {
                cells
                    .into_iter()
                    .map(move |c| ([row.clone(), c.qualifier].concat(), c.ts, c.value))
            })
            .collect()),
        Err(ModelError::MergeFailed(_)) => Err(()),
        Err(e) => panic!("{e}"),
    };
    Reads { gets, rows, scan }
}

/// The same reads through resolvers built by `make`.
pub fn resolver_reads<C, F>(h: &History, snapshot: Seqno, now: Timestamp, mut make: F) -> Reads
where
    C: Cursor<Error = Error>,
    F: FnMut(ResolveOptions) -> CellResolver<C>,
{
    let mut gets = Vec::new();
    let mut rows = Vec::new();
    for row in ROWS {
        for q in QUALS {
            let mut r = make(options(h, snapshot, now, 1));
            r.seek_column(row, q).unwrap();
            gets.push(drain(&mut r).map(|cells| {
                assert!(
                    cells.len() <= 1,
                    "a point get returned {} cells",
                    cells.len()
                );
                cells
                    .into_iter()
                    .map(|(k, ts, v)| (qualifier(&k), ts, v))
                    .collect()
            }));
        }
        for versions in [0, 1, 2] {
            let mut r = make(options(h, snapshot, now, versions));
            r.set_upper_bound(Some(&row_end(row)));
            r.seek(&row_prefix(row)).unwrap();
            rows.push(drain(&mut r).map(|cells| {
                cells
                    .into_iter()
                    .map(|(k, ts, v)| (qualifier(&k), ts, v))
                    .collect()
            }));
        }
    }
    let mut r = make(options(h, snapshot, now, 1));
    r.seek(b"").unwrap();
    let scan = drain(&mut r).map(|cells| {
        cells
            .into_iter()
            .map(|(k, ts, v)| ([row_of(&k), qualifier(&k)].concat(), ts, v))
            .collect()
    });
    Reads { gets, rows, scan }
}

/// Asserts two read sets agree.
pub fn assert_same(what: &str, expected: &Reads, actual: &Reads) {
    for (i, (e, a)) in expected.gets.iter().zip(&actual.gets).enumerate() {
        assert_eq!(
            e,
            a,
            "{what}: get #{i} (row {}, qualifier {})",
            i / 3,
            i % 3
        );
    }
    for (i, (e, a)) in expected.rows.iter().zip(&actual.rows).enumerate() {
        assert_eq!(
            e,
            a,
            "{what}: read_row #{i} (row {}, versions {})",
            i / 3,
            [0, 1, 2][i % 3]
        );
    }
    assert_eq!(expected.scan, actual.scan, "{what}: scan");
}

/// Splits entries into `n` random in-memory sources.
pub fn vec_sources(entries: &[Entry], n: usize, rng: &mut Rng) -> Vec<VecCursor> {
    let mut parts = vec![Vec::new(); n];
    for (k, v, _) in entries {
        parts[rng.below(n as u64) as usize].push((k.clone(), v.clone()));
    }
    parts.into_iter().map(VecCursor::new).collect()
}

/// `SstIter` with this crate's error type, so the resolver can report merge failures.
#[derive(Debug)]
pub struct Src(pub SstIter);

impl Cursor for Src {
    type Error = Error;
    fn valid(&self) -> bool {
        self.0.valid()
    }
    fn key(&self) -> &[u8] {
        self.0.key()
    }
    fn value(&self) -> &[u8] {
        self.0.value()
    }
    fn seek_to_first(&mut self) -> Result<(), Error> {
        Ok(self.0.seek_to_first()?)
    }
    fn seek(&mut self, target: &[u8]) -> Result<(), Error> {
        Ok(self.0.seek(target)?)
    }
    fn next(&mut self) -> Result<(), Error> {
        Ok(self.0.next()?)
    }
    fn skip_row(&mut self) -> Result<(), Error> {
        Ok(self.0.skip_row()?)
    }
}

/// A resolver over open SSTs.
pub fn sst_resolver(
    ssts: &[Arc<SstReader>],
    options: ResolveOptions,
) -> CellResolver<MergingCursor<Src>> {
    let sources = ssts
        .iter()
        .map(|s| Src(s.iter(ScanFilter::all(), ReadOptions::default())))
        .collect();
    CellResolver::new(MergingCursor::new(sources), options)
}

/// Family options for SSTs in tests: small blocks so SSTs have several.
pub fn family_options(h: &History) -> FamilyOptions {
    FamilyOptions {
        block_size: 512,
        max_versions: h.family.max_versions,
        ttl_micros: h.family.ttl_micros,
        merge_operator: "pigeonhole.i64_add".into(),
        ..FamilyOptions::default()
    }
}

/// Writes sorted `entries` as one SST.
pub fn write_sst(
    pager: &Pager,
    id: u64,
    family: &FamilyOptions,
    entries: &[(Vec<u8>, Vec<u8>)],
) -> SstMeta {
    let bytes: usize = entries.iter().map(|(k, v)| k.len() + v.len() + 16).sum();
    let extent = pager.allocate((bytes as u64 * 2).max(64 << 10)).unwrap();
    let opts = SstWriterOptions::for_family(family, TableId(1), FamilyId(1), TabletId(1));
    let mut w = SstWriter::new(pager.file().clone(), extent, SstId(id), opts);
    for (k, v) in entries {
        w.add(k, v).unwrap();
    }
    w.finish().unwrap()
}

pub fn open_sst(pager: &Pager, cache: &Arc<BlockCache>, meta: &SstMeta) -> Arc<SstReader> {
    Arc::new(SstReader::open(pager.file().clone(), meta, cache.clone(), Priority::Normal).unwrap())
}

/// Every entry of an SST.
pub fn sst_entries(sst: &Arc<SstReader>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut it = sst.iter(ScanFilter::all(), ReadOptions::default());
    it.seek_to_first().unwrap();
    let mut out = Vec::new();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.next().unwrap();
    }
    out
}

/// A merge error converts into the crate error (resolvers need `From<MergeError>`).
pub fn _assert_from(e: MergeError) -> Error {
    e.into()
}

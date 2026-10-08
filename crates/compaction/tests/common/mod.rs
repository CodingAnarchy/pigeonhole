//! Shared test harness: random model histories, their stored entries, and read comparison
//! between `pigeonhole_sim::Model` and `CellResolver`.
// Shared by several test binaries, each using a subset of it.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use pigeonhole_cache::{BlockCache, Cell, Priority};
use pigeonhole_compaction::{
    BlobFetch, CellResolver, Error, I64Add, MergeError, MergingCursor, NewBlobFile, ResolveOptions,
    ValuePredicate, VecCursor, blob_pointer,
};
use pigeonhole_format::key::{Kind, TERMINATOR, encode_key, encode_marker_key, escape_into};
use pigeonhole_format::manifest::{FamilyOptions, SstMeta};
use pigeonhole_format::value::BlobPointer;
use pigeonhole_format::value::ValueTag;
use pigeonhole_format::{
    BlobFileId, Cursor, Durability, FamilyId, Seqno, SstId, TableId, TabletId, Timestamp,
};
use pigeonhole_pager::Pager;
use pigeonhole_sim::{Model, ModelError, ModelFamily, ModelOp, Rng};
use pigeonhole_sst::{
    BlobReader, ReadOptions, ScanFilter, SstIter, SstReader, SstWriter, SstWriterOptions,
};

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
    if cfg!(miri) { base.min(1) } else { base }
}

/// One stored entry: internal key, stored value, seqno.
pub type Entry = (Vec<u8>, Vec<u8>, Seqno);

/// A model history and the same history as stored entries.
pub struct History {
    pub model: Model,
    pub family: ModelFamily,
    pub entries: Vec<Entry>,
    pub last_ts: Timestamp,
    /// Timestamps used so far (targets for explicit puts and cell deletes).
    used_ts: Vec<Timestamp>,
    commits: usize,
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
    let mut h = History {
        model,
        family,
        entries: Vec::new(),
        last_ts: 0,
        used_ts: vec![5],
        commits: 0,
    };
    extend_history(&mut h, &mut rng, commits, false);
    h
}

/// Appends `commits` random commits to `h` and returns their entries. With `later`, only
/// writes at the commit timestamp (default-timestamp puts, increments, column, family and
/// row deletes): commit timestamps are multiples of 10 and explicit ones never are, so such
/// writes never meet an existing timestamp exactly and never sort below a purged delete.
pub fn extend_history(h: &mut History, rng: &mut Rng, commits: usize, later: bool) -> Vec<Entry> {
    let mut added = Vec::new();
    for _ in 0..commits {
        h.commits += 1;
        let commit_ts = 10 * h.commits as u64;
        h.last_ts = commit_ts;
        let used_ts = &mut h.used_ts;
        let model = &mut h.model;
        let n_ops = 1 + rng.below(3) as usize;
        let mut ops = Vec::new();
        for _ in 0..n_ops {
            let row = pick(rng, &ROWS).to_vec();
            let qualifier = pick(rng, &QUALS).to_vec();
            let (table, family_name) = (TABLE.to_string(), FAMILY.to_string());
            let mut choice = rng.below(20);
            if later && (12..=14).contains(&choice) {
                choice = 15; // no cell deletes at old timestamps
            }
            let op = match choice {
                0..=6 => {
                    let ts = match rng.below(4) {
                        _ if later => None,
                        0 => Some(used_ts[rng.below(used_ts.len() as u64) as usize]),
                        // Above later commit timestamps, but never equal to one.
                        1 => Some(commit_ts + 5 + 10 * rng.below(5)),
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
        added.extend(commit_ops(model, used_ts, &ops, commit_ts));
        used_ts.push(commit_ts);
    }
    h.entries.extend(added.iter().cloned());
    added
}

/// Commits `ops` to the model and returns the same commit as stored entries, collapsed like
/// the model (D34); records the timestamps it used.
fn commit_ops(
    model: &mut Model,
    used_ts: &mut Vec<Timestamp>,
    ops: &[ModelOp],
    commit_ts: Timestamp,
) -> Vec<Entry> {
    let seqno = model.commit(ops, commit_ts, Durability::Sync);
    // The same commit as stored entries, collapsed like the model (D34).
    let mut cells: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut collapse: BTreeMap<(Vec<u8>, Vec<u8>, Timestamp), Vec<u8>> = BTreeMap::new();
    for op in ops {
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
    cells.into_iter().map(|(k, v)| (k, v, seqno)).collect()
}

impl History {
    /// An empty history of one family.
    pub fn new(family: ModelFamily) -> Self {
        let mut model = Model::new();
        model.create_table(TABLE, vec![family.clone()]);
        Self {
            model,
            family,
            entries: Vec::new(),
            last_ts: 0,
            used_ts: vec![5],
            commits: 0,
        }
    }

    /// Commits `ops` at `commit_ts` and returns its entries.
    pub fn commit(&mut self, ops: &[ModelOp], commit_ts: Timestamp) -> Vec<Entry> {
        self.commits += 1;
        self.last_ts = self.last_ts.max(commit_ts);
        let added = commit_ops(&mut self.model, &mut self.used_ts, ops, commit_ts);
        self.entries.extend(added.iter().cloned());
        added
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

/// Readers of the blob files written so far.
#[derive(Debug, Default)]
pub struct Blobs(pub Vec<(BlobFileId, Arc<BlobReader>)>);

impl Blobs {
    /// Opens readers for `files`.
    pub fn add(&mut self, pager: &Pager, cache: &Arc<BlockCache>, files: &[NewBlobFile]) {
        for f in files {
            let r = BlobReader::new(pager.file().clone(), f.id, f.extents.clone(), cache.clone());
            self.0.push((f.id, Arc::new(r)));
        }
    }

    /// Reads the value `ptr` names; panics if its file is unknown.
    pub fn read(&self, ptr: &BlobPointer) -> Cell {
        let (_, r) = self
            .0
            .iter()
            .find(|(id, _)| *id == ptr.blob_file)
            .unwrap_or_else(|| panic!("no blob file {:?}", ptr.blob_file));
        r.read(ptr).unwrap()
    }
}

impl BlobFetch for Blobs {
    fn fetch(&self, ptr: &BlobPointer) -> Option<Cell> {
        Some(self.read(ptr))
    }
}

/// An SST cursor whose separated values read as the values their pointers name, so the
/// model oracle sees the same values before and after separation.
#[derive(Debug)]
pub struct Resolved {
    src: Src,
    blobs: Arc<Blobs>,
    value: Option<Cell>,
}

impl Resolved {
    fn refresh(&mut self) {
        self.value = (self.src.valid())
            .then(|| blob_pointer(self.src.value()))
            .flatten()
            .map(|p| self.blobs.read(&p));
    }
}

impl Cursor for Resolved {
    type Error = Error;
    fn valid(&self) -> bool {
        self.src.valid()
    }
    fn key(&self) -> &[u8] {
        self.src.key()
    }
    fn value(&self) -> &[u8] {
        match &self.value {
            Some(v) => v,
            None => self.src.value(),
        }
    }
    fn seek_to_first(&mut self) -> Result<(), Error> {
        self.src.seek_to_first()?;
        self.refresh();
        Ok(())
    }
    fn seek(&mut self, target: &[u8]) -> Result<(), Error> {
        self.src.seek(target)?;
        self.refresh();
        Ok(())
    }
    fn next(&mut self) -> Result<(), Error> {
        self.src.next()?;
        self.refresh();
        Ok(())
    }
    fn skip_row(&mut self) -> Result<(), Error> {
        self.src.skip_row()?;
        self.refresh();
        Ok(())
    }
}

/// A resolver over open SSTs whose blob pointers are read through `blobs`.
pub fn blob_sst_resolver(
    ssts: &[Arc<SstReader>],
    blobs: &Arc<Blobs>,
    options: ResolveOptions,
) -> CellResolver<MergingCursor<Resolved>> {
    let sources = ssts
        .iter()
        .map(|s| Resolved {
            src: Src(s.iter(ScanFilter::all(), ReadOptions::default())),
            blobs: Arc::clone(blobs),
            value: None,
        })
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

/// A model row read as `(row, qualifier, ts, value)` cells, or `None` if it fails.
fn model_rows(
    h: &History,
    snapshot: Seqno,
    now: Timestamp,
    versions: u32,
) -> Option<Vec<Vec<FullCell>>> {
    let mut rows = Vec::new();
    for row in ROWS {
        let cells = h
            .model
            .try_read_row(TABLE, row, &[FAMILY], versions, snapshot, now)
            .ok()?;
        rows.push(
            cells
                .into_iter()
                .map(|c| (row.to_vec(), c.qualifier, c.ts, c.value))
                .collect(),
        );
    }
    Some(rows)
}

type FullCell = (Vec<u8>, Vec<u8>, Timestamp, Vec<u8>);

fn full(key: &[u8], ts: Timestamp, value: &[u8]) -> FullCell {
    (row_of(key), qualifier(key), ts, unstored(value))
}

/// Groups one row's cells by qualifier, preserving order.
fn columns(row: &[FullCell]) -> Vec<Vec<FullCell>> {
    let mut out: Vec<Vec<FullCell>> = Vec::new();
    for c in row {
        match out.last_mut() {
            Some(col) if col[0].1 == c.1 => col.push(c.clone()),
            _ => out.push(vec![c.clone()]),
        }
    }
    out
}

/// Scans the whole family through a fresh resolver, calling `after_cell` after each cell
/// (true: skip the rest of the row). `Err(())` on a merge failure.
fn scan_with<C, F>(r: &mut CellResolver<C>, mut after_cell: F) -> Result<Vec<FullCell>, ()>
where
    C: Cursor<Error = Error>,
    F: FnMut() -> bool,
{
    r.seek(b"").unwrap();
    let mut out = Vec::new();
    loop {
        match r.next_cell() {
            Ok(Some(c)) => out.push(full(c.key, c.ts, c.value)),
            Ok(None) => return Ok(out),
            Err(Error::Merge(_)) => return Err(()),
            Err(e) => panic!("read failed: {e}"),
        }
        if after_cell() {
            r.skip_row().unwrap();
        }
    }
}

/// Reads the oracle checks beyond gets, row reads and latest-only scans: multi-version
/// scans across rows, `columns_per_row`, value predicates, caller `skip_row`, and
/// resolved-version time ranges (D22 amendment). Cases where the model's read fails are
/// checked only for multi-version scans (a limit or filter may legitimately avoid the
/// failing version).
pub fn check_extras<C, F>(h: &History, snapshot: Seqno, now: Timestamp, rng: &mut Rng, mut make: F)
where
    C: Cursor<Error = Error>,
    F: FnMut(ResolveOptions) -> CellResolver<C>,
{
    let what = format!("snapshot {snapshot} now {now}");
    for versions in [0u32, 2] {
        let expected = model_rows(h, snapshot, now, versions);
        let mut r = make(options(h, snapshot, now, versions));
        let got = scan_with(&mut r, || false);
        match &expected {
            Some(rows) => assert_eq!(
                got.as_ref().ok(),
                Some(&rows.concat()),
                "{what}: scan v{versions}"
            ),
            None => assert!(got.is_err(), "{what}: scan v{versions} should fail"),
        }
        let Some(rows) = expected else { continue };

        // Columns per row.
        let limit = 1 + rng.below(2) as u32;
        let mut o = options(h, snapshot, now, versions);
        o.columns_per_row = limit;
        let want: Vec<FullCell> = rows
            .iter()
            .flat_map(|row| columns(row).into_iter().take(limit as usize).flatten())
            .collect();
        assert_eq!(
            scan_with(&mut make(o), || false),
            Ok(want),
            "{what}: columns_per_row {limit} v{versions}"
        );

        // A value predicate on each column's newest version.
        let pred = match rng.below(3) {
            0 => ValuePredicate::Prefix(vec![rng.below(256) as u8]),
            1 => ValuePredicate::I64(std::cmp::Ordering::Greater, rng.below(400) as i64 - 200),
            _ => ValuePredicate::Range(Bound::Included(vec![0x40]), Bound::Unbounded),
        };
        let mut o = options(h, snapshot, now, versions);
        o.value = Some(pred.clone());
        let want: Vec<FullCell> = rows
            .iter()
            .flat_map(|row| columns(row))
            .filter(|col| pred.matches(&stored(&col[0].3)))
            .flatten()
            .collect();
        assert_eq!(
            scan_with(&mut make(o), || false),
            Ok(want),
            "{what}: predicate {pred:?} v{versions}"
        );

        // The caller skipping rows at random points.
        let decide_seed = rng.next_u64();
        let mut decide = Rng::new(decide_seed);
        let mut want = Vec::new();
        for row in &rows {
            for c in row {
                want.push(c.clone());
                if decide.below(3) == 0 {
                    break;
                }
            }
        }
        let mut decide = Rng::new(decide_seed);
        let got = scan_with(&mut make(options(h, snapshot, now, versions)), || {
            decide.below(3) == 0
        });
        assert_eq!(got, Ok(want), "{what}: skip_row v{versions}");
    }

    // A resolved-version time range (families with a merge operator, D22 amendment).
    if h.family.max_versions == 0
        && let Some(rows) = model_rows(h, snapshot, now, 0)
    {
        let lo = rng.below(h.last_ts + 60);
        let hi = lo + rng.below(200);
        let versions = rng.below(3) as u32;
        let mut filter = ScanFilter::all();
        let mut o = options(h, snapshot, now, versions);
        o.route_time_range(&mut filter, Some((lo, hi)));
        assert!(filter.time_range.is_none());
        let want: Vec<FullCell> = rows
            .iter()
            .flat_map(|row| columns(row))
            .flat_map(|col| {
                let kept = col.into_iter().filter(|c| lo <= c.2 && c.2 < hi);
                kept.take(if versions == 0 {
                    usize::MAX
                } else {
                    versions as usize
                })
            })
            .collect();
        assert_eq!(
            scan_with(&mut make(o), || false),
            Ok(want),
            "{what}: time range [{lo}, {hi}) v{versions}"
        );
    }
}

/// History length, scaled down under Miri.
pub fn commits(n: usize) -> usize {
    if cfg!(miri) { n.min(5) } else { n }
}

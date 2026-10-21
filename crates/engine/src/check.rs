//! The offline integrity check (ICR 0048, #692): walks every structure of a database no
//! writer has open and reports what is damaged, without replaying or writing anything.
//!
//! It takes the writer lock (so no writer starts while it reads), then checks, collecting
//! findings instead of stopping at the first:
//! - **superblocks:** both slots decoded on their own (the pager's fallback to the other slot
//!   would hide a damaged one);
//! - **manifest:** the snapshot and delta blocks the root names, through the same decode as
//!   open;
//! - **allocation:** every extent the root and the manifest reference is a valid extent,
//!   inside the file, and overlaps no other;
//! - **SSTs:** every block read from disk with its checksum verified (a disabled block
//!   cache: a cached block skips verification), keys in strictly increasing order, and the
//!   properties block and the key walk agreeing with the manifest's record of the SST, plus
//!   each SST's blob references;
//! - **blob files:** every extent header, and every record's checksum, walked end to end;
//! - **WAL sidecars:** every segment header, and the log from each stream's checkpoint,
//!   chained segment to segment as replay would follow it, with damage before the log's
//!   end told apart from a torn tail.
//!
//! With [`CheckOptions::deep`], every blob pointer an SST holds is also read through and
//! verified, and each blob file's live bytes are recounted from the pointers.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_compaction::{blob_pointer, note_blob_ref, record_bytes};
use pigeonhole_format::blob::{
    BLOB_EXTENT_HEADER_LEN, BLOB_RECORD_HEADER_LEN, BlobExtentHeader, verify_record,
};
use pigeonhole_format::key::{Kind, split_suffix};
use pigeonhole_format::manifest::SstMeta;
use pigeonhole_format::superblock::{
    ExtentRef, FIRST_DATA_PAGE, SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock,
};
use pigeonhole_format::wal::{
    Decoded, FRAGMENT_HEADER_LEN, FRAME_SIZE, FrameDecoder, FrameEncoder, SegmentHeader, WalRecord,
};
use pigeonhole_format::{BlobFileId, Cursor, Lsn, PAGE_SIZE, StreamId};
use pigeonhole_io::{FileRef, OpenOptions, VfsRef};
use pigeonhole_pager::Pager;
use pigeonhole_shm::WriterLock;
use pigeonhole_sst::{BlobReader, ReadOptions, ScanFilter, SstReader};

use crate::catalog::Catalog;
use crate::engine::{Engine, check_local};
use crate::{Error, Result};

/// What [`Engine::check`] verifies beyond its default pass.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CheckOptions {
    /// Also read every blob pointer's record and recount each blob file's live bytes.
    pub deep: bool,
    /// Accept a database on a local FUSE mount (as `EngineOptions::allow_fuse`).
    pub allow_fuse: bool,
}

impl CheckOptions {
    /// Sets [`CheckOptions::deep`].
    #[must_use]
    pub fn deep(mut self, deep: bool) -> Self {
        self.deep = deep;
        self
    }

    /// Sets [`CheckOptions::allow_fuse`].
    #[must_use]
    pub fn allow_fuse(mut self, allow: bool) -> Self {
        self.allow_fuse = allow;
        self
    }
}

/// The structure a [`Finding`] is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum Structure {
    /// A superblock slot (the file's root).
    Superblock,
    /// The manifest's snapshot and delta blocks.
    Manifest,
    /// The extents the root and the manifest reference.
    Allocation,
    /// An SST.
    Sst,
    /// A blob file.
    Blob,
    /// A WAL sidecar file.
    Wal,
}

/// How bad a [`Finding`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum Severity {
    /// Damage: data is unreadable, or the structures disagree.
    Corruption,
    /// Something a crash can leave as well as damage (it names which).
    Warning,
    /// Expected after a crash (a torn WAL tail); reported for completeness.
    Info,
}

/// One problem [`Engine::check`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Finding {
    /// The structure.
    pub structure: Structure,
    /// How bad it is.
    pub severity: Severity,
    /// Which one: the superblock slot, SST id, blob file id or WAL stream.
    pub object: Option<u64>,
    /// The byte offset of the damage or of the structure: in the main file, or for a WAL
    /// finding in its sidecar file.
    pub offset: Option<u64>,
    /// What is wrong, for people.
    pub message: String,
}

/// What an [`Extent`] holds.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExtentKind {
    /// A superblock page (`slot` 0 or 1).
    Superblock {
        /// The slot.
        slot: u8,
    },
    /// The manifest's snapshot block.
    ManifestSnapshot,
    /// The manifest's delta log.
    ManifestLog,
    /// An SST.
    Sst {
        /// Its id.
        sst: u64,
        /// Its table's name.
        table: String,
        /// Its family's name.
        family: String,
        /// Its level.
        level: u8,
        /// Bytes of the extent the SST uses (it ends at its footer).
        used: u64,
    },
    /// One extent of a blob file.
    Blob {
        /// The blob file's id.
        blob_file: u32,
        /// The extent's position in the file.
        index: u32,
        /// Its family's name.
        family: String,
    },
}

/// A region of the main file and what it holds ([`CheckReport::extents`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Extent {
    /// What it holds.
    pub kind: ExtentKind,
    /// Its first byte.
    pub offset: u64,
    /// Its length in bytes (the whole extent; an SST's bytes in use are in its kind).
    pub len: u64,
}

/// A WAL sidecar file ([`CheckReport::wal`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct WalFile {
    /// The stream (one per shard).
    pub stream: u32,
    /// The file.
    pub path: PathBuf,
    /// Its length in bytes.
    pub len: u64,
    /// Its segment size; 0 if no slot holds a valid header.
    pub segment_size: u64,
    /// Slots holding a valid segment header.
    pub segments: u64,
    /// Records replay would read, from the stream's checkpoint to the log's end.
    pub records: u64,
    /// Where replay starts: the manifest's checkpoint for the stream, as (segment epoch,
    /// offset in the segment); `(0, 0)` replays from the oldest segment.
    pub checkpoint: (u32, u32),
    /// Where the log's valid data ends, as (segment epoch, offset in the segment).
    pub end: (u32, u64),
    /// The slot holding the segment the log ends in, if any.
    pub end_slot: Option<u64>,
}

/// How much [`Engine::check`] read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CheckCounts {
    /// Superblock slots holding a valid superblock.
    pub superblocks: u64,
    /// Extents the manifest itself occupies (snapshot and log).
    pub manifest_extents: u64,
    /// Tables in the manifest.
    pub tables: u64,
    /// SSTs, each counted once.
    pub ssts: u64,
    /// Entries read from them.
    pub sst_entries: u64,
    /// Blob files.
    pub blob_files: u64,
    /// Blob records walked.
    pub blob_records: u64,
    /// Blob pointers read through (with [`CheckOptions::deep`]).
    pub blob_pointers: u64,
    /// WAL sidecar files.
    pub wal_files: u64,
    /// WAL records read.
    pub wal_records: u64,
}

/// The result of [`Engine::check`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CheckReport {
    /// Every problem found, in the order the check met them.
    pub findings: Vec<Finding>,
    /// Every region of the main file the root and the manifest reference, by offset (the
    /// superblock pages first). Empty past the manifest if the manifest is unreadable.
    pub extents: Vec<Extent>,
    /// The WAL sidecar files, by stream.
    pub wal: Vec<WalFile>,
    /// How much was read.
    pub counts: CheckCounts,
    /// The main file's length.
    pub file_bytes: u64,
    /// The newest valid root's clean flag (`false` if no slot is valid). It means no root
    /// commit since a clean close, not that nothing was logged since: a clean reopen commits
    /// no root, so a session that then crashed before its first flush leaves it set.
    pub clean_shutdown: bool,
}

impl CheckReport {
    /// Whether no finding is a [`Severity::Corruption`].
    pub fn is_ok(&self) -> bool {
        !self
            .findings
            .iter()
            .any(|f| f.severity == Severity::Corruption)
    }
}

impl Engine {
    /// Checks the database at `path` offline (ICR 0048): both superblock slots, the manifest,
    /// every extent it references, every SST block (checksums forced), every blob record
    /// and the WAL sidecars, chained as replay reads them (see [`Structure`]). Holds
    /// the writer lock while it runs, so it fails with [`Error::WriterLocked`] while a writer
    /// has the database open; it replays nothing and writes nothing. Damage is reported in
    /// [`CheckReport::findings`]; an error means the check could not run (the file is
    /// missing or locked, or reading it failed).
    pub fn check(vfs: &VfsRef, path: &Path, options: &CheckOptions) -> Result<CheckReport> {
        let mut open = OpenOptions::read();
        open.write = true;
        let lock_file = vfs.open(path, open)?;
        check_local(&lock_file, options.allow_fuse)?;
        let _lock = WriterLock::acquire(&lock_file)?;
        let file = vfs.open(path, OpenOptions::read())?;
        let mut c = Checker {
            report: CheckReport {
                file_bytes: file.len()?,
                ..CheckReport::default()
            },
            file,
            options: options.clone(),
        };
        let catalog = c.root(vfs, path)?;
        if let Some(catalog) = &catalog {
            c.map(catalog);
            c.allocation();
            c.ssts(catalog)?;
            c.blobs(catalog)?;
        }
        c.wal(vfs, path, catalog.as_ref())?;
        c.report.extents.sort_by_key(|e| (e.offset, e.len));
        Ok(c.report)
    }
}

struct Checker {
    report: CheckReport,
    file: FileRef,
    options: CheckOptions,
}

impl Checker {
    fn find(
        &mut self,
        structure: Structure,
        severity: Severity,
        object: Option<u64>,
        offset: Option<u64>,
        message: String,
    ) {
        self.report.findings.push(Finding {
            structure,
            severity,
            object,
            offset,
            message,
        });
    }

    fn corrupt(&mut self, s: Structure, object: Option<u64>, offset: Option<u64>, m: String) {
        self.find(s, Severity::Corruption, object, offset, m);
    }

    fn extent(&mut self, kind: ExtentKind, offset: u64, len: u64) {
        self.report.extents.push(Extent { kind, offset, len });
    }

    /// Both superblock slots, then the manifest the chosen one names. `None` if the file
    /// has no usable root or its manifest is unreadable (each a finding).
    fn root(&mut self, vfs: &VfsRef, path: &Path) -> Result<Option<Catalog>> {
        let mut slots = Vec::new();
        for slot in [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B] {
            let offset = slot * PAGE_SIZE as u64;
            self.extent(
                ExtentKind::Superblock { slot: slot as u8 },
                offset,
                PAGE_SIZE as u64,
            );
            let mut page = vec![0u8; PAGE_SIZE];
            let read = if offset + PAGE_SIZE as u64 <= self.report.file_bytes {
                self.file.read_at(&mut page, offset).map(|()| true)?
            } else {
                false
            };
            slots.push((slot, offset, read, page));
        }
        let decoded: Vec<_> = slots
            .iter()
            .map(|(_, _, read, page)| read.then(|| Superblock::decode(page)))
            .collect();
        // A file a newer build wrote isn't damaged: this build can't check it.
        for d in decoded.iter().flatten() {
            if let Err(pigeonhole_format::Error::UnsupportedVersion { found, .. }) = d {
                return Err(Error::UnsupportedFormat(*found));
            }
        }
        let valid: Vec<&Superblock> = decoded.iter().flatten().flatten().collect();
        self.report.counts.superblocks = valid.len() as u64;
        let newest = valid.iter().max_by_key(|s| s.sequence).copied().cloned();
        let clean = newest.as_ref().is_some_and(|s| s.flags & 1 != 0);
        self.report.clean_shutdown = clean;
        for ((slot, offset, read, page), d) in slots.iter().zip(&decoded) {
            let never_written = !read || page.iter().all(|&b| b == 0);
            let Some(Err(e)) = d else { continue };
            if never_written {
                // A new file writes slot A only; slot B is first written by the second root
                // commit.
                continue;
            }
            // Under 512-byte sector atomicity no crash damages a slot: the 128-byte superblock
            // sits in one sector, so a tear leaves the old or the new one. Without it, a crash
            // during a root commit can tear the slot being written, which the newest root's
            // clean flag does not rule out: it means no root commit since a clean close, and a
            // clean reopen commits none, so a session after one can crash in its first root
            // commit while the clean root is still the newest. The rule assumes sector
            // atomicity there (ICR 0048): with a clean newest root (or none valid) it's damage,
            // otherwise a warning.
            let severity = if clean || newest.is_none() {
                Severity::Corruption
            } else {
                Severity::Warning
            };
            let why = if severity == Severity::Warning {
                " (a crash during a root commit leaves this too; the other slot is used)"
            } else {
                ""
            };
            self.find(
                Structure::Superblock,
                severity,
                Some(*slot),
                Some(*offset),
                format!("superblock slot {slot} is damaged: {e}{why}"),
            );
        }
        let Some(sb) = newest else {
            self.corrupt(
                Structure::Superblock,
                None,
                Some(0),
                "no superblock slot is valid: the file has no root".into(),
            );
            return Ok(None);
        };
        let high_water = sb.file_pages * PAGE_SIZE as u64;
        if high_water > self.report.file_bytes {
            self.corrupt(
                Structure::Superblock,
                None,
                Some(self.report.file_bytes),
                format!(
                    "the file is {} bytes but the root records {high_water}: it was truncated",
                    self.report.file_bytes
                ),
            );
        }

        let opened = match Pager::open(vfs, path, false) {
            Ok(o) => o,
            Err(e) => {
                self.corrupt(Structure::Superblock, None, Some(0), format!("open: {e}"));
                return Ok(None);
            }
        };
        let root = opened.root();
        if let Some(e) = root.snapshot {
            self.extent(ExtentKind::ManifestSnapshot, e.offset(), e.len());
        }
        if let Some(e) = root.log {
            self.extent(ExtentKind::ManifestLog, e.offset(), e.len());
        }
        match crate::manifest::load(&opened, 1, Arc::default()) {
            Ok((catalog, own)) => {
                self.report.counts.manifest_extents = own.len() as u64;
                self.report.counts.tables = catalog.tables().count() as u64;
                Ok(Some(catalog))
            }
            Err(e) => {
                let at = root.snapshot.map(|e| e.offset());
                self.corrupt(
                    Structure::Manifest,
                    None,
                    at,
                    format!("the manifest is unreadable: {e}"),
                );
                Ok(None)
            }
        }
    }

    /// The extent map: every SST (once) and every blob file extent the manifest names.
    fn map(&mut self, catalog: &Catalog) {
        let names = names(catalog);
        let mut seen = HashSet::new();
        for ((tablet, family), list) in catalog.ssts.iter() {
            let table = catalog.tablet(*tablet).map(|t| t.table);
            for (level, meta) in list.iter() {
                if !seen.insert(meta.id) {
                    continue;
                }
                let (table, family) = table
                    .and_then(|t| names.get(&(t, *family)))
                    .cloned()
                    .unwrap_or_default();
                let kind = ExtentKind::Sst {
                    sst: meta.id.0,
                    table,
                    family,
                    level: *level,
                    used: meta.len,
                };
                self.extent(kind, meta.extent.offset(), meta.extent.len());
            }
        }
        let families: HashMap<_, _> = names
            .into_iter()
            .map(|((_, f), (_, name))| (f, name))
            .collect();
        for (id, b) in catalog.blob_files.iter() {
            let family = families.get(&b.family).cloned().unwrap_or_default();
            for (i, e) in b.extents.iter().enumerate() {
                let kind = ExtentKind::Blob {
                    blob_file: id.0,
                    index: i as u32,
                    family: family.clone(),
                };
                self.extent(kind, e.offset(), e.len());
            }
        }
    }

    /// Every extent valid, inside the file and overlapping no other.
    fn allocation(&mut self) {
        let file_bytes = self.report.file_bytes;
        let mut spans: Vec<(u64, u64, usize)> = Vec::new();
        for (i, e) in self.report.extents.iter().enumerate() {
            if matches!(e.kind, ExtentKind::Superblock { .. }) {
                continue;
            }
            spans.push((e.offset, e.offset + e.len, i));
        }
        let mut found = Vec::new();
        for &(start, end, i) in &spans {
            let e = &self.report.extents[i];
            if let Err(why) = valid_extent(start, end - start) {
                found.push((
                    Some(start),
                    format!("{} is not a valid extent: {why}", label(e)),
                ));
            } else if end > file_bytes {
                found.push((
                    Some(start),
                    format!(
                        "{} ends at {end}, past the end of the file ({file_bytes})",
                        label(e)
                    ),
                ));
            }
        }
        spans.sort_unstable();
        spans.dedup_by_key(|s| (s.0, s.1));
        for w in spans.windows(2) {
            let ((_, a_end, a), (b_start, _, b)) = (w[0], w[1]);
            if b_start < a_end {
                let (a, b) = (&self.report.extents[a], &self.report.extents[b]);
                found.push((Some(b_start), format!("{} overlaps {}", label(a), label(b))));
            }
        }
        for (offset, m) in found {
            self.corrupt(Structure::Allocation, None, offset, m);
        }
    }

    /// Every SST, once: every block, key order, and agreement with the manifest.
    fn ssts(&mut self, catalog: &Catalog) -> Result<()> {
        let cache = Arc::new(BlockCache::disabled());
        let mut seen = HashSet::new();
        let mut blob_readers: HashMap<BlobFileId, BlobReader> = HashMap::new();
        let mut referenced: BTreeMap<BlobFileId, u64> = BTreeMap::new();
        for ((tablet, family), list) in catalog.ssts.iter() {
            let table = catalog.tablet(*tablet).map(|t| t.table);
            for (_, meta) in list.iter() {
                if !seen.insert(meta.id) {
                    continue;
                }
                self.report.counts.ssts += 1;
                if let Err(m) = self.sst(
                    catalog,
                    meta,
                    table,
                    *family,
                    &cache,
                    &mut blob_readers,
                    &mut referenced,
                ) {
                    self.corrupt(
                        Structure::Sst,
                        Some(meta.id.0),
                        Some(meta.extent.offset()),
                        m,
                    );
                }
            }
        }
        if self.options.deep {
            self.live_bytes(catalog, &cache)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn sst(
        &mut self,
        catalog: &Catalog,
        meta: &Arc<SstMeta>,
        table: Option<pigeonhole_format::TableId>,
        family: pigeonhole_format::FamilyId,
        cache: &Arc<BlockCache>,
        blob_readers: &mut HashMap<BlobFileId, BlobReader>,
        referenced: &mut BTreeMap<BlobFileId, u64>,
    ) -> std::result::Result<(), String> {
        let id = meta.id.0;
        let reader = SstReader::open(self.file.clone(), meta, Arc::clone(cache), Priority::Low)
            .map_err(|e| format!("SST {id}: {e}"))?;
        let props = reader.properties().clone();
        let mut mismatches = Vec::new();
        if Some(props.table) != table {
            mismatches.push(format!("table {:?} (manifest {table:?})", props.table));
        }
        if props.family != family {
            mismatches.push(format!("family {} (manifest {})", props.family.0, family.0));
        }
        if props.smallest_key != meta.smallest_key || props.largest_key != meta.largest_key {
            mismatches.push("key range".into());
        }
        if props.seqno_range != meta.seqno_range {
            mismatches.push(format!(
                "seqnos {:?} (manifest {:?})",
                props.seqno_range, meta.seqno_range
            ));
        }
        if props.ts_range != meta.ts_range {
            mismatches.push("timestamps".into());
        }
        if props.entries != meta.entries || props.deletes != meta.deletes {
            mismatches.push(format!(
                "{} entries, {} deletes (manifest {}, {})",
                props.entries, props.deletes, meta.entries, meta.deletes
            ));
        }
        if !mismatches.is_empty() {
            return Err(format!(
                "SST {id}'s properties disagree with the manifest: {}",
                mismatches.join(", ")
            ));
        }

        let reader = Arc::new(reader);
        let mut read = ReadOptions::default();
        read.fill_cache = false;
        let mut it = reader.iter(ScanFilter::all(), read);
        it.seek_to_first().map_err(|e| format!("SST {id}: {e}"))?;
        let mut prev: Option<Vec<u8>> = None;
        let mut entries = 0u64;
        let mut refs = Vec::new();
        while it.valid() {
            let key = it.key();
            if prev.as_deref().is_some_and(|p| p >= key) {
                return Err(format!("SST {id}: keys out of order at entry {entries}"));
            }
            if key < props.smallest_key.as_slice() || key > props.largest_key.as_slice() {
                return Err(format!(
                    "SST {id}: entry {entries} lies outside its key range"
                ));
            }
            let (_, _, _, kind) =
                split_suffix(key).map_err(|e| format!("SST {id}: entry {entries}: {e}"))?;
            if kind == Kind::Put
                && let Some(p) = blob_pointer(it.value())
            {
                match catalog.blob_files.get(&p.blob_file) {
                    Some(b) if b.family == family => {}
                    Some(b) => {
                        return Err(format!(
                            "SST {id} points into blob file {} of family {}, not its own ({})",
                            p.blob_file.0, b.family.0, family.0
                        ));
                    }
                    None => {
                        return Err(format!(
                            "SST {id} points into blob file {}, which the manifest does not name",
                            p.blob_file.0
                        ));
                    }
                }
                *referenced.entry(p.blob_file).or_default() += record_bytes(p.len);
                if self.options.deep {
                    let b = &catalog.blob_files[&p.blob_file];
                    let r = blob_readers.entry(p.blob_file).or_insert_with(|| {
                        BlobReader::new(
                            self.file.clone(),
                            p.blob_file,
                            b.extents.to_vec(),
                            Arc::clone(cache),
                        )
                    });
                    r.read(&p).map_err(|e| {
                        format!("SST {id}'s pointer into blob file {}: {e}", p.blob_file.0)
                    })?;
                    self.report.counts.blob_pointers += 1;
                }
            }
            note_blob_ref(&mut refs, key, it.value());
            prev = Some(key.to_vec());
            entries += 1;
            it.next()
                .map_err(|e| format!("SST {id}: after entry {entries}: {e}"))?;
        }
        self.report.counts.sst_entries += entries;
        if entries != meta.entries {
            return Err(format!(
                "SST {id} holds {entries} entries; the manifest records {}",
                meta.entries
            ));
        }
        if entries > 0 && prev.as_deref() != Some(props.largest_key.as_slice()) {
            return Err(format!(
                "SST {id}'s last key is not its recorded largest key"
            ));
        }
        match catalog.blob_refs.get(&meta.id) {
            Some(recorded) if *recorded != refs => {
                return Err(format!(
                    "SST {id} holds blob references {refs:?}; the manifest records {recorded:?}"
                ));
            }
            Some(_) => {}
            // No release writes an SST with pointers but no recorded references; the engine
            // treats one as pointing anywhere (blob GC keeps every file of its family).
            None if !refs.is_empty() => self.find(
                Structure::Sst,
                Severity::Warning,
                Some(id),
                Some(meta.extent.offset()),
                format!(
                    "SST {id} holds blob pointers but the manifest records no references for \
                     it: blob GC keeps every file of its family"
                ),
            ),
            None => {}
        }
        Ok(())
    }

    /// Each blob file's live bytes against the pointers into it, counted per tablet for the
    /// tablet's rows (as the engine counts them). A warning: a crash before a large value's
    /// commit settled leaves a file whose bytes nothing references yet.
    fn live_bytes(&mut self, catalog: &Catalog, cache: &Arc<BlockCache>) -> Result<()> {
        let mut refs: BTreeMap<BlobFileId, u64> = BTreeMap::new();
        for tablet in catalog.tablets() {
            let Ok(range) = crate::compact::tablet_range(&tablet) else {
                continue;
            };
            for family in catalog.family_ids_of(tablet.table) {
                let Some(list) = catalog.ssts.get(&(tablet.id, family)) else {
                    continue;
                };
                for (_, meta) in list.iter() {
                    // A damaged SST is already a finding.
                    let Ok(reader) =
                        SstReader::open(self.file.clone(), meta, Arc::clone(cache), Priority::Low)
                    else {
                        continue;
                    };
                    let reader = Arc::new(reader);
                    let mut it = reader.iter(ScanFilter::all(), ReadOptions::default());
                    let ok = match &range.start {
                        Some(s) => it.seek(s),
                        None => it.seek_to_first(),
                    };
                    if ok.is_err() {
                        continue;
                    }
                    while it.valid() {
                        if range.end.as_deref().is_some_and(|e| it.key() >= e) {
                            break;
                        }
                        if let Ok((_, _, _, Kind::Put)) = split_suffix(it.key())
                            && let Some(p) = blob_pointer(it.value())
                        {
                            *refs.entry(p.blob_file).or_default() += record_bytes(p.len);
                        }
                        if it.next().is_err() {
                            break;
                        }
                    }
                }
            }
        }
        for (id, b) in catalog.blob_files.iter() {
            let n = refs.get(id).copied().unwrap_or(0);
            if n != b.live_bytes {
                let why = " (a crash before a large value's commit settled leaves this too)";
                self.find(
                    Structure::Blob,
                    Severity::Warning,
                    Some(u64::from(id.0)),
                    b.extents.first().map(|e| e.offset()),
                    format!(
                        "blob file {}: the manifest records {} live bytes, its pointers \
                         reference {n}{why}",
                        id.0, b.live_bytes
                    ),
                );
            }
        }
        Ok(())
    }

    /// Every blob file: its extent headers, then its records end to end.
    fn blobs(&mut self, catalog: &Catalog) -> Result<()> {
        for (id, b) in catalog.blob_files.iter() {
            self.report.counts.blob_files += 1;
            if let Err((offset, m)) = self.blob(*id, &b.extents, b.total_bytes)? {
                self.corrupt(Structure::Blob, Some(u64::from(id.0)), offset, m);
            }
        }
        Ok(())
    }

    /// One blob file. The outer error is an I/O failure; the inner one a finding.
    #[allow(clippy::type_complexity)]
    fn blob(
        &mut self,
        id: BlobFileId,
        extents: &[ExtentRef],
        total: u64,
    ) -> Result<std::result::Result<(), (Option<u64>, String)>> {
        let n = id.0;
        let Some(first) = extents.first() else {
            return Ok(if total == 0 {
                Ok(())
            } else {
                Err((
                    None,
                    format!("blob file {n} records {total} bytes but no extents"),
                ))
            });
        };
        let mut header = [0u8; BLOB_EXTENT_HEADER_LEN];
        for (i, e) in extents.iter().enumerate() {
            if e.size_class != first.size_class {
                return Ok(Err((
                    Some(e.offset()),
                    format!("blob file {n}: extent {i} has another size than the first"),
                )));
            }
            if e.offset() + e.len() > self.report.file_bytes {
                // An allocation finding already says so.
                return Ok(Ok(()));
            }
            self.file.read_at(&mut header, e.offset())?;
            match BlobExtentHeader::decode(&header) {
                Ok(h) if h.blob_file == id && h.extent_index as usize == i => {}
                Ok(_) => {
                    return Ok(Err((
                        Some(e.offset()),
                        format!(
                            "blob file {n}: extent {i}'s header names another file or position"
                        ),
                    )));
                }
                Err(err) => {
                    return Ok(Err((
                        Some(e.offset()),
                        format!("blob file {n}: extent {i}'s header: {err}"),
                    )));
                }
            }
        }
        let payload = first.len() - BLOB_EXTENT_HEADER_LEN as u64;
        let capacity = payload * extents.len() as u64;
        if total > capacity {
            return Ok(Err((
                Some(first.offset()),
                format!("blob file {n} records {total} bytes in extents holding {capacity}"),
            )));
        }
        let mut at = 0u64;
        let mut head = [0u8; BLOB_RECORD_HEADER_LEN];
        let mut value = Vec::new();
        while at < total {
            let abs = |o: u64| {
                extents[(o / payload) as usize].offset()
                    + BLOB_EXTENT_HEADER_LEN as u64
                    + o % payload
            };
            if at + BLOB_RECORD_HEADER_LEN as u64 > total {
                return Ok(Err((
                    Some(abs(at)),
                    format!("blob file {n}: a record header at {at} runs past its {total} bytes"),
                )));
            }
            self.read_logical(extents, payload, at, &mut head)?;
            let len = u64::from_le_bytes(head[..8].try_into().expect("8 bytes"));
            let end = at + BLOB_RECORD_HEADER_LEN as u64 + len;
            if end > total || len > u64::from(u32::MAX) {
                return Ok(Err((
                    Some(abs(at)),
                    format!("blob file {n}: the record at {at} claims {len} bytes, past its end"),
                )));
            }
            value.resize(len as usize, 0);
            self.read_logical(
                extents,
                payload,
                at + BLOB_RECORD_HEADER_LEN as u64,
                &mut value,
            )?;
            if let Err(e) = verify_record(&head, &value, len as u32) {
                return Ok(Err((
                    Some(abs(at)),
                    format!("blob file {n}: the record at {at}: {e}"),
                )));
            }
            self.report.counts.blob_records += 1;
            at = end;
        }
        Ok(Ok(()))
    }

    /// Reads `buf.len()` bytes at logical offset `at` of a blob file whose extents each hold
    /// `payload` bytes after their header.
    fn read_logical(
        &self,
        extents: &[ExtentRef],
        payload: u64,
        mut at: u64,
        mut buf: &mut [u8],
    ) -> Result<()> {
        while !buf.is_empty() {
            let e = &extents[(at / payload) as usize];
            let within = at % payload;
            let n = (payload - within).min(buf.len() as u64) as usize;
            let (head, rest) = buf.split_at_mut(n);
            self.file
                .read_at(head, e.offset() + BLOB_EXTENT_HEADER_LEN as u64 + within)?;
            buf = rest;
            at += n as u64;
        }
        Ok(())
    }

    /// Every WAL sidecar: its segment headers, then the log from the stream's checkpoint.
    fn wal(&mut self, vfs: &VfsRef, path: &Path, catalog: Option<&Catalog>) -> Result<()> {
        let db_id = match Pager::open(vfs, path, false) {
            Ok(o) => o.db_id(),
            Err(_) => return Ok(()),
        };
        let mut streams = pigeonhole_wal::discover_streams(vfs, path)?;
        streams.sort_unstable();
        for stream in streams {
            let checkpoint = catalog
                .and_then(|c| c.checkpoints.get(&stream).copied())
                .unwrap_or_default();
            let file_path = pigeonhole_wal::stream_path(path, stream);
            let file = vfs.open(&file_path, OpenOptions::read())?;
            let len = file.len()?;
            let mut w = WalFile {
                stream: stream.0,
                path: file_path,
                len,
                segment_size: 0,
                segments: 0,
                records: 0,
                checkpoint: (checkpoint.epoch(), checkpoint.offset()),
                end: (checkpoint.epoch(), u64::from(checkpoint.offset())),
                end_slot: None,
            };
            self.report.counts.wal_files += 1;
            let mut scan = WalScan {
                file,
                stream,
                db_id,
                findings: Vec::new(),
            };
            scan.run(checkpoint, &mut w)?;
            self.report.counts.wal_records += w.records;
            self.report.findings.append(&mut scan.findings);
            self.report.wal.push(w);
        }
        Ok(())
    }
}

/// One WAL stream's read-only walk, as `pigeonhole_wal::Recovery` replays it.
struct WalScan {
    file: FileRef,
    stream: StreamId,
    db_id: [u8; 16],
    findings: Vec<Finding>,
}

const FRAME: u64 = FRAME_SIZE as u64;

impl WalScan {
    fn find(&mut self, severity: Severity, offset: Option<u64>, message: String) {
        self.findings.push(Finding {
            structure: Structure::Wal,
            severity,
            object: Some(u64::from(self.stream.0)),
            offset,
            message,
        });
    }

    fn run(&mut self, checkpoint: Lsn, w: &mut WalFile) -> Result<()> {
        let s = self.stream.0;
        let Some((segment_size, headers)) = self.grid(w.len)? else {
            if checkpoint.epoch() != 0 {
                self.find(
                    Severity::Corruption,
                    Some(0),
                    format!(
                        "WAL stream {s}: no valid segment header, but the manifest's checkpoint \
                         names epoch {}",
                        checkpoint.epoch()
                    ),
                );
            }
            return Ok(());
        };
        w.segment_size = segment_size;
        w.segments = headers.iter().flatten().count() as u64;
        let slot_of = |epoch: u32| {
            headers
                .iter()
                .position(|h| h.as_ref().is_some_and(|h| h.epoch == epoch))
        };
        let start = if checkpoint.epoch() == 0 {
            headers
                .iter()
                .enumerate()
                .filter_map(|(i, h)| h.as_ref().map(|h| (h.epoch, i)))
                .min()
                .map(|(epoch, slot)| (slot, epoch, FRAME))
        } else {
            match slot_of(checkpoint.epoch()) {
                Some(slot) => Some((
                    slot,
                    checkpoint.epoch(),
                    u64::from(checkpoint.offset()).max(FRAME),
                )),
                None => {
                    // As replay: the checkpoint's segment never reached the disk. A segment
                    // chained to the checkpoint continues the log.
                    let next = headers.iter().enumerate().find_map(|(i, h)| {
                        h.as_ref()
                            .filter(|h| {
                                h.prev_epoch == checkpoint.epoch()
                                    && h.prev_end == checkpoint.offset()
                            })
                            .map(|h| (i, h.epoch, FRAME))
                    });
                    if next.is_none()
                        && headers
                            .iter()
                            .flatten()
                            .any(|h| h.epoch > checkpoint.epoch())
                    {
                        self.find(
                            Severity::Corruption,
                            None,
                            format!(
                                "WAL stream {s}: the checkpoint's segment (epoch {}) is missing \
                                 and later segments exist",
                                checkpoint.epoch()
                            ),
                        );
                    }
                    next
                }
            }
        };
        let mut reached = HashSet::new();
        let mut cur = start;
        let mut frame = vec![0u8; FRAME_SIZE];
        while let Some((slot, epoch, from)) = cur {
            if !reached.insert(slot) {
                self.find(
                    Severity::Corruption,
                    Some(slot as u64 * segment_size),
                    format!("WAL stream {s}: the segment chain loops at epoch {epoch}"),
                );
                break;
            }
            let base = slot as u64 * segment_size;
            let mut dec = FrameDecoder::new(epoch, from);
            let mut at = from - from % FRAME;
            let mut last_end = from.max(FRAME);
            let stop = loop {
                if at >= segment_size {
                    break last_end;
                }
                self.file.read_at(&mut frame, base + at)?;
                match dec.decode(&frame) {
                    Ok(Some(Decoded::Record { offset })) => {
                        let len = dec.record().len();
                        last_end =
                            offset + FrameEncoder::new(epoch, offset).encoded_len(len) as u64;
                        if let Err(e) = WalRecord::decode(dec.record()) {
                            self.find(
                                Severity::Corruption,
                                Some(base + offset),
                                format!(
                                    "WAL stream {s}: the record at epoch {epoch} offset {offset} \
                                     has a valid checksum but does not decode: {e}"
                                ),
                            );
                        }
                        w.records += 1;
                    }
                    Ok(Some(Decoded::Stop { offset })) => break offset,
                    Ok(None) => at += FRAME,
                    Err(e) => return Err(Error::Corruption(e.to_string())),
                }
            };
            // As replay: continue in the one segment chained to this one, or end the log.
            let succ: Vec<_> = headers
                .iter()
                .enumerate()
                .filter_map(|(i, h)| h.as_ref().filter(|h| h.prev_epoch == epoch).map(|h| (i, h)))
                .collect();
            // A successor chained exactly at the stop was started there by a recovery, which
            // already dropped what follows.
            let resolved = matches!(succ.as_slice(), [(_, h)] if u64::from(h.prev_end) == stop);
            self.classify_stop(base, segment_size, epoch, stop, resolved)?;
            w.end = (epoch, stop);
            w.end_slot = Some(slot as u64);
            cur = match succ.as_slice() {
                [] => None,
                [(i, h)] if u64::from(h.prev_end) == stop => Some((*i, h.epoch, FRAME)),
                [(i, h)] => {
                    self.find(
                        Severity::Corruption,
                        Some(base + stop),
                        format!(
                            "WAL stream {s}: segment epoch {epoch}'s valid data ends at {stop}, \
                             but its successor (epoch {}) continues from {}: records between are \
                             damaged or missing",
                            h.epoch, h.prev_end
                        ),
                    );
                    Some((*i, h.epoch, FRAME))
                }
                _ => {
                    self.find(
                        Severity::Corruption,
                        Some(base),
                        format!("WAL stream {s}: segment epoch {epoch} has several successors"),
                    );
                    None
                }
            };
        }
        // Segments newer than the checkpoint that the chain never reached hold synced
        // records replay would skip.
        let floor = start.map_or(checkpoint.epoch(), |(_, e, _)| e);
        for (i, h) in headers.iter().enumerate() {
            if let Some(h) = h
                && h.epoch > floor
                && !reached.contains(&i)
            {
                self.find(
                    Severity::Corruption,
                    Some(i as u64 * segment_size),
                    format!(
                        "WAL stream {s}: segment epoch {} is not reachable from the checkpoint: \
                         a segment before it is damaged",
                        h.epoch
                    ),
                );
            }
        }
        Ok(())
    }

    /// A segment's data stopped at `stop`: a clean end (unused space or an older epoch), a
    /// torn tail, or a gap with valid fragments of this epoch after it. A gap is never proof
    /// of damage: a power loss tears the last unsynced write at sector granularity, and the
    /// sectors that survive need not be a prefix, so a crash leaves the same picture, and the
    /// WAL does not record what was synced. Nor does the root's clean flag: it means no root
    /// commit since a clean close, and a clean reopen commits none, so a crashed session
    /// after one leaves it set (perf287's re-review of #793). Damage inside the WAL is
    /// provable only where a successor exists (FORMAT §10.1: a full segment is synced before
    /// its successor's header), which is the `prev_end` check. `resolved`: a recovery already
    /// chained a new segment at `stop` and dropped what follows.
    fn classify_stop(
        &mut self,
        base: u64,
        segment_size: u64,
        epoch: u32,
        stop: u64,
        resolved: bool,
    ) -> Result<()> {
        let s = self.stream.0;
        let mut frame = vec![0u8; FRAME_SIZE];
        let stop_frame = stop - stop % FRAME;
        // A valid fragment of this epoch after the stop: in the rest of the stop's frame
        // (after damage the fragment lengths can't be trusted, so try every offset), then at
        // the start of any later frame.
        let mut later = None;
        if stop < segment_size {
            self.file.read_at(&mut frame, base + stop_frame)?;
            let from = (stop - stop_frame) as usize + 1;
            later = (from..FRAME_SIZE.saturating_sub(FRAGMENT_HEADER_LEN))
                .find(|&at| fragment_ok(&frame[at..], epoch))
                .map(|at| stop_frame + at as u64);
        }
        let mut at = stop_frame + FRAME;
        while later.is_none() && at < segment_size {
            self.file.read_at(&mut frame, base + at)?;
            if fragment_ok(&frame[..], epoch) {
                later = Some(at);
                break;
            }
            at += FRAME;
        }
        if let Some(at) = later {
            let (severity, why) = if resolved {
                (
                    Severity::Info,
                    "a recovery already continued the log at the stop and dropped them",
                )
            } else {
                (
                    Severity::Warning,
                    "replay will drop them: a power loss that tore the last unsynced write leaves \
                     this, and so does damage",
                )
            };
            self.find(
                severity,
                Some(base + stop),
                format!(
                    "WAL stream {s}: segment epoch {epoch}'s data stops at offset {stop}, but valid \
                     records follow at {at}: {why}"
                ),
            );
            return Ok(());
        }
        if stop < segment_size {
            self.file.read_at(&mut frame, base + stop_frame)?;
            let at = (stop - stop_frame) as usize;
            let h = &frame[at..(at + FRAGMENT_HEADER_LEN).min(FRAME_SIZE)];
            let unused = h.iter().all(|&b| b == 0)
                || (h.len() >= 8
                    && u32::from_le_bytes(h[4..8].try_into().expect("4 bytes")) != epoch);
            if !unused {
                self.find(
                    Severity::Info,
                    Some(base + stop),
                    format!(
                        "WAL stream {s}: segment epoch {epoch} ends in a torn write at offset \
                         {stop} (expected after a crash; replay stops there)"
                    ),
                );
            }
        }
        Ok(())
    }

    /// Every slot's header, read-only (as the wal crate's grid). `None` if no slot holds a
    /// valid header.
    #[allow(clippy::type_complexity)]
    fn grid(&mut self, len: u64) -> Result<Option<(u64, Vec<Option<SegmentHeader>>)>> {
        let s = self.stream.0;
        let mut probe = [0u8; 64];
        let mut segment_size = None;
        let mut off = 0;
        while off + probe.len() as u64 <= len {
            self.file.read_at(&mut probe, off)?;
            match SegmentHeader::decode(&probe) {
                Ok(h) if h.segment_size > 0 && off.is_multiple_of(h.segment_size) => {
                    segment_size = Some(h.segment_size);
                    break;
                }
                // A newer build's segment: the check can't judge it (as for the superblock).
                Err(pigeonhole_format::Error::UnsupportedVersion { found, .. }) => {
                    return Err(Error::UnsupportedFormat(found));
                }
                _ => {}
            }
            off += FRAME;
        }
        let Some(segment_size) = segment_size else {
            return Ok(None);
        };
        let slots = len / segment_size;
        let mut headers = Vec::new();
        for slot in 0..slots {
            let at = slot * segment_size;
            self.file.read_at(&mut probe, at)?;
            headers.push(match SegmentHeader::decode(&probe) {
                Ok(h) if h.db_id != self.db_id => {
                    self.find(
                        Severity::Corruption,
                        Some(at),
                        format!("WAL stream {s}: slot {slot} holds a segment of another database"),
                    );
                    None
                }
                Ok(h) if h.stream != self.stream || h.segment_size != segment_size => {
                    self.find(
                        Severity::Corruption,
                        Some(at),
                        format!(
                            "WAL stream {s}: slot {slot}'s header names stream {} and segment \
                             size {}",
                            h.stream.0, h.segment_size
                        ),
                    );
                    None
                }
                Ok(h) => Some(h),
                // A newer build's segment: the check can't judge it (as for the superblock).
                Err(pigeonhole_format::Error::UnsupportedVersion { found, .. }) => {
                    return Err(Error::UnsupportedFormat(found));
                }
                // Blank, or a torn header write: an unused slot.
                Err(_) => None,
            });
        }
        Ok(Some((segment_size, headers)))
    }
}

/// Whether `frame` starts with a fragment of `epoch` whose checksum holds.
fn fragment_ok(frame: &[u8], epoch: u32) -> bool {
    if frame.len() < FRAGMENT_HEADER_LEN {
        return false;
    }
    let h = &frame[..FRAGMENT_HEADER_LEN];
    let len = usize::from(u16::from_le_bytes([h[8], h[9]]));
    let end = FRAGMENT_HEADER_LEN + len;
    let ty = h[10];
    (1..=4).contains(&ty)
        && u32::from_le_bytes(h[4..8].try_into().expect("4 bytes")) == epoch
        && end <= frame.len()
        && pigeonhole_format::checksum::crc32c_append(
            pigeonhole_format::checksum::crc32c(&h[4..]),
            &frame[FRAGMENT_HEADER_LEN..end],
        ) == u32::from_le_bytes(h[..4].try_into().expect("4 bytes"))
}

/// The pager's extent rules: past the reserved first pages, a power of two from 64 KiB to
/// 64 MiB, aligned to its own size.
fn valid_extent(offset: u64, len: u64) -> std::result::Result<(), &'static str> {
    let unit = 64 * 1024u64;
    let page = offset / PAGE_SIZE as u64;
    if !offset.is_multiple_of(PAGE_SIZE as u64) || page < FIRST_DATA_PAGE {
        return Err("it starts inside the reserved header");
    }
    if !len.is_power_of_two() || len < unit || len > unit << ExtentRef::MAX_CLASS {
        return Err("its size is not a power of two from 64 KiB to 64 MiB");
    }
    if !offset.is_multiple_of(len) {
        return Err("it is not aligned to its size");
    }
    Ok(())
}

fn label(e: &Extent) -> String {
    match &e.kind {
        ExtentKind::Superblock { slot } => format!("superblock slot {slot}"),
        ExtentKind::ManifestSnapshot => "the manifest snapshot".into(),
        ExtentKind::ManifestLog => "the manifest log".into(),
        ExtentKind::Sst { sst, .. } => format!("SST {sst} (at {})", e.offset),
        ExtentKind::Blob {
            blob_file, index, ..
        } => format!("blob file {blob_file} extent {index} (at {})", e.offset),
    }
}

/// `(table, family) -> (table name, family name)`.
fn names(
    catalog: &Catalog,
) -> HashMap<(pigeonhole_format::TableId, pigeonhole_format::FamilyId), (String, String)> {
    let mut out = HashMap::new();
    for t in catalog.tables() {
        for f in &t.families {
            out.insert((t.id, f.id), (t.name.clone(), f.name.clone()));
        }
    }
    out
}

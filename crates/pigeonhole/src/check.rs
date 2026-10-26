//! The offline integrity check (ICR 0048): [`check`].

use std::path::{Path, PathBuf};

use pigeonhole_engine::{Engine, ExtentKind};

use crate::Result;

/// Checks the database at `path` for damage, offline: every superblock slot, the manifest,
/// the extents it references, every SST block (checksum, key order, agreement with the
/// manifest), every blob record, and the WAL sidecars a crash left (ICR 0048).
///
/// It takes the writer lock while it runs, so it fails with
/// [`ErrorCode::WriterLocked`](crate::ErrorCode::WriterLocked) while any process has the
/// database open as the writer. It never replays the WAL and never writes, so what it reports
/// is the file as it is on disk; a crashed database checks as it was left (a torn WAL tail is
/// an [`Severity::Info`] finding, not damage).
///
/// Damage is reported in [`CheckReport::findings`], not as an error: an error means the check
/// could not run (the file is missing or locked, or a read failed). A file a newer build
/// wrote fails with [`ErrorCode::UnsupportedFormat`](crate::ErrorCode::UnsupportedFormat).
/// The lock needs write access to the file, so a copy on read-only media can't be checked
/// in place.
///
/// ```
/// use pigeonhole::{CheckOptions, Family, Options, Pigeonhole};
///
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let path = dir.join("app.phdb");
/// let db = Pigeonhole::open(&path, Options::default())?;
/// db.table("t")?.family("f", Family::default()).create()?;
/// db.close()?;
///
/// let report = pigeonhole::check(&path, CheckOptions::default())?;
/// assert!(report.is_ok(), "{:?}", report.findings);
/// # Ok::<(), pigeonhole::Error>(())
/// ```
pub fn check(path: impl AsRef<Path>, options: CheckOptions) -> Result<CheckReport> {
    let vfs: pigeonhole_io::VfsRef = pigeonhole_io::pread::PreadVfs::new(0);
    let engine = pigeonhole_engine::CheckOptions::default()
        .deep(options.deep)
        .allow_fuse(options.allow_fuse);
    Ok(CheckReport::from_engine(Engine::check(
        &vfs,
        path.as_ref(),
        &engine,
    )?))
}

/// What [`check`] verifies, beyond its default pass.
#[derive(Clone, Default)]
pub struct CheckOptions {
    deep: bool,
    allow_fuse: bool,
}

impl std::fmt::Debug for CheckOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckOptions")
            .field("deep", &self.deep)
            .field("allow_fuse", &self.allow_fuse)
            .finish_non_exhaustive()
    }
}

impl CheckOptions {
    /// Also read through every blob pointer the SSTs hold, and recount each blob file's live
    /// bytes from them (default off: it reads each separated value once more).
    pub fn deep(mut self, deep: bool) -> Self {
        self.deep = deep;
        self
    }

    /// Accept a database on a local FUSE mount (as [`Options::allow_fuse`](crate::Options::allow_fuse)).
    pub fn allow_fuse(mut self, allow: bool) -> Self {
        self.allow_fuse = allow;
        self
    }
}

/// The structure a [`Finding`] is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum Structure {
    /// A superblock slot: the file's root.
    Superblock,
    /// The manifest: the catalog of tables, tablets and files.
    Manifest,
    /// The extents the root and the manifest reference: valid, inside the file, disjoint.
    Allocation,
    /// An SST.
    Sst,
    /// A blob file (separated values).
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
    /// Something a crash can also leave; the message says which.
    Warning,
    /// Expected after a crash, such as a torn WAL tail; reported for completeness.
    Info,
}

/// One problem [`check`] found.
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
    /// What is wrong, for people; the wording is not stable.
    pub message: String,
}

/// What a [`Region`] of the main file holds.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RegionKind {
    /// A superblock page (slot 0 or 1).
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
        /// Its table.
        table: String,
        /// Its family.
        family: String,
        /// Its level (0 is the newest).
        level: u8,
        /// Bytes of the region the SST uses; it ends at its footer.
        used: u64,
    },
    /// One extent of a blob file.
    Blob {
        /// The blob file's id.
        blob_file: u32,
        /// The extent's position in the file.
        index: u32,
        /// Its family.
        family: String,
    },
}

/// A region of the main file and what it holds ([`CheckReport::regions`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Region {
    /// What it holds.
    pub kind: RegionKind,
    /// Its first byte.
    pub offset: u64,
    /// Its length in bytes.
    pub len: u64,
}

/// A WAL sidecar file a crash left ([`CheckReport::wal`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct WalFile {
    /// The stream (one per shard).
    pub stream: u32,
    /// The file.
    pub path: PathBuf,
    /// Its length in bytes.
    pub len: u64,
    /// Its segment size; 0 if no segment header is valid.
    pub segment_size: u64,
    /// Slots holding a valid segment header.
    pub segments: u64,
    /// Records the next open would replay.
    pub records: u64,
    /// Where replay starts (the manifest's checkpoint), as (segment epoch, offset); `(0, 0)`
    /// is the oldest segment.
    pub checkpoint: (u32, u32),
    /// Where the log's valid data ends, as (segment epoch, offset).
    pub end: (u32, u64),
    /// The slot of the segment the log ends in.
    pub end_slot: Option<u64>,
}

/// How much [`check`] read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CheckCounts {
    /// Superblock slots holding a valid superblock (a new file has written only one).
    pub superblocks: u64,
    /// Tables in the manifest.
    pub tables: u64,
    /// SSTs, each counted once.
    pub ssts: u64,
    /// Entries read from them.
    pub sst_entries: u64,
    /// Blob files.
    pub blob_files: u64,
    /// Blob records verified.
    pub blob_records: u64,
    /// Blob pointers read through ([`CheckOptions::deep`]).
    pub blob_pointers: u64,
    /// WAL sidecar files.
    pub wal_files: u64,
    /// WAL records read.
    pub wal_records: u64,
}

/// The result of [`check`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CheckReport {
    /// Every problem found.
    pub findings: Vec<Finding>,
    /// Every region of the main file the root and the manifest reference, by offset.
    pub regions: Vec<Region>,
    /// The WAL sidecar files, by stream (none after a clean close).
    pub wal: Vec<WalFile>,
    /// How much was read.
    pub counts: CheckCounts,
    /// The main file's length.
    pub file_bytes: u64,
    /// The newest root's clean flag: no root commit since a clean close. A database
    /// reopened after a clean close and then crashed before its first flush still has it set,
    /// so it says nothing about the WAL.
    pub clean_shutdown: bool,
}

impl CheckReport {
    /// Whether no finding is [`Severity::Corruption`].
    pub fn is_ok(&self) -> bool {
        !self
            .findings
            .iter()
            .any(|f| f.severity == Severity::Corruption)
    }
}

impl CheckReport {
    pub(crate) fn from_engine(r: pigeonhole_engine::CheckReport) -> Self {
        use pigeonhole_engine::{Severity as S, Structure as T};
        Self {
            findings: r
                .findings
                .into_iter()
                .map(|f| Finding {
                    structure: match f.structure {
                        T::Superblock => Structure::Superblock,
                        T::Manifest => Structure::Manifest,
                        T::Allocation => Structure::Allocation,
                        T::Sst => Structure::Sst,
                        T::Blob => Structure::Blob,
                        _ => Structure::Wal,
                    },
                    severity: match f.severity {
                        S::Warning => Severity::Warning,
                        S::Info => Severity::Info,
                        _ => Severity::Corruption,
                    },
                    object: f.object,
                    offset: f.offset,
                    message: f.message,
                })
                .collect(),
            regions: r
                .extents
                .into_iter()
                .map(|e| Region {
                    kind: match e.kind {
                        ExtentKind::Superblock { slot } => RegionKind::Superblock { slot },
                        ExtentKind::ManifestSnapshot => RegionKind::ManifestSnapshot,
                        ExtentKind::ManifestLog => RegionKind::ManifestLog,
                        ExtentKind::Sst {
                            sst,
                            table,
                            family,
                            level,
                            used,
                        } => RegionKind::Sst {
                            sst,
                            table,
                            family,
                            level,
                            used,
                        },
                        ExtentKind::Blob {
                            blob_file,
                            index,
                            family,
                        } => RegionKind::Blob {
                            blob_file,
                            index,
                            family,
                        },
                        // The engine's enum is non-exhaustive; this build knows every kind.
                        _ => unreachable!("an extent kind this build does not know"),
                    },
                    offset: e.offset,
                    len: e.len,
                })
                .collect(),
            wal: r
                .wal
                .into_iter()
                .map(|w| WalFile {
                    stream: w.stream,
                    path: w.path,
                    len: w.len,
                    segment_size: w.segment_size,
                    segments: w.segments,
                    records: w.records,
                    checkpoint: w.checkpoint,
                    end: w.end,
                    end_slot: w.end_slot,
                })
                .collect(),
            counts: CheckCounts {
                superblocks: r.counts.superblocks,
                tables: r.counts.tables,
                ssts: r.counts.ssts,
                sst_entries: r.counts.sst_entries,
                blob_files: r.counts.blob_files,
                blob_records: r.counts.blob_records,
                blob_pointers: r.counts.blob_pointers,
                wal_files: r.counts.wal_files,
                wal_records: r.counts.wal_records,
            },
            file_bytes: r.file_bytes,
            clean_shutdown: r.clean_shutdown,
        }
    }
}

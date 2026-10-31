//! What a database holds and how it is stored (ICR 0047): [`Pigeonhole::info`],
//! [`PigeonholeReader::info`], and each family's stored options ([`Table::family_info`],
//! [`ReadTable::family_info`]).
//!
//! [`Pigeonhole::info`]: crate::Pigeonhole::info
//! [`PigeonholeReader::info`]: crate::PigeonholeReader::info
//! [`Table::family_info`]: crate::Table::family_info
//! [`ReadTable::family_info`]: crate::ReadTable::family_info

use std::time::Duration;

use pigeonhole_engine::{CachePriority, CompactionStyle, Compression as Codec, FamilyKind};

use crate::{Compaction, Family, Priority};

/// What a database holds and how it is stored, read from the current catalog
/// ([`Pigeonhole::info`](crate::Pigeonhole::info),
/// [`PigeonholeReader::info`](crate::PigeonholeReader::info)).
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("info.phdb"), Options::default())?;
/// db.table("users")?.family("profile", Family::default().max_versions(1)).create()?;
/// let info = db.info()?;
/// assert_eq!(info.tables[0].name, "users");
/// assert_eq!(info.tables[0].families[0].max_versions, 1);
/// # Ok::<(), pigeonhole::Error>(())
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DatabaseInfo {
    /// The database's random id, chosen at creation (WAL segments and the shared-memory
    /// region carry it too).
    pub db_id: [u8; 16],
    /// The file format version. A writer's open upgrades an older file, so a writable
    /// database reports the version this build writes; a read-only open (a reader process, or
    /// [`Options::read_only_fallback`](crate::Options::read_only_fallback)) writes nothing and
    /// reports the file's own.
    pub format_version: u32,
    /// The file's feature sets (FORMAT §8.2).
    pub features: FileFeatures,
    /// Whether this handle is a reader process ([`PigeonholeReader`](crate::PigeonholeReader)).
    pub reader: bool,
    /// Whether this handle cannot write: a reader process, or a writer that
    /// [`Options::read_only_fallback`](crate::Options::read_only_fallback) opened read-only.
    pub read_only: bool,
    /// Shards (for a reader, the writer's).
    pub shards: usize,
    /// The main file's length in bytes, the WAL sidecars not included.
    pub file_bytes: u64,
    /// Bytes of the main file no live or retired extent holds: what
    /// [`Pigeonhole::shrink`](crate::Pigeonhole::shrink) could release at most, plus the
    /// 64 KiB file header. `None` on a reader, which does not track allocations.
    pub free_bytes: Option<u64>,
    /// Every table, by name.
    pub tables: Vec<TableInfo>,
}

/// The file's feature sets ([`DatabaseInfo::features`]; FORMAT §8.2). A 1.x file sets a
/// bit only once it uses a feature added after 1.0; a build that does not know a bit refuses
/// the file (`incompat`, [`ErrorCode::UnsupportedFeature`](crate::ErrorCode::UnsupportedFeature)),
/// opens it read-only only (`ro_compat`, see
/// [`Options::read_only_fallback`](crate::Options::read_only_fallback)), or ignores it
/// (`compat`). No bit is defined yet.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct FileFeatures {
    /// Features a build that does not know them may read and write.
    pub compat: u64,
    /// Features a build that does not know them may read but not write.
    pub ro_compat: u64,
    /// Features a build that does not know them must refuse the file for.
    pub incompat: u64,
}

impl FileFeatures {
    // Not a `From` impl: the format crate's types stay out of the public API.
    fn of(f: pigeonhole_format::superblock::Features) -> Self {
        Self {
            compat: f.compat,
            ro_compat: f.ro_compat,
            incompat: f.incompat,
        }
    }
}

/// One table of [`DatabaseInfo`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TableInfo {
    /// The table's name.
    pub name: String,
    /// Tablets (contiguous row ranges) the table is split into now.
    pub tablets: usize,
    /// The families' stored options, in creation order.
    pub families: Vec<FamilyInfo>,
    /// The families' files, in the same order as `families`.
    pub storage: Vec<FamilyStorage>,
}

/// A family's stored options, as the file records them (they persist across opens; see
/// [`Family`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FamilyInfo {
    /// The family's name.
    pub name: String,
    /// The same options as a builder: passing it to
    /// [`TableBuilder::family`](crate::TableBuilder::family) defines an identical family, in
    /// this database or another.
    pub family: Family,
    /// A counter family ([`Family::counter`]): the aggregate family of
    /// [`Aggregation::Sum`](crate::Aggregation::Sum).
    pub counter: bool,
    /// The aggregation of an aggregate family ([`Family::aggregate`]), `None` for an
    /// ordinary family. A counter family's is `Some(Aggregation::Sum)`.
    pub aggregation: Option<crate::Aggregation>,
    /// Versions kept per column; 0 keeps all.
    pub max_versions: u32,
    /// Cells older than this by timestamp expire; `None` keeps them.
    pub ttl: Option<Duration>,
    /// Bloom filter bits per key; 0 means no filters.
    pub bloom_bits: u8,
    /// Values longer than this are stored in blob files; `u32::MAX` never.
    pub blob_threshold: u32,
    /// Block compression.
    pub compression: Compression,
    /// Target data block size in bytes.
    pub block_size: u32,
    /// The registered merge operator the family names, if any (`None` for a counter
    /// family: [`counter`](Self::counter) says it).
    pub merge_operator: Option<String>,
    /// Block cache priority.
    pub cache_priority: Priority,
    /// Compaction strategy.
    pub compaction: Compaction,
}

/// A family's block compression: set with [`Family::compression`], read back in
/// [`FamilyInfo::compression`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Compression {
    /// Stored as is.
    None,
    /// LZ4, the default.
    Lz4,
    /// zstd at this libzstd level (1 to 22; 0 means 3).
    Zstd(i8),
}

/// One family's files ([`TableInfo::storage`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct FamilyStorage {
    /// The family's name.
    pub family: String,
    /// SSTs per level, for the levels holding any, lowest (newest) first.
    pub levels: Vec<LevelInfo>,
    /// Blob files of the family.
    pub blob_files: u64,
    /// Their bytes.
    pub blob_bytes: u64,
    /// Bytes of those that live SSTs still reference; blob GC rewrites a file once about
    /// half of it is garbage.
    pub blob_live_bytes: u64,
}

impl FamilyStorage {
    /// SSTs over every level.
    pub fn ssts(&self) -> u64 {
        self.levels.iter().map(|l| l.ssts).sum()
    }

    /// SST bytes over every level.
    pub fn sst_bytes(&self) -> u64 {
        self.levels.iter().map(|l| l.bytes).sum()
    }
}

/// The SSTs at one level of one family ([`FamilyStorage::levels`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LevelInfo {
    /// The level; 0 holds the newest data.
    pub level: u8,
    /// SSTs, each counted once even while several tablets share it after a split.
    pub ssts: u64,
    /// Their bytes on disk.
    pub bytes: u64,
    /// Entries they hold: cell versions and deletion markers, before compaction drops what
    /// is shadowed or expired.
    pub entries: u64,
}

impl FamilyInfo {
    pub(crate) fn from_engine(f: &pigeonhole_engine::FamilyInfo) -> Self {
        let o = &f.options;
        Self {
            name: f.name.clone(),
            family: Family::from_engine(o.clone()),
            counter: o.kind == FamilyKind::Counter,
            aggregation: crate::Aggregation::of_options(o),
            max_versions: o.max_versions,
            ttl: (o.ttl_micros != 0).then(|| Duration::from_micros(o.ttl_micros)),
            bloom_bits: o.bloom_bits,
            blob_threshold: o.blob_threshold,
            compression: match o.compression {
                Codec::None => Compression::None,
                Codec::Zstd => Compression::Zstd(o.compression_level),
                _ => Compression::Lz4,
            },
            block_size: o.block_size,
            // An aggregate family's operator is the engine's own: `aggregation` says it.
            merge_operator: (!o.merge_operator.is_empty() && !o.kind.is_aggregate())
                .then(|| o.merge_operator.clone()),
            cache_priority: match o.cache_priority {
                CachePriority::Low => Priority::Low,
                CachePriority::High => Priority::High,
                _ => Priority::Normal,
            },
            compaction: match o.compaction {
                CompactionStyle::Tiered => Compaction::Tiered,
                CompactionStyle::FifoByTime => Compaction::FifoByTime,
                _ => Compaction::Leveled,
            },
        }
    }
}

impl DatabaseInfo {
    pub(crate) fn from_engine(d: pigeonhole_engine::DbInfo) -> Self {
        Self {
            db_id: d.db_id,
            format_version: d.format_version,
            features: FileFeatures::of(d.features),
            reader: d.role == pigeonhole_engine::Role::Reader,
            read_only: d.read_only,
            shards: d.shards,
            file_bytes: d.file_bytes,
            free_bytes: d.free_bytes,
            tables: d
                .tables
                .into_iter()
                .map(|t| TableInfo {
                    name: t.info.name.clone(),
                    tablets: t.tablets,
                    families: t
                        .info
                        .families
                        .iter()
                        .map(FamilyInfo::from_engine)
                        .collect(),
                    storage: t
                        .info
                        .families
                        .iter()
                        .zip(t.families)
                        .map(|(f, s)| FamilyStorage {
                            family: f.name.clone(),
                            levels: s
                                .levels
                                .into_iter()
                                .map(|l| LevelInfo {
                                    level: l.level,
                                    ssts: l.ssts,
                                    bytes: l.bytes,
                                    entries: l.entries,
                                })
                                .collect(),
                            blob_files: s.blob_files,
                            blob_bytes: s.blob_bytes,
                            blob_live_bytes: s.blob_live_bytes,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Options, Pigeonhole, days};

    #[test]
    fn family_options_read_back() -> crate::Result<()> {
        let dir = crate::doc_support::temp_dir();
        let db = Pigeonhole::open(dir.join("f.phdb"), Options::default())?;
        let custom = Family::default()
            .max_versions(3)
            .ttl(days(7))
            .bloom_bits(0)
            .blob_threshold(1024)
            .compression(crate::Compression::Zstd(5))
            .block_size(8192)
            .cache_priority(Priority::High)
            .compaction(Compaction::Tiered);
        let t = db
            .table("t")?
            .family("plain", Family::default())
            .family("custom", custom.clone())
            .family("hits", Family::counter())
            .create()?;

        let plain = t.family_info("plain").expect("plain");
        assert_eq!(plain.family, Family::default());
        assert_eq!(plain.compression, Compression::Lz4);
        assert_eq!(plain.ttl, None);
        assert_eq!(plain.merge_operator, None);
        assert!(!plain.counter);

        let c = t.family_info("custom").expect("custom");
        assert_eq!(c.family, custom);
        assert_eq!(c.max_versions, 3);
        assert_eq!(c.ttl, Some(days(7)));
        assert_eq!(c.bloom_bits, 0);
        assert_eq!(c.blob_threshold, 1024);
        assert_eq!(c.compression, Compression::Zstd(5));
        assert_eq!(c.block_size, 8192);
        assert_eq!(c.cache_priority, Priority::High);
        assert_eq!(c.compaction, Compaction::Tiered);

        let hits = t.family_info("hits").expect("hits");
        assert!(hits.counter);
        assert_eq!(hits.merge_operator, None);
        assert_eq!(hits.family, Family::counter());
        assert!(t.family_info("missing").is_none());
        db.close()
    }

    #[test]
    fn info_counts_tables_files_and_levels() -> crate::Result<()> {
        let dir = crate::doc_support::temp_dir();
        let db = Pigeonhole::open(dir.join("i.phdb"), Options::default())?;
        let b = db.table("b")?.family("f", Family::default()).create()?;
        db.table("a")?
            .family("x", Family::default())
            .family("y", Family::default())
            .create()?;
        for i in 0..100u32 {
            b.mutate(format!("row{i:03}").as_bytes())
                .put("f", b"q", b"v")
                .commit()?;
        }
        db.flush()?;
        let info = db.info()?;
        assert!(!info.reader);
        assert!(!info.read_only);
        assert_eq!(info.features, crate::FileFeatures::default());
        assert_eq!(
            info.format_version,
            pigeonhole_format::FormatVersion::CURRENT.0
        );
        assert!(info.file_bytes > 0);
        assert!(info.free_bytes.is_some_and(|f| f <= info.file_bytes));
        let names: Vec<_> = info.tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["a", "b"]);
        let a = &info.tables[0];
        assert_eq!(a.families.len(), 2);
        assert_eq!(a.storage.len(), 2);
        assert!(a.storage.iter().all(|s| s.ssts() == 0));
        let b = &info.tables[1];
        assert!(b.tablets >= 1);
        assert_eq!(b.storage[0].family, "f");
        assert!(b.storage[0].ssts() >= 1);
        assert!(b.storage[0].sst_bytes() > 0);
        assert_eq!(
            b.storage[0].levels.iter().map(|l| l.entries).sum::<u64>(),
            100
        );
        db.close()
    }
}

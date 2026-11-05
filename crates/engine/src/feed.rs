//! The change feed's writer side (D310, #1248): what keeps history for the feed reader.
//!
//! - **Retention.** Each WAL stream keeps `retain_segments` segments past its checkpoint
//!   ([`pigeonhole_wal::WalOptions::retain_segments`]); the shard's maintenance ages them out
//!   ([`Wal::trim_retained`](pigeonhole_wal::Wal::trim_retained)), never the commit path.
//! - **The blob GC horizon.** A retained record may point into a blob file that compaction
//!   or blob GC has emptied. With the feed on, the manifest writer turns each such drop into
//!   a hold ([`Feed::hold_drops`]): the file stays in the catalog with no live bytes, and the
//!   hold records every stream's current epoch. Once every stream's history floor (the
//!   oldest epoch it keeps) has passed that epoch, no record that can point into the file is
//!   left, and maintenance drops it ([`release_held`]).
//! - **The sidecar** (`<db>-feed`, FORMAT §10.4): each open of this build appends its
//!   session (or "fed: none" with the feed off, when the sidecar exists), and a feeding
//!   session appends the superblock `sequence` after each root commit
//!   ([`Feed::note_root`]), so a reader can tell when another session, of any build, wrote
//!   the database in between.
//! - **A clean close keeps the WAL files** with the feed on (their checkpoints at their
//!   ends), and the next open adopts them without replaying anything (`final_close`).

use pigeonhole_format::hash::FastMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use pigeonhole_format::BlobFileId;
use pigeonhole_format::feed::{
    FEED_SUFFIX, FeedEntry, FeedHeader, FeedTables, decode_entries, decode_entries_at,
};
use pigeonhole_format::manifest::Edit;
use pigeonhole_io::{FileRef, OpenOptions, VfsRef};
use pigeonhole_runtime::ShardId;
use pigeonhole_wal::FeedMarks;

use crate::catalog::Catalog;
use crate::manifest::{self, ManifestReq, ReqKind};
use crate::shard::Shared;
use crate::{EngineOptions, Error, FeedOptions, Result};

impl LastSession {
    /// The feed settings an open that doesn't mention the feed inherits from this session:
    /// its retention and age, and its fed tables by name (a table dropped since is left out).
    pub(crate) fn feed_options(&self, catalog: &Catalog) -> FeedOptions {
        FeedOptions {
            retention_bytes: self.retention_bytes,
            max_age_micros: (self.retention_age_secs > 0)
                .then(|| self.retention_age_secs.saturating_mul(1_000_000)),
            tables: match &self.tables {
                FeedTables::All => None,
                FeedTables::Only(ids) => Some(
                    ids.iter()
                        .filter_map(|&id| catalog.table(id).map(|t| t.name.clone()))
                        .collect(),
                ),
            },
            sync_interval_micros: None,
        }
    }
}

/// The sidecar's path: `data.phdb-feed` beside `data.phdb`.
pub(crate) fn sidecar_path(db: &Path) -> PathBuf {
    let mut p = db.as_os_str().to_owned();
    p.push(FEED_SUFFIX);
    PathBuf::from(p)
}

/// The sidecar file, open for appends.
#[derive(Debug)]
pub(crate) struct Sidecar {
    file: FileRef,
    /// Where the next entry goes.
    end: u64,
    /// The `RootIntent` this session appended for the root commit in progress, and where:
    /// its Root entry overwrites exactly that one (simp724's R2 on #1504). Root commits
    /// never overlap, so one.
    intent: Option<(u64, u64)>,
}

impl Sidecar {
    fn append(&mut self, entry: &FeedEntry) -> Result<()> {
        let mut bytes = Vec::with_capacity(64);
        entry.encode(&mut bytes);
        self.append_bytes(&bytes)
    }

    /// Appends encoded entries in one write.
    fn append_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.file.write_at(bytes, self.end)?;
        self.end += bytes.len() as u64;
        Ok(())
    }

    /// Overwrites the entry at `at` with `entry`, of the same framed length (a `RootIntent`
    /// by its `Root`): the sidecar's only write in place.
    fn overwrite(&mut self, at: u64, entry: &FeedEntry) -> Result<()> {
        let mut bytes = Vec::with_capacity(32);
        entry.encode(&mut bytes);
        self.file.write_at(&bytes, at)?;
        Ok(())
    }
}

/// Opens the sidecar and appends `entry`, synced. With `create`, a missing sidecar (or one
/// whose header is not this database's) is started afresh; without, a missing one is left
/// missing and `None` returned. A torn last entry (a crash in an append) is cut off first,
/// so the new entry follows the valid ones.
pub(crate) fn open_sidecar(
    vfs: &VfsRef,
    db: &Path,
    db_id: [u8; 16],
    entry: &FeedEntry,
    create: bool,
    opened: OpenedRoot,
) -> Result<Option<Sidecar>> {
    let path = sidecar_path(db);
    let exists = vfs.exists(&path)?;
    if !exists && !create {
        return Ok(None);
    }
    let file = vfs.open(&path, OpenOptions::read_write_create())?;
    let len = file.len()?;
    let mut bytes = vec![0u8; len as usize];
    if len > 0 {
        file.read_at(&mut bytes, 0)?;
    }
    let valid = match FeedHeader::decode(&bytes) {
        Ok(h) if h.db_id == db_id => {
            let (entries, end) = decode_entries_at(&bytes[FeedHeader::LEN..]);
            Some(resolve_intent(
                &file,
                &entries,
                FeedHeader::LEN + end,
                opened,
            )?)
        }
        // Another database's, or a torn or damaged header: its history is not ours. A reader
        // finds no session for the segments' marks and expires.
        _ => None,
    };
    let rewritten = valid.is_none();
    let mut sidecar = match valid {
        Some(end) => {
            if end as u64 != len {
                file.set_len(end as u64)?;
            }
            Sidecar {
                file,
                end: end as u64,
                intent: None,
            }
        }
        None => {
            if !create {
                return Ok(None);
            }
            file.set_len(0)?;
            file.write_at(&FeedHeader { db_id }.encode(), 0)?;
            Sidecar {
                file,
                end: FeedHeader::LEN as u64,
                intent: None,
            }
        }
    };
    sidecar.append(entry)?;
    sidecar.file.sync_all()?;
    // A new file's name, and a rewritten one's (its creator may have died before its own
    // directory sync, leaving the name in the page cache only; simp724 on fix A).
    if !exists || rewritten {
        let dir = match db.parent() {
            Some(d) if !d.as_os_str().is_empty() => d,
            _ => Path::new("."),
        };
        vfs.sync_dir(dir)?;
    }
    Ok(Some(sidecar))
}

/// The superblock an open read, before any root commit of its own: its `sequence` and
/// checksum (FORMAT §8.2), which decide a trailing `RootIntent` (D315 amendment).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OpenedRoot {
    pub(crate) sequence: u64,
    pub(crate) checksum: u64,
}

/// Resolves a `RootIntent` the previous session left as the sidecar's last entry (D315
/// amendment, #1504), before this open appends anything: a process crash came between that
/// root commit's intent and its Root entry. Returns where the valid entries now end.
///
/// - The root landed (the superblock this open read has the intent's sequence and
///   checksum): the intent becomes that session's `Root`, written over it in place, so the
///   history reads as if no crash happened, for any build.
/// - It didn't (its sequence is one past the superblock's): the intent goes, with the
///   `Catalog` entry of the same root before it, if any, cut off as a torn tail is.
/// - Anything else (a build that knows no intents opened and committed since): left alone,
///   and readers fail closed on it (`chain`).
///
/// Offsets in `entries` are past the header.
fn resolve_intent(
    file: &FileRef,
    entries: &[(usize, FeedEntry)],
    end: usize,
    opened: OpenedRoot,
) -> Result<usize> {
    // A lone Catalog entry at the end, for a root one past the superblock's: its intent was
    // lost (a power loss kept the first part of their one write) or never appended (a failed
    // append, then a failed commit). Its root never landed: cut it off, as its intent would
    // be (simp724's F1 on #1526). One for the superblock's own root lost its intent after the
    // root landed: the chain then misses that Root and fails closed.
    if let Some((at, FeedEntry::Catalog { sequence, .. })) = entries.last()
        && *sequence == opened.sequence + 1
    {
        let cut = (FeedHeader::LEN + at) as u64;
        file.set_len(cut)?;
        return Ok(cut as usize);
    }
    let Some((at, FeedEntry::RootIntent { sequence, checksum })) = entries.last() else {
        return Ok(end);
    };
    let at = (FeedHeader::LEN + at) as u64;
    if *sequence == opened.sequence && *checksum == opened.checksum {
        // The session that wrote it: the last Session entry before it.
        let session = entries.iter().rev().find_map(|(_, e)| match e {
            FeedEntry::Session { session, .. } => Some(*session),
            _ => None,
        });
        if let Some(session) = session {
            let mut root = Vec::with_capacity(32);
            FeedEntry::Root {
                session,
                sequence: *sequence,
            }
            .encode(&mut root);
            file.write_at(&root, at)?;
        }
        return Ok(end);
    }
    if *sequence == opened.sequence + 1 {
        let cut = match entries.iter().rev().nth(1) {
            Some((before, FeedEntry::Catalog { sequence: s, .. })) if s == sequence => {
                (FeedHeader::LEN + before) as u64
            }
            _ => at,
        };
        file.set_len(cut)?;
        return Ok(cut as usize);
    }
    Ok(end)
}

/// What an open inherits from the sidecar's last session (the D310 amendment, the owner
/// 2026-11-02): its retention, age, stream count and fed tables, so an open that doesn't
/// mention the feed (a tool's, `phdb`'s) continues its history instead of ending it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LastSession {
    pub(crate) retention_bytes: u64,
    pub(crate) retention_age_secs: u64,
    pub(crate) streams: u32,
    pub(crate) tables: FeedTables,
}

/// The sidecar's last session, read-only: `None` without a sidecar, with another database's
/// or a damaged header, or when the last open recorded "fed: none" (or an entry this build
/// doesn't know, which fails closed). Root entries after the session don't change it.
pub(crate) fn last_session(
    vfs: &VfsRef,
    db: &Path,
    db_id: [u8; 16],
) -> Result<Option<LastSession>> {
    let path = sidecar_path(db);
    if !vfs.exists(&path)? {
        return Ok(None);
    }
    let file = vfs.open(&path, OpenOptions::read())?;
    let mut bytes = vec![0u8; file.len()? as usize];
    file.read_at(&mut bytes, 0)?;
    match FeedHeader::decode(&bytes) {
        Ok(h) if h.db_id == db_id => {}
        _ => return Ok(None),
    }
    let (entries, _) = decode_entries(&bytes[FeedHeader::LEN..]);
    Ok(last_of(&entries))
}

/// [`last_session`]'s rule over decoded entries.
pub(crate) fn last_of(entries: &[FeedEntry]) -> Option<LastSession> {
    for e in entries.iter().rev() {
        match e {
            // A catalog change's entry (D315) is its session's, as its Root entry is.
            FeedEntry::Root { .. } | FeedEntry::Catalog { .. } => continue,
            FeedEntry::Session {
                retention_bytes,
                retention_age_secs,
                streams,
                tables,
                ..
            } => {
                return Some(LastSession {
                    retention_bytes: *retention_bytes,
                    retention_age_secs: *retention_age_secs,
                    streams: *streams,
                    tables: tables.clone(),
                });
            }
            // An entry this build doesn't know may change what the session was: fail closed.
            FeedEntry::Unfed { .. } | FeedEntry::Unknown(_) | _ => return None,
        }
    }
    None
}

/// The change feed's session at open (D310): `None` with the feed off, after appending
/// "fed: none" to an existing sidecar. With it on: the retention per stream (`bytes / N`,
/// at least two segments), a random session id, and the session's sidecar entry, synced.
pub(crate) fn open_session(
    vfs: &VfsRef,
    path: &Path,
    db_id: [u8; 16],
    catalog: &Catalog,
    options: &EngineOptions,
    shards: usize,
    opened: OpenedRoot,
) -> Result<Option<Arc<Feed>>> {
    let opened_at = opened.sequence;
    let Some(cfg) = &options.feed else {
        open_sidecar(
            vfs,
            path,
            db_id,
            &FeedEntry::Unfed {
                sequence: opened_at,
            },
            false,
            opened,
        )?;
        return Ok(None);
    };
    let tables = match &cfg.tables {
        None => FeedTables::All,
        Some(names) => {
            let mut ids = Vec::with_capacity(names.len());
            for name in names {
                let Some(t) = catalog.table_by_name(name) else {
                    return Err(Error::InvalidArgument(format!(
                        "the change feed names table {name:?}, which does not exist"
                    )));
                };
                ids.push(t.id);
            }
            ids.sort_unstable();
            ids.dedup();
            FeedTables::Only(ids)
        }
    };
    let segment = options.wal.segment_size.max(1);
    let streams = shards.max(1) as u64;
    let retain = retain_segments(cfg.retention_bytes / streams, segment);
    let mut session = 0;
    while session == 0 {
        session = vfs.random_u64();
    }
    let entry = FeedEntry::Session {
        session,
        opened_at,
        retention_bytes: cfg.retention_bytes,
        retention_age_secs: cfg.max_age_micros.map_or(0, |a| a.div_ceil(1_000_000)),
        streams: streams as u32,
        tables: tables.clone(),
    };
    let sidecar = open_sidecar(vfs, path, db_id, &entry, true, opened)?
        .ok_or_else(|| Error::Corruption("the feed sidecar could not be created".into()))?;
    let mut feed = Feed::new(session, retain, cfg.max_age_micros, tables, shards);
    feed.retain_bytes_per_stream = cfg.retention_bytes / streams;
    if let Some(us) = cfg.sync_interval_micros {
        feed.sync_interval_nanos = us.saturating_mul(1_000).max(1);
    }
    feed.retention_bytes = u64::from(retain) * segment * streams;
    if feed.retention_bytes > cfg.retention_bytes {
        crate::shard::trace!(
            "feed: retention {} raised to {} ({MIN_RETAINED} segments per stream)",
            cfg.retention_bytes,
            feed.retention_bytes
        );
    }
    feed.set_sidecar(sidecar);
    Ok(Some(Arc::new(feed)))
}

/// The lazy flush's interval, *T* (D310 §2.3): 10 ms.
const FLUSH_INTERVAL_NANOS: u64 = 10_000_000;

/// The background sync's default interval, *T_sync* (D310; simp724's M1 and perf287's
/// review on #1297): 100 ms, and 1 s on macOS, where `F_FULLFSYNC` is device-wide and
/// takes milliseconds. A durable feed on fed `Buffered` or `None` tables nothing else syncs
/// lags by about this plus a sync.
const SYNC_INTERVAL_NANOS: u64 = if cfg!(target_os = "macos") {
    1_000_000_000
} else {
    100_000_000
};

/// The fewest segments a stream retains with the feed on (D310 §2.1).
const MIN_RETAINED: u64 = 2;

/// `bytes` of retention in whole `slot_size`-byte segments, at least [`MIN_RETAINED`].
fn retain_segments(bytes: u64, slot_size: u64) -> u32 {
    bytes
        .div_ceil(slot_size.max(1))
        .clamp(MIN_RETAINED, u64::from(u32::MAX)) as u32
}

/// A blob file the horizon keeps ([`Feed::hold_drops`]).
#[derive(Debug)]
struct HeldBlob {
    id: BlobFileId,
    bytes: u64,
    /// Each stream's head epoch when the drop was held: every record that can point into
    /// the file is in a segment of at most that epoch.
    heads: Vec<u32>,
}

#[derive(Debug, Default)]
struct Held {
    files: Vec<HeldBlob>,
    /// A release is in the manifest queue.
    releasing: bool,
    /// The files that release claimed (`take_releasable`): the only drops of a file at 0
    /// live bytes the manifest writer lets through (`hold_drops`).
    claimed: Vec<BlobFileId>,
}

/// The change feed's writer-side state, in `Shared::feed` while the feed is on.
#[derive(Debug)]
pub(crate) struct Feed {
    /// This session's id, in every segment header it writes and in its sidecar entries.
    pub session: u64,
    /// Retained segments per stream, counted in the configured segment size: a new stream's.
    /// A stream whose file already exists keeps its own slot size and takes
    /// [`Feed::retain_for`] that size instead (#1471).
    pub retain_segments: u32,
    /// The requested retention per stream, in bytes (`retention_bytes / streams`).
    pub retain_bytes_per_stream: u64,
    /// The retention in effect, for the whole database: the request, rounded up to whole
    /// segments per stream and raised to two of them (`DatabaseInfo` reports it).
    pub retention_bytes: u64,
    /// Retention age, microseconds of the VFS clock.
    pub age_micros: Option<u64>,
    /// The tables this session feeds.
    pub tables: FeedTables,
    /// The lazy flush's interval for fed `Durability::None` records (D310 §2.3, *T*).
    pub flush_interval_nanos: u64,
    /// The background sync's interval for fed records no sync of their group covered
    /// (`None` and `Buffered`; *T_sync*, simp724's M1 on #1297).
    pub sync_interval_nanos: u64,
    /// Each stream's feed marks, set as its stream opens.
    marks: Vec<OnceLock<Arc<FeedMarks>>>,
    held: Mutex<Held>,
    /// Bytes of the held files (`DatabaseInfo`).
    held_bytes: AtomicU64,
    sidecar: Mutex<Option<Sidecar>>,
    /// The coalesced sidecar sync's state ([`Feed::request_sidecar_sync`]).
    sidecar_sync: Mutex<SidecarSync>,
    /// Sidecar syncs completed.
    sidecar_syncs: AtomicU64,
    /// Each stream's checkpoint history, for the feed reader's start positions
    /// ([`Feed::start_at`]).
    checkpoints: Vec<Mutex<CheckpointHistory>>,
    /// A catalog change's ceiling and edits (D315), staged by the manifest commit that made
    /// it, before its root commit, and appended as a `Catalog` sidecar entry just before that
    /// commit's Root entry. Taken by the root-commit observer, or dropped by
    /// `unstage_catalog` if the commit failed. Root commits never overlap, so one slot.
    staged_catalog: Mutex<Option<StagedCatalog>>,
    /// The session's sidecar appends stopped (D315; simp724 on #1493): an append or a sync
    /// failed, so no later catalog change can get its `Catalog` entry. Terminal. Feeds of
    /// every table in this process expire on seeing it; the manifest pump freezes the catalog
    /// hold (`catalog_hold_frozen`) so no feed's frontier passes a change that has no entry.
    appends_stopped: std::sync::atomic::AtomicBool,
    /// The catalog hold is frozen for the rest of the session (set once, by the manifest
    /// pump, after `appends_stopped`): never released.
    catalog_hold_frozen: std::sync::atomic::AtomicBool,
    /// Async feed readers waiting for one of the feed's own holds (WRITTEN/SYNCED) to let
    /// go (D310 PR 5a), by reader key. The watermark registries wake a reader whose frontier
    /// waits on the visible or durable watermark, but a hold releasing moves the frontier
    /// without a publish there, so the shard wakes these when it releases one.
    hold_waiters: Mutex<FastMap<u64, std::task::Waker>>,
    /// Whether `hold_waiters` may be non-empty: the shard's release checks this alone, so a
    /// release with no async feed waiting costs one load.
    has_hold_waiters: std::sync::atomic::AtomicBool,
    /// A hold seam (`Engine::delay_root_observer`, #1504): nanoseconds each root commit's
    /// observer waits before it records the root, widening the window between a durable
    /// superblock and its sidecar entry. 0 (the default) waits nothing.
    #[cfg(feature = "test-hooks")]
    pub(crate) root_observer_delay_nanos: AtomicU64,
    /// A hold seam (`Engine::delay_root_intent`, D315 amendment): nanoseconds each root
    /// commit waits after its intent is appended, before it writes anything, widening the
    /// window between the intent and the superblock. 0 (the default) waits nothing.
    #[cfg(feature = "test-hooks")]
    pub(crate) root_intent_delay_nanos: AtomicU64,
}

/// A catalog change staged for its root commit's `Catalog` sidecar entry (D315).
#[derive(Debug)]
pub(crate) struct StagedCatalog {
    /// The manifest commit's `seqno_ceiling` (its catalog hold).
    pub(crate) ceiling: u64,
    /// The manifest commit's `ts_floor`.
    pub(crate) timestamp: u64,
    /// Its `CreateTable`, `PutFamily` and `DropTable` edits, in the order they applied.
    pub(crate) edits: Vec<Edit>,
    /// The names, from the catalog before the edits, of the tables they name without a
    /// `CreateTable` of their own.
    pub(crate) names: Vec<(pigeonhole_format::TableId, String)>,
}

/// Whether a sidecar sync is in flight, and whether appends since its submission wait for
/// the next.
#[derive(Debug, Default)]
struct SidecarSync {
    inflight: bool,
    dirty: bool,
}

/// One stream's recent checkpoints, each with the largest key (the seqno a feed reader
/// orders a record by) of any record before it (D310 PR 3). Keys only grow along the list.
/// Written by the stream's shard as its checkpoints commit, and pruned in its maintenance
/// once the stream's floor passes them; read by feed readers choosing where to start.
#[derive(Debug, Default)]
struct CheckpointHistory {
    entries: std::collections::VecDeque<(pigeonhole_format::Lsn, pigeonhole_format::Seqno)>,
}

impl Feed {
    pub(crate) fn new(
        session: u64,
        retain_segments: u32,
        age_micros: Option<u64>,
        tables: FeedTables,
        streams: usize,
    ) -> Self {
        Feed {
            session,
            retain_segments,
            retain_bytes_per_stream: 0,
            retention_bytes: 0,
            age_micros,
            tables,
            flush_interval_nanos: FLUSH_INTERVAL_NANOS,
            sync_interval_nanos: SYNC_INTERVAL_NANOS,
            marks: (0..streams).map(|_| OnceLock::new()).collect(),
            held: Mutex::default(),
            held_bytes: AtomicU64::new(0),
            sidecar: Mutex::new(None),
            sidecar_sync: Mutex::default(),
            sidecar_syncs: AtomicU64::new(0),
            checkpoints: (0..streams).map(|_| Mutex::default()).collect(),
            staged_catalog: Mutex::default(),
            appends_stopped: std::sync::atomic::AtomicBool::new(false),
            catalog_hold_frozen: std::sync::atomic::AtomicBool::new(false),
            hold_waiters: Mutex::default(),
            has_hold_waiters: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "test-hooks")]
            root_observer_delay_nanos: AtomicU64::new(0),
            #[cfg(feature = "test-hooks")]
            root_intent_delay_nanos: AtomicU64::new(0),
        }
    }

    /// Stream `stream`'s checkpoint moved to `at`, past records of keys at most `key`
    /// (`ShardState::ckpt_key`). Called before the stream's own checkpoint mark is stored,
    /// so a reader that loaded the mark finds an entry at least that new. At most two
    /// entries per segment: the first, and the latest.
    pub(crate) fn note_checkpoint(
        &self,
        stream: usize,
        at: pigeonhole_format::Lsn,
        key: pigeonhole_format::Seqno,
    ) {
        let Some(h) = self.checkpoints.get(stream) else {
            return;
        };
        let mut h = h.lock().unwrap_or_else(PoisonError::into_inner);
        let e = &mut h.entries;
        let key = e.back().map_or(key, |&(_, k)| k.max(key));
        match e.back() {
            Some(&(last, _)) if last >= at => return,
            _ => {}
        }
        let n = e.len();
        if n >= 2 && e[n - 1].0.epoch() == at.epoch() && e[n - 2].0.epoch() == at.epoch() {
            e[n - 1] = (at, key);
        } else {
            e.push_back((at, key));
        }
    }

    /// Drops stream `stream`'s checkpoint entries below `floor` (its oldest segment kept),
    /// keeping the first at or past the floor's start: its key bounds every record before
    /// it, the lost ones included.
    pub(crate) fn prune_checkpoints(&self, stream: usize, floor: u32) {
        let Some(h) = self.checkpoints.get(stream) else {
            return;
        };
        let start = pigeonhole_format::Lsn::new(floor, 0);
        let mut h = h.lock().unwrap_or_else(PoisonError::into_inner);
        while h.entries.len() >= 2 && h.entries[1].0 <= start {
            h.entries.pop_front();
        }
    }

    /// Where a feed reader of stream `stream` starts to see every record with a key above
    /// `bound`: the latest checkpoint whose records before it all have keys at most `bound`.
    /// `None` when records the stream has already dropped (below `floor`) may have keys
    /// above `bound`: that history is gone.
    pub(crate) fn start_at(
        &self,
        stream: usize,
        bound: pigeonhole_format::Seqno,
        floor: u32,
    ) -> Option<pigeonhole_format::Lsn> {
        let h = self.checkpoints.get(stream)?;
        let h = h.lock().unwrap_or_else(PoisonError::into_inner);
        let start = pigeonhole_format::Lsn::new(floor, 0);
        // The first entry at or past the floor's start bounds what was dropped.
        let lost = h
            .entries
            .iter()
            .find(|&&(at, _)| at >= start)
            .or(h.entries.back())
            .map_or(0, |&(_, k)| k);
        if lost > bound {
            return None;
        }
        h.entries
            .iter()
            .rev()
            .find(|&&(at, k)| k <= bound && at >= start)
            .map(|&(at, _)| at)
            .or(Some(start))
    }

    /// The smallest start bound stream `stream`'s retained history serves (above `floor`):
    /// the key bounding what it already dropped. [`FeedStart::Oldest`](crate::FeedStart)
    /// starts at the largest of these over the streams.
    pub(crate) fn oldest_bound(&self, stream: usize, floor: u32) -> pigeonhole_format::Seqno {
        let Some(h) = self.checkpoints.get(stream) else {
            return 0;
        };
        let h = h.lock().unwrap_or_else(PoisonError::into_inner);
        let start = pigeonhole_format::Lsn::new(floor, 0);
        h.entries
            .iter()
            .find(|&&(at, _)| at >= start)
            .or(h.entries.back())
            .map_or(0, |&(_, k)| k)
    }

    /// The largest key any stream's checkpoint has passed (a start bound every stream can
    /// serve from its checkpoint).
    pub(crate) fn checkpoint_key(&self) -> pigeonhole_format::Seqno {
        self.checkpoints
            .iter()
            .filter_map(|h| {
                h.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .entries
                    .back()
                    .map(|&(_, k)| k)
            })
            .max()
            .unwrap_or(0)
    }

    /// Stream `stream`'s marks, if its stream is open.
    pub(crate) fn marks(&self, stream: usize) -> Option<&Arc<FeedMarks>> {
        self.marks.get(stream).and_then(OnceLock::get)
    }

    /// Whether the feed needs a record (D310): a batch or PREPARE that writes a fed table,
    /// or a decision (whose shares may), given as `None`. `batch` is asked for only when
    /// the feed names its tables; a batch that doesn't decode is not needed (its record was
    /// never appended). Only asked of `None` and `Buffered` records.
    pub(crate) fn needs<'a>(
        &self,
        batch: impl FnOnce() -> Option<pigeonhole_format::Result<pigeonhole_format::wal::BatchRef<'a>>>,
    ) -> bool {
        match &self.tables {
            FeedTables::All => true,
            FeedTables::Only(_) => match batch() {
                None => true,
                Some(Ok(batch)) => batch
                    .iter()
                    .flatten()
                    .any(|m| self.tables.contains(m.table)),
                Some(Err(e)) => {
                    // Admission decodes every batch before it can be appended (perf287 on
                    // #1297): one that fails here never reaches the log.
                    debug_assert!(false, "a batch admission decoded fails to decode: {e}");
                    false
                }
            },
        }
    }

    /// Registers `waker` (reader `key`, replacing its last one) to be woken when a hold lets
    /// go. The caller loads the frontier again afterwards: a release that published before
    /// this registration is seen there; one after it finds the waker.
    pub(crate) fn wait_for_hold(&self, key: u64, waker: &std::task::Waker) {
        let mut w = self
            .hold_waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        w.insert(key, waker.clone());
        self.has_hold_waiters.store(true, Ordering::SeqCst);
        drop(w);
        // Pairs with the fence in `wake_hold_waiters`: the caller's re-check of the frontier
        // (an Acquire load of WRITTEN/SYNCED) can't be ordered before this store, so either
        // it sees the shard's release or the shard sees this flag.
        std::sync::atomic::fence(Ordering::SeqCst);
    }

    /// Forgets reader `key`'s waker (the reader is dropped).
    pub(crate) fn forget_hold_waiter(&self, key: u64) {
        let mut w = self
            .hold_waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        w.remove(&key);
        if w.is_empty() {
            self.has_hold_waiters.store(false, Ordering::SeqCst);
        }
    }

    /// After a shard let one of its holds go (its WRITTEN or SYNCED store published first):
    /// wakes every async reader waiting on a hold. One load when none waits; out of line, so
    /// the shard's settle stays small (perf287's codegen lesson on #1297).
    #[inline(never)]
    pub(crate) fn wake_hold_waiters(&self) {
        // The release's WRITTEN/SYNCED store (Release) before the flag's load: store then
        // load on different atomics needs a SeqCst fence on each side (`wait_for_hold`).
        std::sync::atomic::fence(Ordering::SeqCst);
        if !self.has_hold_waiters.load(Ordering::SeqCst) {
            return;
        }
        let woken: Vec<std::task::Waker> = {
            let mut w = self
                .hold_waiters
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.has_hold_waiters.store(false, Ordering::SeqCst);
            w.drain().map(|(_, waker)| waker).collect()
        };
        for waker in woken {
            waker.wake();
        }
    }

    /// Stream `stream`'s marks, once its stream is open.
    pub(crate) fn set_marks(&self, stream: usize, marks: Arc<FeedMarks>) {
        if let Some(slot) = self.marks.get(stream) {
            let _ = slot.set(marks);
        }
    }

    /// The sidecar, once the open has written the session's entry.
    pub(crate) fn set_sidecar(&self, sidecar: Sidecar) {
        *self.sidecar.lock().unwrap_or_else(PoisonError::into_inner) = Some(sidecar);
    }

    /// Records the root commit about to happen (D315 amendment, #1504): the pager's intent
    /// hook calls it before the commit writes anything. Appends the commit's staged
    /// `Catalog` entry, if any, then a `RootIntent` naming the new superblock, unsynced: a
    /// process crash keeps the page cache, and the superblock write comes after. A failed
    /// append ends the session's appends, as any sidecar failure does, but never the commit
    /// (simp724's R3).
    pub(crate) fn note_intent(&self, sequence: u64, checksum: u64) {
        let staged = self
            .staged_catalog
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let mut sidecar = self.sidecar.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(s) = sidecar.as_mut() else {
            return;
        };
        let catalog = staged.map(|c| FeedEntry::Catalog {
            session: self.session,
            sequence,
            seqno_ceiling: c.ceiling,
            timestamp: c.timestamp,
            edits: c.edits,
            names: c.names,
        });
        // One write for both (simp724's F1 on #1526): a crash between two would leave the
        // Catalog entry without its intent, which no open could place.
        let mut bytes = Vec::with_capacity(128);
        if let Some(c) = &catalog {
            c.encode(&mut bytes);
        }
        let at = s.end + bytes.len() as u64;
        FeedEntry::RootIntent { sequence, checksum }.encode(&mut bytes);
        let appended = s
            .append_bytes(&bytes)
            .map(|()| s.intent = Some((sequence, at)));
        if let Err(e) = appended {
            crate::shard::trace!("feed: sidecar append of intent {sequence} failed: {e}");
            *sidecar = None;
            self.appends_stopped.store(true, Ordering::Release);
        }
        drop(sidecar);
        #[cfg(feature = "test-hooks")]
        {
            let delay = self.root_intent_delay_nanos.load(Ordering::Relaxed);
            if delay > 0 {
                std::thread::sleep(std::time::Duration::from_nanos(delay));
            }
        }
    }

    /// Records a root commit (the pager's root-commit observer, after the commit is
    /// durable): its `Root` entry, written over the `RootIntent` this session appended for
    /// it, or appended if there is none (simp724's R2 on #1504: never over another), then
    /// the coalesced sync.
    pub(crate) fn note_root(self: &Arc<Self>, sequence: u64) {
        #[cfg(feature = "test-hooks")]
        {
            let delay = self.root_observer_delay_nanos.load(Ordering::Relaxed);
            if delay > 0 {
                std::thread::sleep(std::time::Duration::from_nanos(delay));
            }
        }
        // A catalog change whose intent didn't append it (no intent hook ran): its entry
        // goes first, in the same append as the Root entry.
        let staged = self
            .staged_catalog
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let file = {
            let mut sidecar = self.sidecar.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(s) = sidecar.as_mut() else {
                return;
            };
            let root = FeedEntry::Root {
                session: self.session,
                sequence,
            };
            let written = match s.intent.take() {
                Some((seq, at)) if seq == sequence && staged.is_none() => s.overwrite(at, &root),
                _ => staged
                    .map(|c| FeedEntry::Catalog {
                        session: self.session,
                        sequence,
                        seqno_ceiling: c.ceiling,
                        timestamp: c.timestamp,
                        edits: c.edits,
                        names: c.names,
                    })
                    .iter()
                    .try_for_each(|c| s.append(c))
                    .and_then(|()| s.append(&root)),
            };
            if let Err(e) = written {
                crate::shard::trace!("feed: sidecar write of root {sequence} failed: {e}");
                *sidecar = None;
                self.appends_stopped.store(true, Ordering::Release);
                return;
            }
            Arc::clone(&s.file)
        };
        self.request_sidecar_sync(file);
    }

    /// Stages a catalog change for its root commit's `Catalog` entry (D315), before that
    /// commit.
    pub(crate) fn stage_catalog(&self, change: StagedCatalog) {
        *self
            .staged_catalog
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(change);
    }

    /// Whether the session's sidecar appends stopped (D315): see `appends_stopped`.
    pub(crate) fn appends_stopped(&self) -> bool {
        self.appends_stopped.load(Ordering::Acquire)
    }

    /// Marks the catalog hold frozen for the rest of the session: the hold `release_catalog`
    /// would have let go stays up.
    pub(crate) fn freeze_catalog_hold(&self) {
        self.catalog_hold_frozen.store(true, Ordering::Release);
    }

    /// Whether the catalog hold is frozen for the rest of the session.
    pub(crate) fn catalog_hold_frozen(&self) -> bool {
        self.catalog_hold_frozen.load(Ordering::Acquire)
    }

    /// Drops a staged catalog change whose root commit didn't happen (D315).
    pub(crate) fn unstage_catalog(&self) {
        self.staged_catalog
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }

    /// Makes the sidecar's appends durable (D310 call 4, fix A): an entry a power loss
    /// dropped would break the session chain at the next open and expire every durable
    /// feed, which call 4 promises survives a restart; now only a crash before the sync
    /// lands does (the safe direction).
    ///
    /// - **Coalesced** (perf287): at most one sync in flight; appends made meanwhile share
    ///   the next one. At most one per root commit, never one per data commit.
    /// - **Never waited on:** the thread that completes a root commit (the manifest pump's,
    ///   or an application thread making a catalog change) only submits it. The pread
    ///   backend runs it on its pool and io_uring in the kernel, so no thread blocks on it, a
    ///   shard's least of all. The simulator runs it inline, to keep seeds deterministic.
    /// - **At the pager's durability** (simp724): the VFS's `sync_all`, `F_FULLFSYNC` on
    ///   macOS, and `sync_all` since an append grows the file.
    /// - **Fail closed:** a failed sync ends the session's appends (no retry).
    fn request_sidecar_sync(self: &Arc<Self>, file: FileRef) {
        {
            let mut st = self
                .sidecar_sync
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if st.inflight {
                st.dirty = true;
                return;
            }
            st.inflight = true;
        }
        let this = Arc::clone(self);
        let again = Arc::clone(&file);
        // `sync_all`: an append grows the file, and its new length must be durable with it.
        drop(file.submit_sync_all().map(move |r| {
            if let Err(e) = &r {
                // Fail closed (simp724): no retry, and no append that a later sync could
                // report clean over pages the kernel dropped. The next open finds the
                // sequence ahead of the last entry and expires every token.
                crate::shard::trace!("feed: sidecar sync failed, no more appends: {e}");
                *this.sidecar.lock().unwrap_or_else(PoisonError::into_inner) = None;
                this.appends_stopped.store(true, Ordering::Release);
                let mut st = this
                    .sidecar_sync
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                st.inflight = false;
                st.dirty = false;
                return Ok(());
            }
            this.sidecar_syncs.fetch_add(1, Ordering::Relaxed);
            let more = {
                let mut st = this
                    .sidecar_sync
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                st.inflight = false;
                std::mem::take(&mut st.dirty)
            };
            if more {
                this.request_sidecar_sync(again);
            }
            Ok(())
        }));
    }

    /// Whether a sidecar sync is in flight or wanted: Root entries a power loss may still
    /// drop (an inspection helper for tests).
    #[cfg(feature = "test-hooks")]
    pub(crate) fn sidecar_sync_pending(&self) -> bool {
        let st = self
            .sidecar_sync
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        st.inflight || st.dirty
    }

    /// Sidecar syncs completed (`Engine::feed_sidecar_syncs`).
    pub(crate) fn sidecar_syncs(&self) -> u64 {
        self.sidecar_syncs.load(Ordering::Relaxed)
    }

    /// The WAL bytes retained as history, over every stream.
    pub(crate) fn retained_bytes(&self) -> u64 {
        self.marks
            .iter()
            .filter_map(OnceLock::get)
            .map(|m| m.retained_bytes())
            .sum()
    }

    /// The segments a stream whose file has `slot_size`-byte slots retains: the requested
    /// bytes per stream in whole slots, at least [`MIN_RETAINED`] (#1471: an existing file
    /// keeps its slot size whatever the open's `wal_segment_size`, so counting in the
    /// configured size kept too little history or far too much).
    pub(crate) fn retain_for(&self, slot_size: u64) -> u32 {
        retain_segments(self.retain_bytes_per_stream, slot_size)
    }

    /// The retention in effect, for the whole database: each stream's retained segments
    /// times its file's own slot size, once every stream is open; until then, as computed
    /// at open in the configured size.
    pub(crate) fn retention_in_effect(&self) -> u64 {
        let open: Vec<u64> = self
            .marks
            .iter()
            .filter_map(OnceLock::get)
            .map(|m| m.retention())
            .collect();
        if open.len() == self.marks.len() {
            open.iter().sum()
        } else {
            self.retention_bytes
        }
    }

    /// The bytes of blob files kept only for the horizon.
    pub(crate) fn held_bytes(&self) -> u64 {
        self.held_bytes.load(Ordering::Relaxed)
    }

    /// Holds `id` (`bytes` long) until every stream's history passes its current head.
    pub(crate) fn hold(&self, id: BlobFileId, bytes: u64) {
        let heads: Vec<u32> = self
            .marks
            .iter()
            .map(|m| m.get().map_or(u32::MAX, |m| m.head()))
            .collect();
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(h) = held.files.iter_mut().find(|h| h.id == id) {
            h.heads = heads;
            return;
        }
        held.files.push(HeldBlob { id, bytes, heads });
        self.held_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// The manifest writer's interception (under its exclusion, against the catalog the
    /// edits apply to): each drop of a blob file becomes a live-0 put of the same file, and
    /// a hold, unless it is the release in flight dropping a file it claimed. That covers
    /// a compaction's, blob GC's or a large value's release. Whether a drop is a release is
    /// never inferred from the file having no live bytes: a later path that wrote a live-0
    /// file and dropped it in another commit would bypass the horizon (simp724's H1).
    pub(crate) fn hold_drops(&self, catalog: &Catalog, edits: Vec<Edit>) -> Vec<Edit> {
        if !edits.iter().any(|e| matches!(e, Edit::DropBlobFile { .. })) {
            return edits;
        }
        let claimed = {
            let held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
            held.claimed.clone()
        };
        edits
            .into_iter()
            .map(|e| match e {
                Edit::DropBlobFile { blob_file } if !claimed.contains(&blob_file) => {
                    match catalog.blob_files.get(&blob_file) {
                        Some(b) => {
                            self.hold(blob_file, b.total_bytes);
                            Edit::PutBlobFile {
                                blob_file,
                                family: b.family,
                                extents: b.extents.to_vec(),
                                total_bytes: b.total_bytes,
                                live_bytes: 0,
                            }
                        }
                        // Already gone from the catalog: the drop is a no-op either way.
                        None => e,
                    }
                }
                e => e,
            })
            .collect()
    }

    /// The held files no retained record can point into any more, claimed for one release
    /// (`None` while one is in flight or none is due).
    fn take_releasable(&self) -> Option<Vec<BlobFileId>> {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        if held.releasing || held.files.is_empty() {
            return None;
        }
        let floors: Vec<Option<u32>> = self
            .marks
            .iter()
            .map(|m| m.get().map(|m| m.floor()))
            .collect();
        let due: Vec<BlobFileId> = held
            .files
            .iter()
            .filter(|h| {
                h.heads
                    .iter()
                    .zip(&floors)
                    .all(|(&head, floor)| floor.is_some_and(|f| f > head))
            })
            .map(|h| h.id)
            .collect();
        if due.is_empty() {
            return None;
        }
        held.releasing = true;
        held.claimed.clone_from(&due);
        Some(due)
    }

    /// A release's outcome: the files go from the hold list on success (whether or not the
    /// catalog still had them at 0 live bytes); on failure they stay for the next one.
    fn released(&self, ids: &[BlobFileId], ok: bool) {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        held.releasing = false;
        held.claimed.clear();
        if ok {
            let mut freed = 0;
            held.files.retain(|h| {
                let gone = ids.contains(&h.id);
                if gone {
                    freed += h.bytes;
                }
                !gone
            });
            self.held_bytes.fetch_sub(freed, Ordering::Relaxed);
        }
    }
}

/// Drops the held blob files the history has passed (from a shard's maintenance): one
/// manifest request at a time, computed against the catalog at commit, so a file whose
/// hold came from a commit that failed (still live there) is left alone.
pub(crate) fn release_held(shared: &Arc<Shared>, shard: ShardId) {
    let Some(feed) = &shared.feed else {
        return;
    };
    let Some(ids) = feed.take_releasable() else {
        return;
    };
    crate::shard::trace!("feed: releasing held blob files {ids:?}");
    let drop_ids = ids.clone();
    let change = move |catalog: &mut Catalog| {
        Ok(drop_ids
            .iter()
            .filter(|id| {
                catalog
                    .blob_files
                    .get(id)
                    .is_some_and(|b| b.live_bytes == 0)
            })
            .map(|id| Edit::DropBlobFile { blob_file: *id })
            .collect())
    };
    let weak = Arc::downgrade(feed);
    let req = ManifestReq::edits(Vec::new(), move |r| {
        if let Some(feed) = weak.upgrade() {
            feed.released(&ids, r.is_ok());
        }
    });
    let req = ManifestReq {
        kind: ReqKind::Catalog(Box::new(change)),
        ..req
    };
    manifest::submit(shared, shard, req);
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_format::TableId;

    fn session(streams: u32) -> FeedEntry {
        FeedEntry::Session {
            session: 1,
            opened_at: 1,
            retention_bytes: 1 << 20,
            retention_age_secs: 60,
            streams,
            tables: FeedTables::Only(vec![TableId(2)]),
        }
    }

    /// D310's amendment: an open inherits the last session's settings, through any Root
    /// entries after it, and nothing once a later open recorded the feed off (or an entry
    /// this build doesn't know).
    #[test]
    fn the_last_session_is_inherited_until_an_open_turns_the_feed_off() {
        assert_eq!(last_of(&[]), None);
        let want = LastSession {
            retention_bytes: 1 << 20,
            retention_age_secs: 60,
            streams: 3,
            tables: FeedTables::Only(vec![TableId(2)]),
        };
        let roots = [
            session(3),
            FeedEntry::Root {
                session: 1,
                sequence: 2,
            },
            FeedEntry::Root {
                session: 1,
                sequence: 3,
            },
        ];
        assert_eq!(last_of(&roots), Some(want.clone()));
        // The newest session wins.
        assert_eq!(
            last_of(&[session(2), session(3)]).map(|l| l.streams),
            Some(3)
        );
        // A feed-off open since: nothing to inherit.
        assert_eq!(
            last_of(&[session(3), FeedEntry::Unfed { sequence: 4 }]),
            None
        );
        // An entry this build doesn't know: fail closed.
        assert_eq!(last_of(&[session(3), FeedEntry::Unknown(0xee)]), None);
        // A feed-off open, then a feeding one: that one.
        assert_eq!(
            last_of(&[FeedEntry::Unfed { sequence: 1 }, session(3)]),
            Some(want)
        );
    }
}

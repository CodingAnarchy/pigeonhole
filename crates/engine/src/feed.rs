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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use pigeonhole_format::BlobFileId;
use pigeonhole_format::feed::{FEED_SUFFIX, FeedEntry, FeedHeader, FeedTables, decode_entries};
use pigeonhole_format::manifest::Edit;
use pigeonhole_io::{FileRef, OpenOptions, VfsRef};
use pigeonhole_runtime::ShardId;
use pigeonhole_wal::FeedMarks;

use crate::catalog::Catalog;
use crate::manifest::{self, ManifestReq, ReqKind};
use crate::shard::Shared;
use crate::{EngineOptions, Error, Result};

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
}

impl Sidecar {
    fn append(&mut self, entry: &FeedEntry) -> Result<()> {
        let mut bytes = Vec::with_capacity(64);
        entry.encode(&mut bytes);
        self.file.write_at(&bytes, self.end)?;
        self.end += bytes.len() as u64;
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
            let (_, end) = decode_entries(&bytes[FeedHeader::LEN..]);
            Some(FeedHeader::LEN + end)
        }
        // Another database's, or a torn or damaged header: its history is not ours. A reader
        // finds no session for the segments' marks and expires.
        _ => None,
    };
    let mut sidecar = match valid {
        Some(end) => {
            if end as u64 != len {
                file.set_len(end as u64)?;
            }
            Sidecar {
                file,
                end: end as u64,
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
            }
        }
    };
    sidecar.append(entry)?;
    sidecar.file.sync_all()?;
    if !exists {
        let dir = match db.parent() {
            Some(d) if !d.as_os_str().is_empty() => d,
            _ => Path::new("."),
        };
        vfs.sync_dir(dir)?;
    }
    Ok(Some(sidecar))
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
    opened_at: u64,
) -> Result<Option<Arc<Feed>>> {
    let Some(cfg) = &options.feed else {
        open_sidecar(
            vfs,
            path,
            db_id,
            &FeedEntry::Unfed {
                sequence: opened_at,
            },
            false,
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
    let retain = (cfg.retention_bytes / streams)
        .div_ceil(segment)
        .clamp(MIN_RETAINED, u64::from(u32::MAX)) as u32;
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
    let sidecar = open_sidecar(vfs, path, db_id, &entry, true)?
        .ok_or_else(|| Error::Corruption("the feed sidecar could not be created".into()))?;
    let mut feed = Feed::new(session, retain, cfg.max_age_micros, tables, shards);
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
    /// Retained segments per stream.
    pub retain_segments: u32,
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
            retention_bytes: 0,
            age_micros,
            tables,
            flush_interval_nanos: FLUSH_INTERVAL_NANOS,
            sync_interval_nanos: SYNC_INTERVAL_NANOS,
            marks: (0..streams).map(|_| OnceLock::new()).collect(),
            held: Mutex::default(),
            held_bytes: AtomicU64::new(0),
            sidecar: Mutex::new(None),
        }
    }

    /// Whether the feed needs `record` (D310): a batch or PREPARE that writes a fed table,
    /// or a decision (whose shares may). Only asked of `None` and `Buffered` records.
    pub(crate) fn needs(&self, record: &pigeonhole_format::wal::WalRecord<'_>) -> bool {
        use pigeonhole_format::wal::WalRecord;
        match record {
            WalRecord::Batch { batch, .. } | WalRecord::Prepare { batch, .. } => match &self.tables
            {
                FeedTables::All => true,
                FeedTables::Only(_) => batch
                    .iter()
                    .flatten()
                    .any(|m| self.tables.contains(m.table)),
            },
            _ => true,
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

    /// After a root commit of this session (the pager's observer): appends its sequence.
    /// Not synced, and a failure is only traced: a lost entry makes the next feeding open
    /// see the sequence ahead of the last one recorded, so every token expires, which is the
    /// safe direction (D310 §2.2).
    pub(crate) fn note_root(&self, sequence: u64) {
        let mut sidecar = self.sidecar.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(s) = sidecar.as_mut()
            && let Err(e) = s.append(&FeedEntry::Root {
                session: self.session,
                sequence,
            })
        {
            crate::shard::trace!("feed: sidecar append after root {sequence} failed: {e}");
        }
    }

    /// The WAL bytes retained as history, over every stream.
    pub(crate) fn retained_bytes(&self) -> u64 {
        self.marks
            .iter()
            .filter_map(OnceLock::get)
            .map(|m| m.retained_bytes())
            .sum()
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

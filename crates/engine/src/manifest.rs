//! The single manifest writer: a snapshot block plus a delta log in the page file (decision
//! D7, FORMAT §9). Reads at open, appends a delta per commit, rewrites the snapshot when the
//! log is full or outgrows it.
//!
//! Every manifest change (catalog edits from application threads, flush and compaction
//! outputs from background tasks, checkpoints from shards) is a [`ManifestReq`] on one
//! queue. Whoever processes the queue takes the `busy` flag, applies every queued request to
//! a copy of the catalog, writes one delta block (or a new snapshot), commits the root, and
//! then publishes a new view, retires the extents no tablet references any more and resolves
//! each request's reply. A background [`ManifestPump`] task does this with
//! `Pager::submit_commit_root` and blocks on the completion (no fsync on a shard's foreground
//! loop, decision D30); an application thread making a catalog change does the same inline
//! with the blocking `Pager::commit_root`.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;

use pigeonhole_format::manifest::{
    Edit, LOG_EXTENT_LEN, MANIFEST_HEADER_LEN, ManifestBlockKind, ManifestHeader, decode_block,
    encode_block,
};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{FormatVersion, ManifestVersion, SstId, TabletId};
use pigeonhole_io::{Completion, FileRef};
use pigeonhole_pager::{Extent, OpenedPager, Pager, Root};
use pigeonhole_runtime::{Notifier, ShardId, Task, TaskPoll, TaskWaker, Waiter, completion};
use pigeonhole_sst::SstReader;

use crate::catalog::Catalog;
use crate::compact::CompactionRecord;
use crate::shard::{ShardMsg, Shared};
use crate::snapshot::{ShardMems, SstSet, TabletMap, View, ViewPin};
use crate::waker::StdWaker;
use crate::{Error, Result};

/// Reads the manifest named by `opened`'s root and rebuilds the catalog from it. Returns the
/// catalog and the live manifest extents (snapshot and log).
pub(crate) fn load(
    opened: &OpenedPager,
    shards: usize,
    registry: Arc<pigeonhole_compaction::MergeRegistry>,
) -> Result<(Catalog, Vec<Extent>)> {
    let root = opened.root();
    let file = opened.file();
    let mut catalog = Catalog::with_registry(registry);
    let mut live = Vec::new();
    if let Some(snapshot) = root.snapshot {
        live.push(snapshot);
        let bytes = read(file, snapshot, 0, root.snapshot_len as usize)?;
        let (header, edits, used) = decode_block(&bytes)?;
        if header.kind != ManifestBlockKind::Snapshot || used != bytes.len() {
            return Err(Error::Corruption("manifest snapshot block".to_owned()));
        }
        for e in &edits {
            catalog.apply(e, shards)?;
        }
        let mut version = header.manifest_version;
        if let Some(log) = root.log {
            live.push(log);
            let log_bytes = read(file, log, 0, root.log_len as usize)?;
            let mut at = 0;
            while at < log_bytes.len() {
                let (header, edits, used) = decode_block(&log_bytes[at..])?;
                if header.kind != ManifestBlockKind::Delta || header.manifest_version != version + 1
                {
                    return Err(Error::Corruption("manifest delta log".to_owned()));
                }
                version = header.manifest_version;
                for e in &edits {
                    catalog.apply(e, shards)?;
                }
                at += used;
            }
        }
        if version != root.manifest_version {
            return Err(Error::Corruption(
                "manifest version does not match the superblock".to_owned(),
            ));
        }
    } else if root.manifest_version != 0 || root.log.is_some() {
        return Err(Error::Corruption(
            "superblock names a manifest version without a snapshot".to_owned(),
        ));
    }
    Ok((catalog, live))
}

fn read(file: &FileRef, extent: Extent, offset: u64, len: usize) -> Result<Vec<u8>> {
    if offset + len as u64 > extent.len() {
        return Err(Error::Corruption(
            "manifest block extends past its extent".to_owned(),
        ));
    }
    let mut buf = vec![0u8; len];
    file.read_at(&mut buf, extent.offset() + offset)?;
    Ok(buf)
}

/// A manifest write in flight: the root to commit and what to do once it is durable.
#[derive(Debug)]
pub(crate) struct Prepared {
    pub root: Root,
    /// Manifest extents a snapshot rewrite replaces (retired once the root is durable).
    retire: Vec<Extent>,
    /// Extents the rewrite allocated (abandoned if the commit fails).
    fresh: Vec<Extent>,
}

/// The writer's manifest state: the current root and the pager that commits it.
#[derive(Debug)]
pub(crate) struct ManifestWriter {
    pager: Arc<Pager>,
    root: Root,
    /// Length of the live snapshot block (for the "log outgrows the snapshot" rule).
    snapshot_len: u32,
    /// Set once a root commit failed: the pager is poisoned (decision D58) and the engine
    /// requires a reopen.
    poisoned: bool,
}

impl ManifestWriter {
    pub(crate) fn new(pager: Arc<Pager>) -> Self {
        let root = pager.root();
        Self {
            pager,
            snapshot_len: root.snapshot_len,
            root,
            poisoned: false,
        }
    }

    pub(crate) fn poisoned_error() -> Error {
        Error::Io(pigeonhole_io::Error::new(
            pigeonhole_io::ErrorKind::Other,
            "an earlier manifest commit failed; reopen the database (decision D58)",
        ))
    }

    /// The version the next commit gets.
    pub(crate) fn next_version(&self) -> ManifestVersion {
        self.root.manifest_version + 1
    }

    /// The current root.
    pub(crate) fn root(&self) -> Root {
        self.root
    }

    /// Writes `edits` as manifest version `current + 1` (a delta, or a new snapshot when the
    /// delta does not fit the log, outgrows the snapshot, or `rewrite` asks for one) and
    /// returns the root to commit. `catalog` is the state *after* `edits`.
    pub(crate) fn prepare(
        &mut self,
        catalog: &Catalog,
        edits: &[Edit],
        rewrite: bool,
    ) -> Result<Prepared> {
        if self.poisoned {
            return Err(Self::poisoned_error());
        }
        let version = self.next_version();
        let mut delta = Vec::new();
        encode_block(
            &ManifestHeader {
                version: FormatVersion::CURRENT,
                kind: ManifestBlockKind::Delta,
                manifest_version: version,
                edit_count: 0,
                body_len: 0,
            },
            edits,
            &mut delta,
        );
        let fits_log = !rewrite
            && match self.root.log {
                Some(_) if self.root.snapshot.is_some() => {
                    let end = self.root.log_len as usize + delta.len();
                    end <= LOG_EXTENT_LEN
                        && end <= self.snapshot_len.max(MANIFEST_HEADER_LEN as u32) as usize
                }
                _ => false,
            };
        let result = if fits_log {
            self.append_delta(&delta, version)
        } else {
            self.write_snapshot(catalog, version)
        };
        result.inspect_err(|_| self.poisoned = true)
    }

    fn append_delta(&mut self, delta: &[u8], version: ManifestVersion) -> Result<Prepared> {
        let log = self.root.log.expect("checked by caller");
        self.pager.write(log, u64::from(self.root.log_len), delta)?;
        Ok(Prepared {
            root: Root {
                log_len: self.root.log_len + delta.len() as u32,
                manifest_version: version,
                ..self.root
            },
            retire: Vec::new(),
            fresh: Vec::new(),
        })
    }

    fn write_snapshot(&mut self, catalog: &Catalog, version: ManifestVersion) -> Result<Prepared> {
        let mut block = Vec::new();
        encode_block(
            &ManifestHeader {
                version: FormatVersion::CURRENT,
                kind: ManifestBlockKind::Snapshot,
                manifest_version: version,
                edit_count: 0,
                body_len: 0,
            },
            &catalog.snapshot_edits(),
            &mut block,
        );
        let snapshot = self.pager.allocate(block.len() as u64)?;
        let log = match self.pager.allocate(LOG_EXTENT_LEN as u64) {
            Ok(log) => log,
            Err(e) => {
                self.pager.abandon(snapshot);
                return Err(e.into());
            }
        };
        if let Err(e) = self.pager.write(snapshot, 0, &block) {
            self.pager.abandon(snapshot);
            self.pager.abandon(log);
            return Err(e.into());
        }
        Ok(Prepared {
            root: Root {
                snapshot: Some(snapshot),
                snapshot_len: block.len() as u32,
                log: Some(log),
                log_len: 0,
                manifest_version: version,
            },
            retire: [self.root.snapshot, self.root.log]
                .into_iter()
                .flatten()
                .collect(),
            fresh: vec![snapshot, log],
        })
    }

    /// Finishes a prepared commit with the root commit's outcome. On success the old
    /// manifest extents a rewrite replaced are retired at the new version (decision D61
    /// clamps reclaim to the durable root, so a later `reclaim` frees them).
    pub(crate) fn complete(
        &mut self,
        p: Prepared,
        result: std::result::Result<(), pigeonhole_io::Error>,
    ) -> Result<ManifestVersion> {
        match result {
            Ok(()) => {
                self.root = p.root;
                self.snapshot_len = p.root.snapshot_len;
                for e in p.retire {
                    self.pager.retire(e, p.root.manifest_version);
                }
                Ok(p.root.manifest_version)
            }
            Err(e) => {
                self.poisoned = true;
                for e in p.fresh {
                    self.pager.abandon(e);
                }
                Err(e.into())
            }
        }
    }

    /// Commits synchronously (open-time flushes, before the shards run).
    pub(crate) fn commit(&mut self, catalog: &Catalog, edits: &[Edit]) -> Result<ManifestVersion> {
        let p = self.prepare(catalog, edits, false)?;
        let r = self.pager.commit_root(p.root).map_err(pager_io);
        self.complete(p, r)
    }

    /// Records a clean close (one more root commit).
    pub(crate) fn mark_clean(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(Self::poisoned_error());
        }
        self.pager.mark_clean().map_err(|e| {
            self.poisoned = true;
            e.into()
        })
    }
}

/// A catalog change computed against the current catalog under the writer's exclusion.
pub(crate) type CatalogChange = Box<dyn FnOnce(&mut Catalog) -> Result<Vec<Edit>> + Send>;

/// A tablet split, merge or move computed against the current catalog: its edits, then the
/// owners to give tablets once the edits are applied (owners are not persisted).
pub(crate) type TabletChange = Box<dyn FnOnce(&mut Catalog) -> Result<TabletEdits> + Send>;

/// A tablet change's edits and the owners it hands out.
pub(crate) type TabletEdits = (Vec<Edit>, Vec<(TabletId, ShardId)>);

/// What a request changes.
pub(crate) enum ReqKind {
    /// Edits computed by the caller (flush and compaction outputs, checkpoints).
    Edits(Vec<Edit>),
    /// A catalog change (table and family creation allocate ids).
    Catalog(CatalogChange),
    /// A tablet split, merge or move: publishes a new tablet map.
    Tablets(TabletChange),
}

/// One queued manifest change.
pub(crate) struct ManifestReq {
    pub kind: ReqKind,
    /// Readers of the SSTs the edits add, opened by whoever wrote them.
    pub readers: Vec<(SstId, Arc<SstReader>)>,
    /// Memtables the edits flush, as `(shard, root)`: dropped from the published view.
    pub flushed_roots: Vec<(u16, u32)>,
    /// A compaction to record (test hook; the version is filled in at commit).
    pub compaction: Option<CompactionRecord>,
    /// Rewrite the manifest snapshot (shrink relocates the manifest extents).
    pub rewrite_snapshot: bool,
    /// Called with the outcome once the commit is durable (or failed).
    pub reply: Box<dyn FnOnce(Result<ManifestVersion>) + Send>,
}

impl std::fmt::Debug for ManifestReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestReq")
            .field("readers", &self.readers.len())
            .field("flushed_roots", &self.flushed_roots)
            .finish_non_exhaustive()
    }
}

impl ManifestReq {
    /// A request with precomputed edits and a reply closure.
    pub(crate) fn edits(
        edits: Vec<Edit>,
        reply: impl FnOnce(Result<ManifestVersion>) + Send + 'static,
    ) -> Self {
        Self {
            kind: ReqKind::Edits(edits),
            readers: Vec::new(),
            flushed_roots: Vec::new(),
            compaction: None,
            rewrite_snapshot: false,
            reply: Box::new(reply),
        }
    }

    /// A request answered through a [`Waiter`].
    pub(crate) fn with_waiter(kind: ReqKind) -> (Self, Waiter<Result<ManifestVersion>>) {
        let (tx, rx) = completion();
        let req = Self {
            kind,
            readers: Vec::new(),
            flushed_roots: Vec::new(),
            compaction: None,
            rewrite_snapshot: false,
            reply: Box::new(move |r| tx.notify(r)),
        };
        (req, rx)
    }
}

/// The queue of pending requests.
#[derive(Default)]
pub(crate) struct ManifestQueue {
    reqs: Mutex<Vec<ManifestReq>>,
}

impl std::fmt::Debug for ManifestQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestQueue").finish_non_exhaustive()
    }
}

impl ManifestQueue {
    pub(crate) fn push(&self, req: ManifestReq) {
        self.reqs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(req);
    }

    fn take(&self) -> Vec<ManifestReq> {
        std::mem::take(&mut *self.reqs.lock().unwrap_or_else(PoisonError::into_inner))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.reqs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    }
}

/// A commit in flight: the root is being written; everything else waits for its outcome.
pub(crate) struct Commit {
    reqs: Vec<(ManifestReq, Result<()>)>,
    old: Arc<Catalog>,
    catalog: Arc<Catalog>,
    prepared: Prepared,
    readers: HashMap<SstId, Arc<SstReader>>,
    flushed_roots: Vec<(u16, u32)>,
    tablets_changed: bool,
    /// A split, merge or move (`ReqKind::Tablets`) committed: shards re-check what waited on
    /// the tablet map.
    tablet_change: bool,
    sst_changed: bool,
    checkpoints_changed: bool,
}

impl std::fmt::Debug for Commit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Commit")
            .field("version", &self.prepared.root.manifest_version)
            .field("requests", &self.reqs.len())
            .finish_non_exhaustive()
    }
}

impl Commit {
    pub(crate) fn root(&self) -> Root {
        self.prepared.root
    }
}

/// Whether `edit` names a tablet that no longer exists (an output of a flush or compaction
/// that raced a table drop).
fn orphaned(catalog: &Catalog, edit: &Edit, created: &[TabletId]) -> bool {
    match edit {
        Edit::AddSst { tablet, .. } | Edit::SetFlushed { tablet, .. } => {
            catalog.tablet(*tablet).is_none() && !created.contains(tablet)
        }
        _ => false,
    }
}

/// Extents of SSTs an edit list newly adds (to abandon if the request is refused). An SST
/// the same list also removes is moved (a trivial move re-adds it at another level), not
/// new: its extent is not this request's to free.
fn added_extents(edits: &[Edit]) -> Vec<ExtentRef> {
    let moved: Vec<SstId> = edits
        .iter()
        .filter_map(|e| match e {
            Edit::RemoveSst { sst, .. } => Some(*sst),
            _ => None,
        })
        .collect();
    edits
        .iter()
        .filter_map(|e| match e {
            Edit::AddSst { meta, .. } if !moved.contains(&meta.id) => Some(meta.extent),
            _ => None,
        })
        .collect()
}

/// Takes the queued requests and prepares one commit for them. Returns `None` if the queue
/// was empty. Requests that cannot be applied are answered at once with their error; a
/// poisoned pager answers every request with the poison error.
///
/// Must be called with the `manifest_busy` flag held.
pub(crate) fn begin(shared: &Shared) -> Option<Commit> {
    let reqs = shared.manifest_queue.take();
    if reqs.is_empty() {
        return None;
    }
    crate::shard::trace!("manifest: begin with {} requests", reqs.len());
    let mut writer = shared
        .manifest
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let version = writer.next_version();
    let current = shared.view.load_full();
    let old = Arc::clone(&current.catalog);
    let mut catalog = (*old).clone();
    let mut edits: Vec<Edit> = Vec::new();
    let mut outcomes: Vec<(ManifestReq, Result<()>)> = Vec::with_capacity(reqs.len());
    let mut readers = HashMap::new();
    let mut flushed_roots = Vec::new();
    let mut tablets_changed = false;
    let mut tablet_change = false;
    let mut sst_changed = false;
    let mut checkpoints_changed = false;
    let mut rewrite = false;
    // Owners handed out by tablet changes, re-applied whenever the catalog is rebuilt.
    let mut retargets: Vec<(TabletId, ShardId)> = Vec::new();
    for mut req in reqs {
        let kind = std::mem::replace(&mut req.kind, ReqKind::Edits(Vec::new()));
        let mut owners = Vec::new();
        let is_tablets = matches!(kind, ReqKind::Tablets(_));
        let own: Result<Vec<Edit>> = match kind {
            ReqKind::Edits(e) => Ok(e),
            ReqKind::Catalog(f) => f(&mut catalog),
            ReqKind::Tablets(f) => f(&mut catalog).map(|(edits, o)| {
                owners = o;
                tablets_changed = true;
                tablet_change = true;
                edits
            }),
        };
        let own = own.and_then(|own| {
            // Tablets the request itself creates (a split's or merge's outputs) are not
            // orphans.
            let created: Vec<TabletId> = own
                .iter()
                .filter_map(|e| match e {
                    Edit::PutTablet { tablet, .. } => Some(*tablet),
                    _ => None,
                })
                .collect();
            if own.iter().any(|e| orphaned(&catalog, e, &created)) {
                // Output for a dropped table is never published: free it now. A tablet
                // change only re-references SSTs it does not own, so it frees nothing.
                if !is_tablets {
                    for x in added_extents(&own) {
                        shared.pager.abandon(x);
                    }
                }
                return Err(Error::TableNotFound("the table was dropped".to_owned()));
            }
            Ok(own)
        });
        match own {
            Ok(own) => {
                let mut applied = Ok(());
                for e in &own {
                    if let Err(err) = catalog.apply(e, shared.shards) {
                        applied = Err(err);
                        break;
                    }
                    match e {
                        Edit::CreateTable { .. }
                        | Edit::DropTable { .. }
                        | Edit::PutTablet { .. }
                        | Edit::DropTablet { .. } => tablets_changed = true,
                        Edit::AddSst { .. } | Edit::RemoveSst { .. } => sst_changed = true,
                        Edit::WalCheckpoint { .. } => checkpoints_changed = true,
                        _ => {}
                    }
                    if matches!(e, Edit::DropTable { .. }) {
                        sst_changed = true;
                    }
                }
                match applied {
                    Ok(()) => {
                        for (t, shard) in &owners {
                            catalog.set_shard(*t, *shard);
                        }
                        retargets.extend(owners);
                        edits.extend(own);
                        for (id, r) in req.readers.drain(..) {
                            readers.insert(id, r);
                        }
                        flushed_roots.append(&mut req.flushed_roots);
                        rewrite |= req.rewrite_snapshot;
                        if let Some(mut c) = req.compaction.take() {
                            c.manifest_version = version;
                            shared
                                .compactions
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .push(c);
                        }
                        outcomes.push((req, Ok(())));
                    }
                    Err(e) => {
                        for x in added_extents(&own) {
                            shared.pager.abandon(x);
                        }
                        // A half-applied request: start over from the old catalog.
                        catalog = (*old).clone();
                        for e in &edits {
                            let _ = catalog.apply(e, shared.shards);
                        }
                        for (t, shard) in &retargets {
                            catalog.set_shard(*t, *shard);
                        }
                        outcomes.push((req, Err(e)));
                    }
                }
            }
            Err(e) => {
                outcomes.push((req, Err(e)));
            }
        }
    }
    if outcomes.iter().all(|(_, r)| r.is_err()) {
        drop(writer);
        for (req, r) in outcomes {
            (req.reply)(r.map(|()| 0));
        }
        return None;
    }
    // Counters go with every commit: ids, the seqno ceiling and the timestamp floor.
    catalog.counters.next_sst = shared.sst_ids.load(Ordering::Relaxed);
    catalog.counters.next_blob_file = shared.blob_ids.load(Ordering::Relaxed);
    catalog.counters.seqno_ceiling = shared.shm.next_seqno();
    catalog.counters.ts_floor = shared
        .ts_floors
        .iter()
        .map(|f| f.0.load(Ordering::Acquire))
        .max()
        .unwrap_or(0)
        .max(catalog.counters.ts_floor);
    let counters = catalog.counters_edit();
    let _ = catalog.apply(&counters, shared.shards);
    edits.push(counters);
    match writer.prepare(&catalog, &edits, rewrite) {
        Ok(prepared) => {
            drop(writer);
            Some(Commit {
                reqs: outcomes,
                old,
                catalog: Arc::new(catalog),
                prepared,
                readers,
                flushed_roots,
                tablets_changed,
                tablet_change,
                sst_changed,
                checkpoints_changed,
            })
        }
        Err(e) => {
            drop(writer);
            shared.pager_poisoned.store(true, Ordering::Release);
            let msg = e.to_string();
            for (req, r) in outcomes {
                (req.reply)(
                    r.and_then(|()| Err(crate::error::io_other("manifest commit", msg.clone()))),
                );
            }
            None
        }
    }
}

/// Finishes a commit whose root commit returned `result`: publishes the view, retires what
/// the new catalog no longer references, reclaims, and answers every request.
///
/// Must be called with the `manifest_busy` flag held.
pub(crate) fn end(
    shared: &Shared,
    commit: Commit,
    result: std::result::Result<(), pigeonhole_io::Error>,
) {
    let Commit {
        reqs,
        old,
        catalog,
        prepared,
        mut readers,
        flushed_roots,
        tablets_changed,
        tablet_change,
        sst_changed,
        checkpoints_changed,
    } = commit;
    let version = {
        let mut writer = shared
            .manifest
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        writer.complete(prepared, result)
    };
    crate::shard::trace!("manifest: end -> {version:?}");
    let version = match version {
        Ok(v) => v,
        Err(e) => {
            shared.pager_poisoned.store(true, Ordering::Release);
            let msg = e.to_string();
            for (req, r) in reqs {
                (req.reply)(
                    r.and_then(|()| Err(crate::error::io_other("manifest commit", msg.clone()))),
                );
            }
            return;
        }
    };
    let tablets = catalog.tablets();
    let live: std::collections::HashSet<_> = tablets.iter().map(|t| t.id).collect();
    let published = shared.publish_view(|cur, view_version| {
        let mems = cur
            .mems
            .iter()
            .enumerate()
            .map(|(i, piece)| {
                let shard = i as u16;
                let dropped = |root: u32| flushed_roots.contains(&(shard, root));
                let piece = piece.without(&dropped).unwrap_or_else(|| Arc::clone(piece));
                if piece.map.keys().all(|k| live.contains(&k.0)) {
                    piece
                } else {
                    Arc::new(ShardMems {
                        map: piece
                            .map
                            .iter()
                            .filter(|(k, _)| live.contains(&k.0))
                            .map(|(k, v)| (*k, Arc::clone(v)))
                            .collect(),
                    })
                }
            })
            .collect();
        let ssts = Arc::new(SstSet::build(
            &catalog,
            Some(&cur.ssts),
            &mut readers,
            shared.pager.file().clone(),
            Arc::clone(&shared.cache),
        ));
        View {
            version: view_version,
            manifest_version: version,
            tablets: if tablets_changed {
                Arc::new(TabletMap::build(cur.tablets.version() + 1, &tablets))
            } else {
                Arc::clone(&cur.tablets)
            },
            catalog: Arc::clone(&catalog),
            mems,
            ssts,
            _pin: Some(ViewPin::new(&shared.live_views, version)),
        }
    });
    if let Err(e) = &published {
        // The manifest is durable but the view could not be published (an oversized view,
        // decision D28). The published view now lags the durable catalog, and the next
        // commit would retire what this one already retired: no further commits (the
        // pager is poisoned until reopen), and the requesters must not touch their extents.
        shared.pager_poisoned.store(true, Ordering::Release);
        let msg = e.to_string();
        for (req, r) in reqs {
            (req.reply)(r.and_then(|()| Err(Error::Corruption(msg.clone()))));
        }
        return;
    }
    // Memtables whose SSTs this commit adds leave every view from now on; their shards keep
    // excluding them until they retire them (`Shared::flushed_roots`). Only once the view
    // is published: a failed publish keeps the old view, which still needs them.
    if !flushed_roots.is_empty() {
        shared
            .flushed_roots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(flushed_roots.iter().copied());
    }
    // Extents no tablet references any more are reclaimable once no view older than this
    // version lives (decision D61).
    for meta in catalog.removed_since(&old) {
        shared.pager.retire(meta.extent, version);
        shared
            .cache
            .erase_files(&[pigeonhole_sst::sst_cache_file(meta.id)]);
    }
    for (id, blob) in &old.blob_files {
        if !catalog.blob_files.contains_key(id) {
            for e in &blob.extents {
                shared.pager.retire(*e, version);
            }
            shared
                .cache
                .erase_files(&[pigeonhole_sst::blob_cache_file(*id)]);
        }
    }
    shared.reclaim();
    for (req, r) in reqs {
        (req.reply)(r.map(|()| version));
    }
    if sst_changed || checkpoints_changed || tablet_change {
        shared.broadcast(|| ShardMsg::Maintain);
    }
}

/// Claims the writer's exclusion; `false` if someone else holds it.
pub(crate) fn claim(shared: &Shared) -> bool {
    !shared.manifest_busy.swap(true, Ordering::AcqRel)
}

/// Releases the exclusion, then runs the close's final step if it was waiting for it.
pub(crate) fn release(shared: &Shared) {
    shared.manifest_busy.store(false, Ordering::Release);
    // Pairs with the fence in `Shared::try_final_close`: either it claims the exclusion
    // released here, or this sees its pending flag.
    std::sync::atomic::fence(Ordering::SeqCst);
    shared.try_final_close();
}

/// Commits `kind` on a thread that already holds the exclusion: drains the queue (what is
/// queued commits first, ahead of or with `kind`) and returns `kind`'s outcome.
pub(crate) fn commit_held(shared: &Shared, kind: ReqKind) -> Result<ManifestVersion> {
    let (req, mut waiter) = ManifestReq::with_waiter(kind);
    shared.manifest_queue.push(req);
    drain_sync(shared);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::Pin::new(&mut waiter).poll(&mut cx) {
        Poll::Ready(Some(r)) => r,
        Poll::Ready(None) | Poll::Pending => Err(Error::Closed),
    }
}

/// Processes the queue on the calling (application) thread until it is empty, blocking on
/// each root commit. Must hold the exclusion.
pub(crate) fn drain_sync(shared: &Shared) {
    while let Some(commit) = begin(shared) {
        let r = shared.pager.commit_root(commit.root()).map_err(pager_io);
        end(shared, commit, r);
    }
}

pub(crate) fn pager_io(e: pigeonhole_pager::Error) -> pigeonhole_io::Error {
    match e {
        pigeonhole_pager::Error::Io(e) => e,
        other => pigeonhole_io::Error::os("root commit", std::io::Error::other(other.to_string())),
    }
}

/// Submits `req` from an application thread and waits for its outcome: processes the queue
/// inline when the writer is free, otherwise waits for whoever holds it (a background pump
/// re-checks the queue before it lets go).
pub(crate) fn commit_from_thread(shared: &Shared, kind: ReqKind) -> Result<ManifestVersion> {
    let (req, waiter) = ManifestReq::with_waiter(kind);
    commit_req_from_thread(shared, req, waiter)
}

/// As [`commit_from_thread`], for a request built with [`ManifestReq::with_waiter`].
pub(crate) fn commit_req_from_thread(
    shared: &Shared,
    req: ManifestReq,
    mut waiter: Waiter<Result<ManifestVersion>>,
) -> Result<ManifestVersion> {
    shared.manifest_queue.push(req);
    let mut spins = 0u32;
    loop {
        if claim(shared) {
            // Release, then re-check: a request pushed after the last `begin` returned
            // `None` found the exclusion held, and its pump left; nobody else will run it.
            loop {
                drain_sync(shared);
                #[cfg(feature = "test-hooks")]
                race_window(shared);
                release(shared);
                if shared.manifest_queue.is_empty() || !claim(shared) {
                    break;
                }
            }
        }
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        match std::pin::Pin::new(&mut waiter).poll(&mut cx) {
            Poll::Ready(Some(r)) => return r,
            Poll::Ready(None) => return Err(Error::Closed),
            Poll::Pending => {}
        }
        spins += 1;
        if spins < 64 {
            std::thread::yield_now();
        } else {
            std::thread::sleep(std::time::Duration::from_micros(50));
        }
    }
}

/// Test hook: when armed, pushes a request into the queue inside the window between the
/// last `begin` of a drain and the release of the exclusion (as a shard's submit would,
/// whose pump then finds the exclusion held and leaves), and keeps its waiter.
#[cfg(feature = "test-hooks")]
fn race_window(shared: &Shared) {
    if shared.manifest_race.swap(false, Ordering::AcqRel) {
        let (req, waiter) = ManifestReq::with_waiter(ReqKind::Edits(Vec::new()));
        shared.manifest_queue.push(req);
        *shared
            .manifest_race_waiter
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(waiter);
    }
}

/// Test hook: while `Shared::manifest_park` is set, a pump whose compaction commit (SSTs
/// changed, no memtable flushed) completed waits before `end` (the window in which a close
/// once committed over it, issue #78).
#[cfg(feature = "test-hooks")]
fn parked(shared: &Shared, commit: &Commit, slot: &Slot, waker: &TaskWaker) -> bool {
    if !shared.manifest_park.load(Ordering::Acquire)
        || !commit.sst_changed
        || !commit.flushed_roots.is_empty()
        || slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
    {
        return false;
    }
    *shared
        .manifest_parked
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(waker.clone());
    true
}

/// Submits `req` from a shard or task: queues it and makes sure a pump runs on `shard`.
pub(crate) fn submit(shared: &Shared, shard: pigeonhole_runtime::ShardId, req: ManifestReq) {
    shared.manifest_queue.push(req);
    let _ = shared.submitter(shard).submit(ShardMsg::PumpManifest);
}

/// The outcome slot a pump's root commit resolves into.
type Slot = Arc<Mutex<Option<std::result::Result<(), pigeonhole_io::Error>>>>;

/// A cooperative task that processes the manifest queue: one commit at a time, blocked on
/// the root commit's completion between slices.
pub(crate) struct ManifestPump {
    shared: Arc<Shared>,
    inflight: Option<(Commit, Slot)>,
    waker: StdWaker,
}

impl ManifestPump {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            inflight: None,
            waker: StdWaker::default(),
        }
    }

    fn start(&mut self, commit: Commit, task: &TaskWaker) {
        let slot: Slot = Arc::new(Mutex::new(None));
        let completion: Completion<()> = self.shared.pager.submit_commit_root(commit.root());
        let (s, w) = (Arc::clone(&slot), task.clone());
        let _ = self.waker.get(task);
        drop(completion.map(move |r| {
            *s.lock().unwrap_or_else(PoisonError::into_inner) = Some(r);
            w.wake();
            Ok(())
        }));
        self.inflight = Some((commit, slot));
    }
}

impl Drop for ManifestPump {
    /// A pump dropped with its commit in flight (an application-owned shard dropped before
    /// the commit finished) still holds the exclusion. Finish the commit with its root
    /// commit's outcome, or as failed if that has not arrived (the outcome is unknown, so
    /// the pager is poisoned as for any failed commit), then release, which runs a pending
    /// final close.
    fn drop(&mut self) {
        let Some((commit, slot)) = self.inflight.take() else {
            return;
        };
        let result = slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| {
                Err(pigeonhole_io::Error::new(
                    pigeonhole_io::ErrorKind::Other,
                    "the shard running the manifest commit was dropped",
                ))
            });
        end(&self.shared, commit, result);
        release(&self.shared);
    }
}

impl Task for ManifestPump {
    fn run(&mut self, _deadline_nanos: u64, waker: &TaskWaker) -> TaskPoll {
        loop {
            if let Some((_commit, slot)) = &self.inflight {
                #[cfg(feature = "test-hooks")]
                if parked(&self.shared, _commit, slot, waker) {
                    return TaskPoll::Blocked;
                }
                let done = slot.lock().unwrap_or_else(PoisonError::into_inner).take();
                let Some(result) = done else {
                    return TaskPoll::Blocked;
                };
                let (commit, _) = self.inflight.take().expect("checked");
                end(&self.shared, commit, result);
                release(&self.shared);
                if self.shared.manifest_queue.is_empty() {
                    return TaskPoll::Done;
                }
            }
            if !claim(&self.shared) {
                // Someone else is committing and re-checks the queue before letting go.
                crate::shard::trace!("manifest pump: busy, leaving");
                return TaskPoll::Done;
            }
            match begin(&self.shared) {
                Some(commit) => {
                    self.start(commit, waker);
                    return TaskPoll::Blocked;
                }
                None => {
                    release(&self.shared);
                    if self.shared.manifest_queue.is_empty() {
                        return TaskPoll::Done;
                    }
                }
            }
        }
    }

    fn name(&self) -> &'static str {
        "manifest"
    }
}

/// A reply closure resolving a [`Notifier`].
pub(crate) fn notify(
    tx: Notifier<Result<ManifestVersion>>,
) -> impl FnOnce(Result<ManifestVersion>) {
    move |r| tx.notify(r)
}

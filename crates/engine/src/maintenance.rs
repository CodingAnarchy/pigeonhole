//! Online backup and shrink.
//!
//! **Backup** writes a brand-new database at a snapshot: every `(tablet, family)`'s sources
//! (memtables and SSTs) are merged in key order and every entry with a seqno at or below the
//! snapshot's is written into SSTs at the last level of the new file, whose manifest records
//! them as flushed through that seqno. The copy is exactly what a read at the snapshot sees,
//! writers keep running meanwhile, and the result is one clean file.
//!
//! It runs in two phases so the snapshot's memtables (arena chunks writers need) are held
//! only while they are copied (#262): first every memtable's entries are written into
//! temporary SSTs in the new file (at most an arena's worth, compressed per family), then
//! the snapshot is released except for its SST set and the long merge reads the temporary
//! SSTs and the source SSTs. The temporary SSTs' extents are freed after the merge, which
//! reuses them only in part: the copy can end up to about an arena larger than a freshly
//! compacted file (still valid; free space is rebuilt from the manifest at open, D8).
//!
//! Separated values (#58): the temporary SSTs keep every value inline, so phase 1 writes no
//! blob file into the copy. Phase 2 reads each source blob pointer through the snapshot's
//! SST view (its pinned manifest version keeps the source blob files' extents) and writes
//! the value, like the large values of the temporary SSTs, into the copy's own blob files,
//! which hold exactly what the copy references. Each slot's merge is clamped to its
//! tablet's rows: after a split, children share SSTs that hold their siblings' rows too.
//!
//! **Shrink** truncates the free tail, relocates the extents past the pager's shrink point
//! that the manifest names (decision D60: never an in-flight flush or compaction output,
//! which the manifest does not name yet, nor an SST a running compaction reads), publishes
//! the moves against the catalog current at commit time, and truncates the file again once
//! everything retired has been reclaimed.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_compaction::{MergingCursor, NewBlobFile};
use pigeonhole_format::key::split_suffix;
use pigeonhole_format::manifest::{Edit, SstMeta};
use pigeonhole_format::scan::ScanFilter;
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{BlobFileId, Cursor, FamilyId, SstId};
use pigeonhole_pager::Pager;
use pigeonhole_sst::{ReadOptions, SstReader, SstWriterOptions};

use crate::catalog::Catalog;
use crate::flush::SstSink;
use crate::manifest::{self, ManifestReq, ManifestWriter, ReqKind};
use crate::read::clamp_to_tablet;
use crate::shard::Shared;
use crate::snapshot::{ShardMems, Snapshot, SstSet, View, ViewPin};
use crate::source::{Source, mem_sources};
use crate::{Error, Result};

/// Entries `backup` copies between checks for a close.
const CLOSE_CHECK_EVERY: u64 = 4096;

/// Writes a consistent copy of `snapshot` to `dest` (which must not exist). Stops with
/// `Closed` (removing the partial copy) once the engine is closing. The snapshot's memtables
/// are released after the first phase; see the module docs.
pub(crate) fn backup(shared: &Shared, snapshot: Snapshot, dest: &Path) -> Result<()> {
    let seqno = snapshot.seqno;
    let view = Arc::clone(&snapshot.view);
    let source = Arc::clone(&view.catalog);
    let pager = Arc::new(Pager::create(&shared.vfs, dest)?);
    let mut catalog = Catalog::with_registry(Arc::clone(source.registry()));
    let mut edits = Vec::new();
    for t in source.tables() {
        edits.push(Edit::CreateTable {
            table: t.id,
            name: t.name.clone(),
        });
        for f in &t.families {
            edits.push(Edit::PutFamily {
                table: t.id,
                family: f.id,
                name: f.name.clone(),
                options: f.options.clone(),
            });
        }
    }
    for t in source.tablets() {
        edits.push(Edit::PutTablet {
            tablet: t.id,
            table: t.table,
            start: t.start.clone(),
            end: t.end.clone(),
        });
    }
    let sst_ids = Arc::new(AtomicU64::new(1));
    let blob_ids = Arc::new(AtomicU32::new(1));
    let last = shared.picker.max_levels.max(2) - 1;
    let created = shared.vfs.now_micros();
    let all = ScanFilter::all();
    // `close` waits for this call (its maintenance guard): stop at the next check instead.
    let closing = || {
        if shared.closing.load(Ordering::Acquire) {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    };
    // Reads of the new file's temporary SSTs: their ids start again at 1, so they must not
    // share the engine's cache, which is keyed by SST id.
    let temp_cache = Arc::new(BlockCache::new(4 << 20, 1));
    let mut snapshot = Some(snapshot);
    let mut view = Some(view);
    let result = (|| -> Result<()> {
        // Phase 1: every slot's memtable entries at or below the snapshot into temporary
        // SSTs (bounded by the memtable arena), while the snapshot pins the memtables.
        let mut temps: HashMap<(pigeonhole_format::TabletId, FamilyId), Vec<SstMeta>> =
            HashMap::new();
        let full = view.as_ref().expect("held through phase 1");
        for t in source.tablets() {
            for f in source.family_ids_of(t.table) {
                closing()?;
                let (Some(meta), Some(set)) = (source.family(f), full.memtables(t.shard, t.id, f))
                else {
                    continue;
                };
                let mut sources = Vec::new();
                mem_sources(set, &all, &mut sources);
                let options = writer_options(&meta.options, t.table, f, t.id, created);
                // Inline: blob files written here would be the copy's, and the temporary
                // SSTs' extents are freed after phase 2 (#58).
                let (sink, _) = copy_at(
                    shared, &pager, &sst_ids, options, sources, seqno, None, &closing,
                )?;
                if !sink.outputs.is_empty() {
                    temps.insert((t.id, f), sink.outputs);
                }
            }
        }
        // Release the memtables: keep only an SST view of the snapshot, pinned (its manifest
        // version registered) so no SST it names is reclaimed while the merge reads it.
        let full = view.take().expect("held through phase 1");
        let ssts_only = Arc::new(View {
            version: full.version,
            manifest_version: full.manifest_version,
            tablets: Arc::clone(&full.tablets),
            catalog: Arc::clone(&full.catalog),
            mems: (0..full.mems.len())
                .map(|_| Arc::new(ShardMems::default()))
                .collect(),
            ssts: Arc::clone(&full.ssts),
            _pin: Some(ViewPin::new(&shared.live_views, full.manifest_version)),
        });
        drop(full);
        drop(snapshot.take());
        #[cfg(feature = "test-hooks")]
        shared.hooks.after_backup_releases_memtables.run();

        // Phase 2: each slot's SSTs merged with its temporary SSTs into the copy.
        for t in source.tablets() {
            for f in source.family_ids_of(t.table) {
                closing()?;
                let Some(meta) = source.family(f) else {
                    continue;
                };
                let (start, end) = clamp_to_tablet(&t, None, None)?;
                let mut sources = ssts_only.scan_sources(
                    t.shard,
                    t.id,
                    f,
                    &all,
                    start.as_deref(),
                    end.as_deref(),
                )?;
                let temp = temps.remove(&(t.id, f)).unwrap_or_default();
                // The temporary SSTs (the snapshot's memtables, so the newest entries) go
                // after the SST sources, against the usual newest-first order. The merge does
                // not depend on source order: entries carry their seqnos, and nothing here
                // resolves versions.
                for m in &temp {
                    let reader = Arc::new(SstReader::open(
                        pager.file().clone(),
                        m,
                        Arc::clone(&temp_cache),
                        Priority::Low,
                    )?);
                    sources.push(Source::Sst(
                        reader.iter(all.clone(), ReadOptions::default()).into(),
                    ));
                }
                if sources.is_empty() {
                    continue;
                }
                let options = writer_options(&meta.options, t.table, f, t.id, created);
                let separate = Separate {
                    source: &ssts_only.ssts,
                    blob_ids: &blob_ids,
                    threshold: meta.options.blob_threshold,
                    rows: (start, end),
                };
                let (mut outputs, blob_files) = copy_at(
                    shared,
                    &pager,
                    &sst_ids,
                    options,
                    sources,
                    seqno,
                    Some(separate),
                    &closing,
                )?;
                for b in blob_files {
                    edits.push(Edit::PutBlobFile {
                        blob_file: b.id,
                        family: f,
                        extents: b.extents,
                        total_bytes: b.total_bytes,
                        live_bytes: b.total_bytes,
                    });
                }
                // The temporary SSTs are copied: their extents are free for the next slots.
                for m in temp {
                    pager.abandon(m.extent);
                }
                edits.extend(outputs.take_edits(t.id, f, last));
                edits.push(Edit::SetFlushed {
                    tablet: t.id,
                    family: f,
                    seqno,
                });
            }
        }
        let c = source.counters;
        edits.push(Edit::Counters {
            next_table: c.next_table,
            next_family: c.next_family,
            next_tablet: c.next_tablet,
            next_sst: sst_ids.load(Ordering::Relaxed),
            next_blob_file: blob_ids.load(Ordering::Relaxed),
            seqno_ceiling: seqno + 1,
            ts_floor: shared
                .ts_floors
                .iter()
                .map(|f| f.0.load(Ordering::Acquire))
                .max()
                .unwrap_or(0)
                .max(c.ts_floor),
        });
        for e in &edits {
            catalog.apply(e, 1)?;
        }
        closing()?;
        let mut writer = ManifestWriter::new(Arc::clone(&pager));
        writer.commit(&catalog, &edits)?;
        writer.mark_clean()
    })();
    if result.is_err() {
        // Leave nothing half-written behind: the copy is useless without its manifest.
        drop(pager);
        let _ = shared.vfs.remove(dest);
    }
    result
}

/// SST writer options for a backup's output of `(table, family, tablet)`.
fn writer_options(
    options: &pigeonhole_format::manifest::FamilyOptions,
    table: pigeonhole_format::TableId,
    family: FamilyId,
    tablet: pigeonhole_format::TabletId,
    created: u64,
) -> SstWriterOptions {
    let mut o = SstWriterOptions::for_family(options, table, family, tablet);
    o.created_micros = created;
    o
}

/// Phase 2's handling of separated values: source pointers are read through `source` and
/// values above `threshold` written into the copy's blob files (ids from `blob_ids`); only
/// `rows` (row prefixes, `None` unbounded) are copied.
struct Separate<'a> {
    source: &'a SstSet,
    blob_ids: &'a Arc<AtomicU32>,
    threshold: u32,
    rows: (Option<Vec<u8>>, Option<Vec<u8>>),
}

/// Merges `sources` in key order and writes every entry with a seqno at or below `seqno`
/// into new SSTs of `pager`; returns the sink holding them (and their blob references) and
/// the blob files written. Without `separate` (phase 1), every value is written as it is:
/// a memtable pointer (a value separated at commit time, #230) still names the source's
/// blob file, and phase 2's `separate` reads the value from there.
#[allow(clippy::too_many_arguments)]
fn copy_at(
    shared: &Shared,
    pager: &Arc<Pager>,
    sst_ids: &Arc<AtomicU64>,
    options: SstWriterOptions,
    sources: Vec<Source>,
    seqno: pigeonhole_format::Seqno,
    separate: Option<Separate<'_>>,
    closing: &dyn Fn() -> Result<()>,
) -> Result<(SstSink, Vec<NewBlobFile>)> {
    let mut sink = SstSink::new(
        Arc::clone(pager),
        Arc::clone(sst_ids),
        options,
        shared.picker.target_sst_bytes,
    );
    if sources.is_empty() {
        return Ok((sink, Vec::new()));
    }
    let mut merged = MergingCursor::new(sources);
    let (start, end) = separate
        .as_ref()
        .map_or((None, None), |s| (s.rows.0.as_deref(), s.rows.1.as_deref()));
    match start {
        Some(s) => merged.seek(s)?,
        None => merged.seek_to_first()?,
    }
    if let Some(s) = &separate {
        sink = sink.separating(
            Arc::clone(s.blob_ids),
            s.threshold,
            shared.picker.target_sst_bytes,
        );
    }
    let mut n = 0u64;
    while merged.valid() && end.is_none_or(|e| merged.key() < e) {
        n += 1;
        if n.is_multiple_of(CLOSE_CHECK_EVERY) {
            closing()?;
        }
        let (_, _, s, _) = split_suffix(merged.key())?;
        if s <= seqno {
            match separate
                .as_ref()
                .map(|s| s.source.read_blob(merged.value()))
            {
                Some(Ok(Some(v))) => sink.add(merged.key(), &v)?,
                Some(Err(e)) => return Err(e),
                _ => sink.add(merged.key(), merged.value())?,
            }
        }
        merged.next()?;
    }
    sink.cut()?;
    sink.finish_blobs()?;
    let blob_files = std::mem::take(&mut sink.blob_files);
    Ok((sink, blob_files))
}

/// A region `shrink` cleared for `big` (`Pager::clear_for`), held until `big` moves in;
/// released when dropped (a no-op once `Pager::relocate_into` took it).
struct Held {
    pager: Arc<Pager>,
    big: ExtentRef,
    region: ExtentRef,
}

impl Drop for Held {
    fn drop(&mut self) {
        self.pager.release_region(self.region);
    }
}

/// Where `shrink` moves an extent.
#[derive(Clone, Copy)]
enum Target {
    /// The lowest free extent of its class below this one (itself, or the extent a region
    /// is cleared for).
    Below(ExtentRef),
    /// A held region (`Held`).
    Into(ExtentRef),
}

/// What became of an extent `shrink` tried to move.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Moved {
    Yes,
    /// No room: no free extent of its class below, or the held region still has an
    /// occupant's old extent in it.
    NoSpace,
    /// Not moved, and room has nothing to do with it: claimed by a compaction, not named by
    /// the manifest (output in flight), or retired since the catalog was read.
    Skipped,
}

/// Relocates manifest-named extents past the shrink point and truncates the file. Returns
/// the bytes the file shrank by, net: a round's manifest commit can grow the file by what
/// a later truncation gives back.
///
/// Every round first gives back the free space already at the tail, so a file whose tail
/// holds nothing live (everything there was deleted and compacted away) shrinks without
/// moving anything. An extent with no free extent of its class below it is skipped, not an
/// error (the file then ends after it), and the extents past it still move.
pub(crate) fn shrink(shared: &Shared) -> Result<u64> {
    let start = shared.pager.stats().file_bytes;
    let shrunk = || start.saturating_sub(shared.pager.stats().file_bytes);
    // A region cleared for a large extent (#314): held across rounds, so the space the
    // occupants leave goes to the large extent and not to a flush meanwhile.
    let mut held: Option<Held> = None;
    // A cleared region went unused (the large extent left the plan, or a pinned version
    // kept the occupants' old extents): clear no other this call.
    let mut wasted = false;
    for _round in 0..8 {
        if shared.closing.load(Ordering::Acquire) {
            // The close waits for this call: stop between rounds.
            return Ok(shrunk());
        }
        // Retired extents no view uses any more are free space: at the tail they are cut
        // off now, below it the relocations can move into them (without this the targets
        // would be allocated past the end of the file).
        shared.reclaim();
        shared.pager.truncate_tail()?;
        let plan = shared.pager.shrink_plan();
        if held.as_ref().is_some_and(|h| !plan.contains(&h.big)) {
            // Moved or replaced otherwise: the region is not needed.
            held = None;
            wasted = true;
        }
        if plan.is_empty() {
            break;
        }
        let view = shared.view.load_full();
        let catalog = Arc::clone(&view.catalog);
        let root = shared
            .manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .root();
        // Every SST by extent, with a family that references it (for its cache priority).
        // Where it is referenced is decided again at commit time (`Moves::edits`).
        let mut by_extent: HashMap<(u64, u8), (FamilyId, Arc<SstMeta>)> = HashMap::new();
        for ((_, family), list) in &catalog.ssts {
            for (_, meta) in list {
                by_extent
                    .entry((meta.extent.page, meta.extent.size_class))
                    .or_insert_with(|| (*family, Arc::clone(meta)));
            }
        }
        #[cfg(feature = "test-hooks")]
        shared.hooks.before_shrink_relocates.run();
        // Every blob extent by extent: its file and position in the file's extent list.
        let mut blob_by_extent: HashMap<(u64, u8), (BlobFileId, usize)> = HashMap::new();
        for (id, b) in &catalog.blob_files {
            for (i, e) in b.extents.iter().enumerate() {
                blob_by_extent.insert((e.page, e.size_class), (*id, i));
            }
        }
        let mut moves = Moves {
            pager: Arc::clone(&shared.pager),
            list: Vec::new(),
            blobs: Vec::new(),
        };
        let mut readers = Vec::new();
        let mut rewrite = false;
        let mut claimed: Vec<SstId> = Vec::new();
        // An extent retired since the catalog was read: the plan is stale, so a round with
        // nothing to move plans again instead of stopping.
        let mut stale = false;
        let roots: Vec<ExtentRef> = [root.snapshot, root.log].into_iter().flatten().collect();
        let relocate = |extent: ExtentRef, to: Target| match to {
            Target::Below(limit) => shared.pager.relocate_below(extent, limit),
            Target::Into(region) => shared.pager.relocate_into(extent, region),
        };
        let mut move_to = |extent: ExtentRef, to: Target| -> Result<Moved> {
            if roots.contains(&extent) {
                // The manifest's own extents move by a snapshot rewrite.
                rewrite = true;
                return Ok(Moved::Yes);
            }
            if let Some(&(blob_file, index)) = blob_by_extent.get(&(extent.page, extent.size_class))
            {
                // A blob extent (#231): blob files are never written once published, so a
                // copy needs no claim; the commit replaces the extent at its position if the
                // file still has it there.
                return match relocate(extent, to) {
                    Ok(target) => {
                        moves.blobs.push((blob_file, index, extent, target));
                        Ok(Moved::Yes)
                    }
                    Err(pigeonhole_pager::Error::NoSpace) => Ok(Moved::NoSpace),
                    // Dropped (blob GC, `drop_table`) since the catalog was read.
                    Err(_) if !shared.pager.is_live(extent) => {
                        stale = true;
                        Ok(Moved::Skipped)
                    }
                    Err(e) => Err(e.into()),
                };
            }
            let Some((family, meta)) = by_extent.get(&(extent.page, extent.size_class)) else {
                // Not named by the manifest: an output in flight (decision D60).
                return Ok(Moved::Skipped);
            };
            {
                let mut busy = shared
                    .busy_ssts
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if busy.contains(&meta.id) {
                    return Ok(Moved::Skipped);
                }
                busy.insert(meta.id);
            }
            claimed.push(meta.id);
            let target = match relocate(extent, to) {
                Ok(t) => t,
                // No room: it stays, and the smaller ones past it may still move.
                Err(pigeonhole_pager::Error::NoSpace) => return Ok(Moved::NoSpace),
                // A compaction that committed after the catalog was read retired it (the
                // view held here keeps it from being reclaimed, so it is not reused).
                Err(_) if !shared.pager.is_live(extent) => {
                    stale = true;
                    return Ok(Moved::Skipped);
                }
                Err(e) => return Err(e.into()),
            };
            let id = SstId(shared.sst_ids.fetch_add(1, Ordering::Relaxed));
            let mut new_meta = (**meta).clone();
            new_meta.id = id;
            new_meta.extent = target;
            moves.list.push((meta.id, new_meta.clone()));
            let priority = catalog
                .family(*family)
                .map_or(pigeonhole_cache::Priority::Normal, |m| {
                    SstSet::priority(m.options.cache_priority)
                });
            let r = SstReader::open(
                shared.pager.file().clone(),
                &new_meta,
                Arc::clone(&shared.cache),
                priority,
            )?;
            readers.push((id, Arc::new(r)));
            Ok(Moved::Yes)
        };
        // Whether an extent can be moved out of a region cleared for a larger one: the
        // manifest's, a blob extent, or an SST no compaction held when the round began (a
        // copy: `Pager::clear_for` calls this under the allocator's lock; a claim taken
        // since makes that move a no-op).
        let busy: std::collections::HashSet<SstId> = shared
            .busy_ssts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let movable = |e: ExtentRef| {
            roots.contains(&e)
                || blob_by_extent.contains_key(&(e.page, e.size_class))
                || by_extent
                    .get(&(e.page, e.size_class))
                    .is_some_and(|(_, m)| !busy.contains(&m.id))
        };
        let mut failed = None;
        for extent in plan {
            let to = match &held {
                Some(h) if h.big == extent => Target::Into(h.region),
                _ => Target::Below(extent),
            };
            match move_to(extent, to) {
                Ok(Moved::Yes) => {
                    if matches!(to, Target::Into(_)) {
                        // The region is the large extent's now (releasing it is a no-op).
                        held = None;
                    }
                }
                Ok(Moved::NoSpace) if matches!(to, Target::Into(_)) => {
                    // An occupant's old extent is still in the region: a pinned version
                    // keeps it from being reclaimed. Wait (held) without clearing more.
                    wasted = true;
                }
                Ok(Moved::NoSpace) if held.is_none() && !wasted => {
                    // Small extents fragment every aligned hole of its class below it. Clear
                    // one region (for the largest such extent that has one) and hold it; the
                    // extent moves in once the occupants' old extents are reclaimed.
                    let Some(clearing) = shared.pager.clear_for(extent, movable) else {
                        continue;
                    };
                    crate::shard::trace!(
                        "shrink: clearing {:?} of {:?} for {extent:?}",
                        clearing.region,
                        clearing.occupants
                    );
                    held = Some(Held {
                        pager: Arc::clone(&shared.pager),
                        big: extent,
                        region: clearing.region,
                    });
                    for occupant in clearing.occupants {
                        if let Err(e) = move_to(occupant, Target::Below(extent)) {
                            failed = Some(e);
                            break;
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => failed = Some(e),
            }
            if failed.is_some() {
                break;
            }
        }
        if let Some(e) = failed {
            unclaim(shared, &claimed);
            return Err(e);
        }
        if moves.list.is_empty() && moves.blobs.is_empty() && !rewrite {
            unclaim(shared, &claimed);
            if stale {
                continue;
            }
            break;
        }
        #[cfg(feature = "test-hooks")]
        shared.hooks.before_shrink_commits.run();
        // The edits are computed against the catalog at commit time, not the one read
        // above: a compaction or `drop_table` that committed meanwhile removed an SST (its
        // copy is abandoned), and a trivial move changed its level (the copy goes to the
        // current one).
        let (req, waiter) =
            ManifestReq::with_waiter(ReqKind::Catalog(Box::new(move |catalog: &mut Catalog| {
                Ok(moves.edits(catalog))
            })));
        let req = ManifestReq {
            readers,
            rewrite_snapshot: rewrite,
            ..req
        };
        let committed = manifest::commit_req_from_thread(shared, req, waiter);
        unclaim(shared, &claimed);
        drop(view);
        drop(catalog);
        // On an error the targets are the manifest's to abandon (`begin` does, for a
        // refused request, and `Moves` does for a request dropped unrun) or already named
        // (a commit whose view publish failed).
        committed?;
    }
    // A region still held is free space again before the tail is cut.
    drop(held);
    if !shared.closing.load(Ordering::Acquire) {
        // What the last round's moves retired.
        shared.reclaim();
        shared.pager.truncate_tail()?;
    }
    Ok(shrunk())
}

/// Shrink's relocated copies, `(old SST, its copy)` and `(blob file, extent index, old
/// extent, its copy)`, until the commit turns them into edits. Copies it never turns into
/// edits (the request was dropped unrun) are abandoned.
struct Moves {
    pager: Arc<Pager>,
    list: Vec<(SstId, SstMeta)>,
    blobs: Vec<(BlobFileId, usize, ExtentRef, ExtentRef)>,
}

impl Moves {
    /// Replaces each old SST by its copy wherever the current `catalog` references it, at
    /// the level it is at now. A copy of an SST the catalog no longer references is
    /// abandoned.
    fn edits(&mut self, catalog: &Catalog) -> Vec<Edit> {
        let mut edits = Vec::new();
        for (old, copy) in std::mem::take(&mut self.list) {
            let refs: Vec<_> = catalog
                .ssts
                .iter()
                .flat_map(|(key, list)| {
                    list.iter()
                        .filter(|(_, m)| m.id == old)
                        .map(move |(level, _)| (*key, *level))
                })
                .collect();
            if refs.is_empty() {
                self.pager.abandon(copy.extent);
                continue;
            }
            for ((tablet, family), level) in refs {
                edits.push(Edit::RemoveSst {
                    tablet,
                    family,
                    sst: old,
                });
                edits.push(Edit::AddSst {
                    tablet,
                    family,
                    level,
                    meta: copy.clone(),
                });
            }
            // The copy holds the same entries, so the same blob references.
            if let Some(refs) = catalog.blob_refs.get(&old) {
                edits.push(Edit::SstBlobRefs {
                    sst: copy.id,
                    refs: refs.clone(),
                });
            }
        }
        // Blob extents: each copy replaces its extent where the file still has it (a blob
        // GC or `drop_table` may have dropped the file meanwhile).
        let mut changed: Vec<(BlobFileId, Vec<ExtentRef>)> = Vec::new();
        for (blob_file, index, old, copy) in std::mem::take(&mut self.blobs) {
            let current = changed
                .iter_mut()
                .find(|(id, _)| *id == blob_file)
                .map(|(_, e)| e.clone())
                .or_else(|| {
                    catalog
                        .blob_files
                        .get(&blob_file)
                        .map(|b| b.extents.clone())
                });
            match current {
                Some(mut extents) if extents.get(index) == Some(&old) => {
                    extents[index] = copy;
                    match changed.iter_mut().find(|(id, _)| *id == blob_file) {
                        Some((_, e)) => *e = extents,
                        None => changed.push((blob_file, extents)),
                    }
                }
                _ => self.pager.abandon(copy),
            }
        }
        for (blob_file, extents) in changed {
            let b = &catalog.blob_files[&blob_file];
            edits.push(Edit::PutBlobFile {
                blob_file,
                family: b.family,
                extents,
                total_bytes: b.total_bytes,
                live_bytes: b.live_bytes,
            });
        }
        edits
    }
}

impl Drop for Moves {
    fn drop(&mut self) {
        for (_, copy) in self.list.drain(..) {
            self.pager.abandon(copy.extent);
        }
        for (_, _, _, copy) in self.blobs.drain(..) {
            self.pager.abandon(copy);
        }
    }
}

fn unclaim(shared: &Shared, ids: &[SstId]) {
    let mut busy = shared
        .busy_ssts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for id in ids {
        busy.remove(id);
    }
}

//! Online backup and shrink.
//!
//! **Backup** writes a brand-new database at a snapshot, holding exactly what a read at the
//! snapshot sees, while writers keep running; the result is one clean file.
//!
//! It runs in two phases so the snapshot's memtables (arena chunks writers need) are held
//! only while they are copied (#262): first every memtable's entries at or below the
//! snapshot are written into level-0 SSTs of the new file (a flush, in effect), then the
//! snapshot is released except for its SST set, and the source's SSTs and blob files are
//! copied byte for byte (#715). An SST's block addresses are relative to its own start and
//! it records no id or file position, and a blob extent's header names its blob file and
//! its place in it, so a copy is valid at any extent of its size with the same ids: only
//! the manifest is new. Shared SSTs (a split's children) are copied once and listed under
//! each tablet, as in the source. An SST holding commits above the snapshot (flushed but not
//! yet durable, under a durable snapshot) is rewritten without them instead, at its level.
//! Memtable values separated at commit time (#230) keep pointing at their blob files, which
//! the copy holds under the same ids.
//!
//! The copy keeps the source's level shape, with every version and tombstone at or below
//! the snapshot, as a file-level backup would. Its extents are allocated largest first, so
//! the smaller ones fill the alignment gaps the larger ones leave, it never grows ahead of
//! need, and its free tail is truncated: the file is about the size of the source's live
//! extents.
//!
//! **Shrink** truncates the free tail, relocates the extents past the pager's shrink point
//! that the manifest names (decision D60: never an in-flight flush or compaction output,
//! which the manifest does not name yet, nor an SST a running compaction reads), publishes
//! the moves against the catalog current at commit time, and truncates the file again once
//! everything retired has been reclaimed.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_compaction::MergingCursor;
use pigeonhole_format::key::split_suffix;
use pigeonhole_format::manifest::{Edit, SstMeta};
use pigeonhole_format::scan::ScanFilter;
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{BlobFileId, Cursor, FamilyId, SstId, TabletId};
use pigeonhole_pager::Pager;
use pigeonhole_sst::{ReadOptions, SstReader, SstWriterOptions};

use crate::catalog::Catalog;
use crate::flush::SstSink;
use crate::manifest::{self, ManifestReq, ManifestWriter, ReqKind, Rewrite};
use crate::shard::Shared;
use crate::snapshot::{ShardMems, Snapshot, SstSet, View, ViewPin};
use crate::source::{Source, mem_sources};
use crate::{Error, Result};

/// Entries `backup` copies between checks for a close.
const CLOSE_CHECK_EVERY: u64 = 4096;

/// Writes a consistent copy of `snapshot` to `dest` (which must not exist), in the writer
/// process or a reader process (ICR 0049; a reader holds its snapshot's pin throughout, see
/// below, and stops with `SnapshotExpired` if the writer restarts). Stops with
/// `Closed` (removing the partial copy) once the engine is closing. The snapshot's memtables
/// are released after the first phase; see the module docs.
pub(crate) fn backup(shared: &Shared, snapshot: Snapshot, dest: &Path) -> Result<()> {
    let seqno = snapshot.seqno;
    let view = Arc::clone(&snapshot.view);
    let source = Arc::clone(&view.catalog);
    let pager = Arc::new(Pager::create(&shared.vfs, dest)?);
    // Written once, to its final size: a block grown ahead of need would only be free space
    // in the copy (#715).
    pager.set_grow_ahead(
        ExtentRef {
            page: 0,
            size_class: ExtentRef::MAX_CLASS,
        }
        .len(),
        u64::MAX,
    )?;
    // New SSTs (the memtables', and any rewritten) take ids above every source SST's.
    let sst_ids = Arc::new(AtomicU64::new(source.counters.next_sst));
    let created = shared.vfs.now_micros();
    let all = ScanFilter::all();
    // A reader process (ICR 0049) pins what it reads through its shm slot, which pins a
    // whole view, memtables included (FORMAT §11.5): it can't release the memtables after
    // phase 1 and keep only the SSTs pinned, as the writer does below. So it keeps its pin
    // (a clone of the snapshot's live count) for the whole copy, and the writer frees nothing
    // of that view meanwhile. A writer restart expires the snapshot: stop then.
    let reader_pin = snapshot._live.clone();
    // `close` waits for this call (its maintenance guard): stop at the next check instead.
    let closing = || {
        if shared.closing.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        reader_pin.as_ref().map_or(Ok(()), |l| l.check_current())
    };
    // Reads of SSTs the copy rewrites: kept off the engine's cache, which they would only
    // churn.
    let rewrite_cache = Arc::new(BlockCache::new(4 << 20, 1));
    let mut snapshot = Some(snapshot);
    let mut view = Some(view);
    let result = (|| -> Result<()> {
        // What phase 2 copies byte for byte, and what it rewrites, from the snapshot's catalog.
        let mut edits = source.snapshot_edits();
        let mut verbatim: HashMap<SstId, ExtentRef> = HashMap::new();
        let mut verbatim_family: HashMap<SstId, FamilyId> = HashMap::new();
        let mut verbatim_len: HashMap<ExtentRef, u64> = HashMap::new();
        // By id, so a seed lays the copy out the same way every run.
        let mut rewrite: BTreeMap<SstId, (Arc<SstMeta>, Vec<Slot>)> = BTreeMap::new();
        for e in &edits {
            if let Edit::AddSst {
                tablet,
                family,
                level,
                meta,
            } = e
            {
                if meta.seqno_range.1 <= seqno {
                    verbatim.insert(meta.id, meta.extent);
                    verbatim_family.insert(meta.id, *family);
                    verbatim_len.insert(meta.extent, meta.len);
                } else {
                    rewrite
                        .entry(meta.id)
                        .or_insert_with(|| (Arc::new(meta.clone()), Vec::new()))
                        .1
                        .push((*tablet, *family, *level));
                }
            }
        }
        // Their extents in the copy, reserved now, largest first, before phase 1's SSTs take
        // any space: an extent is aligned to its size, and the smaller ones (the memtables'
        // SSTs among them) then fill the gaps the larger ones leave. The blob files the kept
        // SSTs reference are reserved too; those only new SSTs reference follow in phase 2.
        let mut recorded: HashMap<SstId, &Vec<(BlobFileId, u64)>> = HashMap::new();
        for e in &edits {
            if let Edit::SstBlobRefs { sst, refs } = e
                && verbatim.contains_key(sst)
            {
                recorded.insert(*sst, refs);
            }
        }
        let any_of: HashSet<FamilyId> = verbatim_family
            .iter()
            .filter(|(id, _)| !recorded.contains_key(id))
            .map(|(_, f)| *f)
            .collect();
        let kept_refs: HashSet<BlobFileId> = recorded
            .values()
            .flat_map(|refs| refs.iter().map(|(b, _)| *b))
            .collect();
        let mut moved: HashMap<ExtentRef, ExtentRef> = HashMap::new();
        reserve(
            &pager,
            &mut moved,
            extents_to_copy(&edits, &verbatim, |b, f| {
                kept_refs.contains(b) || any_of.contains(f)
            }),
        )?;
        drop(recorded);
        // Phase 1: every slot's memtable entries at or below the snapshot into level-0 SSTs
        // of the copy (a flush, in effect), while the snapshot pins the memtables.
        let mut mem_edits = Vec::new();
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
                let mut sink =
                    copy_at(shared, &pager, &sst_ids, options, sources, seqno, &closing)?;
                mem_edits.extend(sink.take_edits(t.id, f, 0));
            }
        }
        // Release the memtables: keep only an SST view of the snapshot, pinned (its manifest
        // version registered) so no extent it names is reclaimed while phase 2 reads it.
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
            // The writer's protection for the extents below. In a reader process this
            // registers in its own `live_views`, which nothing there reclaims against:
            // harmless, and not the reader's protection (that is `reader_pin` above).
            _pin: Some(ViewPin::new(&shared.live_views, full.manifest_version)),
            released: false,
        });
        drop(full);
        drop(snapshot.take());
        #[cfg(feature = "test-hooks")]
        shared
            .hooks
            .sites
            .run(crate::engine::hooks::HookSite::BackupReleasedMemtables);

        // Phase 2: the source catalog's SSTs and blob files, copied byte for byte (#715): an
        // SST's block addresses are relative to its start and it holds no id or file offset,
        // and a blob extent names its own file and position, so only their extents change.
        // An SST holding commits above the snapshot (flushed but not yet durable, under a
        // durable snapshot) is rewritten without them instead.
        let src = &ssts_only.ssts.file;
        // SSTs holding commits above the snapshot, rewritten first (their blob references
        // count below): each once however many tablets share it, unclamped, its values
        // written as they are.
        let mut rewritten = Vec::new();
        for (meta, slots) in rewrite.into_values() {
            closing()?;
            let (tablet, family, _) = slots[0];
            let (Some(info), Some(t)) = (source.family(family), source.tablet(tablet)) else {
                return Err(Error::Corruption(format!(
                    "backup: SST {:?} is under an unknown tablet or family",
                    meta.id
                )));
            };
            let table = t.table;
            let reader = Arc::new(SstReader::open(
                src.clone(),
                &meta,
                Arc::clone(&rewrite_cache),
                Priority::Low,
            )?);
            let sources = vec![Source::Sst(
                reader.iter(all.clone(), ReadOptions::default()).into(),
            )];
            let options = writer_options(&info.options, table, family, tablet, created);
            let mut sink = copy_at(shared, &pager, &sst_ids, options, sources, seqno, &closing)?;
            for (out, refs) in sink.outputs.drain(..).zip(sink.refs.drain(..)) {
                for &(tablet, family, level) in &slots {
                    rewritten.push(Edit::AddSst {
                        tablet,
                        family,
                        level,
                        meta: out.clone(),
                    });
                }
                rewritten.push(Edit::SstBlobRefs { sst: out.id, refs });
            }
        }
        // The blob files the copy references and the bytes it references in each (their
        // `live_bytes`): the kept SSTs' recorded references and the new SSTs'. A kept SST
        // with none recorded (an older build's) may point anywhere in its family, whose files
        // are then all kept at their source live bytes. Files nothing references (values of
        // commits above the snapshot, a failed commit's) stay behind.
        let mut live: HashMap<BlobFileId, u64> = HashMap::new();
        let new_refs = mem_edits.iter().chain(&rewritten);
        for e in edits
            .iter()
            .filter(|e| matches!(e, Edit::SstBlobRefs { sst, .. } if verbatim.contains_key(sst)))
            .chain(new_refs)
        {
            if let Edit::SstBlobRefs { refs, .. } = e {
                for (blob, bytes) in refs {
                    *live.entry(*blob).or_default() += bytes;
                }
            }
        }
        let listed: HashSet<BlobFileId> = edits
            .iter()
            .filter_map(|e| match e {
                Edit::PutBlobFile { blob_file, .. } => Some(*blob_file),
                _ => None,
            })
            .collect();
        if let Some(missing) = live.keys().find(|b| !listed.contains(b)) {
            return Err(Error::Corruption(format!(
                "backup: an SST points into blob file {missing:?}, which the snapshot does not list"
            )));
        }
        let kept = |blob: &BlobFileId, family: &FamilyId| {
            live.contains_key(blob) || any_of.contains(family)
        };
        // Blob files only the new SSTs reference get their extents now; then every reserved
        // extent is filled from the source.
        reserve(&pager, &mut moved, extents_to_copy(&edits, &verbatim, kept))?;
        // In the copy's order, so its writes are sequential.
        let mut order: Vec<(&ExtentRef, &ExtentRef)> = moved.iter().collect();
        order.sort_by_key(|(_, to)| **to);
        let mut buf = vec![0u8; COPY_CHUNK];
        for (from, to) in order {
            closing()?;
            let len = verbatim_len.get(from).copied().unwrap_or(from.len());
            let mut at = 0;
            while at < len {
                let n = (len - at).min(COPY_CHUNK as u64) as usize;
                src.read_at(&mut buf[..n], from.offset() + at)?;
                pager.write(*to, at, &buf[..n])?;
                at += n as u64;
            }
        }
        // The source's edits, pointed at the copies: the WAL checkpoints go (the copy has no
        // log), every slot is flushed through the snapshot, and the counters come last.
        edits.retain_mut(|e| match e {
            Edit::AddSst { meta, .. } => match moved.get(&meta.extent) {
                Some(to) if verbatim.contains_key(&meta.id) => {
                    meta.extent = *to;
                    true
                }
                _ => false,
            },
            Edit::SstBlobRefs { sst, .. } => verbatim.contains_key(sst),
            Edit::PutBlobFile {
                blob_file,
                family,
                extents,
                live_bytes,
                ..
            } => {
                if !kept(blob_file, family) {
                    return false;
                }
                for x in extents.iter_mut() {
                    *x = moved[x];
                }
                if !any_of.contains(family) {
                    *live_bytes = live[blob_file];
                }
                true
            }
            Edit::SetFlushed { .. } | Edit::WalCheckpoint { .. } | Edit::Counters { .. } => false,
            _ => true,
        });
        edits.extend(rewritten);
        edits.extend(mem_edits);
        for t in source.tablets() {
            for f in source.family_ids_of(t.table) {
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
            next_blob_file: c.next_blob_file,
            seqno_ceiling: seqno + 1,
            // D271: every default timestamp handed out is at or below the allocator's `H`.
            ts_floor: shared.clock.last_ts().max(c.ts_floor),
        });
        let mut catalog = Catalog::with_registry(Arc::clone(source.registry()));
        for e in &edits {
            catalog.apply(e, 1)?;
        }
        closing()?;
        let mut writer = ManifestWriter::new(Arc::clone(&pager));
        writer.commit(&catalog, &edits)?;
        // Whatever is free past the last extent (an allocation's alignment growth the
        // smaller extents did not fill); the clean root below records the length.
        pager.truncate_tail()?;
        writer.mark_clean()
    })();
    // A reader whose writer restarted mid-copy may have read bytes the new writer reused, and
    // failed on them: the cause is the expired snapshot, not damage.
    let result = match (result, reader_pin.as_ref().map(|l| l.check_current())) {
        (Err(_), Some(Err(expired))) => Err(expired),
        (r, _) => r,
    };
    if result.is_err() {
        // Leave nothing half-written behind: the copy is useless without its manifest.
        drop(pager);
        let _ = shared.vfs.remove(dest);
    }
    result
}

/// Where the source lists an SST: its tablet, family and level.
type Slot = (TabletId, FamilyId, u8);

/// The source extents a backup copies (`edits` the snapshot catalog's): the SSTs in
/// `verbatim`, with the bytes each uses, and every extent of the blob files `kept` keeps.
fn extents_to_copy(
    edits: &[Edit],
    verbatim: &HashMap<SstId, ExtentRef>,
    kept: impl Fn(&BlobFileId, &FamilyId) -> bool,
) -> Vec<(ExtentRef, u64)> {
    let mut out = Vec::new();
    for e in edits {
        match e {
            Edit::AddSst { meta, .. } if verbatim.contains_key(&meta.id) => {
                out.push((meta.extent, meta.len));
            }
            Edit::PutBlobFile {
                blob_file,
                family,
                extents,
                ..
            } if kept(blob_file, family) => out.extend(extents.iter().map(|x| (*x, x.len()))),
            _ => {}
        }
    }
    out
}

/// Allocates the copy's extent for each of `extents` not in `moved` yet, largest first (the
/// smaller ones then fill the alignment gaps the larger ones leave), and records it there.
fn reserve(
    pager: &Pager,
    moved: &mut HashMap<ExtentRef, ExtentRef>,
    mut extents: Vec<(ExtentRef, u64)>,
) -> Result<()> {
    extents.retain(|(x, _)| !moved.contains_key(x));
    extents.sort_by_key(|(x, len)| (std::cmp::Reverse(*len), *x));
    extents.dedup_by_key(|(x, _)| *x);
    for (from, len) in extents {
        moved.insert(from, pager.allocate(len)?);
    }
    Ok(())
}

/// Bytes `backup` copies per read and write.
const COPY_CHUNK: usize = 1 << 20;

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

/// Merges `sources` in key order and writes every entry with a seqno at or below `seqno`
/// into new SSTs of `pager`; returns the sink holding them and their blob references. Values
/// are written as they are: a blob pointer keeps naming its blob file, which the copy holds
/// under the same id.
fn copy_at(
    shared: &Shared,
    pager: &Arc<Pager>,
    sst_ids: &Arc<AtomicU64>,
    options: SstWriterOptions,
    sources: Vec<Source>,
    seqno: pigeonhole_format::Seqno,
    closing: &dyn Fn() -> Result<()>,
) -> Result<SstSink> {
    let mut sink = SstSink::new(
        Arc::clone(pager),
        Arc::clone(sst_ids),
        options,
        shared.picker.target_sst_bytes,
    );
    if sources.is_empty() {
        return Ok(sink);
    }
    let mut merged = MergingCursor::new(sources);
    merged.seek_to_first()?;
    let mut n = 0u64;
    while merged.valid() {
        n += 1;
        if n.is_multiple_of(CLOSE_CHECK_EVERY) {
            closing()?;
        }
        let (_, _, s, _) = split_suffix(merged.key())?;
        if s <= seqno {
            sink.add(merged.key(), merged.value())?;
        }
        merged.next()?;
    }
    sink.cut()?;
    Ok(sink)
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
    // A rewrite that could not grow the file found no free space below the manifest's
    // extents: ask for none again this call.
    let mut manifest_stays = false;
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
        // What this round retires stays allocated until the call ends if a view keeps
        // retired extents from being reclaimed now, or a snapshot (or a backup), or a
        // reader process's slot, will once this round retires more. Its commit must not grow the file then: the space would
        // stay past the end, and the next round would plan to move the manifest out of it
        // again (#1098).
        let pinned = shared.pager.stats().retired_bytes > 0
            || !shared.live_seqnos.list().is_empty()
            || shared.shm.oldest_reader_pin().is_some();
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
        for ((_, family), list) in catalog.ssts.iter() {
            for (_, meta) in list.iter() {
                by_extent
                    .entry((meta.extent.page, meta.extent.size_class))
                    .or_insert_with(|| (*family, Arc::clone(meta)));
            }
        }
        #[cfg(feature = "test-hooks")]
        shared
            .hooks
            .sites
            .run(crate::engine::hooks::HookSite::ShrinkRelocates);
        // Every blob extent by extent: its file and position in the file's extent list.
        let mut blob_by_extent: HashMap<(u64, u8), (BlobFileId, usize)> = HashMap::new();
        for (id, b) in catalog.blob_files.iter() {
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
        // Rewrite the manifest snapshot, to below this extent.
        let mut rewrite: Option<ExtentRef> = None;
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
                // The manifest's own extents move by a snapshot rewrite: below the limit
                // while pinned, else into the lowest free space. Never into a held region:
                // a root never runs out of room, so none is cleared for one.
                let (Target::Below(limit), false) = (to, manifest_stays) else {
                    return Ok(Moved::Skipped);
                };
                rewrite = Some(match rewrite {
                    Some(r) if r.page > limit.page => r,
                    _ => limit,
                });
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
                shared.pager.data_file().clone(),
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
        if moves.list.is_empty() && moves.blobs.is_empty() && rewrite.is_none() {
            unclaim(shared, &claimed);
            if stale {
                continue;
            }
            break;
        }
        #[cfg(feature = "test-hooks")]
        shared
            .hooks
            .sites
            .run(crate::engine::hooks::HookSite::ShrinkCommits);
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
            rewrite_snapshot: match (rewrite, pinned) {
                (limit, true) => Rewrite::NoGrowth(limit),
                (Some(_), false) => Rewrite::Lowest,
                (None, false) => Rewrite::AsNeeded,
            },
            ..req
        };
        let committed = manifest::commit_req_from_thread(shared, req, waiter);
        if pinned && rewrite.is_some() {
            let now = shared
                .manifest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .root();
            // Still where they were: the rewrite found no room below them.
            manifest_stays |= [now.snapshot, now.log]
                .into_iter()
                .flatten()
                .any(|e| roots.contains(&e));
        }
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
                        .map(|b| b.extents.to_vec())
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

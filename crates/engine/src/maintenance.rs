//! Online backup and shrink.
//!
//! **Backup** writes a brand-new database at a snapshot: every `(tablet, family)`'s sources
//! (memtables and SSTs) are merged in key order and every entry with a seqno at or below the
//! snapshot's is written into SSTs at the last level of the new file, whose manifest records
//! them as flushed through that seqno. The copy is exactly what a read at the snapshot sees,
//! writers keep running meanwhile, and the result is one clean file.
//!
//! It runs in two phases so the snapshot's memtables (arena chunks writers need) are held
//! only briefly (#262): first every memtable's entries are copied into temporary SSTs in the
//! new file (bounded by the arena, so quick), then the snapshot is released except for its
//! SST set and the long merge reads the temporary SSTs and the source SSTs.
//!
//! **Shrink** truncates the free tail, relocates the extents past the pager's shrink point
//! that the manifest names (decision D60: never an in-flight flush or compaction output,
//! which the manifest does not name yet, nor an SST a running compaction reads), publishes
//! the moves against the catalog current at commit time, and truncates the file again once
//! everything retired has been reclaimed.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_compaction::MergingCursor;
use pigeonhole_format::key::split_suffix;
use pigeonhole_format::manifest::{Edit, SstMeta};
use pigeonhole_format::scan::ScanFilter;
use pigeonhole_format::{Cursor, FamilyId, SstId};
use pigeonhole_pager::Pager;
use pigeonhole_sst::{ReadOptions, SstReader, SstWriterOptions};

use crate::catalog::Catalog;
use crate::flush::SstSink;
use crate::manifest::{self, ManifestReq, ManifestWriter, ReqKind};
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
    if !source.blob_files.is_empty() {
        // The copy would hold dangling blob pointers (issue #58).
        return Err(crate::Error::Unsupported(
            "backup of a database with blob extents is not available yet",
        ));
    }
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
                let outputs = copy_at(shared, &pager, &sst_ids, options, sources, seqno, &closing)?;
                if !outputs.is_empty() {
                    temps.insert((t.id, f), outputs);
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
                let mut sources = ssts_only.scan_sources(t.shard, t.id, f, &all, None, None)?;
                let temp = temps.remove(&(t.id, f)).unwrap_or_default();
                for m in &temp {
                    let reader = Arc::new(SstReader::open(
                        pager.file().clone(),
                        m,
                        Arc::clone(&temp_cache),
                        Priority::Low,
                    )?);
                    sources.push(Source::Sst(
                        reader.iter(all.clone(), ReadOptions::default()),
                    ));
                }
                if sources.is_empty() {
                    continue;
                }
                let options = writer_options(&meta.options, t.table, f, t.id, created);
                let outputs = copy_at(shared, &pager, &sst_ids, options, sources, seqno, &closing)?;
                // The temporary SSTs are copied: their extents are free for the next slots.
                for m in temp {
                    pager.abandon(m.extent);
                }
                for meta in outputs {
                    edits.push(Edit::AddSst {
                        tablet: t.id,
                        family: f,
                        level: last,
                        meta,
                    });
                }
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

/// Merges `sources` in key order and writes every entry with a seqno at or below `seqno`
/// into new SSTs of `pager`; returns them.
fn copy_at(
    shared: &Shared,
    pager: &Arc<Pager>,
    sst_ids: &Arc<AtomicU64>,
    options: SstWriterOptions,
    sources: Vec<Source>,
    seqno: pigeonhole_format::Seqno,
    closing: &dyn Fn() -> Result<()>,
) -> Result<Vec<SstMeta>> {
    if sources.is_empty() {
        return Ok(Vec::new());
    }
    let mut merged = MergingCursor::new(sources);
    merged.seek_to_first()?;
    let mut sink = SstSink::new(
        Arc::clone(pager),
        Arc::clone(sst_ids),
        options,
        shared.picker.target_sst_bytes,
    );
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
    Ok(std::mem::take(&mut sink.outputs))
}

/// Relocates manifest-named extents past the shrink point and truncates the file. Returns
/// the bytes the file shrank by.
///
/// Every round first gives back the free space already at the tail, so a file whose tail
/// holds nothing live (everything there was deleted and compacted away) shrinks without
/// moving anything. An extent with no free extent of its class below it is skipped, not an
/// error (the file then ends after it), and the extents past it still move.
pub(crate) fn shrink(shared: &Shared) -> Result<u64> {
    let mut released = 0;
    for _round in 0..8 {
        if shared.closing.load(Ordering::Acquire) {
            // The close waits for this call: stop between rounds.
            return Ok(released);
        }
        // Retired extents no view uses any more are free space: at the tail they are cut
        // off now, below it the relocations can move into them (without this the targets
        // would be allocated past the end of the file).
        shared.reclaim();
        released += shared.pager.truncate_tail()?;
        let plan = shared.pager.shrink_plan();
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
        let mut moves = Moves {
            pager: Arc::clone(&shared.pager),
            list: Vec::new(),
        };
        let mut readers = Vec::new();
        let mut rewrite = false;
        let mut claimed: Vec<SstId> = Vec::new();
        // An extent retired since the catalog was read: the plan is stale, so a round with
        // nothing to move plans again instead of stopping.
        let mut stale = false;
        for extent in plan {
            if [root.snapshot, root.log]
                .into_iter()
                .flatten()
                .any(|e| e == extent)
            {
                rewrite = true;
                continue;
            }
            let Some((family, meta)) = by_extent.get(&(extent.page, extent.size_class)) else {
                // Not named by the manifest: an output in flight (decision D60).
                continue;
            };
            {
                let mut busy = shared
                    .busy_ssts
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if busy.contains(&meta.id) {
                    continue;
                }
                busy.insert(meta.id);
            }
            claimed.push(meta.id);
            let target = match shared.pager.relocate(extent) {
                Ok(t) => t,
                // No free extent of its class below it: it stays, and the smaller ones
                // past it may still move.
                Err(pigeonhole_pager::Error::NoSpace) => continue,
                // A compaction that committed after the catalog was read retired it (the
                // view held here keeps it from being reclaimed, so it is not reused).
                Err(_) if !shared.pager.is_live(extent) => {
                    stale = true;
                    continue;
                }
                Err(e) => {
                    unclaim(shared, &claimed);
                    return Err(e.into());
                }
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
            match SstReader::open(
                shared.pager.file().clone(),
                &new_meta,
                Arc::clone(&shared.cache),
                priority,
            ) {
                Ok(r) => readers.push((id, Arc::new(r))),
                Err(e) => {
                    unclaim(shared, &claimed);
                    return Err(e.into());
                }
            }
        }
        if moves.list.is_empty() && !rewrite {
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
    if !shared.closing.load(Ordering::Acquire) {
        // What the last round's moves retired.
        shared.reclaim();
        released += shared.pager.truncate_tail()?;
    }
    Ok(released)
}

/// Shrink's relocated copies, `(old SST, its copy)`, until the commit turns them into edits.
/// Copies it never turns into edits (the request was dropped unrun) are abandoned.
struct Moves {
    pager: Arc<Pager>,
    list: Vec<(SstId, SstMeta)>,
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
        }
        edits
    }
}

impl Drop for Moves {
    fn drop(&mut self) {
        for (_, copy) in self.list.drain(..) {
            self.pager.abandon(copy.extent);
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

//! Online backup and shrink.
//!
//! **Backup** writes a brand-new database at a snapshot: every `(tablet, family)`'s sources
//! (memtables and SSTs) are merged in key order and every entry with a seqno at or below the
//! snapshot's is written into SSTs at the last level of the new file, whose manifest records
//! them as flushed through that seqno. The copy is exactly what a read at the snapshot sees,
//! writers keep running meanwhile, and the result is one clean file.
//!
//! **Shrink** relocates the extents past the pager's shrink point that the manifest names
//! (decision D60: never an in-flight flush or compaction output, which the manifest does not
//! name yet, nor an SST a running compaction reads), publishes the moves, and truncates the
//! file after everything retired has been reclaimed.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pigeonhole_compaction::MergingCursor;
use pigeonhole_format::key::split_suffix;
use pigeonhole_format::manifest::Edit;
use pigeonhole_format::scan::ScanFilter;
use pigeonhole_format::{Cursor, SstId};
use pigeonhole_pager::Pager;
use pigeonhole_sst::{SstReader, SstWriterOptions};

use crate::Result;
use crate::catalog::Catalog;
use crate::flush::SstSink;
use crate::manifest::{self, ManifestReq, ManifestWriter, ReqKind};
use crate::shard::Shared;
use crate::snapshot::{Snapshot, SstSet};

/// Writes a consistent copy of `snapshot` to `dest` (which must not exist).
pub(crate) fn backup(shared: &Shared, snapshot: &Snapshot, dest: &Path) -> Result<()> {
    let view = &snapshot.view;
    let seqno = snapshot.seqno;
    let source = &view.catalog;
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
    let result = (|| -> Result<()> {
        for t in source.tablets() {
            for f in source.family_ids_of(t.table) {
                let Some(meta) = source.family(f) else {
                    continue;
                };
                let sources = view.scan_sources(t.shard, t.id, f, &all, None, None)?;
                if sources.is_empty() {
                    continue;
                }
                let mut merged = MergingCursor::new(sources);
                merged.seek_to_first()?;
                let mut options = SstWriterOptions::for_family(&meta.options, t.table, f, t.id);
                options.created_micros = created;
                let mut sink = SstSink::new(
                    Arc::clone(&pager),
                    Arc::clone(&sst_ids),
                    options,
                    shared.picker.target_sst_bytes,
                );
                while merged.valid() {
                    let (_, _, s, _) = split_suffix(merged.key())?;
                    if s <= seqno {
                        sink.add(merged.key(), merged.value())?;
                    }
                    merged.next()?;
                }
                sink.cut()?;
                for meta in sink.outputs.drain(..) {
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

/// Where an SST is referenced: `((tablet, family), level)`.
type SstRef = (
    (pigeonhole_format::TabletId, pigeonhole_format::FamilyId),
    u8,
);

/// Relocates manifest-named extents past the shrink point and truncates the file. Returns
/// bytes released.
pub(crate) fn shrink(shared: &Shared) -> Result<u64> {
    let mut released = 0;
    for _round in 0..8 {
        if shared.closing.load(Ordering::Acquire) {
            // The close waits for this call: stop between rounds.
            break;
        }
        // Retired extents no view uses any more are free space the relocations can move
        // into; without this the targets would be allocated past the end of the file.
        shared.reclaim();
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
        // Every SST by extent, with everywhere it is referenced.
        let mut by_extent: HashMap<(u64, u8), Vec<SstRef>> = HashMap::new();
        let mut metas: HashMap<SstId, Arc<pigeonhole_format::manifest::SstMeta>> = HashMap::new();
        for ((tablet, family), list) in &catalog.ssts {
            for (level, meta) in list {
                by_extent
                    .entry((meta.extent.page, meta.extent.size_class))
                    .or_default()
                    .push(((*tablet, *family), *level));
                metas.insert(meta.id, Arc::clone(meta));
            }
        }
        let mut edits = Vec::new();
        let mut readers = Vec::new();
        let mut rewrite = false;
        let mut claimed: Vec<SstId> = Vec::new();
        let mut fresh = Vec::new();
        let mut stop = false;
        for extent in plan {
            if [root.snapshot, root.log]
                .into_iter()
                .flatten()
                .any(|e| e == extent)
            {
                rewrite = true;
                continue;
            }
            let Some(refs) = by_extent.get(&(extent.page, extent.size_class)) else {
                // Not named by the manifest: an output in flight (decision D60).
                continue;
            };
            let Some(meta) = catalog
                .ssts
                .get(&refs[0].0)
                .and_then(|l| l.iter().find(|(_, m)| m.extent == extent))
                .map(|(_, m)| Arc::clone(m))
            else {
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
                Err(pigeonhole_pager::Error::NoSpace) => {
                    stop = true;
                    break;
                }
                Err(e) => {
                    unclaim(shared, &claimed);
                    for e in fresh {
                        shared.pager.abandon(e);
                    }
                    return Err(e.into());
                }
            };
            fresh.push(target);
            let id = SstId(shared.sst_ids.fetch_add(1, Ordering::Relaxed));
            let mut new_meta = (*meta).clone();
            new_meta.id = id;
            new_meta.extent = target;
            let priority = catalog
                .family(refs[0].0.1)
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
                    for e in fresh {
                        shared.pager.abandon(e);
                    }
                    return Err(e.into());
                }
            }
            for ((tablet, family), level) in refs {
                edits.push(Edit::RemoveSst {
                    tablet: *tablet,
                    family: *family,
                    sst: meta.id,
                });
                edits.push(Edit::AddSst {
                    tablet: *tablet,
                    family: *family,
                    level: *level,
                    meta: new_meta.clone(),
                });
            }
        }
        if edits.is_empty() && !rewrite {
            unclaim(shared, &claimed);
            break;
        }
        let (req, waiter) = ManifestReq::with_waiter(ReqKind::Edits(edits));
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
        // refused request) or already named (a commit whose view publish failed).
        committed?;
        shared.reclaim();
        released += shared.pager.truncate_tail()?;
        if stop {
            break;
        }
    }
    Ok(released)
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

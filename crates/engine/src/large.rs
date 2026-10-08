//! Values above the inline limit (#230): separated into blob files when the batch is routed,
//! so the WAL record and the memtable entry hold only a 17-byte pointer.
//!
//! A value longer than D16's inline limit cannot pass through one WAL record and one memtable
//! entry. [`separate`] writes each such put's stored value into a new blob file (one
//! [`BlobSink`] per family), commits the files in the manifest (the root commit's sync makes
//! their bytes durable first), and rewrites the batch with pointers. From there a pointer is
//! like any separated value: flushes, compactions, blob GC and reads handle it.
//!
//! A [`LargeValues`] guard owns the new files until the commit's outcome is known. What
//! decides is where the commit stopped, not the error it returned: a shard notes the files
//! of every batch whose record its WAL stream accepted (`note_logged`, the commit point of
//! a single-shard batch or a cross-shard PREPARE). From there the batch may be applied, or
//! replayed, so the files stay whatever the outcome (an apply that fails after the append
//! poisons the shard; a failed commit's files are swept at the next open, `Engine::open`,
//! when nothing points into them). A commit that fails before any of its records was
//! logged, or is never submitted (including an unwind before submission), releases them.
//! A value the same-commit collapse (D34) drops at apply has a file of its own, which the
//! shard reports (`Shared::large_dropped`) and the guard releases.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};

use pigeonhole_compaction::{BlobSink, NewBlobFile, blob_pointer, encode_blob_stored};
use pigeonhole_format::key::Kind;
use pigeonhole_format::manifest::Edit;
use pigeonhole_format::wal::{BatchBuilder, BatchRef};
use pigeonhole_format::{BlobFileId, FamilyId};
use pigeonhole_runtime::{Notifier, ShardId};

use crate::catalog::Catalog;
use crate::manifest::{self, ManifestReq, ReqKind};
use crate::shard::Shared;
use crate::{CommitInfo, Error, Result};

/// A mutation's same-commit collapse key (D34): table, family, whether it is a family
/// marker, row, qualifier and timestamp (`None`: the commit's).
type MutKey<'a> = (
    pigeonhole_format::TableId,
    FamilyId,
    bool,
    &'a [u8],
    &'a [u8],
    Option<pigeonhole_format::Timestamp>,
);

/// Largest stored value a blob pointer can name (its length is a `u32`).
const MAX_STORED: usize = u32::MAX as usize;

/// The smallest extents (the sink rounds up to 64 KiB) for a large value's blob file: the
/// sink takes larger ones for a value more than `VALUE_EXTENTS` times as long. Larger
/// fixed extents trimmed to a short file would leave free runs too short for the next
/// one, so every separation would grow the file.
const EXTENT_BYTES: u64 = 64 << 10;

/// Extents a large value may span: a multi-extent file's last extent keeps its unused tail
/// (FORMAT §7: one size class per file), so this bounds the waste to one extent of at most
/// about 2/256 of the value (or 64 KiB), not the up to half a sink's default four allow.
const VALUE_EXTENTS: u32 = 256;

/// The blob files a batch's large values went to, until the commit's outcome decides whether
/// they stay. Dropped while it still holds files (not handed to a notifier, or the commit
/// stopped before its commit point), it queues their `DropBlobFile`.
pub(crate) struct LargeValues {
    shared: Arc<Shared>,
    files: Vec<BlobFileId>,
}

impl std::fmt::Debug for LargeValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LargeValues")
            .field("files", &self.files)
            .finish_non_exhaustive()
    }
}

impl LargeValues {
    fn new(shared: &Arc<Shared>, files: Vec<BlobFileId>) -> Self {
        shared.large_open.fetch_add(1, Ordering::AcqRel);
        LargeValues {
            shared: Arc::clone(shared),
            files,
        }
    }

    /// Hands the guard to the commit's notifier. When the commit resolves, a file the
    /// collapse dropped is released; the others stay when `succeeded` says the batch was
    /// applied or a shard logged it, and are released otherwise.
    pub(crate) fn settle_on<T: Send + 'static>(
        self,
        notifier: &Notifier<T>,
        succeeded: fn(Option<&T>) -> bool,
    ) {
        notifier.on_resolve(move |outcome| {
            let mut guard = self;
            let succeeded = succeeded(outcome);
            let shared = Arc::clone(&guard.shared);
            let mut logged = shared
                .large_logged
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let mut dropped = shared
                .large_dropped
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let mut pending = shared
                .large_pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            guard.files.retain(|id| {
                let was_logged = logged.remove(id);
                if dropped.remove(id) {
                    return true;
                }
                if succeeded {
                    // Applied: the batch points into it.
                    pending.remove(id);
                } else if !was_logged {
                    // Never logged: nothing points into it, now or after a replay.
                    return true;
                }
                // Logged but failed (applied, half applied or not): kept, and left pending,
                // so nothing counts on what points into it until the next open sweeps it
                // or finds the replayed pointers.
                false
            });
            drop((logged, dropped, pending));
        });
    }

    /// Queues the `DropBlobFile` of every file still held (computed against the catalog at
    /// commit time: a file already gone is left alone). Never blocks.
    fn release(&mut self) {
        let files = std::mem::take(&mut self.files);
        if files.is_empty() {
            return;
        }
        crate::shard::trace!("large values: releasing blob files {files:?}");
        let ids = files.clone();
        let change = move |catalog: &mut Catalog| {
            Ok(files
                .iter()
                .filter(|id| catalog.blob_files.contains_key(id))
                .map(|id| Edit::DropBlobFile { blob_file: *id })
                .collect())
        };
        let shared = Arc::clone(&self.shared);
        let req = ManifestReq::edits(Vec::new(), move |_| {
            let mut pending = shared
                .large_pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            for id in ids {
                pending.remove(&id);
            }
        });
        let req = ManifestReq {
            kind: ReqKind::Catalog(Box::new(change)),
            ..req
        };
        manifest::submit(&self.shared, ShardId(0), req);
    }
}

impl Drop for LargeValues {
    fn drop(&mut self) {
        self.release();
        self.shared.large_open.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Whether a commit's outcome says its batch was applied.
pub(crate) fn commit_succeeded(outcome: Option<&Result<CommitInfo>>) -> bool {
    matches!(outcome, Some(Ok(_)))
}

/// As [`commit_succeeded`], for a `check_and_mutate`, which does not apply when its predicate
/// is false.
pub(crate) fn check_succeeded(outcome: Option<&Result<(bool, Option<CommitInfo>)>>) -> bool {
    matches!(outcome, Some(Ok((true, _))))
}

/// Notes the blob files `batch`'s pointers name as logged: its record was handed to the WAL
/// (the shard's commit point), so the guard keeps them whatever the outcome. Called by the
/// shard while any guard is alive.
pub(crate) fn note_logged(shared: &Shared, batch: BatchRef<'_>) {
    let mut ids = HashSet::new();
    for m in batch.iter().flatten() {
        if m.kind == Kind::Put
            && let Some(p) = blob_pointer(m.value)
        {
            ids.insert(p.blob_file);
        }
    }
    if ids.is_empty() {
        return;
    }
    let pending = shared
        .large_pending
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    ids.retain(|id| pending.contains(id));
    drop(pending);
    shared
        .large_logged
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .extend(ids);
}

/// Rewrites `builder` with every put whose stored value is longer than `limit` separated into
/// new blob files, committed in the manifest before this returns. Returns the builder
/// unchanged and no guard when no value is that long. Blocks on the manifest commit.
pub(crate) fn separate(
    shared: &Arc<Shared>,
    builder: BatchBuilder,
    limit: usize,
) -> Result<(BatchBuilder, Option<LargeValues>)> {
    let batch = builder.batch();
    let mut large = false;
    for m in batch.iter() {
        let m = m?;
        if m.value.len() > limit {
            if m.kind != Kind::Put || m.value.len() > MAX_STORED {
                // Merge operands are never separated, and a pointer's length is a `u32`.
                return Err(Error::ValueTooLarge);
            }
            large = true;
        }
    }
    if !large {
        return Ok((builder, None));
    }
    // Same-commit collapse (D34): only the last mutation per (column, timestamp) is
    // applied, so the earlier ones are left out here, and no value of theirs goes to a blob
    // file that nothing would point into. Default timestamps all become the commit's, so
    // they compare equal. An explicit timestamp equal to the commit's own cannot be told at
    // routing (the shard assigns it): a value a later mutation of its column may displace
    // that way goes to a blob file of its own, which the shard reports when it drops the
    // value (`Shared::large_dropped`), so the guard releases exactly its bytes.
    let mut last: HashMap<MutKey<'_>, usize> = HashMap::new();
    let mutations: Vec<_> = builder
        .batch()
        .iter()
        .collect::<std::result::Result<_, _>>()?;
    for (i, m) in mutations.iter().enumerate() {
        last.insert(
            (
                m.table,
                m.family,
                m.kind == Kind::FamilyDelete,
                m.row,
                m.qualifier,
                m.ts,
            ),
            i,
        );
    }
    // The last mutation per column and timestamp kind (default or explicit).
    let mut last_kind: HashMap<(MutKey<'_>, bool), usize> = HashMap::new();
    for (key, i) in &last {
        let column = (key.0, key.1, key.2, key.3, key.4, None);
        let at = last_kind.entry((column, key.5.is_some())).or_insert(*i);
        *at = (*at).max(*i);
    }
    let displaceable = |i: usize, key: &MutKey<'_>| {
        let column = (key.0, key.1, key.2, key.3, key.4, None);
        last_kind
            .get(&(column, key.5.is_none()))
            .is_some_and(|j| *j > i)
    };
    // One sink per family, plus one per displaceable value (`None` here: never shared).
    let mut sinks: Vec<(FamilyId, bool, BlobSink)> = Vec::new();
    let mut out = BatchBuilder::new();
    let written = (|| -> Result<()> {
        for (i, m) in mutations.iter().enumerate() {
            let key = (
                m.table,
                m.family,
                m.kind == Kind::FamilyDelete,
                m.row,
                m.qualifier,
                m.ts,
            );
            if last.get(&key) != Some(&i) {
                continue;
            }
            if m.value.len() <= limit {
                out.push(m.table, m.family, m.kind, m.row, m.qualifier, m.ts, m.value)?;
                continue;
            }
            let shared_sink = !displaceable(i, &key);
            let found = shared_sink
                .then(|| sinks.iter().position(|(f, s, _)| *s && *f == m.family))
                .flatten();
            let sink = match found {
                Some(i) => &mut sinks[i].2,
                None => {
                    let sink = BlobSink::new(
                        Arc::clone(&shared.pager),
                        Arc::clone(&shared.blob_ids),
                        EXTENT_BYTES,
                        u64::MAX,
                    )
                    .spread_values(VALUE_EXTENTS);
                    sinks.push((m.family, shared_sink, sink));
                    &mut sinks.last_mut().expect("pushed").2
                }
            };
            let ptr = sink.append(m.value)?;
            out.push(
                m.table,
                m.family,
                m.kind,
                m.row,
                m.qualifier,
                m.ts,
                &encode_blob_stored(&ptr),
            )?;
        }
        Ok(())
    })();
    if let Err(e) = written {
        for (_, _, mut sink) in sinks {
            sink.abandon();
        }
        return Err(e);
    }
    let mut files: Vec<(FamilyId, NewBlobFile)> = Vec::new();
    for (family, _, sink) in sinks {
        // A failed finish gives its own extents back; the files finished before it are
        // still unpublished, so give those back too.
        match sink.finish() {
            Ok(done) => files.extend(done.into_iter().map(|f| (family, f))),
            Err(e) => {
                for (_, f) in files {
                    for x in f.extents {
                        shared.pager.abandon(x);
                    }
                }
                return Err(e.into());
            }
        }
    }
    let ids: Vec<BlobFileId> = files.iter().map(|(_, f)| f.id).collect();
    let edits: Vec<Edit> = files
        .into_iter()
        .map(|(family, f)| Edit::PutBlobFile {
            blob_file: f.id,
            family,
            extents: f.extents,
            total_bytes: f.total_bytes,
            live_bytes: f.total_bytes,
        })
        .collect();
    // Pending before the commit publishes them: until a batch points into them, nothing
    // may count on what points into them (`check_blob_accounting`).
    shared
        .large_pending
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .extend(ids.iter().copied());
    // A refused commit frees the files' extents (`manifest::begin`); a failed one poisons
    // the pager, and the extents are free space at the next open (D8).
    if let Err(e) = manifest::commit_from_thread(shared, ReqKind::Edits(edits)) {
        let mut pending = shared
            .large_pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for id in &ids {
            pending.remove(id);
        }
        return Err(e);
    }
    Ok((out, Some(LargeValues::new(shared, ids))))
}

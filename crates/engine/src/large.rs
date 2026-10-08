//! Values above the inline limit (#230): separated into blob files when the batch is routed,
//! so the WAL record and the memtable entry hold only a 17-byte pointer.
//!
//! A value longer than D16's inline limit cannot pass through one WAL record and one memtable
//! entry. [`separate`] writes each such put's stored value into a new blob file (one
//! [`BlobSink`] per family), commits the files in the manifest (the root commit's sync makes
//! their bytes durable first), and rewrites the batch with pointers. From there a pointer is
//! like any separated value: flushes, compactions, blob GC and reads handle it.
//!
//! A [`LargeValues`] guard owns the new files until the commit's outcome is known. It
//! queues a `DropBlobFile` for them when the commit was certainly not applied (refused
//! before any shard applied it, or never submitted, including an unwind before
//! submission). An ambiguous failure (an I/O error after the batch may have been applied, a
//! closing engine) keeps the files: such failures poison the shard or close the engine, and
//! the open-time sweep (`Engine::open`) drops a blob file that nothing points into.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError};

use pigeonhole_compaction::{BlobSink, NewBlobFile, encode_blob_stored};
use pigeonhole_format::key::Kind;
use pigeonhole_format::manifest::Edit;
use pigeonhole_format::wal::BatchBuilder;
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

/// Extents of about this size for a large value's blob file (the sink takes larger ones for
/// a value more than four times as long).
const EXTENT_BYTES: u64 = 1 << 20;

/// The blob files a batch's large values went to, until the commit's outcome decides whether
/// they stay. Dropped while it still holds files (not handed to a notifier, or the outcome
/// says the batch was not applied), it queues their `DropBlobFile`.
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
    /// Hands the guard to the commit's notifier: the files stay unless `refused` says the
    /// outcome proves the batch was never applied.
    pub(crate) fn settle_on<T: Send + 'static>(
        self,
        notifier: &Notifier<T>,
        refused: fn(Option<&T>) -> bool,
    ) {
        notifier.on_resolve(move |outcome| {
            let mut guard = self;
            if !refused(outcome) {
                // Kept: the batch points into them (or may, after an ambiguous failure).
                let mut pending = guard
                    .shared
                    .large_pending
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                for id in guard.files.drain(..) {
                    pending.remove(&id);
                }
            }
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
    }
}

/// Whether a commit's outcome proves its batch was never applied: refused before any shard
/// applied it. Anything else (success, an I/O error, a closing engine, a dropped reply) may
/// have applied it.
pub(crate) fn commit_refused(outcome: Option<&Result<CommitInfo>>) -> bool {
    matches!(outcome, Some(Err(e)) if refused_error(e))
}

/// Errors returned only before a batch is applied.
fn refused_error(e: &Error) -> bool {
    matches!(
        e,
        Error::Conflict
            | Error::Busy
            | Error::BatchTooLarge
            | Error::RecordTooLarge
            | Error::KeyTooLarge
            | Error::ValueTooLarge
            | Error::InvalidArgument(_)
    )
}

/// As [`commit_refused`], for a `check_and_mutate`, which also does not apply when its
/// predicate is false.
pub(crate) fn check_refused(outcome: Option<&Result<(bool, Option<CommitInfo>)>>) -> bool {
    match outcome {
        Some(Ok((applied, _))) => !applied,
        Some(Err(e)) => refused_error(e),
        None => false,
    }
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
    // they compare equal; an explicit timestamp equal to the commit's own cannot be told
    // at routing (the shard assigns it), and would at worst leave a value's bytes counted
    // live with nothing pointing into them.
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
    let mut sinks: Vec<(FamilyId, BlobSink)> = Vec::new();
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
            let sink = match sinks.iter_mut().position(|(f, _)| *f == m.family) {
                Some(i) => &mut sinks[i].1,
                None => {
                    let sink = BlobSink::new(
                        Arc::clone(&shared.pager),
                        Arc::clone(&shared.blob_ids),
                        EXTENT_BYTES,
                        u64::MAX,
                    );
                    sinks.push((m.family, sink));
                    &mut sinks.last_mut().expect("pushed").1
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
        for (_, mut sink) in sinks {
            sink.abandon();
        }
        return Err(e);
    }
    let mut files: Vec<(FamilyId, NewBlobFile)> = Vec::new();
    for (family, sink) in sinks {
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
    // A refused commit frees the files' extents (`manifest::begin`); a failed one poisons
    // the pager, and the extents are free space at the next open (D8).
    manifest::commit_from_thread(shared, ReqKind::Edits(edits))?;
    shared
        .large_pending
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .extend(ids.iter().copied());
    Ok((
        out,
        Some(LargeValues {
            shared: Arc::clone(shared),
            files: ids,
        }),
    ))
}

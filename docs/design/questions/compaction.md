# Compaction questions (blob separation and blob GC, #33)

## Proposed decision: what a blob record holds, and which values are separated
FORMAT §7 left open whether a blob record holds the value's payload or its stored form, and §3 says the pointer's `len` is the "value length". The spec says values "above a per-family threshold" are separated.

**Interim behavior:** a blob record holds the stored value the pointer replaced, tag byte included, and the pointer's `len` is that stored length; a read returns the record's bytes as the stored value, pinned in the block cache, with no copy and no re-tagging. A put is separated when its stored value has tag `Bytes` and its payload is longer than `blob_threshold` (`u32::MAX` never; `separates`). Typed values (at most nine bytes) and merge operands are never separated, so an `i64` counter's base is never a pointer whatever the threshold. FORMAT §7 says so.

## Proposed decision: separation happens at flush as well as at compaction
Issue #33 says "at the output level". With separation only in compaction outputs, a lone L0 SST that the picker trivially moves down keeps its large values inline indefinitely (sequential loads do this), and every value is written once into an SST before it reaches a blob file, while the spec says large values are "written once to a blob extent, so compaction never rewrites them".

**Interim behavior:** the engine's flush task separates (through the same `BlobSink` and `separates` rule), and every compaction separates whatever is still inline (values flushed before a threshold change, values written by an open-time spill, which stays inline). The flush commits `PutBlobFile` edits with its `AddSst`s; a flush for a table dropped meanwhile frees its blob files with its SSTs.

## Proposed decision: the engine drops blob files; `dropped_blob_files` stays empty
D80 already said the job cannot know a file's live bytes. After a split both children reference the parent's SSTs and therefore its blob files, so no single job can tell that a file is empty.

**Interim behavior:** `CompactionOutput::dropped_blob_files` is always empty. The job reports `blob_live_delta`: `-(16 + len)` for every pointer it drops (D80) and for every value a blob GC copies out of a file it empties. The engine applies the deltas to the catalog **at commit time** (a `ReqKind::Catalog` request, since other tablets' compactions change the same files) and emits `DropBlobFile` for a file whose count reaches zero. Invariant (checked by the `check_blob_accounting` test hook at every full dump of the engine model harness): a file's live bytes equal `16 + len` summed over the pointers the SSTs hold within their tablets' rows, an SST shared after a split counting once per tablet.

## Proposed decision: how blob GC picks work
Issue #33 asks to pick blob GC "from per-file live-byte ratios". The manifest does not record which SSTs point into which blob file.

**Interim behavior:** a file is a candidate when it is at least half garbage and holds at least `target_sst_bytes / 16` garbage bytes (`pick_blob_gc`). A `TaskKind::BlobGc` task rewrites **every** SST of one `(tablet, family)` into the last level, copying the values still in its candidate files into new files. Each shard remembers in memory which slots it has emptied each candidate from and does not pick them again for that file (a slot that committed a blob GC holds no pointer into the file afterwards), so the work terminates even if some count were off; after a reopen, a merge or a move a slot may be rewritten once more for nothing. Blob GC runs only when no other compaction is due. `Engine::compact` turns each full-compaction rewrite into a blob GC of every file of the family with any garbage, so `compact()` reclaims what it can. No knob yet; a public one can come with the Phase 2 options work.

## Proposed decision: blob extents are at most 1 MiB unless a value needs more
All extents of a blob file share one size class (FORMAT §7) and cannot be trimmed one by one, so the last extent's unused tail is wasted.

**Interim behavior:** `BlobSink` takes extents of about half the input (or memtable) size, capped at 1 MiB, and cuts files near `4 × target_sst_bytes`. A value more than four extents long starts a new file with extents of a quarter of its size (up to 64 MiB). A file of a single extent is trimmed to its length at finish (`Pager::trim`, D128).

## Proposed decision (amends D77): value predicates see separated values
D77 said a blob pointer matches no byte predicate because the resolver does not read blobs. With separation, a predicate's result would then depend on whether a compaction had run.

**Interim behavior:** additive `ResolveOptions::blobs: Option<Arc<dyn BlobFetch>>`. When it is set, the resolver tests a value predicate on the value a pointer names, and loads a separated merge base before folding operands onto it. Because the resolver's error type is its cursor's, `BlobFetch::fetch` returns `None` on failure and the implementation keeps the error: the engine reports it after each resolver step (`PredicateBlobs::check`), so a failed blob read is an error, never a silent non-match. The engine sets the hook only for reads with a value predicate on a version naming blob files; `check_and_mutate` resolves the pointer before testing its predicate.

## Q: does a `Drop` task (FIFO-by-time, #32) account for blob bytes?
A `Drop` removes SSTs without reading them, so it reports no `blob_live_delta`, and the blob files its SSTs point into would never reach zero (a space leak, not a read error).

**Interim behavior:** no picker emits `Drop` yet. Noted on #32: when it does, the engine must account for the dropped SSTs' pointers (read them before committing, or rewrite instead of dropping an SST that holds pointers).

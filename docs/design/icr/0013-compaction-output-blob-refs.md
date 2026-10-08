# ICR 0013: `CompactionOutput::blob_refs`

**Status:** proposed with the implementation (PR for #240), at the coordinator's request ("record which blob files each SST references").

## Change
`pigeonhole_compaction::CompactionOutput` gains one public field:

```rust
/// Per added SST, the blob files its puts point into and the bytes they reference
/// (`Edit::SstBlobRefs`, #240), sorted by blob file; an empty list for an SST with no
/// pointer. Every SST of `added` has an entry.
pub blob_refs: Vec<(SstId, Vec<(BlobFileId, u64)>)>,
```

`CompactionOutput` is not `#[non_exhaustive]`, so a struct literal that names every field stops compiling. The additive pieces need no ICR: the new `Edit::SstBlobRefs` variant (`Edit` is `#[non_exhaustive]`; manifest tag 13, skipped by older readers, FORMAT §9.3) and the new function `pigeonhole_compaction::note_blob_ref`.

## Why
The engine must record every new SST's blob references in the manifest. Only the job sees the entries it writes; reading the outputs back at commit time would be extra I/O on the commit path.

## Callers
- `crates/compaction/src/job.rs`: `CompactionJob::finish` builds it (the only constructor; `Default` stays).
- `crates/engine/src/compact.rs`: `CompactionWork::edits` turns each entry into an `Edit::SstBlobRefs` after its `AddSst`.
- Tests construct outputs only through jobs, so no struct literal exists in the workspace.

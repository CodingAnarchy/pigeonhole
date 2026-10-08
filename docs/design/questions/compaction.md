# Compaction questions (per-SST blob references, #240)

## Proposed decision (refines D180): blob GC follows the SSTs' recorded blob references
D180's blob GC rewrote each candidate blob file out of every slot of its family, and remembered in memory which slots it had emptied, so after a reopen (or for a tablet a merge created or a move brought) a slot could be rewritten once more for nothing.

**Interim behavior:**
- Every commit that adds a new SST also commits `Edit::SstBlobRefs` (manifest tag 13, FORMAT §9.3). It lists the blob files the SST's puts point into, with the bytes they reference (`16 + len` per pointer), and an empty list when the SST holds no pointer. Flushes, compactions, open-time spills, backups and shrink copies all write one. Writers count with `note_blob_ref` as they add entries, so a reference costs no extra I/O. The catalog drops the references of SSTs no tablet references after each batch, so a trivial move or a split keeps them.
- Blob GC picks a slot for a candidate file only when one of the slot's SSTs records a reference into it. An SST with no record (written before tag 13) counts as pointing anywhere. After a slot's blob GC commits, its new SSTs record no reference into the file, so the slot is not picked again, across reopens too. The in-memory record of emptied slots is gone.
- A FIFO `Drop` (D180) uses a dropped SST's recorded bytes instead of reading it when the SST holds only this tablet's rows (not shared with a sibling and not inherited from a split's parent). Otherwise it still reads the SST within the tablet's rows, since the record counts every row.
- `check_blob_accounting` (test hook, run at every full dump of the engine model harness) also checks that each SST's record equals the pointers it holds.

## Q: `CompactionOutput` is not `#[non_exhaustive]`
The job reports each output's references through a new public field, `CompactionOutput::blob_refs`.

**Interim behavior:** ICR 0013 describes the change and its callers (the job builds it; the engine reads it; nothing else constructs the struct).

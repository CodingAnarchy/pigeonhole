# Engine questions (shrink and blob extents, #231)

## Proposed decision (amends D160): shrink relocates blob extents
D160's `shrink` relocated SST extents only, and treated blob extents like output in flight, so a blob file at the tail set a floor on the file.

**Interim behavior:**
- `shrink` also maps the catalog's blob extents and relocates any past the shrink point into a free extent of the same class below. The copy keeps the extent header (file id and position), so it can replace the original in place.
- The move commits as a `PutBlobFile` whose extent list has the copy at the same position, computed against the catalog at commit time. If the file was dropped meanwhile (blob GC or `drop_table`) or no longer lists that extent, the copy is abandoned. A refused request frees the copies.
- Blob files are never written once published, so a move needs no claim. A blob GC or backup reading the file keeps reading the old extents through its view, which pins their manifest version.
- The manifest retires the extents a file's new list drops. Views build a new blob reader for a file whose extents changed, while older views keep theirs, which reads the retired extents they pin. Cached records stay valid: they are keyed by the file and the logical offset, and the bytes are the same.

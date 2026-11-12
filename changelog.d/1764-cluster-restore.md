### Added
- `phdb cluster restore DIR --node URL`: a `phdb cluster backup` put back (D317 ticket 8b, ICR 0117).
  - Each partition's copy is checked against the manifest, uploaded to the node the backup's map (or `--map`) gives it, and installed there at a new epoch; then the backup's map is set by the forced path, its tables exactly as the backup has them, and each partition is unfenced once it's open.
  - Fence every partition first, and close each on the node it's restored to. Writes since the backup are lost, and feed tokens from before the restore answer `FeedExpired`.
  - `--cluster-id` restores under another cluster id; `--old-unreachable` skips the check on a node that's down; `--parallel N` uploads that many at once.

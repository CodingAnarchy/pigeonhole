# 0135: `Pigeonhole::database_files` and `Pigeonhole::database_file_suffix`

**Status:** Approved. The number is from the coordinator (`.coord/numbers.md`), for D317 ticket 6. Both functions are additive, and the coordinator split them out of ticket 6 PR 1 so #1748 can use them.

## The change
- **`Pigeonhole::database_files(path) -> Result<Vec<PathBuf>>`:** every file of the database at `path` that exists now, listed without opening it. The order is:
  1. the main file;
  2. each WAL stream file (`-wal-<n>`), in stream order;
  3. the change feed's sidecar (`-feed`).
  - **How it finds them:** it scans the directory rather than reading the main file. So it lists sidecars whose main file is gone.
  - **Nothing there:** a path with no files, or no directory, answers an empty list.
  - **Not listed:** the shared-memory region. It isn't beside the database (`/dev/shm`, or `<device-inode>.phdb-shm` named by the main file's device and inode), and each open rebuilds it.
- **`Pigeonhole::database_file_suffix(db, file) -> Option<&str>`:** `file`'s suffix after `db` when it's one of `db`'s files: `""`, `-wal-<n>` or `-feed`. Anything else is `None`, including another database's file and a WAL number written in any other form (`-wal-02`).
  - Callers name a copied file by this suffix, so nothing parses file names itself.
  - Paths are compared as written, not canonicalized.
- **Both are associated functions,** taking no open database. Names are matched by suffix alone: a database whose name is another's plus `-feed` or `-wal-<n>`, in the same directory, would be listed as the other's sidecar. The engine opens such a name as the sidecar too.
- **Internal crates:** none change. The feed suffix is `pigeonhole-format`'s `FEED_SUFFIX`, and a test pins the WAL name to `pigeonhole_wal::stream_path`.

## Why
D317 ticket 6 moves a partition between nodes as its set of files. Close hashes the set, the upload sends each file named by its suffix, and the install replaces the whole set. Only the engine knows which files make a database: with a change feed, a clean close keeps its WAL files as the feed's history (D310 §2.1). This keeps that knowledge in one place.

## Callers
- **`phdb serve` (D317 ticket 6):** a closed partition's files and their hashes; the install's set (files outside the manifest move aside); `drop-partition`'s move; the relocation's upload.
- **The replica bootstrap (#1748, feed1248):** removing a side copy's whole file set before a download and after a failure, its main file present or not.

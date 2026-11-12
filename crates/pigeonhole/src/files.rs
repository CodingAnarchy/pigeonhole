//! A database's files on disk (ICR 0135): the main file and the sidecars beside it.

use std::path::{Path, PathBuf};

use pigeonhole_format::feed::FEED_SUFFIX;

use crate::{Error, ErrorCode, Pigeonhole, Result};

/// A WAL stream file's suffix before its stream number (`pigeonhole_wal::stream_path`).
const WAL_INFIX: &str = "-wal-";

impl Pigeonhole {
    /// Every file of the database at `path` that exists now, without opening it: the main
    /// file, then each WAL stream file (`-wal-<n>`, in stream order), then the change
    /// feed's sidecar (`-feed`). It's what moving or copying a closed database needs.
    ///
    /// After a clean close by the last process, a database without a change feed is its main
    /// file alone: its WAL files are removed. With a change feed they stay, holding the
    /// feed's history (D310 §2.1), so the set is the main file, every WAL file and the
    /// sidecar. A WAL file left by a process that didn't close is listed too: it holds
    /// commits the main file doesn't have yet.
    ///
    /// The list is a snapshot of the directory. While the database is open, its files keep
    /// changing, so copy them only once it's closed.
    ///
    /// The directory is scanned, not the main file read: sidecars whose main file is gone
    /// are listed (what a removal left half done, so a caller can finish it), and a path
    /// with no files, or no directory, answers an empty list, so a cleanup can run again.
    ///
    /// Names are matched by suffix alone: don't keep a database whose name is another's
    /// plus `-feed` or `-wal-<n>` in the same directory, since it would be listed as the
    /// other's sidecar (the engine would open it as one too). A path that isn't UTF-8 lists
    /// nothing.
    ///
    /// ```
    /// use pigeonhole::{Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let path = dir.join("data.phdb");
    /// Pigeonhole::open(&path, Options::default().create_if_missing(true))?.close()?;
    /// assert_eq!(Pigeonhole::database_files(&path)?, [path.clone()]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn database_files(path: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
        let path = path.as_ref();
        let dir = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        let io = |e: std::io::Error| {
            Error::new(
                ErrorCode::Io,
                format!("list {}'s files: {e}", path.display()),
            )
        };
        let mut main = None;
        let mut wal = Vec::new();
        let mut feed = None;
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(io(e)),
        };
        for entry in entries {
            // Named beside `path` as given, so a bare relative path (`data.phdb`, read from
            // `.`) matches its own files.
            let file = path.with_file_name(entry.map_err(io)?.file_name());
            match Self::database_file_suffix(path, &file) {
                Some("") => main = Some(file),
                Some(FEED_SUFFIX) => feed = Some(file),
                Some(s) => {
                    let n: u32 = s[WAL_INFIX.len()..].parse().expect("a checked suffix");
                    wal.push((n, file));
                }
                None => {}
            }
        }
        wal.sort_unstable_by_key(|(n, _)| *n);
        Ok(main
            .into_iter()
            .chain(wal.into_iter().map(|(_, f)| f))
            .chain(feed)
            .collect())
    }

    /// `file`'s suffix after the path of the database at `db`, when `file` is one of its
    /// files: `""` for the main file, `-wal-<n>` for a WAL stream file, `-feed` for the
    /// change feed's sidecar. `None` for any other file, including another database's.
    ///
    /// A file is named by this suffix when it's copied to where another path holds the same
    /// database (`db` there plus the suffix), so nothing parses file names itself.
    ///
    /// Paths are compared as written, not canonicalized (`./data/x` isn't `data/x`): pass
    /// files named from the same `db`, as [`database_files`](Self::database_files) gives.
    ///
    /// ```
    /// use std::path::Path;
    /// use pigeonhole::Pigeonhole;
    ///
    /// let db = Path::new("/data/p1.phdb");
    /// assert_eq!(Pigeonhole::database_file_suffix(db, Path::new("/data/p1.phdb")), Some(""));
    /// assert_eq!(
    ///     Pigeonhole::database_file_suffix(db, Path::new("/data/p1.phdb-wal-2")),
    ///     Some("-wal-2")
    /// );
    /// assert_eq!(
    ///     Pigeonhole::database_file_suffix(db, Path::new("/data/p1.phdb-feed")),
    ///     Some("-feed")
    /// );
    /// // Another database's file, another directory, or another name.
    /// assert_eq!(Pigeonhole::database_file_suffix(db, Path::new("/data/p10.phdb")), None);
    /// assert_eq!(Pigeonhole::database_file_suffix(db, Path::new("/other/p1.phdb")), None);
    /// assert_eq!(Pigeonhole::database_file_suffix(db, Path::new("/data/p1.phdb-wal-02")), None);
    /// ```
    pub fn database_file_suffix<'a>(db: &Path, file: &'a Path) -> Option<&'a str> {
        if file.parent() != db.parent() {
            return None;
        }
        let name = db.file_name()?.to_str()?;
        let suffix = file.file_name()?.to_str()?.strip_prefix(name)?;
        let known = match suffix {
            "" => true,
            s if s == FEED_SUFFIX => true,
            s => s.strip_prefix(WAL_INFIX).is_some_and(|n| {
                // The number exactly as `stream_path` writes it: no sign, no leading zero.
                n.parse::<u32>().is_ok_and(|v| v.to_string() == n)
            }),
        };
        known.then_some(suffix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Options;

    fn suffixes(path: &Path) -> Vec<String> {
        Pigeonhole::database_files(path)
            .unwrap()
            .iter()
            .map(|f| {
                Pigeonhole::database_file_suffix(path, f)
                    .unwrap()
                    .to_owned()
            })
            .collect()
    }

    #[test]
    fn a_closed_database_is_its_main_file_alone() {
        let dir = crate::doc_support::temp_dir();
        let path = dir.join("p1.phdb");
        let db = Pigeonhole::open(&path, Options::default().create_if_missing(true)).unwrap();
        assert!(suffixes(&path).iter().any(|s| s.starts_with("-wal-")));
        db.close().unwrap();
        assert_eq!(suffixes(&path), [""]);
    }

    /// With a change feed, a clean close keeps the WAL files: they're the feed's history.
    #[test]
    fn a_closed_database_with_a_feed_keeps_its_wal_files_and_its_sidecar() {
        let dir = crate::doc_support::temp_dir();
        let path = dir.join("p1.phdb");
        let db = Pigeonhole::open(
            &path,
            Options::default()
                .create_if_missing(true)
                .change_feed(crate::FeedRetention::bytes(1 << 20)),
        )
        .unwrap();
        let shards = db.info().unwrap().shards;
        db.close().unwrap();
        let wal: Vec<String> = (0..shards).map(|n| format!("-wal-{n}")).collect();
        let mut want = vec![String::new()];
        want.extend(wal);
        want.push("-feed".into());
        assert_eq!(suffixes(&path), want);
    }

    #[test]
    fn wal_files_are_listed_in_stream_order_and_other_files_are_not() {
        let dir = crate::doc_support::temp_dir();
        let path = dir.join("p1.phdb");
        for name in [
            "p1.phdb",
            "p1.phdb-wal-10",
            "p1.phdb-wal-2",
            "p1.phdb-feed",
            // Not this database's.
            "p1.phdb-wal-02",
            "p1.phdb-wal-x",
            "p1.phdb-feedx",
            "p1.phdb.ready",
            "p10.phdb",
            "p1.phdb-wal-",
        ] {
            std::fs::write(dir.join(name), b"").unwrap();
        }
        assert_eq!(suffixes(&path), ["", "-wal-2", "-wal-10", "-feed"]);
    }

    /// The WAL names stay the WAL crate's (`stream_path`), without this crate depending on
    /// it outside its tests.
    #[test]
    fn the_wal_suffix_is_the_wal_crates_stream_name() {
        let db = Path::new("/data/p1.phdb");
        for n in [0, 3, 10, u32::MAX] {
            let f = pigeonhole_wal::stream_path(db, pigeonhole_format::StreamId(n));
            assert_eq!(
                Pigeonhole::database_file_suffix(db, &f),
                Some(format!("-wal-{n}").as_str())
            );
        }
    }

    /// A removal that stopped after the main file (feed1248's #1748: a replica bootstrap's
    /// side copy): its sidecars are still listed, so they can be removed too.
    #[test]
    fn sidecars_whose_main_file_is_gone_are_listed() {
        let dir = crate::doc_support::temp_dir();
        let path = dir.join("p1.phdb.bootstrap");
        for name in [
            "p1.phdb.bootstrap-wal-0",
            "p1.phdb.bootstrap-feed",
            "p1.phdb",
        ] {
            std::fs::write(dir.join(name), b"").unwrap();
        }
        assert_eq!(suffixes(&path), ["-wal-0", "-feed"]);
    }

    #[test]
    fn a_database_that_doesnt_exist_has_no_files() {
        let dir = crate::doc_support::temp_dir();
        assert!(
            Pigeonhole::database_files(dir.join("none.phdb"))
                .unwrap()
                .is_empty()
        );
        // Nor one whose directory is gone: a cleanup that runs again (cli692's N2).
        assert!(
            Pigeonhole::database_files(dir.join("gone").join("none.phdb"))
                .unwrap()
                .is_empty()
        );
    }

    /// A bare relative path's files, as `database_files` names them read from `.`: beside
    /// the path as given, never `./`-prefixed (cli692's N5).
    #[test]
    fn a_bare_relative_paths_files_match_it() {
        let db = Path::new("data.phdb");
        assert_eq!(
            Pigeonhole::database_file_suffix(db, &db.with_file_name("data.phdb-wal-0")),
            Some("-wal-0")
        );
        assert_eq!(
            Pigeonhole::database_file_suffix(db, Path::new("data.phdb-feed")),
            Some("-feed")
        );
        // What `read_dir(".")` gives: compared as written, not this database's form.
        assert_eq!(
            Pigeonhole::database_file_suffix(db, Path::new("./data.phdb-wal-0")),
            None
        );
    }
}

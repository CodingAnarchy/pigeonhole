//! #1348 (ICR 0109 amendment): `Options::read_only(true)` is a true read-only open. It
//! opens the file read-only and takes the writer byte shared, so read-only opens run beside
//! one another, a writer can't open under one (nor one under a running writer), and a
//! database on read-only media opens, its log replayed into memory. It creates no shared
//! memory.

use std::path::{Path, PathBuf};

use pigeonhole::{ErrorCode, Family, IoBackend, Options, Pigeonhole, ReaderOptions};

/// The backends a test runs on: `pread` everywhere, and io_uring where it's built.
fn backends() -> Vec<IoBackend> {
    let mut b = vec![IoBackend::Pread];
    if cfg!(target_os = "linux") {
        b.push(IoBackend::Uring);
    }
    b
}

fn writer(backend: IoBackend, shm: &Path) -> Options {
    Options::default()
        .shards(2)
        .io_backend(backend)
        .shm_dir(shm)
        .create_if_missing(true)
}

fn read_only(backend: IoBackend, shm: &Path) -> Options {
    Options::default()
        .io_backend(backend)
        .shm_dir(shm)
        .read_only(true)
}

/// A database with `rows` rows, left as a crashed writer leaves it: nothing flushed, every
/// commit only in the log (the writer handle is leaked, never closed).
fn crashed(path: &Path, shm: &Path, backend: IoBackend, rows: u32) {
    let db = Pigeonhole::open(path, writer(backend, shm)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    for i in 0..rows {
        t.mutate(&i.to_be_bytes())
            .put("f", b"q", b"v")
            .commit()
            .unwrap();
    }
    db.close().unwrap();
}

fn fresh(name: &str) -> (PathBuf, PathBuf) {
    let dir = pigeonhole::doc_support::temp_dir().join(name);
    let shm = dir.join("shm");
    std::fs::create_dir_all(&shm).unwrap();
    (dir.join("data.phdb"), shm)
}

/// The shm directory's files, with their lengths.
fn listing(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (e.path(), e.metadata().unwrap().len())
        })
        .collect();
    v.sort();
    v
}

fn count(db: &Pigeonhole) -> usize {
    let t = db.table("t").unwrap().open().unwrap();
    let scan = t.scan_prefix(b"");
    let mut it = scan.iter().unwrap();
    let mut n = 0;
    while it.next_ref().unwrap().is_some() {
        n += 1;
    }
    n
}

#[test]
fn read_only_opens_run_together_and_exclude_a_writer_both_ways() {
    for backend in backends() {
        let (path, shm) = fresh(&format!("ro-locks-{backend:?}"));
        crashed(&path, &shm, backend, 10);

        // What the closed writer left in the shm directory (its directory region).
        let left = listing(&shm);
        // (a) A read-only open, then a writer in the same process: refused.
        let ro = Pigeonhole::open(&path, read_only(backend, &shm)).unwrap();
        let e = Pigeonhole::open(&path, writer(backend, &shm)).unwrap_err();
        assert_eq!(e.code(), ErrorCode::WriterLocked, "{backend:?}: {e}");
        assert!(e.to_string().contains("read-only open"), "{e}");
        // (c) A second read-only open beside it; closing one still refuses a writer.
        let ro2 = Pigeonhole::open(&path, read_only(backend, &shm)).unwrap();
        assert_eq!(count(&ro2), 10);
        ro.close().unwrap();
        let e = Pigeonhole::open(&path, writer(backend, &shm)).unwrap_err();
        assert_eq!(e.code(), ErrorCode::WriterLocked, "{backend:?}: {e}");
        // No shared memory: the read-only opens made no region, and a reader process finds
        // no writer.
        assert_eq!(listing(&shm), left, "{backend:?}");
        let e = Pigeonhole::open_reader(&path, ReaderOptions::default().shm_dir(&shm))
            .expect_err("no writer to attach to");
        assert_eq!(e.code(), ErrorCode::NoWriter, "{backend:?}: {e}");
        ro2.close().unwrap();

        // (b) A writer, then a read-only open: refused; after the writer, it opens.
        let w = Pigeonhole::open(&path, writer(backend, &shm)).unwrap();
        let e = Pigeonhole::open(&path, read_only(backend, &shm)).unwrap_err();
        assert_eq!(e.code(), ErrorCode::WriterLocked, "{backend:?}: {e}");
        w.close().unwrap();
        let ro = Pigeonhole::open(&path, read_only(backend, &shm)).unwrap();
        assert_eq!(count(&ro), 10);
        ro.close().unwrap();
    }
}

/// A database on read-only media (the file and its directory read-only), left by a writer
/// that crashed: it opens, its log replayed into memory, and its files are left byte for
/// byte.
#[cfg(unix)]
#[test]
fn a_database_on_read_only_media_opens_and_is_left_as_it_was() {
    use std::os::unix::fs::PermissionsExt;
    for backend in backends() {
        let (path, shm) = fresh(&format!("ro-media-{backend:?}"));
        let dir = path.parent().unwrap().to_path_buf();
        // The writer is a child process that exits without closing: a crash, its commits in
        // the log only, its locks gone with it.
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "--ignored",
                "crashed_writer_child",
                "--nocapture",
            ])
            .env("PHDB_RO_PATH", &path)
            .env("PHDB_RO_SHM", &shm)
            .env("PHDB_RO_BACKEND", format!("{backend:?}"))
            .status()
            .unwrap();
        assert!(status.success(), "{backend:?}: the writer child failed");
        let files: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.is_file())
            .collect();
        assert!(files.len() > 1, "{backend:?}: the log was left: {files:?}");
        let before: Vec<Vec<u8>> = files.iter().map(|f| std::fs::read(f).unwrap()).collect();
        let set = |mode: u32, p: &Path| {
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode)).unwrap()
        };
        for f in &files {
            set(0o444, f);
        }
        set(0o555, &dir);
        let opened = std::panic::catch_unwind(|| {
            let db = Pigeonhole::open(&path, read_only(backend, &shm)).unwrap();
            assert!(db.info().unwrap().read_only);
            assert_eq!(count(&db), 20, "the log replayed into memory");
            let t = db.table("t").unwrap().open().unwrap();
            let e = t.mutate(b"x").put("f", b"q", b"v").commit().unwrap_err();
            assert_eq!(e.code(), ErrorCode::ReadOnly, "{e}");
            drop(t);
            db.close().unwrap();
        });
        set(0o755, &dir);
        for f in &files {
            set(0o644, f);
        }
        if let Err(p) = opened {
            std::panic::resume_unwind(p);
        }
        let after: Vec<Vec<u8>> = files.iter().map(|f| std::fs::read(f).unwrap()).collect();
        assert!(
            before == after,
            "{backend:?}: a read-only open changed the files"
        );
    }
}

/// The writer of [`a_database_on_read_only_media_opens_and_is_left_as_it_was`]: 20 commits,
/// then an exit without a close.
#[cfg(unix)]
#[test]
#[ignore = "run by a_database_on_read_only_media_opens_and_is_left_as_it_was"]
fn crashed_writer_child() {
    let (Some(path), Some(shm), Some(backend)) = (
        std::env::var_os("PHDB_RO_PATH"),
        std::env::var_os("PHDB_RO_SHM"),
        std::env::var("PHDB_RO_BACKEND").ok(),
    ) else {
        return;
    };
    let backend = if backend == "Uring" {
        IoBackend::Uring
    } else {
        IoBackend::Pread
    };
    let db = Pigeonhole::open(&path, writer(backend, Path::new(&shm))).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    for i in 0..20u32 {
        t.mutate(&i.to_be_bytes())
            .put("f", b"q", b"v")
            .commit()
            .unwrap();
    }
    std::process::exit(0);
}

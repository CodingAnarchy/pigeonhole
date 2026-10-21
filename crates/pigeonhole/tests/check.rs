//! `pigeonhole::check` on the real filesystem (ICR 0048): clean on a healthy database,
//! damage found where bytes are flipped, refused while a writer has the database open. The
//! engine's tests cover every structure on the simulated filesystem.

use std::io::{Read, Seek, SeekFrom, Write};

use pigeonhole::{
    CheckOptions, ErrorCode, Family, Options, Pigeonhole, RegionKind, Severity, Structure,
};

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let p =
            std::env::temp_dir().join(format!("pigeonhole-check-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn sample(dir: &TempDir) -> std::path::PathBuf {
    let path = dir.0.join("c.phdb");
    let db = Pigeonhole::open(&path, Options::default().shards(1).pin_threads(false)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default().blob_threshold(100))
        .create()
        .unwrap();
    for i in 0..300u32 {
        let v = if i % 10 == 0 {
            vec![b'b'; 400]
        } else {
            vec![b's'; 20]
        };
        t.mutate(format!("r{i:04}").as_bytes())
            .put("f", b"q", &v)
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    db.close().unwrap();
    path
}

#[test]
fn healthy_then_damaged() {
    let dir = TempDir::new("damage");
    let path = sample(&dir);
    let r = pigeonhole::check(&path, CheckOptions::default().deep(true)).unwrap();
    assert!(r.findings.is_empty(), "{:#?}", r.findings);
    assert!(r.clean_shutdown);
    assert_eq!(r.counts.sst_entries, 300);
    assert_eq!(r.counts.blob_pointers, 30);
    assert!(r.wal.is_empty());

    let sst = r
        .regions
        .iter()
        .find(|g| matches!(g.kind, RegionKind::Sst { .. }))
        .expect("an SST");
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let at = sst.offset + 40;
    let mut b = [0u8];
    f.seek(SeekFrom::Start(at)).unwrap();
    f.read_exact(&mut b).unwrap();
    f.seek(SeekFrom::Start(at)).unwrap();
    f.write_all(&[b[0] ^ 0xff]).unwrap();
    f.sync_all().unwrap();
    drop(f);

    let r = pigeonhole::check(&path, CheckOptions::default()).unwrap();
    assert!(!r.is_ok());
    let f = &r.findings[0];
    assert_eq!(
        (f.structure, f.severity),
        (Structure::Sst, Severity::Corruption),
        "{f:?}"
    );
}

#[test]
fn a_running_writer_refuses_the_check() {
    let dir = TempDir::new("locked");
    let path = sample(&dir);
    let db = Pigeonhole::open(&path, Options::default().shards(1).pin_threads(false)).unwrap();
    let e = pigeonhole::check(&path, CheckOptions::default()).unwrap_err();
    assert_eq!(e.code(), ErrorCode::WriterLocked);
    db.close().unwrap();
    assert!(
        pigeonhole::check(&path, CheckOptions::default())
            .unwrap()
            .is_ok()
    );
}

#[test]
fn a_missing_file_is_an_error() {
    let dir = TempDir::new("missing");
    let e = pigeonhole::check(dir.0.join("nope.phdb"), CheckOptions::default()).unwrap_err();
    assert_eq!(e.code(), ErrorCode::Io);
}

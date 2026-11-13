//! Writes the rollback corpus: databases a 1.2 build left after a crash inside a root
//! commit, then opened, written and closed by 1.1.3 (#1837), which every later 1.x build must
//! reopen with the change feed's history intact (`tests/corpus.rs`,
//! `a_rollback_to_1_1_3_and_back_keeps_the_feed`).
//!
//! `cargo run --release -- <out dir>` writes `<out>/<case>/`, for the four cases 1.1.3 resolves
//! (`landed`, `landed_catalog`, `not_landed`, `not_landed_catalog`): the database's files as
//! 1.1.3 left them. The `feed_tokens` table holds a feed token for `events` taken before its
//! first commit, and the seqnos of the two commits a resumed feed must deliver (`one`, written
//! before the rollback, and `two`, written by 1.1.3).
//!
//! What a 1.2 crash leaves is written into the sidecar by hand, as 1.2 writes it (FORMAT §10;
//! the Catalog entry is a real one from a 1.2.0 sidecar, cli692's corpus): its RootIntent
//! (type 5), and for a catalog change its Catalog entry (type 4), for a root that landed (the
//! intent names the superblock the next open reads, in place of its Root entry) or one that
//! didn't (the next sequence, after the last Root). The session before the crash is written
//! by this build: its Session and Root entries are the same bytes 1.2 writes.

use std::path::Path;

use pigeonhole::{Family, FeedRetention, FeedToken, Options, Pigeonhole};
use pigeonhole_format::feed::{FEED_SUFFIX, FeedEntry, FeedHeader, decode_entries};

/// The database file's name inside an entry.
const DB: &str = "data.phdb";

/// A 1.2.0 Catalog entry, framed (len 94): a CreateTable of `feed_tokens` with one family
/// `t`, from the v1.2.0 corpus's `feed_catalog` sidecar. Its `session` and `sequence` are set
/// for each entry.
const CATALOG_1_2: &str = "5e000000ce78949204847d748c7c55f5c6030000000000000001000000000000000000000000000000020000000110010000000b666565645f746f6b656e730325010000000100000001740103004000000a0000000000000000000000000010000000010000";

fn options() -> Options {
    Options::default()
        .shards(1)
        .memtable_budget(4 << 20)
        .wal_segment_size(256 << 10)
}

/// A framed sidecar entry: `len` u32, CRC32C u32, then the payload.
fn frame(payload: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(8 + payload.len());
    f.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    f.extend_from_slice(&pigeonhole_format::checksum::crc32c(payload).to_le_bytes());
    f.extend_from_slice(payload);
    f
}

fn root_intent(sequence: u64, checksum: u64) -> Vec<u8> {
    let mut p = vec![5u8];
    p.extend_from_slice(&sequence.to_le_bytes());
    p.extend_from_slice(&checksum.to_le_bytes());
    frame(&p)
}

fn catalog(session: u64, sequence: u64) -> Vec<u8> {
    let bytes: Vec<u8> = (0..CATALOG_1_2.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&CATALOG_1_2[i..i + 2], 16).unwrap())
        .collect();
    let mut p = bytes[8..].to_vec();
    assert_eq!(p[0], 4, "a Catalog entry");
    p[1..9].copy_from_slice(&session.to_le_bytes());
    p[9..17].copy_from_slice(&sequence.to_le_bytes());
    frame(&p)
}

fn write(dir: &Path, landed: bool, with_catalog: bool) {
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join(DB);

    // The session before the crash: the feed on, a token for `events` before its first
    // commit, stored, and one commit.
    let db = Pigeonhole::open(
        &path,
        options()
            .create_if_missing(true)
            .change_feed(FeedRetention::bytes(4 << 20)),
    )
    .unwrap();
    let events = db
        .table("events")
        .unwrap()
        .family("e", Family::default())
        .create()
        .unwrap();
    let tokens = db
        .table("feed_tokens")
        .unwrap()
        .family("t", Family::default())
        .create()
        .unwrap();
    let token: FeedToken = events.feed().open().unwrap().token();
    tokens
        .mutate(b"events")
        .put("t", b"token", token.as_bytes())
        .commit()
        .unwrap();
    let one = events
        .mutate(b"r")
        .put("e", b"q", b"one")
        .commit()
        .unwrap()
        .seqno;
    tokens
        .mutate(b"events")
        .put_i64("t", b"one", one as i64)
        .commit()
        .unwrap();
    db.close().unwrap();

    // The 1.2 crash: its intent (and Catalog entry) at the sidecar's end.
    let (sequence, checksum) = {
        let vfs: pigeonhole_io::VfsRef = pigeonhole_io::pread::PreadVfs::new(0);
        let p = pigeonhole_pager::Pager::open(&vfs, &path, false).unwrap();
        (p.sequence(), p.checksum())
    };
    let sidecar = dir.join(format!("{DB}{FEED_SUFFIX}"));
    let mut bytes = std::fs::read(&sidecar).unwrap();
    let (entries, end) = decode_entries(&bytes[FeedHeader::LEN..]);
    assert_eq!(FeedHeader::LEN + end, bytes.len(), "a clean sidecar");
    let Some(&FeedEntry::Root {
        session,
        sequence: last,
    }) = entries.last()
    else {
        panic!("the close's Root entry last: {entries:?}");
    };
    assert_eq!(last, sequence);
    if landed {
        // The close's root landed; its Root entry was never written over its intent.
        bytes.truncate(bytes.len() - 25);
        if with_catalog {
            bytes.extend(catalog(session, sequence));
        }
        bytes.extend(root_intent(sequence, checksum));
    } else {
        if with_catalog {
            bytes.extend(catalog(session, sequence + 1));
        }
        bytes.extend(root_intent(sequence + 1, checksum ^ 1));
    }
    std::fs::write(&sidecar, &bytes).unwrap();

    // 1.1.3: an open that doesn't name the feed (`phdb`, a tool), which keeps it and resolves
    // the intent, one commit, and a clean close.
    let db = Pigeonhole::open(&path, options()).unwrap();
    assert!(db.info().unwrap().feed.is_some(), "1.1.3 kept the feed");
    let events = db.table("events").unwrap().open().unwrap();
    let tokens = db.table("feed_tokens").unwrap().open().unwrap();
    let two = events
        .mutate(b"r")
        .put("e", b"q", b"two")
        .commit()
        .unwrap()
        .seqno;
    tokens
        .mutate(b"events")
        .put_i64("t", b"two", two as i64)
        .commit()
        .unwrap();
    db.close().unwrap();
    let (entries, _) = decode_entries(&std::fs::read(&sidecar).unwrap()[FeedHeader::LEN..]);
    assert!(
        !entries.contains(&FeedEntry::Unknown(5)),
        "1.1.3 resolved the intent: {entries:?}"
    );
}

fn main() {
    let out = std::env::args()
        .nth(1)
        .expect("usage: rollback-gen <out dir>");
    let out = Path::new(&out);
    let mut frozen = vec![
        "# The rollback corpus, written by 1.1.3's code (rollback-gen): path, length, xxh3-64."
            .to_owned(),
        "# Frozen: no file or line ever changes.".to_owned(),
    ];
    for (landed, with_catalog) in [(true, false), (true, true), (false, false), (false, true)] {
        let name = format!(
            "{}{}",
            if landed { "landed" } else { "not_landed" },
            if with_catalog { "_catalog" } else { "" }
        );
        let dir = out.join(&name);
        assert!(
            !dir.exists(),
            "{} exists: write into a new directory",
            dir.display()
        );
        write(&dir, landed, with_catalog);
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|f| f.unwrap().file_name().into_string().unwrap())
            .collect();
        files.sort();
        for f in files {
            let bytes = std::fs::read(dir.join(&f)).unwrap();
            frozen.push(format!(
                "{name}/{f} {} {:016x}",
                bytes.len(),
                pigeonhole_format::checksum::xxh3_64(&bytes)
            ));
        }
        println!("wrote {}", dir.display());
    }
    frozen.push(String::new());
    std::fs::write(out.join("frozen.txt"), frozen.join("\n")).unwrap();
}

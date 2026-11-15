//! `Options::wal_pad_unit` (D327, ICR 0137, #1708) through the public crate (audit120's S1 on
//! #1929): an open with a unit set flags the file with `incompat` bit 1, which stays when a
//! later open doesn't set it; without it the file isn't flagged; and a unit out of range fails
//! the open with `InvalidArgument` before the file is created.

use pigeonhole::{ErrorCode, Options, Pigeonhole};

/// `incompat` bit 1, WAL pad fragments (FORMAT §8.2).
const PAD_BIT: u64 = 1 << 1;

fn options(unit: u32) -> Options {
    Options::default()
        .create_if_missing(true)
        .shards(1)
        .wal_pad_unit(unit)
}

#[test]
fn a_unit_flags_the_file_for_good_and_none_leaves_it_unflagged() {
    let dir = pigeonhole::doc_support::temp_dir();
    let off = dir.join("off.phdb");
    let db = Pigeonhole::open(&off, options(0)).expect("open");
    assert_eq!(db.info().expect("info").features.incompat & PAD_BIT, 0);
    db.close().expect("close");

    let on = dir.join("on.phdb");
    let db = Pigeonhole::open(&on, options(4096)).expect("open");
    assert_eq!(
        db.info().expect("info").features.incompat & PAD_BIT,
        PAD_BIT
    );
    db.close().expect("close");
    // Sticky: an open without the option keeps the flag.
    let db = Pigeonhole::open(&on, options(0)).expect("reopen");
    assert_eq!(
        db.info().expect("info").features.incompat & PAD_BIT,
        PAD_BIT
    );
    db.close().expect("close");
}

#[test]
fn a_unit_out_of_range_fails_the_open_before_the_file_exists() {
    let dir = pigeonhole::doc_support::temp_dir();
    for unit in [100, 16, 65_536] {
        let path = dir.join(format!("bad-{unit}.phdb"));
        match Pigeonhole::open(&path, options(unit)) {
            Err(e) => {
                assert_eq!(e.code(), ErrorCode::InvalidArgument, "{unit}: {e}");
                assert!(e.to_string().contains("pad unit"), "{unit}: {e}");
            }
            Ok(_) => panic!("{unit}: opened"),
        }
        assert!(!path.exists(), "{unit}: the file was created");
    }
}

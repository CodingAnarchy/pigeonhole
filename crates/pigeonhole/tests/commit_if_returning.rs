//! `RowMutation::commit_if_returning` (D314 amendment 2, ICR 0113, #1446): a conditional
//! commit that returns the row's state whichever way the condition goes.

use std::sync::Arc;

use pigeonhole::{Checked, Condition, ErrorCode, Family, Options, Pigeonhole, Table, ValueFilter};
use pigeonhole_io::sim::SimVfs;

fn open(seed: u64) -> (Arc<Pigeonhole>, Table) {
    let vfs = SimVfs::new(seed);
    let db = Arc::new(
        Pigeonhole::open(
            "/db/t.phdb",
            Options::default()
                .vfs(vfs as _)
                .shards(2)
                .memtable_budget(4 << 20)
                .wal_segment_size(256 << 10),
        )
        .unwrap(),
    );
    let t = db
        .table("t")
        .unwrap()
        .family("s", Family::default())
        .family("c", Family::counter())
        .family("b", Family::default().blob_threshold(64))
        .create()
        .unwrap();
    (db, t)
}

fn equals(family: &str, qualifier: &[u8], value: &[u8]) -> Condition {
    Condition::Value {
        family: family.into(),
        qualifier: qualifier.to_vec(),
        filter: ValueFilter::Equals(value.to_vec()),
    }
}

fn value(c: pigeonhole::Result<Option<pigeonhole::Cell>>) -> Option<Vec<u8>> {
    c.unwrap().map(|c| c.value().to_vec())
}

/// Applied: the condition's column as of the commit (the new value), return_previous's
/// old one, return_current's other columns, and the folded counters. NotApplied: the
/// columns as the condition saw them, and nothing written.
#[test]
fn both_outcomes_report_the_row() {
    let (_db, t) = open(1446);
    t.mutate(b"r")
        .put("s", b"state", b"packed")
        .put("s", b"note", b"fragile")
        .incr("c", b"n", 5)
        .commit()
        .unwrap();
    let r = t
        .mutate(b"r")
        .put("s", b"state", b"shipped")
        .incr("c", b"n", 1)
        .return_previous("s", b"state")
        .return_current("s", b"note")
        .commit_if_returning(&equals("s", b"state", b"packed"))
        .unwrap();
    assert!(r.is_applied());
    let Checked::Applied(ret) = r else {
        unreachable!()
    };
    assert_eq!(
        value(ret.current("s", b"state")).as_deref(),
        Some(&b"shipped"[..])
    );
    assert_eq!(
        value(ret.previous("s", b"state")).as_deref(),
        Some(&b"packed"[..])
    );
    assert_eq!(
        value(ret.current("s", b"note")).as_deref(),
        Some(&b"fragile"[..])
    );
    assert_eq!(ret.value("c", b"n").unwrap(), Some(6));
    assert_eq!(
        ret.current("s", b"other").unwrap_err().code(),
        ErrorCode::InvalidArgument
    );
    // Now it fails: "shipped" is not "packed".
    let r = t
        .mutate(b"r")
        .put("s", b"state", b"lost")
        .return_current("s", b"note")
        .commit_if_returning(&equals("s", b"state", b"packed"))
        .unwrap();
    let Checked::NotApplied(seen) = r else {
        panic!("the condition holds")
    };
    assert_eq!(
        value(seen.current("s", b"state")).as_deref(),
        Some(&b"shipped"[..])
    );
    assert_eq!(
        value(seen.current("s", b"note")).as_deref(),
        Some(&b"fragile"[..])
    );
    assert!(seen.seqno() > 0);
    // Nothing written.
    let now = t.get(b"r", "s", b"state").unwrap().unwrap();
    assert_eq!(now.value(), b"shipped");
}

/// A condition that fails on a missing cell reports it missing; an `Absent` condition
/// that holds reports the cell it wrote.
#[test]
fn a_missing_cell_is_reported_missing() {
    let (_db, t) = open(2);
    let Checked::NotApplied(seen) = t
        .mutate(b"r")
        .put("s", b"q", b"v")
        .commit_if_returning(&Condition::Exists {
            family: "s".into(),
            qualifier: b"q".to_vec(),
        })
        .unwrap()
    else {
        panic!("nothing exists")
    };
    assert!(seen.current("s", b"q").unwrap().is_none());
    let Checked::Applied(ret) = t
        .mutate(b"r")
        .put("s", b"q", b"v")
        .commit_if_returning(&Condition::Absent {
            family: "s".into(),
            qualifier: b"q".to_vec(),
        })
        .unwrap()
    else {
        panic!("absent")
    };
    assert_eq!(value(ret.current("s", b"q")).as_deref(), Some(&b"v"[..]));
}

/// A separated value is compared and returned as the value, not its pointer.
#[test]
fn a_separated_value_is_compared_and_returned_as_the_value() {
    let (db, t) = open(3);
    let big = vec![7u8; 5_000];
    t.mutate(b"r").put("b", b"q", &big).commit().unwrap();
    db.flush().unwrap();
    let Checked::NotApplied(seen) = t
        .mutate(b"r")
        .put("b", b"q", b"small")
        .commit_if_returning(&equals("b", b"q", b"other"))
        .unwrap()
    else {
        panic!("it isn't 'other'")
    };
    assert_eq!(value(seen.current("b", b"q")), Some(big.clone()));
    assert!(
        t.mutate(b"r")
            .put("b", b"q", b"small")
            .commit_if_returning(&equals("b", b"q", &big))
            .unwrap()
            .is_applied()
    );
}

/// A compare-and-swap counter under N threads: each attempt swaps from the value it last
/// saw; a failed one retries from the value its `Seen` returns, with no extra read. Every
/// value 0..N*K is taken by exactly one success.
#[test]
fn a_compare_and_swap_loop_hands_out_each_value_once() {
    let (_db, t) = open(4);
    t.mutate(b"seq").put("s", b"v", b"0").commit().unwrap();
    const THREADS: usize = 6;
    const EACH: usize = 40;
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let t = t.clone();
            std::thread::spawn(move || {
                let mut got = Vec::new();
                let mut seen: u64 = 0;
                while got.len() < EACH {
                    let next = (seen + 1).to_string();
                    match t
                        .mutate(b"seq")
                        .put("s", b"v", next.as_bytes())
                        .commit_if_returning(&equals("s", b"v", seen.to_string().as_bytes()))
                        .unwrap()
                    {
                        Checked::Applied(_) => {
                            got.push(seen);
                            seen += 1;
                        }
                        Checked::NotApplied(s) => {
                            let v = value(s.current("s", b"v")).expect("always set");
                            seen = std::str::from_utf8(&v).unwrap().parse().unwrap();
                        }
                    }
                }
                got
            })
        })
        .collect();
    let mut all: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    all.sort_unstable();
    assert_eq!(all, (0..(THREADS * EACH) as u64).collect::<Vec<_>>());
}

/// `return_current` is refused everywhere but `commit_if_returning`, with the transaction
/// hint; an unknown family is `FamilyNotFound`.
#[test]
fn return_current_is_refused_elsewhere() {
    let (_db, t) = open(5);
    let c = || equals("s", b"q", b"x");
    let e = t
        .mutate(b"r")
        .put("s", b"q", b"a")
        .return_current("s", b"q")
        .commit()
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument);
    assert!(e.to_string().contains("use a transaction"), "{e}");
    let e = t
        .mutate(b"r")
        .incr("c", b"n", 1)
        .return_current("s", b"q")
        .commit_returning()
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument);
    let e = t
        .mutate(b"r")
        .put("s", b"q", b"a")
        .return_current("s", b"q")
        .commit_if(&c())
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument);
    assert!(
        t.get(b"r", "s", b"q").unwrap().is_none(),
        "nothing committed"
    );
    let e = t
        .mutate(b"r")
        .put("s", b"q", b"a")
        .return_current("nope", b"q")
        .commit_if_returning(&c())
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::FamilyNotFound);
}

#[cfg(feature = "async")]
#[test]
fn the_async_form_returns_the_same() {
    use pigeonhole::doc_support::block_on;
    let (_db, t) = open(6);
    t.mutate(b"r").put("s", b"q", b"a").commit().unwrap();
    let r = block_on(
        t.mutate(b"r")
            .put("s", b"q", b"b")
            .commit_if_returning_async(&equals("s", b"q", b"a")),
    )
    .unwrap();
    let Checked::Applied(ret) = r else {
        panic!("it was a")
    };
    assert_eq!(value(ret.current("s", b"q")).as_deref(), Some(&b"b"[..]));
    let r = block_on(
        t.mutate(b"r")
            .put("s", b"q", b"c")
            .commit_if_returning_async(&equals("s", b"q", b"a")),
    )
    .unwrap();
    let Checked::NotApplied(seen) = r else {
        panic!("it is b now")
    };
    assert_eq!(value(seen.current("s", b"q")).as_deref(), Some(&b"b"[..]));
}

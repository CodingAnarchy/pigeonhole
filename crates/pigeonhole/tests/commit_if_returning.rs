//! `RowMutation::commit_if_returning` (D314 amendment 2, ICR 0113, #1446): a conditional
//! commit that returns the row's state whichever way the condition goes.

use std::sync::Arc;
use std::time::Duration;

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
/// Runs `condition` as a plain `commit_if` on row `a` and as `commit_if_returning` on row
/// `b`, after `setup` wrote both rows the same way, and checks they agree (simp724's T1 on
/// #1566): the same outcome, and on `NotApplied`, `Seen::current` is the column the
/// condition compared, as a read of the row gives it.
fn agree(
    t: &Table,
    case: &str,
    setup: &dyn Fn(&[u8]),
    condition: &Condition,
    (family, qualifier): (&str, &[u8]),
    write: &str,
    want_applied: bool,
) {
    let a = format!("{case}/a").into_bytes();
    let b = format!("{case}/b").into_bytes();
    setup(&a);
    setup(&b);
    let plain = t
        .mutate(&a)
        .put(write, b"w", b"1")
        .commit_if(condition)
        .unwrap()
        .is_some();
    let read = t
        .get(&b, family, qualifier)
        .unwrap()
        .map(|c| (c.value().to_vec(), c.timestamp()));
    let returning = t
        .mutate(&b)
        .put(write, b"w", b"1")
        .commit_if_returning(condition)
        .unwrap();
    assert_eq!(plain, want_applied, "{case}: commit_if");
    assert_eq!(
        returning.is_applied(),
        plain,
        "{case}: the two forms disagree"
    );
    if let Checked::NotApplied(seen) = returning {
        let saw = seen
            .current(family, qualifier)
            .unwrap()
            .map(|c| (c.value().to_vec(), c.timestamp()));
        assert_eq!(saw, read, "{case}: Seen::current is the compared column");
        assert!(
            t.get(&b, write, b"w").unwrap().is_none(),
            "{case}: nothing written"
        );
    }
}

/// A separated value and a counter column: `commit_if` and `commit_if_returning` agree,
/// and a failed condition reports the value it compared (the blob's bytes, the folded sum).
#[test]
fn both_forms_agree_on_blobs_and_counters() {
    let (_db, t) = open(1566);
    let big = vec![b'x'; 300];
    let blob = |row: &[u8]| {
        t.mutate(row).put("b", b"q", &big).commit().unwrap();
    };
    agree(
        &t,
        "blob-eq",
        &blob,
        &equals("b", b"q", &big),
        ("b", b"q"),
        "s",
        true,
    );
    agree(
        &t,
        "blob-ne",
        &blob,
        &equals("b", b"q", b"small"),
        ("b", b"q"),
        "s",
        false,
    );
    let mut other = big.clone();
    other[299] = b'y';
    agree(
        &t,
        "blob-last",
        &blob,
        &equals("b", b"q", &other),
        ("b", b"q"),
        "s",
        false,
    );

    // Two commits, so the column holds operands the read folds.
    let counter = |row: &[u8]| {
        t.mutate(row).incr("c", b"n", 5).commit().unwrap();
        t.mutate(row).incr("c", b"n", 2).commit().unwrap();
    };
    counter(b"probe");
    let folded = t
        .get(b"probe", "c", b"n")
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    agree(
        &t,
        "ctr-eq",
        &counter,
        &equals("c", b"n", &folded),
        ("c", b"n"),
        "s",
        true,
    );
    let mut five = folded.clone();
    five[0] = 5;
    agree(
        &t,
        "ctr-ne",
        &counter,
        &equals("c", b"n", &five),
        ("c", b"n"),
        "s",
        false,
    );
    let exists = Condition::Exists {
        family: "c".into(),
        qualifier: b"n".to_vec(),
    };
    agree(&t, "ctr-exists", &counter, &exists, ("c", b"n"), "s", true);
}

/// A column at the family's TTL: a version past it is missing to both forms, and a column
/// whose old version expired is compared (and reported) at its live one.
#[test]
fn both_forms_agree_at_the_ttl() {
    // A real clock: expiry is against wall-clock microseconds.
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("ttl.phdb"), Options::default()).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("e", Family::default().ttl(Duration::from_secs(3_600)))
        .family("s", Family::default())
        .create()
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    let hour = 3_600_000_000u64;
    let rows = |row: &[u8]| {
        t.mutate(row)
            .put_at("e", b"dead", now - hour - hour / 10, b"gone")
            .put_at("e", b"live", now - hour - hour / 10, b"old")
            .put_at("e", b"live", now - hour + hour / 10, b"new")
            .commit()
            .unwrap();
    };
    let exists = |q: &[u8]| Condition::Exists {
        family: "e".into(),
        qualifier: q.to_vec(),
    };
    let absent = Condition::Absent {
        family: "e".into(),
        qualifier: b"dead".to_vec(),
    };
    agree(
        &t,
        "dead-exists",
        &rows,
        &exists(b"dead"),
        ("e", b"dead"),
        "s",
        false,
    );
    agree(&t, "dead-absent", &rows, &absent, ("e", b"dead"), "s", true);
    agree(
        &t,
        "dead-eq",
        &rows,
        &equals("e", b"dead", b"gone"),
        ("e", b"dead"),
        "s",
        false,
    );
    agree(
        &t,
        "live-new",
        &rows,
        &equals("e", b"live", b"new"),
        ("e", b"live"),
        "s",
        true,
    );
    agree(
        &t,
        "live-old",
        &rows,
        &equals("e", b"live", b"old"),
        ("e", b"live"),
        "s",
        false,
    );
    agree(
        &t,
        "live-exists",
        &rows,
        &exists(b"live"),
        ("e", b"live"),
        "s",
        true,
    );
}

/// A mutation that writes nothing is a check-and-read in both forms (cli692's Q1 on #1566):
/// the same outcome as plain `commit_if`, and the columns as they are.
#[test]
fn an_empty_mutation_is_a_check_and_read() {
    let (_db, t) = open(1567);
    for row in [&b"e/a"[..], b"e/b"] {
        t.mutate(row).put("s", b"state", b"paid").commit().unwrap();
    }
    for (cond, want) in [(&b"paid"[..], true), (b"packed", false)] {
        let plain = t
            .mutate(b"e/a")
            .commit_if(&equals("s", b"state", cond))
            .unwrap();
        assert_eq!(plain.is_some(), want, "commit_if on {cond:?}");
        let r = t
            .mutate(b"e/b")
            .commit_if_returning(&equals("s", b"state", cond))
            .unwrap();
        assert_eq!(r.is_applied(), want, "commit_if_returning on {cond:?}");
        let state = match &r {
            Checked::Applied(ret) => value(ret.current("s", b"state")),
            Checked::NotApplied(seen) => value(seen.current("s", b"state")),
        };
        assert_eq!(state.as_deref(), Some(&b"paid"[..]));
    }
}

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

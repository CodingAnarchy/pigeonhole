//! D299 (ICR 0097): a multi-get (`Table::keys`) returns exactly what one `RowRead::read` per
//! key returns under the same snapshot, in key order, skipping keys with no row: across a
//! memtable and several SSTs and levels (whose row filters prune per key), deletes of every
//! scope, versions, several families, selections, and keys spread over several tablets. The
//! model target (`tests/model.rs`) checks the same under faults and concurrent changes.

use pigeonhole::{Family, KeySet, Options, Pigeonhole, Snapshot, Table};

type Cells = Vec<(String, Vec<u8>, u64, Vec<u8>)>;

fn cells(row: &pigeonhole::RowRef<'_>) -> Cells {
    row.iter()
        .map(|e| {
            (
                e.family.to_owned(),
                e.qualifier.to_vec(),
                e.cell.timestamp(),
                e.cell.value().to_vec(),
            )
        })
        .collect()
}

fn key(i: u32) -> Vec<u8> {
    format!("r{i:05}").into_bytes()
}

/// One `read()` per key, as `keys().read()` should return them.
fn per_key(
    t: &Table,
    keys: &KeySet,
    s: &Snapshot,
    versions: u32,
    family: Option<&str>,
) -> Vec<(usize, Cells)> {
    let mut out = Vec::new();
    for (i, k) in keys.iter().enumerate() {
        let mut r = t.row(k).snapshot(s).versions(versions);
        if let Some(f) = family {
            r = r.family(f);
        }
        if let Some(row) = r.read().expect("read") {
            out.push((i, cells(&row)));
        }
    }
    out
}

fn multi(
    t: &Table,
    keys: &KeySet,
    s: &Snapshot,
    versions: u32,
    family: Option<&str>,
) -> Vec<(usize, Cells)> {
    let mut r = t.keys(keys).snapshot(s).versions(versions);
    if let Some(f) = family {
        r = r.family(f);
    }
    let mut rows = r.read().expect("keys");
    let mut out = Vec::new();
    while let Some((i, row)) = rows.next().expect("next") {
        out.push((i, cells(&row)));
    }
    out
}

/// A table of 1,500 rows over two families, written in four rounds with a flush after each of
/// the first three (so level 0 holds several SSTs and the memtable the last round), with
/// overwrites, versions and deletes of every scope.
fn build(db: &Pigeonhole, t: &Table) {
    for round in 0u32..4 {
        for i in (round..1_500).step_by(3) {
            let mut m = t.mutate(&key(i));
            m = m
                .put_at(
                    "a",
                    b"q",
                    u64::from(round + 1) * 10,
                    format!("a{i}@{round}").as_bytes(),
                )
                .put("b", format!("x{}", i % 7).as_bytes(), &[round as u8; 9]);
            if i % 11 == round {
                m = m.delete_column("a", b"q");
            }
            if i % 13 == round {
                m = m.delete_family("b");
            }
            if i % 29 == round {
                m = m.delete_row();
            }
            m.commit().expect("commit");
        }
        if round < 3 {
            db.flush().expect("flush");
        }
    }
}

fn open() -> (pigeonhole::doc_support::TempDir, Pigeonhole, Table) {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("k.phdb"), Options::default().shards(2)).expect("open");
    let t = db
        .table("t")
        .expect("table")
        .family("a", Family::default().max_versions(0))
        .family("b", Family::default())
        .create_if_missing()
        .expect("create");
    (dir, db, t)
}

#[test]
fn a_multi_get_returns_what_a_read_per_key_returns() {
    let (_dir, db, t) = open();
    build(&db, &t);
    let s = db.snapshot().expect("snapshot");
    // Writes after the snapshot are not seen.
    t.mutate(&key(3))
        .put("a", b"late", b"x")
        .commit()
        .expect("commit");
    let sets = [
        // Dense, sparse, with absent keys, one key.
        KeySet::from_unsorted((100..400).map(key)),
        KeySet::from_unsorted((0..1_500).step_by(97).map(key)),
        KeySet::from_unsorted((0..50).map(|i| key(i * 30)).chain([
            b"zz".to_vec(),
            b"a".to_vec(),
            key(99_999),
        ])),
        KeySet::from_unsorted([key(42)]),
    ];
    for (n, keys) in sets.iter().enumerate() {
        for versions in [1u32, 0] {
            for family in [None, Some("a"), Some("b")] {
                let want = per_key(&t, keys, &s, versions, family);
                let got = multi(&t, keys, &s, versions, family);
                assert_eq!(got, want, "set {n}, versions {versions}, family {family:?}");
            }
        }
    }
    db.close().expect("close");
}

#[test]
fn a_multi_get_spans_tablets_and_compactions() {
    let (_dir, db, t) = open();
    build(&db, &t);
    // Several tablets on both shards, then a compaction: deeper levels with several SSTs.
    t.split_over_shards((1..8).map(|i| key(i * 187)).collect())
        .expect("split");
    db.compact().expect("compact");
    t.mutate(&key(10))
        .put("b", b"new", b"after")
        .commit()
        .expect("commit");
    let s = db.snapshot().expect("snapshot");
    let keys = KeySet::from_unsorted((0..1_500).step_by(7).map(key));
    assert_eq!(
        multi(&t, &keys, &s, 0, None),
        per_key(&t, &keys, &s, 0, None)
    );
    db.close().expect("close");
}

#[test]
fn selections_apply_to_every_row() {
    let (_dir, db, t) = open();
    build(&db, &t);
    let s = db.snapshot().expect("snapshot");
    let keys = KeySet::from_unsorted((0..600).map(key));
    let read_each =
        |f: &dyn Fn(pigeonhole::RowRead<'_>) -> pigeonhole::RowRead<'_>| -> Vec<(usize, Cells)> {
            keys.iter()
                .enumerate()
                .filter_map(|(i, k)| {
                    f(t.row(k).snapshot(&s))
                        .read()
                        .expect("read")
                        .map(|r| (i, cells(&r)))
                })
                .collect()
        };
    let read_keys =
        |f: &dyn Fn(pigeonhole::KeysRead<'_>) -> pigeonhole::KeysRead<'_>| -> Vec<(usize, Cells)> {
            let mut rows = f(t.keys(&keys).snapshot(&s)).read().expect("keys");
            let mut out = Vec::new();
            while let Some((i, r)) = rows.next().expect("next") {
                out.push((i, cells(&r)));
            }
            out
        };
    assert_eq!(
        read_keys(&|r| r.family("b").qualifier_prefix(b"x3")),
        read_each(&|r| r.family("b").qualifier_prefix(b"x3"))
    );
    assert_eq!(
        read_keys(&|r| r.column_limit(1)),
        read_each(&|r| r.column_limit(1))
    );
    assert_eq!(
        read_keys(&|r| r.versions(0).time_range(15..35)),
        read_each(&|r| r.versions(0).time_range(15..35))
    );
    db.close().expect("close");
}

#[test]
fn key_sets_sort_dedupe_and_check() {
    let k = KeySet::from_unsorted([&b"b"[..], b"a", b"b", b""]);
    assert_eq!(k.iter().collect::<Vec<_>>(), [&b""[..], b"a", b"b"]);
    assert!(KeySet::from_sorted([&b"a"[..], b"a"]).is_err());
    assert!(KeySet::from_sorted([&b"a"[..], b"b"]).is_ok());
    assert!(KeySet::from_unsorted(Vec::<Vec<u8>>::new()).is_empty());
}

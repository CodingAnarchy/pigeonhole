//! D297 (ICR 0095): walking a row cell by cell (`RowRead::cells`), in pages
//! (`RowRead::pages`), resumed by position (`RowRead::resume_after`), and a scan's cells
//! (`Scan::cells`). Each walk returns exactly what `RowRead::read` (or `Scan::iter`) returns,
//! in the same order. The model target (`tests/model.rs`) checks the same under faults,
//! flushes, compactions and tablet changes.

use pigeonhole::{ErrorCode, Family, Options, Pigeonhole, RowPosition, Table};

type Cells = Vec<(String, Vec<u8>, u64, Vec<u8>)>;

fn open(families: &[&str]) -> (pigeonhole::doc_support::TempDir, Pigeonhole, Table) {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("w.phdb"), Options::default().shards(1)).expect("open");
    let mut b = db.table("t").expect("table");
    for f in families {
        b = b.family(f, Family::default().max_versions(0));
    }
    let t = b.create_if_missing().expect("create");
    (dir, db, t)
}

fn read_all(t: &Table, row: &[u8], versions: u32) -> Cells {
    t.row(row)
        .versions(versions)
        .read()
        .expect("read")
        .map(|r| {
            r.iter()
                .map(|e| {
                    let c = &e.cell;
                    (
                        e.family.to_owned(),
                        e.qualifier.to_vec(),
                        c.timestamp(),
                        c.value().to_vec(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn pages_of(t: &Table, row: &[u8], versions: u32, n: usize) -> Vec<Cells> {
    let mut out = Vec::new();
    let mut pages = t.row(row).versions(versions).pages(n).expect("pages");
    while let Some(p) = pages.next().expect("next") {
        out.push(
            p.iter()
                .map(|e| {
                    let c = &e.cell;
                    (
                        e.family.to_owned(),
                        e.qualifier.to_vec(),
                        c.timestamp(),
                        c.value().to_vec(),
                    )
                })
                .collect(),
        );
    }
    out
}

/// Three families of five columns, three versions each.
fn fill(t: &Table, row: &[u8]) {
    for ts in [10u64, 20, 30] {
        let mut m = t.mutate(row);
        for f in ["a", "b", "c"] {
            for q in 0u8..5 {
                m = m.put_at(f, &[q], ts, format!("{f}{q}@{ts}").as_bytes());
            }
        }
        m.commit().expect("commit");
    }
}

#[test]
fn cells_and_pages_return_what_read_returns() {
    let (_dir, db, t) = open(&["a", "b", "c"]);
    fill(&t, b"r");
    for versions in [0u32, 1, 2] {
        let want = read_all(&t, b"r", versions);
        let mut got = Vec::new();
        let mut cells = t.row(b"r").versions(versions).cells().expect("cells");
        while let Some(e) = cells.next().expect("next") {
            got.push((
                e.family.to_owned(),
                e.qualifier.to_vec(),
                e.cell.timestamp(),
                e.cell.value().to_vec(),
            ));
        }
        assert_eq!(got, want, "cells, versions {versions}");
        for n in [1usize, 2, 3, 4, 7, 100] {
            let pages = pages_of(&t, b"r", versions, n);
            assert_eq!(pages.concat(), want, "pages({n}), versions {versions}");
        }
    }
    db.close().expect("close");
}

#[test]
fn a_page_ends_only_at_a_column_boundary() {
    let (_dir, db, t) = open(&["a", "b", "c"]);
    fill(&t, b"r");
    // Fifteen columns of three versions each: pages hold whole columns, at most n cells
    // unless one column alone is more. Pages of two and four hold one column (three
    // cells), pages of six two columns (the last page one).
    for (n, size) in [(2usize, 3usize), (4, 3), (6, 6)] {
        let pages = pages_of(&t, b"r", 0, n);
        for (i, p) in pages.iter().enumerate() {
            let last = i + 1 == pages.len();
            assert!(
                p.len() == size || (last && p.len() < size),
                "pages({n}): {}",
                p.len()
            );
            assert!(
                p.chunks(3)
                    .all(|c| c.iter().all(|x| (&x.0, &x.1) == (&c[0].0, &c[0].1)))
            );
        }
        assert_eq!(pages.iter().map(Vec::len).sum::<usize>(), 45);
    }
    db.close().expect("close");
}

#[test]
fn a_chain_of_resumed_pages_is_the_whole_row_across_families() {
    let (_dir, db, t) = open(&["a", "b", "c"]);
    fill(&t, b"r");
    let snapshot = db.snapshot().expect("snapshot");
    // Writes after the snapshot are not seen by the chain.
    t.mutate(b"r")
        .put_at("b", &[9], 40, b"late")
        .commit()
        .expect("commit");
    let want: Cells = {
        let r = t
            .row(b"r")
            .versions(0)
            .snapshot(&snapshot)
            .read()
            .expect("read")
            .expect("row");
        r.iter()
            .map(|e| {
                (
                    e.family.to_owned(),
                    e.qualifier.to_vec(),
                    e.cell.timestamp(),
                    e.cell.value().to_vec(),
                )
            })
            .collect()
    };
    for n in [1usize, 4, 10] {
        let mut got = Vec::new();
        let mut at: Option<RowPosition> = None;
        loop {
            let mut r = t.row(b"r").versions(0).snapshot(&snapshot);
            if let Some(p) = &at {
                r = r.resume_after(p);
            }
            let mut pages = r.pages(n).expect("pages");
            let Some(page) = pages.next().expect("next") else {
                break;
            };
            got.extend(page.iter().map(|e| {
                (
                    e.family.to_owned(),
                    e.qualifier.to_vec(),
                    e.cell.timestamp(),
                    e.cell.value().to_vec(),
                )
            }));
            at = pages.position();
        }
        assert_eq!(got, want, "pages({n}) resumed");
    }
    // `read` resumes too: after column a/4, the row is b's and c's columns.
    let at = {
        let mut pages = t
            .row(b"r")
            .versions(0)
            .snapshot(&snapshot)
            .pages(15)
            .expect("pages");
        pages.next().expect("next").expect("page");
        pages.position().expect("a position")
    };
    let rest = t
        .row(b"r")
        .versions(0)
        .snapshot(&snapshot)
        .resume_after(&at)
        .read()
        .expect("read")
        .expect("row");
    assert_eq!(rest.len(), 30);
    assert_eq!(rest.entry(0).expect("cell").family, "b");
    db.close().expect("close");
}

#[test]
fn resume_respects_the_qualifier_selection_and_the_family_order() {
    let (_dir, db, t) = open(&["a", "b"]);
    let mut m = t.mutate(b"r");
    for q in [&b"x1"[..], b"x2", b"x3", b"y1"] {
        m = m.put("a", q, b"").put("b", q, b"");
    }
    m.commit().expect("commit");
    let qualifiers = |r: pigeonhole::RowRead<'_>| -> Vec<(String, Vec<u8>)> {
        r.read()
            .expect("read")
            .map(|r| {
                r.iter()
                    .map(|e| (e.family.to_owned(), e.qualifier.to_vec()))
                    .collect()
            })
            .unwrap_or_default()
    };
    // A position in family a, under a prefix: a's rest of the prefix, then b's whole prefix.
    let at = {
        let mut pages = t.row(b"r").qualifier_prefix(b"x").pages(1).expect("pages");
        pages.next().expect("next");
        pages.position().expect("position")
    };
    assert_eq!(
        qualifiers(t.row(b"r").qualifier_prefix(b"x").resume_after(&at)),
        [
            ("a", &b"x2"[..]),
            ("a", b"x3"),
            ("b", b"x1"),
            ("b", b"x2"),
            ("b", b"x3")
        ]
        .map(|(f, q)| (f.to_owned(), q.to_vec()))
    );
    // Families read in the order named: b first, then a; a position in b goes on to a.
    let at = {
        let mut pages = t.row(b"r").families(["b", "a"]).pages(4).expect("pages");
        pages.next().expect("next");
        pages.position().expect("position")
    };
    let rest = qualifiers(t.row(b"r").families(["b", "a"]).resume_after(&at));
    assert_eq!(rest.len(), 4);
    assert!(rest.iter().all(|(f, _)| f == "a"));
    db.close().expect("close");
}

#[test]
fn column_limit_caps_the_walk_and_a_page_never_splits_its_last_column() {
    let (_dir, db, t) = open(&["a", "b", "c"]);
    fill(&t, b"r");
    let mut pages = t
        .row(b"r")
        .family("a")
        .versions(0)
        .column_limit(2)
        .pages(4)
        .expect("pages");
    let mut sizes = Vec::new();
    while let Some(p) = pages.next().expect("next") {
        sizes.push(p.len());
    }
    // Two columns of three versions: one column a page (a second would pass 4 cells).
    assert_eq!(sizes, [3, 3]);
    db.close().expect("close");
}

#[test]
fn a_position_from_another_table_is_refused() {
    let (_dir, db, t) = open(&["a"]);
    t.mutate(b"r")
        .put("a", b"q", b"v")
        .commit()
        .expect("commit");
    let other = db
        .table("u")
        .expect("table")
        .family("a", Family::default())
        .create_if_missing()
        .expect("create");
    other
        .mutate(b"r")
        .put("a", b"q", b"v")
        .commit()
        .expect("commit");
    let at = {
        let mut pages = other.row(b"r").pages(1).expect("pages");
        pages.next().expect("next");
        pages.position().expect("position")
    };
    let err = t
        .row(b"r")
        .resume_after(&at)
        .cells()
        .expect_err("another table's position");
    assert_eq!(err.code(), ErrorCode::InvalidArgument);
    assert!(RowPosition::from_bytes(&[2, 0]).is_err());
    db.close().expect("close");
}

/// The token's byte form is stable (ICR 0095): byte 0 the version (1), the table and family
/// ids (u32 LE), the qualifier length-prefixed. Golden bytes for a fresh database's first
/// table and family.
#[test]
fn the_position_token_is_stable() {
    let (_dir, db, t) = open(&["a"]);
    t.mutate(b"r")
        .put("a", b"q:7", b"v")
        .commit()
        .expect("commit");
    let mut pages = t.row(b"r").pages(1).expect("pages");
    pages.next().expect("next");
    let token = pages.position().expect("position").to_bytes();
    let info = t.family_info("a").expect("family");
    let _ = info;
    assert_eq!(token[0], 1, "version");
    assert_eq!(
        &token[9..],
        &[3, b'q', b':', b'7'],
        "length-prefixed qualifier"
    );
    assert_eq!(
        token, GOLDEN_TOKEN,
        "the first table's first family, column q:7"
    );
    assert_eq!(
        RowPosition::from_bytes(&token).expect("decode").to_bytes(),
        token
    );
    db.close().expect("close");
}

const GOLDEN_TOKEN: [u8; 13] = [1, 1, 0, 0, 0, 1, 0, 0, 0, 3, b'q', b':', b'7'];

#[test]
fn scan_cells_return_what_the_scan_returns() {
    let (_dir, db, t) = open(&["a", "b"]);
    for r in 0u8..20 {
        let mut m = t.mutate(&[b'r', r]);
        for q in 0u8..(r % 7) {
            // Some values past the inline size, which stay pinned.
            let v = vec![r; if q % 3 == 0 { 300 } else { 5 }];
            m = m.put("a", &[q], &v).put("b", &[q], b"x");
        }
        m.commit().expect("commit");
    }
    db.flush().expect("flush");
    let mut want = Vec::new();
    let mut it = t.scan_prefix(b"r").iter().expect("iter");
    while let Some(row) = it.next_ref().expect("next") {
        for e in row.iter() {
            want.push((
                row.key().to_vec(),
                e.family.to_owned(),
                e.qualifier.to_vec(),
                e.cell.value().to_vec(),
            ));
        }
    }
    let mut got = Vec::new();
    let mut cells = t.scan_prefix(b"r").cells().expect("cells");
    while let Some((row, e)) = cells.next().expect("next") {
        got.push((
            row.to_vec(),
            e.family.to_owned(),
            e.qualifier.to_vec(),
            e.cell.value().to_vec(),
        ));
    }
    assert_eq!(got, want);
    assert!(!got.is_empty());
    db.close().expect("close");
}

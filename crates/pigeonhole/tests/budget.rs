//! D308 (ICR 0103): byte-budgeted reads through the public API. A page is the longest prefix
//! of the read's cells, whole columns, whose size (each cell's qualifier, value and 16 bytes,
//! each row's key once) fits; pages chained under one snapshot give every cell exactly once;
//! and the pinned rules hold: `limit` with the budget, an empty cut page, positions clamped
//! to the range, refusals, a rebuilt key set.

use pigeonhole::{
    Element, ErrorCode, Family, KeySet, KeysPosition, Metric, Options, Pigeonhole, Row,
    ScanPosition, Snapshot, Table, VectorSpec,
};

type Cell = (Vec<u8>, String, Vec<u8>, u64, Vec<u8>);

fn db(name: &str) -> (Pigeonhole, Table) {
    let dir = std::env::temp_dir().join(format!("phdb-budget-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = Pigeonhole::open(
        dir.join("b.phdb"),
        Options::default().shards(2).pin_threads(false),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .family("g", Family::default())
        .create_if_missing()
        .unwrap();
    // 60 rows of 1 to 6 columns in two families, values of varied length; some columns
    // with two versions; a flush in the middle, so pages read memtables and SSTs.
    for i in 0..60u32 {
        let row = format!("r{i:03}");
        let mut m = t.mutate(row.as_bytes());
        for c in 0..(1 + i % 6) {
            let fam = if c % 2 == 0 { "f" } else { "g" };
            let v = vec![b'a' + (c as u8); (5 + (i * 7 + c * 13) % 40) as usize];
            m = m.put_at(fam, format!("q{c}").as_bytes(), 10, &v);
            if i % 4 == 0 {
                m = m.put_at(fam, format!("q{c}").as_bytes(), 20, b"newer");
            }
        }
        m.commit().unwrap();
        if i == 30 {
            db.flush().unwrap();
        }
    }
    (db, t)
}

fn cells_of(rows: &[Row]) -> Vec<Cell> {
    rows.iter()
        .flat_map(|r| {
            r.view()
                .iter()
                .map(|e| {
                    (
                        r.key().to_vec(),
                        e.family.to_owned(),
                        e.qualifier.to_vec(),
                        e.cell.timestamp(),
                        e.cell.value().to_vec(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn whole_scan(t: &Table, versions: u32, snap: &Snapshot) -> Vec<Cell> {
    let rows: Vec<Row> = t
        .scan_prefix(b"r")
        .versions(versions)
        .snapshot(snap)
        .iter()
        .unwrap()
        .map(Result::unwrap)
        .collect();
    cells_of(&rows)
}

/// The in-budget prefix of `cells`, cut by hand: whole columns while they fit, each row's key
/// once; with its size and the first left-out column's size (its row key included).
fn by_hand(cells: &[Cell], budget: u64) -> (Vec<Cell>, u64, Option<u64>) {
    let own = |c: &Cell| c.2.len() as u64 + c.4.len() as u64 + 16;
    let (mut used, mut keep, mut i) = (0u64, 0usize, 0usize);
    while i < cells.len() {
        let mut j = i + 1;
        while j < cells.len()
            && (&cells[j].0, &cells[j].1, &cells[j].2) == (&cells[i].0, &cells[i].1, &cells[i].2)
        {
            j += 1;
        }
        let col: u64 = cells[i..j].iter().map(own).sum();
        let row = cells[i].0.len() as u64;
        let charge = if keep == 0 || cells[keep - 1].0 != cells[i].0 {
            row
        } else {
            0
        };
        if used + col + charge > budget {
            return (cells[..keep].to_vec(), used, Some(col + row));
        }
        used += col + charge;
        keep = j;
        i = j;
    }
    (cells.to_vec(), used, None)
}

#[test]
fn a_page_is_the_hand_cut_prefix_at_any_budget() {
    let (db, t) = db("prefix");
    let snap = db.snapshot().unwrap();
    for versions in [1, 2] {
        let cells = whole_scan(&t, versions, &snap);
        for budget in [0, 1, 40, 100, 333, 1000, 2500, 1 << 30] {
            let page = t
                .scan_prefix(b"r")
                .versions(versions)
                .snapshot(&snap)
                .read_within(budget)
                .unwrap();
            let (want, bytes, next) = by_hand(&cells, budget);
            assert_eq!(
                cells_of(page.rows()),
                want,
                "versions {versions}, budget {budget}"
            );
            assert_eq!(page.bytes(), bytes, "versions {versions}, budget {budget}");
            assert!(page.bytes() <= budget);
            assert_eq!(
                page.next_bytes(),
                next,
                "versions {versions}, budget {budget}"
            );
            assert_eq!(page.cut_by_budget(), next.is_some());
            assert_eq!(page.resume().is_some(), next.is_some(), "budget {budget}");
        }
    }
    db.close().unwrap();
}

#[test]
fn chained_pages_under_one_snapshot_give_every_cell_once() {
    let (db, t) = db("chain");
    let snap = db.snapshot().unwrap();
    let cells = whole_scan(&t, 2, &snap);
    for budget in [80, 150, 600] {
        let mut got = Vec::new();
        let mut at: Option<ScanPosition> = None;
        let mut pages = 0;
        let mut next = budget;
        loop {
            let mut scan = t.scan_prefix(b"r").versions(2).snapshot(&snap);
            if let Some(p) = &at {
                // A position survives its byte form.
                scan = scan.resume_after(&ScanPosition::from_bytes(&p.to_bytes()).unwrap());
            }
            let page = scan.read_within(next).unwrap();
            got.extend(cells_of(page.rows()));
            pages += 1;
            // An empty cut page resumes at its own start: retry with what it needs.
            next = match page.next_bytes() {
                Some(need) if page.rows().is_empty() => need,
                _ => budget,
            };
            match page.resume() {
                Some(p) => at = Some(p.clone()),
                None => break,
            }
            assert!(pages < 10_000, "budget {budget}: pages never end");
        }
        assert_eq!(got, cells, "budget {budget}");
        assert!(pages > 2, "budget {budget}: {pages} pages");
    }
    db.close().unwrap();
}

#[test]
fn an_empty_cut_page_resumes_at_its_start_with_next_bytes() {
    let (db, t) = db("empty");
    let snap = db.snapshot().unwrap();
    let first = t.scan_prefix(b"r").snapshot(&snap).read_within(1).unwrap();
    assert!(first.rows().is_empty() && first.cut_by_budget());
    let need = first.next_bytes().unwrap();
    let retry = t
        .scan_prefix(b"r")
        .snapshot(&snap)
        .resume_after(first.resume().expect("an empty cut page resumes"))
        .read_within(need)
        .unwrap();
    let cells = whole_scan(&t, 1, &snap);
    assert_eq!(cells_of(retry.rows()), cells[..1].to_vec());
    // Mid-read too: the page after a cut, given too little, resumes where it was given.
    let a = t
        .scan_prefix(b"r")
        .snapshot(&snap)
        .read_within(300)
        .unwrap();
    let pos = a.resume().unwrap().clone();
    let b = t
        .scan_prefix(b"r")
        .snapshot(&snap)
        .resume_after(&pos)
        .read_within(1)
        .unwrap();
    assert!(b.rows().is_empty());
    assert_eq!(b.resume(), Some(&pos));
    let c = t
        .scan_prefix(b"r")
        .snapshot(&snap)
        .resume_after(&pos)
        .read_within(b.next_bytes().unwrap())
        .unwrap();
    let after_a = cells_of(a.rows()).len();
    assert_eq!(
        cells_of(c.rows()),
        cells[after_a..after_a + cells_of(c.rows()).len()].to_vec()
    );
    assert!(!c.rows().is_empty());
    db.close().unwrap();
}

#[test]
fn limit_and_the_budget_apply_and_whichever_stops_first_wins() {
    let (db, t) = db("limit");
    let snap = db.snapshot().unwrap();
    // The limit first: 3 rows of a large budget.
    let p = t
        .scan_prefix(b"r")
        .snapshot(&snap)
        .limit(3)
        .read_within(1 << 30)
        .unwrap();
    assert_eq!(p.rows().len(), 3);
    assert!(!p.cut_by_budget() && p.next_bytes().is_none());
    assert_eq!(p.resume().unwrap().row(), b"r002");
    let rest = t
        .scan_prefix(b"r")
        .snapshot(&snap)
        .resume_after(p.resume().unwrap())
        .read_within(1 << 30)
        .unwrap();
    assert_eq!(rest.rows()[0].key(), b"r003");
    assert!(rest.resume().is_none(), "exhausted");
    // The budget first.
    let p = t
        .scan_prefix(b"r")
        .snapshot(&snap)
        .limit(50)
        .read_within(200)
        .unwrap();
    assert!(p.cut_by_budget() && p.rows().len() < 50);
    // limit(0) reads nothing.
    let p = t.scan_prefix(b"r").limit(0).read_within(1 << 30).unwrap();
    assert!(p.rows().is_empty() && p.resume().is_none());
    db.close().unwrap();
}

#[test]
fn a_position_outside_the_range_clamps() {
    let (db, t) = db("clamp");
    let snap = db.snapshot().unwrap();
    // A position in r005 (before the range r020..r030) starts at the range.
    let early = t
        .scan_prefix(b"r00")
        .snapshot(&snap)
        .read_within(200)
        .unwrap();
    let pos = early.resume().unwrap().clone();
    let page = t
        .scan(&b"r020"[..]..&b"r030"[..])
        .snapshot(&snap)
        .resume_after(&pos)
        .read_within(1 << 30)
        .unwrap();
    assert_eq!(page.rows()[0].key(), b"r020");
    assert_eq!(page.rows().len(), 10);
    // One past the range's end reads nothing, and there is no more to read.
    let late = t
        .scan_prefix(b"r05")
        .snapshot(&snap)
        .read_within(200)
        .unwrap();
    let page = t
        .scan(&b"r020"[..]..&b"r030"[..])
        .snapshot(&snap)
        .resume_after(late.resume().unwrap())
        .read_within(1 << 30)
        .unwrap();
    assert!(page.rows().is_empty() && page.resume().is_none());
    db.close().unwrap();
}

#[test]
fn bad_positions_are_refused() {
    let (db, t) = db("refuse");
    let other = db
        .table("u")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    other
        .mutate(b"r000")
        .put("f", b"q0", b"v")
        .commit()
        .unwrap();
    let invalid = |r: pigeonhole::Result<_>| match r {
        Err(e) => assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}"),
        Ok(_) => panic!("accepted"),
    };
    // A mid-row position in family g (a cut inside r005's row, which has three g columns).
    let mid = (1..400)
        .map(|b| t.scan_prefix(b"r005").family("g").read_within(b).unwrap())
        .find_map(|p| {
            p.resume()
                .filter(|_| !p.rows().is_empty() && p.next_bytes().is_some())
                .filter(|r| r.row() == b"r005")
                .cloned()
        })
        .expect("a budget that cuts r005 inside its row");
    // Another table's position, and one whose family the read doesn't select.
    invalid(
        other
            .scan_prefix(b"r")
            .resume_after(&mid)
            .read_within(1 << 20)
            .map(|_| ()),
    );
    invalid(
        t.scan_prefix(b"r")
            .family("f")
            .resume_after(&mid)
            .read_within(1 << 20)
            .map(|_| ()),
    );
    invalid(t.scan_prefix(b"r").resume_after(&mid).iter().map(|_| ()));
    invalid(ScanPosition::from_bytes(b"nonsense").map(|_| ()));
    let kp = t
        .keys(&KeySet::from_unsorted([b"r001"]))
        .read_within(1)
        .unwrap();
    let kpos = kp.resume().unwrap();
    invalid(ScanPosition::from_bytes(&kpos.to_bytes()).map(|_| ()));
    invalid(
        t.keys(&KeySet::from_unsorted([b"r001"]))
            .resume_after(kpos)
            .read()
            .map(|_| ()),
    );
    db.close().unwrap();
}

#[test]
fn a_multi_get_pages_by_key_and_resumes_in_a_rebuilt_set() {
    let (db, t) = db("keys");
    let snap = db.snapshot().unwrap();
    let keys: Vec<Vec<u8>> = (0..60)
        .step_by(3)
        .map(|i| format!("r{i:03}").into_bytes())
        .collect();
    let set = KeySet::from_unsorted(&keys);
    let mut whole = Vec::new();
    let mut rows = t.keys(&set).snapshot(&snap).read().unwrap();
    while let Some((_, r)) = rows.next().unwrap() {
        whole.push(r.to_owned());
    }
    let whole = cells_of(&whole);
    let mut got = Vec::new();
    let mut at: Option<KeysPosition> = None;
    loop {
        let mut r = t.keys(&set).snapshot(&snap);
        if let Some(p) = &at {
            r = r.resume_after(&KeysPosition::from_bytes(&p.to_bytes()).unwrap());
        }
        let page = r.read_within(200).unwrap();
        got.extend(cells_of(page.rows()));
        match page.resume() {
            Some(p) => at = Some(p.clone()),
            None => break,
        }
    }
    assert_eq!(got, whole);
    // A set rebuilt between pages (keys added before and after the position, and the
    // position's own key removed) resumes at the first key after it.
    let page = t.keys(&set).snapshot(&snap).read_within(250).unwrap();
    let pos = page.resume().unwrap().clone();
    let mut rebuilt: Vec<Vec<u8>> = keys
        .iter()
        .filter(|k| k.as_slice() != pos.key())
        .cloned()
        .collect();
    rebuilt.extend([b"r001".to_vec(), b"r059".to_vec()]);
    let next = t
        .keys(&KeySet::from_unsorted(&rebuilt))
        .snapshot(&snap)
        .resume_after(&pos)
        .read_within(1 << 30)
        .unwrap();
    let first = next.rows()[0].key().to_vec();
    assert!(
        first.as_slice() >= pos.key(),
        "{first:?} before {:?}",
        pos.key()
    );
    assert!(next.rows().iter().all(|r| r.key() != b"r001"));
    assert_eq!(next.rows().last().unwrap().key(), b"r059");
    db.close().unwrap();
}

#[test]
fn vector_and_blob_values_count_their_whole_value() {
    let (db, _) = db("sizes");
    let spec = VectorSpec::new(8, Element::F32, Metric::L2).unwrap();
    let v = db
        .table("v")
        .unwrap()
        .family("emb", Family::vector(spec))
        .family("big", Family::default().blob_threshold(64))
        .create_if_missing()
        .unwrap();
    v.mutate(b"a")
        .put_vector_f32("emb", b"x", &[0.5; 8])
        .commit()
        .unwrap();
    v.mutate(b"b")
        .put("big", b"y", &vec![7u8; 5000])
        .commit()
        .unwrap();
    db.flush().unwrap();
    let page = v.scan_prefix(b"").read_within(1 << 30).unwrap();
    let mut want = 0;
    for r in page.rows() {
        want += r.key().len() as u64;
        for e in r.view().iter() {
            want += e.qualifier.len() as u64 + e.cell.value().len() as u64 + 16;
        }
    }
    assert_eq!(page.bytes(), want);
    assert_eq!(
        page.rows()[1]
            .view()
            .iter()
            .next()
            .unwrap()
            .cell
            .value()
            .len(),
        5000
    );
    db.close().unwrap();
}

#[test]
fn a_traversal_budget_cuts_a_hop_and_ends_the_traversal() {
    let (db, _) = db("traverse");
    let g = db
        .table("g")
        .unwrap()
        .family("out", Family::default())
        .create_if_missing()
        .unwrap();
    let mut m = g.mutate(b"n:root");
    for i in 0..20 {
        m = m.put("out", format!("n:{i:02}").as_bytes(), b"");
    }
    m.commit().unwrap();
    g.mutate(b"n:00").put("out", b"n:zz", b"").commit().unwrap();
    let seeds = KeySet::from_unsorted([b"n:root"]);
    let node = 4 + 16;
    let t = g
        .traverse(&seeds)
        .family("out")
        .hops(2)
        .byte_budget(node * 5)
        .run()
        .unwrap();
    assert!(t.stats().cut_by_budget);
    assert_eq!(t.hops().len(), 1);
    let nodes: Vec<&[u8]> = t.hops()[0].nodes().iter().collect();
    assert_eq!(nodes, [&b"n:00"[..], b"n:01", b"n:02", b"n:03", b"n:04"]);
    let full = g.traverse(&seeds).family("out").hops(2).run().unwrap();
    assert!(!full.stats().cut_by_budget);
    assert_eq!(full.hops().len(), 2);
    // With parents counted, a node costs 4 bytes more.
    let p = g
        .traverse(&seeds)
        .family("out")
        .parents(true)
        .byte_budget(node * 5)
        .run()
        .unwrap();
    assert_eq!(p.hops()[0].nodes().len(), 4);
    db.close().unwrap();
}

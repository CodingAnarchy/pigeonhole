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
    // Every position carries its table (perf287's S1 on #1281), not only a mid-row one: an
    // after-row position and an empty page's at-row position from t are refused on u.
    let after_row = t.scan_prefix(b"r").limit(1).read_within(1 << 20).unwrap();
    let after_row = after_row.resume().unwrap().clone();
    invalid(
        other
            .scan_prefix(b"r")
            .resume_after(&after_row)
            .read_within(1 << 20)
            .map(|_| ()),
    );
    // limit(0) checks the position too (perf287's note on #1281).
    invalid(
        other
            .scan_prefix(b"r")
            .limit(0)
            .resume_after(&after_row)
            .read_within(1 << 20)
            .map(|_| ()),
    );
    let at_row = t.scan_prefix(b"r").read_within(1).unwrap();
    assert!(at_row.rows().is_empty());
    let at_row = at_row.resume().unwrap().clone();
    invalid(
        other
            .scan_prefix(b"r")
            .resume_after(&at_row)
            .read_within(1 << 20)
            .map(|_| ()),
    );
    let kpos = t
        .keys(&KeySet::from_unsorted([b"r001", b"r002"]))
        .read_within(80)
        .unwrap();
    let kpos = kpos.resume().unwrap().clone();
    invalid(
        other
            .keys(&KeySet::from_unsorted([b"r000"]))
            .resume_after(&kpos)
            .read_within(1 << 20)
            .map(|_| ()),
    );
    // A key length past the bytes, or past usize when added, is refused, not a panic.
    let mut huge = after_row.to_bytes();
    huge[7..11].copy_from_slice(&u32::MAX.to_le_bytes());
    invalid(ScanPosition::from_bytes(&huge).map(|_| ()));
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

/// `Nearest::run_within` (D308 PR 3, #1302): `run`'s ranked neighbours cut to the longest
/// prefix whose sizes (row key + qualifier + 16 each) fit, for every element and metric.
#[test]
fn a_budgeted_nearest_search_is_runs_prefix_that_fits() {
    let dir = std::env::temp_dir().join(format!("phdb-budget-{}-knn", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = Pigeonhole::open(
        dir.join("k.phdb"),
        Options::default().shards(2).pin_threads(false),
    )
    .unwrap();
    let specs = [
        VectorSpec::new(4, Element::F32, Metric::L2).unwrap(),
        VectorSpec::new(4, Element::F32, Metric::Cosine).unwrap(),
        VectorSpec::new(4, Element::F32, Metric::InnerProduct).unwrap(),
        VectorSpec::new(4, Element::I8, Metric::L2).unwrap(),
        VectorSpec::new(16, Element::Bit, Metric::Hamming).unwrap(),
    ];
    let mut seed = 0x1302u64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for (si, spec) in specs.iter().enumerate() {
        let t = db
            .table(&format!("v{si}"))
            .unwrap()
            .family("e", Family::vector(*spec))
            .create_if_missing()
            .unwrap();
        // Rows and qualifiers of varied lengths, so sizes differ.
        for i in 0..80u64 {
            let row = format!("r{}{i}", "x".repeat((next() % 5) as usize)).into_bytes();
            let q = "q".repeat((next() % 4) as usize).into_bytes();
            let m = t.mutate(&row);
            let m = match spec.element() {
                Element::F32 => {
                    let v: Vec<f32> = (0..4).map(|_| (next() % 100) as f32 / 10.0 + 0.1).collect();
                    m.put_vector_f32("e", &q, &v)
                }
                Element::I8 => {
                    let v: Vec<i8> = (0..4).map(|_| (next() % 100) as i8).collect();
                    m.put_vector_i8("e", &q, &v)
                }
                _ => m.put_vector_bits("e", &q, &[next() as u8, next() as u8]),
            };
            m.commit().unwrap();
        }
        let snap = db.snapshot().unwrap();
        let search = |k: usize| {
            let s = t.scan_prefix(b"").family("e").snapshot(&snap);
            match spec.element() {
                Element::F32 => s.nearest_f32(&[1.0, 2.0, 3.0, 4.0], k),
                Element::I8 => s.nearest_i8(&[10, 20, 30, 40], k),
                _ => s.nearest_bits(&[0b1010_1010, 0b0101_0101], k),
            }
        };
        for _ in 0..40 {
            let k = 1 + (next() % 30) as usize;
            let budget = next() % 700;
            let all = search(k).run().unwrap();
            let page = search(k).run_within(budget).unwrap();
            // The hand-cut prefix.
            let size = |n: &pigeonhole::Neighbour| (n.row.len() + n.qualifier.len() + 16) as u64;
            let (mut used, mut keep) = (0u64, 0usize);
            while keep < all.len() && used + size(&all[keep]) <= budget {
                used += size(&all[keep]);
                keep += 1;
            }
            let ctx = format!("spec {si}, k {k}, budget {budget}");
            assert_eq!(page.neighbours(), &all[..keep], "{ctx}");
            assert_eq!(page.bytes(), used, "{ctx}");
            assert!(page.bytes() <= budget, "{ctx}");
            assert_eq!(page.cut_by_budget(), keep < all.len(), "{ctx}");
            assert_eq!(page.next_bytes(), all.get(keep).map(size), "{ctx}");
        }
        // A budget of 0, and one too small for the nearest: empty, cut, with next_bytes.
        let all = search(5).run().unwrap();
        for budget in [0, size_of_first(&all) - 1] {
            let page = search(5).run_within(budget).unwrap();
            assert!(page.neighbours().is_empty() && page.cut_by_budget());
            assert_eq!(page.next_bytes(), Some(size_of_first(&all)));
        }
        // A budget covering every candidate isn't cut.
        let page = search(5).run_within(u64::MAX).unwrap();
        assert_eq!(page.clone().into_neighbours(), all);
        assert!(!page.cut_by_budget() && page.next_bytes().is_none());
    }
    // At an older snapshot, the page is that snapshot's.
    let t = db.table("v0").unwrap().open().unwrap();
    let before = db.snapshot().unwrap();
    t.mutate(b"aa-nearest")
        .put_vector_f32("e", b"", &[1.0, 2.0, 3.0, 4.0])
        .commit()
        .unwrap();
    let then = t
        .scan_prefix(b"")
        .family("e")
        .snapshot(&before)
        .nearest_f32(&[1.0, 2.0, 3.0, 4.0], 3)
        .run_within(1 << 20)
        .unwrap();
    assert!(then.neighbours().iter().all(|n| n.row != b"aa-nearest"));
    let now = t
        .scan_prefix(b"")
        .family("e")
        .nearest_f32(&[1.0, 2.0, 3.0, 4.0], 3)
        .run_within(1 << 20)
        .unwrap();
    assert_eq!(now.neighbours()[0].row, b"aa-nearest");
    db.close().unwrap();
}

fn size_of_first(all: &[pigeonhole::Neighbour]) -> u64 {
    (all[0].row.len() + all[0].qualifier.len() + 16) as u64
}

#[test]
fn the_async_budgeted_nearest_search_equals_the_sync_one() {
    let dir = std::env::temp_dir().join(format!("phdb-budget-{}-knn-async", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = Pigeonhole::open(
        dir.join("k.phdb"),
        Options::default().shards(2).pin_threads(false),
    )
    .unwrap();
    let spec = VectorSpec::new(2, Element::F32, Metric::L2).unwrap();
    let t = db
        .table("v")
        .unwrap()
        .family("e", Family::vector(spec))
        .create_if_missing()
        .unwrap();
    for i in 0..50u32 {
        t.mutate(format!("r{i:02}").as_bytes())
            .put_vector_f32("e", b"", &[i as f32, 1.0])
            .commit()
            .unwrap();
    }
    for budget in [0, 19, 20, 100, 1 << 20] {
        let search = || t.scan_prefix(b"").family("e").nearest_f32(&[10.0, 1.0], 8);
        let sync = search().run_within(budget).unwrap();
        let fut = pigeonhole::doc_support::block_on(search().run_within_async(budget)).unwrap();
        assert_eq!(fut.neighbours(), sync.neighbours(), "budget {budget}");
        assert_eq!(
            (fut.bytes(), fut.next_bytes()),
            (sync.bytes(), sync.next_bytes())
        );
    }
    db.close().unwrap();
}

/// #1286 (ICR 0122): a page's parts, to compare a sync and an async read.
#[cfg(feature = "async")]
type Summary = (Vec<Cell>, u64, Option<u64>, Option<Vec<u8>>);

#[cfg(feature = "async")]
fn scan_summary(p: &pigeonhole::ScanPage) -> Summary {
    (
        cells_of(p.rows()),
        p.bytes(),
        p.next_bytes(),
        p.resume().map(ScanPosition::to_bytes),
    )
}

#[cfg(feature = "async")]
fn keys_summary(p: &pigeonhole::KeysPage) -> Summary {
    (
        cells_of(p.rows()),
        p.bytes(),
        p.next_bytes(),
        p.resume().map(KeysPosition::to_bytes),
    )
}

/// #1286: `read_within_async` gives `read_within`'s page, over the D308 cases. Each case is
/// read at budgets across row and column boundaries, then followed page by page to the end.
/// The cases: versions, column limits, families named out of id order (simp724's A3), a
/// value filter with one and three versions and with a column limit (Q, A1), `limit`, and a
/// key set with a missing key.
#[cfg(feature = "async")]
#[test]
fn an_async_page_is_the_sync_page() {
    use pigeonhole::doc_support::block_on;
    use pigeonhole::{Scan, ValueFilter};
    let (db, t) = db("async-equal");
    let snap = db.snapshot().unwrap();
    // `d` is family g's second column (q3), so a column limit of 1 counts a failing column
    // first unless only passing columns count (simp724's A1).
    let b = || ValueFilter::Prefix(b"d".to_vec());
    type Case = for<'a> fn(Scan<'a>, &dyn Fn() -> ValueFilter) -> Scan<'a>;
    let cases: [(&str, Case); 9] = [
        ("plain", |s, _| s),
        ("versions 3", |s, _| s.versions(3)),
        ("column limit 2", |s, _| s.column_limit(2)),
        ("versions 3, column limit 1", |s, _| {
            s.versions(3).column_limit(1)
        }),
        ("families g, f", |s, _| s.family("g").family("f")),
        ("value filter", |s, b| s.value_filter(b())),
        ("value filter, versions 3", |s, b| {
            s.versions(3).value_filter(b())
        }),
        ("value filter, column limit 1", |s, b| {
            s.column_limit(1).value_filter(b())
        }),
        ("limit 7", |s, _| s.limit(7)),
    ];
    for (name, case) in cases {
        let scan = || case(t.scan_prefix(b"r").snapshot(&snap), &b);
        for budget in [0, 1, 20, 37, 100, 250, 600, 2000, 1 << 30] {
            let sync = scan_summary(&scan().read_within(budget).unwrap());
            let fut = scan_summary(&block_on(scan().read_within_async(budget)).unwrap());
            assert_eq!(fut, sync, "{name}, budget {budget}");
        }
        // Page by page to the end, both resumed from the sync page's position.
        let mut at: Option<ScanPosition> = None;
        let mut pages = 0;
        loop {
            let s = || match &at {
                Some(p) => scan().resume_after(p),
                None => scan(),
            };
            let sync = s().read_within(150).unwrap();
            let fut = block_on(s().read_within_async(150)).unwrap();
            assert_eq!(
                scan_summary(&fut),
                scan_summary(&sync),
                "{name}, page {pages}"
            );
            pages += 1;
            match sync.resume() {
                Some(p) => at = Some(p.clone()),
                None => break,
            }
            assert!(pages < 500, "{name}: no end");
        }
    }
    // A key set, a missing key among them.
    let mut keys: Vec<Vec<u8>> = (0..60u32)
        .step_by(3)
        .map(|i| format!("r{i:03}").into_bytes())
        .collect();
    keys.push(b"r0005".to_vec());
    let set = KeySet::from_unsorted(keys.iter());
    type KeysCase = fn(pigeonhole::KeysRead<'_>) -> pigeonhole::KeysRead<'_>;
    let cases: [(&str, KeysCase); 4] = [
        ("plain", |k| k),
        ("versions 3", |k| k.versions(3)),
        ("families g, f", |k| k.family("g").family("f")),
        ("value filter, column limit 1", |k| {
            k.column_limit(1)
                .value_filter(ValueFilter::Prefix(b"d".to_vec()))
        }),
    ];
    for (name, case) in cases {
        let read = || case(t.keys(&set).snapshot(&snap));
        for budget in [0, 20, 100, 600, 1 << 30] {
            let sync = keys_summary(&read().read_within(budget).unwrap());
            let fut = keys_summary(&block_on(read().read_within_async(budget)).unwrap());
            assert_eq!(fut, sync, "keys {name}, budget {budget}");
        }
        let mut at: Option<KeysPosition> = None;
        loop {
            let r = || match &at {
                Some(p) => read().resume_after(p),
                None => read(),
            };
            let sync = r().read_within(120).unwrap();
            let fut = block_on(r().read_within_async(120)).unwrap();
            assert_eq!(keys_summary(&fut), keys_summary(&sync), "keys {name}");
            match sync.resume() {
                Some(p) => at = Some(p.clone()),
                None => break,
            }
        }
    }
    // The refusals are the sync ones.
    let other = db
        .table("u")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    other.mutate(b"x").put("f", b"q", b"v").commit().unwrap();
    let from_other = other.scan_prefix(b"").read_within(1).unwrap();
    let pos = from_other.resume().expect("cut").clone();
    let e = block_on(
        t.scan_prefix(b"r")
            .resume_after(&pos)
            .read_within_async(100),
    )
    .unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument);
    db.close().unwrap();
}

/// Polls `fut` to completion with a no-op waker: its output, and how often it was `Pending`.
#[cfg(feature = "async")]
fn poll_counting<F: std::future::Future>(fut: F) -> (F::Output, usize) {
    use std::task::{Context, Poll, Waker};
    let mut fut = std::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    let mut pending = 0;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(r) => return (r, pending),
            Poll::Pending => pending += 1,
        }
        assert!(
            std::time::Instant::now() < deadline,
            "still pending after 60 s"
        );
        std::thread::yield_now();
    }
}

/// #1286, simp724's A2: separated values are read before the value filter and the visitor,
/// as the sync read's lanes hand them over: the async pages equal the sync ones.
#[cfg(feature = "async")]
#[test]
fn an_async_page_reads_separated_values() {
    use pigeonhole::ValueFilter;
    use pigeonhole::doc_support::block_on;
    let dir = std::env::temp_dir().join(format!("phdb-budget-{}-async-blob", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = Pigeonhole::open(dir.join("b.phdb"), Options::default().shards(2)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("b", Family::default().blob_threshold(64))
        .create_if_missing()
        .unwrap();
    for i in 0..40u32 {
        let mut m = t.mutate(format!("r{i:02}").as_bytes());
        for c in 0..3u8 {
            let v = vec![b'a' + c; 100 + (i as usize * 13) % 300];
            m = m.put("b", &[b'q', c], &v);
        }
        m.commit().unwrap();
    }
    db.flush().unwrap();
    let filters = [None, Some(ValueFilter::Prefix(b"b".to_vec()))];
    for filter in filters {
        for budget in [0, 150, 600, 3000, 1 << 30] {
            let scan = || {
                let s = t.scan_prefix(b"r");
                match &filter {
                    Some(f) => s.value_filter(f.clone()),
                    None => s,
                }
            };
            let sync = scan_summary(&scan().read_within(budget).unwrap());
            let fut = scan_summary(&block_on(scan().read_within_async(budget)).unwrap());
            assert_eq!(fut, sync, "filter {filter:?}, budget {budget}");
        }
    }
    db.close().unwrap();
}

/// #1286: a poll yields after a bounded number of cells or value bytes, a value filter's
/// misses included, so a wide row never holds the executor; and the page is the sync one.
#[cfg(feature = "async")]
#[test]
fn an_async_page_yields_within_a_wide_row() {
    use pigeonhole::ValueFilter;
    let dir = std::env::temp_dir().join(format!("phdb-budget-{}-async-wide", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = Pigeonhole::open(dir.join("w.phdb"), Options::default().shards(1)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    // One row of 12,288 small cells (3 polls' worth by cells), 1% matching a filter.
    for chunk in (0..12_288u32).collect::<Vec<_>>().chunks(2_048) {
        let mut m = t.mutate(b"wide");
        for &i in chunk {
            let v: &[u8] = if i % 100 == 0 { b"keep" } else { b"drop" };
            m = m.put("f", format!("q{i:05}").as_bytes(), v);
        }
        m.commit().unwrap();
    }
    // One row of 48 values of 64 KiB: 3 MiB, 3 polls' worth by bytes.
    let mut m = t.mutate(b"wide2");
    for i in 0..48u32 {
        m = m.put("f", format!("q{i:02}").as_bytes(), &vec![b'x'; 64 << 10]);
    }
    m.commit().unwrap();
    db.flush().unwrap();
    for (name, row, filter) in [
        ("cells", &b"wide"[..], None),
        (
            "filtered cells",
            b"wide",
            Some(ValueFilter::Equals(b"keep".to_vec())),
        ),
        ("bytes", b"wide2", None),
    ] {
        let scan = || {
            let s = t.scan_prefix(row).family("f");
            match &filter {
                Some(f) => s.value_filter(f.clone()),
                None => s,
            }
        };
        let sync = scan_summary(&scan().read_within(1 << 30).unwrap());
        let (got, pending) = poll_counting(scan().read_within_async(1 << 30));
        assert_eq!(scan_summary(&got.unwrap()), sync, "{name}");
        assert!(pending >= 2, "{name}: {pending} yields");
    }
    db.close().unwrap();
}

/// simp724's B1 on #1556: cells a value filter drops count their value bytes toward the
/// poll's bound too, since the cursor read each separated value from its blob file. One row
/// of 64 separated values of 64 KiB (4 MiB), none passing the filter, yields by bytes.
#[cfg(feature = "async")]
#[test]
fn an_async_page_yields_over_filtered_separated_values() {
    use pigeonhole::ValueFilter;
    let dir = std::env::temp_dir().join(format!("phdb-budget-{}-async-misses", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = Pigeonhole::open(dir.join("m.phdb"), Options::default().shards(1)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("b", Family::default().blob_threshold(64))
        .create_if_missing()
        .unwrap();
    let mut m = t.mutate(b"row");
    for i in 0..64u32 {
        m = m.put("b", format!("q{i:02}").as_bytes(), &vec![b'x'; 64 << 10]);
    }
    m.commit().unwrap();
    db.flush().unwrap();
    let scan = || {
        t.scan_prefix(b"row")
            .value_filter(ValueFilter::Prefix(b"never".to_vec()))
    };
    let sync = scan_summary(&scan().read_within(1 << 30).unwrap());
    assert!(sync.0.is_empty());
    let (got, pending) = poll_counting(scan().read_within_async(1 << 30));
    assert_eq!(scan_summary(&got.unwrap()), sync);
    assert!(
        pending >= 2,
        "{pending} yields over 4 MiB of filtered values"
    );
    db.close().unwrap();
}

/// #1286, #1652: from a cold block cache (a reopen), the async read fetches the blocks it
/// reads, a row's later blocks too, and its page is the sync one. On real files a fetch whose
/// read completes at submit is read in place and counted (D292, #1629), so the exact count of
/// synchronous reads, 0, is `keyset_cells_async.rs`'s, on the simulated device.
#[cfg(feature = "async")]
#[test]
fn an_async_page_from_a_cold_cache_fetches_its_rows_blocks() {
    let dir = std::env::temp_dir().join(format!("phdb-budget-{}-async-cold", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let open = || Pigeonhole::open(dir.join("c.phdb"), Options::default().shards(1)).unwrap();
    let db = open();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    // Rows wide enough that a row's cells span several blocks.
    for i in 0..30u32 {
        let mut m = t.mutate(format!("r{i:02}").as_bytes());
        for c in 0..200u32 {
            m = m.put("f", format!("q{c:03}").as_bytes(), &[b'v'; 100]);
        }
        m.commit().unwrap();
    }
    db.flush().unwrap();
    drop(t);
    db.close().unwrap();
    let db = open();
    let t = db.table("t").unwrap().open().unwrap();
    let before = db.engine_metrics();
    let (got, pending) = poll_counting(t.scan_prefix(b"r").read_within_async(1 << 30));
    let after = db.engine_metrics();
    let got = got.unwrap();
    assert!(pending > 0, "nothing was fetched");
    assert!(
        after.block_cache_misses > before.block_cache_misses,
        "the cache was cold"
    );
    let sync = t.scan_prefix(b"r").read_within(1 << 30).unwrap();
    assert_eq!(scan_summary(&got), scan_summary(&sync));
    db.close().unwrap();
}

/// #1286: the async reads' futures are `Send`, so a multi-threaded executor can run them
/// (compile-time).
#[cfg(feature = "async")]
#[allow(dead_code)]
fn read_within_async_is_send(t: &Table, keys: &KeySet) {
    fn send<T: Send>(_: T) {}
    send(t.scan_prefix(b"").read_within_async(1));
    send(t.keys(keys).read_within_async(1));
}

//! D331 PR 2: a range move's gates through every phase, across three nodes, with the maps
//! pushed by hand (the driver is PR 5's): the source fences the range's writes, then answers
//! it `WrongPartition`; the target serves it only from CLEANING; a node routing by an older
//! map catches up; and the source's hand-over raises its epoch and records the range in its
//! file.

use super::*;
use crate::serve::proto::{MoveKind, MovePhase, PartitionRequest, PushMapRequest, RangeMove};

async fn try_put(port: u16, row: &str, value: &str) -> Result<(), tonic::Status> {
    data(port)
        .await
        .mutate(crate::serve::proto::MutateRequest {
            writes: Some(put_row(row, value)),
            durability: 0,
        })
        .await
        .map(drop)
}

async fn push(ports: &[u16], map: &ClusterMap) {
    for port in ports {
        client(*port)
            .await
            .push_map(PushMapRequest {
                map: Some(map.clone()),
            })
            .await
            .unwrap();
    }
}

/// `v1` with `users` `[c, f)` moving from p1 (on a) to p2 (on b), at `phase`, as map
/// `version`. From HANDED_OVER the range is p2's, p1's epoch is 2, and the hand-over map is
/// version 5.
fn moving(v1: &ClusterMap, version: u64, phase: Option<MovePhase>) -> ClusterMap {
    let mut m = v1.clone();
    m.map_version = version;
    let Some(phase) = phase else {
        // Done: the range stays p2's.
        m.tables[0] = super::super::map::handed_over(&m.tables[0], b"c", Some(b"f"), "p2");
        m.partitions[0].epoch = 2;
        return m;
    };
    let handed = super::super::map::committed(phase);
    m.moves.push(RangeMove {
        id: "mv1".into(),
        kind: MoveKind::Range as i32,
        table: "users".into(),
        start: Some(text("c")),
        end: Some(text("f")),
        from: "p1".into(),
        to: "p2".into(),
        phase: phase as i32,
        handed_over_version: if handed { 5 } else { 0 },
        ..Default::default()
    });
    if handed {
        m.tables[0] = super::super::map::handed_over(&m.tables[0], b"c", Some(b"f"), "p2");
        m.partitions[0].epoch = 2;
    }
    m
}

#[test]
fn a_range_move_gates_each_phase_on_its_source_and_its_target() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut v1 = the_map(1, &[ports[0], ports[1], ports[2]]);
    v1.partitions.retain(|p| p.id != "p3");
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::super::write_map(dir, &super::super::map::canonical_json(&v1)).unwrap();
    }
    let mut it = listeners.into_iter();
    let (_stop_a, db_a) = data_node(&rt, &dirs[0], "a", Some("p1"), it.next().unwrap());
    let (_stop_b, _) = data_node(&rt, &dirs[1], "b", Some("p2"), it.next().unwrap());
    let (_stop_c, _) = data_node(&rt, &dirs[2], "c", None, it.next().unwrap());
    let db_a = db_a.unwrap();
    let (a, b, c) = (ports[0], ports[1], ports[2]);
    rt.block_on(async {
        eventually("p1 takes writes", async || {
            try_put(c, "d", "1").await.is_ok()
        })
        .await;
        // COPYING and CATCHING_UP: nothing changes for a client.
        for (version, phase) in [(2, MovePhase::Copying), (3, MovePhase::CatchingUp)] {
            push(&ports, &moving(&v1, version, Some(phase))).await;
            try_put(c, "d", "2").await.unwrap();
            assert_eq!(get_row(c, "d").await.unwrap(), Some(b"2".to_vec()));
        }
        // A transaction that writes, begun before the fence.
        let early = begin(c, "alice", None).await.unwrap();
        txn_write(c, &early, vec![put_row("alice", "0")], None)
            .await
            .unwrap();
        // HANDING_OVER: the range's writes are fenced, retry-safe; its reads and p1's other
        // keys go on.
        push(&ports, &moving(&v1, 4, Some(MovePhase::HandingOver))).await;
        // simp724's B2: open across the fence, it's refused whatever it wrote.
        let e = commit(c, &early, None).await.unwrap_err();
        assert_eq!(code(&e), "Fenced", "{e}");
        // simp724's B1: one begun after the fence, committing rows into the range, too; and
        // nothing is written.
        let late = begin(c, "alice", None).await.unwrap();
        let e = txns(c)
            .await
            .commit(with(
                crate::serve::proto::TxnCommitRequest {
                    txn_id: late,
                    rows: vec![put_row("d", "txn")],
                },
                None,
            ))
            .await
            .unwrap_err();
        assert_eq!(code(&e), "Fenced", "{e}");
        let e = try_put(c, "d", "3").await.unwrap_err();
        assert_eq!(code(&e), "Fenced", "{e}");
        assert!(e.message().contains("move \"mv1\""), "{e}");
        assert_eq!(get_row(c, "d").await.unwrap(), Some(b"2".to_vec()));
        try_put(c, "alice", "1").await.unwrap();
        try_put(c, "zed", "1").await.unwrap();

        // simp724's B3: a's stamp fails once, so the hand-over stops after its row.
        *super::super::moves::FAIL_STAMP.lock().unwrap() = Some(dirs[0].clone());
        // HANDED_OVER reaches a and b, not c. c routes `d` to p1 by its older map; a answers
        // `WrongPartition`, c pulls the newer map and routes to p2, which doesn't serve the
        // range until CLEANING.
        push(&[a, b], &moving(&v1, 5, Some(MovePhase::HandedOver))).await;
        let e = try_put(c, "d", "4").await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        assert_eq!(version(c).await, 5, "c pulled the hand-over map");
        let e = get_row(c, "d").await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        // Its lease lapsed, and it can't ask at epoch 2 with its file at 1: no writes.
        assert!(try_put(c, "alice", "2").await.is_err());
        // CLEANING: the next map finishes a's hand-over (the stamp, then the lease at
        // epoch 2, asked at once), and p2 serves the range and takes its writes.
        push(&ports, &moving(&v1, 6, Some(MovePhase::Cleaning))).await;
        eventually("p1 takes writes at epoch 2", async || {
            try_put(c, "alice", "2").await.is_ok()
        })
        .await;
        try_put(c, "zed", "2").await.unwrap();
        // p2 serves the range as b's follower copied it (D331 PR 4b).
        assert_eq!(get_row(c, "d").await.unwrap(), Some(b"2".to_vec()));
        try_put(c, "d", "5").await.unwrap();
        // Done: the record goes, and the range stays p2's.
        push(&ports, &moving(&v1, 7, None)).await;
        assert_eq!(get_row(c, "d").await.unwrap(), Some(b"5".to_vec()));
        try_put(c, "e", "1").await.unwrap();
        assert_eq!(get_row(b, "e").await.unwrap(), Some(b"1".to_vec()));
    });
    // p1's file: its epoch raised by the hand-over, and the range recorded, so a restart
    // gates it before serving.
    let ident = super::super::identity::read(&db_a).unwrap().unwrap();
    assert_eq!(ident.epoch, 2);
    let moved = crate::serve::ranges::read_moved(&db_a).unwrap();
    assert_eq!(moved.len(), 1, "{moved:?}");
    assert_eq!(
        (
            moved[0].id.as_str(),
            moved[0].start.as_slice(),
            moved[0].end.as_deref()
        ),
        ("mv1", &b"c"[..], Some(&b"f"[..]))
    );
    assert_eq!(moved[0].version, 5);
    // p1's copy of the range wasn't written after the fence, and CLEANING dropped it (D331
    // PR 5); the rest of p1 stays.
    let t = db_a.table("users").unwrap().open().unwrap();
    let value = |row: &[u8]| t.get(row, "f", b"q").unwrap().map(|c| c.value().to_vec());
    assert_ne!(value(b"d"), Some(b"3".to_vec()), "a write after the fence");
    rt.block_on(eventually("p1 drops the moved range", async || {
        value(b"d").is_none()
    }));
    assert_eq!(value(b"alice"), Some(b"2".to_vec()));
    let _ = db_a.close();
}

/// A partition opening after its move is done and gone from the map (a restart, or a
/// promoted replica copy holding the row through the feed) gates the moved range from its
/// file's row before it serves.
#[test]
fn an_opening_partition_gates_a_moved_range_from_its_file() {
    let ports = [7001, 7002, 7003];
    let dir = test_dir();
    let n = node(&dir, "a");
    let done = moving(&the_map(1, &ports), 7, None);
    n.push_map(done.clone(), None).unwrap();
    let db = pigeonhole::Pigeonhole::open(
        dir.join("p1.phdb"),
        pigeonhole::Options::default().shards(1),
    )
    .unwrap();
    crate::serve::ranges::write_moved(
        &db,
        &crate::serve::ranges::Moved {
            id: "mv1".into(),
            table: "users".into(),
            start: b"c".to_vec(),
            end: Some(b"f".to_vec()),
            term: 0,
            version: 5,
            seqno: 3,
            at_ms: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let gate = n.open_gate("p1", 2);
    n.arm_ranges(&gate, "p1", &db, || ()).unwrap();
    let state = (*gate.ranges().load())
        .clone()
        .expect("the row gates the range");
    let get = |row: &str| {
        crate::serve::service::ServiceOp::Get(crate::serve::proto::GetRequest {
            table: "users".into(),
            row: row.to_owned().into(),
            ..Default::default()
        })
    };
    assert_eq!(
        state.check(&get("d"), false).unwrap_err().code,
        "WrongPartition"
    );
    state.check(&get("a"), false).unwrap();
    let _ = db.close();
}

/// simp724's B3, the crash case: the map at the hand-over's epoch persisted, the source's
/// file still at the epoch before (a crash before the stamp). Its open finishes the
/// hand-over: the row, then the stamp, and it opens at the map's epoch.
#[test]
fn an_open_finishes_a_hand_over_a_crash_cut_before_the_stamp() {
    let ports = [7001, 7002, 7003];
    let dir = test_dir();
    let handed = moving(&the_map(1, &ports), 5, Some(MovePhase::HandedOver));
    super::super::write_map(&dir, &super::super::map::canonical_json(&handed)).unwrap();
    let path = dir.join("p1.phdb");
    let db = pigeonhole::Pigeonhole::open(&path, pigeonhole::Options::default().shards(1)).unwrap();
    super::super::identity::write(
        &db,
        &super::super::identity::Identity {
            cluster_id: "c1".into(),
            partition_id: "p1".into(),
            epoch: 1,
        },
    )
    .unwrap();
    db.close().unwrap();
    let db = super::super::open_partition(
        &dir,
        "c1",
        "p1",
        2,
        pigeonhole::Options::default().shards(1),
    )
    .unwrap()
    .expect("opened at the map's epoch");
    assert_eq!(super::super::identity::read(&db).unwrap().unwrap().epoch, 2);
    let moved = crate::serve::ranges::read_moved(&db).unwrap();
    assert_eq!(moved.len(), 1, "{moved:?}");
    assert_eq!(moved[0].id, "mv1");
    let _ = db.close();
}

/// simp724's S4: a source that opens while its move is HANDING_OVER (a restart) fences the
/// range again before it serves.
#[test]
fn an_opening_source_fences_a_handing_over_range() {
    let ports = [7001, 7002, 7003];
    let dir = test_dir();
    let n = node(&dir, "a");
    n.push_map(
        moving(&the_map(1, &ports), 4, Some(MovePhase::HandingOver)),
        None,
    )
    .unwrap();
    let db = pigeonhole::Pigeonhole::open(
        dir.join("p1.phdb"),
        pigeonhole::Options::default().shards(1),
    )
    .unwrap();
    let gate = n.open_gate("p1", 1);
    n.arm_ranges(&gate, "p1", &db, || ()).unwrap();
    let state = (*gate.ranges().load()).clone().expect("fenced");
    let put = crate::serve::service::ServiceOp::Mutate(crate::serve::proto::MutateRequest {
        writes: Some(put_row("d", "x")),
        durability: 0,
    });
    assert_eq!(state.check(&put, true).unwrap_err().code, "Fenced");
    let _ = db.close();
}

/// D331 PR 3 (design §6): a restored file is clipped to its map's ranges. Rows outside the
/// partition's ranges go, moved-away rows the map gives back are cut to the rest, the
/// timestamp floor covers what it kept, and a rerun finds it done (cli692's F5).
#[test]
fn a_restored_file_is_clipped_to_the_maps_ranges_and_a_rerun_finds_it_done() {
    use pigeonhole::Family;
    let ports = [7001, 7002, 7003];
    let dir = test_dir();
    // The restored map: `users` split at "m", p1 holding `[.., m)`.
    let map = the_map(1, &ports);
    let db = pigeonhole::Pigeonhole::open(
        dir.join("p1.phdb"),
        pigeonhole::Options::default().shards(1),
    )
    .unwrap();
    let t = db
        .table("users")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    for row in ["alice", "dave", "mike", "zed"] {
        t.mutate(row.as_bytes())
            .put("f", b"q", row.as_bytes())
            .commit()
            .unwrap();
    }
    // An older move took `[c, f)` away; this map gives p1 `[.., m)`, so `[c, f)` is home.
    crate::serve::ranges::write_moved(
        &db,
        &crate::serve::ranges::Moved {
            id: "mv0".into(),
            table: "users".into(),
            start: b"c".to_vec(),
            end: Some(b"q".to_vec()),
            term: 0,
            version: 3,
            seqno: 1,
            at_ms: 0,
            ..Default::default()
        },
    )
    .unwrap();
    crate::serve::ranges::mark_clip(&db).unwrap();
    assert!(crate::serve::ranges::clip_pending(&db).unwrap());
    let c = super::super::moves::clip_restored(&db, &map, "p1").unwrap();
    assert_eq!((c.deleted, c.in_doubt), (2, 0), "{c:?}");
    assert!(c.newest_ts > 0);
    let keys: Vec<Vec<u8>> = t
        .scan_bounds(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .iter()
        .unwrap()
        .map(|r| r.unwrap().key().to_vec())
        .collect();
    assert_eq!(keys, [b"alice".to_vec(), b"dave".to_vec()]);
    // The moved-away row keeps only `[m, q)`, the part the map doesn't give p1.
    let moved = crate::serve::ranges::read_moved(&db).unwrap();
    assert_eq!(moved.len(), 1, "{moved:?}");
    assert_eq!(
        (
            moved[0].id.as_str(),
            moved[0].start.as_slice(),
            moved[0].end.as_deref()
        ),
        ("mv0~0", &b"m"[..], Some(&b"q"[..]))
    );
    // Its hand-over pair is reset (cli692): a move back in the restored cluster, whose
    // serving row carries any pair of its own, counts as newer, so p1 serves the range.
    assert_eq!((moved[0].term, moved[0].version), (0, 0));
    let mut back = map.clone();
    back.tables[0].splits = vec![text("q")];
    let gate = crate::serve::ranges::RangeGate::default();
    gate.load_rows(
        moved.clone(),
        vec![crate::serve::ranges::Moved {
            id: "mv9".into(),
            table: "users".into(),
            start: b"m".to_vec(),
            end: Some(b"q".to_vec()),
            term: 0,
            version: 1,
            seqno: 5,
            at_ms: 0,
            ..Default::default()
        }],
    );
    gate.update(&back, "p1", 6);
    assert!(gate.load().is_none(), "the move back serves `[m, q)` on p1");
    assert!(!crate::serve::ranges::clip_pending(&db).unwrap());
    // A rerun (a clip that stopped before its marker went) finds nothing to do.
    crate::serve::ranges::mark_clip(&db).unwrap();
    let again = super::super::moves::clip_restored(&db, &map, "p1").unwrap();
    assert_eq!((again.deleted, again.in_doubt), (0, 0));
    assert_eq!(crate::serve::ranges::read_moved(&db).unwrap(), moved);
    let _ = db.close();
}

/// simp724's S1 on PR 3a: a row outside the ranges with a cell timestamped ahead of the
/// clock (a move's copy keeps the source's timestamps) is gone after the clip: the floor
/// rises over every row before the deletes.
#[test]
fn a_clip_deletes_a_row_timestamped_ahead_of_the_clock() {
    use pigeonhole::Family;
    let dir = test_dir();
    let map = the_map(1, &[7001, 7002, 7003]);
    let db = pigeonhole::Pigeonhole::open(
        dir.join("p1.phdb"),
        pigeonhole::Options::default().shards(1),
    )
    .unwrap();
    let t = db
        .table("users")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let ahead = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
        + 3_600_000_000;
    t.mutate(b"zed")
        .put_at("f", b"q", ahead, b"future")
        .commit()
        .unwrap();
    crate::serve::ranges::mark_clip(&db).unwrap();
    let c = super::super::moves::clip_restored(&db, &map, "p1").unwrap();
    assert_eq!(c.deleted, 1, "{c:?}");
    assert!(c.newest_ts >= ahead);
    assert_eq!(
        t.get(b"zed", "f", b"q")
            .unwrap()
            .map(|c| c.value().to_vec()),
        None
    );
    let _ = db.close();
}

/// simp724's S2 on PR 3a: a D329 share in doubt keeps the clip's marker (a blind delete
/// isn't refused by it, and decided commit, its writes would come back); once it's
/// decided, the clip run again deletes what it wrote and clears the marker.
#[test]
fn a_share_in_doubt_keeps_the_clip_until_it_is_decided() {
    use pigeonhole::Family;
    let dir = test_dir();
    let map = the_map(1, &[7001, 7002, 7003]);
    let db = pigeonhole::Pigeonhole::open(
        dir.join("p1.phdb"),
        pigeonhole::Options::default().shards(1),
    )
    .unwrap();
    let t = db
        .table("users")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let mut txn = db.transaction().unwrap();
    txn.put(&t, b"zed", "f", b"q", b"in doubt");
    txn.prepare(b"x1", pigeonhole::Durability::GroupSync)
        .unwrap();
    crate::serve::ranges::mark_clip(&db).unwrap();
    let c = super::super::moves::clip_restored(&db, &map, "p1").unwrap();
    assert_eq!(c.in_doubt, 1, "{c:?}");
    assert!(
        crate::serve::ranges::clip_pending(&db).unwrap(),
        "the marker stays"
    );
    db.decide(b"x1", true).unwrap();
    assert!(
        t.get(b"zed", "f", b"q").unwrap().is_some(),
        "the share's write landed"
    );
    let c = super::super::moves::clip_restored(&db, &map, "p1").unwrap();
    assert_eq!((c.deleted, c.in_doubt), (1, 0), "{c:?}");
    assert!(!crate::serve::ranges::clip_pending(&db).unwrap());
    assert!(t.get(b"zed", "f", b"q").unwrap().is_none());
    let _ = db.close();
}

/// #2070 (ai1249's review): the open that finishes a hand-over cut before the stamp takes the
/// file only at the hand-over's exact jump. A file at epoch 1 under a hand-over map at 3
/// finishes when an abandoned promote recorded epoch 2 (the floor the hand-over went past),
/// and is refused when nothing explains the gap (a stale file).
#[test]
fn an_open_finishes_a_hand_over_past_a_promotes_floor_and_no_further() {
    use crate::serve::proto::{FailoverEvent, FailoverKind, FailoverState};
    for floor in [true, false] {
        let ports = [7001, 7002, 7003];
        let dir = test_dir();
        let mut handed = moving(&the_map(1, &ports), 5, Some(MovePhase::HandedOver));
        handed.partitions[0].epoch = 3;
        if floor {
            let mut e = FailoverEvent {
                partition: "p1".into(),
                from: "a".into(),
                to: "b".into(),
                to_epoch: 2,
                ..Default::default()
            };
            e.set_kind(FailoverKind::Partition);
            e.set_state(FailoverState::Abandoned);
            handed.failovers.push(e);
        }
        super::super::write_map(&dir, &super::super::map::canonical_json(&handed)).unwrap();
        let path = dir.join("p1.phdb");
        let db =
            pigeonhole::Pigeonhole::open(&path, pigeonhole::Options::default().shards(1)).unwrap();
        super::super::identity::write(
            &db,
            &super::super::identity::Identity {
                cluster_id: "c1".into(),
                partition_id: "p1".into(),
                epoch: 1,
            },
        )
        .unwrap();
        db.close().unwrap();
        let opened = super::super::open_partition(
            &dir,
            "c1",
            "p1",
            3,
            pigeonhole::Options::default().shards(1),
        );
        if floor {
            let db = opened.unwrap().expect("opened at the map's epoch");
            assert_eq!(super::super::identity::read(&db).unwrap().unwrap().epoch, 3);
            assert_eq!(crate::serve::ranges::read_moved(&db).unwrap().len(), 1);
            let _ = db.close();
        } else {
            assert!(
                !matches!(opened, Ok(Some(_))),
                "a file two epochs below with no floor was opened"
            );
        }
    }
}

/// Whether the follower on the node in `dir` has taken its armed hold in `seam` (the seams
/// are shared by the tests running at once: each looks at its own).
fn taken(seam: &std::sync::Mutex<Vec<super::super::range_follow::HeldChunk>>, dir: &Path) -> bool {
    !seam.lock().unwrap().iter().any(|((d, _), _)| d == dir)
}

/// The records move `mv1`'s copy has written in `db` (the head's `n`).
fn records(db: &pigeonhole::Pigeonhole) -> Option<Vec<u8>> {
    db.table(super::super::identity::TABLE)
        .ok()?
        .open()
        .ok()?
        .get(b"copy/mv1", "moved", b"n")
        .ok()?
        .map(|c| c.value().to_vec())
}

/// Partition `p`'s view of move `id` on the node at `port`, as its `Status` reports it.
async fn move_status(port: u16, p: &str, id: &str) -> Option<crate::serve::proto::MoveStatus> {
    client(port)
        .await
        .status(StatusRequest::default())
        .await
        .ok()?
        .into_inner()
        .partitions
        .into_iter()
        .find(|lp| lp.id == p)?
        .moves
        .into_iter()
        .find(|m| m.id == id)
}

/// D331 PR 4b: a range move's target copies its range from the source chunk by chunk, then
/// catches up on the source's history of it from the copy's first token through the move's
/// last seqno: writes during the copy and the catch-up all arrive, and nothing outside the
/// range does. The progress rows sit beside the moved-away and serving rows without
/// showing in either (cli692's M4). The move records `from_epoch`, which the hand-over
/// raises the source past at HANDED_OVER, and the follower goes on (cli692 on #2107).
#[test]
fn a_range_moves_target_copies_its_range_and_catches_up_to_the_last_seqno() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut v1 = the_map(1, &[ports[0], ports[1], ports[2]]);
    v1.partitions.retain(|p| p.id != "p3");
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::super::write_map(dir, &super::super::map::canonical_json(&v1)).unwrap();
    }
    let mut it = listeners.into_iter();
    let (_stop_a, db_a) = data_node(&rt, &dirs[0], "a", Some("p1"), it.next().unwrap());
    let (_stop_b, db_b) = data_node(&rt, &dirs[1], "b", Some("p2"), it.next().unwrap());
    let (_stop_c, _) = data_node(&rt, &dirs[2], "c", None, it.next().unwrap());
    let (db_a, db_b) = (db_a.unwrap(), db_b.unwrap());
    let (b, c) = (ports[1], ports[2]);
    rt.block_on(async {
        eventually("p1 takes writes", async || {
            try_put(c, "d", "0").await.is_ok()
        })
        .await;
        // The range's bounds and both sides of each: `c` is in, `f` is out.
        for row in ["b", "c", "e", "f"] {
            try_put(c, row, "0").await.unwrap();
        }
        // COPYING: b's follower copies [c, f) from a.
        push(&ports, &moving_from(&v1, 2, Some(MovePhase::Copying), 1)).await;
        try_put(c, "d", "1").await.unwrap();
        eventually("b copies the range", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.copy_done)
        })
        .await;
        let s = move_status(b, "p2", "mv1").await.unwrap();
        assert!(s.copied_bytes > 0, "{s:?}");
        // CATCHING_UP: a write now arrives by the catch-up. So do the range's first row and
        // not the row at its end (feed1248's S2 on #2107: the bounds on the stream itself).
        push(&ports, &moving_from(&v1, 3, Some(MovePhase::CatchingUp), 1)).await;
        try_put(c, "e", "2").await.unwrap();
        try_put(c, "c", "3").await.unwrap();
        try_put(c, "f", "3").await.unwrap();
        try_put(c, "zed", "2").await.unwrap();
        // HANDING_OVER, then HANDED_OVER with p1's last seqno before the fence.
        push(
            &ports,
            &moving_from(&v1, 4, Some(MovePhase::HandingOver), 1),
        )
        .await;
        let last = db_a.visible_seqno();
        let mut handed = moving_from(&v1, 5, Some(MovePhase::HandedOver), 1);
        handed.moves[0].final_seqno = last;
        push(&ports, &handed).await;
        eventually("b's catch-up passes the move's last seqno", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.source_applied_seqno >= last)
        })
        .await;
        let mut cleaning = moving_from(&v1, 6, Some(MovePhase::Cleaning), 1);
        cleaning.moves[0].final_seqno = last;
        push(&ports, &cleaning).await;
        eventually("p2 serves the range", async || {
            get_row(c, "e")
                .await
                .is_ok_and(|v| v == Some(b"2".to_vec()))
        })
        .await;
        assert_eq!(get_row(c, "c").await.unwrap(), Some(b"3".to_vec()));
        assert_eq!(get_row(c, "d").await.unwrap(), Some(b"1".to_vec()));
        assert_eq!(
            get_row(c, "f").await.unwrap(),
            Some(b"3".to_vec()),
            "f is p1's still"
        );
    });
    // Nothing outside the range reached p2 (`zed` is p2's own, past the split at `m`).
    let t = db_b.table("users").unwrap().open().unwrap();
    for row in ["b", "f"] {
        assert!(t.get(row.as_bytes(), "f", b"q").unwrap().is_none(), "{row}");
    }
    // M4: the progress rows are in `moved`, and neither reader sees them.
    let ids: Vec<String> = crate::serve::ranges::read_serving(&db_b)
        .unwrap()
        .into_iter()
        .map(|m| m.id)
        .collect();
    assert_eq!(ids, ["mv1"]);
    assert!(crate::serve::ranges::read_moved(&db_b).unwrap().is_empty());
    let progress = db_b
        .table(super::super::identity::TABLE)
        .unwrap()
        .open()
        .unwrap();
    assert!(
        progress
            .get(b"copy/mv1", "moved", b"done")
            .unwrap()
            .is_some()
    );
    assert!(
        progress
            .get(b"follow/mv1", "moved", b"token")
            .unwrap()
            .is_some()
    );
    let _ = (db_a.close(), db_b.close());
}

/// B4 (D331 PR 4b), on an aggregate family: increments that land after the copy's first
/// token and before a later chunk's snapshot are in that chunk's copy and in the catch-up
/// both. The catch-up passes over them, so the moved counter is exact. The follower is held
/// before its second chunk while they land.
#[test]
fn a_counter_moved_in_a_later_chunk_counts_each_increment_once() {
    use super::several::{add_counter, counter, incr_row};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut v1 = the_map(1, &[ports[0], ports[1], ports[2]]);
    v1.partitions.retain(|p| p.id != "p3");
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::super::write_map(dir, &super::super::map::canonical_json(&v1)).unwrap();
    }
    let mut it = listeners.into_iter();
    let (_stop_a, db_a) = data_node(&rt, &dirs[0], "a", Some("p1"), it.next().unwrap());
    let (_stop_b, db_b) = data_node(&rt, &dirs[1], "b", Some("p2"), it.next().unwrap());
    let (_stop_c, _) = data_node(&rt, &dirs[2], "c", None, it.next().unwrap());
    let (db_a, db_b) = (db_a.unwrap(), db_b.unwrap());
    let (b, c) = (ports[1], ports[2]);
    let incr = async |row: &str, by: i64| {
        data(c)
            .await
            .mutate(crate::serve::proto::MutateRequest {
                writes: Some(incr_row(row, by)),
                durability: 0,
            })
            .await
            .unwrap();
    };
    rt.block_on(async {
        eventually("p1 takes writes", async || {
            try_put(c, "c", "0").await.is_ok()
        })
        .await;
        add_counter(&ports, c).await;
        // Rows before `d` large enough that the first chunk ends before it.
        let big = "x".repeat(700 << 10);
        for row in ["c1", "c2", "c3", "c4"] {
            try_put(c, row, &big).await.unwrap();
        }
        for _ in 0..5 {
            incr("d", 1).await;
        }
        let base = client(c)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        let v = base.map_version;
        // Held before chunk 1: chunk 0 (and the copy's first token) is taken, `d` isn't.
        let (release, held) = tokio::sync::oneshot::channel();
        super::super::range_follow::HOLD_CHUNK
            .lock()
            .unwrap()
            .push(((dirs[1].clone(), 2), held));
        push(&ports, &moving(&base, v + 1, Some(MovePhase::Copying))).await;
        eventually("b copies the first chunk", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.copied_bytes > 0)
        })
        .await;
        for _ in 0..5 {
            incr("d", 1).await;
        }
        let _ = release.send(());
        eventually("b copies the range", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.copy_done)
        })
        .await;
        push(&ports, &moving(&base, v + 2, Some(MovePhase::CatchingUp))).await;
        for _ in 0..5 {
            incr("d", 1).await;
        }
        push(&ports, &moving(&base, v + 3, Some(MovePhase::HandingOver))).await;
        let last = db_a.visible_seqno();
        let mut handed = moving(&base, v + 4, Some(MovePhase::HandedOver));
        handed.moves[0].final_seqno = last;
        push(&ports, &handed).await;
        eventually("b's catch-up passes the move's last seqno", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.source_applied_seqno >= last)
        })
        .await;
        let mut cleaning = moving(&base, v + 5, Some(MovePhase::Cleaning));
        cleaning.moves[0].final_seqno = last;
        push(&ports, &cleaning).await;
        eventually("p2 serves the range", async || {
            get_row(c, "c")
                .await
                .is_ok_and(|v| v == Some(b"0".to_vec()))
        })
        .await;
        assert_eq!(counter(c, "d").await, 15, "each increment counted once");
    });
    let _ = (db_a.close(), db_b.close());
}

/// simp724 (D331 PR 4b): a target that restarts between two chunks, and again during the
/// catch-up, resumes from its progress rows (the follower keeps nothing else), on an
/// aggregate family: each increment still counts once.
#[test]
fn a_target_restarted_between_chunks_and_in_its_catch_up_resumes_and_counts_once() {
    use super::several::{add_counter, counter, incr_row};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut v1 = the_map(1, &[ports[0], ports[1], ports[2]]);
    v1.partitions.retain(|p| p.id != "p3");
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::super::write_map(dir, &super::super::map::canonical_json(&v1)).unwrap();
    }
    let mut it = listeners.into_iter();
    let (_stop_a, db_a) = data_node(&rt, &dirs[0], "a", Some("p1"), it.next().unwrap());
    let (stop_b, db_b) = data_node(&rt, &dirs[1], "b", Some("p2"), it.next().unwrap());
    let (_stop_c, _) = data_node(&rt, &dirs[2], "c", None, it.next().unwrap());
    let (db_a, db_b) = (db_a.unwrap(), db_b.unwrap());
    let (b, c) = (ports[1], ports[2]);
    let incr = async |row: &str, by: i64| {
        data(c)
            .await
            .mutate(crate::serve::proto::MutateRequest {
                writes: Some(incr_row(row, by)),
                durability: 0,
            })
            .await
            .unwrap();
    };
    // b's server stops, then starts again on its port over the same database.
    let restart = |stop: tokio::sync::oneshot::Sender<()>, db: pigeonhole::Pigeonhole| {
        let _ = stop.send(());
        rt.block_on(eventually("b to stop", async || {
            tokio::net::TcpStream::connect(("127.0.0.1", b))
                .await
                .is_err()
        }));
        let listener = HeldPort::take(b).listen();
        let limits = crate::serve::server_tests::limits();
        let (stop, _) = data_node_on(
            &rt,
            &dirs[1],
            "b",
            Some("p2"),
            listener,
            limits,
            Some(db.clone()),
        );
        (stop, db)
    };
    let base = rt.block_on(async {
        eventually("p1 takes writes", async || {
            try_put(c, "c", "0").await.is_ok()
        })
        .await;
        add_counter(&ports, c).await;
        let big = "x".repeat(700 << 10);
        for row in ["c1", "c2", "c3", "c4"] {
            try_put(c, row, &big).await.unwrap();
        }
        for _ in 0..5 {
            incr("d", 1).await;
        }
        client(c)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner()
    });
    let v = base.map_version;
    // Held before chunk 1 for good: the old follower, stopped with its server, leaves the
    // hold and ends without a commit, and a new one copies the rest.
    let (_held_for_good, held) = tokio::sync::oneshot::channel::<()>();
    super::super::range_follow::HOLD_CHUNK
        .lock()
        .unwrap()
        .push(((dirs[1].clone(), 2), held));
    rt.block_on(async {
        push(&ports, &moving(&base, v + 1, Some(MovePhase::Copying))).await;
        eventually("b copies the first chunk", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.copied_bytes > 0)
        })
        .await;
        for _ in 0..5 {
            incr("d", 1).await;
        }
    });
    let (stop_b, db_b) = restart(stop_b, db_b);
    rt.block_on(async {
        eventually("b's new follower copies the rest", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.copy_done)
        })
        .await;
        push(&ports, &moving(&base, v + 2, Some(MovePhase::CatchingUp))).await;
        for _ in 0..5 {
            incr("d", 1).await;
        }
        eventually("b's catch-up starts", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.source_applied_seqno > 0)
        })
        .await;
    });
    let (_stop_b, db_b) = restart(stop_b, db_b);
    rt.block_on(async {
        for _ in 0..5 {
            incr("d", 1).await;
        }
        push(&ports, &moving(&base, v + 3, Some(MovePhase::HandingOver))).await;
        let last = db_a.visible_seqno();
        let mut handed = moving(&base, v + 4, Some(MovePhase::HandedOver));
        handed.moves[0].final_seqno = last;
        push(&ports, &handed).await;
        eventually("b's catch-up passes the move's last seqno", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.source_applied_seqno >= last)
        })
        .await;
        let mut cleaning = moving(&base, v + 5, Some(MovePhase::Cleaning));
        cleaning.moves[0].final_seqno = last;
        push(&ports, &cleaning).await;
        eventually("p2 serves the range", async || {
            get_row(c, "c")
                .await
                .is_ok_and(|v| v == Some(b"0".to_vec()))
        })
        .await;
        assert_eq!(counter(c, "d").await, 20, "each increment counted once");
    });
    let _ = (db_a.close(), db_b.close());
}

/// feed1248's G1 and cli692's M3 (D331 PR 4b): a range move's catch-up stream is served only
/// raw, only by the source's owner, only for exactly a live move's bounds, and only from a
/// token. A range bound on any other forwarded subscription is refused.
#[test]
fn a_raw_catch_up_stream_is_served_only_for_a_live_moves_exact_range() {
    use crate::serve::proto::{ForwardSubscribeRequest, SubscribeRequest, subscribe_request};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut v1 = the_map(1, &[ports[0], ports[1], ports[2]]);
    v1.partitions.retain(|p| p.id != "p3");
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::super::write_map(dir, &super::super::map::canonical_json(&v1)).unwrap();
    }
    let mut it = listeners.into_iter();
    let (_stop_a, db_a) = data_node(&rt, &dirs[0], "a", Some("p1"), it.next().unwrap());
    let (_stop_b, db_b) = data_node(&rt, &dirs[1], "b", Some("p2"), it.next().unwrap());
    let (_stop_c, _) = data_node(&rt, &dirs[2], "c", None, it.next().unwrap());
    let (db_a, db_b) = (db_a.unwrap(), db_b.unwrap());
    let (a, b, c) = (ports[0], ports[1], ports[2]);
    let token = db_a
        .table("users")
        .unwrap()
        .open()
        .unwrap()
        .feed()
        .open()
        .unwrap()
        .token()
        .as_bytes()
        .to_vec();
    let req =
        |raw: bool, end: &str, start: Option<subscribe_request::Start>| ForwardSubscribeRequest {
            partition: "p1".into(),
            request: Some(SubscribeRequest {
                tables: vec!["users".into()],
                start,
                ..Default::default()
            }),
            range_start: Some(text("c")),
            range_end: Some(text(end)),
            raw,
            ..Default::default()
        };
    let from_token = || Some(subscribe_request::Start::Resume(token.clone().into()));
    rt.block_on(async {
        eventually("p1 takes writes", async || {
            try_put(c, "d", "0").await.is_ok()
        })
        .await;
        push(&ports, &moving(&v1, 2, Some(MovePhase::Copying))).await;
        let refused = |port: u16, r: ForwardSubscribeRequest| async move {
            client(port)
                .await
                .forward_subscribe(r)
                .await
                .err()
                .map(|e| code(&e).to_owned())
        };
        assert_eq!(
            refused(a, req(false, "f", from_token())).await.as_deref(),
            Some("InvalidArgument"),
            "a range bound without raw"
        );
        assert_eq!(
            refused(a, req(true, "g", from_token())).await.as_deref(),
            Some("WrongPartition"),
            "bounds no move has"
        );
        assert_eq!(
            refused(b, req(true, "f", from_token())).await.as_deref(),
            Some("WrongPartition"),
            "not the source's owner"
        );
        assert_eq!(
            refused(
                a,
                req(true, "f", Some(subscribe_request::Start::FromOldest(true)))
            )
            .await
            .as_deref(),
            Some("InvalidArgument"),
            "not from a token"
        );
        assert_eq!(
            refused(a, req(true, "f", from_token())).await,
            None,
            "the move's own stream"
        );
        // D331 PR 5: an aborting move has no stream (ABORTING is above HANDED_OVER in
        // number, out of the phases' order).
        push(&ports, &moving(&v1, 3, Some(MovePhase::Aborting))).await;
        eventually("a refuses the aborting move's stream", async || {
            refused(a, req(true, "f", from_token())).await.as_deref() == Some("FailedPrecondition")
        })
        .await;
    });
    let _ = (db_a.close(), db_b.close());
}

/// simp724's M2 (D331 PR 4b): a map that takes the move away stops its follower between
/// commits. Held after its second chunk arrived and before its first commit, and woken
/// only by that map's stop: it commits nothing more, and its records stay the first
/// chunk's.
#[test]
fn a_follower_stopped_mid_copy_commits_nothing_more() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut v1 = the_map(1, &[ports[0], ports[1], ports[2]]);
    v1.partitions.retain(|p| p.id != "p3");
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::super::write_map(dir, &super::super::map::canonical_json(&v1)).unwrap();
    }
    let mut it = listeners.into_iter();
    let (_stop_a, db_a) = data_node(&rt, &dirs[0], "a", Some("p1"), it.next().unwrap());
    let (_stop_b, db_b) = data_node(&rt, &dirs[1], "b", Some("p2"), it.next().unwrap());
    let (_stop_c, _) = data_node(&rt, &dirs[2], "c", None, it.next().unwrap());
    let (db_a, db_b) = (db_a.unwrap(), db_b.unwrap());
    let (b, c) = (ports[1], ports[2]);
    let mut recorded_before = None;
    // Never released: only the follower's stop wakes it from the hold. The sender must live
    // to the test's end: a bare `_` would drop it at once, which wakes the hold too, with no
    // stop at all (cli692 on #2115).
    let (_held_for_good, held) = tokio::sync::oneshot::channel::<()>();
    super::super::range_follow::HOLD_COMMIT
        .lock()
        .unwrap()
        .push(((dirs[1].clone(), 2), held));
    rt.block_on(async {
        eventually("p1 takes writes", async || {
            try_put(c, "c", "0").await.is_ok()
        })
        .await;
        let big = "x".repeat(700 << 10);
        for row in ["c1", "c2", "c3", "c4"] {
            try_put(c, row, &big).await.unwrap();
        }
        try_put(c, "d", "1").await.unwrap();
        push(&ports, &moving(&v1, 2, Some(MovePhase::Copying))).await;
        // The follower holds its second chunk: the first chunk's commits are all in (#2107's
        // Windows flake: a count read while they still landed moved after it).
        eventually_within(
            Duration::from_secs(60),
            "b holds its second chunk",
            async || taken(&super::super::range_follow::HOLD_COMMIT, &dirs[1]),
        )
        .await;
        // The move goes from the map (an abort at COPYING) while the chunk is in hand. The
        // hold is never released, so only the stop wakes the follower, and its own check
        // must keep it from committing.
        let mut aborted = v1.clone();
        aborted.map_version = 3;
        push(&ports, &aborted).await;
        eventually("b takes the abort", async || {
            move_status(b, "p2", "mv1").await.is_none()
        })
        .await;
        recorded_before = records(&db_b);
        tokio::time::sleep(Duration::from_millis(500)).await;
    });
    let users = db_b.table("users").unwrap().open().unwrap();
    assert!(
        users.get(b"d", "f", b"q").unwrap().is_none(),
        "the second chunk wasn't written"
    );
    // No record after the stop: the first chunk's, or none once the abort's reconcile
    // (D331 PR 5) pruned them.
    let after = records(&db_b);
    assert!(
        after.is_none() || after == recorded_before,
        "a record after the stop"
    );
    assert!(recorded_before.is_some(), "the first chunk's records");
    let _ = (db_a.close(), db_b.close());
}

/// D331 PR 5: an aborting move is before its commit point and inactive. Taken mid-copy, the
/// follower stops between commits and records nothing more; the copy server lends no more
/// chunks; the target serves none of the range and writes no serving row; the source
/// serves it, unfenced, and hands nothing over.
#[test]
fn an_aborting_move_stops_its_follower_and_leaves_the_range_its_sources() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut v1 = the_map(1, &[ports[0], ports[1], ports[2]]);
    v1.partitions.retain(|p| p.id != "p3");
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::super::write_map(dir, &super::super::map::canonical_json(&v1)).unwrap();
    }
    let mut it = listeners.into_iter();
    let (_stop_a, db_a) = data_node(&rt, &dirs[0], "a", Some("p1"), it.next().unwrap());
    let (_stop_b, db_b) = data_node(&rt, &dirs[1], "b", Some("p2"), it.next().unwrap());
    let (_stop_c, _) = data_node(&rt, &dirs[2], "c", None, it.next().unwrap());
    let (db_a, db_b) = (db_a.unwrap(), db_b.unwrap());
    let (a, b, c) = (ports[0], ports[1], ports[2]);
    let mut recorded_before = None;
    // Never released, as in a_follower_stopped_mid_copy_commits_nothing_more: only the
    // follower's stop wakes it.
    let (_held_for_good, held) = tokio::sync::oneshot::channel::<()>();
    super::super::range_follow::HOLD_COMMIT
        .lock()
        .unwrap()
        .push(((dirs[1].clone(), 2), held));
    rt.block_on(async {
        eventually("p1 takes writes", async || {
            try_put(c, "c", "0").await.is_ok()
        })
        .await;
        let big = "x".repeat(700 << 10);
        for row in ["c1", "c2", "c3", "c4"] {
            try_put(c, row, &big).await.unwrap();
        }
        try_put(c, "d", "1").await.unwrap();
        push(&ports, &moving(&v1, 2, Some(MovePhase::Copying))).await;
        eventually_within(
            Duration::from_secs(60),
            "b holds its second chunk",
            async || taken(&super::super::range_follow::HOLD_COMMIT, &dirs[1]),
        )
        .await;
        push(&ports, &moving(&v1, 3, Some(MovePhase::Aborting))).await;
        eventually("b takes the abort", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.phase == MovePhase::Aborting as i32)
        })
        .await;
        recorded_before = records(&db_b);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let t = move_status(b, "p2", "mv1").await.unwrap();
        assert!(!t.serving, "the target serves none of it");
        let s = move_status(a, "p1", "mv1").await.unwrap();
        assert!(s.serving && !s.fenced, "the source serves it, unfenced");
        // The source takes the range's writes (the target's gate refusing them is
        // ranges.rs's an_aborting_move_gates_its_target_and_leaves_its_source_serving).
        try_put(c, "d", "2").await.unwrap();
    });
    let users = db_b.table("users").unwrap().open().unwrap();
    assert!(
        users.get(b"d", "f", b"q").unwrap().is_none(),
        "the second chunk wasn't written"
    );
    let after = records(&db_b);
    assert!(
        after.is_none() || after == recorded_before,
        "a record after the stop"
    );
    assert!(recorded_before.is_some(), "the first chunk's records");
    assert!(
        !crate::serve::ranges::read_serving(&db_b)
            .unwrap()
            .iter()
            .any(|r| r.id == "mv1"),
        "no serving row on the target"
    );
    assert!(
        crate::serve::ranges::read_moved(&db_a).unwrap().is_empty(),
        "the source wrote no moved-away row"
    );
    assert_eq!(
        super::super::identity::read(&db_a).unwrap().unwrap().epoch,
        1,
        "the source's epoch wasn't raised"
    );
    let _ = (db_a.close(), db_b.close());
}

/// Three nodes as the PR 4b tests run them: p1 on a, p2 on b, c routing. The databases of a
/// and b, the map, the ports, the directories and the stop senders.
struct Three {
    rt: tokio::runtime::Runtime,
    v1: ClusterMap,
    ports: Vec<u16>,
    dirs: Vec<PathBuf>,
    db_a: pigeonhole::Pigeonhole,
    db_b: pigeonhole::Pigeonhole,
    _stops: Vec<tokio::sync::oneshot::Sender<()>>,
}

fn three() -> Three {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut v1 = the_map(1, &[ports[0], ports[1], ports[2]]);
    v1.partitions.retain(|p| p.id != "p3");
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::super::write_map(dir, &super::super::map::canonical_json(&v1)).unwrap();
    }
    let mut it = listeners.into_iter();
    let (s_a, db_a) = data_node(&rt, &dirs[0], "a", Some("p1"), it.next().unwrap());
    let (s_b, db_b) = data_node(&rt, &dirs[1], "b", Some("p2"), it.next().unwrap());
    let (s_c, _) = data_node(&rt, &dirs[2], "c", None, it.next().unwrap());
    rt.block_on(eventually("p1 takes writes", async || {
        try_put(ports[2], "c", "0").await.is_ok()
    }));
    Three {
        rt,
        v1,
        ports,
        dirs,
        db_a: db_a.unwrap(),
        db_b: db_b.unwrap(),
        _stops: vec![s_a, s_b, s_c],
    }
}

/// D331 PR 5 (feedback F1–F3): a source at HANDING_OVER reports its fence's seqno once the
/// writes admitted before the fence finished, at or past every write to the range it took;
/// writes outside the range go on, and the seqno it reports stays the same.
#[test]
fn a_fenced_source_reports_its_fence_seqno_past_every_range_write() {
    let t = three();
    let (a, c) = (t.ports[0], t.ports[2]);
    t.rt.block_on(async {
        for i in 0..20 {
            try_put(c, "d", &i.to_string()).await.unwrap();
        }
        push(&t.ports, &moving(&t.v1, 2, Some(MovePhase::Copying))).await;
        push(&t.ports, &moving(&t.v1, 3, Some(MovePhase::CatchingUp))).await;
        try_put(c, "e", "last").await.unwrap();
        let last = t.db_a.visible_seqno();
        assert_eq!(
            move_status(a, "p1", "mv1").await.map(|s| s.fence_seqno),
            Some(0),
            "no fence before HANDING_OVER"
        );
        push(&t.ports, &moving(&t.v1, 4, Some(MovePhase::HandingOver))).await;
        let mut seqno = 0;
        eventually("a reports its fence seqno", async || {
            seqno = move_status(a, "p1", "mv1")
                .await
                .map_or(0, |s| s.fence_seqno);
            seqno != 0
        })
        .await;
        assert!(seqno >= last, "{seqno} < {last}");
        // The range is fenced; the rest of p1 takes writes, and the seqno stays.
        assert_eq!(code(&try_put(c, "d", "x").await.unwrap_err()), "Fenced");
        try_put(c, "a", "1").await.unwrap();
        assert!(t.db_a.visible_seqno() > seqno);
        assert_eq!(
            move_status(a, "p1", "mv1").await.map(|s| s.fence_seqno),
            Some(seqno)
        );
    });
}

/// D331 PR 5: at CLEANING the source drops the moved range from a drop row, keeps the rest
/// of the partition, says so (`dropped`), and clears the row.
#[test]
fn a_cleaning_source_drops_the_moved_range_and_says_so() {
    let t = three();
    let (a, c) = (t.ports[0], t.ports[2]);
    t.rt.block_on(async {
        for row in ["c1", "d", "e9"] {
            try_put(c, row, "v").await.unwrap();
        }
        try_put(c, "a", "kept").await.unwrap();
        push(&t.ports, &moving(&t.v1, 2, Some(MovePhase::Copying))).await;
        finish_move(&t, &t.v1, 3).await;
        eventually("a drops the moved range", async || {
            move_status(a, "p1", "mv1").await.is_some_and(|s| s.dropped)
        })
        .await;
    });
    let users = t.db_a.table("users").unwrap().open().unwrap();
    for row in [&b"c1"[..], b"d", b"e9"] {
        assert!(
            users.get(row, "f", b"q").unwrap().is_none(),
            "{row:?} left on S"
        );
    }
    assert!(
        users.get(b"a", "f", b"q").unwrap().is_some(),
        "outside the range"
    );
    assert!(
        super::super::range_drop::read_pending(&t.db_a)
            .unwrap()
            .is_empty()
    );
    // T serves it.
    let target = t.db_b.table("users").unwrap().open().unwrap();
    assert!(target.get(b"d", "f", b"q").unwrap().is_some());
}

/// simp724's M2 on the D331 PR 5 design: a target taking an aborting map stops its
/// follower, drops what it copied of the range, prunes its progress rows and says so; a
/// later move of the same range copies afresh.
#[test]
fn an_aborting_moves_target_drops_its_copy_and_a_new_move_copies_afresh() {
    let t = three();
    let (b, c) = (t.ports[1], t.ports[2]);
    t.rt.block_on(async {
        for row in ["c1", "d", "e9"] {
            try_put(c, row, "v").await.unwrap();
        }
        push(&t.ports, &moving(&t.v1, 2, Some(MovePhase::Copying))).await;
        eventually("b copies the range", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.copy_done)
        })
        .await;
        push(&t.ports, &moving(&t.v1, 3, Some(MovePhase::Aborting))).await;
        eventually("b drops its copy", async || {
            move_status(b, "p2", "mv1").await.is_some_and(|s| s.dropped)
        })
        .await;
    });
    let users = t.db_b.table("users").unwrap().open().unwrap();
    assert!(
        users.get(b"d", "f", b"q").unwrap().is_none(),
        "the copy is dropped"
    );
    assert!(
        super::super::range_follow::copies(&t.db_b)
            .unwrap()
            .is_empty()
    );
    assert!(
        super::super::range_drop::read_pending(&t.db_b)
            .unwrap()
            .is_empty()
    );
    // The record goes; a new move of the range copies it again, whole.
    t.rt.block_on(async {
        let mut v4 = t.v1.clone();
        v4.map_version = 4;
        push(&t.ports, &v4).await;
        let mut again = moving(&t.v1, 5, Some(MovePhase::Copying));
        again.moves[0].id = "mv2".into();
        push(&t.ports, &again).await;
        eventually("b copies the range again", async || {
            move_status(b, "p2", "mv2")
                .await
                .is_some_and(|s| s.copy_done)
        })
        .await;
    });
    assert!(users.get(b"d", "f", b"q").unwrap().is_some());
}

/// simp724's M2: a target down when the abort lands (the map it comes back to has no
/// record) drops its copy at open, from its own progress rows, before it serves.
#[test]
fn a_target_down_when_its_move_aborted_drops_its_copy_at_open() {
    let t = three();
    let (b, c) = (t.ports[1], t.ports[2]);
    let Three {
        rt,
        v1,
        ports,
        dirs,
        db_b,
        mut _stops,
        ..
    } = t;
    rt.block_on(async {
        for row in ["c1", "d"] {
            try_put(c, row, "v").await.unwrap();
        }
        push(&ports, &moving(&v1, 2, Some(MovePhase::Copying))).await;
        eventually("b copies the range", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.copy_done)
        })
        .await;
    });
    let _ = _stops.swap_remove(1).send(());
    rt.block_on(eventually("b to stop", async || {
        tokio::net::TcpStream::connect(("127.0.0.1", b))
            .await
            .is_err()
    }));
    // The abort's maps landed while b was down: the last has no record.
    let mut gone = v1.clone();
    gone.map_version = 4;
    super::super::write_map(&dirs[1], &super::super::map::canonical_json(&gone)).unwrap();
    let listener = HeldPort::take(b).listen();
    let (_stop, _) = data_node_on(
        &rt,
        &dirs[1],
        "b",
        Some("p2"),
        listener,
        crate::serve::server_tests::limits(),
        Some(db_b.clone()),
    );
    rt.block_on(eventually("b drops the copy", async || {
        super::super::range_follow::copies(&db_b).is_ok_and(|c| c.is_empty())
            && super::super::range_drop::read_pending(&db_b).is_ok_and(|p| p.is_empty())
    }));
    let users = db_b.table("users").unwrap().open().unwrap();
    assert!(users.get(b"d", "f", b"q").unwrap().is_none());
    let _ = db_b.close();
}

/// D331 PR 5: `phdb cluster move-range` end to end, under writes. Every write the cluster
/// acknowledged is read back from the target; the source dropped its copy; the map gives
/// the range to the target, with the source's epoch raised, and no record is left.
#[test]
fn move_range_moves_a_range_under_writes_end_to_end() {
    let t = three();
    let (a, c) = (t.ports[0], t.ports[2]);
    let acked: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>> = Default::default();
    t.rt.block_on(async {
        for i in 0..50 {
            try_put(c, &format!("d{i:02}"), "0").await.unwrap();
        }
        // Writes into the range and outside it all through the move; a fenced one is
        // retried, as a client does.
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let (acked, stop) = (std::sync::Arc::clone(&acked), std::sync::Arc::clone(&stop));
            tokio::spawn(async move {
                let mut i = 0u32;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let (row, value) = (format!("d{:02}", i % 50), i.to_string());
                    try_put(c, "a", &value).await.ok();
                    if try_put(c, &row, &value).await.is_ok() {
                        acked.lock().unwrap().push((row, value));
                    }
                    i += 1;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
        };
        let spec = super::super::range_move::Spec {
            table: "users".into(),
            from: "p1".into(),
            to: "p2".into(),
            start: Some(b"c".to_vec()),
            end: Some(b"f".to_vec()),
        };
        super::super::range_move::run(
            &super::backup::args(a),
            &spec,
            Some(Duration::from_secs(120)),
            true,
        )
        .await
        .unwrap();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        writer.await.unwrap();
        // The last acknowledged value of each row reads back, from the target.
        let mut last = std::collections::HashMap::new();
        for (row, value) in acked.lock().unwrap().iter() {
            last.insert(row.clone(), value.clone());
        }
        assert!(!last.is_empty());
        for (row, value) in &last {
            assert_eq!(
                get_row(c, row).await.unwrap(),
                Some(value.as_bytes().to_vec()),
                "row {row}"
            );
        }
        let m = client(a)
            .await
            .get_map(crate::serve::proto::GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        assert!(m.moves.is_empty(), "{:?}", m.moves);
        let users = &m.tables[0];
        assert!(super::super::map::inside(users, b"c", Some(b"f"), "p2"));
        assert_eq!(m.partitions[0].epoch, 2, "the source's epoch rose");
    });
    let src = t.db_a.table("users").unwrap().open().unwrap();
    assert!(
        src.get(b"d00", "f", b"q").unwrap().is_none(),
        "the source dropped its copy"
    );
    assert!(src.get(b"a", "f", b"q").unwrap().is_some());
}

/// `row`'s puts of `cells` values of `bytes` each, qualifiers `q0`.. (one commit).
fn wide_row(row: &str, cells: usize, bytes: usize) -> crate::serve::proto::RowWrites {
    use crate::serve::proto::{Put, RowKey, RowWrites, Value, Write, value, write};
    RowWrites {
        row: Some(RowKey {
            table: "users".into(),
            row: row.as_bytes().to_vec().into(),
        }),
        writes: (0..cells)
            .map(|i| Write {
                op: Some(write::Op::Put(Put {
                    family: "f".into(),
                    qualifier: format!("q{i}").into_bytes().into(),
                    timestamp: None,
                    value: Some(Value {
                        kind: Some(value::Kind::Bytes(vec![b'w'; bytes].into())),
                    }),
                })),
            })
            .collect(),
    }
}

async fn mutate(port: u16, writes: crate::serve::proto::RowWrites) {
    data(port)
        .await
        .mutate(crate::serve::proto::MutateRequest {
            writes: Some(writes),
            durability: 0,
        })
        .await
        .unwrap();
}

/// The move from COPYING to CLEANING after `base` (versions from `v`), the follower's last
/// seqno checked, once its copy is done.
async fn finish_move(t: &Three, base: &ClusterMap, v: u64) {
    let (b, ports) = (t.ports[1], &t.ports);
    eventually("b copies the range", async || {
        move_status(b, "p2", "mv1")
            .await
            .is_some_and(|s| s.copy_done)
    })
    .await;
    push(ports, &moving(base, v, Some(MovePhase::CatchingUp))).await;
    push(ports, &moving(base, v + 1, Some(MovePhase::HandingOver))).await;
    let last = t.db_a.visible_seqno();
    let mut handed = moving(base, v + 2, Some(MovePhase::HandedOver));
    handed.moves[0].final_seqno = last;
    push(ports, &handed).await;
    eventually("b's catch-up passes the move's last seqno", async || {
        move_status(b, "p2", "mv1")
            .await
            .is_some_and(|s| s.source_applied_seqno >= last)
    })
    .await;
    let mut cleaning = moving(base, v + 3, Some(MovePhase::Cleaning));
    cleaning.moves[0].final_seqno = last;
    push(ports, &cleaning).await;
}

/// cli692's M-a on #2107, with simp724's and cli692's checks on its fix: nothing is copied
/// twice. Held before its second chunk, the follower has written the first chunk's rows,
/// each commit with its own record. Meanwhile S deletes one of them and compacts the delete
/// away, and counters are incremented in rows just past each of them (`last ++ 0x00`,
/// where a record ends). The deleted row reaches T through the catch-up, and each counter
/// counts each increment once.
#[test]
fn a_row_deleted_and_compacted_on_the_source_after_its_copy_is_gone_on_the_target() {
    use super::several::{add_counter, counter, incr_row};
    let t = three();
    let (b, c) = (t.ports[1], t.ports[2]);
    let big = "x".repeat(700 << 10);
    let base = t.rt.block_on(async {
        add_counter(&t.ports, c).await;
        for row in ["c1", "c2", "c3", "c4"] {
            try_put(c, row, &big).await.unwrap();
        }
        client(c)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner()
    });
    let v = base.map_version;
    let (release, held) = tokio::sync::oneshot::channel::<()>();
    super::super::range_follow::HOLD_CHUNK
        .lock()
        .unwrap()
        .push(((t.dirs[1].clone(), 2), held));
    let gaps = ["c\0", "c1\0", "c2\0", "c3\0", "c4\0"];
    t.rt.block_on(async {
        push(&t.ports, &moving(&base, v + 1, Some(MovePhase::Copying))).await;
        eventually("b copies the first chunk", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.copied_bytes > 0)
        })
        .await;
        let mut gone = put_row("c1", "");
        gone.writes = vec![crate::serve::proto::Write {
            op: Some(crate::serve::proto::write::Op::DeleteRow(
                crate::serve::proto::DeleteRow {},
            )),
        }];
        mutate(c, gone).await;
        for g in gaps {
            for _ in 0..5 {
                mutate(c, incr_row(g, 1)).await;
            }
        }
    });
    t.db_a.flush().unwrap();
    t.db_a.compact().unwrap();
    drop(release);
    t.rt.block_on(async {
        finish_move(&t, &base, v + 2).await;
        eventually("p2 serves the range", async || {
            get_row(c, "c2").await.is_ok_and(|v| v.is_some())
        })
        .await;
        assert_eq!(
            get_row(c, "c1").await.unwrap(),
            None,
            "deleted on the source"
        );
        for g in gaps {
            assert_eq!(counter(c, g).await, 5, "{g:?} counted once");
        }
    });
    let _ = (t.db_a.close(), t.db_b.close());
}

/// simp724 on the M-a fix: a stop that comes inside a row spanning several commits, with
/// little of it left, waits for the row, so nothing is left half-copied, and the move goes
/// on once the map gives it back.
#[test]
fn a_stop_inside_a_small_split_row_finishes_the_row_and_the_move_goes_on() {
    let t = three();
    let (b, c) = (t.ports[1], t.ports[2]);
    let (release, held) = tokio::sync::oneshot::channel::<()>();
    super::super::range_follow::HOLD_ROW
        .lock()
        .unwrap()
        .push(((t.dirs[1].clone(), 1), held));
    t.rt.block_on(async {
        // Three cells of 200 KB: three commits, about 600 KB, under the 1 MiB a stop waits for.
        mutate(c, wide_row("d", 3, 200 << 10)).await;
        push(&t.ports, &moving(&t.v1, 2, Some(MovePhase::Copying))).await;
        eventually_within(
            Duration::from_secs(60),
            "b holds inside the row",
            async || taken(&super::super::range_follow::HOLD_ROW, &t.dirs[1]),
        )
        .await;
        let mut aborted = t.v1.clone();
        aborted.map_version = 3;
        push(&t.ports, &aborted).await;
        let _ = release.send(());
        tokio::time::sleep(Duration::from_millis(500)).await;
        // The move again: the row was finished, so the copy goes on.
        let mut back = t.v1.clone();
        back.map_version = 3;
        push(&t.ports, &moving(&back, 4, Some(MovePhase::Copying))).await;
        eventually("b copies the range", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.copy_done && s.follower_error.is_empty())
        })
        .await;
    });
    let users = t.db_b.table("users").unwrap().open().unwrap();
    for q in ["q0", "q1", "q2"] {
        assert!(users.get(b"d", "f", q.as_bytes()).unwrap().is_some(), "{q}");
    }
    let _ = (t.db_a.close(), t.db_b.close());
}

/// simp724 on the M-a fix: a stop inside a row with more of it left than a stop waits for
/// leaves the row marked partial, as a crash would, and the next follower stops the move,
/// naming the row, rather than copy any of it again.
#[test]
fn a_row_left_partly_copied_stops_the_move_naming_it() {
    let t = three();
    let (b, c) = (t.ports[1], t.ports[2]);
    let (release, held) = tokio::sync::oneshot::channel::<()>();
    super::super::range_follow::HOLD_ROW
        .lock()
        .unwrap()
        .push(((t.dirs[1].clone(), 1), held));
    t.rt.block_on(async {
        // Six cells of 250 KB: about 1.5 MB, past the 1 MiB a stop waits for. A commit
        // each, under the request limit; one row all the same.
        for i in 0..6 {
            let mut one = wide_row("d", 1, 250 << 10);
            if let Some(crate::serve::proto::write::Op::Put(p)) = &mut one.writes[0].op {
                p.qualifier = format!("q{i}").into_bytes().into();
            }
            mutate(c, one).await;
        }
        push(&t.ports, &moving(&t.v1, 2, Some(MovePhase::Copying))).await;
        eventually_within(
            Duration::from_secs(60),
            "b holds inside the row",
            async || taken(&super::super::range_follow::HOLD_ROW, &t.dirs[1]),
        )
        .await;
    });
    // b's server stops inside the row, past the 1 MiB a stop finishes (D331 PR 5: an abort
    // would drop the copy, so a restart under the same record leaves the row partial).
    let Three {
        rt,
        dirs,
        db_a,
        db_b,
        mut _stops,
        ..
    } = t;
    let _ = _stops.swap_remove(1).send(());
    let _ = release.send(());
    rt.block_on(eventually("b to stop", async || {
        tokio::net::TcpStream::connect(("127.0.0.1", b))
            .await
            .is_err()
    }));
    let (_stop_b2, _) = data_node_on(
        &rt,
        &dirs[1],
        "b",
        Some("p2"),
        HeldPort::take(b).listen(),
        crate::serve::server_tests::limits(),
        Some(db_b.clone()),
    );
    rt.block_on(async {
        eventually("b's follower stops, naming the row", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.follower_error.contains("row \"d\""))
        })
        .await;
        let s = move_status(b, "p2", "mv1").await.unwrap();
        assert!(!s.copy_done, "{s:?}");
    });
    let _ = (c, db_a.close(), db_b.close());
}

/// `moving` with the move's `from_epoch` set: the source's epoch when it began copying.
fn moving_from(v1: &ClusterMap, version: u64, phase: Option<MovePhase>, epoch: u64) -> ClusterMap {
    let mut m = moving(v1, version, phase);
    for mv in &mut m.moves {
        mv.from_epoch = epoch;
    }
    m
}

/// cli692's M1 on the design, tested (M-b on #2107): every follower commit goes through the
/// target's gate. Fenced with a chunk in hand, the follower writes nothing; unfenced, the
/// copy goes on.
#[test]
fn a_fenced_target_takes_no_copy_until_unfenced() {
    let t = three();
    let (b, c) = (t.ports[1], t.ports[2]);
    let (release, held) = tokio::sync::oneshot::channel::<()>();
    super::super::range_follow::HOLD_COMMIT
        .lock()
        .unwrap()
        .push(((t.dirs[1].clone(), 2), held));
    let big = "x".repeat(700 << 10);
    let mut before = None;
    t.rt.block_on(async {
        for row in ["c1", "c2", "c3", "c4"] {
            try_put(c, row, &big).await.unwrap();
        }
        try_put(c, "d", "1").await.unwrap();
        push(&t.ports, &moving(&t.v1, 2, Some(MovePhase::Copying))).await;
        eventually_within(
            Duration::from_secs(60),
            "b holds its second chunk",
            async || taken(&super::super::range_follow::HOLD_COMMIT, &t.dirs[1]),
        )
        .await;
        client(b)
            .await
            .fence(PartitionRequest {
                partition: "p2".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        before = records(&t.db_b);
        let _ = release.send(());
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(records(&t.db_b), before, "nothing written while fenced");
        let users = t.db_b.table("users").unwrap().open().unwrap();
        assert!(users.get(b"d", "f", b"q").unwrap().is_none());
        client(b)
            .await
            .unfence(PartitionRequest {
                partition: "p2".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        eventually("b copies the range once unfenced", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.copy_done)
        })
        .await;
    });
    let users = t.db_b.table("users").unwrap().open().unwrap();
    assert!(users.get(b"d", "f", b"q").unwrap().is_some());
    let _ = (t.db_a.close(), t.db_b.close());
}

/// cli692's M4 (M-b on #2107), across a reopen: the target's progress rows sit in `moved`
/// beside the gates' rows, and a target reopened mid-copy gates nothing by them: its own
/// rows take writes and reads as before.
#[test]
fn a_target_reopened_mid_copy_gates_nothing_by_its_progress_rows() {
    let t = three();
    let (b, c) = (t.ports[1], t.ports[2]);
    let (stop_b, db_b) = {
        let mut stops = t._stops;
        let s = stops.remove(1);
        ((s, stops), t.db_b.clone())
    };
    let (stop_b, rest) = stop_b;
    t.rt.block_on(async {
        try_put(c, "d", "1").await.unwrap();
        push(&t.ports, &moving(&t.v1, 2, Some(MovePhase::Copying))).await;
        eventually("b copies the range", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.copy_done)
        })
        .await;
    });
    assert!(records(&db_b).is_some(), "progress rows written");
    let _ = stop_b.send(());
    t.rt.block_on(eventually("b to stop", async || {
        tokio::net::TcpStream::connect(("127.0.0.1", b))
            .await
            .is_err()
    }));
    let listener = HeldPort::take(b).listen();
    let limits = crate::serve::server_tests::limits();
    let (_stop_b2, _) = data_node_on(
        &t.rt,
        &t.dirs[1],
        "b",
        Some("p2"),
        listener,
        limits,
        Some(db_b.clone()),
    );
    t.rt.block_on(async {
        eventually("p2 takes its own rows' writes again", async || {
            try_put(c, "zed", "2").await.is_ok()
        })
        .await;
        assert_eq!(get_row(c, "zed").await.unwrap(), Some(b"2".to_vec()));
    });
    assert!(crate::serve::ranges::read_moved(&db_b).unwrap().is_empty());
    drop(rest);
    let _ = (t.db_a.close(), db_b.close());
}

/// cli692's S2 on #2107: a source that fails over mid-move (its epoch moves off the one the
/// copy began at) stops the follower for good, and `MoveStatus.follower_error` says why.
#[test]
fn a_source_failed_over_mid_catch_up_stops_the_follower() {
    let t = three();
    let b = t.ports[1];
    t.rt.block_on(async {
        push(
            &t.ports,
            &moving_from(&t.v1, 2, Some(MovePhase::Copying), 1),
        )
        .await;
        eventually("b copies the range", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.copy_done)
        })
        .await;
        push(
            &t.ports,
            &moving_from(&t.v1, 3, Some(MovePhase::CatchingUp), 1),
        )
        .await;
        let mut moved = moving_from(&t.v1, 4, Some(MovePhase::CatchingUp), 1);
        moved
            .partitions
            .iter_mut()
            .filter(|p| p.id == "p1")
            .for_each(|p| p.epoch = 2);
        push(&t.ports, &moved).await;
        eventually("b's follower stops, saying why", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.follower_error.contains("failed over"))
        })
        .await;
    });
    let _ = (t.db_a.close(), t.db_b.close());
}

/// cli692's S6 and the coordinator's D34 rule on #2108: a commit straddling the range's end
/// brings only its in-range row, and two increments at one timestamp in two source commits
/// both count, however the catch-up batches them.
#[test]
fn a_straddling_commit_brings_its_in_range_part_and_same_timestamp_increments_both_count() {
    use super::several::{add_counter, counter};
    use crate::serve::proto::{BatchRequest, Incr, RowKey, RowWrites, Write, write};
    let t = three();
    let (b, c) = (t.ports[1], t.ports[2]);
    let incr_at = |ts: u64| RowWrites {
        row: Some(RowKey {
            table: "users".into(),
            row: b"d".to_vec().into(),
        }),
        writes: vec![Write {
            op: Some(write::Op::Incr(Incr {
                family: "n".into(),
                qualifier: b"c".to_vec().into(),
                timestamp: Some(ts),
                delta: 1,
            })),
        }],
    };
    let base = t.rt.block_on(async {
        add_counter(&t.ports, c).await;
        client(c)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner()
    });
    let v = base.map_version;
    t.rt.block_on(async {
        push(&t.ports, &moving(&base, v + 1, Some(MovePhase::Copying))).await;
        eventually("b copies the range", async || {
            move_status(b, "p2", "mv1")
                .await
                .is_some_and(|s| s.copy_done)
        })
        .await;
        // During the catch-up: one commit with e (in) and g (out); then two increments of
        // d at one timestamp, as two commits.
        data(c)
            .await
            .batch(BatchRequest {
                rows: vec![put_row("e", "5"), put_row("g", "5")],
                durability: 0,
            })
            .await
            .unwrap();
        mutate(c, incr_at(1_000_000)).await;
        mutate(c, incr_at(1_000_000)).await;
        finish_move(&t, &base, v + 2).await;
        eventually("p2 serves the range", async || {
            get_row(c, "e")
                .await
                .is_ok_and(|v| v == Some(b"5".to_vec()))
        })
        .await;
        assert_eq!(counter(c, "d").await, 2, "both increments");
        assert_eq!(
            get_row(c, "g").await.unwrap(),
            Some(b"5".to_vec()),
            "g is p1's"
        );
    });
    let users = t.db_b.table("users").unwrap().open().unwrap();
    assert!(
        users.get(b"g", "f", b"q").unwrap().is_none(),
        "g never reached p2"
    );
    let _ = (t.db_a.close(), t.db_b.close());
}

/// The entries of `dir/.phdb-dropped/` for partition `p`.
fn dropped_files(dir: &Path, p: &str) -> Vec<String> {
    std::fs::read_dir(dir.join(super::super::close::DROPPED))
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with(&format!("{p}-")))
                .collect()
        })
        .unwrap_or_default()
}

/// `phdb cluster split`'s create on `port`'s node: `p`, marked for `move_id` at `version`.
async fn create(port: u16, p: &str, move_id: &str, version: u64) -> Result<(), tonic::Status> {
    client(port)
        .await
        .stamp(crate::serve::proto::StampRequest {
            cluster_id: "c1".into(),
            partition: p.into(),
            from_epoch: 0,
            to_epoch: 1,
            create: true,
            create_move: move_id.into(),
            create_version: version,
            ..Default::default()
        })
        .await
        .map(drop)
}

/// `t`'s map at version 2, with `users`' family `f` in its placement, as a table created
/// through the cluster has: what a new partition's schema reconcile creates it from.
async fn with_families(t: &Three) {
    let mut v2 = t.v1.clone();
    v2.map_version = 2;
    v2.tables[0].families = vec![crate::serve::proto::FamilySpec {
        name: "f".into(),
        ..Default::default()
    }];
    push(&t.ports, &v2).await;
}

/// D331 PR 5: `phdb cluster split` end to end, under writes. p1's range of `users` splits at
/// "d" into a new partition p1-<v> on node c (the default id, v the version of the change
/// that recorded the split): every acknowledged write reads back, the map places it on c
/// holding `[d, m)`, p1 dropped its copy, and no record is
/// left.
#[test]
fn split_moves_a_range_into_a_new_partition_under_writes_end_to_end() {
    let t = three();
    let (a, c) = (t.ports[0], t.ports[2]);
    let acked: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>> = Default::default();
    t.rt.block_on(async {
        with_families(&t).await;
        for i in 0..50 {
            try_put(c, &format!("d{i:02}"), "0").await.unwrap();
        }
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let (acked, stop) = (std::sync::Arc::clone(&acked), std::sync::Arc::clone(&stop));
            tokio::spawn(async move {
                let mut i = 0u32;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let (row, value) = (format!("d{:02}", i % 50), i.to_string());
                    try_put(c, "a", &value).await.ok();
                    if try_put(c, &row, &value).await.is_ok() {
                        acked.lock().unwrap().push((row, value));
                    }
                    i += 1;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
        };
        let spec = super::super::range_move::SplitSpec {
            table: "users".into(),
            from: "p1".into(),
            key: b"d".to_vec(),
            node: "c".into(),
            id: None,
        };
        super::super::range_move::split(
            &super::backup::args(a),
            &spec,
            Some(Duration::from_secs(120)),
            true,
        )
        .await
        .unwrap();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        writer.await.unwrap();
        let mut last = std::collections::HashMap::new();
        for (row, value) in acked.lock().unwrap().iter() {
            last.insert(row.clone(), value.clone());
        }
        assert!(!last.is_empty());
        for (row, value) in &last {
            assert_eq!(
                get_row(c, row).await.unwrap(),
                Some(value.as_bytes().to_vec()),
                "row {row}"
            );
        }
        let m = client(a)
            .await
            .get_map(crate::serve::proto::GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        assert!(m.moves.is_empty(), "{:?}", m.moves);
        // `p1-<v>`, v the version of the change that recorded the split.
        let new = m
            .partitions
            .iter()
            .find(|p| p.node == "c" && p.id.starts_with("p1-"))
            .map(|p| p.id.clone())
            .unwrap_or_else(|| panic!("{:?}", m.partitions));
        assert!(
            new["p1-".len()..]
                .parse::<u64>()
                .is_ok_and(|v| v > 1 && v < m.map_version),
            "{new}, map version {}",
            m.map_version
        );
        assert!(super::super::map::inside(
            &m.tables[0],
            b"d",
            Some(b"m"),
            &new
        ));
        assert!(super::super::map::inside(
            &m.tables[0],
            b"a",
            Some(b"d"),
            "p1"
        ));
    });
    let src = t.db_a.table("users").unwrap().open().unwrap();
    assert!(
        src.get(b"d00", "f", b"q").unwrap().is_none(),
        "p1 dropped its copy"
    );
    assert!(src.get(b"a", "f", b"q").unwrap().is_some());
}

/// D331 PR 5 (cli692's Must 5): a split aborted before its commit point takes its new
/// partition with it: the record (of kind PARTITION) stays ABORTING until the new partition
/// p1-<v> dropped what it copied, then the map drops the record and it together, and its
/// file is moved aside on c. p1 serves
/// the range as before.
#[test]
fn an_aborted_split_removes_its_new_partition_and_moves_its_file_aside() {
    let t = three();
    let (a, c) = (t.ports[0], t.ports[2]);
    t.rt.block_on(async {
        for i in 0..20 {
            try_put(c, &format!("d{i:02}"), "0").await.unwrap();
        }
        let args = super::backup::args(a);
        let spec = super::super::range_move::SplitSpec {
            table: "users".into(),
            from: "p1".into(),
            key: b"d".to_vec(),
            node: "c".into(),
            id: None,
        };
        let before = client(a)
            .await
            .get_map(crate::serve::proto::GetMapRequest {})
            .await
            .unwrap()
            .into_inner()
            .map_version;
        let (id, new) = super::super::range_move::begin_split(
            &args,
            &spec,
            &super::super::moving::Start {
                prefix: "split",
                ..Default::default()
            },
        )
        .await
        .unwrap();
        // Nothing else changes the map here: the change recording it is the next one.
        assert_eq!(new, format!("p1-{}", before + 1));
        let recorded = client(a)
            .await
            .get_map(crate::serve::proto::GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        let r = recorded.moves.iter().find(|r| r.id == id).unwrap();
        assert!(
            super::super::range_move::is_split(r) && r.to == new,
            "{r:?}"
        );
        super::super::range_move::abort(&args, &id, Some(Duration::from_secs(60)))
            .await
            .unwrap();
        let m = client(a)
            .await
            .get_map(crate::serve::proto::GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        assert!(m.moves.is_empty(), "{:?}", m.moves);
        assert!(
            !m.partitions.iter().any(|p| p.id == new),
            "{:?}",
            m.partitions
        );
        assert!(!t.dirs[2].join(format!("{new}.phdb")).exists());
        assert_eq!(dropped_files(&t.dirs[2], &new).len(), 1);
        try_put(c, "d05", "after").await.unwrap();
        assert_eq!(get_row(c, "d05").await.unwrap(), Some(b"after".to_vec()));
    });
}

/// D331 PR 5 (cli692, D332 §7.3): a split whose driver stopped once the map gave the range
/// over (HANDED_OVER, past its commit point) is finished by [`resume`] from the record's id
/// alone, as a new writer finishes one: the new partition holds the range, the record is
/// gone, and the rows read back through it.
///
/// [`resume`]: super::super::range_move::resume
#[test]
fn a_split_stopped_at_handed_over_is_finished_by_resume() {
    let t = three();
    let (a, c) = (t.ports[0], t.ports[2]);
    t.rt.block_on(async {
        with_families(&t).await;
        for i in 0..20 {
            try_put(c, &format!("d{i:02}"), "0").await.unwrap();
        }
        let args = super::backup::args(a);
        let spec = super::super::range_move::SplitSpec {
            table: "users".into(),
            from: "p1".into(),
            key: b"d".to_vec(),
            node: "c".into(),
            id: None,
        };
        super::super::range_move::STOP
            .lock()
            .unwrap()
            .push((args.node.clone(), MovePhase::HandedOver));
        let stopped =
            super::super::range_move::split(&args, &spec, Some(Duration::from_secs(120)), true)
                .await;
        assert!(stopped.is_err(), "the split ran past HANDED_OVER");
        let map = async || {
            client(a)
                .await
                .get_map(crate::serve::proto::GetMapRequest {})
                .await
                .unwrap()
                .into_inner()
        };
        let m = map().await;
        let r = m
            .moves
            .iter()
            .find(|r| super::super::range_move::is_split(r))
            .cloned()
            .unwrap_or_else(|| panic!("{:?}", m.moves));
        assert_eq!(super::super::map::phase(&r), MovePhase::HandedOver);
        super::super::range_move::resume(&args, &r.id, Some(Duration::from_secs(120)))
            .await
            .unwrap();
        let m = map().await;
        assert!(m.moves.is_empty(), "{:?}", m.moves);
        assert!(super::super::map::inside(
            &m.tables[0],
            b"d",
            Some(b"m"),
            &r.to
        ));
        assert_eq!(get_row(c, "d05").await.unwrap(), Some(b"0".to_vec()));
    });
}

/// D331 PR 5 (audit120's point 1, simp724's case): a split's create whose COPYING change
/// never lands leaves a marked, empty file no map places. The node moves it aside only once
/// it holds a map past the version the change would have carried; a map at that version
/// (another change won it) leaves it.
#[test]
fn a_split_created_file_whose_map_never_landed_is_moved_aside_past_its_version() {
    let t = three();
    let c = t.ports[2];
    t.rt.block_on(async {
        create(c, "p1-1", "split-9", 2).await.unwrap();
        assert!(t.dirs[2].join("p1-1.phdb").exists());
        let passes = super::super::open::missing_passes(&t.dirs[2]);
        let mut v2 = t.v1.clone();
        v2.map_version = 2;
        push(&t.ports, &v2).await;
        eventually("c's pass on v2", async || {
            super::super::open::missing_passes(&t.dirs[2]) > passes
        })
        .await;
        assert!(
            t.dirs[2].join("p1-1.phdb").exists(),
            "v2 isn't past the mark's version"
        );
        let mut v3 = t.v1.clone();
        v3.map_version = 3;
        push(&t.ports, &v3).await;
        eventually("c moves p1-1's file aside", async || {
            !t.dirs[2].join("p1-1.phdb").exists()
        })
        .await;
        assert_eq!(dropped_files(&t.dirs[2], "p1-1").len(), 1);
    });
}

/// D331 PR 5 (cli692's Must 5's test): the CLI stops between the map that drops an aborted
/// split's record and partition, and its drop-partition. The node's reconcile moves the
/// marked, empty, unmapped file aside on that map.
#[test]
fn an_aborted_splits_file_left_by_a_stopped_cli_is_moved_aside() {
    let t = three();
    let c = t.ports[2];
    t.rt.block_on(async {
        create(c, "p1-1", "split-9", 2).await.unwrap();
        let mut v2 = t.v1.clone();
        v2.map_version = 2;
        v2.partitions.push(crate::serve::proto::Partition {
            id: "p1-1".into(),
            node: "c".into(),
            epoch: 1,
            ..Default::default()
        });
        let mut r = RangeMove {
            id: "split-9".into(),
            kind: MoveKind::Partition as i32,
            table: "users".into(),
            start: Some(super::super::map::canonical_key(b"d")),
            end: Some(super::super::map::canonical_key(b"m")),
            from: "p1".into(),
            to: "p1-1".into(),
            from_epoch: 1,
            to_epoch: 1,
            ..RangeMove::default()
        };
        r.set_phase(MovePhase::Aborting);
        v2.moves.push(r);
        push(&t.ports, &v2).await;
        eventually("c opens p1-1", async || {
            client(c)
                .await
                .status(crate::serve::proto::StatusRequest::default())
                .await
                .is_ok_and(|s| s.into_inner().partitions.iter().any(|lp| lp.id == "p1-1"))
        })
        .await;
        client(c)
            .await
            .close(PartitionRequest {
                partition: "p1-1".into(),
                force: false,
            })
            .await
            .unwrap();
        // The removal map; the CLI stops before its drop-partition.
        let mut v3 = t.v1.clone();
        v3.map_version = 3;
        push(&t.ports, &v3).await;
        eventually("c moves p1-1's file aside", async || {
            !t.dirs[2].join("p1-1.phdb").exists()
        })
        .await;
        assert_eq!(dropped_files(&t.dirs[2], "p1-1").len(), 1);
    });
}

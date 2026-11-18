//! D331 PR 2: a range move's gates through every phase, across three nodes, with the maps
//! pushed by hand (the driver is PR 5's): the source fences the range's writes, then answers
//! it `WrongPartition`; the target serves it only from CLEANING; a node routing by an older
//! map catches up; and the source's hand-over raises its epoch and records the range in its
//! file.

use super::*;
use crate::serve::proto::{MoveKind, MovePhase, PushMapRequest, RangeMove};

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
    let handed = phase >= MovePhase::HandedOver;
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
        // HANDING_OVER: the range's writes are fenced, retry-safe; its reads and p1's other
        // keys go on.
        push(&ports, &moving(&v1, 4, Some(MovePhase::HandingOver))).await;
        let e = try_put(c, "d", "3").await.unwrap_err();
        assert_eq!(code(&e), "Fenced", "{e}");
        assert!(e.message().contains("move \"mv1\""), "{e}");
        assert_eq!(get_row(c, "d").await.unwrap(), Some(b"2".to_vec()));
        try_put(c, "alice", "1").await.unwrap();
        try_put(c, "zed", "1").await.unwrap();

        // HANDED_OVER reaches a and b, not c. c routes `d` to p1 by its older map; a answers
        // `WrongPartition`, c pulls the newer map and routes to p2, which doesn't serve the
        // range until CLEANING.
        push(&[a, b], &moving(&v1, 5, Some(MovePhase::HandedOver))).await;
        let e = try_put(c, "d", "4").await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        assert_eq!(version(c).await, 5, "c pulled the hand-over map");
        let e = get_row(c, "d").await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        // p1 asks for its lease at its new epoch at once, and serves its other keys again.
        eventually("p1 takes writes at epoch 2", async || {
            try_put(c, "alice", "2").await.is_ok()
        })
        .await;
        try_put(c, "zed", "2").await.unwrap();

        // CLEANING: p2 serves the range (empty here: PR 4 copies it), and takes its writes.
        push(&ports, &moving(&v1, 6, Some(MovePhase::Cleaning))).await;
        assert_eq!(get_row(c, "d").await.unwrap(), None);
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
    // p1's copy of the range wasn't written after the fence.
    let t = db_a.table("users").unwrap().open().unwrap();
    assert_eq!(
        t.get(b"d", "f", b"q").unwrap().map(|c| c.value().to_vec()),
        Some(b"2".to_vec())
    );
    let _ = db_a.close();
}

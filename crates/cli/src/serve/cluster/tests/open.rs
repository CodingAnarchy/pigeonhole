//! Opening a partition on a running node (D317 ticket 6, §3): a map that gives a node a
//! partition it doesn't have open opens it, fenced, and that map, naming the node at the
//! file's epoch, unfences it: the node serves it at once, its schema the map's.

use super::*;
use crate::serve::proto::{
    CreateTableRequest, FamilySpec, LocalPartitionState, TablePlacementRequest,
    schema_client::SchemaClient,
};

#[test]
fn a_map_that_gives_a_node_a_partition_opens_it_and_it_serves() {
    // a serves p1, b p2; c only routes, with p3's file in its directory.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    map.partitions.retain(|p| p.id != "p3");
    let dirs = [test_dir(), test_dir(), test_dir()];
    test_partition(&dirs[2], "p3").close().unwrap();
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, dir), (id, local)) in
        listeners
            .into_iter()
            .zip(&dirs)
            .zip([("a", Some("p1")), ("b", Some("p2")), ("c", None)])
    {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, db) = data_node_tokens(&rt, dir, id, local, l, "");
        stops.push(stop);
        dbs.extend(db);
    }
    let [a, b, c] = ports;
    rt.block_on(async {
        // Version 2 gives c partition p3, through a, the writer.
        client(a)
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: 1,
                map: Some(the_map(2, &ports)),
            })
            .await
            .unwrap();
        eventually("c to open p3 and serve it", async || {
            client(c)
                .await
                .status(StatusRequest {})
                .await
                .unwrap()
                .into_inner()
                .partitions
                .iter()
                .any(|lp| lp.id == "p3" && lp.state() == LocalPartitionState::Serving)
        })
        .await;
        // A table placed on p3 alone, through b: c's p3 takes it, and its rows.
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{b}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        SchemaClient::new(channel)
            .create_table(CreateTableRequest {
                name: "items".into(),
                families: vec![FamilySpec {
                    name: "f".into(),
                    ..Default::default()
                }],
                if_missing: false,
                placement: Some(TablePlacementRequest {
                    group: String::new(),
                    splits: Vec::new(),
                    partitions: vec!["p3".into()],
                }),
            })
            .await
            .unwrap();
        let mut w = put_row("pen", "1");
        w.row.as_mut().unwrap().table = "items".into();
        data(a)
            .await
            .mutate(crate::serve::proto::MutateRequest {
                writes: Some(w),
                durability: 0,
            })
            .await
            .unwrap();
        let got = data(b)
            .await
            .get(crate::serve::proto::GetRequest {
                table: "items".into(),
                row: b"pen".to_vec().into(),
                family: "f".into(),
                qualifier: b"q".to_vec().into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(got.cell.map(|c| c.value.to_vec()), Some(b"1".to_vec()));
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}

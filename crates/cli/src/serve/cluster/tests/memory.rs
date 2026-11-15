//! Agent memory on a cluster (#1915, ICR 0139): a store per partition the map places it on,
//! made from the node's `--memory`; each call routed to the partition holding its
//! namespace, through any node; and the map's writer adding a store only once every node
//! keeps it.

use super::*;
use crate::serve::proto::{
    ForgetRequest, MemoryPlacement, NewMemory, RecallRequest, RememberRequest,
    memory_client::MemoryClient,
};

/// `--memory notes`, no embedder or model: memories are recalled by keyword.
fn recipe() -> crate::serve::memslot::Recipe {
    crate::serve::memslot::Recipe {
        name: "notes".into(),
        embedder: None,
        create: Arc::new(|_| Ok(pigeonhole_memory::StoreOptions::new())),
        budget: (1 << 20, 8 << 20),
        args: crate::args::MemoryArgs::default(),
    }
}

/// The store `notes`, namespaces below "m" on p1 and the rest on p2.
fn placement() -> MemoryPlacement {
    MemoryPlacement {
        store: "notes".into(),
        splits: vec![text("m")],
        partitions: vec!["p1".into(), "p2".into()],
    }
}

async fn memory(port: u16) -> MemoryClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    MemoryClient::new(channel)
}

async fn remember(port: u16, ns: &str, text: &str) -> Result<Vec<u8>, tonic::Status> {
    memory(port)
        .await
        .remember(RememberRequest {
            namespace: ns.into(),
            memory: Some(NewMemory {
                text: text.into(),
                ..Default::default()
            }),
        })
        .await
        .map(|r| r.into_inner().id.to_vec())
}

async fn recall(port: u16, namespaces: &[&str], text: &str) -> Result<Vec<String>, tonic::Status> {
    memory(port)
        .await
        .recall(RecallRequest {
            namespaces: namespaces.iter().map(|n| (*n).to_owned()).collect(),
            text: text.into(),
            k: 10,
            ..Default::default()
        })
        .await
        .map(|r| r.into_inner().hits.into_iter().map(|h| h.text).collect())
}

/// The namespaces whose memory rows (`{ns}\0m…`) partition `db`'s store holds.
fn namespaces_in(db: &pigeonhole::Pigeonhole) -> Vec<String> {
    let t = db.table("mem_notes").unwrap().open().unwrap();
    let page = t.scan_prefix(b"").read_within(1 << 20).unwrap();
    let mut out: Vec<String> = Vec::new();
    for row in page.rows() {
        if let Some(i) = row.key().windows(2).position(|w| w == b"\0m") {
            let ns = String::from_utf8_lossy(&row.key()[..i]).into_owned();
            if !out.contains(&ns) {
                out.push(ns);
            }
        }
    }
    out
}

#[test]
fn a_memory_call_goes_to_the_partition_that_holds_its_namespace_through_any_node() {
    // a serves p1 (namespaces < "m"), b serves p2, c serves nothing and routes. Each runs
    // with --memory notes.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    map.partitions.retain(|p| p.id != "p3");
    map.memory = Some(placement());
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
    }
    let start = |dir: &Path, id: &str, local: Option<&str>, listener| {
        let placement = crate::serve::placement::Placement::routes_only(Some(2), Some(2));
        let locals = local.map(|p| (p, None)).into_iter().collect();
        let limits = crate::serve::server_tests::limits();
        data_node_served(
            &rt,
            dir,
            id,
            locals,
            listener,
            "",
            limits,
            false,
            placement,
            None,
            Some(recipe()),
        )
    };
    let mut it = listeners.into_iter();
    let (_stop_a, dbs_a, _) = start(&dirs[0], "a", Some("p1"), it.next().unwrap());
    let (_stop_b, dbs_b, _) = start(&dirs[1], "b", Some("p2"), it.next().unwrap());
    let (_stop_c, _, _) = start(&dirs[2], "c", None, it.next().unwrap());
    // Each partition's store is there before any call: the start's reconcile made it.
    for db in [&dbs_a[0], &dbs_b[0]] {
        assert!(db.tables().contains(&"mem_notes".to_owned()));
    }
    let [a, b, c] = ports;
    rt.block_on(async {
        // Written through the routing node, each to its namespace's partition.
        let alice = remember(c, "alice", "alice ate lunch at noon")
            .await
            .unwrap();
        remember(c, "zed", "zed skipped lunch").await.unwrap();
        // Read through every node, owner or not.
        for port in [a, b, c] {
            let hits = recall(port, &["alice"], "lunch").await.unwrap();
            assert_eq!(hits, ["alice ate lunch at noon"], "through {port}");
            let hits = recall(port, &["zed"], "lunch").await.unwrap();
            assert_eq!(hits, ["zed skipped lunch"], "through {port}");
        }
        // A recall across both partitions isn't served whole yet: a call per partition.
        let e = recall(c, &["alice", "zed"], "lunch").await.unwrap_err();
        assert_eq!(code(&e), "Unsupported", "{e}");
        assert!(e.message().contains("more than one partition"), "{e}");
        // An id carries its namespace: a forget through b goes to a's p1.
        let forgot = memory(b)
            .await
            .forget(ForgetRequest {
                ids: vec![alice.into()],
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(forgot.forgotten, 1);
        assert!(recall(a, &["alice"], "lunch").await.unwrap().is_empty());
        // A namespace the store would refuse is refused before it's routed (simp724's N1).
        for ns in ["", "a\0b"] {
            let e = remember(c, ns, "x").await.unwrap_err();
            assert_eq!(code(&e), "InvalidArgument", "{ns:?}: {e}");
        }
    });
    // Each memory is in its own partition's store, and only there: zed's in p2's, and p1's
    // holds none (alice's was forgotten).
    assert_eq!(namespaces_in(&dbs_a[0]), Vec::<String>::new());
    assert_eq!(namespaces_in(&dbs_b[0]), ["zed"]);
}

#[test]
fn a_cluster_without_a_placed_store_answers_memory_calls_unsupported() {
    // The node runs with --memory, but the map places no store: nothing is opened, and
    // a call says why.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut map = the_map(1, &[port, port, port]);
    map.nodes.truncate(1);
    map.partitions = map.partitions.into_iter().take(1).collect();
    map.tables[0].splits.clear();
    map.tables[0].partitions = vec!["p1".into()];
    let dir = test_dir();
    super::super::write_map(&dir, &super::super::map::canonical_json(&map)).unwrap();
    let placement = crate::serve::placement::Placement::routes_only(Some(2), Some(2));
    let (_stop, dbs, _) = data_node_served(
        &rt,
        &dir,
        "a",
        vec![("p1", None)],
        listener,
        "",
        crate::serve::server_tests::limits(),
        false,
        placement,
        None,
        Some(recipe()),
    );
    assert!(!dbs[0].tables().contains(&"mem_notes".to_owned()));
    rt.block_on(async {
        let e = remember(port, "alice", "x").await.unwrap_err();
        assert_eq!(code(&e), "Unsupported", "{e}");
        assert!(e.message().contains("no memory store"), "{e}");
    });
}

#[test]
fn the_writer_adds_a_memory_store_only_once_every_node_keeps_it() {
    // c isn't running: the writer (a) can't learn that it keeps `ClusterMap.memory`, so a
    // map adding a store is refused, naming c, and the map stays as it was. Once c runs a
    // build that does, the same map is set.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dirs = [test_dir(), test_dir(), test_dir()];
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let nodes = [
        node(&dirs[0], "a"),
        node(&dirs[1], "b"),
        node(&dirs[2], "c"),
    ];
    let mut listeners = listeners.into_iter();
    let _a = serve(&rt, &nodes[0], listeners.next().unwrap());
    let _b = serve(&rt, &nodes[1], listeners.next().unwrap());
    let held_c = listeners.next().unwrap();
    // c's port, closed: nothing answers there.
    let c_listener = {
        let addr = held_c.local_addr().unwrap();
        drop(held_c);
        addr
    };
    rt.block_on(async {
        let set = async |expected: u64, map: ClusterMap| {
            client(ports[0])
                .await
                .set_map(SetMapRequest {
                    expected_map_version: expected,
                    map: Some(map),
                    copies: Vec::new(),
                    force: false,
                })
                .await
        };
        let mut v1 = the_map(1, &ports);
        v1.memory = Some(placement());
        // A first map is checked too.
        let e = set(0, v1.clone()).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e:?}");
        assert!(e.message().contains("c (no answer"), "{e:?}");
        v1.memory = None;
        set(0, v1).await.unwrap();
        let mut v2 = the_map(2, &ports);
        v2.memory = Some(placement());
        let e = set(1, v2.clone()).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e:?}");
        assert!(e.message().contains("c (no answer"), "{e:?}");
        assert!(e.message().contains("Finish the upgrade"), "{e:?}");
        assert_eq!(version(ports[0]).await, 1);
        // c is up, on this build: the store goes in.
        let listener = std::net::TcpListener::bind(c_listener).unwrap();
        let _c = serve(&rt, &nodes[2], listener);
        set(1, v2).await.unwrap();
        assert_eq!(version(ports[0]).await, 2);
    });
}

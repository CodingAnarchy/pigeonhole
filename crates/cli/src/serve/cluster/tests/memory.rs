//! Agent memory on a cluster (#1915, ICR 0139): a store per partition the map places it on,
//! made from the node's `--memory`; each call routed to the partition holding its
//! namespace, through any node; and the map's writer adding a store only once every node
//! keeps it.

use super::*;
use crate::serve::proto::{
    ForgetRequest, LinkRequest, LinkType, MarkUsedRequest, MemoryPlacement, NewMemory,
    RecallRequest, RememberRequest, link_type, memory_client::MemoryClient,
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
        // A recall across both partitions is served whole, through any node (PR 2).
        for port in [a, b, c] {
            let mut hits = recall(port, &["alice", "zed"], "lunch").await.unwrap();
            hits.sort();
            assert_eq!(
                hits,
                ["alice ate lunch at noon", "zed skipped lunch"],
                "through {port}"
            );
        }
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

/// A one-node cluster on `port` with p1 and the store placed on it, its node started with
/// `memory` (`None`: without `--memory`) and serving `db` if given.
fn one_node(
    rt: &tokio::runtime::Runtime,
    listener: std::net::TcpListener,
    db: Option<pigeonhole::Pigeonhole>,
    memory: Option<crate::serve::memslot::Recipe>,
) -> (
    tokio::sync::oneshot::Sender<()>,
    Vec<pigeonhole::Pigeonhole>,
    PathBuf,
) {
    let port = listener.local_addr().unwrap().port();
    let mut map = the_map(1, &[port, port, port]);
    map.nodes.truncate(1);
    map.partitions.truncate(1);
    map.tables[0].splits.clear();
    map.tables[0].partitions = vec!["p1".into()];
    map.memory = Some(MemoryPlacement {
        store: "notes".into(),
        splits: Vec::new(),
        partitions: vec!["p1".into()],
    });
    let dir = test_dir();
    super::super::write_map(&dir, &super::super::map::canonical_json(&map)).unwrap();
    let placement = crate::serve::placement::Placement::routes_only(Some(2), Some(2));
    let (stop, dbs, _) = data_node_served(
        rt,
        &dir,
        "a",
        vec![("p1", db)],
        listener,
        "",
        crate::serve::server_tests::limits(),
        false,
        placement,
        None,
        memory,
    );
    (stop, dbs, dir)
}

/// p1's reconcile errors, as `Cluster.Status` gives them.
async fn reconcile_errors(port: u16) -> Vec<String> {
    let status = client(port)
        .await
        .status(super::super::proto::StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    let p1 = status.partitions.iter().find(|p| p.id == "p1").unwrap();
    p1.reconcile_errors.clone()
}

#[test]
fn a_partition_whose_store_doesnt_match_the_node_says_so_in_status_and_its_calls() {
    // simp724's ask 1 on #1920: p1's store is pinned to model m4, the node runs with
    // --memory-model other. The partition serves (it isn't failed), its Status says why its
    // store isn't, and its memory calls answer FailedPrecondition.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let dir = test_dir();
    let db = test_partition(&dir, "p1");
    let model = pigeonhole_memory::EmbeddingModel::new("m4", 4, pigeonhole::Element::F32).unwrap();
    let store = pigeonhole_memory::Store::create(
        &db,
        "notes",
        pigeonhole_memory::StoreOptions::new().embedding(model),
    )
    .unwrap();
    drop(store);
    let mut other = recipe();
    other.args.memory_model = Some("other".into());
    let (_stop, _dbs, _dir) = one_node(&rt, listener, Some(db), Some(other));
    rt.block_on(async {
        let errors = reconcile_errors(port).await;
        assert!(
            errors.iter().any(|e| e.contains("memory store notes")),
            "{errors:?}"
        );
        let e = remember(port, "alice", "x").await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        // The partition's data calls serve: only its store is refused.
        assert_eq!(get_row(port, "alice").await.unwrap(), None);
    });
}

#[test]
fn a_node_without_memory_holding_a_placed_partition_names_the_missing_flag() {
    // simp724's ask 2 on #1920: the map places the store on p1, whose node was started
    // without --memory. Its Status and its memory calls say so, not a generic Unsupported.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (_stop, dbs, _dir) = one_node(&rt, listener, None, None);
    assert!(!dbs[0].tables().contains(&"mem_notes".to_owned()));
    rt.block_on(async {
        let errors = reconcile_errors(port).await;
        assert!(
            errors
                .iter()
                .any(|e| e.contains("started without --memory")),
            "{errors:?}"
        );
        let e = remember(port, "alice", "x").await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert!(e.message().contains("--memory"), "{e}");
        let e = recall(port, &["alice"], "x").await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
    });
}

/// Three nodes as in the first test (a: p1, b: p2, c: routes only), the store's split at
/// "m", started with `--memory notes`: the stops, and each data node's partition database.
fn three_nodes(
    rt: &tokio::runtime::Runtime,
) -> (
    Vec<tokio::sync::oneshot::Sender<()>>,
    [pigeonhole::Pigeonhole; 2],
    [u16; 3],
) {
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
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((dir, (id, local)), listener) in dirs
        .iter()
        .zip([("a", Some("p1")), ("b", Some("p2")), ("c", None)])
        .zip(listeners)
    {
        let placement = crate::serve::placement::Placement::routes_only(Some(2), Some(2));
        let locals = local.map(|p| (p, None)).into_iter().collect();
        let (stop, d, _) = data_node_served(
            rt,
            dir,
            id,
            locals,
            listener,
            "",
            crate::serve::server_tests::limits(),
            false,
            placement,
            None,
            Some(recipe()),
        );
        stops.push(stop);
        dbs.extend(d);
    }
    let [p1, p2]: [pigeonhole::Pigeonhole; 2] = dbs.try_into().ok().unwrap();
    (stops, [p1, p2], ports)
}

/// The words the memories are made of.
const WORDS: &[&str] = &[
    "lunch", "deploy", "postgres", "meeting", "tuesday", "index", "budget", "agent",
];

#[test]
fn a_recall_across_partitions_is_the_steps_over_the_partitions_stores() {
    // The cluster's recall against `pigeonhole_memory`'s steps run here over the two
    // partitions' own stores: equal hits and scores, through every node, with and without
    // a graph expansion and a byte budget. (Its fusion is RRF, which ranks recency, so the
    // server's clock and this one's give the same order.) The steps themselves equal one
    // store's recall: `pigeonhole-memory`'s `recall_steps` test.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, [p1, p2], [a, b, c]) = three_nodes(&rt);
    let namespaces = ["alice", "bob", "yan", "zed"];
    rt.block_on(async {
        let mut x = 0x1915u64;
        let mut next = |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        let mut ids: std::collections::BTreeMap<&str, Vec<Vec<u8>>> = Default::default();
        for i in 0..60 {
            let ns = namespaces[i % 4];
            let words: Vec<&str> = (0..2 + next(3))
                .map(|_| WORDS[next(WORDS.len() as u64) as usize])
                .collect();
            let text = format!("{} {ns}{i}", words.join(" "));
            ids.entry(ns)
                .or_default()
                .push(remember(c, ns, &text).await.unwrap());
        }
        // Links within each namespace, a hub in each.
        for list in ids.values() {
            for (j, to) in list.iter().enumerate().skip(1) {
                let from = if j % 2 == 0 { &list[0] } else { &list[j - 1] };
                memory(c)
                    .await
                    .link(LinkRequest {
                        from: from.clone().into(),
                        to: to.clone().into(),
                        link_type: Some(LinkType {
                            kind: Some(link_type::Kind::Custom(16)),
                        }),
                        weight: (1 + next(1000)) as f32 / 1000.0,
                    })
                    .await
                    .unwrap();
            }
        }
        let open = |db: &pigeonhole::Pigeonhole| {
            pigeonhole_memory::Store::open_with(
                db,
                "notes",
                pigeonhole_memory::OpenOptions::new()
                    .without_embeddings()
                    .own_forgetter()
                    .own_rebalancer(),
            )
            .unwrap()
        };
        let stores = [(open(&p1), ["alice", "bob"]), (open(&p2), ["yan", "zed"])];
        // The cases where the byte budget cut the walk: it changed the hits.
        let mut budget_cut = 0;
        for (k, hops, budget) in [
            (10, 0, 0),
            (3, 0, 0),
            (25, 1, 0),
            (25, 2, 0),
            (40, 3, 0),
            (10, 1, 700),
            (40, 2, 3000),
            (12, 1, 1000),
            (25, 2, 1500),
            (30, 3, 2500),
        ] {
            let text = "lunch deploy postgres";
            let want = in_steps(&stores, text, k, hops, budget, true);
            assert!(!want.is_empty());
            if budget > 0 && hops > 0 && want != in_steps(&stores, text, k, hops, budget, false) {
                budget_cut += 1;
            }
            for port in [a, b, c] {
                let got: Vec<(Vec<u8>, f64)> = memory(port)
                    .await
                    .recall(RecallRequest {
                        namespaces: namespaces.iter().map(|n| (*n).to_owned()).collect(),
                        text: text.into(),
                        k: k as u32,
                        byte_budget: budget,
                        expand_hops: hops,
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .into_inner()
                    .hits
                    .into_iter()
                    .map(|h| (h.id.to_vec(), h.score))
                    .collect();
                assert_eq!(
                    got, want,
                    "k {k}, hops {hops}, budget {budget}, through {port}"
                );
            }
        }
        assert!(budget_cut > 0, "no case's budget cut its walk");
        // At most 64 namespaces across partitions (a recall on one keeps a single node's
        // rules).
        let many: Vec<String> = (0..65)
            .map(|i| format!("{}{i:02}", if i % 2 == 0 { "a" } else { "n" }))
            .collect();
        let e = memory(c)
            .await
            .recall(RecallRequest {
                namespaces: many,
                text: "lunch".into(),
                k: 5,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "InvalidArgument", "{e}");
        assert!(e.message().contains("at most 64"), "{e}");
    });
}

/// `pigeonhole_memory`'s steps over `stores` (each with its namespaces), as the entry node
/// runs them: `(id, score)` of each hit. `walk_budget` false: the walk has no byte budget
/// (to tell the cases where it cuts).
fn in_steps(
    stores: &[(pigeonhole_memory::Store, [&str; 2])],
    text: &str,
    k: usize,
    hops: u32,
    budget: u64,
    walk_budget: bool,
) -> Vec<(Vec<u8>, f64)> {
    use pigeonhole_memory::{Expand, Fusion, RecallBodies, RecallPool, Weights};
    let budget = if budget == 0 {
        recipe().budget.0
    } else {
        budget
    };
    let at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    let mk = |i: usize| {
        let (s, ns) = &stores[i];
        s.recall(ns).text(text).k(k).byte_budget(budget).now_us(at)
    };
    let pools = (0..stores.len()).map(|i| mk(i).gather().unwrap());
    let mut ranking = RecallPool::merge(pools)
        .unwrap()
        .rank(Fusion::default(), Weights::default());
    let store_of = |ns: &str| stores.iter().position(|(_, n)| n.contains(&ns)).unwrap();
    if hops > 0 {
        let mut walk = ranking.walk(Expand::hops(hops), walk_budget.then_some(budget));
        while let Some(hop) = walk.next_hop() {
            let parts = hop
                .split(store_of)
                .into_iter()
                .map(|(i, share)| mk(i).hop(&share).unwrap())
                .collect::<Vec<_>>();
            walk.settle(parts).unwrap();
        }
        ranking = walk.finish();
    }
    let top = ranking.top(k + 4);
    let bodies = (0..stores.len()).map(|i| {
        let ids: Vec<_> = top
            .iter()
            .filter(|id| store_of(id.namespace()) == i)
            .cloned()
            .collect();
        mk(i).bodies(&ids).unwrap()
    });
    ranking
        .finish(RecallBodies::merge(bodies), k, Some(budget), false)
        .0
        .into_iter()
        .map(|h| (h.id.to_bytes(), h.score))
        .collect()
}

#[test]
fn mark_used_and_forget_across_partitions_split_and_sum() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, [p1, p2], [a, _b, c]) = three_nodes(&rt);
    rt.block_on(async {
        let mut ids = Vec::new();
        for (ns, t) in [
            ("alice", "alice ate lunch"),
            ("bob", "bob ate lunch"),
            ("zed", "zed skipped lunch"),
        ] {
            ids.push(remember(c, ns, t).await.unwrap());
        }
        let marked = memory(a)
            .await
            .mark_used(MarkUsedRequest {
                ids: ids.iter().map(|i| i.clone().into()).collect(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(marked.marked, 3);
        // Forgotten across both, through the routing node; again, nothing's left.
        for want in [3, 0] {
            let forgot = memory(c)
                .await
                .forget(ForgetRequest {
                    ids: ids.iter().map(|i| i.clone().into()).collect(),
                })
                .await
                .unwrap()
                .into_inner();
            assert_eq!(forgot.forgotten, want);
        }
        assert!(
            recall(c, &["alice", "bob", "zed"], "lunch")
                .await
                .unwrap()
                .is_empty()
        );
    });
    assert_eq!(namespaces_in(&p1), Vec::<String>::new());
    assert_eq!(namespaces_in(&p2), Vec::<String>::new());
}

#[test]
fn a_split_memory_write_names_what_applied_and_says_when_a_part_may_have() {
    // a serves p1 and c routes; p2's node b is a closed port (nothing sent: the part
    // failed), then a listener that takes connections and never answers (sent, no answer:
    // the part may have applied). Either way the call names the partition that applied.
    let rt = tokio::runtime::Runtime::new().unwrap();
    for silent in [false, true] {
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
        let mut it = listeners.into_iter();
        let (la, lb, lc) = (it.next().unwrap(), it.next().unwrap(), it.next().unwrap());
        // b: closed, or silent (its accepted sockets held open, unread).
        let _held = if silent {
            Some(std::thread::spawn(move || {
                let mut held = Vec::new();
                for s in lb.incoming() {
                    held.push(s);
                }
            }))
        } else {
            drop(lb);
            None
        };
        let mut limits = crate::serve::server_tests::limits();
        limits.request_timeout = std::time::Duration::from_millis(800);
        let start = |dir: &Path, id: &str, local: Option<&str>, listener| {
            let placement = crate::serve::placement::Placement::routes_only(Some(2), Some(2));
            let locals = local.map(|p| (p, None)).into_iter().collect();
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
        let (_stop_a, _, _) = start(&dirs[0], "a", Some("p1"), la);
        let (_stop_c, _, _) = start(&dirs[2], "c", None, lc);
        rt.block_on(async {
            let alice = remember(ports[2], "alice", "alice ate lunch")
                .await
                .unwrap();
            // A memory id in zed's namespace (on p2): its part goes to b whether or not
            // such a memory exists.
            let zed = pigeonhole_memory::MemoryId::from_bytes(
                &[&3u16.to_le_bytes()[..], b"zed", &[7u8; 16]].concat(),
            )
            .unwrap()
            .to_bytes();
            let e = memory(ports[2])
                .await
                .forget(ForgetRequest {
                    ids: vec![alice.into(), zed.into()],
                })
                .await
                .unwrap_err();
            let want = if silent {
                "OutcomeUnknown"
            } else {
                "PartitionUnavailable"
            };
            assert_eq!(code(&e), want, "silent {silent}: {e}");
            assert!(e.message().contains("p2"), "silent {silent}: {e}");
            assert!(
                e.message().contains("applied on: p1 (1)"),
                "silent {silent}: {e}"
            );
            assert_eq!(
                e.message().contains("may or may not have applied"),
                silent,
                "{e}"
            );
            // p1's part applied.
            assert!(
                recall(ports[0], &["alice"], "lunch")
                    .await
                    .unwrap()
                    .is_empty()
            );
        });
    }
}

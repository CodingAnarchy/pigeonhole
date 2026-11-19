//! Vector and text indexes on a cluster (#1722 PR 1a, ICR 0138): an index placement in the
//! map, made through any node by the map's writer; each partition it's placed on creating a
//! whole index of its own range at reconcile; each write routed by its id; and the writer
//! adding the first placement only once every node keeps them.

use super::*;
use crate::serve::proto::{
    CreateTableRequest, CreateTextIndexRequest, CreateVectorIndexRequest, DropTableRequest,
    Element, FamilySpec, Metric, TablePlacementRequest, TextIndexRequest, TextPutRequest, Vector,
    VectorInsertRequest, VectorSearchRequest, VectorSpec, VectorVerifyRequest,
    schema_client::SchemaClient, text_client::TextClient, vectors_client::VectorsClient,
};

/// Three nodes: a serves p1, b serves p2, c only routes; `users` split at "m" over p1 and
/// p2. The stops, each data node's partition database, the ports, and the directories.
fn three_nodes(
    rt: &tokio::runtime::Runtime,
) -> (
    Vec<tokio::sync::oneshot::Sender<()>>,
    [pigeonhole::Pigeonhole; 2],
    [u16; 3],
    Vec<PathBuf>,
) {
    let (stops, dbs, ports, dirs) = nodes(rt, false);
    let [p1, p2]: [pigeonhole::Pigeonhole; 2] = dbs.try_into().ok().unwrap();
    (stops, [p1, p2], ports, dirs)
}

/// [`three_nodes`], and with `a_holds_p3` a also serves p3 (which no table is placed on):
/// its databases are then p1's, p3's and p2's, in that order.
fn nodes(
    rt: &tokio::runtime::Runtime,
    a_holds_p3: bool,
) -> (
    Vec<tokio::sync::oneshot::Sender<()>>,
    Vec<pigeonhole::Pigeonhole>,
    [u16; 3],
    Vec<PathBuf>,
) {
    nodes_limited(rt, a_holds_p3, crate::serve::server_tests::limits())
}

/// [`nodes`], each node with `limits`.
fn nodes_limited(
    rt: &tokio::runtime::Runtime,
    a_holds_p3: bool,
    limits: crate::serve::config::Limits,
) -> (
    Vec<tokio::sync::oneshot::Sender<()>>,
    Vec<pigeonhole::Pigeonhole>,
    [u16; 3],
    Vec<PathBuf>,
) {
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    if a_holds_p3 {
        for p in &mut map.partitions {
            if p.id == "p3" {
                p.node = "a".into();
            }
        }
    } else {
        map.partitions.retain(|p| p.id != "p3");
    }
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
    }
    let a_parts: &[&str] = if a_holds_p3 { &["p1", "p3"] } else { &["p1"] };
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((dir, (id, local)), listener) in dirs
        .iter()
        .zip([("a", a_parts), ("b", &["p2"][..]), ("c", &[][..])])
        .zip(listeners)
    {
        let placement = crate::serve::placement::Placement::routes_only(Some(2), Some(2));
        let locals = local.iter().map(|p| (*p, None)).collect();
        let (stop, d, _) = data_node_served(
            rt, dir, id, locals, listener, "", limits, false, placement, None, None, None,
        );
        stops.push(stop);
        dbs.extend(d);
    }
    (stops, dbs, ports, dirs)
}

async fn channel(port: u16) -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

/// `users` with its family `f`, made through the map (the map's placement has none yet).
async fn users_family(port: u16) {
    SchemaClient::new(channel(port).await)
        .create_table(CreateTableRequest {
            name: "users".into(),
            families: vec![FamilySpec {
                name: "f".into(),
                ..Default::default()
            }],
            if_missing: true,
            ..Default::default()
        })
        .await
        .unwrap();
}

fn vector_index(name: &str) -> CreateVectorIndexRequest {
    CreateVectorIndexRequest {
        name: name.into(),
        spec: Some(VectorSpec {
            dim: 2,
            element: Element::F32 as i32,
            metric: Metric::L2 as i32,
        }),
        ..Default::default()
    }
}

fn text_index(name: &str) -> CreateTextIndexRequest {
    CreateTextIndexRequest {
        name: name.into(),
        source_table: "users".into(),
        source_family: "f".into(),
        source_qualifier: b"body".to_vec().into(),
        ..Default::default()
    }
}

async fn insert(port: u16, index: &str, id: &[u8]) -> Result<(), tonic::Status> {
    let data: Vec<u8> = [1.0f32, 2.0].iter().flat_map(|x| x.to_le_bytes()).collect();
    VectorsClient::new(channel(port).await)
        .insert(VectorInsertRequest {
            index: index.into(),
            id: id.to_vec().into(),
            vector: Some(Vector {
                element: Element::F32 as i32,
                dim: 2,
                data: data.into(),
            }),
        })
        .await
        .map(drop)
}

/// The vectors partition `db`'s copy of index `name` holds.
fn vectors(db: &pigeonhole::Pigeonhole, name: &str) -> u64 {
    pigeonhole_vector::Index::open(db, name)
        .unwrap()
        .verify()
        .unwrap()
        .vectors
}

fn has_table(db: &pigeonhole::Pigeonhole, name: &str) -> bool {
    db.tables().iter().any(|t| t == name)
}

/// An index created through any node is placed in the map, and each partition of the table
/// it follows creates one of its range. A write through any node lands in the partition
/// holding its id, a text document's postings beside its source row. The followed table
/// can't be dropped while its indexes are there; dropping them drops them everywhere.
#[test]
fn an_index_created_through_any_node_holds_each_partitions_range() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, [p1, p2], [a, b, c], _) = three_nodes(&rt);
    rt.block_on(async {
        users_family(a).await;
        // A source row from before the text index: its partition fills it in the
        // background, a document at a time (#1756's resume), not in the reconcile.
        p1.table("users")
            .unwrap()
            .open()
            .unwrap()
            .mutate(b"alice")
            .put("f", b"body", b"lunch with the team")
            .commit()
            .unwrap();
        let mut v = vector_index("uv");
        v.follows = "users".into();
        VectorsClient::new(channel(c).await)
            .create_index(v.clone())
            .await
            .unwrap();
        TextClient::new(channel(b).await)
            .create_index(text_index("ut"))
            .await
            .unwrap();
        // Again: TableExists, or with if_missing the index as it is.
        let e = VectorsClient::new(channel(a).await)
            .create_index(v.clone())
            .await
            .unwrap_err();
        assert_eq!(code(&e), "TableExists", "{e}");
        v.if_missing = true;
        VectorsClient::new(channel(a).await)
            .create_index(v)
            .await
            .unwrap();
        // A table of an index's name: TableExists, as on a single node (audit120's S4).
        let e = SchemaClient::new(channel(c).await)
            .create_table(CreateTableRequest {
                name: "uv".into(),
                if_missing: true,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "TableExists", "{e}");
        // A text index over a family the table hasn't: refused at the writer, before the
        // map fixes a placement no partition can make (audit120's M3).
        let mut nofam = text_index("un");
        nofam.source_family = "g".into();
        let e = TextClient::new(channel(c).await)
            .create_index(nofam)
            .await
            .unwrap_err();
        assert_eq!(code(&e), "InvalidArgument", "{e}");
        assert!(e.message().contains("no family \"g\""), "{e}");
        eventually("both partitions create both indexes", async || {
            ["uv", "ut"]
                .iter()
                .all(|n| has_table(&p1, n) && has_table(&p2, n))
        })
        .await;

        // Writes by id, through every node: "alice" is p1's, "zed" p2's.
        for port in [a, b, c] {
            insert(port, "uv", b"alice").await.unwrap();
            insert(port, "uv", b"zed").await.unwrap();
        }
        assert_eq!((vectors(&p1, "uv"), vectors(&p2, "uv")), (1, 1));
        // Another schema change reconciles every partition again: the indexes it made are
        // the map's, and keep their contents.
        SchemaClient::new(channel(b).await)
            .create_table(CreateTableRequest {
                name: "other".into(),
                families: vec![FamilySpec {
                    name: "f".into(),
                    ..Default::default()
                }],
                placement: Some(TablePlacementRequest {
                    partitions: vec!["p1".into()],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        eventually("p1 creates the other table", async || {
            has_table(&p1, "other")
        })
        .await;
        assert_eq!((vectors(&p1, "uv"), vectors(&p2, "uv")), (1, 1));
        TextClient::new(channel(a).await)
            .put(TextPutRequest {
                index: "ut".into(),
                id: b"zed".to_vec().into(),
                text: "the meeting moved".into(),
            })
            .await
            .unwrap();
        let source = |db: &pigeonhole::Pigeonhole| {
            db.table("users")
                .unwrap()
                .open()
                .unwrap()
                .get(b"zed", "f", b"body")
                .unwrap()
                .is_some()
        };
        assert_eq!((source(&p1), source(&p2)), (false, true));
        let docs = |db: &pigeonhole::Pigeonhole| {
            pigeonhole_text::Index::open(db, "ut")
                .unwrap()
                .verify()
                .unwrap()
                .documents
        };
        eventually(
            "p1 fills its text index from the row before it",
            async || {
                docs(&p1) == 1
                    && !pigeonhole_text::Index::open(&p1, "ut")
                        .unwrap()
                        .rebuild_pending()
                        .unwrap()
            },
        )
        .await;
        assert_eq!((docs(&p1), docs(&p2)), (1, 1));

        // A search through any node fans out to the partitions its prefix meets, and the
        // hits merge by (distance, id); a check sums every partition's counts (PR 1b).
        let mut vectors = VectorsClient::new(channel(c).await);
        let search = |prefix: &[u8], stats| VectorSearchRequest {
            index: "uv".into(),
            query: Some(Vector {
                element: Element::F32 as i32,
                dim: 2,
                data: [1.0f32, 2.0]
                    .iter()
                    .flat_map(|x| x.to_le_bytes())
                    .collect::<Vec<u8>>()
                    .into(),
            }),
            k: 5,
            id_prefix: prefix.to_vec().into(),
            stats,
            ..Default::default()
        };
        let ids = |r: crate::serve::proto::VectorSearchResponse| -> Vec<Vec<u8>> {
            r.hits.into_iter().map(|h| h.id.to_vec()).collect()
        };
        let all = vectors
            .search(search(b"", true))
            .await
            .unwrap()
            .into_inner();
        let lists = |db: &pigeonhole::Pigeonhole| {
            pigeonhole_vector::Index::open(db, "uv")
                .unwrap()
                .verify()
                .unwrap()
                .list_lens
                .len() as u32
        };
        assert_eq!(all.stats.unwrap().lists, lists(&p1) + lists(&p2));
        assert_eq!(ids(all), [b"alice".to_vec(), b"zed".to_vec()]);
        let z = vectors
            .search(search(b"z", false))
            .await
            .unwrap()
            .into_inner();
        assert!(z.stats.is_none());
        assert_eq!(ids(z), [b"zed".to_vec()]);
        let checked = vectors
            .verify(VectorVerifyRequest { index: "uv".into() })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(checked.vectors, 2);
        let mut text = TextClient::new(channel(a).await);
        let ut = || TextIndexRequest { index: "ut".into() };
        let checked = text.verify(ut()).await.unwrap().into_inner();
        assert_eq!(
            (
                checked.documents,
                checked.forward_mismatch,
                checked.source_mismatch
            ),
            (2, 0, 0)
        );
        let e = vectors
            .search(VectorSearchRequest {
                index: "nope".into(),
                ..search(b"", false)
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "TableNotFound", "{e}");

        // A rebuild runs on every partition, the counts summed (PR 2c): nothing to change.
        let r = text.rebuild(ut()).await.unwrap().into_inner();
        assert_eq!((r.documents, r.changed), (2, 0));

        // The followed table stays while its indexes do (#1916's rule).
        let mut schema = SchemaClient::new(channel(b).await);
        let e = schema
            .drop_table(DropTableRequest {
                name: "users".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert!(
            e.message().contains("\"uv\"") && e.message().contains("\"ut\""),
            "{e}"
        );
        for name in ["uv", "ut"] {
            schema
                .drop_table(DropTableRequest { name: name.into() })
                .await
                .unwrap();
        }
        eventually("both partitions drop both indexes", async || {
            ["uv", "ut"]
                .iter()
                .all(|n| !has_table(&p1, n) && !has_table(&p2, n))
        })
        .await;
        schema
            .drop_table(DropTableRequest {
                name: "users".into(),
            })
            .await
            .unwrap();
    });
}

/// A vector index with ranges of its own routes by them; a create with neither `follows`
/// nor a placement, or with both, is refused, as is one following a table the map lacks.
#[test]
fn a_vector_index_of_its_own_ranges_routes_by_them() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, [p1, p2], [a, _b, c], _) = three_nodes(&rt);
    rt.block_on(async {
        let mut vectors_api = VectorsClient::new(channel(c).await);
        let e = vectors_api
            .create_index(vector_index("own"))
            .await
            .unwrap_err();
        assert_eq!(code(&e), "InvalidArgument", "{e}");
        let mut both = vector_index("own");
        both.follows = "users".into();
        both.placement = Some(TablePlacementRequest::default());
        let e = vectors_api.create_index(both).await.unwrap_err();
        assert_eq!(code(&e), "InvalidArgument", "{e}");
        let mut lost = vector_index("own");
        lost.follows = "nope".into();
        let e = vectors_api.create_index(lost).await.unwrap_err();
        assert_eq!(code(&e), "TableNotFound", "{e}");

        let mut own = vector_index("own");
        own.placement = Some(TablePlacementRequest {
            group: String::new(),
            splits: vec![b"k".to_vec().into()],
            partitions: vec!["p2".into(), "p1".into()],
        });
        vectors_api.create_index(own).await.unwrap();
        eventually("both partitions create it", async || {
            has_table(&p1, "own") && has_table(&p2, "own")
        })
        .await;
        insert(a, "own", b"j").await.unwrap();
        insert(a, "own", b"k").await.unwrap();
        insert(a, "own", b"z").await.unwrap();
        assert_eq!((vectors(&p1, "own"), vectors(&p2, "own")), (2, 1));
    });
}

/// The first index placement goes into the map only once every node keeps
/// `ClusterMap.indexes` (#1722): a 1.2 node would drop it and read as divergent.
#[test]
fn the_first_index_waits_for_every_node_to_keep_indexes() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, [p1, _p2], [a, _b, c], dirs) = three_nodes(&rt);
    rt.block_on(async {
        users_family(a).await;
        super::super::OLD_INDEXES
            .lock()
            .unwrap()
            .push(dirs[2].clone());
        let mut v = vector_index("uv");
        v.follows = "users".into();
        let e = VectorsClient::new(channel(a).await)
            .create_index(v.clone())
            .await
            .unwrap_err();
        super::super::OLD_INDEXES
            .lock()
            .unwrap()
            .retain(|d| *d != dirs[2]);
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert!(e.message().contains('c'), "{e}");
        assert!(!has_table(&p1, "uv"));
        // Once it keeps them, the create goes through.
        VectorsClient::new(channel(c).await)
            .create_index(v)
            .await
            .unwrap();
        eventually("p1 creates it", async || has_table(&p1, "uv")).await;
    });
}

/// A vector search across an index's partitions, with `nprobe` covering every list, is an
/// exact search over every row: its top `k` are a single index's. A search whose prefix
/// is in a reachable partition goes on while another partition is down; one that needs the
/// down partition, and a check, fail naming it.
#[test]
fn a_vector_search_across_partitions_is_exact_with_every_list_probed() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (mut stops, _dbs, [a, _b, c], _) = three_nodes(&rt);
    rt.block_on(async {
        let mut api = VectorsClient::new(channel(c).await);
        let mut own = vector_index("own");
        // p2 below "k", p1 from it.
        own.placement = Some(TablePlacementRequest {
            group: String::new(),
            splits: vec![b"k".to_vec().into()],
            partitions: vec!["p2".into(), "p1".into()],
        });
        api.create_index(own).await.unwrap();
        // A fixed seed's vectors, the ids spread over both ranges.
        let mut x: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 40) as f32 / (1u64 << 24) as f32
        };
        let mut rows = Vec::new();
        for i in 0..300u32 {
            let id = format!("{}{i:03}", char::from(b'a' + (i % 26) as u8)).into_bytes();
            let v = [next(), next()];
            let data: Vec<u8> = v.iter().flat_map(|x| x.to_le_bytes()).collect();
            let insert = VectorInsertRequest {
                index: "own".into(),
                id: id.clone().into(),
                vector: Some(Vector {
                    element: Element::F32 as i32,
                    dim: 2,
                    data: data.into(),
                }),
            };
            let mut tries = 0;
            // The partitions create the index at their reconcile, just after the map has it.
            while let Err(e) = api.insert(insert.clone()).await {
                tries += 1;
                assert!(tries < 200, "{e}");
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            rows.push((id, v));
        }
        let q = [0.5f32, 0.25];
        let query = Vector {
            element: Element::F32 as i32,
            dim: 2,
            data: q
                .iter()
                .flat_map(|x| x.to_le_bytes())
                .collect::<Vec<u8>>()
                .into(),
        };
        let lists = api
            .verify(VectorVerifyRequest {
                index: "own".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(lists.vectors, 300);
        let k = 10;
        let got = api
            .search(VectorSearchRequest {
                index: "own".into(),
                query: Some(query.clone()),
                k,
                nprobe: lists.lists as u32,
                stats: true,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let stats = got.stats.unwrap();
        assert_eq!(stats.lists_probed, stats.lists);
        let mut want: Vec<(f32, Vec<u8>)> = rows
            .iter()
            .map(|(id, v)| {
                let d = (v[0] - q[0]).powi(2) + (v[1] - q[1]).powi(2);
                (d, id.clone())
            })
            .collect();
        want.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let want: Vec<Vec<u8>> = want.into_iter().take(k as usize).map(|w| w.1).collect();
        let ids: Vec<Vec<u8>> = got.hits.iter().map(|h| h.id.to_vec()).collect();
        assert_eq!(ids, want);
        assert!(got.hits.windows(2).all(|w| w[0].distance <= w[1].distance));

        // b (p2, below "k") stops: a search within p1's range goes on, one needing p2
        // fails, and so does a check, naming p2.
        stops.remove(1).send(()).unwrap();
        let mut api = VectorsClient::new(channel(a).await);
        let search = |prefix: &[u8]| VectorSearchRequest {
            index: "own".into(),
            query: Some(query.clone()),
            k,
            id_prefix: prefix.to_vec().into(),
            ..Default::default()
        };
        let found = api.search(search(b"m")).await.unwrap().into_inner();
        assert!(!found.hits.is_empty());
        assert!(found.hits.iter().all(|h| h.id.starts_with(b"m")));
        let e = api.search(search(b"")).await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        let e = api
            .verify(VectorVerifyRequest {
                index: "own".into(),
            })
            .await
            .unwrap_err();
        assert!(e.message().contains("partition p2"), "{e}");
    });
}

/// One node owning two partitions of an index (`ForwardRequest.partition`'s reason): each
/// part is served on the partition it names, through the owner itself and through nodes
/// that forward to it, so a search and a check see both partitions once each.
#[test]
fn a_node_owning_two_partitions_of_an_index_serves_each_part_on_its_own() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, dbs, [a, b, c], _) = nodes(&rt, true);
    let (p1, p3) = (&dbs[0], &dbs[1]);
    rt.block_on(async {
        let mut own = vector_index("own");
        // Both on a: p1 below "k", p3 from it.
        own.placement = Some(TablePlacementRequest {
            group: String::new(),
            splits: vec![b"k".to_vec().into()],
            partitions: vec!["p1".into(), "p3".into()],
        });
        VectorsClient::new(channel(c).await)
            .create_index(own)
            .await
            .unwrap();
        eventually("a creates it on both partitions", async || {
            has_table(p1, "own") && has_table(p3, "own")
        })
        .await;
        for id in [&b"b"[..], b"j", b"m", b"z"] {
            insert(b, "own", id).await.unwrap();
        }
        assert_eq!((vectors(p1, "own"), vectors(p3, "own")), (2, 2));
        let query = Vector {
            element: Element::F32 as i32,
            dim: 2,
            data: [1.0f32, 2.0]
                .iter()
                .flat_map(|x| x.to_le_bytes())
                .collect::<Vec<u8>>()
                .into(),
        };
        for port in [a, b, c] {
            let mut api = VectorsClient::new(channel(port).await);
            let search = |prefix: &[u8]| VectorSearchRequest {
                index: "own".into(),
                query: Some(query.clone()),
                k: 10,
                id_prefix: prefix.to_vec().into(),
                ..Default::default()
            };
            let ids = |r: crate::serve::proto::VectorSearchResponse| -> Vec<Vec<u8>> {
                r.hits.into_iter().map(|h| h.id.to_vec()).collect()
            };
            let all = api.search(search(b"")).await.unwrap().into_inner();
            assert_eq!(
                ids(all),
                [b"b".to_vec(), b"j".to_vec(), b"m".to_vec(), b"z".to_vec()],
                "through port {port}"
            );
            let z = api.search(search(b"z")).await.unwrap().into_inner();
            assert_eq!(ids(z), [b"z".to_vec()], "through port {port}");
            let checked = api
                .verify(VectorVerifyRequest {
                    index: "own".into(),
                })
                .await
                .unwrap()
                .into_inner();
            assert_eq!(checked.vectors, 4, "through port {port}");
        }
    });
}

/// How long a test waits for a new text index's background fill: far longer than a small
/// source takes, for a loaded runner (audit120 on #2029).
const FILL: Duration = Duration::from_secs(30);

/// `Text.Put` through `port`, tried again while the partitions are still making the index
/// at their reconcile.
async fn put_text(port: u16, index: &str, id: &str, text: &str) {
    let mut api = TextClient::new(channel(port).await);
    let put = TextPutRequest {
        index: index.into(),
        id: id.as_bytes().to_vec().into(),
        text: text.into(),
    };
    let mut tries = 0;
    while let Err(e) = api.put(put.clone()).await {
        tries += 1;
        assert!(tries < 200, "{e}");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// A skewed corpus over `users`' two ranges (p1 below "m", p2 from it): `alpha` is rare in
/// p1 and in most of p2, `beta` the other way round, and `zeta` is in p2's `z` documents
/// only. With each partition's own statistics, the two would rank differently.
fn skewed_corpus() -> Vec<(String, String)> {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = |n: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % n
    };
    let mut docs = Vec::new();
    let mut doc = |id: String, words: &[(&str, u64)], next: &mut dyn FnMut(u64) -> u64| {
        let mut text = vec!["pad"; 4 + next(8) as usize];
        for (word, in_100) in words {
            for _ in 0..(1 + next(3)) {
                if next(100) < *in_100 {
                    text.push(word);
                }
            }
        }
        let last = text.len() - 1;
        text.swap(0, last);
        docs.push((id, text.join(" ")));
    };
    for i in 0..40 {
        let words = [("alpha", 10), ("beta", 70), ("gamma", 40)];
        doc(format!("a{i:03}"), &words, &mut next);
    }
    for i in 0..120 {
        let words = [("alpha", 80), ("beta", 5), ("gamma", 40)];
        doc(format!("n{i:03}"), &words, &mut next);
    }
    for i in 0..10 {
        let words = [("alpha", 30), ("zeta", 90), ("beta", 50)];
        doc(format!("z{i:03}"), &words, &mut next);
    }
    docs
}

/// #1722 PR 2b: a text search across an index's partitions gives the hits and scores one
/// index holding every document gives (a reference index on a database of its own, made
/// with the same options): terms, a phrase, a fuzzy term and mixes, with `id_prefix`
/// inside one range and across both, a term only one partition holds, and a skewed corpus
/// where each partition's own statistics would score differently. A fuzzy term cut at
/// `max_expansions` is flagged. An index on one partition answers in one round. With a partition's node stopped, a search fails unless it
/// has `allow_partial`, which scores with the partitions that answered, names what it
/// missed, and sets `partial` whenever anything is missing.
#[test]
fn a_text_search_across_partitions_scores_as_one_index() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (mut stops, [p1, p2], [a, _b, c], _) = three_nodes(&rt);
    // A new index fills from its source in the background, and a strict search on a
    // partition still filling answers Unavailable, so each step waits for its fill.
    let filled = |db: &pigeonhole::Pigeonhole, index: &str| {
        pigeonhole_text::Index::open(db, index).is_ok_and(|i| !i.fill_pending().unwrap())
    };
    let reference = test_dir();
    let db = pigeonhole::Pigeonhole::open(reference.join("ref.db"), pigeonhole::Options::default())
        .unwrap();
    let table = db
        .table("users")
        .unwrap()
        .family("f", pigeonhole::Family::default())
        .create_if_missing()
        .unwrap();
    let options = crate::serve::texts::options(&text_index("ut")).unwrap();
    let one = pigeonhole_text::Index::create(
        &db,
        "ut",
        pigeonhole_text::Source::new(&table, "f", b"body"),
        options,
    )
    .unwrap();
    // A fuzzy term with more than `max_expansions` (50) terms one edit from `abcd`, half of
    // them in each partition.
    let mut variants: Vec<String> = Vec::new();
    for ch in b'a'..=b'z' {
        let ch = char::from(ch);
        if ch != 'c' {
            variants.push(format!("ab{ch}d"));
        }
        if ch != 'd' {
            variants.push(format!("abc{ch}"));
        }
    }
    variants.push("abcde".into());
    variants.push("abc".into());
    assert!(variants.len() > 50);
    let mut docs = skewed_corpus();
    for (i, v) in variants.iter().enumerate() {
        let id = if i % 2 == 0 { "c" } else { "q" };
        docs.push((format!("{id}{i:03}"), format!("{v} pad pad")));
    }
    for (id, text) in &docs {
        one.put(id.as_bytes(), text).unwrap();
    }
    rt.block_on(async {
        users_family(a).await;
        TextClient::new(channel(c).await)
            .create_index(text_index("ut"))
            .await
            .unwrap();
        eventually_within(FILL, "both partitions fill ut", async || {
            filled(&p1, "ut") && filled(&p2, "ut")
        })
        .await;
        for (id, text) in &docs {
            put_text(c, "ut", id, text).await;
        }
        let mut api = TextClient::new(channel(c).await);
        let search = |syntax: &str, prefix: &str| crate::serve::proto::TextSearchRequest {
            index: "ut".into(),
            query: Some(crate::serve::proto::text_search_request::Query::Syntax(
                syntax.into(),
            )),
            k: 10,
            id_prefix: prefix.as_bytes().to_vec().into(),
            ..Default::default()
        };
        let close = |a: f64, b: f64| (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0);
        for syntax in [
            "alpha",
            "beta",
            "zeta",
            "alpha beta gamma",
            "zeta alpha",
            "\"alpha beta\"",
            "alhpa~1 gamma",
            "zeta~1",
        ] {
            for prefix in ["", "a", "n", "z"] {
                let got = api
                    .search(search(syntax, prefix))
                    .await
                    .unwrap()
                    .into_inner();
                let mut s = one.search(syntax, 10);
                if !prefix.is_empty() {
                    s = s.id_prefix(prefix.as_bytes().to_vec());
                }
                let want = s.run().unwrap();
                let ids = |h: &[crate::serve::proto::TextHit]| -> Vec<Vec<u8>> {
                    h.iter().map(|h| h.id.to_vec()).collect()
                };
                let want_ids: Vec<Vec<u8>> = want.iter().map(|h| h.id.clone()).collect();
                assert_eq!(ids(&got.hits), want_ids, "{syntax:?} under {prefix:?}");
                for (g, w) in got.hits.iter().zip(&want) {
                    assert!(
                        close(g.score, w.score),
                        "{syntax:?}: {} vs {}",
                        g.score,
                        w.score
                    );
                }
                assert!(!got.partial && got.missing.is_empty() && got.cut_clauses.is_empty());
                // Something to compare: every query matches across both ranges, and `zeta`
                // (in p2's `z` documents only) under `z`.
                if prefix.is_empty() || (syntax.starts_with("zeta") && prefix == "z") {
                    assert!(!got.hits.is_empty(), "{syntax:?} under {prefix:?}");
                }
            }
        }
        // The round matters: p1's own statistics score its `alpha` documents otherwise.
        let local = pigeonhole_text::Index::open(&p1, "ut")
            .unwrap()
            .search("alpha", 10)
            .run()
            .unwrap();
        let global = api.search(search("alpha", "a")).await.unwrap().into_inner();
        assert_eq!(local[0].id, global.hits[0].id.to_vec());
        assert!(!close(local[0].score, global.hits[0].score));

        // The cut: flagged on the cluster as on the one index (its expansions may differ).
        let got = api.search(search("abcd~1", "")).await.unwrap().into_inner();
        assert_eq!(got.cut_clauses, [0]);
        let (_, one_stats) = one.search("abcd~1", 10).run_with_stats().unwrap();
        assert!(one_stats.fuzzy[0].cut);

        // An index on one partition: one round, its own statistics the whole index's.
        SchemaClient::new(channel(c).await)
            .create_table(CreateTableRequest {
                name: "solo".into(),
                families: vec![FamilySpec {
                    name: "f".into(),
                    ..Default::default()
                }],
                placement: Some(TablePlacementRequest {
                    group: String::new(),
                    splits: Vec::new(),
                    partitions: vec!["p1".into()],
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        TextClient::new(channel(c).await)
            .create_index(CreateTextIndexRequest {
                source_table: "solo".into(),
                ..text_index("st")
            })
            .await
            .unwrap();
        eventually_within(FILL, "p1 fills st", async || filled(&p1, "st")).await;
        for (id, text) in docs.iter().take(30) {
            put_text(c, "st", id, text).await;
        }
        let got = api
            .search(crate::serve::proto::TextSearchRequest {
                index: "st".into(),
                ..search("alpha beta", "")
            })
            .await
            .unwrap()
            .into_inner();
        let own = pigeonhole_text::Index::open(&p1, "st")
            .unwrap()
            .search("alpha beta", 10)
            .run()
            .unwrap();
        assert!(!own.is_empty());
        assert_eq!(got.hits.len(), own.len());
        for (g, w) in got.hits.iter().zip(&own) {
            assert_eq!(g.id.to_vec(), w.id);
            assert!(close(g.score, w.score));
        }

        // And one on p2 only, for a search that misses only its one round below.
        SchemaClient::new(channel(c).await)
            .create_table(CreateTableRequest {
                name: "solo2".into(),
                families: vec![FamilySpec {
                    name: "f".into(),
                    ..Default::default()
                }],
                placement: Some(TablePlacementRequest {
                    group: String::new(),
                    splits: Vec::new(),
                    partitions: vec!["p2".into()],
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        TextClient::new(channel(c).await)
            .create_index(CreateTextIndexRequest {
                source_table: "solo2".into(),
                ..text_index("st2")
            })
            .await
            .unwrap();
        eventually_within(FILL, "p2 fills st2", async || filled(&p2, "st2")).await;
        put_text(c, "st2", "x1", "alpha pad").await;

        // b (p2, from "m") stops. A search needs every partition's statistics, so even one
        // whose prefix is in p1 fails, unless it allows a partial answer.
        stops.remove(1).send(()).unwrap();
        let mut api = TextClient::new(channel(a).await);
        let e = api.search(search("alpha", "a")).await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        assert!(e.message().contains("partition p2"), "{e}");
        let partial = |syntax: &str, prefix: &str| crate::serve::proto::TextSearchRequest {
            allow_partial: true,
            ..search(syntax, prefix)
        };
        let got = api
            .search(partial("alpha", "a"))
            .await
            .unwrap()
            .into_inner();
        assert!(got.partial);
        assert!(got.missing.is_empty(), "{:?}", got.missing);
        assert!(!got.hits.is_empty());
        assert!(got.hits.iter().all(|h| h.id.starts_with(b"a")));
        // Scored with p1's statistics alone: p1's own scores.
        let own = pigeonhole_text::Index::open(&p1, "ut")
            .unwrap()
            .search("alpha", 10)
            .id_prefix(b"a".to_vec())
            .run()
            .unwrap();
        assert_eq!(got.hits.len(), own.len());
        for (g, w) in got.hits.iter().zip(&own) {
            assert_eq!(g.id.to_vec(), w.id);
            assert!(close(g.score, w.score));
        }
        let got = api.search(partial("alpha", "")).await.unwrap().into_inner();
        assert!(got.partial);
        assert_eq!(got.missing.len(), 1, "{:?}", got.missing);
        let m = &got.missing[0];
        assert_eq!(
            (
                m.partition.as_str(),
                m.table.as_str(),
                &m.start[..],
                &m.end[..]
            ),
            ("p2", "ut", &b"m"[..], &b""[..])
        );
        assert!(got.hits.iter().all(|h| h.id.as_ref() < &b"m"[..]));
        // Missing only round 2 (an index on one partition has no round 1): `partial` too,
        // so a client reading it alone never takes a short answer as whole.
        let solo2 = |allow_partial| crate::serve::proto::TextSearchRequest {
            index: "st2".into(),
            allow_partial,
            ..search("alpha", "")
        };
        let e = api.search(solo2(false)).await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        let got = api.search(solo2(true)).await.unwrap().into_inner();
        assert!(got.hits.is_empty());
        assert!(got.partial);
        assert_eq!(got.missing.len(), 1, "{:?}", got.missing);
        assert_eq!(
            (
                got.missing[0].partition.as_str(),
                got.missing[0].table.as_str()
            ),
            ("p2", "st2")
        );
    });
}

/// #1722 PR 2c: `Text.Rebuild` through any node rebuilds the index on every partition and
/// sums the counts. The partitions one node serves rebuild in turn (a node's one rebuild
/// slot refuses a second at once), and a rebuild leaves the index searchable, strictly. A
/// partition that can't be reached fails the call, naming it.
#[test]
fn a_text_rebuild_runs_on_every_partition_of_its_index() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    // a serves p1 and p3, b p2; the databases are p1's, p3's and p2's.
    let (mut stops, dbs, [_a, b, c], _) = nodes(&rt, true);
    let filled = |db: &pigeonhole::Pigeonhole, index: &str| {
        pigeonhole_text::Index::open(db, index).is_ok_and(|i| !i.fill_pending().unwrap())
    };
    let documents = |db: &pigeonhole::Pigeonhole, index: &str| {
        pigeonhole_text::Index::open(db, index)
            .unwrap()
            .verify()
            .unwrap()
            .documents
    };
    // Source rows written around the index, as a client writing the table directly does:
    // only a rebuild indexes them.
    let around = |db: &pigeonhole::Pigeonhole, table: &str, first: char| {
        let t = db.table(table).unwrap().open().unwrap();
        for i in 0..20 {
            t.mutate(format!("{first}{i:02}").as_bytes())
                .put("f", b"body", b"alpha pad")
                .commit()
                .unwrap();
        }
    };
    rt.block_on(async {
        users_family(c).await;
        // `two`, split at "m" over p1 and p3: both a's.
        SchemaClient::new(channel(c).await)
            .create_table(CreateTableRequest {
                name: "two".into(),
                families: vec![FamilySpec {
                    name: "f".into(),
                    ..Default::default()
                }],
                placement: Some(TablePlacementRequest {
                    group: String::new(),
                    splits: vec![b"m".to_vec().into()],
                    partitions: vec!["p1".into(), "p3".into()],
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut api = TextClient::new(channel(c).await);
        api.create_index(text_index("ut")).await.unwrap();
        api.create_index(CreateTextIndexRequest {
            source_table: "two".into(),
            ..text_index("tt")
        })
        .await
        .unwrap();
        let [p1, p3, p2] = &dbs[..] else {
            unreachable!()
        };
        eventually_within(FILL, "every partition fills ut and tt", async || {
            filled(p1, "ut") && filled(p2, "ut") && filled(p1, "tt") && filled(p3, "tt")
        })
        .await;
        around(p1, "users", 'a');
        around(p2, "users", 'n');
        around(p1, "two", 'a');
        around(p3, "two", 'n');
        assert_eq!((documents(p1, "ut"), documents(p2, "ut")), (0, 0));

        // Across a's and b's partitions, through c.
        let r = api
            .rebuild(TextIndexRequest { index: "ut".into() })
            .await
            .unwrap()
            .into_inner();
        assert_eq!((r.documents, r.changed, r.invalid_utf8), (40, 40, 0));
        assert_eq!((documents(p1, "ut"), documents(p2, "ut")), (20, 20));
        let hits = api
            .search(crate::serve::proto::TextSearchRequest {
                index: "ut".into(),
                query: Some(crate::serve::proto::text_search_request::Query::Syntax(
                    "alpha".into(),
                )),
                k: 100,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(hits.hits.len(), 40);
        assert!(!hits.partial);

        // Both of tt's partitions on a, in turn, through b. Again, nothing changes.
        let mut through_b = TextClient::new(channel(b).await);
        let r = through_b
            .rebuild(TextIndexRequest { index: "tt".into() })
            .await
            .unwrap()
            .into_inner();
        assert_eq!((r.documents, r.changed), (40, 40));
        assert_eq!((documents(p1, "tt"), documents(p3, "tt")), (20, 20));
        let r = api
            .rebuild(TextIndexRequest { index: "tt".into() })
            .await
            .unwrap()
            .into_inner();
        assert_eq!((r.documents, r.changed), (40, 0));

        // b (p2) stops: ut's rebuild fails, naming p2.
        stops.remove(1).send(()).unwrap();
        let e = api
            .rebuild(TextIndexRequest { index: "ut".into() })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        assert!(e.message().contains("partition p2"), "{e}");
    });
}

/// A BulkLoad message of `ids`' vectors (2 dimensions, the first element the id's
/// position): the first message of a load names the index and `expected`.
fn bulk(index: &str, expected: u64, ids: &[String]) -> crate::serve::proto::VectorBulkLoadRequest {
    crate::serve::proto::VectorBulkLoadRequest {
        index: index.into(),
        expected,
        items: ids
            .iter()
            .enumerate()
            .map(|(i, id)| crate::serve::proto::VectorItem {
                id: id.as_bytes().to_vec().into(),
                vector: Some(Vector {
                    element: Element::F32 as i32,
                    dim: 2,
                    data: [i as f32, 1.0]
                        .iter()
                        .flat_map(|x| x.to_le_bytes())
                        .collect::<Vec<u8>>()
                        .into(),
                }),
            })
            .collect(),
    }
}

/// `Vectors.BulkLoad` through `port` of `messages`.
async fn load(
    port: u16,
    messages: Vec<crate::serve::proto::VectorBulkLoadRequest>,
) -> Result<(), tonic::Status> {
    VectorsClient::new(channel(port).await)
        .bulk_load(tonic::codegen::tokio_stream::iter(messages))
        .await
        .map(drop)
}

/// #1722 PR 3: `Vectors.BulkLoad` through any node splits the client's stream by id, each
/// vector landing in its id's partition: through a node serving neither partition, and
/// through one serving one of them (a load of its own beside a forwarded one). A partition
/// no id falls in is left alone. A load into an index that isn't empty fails, naming the
/// partition; so does one with a partition's node stopped.
#[test]
fn a_bulk_load_through_any_node_splits_by_id_across_partitions() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (mut stops, [p1, p2], [a, b, c], _) = three_nodes(&rt);
    // Ids on both sides of `users`' split at "m", interleaved in every message.
    let ids = |n: usize| -> Vec<String> {
        (0..n)
            .map(|i| {
                if i % 2 == 0 {
                    format!("a{i:03}")
                } else {
                    format!("n{i:03}")
                }
            })
            .collect()
    };
    rt.block_on(async {
        users_family(c).await;
        let mut api = VectorsClient::new(channel(c).await);
        for name in ["uv", "uv2", "uv3", "uv4"] {
            let mut v = vector_index(name);
            v.follows = "users".into();
            api.create_index(v).await.unwrap();
        }
        eventually_within(FILL, "both partitions create the indexes", async || {
            ["uv", "uv2", "uv3", "uv4"]
                .iter()
                .all(|n| has_table(&p1, n) && has_table(&p2, n))
        })
        .await;
        let all = ids(80);
        let messages = |index: &str| -> Vec<_> {
            all.chunks(20)
                .enumerate()
                .map(|(i, chunk)| {
                    let mut m = bulk(index, 80, chunk);
                    if i > 0 {
                        m.index.clear();
                        m.expected = 0;
                    }
                    m
                })
                .collect()
        };

        // Through c, which serves neither partition: both parts forwarded. An empty message
        // in the middle reaches no partition.
        let mut with_empty = messages("uv");
        with_empty.insert(2, crate::serve::proto::VectorBulkLoadRequest::default());
        load(c, with_empty).await.unwrap();
        assert_eq!((vectors(&p1, "uv"), vectors(&p2, "uv")), (40, 40));
        let checked = api
            .verify(VectorVerifyRequest { index: "uv".into() })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(checked.vectors, 80);

        // Through a, which serves p1: its part is a load of its own.
        load(a, messages("uv2")).await.unwrap();
        assert_eq!((vectors(&p1, "uv2"), vectors(&p2, "uv2")), (40, 40));

        // Every id in p1: p2's index is never opened, and stays empty.
        let low: Vec<String> = (0..30).map(|i| format!("b{i:03}")).collect();
        load(c, vec![bulk("uv3", 30, &low)]).await.unwrap();
        assert_eq!((vectors(&p1, "uv3"), vectors(&p2, "uv3")), (30, 0));

        // Again into a loaded index: refused by its partitions, naming one.
        let e = load(c, messages("uv")).await.unwrap_err();
        assert!(e.message().contains("partition p"), "{e}");
        assert_eq!((vectors(&p1, "uv"), vectors(&p2, "uv")), (40, 40));

        // b (p2) stops. Its feature answer is still cached, so the load starts: p2's part
        // can't connect, and the load fails naming it. p1's part, which took the first
        // message's vectors, keeps them, and the error says so.
        stops.remove(1).send(()).unwrap();
        // Once b refuses connections: a forward that reached it while it shut down answers
        // OutcomeUnknown instead, rightly (a Linux CI race on #2087).
        eventually("b to stop", async || {
            tokio::net::TcpStream::connect(("127.0.0.1", b))
                .await
                .is_err()
        })
        .await;
        let e = load(a, messages("uv4")).await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        assert!(e.message().contains("partition p2"), "{e}");
        let kept = vectors(&p1, "uv4");
        assert!(kept <= 40, "{kept}");
        assert_eq!(kept > 0, e.message().contains("loaded"), "{kept}: {e}");
    });
}

/// #1722 PR 3: a node serving a part of the index that runs a build before
/// `ForwardBulkLoad` (no `index_bulk_load` in its `Status`) makes the load `Unsupported`,
/// naming it, before anything is read: the other partition takes nothing.
#[test]
fn a_bulk_load_with_an_older_node_holding_a_part_is_refused_up_front() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, [p1, p2], [a, _b, c], dirs) = three_nodes(&rt);
    super::super::OLD_BULK_LOAD
        .lock()
        .unwrap()
        .push(dirs[1].clone());
    rt.block_on(async {
        users_family(c).await;
        let mut v = vector_index("uv");
        v.follows = "users".into();
        VectorsClient::new(channel(c).await)
            .create_index(v)
            .await
            .unwrap();
        eventually_within(FILL, "both partitions create uv", async || {
            has_table(&p1, "uv") && has_table(&p2, "uv")
        })
        .await;
        let ids: Vec<String> = ["a1", "a2", "n1", "n2"].map(str::to_owned).to_vec();
        let e = load(a, vec![bulk("uv", 4, &ids)]).await.unwrap_err();
        assert_eq!(code(&e), "Unsupported", "{e}");
        assert!(e.message().contains("b (an older build)"), "{e}");
        assert_eq!((vectors(&p1, "uv"), vectors(&p2, "uv")), (0, 0));
    });
    super::super::OLD_BULK_LOAD
        .lock()
        .unwrap()
        .retain(|d| *d != dirs[1]);
}

/// #1722 PR 3 (audit120's S1, simp724's M1 on #2045): every part of a load reserves the
/// whole load's worst case, so a node serving two partitions of the index whose
/// `--bulk-load-bytes` holds only one part refuses the load at once, saying so, rather than
/// waiting for the bytes its own other part holds. Through a node serving neither (the
/// node's first part refused as it opens) and through the node itself (before anything is
/// read): nothing is loaded either way, and the error isn't a partial load's.
#[test]
fn a_node_that_cant_hold_a_part_for_each_of_its_partitions_refuses_the_load_at_once() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    // One part of 80 2-dim vectors holds its 50,000-vector sample, about 8.8 MB: room for
    // one, not two.
    let mut limits = crate::serve::server_tests::limits();
    limits.bulk_load_bytes = 12 << 20;
    // a serves p1 and p3, b p2; the databases are p1's, p3's and p2's.
    let (_stops, dbs, [a, _b, c], _) = nodes_limited(&rt, true, limits);
    let [p1, p3, _p2] = &dbs[..] else {
        unreachable!()
    };
    rt.block_on(async {
        // `two`, split at "m" over p1 and p3: both a's.
        SchemaClient::new(channel(c).await)
            .create_table(CreateTableRequest {
                name: "two".into(),
                families: vec![FamilySpec {
                    name: "f".into(),
                    ..Default::default()
                }],
                placement: Some(TablePlacementRequest {
                    group: String::new(),
                    splits: vec![b"m".to_vec().into()],
                    partitions: vec!["p1".into(), "p3".into()],
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut v = vector_index("tv");
        v.follows = "two".into();
        VectorsClient::new(channel(c).await)
            .create_index(v)
            .await
            .unwrap();
        eventually_within(FILL, "a creates tv on p1 and p3", async || {
            has_table(p1, "tv") && has_table(p3, "tv")
        })
        .await;
        let ids: Vec<String> = (0..80)
            .map(|i| format!("{}{i:03}", if i % 2 == 0 { 'a' } else { 'n' }))
            .collect();
        for port in [c, a] {
            let started = std::time::Instant::now();
            let e = load(port, vec![bulk("tv", 80, &ids)]).await.unwrap_err();
            assert_eq!(code(&e), "InvalidArgument", "through {port}: {e}");
            assert!(
                e.message().contains("this node serves 2 of them"),
                "through {port}: {e}"
            );
            assert!(!e.message().contains("loaded"), "through {port}: {e}");
            // At once, not after the request timeout.
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "through {port}"
            );
            assert_eq!((vectors(p1, "tv"), vectors(p3, "tv")), (0, 0));
        }
    });
}

/// #1722 PR 3 (audit120's S2 on #2045): a part that fails mid-load (p2 meets a vector of
/// another dimension) fails the load, naming p2 and saying vectors may have been loaded.
/// p1 keeps what it took, and has stopped by the time the client has the error: a load
/// right after finds its slot free. A part that fails with the client's last message,
/// after its stream ended, does the same.
#[test]
fn a_part_that_fails_fails_the_load_and_the_others_keep_what_they_took() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, [p1, p2], [_a, _b, c], _) = three_nodes(&rt);
    let ids = |prefix: char, from: usize, n: usize| -> Vec<String> {
        (from..from + n)
            .map(|i| format!("{prefix}{i:03}"))
            .collect()
    };
    // `ids`' vectors, with the one at `bad` 3-dimensional.
    let with_bad = |index: &str, ids: &[String], bad: usize| {
        let mut m = bulk(index, 0, ids);
        m.index.clear();
        let v = m.items[bad].vector.as_mut().unwrap();
        v.dim = 3;
        v.data = [0.0f32, 1.0, 2.0]
            .iter()
            .flat_map(|x| x.to_le_bytes())
            .collect::<Vec<u8>>()
            .into();
        m
    };
    rt.block_on(async {
        users_family(c).await;
        let mut api = VectorsClient::new(channel(c).await);
        for name in ["uf", "ug", "uh"] {
            let mut v = vector_index(name);
            v.follows = "users".into();
            api.create_index(v).await.unwrap();
        }
        eventually_within(FILL, "both partitions create the indexes", async || {
            ["uf", "ug", "uh"]
                .iter()
                .all(|n| has_table(&p1, n) && has_table(&p2, n))
        })
        .await;

        // Mid-load: the second message's third vector, p2's, is 3-dimensional. p1's first
        // part is large enough that its write takes a while.
        let mut second = ids('a', 2000, 10);
        second.extend(ids('n', 10, 10));
        let messages = vec![
            bulk("uf", 4000, &[ids('a', 0, 2000), ids('n', 0, 10)].concat()),
            with_bad("uf", &second, 12),
            bulk("", 0, &ids('a', 2010, 10)),
        ];
        let e = load(c, messages).await.unwrap_err();
        assert!(e.message().contains("partition p2"), "{e}");
        assert!(e.message().contains("loaded"), "{e}");
        // p2 has its vectors before the bad one; p1 at least its first message's, all
        // written by the time the client has the error (audit120's S4).
        assert_eq!(vectors(&p2, "uf"), 12);
        let kept = vectors(&p1, "uf");
        assert!((2000..=2020).contains(&kept), "{kept}");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(vectors(&p1, "uf"), kept, "p1's load was still running");
        // p1's load has stopped: its slot is free for the next.
        let started = std::time::Instant::now();
        load(c, vec![bulk("ug", 10, &ids('a', 0, 10))])
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(10));
        assert_eq!(vectors(&p1, "ug"), 10);

        // At the end: the last message's last vector, p2's, is the bad one.
        let last = [ids('a', 10, 5), ids('n', 10, 5)].concat();
        let messages = vec![
            bulk("uh", 100, &[ids('a', 0, 10), ids('n', 0, 10)].concat()),
            with_bad("uh", &last, 9),
        ];
        let e = load(c, messages).await.unwrap_err();
        assert!(e.message().contains("partition p2"), "{e}");
        assert!(e.message().contains("loaded"), "{e}");
        assert_eq!((vectors(&p1, "uh"), vectors(&p2, "uh")), (15, 14));
    });
}

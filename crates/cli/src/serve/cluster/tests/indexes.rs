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
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    map.partitions.retain(|p| p.id != "p3");
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
            None,
            None,
        );
        stops.push(stop);
        dbs.extend(d);
    }
    let [p1, p2]: [pigeonhole::Pigeonhole; 2] = dbs.try_into().ok().unwrap();
    (stops, [p1, p2], ports, dirs)
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
        assert_eq!((checked.documents, checked.forward_mismatch), (2, 0));
        let e = vectors
            .search(VectorSearchRequest {
                index: "nope".into(),
                ..search(b"", false)
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "TableNotFound", "{e}");

        // Rebuilds across partitions come with PR 2.
        let e = text.rebuild(ut()).await.unwrap_err();
        assert_eq!(code(&e), "Unsupported", "{e}");
        assert!(
            e.message().contains("isn't supported on a cluster yet"),
            "{e}"
        );

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
